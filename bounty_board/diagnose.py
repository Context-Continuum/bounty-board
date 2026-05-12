"""bounty_board.diagnose — Option D self-diagnostic replay.

The agent reads its own failure dossier (stack + prompt_state at
failure) PLUS the most-similar prior SUCCESSFUL task's trajectory,
sees the diff, and emits a `diagnose` event with its hypothesis +
optional proposed patch + confidence.

If the agent's confidence is below threshold OR no patch proposed,
the caller can route to structured escalation (W1 pattern) instead
of blind replay. The substrate-discipline win: every replay carries
the substrate's accumulated knowledge of "what made this fail vs.
what made similar things succeed."

Per design lane decision_id ``cluster_brokerless_task_queue_pitch_v0``
+ design notes scratchpad id=557541232031614450. No schema changes
needed; reads from existing `tasks` + `task_events` rows.

Surface:

  diagnose.find_similar_success(queue, payload_signature)
                                              -> DLQEntry-like | None
  diagnose.build_dossier(queue, dlq_entry)    -> dict (the agent's
                                              input for self-diagnosis)
  diagnose.compute_payload_diff(failed_payload, success_payload)
                                              -> dict
  diagnose.emit_diagnosis(queue, task_id, agent_id, *, hypothesis,
                          proposed_patch=None, confidence=0.0)
                                              -> int (event id)

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from bounty_board.dlq import DLQEntry, TaskEvent

if TYPE_CHECKING:
    from bounty_board.queue import Queue


# Threshold below which agent should escalate, not auto-replay.
DEFAULT_CONFIDENCE_FLOOR = 0.5


@dataclass
class SuccessSnapshot:
    """A trimmed view of a prior successful task — enough for the
    diff but not the full DLQEntry (we don't need fail_event etc.)."""

    task_id: str
    payload: dict
    completed_at: float
    trajectory: list[TaskEvent] = field(default_factory=list)


# ─── lookup ─────────────────────────────────────────────────────────


def find_similar_success(
    queue: Queue, payload_signature: str, *,
    exclude_task_id: str | None = None,
) -> SuccessSnapshot | None:
    """Return the most recent successfully-completed task with the
    given ``payload_signature``, or ``None``.

    ``exclude_task_id`` lets callers skip a specific task (e.g. when
    diagnosing a replay, the parent task may itself be in the
    success history — exclude it so we don't diff against ourselves).
    """
    if exclude_task_id is not None:
        row = queue._conn.execute(
            """
            SELECT * FROM tasks
            WHERE status = 'done'
              AND payload_signature = ?
              AND id != ?
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            (payload_signature, exclude_task_id),
        ).fetchone()
    else:
        row = queue._conn.execute(
            """
            SELECT * FROM tasks
            WHERE status = 'done'
              AND payload_signature = ?
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            (payload_signature,),
        ).fetchone()
    if row is None:
        return None
    trajectory = [
        TaskEvent(
            id=e["id"],
            task_id=e["task_id"],
            event_kind=e["event_kind"],
            ts=e["ts"],
            agent_id=e["agent_id"],
            payload=(json.loads(e["payload_json"])
                     if e["payload_json"] else None),
            token_count=e["token_count"],
        )
        for e in queue._conn.execute(
            "SELECT * FROM task_events WHERE task_id = ? ORDER BY id ASC",
            (row["id"],),
        ).fetchall()
    ]
    return SuccessSnapshot(
        task_id=row["id"],
        payload=json.loads(row["payload_json"]),
        completed_at=row["completed_at"],
        trajectory=trajectory,
    )


# ─── diff ───────────────────────────────────────────────────────────


def compute_payload_diff(failed_payload: dict, success_payload: dict) -> dict:
    """Structural diff of two payload dicts. Returns:

        {
          "size_delta_bytes": int (failed_size - success_size),
          "new_fields": list[str],       # in failed not in success
          "removed_fields": list[str],   # in success not in failed
          "changed_fields": list[str],   # in both but different values
        }

    No deep recursion — top-level only at V1. Enough to surface
    "agent A had a 200KB extra field" or "agent B's payload was
    missing the auth_token field" without needing structural recursion.
    """
    failed_bytes = len(json.dumps(failed_payload, default=str))
    success_bytes = len(json.dumps(success_payload, default=str))
    failed_keys = set(failed_payload.keys())
    success_keys = set(success_payload.keys())
    new = sorted(failed_keys - success_keys)
    removed = sorted(success_keys - failed_keys)
    shared = failed_keys & success_keys
    changed = sorted(
        k for k in shared
        if failed_payload[k] != success_payload[k]
    )
    return {
        "size_delta_bytes": failed_bytes - success_bytes,
        "new_fields": new,
        "removed_fields": removed,
        "changed_fields": changed,
    }


# ─── dossier ────────────────────────────────────────────────────────


def build_dossier(queue: Queue, dlq_entry: DLQEntry) -> dict:
    """Assemble the full failure dossier the AGENT reads to self-
    diagnose.

    Returns a dict shaped:

        {
          "original_payload": <the failed task's payload>,
          "failure_dossier": {
            "task_id": str,
            "attempts": int,
            "stack": str | None,
            "prompt_state": dict | None,
            "diff_vs_last_success": dict | None,
            "last_success_task_id": str | None,
          },
          "self_diagnosis_prompt": str,
        }

    When no prior-success exists for this signature, ``diff_vs_last_
    success`` and ``last_success_task_id`` are None — the agent
    sees that and knows it's the FIRST failure of this signature,
    which itself is signal (often: "this signature is new and the
    workflow hasn't been validated yet, escalate").
    """
    fail_ev = dlq_entry.final_fail_event
    stack = fail_ev.payload.get("stack") if fail_ev and fail_ev.payload else None
    prompt_state = (
        fail_ev.payload.get("prompt_state")
        if fail_ev and fail_ev.payload else None
    )

    similar = find_similar_success(
        queue, dlq_entry.payload_signature,
        exclude_task_id=dlq_entry.task_id,
    )
    if similar is not None:
        diff = compute_payload_diff(dlq_entry.payload, similar.payload)
        last_success_id: str | None = similar.task_id
    else:
        diff = None
        last_success_id = None

    return {
        "original_payload": dlq_entry.payload,
        "failure_dossier": {
            "task_id": dlq_entry.task_id,
            "attempts": dlq_entry.attempts,
            "stack": stack,
            "prompt_state": prompt_state,
            "diff_vs_last_success": diff,
            "last_success_task_id": last_success_id,
        },
        "self_diagnosis_prompt": _build_self_diagnosis_prompt(
            has_prior_success=similar is not None,
            attempts=dlq_entry.attempts,
        ),
    }


def _build_self_diagnosis_prompt(*, has_prior_success: bool,
                                 attempts: int) -> str:
    """Render the instruction text the agent reads alongside the
    dossier. Stable text helps the agent's reasoning be deterministic
    across replays."""
    if has_prior_success:
        return (
            f"Your previous attempt (attempt #{attempts}) failed. Here's "
            f"the failure dossier including a structural diff against "
            f"the most recent SUCCESSFUL task of this signature. "
            f"Diagnose what went wrong, then either propose a patch "
            f"(payload-transformer) if you can confidently fix the input "
            f"shape, OR escalate with a structured reason. Do NOT retry "
            f"blindly. Output a `diagnose` event via "
            f"diagnose.emit_diagnosis() with your hypothesis, optional "
            f"proposed_patch, and a confidence in [0.0, 1.0]."
        )
    return (
        f"Your previous attempt (attempt #{attempts}) failed. NO prior "
        f"successful task of this signature exists — you're the first "
        f"agent to encounter this shape, so there's no baseline to diff "
        f"against. Likely actions: (a) escalate with reason="
        f"'no_baseline_for_signature' so an operator or supervisor can "
        f"hand-validate, OR (b) propose a patch only if you have high "
        f"confidence from the stack trace alone. Emit a `diagnose` "
        f"event with your assessment."
    )


# ─── emit ───────────────────────────────────────────────────────────


def emit_diagnosis(queue: Queue, task_id: str, agent_id: str, *,
                   hypothesis: str,
                   proposed_patch: dict | None = None,
                   confidence: float = 0.0,
                   token_count: int = 0) -> int:
    """Write a `diagnose` event row to task_events for ``task_id``.

    Returns the new event_id.

    The diagnosis row is the canonical artifact of Option D: it
    crystallizes the agent's reasoning about WHY a task failed.
    Downstream consumers (the patches.py auto-promote logic in #7,
    or supervisors reviewing escalation) read these rows to decide
    next steps.

    ``confidence`` is a float in [0.0, 1.0]; callers should escalate
    when confidence < DEFAULT_CONFIDENCE_FLOOR (0.5) rather than
    auto-replay with a low-confidence patch.

    ``proposed_patch`` shape is a structured payload-transformer:

        {"kind": "prepend_system_msg" | "truncate_field" | "swap_model" | ...,
         "args": {...kind-specific...}}

    Stored verbatim in payload_json under the ``proposed_patch`` key.
    The #7 patches.py module reads this when promoting candidates.
    """
    if not 0.0 <= confidence <= 1.0:
        raise ValueError(
            f"confidence must be in [0.0, 1.0]; got {confidence!r}"
        )
    now = time.time()
    payload = {
        "hypothesis": hypothesis,
        "proposed_patch": proposed_patch,
        "confidence": confidence,
    }
    with queue._conn:
        cur = queue._conn.execute(
            """
            INSERT INTO task_events
                (task_id, event_kind, ts, agent_id, payload_json,
                 token_count)
            VALUES (?, 'diagnose', ?, ?, ?, ?)
            """,
            (task_id, now, agent_id, json.dumps(payload), token_count),
        )
        event_id = cur.lastrowid
    return event_id
