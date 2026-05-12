"""bounty_board.patches — #7 replay-time patch substrate.

A patch is a structured ``payload_transformer`` pinned to a
``payload_signature``. Agents propose patches when self-diagnosing a
failure (see ``diagnose.py``'s ``proposed_patch`` field). The
substrate keeps the patch in ``status='candidate'`` and exposes it
ONLY to the proposing agent's own replays — that's the experiment
boundary. Once ``n_successes`` crosses the queue's promotion
threshold (default 3), the substrate auto-promotes to
``status='canonical'`` and the patch then applies to every agent
claiming a matching signature.

Per design lane decision_id ``cluster_brokerless_task_queue_pitch_v0``
+ design notes scratchpad id=557541232031614450. Schema substrate
lives in ``0001_initial.sql`` under the ``patches`` table; no new
migrations required.

Surface:

  patches.propose(queue, payload_signature, transformer, *, agent_id)
                                              -> int (new patch id)
  patches.get(queue, patch_id)                -> Patch | None
  patches.list_by_signature(queue, payload_signature, *, status=None)
                                              -> list[Patch]
  patches.find_applicable(queue, payload_signature, *, agent_id)
                                              -> list[Patch]
  patches.record_outcome(queue, patch_id, *, success)  -> None
  patches.promote_eligible(queue, *, threshold=3)      -> int
  patches.retire(queue, patch_id)                      -> None
  patches.apply_transformer(payload, transformer)      -> dict

Plus the two breadcrumb-named ergonomic wrappers per design lane:

  PatchProposer(queue, agent_id).propose(signature, transformer) -> int
  PatchPromoter(queue, threshold=3).tick() -> int

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import copy
import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from bounty_board.queue import Queue


# Default n_successes threshold above which a candidate auto-promotes
# to canonical. Tunable per-call on ``promote_eligible``; the
# breadcrumb-canonical default is 3.
DEFAULT_PROMOTION_THRESHOLD = 3

# Statuses a patch row can take. Mirrors the CHECK constraint on the
# ``patches`` table in ``0001_initial.sql``.
PATCH_STATUSES: tuple[str, ...] = ("candidate", "canonical", "retired")


# ─── transformer kinds ──────────────────────────────────────────────


class UnknownTransformerKindError(ValueError):
    """Raised when ``apply_transformer`` sees a ``kind`` it doesn't
    know how to apply. Better to fail loudly than to silently no-op
    — a malformed candidate must never get promoted to canonical."""


# Known V1 transformer kinds. The diagnose.py docstring lists these as
# the canonical set. New kinds should land here together with a
# matching apply branch — adding a kind to one place but not the other
# silently breaks promotion paths.
KNOWN_KINDS: tuple[str, ...] = (
    "prepend_system_msg",
    "truncate_field",
    "swap_model",
)


def apply_transformer(payload: dict, transformer: dict) -> dict:
    """Apply ``transformer`` to ``payload``, returning a NEW dict.

    Never mutates the input. The shape of ``transformer`` is

        {"kind": <one of KNOWN_KINDS>, "args": {...kind-specific...}}

    Per-kind semantics:

      * ``prepend_system_msg`` — args ``{"text": str}``. If the payload
        already has a ``messages`` list, prepend a
        ``{"role": "system", "content": text}`` dict to it. Else set
        ``payload["system_prompt"] = text`` (V1; replaces any existing
        value).
      * ``truncate_field`` — args ``{"field": str, "max_chars": int}``.
        If ``payload[field]`` is a string longer than ``max_chars``,
        truncate to ``max_chars`` characters. No-op when the field is
        absent or already short enough.
      * ``swap_model`` — args ``{"to": str}``. Set ``payload["model"]
        = to`` unconditionally.

    Unknown kinds raise ``UnknownTransformerKindError``. Missing or
    malformed args raise ``ValueError``.
    """
    if not isinstance(transformer, dict):
        raise ValueError(
            f"transformer must be a dict; got {type(transformer).__name__}"
        )
    kind = transformer.get("kind")
    args = transformer.get("args", {})
    if not isinstance(kind, str):
        raise ValueError(f"transformer.kind must be a str; got {kind!r}")
    if not isinstance(args, dict):
        raise ValueError(
            f"transformer.args must be a dict; got {type(args).__name__}"
        )

    out = copy.deepcopy(payload)
    if kind == "prepend_system_msg":
        text = args.get("text")
        if not isinstance(text, str):
            raise ValueError(
                f"prepend_system_msg requires args.text:str; got {text!r}"
            )
        messages = out.get("messages")
        if isinstance(messages, list):
            out["messages"] = [
                {"role": "system", "content": text}, *messages,
            ]
        else:
            out["system_prompt"] = text
        return out

    if kind == "truncate_field":
        field = args.get("field")
        max_chars = args.get("max_chars")
        if not isinstance(field, str):
            raise ValueError(
                f"truncate_field requires args.field:str; got {field!r}"
            )
        if not isinstance(max_chars, int) or max_chars < 0:
            raise ValueError(
                f"truncate_field requires args.max_chars:int>=0; "
                f"got {max_chars!r}"
            )
        v = out.get(field)
        if isinstance(v, str) and len(v) > max_chars:
            out[field] = v[:max_chars]
        return out

    if kind == "swap_model":
        to = args.get("to")
        if not isinstance(to, str):
            raise ValueError(
                f"swap_model requires args.to:str; got {to!r}"
            )
        out["model"] = to
        return out

    raise UnknownTransformerKindError(
        f"unknown transformer kind {kind!r}; "
        f"known kinds: {KNOWN_KINDS}"
    )


# ─── data shape ─────────────────────────────────────────────────────


@dataclass
class Patch:
    """A hydrated ``patches`` row. Read-only view; mutate via the
    module functions, not by reaching into the dataclass."""

    id: int
    payload_signature: str
    transformer: dict
    status: str
    n_successes: int
    n_failures: int
    proposed_by_agent_id: str
    proposed_at: float
    promoted_at: float | None

    @property
    def is_canonical(self) -> bool:
        return self.status == "canonical"

    @property
    def is_candidate(self) -> bool:
        return self.status == "candidate"

    @property
    def is_retired(self) -> bool:
        return self.status == "retired"


def _row_to_patch(row: Any) -> Patch:
    return Patch(
        id=row["id"],
        payload_signature=row["payload_signature"],
        transformer=json.loads(row["transformer_json"]),
        status=row["status"],
        n_successes=row["n_successes"],
        n_failures=row["n_failures"],
        proposed_by_agent_id=row["proposed_by_agent_id"],
        proposed_at=row["proposed_at"],
        promoted_at=row["promoted_at"],
    )


# ─── propose / lifecycle ────────────────────────────────────────────


def propose(queue: Queue, payload_signature: str, transformer: dict,
            *, agent_id: str) -> int:
    """Record a new candidate patch. Returns the new patch id.

    Validates the transformer shape (must be apply-able) before
    insert; an unapply-able transformer is rejected at the boundary
    rather than landing as a poisoned candidate row.
    """
    # Validate against an empty payload — apply_transformer raises on
    # shape errors regardless of whether the transformer actually
    # fires. UnknownTransformerKindError + ValueError both propagate.
    apply_transformer({}, transformer)

    now = time.time()
    with queue._conn:
        cur = queue._conn.execute(
            """
            INSERT INTO patches (
                payload_signature, transformer_json, status,
                n_successes, n_failures,
                proposed_by_agent_id, proposed_at
            ) VALUES (?, ?, 'candidate', 0, 0, ?, ?)
            """,
            (payload_signature, json.dumps(transformer),
             agent_id, now),
        )
        return cur.lastrowid


def get(queue: Queue, patch_id: int) -> Patch | None:
    """Look up a single patch by id, or None."""
    row = queue._conn.execute(
        "SELECT * FROM patches WHERE id = ?", (patch_id,),
    ).fetchone()
    return _row_to_patch(row) if row is not None else None


def list_by_signature(queue: Queue, payload_signature: str,
                      *, status: str | None = None) -> list[Patch]:
    """All patches matching ``payload_signature``, optionally further
    filtered by ``status``. Ordered by proposed_at ASC (oldest first).
    """
    if status is not None:
        if status not in PATCH_STATUSES:
            raise ValueError(
                f"status must be one of {PATCH_STATUSES}; got {status!r}"
            )
        rows = queue._conn.execute(
            """
            SELECT * FROM patches
            WHERE payload_signature = ? AND status = ?
            ORDER BY proposed_at ASC
            """,
            (payload_signature, status),
        ).fetchall()
    else:
        rows = queue._conn.execute(
            """
            SELECT * FROM patches
            WHERE payload_signature = ?
            ORDER BY proposed_at ASC
            """,
            (payload_signature,),
        ).fetchall()
    return [_row_to_patch(r) for r in rows]


def find_applicable(queue: Queue, payload_signature: str,
                    *, agent_id: str) -> list[Patch]:
    """Patches that should apply when ``agent_id`` claims a task with
    ``payload_signature``.

    Returns canonical patches for the signature (apply to everyone)
    PLUS candidate patches proposed by ``agent_id`` themselves (the
    experiment-boundary rule: a candidate applies only to its
    proposer's replays until it earns canonical status). Excludes
    retired patches and other agents' candidates. Ordered canonical-
    first, then candidates by proposed_at ASC — apply in this order
    when composing transformers on a single payload.
    """
    rows = queue._conn.execute(
        """
        SELECT * FROM patches
        WHERE payload_signature = ?
          AND (
            status = 'canonical'
            OR (status = 'candidate' AND proposed_by_agent_id = ?)
          )
        ORDER BY
          CASE status WHEN 'canonical' THEN 0 ELSE 1 END ASC,
          proposed_at ASC
        """,
        (payload_signature, agent_id),
    ).fetchall()
    return [_row_to_patch(r) for r in rows]


def record_outcome(queue: Queue, patch_id: int, *, success: bool) -> None:
    """Bump ``n_successes`` or ``n_failures`` on a patch.

    Callers wire this in at the queue lifecycle: when a task that had
    a patch applied completes, call with ``success=True``; on
    ``fail`` (max attempts exhausted), call with ``success=False``.
    Raises ``ValueError`` if the patch doesn't exist.
    """
    col = "n_successes" if success else "n_failures"
    with queue._conn:
        cur = queue._conn.execute(
            f"UPDATE patches SET {col} = {col} + 1 WHERE id = ?",
            (patch_id,),
        )
        if cur.rowcount == 0:
            raise ValueError(f"no patch with id {patch_id!r}")


def promote_eligible(queue: Queue, *,
                     threshold: int = DEFAULT_PROMOTION_THRESHOLD) -> int:
    """Promote every ``candidate`` whose ``n_successes >= threshold``
    to ``canonical``. Stamps ``promoted_at`` with the current time.
    Returns count promoted.

    Idempotent: a patch already canonical is unaffected, so calling
    this on a tick loop is safe regardless of cadence. Atomic via
    SQLite's implicit transaction wrapping the UPDATE.
    """
    if threshold < 1:
        raise ValueError(f"threshold must be >= 1; got {threshold!r}")
    now = time.time()
    with queue._conn:
        cur = queue._conn.execute(
            """
            UPDATE patches
            SET status = 'canonical', promoted_at = ?
            WHERE status = 'candidate' AND n_successes >= ?
            """,
            (now, threshold),
        )
        return cur.rowcount


def retire(queue: Queue, patch_id: int) -> None:
    """Mark a patch as retired. Retired patches never apply (excluded
    from ``find_applicable``). Useful for ops walking back a
    canonical patch that turned out to regress in the field. Raises
    ``ValueError`` if the patch doesn't exist."""
    with queue._conn:
        cur = queue._conn.execute(
            "UPDATE patches SET status = 'retired' WHERE id = ?",
            (patch_id,),
        )
        if cur.rowcount == 0:
            raise ValueError(f"no patch with id {patch_id!r}")


# ─── ergonomic wrappers ─────────────────────────────────────────────


class PatchProposer:
    """Agent-side ergonomic wrapper. Binds a ``Queue`` + ``agent_id``
    so the agent's diagnose-replay path can call
    ``proposer.propose(sig, transformer)`` without re-passing agent_id.

    Per breadcrumb spec id=557541232031614450 — the canonical
    proposer-side surface for #7 patches.
    """

    def __init__(self, queue: Queue, agent_id: str):
        self._queue = queue
        self._agent_id = agent_id

    def propose(self, payload_signature: str, transformer: dict) -> int:
        return propose(
            self._queue, payload_signature, transformer,
            agent_id=self._agent_id,
        )


class PatchPromoter:
    """Substrate-side sweeper. Run ``tick()`` on a cadence (operator
    loop, inspect dashboard refresh, etc.) to surface eligible
    candidates into canonical.

    Per breadcrumb spec id=557541232031614450 — the canonical
    substrate-side surface for #7 patches.
    """

    def __init__(self, queue: Queue, *,
                 threshold: int = DEFAULT_PROMOTION_THRESHOLD):
        self._queue = queue
        self._threshold = threshold

    def tick(self) -> int:
        return promote_eligible(self._queue, threshold=self._threshold)
