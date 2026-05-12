"""Tests for bounty_board.diagnose — Option D self-diagnostic replay.

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from bounty_board.diagnose import (
    DEFAULT_CONFIDENCE_FLOOR,
    build_dossier,
    compute_payload_diff,
    emit_diagnosis,
    find_similar_success,
)
from bounty_board.queue import OPEN_SIGNATURE, Queue


def _credit_agent(q: Queue, agent_id: str, signature: str, success_n: int = 1) -> None:
    """Helper: pre-credit an agent's track-record on a signature."""
    q._conn.execute(
        """INSERT INTO agent_track_record
           (agent_id, payload_signature, success_n, last_seen_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT (agent_id, payload_signature) DO UPDATE
           SET success_n = success_n + excluded.success_n""",
        (agent_id, signature, success_n, time.time()),
    )
    q._conn.commit()


# ─── find_similar_success ────────────────────────────────────────────


def test_find_similar_success_returns_none_when_no_prior(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert find_similar_success(q, "nope") is None


def test_find_similar_success_returns_most_recent_done(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    # Two completed tasks of signature "X"; the most recent should win.
    _credit_agent(q, "a", "X")
    q.post(task_type="t", payload={"v": 1}, payload_signature="X")
    q.claim(agent_id="a").complete()
    t2 = q.post(task_type="t", payload={"v": 2}, payload_signature="X")
    q.claim(agent_id="a").complete()

    snap = find_similar_success(q, "X")
    assert snap is not None
    assert snap.task_id == t2  # most recent
    assert snap.payload == {"v": 2}
    # Trajectory non-empty (claim + complete events)
    kinds = [e.event_kind for e in snap.trajectory]
    assert "claim" in kinds and "complete" in kinds


def test_find_similar_success_excludes_specified_task(tmp_path: Path):
    """Caller can exclude a specific task — useful when diagnosing
    a replay whose parent is the one done task."""
    q = Queue(tmp_path / "q.db")
    _credit_agent(q, "a", "X")
    t1 = q.post(task_type="t", payload={}, payload_signature="X")
    q.claim(agent_id="a").complete()
    # Only one done task; exclude it → no result.
    assert find_similar_success(q, "X", exclude_task_id=t1) is None


def test_find_similar_success_ignores_failed_and_queued(tmp_path: Path):
    """Only status='done' counts. Failed + queued are ignored."""
    q = Queue(tmp_path / "q.db")
    # A failed task with the signature — should NOT match.
    q.post(task_type="t", payload={}, payload_signature="X", max_attempts=1)
    task = q.claim(agent_id="a")
    if task is None:
        _credit_agent(q, "a", "X")
        task = q.claim(agent_id="a")
    task.fail(stack="boom")
    # A queued task with the signature — should NOT match.
    q.post(task_type="t", payload={}, payload_signature="X")
    assert find_similar_success(q, "X") is None


# ─── compute_payload_diff ────────────────────────────────────────────


def test_diff_identifies_new_fields(tmp_path: Path):
    diff = compute_payload_diff(
        failed_payload={"a": 1, "b": 2, "big_extra": "x" * 10_000},
        success_payload={"a": 1, "b": 2},
    )
    assert diff["new_fields"] == ["big_extra"]
    assert diff["removed_fields"] == []
    assert diff["changed_fields"] == []
    assert diff["size_delta_bytes"] > 5_000  # extra payload bloats


def test_diff_identifies_removed_fields():
    diff = compute_payload_diff(
        failed_payload={"a": 1},
        success_payload={"a": 1, "auth_token": "secret-xyz"},
    )
    assert diff["removed_fields"] == ["auth_token"]
    assert diff["new_fields"] == []
    assert diff["size_delta_bytes"] < 0


def test_diff_identifies_changed_fields():
    diff = compute_payload_diff(
        failed_payload={"a": 1, "model": "claude-haiku"},
        success_payload={"a": 1, "model": "claude-opus"},
    )
    assert diff["changed_fields"] == ["model"]
    assert diff["new_fields"] == []
    assert diff["removed_fields"] == []


def test_diff_identical_payloads_returns_empty_lists():
    diff = compute_payload_diff(
        failed_payload={"a": 1, "b": 2},
        success_payload={"a": 1, "b": 2},
    )
    assert diff["new_fields"] == []
    assert diff["removed_fields"] == []
    assert diff["changed_fields"] == []
    assert diff["size_delta_bytes"] == 0


# ─── build_dossier ───────────────────────────────────────────────────


def test_dossier_has_no_diff_when_no_prior_success(tmp_path: Path):
    """First-failure-of-this-signature case: diff_vs_last_success is
    None and the prompt text reflects that."""
    q = Queue(tmp_path / "q.db")
    _credit_agent(q, "a", "X")
    tid = q.post(task_type="t", payload={"x": 1},
                 payload_signature="X", max_attempts=1)
    task = q.claim(agent_id="a")
    task.fail(stack="boom", prompt_state={"k": "v"})

    entry = q.dlq().get(tid)
    dossier = build_dossier(q, entry)
    assert dossier["original_payload"] == {"x": 1}
    assert dossier["failure_dossier"]["stack"] == "boom"
    assert dossier["failure_dossier"]["prompt_state"] == {"k": "v"}
    assert dossier["failure_dossier"]["diff_vs_last_success"] is None
    assert dossier["failure_dossier"]["last_success_task_id"] is None
    assert "NO prior successful task" in dossier["self_diagnosis_prompt"]


def test_dossier_has_diff_when_prior_success_exists(tmp_path: Path):
    """The full Option D path: prior success exists, dossier includes
    structural diff."""
    q = Queue(tmp_path / "q.db")
    _credit_agent(q, "a", "X")
    # Prior success with small payload.
    success_tid = q.post(task_type="t", payload={"a": 1},
                         payload_signature="X")
    q.claim(agent_id="a").complete()
    # Then a failed task with a giant extra field.
    fail_tid = q.post(task_type="t",
                      payload={"a": 1, "huge": "x" * 50_000},
                      payload_signature="X", max_attempts=1)
    task = q.claim(agent_id="a")
    task.fail(stack="context_too_big")

    entry = q.dlq().get(fail_tid)
    dossier = build_dossier(q, entry)
    assert dossier["failure_dossier"]["last_success_task_id"] == success_tid
    diff = dossier["failure_dossier"]["diff_vs_last_success"]
    assert diff is not None
    assert "huge" in diff["new_fields"]
    assert diff["size_delta_bytes"] > 40_000
    assert "structural diff" in dossier["self_diagnosis_prompt"]


def test_dossier_excludes_self_from_prior_success_lookup(tmp_path: Path):
    """A replay's parent might itself be the prior success — exclude
    to avoid diffing-against-self."""
    q = Queue(tmp_path / "q.db")
    _credit_agent(q, "a", "X")
    t1 = q.post(task_type="t", payload={"v": 1},
                payload_signature="X")
    q.claim(agent_id="a").complete()
    # Now post + fail a task with the same signature.
    t2 = q.post(task_type="t", payload={"v": 2},
                payload_signature="X", max_attempts=1)
    q.claim(agent_id="a").fail(stack="x")
    entry = q.dlq().get(t2)
    dossier = build_dossier(q, entry)
    # Diff target should be t1 (the prior success), not t2 (self).
    assert dossier["failure_dossier"]["last_success_task_id"] == t1


# ─── emit_diagnosis ──────────────────────────────────────────────────


def test_emit_diagnosis_writes_event_row(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _credit_agent(q, "a", "X")
    tid = q.post(task_type="t", payload={},
                 payload_signature="X", max_attempts=1)
    q.claim(agent_id="a").fail(stack="x")

    event_id = emit_diagnosis(
        q, tid, agent_id="a",
        hypothesis="context_too_big — truncate `huge` field",
        proposed_patch={"kind": "truncate_field",
                        "args": {"field": "huge", "max_bytes": 10_000}},
        confidence=0.85,
        token_count=42,
    )
    assert event_id is not None
    row = q._conn.execute(
        "SELECT * FROM task_events WHERE id = ?", (event_id,)
    ).fetchone()
    assert row["event_kind"] == "diagnose"
    assert row["agent_id"] == "a"
    assert row["token_count"] == 42
    payload = json.loads(row["payload_json"])
    assert payload["hypothesis"].startswith("context_too_big")
    assert payload["proposed_patch"]["kind"] == "truncate_field"
    assert payload["confidence"] == 0.85


def test_emit_diagnosis_refuses_invalid_confidence(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    with pytest.raises(ValueError, match="confidence must be in"):
        emit_diagnosis(q, tid, agent_id="a", hypothesis="x", confidence=1.5)
    with pytest.raises(ValueError, match="confidence must be in"):
        emit_diagnosis(q, tid, agent_id="a", hypothesis="x", confidence=-0.1)


def test_emit_diagnosis_with_no_patch_low_confidence(tmp_path: Path):
    """Agent unsure → emits diagnose with proposed_patch=None +
    confidence below floor. Downstream caller would escalate."""
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    event_id = emit_diagnosis(
        q, tid, agent_id="a",
        hypothesis="not sure why",
        proposed_patch=None,
        confidence=0.2,
    )
    row = q._conn.execute(
        "SELECT * FROM task_events WHERE id = ?", (event_id,)
    ).fetchone()
    payload = json.loads(row["payload_json"])
    assert payload["proposed_patch"] is None
    assert payload["confidence"] < DEFAULT_CONFIDENCE_FLOOR


# ─── end-to-end Option D scenario ───────────────────────────────────


def test_option_d_end_to_end(tmp_path: Path):
    """The substrate-discipline narrative test:

    1. Agent A completes 'review_pr' task successfully (small payload).
    2. Task posted with same signature but huge payload → fails.
    3. DLQ entry retrieved.
    4. Dossier built — diff highlights the new 'huge' field + size delta.
    5. Agent emits diagnose event with patch proposal.
    6. Replay queues new task; the substrate carries the diagnosis
       forward so the next claimant (or supervisor) sees it.
    """
    q = Queue(tmp_path / "q.db")
    _credit_agent(q, "a", "review_pr")

    # 1. Prior success.
    succ_tid = q.post(
        task_type="review_pr",
        payload={"pr_id": 1, "diff_text": "small diff"},
        payload_signature="review_pr",
    )
    q.claim(agent_id="a").complete(result={"ok": True}, token_count=2_000)

    # 2. Same signature, huge payload, fails.
    fail_tid = q.post(
        task_type="review_pr",
        payload={"pr_id": 2, "diff_text": "small diff",
                 "generated_artifact": "x" * 100_000},
        payload_signature="review_pr",
        max_attempts=1,
    )
    task = q.claim(agent_id="a")
    task.fail(stack="context_too_big", token_count=50_000)

    # 3.
    entry = q.dlq().get(fail_tid)
    assert entry is not None
    # 4.
    dossier = build_dossier(q, entry)
    assert dossier["failure_dossier"]["last_success_task_id"] == succ_tid
    diff = dossier["failure_dossier"]["diff_vs_last_success"]
    assert "generated_artifact" in diff["new_fields"]
    # 5.
    diag_id = emit_diagnosis(
        q, fail_tid, agent_id="a",
        hypothesis=(
            "Payload grew by 100KB due to generated_artifact field. "
            "Truncate before processing."
        ),
        proposed_patch={
            "kind": "truncate_field",
            "args": {"field": "generated_artifact", "max_bytes": 10_000},
        },
        confidence=0.9,
        token_count=300,
    )
    assert diag_id is not None
    # 6.
    new_tid = q.dlq().replay(fail_tid)
    assert new_tid is not None
    # The replay event + the diagnose event are both readable downstream
    # via task_events JOINs on task_id chains.
    diag_row = q._conn.execute(
        "SELECT * FROM task_events WHERE task_id = ? AND event_kind = 'diagnose'",
        (fail_tid,),
    ).fetchone()
    assert diag_row is not None
    diag_payload = json.loads(diag_row["payload_json"])
    assert diag_payload["proposed_patch"]["kind"] == "truncate_field"
