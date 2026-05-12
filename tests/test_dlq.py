"""Tests for bounty_board.dlq — DLQ list / get / replay / purge.

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import time
from pathlib import Path

from bounty_board.queue import OPEN_SIGNATURE, Queue


def _fail_a_task_to_max(q: Queue, *, signature: str = OPEN_SIGNATURE,
                       agent_id: str = "a", token_count: int = 0,
                       stack: str = "boom") -> str:
    """Helper: post a task with max_attempts=1, claim, fail, return task_id.
    After this the task is parked in status='failed' (DLQ).

    Pre-credits the agent on the signature (success_n=1) so the
    earned-capability gate lets the claim through even for non-'open'
    signatures. The pre-credit is harmless for the DLQ tests — they
    only care about the post-fail state.
    """
    import time as _t
    if signature != OPEN_SIGNATURE:
        q._conn.execute(
            """
            INSERT INTO agent_track_record
                (agent_id, payload_signature, success_n, last_seen_at)
            VALUES (?, ?, 1, ?)
            ON CONFLICT (agent_id, payload_signature) DO UPDATE
            SET success_n = success_n + 1
            """,
            (agent_id, signature, _t.time()),
        )
        q._conn.commit()
    tid = q.post(task_type="t", payload={"x": 1},
                 payload_signature=signature, max_attempts=1)
    task = q.claim(agent_id=agent_id)
    assert task is not None and task.id == tid
    task.fail(stack=stack, token_count=token_count)
    return tid


# ─── list / get / depth ──────────────────────────────────────────────


def test_dlq_empty(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert q.dlq().list() == []
    assert q.dlq().depth() == 0


def test_dlq_list_returns_failed_tasks(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid1 = _fail_a_task_to_max(q)
    tid2 = _fail_a_task_to_max(q)
    entries = q.dlq().list()
    assert len(entries) == 2
    ids = {e.task_id for e in entries}
    assert ids == {tid1, tid2}
    assert q.dlq().depth() == 2
    for e in entries:
        assert e.status == "failed"


def test_dlq_list_does_not_include_done_or_queued(tmp_path: Path):
    """Only 'failed'/'unclaimable' tasks appear; done + queued are
    not DLQ."""
    q = Queue(tmp_path / "q.db")
    # Queued task (not yet claimed) — not DLQ
    q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    # Done task — not DLQ
    done_tid = q.post(task_type="t", payload={},
                      payload_signature=OPEN_SIGNATURE)
    done_task = q.claim(agent_id="a")
    if done_task is not None and done_task.id != done_tid:
        # The first queued one might claim first; complete whichever did
        done_task.complete()
        done_task = q.claim(agent_id="a")
    if done_task is not None:
        done_task.complete()
    # Failed task — IS DLQ
    failed_tid = _fail_a_task_to_max(q)
    entries = q.dlq().list()
    assert len(entries) == 1
    assert entries[0].task_id == failed_tid


def test_dlq_list_payload_signature_filter(tmp_path: Path):
    """Filter by payload_signature scopes the DLQ view."""
    q = Queue(tmp_path / "q.db")
    sig_a = _fail_a_task_to_max(q, signature="sig_A")
    _fail_a_task_to_max(q, signature="sig_B")
    entries = q.dlq().list(payload_signature="sig_A")
    assert len(entries) == 1
    assert entries[0].task_id == sig_a
    assert entries[0].payload_signature == "sig_A"


def test_dlq_list_limit_caps_results(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    for _ in range(5):
        _fail_a_task_to_max(q)
    entries = q.dlq().list(limit=2)
    assert len(entries) == 2


def test_dlq_get_returns_none_for_unknown(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert q.dlq().get("not-a-real-id") is None


def test_dlq_get_returns_none_for_non_dlq_task(tmp_path: Path):
    """A queued or done task isn't in the DLQ — get() returns None."""
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    assert q.dlq().get(tid) is None  # task is queued, not failed


def test_dlq_get_carries_full_trajectory(tmp_path: Path):
    """get() returns the task PLUS its task_events trajectory."""
    q = Queue(tmp_path / "q.db")
    tid = _fail_a_task_to_max(q, stack="kaboom", token_count=99)
    entry = q.dlq().get(tid)
    assert entry is not None
    # Trajectory should contain at least 'claim' + 'fail'
    kinds = [ev.event_kind for ev in entry.trajectory]
    assert "claim" in kinds
    assert "fail" in kinds
    # final_fail_event helper
    fail_ev = entry.final_fail_event
    assert fail_ev is not None
    assert fail_ev.payload["stack"] == "kaboom"
    assert fail_ev.token_count == 99


def test_dlq_entry_total_token_count_sums_trajectory(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={},
                 payload_signature=OPEN_SIGNATURE, max_attempts=1)
    task = q.claim(agent_id="a")
    task.fail(stack="bad", token_count=500)
    entry = q.dlq().get(tid)
    # claim event = 0 tokens, fail event = 500 tokens
    assert entry.total_token_count == 500


# ─── replay ──────────────────────────────────────────────────────────


def test_replay_creates_new_task_with_same_payload(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    src_tid = _fail_a_task_to_max(q)
    new_tid = q.dlq().replay(src_tid)
    # New id, different from source
    assert new_tid != src_tid
    # New task is queued
    new_row = q.get_task(new_tid)
    assert new_row["status"] == "queued"
    assert new_row["parent_id"] == src_tid
    assert new_row["payload_signature"] == OPEN_SIGNATURE
    # Source stays in 'failed' for audit
    src_row = q.get_task(src_tid)
    assert src_row["status"] == "failed"


def test_replay_emits_replay_event_pointing_back(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    src_tid = _fail_a_task_to_max(q)
    new_tid = q.dlq().replay(src_tid)
    # New task has a 'replay' event citing the source
    row = q._conn.execute(
        "SELECT event_kind, payload_json FROM task_events "
        "WHERE task_id = ? AND event_kind = 'replay'",
        (new_tid,),
    ).fetchone()
    assert row is not None
    import json
    assert json.loads(row["payload_json"]) == {"replayed_from": src_tid}


def test_replay_refuses_non_dlq_task(tmp_path: Path):
    """Can only replay tasks in 'failed' or 'unclaimable' status."""
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    # Task is queued, not failed; replay should refuse.
    import pytest
    with pytest.raises(ValueError, match="not in a DLQ status"):
        q.dlq().replay(tid)


def test_replay_refuses_unknown_task(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    import pytest
    with pytest.raises(ValueError, match="no task with id"):
        q.dlq().replay("not-a-real-id")


def test_replayed_task_is_claimable(tmp_path: Path):
    """End-to-end: failed → DLQ → replay → fresh claim works."""
    q = Queue(tmp_path / "q.db")
    src_tid = _fail_a_task_to_max(q)
    new_tid = q.dlq().replay(src_tid)
    task = q.claim(agent_id="b")
    assert task is not None
    assert task.id == new_tid
    # Same payload as original
    assert task.payload == {"x": 1}


# ─── purge ───────────────────────────────────────────────────────────


def test_purge_zero_when_nothing_old(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _fail_a_task_to_max(q)
    # Default 30d cutoff; task is brand new
    assert q.dlq().purge_older_than(days=30) == 0
    assert q.dlq().depth() == 1


def test_purge_removes_old_entries(tmp_path: Path):
    """Backdate a failed task's created_at, then purge."""
    q = Queue(tmp_path / "q.db")
    tid = _fail_a_task_to_max(q)
    # Backdate created_at to 100 days ago.
    backdated = time.time() - (100 * 86400)
    q._conn.execute(
        "UPDATE tasks SET created_at = ? WHERE id = ?",
        (backdated, tid),
    )
    q._conn.commit()
    n = q.dlq().purge_older_than(days=30)
    assert n == 1
    assert q.dlq().depth() == 0
    # Task is fully gone
    assert q.get_task(tid) is None
    # Events are gone too (cascade)
    rows = q._conn.execute(
        "SELECT COUNT(*) AS n FROM task_events WHERE task_id = ?", (tid,)
    ).fetchone()
    assert rows["n"] == 0


def test_purge_preserves_recent_dlq_entries(tmp_path: Path):
    """Only entries older than the cutoff get purged."""
    q = Queue(tmp_path / "q.db")
    old_tid = _fail_a_task_to_max(q)
    new_tid = _fail_a_task_to_max(q)
    # Backdate only the old one
    q._conn.execute(
        "UPDATE tasks SET created_at = ? WHERE id = ?",
        (time.time() - (60 * 86400), old_tid),
    )
    q._conn.commit()
    n = q.dlq().purge_older_than(days=30)
    assert n == 1
    # Recent entry survives
    assert q.dlq().depth() == 1
    assert q.dlq().get(new_tid) is not None
    assert q.dlq().get(old_tid) is None
