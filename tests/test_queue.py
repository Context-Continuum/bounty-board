"""Tests for bounty_board.queue — atomic claim path + earned-capability.

Covers:
  - Round-trip: post → claim → complete
  - Atomic claim race: N concurrent claimers, 1 task, exactly-one-wins
  - Earned-capability gate (agent with no track record can't claim
    non-'open' signature)
  - Bootstrap (a): 'open' sentinel claimable by any agent
  - Bootstrap (d): stale-open auto-relaxation
  - complete / fail / decline track-record bookkeeping
  - fail re-queue when attempts < max_attempts
  - FIFO priority order

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from bounty_board.queue import OPEN_SIGNATURE, Queue


# ─── Round-trip ──────────────────────────────────────────────────────


def test_post_then_claim_round_trip(tmp_path: Path):
    """A posted task is claimable by the OPEN sentinel rule."""
    q = Queue(tmp_path / "q.db")
    q.post(task_type="hello", payload={"x": 1},
           payload_signature=OPEN_SIGNATURE)
    task = q.claim(agent_id="agent_1")
    assert task is not None
    assert task.task_type == "hello"
    assert task.payload == {"x": 1}
    assert task.payload_signature == OPEN_SIGNATURE
    assert task.claimed_by == "agent_1"
    assert task.attempts == 1


def test_complete_updates_track_record_and_status(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t1", payload={}, payload_signature=OPEN_SIGNATURE)
    task = q.claim(agent_id="agent_x")
    task.complete(result={"ok": True}, token_count=42)

    row = q.get_task(tid)
    assert row["status"] == "done"
    assert row["completed_at"] is not None

    # Track record bumped success_n
    track = q._conn.execute(
        "SELECT * FROM agent_track_record WHERE agent_id = ?",
        ("agent_x",),
    ).fetchone()
    assert track["success_n"] == 1
    assert track["fail_n"] == 0
    assert track["decline_n"] == 0


def test_fail_under_max_attempts_requeues_task(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t1", payload={}, payload_signature=OPEN_SIGNATURE,
                 max_attempts=3)
    task = q.claim(agent_id="a")
    task.fail(stack="boom", token_count=10)

    row = q.get_task(tid)
    assert row["status"] == "queued"  # re-queued
    assert row["claimed_by"] is None
    assert row["attempts"] == 1

    # fail_n bumped, success_n unchanged
    track = q._conn.execute(
        "SELECT * FROM agent_track_record WHERE agent_id = ?", ("a",)
    ).fetchone()
    assert track["fail_n"] == 1
    assert track["success_n"] == 0


def test_fail_at_max_attempts_parks_in_failed(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t1", payload={}, payload_signature=OPEN_SIGNATURE,
                 max_attempts=1)
    task = q.claim(agent_id="a")
    task.fail(stack="boom")

    row = q.get_task(tid)
    assert row["status"] == "failed"  # parked, not re-queued
    assert row["attempts"] == 1


def test_decline_requeues_and_bumps_decline_n_only(tmp_path: Path):
    """Decline is cooperative — bumps decline_n, NOT fail_n."""
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t1", payload={}, payload_signature=OPEN_SIGNATURE)
    task = q.claim(agent_id="a")
    task.decline(reason="too_expensive_for_my_tier")

    row = q.get_task(tid)
    assert row["status"] == "queued"
    assert row["claimed_by"] is None

    track = q._conn.execute(
        "SELECT * FROM agent_track_record WHERE agent_id = ?", ("a",)
    ).fetchone()
    assert track["decline_n"] == 1
    assert track["fail_n"] == 0
    assert track["success_n"] == 0


# ─── Earned-capability gate ──────────────────────────────────────────


def test_unearned_signature_not_claimable_by_new_agent(tmp_path: Path):
    """A task with a real (non-'open') signature is NOT claimable by
    an agent who has never succeeded on that signature."""
    q = Queue(tmp_path / "q.db")
    q.post(task_type="specialized", payload={}, payload_signature="specialized")
    task = q.claim(agent_id="fresh_agent")
    assert task is None  # no track record => no claim


def test_earned_signature_claimable_by_agent_with_record(tmp_path: Path):
    """Agent earns 'specialized' via an 'open' task; subsequent
    'specialized' task is claimable by them."""
    q = Queue(tmp_path / "q.db")
    # Manually credit the agent (simulating prior success).
    q._conn.execute(
        """INSERT INTO agent_track_record
           (agent_id, payload_signature, success_n, last_seen_at)
           VALUES (?, ?, 1, ?)""",
        ("earned_agent", "specialized", time.time()),
    )
    q._conn.commit()

    q.post(task_type="t", payload={}, payload_signature="specialized")
    task = q.claim(agent_id="earned_agent")
    assert task is not None


def test_open_sentinel_claimable_by_any_agent(tmp_path: Path):
    """Bootstrap rule (a): 'open' signature has no earned gate."""
    q = Queue(tmp_path / "q.db")
    q.post(task_type="x", payload={}, payload_signature=OPEN_SIGNATURE)
    task = q.claim(agent_id="brand_new_agent_no_record")
    assert task is not None


# ─── Bootstrap rule (d): stale-open auto-relaxation ──────────────────


def test_stale_open_relaxation_kicks_in_after_window(tmp_path: Path):
    """Bootstrap rule (d): task with real signature posted longer ago
    than ``stale_open_seconds`` becomes claimable by any agent."""
    q = Queue(tmp_path / "q.db", stale_open_seconds=0.1)
    tid = q.post(task_type="specialized", payload={},
                 payload_signature="specialized")

    # Immediately: not claimable by a new agent (rule (d) hasn't fired).
    assert q.claim(agent_id="new_agent") is None

    # Wait past the stale_open window.
    time.sleep(0.2)

    # Now claimable.
    task = q.claim(agent_id="new_agent")
    assert task is not None
    assert task.id == tid


def test_fresh_task_not_yet_stale_open(tmp_path: Path):
    """Inverse of above: a freshly-posted task is not stale-open."""
    q = Queue(tmp_path / "q.db", stale_open_seconds=60.0)
    q.post(task_type="t", payload={}, payload_signature="specialized")
    assert q.claim(agent_id="fresh_agent") is None


# ─── FIFO + priority ─────────────────────────────────────────────────


def test_priority_then_fifo_order(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    a = q.post(task_type="t", payload={"x": "a"},
               payload_signature=OPEN_SIGNATURE, priority=0)
    time.sleep(0.01)
    b = q.post(task_type="t", payload={"x": "b"},
               payload_signature=OPEN_SIGNATURE, priority=10)
    time.sleep(0.01)
    c = q.post(task_type="t", payload={"x": "c"},
               payload_signature=OPEN_SIGNATURE, priority=0)

    # b has highest priority — claimed first.
    t1 = q.claim(agent_id="ag")
    assert t1.id == b
    # Then a (older) before c (newer) at the lower priority tier.
    t2 = q.claim(agent_id="ag")
    assert t2.id == a
    t3 = q.claim(agent_id="ag")
    assert t3.id == c


# ─── Atomic claim race (the load-bearing test) ───────────────────────


def test_atomic_claim_exactly_one_winner(tmp_path: Path):
    """The headline correctness test: N concurrent claimants, 1 task,
    EXACTLY ONE wins. This is the test that justifies the
    BEGIN IMMEDIATE + conditional UPDATE pattern.
    """
    db_path = str(tmp_path / "race.db")
    # Post one task as 'open' so any agent could in principle claim it.
    q_setup = Queue(db_path)
    q_setup.post(task_type="t", payload={"only_one_wins": True},
                 payload_signature=OPEN_SIGNATURE)
    q_setup.close()

    N = 20
    winners = []
    nones = []
    lock = threading.Lock()
    start_gate = threading.Event()

    def claimant(i: int) -> None:
        # Each thread opens its own Queue (own connection); they all
        # share the same SQLite file.
        q = Queue(db_path)
        start_gate.wait()
        result = q.claim(agent_id=f"agent_{i}")
        with lock:
            if result is None:
                nones.append(i)
            else:
                winners.append((i, result.id))
        q.close()

    threads = [threading.Thread(target=claimant, args=(i,))
               for i in range(N)]
    for t in threads:
        t.start()
    # Release them all at once.
    start_gate.set()
    for t in threads:
        t.join()

    assert len(winners) == 1, (
        f"expected exactly 1 winner, got {len(winners)}: {winners}"
    )
    assert len(nones) == N - 1


def test_atomic_claim_multiple_tasks_no_double_claim(tmp_path: Path):
    """N concurrent claimants, M < N tasks, each task wins exactly once."""
    db_path = str(tmp_path / "multi-race.db")
    M = 5
    q_setup = Queue(db_path)
    for _ in range(M):
        q_setup.post(task_type="t", payload={},
                     payload_signature=OPEN_SIGNATURE)
    q_setup.close()

    N = 20
    winners = []
    lock = threading.Lock()
    start_gate = threading.Event()

    def claimant(i: int) -> None:
        q = Queue(db_path)
        start_gate.wait()
        result = q.claim(agent_id=f"agent_{i}")
        if result is not None:
            with lock:
                winners.append(result.id)
        q.close()

    threads = [threading.Thread(target=claimant, args=(i,))
               for i in range(N)]
    for t in threads:
        t.start()
    start_gate.set()
    for t in threads:
        t.join()

    # Exactly M wins, all distinct.
    assert len(winners) == M
    assert len(set(winners)) == M


# ─── Event-ledger emissions ──────────────────────────────────────────


def test_claim_emits_claim_event(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    q.claim(agent_id="a")
    rows = q._conn.execute(
        "SELECT * FROM task_events WHERE task_id = ? ORDER BY id", (tid,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["event_kind"] == "claim"
    assert rows[0]["agent_id"] == "a"


def test_complete_emits_complete_event_with_token_count(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    task = q.claim(agent_id="a")
    task.complete(result={"r": 1}, token_count=123)
    rows = q._conn.execute(
        "SELECT * FROM task_events WHERE task_id = ? ORDER BY id", (tid,)
    ).fetchall()
    assert [r["event_kind"] for r in rows] == ["claim", "complete"]
    assert rows[1]["token_count"] == 123
    assert json.loads(rows[1]["payload_json"]) == {"r": 1}


def test_fail_emits_fail_event_with_stack_and_post_status(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE,
                 max_attempts=1)
    task = q.claim(agent_id="a")
    task.fail(stack="Traceback...", prompt_state={"foo": "bar"},
              token_count=99)
    rows = q._conn.execute(
        "SELECT * FROM task_events WHERE task_id = ? ORDER BY id", (tid,)
    ).fetchall()
    assert rows[-1]["event_kind"] == "fail"
    payload = json.loads(rows[-1]["payload_json"])
    assert payload["stack"] == "Traceback..."
    assert payload["prompt_state"] == {"foo": "bar"}
    assert payload["post_status"] == "failed"  # max_attempts=1 → parked


def test_decline_emits_decline_event_with_reason(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    task = q.claim(agent_id="a")
    task.decline(reason="busy")
    rows = q._conn.execute(
        "SELECT event_kind, payload_json FROM task_events "
        "WHERE task_id = ? ORDER BY id", (tid,)
    ).fetchall()
    assert rows[-1]["event_kind"] == "decline"
    assert json.loads(rows[-1]["payload_json"]) == {"reason": "busy"}


# ─── Depth helper ────────────────────────────────────────────────────


def test_depth_counts_queued_only(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert q.depth() == 0
    q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    assert q.depth() == 2
    task = q.claim(agent_id="a")
    assert q.depth() == 1  # claimed task no longer "queued"
    task.complete()
    assert q.depth() == 1  # still 1 queued; the completed one is now 'done'
