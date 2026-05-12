"""Tests for bounty_board.interventions — #11 voluntary-honor cooperative
substrate.

Covers:
  * post / get / list_for_task / check_pending lifecycle
  * payload JSON round-trip + None payload
  * honor double-write atomicity (intervention update + task_event row)
  * honor refuses double-honor + missing-id
  * post against missing task raises cleanly (not raw FK error)
  * InterventionHonor.pending / honor / honor_all ergonomic wrapper
  * End-to-end: supervisor posts → working agent's poll detects →
    honor → trajectory carries the intervene event

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from bounty_board import interventions
from bounty_board.interventions import (
    Intervention,
    InterventionHonor,
)
from bounty_board.queue import OPEN_SIGNATURE, Queue


def _post_task(q: Queue) -> str:
    """Helper: post a task and return its id. No claim needed for
    intervention tests."""
    return q.post(
        task_type="t", payload={"x": 1},
        payload_signature=OPEN_SIGNATURE,
    )


# ─── post ───────────────────────────────────────────────────────────


def test_post_inserts_row(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(
        q, tid, "cancel", payload={"reason": "duplicate"},
        agent_id="supervisor",
    )
    assert isinstance(iid, int)
    iv = interventions.get(q, iid)
    assert iv is not None
    assert iv.id == iid
    assert iv.task_id == tid
    assert iv.kind == "cancel"
    assert iv.payload == {"reason": "duplicate"}
    assert iv.posted_by_agent_id == "supervisor"
    assert iv.honored_at is None
    assert iv.is_pending
    assert not iv.is_honored
    assert abs(iv.posted_at - time.time()) < 2.0


def test_post_with_none_payload(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(q, tid, "nudge", agent_id="op")
    iv = interventions.get(q, iid)
    assert iv is not None
    assert iv.payload is None


def test_post_raises_on_missing_task(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError, match="no task with id"):
        interventions.post(q, "nonexistent", "cancel", agent_id="op")


def test_post_serializes_complex_payload(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    payload = {
        "nested": {"levels": [1, 2, {"deep": True}]},
        "list": ["a", "b", "c"],
    }
    iid = interventions.post(
        q, tid, "swap_model", payload=payload, agent_id="op",
    )
    iv = interventions.get(q, iid)
    assert iv is not None
    assert iv.payload == payload


# ─── get / list_for_task / check_pending ────────────────────────────


def test_get_returns_none_for_missing(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert interventions.get(q, 999) is None


def test_list_for_task_returns_all_ordered(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    i1 = interventions.post(q, tid, "nudge", agent_id="op")
    time.sleep(0.01)
    i2 = interventions.post(q, tid, "swap_model", agent_id="op",
                            payload={"to": "claude-sonnet"})
    out = interventions.list_for_task(q, tid)
    assert [iv.id for iv in out] == [i1, i2]


def test_list_for_task_pending_only_filters_honored(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    i1 = interventions.post(q, tid, "nudge", agent_id="op")
    i2 = interventions.post(q, tid, "cancel", agent_id="op")
    # Honor i1.
    interventions.honor(q, i1, agent_id="worker")
    pending = interventions.list_for_task(q, tid, pending_only=True)
    assert [iv.id for iv in pending] == [i2]
    all_ = interventions.list_for_task(q, tid, pending_only=False)
    assert [iv.id for iv in all_] == [i1, i2]


def test_check_pending_alias(tmp_path: Path):
    """check_pending == list_for_task(pending_only=True)."""
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    interventions.post(q, tid, "nudge", agent_id="op")
    a = interventions.check_pending(q, tid)
    b = interventions.list_for_task(q, tid, pending_only=True)
    assert [iv.id for iv in a] == [iv.id for iv in b]


def test_list_for_task_empty_for_unknown_task(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    # No FK check on read — returning empty list for a missing task is
    # the correct "no interventions" semantic.
    assert interventions.list_for_task(q, "nonexistent") == []


# ─── honor ──────────────────────────────────────────────────────────


def test_honor_marks_honored(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(q, tid, "nudge", agent_id="op")
    event_id = interventions.honor(q, iid, agent_id="worker")
    assert isinstance(event_id, int)
    iv = interventions.get(q, iid)
    assert iv is not None
    assert iv.honored_at is not None
    assert iv.is_honored
    assert not iv.is_pending


def test_honor_emits_intervene_task_event(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(
        q, tid, "swap_model",
        payload={"to": "claude-sonnet"}, agent_id="supervisor",
    )
    event_id = interventions.honor(
        q, iid, agent_id="worker",
        note="reasonable; switching models", token_count=42,
    )
    row = q._conn.execute(
        "SELECT * FROM task_events WHERE id = ?", (event_id,),
    ).fetchone()
    assert row is not None
    assert row["task_id"] == tid
    assert row["event_kind"] == "intervene"
    assert row["agent_id"] == "worker"
    assert row["token_count"] == 42
    import json
    payload = json.loads(row["payload_json"])
    assert payload["intervention_id"] == iid
    assert payload["kind"] == "swap_model"
    assert payload["posted_by_agent_id"] == "supervisor"
    assert payload["note"] == "reasonable; switching models"


def test_honor_raises_on_missing(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError, match="no intervention"):
        interventions.honor(q, 999, agent_id="worker")


def test_honor_refuses_double_honor(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(q, tid, "nudge", agent_id="op")
    interventions.honor(q, iid, agent_id="worker")
    with pytest.raises(ValueError, match="already honored"):
        interventions.honor(q, iid, agent_id="worker")


def test_honor_atomic_on_failure(tmp_path: Path):
    """Double-honor refusal happens before the UPDATE/INSERT, so the
    failed call leaves no half-state. Defensive: verify the trajectory
    only carries one intervene event even after a refusal."""
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(q, tid, "nudge", agent_id="op")
    interventions.honor(q, iid, agent_id="worker")
    try:
        interventions.honor(q, iid, agent_id="worker2")
    except ValueError:
        pass
    n = q._conn.execute(
        "SELECT COUNT(*) AS n FROM task_events "
        "WHERE task_id = ? AND event_kind = 'intervene'",
        (tid,),
    ).fetchone()["n"]
    assert n == 1


def test_honor_optional_note_defaults_to_none(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(q, tid, "nudge", agent_id="op")
    event_id = interventions.honor(q, iid, agent_id="worker")
    row = q._conn.execute(
        "SELECT * FROM task_events WHERE id = ?", (event_id,),
    ).fetchone()
    import json
    payload = json.loads(row["payload_json"])
    assert payload["note"] is None


# ─── InterventionHonor (ergonomic wrapper) ──────────────────────────


def test_intervention_honor_pending_filters_to_task(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    t1 = _post_task(q)
    t2 = _post_task(q)
    interventions.post(q, t1, "nudge", agent_id="op")
    interventions.post(q, t2, "cancel", agent_id="op")
    h = InterventionHonor(q, t1, agent_id="worker")
    pending = h.pending()
    assert len(pending) == 1
    assert pending[0].task_id == t1
    assert pending[0].kind == "nudge"


def test_intervention_honor_honor_wraps_module(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(q, tid, "nudge", agent_id="op")
    h = InterventionHonor(q, tid, agent_id="worker")
    event_id = h.honor(iid, note="ok")
    assert isinstance(event_id, int)
    iv = interventions.get(q, iid)
    assert iv is not None
    assert iv.is_honored


def test_intervention_honor_honor_all_clears_pending(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    interventions.post(q, tid, "nudge", agent_id="op")
    interventions.post(q, tid, "cancel", agent_id="op")
    h = InterventionHonor(q, tid, agent_id="worker")
    assert len(h.pending()) == 2
    event_ids = h.honor_all(note="batch-clear before exit")
    assert len(event_ids) == 2
    assert h.pending() == []


def test_intervention_honor_pending_isolated_across_workers(
    tmp_path: Path,
):
    """The wrapper binds to (task_id, agent_id) but pending() reads
    from the schema — every worker on the same task sees the same
    pending list. Voluntary-honor is per-call, not per-binding."""
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(q, tid, "nudge", agent_id="op")
    h1 = InterventionHonor(q, tid, agent_id="worker-1")
    h2 = InterventionHonor(q, tid, agent_id="worker-2")
    assert [iv.id for iv in h1.pending()] == [iid]
    assert [iv.id for iv in h2.pending()] == [iid]
    # Either worker can honor; the other's view updates.
    h1.honor(iid)
    assert h2.pending() == []


# ─── sanity ─────────────────────────────────────────────────────────


def test_intervention_dataclass_round_trip(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _post_task(q)
    iid = interventions.post(
        q, tid, "swap_model",
        payload={"to": "claude-sonnet"}, agent_id="op",
    )
    iv = interventions.get(q, iid)
    assert isinstance(iv, Intervention)


# ─── end-to-end ─────────────────────────────────────────────────────


def test_end_to_end_supervisor_posts_worker_honors(tmp_path: Path):
    """Canonical #11 user story:

      1. A task is queued + claimed by a working agent
      2. Supervisor (separate agent) posts an intervention
      3. Worker's safe-point poll detects the pending intervention
      4. Worker honors it, emitting an `intervene` event into the
         trajectory
      5. Subsequent polls show no pending interventions
      6. Option D's dossier (via task_events) carries the full record
    """
    q = Queue(tmp_path / "q.db")
    # 1. queue + claim
    tid = q.post(
        task_type="t", payload={"prompt": "do the thing"},
        payload_signature=OPEN_SIGNATURE,
    )
    task = q.claim(agent_id="worker")
    assert task is not None
    assert task.id == tid

    # 2. supervisor posts an intervention
    iid = interventions.post(
        q, tid, "nudge",
        payload={"hint": "watch for off-by-one"},
        agent_id="supervisor",
    )

    # 3. worker polls at a safe point
    honor_helper = InterventionHonor(q, tid, agent_id="worker")
    pending = honor_helper.pending()
    assert [iv.id for iv in pending] == [iid]
    assert pending[0].kind == "nudge"
    assert pending[0].payload == {"hint": "watch for off-by-one"}

    # 4. worker honors
    event_id = honor_helper.honor(
        iid, note="useful hint; adjusting approach",
    )
    assert isinstance(event_id, int)

    # 5. subsequent polls clean
    assert honor_helper.pending() == []

    # 6. trajectory carries it (the dossier-readable record)
    events = q._conn.execute(
        "SELECT * FROM task_events WHERE task_id = ? "
        "AND event_kind = 'intervene' ORDER BY id ASC",
        (tid,),
    ).fetchall()
    assert len(events) == 1
    import json
    payload = json.loads(events[0]["payload_json"])
    assert payload["intervention_id"] == iid
    assert payload["kind"] == "nudge"
    assert payload["posted_by_agent_id"] == "supervisor"
    assert payload["note"] == "useful hint; adjusting approach"
