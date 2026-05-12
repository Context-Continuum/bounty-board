"""Tests for bounty_board.stream — #9 long-poll SSE adapter.

Covers:
  * events_since cursor + event_kind filter
  * events_since validation
  * format_sse / format_heartbeat wire output
  * iter_events with injected clock/sleep (deterministic)
  * iter_events max_events termination
  * iter_events max_idle_seconds termination
  * iter_sse heartbeat emission during idle
  * Stream ergonomic wrapper
  * End-to-end: queue activity → cursor-paginated drain

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from bounty_board import stream
from bounty_board.dlq import TaskEvent
from bounty_board.queue import OPEN_SIGNATURE, Queue
from bounty_board.stream import Stream


def _emit_events(q: Queue, count: int, *, kind: str = "claim") -> list[int]:
    """Helper: insert ``count`` raw task_events. Returns the ids."""
    # Need a task to FK to. Create one if none exist.
    row = q._conn.execute("SELECT id FROM tasks LIMIT 1").fetchone()
    if row is None:
        tid = q.post(task_type="t", payload={},
                     payload_signature=OPEN_SIGNATURE)
    else:
        tid = row["id"]
    ids = []
    for i in range(count):
        cur = q._conn.execute(
            """
            INSERT INTO task_events
                (task_id, event_kind, ts, agent_id, payload_json,
                 token_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (tid, kind, time.time() + i * 0.001, "a",
             json.dumps({"i": i}), 0),
        )
        ids.append(cur.lastrowid)
    q._conn.commit()
    return ids


# ─── events_since ───────────────────────────────────────────────────


def test_events_since_empty(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert stream.events_since(q) == []


def test_events_since_returns_after_cursor(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    ids = _emit_events(q, 5)
    out = stream.events_since(q, since_id=ids[2])
    assert [e.id for e in out] == ids[3:]


def test_events_since_filters_by_event_kind(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 3, kind="claim")
    _emit_events(q, 2, kind="complete")
    claims = stream.events_since(q, event_kind="claim")
    completes = stream.events_since(q, event_kind="complete")
    assert all(e.event_kind == "claim" for e in claims)
    assert all(e.event_kind == "complete" for e in completes)
    assert len(claims) == 3
    assert len(completes) == 2


def test_events_since_respects_limit(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 10)
    out = stream.events_since(q, limit=3)
    assert len(out) == 3


def test_events_since_negative_since_raises(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        stream.events_since(q, since_id=-1)


def test_events_since_zero_limit_raises(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        stream.events_since(q, limit=0)


def test_events_since_returns_task_event_dataclass(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 1)
    out = stream.events_since(q)
    assert isinstance(out[0], TaskEvent)
    assert out[0].payload == {"i": 0}
    assert out[0].agent_id == "a"


# ─── format_sse / format_heartbeat ──────────────────────────────────


def test_format_sse_wire_shape():
    ev = TaskEvent(
        id=42, task_id="t1", event_kind="claim", ts=1234.5,
        agent_id="agent-a", payload={"k": "v"}, token_count=7,
    )
    out = stream.format_sse(ev)
    assert out.startswith("id: 42\n")
    assert "event: claim\n" in out
    assert "data: " in out
    assert out.endswith("\n\n")
    # data line must parse as JSON
    data_line = next(
        ln for ln in out.split("\n") if ln.startswith("data: ")
    )
    payload = json.loads(data_line[len("data: "):])
    assert payload["id"] == 42
    assert payload["task_id"] == "t1"
    assert payload["event_kind"] == "claim"
    assert payload["payload"] == {"k": "v"}
    assert payload["token_count"] == 7


def test_format_sse_handles_none_payload():
    ev = TaskEvent(
        id=1, task_id="t1", event_kind="claim", ts=0,
        agent_id=None, payload=None, token_count=0,
    )
    out = stream.format_sse(ev)
    # No exception; payload is null
    data_line = next(ln for ln in out.split("\n") if ln.startswith("data: "))
    payload = json.loads(data_line[len("data: "):])
    assert payload["payload"] is None
    assert payload["agent_id"] is None


def test_format_heartbeat_is_sse_comment():
    out = stream.format_heartbeat()
    assert out == ": heartbeat\n\n"


# ─── iter_events (deterministic with injected clock) ────────────────


class _FakeClock:
    """A monotonic clock that advances on _sleep() calls. No real
    sleep; injected into iter_events so tests are deterministic."""

    def __init__(self, start: float = 0.0):
        self.now = start
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, d: float) -> None:
        self.sleeps.append(d)
        self.now += d


def test_iter_events_drains_existing(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    ids = _emit_events(q, 3)
    clock = _FakeClock()
    out = list(stream.iter_events(
        q, max_events=3, _now=clock.time, _sleep=clock.sleep,
    ))
    assert [e.id for e in out] == ids


def test_iter_events_max_events_terminates(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 10)
    clock = _FakeClock()
    out = list(stream.iter_events(
        q, max_events=4, _now=clock.time, _sleep=clock.sleep,
    ))
    assert len(out) == 4


def test_iter_events_event_kind_filter(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 2, kind="claim")
    _emit_events(q, 3, kind="complete")
    clock = _FakeClock()
    out = list(stream.iter_events(
        q, event_kind="complete", max_events=3,
        _now=clock.time, _sleep=clock.sleep,
    ))
    assert len(out) == 3
    assert all(e.event_kind == "complete" for e in out)


def test_iter_events_max_idle_terminates(tmp_path: Path):
    """No events present; loop should sleep until max_idle_seconds
    elapses, then return without yielding anything."""
    q = Queue(tmp_path / "q.db")
    clock = _FakeClock()
    out = list(stream.iter_events(
        q,
        max_idle_seconds=2.0,
        poll_interval=0.5,
        _now=clock.time, _sleep=clock.sleep,
    ))
    assert out == []
    # Slept at least 4 times to elapse 2 seconds at 0.5s intervals.
    assert len(clock.sleeps) >= 4
    assert sum(clock.sleeps) >= 2.0


def test_iter_events_sentinel_after_drain(tmp_path: Path):
    """After draining all available, idle countdown kicks in."""
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 2)
    clock = _FakeClock()
    out = list(stream.iter_events(
        q, max_idle_seconds=1.0, poll_interval=0.5,
        _now=clock.time, _sleep=clock.sleep,
    ))
    # Drained 2, then idled out.
    assert len(out) == 2


def test_iter_events_advances_cursor_across_inserts(tmp_path: Path):
    """Cursor advances; we don't yield the same event twice. To prove
    this without a real concurrent insert, we drive the loop in
    chunks via max_events and confirm subsequent calls pick up
    where the prior left off."""
    q = Queue(tmp_path / "q.db")
    ids = _emit_events(q, 5)
    clock = _FakeClock()
    out1 = list(stream.iter_events(
        q, max_events=2, _now=clock.time, _sleep=clock.sleep,
    ))
    last = out1[-1].id
    out2 = list(stream.iter_events(
        q, since_id=last, max_events=3,
        _now=clock.time, _sleep=clock.sleep,
    ))
    assert [e.id for e in out1] + [e.id for e in out2] == ids


def test_iter_events_invalid_poll_interval(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        list(stream.iter_events(q, poll_interval=0))


def test_iter_events_invalid_max_events(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        list(stream.iter_events(q, max_events=0))


# ─── iter_sse ───────────────────────────────────────────────────────


def test_iter_sse_emits_sse_strings(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 2)
    clock = _FakeClock()
    out = list(stream.iter_sse(
        q, max_events=2, _now=clock.time, _sleep=clock.sleep,
    ))
    assert len(out) == 2
    for s in out:
        assert isinstance(s, str)
        assert s.startswith("id: ")
        assert s.endswith("\n\n")


def test_iter_sse_heartbeat_during_idle(tmp_path: Path):
    """During an idle stretch the generator should emit heartbeat
    comments at the configured interval."""
    q = Queue(tmp_path / "q.db")
    clock = _FakeClock()
    out = list(stream.iter_sse(
        q,
        heartbeat_interval=1.0,
        poll_interval=0.5,
        max_idle_seconds=3.0,
        _now=clock.time, _sleep=clock.sleep,
    ))
    # Some heartbeats should have been emitted (the loop slept 6
    # times at 0.5s = 3.0s total idle; each 1.0s elapsed yields a
    # heartbeat). The exact count depends on the loop ordering, but
    # at least 2 heartbeats must appear before the idle-timeout.
    assert all(s == stream.format_heartbeat() for s in out)
    assert len(out) >= 2


def test_iter_sse_no_heartbeat_when_events_flow(tmp_path: Path):
    """When events are always available, the loop yields event
    strings and never emits a heartbeat."""
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 5)
    clock = _FakeClock()
    out = list(stream.iter_sse(
        q, max_events=5, heartbeat_interval=0.001,
        _now=clock.time, _sleep=clock.sleep,
    ))
    assert len(out) == 5
    # None of the messages should be heartbeats.
    assert not any(s.startswith(": ") for s in out)


def test_iter_sse_invalid_heartbeat_interval(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        list(stream.iter_sse(q, heartbeat_interval=0))


# ─── Stream wrapper ─────────────────────────────────────────────────


def test_stream_wrapper_events_since(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 2)
    s = Stream(q)
    out = s.events_since()
    assert len(out) == 2


def test_stream_wrapper_iter_events(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 3)
    s = Stream(q)
    clock = _FakeClock()
    out = list(s.iter_events(
        max_events=3, _now=clock.time, _sleep=clock.sleep,
    ))
    assert len(out) == 3


def test_stream_wrapper_iter_sse(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _emit_events(q, 2)
    s = Stream(q)
    clock = _FakeClock()
    out = list(s.iter_sse(
        max_events=2, _now=clock.time, _sleep=clock.sleep,
    ))
    assert len(out) == 2


# ─── end-to-end ─────────────────────────────────────────────────────


def test_end_to_end_cursor_resume(tmp_path: Path):
    """The canonical #9 user story:

      1. Client opens a stream from since_id=0
      2. Drains the existing backlog
      3. Disconnects (in real life: network blip / proxy timeout)
      4. Reconnects passing the last-id-seen as since_id
      5. No duplicate events; resumes exactly where it left off
    """
    q = Queue(tmp_path / "q.db")
    ids = _emit_events(q, 5)

    clock = _FakeClock()
    first_drain = list(stream.iter_events(
        q, max_events=5, _now=clock.time, _sleep=clock.sleep,
    ))
    assert [e.id for e in first_drain] == ids
    last_id = first_drain[-1].id

    # 3. Disconnect (no-op for the cursor)
    # 4. New events arrive while disconnected
    more = _emit_events(q, 3)

    # 5. Reconnect from last_id
    second_drain = list(stream.iter_events(
        q, since_id=last_id, max_events=3,
        _now=clock.time, _sleep=clock.sleep,
    ))
    assert [e.id for e in second_drain] == more
