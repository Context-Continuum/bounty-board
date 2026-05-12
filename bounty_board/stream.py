"""bounty_board.stream — #9 long-poll SSE adapter over task_events.

Polling-cursor model: clients pass ``since_id`` and the adapter
returns ``task_events`` rows with ``id > since_id``. The WAKE
substrate (millisecond cross-agent push) is deliberately reserved
for the commercial PSE per the WAKE→POLL pivot ratification
(scratchpad id=1111031850905726115); the V1 OSS event stream is a
poll-based long-poll generator with optional event-kind filtering
and SSE-formatted output.

This module exists separately from ``inspect.py``'s ``/api/events``
endpoint by design — that endpoint is a single-shot JSON poll for
the dashboard UI; this module is the streaming generator surface
that FastAPI / Starlette / a CLI / a test harness can wrap to emit
``text/event-stream`` responses. Both read from the same
``task_events`` table; the substrate is the source of truth.

Surface:

  stream.events_since(queue, *, since_id=0, event_kind=None, limit=100)
                                              -> list[TaskEvent]
  stream.format_sse(event)                    -> str (SSE wire bytes)
  stream.format_heartbeat()                   -> str (SSE comment)
  stream.iter_events(queue, *, since_id, event_kind, poll_interval,
                     max_idle_seconds, max_events, _now=time.monotonic,
                     _sleep=time.sleep)
                                              -> Iterator[TaskEvent]
  stream.iter_sse(queue, ...)                 -> Iterator[str]
  Stream(queue) — ergonomic wrapper

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING

from bounty_board.dlq import TaskEvent

if TYPE_CHECKING:
    from bounty_board.queue import Queue


# Default polling cadence. 500ms is the V1 OSS floor — fast enough to
# feel live, slow enough to keep SQLite chatter low under contention.
# Production deployments that want sub-second cross-agent wake are
# the intended PSE customers per the WAKE→POLL pivot.
DEFAULT_POLL_INTERVAL_SECONDS = 0.5

# Default heartbeat cadence. SSE clients (browser EventSource, curl)
# need a periodic message to keep the connection from being reaped
# by intermediate proxies / load-balancers. A comment line
# (": heartbeat\n\n") is the canonical no-op message.
DEFAULT_HEARTBEAT_SECONDS = 15.0

# Default max-idle before the generator naturally terminates. Set
# generously so SSE consumers stay connected through quiet periods;
# pass ``max_idle_seconds=None`` to never auto-terminate (the loop
# then runs until the caller breaks the iterator).
DEFAULT_MAX_IDLE_SECONDS: float | None = None


# ─── poll ───────────────────────────────────────────────────────────


def events_since(queue: Queue, *, since_id: int = 0,
                 event_kind: str | None = None,
                 limit: int = 100) -> list[TaskEvent]:
    """Return up to ``limit`` events with ``id > since_id``, ordered
    ascending by id. Optional ``event_kind`` filter.

    The query is index-backed (``idx_task_events_kind_ts`` for the
    filtered path, primary-key for the unfiltered). A cursor-based
    paginated read; callers advance ``since_id`` to the last id
    returned to resume.
    """
    if limit < 1:
        raise ValueError(f"limit must be >= 1; got {limit!r}")
    if since_id < 0:
        raise ValueError(f"since_id must be >= 0; got {since_id!r}")
    if event_kind is not None:
        rows = queue._conn.execute(
            """
            SELECT * FROM task_events
            WHERE id > ? AND event_kind = ?
            ORDER BY id ASC LIMIT ?
            """,
            (since_id, event_kind, limit),
        ).fetchall()
    else:
        rows = queue._conn.execute(
            """
            SELECT * FROM task_events
            WHERE id > ? ORDER BY id ASC LIMIT ?
            """,
            (since_id, limit),
        ).fetchall()
    return [
        TaskEvent(
            id=r["id"],
            task_id=r["task_id"],
            event_kind=r["event_kind"],
            ts=r["ts"],
            agent_id=r["agent_id"],
            payload=(json.loads(r["payload_json"])
                     if r["payload_json"] else None),
            token_count=r["token_count"],
        )
        for r in rows
    ]


# ─── SSE formatting ─────────────────────────────────────────────────


def format_sse(event: TaskEvent) -> str:
    """Render a ``TaskEvent`` as one SSE message.

    SSE wire format: ``id: <n>\\nevent: <kind>\\ndata: <json>\\n\\n``.
    Consumers branch on ``event.event_kind`` (which becomes the SSE
    ``event:`` field), so JS ``EventSource`` listeners can do
    ``addEventListener("claim", ...)``.
    """
    data = {
        "id": event.id,
        "task_id": event.task_id,
        "event_kind": event.event_kind,
        "ts": event.ts,
        "agent_id": event.agent_id,
        "payload": event.payload,
        "token_count": event.token_count,
    }
    return (
        f"id: {event.id}\n"
        f"event: {event.event_kind}\n"
        f"data: {json.dumps(data)}\n\n"
    )


def format_heartbeat() -> str:
    """SSE comment line. Keeps the connection from being reaped by
    proxies during quiet periods without surfacing as a data event."""
    return ": heartbeat\n\n"


# ─── generator ──────────────────────────────────────────────────────


def iter_events(
    queue: Queue, *,
    since_id: int = 0,
    event_kind: str | None = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    max_idle_seconds: float | None = DEFAULT_MAX_IDLE_SECONDS,
    max_events: int | None = None,
    batch_limit: int = 100,
    _now: Callable[[], float] = time.monotonic,
    _sleep: Callable[[float], None] = time.sleep,
) -> Iterator[TaskEvent]:
    """Yield ``TaskEvent`` rows as they land, advancing an internal
    cursor.

    Loop terminates when:

      * ``max_events`` events have been yielded (if set), OR
      * ``max_idle_seconds`` have elapsed with no new events (if set),
        OR
      * the caller breaks out of the iterator.

    The ``_now`` / ``_sleep`` injection is for deterministic testing
    — tests pass fake clocks and pass-through sleeps so the loop's
    state machine is exercised without real time delay. Production
    callers omit these and get the real ``time.monotonic`` /
    ``time.sleep``.
    """
    if poll_interval <= 0:
        raise ValueError(
            f"poll_interval must be > 0; got {poll_interval!r}"
        )
    if max_events is not None and max_events < 1:
        raise ValueError(
            f"max_events must be >= 1 or None; got {max_events!r}"
        )

    cursor = since_id
    emitted = 0
    last_event_at = _now()

    while True:
        batch = events_since(
            queue, since_id=cursor, event_kind=event_kind,
            limit=batch_limit,
        )
        if batch:
            for ev in batch:
                yield ev
                emitted += 1
                cursor = ev.id
                if max_events is not None and emitted >= max_events:
                    return
            last_event_at = _now()
            continue

        # No new events. Check idle timeout.
        if max_idle_seconds is not None:
            idle = _now() - last_event_at
            if idle >= max_idle_seconds:
                return
        _sleep(poll_interval)


def iter_sse(
    queue: Queue, *,
    since_id: int = 0,
    event_kind: str | None = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    heartbeat_interval: float = DEFAULT_HEARTBEAT_SECONDS,
    max_idle_seconds: float | None = DEFAULT_MAX_IDLE_SECONDS,
    max_events: int | None = None,
    batch_limit: int = 100,
    _now: Callable[[], float] = time.monotonic,
    _sleep: Callable[[float], None] = time.sleep,
) -> Iterator[str]:
    """SSE-formatted variant of ``iter_events``.

    Same cursor-poll loop, but yields wire-formatted SSE strings
    suitable for streaming directly to an HTTP response body. Emits
    heartbeat comments every ``heartbeat_interval`` seconds during
    idle stretches to keep the connection open across proxies.

    Typical FastAPI usage::

        from fastapi.responses import StreamingResponse
        @app.get("/api/sse")
        def sse(since: int = 0):
            return StreamingResponse(
                stream.iter_sse(queue, since_id=since),
                media_type="text/event-stream",
            )
    """
    if heartbeat_interval <= 0:
        raise ValueError(
            f"heartbeat_interval must be > 0; got {heartbeat_interval!r}"
        )
    if poll_interval <= 0:
        raise ValueError(
            f"poll_interval must be > 0; got {poll_interval!r}"
        )
    if max_events is not None and max_events < 1:
        raise ValueError(
            f"max_events must be >= 1 or None; got {max_events!r}"
        )

    cursor = since_id
    emitted = 0
    now = _now()
    last_event_at = now
    last_heartbeat_at = now

    while True:
        batch = events_since(
            queue, since_id=cursor, event_kind=event_kind,
            limit=batch_limit,
        )
        if batch:
            for ev in batch:
                yield format_sse(ev)
                emitted += 1
                cursor = ev.id
                if max_events is not None and emitted >= max_events:
                    return
            now = _now()
            last_event_at = now
            last_heartbeat_at = now
            continue

        now = _now()
        # Heartbeat tick.
        if now - last_heartbeat_at >= heartbeat_interval:
            yield format_heartbeat()
            last_heartbeat_at = now
        # Idle-timeout tick.
        if max_idle_seconds is not None:
            if now - last_event_at >= max_idle_seconds:
                return
        _sleep(poll_interval)


# ─── ergonomic wrapper ──────────────────────────────────────────────


class Stream:
    """Queue-bound ergonomic wrapper. Constructor is cheap (just
    holds the queue ref); fine to instantiate per request in an HTTP
    handler.
    """

    def __init__(self, queue: Queue):
        self._queue = queue

    def events_since(self, *, since_id: int = 0,
                     event_kind: str | None = None,
                     limit: int = 100) -> list[TaskEvent]:
        return events_since(
            self._queue, since_id=since_id,
            event_kind=event_kind, limit=limit,
        )

    def iter_events(self, **kwargs) -> Iterator[TaskEvent]:
        return iter_events(self._queue, **kwargs)

    def iter_sse(self, **kwargs) -> Iterator[str]:
        return iter_sse(self._queue, **kwargs)
