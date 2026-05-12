"""bounty_board.aio — async wrapper over the sync core.

Zero new dependencies. Uses ``asyncio`` + ``concurrent.futures`` from
the stdlib; SQLite's ``check_same_thread=True`` default is honored
because every queue lives on a dedicated single-worker
``ThreadPoolExecutor`` — all DB calls land on the same worker
thread, so the connection's thread-affinity invariant holds without
either monkey-patching the connection or pulling in ``aiosqlite``.

Per design lane decision_id ``cluster_brokerless_task_queue_pitch_v0``:
keep the sync core as the substrate-of-record and provide a thin
async surface that async event loops can ``await`` without blocking.
The async surface delegates 1:1 to the sync surface; semantics are
identical.

Surface:

  AsyncQueue(path, *, stale_open_seconds=...)
    .post(...) -> str
    .claim(*, agent_id) -> AsyncTask | None
    .depth() -> int
    .get_task(task_id) -> dict | None
    .close() -> None
    async-context-manager (__aenter__ / __aexit__)
    .run(fn, *a, **kw) — escape hatch for other modules
                         (dlq/diagnose/patches/budget/stream...)

  AsyncTask(_task, _aqueue)
    .id, .task_type, .payload_signature, .payload (read-only)
    .complete(result=None, token_count=0)  -> None
    .fail(stack, prompt_state=None, token_count=0) -> None
    .decline(reason)                        -> None

Anti-goal: re-implementing every module's surface as async wrappers
right now. The single-worker executor model means callers can wrap
any sync call themselves via ``await aqueue.run(stream.events_since,
aqueue.sync, since_id=cursor)``. The async substrate is the
executor; the convenience wrappers above cover the hot Queue/Task
loop.

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from bounty_board.queue import (
    DEFAULT_STALE_OPEN_SECONDS,
    Queue,
    Task,
)

if TYPE_CHECKING:
    pass


T = TypeVar("T")


class AsyncQueue:
    """Async-await wrapper around :class:`Queue`.

    All DB operations run on a single-worker executor created at
    construction time; the ``Queue`` is itself constructed on that
    worker so its SQLite connection's thread-affinity invariant
    holds across the lifetime of the AsyncQueue.

    Usage::

        async with AsyncQueue(\"q.db\") as aq:
            tid = await aq.post(task_type=\"t\", payload={\"x\": 1})
            task = await aq.claim(agent_id=\"a\")
            if task is not None:
                await task.complete(token_count=100)
    """

    def __init__(self, path: str | Path,
                 *, stale_open_seconds: float = DEFAULT_STALE_OPEN_SECONDS):
        # Dedicated single-worker thread so every SQLite call lands
        # on the same thread (Python's sqlite3 module enforces
        # check_same_thread=True by default).
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="bounty-board-aio",
        )
        # Construct the underlying Queue ON the worker thread so its
        # connection is owned by that thread.
        self._queue: Queue = self._executor.submit(
            lambda: Queue(path, stale_open_seconds=stale_open_seconds),
        ).result()
        self._closed = False

    @property
    def sync(self) -> Queue:
        """Escape hatch: the underlying sync ``Queue`` for callers
        that need to pass it into another module function.

        IMPORTANT: any call on this object must be dispatched via
        ``self.run(...)`` to land on the right thread. Reaching
        into ``.sync._conn`` from the asyncio thread will trigger
        ``ProgrammingError: SQLite objects created in a thread
        can only be used in that same thread``.
        """
        return self._queue

    @property
    def path(self) -> Path:
        return self._queue.path

    async def run(self, fn: Callable[..., T], *args: Any,
                  **kwargs: Any) -> T:
        """Dispatch an arbitrary callable to the queue's worker
        thread. The general-purpose escape hatch for wrapping any
        sync module function (dlq, diagnose, patches, budget,
        stream...) with async semantics.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, lambda: fn(*args, **kwargs),
        )

    # ─── post / claim / depth / get_task ────────────────────────────

    async def post(self, *, task_type: str, payload: dict,
                   payload_signature: str | None = None,
                   priority: int = 0,
                   max_attempts: int = 3,
                   parent_id: str | None = None) -> str:
        return await self.run(
            self._queue.post,
            task_type=task_type, payload=payload,
            payload_signature=payload_signature,
            priority=priority, max_attempts=max_attempts,
            parent_id=parent_id,
        )

    async def claim(self, *, agent_id: str) -> AsyncTask | None:
        task = await self.run(self._queue.claim, agent_id=agent_id)
        if task is None:
            return None
        return AsyncTask(task, self)

    async def depth(self) -> int:
        return await self.run(self._queue.depth)

    async def get_task(self, task_id: str) -> dict | None:
        return await self.run(self._queue.get_task, task_id)

    # ─── lifecycle ──────────────────────────────────────────────────

    async def close(self) -> None:
        """Close the underlying connection + shut down the executor.
        Idempotent — calling twice is safe."""
        if self._closed:
            return
        self._closed = True
        # Close on the worker thread (the connection's owner).
        await self.run(self._queue.close)
        self._executor.shutdown(wait=True)

    async def __aenter__(self) -> AsyncQueue:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


class AsyncTask:
    """Async-await wrapper around :class:`Task`. Delegates lifecycle
    methods (complete / fail / decline) to the parent ``AsyncQueue``'s
    executor so the underlying ``Task``'s connection access lands on
    the right thread."""

    def __init__(self, task: Task, aqueue: AsyncQueue):
        self._task = task
        self._aqueue = aqueue

    @property
    def id(self) -> str:
        return self._task.id

    @property
    def task_type(self) -> str:
        return self._task.task_type

    @property
    def payload_signature(self) -> str:
        return self._task.payload_signature

    @property
    def payload(self) -> dict:
        return self._task.payload

    @property
    def attempts(self) -> int:
        return self._task.attempts

    @property
    def max_attempts(self) -> int:
        return self._task.max_attempts

    async def complete(self, *, result: dict | None = None,
                       token_count: int = 0) -> None:
        await self._aqueue.run(
            self._task.complete, result=result, token_count=token_count,
        )

    async def fail(self, *, stack: str,
                   prompt_state: dict | None = None,
                   token_count: int = 0) -> None:
        await self._aqueue.run(
            self._task.fail, stack=stack,
            prompt_state=prompt_state, token_count=token_count,
        )

    async def decline(self, *, reason: str) -> None:
        await self._aqueue.run(self._task.decline, reason=reason)
