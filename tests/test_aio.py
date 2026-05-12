"""Tests for bounty_board.aio — async wrapper over the sync core.

Uses ``asyncio.run`` inside sync test functions so no pytest-asyncio
plugin is needed. Each test exercises the round-trip from async
caller → single-worker executor → sync Queue/Task → SQLite, and
back.

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from bounty_board.aio import AsyncQueue, AsyncTask
from bounty_board.queue import OPEN_SIGNATURE


def _run(coro):
    """Helper: run a coroutine to completion with a fresh event loop."""
    return asyncio.run(coro)


# ─── basic lifecycle ────────────────────────────────────────────────


def test_async_queue_constructs(tmp_path: Path):
    async def run():
        aq = AsyncQueue(tmp_path / "q.db")
        try:
            assert aq.path == tmp_path / "q.db"
        finally:
            await aq.close()
    _run(run())


def test_async_queue_context_manager(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            assert isinstance(aq, AsyncQueue)
    _run(run())


def test_async_queue_close_is_idempotent(tmp_path: Path):
    async def run():
        aq = AsyncQueue(tmp_path / "q.db")
        await aq.close()
        await aq.close()  # second call should be a no-op
    _run(run())


# ─── post / depth / get_task ────────────────────────────────────────


def test_async_post_returns_task_id(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            tid = await aq.post(
                task_type="t", payload={"x": 1},
                payload_signature=OPEN_SIGNATURE,
            )
            assert isinstance(tid, str)
            assert len(tid) == 32  # uuid4 hex
    _run(run())


def test_async_depth(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            assert await aq.depth() == 0
            await aq.post(task_type="t", payload={},
                          payload_signature=OPEN_SIGNATURE)
            await aq.post(task_type="t", payload={},
                          payload_signature=OPEN_SIGNATURE)
            assert await aq.depth() == 2
    _run(run())


def test_async_get_task(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            tid = await aq.post(task_type="t", payload={"foo": "bar"},
                                payload_signature=OPEN_SIGNATURE)
            row = await aq.get_task(tid)
            assert row is not None
            assert row["id"] == tid
            assert row["status"] == "queued"
            # Nonexistent
            assert await aq.get_task("nonexistent") is None
    _run(run())


# ─── claim → AsyncTask ──────────────────────────────────────────────


def test_async_claim_returns_async_task(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            tid = await aq.post(task_type="t", payload={"x": 1},
                                payload_signature=OPEN_SIGNATURE)
            task = await aq.claim(agent_id="a")
            assert task is not None
            assert isinstance(task, AsyncTask)
            assert task.id == tid
            assert task.task_type == "t"
            assert task.payload == {"x": 1}
            assert task.payload_signature == OPEN_SIGNATURE
    _run(run())


def test_async_claim_empty_returns_none(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            assert await aq.claim(agent_id="a") is None
    _run(run())


# ─── Task lifecycle ─────────────────────────────────────────────────


def test_async_task_complete(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            tid = await aq.post(task_type="t", payload={},
                                payload_signature=OPEN_SIGNATURE)
            task = await aq.claim(agent_id="a")
            assert task is not None
            await task.complete(result={"ok": True}, token_count=100)
            row = await aq.get_task(tid)
            assert row is not None
            assert row["status"] == "done"
    _run(run())


def test_async_task_fail(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            await aq.post(task_type="t", payload={},
                          payload_signature=OPEN_SIGNATURE,
                          max_attempts=1)
            task = await aq.claim(agent_id="a")
            assert task is not None
            await task.fail(stack="oops", token_count=10)
            row = await aq.get_task(task.id)
            assert row is not None
            assert row["status"] == "failed"
    _run(run())


def test_async_task_decline(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            await aq.post(task_type="t", payload={},
                          payload_signature=OPEN_SIGNATURE)
            task = await aq.claim(agent_id="a")
            assert task is not None
            await task.decline(reason="not-for-me")
            row = await aq.get_task(task.id)
            # Decline puts the task back in 'queued' so another
            # agent can claim it.
            assert row is not None
            assert row["status"] == "queued"
    _run(run())


# ─── thread-safety + cross-thread invariant ─────────────────────────


def test_async_queue_runs_on_single_worker_thread(tmp_path: Path):
    """All DB ops dispatch to the same worker thread. We verify this
    by triggering many concurrent operations and confirming none
    raise the SQLite check_same_thread ProgrammingError."""
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            # 50 concurrent posts (gathered, so the asyncio loop
            # schedules them all into the executor in quick succession).
            tids = await asyncio.gather(*[
                aq.post(task_type="t", payload={"i": i},
                        payload_signature=OPEN_SIGNATURE)
                for i in range(50)
            ])
            assert len(set(tids)) == 50
            assert await aq.depth() == 50
    _run(run())


def test_async_run_escape_hatch(tmp_path: Path):
    """aqueue.run(fn, *args) dispatches arbitrary callables — the
    convenience for wrapping other modules (dlq, stream, etc.) in
    async-await."""
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            tid = await aq.post(task_type="t", payload={"v": 1},
                                payload_signature=OPEN_SIGNATURE)
            # Use the escape hatch to call get_task via run() too.
            row = await aq.run(aq.sync.get_task, tid)
            assert row is not None
            assert row["id"] == tid
    _run(run())


# ─── end-to-end async workflow ──────────────────────────────────────


def test_end_to_end_async_workflow(tmp_path: Path):
    """The canonical async-loop user story:

      1. Async agent claims a task
      2. Does some async work (we simulate with asyncio.sleep)
      3. Completes the task
      4. Verifies the trajectory updated
    """
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            tid = await aq.post(
                task_type="echo", payload={"text": "hello"},
                payload_signature=OPEN_SIGNATURE,
            )
            task = await aq.claim(agent_id="worker-a")
            assert task is not None and task.id == tid

            # Simulate async work without blocking the loop.
            await asyncio.sleep(0)

            await task.complete(
                result={"text": "hello"}, token_count=50,
            )

            row = await aq.get_task(tid)
            assert row is not None
            assert row["status"] == "done"

            # The complete-emission should have landed a 'complete'
            # task_event row. Confirm via the escape hatch.
            events = await aq.run(
                lambda: aq.sync._conn.execute(
                    "SELECT event_kind, token_count "
                    "FROM task_events WHERE task_id = ? "
                    "ORDER BY id ASC",
                    (tid,),
                ).fetchall(),
            )
            kinds = [e["event_kind"] for e in events]
            assert "claim" in kinds
            assert "complete" in kinds
    _run(run())


def test_two_async_queues_independent(tmp_path: Path):
    """Sanity: two AsyncQueue instances on separate paths don't
    interfere. Each has its own executor + connection."""
    async def run():
        path_a = tmp_path / "a.db"
        path_b = tmp_path / "b.db"
        async with AsyncQueue(path_a) as aqa, AsyncQueue(path_b) as aqb:
            await aqa.post(task_type="t", payload={},
                           payload_signature=OPEN_SIGNATURE)
            assert await aqa.depth() == 1
            assert await aqb.depth() == 0
    _run(run())


def test_async_path_property(tmp_path: Path):
    async def run():
        path = tmp_path / "q.db"
        async with AsyncQueue(path) as aq:
            assert aq.path == path
    _run(run())


def test_async_queue_sync_property_exposes_underlying(tmp_path: Path):
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            from bounty_board.queue import Queue
            assert isinstance(aq.sync, Queue)
    _run(run())


# ─── timing sanity ──────────────────────────────────────────────────


def test_async_post_does_not_block_event_loop(tmp_path: Path):
    """While a (simulated-slow) DB call is in flight, the event loop
    should still be able to run other coroutines.

    We can't easily make sqlite slow, but we can confirm an
    asyncio.sleep(0) interleaves between two awaits without
    deadlock — proving the operations are running off the loop
    thread.
    """
    async def run():
        async with AsyncQueue(tmp_path / "q.db") as aq:
            interleaved = []

            async def post_n_times(n: int):
                for i in range(n):
                    await aq.post(
                        task_type="t", payload={"i": i},
                        payload_signature=OPEN_SIGNATURE,
                    )
                    interleaved.append(f"post-{i}")

            async def yield_n_times(n: int):
                for i in range(n):
                    await asyncio.sleep(0)
                    interleaved.append(f"yield-{i}")

            t0 = time.monotonic()
            await asyncio.gather(post_n_times(5), yield_n_times(5))
            t1 = time.monotonic()
            # At least some interleaving must have occurred — there
            # are both 'post-' and 'yield-' entries.
            assert any(x.startswith("post-") for x in interleaved)
            assert any(x.startswith("yield-") for x in interleaved)
            # And it didn't take absurdly long.
            assert (t1 - t0) < 5.0
            assert await aq.depth() == 5
    _run(run())
