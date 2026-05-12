"""bounty_board.queue — atomic claim path + earned-capability matching.

V1 sync surface. Async wrapper (asyncio.to_thread shim, no aiosqlite
dep) lands in a follow-up PR.

Earned-capability model (per design lane locked at decision_id
``cluster_brokerless_task_queue_pitch_v0``): tasks do NOT carry a
``capabilities[]`` column. Agents prove a capability by claiming +
succeeding on tasks with a given ``payload_signature``; the substrate
records that history in ``agent_track_record``. Routing offers
matching tasks to agents whose record-on-signature has success_n > 0.

Bootstrap rule for new agents (combined (a)+(d)):
  (a) SENTINEL: tasks with ``payload_signature='open'`` are claimable
      by any agent regardless of track record. Posters mark exploratory
      work with this sentinel.
  (d) STALE-OPEN AUTO-RELAXATION: tasks with a real (non-'open')
      payload_signature that have been queued longer than
      ``stale_open_seconds`` (default 300s = 5 min) without a
      qualified-track-record agent claiming become claimable by any
      agent. Substrate-side rule, no poster involvement.

Claim atomicity: SQLite ``BEGIN IMMEDIATE`` + conditional UPDATE...
RETURNING. The transaction holds the SQLite reserved-lock; concurrent
claimants serialize at the lock boundary, and the UPDATE's WHERE
clause ensures only one wins (status=queued check is re-evaluated
inside the transaction).

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from bounty_board._meta import open_db

# Default lookback for bootstrap rule (d) — task is "stale-open" if it
# has been queued longer than this without a qualified-track-record
# agent claiming. 300s = 5 min. Tunable per-queue at construction.
DEFAULT_STALE_OPEN_SECONDS = 300.0

# Sentinel value for bootstrap rule (a) — tasks tagged 'open' are
# claimable by any agent regardless of earned capabilities.
OPEN_SIGNATURE = "open"


@dataclass
class Task:
    """A claimed task. Lifecycle methods (``complete``, ``fail``,
    ``decline``) write to the queue's underlying DB.

    Constructed by ``Queue.claim``; not instantiable directly by
    callers (private constructor convention).
    """

    id: str
    payload: dict
    task_type: str
    payload_signature: str
    priority: int
    attempts: int
    max_attempts: int
    claimed_by: str
    claimed_at: float
    _queue: Queue

    def complete(self, result: dict | None = None,
                 token_count: int = 0) -> None:
        """Mark task done. Increments ``success_n`` on the agent's
        track record for this signature, emits a ``complete`` event."""
        self._queue._mark_complete(
            self.id, agent_id=self.claimed_by,
            payload_signature=self.payload_signature,
            result=result, token_count=token_count,
        )

    def fail(self, stack: str, prompt_state: dict | None = None,
             token_count: int = 0) -> None:
        """Mark task failed (forensic capture). Increments ``fail_n``
        on the agent's track record + emits a ``fail`` event carrying
        the stack + prompt_state for Option D self-diagnostic replay.

        If ``attempts < max_attempts``, the task returns to ``queued``
        for replay (with attempts incremented). Otherwise it stays
        in ``failed`` for DLQ inspection.
        """
        self._queue._mark_fail(
            self.id, agent_id=self.claimed_by,
            payload_signature=self.payload_signature,
            stack=stack, prompt_state=prompt_state,
            token_count=token_count,
        )

    def decline(self, reason: str) -> None:
        """Return task to queue (cooperative mismatch, not a failure).
        Increments ``decline_n`` SEPARATELY from fail_n. Decline is a
        routing hint ("don't re-offer this signature to this agent for
        a while") but does NOT count against the agent's earned-
        capability ratio.
        """
        self._queue._mark_decline(
            self.id, agent_id=self.claimed_by,
            payload_signature=self.payload_signature,
            reason=reason,
        )


class Queue:
    """A bounty board. One SQLite file = one queue.

    Usage:
        q = Queue("./my_queue.db")
        q.post(task_type="review_pr", payload={"pr_id": 123},
               payload_signature="review_pr")
        task = q.claim(agent_id="agent_42")
        if task:
            try:
                # ... agent does work ...
                task.complete(result={"ok": True}, token_count=5000)
            except Exception:
                import traceback
                task.fail(stack=traceback.format_exc())
    """

    def __init__(self, path: str | Path, *,
                 stale_open_seconds: float = DEFAULT_STALE_OPEN_SECONDS):
        self.path = Path(path)
        self.stale_open_seconds = stale_open_seconds
        # open_db handles schema migrations + WAL mode.
        self._conn = open_db(self.path)
        self._conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Queue:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ─── Post side ─────────────────────────────────────────────────

    def post(self, *, task_type: str, payload: dict,
             payload_signature: str | None = None,
             priority: int = 0,
             max_attempts: int = 3,
             parent_id: str | None = None) -> str:
        """Post a new task. Returns the task id (uuid4 hex).

        ``payload_signature`` defaults to ``task_type`` when omitted.
        Use ``OPEN_SIGNATURE`` ('open') to make the task claimable
        by any agent regardless of track record (bootstrap rule a).
        """
        task_id = uuid.uuid4().hex
        signature = payload_signature or task_type
        now = time.time()
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO tasks (
                    id, payload_json, task_type, payload_signature,
                    priority, status, max_attempts, parent_id, created_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                """,
                (task_id, json.dumps(payload), task_type, signature,
                 priority, max_attempts, parent_id, now),
            )
        return task_id

    # ─── Claim side ────────────────────────────────────────────────

    def claim(self, *, agent_id: str) -> Task | None:
        """Atomically claim one task matching the agent's earned
        capabilities, or the (a)+(d) bootstrap rules.

        Returns None when no claimable task exists. SQLite-atomic via
        ``BEGIN IMMEDIATE`` — concurrent claimants serialize at the
        reserved-lock boundary and only one wins each task.
        """
        now = time.time()
        stale_cutoff = now - self.stale_open_seconds

        # We BEGIN IMMEDIATE so the reserved-lock is taken before the
        # SELECT — otherwise two concurrent claimants could both see
        # the same row + race on the UPDATE. With BEGIN IMMEDIATE the
        # second transaction blocks until the first commits/rolls
        # back, then re-reads.
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                """
                SELECT id, payload_json, task_type, payload_signature,
                       priority, attempts, max_attempts
                FROM tasks
                WHERE status = 'queued'
                  AND (
                       -- Rule (a): sentinel 'open' — claimable by anyone
                       payload_signature = ?
                       -- Normal earned route: agent has track record
                       OR payload_signature IN (
                           SELECT payload_signature
                           FROM agent_track_record
                           WHERE agent_id = ?
                             AND success_n > 0
                       )
                       -- Rule (d): stale-open — no qualified agent has
                       -- claimed within stale_cutoff; opens to anyone.
                       -- We approximate "no qualified agent has tried"
                       -- by checking created_at < stale_cutoff (i.e.
                       -- the task has been queued past the window).
                       --
                       -- V1-LIMIT (intentional, per Win review of PR #4):
                       -- "agent has decline_n > 0" is a PERMANENT shadow
                       -- on the (agent, signature) pair — a single ancient
                       -- decline keeps the agent out of stale-open route
                       -- on that signature forever. Substrate-correct
                       -- ("don't re-offer") but probably too aggressive
                       -- across weeks/months. V2 plan: time-window via
                       -- agent_track_record.last_seen_at — "declines
                       -- older than 24h don't count." Substrate already
                       -- carries last_seen_at, so V2 needs no schema
                       -- change.
                       OR (
                           created_at < ?
                           AND payload_signature NOT IN (
                               SELECT payload_signature
                               FROM agent_track_record
                               WHERE agent_id = ? AND decline_n > 0
                           )
                       )
                  )
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (OPEN_SIGNATURE, agent_id, stale_cutoff, agent_id),
            ).fetchone()

            if row is None:
                self._conn.execute("ROLLBACK")
                return None

            # Atomic state transition: queued -> claimed. WHERE clause
            # re-checks status='queued' to handle the (rare) case where
            # another writer mutated the row between our SELECT and the
            # UPDATE within the same transaction (shouldn't happen with
            # BEGIN IMMEDIATE, but belt-and-suspenders).
            updated = self._conn.execute(
                """
                UPDATE tasks
                SET status = 'claimed',
                    claimed_by = ?,
                    claimed_at = ?,
                    attempts = attempts + 1
                WHERE id = ? AND status = 'queued'
                """,
                (agent_id, now, row["id"]),
            ).rowcount

            if updated != 1:
                # Lost the race somehow — bail out cleanly.
                self._conn.execute("ROLLBACK")
                return None

            # Emit 'claim' event for the time-travel ledger / #9 stream.
            self._conn.execute(
                """
                INSERT INTO task_events
                    (task_id, event_kind, ts, agent_id, payload_json,
                     token_count)
                VALUES (?, 'claim', ?, ?, NULL, 0)
                """,
                (row["id"], now, agent_id),
            )

            # Touch the agent's last_seen on this signature (no
            # success/fail counter change at claim time — those happen
            # on complete/fail/decline).
            self._conn.execute(
                """
                INSERT INTO agent_track_record
                    (agent_id, payload_signature, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT (agent_id, payload_signature) DO UPDATE
                SET last_seen_at = excluded.last_seen_at
                """,
                (agent_id, row["payload_signature"], now),
            )

            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        return Task(
            id=row["id"],
            payload=json.loads(row["payload_json"]),
            task_type=row["task_type"],
            payload_signature=row["payload_signature"],
            priority=row["priority"],
            attempts=row["attempts"] + 1,
            max_attempts=row["max_attempts"],
            claimed_by=agent_id,
            claimed_at=now,
            _queue=self,
        )

    # ─── Task-lifecycle internals (called by Task methods) ─────────

    def _mark_complete(self, task_id: str, *, agent_id: str,
                       payload_signature: str,
                       result: dict | None, token_count: int) -> None:
        now = time.time()
        with self._conn:
            self._conn.execute(
                """
                UPDATE tasks
                SET status = 'done', completed_at = ?
                WHERE id = ?
                """,
                (now, task_id),
            )
            self._conn.execute(
                """
                INSERT INTO task_events
                    (task_id, event_kind, ts, agent_id, payload_json,
                     token_count)
                VALUES (?, 'complete', ?, ?, ?, ?)
                """,
                (task_id, now, agent_id,
                 json.dumps(result) if result else None,
                 token_count),
            )
            self._conn.execute(
                """
                INSERT INTO agent_track_record
                    (agent_id, payload_signature, success_n, last_seen_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT (agent_id, payload_signature) DO UPDATE
                SET success_n = success_n + 1,
                    last_seen_at = excluded.last_seen_at
                """,
                (agent_id, payload_signature, now),
            )

    def _mark_fail(self, task_id: str, *, agent_id: str,
                   payload_signature: str, stack: str,
                   prompt_state: dict | None,
                   token_count: int) -> None:
        now = time.time()
        # Decide post-fail status: re-queue if attempts < max, else
        # park in 'failed' for DLQ.
        with self._conn:
            cur = self._conn.execute(
                "SELECT attempts, max_attempts FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if cur is not None and cur["attempts"] < cur["max_attempts"]:
                new_status = "queued"
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'queued', claimed_by = NULL, claimed_at = NULL
                    WHERE id = ?
                    """,
                    (task_id,),
                )
            else:
                new_status = "failed"
                self._conn.execute(
                    "UPDATE tasks SET status = 'failed' WHERE id = ?",
                    (task_id,),
                )
            self._conn.execute(
                """
                INSERT INTO task_events
                    (task_id, event_kind, ts, agent_id, payload_json,
                     token_count)
                VALUES (?, 'fail', ?, ?, ?, ?)
                """,
                (task_id, now, agent_id,
                 json.dumps({
                     "stack": stack,
                     "prompt_state": prompt_state,
                     "post_status": new_status,
                 }),
                 token_count),
            )
            self._conn.execute(
                """
                INSERT INTO agent_track_record
                    (agent_id, payload_signature, fail_n, last_seen_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT (agent_id, payload_signature) DO UPDATE
                SET fail_n = fail_n + 1,
                    last_seen_at = excluded.last_seen_at
                """,
                (agent_id, payload_signature, now),
            )

    def _mark_decline(self, task_id: str, *, agent_id: str,
                      payload_signature: str, reason: str) -> None:
        """DECLINE: return to queue, increment decline_n SEPARATELY
        from fail_n. Decline is cooperative routing hint, not failure.
        """
        now = time.time()
        with self._conn:
            self._conn.execute(
                """
                UPDATE tasks
                SET status = 'queued', claimed_by = NULL, claimed_at = NULL
                WHERE id = ?
                """,
                (task_id,),
            )
            self._conn.execute(
                """
                INSERT INTO task_events
                    (task_id, event_kind, ts, agent_id, payload_json,
                     token_count)
                VALUES (?, 'decline', ?, ?, ?, 0)
                """,
                (task_id, now, agent_id,
                 json.dumps({"reason": reason})),
            )
            self._conn.execute(
                """
                INSERT INTO agent_track_record
                    (agent_id, payload_signature, decline_n, last_seen_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT (agent_id, payload_signature) DO UPDATE
                SET decline_n = decline_n + 1,
                    last_seen_at = excluded.last_seen_at
                """,
                (agent_id, payload_signature, now),
            )

    # ─── Read-side helpers (for tests + #9 SSE later) ──────────────

    def depth(self) -> int:
        """Number of queued tasks."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM tasks WHERE status = 'queued'"
        ).fetchone()
        return row["n"]

    def get_task(self, task_id: str) -> dict | None:
        """Return task row as dict, or None."""
        row = self._conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        return dict(row) if row else None

    def dlq(self):
        """Return the DLQ surface bound to this queue. See
        ``bounty_board.dlq.DLQ`` for the list / get / replay /
        purge_older_than / depth methods.

        Untyped signature to avoid a circular-import dance — dlq.py
        imports Queue under TYPE_CHECKING for its own annotations."""
        from bounty_board.dlq import DLQ
        return DLQ(self)
