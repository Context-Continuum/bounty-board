"""bounty_board.dlq — DLQ list / get / replay surface.

The "time-travel" Dead Letter Queue per the Bounty Board pitch. Built
on the existing schema (no schema changes): a "failed" or
"unclaimable" task in `tasks` is a DLQ entry; its full forensic
trajectory lives in `task_events`.

V0 surface (what this PR ships):

  Queue.dlq().list(limit=50)            -> list[DLQEntry]
  Queue.dlq().get(task_id)              -> DLQEntry | None
  Queue.dlq().replay(task_id)           -> str (new task_id)
  Queue.dlq().purge_older_than(days=30) -> int

DLQEntry carries the task row PLUS the trajectory (every task_events
row for that task in order). Operators / agents read it for
post-mortem analysis. Replay creates a FRESH task (new task_id) with
``parent_id`` pointing at the original — provenance chain preserved,
not idempotent replay. The original failed task stays in 'failed'
for audit trail.

Option D self-diagnostic (agent reads its own failure dossier) lands
in a separate PR (diagnose.py) on top of this surface.

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bounty_board.queue import Queue


# Statuses that put a task into the DLQ. 'failed' = exhausted
# max_attempts. 'unclaimable' = (V1 reserved; not currently emitted by
# claim path, but spec'd for "all candidate agents declined" surfaces
# in future routing work).
DLQ_STATUSES: tuple[str, ...] = ("failed", "unclaimable")


@dataclass
class TaskEvent:
    """One row from task_events. Flattened for ergonomic access."""

    id: int
    task_id: str
    event_kind: str
    ts: float
    agent_id: str | None
    payload: dict | None
    token_count: int


@dataclass
class DLQEntry:
    """A failed task + its full event trajectory.

    Read-only forensic view. To operate on a DLQ entry use the parent
    DLQ surface methods (replay, purge).
    """

    task_id: str
    task_type: str
    payload_signature: str
    payload: dict
    status: str
    attempts: int
    max_attempts: int
    created_at: float
    completed_at: float | None
    parent_id: str | None
    trajectory: list[TaskEvent] = field(default_factory=list)

    @property
    def final_fail_event(self) -> TaskEvent | None:
        """The most-recent 'fail' event in the trajectory, or None.
        This is the row carrying {stack, prompt_state, post_status}
        — the canonical forensic payload for Option D self-diagnosis.
        """
        for ev in reversed(self.trajectory):
            if ev.event_kind == "fail":
                return ev
        return None

    @property
    def total_token_count(self) -> int:
        """Sum of token_count across every event in the trajectory."""
        return sum(ev.token_count for ev in self.trajectory)


class DLQ:
    """The DLQ surface. Not instantiable directly — use
    ``Queue.dlq()`` to get one bound to a queue.
    """

    def __init__(self, queue: Queue):
        self._queue = queue
        self._conn = queue._conn

    # ─── list ──────────────────────────────────────────────────────

    def list(self, *, limit: int = 50,
             payload_signature: str | None = None) -> list[DLQEntry]:
        """Return DLQ entries ordered by most-recently-failed first.

        Filter by ``payload_signature`` to scope the view. Each
        returned entry carries its full trajectory.
        """
        if payload_signature is not None:
            rows = self._conn.execute(
                """
                SELECT * FROM tasks
                WHERE status IN ('failed', 'unclaimable')
                  AND payload_signature = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (payload_signature, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT * FROM tasks
                WHERE status IN ('failed', 'unclaimable')
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._entry_for_row(r) for r in rows]

    def get(self, task_id: str) -> DLQEntry | None:
        """Look up a single DLQ entry by task_id. Returns None if
        the task doesn't exist OR isn't in a DLQ status."""
        row = self._conn.execute(
            "SELECT * FROM tasks WHERE id = ? AND status IN ('failed', 'unclaimable')",
            (task_id,),
        ).fetchone()
        if row is None:
            return None
        return self._entry_for_row(row)

    def _entry_for_row(self, row) -> DLQEntry:
        """Hydrate a tasks-row + its task_events trajectory into a
        DLQEntry."""
        events = [
            TaskEvent(
                id=e["id"],
                task_id=e["task_id"],
                event_kind=e["event_kind"],
                ts=e["ts"],
                agent_id=e["agent_id"],
                payload=(json.loads(e["payload_json"])
                         if e["payload_json"] else None),
                token_count=e["token_count"],
            )
            for e in self._conn.execute(
                """
                SELECT * FROM task_events
                WHERE task_id = ? ORDER BY id ASC
                """,
                (row["id"],),
            ).fetchall()
        ]
        return DLQEntry(
            task_id=row["id"],
            task_type=row["task_type"],
            payload_signature=row["payload_signature"],
            payload=json.loads(row["payload_json"]),
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
            parent_id=row["parent_id"],
            trajectory=events,
        )

    # ─── replay ────────────────────────────────────────────────────

    def replay(self, task_id: str) -> str:
        """Replay a DLQ entry as a FRESH task.

        Provenance-chain replay (not idempotent): we create a new
        task_id with parent_id = the original. The original failed
        task STAYS in 'failed' state for the audit trail; the new
        task starts at attempts=0 with the same payload and signature.

        The 'replay' relationship is queryable later via tasks.parent_id
        joins on the original task_id.

        Returns the new task_id. Raises ValueError if the source
        task is not in a DLQ status (failed/unclaimable).
        """
        src = self._conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if src is None:
            raise ValueError(f"no task with id {task_id!r}")
        if src["status"] not in DLQ_STATUSES:
            raise ValueError(
                f"task {task_id!r} is not in a DLQ status "
                f"(status={src['status']!r}); replay refused"
            )

        new_id = uuid.uuid4().hex
        now = time.time()
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO tasks (
                    id, payload_json, task_type, payload_signature,
                    priority, status, max_attempts, parent_id, created_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                """,
                (new_id, src["payload_json"], src["task_type"],
                 src["payload_signature"], src["priority"],
                 src["max_attempts"], task_id, now),
            )
            # Emit a 'replay' event on the NEW task pointing back at the
            # original for audit-trail traversal.
            self._conn.execute(
                """
                INSERT INTO task_events
                    (task_id, event_kind, ts, agent_id, payload_json,
                     token_count)
                VALUES (?, 'replay', ?, NULL, ?, 0)
                """,
                (new_id, now,
                 json.dumps({"replayed_from": task_id})),
            )
        return new_id

    # ─── purge ─────────────────────────────────────────────────────

    def purge_older_than(self, *, days: float) -> int:
        """Delete DLQ entries older than ``days`` (by tasks.created_at).
        Returns count purged.

        Cascades to task_events for the purged task_ids — full audit
        trail goes away with the parent task. This is the lifecycle-
        ops hook from Win's V1 design notes; agents who want to
        archive before purge can subscribe to a future archive hook
        or read .list() before calling purge.
        """
        cutoff = time.time() - (days * 86400)
        with self._conn:
            # Get the task_ids first so we know what to delete events for.
            rows = self._conn.execute(
                """
                SELECT id FROM tasks
                WHERE status IN ('failed', 'unclaimable')
                  AND created_at < ?
                """,
                (cutoff,),
            ).fetchall()
            ids = [r["id"] for r in rows]
            if not ids:
                return 0
            # Delete events first (FK references tasks.id).
            placeholders = ",".join("?" * len(ids))
            self._conn.execute(
                f"DELETE FROM task_events WHERE task_id IN ({placeholders})",
                ids,
            )
            self._conn.execute(
                f"DELETE FROM tasks WHERE id IN ({placeholders})",
                ids,
            )
        return len(ids)

    # ─── counts ────────────────────────────────────────────────────

    def depth(self) -> int:
        """Number of tasks currently in a DLQ status."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM tasks "
            "WHERE status IN ('failed', 'unclaimable')"
        ).fetchone()
        return row["n"]
