"""bounty_board.interventions — #11 voluntary-honor cooperative substrate.

A *supervising* agent (or the operator) posts an intervention against
an in-flight task; the *working* agent's claim loop reads pending
interventions at each tool-call safe boundary and *voluntarily*
honors them. The voluntary-honor model is deliberate per design lane
decision_id ``cluster_brokerless_task_queue_pitch_v0``:

  * The working agent is the canonical authority on what's safe to do
    next — only it knows whether the current tool call is mid-write,
    mid-network, mid-anything-non-resumable. A coercive kill-switch
    would corrupt task trajectories and break Option D forensic
    continuity (diagnose.py reads the trajectory; a coerced halt
    leaves the dossier ambiguous about what state the task was in).
  * The honor stamp + the ``intervene`` task_event row together make
    the cooperation legible: future audits can see which interventions
    were posted, which were honored, when, and (via the event note)
    why the agent chose to honor that one specifically.

Schema: ``interventions`` table from ``0001_initial.sql``. No new
migrations.

Surface:

  interventions.post(queue, task_id, kind, *, payload=None, agent_id)
                                              -> int (intervention id)
  interventions.get(queue, intervention_id)   -> Intervention | None
  interventions.list_for_task(queue, task_id, *, pending_only=False)
                                              -> list[Intervention]
  interventions.check_pending(queue, task_id) -> list[Intervention]
  interventions.honor(queue, intervention_id, *, agent_id, note=None)
                                              -> int (event id)
  InterventionHonor(queue, task_id, agent_id) — agent-side wrapper

Note: ``inspect.py`` writes directly to the table for the UI's
intervene-form POST (HTMX partial swap). That's fine — this module
provides the canonical agent-side surface; the UI's direct write
goes through the same schema. Eventually inspect.py can be refactored
to call ``interventions.post`` for consistency, but V1 keeps both
paths to minimize blast radius.

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from bounty_board.queue import Queue


# ─── data shape ─────────────────────────────────────────────────────


@dataclass
class Intervention:
    """A hydrated ``interventions`` row. Read-only view; mutate via
    the module-level functions, not the dataclass."""

    id: int
    task_id: str
    kind: str
    payload: dict | None
    posted_by_agent_id: str
    posted_at: float
    honored_at: float | None

    @property
    def is_pending(self) -> bool:
        return self.honored_at is None

    @property
    def is_honored(self) -> bool:
        return self.honored_at is not None


def _row_to_intervention(row: Any) -> Intervention:
    payload_raw = row["payload_json"]
    return Intervention(
        id=row["id"],
        task_id=row["task_id"],
        kind=row["kind"],
        payload=json.loads(payload_raw) if payload_raw else None,
        posted_by_agent_id=row["posted_by_agent_id"],
        posted_at=row["posted_at"],
        honored_at=row["honored_at"],
    )


# ─── post (supervising-side) ────────────────────────────────────────


def post(queue: Queue, task_id: str, kind: str,
         *, payload: dict | None = None, agent_id: str) -> int:
    """Record a new intervention against ``task_id``. Returns the new
    intervention id.

    ``kind`` is free-form (the schema's CHECK leaves it open); the
    cluster convention is to use short verb-like tokens —
    ``'cancel'``, ``'nudge'``, ``'swap_model'``, ``'escalate'``,
    ``'pause'`` — but agents and operators can introduce new kinds
    without a schema change. The receiving agent's honor logic
    determines the meaning.

    Raises ``ValueError`` if ``task_id`` doesn't exist (FK violation
    surfaces here rather than as a cryptic IntegrityError).
    """
    # Pre-check task existence so we return a clean error instead of
    # the raw FK-constraint SQLite message. Cheap — same lock domain
    # as the insert that follows.
    row = queue._conn.execute(
        "SELECT 1 FROM tasks WHERE id = ?", (task_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"no task with id {task_id!r}")

    now = time.time()
    payload_json = json.dumps(payload) if payload is not None else None
    with queue._conn:
        cur = queue._conn.execute(
            """
            INSERT INTO interventions
                (task_id, kind, payload_json,
                 posted_by_agent_id, posted_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (task_id, kind, payload_json, agent_id, now),
        )
        return cur.lastrowid


# ─── read ───────────────────────────────────────────────────────────


def get(queue: Queue, intervention_id: int) -> Intervention | None:
    """Look up a single intervention by id, or None."""
    row = queue._conn.execute(
        "SELECT * FROM interventions WHERE id = ?", (intervention_id,),
    ).fetchone()
    return _row_to_intervention(row) if row is not None else None


def list_for_task(queue: Queue, task_id: str,
                  *, pending_only: bool = False) -> list[Intervention]:
    """All interventions posted against ``task_id`` in posted_at ASC
    order. With ``pending_only=True``, filters out honored rows."""
    if pending_only:
        rows = queue._conn.execute(
            """
            SELECT * FROM interventions
            WHERE task_id = ? AND honored_at IS NULL
            ORDER BY posted_at ASC
            """,
            (task_id,),
        ).fetchall()
    else:
        rows = queue._conn.execute(
            """
            SELECT * FROM interventions
            WHERE task_id = ? ORDER BY posted_at ASC
            """,
            (task_id,),
        ).fetchall()
    return [_row_to_intervention(r) for r in rows]


def check_pending(queue: Queue, task_id: str) -> list[Intervention]:
    """Alias for ``list_for_task(task_id, pending_only=True)``. The
    canonical name for the working-agent safe-point poll path."""
    return list_for_task(queue, task_id, pending_only=True)


# ─── honor (working-side) ───────────────────────────────────────────


def honor(queue: Queue, intervention_id: int, *,
          agent_id: str, note: str | None = None,
          token_count: int = 0) -> int:
    """Mark an intervention honored and emit a corresponding
    ``intervene`` task_event row.

    The dual write (UPDATE + INSERT) lands inside a single transaction
    so the dossier can never see a half-state (event without
    honored_at, or honored without event). Returns the new task_event
    id.

    Raises:

      * ``ValueError`` — intervention id doesn't exist
      * ``ValueError`` — intervention is already honored (no
        double-honor; if you genuinely want to repost, post a new
        intervention)
    """
    row = queue._conn.execute(
        "SELECT * FROM interventions WHERE id = ?", (intervention_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"no intervention with id {intervention_id!r}")
    if row["honored_at"] is not None:
        raise ValueError(
            f"intervention {intervention_id!r} already honored "
            f"at ts={row['honored_at']!r}"
        )

    now = time.time()
    event_payload = {
        "intervention_id": intervention_id,
        "kind": row["kind"],
        "posted_by_agent_id": row["posted_by_agent_id"],
        "note": note,
    }
    with queue._conn:
        queue._conn.execute(
            "UPDATE interventions SET honored_at = ? WHERE id = ?",
            (now, intervention_id),
        )
        cur = queue._conn.execute(
            """
            INSERT INTO task_events
                (task_id, event_kind, ts, agent_id, payload_json,
                 token_count)
            VALUES (?, 'intervene', ?, ?, ?, ?)
            """,
            (row["task_id"], now, agent_id,
             json.dumps(event_payload), token_count),
        )
        return cur.lastrowid


# ─── ergonomic wrapper (agent-side) ─────────────────────────────────


class InterventionHonor:
    """Agent-side ergonomic helper. Bind to a ``(task_id, agent_id)``
    at claim time; the agent calls ``pending()`` at each tool-call
    safe boundary and ``honor()`` on the ones it decides to honor.

    Voluntary-honor is preserved: the helper exposes the pending list
    and exposes the per-id honor call; it never auto-honors. An agent
    that wants "honor everything pending in one shot" has the
    ``honor_all()`` helper, but the default loop is per-intervention.
    """

    def __init__(self, queue: Queue, task_id: str, agent_id: str):
        self._queue = queue
        self._task_id = task_id
        self._agent_id = agent_id

    def pending(self) -> list[Intervention]:
        """Currently-unhonored interventions on this task."""
        return check_pending(self._queue, self._task_id)

    def honor(self, intervention_id: int, *,
              note: str | None = None, token_count: int = 0) -> int:
        """Honor a single intervention. Returns the new task_event id."""
        return honor(
            self._queue, intervention_id,
            agent_id=self._agent_id, note=note,
            token_count=token_count,
        )

    def honor_all(self, *, note: str | None = None) -> list[int]:
        """Convenience: honor every pending intervention right now.
        Returns the list of new task_event ids in honor order.

        Use sparingly — the V1 design intent is per-intervention
        judgment by the working agent, not blanket honoring. Useful
        for cancel-style flows where the agent's already decided to
        stop and is clearing the inbox before exit.
        """
        ids: list[int] = []
        for iv in self.pending():
            ids.append(self.honor(iv.id, note=note))
        return ids
