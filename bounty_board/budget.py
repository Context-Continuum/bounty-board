"""bounty_board.budget — #10 token-budget back-pressure substrate.

Queue-scope cap on accumulated ``task_events.token_count`` SUM, with
three back-pressure policies and a ``budget_state`` task_event the
``#9`` SSE stream surfaces to subscribed agents.

Storage:

  * config lives in the existing ``_meta`` table under the keys
    ``budget.limit_tokens``, ``budget.window_seconds`` (optional —
    omit for lifetime budgets), and ``budget.policy``. No new
    migrations.
  * spend is computed at read time as
    ``SELECT SUM(token_count) FROM task_events`` (optionally
    constrained to ``ts > now - window_seconds``). No counter to
    maintain; reads are the source of truth.

Policies:

  * ``soft`` — never refuses anything; the substrate only emits
    ``budget_state`` events. Operators / agents read those and react
    however they want.
  * ``refuse_claim`` — when ``is_exhausted()`` is true,
    ``check_back_pressure()`` returns ``(False, ...)``. The intended
    wiring is for ``queue.claim`` to consult this and short-circuit
    new claims; existing claims finish unaffected (in-flight work is
    preserved; only new draws are gated).
  * ``freeze`` — same refusal as ``refuse_claim`` plus the convention
    that callers SHOULD raise rather than silently return None. The
    distinction matters when an operator wants budget exhaustion to
    surface loudly in logs instead of as a quiet stall.

Surface:

  budget.set_config(q, *, limit_tokens, window_seconds=None, policy)
                                              -> None
  budget.get_config(q)                        -> BudgetConfig | None
  budget.clear_config(q)                      -> None
  budget.current_spend(q)                     -> int
  budget.snapshot(q)                          -> BudgetState
  budget.is_exhausted(q)                      -> bool
  budget.check_back_pressure(q)               -> tuple[bool, str]
  budget.emit_state(q, task_id, *, agent_id=None) -> int
  Budget(q) — ergonomic wrapper

Note: this module does NOT modify ``queue.py``. The wiring
(``claim`` consults ``check_back_pressure``; spend-emitting
transitions optionally fire ``emit_state``) lands in a follow-up so
the substrate can merge independently of the queue change.

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bounty_board.queue import Queue


# Valid back-pressure policy names. The string values double as the
# stored ``_meta`` value.
POLICY_SOFT = "soft"
POLICY_REFUSE_CLAIM = "refuse_claim"
POLICY_FREEZE = "freeze"
POLICIES: tuple[str, ...] = (POLICY_SOFT, POLICY_REFUSE_CLAIM, POLICY_FREEZE)


# _meta keys for budget config.
_KEY_LIMIT = "budget.limit_tokens"
_KEY_WINDOW = "budget.window_seconds"
_KEY_POLICY = "budget.policy"


# ─── config dataclasses ─────────────────────────────────────────────


@dataclass
class BudgetConfig:
    """The configured budget for a queue. Read-only view; mutate via
    ``set_config`` / ``clear_config``."""

    limit_tokens: int
    window_seconds: float | None
    policy: str


@dataclass
class BudgetState:
    """A point-in-time snapshot. ``remaining`` can go negative when
    spend overshoots — useful signal for ops dashboards that want to
    show how far past the limit a queue went."""

    limit: int
    spent: int
    remaining: int
    exhausted: bool
    window_seconds: float | None
    policy: str

    def as_dict(self) -> dict:
        """JSON-serializable view, suitable for ``payload_json``."""
        return {
            "limit": self.limit,
            "spent": self.spent,
            "remaining": self.remaining,
            "exhausted": self.exhausted,
            "window_seconds": self.window_seconds,
            "policy": self.policy,
        }


# ─── config get/set ─────────────────────────────────────────────────


def set_config(queue: Queue, *, limit_tokens: int,
               window_seconds: float | None = None,
               policy: str = POLICY_SOFT) -> None:
    """Persist a budget config to ``_meta``. Replaces any prior config.

    ``limit_tokens`` must be a non-negative int. Zero is allowed and
    means "no budget" — equivalent to ``clear_config`` but explicit.
    ``window_seconds`` of ``None`` means "lifetime spend"; any
    positive float means "spend over the trailing N seconds".
    """
    if not isinstance(limit_tokens, int) or limit_tokens < 0:
        raise ValueError(
            f"limit_tokens must be int >= 0; got {limit_tokens!r}"
        )
    if window_seconds is not None:
        if not isinstance(window_seconds, (int, float)) or window_seconds <= 0:
            raise ValueError(
                f"window_seconds must be a positive number or None; "
                f"got {window_seconds!r}"
            )
    if policy not in POLICIES:
        raise ValueError(
            f"policy must be one of {POLICIES}; got {policy!r}"
        )

    with queue._conn:
        queue._conn.execute(
            "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
            (_KEY_LIMIT, str(limit_tokens)),
        )
        if window_seconds is None:
            queue._conn.execute(
                "DELETE FROM _meta WHERE key = ?", (_KEY_WINDOW,),
            )
        else:
            queue._conn.execute(
                "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
                (_KEY_WINDOW, str(float(window_seconds))),
            )
        queue._conn.execute(
            "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
            (_KEY_POLICY, policy),
        )


def get_config(queue: Queue) -> BudgetConfig | None:
    """Return the persisted ``BudgetConfig``, or None if no budget is
    set on this queue."""
    rows = queue._conn.execute(
        "SELECT key, value FROM _meta WHERE key IN (?, ?, ?)",
        (_KEY_LIMIT, _KEY_WINDOW, _KEY_POLICY),
    ).fetchall()
    if not rows:
        return None
    kv = {r["key"]: r["value"] for r in rows}
    if _KEY_LIMIT not in kv:
        return None
    return BudgetConfig(
        limit_tokens=int(kv[_KEY_LIMIT]),
        window_seconds=(
            float(kv[_KEY_WINDOW]) if _KEY_WINDOW in kv else None
        ),
        policy=kv.get(_KEY_POLICY, POLICY_SOFT),
    )


def clear_config(queue: Queue) -> None:
    """Remove the budget config entirely. Subsequent ``get_config``
    returns None; ``check_back_pressure`` returns (True, "no_budget")."""
    with queue._conn:
        queue._conn.execute(
            "DELETE FROM _meta WHERE key IN (?, ?, ?)",
            (_KEY_LIMIT, _KEY_WINDOW, _KEY_POLICY),
        )


# ─── spend / state ──────────────────────────────────────────────────


def current_spend(queue: Queue,
                  *, window_seconds: float | None = None) -> int:
    """Sum of ``task_events.token_count``, optionally constrained to
    the trailing ``window_seconds``.

    Used internally by ``snapshot`` (which reads the window from
    config). Exposed at module level so callers can probe spend
    against a custom window without touching config."""
    if window_seconds is not None:
        cutoff = time.time() - window_seconds
        row = queue._conn.execute(
            """
            SELECT COALESCE(SUM(token_count), 0) AS s
            FROM task_events WHERE ts > ?
            """,
            (cutoff,),
        ).fetchone()
    else:
        row = queue._conn.execute(
            "SELECT COALESCE(SUM(token_count), 0) AS s FROM task_events"
        ).fetchone()
    return int(row["s"])


def snapshot(queue: Queue) -> BudgetState:
    """Compute the current budget snapshot. Raises ``ValueError`` if
    no config is set (callers should ``get_config`` first or use
    ``check_back_pressure``)."""
    cfg = get_config(queue)
    if cfg is None:
        raise ValueError("no budget configured for this queue")
    spent = current_spend(queue, window_seconds=cfg.window_seconds)
    remaining = cfg.limit_tokens - spent
    return BudgetState(
        limit=cfg.limit_tokens,
        spent=spent,
        remaining=remaining,
        exhausted=remaining <= 0,
        window_seconds=cfg.window_seconds,
        policy=cfg.policy,
    )


def is_exhausted(queue: Queue) -> bool:
    """True iff a budget is configured and spent >= limit. Returns
    False when no budget is set (no budget = no exhaustion)."""
    cfg = get_config(queue)
    if cfg is None:
        return False
    spent = current_spend(queue, window_seconds=cfg.window_seconds)
    return spent >= cfg.limit_tokens


# ─── back-pressure ──────────────────────────────────────────────────


def check_back_pressure(queue: Queue) -> tuple[bool, str]:
    """Return ``(allow, reason)`` for a hypothetical claim. Callers
    (typically ``queue.claim``) consult this before handing out work.

    Outcomes:

      * No budget configured                  -> (True, "no_budget")
      * policy='soft'                         -> (True, "soft")
      * policy='refuse_claim', not exhausted  -> (True, "refuse_claim:under")
      * policy='refuse_claim', exhausted      -> (False, "refuse_claim:exhausted")
      * policy='freeze', not exhausted        -> (True, "freeze:under")
      * policy='freeze', exhausted            -> (False, "freeze:exhausted")

    The reason string is stable — wire-protocol-grade — so callers
    (and ops dashboards) can branch on it without parsing.
    """
    cfg = get_config(queue)
    if cfg is None:
        return (True, "no_budget")
    if cfg.policy == POLICY_SOFT:
        return (True, "soft")
    exhausted = is_exhausted(queue)
    suffix = "exhausted" if exhausted else "under"
    allow = not exhausted
    return (allow, f"{cfg.policy}:{suffix}")


# ─── emit ───────────────────────────────────────────────────────────


def emit_state(queue: Queue, task_id: str,
               *, agent_id: str | None = None,
               token_count: int = 0) -> int:
    """Write a ``budget_state`` task_event row attached to ``task_id``.

    Caller pattern: after a state transition that changed spend
    (``complete`` / ``fail`` with token_count > 0), the queue (or
    operator tool) computes a fresh snapshot and emits this event so
    the #9 SSE stream surfaces the new state to subscribed agents.

    The event's ``payload_json`` carries the full ``BudgetState`` as
    a dict so consumers don't have to re-query.

    Returns the new task_event id. Raises ``ValueError`` if no budget
    is configured (emitting a budget_state without a budget is
    nonsensical) or if the task doesn't exist (clean error vs raw FK
    violation).
    """
    # Verify task exists for a clean error.
    row = queue._conn.execute(
        "SELECT 1 FROM tasks WHERE id = ?", (task_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"no task with id {task_id!r}")

    state = snapshot(queue)  # raises if no config
    now = time.time()
    with queue._conn:
        cur = queue._conn.execute(
            """
            INSERT INTO task_events
                (task_id, event_kind, ts, agent_id, payload_json,
                 token_count)
            VALUES (?, 'budget_state', ?, ?, ?, ?)
            """,
            (task_id, now, agent_id,
             json.dumps(state.as_dict()), token_count),
        )
        return cur.lastrowid


# ─── ergonomic wrapper ──────────────────────────────────────────────


class Budget:
    """Queue-bound ergonomic wrapper. ``Budget(queue).snapshot()``
    avoids re-passing the queue argument. Stateless beyond holding
    the queue ref — fine to construct freely per call.
    """

    def __init__(self, queue: Queue):
        self._queue = queue

    def configure(self, *, limit_tokens: int,
                  window_seconds: float | None = None,
                  policy: str = POLICY_SOFT) -> None:
        set_config(
            self._queue,
            limit_tokens=limit_tokens,
            window_seconds=window_seconds,
            policy=policy,
        )

    def config(self) -> BudgetConfig | None:
        return get_config(self._queue)

    def clear(self) -> None:
        clear_config(self._queue)

    def snapshot(self) -> BudgetState:
        return snapshot(self._queue)

    def spend(self, *, window_seconds: float | None = None) -> int:
        return current_spend(self._queue, window_seconds=window_seconds)

    def is_exhausted(self) -> bool:
        return is_exhausted(self._queue)

    def check_back_pressure(self) -> tuple[bool, str]:
        return check_back_pressure(self._queue)

    def emit_state(self, task_id: str, *,
                   agent_id: str | None = None,
                   token_count: int = 0) -> int:
        return emit_state(
            self._queue, task_id,
            agent_id=agent_id, token_count=token_count,
        )
