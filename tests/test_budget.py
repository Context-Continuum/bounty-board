"""Tests for bounty_board.budget — #10 token-budget back-pressure substrate.

Covers:
  * set_config / get_config / clear_config round-trip + validation
  * current_spend SUM aggregation (lifetime + windowed)
  * snapshot dataclass + raises-on-unconfigured
  * is_exhausted under / at / over budget
  * check_back_pressure across all three policies
  * emit_state writes the budget_state task_event with payload
  * Budget ergonomic wrapper
  * End-to-end: configure → claim+complete spending tokens → snapshot
    crosses exhaustion → back-pressure refuses claim

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from bounty_board import budget
from bounty_board.budget import (
    POLICIES,
    POLICY_FREEZE,
    POLICY_REFUSE_CLAIM,
    POLICY_SOFT,
    Budget,
    BudgetConfig,
    BudgetState,
)
from bounty_board.queue import OPEN_SIGNATURE, Queue


def _spend(q: Queue, *, tokens: int,
           signature: str = OPEN_SIGNATURE,
           agent_id: str = "a") -> str:
    """Helper: post a task, claim it, complete with the given
    token_count. Returns the task_id."""
    if signature != OPEN_SIGNATURE:
        # Pre-credit so claim's earned-cap gate lets it through.
        q._conn.execute(
            """
            INSERT INTO agent_track_record
                (agent_id, payload_signature, success_n, last_seen_at)
            VALUES (?, ?, 1, ?)
            ON CONFLICT (agent_id, payload_signature) DO UPDATE
            SET success_n = success_n + 1
            """,
            (agent_id, signature, time.time()),
        )
        q._conn.commit()
    tid = q.post(task_type="t", payload={"x": 1},
                 payload_signature=signature)
    task = q.claim(agent_id=agent_id)
    assert task is not None
    task.complete(token_count=tokens)
    return tid


# ─── set_config / get_config / clear_config ─────────────────────────


def test_get_config_returns_none_when_unset(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert budget.get_config(q) is None


def test_set_config_round_trip(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=10_000, policy=POLICY_REFUSE_CLAIM)
    cfg = budget.get_config(q)
    assert isinstance(cfg, BudgetConfig)
    assert cfg.limit_tokens == 10_000
    assert cfg.window_seconds is None
    assert cfg.policy == POLICY_REFUSE_CLAIM


def test_set_config_with_window(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(
        q, limit_tokens=5_000, window_seconds=3600.0,
        policy=POLICY_SOFT,
    )
    cfg = budget.get_config(q)
    assert cfg is not None
    assert cfg.window_seconds == 3600.0


def test_set_config_replaces_window_on_resave(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000, window_seconds=600.0)
    budget.set_config(q, limit_tokens=2000)  # no window
    cfg = budget.get_config(q)
    assert cfg is not None
    assert cfg.window_seconds is None
    assert cfg.limit_tokens == 2000


def test_clear_config(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=100)
    budget.clear_config(q)
    assert budget.get_config(q) is None


def test_set_config_rejects_negative_limit(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        budget.set_config(q, limit_tokens=-1)


def test_set_config_rejects_non_int_limit(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        budget.set_config(q, limit_tokens=10.5)  # type: ignore[arg-type]


def test_set_config_rejects_zero_window(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        budget.set_config(q, limit_tokens=100, window_seconds=0)


def test_set_config_rejects_unknown_policy(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        budget.set_config(q, limit_tokens=100, policy="freak_out")


# ─── current_spend ──────────────────────────────────────────────────


def test_current_spend_zero_with_no_events(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert budget.current_spend(q) == 0


def test_current_spend_sums_token_count(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _spend(q, tokens=100)
    _spend(q, tokens=250)
    _spend(q, tokens=50)
    # Note: each completed task emits TWO events — claim + complete.
    # Only complete carries token_count. Spend = 100 + 250 + 50 = 400.
    assert budget.current_spend(q) == 400


def test_current_spend_window_filters_old(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = q.post(task_type="t", payload={}, payload_signature=OPEN_SIGNATURE)
    task = q.claim(agent_id="a")
    assert task is not None
    # Back-date the complete event so it falls outside the window.
    task.complete(token_count=999)
    q._conn.execute(
        "UPDATE task_events SET ts = ts - 7200 WHERE task_id = ?", (tid,),
    )
    q._conn.commit()
    # Lifetime view sees it; window=3600 (1h) excludes it.
    assert budget.current_spend(q) == 999
    assert budget.current_spend(q, window_seconds=3600) == 0


# ─── snapshot ───────────────────────────────────────────────────────


def test_snapshot_raises_without_config(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError, match="no budget configured"):
        budget.snapshot(q)


def test_snapshot_under_budget(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000)
    _spend(q, tokens=200)
    s = budget.snapshot(q)
    assert isinstance(s, BudgetState)
    assert s.limit == 1000
    assert s.spent == 200
    assert s.remaining == 800
    assert s.exhausted is False
    assert s.policy == POLICY_SOFT


def test_snapshot_at_exhaustion(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=500)
    _spend(q, tokens=500)
    s = budget.snapshot(q)
    assert s.spent == 500
    assert s.remaining == 0
    assert s.exhausted is True


def test_snapshot_over_budget_negative_remaining(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=100)
    _spend(q, tokens=500)
    s = budget.snapshot(q)
    assert s.remaining == -400
    assert s.exhausted is True


def test_snapshot_as_dict_round_trip(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000, window_seconds=600.0,
                      policy=POLICY_REFUSE_CLAIM)
    _spend(q, tokens=300)
    d = budget.snapshot(q).as_dict()
    assert d == {
        "limit": 1000,
        "spent": 300,
        "remaining": 700,
        "exhausted": False,
        "window_seconds": 600.0,
        "policy": POLICY_REFUSE_CLAIM,
    }


# ─── is_exhausted ───────────────────────────────────────────────────


def test_is_exhausted_false_with_no_budget(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    _spend(q, tokens=1_000_000)
    assert budget.is_exhausted(q) is False


def test_is_exhausted_under(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000)
    _spend(q, tokens=999)
    assert budget.is_exhausted(q) is False


def test_is_exhausted_at_limit(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000)
    _spend(q, tokens=1000)
    assert budget.is_exhausted(q) is True


# ─── check_back_pressure ────────────────────────────────────────────


def test_check_back_pressure_no_budget(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    allow, reason = budget.check_back_pressure(q)
    assert allow is True
    assert reason == "no_budget"


def test_check_back_pressure_soft_always_allows(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=100, policy=POLICY_SOFT)
    _spend(q, tokens=500)  # well over
    allow, reason = budget.check_back_pressure(q)
    assert allow is True
    assert reason == "soft"


def test_check_back_pressure_refuse_claim_under(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000, policy=POLICY_REFUSE_CLAIM)
    _spend(q, tokens=500)
    allow, reason = budget.check_back_pressure(q)
    assert allow is True
    assert reason == "refuse_claim:under"


def test_check_back_pressure_refuse_claim_exhausted(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=100, policy=POLICY_REFUSE_CLAIM)
    _spend(q, tokens=200)
    allow, reason = budget.check_back_pressure(q)
    assert allow is False
    assert reason == "refuse_claim:exhausted"


def test_check_back_pressure_freeze_exhausted(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=100, policy=POLICY_FREEZE)
    _spend(q, tokens=200)
    allow, reason = budget.check_back_pressure(q)
    assert allow is False
    assert reason == "freeze:exhausted"


def test_check_back_pressure_freeze_under(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000, policy=POLICY_FREEZE)
    _spend(q, tokens=500)
    allow, reason = budget.check_back_pressure(q)
    assert allow is True
    assert reason == "freeze:under"


# ─── emit_state ─────────────────────────────────────────────────────


def test_emit_state_writes_task_event(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=1000, policy=POLICY_SOFT)
    tid = _spend(q, tokens=300)
    event_id = budget.emit_state(q, tid, agent_id="watcher")
    row = q._conn.execute(
        "SELECT * FROM task_events WHERE id = ?", (event_id,),
    ).fetchone()
    assert row is not None
    assert row["event_kind"] == "budget_state"
    assert row["task_id"] == tid
    assert row["agent_id"] == "watcher"
    payload = json.loads(row["payload_json"])
    assert payload["limit"] == 1000
    assert payload["spent"] == 300
    assert payload["remaining"] == 700
    assert payload["exhausted"] is False
    assert payload["policy"] == POLICY_SOFT


def test_emit_state_raises_without_budget(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    tid = _spend(q, tokens=10)
    with pytest.raises(ValueError, match="no budget configured"):
        budget.emit_state(q, tid)


def test_emit_state_raises_on_missing_task(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    budget.set_config(q, limit_tokens=100)
    with pytest.raises(ValueError, match="no task"):
        budget.emit_state(q, "nonexistent")


# ─── Budget ergonomic wrapper ───────────────────────────────────────


def test_budget_wrapper_round_trip(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    b = Budget(q)
    b.configure(limit_tokens=1000, policy=POLICY_REFUSE_CLAIM)
    cfg = b.config()
    assert cfg is not None
    assert cfg.limit_tokens == 1000
    assert cfg.policy == POLICY_REFUSE_CLAIM


def test_budget_wrapper_snapshot_and_spend(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    b = Budget(q)
    b.configure(limit_tokens=1000)
    _spend(q, tokens=400)
    assert b.spend() == 400
    s = b.snapshot()
    assert s.spent == 400
    assert s.remaining == 600


def test_budget_wrapper_back_pressure(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    b = Budget(q)
    b.configure(limit_tokens=100, policy=POLICY_REFUSE_CLAIM)
    _spend(q, tokens=200)
    allow, reason = b.check_back_pressure()
    assert allow is False
    assert reason == "refuse_claim:exhausted"


def test_budget_wrapper_clear(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    b = Budget(q)
    b.configure(limit_tokens=100)
    b.clear()
    assert b.config() is None


def test_budget_wrapper_emit_state(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    b = Budget(q)
    b.configure(limit_tokens=1000)
    tid = _spend(q, tokens=100)
    event_id = b.emit_state(tid, agent_id="watcher")
    assert isinstance(event_id, int)


# ─── sanity ─────────────────────────────────────────────────────────


def test_policies_constant():
    assert set(POLICIES) == {POLICY_SOFT, POLICY_REFUSE_CLAIM, POLICY_FREEZE}


# ─── end-to-end ─────────────────────────────────────────────────────


def test_end_to_end_back_pressure_lifecycle(tmp_path: Path):
    """Canonical #10 user story:

      1. Operator configures a queue with a token cap + refuse_claim
         policy
      2. Several tasks claim + complete, accumulating spend below cap
      3. check_back_pressure returns allow=True
      4. Spend crosses the cap on a subsequent task
      5. check_back_pressure now returns allow=False — wired wiring
         in queue.claim would short-circuit further claims
      6. emit_state writes a budget_state event so SSE subscribers
         see the exhaustion
      7. Operator clears the budget — back to no_budget allow=True
    """
    q = Queue(tmp_path / "q.db")
    b = Budget(q)
    # 1. configure
    b.configure(limit_tokens=1000, policy=POLICY_REFUSE_CLAIM)

    # 2. spend below cap
    _spend(q, tokens=300)
    _spend(q, tokens=400)

    # 3. allow
    allow, reason = b.check_back_pressure()
    assert allow is True
    assert reason == "refuse_claim:under"

    # 4. cross the cap
    last_tid = _spend(q, tokens=500)
    s = b.snapshot()
    assert s.spent == 1200
    assert s.exhausted is True

    # 5. refused
    allow, reason = b.check_back_pressure()
    assert allow is False
    assert reason == "refuse_claim:exhausted"

    # 6. emit_state so SSE subscribers see it
    event_id = b.emit_state(last_tid, agent_id="watcher")
    row = q._conn.execute(
        "SELECT * FROM task_events WHERE id = ?", (event_id,),
    ).fetchone()
    payload = json.loads(row["payload_json"])
    assert payload["exhausted"] is True
    assert payload["spent"] == 1200

    # 7. clear → no_budget
    b.clear()
    allow, reason = b.check_back_pressure()
    assert allow is True
    assert reason == "no_budget"
