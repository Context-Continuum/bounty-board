"""Tests for the demo data generator.

Verifies the demo populates a coherent queue + exercises every
substrate row we care about for the /inspect dashboard (tasks across
multiple statuses, task_events of every kind we drive, track_record
updates, interventions). Fixture-fast mode (pace_seconds=0) so the
test runs in <1s.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from bounty_board._meta import open_db
from bounty_board.demo import AGENTS, TASK_SEEDS, generate, main


@pytest.fixture
def demo_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "demo.bounty.db"
    generate(db_path, n_cycles=40, pace_seconds=0.0, seed=42, quiet=True)
    return db_path


def test_generate_posts_all_seed_tasks(demo_db: Path):
    """Every TASK_SEEDS entry lands as a row in tasks."""
    conn = open_db(demo_db)
    n = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    conn.close()
    assert n == len(TASK_SEEDS)


def test_generate_produces_mixed_task_statuses(demo_db: Path):
    """After driving the lifecycle, tasks span multiple statuses."""
    conn = open_db(demo_db)
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM tasks GROUP BY status"
    ).fetchall()
    conn.close()
    statuses = {r[0] for r in rows}
    # At minimum we expect 'done' + at least one other (queued/failed)
    # because the outcome distribution is 75% complete, 15% fail,
    # 10% decline (declines go back to queued).
    assert "done" in statuses, f"no 'done' tasks; statuses={statuses}"
    assert len(statuses) >= 2, f"only one status: {statuses}"


def test_generate_writes_track_record_rows(demo_db: Path):
    """Each agent that participated has at least one track-record row."""
    conn = open_db(demo_db)
    rows = conn.execute(
        "SELECT DISTINCT agent_id FROM agent_track_record"
    ).fetchall()
    conn.close()
    agents_with_records = {r[0] for r in rows}
    # alice (pre-seeded earned-all) + at least one driven agent must be present
    assert "agent_alice" in agents_with_records
    # The pre-seed gives bob earned-half too
    assert "agent_bob" in agents_with_records


def test_generate_emits_diverse_event_kinds(demo_db: Path):
    """The driven lifecycle produces 'claim' + 'complete' + at least
    one of {'fail', 'decline'} event-kinds in task_events."""
    conn = open_db(demo_db)
    rows = conn.execute(
        "SELECT DISTINCT event_kind FROM task_events"
    ).fetchall()
    conn.close()
    kinds = {r[0] for r in rows}
    assert "claim" in kinds
    assert "complete" in kinds
    # At seed=42, both fail and decline should fire in 40 cycles
    assert kinds & {"fail", "decline"}, (
        f"expected at least one fail/decline event; saw kinds={kinds}"
    )


def test_generate_posts_interventions(demo_db: Path):
    """The demo drops at least one intervention row for /inspect to render."""
    conn = open_db(demo_db)
    n = conn.execute("SELECT COUNT(*) FROM interventions").fetchone()[0]
    conn.close()
    assert n >= 1


def test_generate_returns_summary_shape(tmp_path: Path):
    """The summary dict carries the keys callers + tests need."""
    summary = generate(
        tmp_path / "summary.db", n_cycles=10, pace_seconds=0.0,
        seed=7, quiet=True,
    )
    assert summary["tasks_posted"] == len(TASK_SEEDS)
    assert "lifecycle_outcomes" in summary
    assert "interventions_posted" in summary
    assert set(summary["lifecycle_outcomes"]).issubset(
        {"complete", "fail", "decline", "no_claim"}
    )


def test_generate_is_deterministic_with_seed(tmp_path: Path):
    """Same seed = same outcome distribution (within rounding)."""
    s1 = generate(
        tmp_path / "a.db", n_cycles=20, pace_seconds=0.0, seed=99, quiet=True,
    )
    s2 = generate(
        tmp_path / "b.db", n_cycles=20, pace_seconds=0.0, seed=99, quiet=True,
    )
    assert s1["lifecycle_outcomes"] == s2["lifecycle_outcomes"]


def test_main_cli_smoke(tmp_path: Path, capsys):
    """`python -m bounty_board.demo --fast --quiet` runs end-to-end."""
    db_path = tmp_path / "cli.db"
    rc = main([
        "--db", str(db_path),
        "--cycles", "5",
        "--fast",
        "--quiet",
        "--seed", "1",
    ])
    assert rc == 0
    assert db_path.exists()


def test_agents_list_is_three(demo_db: Path):
    """Sanity: the demo uses exactly three simulated agents."""
    assert len(AGENTS) == 3
    assert set(AGENTS) == {"agent_alice", "agent_bob", "agent_clio"}
