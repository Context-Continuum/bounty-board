"""Tests for the dlq CLI subcommands (list / get / replay / purge).

These wrap the dlq.DLQ surface; tests exercise the CLI argparse + JSON
output path, while substrate correctness is covered separately in
test_dlq.py.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from bounty_board._cli import main as cli_main
from bounty_board._meta import open_db


@pytest.fixture
def queue_with_failures(tmp_path: Path) -> Path:
    """A queue containing two failed tasks + one done task."""
    db_path = tmp_path / "dlq.bounty.db"
    cli_main(["init", str(db_path)])

    conn = open_db(db_path)
    now = time.time()
    for tid, status in [
        ("f1", "failed"),
        ("f2", "failed"),
        ("d1", "done"),
    ]:
        conn.execute(
            "INSERT INTO tasks (id, payload_json, task_type, payload_signature, "
            "status, attempts, max_attempts, created_at, completed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (tid, '{"x": 1}', "echo", "sig:echo:v1", status, 3, 3, now, now),
        )
    # Some events for f1
    conn.execute(
        "INSERT INTO task_events (task_id, event_kind, ts, agent_id, payload_json, token_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("f1", "claim", now, "agent_a", None, 0),
    )
    conn.execute(
        "INSERT INTO task_events (task_id, event_kind, ts, agent_id, payload_json, token_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("f1", "fail", now, "agent_a",
         '{"stack": "Traceback (boom)", "prompt_state": null, "post_status": "failed"}', 75),
    )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_dlq_list_plaintext(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "list", str(queue_with_failures)])
    assert rc == 0
    out = capfd.readouterr().out
    assert "f1" in out
    assert "f2" in out
    assert "d1" not in out  # done tasks are not DLQ


def test_dlq_list_json(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "list", str(queue_with_failures), "--json"])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert len(parsed) == 2
    task_ids = {e["task_id"] for e in parsed}
    assert task_ids == {"f1", "f2"}
    # f1 has a trajectory carrying the fail event
    f1 = next(e for e in parsed if e["task_id"] == "f1")
    assert len(f1["trajectory"]) == 2
    assert any(ev["event_kind"] == "fail" for ev in f1["trajectory"])


def test_dlq_list_signature_filter(queue_with_failures: Path, capfd):
    rc = cli_main([
        "dlq", "list", str(queue_with_failures),
        "--signature", "sig:echo:v1", "--json",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert len(parsed) == 2

    rc = cli_main([
        "dlq", "list", str(queue_with_failures),
        "--signature", "nonexistent", "--json",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed == []


def test_dlq_list_empty_queue(tmp_path: Path, capfd):
    db_path = tmp_path / "empty.db"
    cli_main(["init", str(db_path)])
    capfd.readouterr()

    rc = cli_main(["dlq", "list", str(db_path)])
    assert rc == 0
    out = capfd.readouterr().out
    assert "empty" in out.lower()


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


def test_dlq_get_returns_full_dossier(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "get", str(queue_with_failures), "--task", "f1"])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["task_id"] == "f1"
    assert parsed["status"] == "failed"
    assert len(parsed["trajectory"]) == 2
    fail_ev = next(e for e in parsed["trajectory"] if e["event_kind"] == "fail")
    assert fail_ev["token_count"] == 75
    assert "boom" in fail_ev["payload"]["stack"]


def test_dlq_get_not_in_dlq_exits_1(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "get", str(queue_with_failures), "--task", "d1"])
    assert rc == 1
    err = capfd.readouterr().err
    assert "not in DLQ" in err


def test_dlq_get_missing_task_exits_1(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "get", str(queue_with_failures), "--task", "ghost"])
    assert rc == 1


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------


def test_dlq_replay_creates_fresh_task(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "replay", str(queue_with_failures), "--task", "f1"])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["replayed_from"] == "f1"
    new_id = parsed["new_task_id"]
    assert new_id != "f1"

    # Substrate: original stays failed, new task is queued + parent_id linked
    conn = open_db(queue_with_failures)
    orig = conn.execute(
        "SELECT status FROM tasks WHERE id = ?", ("f1",)
    ).fetchone()
    new_row = conn.execute(
        "SELECT status, parent_id FROM tasks WHERE id = ?", (new_id,)
    ).fetchone()
    conn.close()
    assert orig[0] == "failed"
    assert new_row == ("queued", "f1")


def test_dlq_replay_non_dlq_task_exits_1(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "replay", str(queue_with_failures), "--task", "d1"])
    assert rc == 1
    err = capfd.readouterr().err
    assert "DLQ" in err or "status" in err


def test_dlq_replay_missing_task_exits_1(queue_with_failures: Path, capfd):
    rc = cli_main(["dlq", "replay", str(queue_with_failures), "--task", "ghost"])
    assert rc == 1


# ---------------------------------------------------------------------------
# purge
# ---------------------------------------------------------------------------


def test_dlq_purge_returns_count(queue_with_failures: Path, capfd):
    # Purge with --days 0 means "everything older than 0 days ago" = all
    rc = cli_main([
        "dlq", "purge", str(queue_with_failures), "--days", "0",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["purged_n"] == 2
    assert parsed["older_than_days"] == 0.0

    # Verify the substrate-side cleanup
    conn = open_db(queue_with_failures)
    n_failed = conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE status = 'failed'"
    ).fetchone()[0]
    n_events_for_f1 = conn.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id = 'f1'"
    ).fetchone()[0]
    conn.close()
    assert n_failed == 0
    assert n_events_for_f1 == 0  # cascaded


def test_dlq_purge_with_huge_window_purges_nothing(queue_with_failures: Path, capfd):
    rc = cli_main([
        "dlq", "purge", str(queue_with_failures), "--days", "365",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["purged_n"] == 0
