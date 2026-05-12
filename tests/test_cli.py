"""Tests for the CLI subcommands (init / status).

The inspect subcommand is exercised via test_inspect.py's
TestClient harness; the CLI dispatch for it is tested in test_demo.py
via a similar shell-style smoke. This file focuses on the
substrate-only verbs that don't require FastAPI.
"""
from __future__ import annotations

import json as _json
import sqlite3
import time
from pathlib import Path

from bounty_board._cli import main as cli_main
from bounty_board._meta import SCHEMA_VERSION_EXPECTED, open_db

# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


def test_init_creates_new_queue(tmp_path: Path, capfd):
    db_path = tmp_path / "fresh.bounty.db"
    rc = cli_main(["init", str(db_path)])
    assert rc == 0
    assert db_path.exists()

    # Verify schema_version row landed
    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = 'schema_version'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert int(row[0]) == SCHEMA_VERSION_EXPECTED

    err = capfd.readouterr().err
    assert "created new queue" in err


def test_init_existing_queue_migrates_forward(tmp_path: Path, capfd):
    db_path = tmp_path / "existing.bounty.db"
    # Create it via the substrate first
    conn = open_db(db_path)
    conn.close()

    rc = cli_main(["init", str(db_path)])
    assert rc == 0

    err = capfd.readouterr().err
    assert "already existed" in err


def test_init_json_emits_machine_readable(tmp_path: Path, capfd):
    db_path = tmp_path / "json.bounty.db"
    rc = cli_main(["init", str(db_path), "--json"])
    assert rc == 0

    out = capfd.readouterr().out
    parsed = _json.loads(out)
    assert parsed["db_path"] == str(db_path)
    assert parsed["schema_version"] == SCHEMA_VERSION_EXPECTED
    assert parsed["existed_before"] is False


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _seed_for_status(db_path: Path) -> None:
    conn = open_db(db_path)
    now = time.time()
    # 3 queued, 1 done, 1 failed
    for i, status in enumerate(["queued", "queued", "queued", "done", "failed"]):
        conn.execute(
            "INSERT INTO tasks (id, payload_json, task_type, payload_signature, "
            "status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (f"t{i}", "{}", "echo", "sig:echo:v1", status, now),
        )
    # A couple events
    conn.execute(
        "INSERT INTO task_events (task_id, event_kind, ts) VALUES (?, ?, ?)",
        ("t0", "claim", now),
    )
    conn.execute(
        "INSERT INTO task_events (task_id, event_kind, ts) VALUES (?, ?, ?)",
        ("t3", "complete", now),
    )
    # Two agent records
    conn.execute(
        "INSERT INTO agent_track_record "
        "(agent_id, payload_signature, success_n, last_seen_at) "
        "VALUES (?, ?, ?, ?)",
        ("agent_a", "sig:echo:v1", 5, now),
    )
    conn.execute(
        "INSERT INTO agent_track_record "
        "(agent_id, payload_signature, success_n, last_seen_at) "
        "VALUES (?, ?, ?, ?)",
        ("agent_b", "sig:echo:v1", 1, now),
    )
    # One canonical patch, one pending intervention
    conn.execute(
        "INSERT INTO patches (payload_signature, transformer_json, status, "
        "n_successes, proposed_by_agent_id, proposed_at, promoted_at) "
        "VALUES (?, ?, 'canonical', 5, ?, ?, ?)",
        ("sig:echo:v1", '{"kind": "prepend"}', "agent_a", now, now),
    )
    conn.execute(
        "INSERT INTO interventions (task_id, kind, posted_by_agent_id, posted_at) "
        "VALUES (?, ?, ?, ?)",
        ("t0", "inject_hint", "supervisor_a", now),
    )
    conn.commit()
    conn.close()


def test_status_missing_db_exits_1(tmp_path: Path, capfd):
    rc = cli_main(["status", str(tmp_path / "nope.bounty.db")])
    assert rc == 1
    err = capfd.readouterr().err
    assert "does not exist" in err
    assert "bounty-board init" in err


def test_status_plaintext_columns(tmp_path: Path, capfd):
    db_path = tmp_path / "status.bounty.db"
    _seed_for_status(db_path)

    rc = cli_main(["status", str(db_path)])
    assert rc == 0
    out = capfd.readouterr().out
    assert "queue:" in out
    assert "total_tasks:" in out
    assert "queued" in out
    assert "done" in out
    assert "failed" in out
    assert "canonical_patches:" in out
    assert "pending_interventions:" in out


def test_status_json_emits_machine_readable(tmp_path: Path, capfd):
    db_path = tmp_path / "status.bounty.db"
    _seed_for_status(db_path)

    rc = cli_main(["status", str(db_path), "--json"])
    assert rc == 0

    out = capfd.readouterr().out
    parsed = _json.loads(out)
    assert parsed["db_path"] == str(db_path)
    assert parsed["total_tasks"] == 5
    assert parsed["depth"]["queued"] == 3
    assert parsed["depth"]["done"] == 1
    assert parsed["depth"]["failed"] == 1
    assert parsed["known_agents"] == 2
    assert parsed["canonical_patches"] == 1
    assert parsed["pending_interventions"] == 1


def test_status_empty_queue_zeros(tmp_path: Path, capfd):
    db_path = tmp_path / "empty.bounty.db"
    cli_main(["init", str(db_path)])
    capfd.readouterr()  # drain init noise

    rc = cli_main(["status", str(db_path), "--json"])
    assert rc == 0
    parsed = _json.loads(capfd.readouterr().out)
    assert parsed["total_tasks"] == 0
    assert parsed["depth"]["queued"] == 0
    assert parsed["total_events"] == 0
    assert parsed["known_agents"] == 0


def test_init_then_status_round_trip(tmp_path: Path, capfd):
    """End-to-end: init → status both happy on a fresh queue."""
    db_path = tmp_path / "e2e.bounty.db"
    assert cli_main(["init", str(db_path)]) == 0
    capfd.readouterr()
    assert cli_main(["status", str(db_path), "--json"]) == 0
    parsed = _json.loads(capfd.readouterr().out)
    assert parsed["total_tasks"] == 0
