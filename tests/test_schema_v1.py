"""Tests for the V1 initial schema (0001_initial.sql).

V1 lands the earned-capability schema + all six elegance-feature
substrates at once (D, #7, #8, #9, #10, #11). See
``bounty_board/migrations/0001_initial.sql`` for the full schema and
the design lane on the cluster scratchpad (decision_id
``cluster_brokerless_task_queue_pitch_v0``).
"""
from __future__ import annotations

import sqlite3
import time

import pytest

from bounty_board._meta import SCHEMA_VERSION_EXPECTED, open_db

EXPECTED_TABLES = {
    "_meta",
    "tasks",
    "task_events",
    "agent_track_record",
    "patches",
    "interventions",
}

EXPECTED_INDICES = {
    "idx_tasks_status_priority_created",
    "idx_tasks_payload_signature",
    "idx_task_events_task_id_ts",
    "idx_task_events_kind_ts",
    "idx_track_record_signature",
    "idx_patches_signature_status",
    "idx_interventions_task_posted",
}


def _tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r[0] for r in rows}


def _indices(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex_%'"
    ).fetchall()
    return {r[0] for r in rows}


def test_v1_creates_all_tables(tmp_path):
    """All six V1 tables exist after a fresh open_db."""
    conn = open_db(tmp_path / "v1.bounty.db")
    actual = _tables(conn)
    missing = EXPECTED_TABLES - actual
    assert not missing, f"missing tables: {missing}"
    conn.close()


def test_v1_creates_all_indices(tmp_path):
    """All seven named indices exist after a fresh open_db."""
    conn = open_db(tmp_path / "v1.bounty.db")
    actual = _indices(conn)
    missing = EXPECTED_INDICES - actual
    assert not missing, f"missing indices: {missing}"
    conn.close()


def test_v1_schema_version_row_is_one(tmp_path):
    """_meta carries schema_version='1' after migration runs."""
    conn = open_db(tmp_path / "v1.bounty.db")
    row = conn.execute(
        "SELECT value FROM _meta WHERE key='schema_version'"
    ).fetchone()
    assert row is not None, "schema_version row missing"
    assert int(row[0]) == SCHEMA_VERSION_EXPECTED == 1
    conn.close()


def test_v1_tasks_status_check_constraint(tmp_path):
    """tasks.status CHECK constraint rejects unknown statuses."""
    conn = open_db(tmp_path / "v1.bounty.db")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO tasks (id, payload_json, task_type, payload_signature, "
            "status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("t1", "{}", "echo", "sig:echo:v1", "garbage_status", time.time()),
        )
        conn.commit()
    conn.close()


def test_v1_patches_status_check_constraint(tmp_path):
    """patches.status CHECK constraint rejects unknown statuses."""
    conn = open_db(tmp_path / "v1.bounty.db")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO patches (payload_signature, transformer_json, status, "
            "proposed_by_agent_id, proposed_at) VALUES (?, ?, ?, ?, ?)",
            ("sig:x", "{}", "garbage_status", "Win/Claude", time.time()),
        )
        conn.commit()
    conn.close()


def test_v1_task_events_foreign_key_enforced(tmp_path):
    """task_events.task_id FK is enforced (PRAGMA foreign_keys=ON is set)."""
    conn = open_db(tmp_path / "v1.bounty.db")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO task_events (task_id, event_kind, ts) VALUES (?, ?, ?)",
            ("nonexistent", "claim", time.time()),
        )
        conn.commit()
    conn.close()


def test_v1_interventions_foreign_key_enforced(tmp_path):
    """interventions.task_id FK is enforced."""
    conn = open_db(tmp_path / "v1.bounty.db")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO interventions (task_id, kind, posted_by_agent_id, posted_at) "
            "VALUES (?, ?, ?, ?)",
            ("nonexistent", "cancel", "Win/Claude", time.time()),
        )
        conn.commit()
    conn.close()


def test_v1_agent_track_record_composite_pk(tmp_path):
    """agent_track_record PK is (agent_id, payload_signature) — duplicates rejected."""
    conn = open_db(tmp_path / "v1.bounty.db")
    conn.execute(
        "INSERT INTO agent_track_record (agent_id, payload_signature) VALUES (?, ?)",
        ("Win/Claude", "sig:echo:v1"),
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO agent_track_record (agent_id, payload_signature) VALUES (?, ?)",
            ("Win/Claude", "sig:echo:v1"),
        )
        conn.commit()
    # Same agent, different signature — fine
    conn.execute(
        "INSERT INTO agent_track_record (agent_id, payload_signature) VALUES (?, ?)",
        ("Win/Claude", "sig:other:v1"),
    )
    conn.commit()
    conn.close()


def test_v1_round_trip_smoke(tmp_path):
    """Smoke: insert a task + event + track-record + patch + intervention; read back."""
    conn = open_db(tmp_path / "v1.bounty.db")
    now = time.time()

    conn.execute(
        "INSERT INTO tasks (id, payload_json, task_type, payload_signature, "
        "created_at) VALUES (?, ?, ?, ?, ?)",
        ("t1", '{"prompt": "hello"}', "echo", "sig:echo:v1", now),
    )
    conn.execute(
        "INSERT INTO task_events (task_id, event_kind, ts, agent_id, token_count) "
        "VALUES (?, ?, ?, ?, ?)",
        ("t1", "claim", now, "Win/Claude", 0),
    )
    conn.execute(
        "INSERT INTO agent_track_record (agent_id, payload_signature, success_n, "
        "last_seen_at) VALUES (?, ?, ?, ?)",
        ("Win/Claude", "sig:echo:v1", 1, now),
    )
    conn.execute(
        "INSERT INTO patches (payload_signature, transformer_json, "
        "proposed_by_agent_id, proposed_at) VALUES (?, ?, ?, ?)",
        ("sig:echo:v1", '{"kind": "prepend_system_msg", "args": {"text": "be concise"}}',
         "Win/Claude", now),
    )
    conn.execute(
        "INSERT INTO interventions (task_id, kind, payload_json, posted_by_agent_id, "
        "posted_at) VALUES (?, ?, ?, ?, ?)",
        ("t1", "inject_hint", '{"hint": "check edge case X"}', "Win/Claude", now),
    )
    conn.commit()

    # Confirm read-back via the claim-path index
    row = conn.execute(
        "SELECT id, status FROM tasks WHERE status='queued' "
        "ORDER BY priority DESC, created_at ASC LIMIT 1"
    ).fetchone()
    assert row == ("t1", "queued")

    # Confirm earned-lookup index
    row = conn.execute(
        "SELECT agent_id FROM agent_track_record "
        "WHERE payload_signature=? AND success_n > 0 "
        "ORDER BY success_n DESC LIMIT 1",
        ("sig:echo:v1",),
    ).fetchone()
    assert row == ("Win/Claude",)

    conn.close()
