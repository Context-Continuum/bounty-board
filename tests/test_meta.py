"""Tests for the schema-version + forward-migration substrate."""
from __future__ import annotations

import sqlite3

import pytest

from bounty_board._meta import SCHEMA_VERSION_EXPECTED, SchemaError, open_db


def test_open_db_fresh_creates_meta_table(tmp_path):
    """Fresh DB gets the ``_meta`` table and a connection in WAL mode."""
    db_path = tmp_path / "test.bounty.db"
    conn = open_db(db_path)

    # _meta table exists
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_meta'"
    )
    assert cur.fetchone() is not None

    # WAL mode
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"

    # busy_timeout set
    bt = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    assert bt == 5000

    conn.close()


def test_open_db_refuses_future_schema_version(tmp_path):
    """Opening a DB from a future schema_version raises SchemaError."""
    db_path = tmp_path / "test.bounty.db"

    # Pre-create the DB at a future version.
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO _meta (key, value) VALUES ('schema_version', ?)",
        (str(SCHEMA_VERSION_EXPECTED + 1),),
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaError, match="[Dd]owngrade"):
        open_db(db_path)


def test_open_db_idempotent_reopen(tmp_path):
    """Reopening the same DB doesn't break or duplicate _meta rows."""
    db_path = tmp_path / "test.bounty.db"
    conn = open_db(db_path)
    conn.close()

    conn = open_db(db_path)
    rows = conn.execute("SELECT key FROM _meta").fetchall()
    keys = [r[0] for r in rows]
    # _meta may or may not have a schema_version row at V0 since
    # SCHEMA_VERSION_EXPECTED=0 means no migrations ran. The contract is
    # just "doesn't break + no duplicate primary keys."
    assert len(keys) == len(set(keys)), "duplicate keys in _meta"
    conn.close()


def test_open_db_missing_migration_raises(tmp_path, monkeypatch):
    """If SCHEMA_VERSION_EXPECTED points past available migrations, fail loudly."""
    db_path = tmp_path / "test.bounty.db"

    # Patch SCHEMA_VERSION_EXPECTED to a version no migration file backs.
    import bounty_board._meta as meta

    monkeypatch.setattr(meta, "SCHEMA_VERSION_EXPECTED", 99)

    with pytest.raises(SchemaError, match="No migration found"):
        open_db(db_path)
