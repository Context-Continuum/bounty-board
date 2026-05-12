"""Schema-version check + forward-migration runner.

This module is the substrate the rest of ``bounty_board`` is built on. It
handles three concerns and only three:

1. Opening a SQLite file at the right PRAGMAs (WAL, busy_timeout, foreign_keys).
2. Reading/writing the canonical ``schema_version`` row in the ``_meta`` table.
3. Running forward migrations (``migrations/NNNN_*.sql``) to bring a DB from
   its current version up to the version this library expects.

Downgrades are deliberately unsupported. Opening a DB from a future schema
version raises :class:`SchemaError`; the correct fix is to upgrade the
library.

The schema-version pattern keeps the SQLite file shape as a versioned public
contract — external consumers (any language; the schema is the SDK) can
branch on the ``schema_version`` row instead of guessing at column shape.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

# Bumped each time a migration is added to ``bounty_board/migrations/``.
# V1 ships with the earned-capability schema (``0001_initial.sql``). The
# declared-vs-earned decision landed via implicit ratification of the
# supervisor-as-capability principle — supervisors must EARN the
# supervisor capability via successful interventions; declared-tags
# would collapse the substrate-honest ethos.
SCHEMA_VERSION_EXPECTED = 1

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class SchemaError(Exception):
    """Raised when the on-disk DB version is incompatible with this library."""


def open_db(path: str | Path) -> sqlite3.Connection:
    """Open a Bounty Board SQLite database; return a ready-to-use connection.

    Sets WAL mode, a 5 second busy_timeout, and foreign_keys=ON on every
    connection. Runs forward migrations to bring the DB up to
    :data:`SCHEMA_VERSION_EXPECTED`. Raises :class:`SchemaError` if the DB
    is at a FUTURE schema version (downgrade is unsupported).
    """
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")

    _ensure_meta_table(conn)
    current = _get_schema_version(conn)

    if current > SCHEMA_VERSION_EXPECTED:
        raise SchemaError(
            f"Database at schema_version={current}, but this library only "
            f"supports up to {SCHEMA_VERSION_EXPECTED}. Downgrade is "
            f"unsupported; upgrade bounty-board instead."
        )

    if current < SCHEMA_VERSION_EXPECTED:
        _run_migrations(conn, from_version=current, to_version=SCHEMA_VERSION_EXPECTED)

    return conn


def _ensure_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _meta "
        "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    conn.commit()


def _get_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        return 0
    return int(row[0])


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES ('schema_version', ?)",
        (str(version),),
    )


def _run_migrations(
    conn: sqlite3.Connection, from_version: int, to_version: int
) -> None:
    """Run all migrations strictly between ``from_version`` and ``to_version``.

    Each migration file is named ``NNNN_description.sql`` where ``NNNN`` is
    the ``schema_version`` it brings the DB up to. Each file is executed as
    a single ``executescript`` so it can contain multiple statements.
    """
    for v in range(from_version + 1, to_version + 1):
        sql_paths = sorted(MIGRATIONS_DIR.glob(f"{v:04d}_*.sql"))
        if not sql_paths:
            raise SchemaError(
                f"No migration found for schema_version={v}; expected "
                f"{MIGRATIONS_DIR}/{v:04d}_*.sql"
            )
        for sql_path in sql_paths:
            conn.executescript(sql_path.read_text())
        _set_schema_version(conn, v)
        conn.commit()
