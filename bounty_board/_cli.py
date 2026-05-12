"""Bounty Board CLI entry point.

Subcommands:

  bounty-board init   <db_path>          create an empty queue file
  bounty-board status <db_path>          print queue depth + recent counts
  bounty-board inspect <db_path> [...]   run the /inspect dashboard

The CLI is the shell-side surface — a user can ``pip install bounty-board``
and immediately inspect a queue without writing any Python. Each verb maps
to a single substrate operation: no hidden state, no daemons (other than
the optional dashboard server), no implicit migrations beyond what
``open_db`` already runs.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


def _cmd_init(args: argparse.Namespace) -> int:
    from bounty_board._meta import SCHEMA_VERSION_EXPECTED, open_db

    db_path = Path(args.db_path)
    existed = db_path.exists()

    conn = open_db(db_path)
    conn.close()

    if args.json:
        json.dump(
            {
                "db_path": str(db_path),
                "schema_version": SCHEMA_VERSION_EXPECTED,
                "existed_before": existed,
            },
            sys.stdout,
        )
        sys.stdout.write("\n")
    elif existed:
        print(
            f"[bounty-board] {db_path} already existed; ran forward "
            f"migrations to schema_version={SCHEMA_VERSION_EXPECTED}.",
            file=sys.stderr,
        )
    else:
        print(
            f"[bounty-board] created new queue at {db_path} "
            f"(schema_version={SCHEMA_VERSION_EXPECTED}).",
            file=sys.stderr,
        )
    return 0


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


_STATUS_KEYS = ("queued", "claimed", "processing", "done", "failed", "unclaimable")


def _gather_status(db_path: Path) -> dict:
    """Read queue depth + counts directly from the substrate."""
    from bounty_board._meta import open_db

    conn = open_db(db_path)
    try:
        depth = dict.fromkeys(_STATUS_KEYS, 0)
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM tasks GROUP BY status"
        ).fetchall()
        for status, n in rows:
            if status in depth:
                depth[status] = int(n)

        total_tasks = sum(depth.values())
        total_events = conn.execute(
            "SELECT COUNT(*) FROM task_events"
        ).fetchone()[0]
        total_agents = conn.execute(
            "SELECT COUNT(DISTINCT agent_id) FROM agent_track_record"
        ).fetchone()[0]
        n_patches = conn.execute(
            "SELECT COUNT(*) FROM patches WHERE status = 'canonical'"
        ).fetchone()[0]
        n_pending_interventions = conn.execute(
            "SELECT COUNT(*) FROM interventions WHERE honored_at IS NULL"
        ).fetchone()[0]
    finally:
        conn.close()

    return {
        "db_path": str(db_path),
        "depth": depth,
        "total_tasks": total_tasks,
        "total_events": int(total_events),
        "known_agents": int(total_agents),
        "canonical_patches": int(n_patches),
        "pending_interventions": int(n_pending_interventions),
    }


def _cmd_status(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        print(
            f"[bounty-board] {db_path} does not exist. "
            f"Initialize one with: bounty-board init {db_path}",
            file=sys.stderr,
        )
        return 1

    info = _gather_status(db_path)

    if args.json:
        json.dump(info, sys.stdout)
        sys.stdout.write("\n")
        return 0

    # Plaintext (default) — shell-pipeline-friendly columns.
    depth = info["depth"]
    print(f"queue: {info['db_path']}")
    print(f"  total_tasks:           {info['total_tasks']}")
    for k in _STATUS_KEYS:
        print(f"    {k:13s} {depth[k]}")
    print(f"  total_events:          {info['total_events']}")
    print(f"  known_agents:          {info['known_agents']}")
    print(f"  canonical_patches:     {info['canonical_patches']}")
    print(f"  pending_interventions: {info['pending_interventions']}")
    return 0


# ---------------------------------------------------------------------------
# inspect (existing)
# ---------------------------------------------------------------------------


def _cmd_inspect(args: argparse.Namespace) -> int:
    try:
        import uvicorn

        from bounty_board.inspect import create_app
    except ImportError as e:  # pragma: no cover - install-time error path
        print(
            "[bounty-board] /inspect requires the optional 'inspect' extras.\n"
            "  pip install 'bounty-board[inspect]'\n"
            f"  underlying error: {e}",
            file=sys.stderr,
        )
        return 2

    db_path = Path(args.db_path)
    if not db_path.exists():
        # open_db will create on demand; surface the choice to the user.
        print(
            f"[bounty-board] {db_path} does not exist yet — it will be "
            f"created on first request.",
            file=sys.stderr,
        )

    app = create_app(db_path)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bounty-board",
        description=(
            "Bounty Board — single-binary task queue for multi-agent systems."
        ),
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    init_p = subparsers.add_parser(
        "init",
        help="Create an empty queue file (or migrate an existing one forward).",
    )
    init_p.add_argument("db_path", help="path to the queue .db file")
    init_p.add_argument(
        "--json", action="store_true",
        help="emit a JSON summary on stdout instead of prose",
    )
    init_p.set_defaults(func=_cmd_init)

    status_p = subparsers.add_parser(
        "status",
        help="Print queue depth + total tasks/events/agents at a glance.",
    )
    status_p.add_argument("db_path", help="path to the queue .db file")
    status_p.add_argument(
        "--json", action="store_true",
        help="emit a JSON summary on stdout instead of plaintext columns",
    )
    status_p.set_defaults(func=_cmd_status)

    inspect_p = subparsers.add_parser(
        "inspect",
        help="Run the read-only /inspect dashboard over a queue file.",
    )
    inspect_p.add_argument("db_path", help="path to the queue .db file")
    inspect_p.add_argument(
        "--host", default="127.0.0.1", help="bind host (default: 127.0.0.1)"
    )
    inspect_p.add_argument(
        "--port", type=int, default=8888, help="bind port (default: 8888)"
    )
    inspect_p.set_defaults(func=_cmd_inspect)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
