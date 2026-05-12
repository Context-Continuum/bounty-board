"""Bounty Board CLI entry point.

Today the only subcommand is ``inspect``. More may land later (vacuum,
export-dlq, init, etc.) but the principle is the same: every CLI verb
maps to a single substrate operation, no hidden state, no daemons.

Usage::

    bounty-board inspect path/to/queue.db [--host 127.0.0.1] [--port 8888]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bounty-board",
        description=(
            "Bounty Board — single-binary task queue for multi-agent systems."
        ),
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

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
