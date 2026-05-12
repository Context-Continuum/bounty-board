"""Bounty Board CLI entry point.

Subcommands:

  bounty-board init     <db_path>                 create / migrate forward
  bounty-board status   <db_path>                 queue depth + counts
  bounty-board post     <db_path> --type ... [...] post a new task
  bounty-board claim    <db_path> --agent ...     claim a task atomically
  bounty-board complete <db_path> --task ...      mark task done
  bounty-board fail     <db_path> --task ...      mark task failed
  bounty-board decline  <db_path> --task ...      cooperative decline
  bounty-board inspect  <db_path> [...]           /inspect dashboard

The CLI is the shell-side surface — a user can ``pip install bounty-board``
and operate a complete task lifecycle (post → claim → complete) without
writing any Python. Useful for cron jobs, shell pipelines, CI gates, and
quick demos.

Substrate-discipline: each verb maps to a single substrate operation that
writes the same canonical rows the Python API would. ``bounty-board claim``
takes the same SQLite reserved-lock as ``Queue.claim()`` and emits the same
``task_events`` row.

JSON-output is the canonical shape (--json on init/status; default on
post/claim). Plaintext is the convenience.
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
# post / claim / complete / fail / decline (lifecycle)
# ---------------------------------------------------------------------------


def _load_payload(args: argparse.Namespace) -> dict:
    """Resolve --payload (literal JSON) or --payload-stdin or {}."""
    if args.payload_stdin:
        raw = sys.stdin.read()
        return json.loads(raw) if raw.strip() else {}
    if args.payload:
        return json.loads(args.payload)
    return {}


def _resolve_signature_for(conn, task_id: str) -> str | None:
    row = conn.execute(
        "SELECT payload_signature FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()
    return row[0] if row else None


def _cmd_post(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists() and not args.create:
        print(
            f"[bounty-board] {db_path} does not exist. Pass --create to "
            f"initialize on demand, or run `bounty-board init {db_path}` first.",
            file=sys.stderr,
        )
        return 1

    payload = _load_payload(args)
    q = Queue(db_path)
    try:
        task_id = q.post(
            task_type=args.task_type,
            payload=payload,
            payload_signature=args.signature,
            priority=args.priority,
            max_attempts=args.max_attempts,
        )
    finally:
        q.close()

    json.dump({"task_id": task_id}, sys.stdout)
    sys.stdout.write("\n")
    return 0


def _cmd_claim(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(
            f"[bounty-board] {db_path} does not exist. Initialize with "
            f"`bounty-board init {db_path}`.",
            file=sys.stderr,
        )
        return 1

    q = Queue(db_path)
    try:
        task = q.claim(agent_id=args.agent)
    finally:
        q.close()

    if task is None:
        if args.json:
            json.dump({"claimed": False}, sys.stdout)
            sys.stdout.write("\n")
        else:
            print(f"[bounty-board] no claimable task for agent {args.agent}.",
                  file=sys.stderr)
        return 1

    out = {
        "claimed": True,
        "task_id": task.id,
        "task_type": task.task_type,
        "payload_signature": task.payload_signature,
        "payload": task.payload,
        "priority": task.priority,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "claimed_by": task.claimed_by,
        "claimed_at": task.claimed_at,
    }
    json.dump(out, sys.stdout)
    sys.stdout.write("\n")
    return 0


def _cmd_complete(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[bounty-board] {db_path} does not exist.", file=sys.stderr)
        return 1

    result: dict | None = None
    if args.result:
        result = json.loads(args.result)

    q = Queue(db_path)
    try:
        signature = _resolve_signature_for(q._conn, args.task)
        if signature is None:
            print(
                f"[bounty-board] no task with id {args.task!r}",
                file=sys.stderr,
            )
            return 1
        q._mark_complete(
            args.task,
            agent_id=args.agent,
            payload_signature=signature,
            result=result,
            token_count=args.tokens,
        )
    finally:
        q.close()

    json.dump(
        {"task_id": args.task, "status": "done", "agent_id": args.agent},
        sys.stdout,
    )
    sys.stdout.write("\n")
    return 0


def _cmd_fail(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[bounty-board] {db_path} does not exist.", file=sys.stderr)
        return 1

    prompt_state: dict | None = None
    if args.prompt_state:
        prompt_state = json.loads(args.prompt_state)

    stack = args.stack
    if args.stack_stdin:
        stack = sys.stdin.read()

    q = Queue(db_path)
    try:
        signature = _resolve_signature_for(q._conn, args.task)
        if signature is None:
            print(
                f"[bounty-board] no task with id {args.task!r}",
                file=sys.stderr,
            )
            return 1
        q._mark_fail(
            args.task,
            agent_id=args.agent,
            payload_signature=signature,
            stack=stack or "",
            prompt_state=prompt_state,
            token_count=args.tokens,
        )
        # Look up post-fail status to surface in output
        row = q._conn.execute(
            "SELECT status FROM tasks WHERE id = ?", (args.task,)
        ).fetchone()
        post_status = row[0] if row else "unknown"
    finally:
        q.close()

    json.dump(
        {
            "task_id": args.task,
            "status": post_status,
            "agent_id": args.agent,
        },
        sys.stdout,
    )
    sys.stdout.write("\n")
    return 0


def _cmd_decline(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[bounty-board] {db_path} does not exist.", file=sys.stderr)
        return 1

    q = Queue(db_path)
    try:
        signature = _resolve_signature_for(q._conn, args.task)
        if signature is None:
            print(
                f"[bounty-board] no task with id {args.task!r}",
                file=sys.stderr,
            )
            return 1
        q._mark_decline(
            args.task,
            agent_id=args.agent,
            payload_signature=signature,
            reason=args.reason,
        )
    finally:
        q.close()

    json.dump(
        {
            "task_id": args.task,
            "status": "queued",
            "agent_id": args.agent,
            "reason": args.reason,
        },
        sys.stdout,
    )
    sys.stdout.write("\n")
    return 0


# ---------------------------------------------------------------------------
# dlq list / get / replay / purge
# ---------------------------------------------------------------------------


def _dlq_entry_to_dict(entry) -> dict:
    """Serialize a DLQEntry dataclass into JSON-safe dict form."""
    return {
        "task_id": entry.task_id,
        "task_type": entry.task_type,
        "payload_signature": entry.payload_signature,
        "payload": entry.payload,
        "status": entry.status,
        "attempts": entry.attempts,
        "max_attempts": entry.max_attempts,
        "created_at": entry.created_at,
        "completed_at": entry.completed_at,
        "parent_id": entry.parent_id,
        "total_token_count": entry.total_token_count,
        "trajectory": [
            {
                "id": ev.id,
                "event_kind": ev.event_kind,
                "ts": ev.ts,
                "agent_id": ev.agent_id,
                "payload": ev.payload,
                "token_count": ev.token_count,
            }
            for ev in entry.trajectory
        ],
    }


def _cmd_dlq_list(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[bounty-board] {db_path} does not exist.", file=sys.stderr)
        return 1

    q = Queue(db_path)
    try:
        from bounty_board.dlq import DLQ

        entries = DLQ(q).list(
            limit=args.limit,
            payload_signature=args.signature,
        )
    finally:
        q.close()

    if args.json:
        json.dump([_dlq_entry_to_dict(e) for e in entries], sys.stdout)
        sys.stdout.write("\n")
        return 0

    if not entries:
        print("(DLQ is empty)")
        return 0
    for e in entries:
        print(
            f"{e.task_id[:12]}  status={e.status:11s}  "
            f"type={e.task_type:20s}  sig={e.payload_signature:20s}  "
            f"attempts={e.attempts}/{e.max_attempts}"
        )
    return 0


def _cmd_dlq_get(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[bounty-board] {db_path} does not exist.", file=sys.stderr)
        return 1

    q = Queue(db_path)
    try:
        from bounty_board.dlq import DLQ

        entry = DLQ(q).get(args.task)
    finally:
        q.close()

    if entry is None:
        print(
            f"[bounty-board] task {args.task!r} not in DLQ "
            f"(either doesn't exist or status is not failed/unclaimable).",
            file=sys.stderr,
        )
        return 1

    json.dump(_dlq_entry_to_dict(entry), sys.stdout)
    sys.stdout.write("\n")
    return 0


def _cmd_dlq_replay(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[bounty-board] {db_path} does not exist.", file=sys.stderr)
        return 1

    q = Queue(db_path)
    try:
        from bounty_board.dlq import DLQ

        try:
            new_id = DLQ(q).replay(args.task)
        except ValueError as e:
            print(f"[bounty-board] {e}", file=sys.stderr)
            return 1
    finally:
        q.close()

    json.dump({"replayed_from": args.task, "new_task_id": new_id}, sys.stdout)
    sys.stdout.write("\n")
    return 0


def _cmd_dlq_purge(args: argparse.Namespace) -> int:
    from bounty_board.queue import Queue

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[bounty-board] {db_path} does not exist.", file=sys.stderr)
        return 1

    q = Queue(db_path)
    try:
        from bounty_board.dlq import DLQ

        n_purged = DLQ(q).purge_older_than(days=args.days)
    finally:
        q.close()

    json.dump({"purged_n": n_purged, "older_than_days": args.days}, sys.stdout)
    sys.stdout.write("\n")
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

    post_p = subparsers.add_parser(
        "post",
        help="Post a new task to the queue.",
    )
    post_p.add_argument("db_path", help="path to the queue .db file")
    post_p.add_argument(
        "--type", dest="task_type", required=True,
        help="task_type (categorical, e.g. 'summarize')",
    )
    post_p.add_argument(
        "--signature",
        help="payload_signature (defaults to task_type). Use 'open' for "
        "the bootstrap sentinel that any agent can claim.",
    )
    post_p.add_argument(
        "--payload", help="payload as inline JSON (e.g. --payload '{}')",
    )
    post_p.add_argument(
        "--payload-stdin", action="store_true",
        help="read payload JSON from stdin",
    )
    post_p.add_argument(
        "--priority", type=int, default=0,
        help="priority (higher = claimed sooner; default 0)",
    )
    post_p.add_argument(
        "--max-attempts", type=int, default=3,
        help="max claim attempts before parking in 'failed' (default 3)",
    )
    post_p.add_argument(
        "--create", action="store_true",
        help="initialize the queue file if it doesn't exist yet",
    )
    post_p.set_defaults(func=_cmd_post)

    claim_p = subparsers.add_parser(
        "claim",
        help="Atomically claim a task as the named agent.",
    )
    claim_p.add_argument("db_path", help="path to the queue .db file")
    claim_p.add_argument(
        "--agent", required=True, help="agent_id for the claim",
    )
    claim_p.add_argument(
        "--json", action="store_true",
        help="emit {claimed: false} JSON when no claimable task instead of "
        "exit-1 with stderr; useful for shell pipelines that branch on the "
        "result. Exit code is still 1 to signal 'nothing claimed.'",
    )
    claim_p.set_defaults(func=_cmd_claim)

    complete_p = subparsers.add_parser(
        "complete",
        help="Mark a claimed task done.",
    )
    complete_p.add_argument("db_path", help="path to the queue .db file")
    complete_p.add_argument("--task", required=True, help="task_id")
    complete_p.add_argument(
        "--agent", required=True,
        help="agent_id (must match the claiming agent to credit track-record correctly)",
    )
    complete_p.add_argument(
        "--result", help="result as inline JSON (optional)",
    )
    complete_p.add_argument(
        "--tokens", type=int, default=0,
        help="token_count for this lifecycle step (default 0)",
    )
    complete_p.set_defaults(func=_cmd_complete)

    fail_p = subparsers.add_parser(
        "fail",
        help="Mark a claimed task failed (forensic capture).",
    )
    fail_p.add_argument("db_path", help="path to the queue .db file")
    fail_p.add_argument("--task", required=True, help="task_id")
    fail_p.add_argument("--agent", required=True, help="agent_id")
    fail_p.add_argument(
        "--stack", help="stack trace string (inline)",
    )
    fail_p.add_argument(
        "--stack-stdin", action="store_true",
        help="read stack trace from stdin",
    )
    fail_p.add_argument(
        "--prompt-state", help="prompt-state-at-failure as inline JSON",
    )
    fail_p.add_argument(
        "--tokens", type=int, default=0,
        help="token_count consumed before failure (default 0)",
    )
    fail_p.set_defaults(func=_cmd_fail)

    decline_p = subparsers.add_parser(
        "decline",
        help="Return a task to the queue cooperatively (decline_n increments "
        "but fail_n does not).",
    )
    decline_p.add_argument("db_path", help="path to the queue .db file")
    decline_p.add_argument("--task", required=True, help="task_id")
    decline_p.add_argument("--agent", required=True, help="agent_id")
    decline_p.add_argument(
        "--reason", required=True,
        help="structured reason string (e.g. 'task_size_exceeds_budget')",
    )
    decline_p.set_defaults(func=_cmd_decline)

    dlq_p = subparsers.add_parser(
        "dlq",
        help="DLQ surface: list / get / replay / purge failed tasks.",
    )
    dlq_sub = dlq_p.add_subparsers(dest="dlq_verb", required=True)

    dlq_list_p = dlq_sub.add_parser(
        "list", help="List DLQ entries (most-recently-failed first).",
    )
    dlq_list_p.add_argument("db_path", help="path to the queue .db file")
    dlq_list_p.add_argument(
        "--signature", help="filter by payload_signature",
    )
    dlq_list_p.add_argument(
        "--limit", type=int, default=50,
        help="max entries to return (default 50)",
    )
    dlq_list_p.add_argument(
        "--json", action="store_true",
        help="emit JSON array of full forensic dossiers",
    )
    dlq_list_p.set_defaults(func=_cmd_dlq_list)

    dlq_get_p = dlq_sub.add_parser(
        "get", help="Full forensic dossier for one DLQ entry (JSON).",
    )
    dlq_get_p.add_argument("db_path", help="path to the queue .db file")
    dlq_get_p.add_argument("--task", required=True, help="task_id")
    dlq_get_p.set_defaults(func=_cmd_dlq_get)

    dlq_replay_p = dlq_sub.add_parser(
        "replay",
        help="Re-queue a failed task as a FRESH task (provenance-chain "
        "via parent_id; original stays in 'failed' for audit).",
    )
    dlq_replay_p.add_argument("db_path", help="path to the queue .db file")
    dlq_replay_p.add_argument("--task", required=True, help="task_id to replay")
    dlq_replay_p.set_defaults(func=_cmd_dlq_replay)

    dlq_purge_p = dlq_sub.add_parser(
        "purge",
        help="Delete DLQ entries older than N days (cascades to "
        "task_events for the purged tasks).",
    )
    dlq_purge_p.add_argument("db_path", help="path to the queue .db file")
    dlq_purge_p.add_argument(
        "--days", type=float, required=True,
        help="purge entries older than this many days",
    )
    dlq_purge_p.set_defaults(func=_cmd_dlq_purge)

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
