"""Demo data generator for the /inspect dashboard.

Populates a SQLite queue file with a realistic-looking set of tasks
across multiple simulated agents, then drives them through the
lifecycle (post → claim → process → complete/fail/decline) so the
dashboard renders a populated, live-feeling view in ~30 seconds.

Usage::

    # 1. Generate the demo queue
    python -m bounty_board.demo --db demo.db

    # 2. Run the dashboard against it (in another shell)
    bounty-board inspect demo.db

    # 3. (Operator step) Screen-record http://127.0.0.1:8888 for the
    #    README GIF. The dashboard polls every 2s so a 30-second
    #    capture gets 15+ refresh frames.

The script is intentionally substrate-honest: it doesn't touch any
private internals, only the public ``Queue`` API + raw inserts for
the things ``Queue`` doesn't expose yet (interventions). Future
modules (dlq.py / diagnose.py / patches.py) will surface their own
demo verbs and this script will pick them up by composition.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import traceback
from pathlib import Path

from bounty_board._meta import open_db
from bounty_board.queue import OPEN_SIGNATURE, Queue

# Realistic-looking task seeds — names that read like a real
# multi-agent LLM workload, not just lorem ipsum.
TASK_SEEDS: list[tuple[str, str, dict]] = [
    ("summarize_pr",         "review",       {"pr_url": "https://github.com/example/repo/pull/142"}),
    ("summarize_pr",         "review",       {"pr_url": "https://github.com/example/repo/pull/151"}),
    ("review_diff",          "review",       {"sha": "a1b2c3d", "lines": 87}),
    ("draft_release_notes",  "writing",      {"tag": "v1.4.0", "since": "v1.3.2"}),
    ("triage_issue",         "triage",       {"issue": 42, "title": "OOM on large payload"}),
    ("triage_issue",         "triage",       {"issue": 47, "title": "Race in claim path"}),
    ("classify_bug",         "triage",       {"issue": 50}),
    ("answer_customer",      "support",      {"thread": "T-9001", "topic": "auth"}),
    ("answer_customer",      "support",      {"thread": "T-9012", "topic": "billing"}),
    ("translate_doc",        "writing",      {"src": "en", "dst": "es", "doc": "quickstart.md"}),
    ("translate_doc",        "writing",      {"src": "en", "dst": "ja", "doc": "quickstart.md"}),
    ("vector_search",        "retrieval",    {"query": "atomic claim sqlite", "k": 8}),
    ("vector_search",        "retrieval",    {"query": "earned capability routing", "k": 8}),
    ("extract_entities",     "extraction",   {"doc_id": "doc_771", "schema": "person,org"}),
    ("draft_changelog",      "writing",      {"window": "2026-05"}),
    ("answer_customer",      "support",      {"thread": "T-9047", "topic": "performance"}),
    ("review_diff",          "review",       {"sha": "f9e8d7c", "lines": 213}),
    ("classify_bug",         "triage",       {"issue": 53}),
    ("vector_search",        "retrieval",    {"query": "stale-open auto-relaxation", "k": 12}),
    ("triage_issue",         "triage",       {"issue": 55, "title": "decline_n permanent shadow"}),
]


AGENTS = ["agent_alice", "agent_bob", "agent_clio"]


def seed_track_records(db_path: Path) -> None:
    """Pre-seed earned capabilities so agents can immediately claim.

    Without this, every task would either need ``OPEN_SIGNATURE`` or
    wait the 5-minute stale-open window — neither produces a snappy
    demo. Seeds 2 of the 3 agents with success_n on each task_type so
    they're earned-eligible; leaves one agent fresh to demonstrate the
    OPEN sentinel + stale-open bootstrap paths.
    """
    conn = open_db(db_path)
    try:
        now = time.time()
        # alice has earned everything; bob has earned half; clio fresh
        for task_type in {"summarize_pr", "review_diff", "draft_release_notes",
                          "triage_issue", "classify_bug", "answer_customer",
                          "translate_doc", "vector_search", "extract_entities",
                          "draft_changelog"}:
            conn.execute(
                "INSERT OR IGNORE INTO agent_track_record "
                "(agent_id, payload_signature, success_n, last_seen_at) "
                "VALUES (?, ?, ?, ?)",
                ("agent_alice", task_type, random.randint(3, 12), now),
            )
        for task_type in {"summarize_pr", "review_diff", "triage_issue",
                          "answer_customer", "vector_search"}:
            conn.execute(
                "INSERT OR IGNORE INTO agent_track_record "
                "(agent_id, payload_signature, success_n, last_seen_at) "
                "VALUES (?, ?, ?, ?)",
                ("agent_bob", task_type, random.randint(1, 6), now),
            )
        conn.commit()
    finally:
        conn.close()


def post_seed_tasks(q: Queue, rng: random.Random) -> list[str]:
    """Post the curated TASK_SEEDS. Some get OPEN_SIGNATURE so the new
    agent (clio) can grab them — exercises bootstrap rule (a) in the
    visualization.
    """
    posted: list[str] = []
    for task_type, signature, payload in TASK_SEEDS:
        # Roughly 1 in 4 tasks gets the OPEN sentinel
        use_open = rng.random() < 0.25
        tid = q.post(
            task_type=task_type,
            payload=payload,
            payload_signature=OPEN_SIGNATURE if use_open else signature,
            priority=rng.choice([0, 0, 0, 5, 10]),  # mostly default, occasionally elevated
        )
        posted.append(tid)
    return posted


def drive_lifecycle(q: Queue, n_cycles: int, rng: random.Random,
                   pace_seconds: float = 1.0) -> None:
    """Drive tasks through claim → complete/fail/decline at human cadence.

    Each cycle: one of the agents claims one task and either completes
    (most common), fails (occasional — drives the DLQ), or declines
    (rare — exercises #8 cooperative decline). The pace_seconds delay
    is the "feels live" rhythm for the demo recording — fast enough
    to be interesting, slow enough that the eye can follow.
    """
    outcomes = {"complete": 0, "fail": 0, "decline": 0, "no_claim": 0}
    for _ in range(n_cycles):
        agent = rng.choice(AGENTS)
        task = q.claim(agent_id=agent)
        if task is None:
            outcomes["no_claim"] += 1
            time.sleep(pace_seconds * 0.5)
            continue

        # Outcome distribution: 75% complete, 15% fail, 10% decline
        roll = rng.random()
        if roll < 0.75:
            # Pretend to work
            time.sleep(pace_seconds * rng.uniform(0.4, 0.8))
            task.complete(
                result={"ok": True, "by": agent},
                token_count=rng.randint(800, 8000),
            )
            outcomes["complete"] += 1
        elif roll < 0.90:
            # Simulate a failure with a forensic stack
            try:
                raise RuntimeError(
                    f"demo failure on task_type={task.task_type} for agent={agent}"
                )
            except RuntimeError:
                stack = traceback.format_exc()
            task.fail(
                stack=stack,
                prompt_state={"agent": agent, "payload_excerpt": str(task.payload)[:80]},
                token_count=rng.randint(500, 3000),
            )
            outcomes["fail"] += 1
        else:
            task.decline(reason=rng.choice([
                "agent_busy_with_higher_priority",
                "task_size_exceeds_my_context_budget",
                "topic_outside_my_proven_capability",
            ]))
            outcomes["decline"] += 1

        time.sleep(pace_seconds * rng.uniform(0.3, 0.7))
    return outcomes


def post_demo_interventions(db_path: Path, rng: random.Random) -> int:
    """Drop a few intervention rows so the /inspect interventions UI
    has data to render. Targets the most-recently-touched tasks
    regardless of status — interventions are valid on any task (an
    operator might post one on a completed task as a retrospective
    annotation, for example). Direct SQL because ``Queue`` doesn't
    expose intervention posting yet — Mac/B's interventions.py PR
    will land that helper later.
    """
    conn = open_db(db_path)
    n_posted = 0
    try:
        # Pick a handful of recently-touched tasks (any status).
        cur = conn.execute(
            "SELECT id FROM tasks "
            "ORDER BY created_at DESC LIMIT 10"
        )
        rows = [r[0] for r in cur.fetchall()]
        if not rows:
            return 0
        kinds = ["inject_hint", "swap_model", "pause", "cancel"]
        hints = [
            "check the edge case where payload is empty",
            "agent_alice succeeded on similar shape — see task_events",
            "this is the deflake test you've been chasing",
        ]
        now = time.time()
        # Post 3 interventions across the candidates
        for tid in rng.sample(rows, min(3, len(rows))):
            kind = rng.choice(kinds)
            payload = (
                json.dumps({"hint": rng.choice(hints)})
                if kind == "inject_hint" else None
            )
            conn.execute(
                "INSERT INTO interventions "
                "(task_id, kind, payload_json, posted_by_agent_id, posted_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (tid, kind, payload, "supervisor_agent", now),
            )
            n_posted += 1
        conn.commit()
    finally:
        conn.close()
    return n_posted


def generate(
    db_path: Path,
    *,
    n_cycles: int = 30,
    pace_seconds: float = 1.0,
    seed: int | None = None,
    quiet: bool = False,
) -> dict:
    """Populate ``db_path`` with a demo queue + driven lifecycle.

    Returns a summary dict for callers / tests.
    """
    rng = random.Random(seed)

    if not quiet:
        print(f"[demo] target db: {db_path}", file=sys.stderr)
        print("[demo] seeding agent_track_record (alice=earned-all, "
              "bob=earned-half, clio=fresh)...", file=sys.stderr)
    seed_track_records(db_path)

    if not quiet:
        print(f"[demo] posting {len(TASK_SEEDS)} seed tasks...", file=sys.stderr)
    q = Queue(db_path)
    try:
        posted = post_seed_tasks(q, rng)
        if not quiet:
            print(f"[demo] driving lifecycle for {n_cycles} cycles "
                  f"at ~{pace_seconds:.1f}s pace...", file=sys.stderr)
        outcomes = drive_lifecycle(q, n_cycles, rng, pace_seconds=pace_seconds)
    finally:
        q.close()

    if not quiet:
        print("[demo] posting supervisor interventions on in-flight rows...",
              file=sys.stderr)
    n_interventions = post_demo_interventions(db_path, rng)

    summary = {
        "db_path": str(db_path),
        "tasks_posted": len(posted),
        "lifecycle_outcomes": outcomes,
        "interventions_posted": n_interventions,
    }
    if not quiet:
        print("[demo] done:", json.dumps(summary, indent=2), file=sys.stderr)
        print("[demo] start the dashboard:", file=sys.stderr)
        print(f"[demo]   bounty-board inspect {db_path}", file=sys.stderr)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bounty_board.demo",
        description=(
            "Populate a Bounty Board queue with realistic demo data, then "
            "drive tasks through the lifecycle for screen-recording the "
            "/inspect dashboard."
        ),
    )
    parser.add_argument(
        "--db", default="demo.bounty.db",
        help="path to the demo queue file (default: demo.bounty.db)",
    )
    parser.add_argument(
        "--cycles", type=int, default=30,
        help="number of claim→outcome cycles to drive (default: 30)",
    )
    parser.add_argument(
        "--pace", type=float, default=1.0,
        help="seconds between cycles, scales the 'feels live' rhythm "
             "(default: 1.0, total ~30s for 30 cycles)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="random seed for reproducible demo data (default: None)",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="suppress progress messages on stderr",
    )
    parser.add_argument(
        "--fast", action="store_true",
        help="shortcut for --pace 0 --cycles 30 (no sleeps; fixture mode)",
    )
    args = parser.parse_args(argv)

    if args.fast:
        args.pace = 0.0

    generate(
        Path(args.db),
        n_cycles=args.cycles,
        pace_seconds=args.pace,
        seed=args.seed,
        quiet=args.quiet,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
