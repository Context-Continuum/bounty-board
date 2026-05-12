# Bounty Board

> **Single-binary task queue for multi-agent systems.**
> SQLite-backed atomic claims. Time-travel DLQ. Capability pull routing —
> where agents *earn* their tags by doing the work, not by declaring them.

No Redis. No broker daemon. No infra commit.

```bash
pip install bounty-board
```

---

## Why it exists

If you're building anything with multiple LLM agents — a coding swarm, a
research drone fleet, an in-house support agent farm — the queue is the
first piece you wrote yourself, and the first piece that broke under
production load.

Most agent frameworks ship a monolith (you take the whole opinion-stack
or nothing) or assume Redis/Celery (so now you're operating a broker
just to ferry JSON between three Python processes on one box).

Bounty Board is the missing primitive: a single file, a single
dependency, atomic claims, full forensic capture when agents fail, and
a read-only dashboard that shows you what's happening in real time.

The substrate is **a SQLite file**. That's it. You can `cp` it, `grep`
it, attach it to an email, check it into git. What the dashboard shows
IS what the file says — no derived state, no cache, no in-memory index.

---

## 60-second Quickstart

### 1. Install + post a task

```python
from bounty_board.queue import Queue, OPEN_SIGNATURE

q = Queue("bounties.db")

# The 'open' sentinel marks this task as claimable by any agent
# (bootstrap rule for new agents joining the cluster).
task_id = q.post(
    task_type="summarize",
    payload={"text": "The quick brown fox..."},
    payload_signature=OPEN_SIGNATURE,
)
```

### 2. Claim + work

```python
import traceback

task = q.claim(agent_id="agent_1")
if task is None:
    print("no claimable work right now")
else:
    try:
        # ...your agent does the work...
        result = run_my_llm(task.payload["text"])
        task.complete(result={"summary": result}, token_count=4200)
    except Exception:
        task.fail(stack=traceback.format_exc(), token_count=4200)
```

That's it. The claim is **atomic** — even if a thousand agents call
`claim()` simultaneously, exactly one wins each task.

### 3. Watch it live

```bash
pip install 'bounty-board[inspect]'
bounty-board inspect bounties.db
# → open http://127.0.0.1:8888
```

You'll see queue depth, recent events, click-through into task detail
with the full event ledger, a DLQ tab for failed tasks. The dashboard
polls every 2 seconds; ~1–2s perceived latency between events.

---

## The six V1 features

Each one is a single substrate primitive — readable straight from the
SQLite schema, regardless of which language or framework you connected
the agent in.

| | Feature | What it does | Substrate |
|---|---|---|---|
| 1 | **Atomic claims** | `BEGIN IMMEDIATE` + conditional `UPDATE...RETURNING`. Concurrent claimants serialize at the SQLite reserved-lock; exactly one wins each task. | `tasks` |
| 2 | **Time-travel DLQ** | Every state transition writes a row to `task_events` with the full forensic payload — prompt state, stack trace, token count. Replay a failure six months later, exactly. | `task_events` |
| 3 | **Earned pull routing** | Agents don't *declare* capabilities. They *prove* them by claiming + succeeding on tasks with a given `payload_signature`. New agents bootstrap via the `'open'` sentinel or stale-open auto-relaxation. | `agent_track_record` |
| 4 | **Self-diagnostic replay** | When an agent fails, it can read its own task trajectory + diff against a prior-successful run on the same signature, propose a structured patch, and (after N successful replays) auto-promote the patch to canonical. | `task_events`, `patches` |
| 5 | **DECLINE primitive** | `task.decline(reason)` returns the task to the queue *cooperatively*. Tracked separately from `fail_n` — declining doesn't burn the agent's earned-capability record; it's "not for me, try someone else." | `agent_track_record.decline_n` |
| 6 | **Live-debugger attach** | A supervising agent (or operator) posts an intervention row; the working agent checks for interventions at each tool-call safe boundary and voluntarily honors them. Voluntary-honor preserves the forensic trajectory. Supervisor is just another earned capability. | `interventions` |

Token-budget back-pressure (`_meta` budget rows + `task_events.token_count` SUM rollup) is in scope for V1 and ships in a follow-up release. SSE long-poll event stream is exposed at `/api/events?since=<cursor>` once the dashboard is installed.

---

## What this is **not**

Honest framing matters. Bounty Board is:

- **Not a cross-machine wake substrate.** Polling cursor; ~1–2s latency between events. For sub-second cross-agent coordination see *When you outgrow Bounty Board* below.
- **Not a saga orchestrator.** First-class `task_A.then(task_B)` pipeline chaining is V2. Today you chain manually via a 5-line wrapper.
- **Not a message broker.** It's a *task queue* — work-to-do with claim semantics, not pub/sub fan-out.
- **Not a streaming-RPC framework.** Use gRPC.
- **Not real-time.** The polling cadence is the polling cadence. If you need wake-routing, you're looking for the commercial sibling.

---

## When you outgrow Bounty Board

Bounty Board is **in-process**: polled subscribes, single-machine
coordination, ~1–2 second latency between events. That's a sweet spot
for most multi-agent workloads — solo developer with three agents on
one box, small team's internal automation, a research swarm where the
agents coordinate at human-conversation cadence.

For sub-second cross-agent wake routing, cluster-scale coordination
across machines, and live agent-to-agent supervision at production
load, **Bounty Board's commercial sibling is
[Phase Shift Engine](https://github.com/Context-Continuum)** — same
team, same substrate-discipline ethos, scaled up. Reach out via
[github.com/Context-Continuum](https://github.com/Context-Continuum)
if that's what you're hitting.

The same substrate-honest framing applies on that side of the line: PSE
is the *engine*, not the *opinion*.

---

## Architecture

```
┌──────────────┐        ┌──────────────┐
│   Agent A    │        │   Agent B    │
│ (any lang)   │        │ (any lang)   │
└──────┬───────┘        └──────┬───────┘
       │ claim/post/...        │ claim/post/...
       ▼                       ▼
┌─────────────────────────────────────────┐
│      SQLite (WAL mode, atomic claim)    │
│  ┌───────┐ ┌─────────────┐ ┌─────────┐  │
│  │ tasks │ │ task_events │ │ patches │  │
│  └───────┘ └─────────────┘ └─────────┘  │
│  ┌───────────────────┐ ┌──────────────┐ │
│  │ agent_track_record│ │interventions │ │
│  └───────────────────┘ └──────────────┘ │
└─────────────────────────────────────────┘
       ▲
       │ polling cursor (HTMX every 2s)
┌──────┴───────────┐
│ /inspect dashboard │
│ (optional, [inspect]│
│  extras)            │
└────────────────────┘
```

**One queue = one SQLite file.** Portable — `cp` it, version it, ship
it as an artifact. Backup-trivial — `sqlite3 bounties.db .dump`.
Audit-trivial — SQLite is text-readable in any language.

**Atomic claim path** (`bounty_board/queue.py`):

```python
BEGIN IMMEDIATE
  SELECT … FROM tasks
   WHERE status='queued'
     AND (payload_signature = 'open'            -- bootstrap rule (a)
       OR payload_signature IN (
            SELECT payload_signature
            FROM agent_track_record
            WHERE agent_id = ? AND success_n > 0  -- earned route
          )
       OR (created_at < ?                          -- bootstrap rule (d)
           AND payload_signature NOT IN (
                SELECT payload_signature
                FROM agent_track_record
                WHERE agent_id = ? AND decline_n > 0
           )))
   ORDER BY priority DESC, created_at ASC
   LIMIT 1
  UPDATE tasks SET status='claimed', claimed_by=?, claimed_at=?
   WHERE id = ? AND status='queued'             -- re-check inside txn
  RETURNING *
COMMIT
```

The SQLite reserved-lock semantics give us exactly-one-wins under any
amount of concurrency. The `WHERE status='queued'` re-check inside the
transaction is belt-and-suspenders.

**Schema is the SDK.** External readers — Go, Rust, JS, your data team's
Postgres pipe — read directly from the `tasks` + `task_events` tables.
There's no "client SDK" to maintain. Each table is documented in
[`bounty_board/migrations/0001_initial.sql`](bounty_board/migrations/0001_initial.sql).
A `_meta` row carries `schema_version`; the library refuses to open
a database from a future schema version (downgrade unsupported).

---

## Repo layout

```
bounty_board/
  __init__.py
  _meta.py                # schema-version check + forward-migration runner
  queue.py                # atomic claim path + lifecycle methods
  inspect.py              # /inspect dashboard (optional, [inspect] extras)
  _cli.py                 # `bounty-board inspect <db>` console script
  migrations/
    0001_initial.sql      # V1 schema (5 tables + 7 named indices)
tests/
  test_meta.py            # 4 tests — substrate
  test_schema_v1.py       # 9 tests — schema correctness
  test_queue.py           # 18 tests — atomic claim, earned-capability, lifecycle
  test_inspect.py         # 18 tests — dashboard endpoints + HTML routes
pyproject.toml
README.md
```

`pip install bounty-board` gives you the core library (no FastAPI/uvicorn).
`pip install 'bounty-board[inspect]'` adds the dashboard.

---

## Running the test suite

```bash
pip install -e '.[dev]'
pytest -v
# 49 passed in ~1.5s
```

CI: GitHub Actions runs the suite on Linux / macOS / Windows × Python 3.11 / 3.12.

---

## Design lane

The full design discussion (six elegance features, capability model
choice, bootstrap rule, WAKE-vs-poll pivot, supervisor-as-capability
principle) lives in the cluster scratchpad under decision_id
**`cluster_brokerless_task_queue_pitch_v0`**.

The short version of the ethos: *the substrate is the truth, and agents
are first-class consumers of it.* Every line of code in this repo passes
through that filter — if a feature can be expressed by adding a column
or a row, that's where it lives. The library is the thinnest possible
veneer over the schema.

---

## License

[MIT](./LICENSE) — same team that publishes
[Phase Shift Engine](https://github.com/Context-Continuum), under
[Context Continuum](https://github.com/Context-Continuum).
