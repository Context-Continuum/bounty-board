# Architecture

The deeper-than-README design document. Read [README.md](../README.md)
first for the elevator pitch; this file is for the engineer who's
about to embed Bounty Board into a real production system, or who
wants to understand the design choices well enough to fork or extend.

---

## Substrate-discipline

> The substrate is the truth, and the library is the thinnest possible
> veneer over it.

Every feature in Bounty Board is expressible as a SQL query or a row
insertion against the V1 schema. There is no derived state, no cache,
no in-memory index, no daemon you can't stop and restart. The
`/inspect` dashboard, the CLI, the Python API, the schema-as-SDK
contract — they're all consumers of the same canonical rows.

What this buys you:

- **Portability.** One queue = one SQLite file. You can `cp` it,
  `grep` it, attach it to an email, archive it to git-lfs, mount it
  read-only on five containers. The substrate doesn't care.
- **Audit-trivial.** SQLite is plaintext-readable in any language.
  Pop the file open in DB Browser, your data team's Postgres pipe,
  a Rust microservice, anywhere. No proprietary serialization.
- **Forensic-honest.** The `task_events` ledger is append-only. Every
  state transition lands there with timestamp + agent_id + payload.
  Six months later you can replay the exact conditions that produced
  a failure.
- **Refuses-at-write enforcement.** CHECK constraints on `tasks.status`
  and `patches.status` reject unknown values at the substrate boundary,
  not at agent vigilance. (See *Substrate-boundary discipline* in the
  [Context Continuum operating handbook](https://github.com/Context-Continuum/operating-handbook).)

## Single-binary, no broker

A "broker" in conventional queue parlance is a separate process you
operate — Redis, RabbitMQ, NATS — that ferries messages between
producers and consumers. Bounty Board is *brokerless* in the deployment
sense (no separate process to operate) but *still a broker* in the
semantic sense (it brokers tasks between producers and consumers).
That's the right honest framing.

The trade-off is intentional:

| Capability | Bounty Board (in-process, SQLite) | PSE (commercial) |
|---|---|---|
| Coordination latency | ~1–2 seconds (polling) | sub-second (wake) |
| Cross-machine | single-machine | mesh-routed |
| Setup cost | `pip install` | per-cluster install |
| Operational surface | one SQLite file | engine + bridge |

If you're a solo developer with three agents on one machine, the
polling latency is invisible. If you're a multi-tenant SaaS with
agents across regions, the polling latency adds up to dropped SLAs.
[Phase Shift Engine](https://github.com/Context-Continuum) is the
commercial path for that case; Bounty Board is the OSS on-ramp.

---

## The earned-capability model

Most queue libraries route tasks by *declared* tags: agents say
"I can do code review and translation," the broker keeps a list,
and routing matches. The classic failure mode is *overclaim* — an
agent claims everything because there's no cost to declaring, and
the queue starves the agents that are actually good at the work.

Bounty Board flips this: agents don't declare capabilities, they
**earn** them by claiming + succeeding on tasks with a given
`payload_signature`. The substrate records the history in
`agent_track_record`. Routing reads from there:

```sql
SELECT t.id
FROM tasks t
WHERE t.status = 'queued'
  AND (
        t.payload_signature = 'open'                     -- bootstrap (a)
     OR t.payload_signature IN (
          SELECT payload_signature
          FROM agent_track_record
          WHERE agent_id = ? AND success_n > 0           -- earned route
        )
     OR (t.created_at < ?                                 -- bootstrap (d)
         AND t.payload_signature NOT IN (
              SELECT payload_signature
              FROM agent_track_record
              WHERE agent_id = ? AND decline_n > 0
         ))
  )
ORDER BY t.priority DESC, t.created_at ASC
LIMIT 1
```

The bootstrap rules handle new agents:

- **(a) `payload_signature='open'`** — the explicit sentinel. Task
  posters mark exploratory work with this; any agent can claim
  regardless of track record. Once they succeed, the agent earns the
  *derived* signature (e.g. the task type), which lets them claim
  signed work later.
- **(d) stale-open auto-relaxation** — a task with a real signature
  that sits queued longer than `stale_open_seconds` (default 300s)
  becomes claimable by anyone, *except* agents who have declined
  this signature before. Rescue path for the operator-curated case
  where the normal expected takers are all busy.

Why both: (a) handles operator-curated onboarding, (d) handles task
availability rescue. The two failure modes are different; each rule
covers one.

### Supervisor is just another capability

The deepest implication of earned-capability: there is no special
"supervisor" role in the substrate. A supervisor agent is just an
agent that has demonstrated it can correctly intervene on tasks
without making things worse. They claim *supervisor bounties* (tasks
of type "watch task X and intervene if needed") the same way they
claim any other task, and their track record on supervisor signatures
is what gates their privilege.

This collapses operator-tooling and agent-tooling into one surface.
The operator can grant themselves any capability for emergencies, but
the default is that agents handle it. *Tools facilitate agents;
operator is one consumer among many.*

---

## Atomic claim path

The load-bearing correctness property: under arbitrary concurrency,
exactly one agent wins each task. Bounty Board achieves this with
SQLite's `BEGIN IMMEDIATE` + a re-checked `UPDATE`:

```
BEGIN IMMEDIATE                       -- take the reserved-lock
  SELECT … WHERE status='queued' …    -- find a candidate
  UPDATE tasks
    SET status='claimed', claimed_by=?, claimed_at=?, attempts=attempts+1
    WHERE id=? AND status='queued'    -- re-check inside the txn
  RETURNING *
COMMIT
```

`BEGIN IMMEDIATE` acquires the reserved-lock at the *start* of the
transaction, before the `SELECT`. Concurrent claimants serialize at
the lock boundary; the second waits until the first commits, then
re-reads. The `WHERE status='queued'` re-check inside the `UPDATE`
is belt-and-suspenders against any race that slips past.

WAL mode (`PRAGMA journal_mode=WAL`) is required for sane read
concurrency. We set it via `_meta.open_db`. A 5-second `busy_timeout`
absorbs short contention windows without raising.

The atomic claim race test in `tests/test_queue.py` exercises this
with 20 threads against 1 task and asserts exactly-one-winner. It's
the test you don't take a regression on.

---

## The six elegance features

Each maps to a single substrate primitive — readable straight from
the schema regardless of language.

| ID | Feature | Substrate row |
|---:|---|---|
| **D** | Agent self-diagnostic replay | `task_events(event_kind='diagnose')` |
| **#7** | Replay-time patches | `patches` table |
| **#8** | DECLINE primitive | `task_events('decline')` + `agent_track_record.decline_n` |
| **#9** | Long-poll SSE stream | `task_events` polling cursor |
| **#10** | Token-budget back-pressure | `_meta` budget rows + `task_events.token_count` SUM |
| **#11** | Live-debugger-attach | `interventions` table (voluntary-honor) |

**D — agent self-diagnostic replay.** When an agent fails, it reads its
own task_events trajectory + the trajectory of the most-similar prior
successful task (same payload_signature, status='done'). The diff
shapes a diagnosis row: `{hypothesis, proposed_patch, confidence}`.
Below confidence threshold, escalate via W1 attempts pattern.

**#7 — replay-time patches.** A patch is a structured payload
transformer pinned to a payload_signature. Status starts as
`candidate` (applies only to the proposing agent's own replays). At
`n_successes >= queue.patch_threshold` (default 3) the patch
auto-promotes to `canonical` — applies to all future claims of
matching signature, regardless of which agent claims. Patch
promotion is a substrate-side state transition, not a human review.

**#8 — DECLINE primitive.** `task.decline(reason)` returns the task
to `queued` cooperatively. Tracked in `decline_n` separately from
`fail_n` — declining doesn't burn the agent's earned-capability
ratio; it's a structured "not for me." Routing uses `decline_n > 0`
as a "don't re-offer this signature to this agent" hint.

**#9 — long-poll SSE stream.** `GET /api/events?since=<cursor>`
returns `task_events.id > cursor` rows. Clients reconnect with the
last seen id. ~1–2s perceived latency at the default 2s polling
cadence. (Wake-routed sub-second streaming is the commercial PSE
upgrade.)

**#10 — token-budget back-pressure.** Operator sets a per-queue
budget at construction. Rolling-window consumption is computed
lazily from `SUM(task_events.token_count)`. Agents read the
budget state and back off voluntarily; at 100% the claim path
returns `BudgetExceeded` and pauses new claims until consumption
drops below cap.

**#11 — live-debugger-attach.** A supervising agent posts an
intervention row; the working agent's claim loop checks at each
tool-call safe boundary and voluntarily honors it. Voluntary-honor
preserves the forensic trajectory (vs. hard-preemption which would
require kill-and-replay and lose Option D's continuity).

---

## Schema versioning

The SQLite schema is the *public contract*. Any language can read
the tables directly; that's the lead-gen story for the schema-is-SDK
framing. But a public contract that changes silently is a public
contract you can't depend on. Bounty Board's substrate-side answer:

- Every queue file has a `_meta` table with a `schema_version` row.
- `_meta.open_db()` checks the version on every connection.
- Forward migrations live in `bounty_board/migrations/NNNN_*.sql`,
  one file per version bump. Each is executed as a single
  `executescript` so it can contain multiple statements.
- Opening a queue at a *future* schema_version raises `SchemaError`
  with a clear message — downgrade is unsupported. The fix is to
  upgrade the library, not patch around the version check.

External readers can branch on `schema_version` exactly the way the
library does. The contract is self-describing.

---

## The CLI

The shell-side surface is a full lifecycle:

```bash
bounty-board init       <db>                    # create/migrate forward
bounty-board post       <db> --type ...         # post a task
bounty-board claim      <db> --agent ...        # claim atomically
bounty-board complete   <db> --task ...         # mark done
bounty-board fail       <db> --task ...         # mark failed
bounty-board decline    <db> --task ...         # cooperative decline
bounty-board status     <db>                    # depth + counts snapshot
bounty-board dlq list   <db>                    # forensic listings
bounty-board dlq get    <db> --task ...         # full dossier
bounty-board dlq replay <db> --task ...         # provenance-chain replay
bounty-board dlq purge  <db> --days N           # lifecycle cleanup
bounty-board inspect    <db>                    # /inspect dashboard
```

Each verb maps to a single substrate operation. `claim` takes the
same SQLite reserved-lock as `Queue.claim()` and emits the same
`task_events` row. JSON-default-on-claim is shell-pipeline-friendly:

```bash
TASK=$(bounty-board claim my.db --agent worker_$$ | jq -r '.task_id')
if [ -n "$TASK" ]; then
  result=$(do_my_work "$TASK")
  bounty-board complete my.db --task "$TASK" --agent worker_$$ \
    --result "$result" --tokens 4200
fi
```

This composes cleanly with cron, systemd timers, GitHub Actions,
or any other shell-based orchestrator.

---

## What this is *not*

Honest boundaries — Bounty Board is the queue, not the agent runtime
or the orchestration layer:

- **Not a cross-machine wake substrate.** Polling cursor; ~1–2s
  latency between events. PSE territory.
- **Not a saga orchestrator.** No first-class `task_A.then(task_B)`
  chaining at V1. Compose manually via Python.
- **Not a message broker.** Task semantics (claim-and-process), not
  pub/sub fan-out.
- **Not a streaming-RPC framework.** Use gRPC.
- **Not real-time.** Polling cadence is the polling cadence.
- **Not multi-tenant by default.** One queue = one SQLite file = one
  tenant's view. Multi-tenant routing is your application's concern.

---

## Future direction

V2 candidates (not in V1 ship):

- **Pipeline chaining** — `task.then(task_b)` first-class.
  Specification questions about partial-success and payload-size
  guards are real; the design lane has more.
- **Postgres backend** — same schema, same API, scale-out for teams
  that outgrow SQLite. `bounty-board[postgres]` extras dep.
- **Distributed multi-machine claim** — Postgres advisory locks
  unlock the atomic-claim path across machines.
- **Async wrapper** — `async with queue.claim(...) as task:` over the
  sync core via `asyncio.to_thread`. Zero new deps.
- **PSE integration adapter** — the production-stress-test where
  Phase Shift Engine eats its own dog food via Bounty Board.

V1's job is to ship a credible single-machine queue. V2's job is to
ship the next layer once V1 has real users.
