# SQLite schema reference (V1)

> The schema is the SDK.

Every Bounty Board queue is a SQLite file at `schema_version=1`. Any
language with a SQLite driver can read the tables directly — no
client SDK to maintain, no protocol to version. This document is the
public contract.

For the live source-of-truth, see
[`bounty_board/migrations/0001_initial.sql`](../bounty_board/migrations/0001_initial.sql).

---

## Tables

### `_meta`

The version + configuration store. One row per `key`.

| column | type | notes |
|---|---|---|
| `key` | `TEXT PRIMARY KEY` | well-known: `'schema_version'`, future: `'budget_config'`, etc. |
| `value` | `TEXT NOT NULL` | always serialized as text; numerics stored as decimal strings |

**Required rows:**

- `('schema_version', '1')` — present whenever the migration runner
  has been through at least one forward step.

Library behavior: `_meta.open_db()` refuses to open a queue file
where `schema_version` is *greater* than the library's
`SCHEMA_VERSION_EXPECTED`. Equal-or-less is fine; less triggers the
forward migrations.

External readers: check this row first. Branch your code on the
version. If you see a version you don't understand, refuse rather
than guess at the new column shape.

---

### `tasks`

The queue itself. One row per task.

| column | type | notes |
|---|---|---|
| `id` | `TEXT PRIMARY KEY` | UUID hex by convention (32 chars), but any unique string works |
| `payload_json` | `TEXT NOT NULL` | the work payload as JSON |
| `task_type` | `TEXT NOT NULL` | categorical (e.g. `'summarize'`, `'review_diff'`) |
| `payload_signature` | `TEXT NOT NULL` | the **earned-capability key**; agents prove they can handle this signature by past success on it. Use `'open'` for the bootstrap sentinel. |
| `priority` | `INTEGER NOT NULL DEFAULT 0` | higher = claimed sooner; ties broken by `created_at` ASC |
| `status` | `TEXT NOT NULL DEFAULT 'queued'` | enum (see CHECK) |
| `claimed_by` | `TEXT` | agent_id of the claimant, null when unclaimed |
| `claimed_at` | `REAL` | epoch seconds, null when unclaimed |
| `completed_at` | `REAL` | epoch seconds, set on transition to done/failed |
| `attempts` | `INTEGER NOT NULL DEFAULT 0` | incremented on each claim |
| `max_attempts` | `INTEGER NOT NULL DEFAULT 3` | claim path parks the task in `failed` when `attempts >= max_attempts` |
| `parent_id` | `TEXT REFERENCES tasks(id)` | for replay provenance chain (DLQ.replay sets this) |
| `created_at` | `REAL NOT NULL` | epoch seconds |

**CHECK constraint:**

```sql
status IN ('queued', 'claimed', 'processing', 'done', 'failed', 'unclaimable')
```

The `'processing'` status is V1-reserved — current claim path uses
`'claimed'` for the in-flight state. `'unclaimable'` is also V1-
reserved for future routing-failure paths.

**Indices:**

- `idx_tasks_status_priority_created (status, priority DESC, created_at ASC)`
  — the canonical claim-path lookup
- `idx_tasks_payload_signature (payload_signature)` — for routing
  decisions

---

### `task_events`

Append-only ledger. The time-travel DLQ source + the SSE polling-
cursor source. Every state transition + agent action lands one row.

| column | type | notes |
|---|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | the polling-cursor key |
| `task_id` | `TEXT NOT NULL REFERENCES tasks(id)` | FK enforced |
| `event_kind` | `TEXT NOT NULL` | canonical values below |
| `ts` | `REAL NOT NULL` | epoch seconds |
| `agent_id` | `TEXT` | null for substrate-emitted events (e.g. `'replay'`) |
| `payload_json` | `TEXT` | event-kind-specific shape |
| `token_count` | `INTEGER NOT NULL DEFAULT 0` | per-event token spend (#10 budget rollup) |

**Canonical `event_kind` values:**

| kind | when emitted | payload_json shape |
|---|---|---|
| `claim` | atomic claim path succeeds | null |
| `complete` | `task.complete(result, token_count)` | the `result` dict, or null |
| `fail` | `task.fail(stack, prompt_state, token_count)` | `{"stack": str, "prompt_state": dict\|null, "post_status": "queued"\|"failed"}` |
| `decline` | `task.decline(reason)` | `{"reason": str}` |
| `diagnose` | agent self-diagnostic (Option D) | `{"hypothesis": str, "proposed_patch": dict\|null, "confidence": 0.0–1.0}` |
| `replay` | DLQ.replay produces a new task | `{"replayed_from": <original_task_id>}` (emitted on the NEW task) |
| `intervene` | reserved | reserved |
| `budget_state` | reserved (#10) | reserved |

**Indices:**

- `idx_task_events_task_id_ts (task_id, ts)` — DLQ trajectory lookup,
  per-task ledger scan
- `idx_task_events_kind_ts (event_kind, ts)` — server-side event-kind
  filter (#9 SSE stream)

External readers can polling-cursor over the table by `id` alone:

```sql
SELECT * FROM task_events WHERE id > ? ORDER BY id ASC LIMIT N
```

---

### `agent_track_record`

The substrate that makes earned-capability work. One row per
`(agent_id, payload_signature)` pair.

| column | type | notes |
|---|---|---|
| `agent_id` | `TEXT NOT NULL` | composite PK |
| `payload_signature` | `TEXT NOT NULL` | composite PK |
| `success_n` | `INTEGER NOT NULL DEFAULT 0` | times this agent completed this signature |
| `fail_n` | `INTEGER NOT NULL DEFAULT 0` | times this agent failed this signature |
| `decline_n` | `INTEGER NOT NULL DEFAULT 0` | times this agent declined this signature (cooperative; not a failure) |
| `last_seen_at` | `REAL` | epoch seconds; touched on every claim attempt |

**Index:**

- `idx_track_record_signature (payload_signature, success_n DESC)` —
  the "who has earned this signature, ranked by success_n?" lookup

`decline_n > 0` is a routing hint, not a hard exclusion. The stale-
open auto-relaxation rule (`bootstrap (d)`) skips agents with prior
declines on the same signature, but other paths don't. *Decline is
cooperative.*

---

### `patches`

Replay-time patches (feature #7). Each row is a structured payload-
transformer pinned to a `payload_signature`.

| column | type | notes |
|---|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `payload_signature` | `TEXT NOT NULL` | which signature this patch applies to |
| `transformer_json` | `TEXT NOT NULL` | `{"kind": "...", "args": {...}}`; library-known kinds: `'prepend_system_msg'`, `'truncate_field'`, `'swap_model'`, etc. |
| `status` | `TEXT NOT NULL DEFAULT 'candidate'` | enum (see CHECK) |
| `n_successes` | `INTEGER NOT NULL DEFAULT 0` | counter for the candidate→canonical promotion |
| `n_failures` | `INTEGER NOT NULL DEFAULT 0` | offsets `n_successes` for confidence |
| `proposed_by_agent_id` | `TEXT NOT NULL` | the diagnosing agent |
| `proposed_at` | `REAL NOT NULL` | epoch seconds |
| `promoted_at` | `REAL` | set on candidate→canonical transition |

**CHECK constraint:**

```sql
status IN ('candidate', 'canonical', 'retired')
```

- `candidate` — applies only to the proposing agent's own replays
- `canonical` — applies to all future claims of matching signature
- `retired` — superseded or proven harmful; not applied

Promotion threshold defaults to 3 (`n_successes >= 3` on the
proposing agent's replays → transition to `canonical`).

**Index:**

- `idx_patches_signature_status (payload_signature, status)` — for
  the "is there a canonical patch for this signature?" lookup that
  runs on every claim

---

### `interventions`

Live-debugger-attach (feature #11). Supervising agents (or
operators) post intervention rows; working agents voluntarily honor
them at tool-call safe boundaries.

| column | type | notes |
|---|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `task_id` | `TEXT NOT NULL REFERENCES tasks(id)` | FK enforced |
| `kind` | `TEXT NOT NULL` | `'inject_hint' \| 'swap_model' \| 'pause' \| 'cancel'` |
| `payload_json` | `TEXT` | for `inject_hint`: `{"hint": str}`. Else null. |
| `posted_by_agent_id` | `TEXT NOT NULL` | supervising agent's id |
| `posted_at` | `REAL NOT NULL` | epoch seconds |
| `honored_at` | `REAL` | null until working agent honors at next safe boundary |

**Voluntary-honor semantics:** working agents check this table at
each tool-call safe boundary and choose to honor (write
`honored_at`) or proceed. Hard-preemption (kill-and-replay) is
deliberately *not* supported — it would lose Option D's forensic-
state continuity. The substrate records the signal; the agent
controls the response.

**Index:**

- `idx_interventions_task_posted (task_id, posted_at)` — for the
  "what interventions are pending for the task I'm working on?"
  polling query

---

## Read-only adapter recipes

External readers in other languages — each is ~20 lines of code
because the schema is honest.

### Bash + `sqlite3`

```bash
# Queue depth by status
sqlite3 my.bounty.db "SELECT status, COUNT(*) FROM tasks GROUP BY status"

# Recent failed tasks with their stack
sqlite3 my.bounty.db "
  SELECT t.id, t.task_type, e.payload_json
  FROM tasks t
  JOIN task_events e ON e.task_id = t.id AND e.event_kind = 'fail'
  WHERE t.status = 'failed'
  ORDER BY t.completed_at DESC LIMIT 10
"
```

### Go

```go
db, _ := sql.Open("sqlite3", "my.bounty.db?_pragma=foreign_keys(1)")
rows, _ := db.Query(
  "SELECT id, payload_signature, status FROM tasks "+
  "WHERE status IN ('failed', 'unclaimable') ORDER BY completed_at DESC LIMIT 50",
)
defer rows.Close()
for rows.Next() {
  var id, sig, status string
  rows.Scan(&id, &sig, &status)
  fmt.Println(id, sig, status)
}
```

### Rust (`rusqlite`)

```rust
let conn = Connection::open("my.bounty.db")?;
conn.execute_batch("PRAGMA foreign_keys = ON")?;
let mut stmt = conn.prepare(
  "SELECT id, payload_signature FROM tasks WHERE status='queued' LIMIT 10",
)?;
let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
```

### Polling-cursor SSE (any language)

```
GET /api/events?since=42
→ [{"id":43,"task_id":"...","event_kind":"claim",...},
   {"id":44,"task_id":"...","event_kind":"complete",...}]

# Client: last_id = 44; reconnect with ?since=44
```

`/inspect` is the canonical implementation; external clients with
direct SQLite access can do the same query themselves:

```sql
SELECT id, task_id, event_kind, ts, agent_id, payload_json, token_count
FROM task_events
WHERE id > :since
ORDER BY id ASC
LIMIT 100
```

---

## Writers: please use the Python API

Reading directly is encouraged. *Writing* directly is supported but
discouraged for V1 — the Python `Queue` + `DLQ` classes encode the
canonical lifecycle (atomic claim, `_mark_complete` / `_mark_fail` /
`_mark_decline` track-record bookkeeping, FK-safe DLQ purge). Custom
writers that skip those helpers will produce inconsistent track
records and DLQ traversal.

V2 will publish a stable writer contract for non-Python consumers.
For V1, write through `bounty_board.Queue` (or shell out to
`bounty-board post / claim / complete / fail / decline`).

---

## Migration history

| version | migration | what landed |
|---:|---|---|
| **1** | [`0001_initial.sql`](../bounty_board/migrations/0001_initial.sql) | All 5 tables + 7 named indices + the V1 status enums (initial release). |

When V2 lands, this table grows another row. External readers should
treat any unknown version as "stop and upgrade the library."
