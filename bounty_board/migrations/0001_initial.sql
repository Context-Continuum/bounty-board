-- bounty_board V1 initial schema.
--
-- Capability model: EARNED — tasks do NOT carry a `capabilities[]` column;
-- agents prove they can handle a `payload_signature` by claiming + succeeding
-- on prior tasks with the same signature. See decision_id
-- `cluster_brokerless_task_queue_pitch_v0`, design lane on cluster scratchpad.
--
-- All six V1 elegance features land their substrate here at once:
--   D     agent self-diagnostic replay     -> task_events (event_kind='diagnose')
--   #7    replay-time patches              -> patches
--   #8    DECLINE primitive                -> task_events + agent_track_record.decline_n
--   #9    live receipts SSE stream         -> reads from task_events
--   #10   token-budget back-pressure       -> _meta budget rows + task_events.token_count SUM
--   #11   live-debugger-attach             -> interventions

-- ============================================================
-- TASKS — the queue itself
-- ============================================================
CREATE TABLE tasks (
    id                  TEXT PRIMARY KEY,
    payload_json        TEXT NOT NULL,
    task_type           TEXT NOT NULL,
    payload_signature   TEXT NOT NULL,
    priority            INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'queued',
    claimed_by          TEXT,
    claimed_at          REAL,
    completed_at        REAL,
    attempts            INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 3,
    parent_id           TEXT REFERENCES tasks(id),
    created_at          REAL NOT NULL,
    CHECK (status IN ('queued', 'claimed', 'processing', 'done', 'failed', 'unclaimable'))
);

CREATE INDEX idx_tasks_status_priority_created
    ON tasks (status, priority DESC, created_at ASC);

CREATE INDEX idx_tasks_payload_signature
    ON tasks (payload_signature);

-- ============================================================
-- TASK_EVENTS — append-only ledger (time-travel DLQ + #9 SSE source)
--
-- Every state transition + agent action lands a row here. The
-- canonical event_kind values include (non-exhaustive — code may add):
--   'claim', 'process_step', 'complete', 'fail', 'decline',
--   'diagnose', 'intervene', 'budget_state'.
--
-- payload_json shape depends on event_kind. For 'fail' it carries
-- {stack, prompt_state}. For 'diagnose' it carries the hypothesis +
-- proposed_patch (per Option D). token_count is the per-event token
-- spend for #10 rollup.
-- ============================================================
CREATE TABLE task_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             TEXT NOT NULL REFERENCES tasks(id),
    event_kind          TEXT NOT NULL,
    ts                  REAL NOT NULL,
    agent_id            TEXT,
    payload_json        TEXT,
    token_count         INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_task_events_task_id_ts
    ON task_events (task_id, ts);

CREATE INDEX idx_task_events_kind_ts
    ON task_events (event_kind, ts);

-- ============================================================
-- AGENT_TRACK_RECORD — earned-capability lookup
--
-- The substrate that makes EARNED work. Routing decisions consult this:
-- "which agents have success_n > 0 on this payload_signature, and have
-- they been seen recently enough to be considered online?"
--
-- decline_n is tracked separately from fail_n per #8: declining is
-- cooperative ("not for me") and does NOT count against the agent's
-- earned-capability ratio, but DOES act as a routing hint
-- ("don't re-offer this signature to this agent for a while").
-- ============================================================
CREATE TABLE agent_track_record (
    agent_id            TEXT NOT NULL,
    payload_signature   TEXT NOT NULL,
    success_n           INTEGER NOT NULL DEFAULT 0,
    fail_n              INTEGER NOT NULL DEFAULT 0,
    decline_n           INTEGER NOT NULL DEFAULT 0,
    last_seen_at        REAL,
    PRIMARY KEY (agent_id, payload_signature)
);

CREATE INDEX idx_track_record_signature
    ON agent_track_record (payload_signature, success_n DESC);

-- ============================================================
-- PATCHES — #7 replay-time patches
--
-- A patch is a structured payload-transformer pinned to a
-- payload_signature. status='candidate' patches apply only to the
-- proposing agent's own replays; at n_successes >= queue's
-- patch_threshold (default 3) the patch auto-promotes to
-- status='canonical' and applies to all future claims of matching
-- signature.
-- ============================================================
CREATE TABLE patches (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_signature       TEXT NOT NULL,
    transformer_json        TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'candidate',
    n_successes             INTEGER NOT NULL DEFAULT 0,
    n_failures              INTEGER NOT NULL DEFAULT 0,
    proposed_by_agent_id    TEXT NOT NULL,
    proposed_at             REAL NOT NULL,
    promoted_at             REAL,
    CHECK (status IN ('candidate', 'canonical', 'retired'))
);

CREATE INDEX idx_patches_signature_status
    ON patches (payload_signature, status);

-- ============================================================
-- INTERVENTIONS — #11 live-debugger-attach substrate
--
-- A supervising agent (or the operator) posts an intervention; the
-- working agent's claim loop checks this table at each tool-call
-- safe boundary and voluntarily honors it. Voluntary-honor preserves
-- the task trajectory for Option D's forensic-state continuity.
-- ============================================================
CREATE TABLE interventions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id                 TEXT NOT NULL REFERENCES tasks(id),
    kind                    TEXT NOT NULL,
    payload_json            TEXT,
    posted_by_agent_id      TEXT NOT NULL,
    posted_at               REAL NOT NULL,
    honored_at              REAL
);

CREATE INDEX idx_interventions_task_posted
    ON interventions (task_id, posted_at);
