"""``/inspect`` — read-only dashboard over a Bounty Board queue.

Polling-cursor model (WAKE substrate is reserved for the commercial
Phase Shift Engine layer; see decision_id
``cluster_brokerless_task_queue_pitch_v0`` in the design lane). The
dashboard refreshes via HTMX-driven HTTP polls with a per-section
cadence; the long-poll event stream is a cursor-paginated GET that
returns `task_events.id > cursor` rows.

Substrate-discipline: every panel reads directly from the canonical
schema rows. No derived state, no separate cache, no in-memory index.
What the dashboard shows IS what the SQLite file says — making the
substrate the single source of truth.

This module is optional (`pip install bounty-board[inspect]`). It is
not imported from ``bounty_board.__init__`` so the core package stays
free of FastAPI / uvicorn dependencies.

The dashboard handles five surfaces from the V1 schema:

  - Queue depth (counts by status)
  - Recent tasks (last N tasks, click to drill in)
  - Live event stream (polling-cursor with per-event-kind filter)
  - Task detail (full event ledger + intervene button posting to
    ``interventions`` table)
  - DLQ (failed + unclaimable tasks with their forensic state)
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

from bounty_board._meta import open_db

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


@contextmanager
def _conn(db_path: Path):
    """Per-request short-lived connection.

    SQLite + WAL + multiple readers is fine for the dashboard's polling
    cadence; we don't keep a long-lived connection across requests so the
    process can stop cleanly even if the queue file is moved.
    """
    conn = open_db(db_path)
    try:
        yield conn
    finally:
        conn.close()


def _rows_to_dicts(cursor: sqlite3.Cursor, rows: list) -> list[dict[str, Any]]:
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row, strict=False)) for row in rows]


# ---------------------------------------------------------------------------
# Queries (kept pure-SQL + parameterized for substrate-honesty)
# ---------------------------------------------------------------------------


STATUS_KEYS = ("queued", "claimed", "processing", "done", "failed", "unclaimable")


def _queue_depth(conn: sqlite3.Connection) -> dict[str, int]:
    depth = dict.fromkeys(STATUS_KEYS, 0)
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM tasks GROUP BY status"
    ).fetchall()
    for status, n in rows:
        if status in depth:
            depth[status] = int(n)
    return depth


def _recent_tasks(conn: sqlite3.Connection, limit: int = 25) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT id, task_type, payload_signature, status, priority, "
        "claimed_by, attempts, created_at "
        "FROM tasks ORDER BY created_at DESC LIMIT ?",
        (limit,),
    )
    return _rows_to_dicts(cur, cur.fetchall())


def _task_detail(conn: sqlite3.Connection, task_id: str) -> dict[str, Any] | None:
    cur = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
    rows = _rows_to_dicts(cur, cur.fetchall())
    if not rows:
        return None
    task = rows[0]

    cur = conn.execute(
        "SELECT id, task_id, event_kind, ts, agent_id, payload_json, token_count "
        "FROM task_events WHERE task_id = ? ORDER BY ts ASC, id ASC",
        (task_id,),
    )
    events = _rows_to_dicts(cur, cur.fetchall())

    cur = conn.execute(
        "SELECT id, kind, payload_json, posted_by_agent_id, posted_at, honored_at "
        "FROM interventions WHERE task_id = ? ORDER BY posted_at ASC",
        (task_id,),
    )
    interventions = _rows_to_dicts(cur, cur.fetchall())

    return {"task": task, "events": events, "interventions": interventions}


def _events_since(
    conn: sqlite3.Connection,
    since_id: int = 0,
    limit: int = 100,
    event_kind: str | None = None,
) -> list[dict[str, Any]]:
    if event_kind:
        cur = conn.execute(
            "SELECT id, task_id, event_kind, ts, agent_id, payload_json, token_count "
            "FROM task_events WHERE id > ? AND event_kind = ? "
            "ORDER BY id ASC LIMIT ?",
            (since_id, event_kind, limit),
        )
    else:
        cur = conn.execute(
            "SELECT id, task_id, event_kind, ts, agent_id, payload_json, token_count "
            "FROM task_events WHERE id > ? ORDER BY id ASC LIMIT ?",
            (since_id, limit),
        )
    return _rows_to_dicts(cur, cur.fetchall())


def _dlq_tasks(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT id, task_type, payload_signature, status, attempts, "
        "claimed_by, created_at, completed_at "
        "FROM tasks WHERE status IN ('failed', 'unclaimable') "
        "ORDER BY completed_at DESC, created_at DESC LIMIT ?",
        (limit,),
    )
    return _rows_to_dicts(cur, cur.fetchall())


def _budget_state(conn: sqlite3.Connection) -> dict[str, Any]:
    """Read budget config from _meta + current spend from task_events SUM.

    Returns a dict with limit_tokens, window_seconds, policy, current_spend,
    and a derived pct_used. Configured-but-zero limit means no budget; we
    surface that explicitly so the dashboard can render the panel as
    'not configured' instead of 0/0 NaN.
    """
    keys = {
        "budget.limit_tokens": None,
        "budget.window_seconds": None,
        "budget.policy": None,
    }
    rows = conn.execute(
        "SELECT key, value FROM _meta WHERE key IN "
        "('budget.limit_tokens', 'budget.window_seconds', 'budget.policy')"
    ).fetchall()
    for k, v in rows:
        keys[k] = v

    limit_tokens = int(keys["budget.limit_tokens"]) if keys["budget.limit_tokens"] else 0
    window_seconds = (
        float(keys["budget.window_seconds"]) if keys["budget.window_seconds"] else None
    )
    policy = keys["budget.policy"]
    configured = limit_tokens > 0

    # Spend: SUM(token_count) over the (optional) rolling window.
    if window_seconds is not None and configured:
        cutoff = time.time() - window_seconds
        spend = conn.execute(
            "SELECT COALESCE(SUM(token_count), 0) FROM task_events WHERE ts > ?",
            (cutoff,),
        ).fetchone()[0]
    else:
        spend = conn.execute(
            "SELECT COALESCE(SUM(token_count), 0) FROM task_events"
        ).fetchone()[0]
    spend = int(spend)

    pct_used = (spend * 100.0 / limit_tokens) if configured else 0.0

    return {
        "configured": configured,
        "limit_tokens": limit_tokens,
        "window_seconds": window_seconds,
        "policy": policy,
        "current_spend": spend,
        "pct_used": round(pct_used, 1),
    }


def _patches(
    conn: sqlite3.Connection,
    *,
    payload_signature: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Patches read with optional signature + status filters."""
    sql_parts = [
        "SELECT id, payload_signature, transformer_json, status, "
        "n_successes, n_failures, proposed_by_agent_id, proposed_at, "
        "promoted_at FROM patches",
    ]
    wheres = []
    params: list[Any] = []
    if payload_signature is not None:
        wheres.append("payload_signature = ?")
        params.append(payload_signature)
    if status is not None:
        wheres.append("status = ?")
        params.append(status)
    if wheres:
        sql_parts.append("WHERE " + " AND ".join(wheres))
    sql_parts.append("ORDER BY status DESC, proposed_at DESC LIMIT ?")
    params.append(limit)

    cur = conn.execute(" ".join(sql_parts), tuple(params))
    return _rows_to_dicts(cur, cur.fetchall())


def _post_intervention(
    conn: sqlite3.Connection,
    task_id: str,
    kind: str,
    payload_json: str | None,
    posted_by_agent_id: str,
) -> int:
    cur = conn.execute(
        "INSERT INTO interventions (task_id, kind, payload_json, "
        "posted_by_agent_id, posted_at) VALUES (?, ?, ?, ?, ?) RETURNING id",
        (task_id, kind, payload_json, posted_by_agent_id, time.time()),
    )
    row = cur.fetchone()
    conn.commit()
    return int(row[0])


# ---------------------------------------------------------------------------
# HTML (inline templates; no Jinja dep)
# ---------------------------------------------------------------------------


HTMX_CDN = "https://unpkg.com/htmx.org@1.9.10"


def _layout(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} — Bounty Board</title>
<script src="{HTMX_CDN}"></script>
<style>
  :root {{
    --bg: #ffffff;
    --fg: #1a1a1a;
    --muted: #6b7280;
    --border: #e5e7eb;
    --accent: #2a5bd7;
    --surface: #f9fafb;
  }}
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
         max-width: 1100px; margin: 1.5rem auto; padding: 0 1rem; color: var(--fg);
         background: var(--bg); line-height: 1.45; }}
  h1 {{ font-size: 1.5rem; margin: 0 0 0.25rem; letter-spacing: -0.01em; }}
  h2 {{ font-size: 1rem; margin: 1.5rem 0 0.5rem; color: var(--muted);
        text-transform: uppercase; letter-spacing: 0.04em; font-weight: 600; }}
  nav {{ margin-bottom: 1rem; padding-bottom: 0.6rem; border-bottom: 1px solid var(--border); }}
  nav a {{ margin-right: 1.2rem; text-decoration: none; color: var(--accent);
          font-weight: 500; }}
  nav a:hover {{ text-decoration: underline; }}

  /* Tables: responsive horizontal scroll on narrow screens */
  .table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; min-width: 540px; }}
  th, td {{ padding: 0.45rem 0.6rem; text-align: left; border-bottom: 1px solid var(--border);
            vertical-align: middle; }}
  th {{ font-weight: 600; color: var(--muted); background: var(--surface);
        font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; }}
  tbody tr:hover {{ background: var(--surface); }}

  /* Status badges */
  .badge {{ display: inline-block; padding: 0.12rem 0.5rem; border-radius: 999px;
           font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
           letter-spacing: 0.04em; }}
  .b-queued      {{ background: #eef2ff; color: #4338ca; }}
  .b-claimed     {{ background: #fef3c7; color: #92400e; }}
  .b-processing  {{ background: #dbeafe; color: #1e40af; }}
  .b-done        {{ background: #d1fae5; color: #065f46; }}
  .b-failed      {{ background: #fee2e2; color: #991b1b; }}
  .b-unclaimable {{ background: #e5e7eb; color: #4b5563; }}

  /* Queue-depth row: responsive flex */
  .depth-row {{ display: flex; gap: 0.6rem; flex-wrap: wrap; }}
  .depth-row .cell {{ background: var(--surface); padding: 0.55rem 0.85rem;
                      border-radius: 6px; border: 1px solid var(--border);
                      font-size: 0.82rem; color: var(--muted);
                      min-width: 6rem; flex: 0 0 auto;
                      text-transform: uppercase; letter-spacing: 0.04em;
                      font-weight: 600; }}
  .depth-row .n {{ display: block; color: var(--fg); font-weight: 700;
                   font-size: 1.4rem; letter-spacing: -0.02em;
                   text-transform: none; margin-top: 0.1rem; }}

  /* Inline code */
  code {{ font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
         font-size: 0.82rem; background: var(--surface); padding: 0.08rem 0.35rem;
         border-radius: 3px; color: #374151; }}

  /* Empty-state */
  .empty {{ color: var(--muted); padding: 0.8rem 1rem; background: var(--surface);
           border: 1px dashed var(--border); border-radius: 6px;
           font-size: 0.88rem; }}
  .empty strong {{ color: var(--fg); }}

  /* Anchors inside tables */
  a {{ color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}

  /* HTMX swap animation — fade-in fresh content */
  @keyframes htmx-swap-fade {{
    from {{ opacity: 0.45; }}
    to   {{ opacity: 1.0; }}
  }}
  [hx-swap-oob], .htmx-settling, .htmx-swapping ~ * {{
    animation: htmx-swap-fade 220ms ease-out;
  }}

  /* Intervene form on task-detail */
  .intervene-form {{ background: var(--surface); border: 1px solid var(--border);
                     border-radius: 6px; padding: 0.7rem 0.85rem;
                     margin-top: 0.6rem; }}
  .intervene-form label {{ display: inline-block; font-size: 0.78rem;
                            color: var(--muted); margin-right: 0.4rem;
                            text-transform: uppercase; letter-spacing: 0.04em;
                            font-weight: 600; }}
  .intervene-form select, .intervene-form input[type=text] {{
    font-family: inherit; font-size: 0.85rem; padding: 0.3rem 0.5rem;
    border: 1px solid var(--border); border-radius: 4px;
    background: var(--bg); color: var(--fg); margin-right: 0.5rem;
  }}
  .intervene-form input[type=text] {{ min-width: 16rem; }}
  .intervene-form button {{ font-family: inherit; font-size: 0.85rem;
    padding: 0.32rem 0.85rem; border: 1px solid var(--accent);
    background: var(--accent); color: white; border-radius: 4px;
    cursor: pointer; font-weight: 500;
  }}
  .intervene-form button:hover {{ filter: brightness(0.92); }}
  .intervene-form .row {{ display: flex; flex-wrap: wrap; gap: 0.4rem;
                          align-items: center; margin-bottom: 0.4rem; }}
  .intervene-form .row:last-child {{ margin-bottom: 0; }}
  .intervene-form .hint {{ font-size: 0.78rem; color: var(--muted);
                            margin-top: 0.4rem; }}

  /* Budget panel */
  .budget-panel {{ background: var(--surface); border: 1px solid var(--border);
                  border-radius: 6px; padding: 0.7rem 0.85rem;
                  display: flex; flex-wrap: wrap; gap: 1.2rem;
                  align-items: center; }}
  .budget-panel .label {{ font-size: 0.72rem; color: var(--muted);
                          text-transform: uppercase; letter-spacing: 0.04em;
                          font-weight: 600; }}
  .budget-panel .value {{ font-weight: 700; font-size: 1.05rem;
                          letter-spacing: -0.01em; }}
  .budget-bar {{ flex: 1; min-width: 12rem; height: 0.45rem;
                 background: var(--border); border-radius: 999px;
                 overflow: hidden; position: relative; }}
  .budget-bar-fill {{ height: 100%; background: var(--accent);
                      transition: width 250ms ease-out; }}
  .budget-bar-fill.bf-warn {{ background: #f59e0b; }}
  .budget-bar-fill.bf-over {{ background: #dc2626; }}

  /* Diagnose event rendering on task detail */
  .diagnose-card {{ background: var(--surface); border: 1px solid var(--border);
                    border-radius: 6px; padding: 0.7rem 0.85rem;
                    margin: 0.5rem 0; }}
  .diagnose-card .label {{ font-size: 0.72rem; color: var(--muted);
                            text-transform: uppercase; letter-spacing: 0.04em;
                            font-weight: 600; }}
  .diagnose-card .hypothesis {{ font-size: 0.9rem; margin: 0.3rem 0 0.5rem;
                                 line-height: 1.5; }}
  .diagnose-card .confidence-bar {{ height: 0.32rem; background: var(--border);
                                     border-radius: 999px; overflow: hidden;
                                     max-width: 12rem; margin-top: 0.2rem; }}
  .diagnose-card .confidence-fill {{ height: 100%; background: var(--accent); }}
  .diagnose-card pre.patch-json {{ font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
                                    font-size: 0.78rem; margin: 0.3rem 0 0;
                                    padding: 0.4rem 0.5rem; background: var(--bg);
                                    border: 1px solid var(--border); border-radius: 4px;
                                    overflow-x: auto; white-space: pre-wrap;
                                    word-break: break-word; }}

  /* Patch badge variants — share base .badge styling */
  .b-candidate   {{ background: #fef3c7; color: #92400e; }}
  .b-canonical   {{ background: #d1fae5; color: #065f46; }}
  .b-retired     {{ background: #e5e7eb; color: #4b5563; }}

  /* Transformer preview — wrap long JSON */
  .xform {{ font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
            font-size: 0.78rem; color: #374151;
            max-width: 28rem; overflow-wrap: anywhere; word-break: break-word;
            white-space: pre-wrap; }}

  /* DLQ replay button */
  .replay-btn {{ font-family: inherit; font-size: 0.78rem;
    padding: 0.22rem 0.7rem; border: 1px solid var(--border);
    background: var(--bg); color: var(--accent); border-radius: 4px;
    cursor: pointer; font-weight: 500;
  }}
  .replay-btn:hover {{ background: var(--surface); border-color: var(--accent); }}
  .replayed-row {{ opacity: 0.65; }}
  .replayed-row em {{ font-style: italic; color: var(--muted); }}

  /* Narrow viewport tweaks */
  @media (max-width: 640px) {{
    body {{ margin: 1rem auto; padding: 0 0.7rem; }}
    h1 {{ font-size: 1.25rem; }}
    .depth-row .cell {{ min-width: 5rem; font-size: 0.75rem; padding: 0.45rem 0.6rem; }}
    .depth-row .n {{ font-size: 1.15rem; }}
  }}
</style>
</head>
<body>
<nav><a href="/">Dashboard</a><a href="/dlq">DLQ</a><a href="/patches">Patches</a></nav>
<h1>{title}</h1>
{body}
</body>
</html>"""


def _badge(status: str) -> str:
    return f'<span class="badge b-{status}">{status}</span>'


def _render_depth(depth: dict[str, int]) -> str:
    cells = "".join(
        f'<div class="cell">{k}<span class="n">{depth[k]}</span></div>'
        for k in STATUS_KEYS
    )
    return f'<div class="depth-row">{cells}</div>'


def _render_recent_tasks(tasks: list[dict[str, Any]]) -> str:
    if not tasks:
        return (
            '<div class="empty"><strong>No tasks yet.</strong> Post one with '
            '<code>Queue(&quot;…&quot;).post(...)</code>, or run '
            '<code>python -m bounty_board.demo --db demo.bounty.db</code> to seed '
            'a realistic demo queue.</div>'
        )
    rows = "".join(
        f"<tr><td><a href='/tasks/{t['id']}'><code>{t['id'][:12]}</code></a></td>"
        f"<td><code>{t['task_type']}</code></td>"
        f"<td><code>{t['payload_signature']}</code></td>"
        f"<td>{_badge(t['status'])}</td>"
        f"<td>{t['attempts']}</td>"
        f"<td>{t['claimed_by'] or '—'}</td></tr>"
        for t in tasks
    )
    return (
        '<div class="table-wrap">'
        "<table><thead><tr><th>id</th><th>type</th><th>signature</th>"
        "<th>status</th><th>attempts</th><th>claimed_by</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


INTERVENTION_KINDS = ("inject_hint", "swap_model", "pause", "cancel")


def _render_interventions(interventions: list[dict[str, Any]]) -> str:
    """Render the interventions table (a partial — no layout chrome).

    Returned as the HTMX swap target on form submit. Empty state has
    its own helpful copy.
    """
    if not interventions:
        return (
            '<div class="empty"><strong>No interventions posted.</strong> '
            "Supervising agents (or operators) can post intervention rows "
            "for this task; the working agent voluntarily honors them at "
            "tool-call safe boundaries.</div>"
        )
    irows = "".join(
        f"<tr><td><code>{i['kind']}</code></td>"
        f"<td>{i['posted_by_agent_id']}</td>"
        f"<td>{_badge('done') if i['honored_at'] else _badge('queued')}"
        f"{' honored' if i['honored_at'] else ' pending'}</td></tr>"
        for i in interventions
    )
    return (
        '<div class="table-wrap">'
        "<table><thead><tr><th>kind</th><th>posted by</th>"
        "<th>state</th></tr></thead>"
        f"<tbody>{irows}</tbody></table></div>"
    )


def _render_intervene_form(task_id: str) -> str:
    """Render the form that posts an intervention for ``task_id``.

    Uses ``hx-post`` so submission swaps the interventions list in
    place instead of full-page-reloading. The ``hint`` field is only
    meaningful when ``kind == 'inject_hint'`` but is unconditionally
    sent — the API tolerates extra context.
    """
    options = "".join(
        f'<option value="{k}">{k}</option>' for k in INTERVENTION_KINDS
    )
    return (
        f'<form class="intervene-form" '
        f'hx-post="/_partial/task/{task_id}/interventions" '
        f'hx-target="#interventions-list" hx-swap="innerHTML" '
        f'hx-on::after-request="if(event.detail.successful) this.reset()">'
        '<div class="row">'
        '<label for="iv-kind">Kind</label>'
        f'<select id="iv-kind" name="kind" required>{options}</select>'
        '<label for="iv-agent">Posted by</label>'
        '<input id="iv-agent" type="text" name="posted_by_agent_id" '
        'placeholder="supervisor_agent" required>'
        '</div>'
        '<div class="row">'
        '<label for="iv-hint">Hint (for inject_hint)</label>'
        '<input id="iv-hint" type="text" name="hint" '
        'placeholder="check edge case X" style="flex:1;min-width:18rem">'
        '<button type="submit">Post intervention</button>'
        '</div>'
        '<div class="hint">Working agent honors voluntarily at the next '
        'tool-call safe boundary. Voluntary-honor preserves the forensic '
        'trajectory; the substrate records both <code>posted_at</code> and '
        '<code>honored_at</code>.</div>'
        '</form>'
    )


def _render_budget(budget: dict[str, Any]) -> str:
    """Compact one-line panel showing budget config + current spend."""
    if not budget["configured"]:
        return (
            '<div class="budget-panel">'
            '<span class="label">Budget</span>'
            '<span class="value" style="color: var(--muted)">not configured</span>'
            '<span class="label" style="margin-left: auto">'
            "Set via <code>queue.set_budget(...)</code> "
            "or <code>_meta</code> rows starting <code>budget.</code>"
            "</span>"
            '</div>'
        )
    pct = budget["pct_used"]
    bar_cls = "bf-warn" if pct >= 80 else ""
    if pct >= 100:
        bar_cls = "bf-over"
    width = min(100.0, pct)
    window = budget["window_seconds"]
    window_txt = (
        f"rolling {window:g}s window" if window else "lifetime"
    )
    return (
        '<div class="budget-panel">'
        f'<div><span class="label">Spend</span> '
        f'<span class="value">{budget["current_spend"]:,}</span></div>'
        f'<div><span class="label">Limit</span> '
        f'<span class="value">{budget["limit_tokens"]:,}</span></div>'
        f'<div><span class="label">Policy</span> '
        f'<span class="value">{budget["policy"] or "soft"}</span></div>'
        f'<div><span class="label">{window_txt}</span></div>'
        f'<div class="budget-bar">'
        f'<div class="budget-bar-fill {bar_cls}" style="width: {width}%"></div>'
        f'</div>'
        f'<div><span class="value">{pct:.1f}%</span></div>'
        '</div>'
    )


def _render_diagnose_event(event: dict[str, Any]) -> str | None:
    """If event_kind=='diagnose', render the structured payload nicely.

    Returns None for non-diagnose events (caller falls back to generic
    row rendering). The diagnose payload shape per design lane:
        {hypothesis: str, proposed_patch: dict|null, confidence: 0.0-1.0}
    """
    if event.get("event_kind") != "diagnose":
        return None
    payload_raw = event.get("payload_json")
    if not payload_raw:
        return None
    try:
        payload = json.loads(payload_raw)
    except (json.JSONDecodeError, TypeError):
        return None

    hypothesis = payload.get("hypothesis", "")
    confidence = payload.get("confidence")
    patch = payload.get("proposed_patch")
    agent_id = event.get("agent_id", "—")

    conf_html = ""
    if isinstance(confidence, int | float):
        pct = max(0.0, min(1.0, float(confidence))) * 100
        conf_html = (
            f'<div><span class="label">Confidence</span> '
            f'<span class="value">{pct:.0f}%</span></div>'
            f'<div class="confidence-bar">'
            f'<div class="confidence-fill" style="width: {pct}%"></div>'
            f'</div>'
        )

    patch_html = ""
    if patch is not None:
        try:
            patch_json = json.dumps(patch, indent=2)
        except (TypeError, ValueError):
            patch_json = str(patch)
        patch_html = (
            '<div><span class="label">Proposed patch</span></div>'
            f'<pre class="patch-json">{patch_json}</pre>'
        )

    return (
        '<div class="diagnose-card">'
        f'<div><span class="label">Diagnose</span> '
        f'<span style="font-size: 0.78rem; color: var(--muted);"> by {agent_id}</span></div>'
        f'<div class="hypothesis">{hypothesis}</div>'
        f'{conf_html}'
        f'{patch_html}'
        '</div>'
    )


def _render_events(events: list[dict[str, Any]]) -> str:
    if not events:
        return (
            '<div class="empty"><strong>No events yet.</strong> Events will '
            'stream in here as agents claim, complete, fail, or decline tasks. '
            'Refreshes every 2 seconds.</div>'
        )
    rows = "".join(
        f"<tr><td>{e['id']}</td>"
        f"<td><a href='/tasks/{e['task_id']}'><code>{e['task_id'][:12]}</code></a></td>"
        f"<td><code>{e['event_kind']}</code></td>"
        f"<td>{e['agent_id'] or '—'}</td>"
        f"<td>{e['token_count']}</td></tr>"
        for e in events
    )
    return (
        '<div class="table-wrap">'
        "<table><thead><tr><th>event id</th><th>task</th><th>kind</th>"
        "<th>agent</th><th>tokens</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(db_path: str | Path) -> FastAPI:
    """Return a FastAPI app that inspects ``db_path``.

    The dashboard is intentionally per-queue; one app instance == one
    queue file. Supervising multiple queues = run multiple processes
    (each is cheap; SQLite + uvicorn is small).
    """
    db_path = Path(db_path)
    app = FastAPI(
        title="Bounty Board /inspect",
        description=(
            "Read-only dashboard over a single Bounty Board queue. "
            "Polling-cursor model; sub-second cross-agent wake is reserved "
            "for Phase Shift Engine."
        ),
        version="0.1.0",
    )

    # ---- JSON API ----

    @app.get("/api/queue/depth")
    def api_queue_depth() -> dict[str, int]:
        with _conn(db_path) as conn:
            return _queue_depth(conn)

    @app.get("/api/tasks")
    def api_tasks(limit: int = Query(25, ge=1, le=200)) -> list[dict[str, Any]]:
        with _conn(db_path) as conn:
            return _recent_tasks(conn, limit=limit)

    @app.get("/api/tasks/{task_id}")
    def api_task_detail(task_id: str) -> dict[str, Any]:
        with _conn(db_path) as conn:
            data = _task_detail(conn, task_id)
            if data is None:
                raise HTTPException(status_code=404, detail=f"task {task_id} not found")
            return data

    @app.get("/api/events")
    def api_events(
        since: int = Query(0, ge=0, description="cursor: returns events with id > since"),
        limit: int = Query(100, ge=1, le=500),
        event_kind: str | None = Query(None),
    ) -> list[dict[str, Any]]:
        with _conn(db_path) as conn:
            return _events_since(conn, since_id=since, limit=limit, event_kind=event_kind)

    @app.get("/api/dlq")
    def api_dlq(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
        with _conn(db_path) as conn:
            return _dlq_tasks(conn, limit=limit)

    @app.post("/api/dlq/replay/{task_id}")
    def api_dlq_replay(task_id: str) -> dict[str, str]:
        """Replay a DLQ entry as a fresh task with parent_id linkage.

        Returns the new task_id. The original failed task stays in its
        'failed' status as the audit trail anchor.
        """
        from bounty_board.dlq import DLQ
        from bounty_board.queue import Queue

        q = Queue(db_path)
        try:
            try:
                new_id = DLQ(q).replay(task_id)
            except ValueError as e:
                # ValueError signals "task missing or not in DLQ status."
                # Disambiguate for the HTTP status code.
                row = q._conn.execute(
                    "SELECT id FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    raise HTTPException(
                        status_code=404, detail=f"task {task_id} not found"
                    ) from e
                raise HTTPException(status_code=400, detail=str(e)) from e
        finally:
            q.close()
        return {"replayed_from": task_id, "new_task_id": new_id}

    @app.post("/api/interventions")
    def api_post_intervention(payload: dict[str, Any]) -> dict[str, int]:
        task_id = payload.get("task_id")
        kind = payload.get("kind")
        agent_id = payload.get("posted_by_agent_id")
        if not (task_id and kind and agent_id):
            raise HTTPException(
                status_code=400,
                detail="task_id, kind, and posted_by_agent_id are required",
            )
        with _conn(db_path) as conn:
            # Verify task exists for a useful 404.
            task = conn.execute(
                "SELECT id FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise HTTPException(status_code=404, detail=f"task {task_id} not found")

            iid = _post_intervention(
                conn,
                task_id=task_id,
                kind=kind,
                payload_json=payload.get("payload_json"),
                posted_by_agent_id=agent_id,
            )
        return {"id": iid}

    # ---- HTML routes ----

    @app.get("/", response_class=HTMLResponse)
    def html_dashboard() -> str:
        with _conn(db_path) as conn:
            depth = _queue_depth(conn)
            budget = _budget_state(conn)
            tasks = _recent_tasks(conn, limit=15)
            events = _events_since(conn, since_id=0, limit=20)
        # Reverse events so newest-on-top in the dashboard
        events = list(reversed(events))
        body = (
            "<h2>Queue depth</h2>"
            f'<div hx-get="/_partial/depth" hx-trigger="every 2s" '
            f'hx-swap="innerHTML">{_render_depth(depth)}</div>'
            "<h2>Token budget</h2>"
            f'<div hx-get="/_partial/budget" hx-trigger="every 2s" '
            f'hx-swap="innerHTML">{_render_budget(budget)}</div>'
            "<h2>Recent tasks</h2>"
            f"{_render_recent_tasks(tasks)}"
            "<h2>Recent events</h2>"
            f'<div hx-get="/_partial/events" hx-trigger="every 2s" '
            f'hx-swap="innerHTML">{_render_events(events)}</div>'
        )
        return _layout("Dashboard", body)

    @app.get("/_partial/depth", response_class=HTMLResponse)
    def html_partial_depth() -> str:
        with _conn(db_path) as conn:
            return _render_depth(_queue_depth(conn))

    @app.get("/_partial/budget", response_class=HTMLResponse)
    def html_partial_budget() -> str:
        with _conn(db_path) as conn:
            return _render_budget(_budget_state(conn))

    @app.get("/api/budget")
    def api_budget() -> dict[str, Any]:
        with _conn(db_path) as conn:
            return _budget_state(conn)

    @app.get("/_partial/events", response_class=HTMLResponse)
    def html_partial_events() -> str:
        with _conn(db_path) as conn:
            events = _events_since(conn, since_id=0, limit=20)
        return _render_events(list(reversed(events)))

    @app.get("/tasks/{task_id}", response_class=HTMLResponse)
    def html_task_detail(task_id: str) -> str:
        with _conn(db_path) as conn:
            data = _task_detail(conn, task_id)
        if data is None:
            return _layout(
                "Task not found",
                f'<div class="empty"><strong>Task <code>{task_id}</code> '
                f"not found.</strong> It may have been vacuumed, or the id "
                f"may be a typo.</div>",
            )
        task = data["task"]
        events_html = _render_events(data["events"])

        # Surface diagnose events as rich cards (above the ledger) so
        # the operator sees the agent's self-diagnosis immediately,
        # not buried in the generic event row.
        diagnose_cards = []
        for ev in data["events"]:
            rendered = _render_diagnose_event(ev)
            if rendered is not None:
                diagnose_cards.append(rendered)
        diagnose_html = (
            f"<h2>Self-diagnosis</h2>{''.join(diagnose_cards)}"
            if diagnose_cards else ""
        )

        interventions_html = _render_interventions(data["interventions"])
        form_html = _render_intervene_form(task["id"])
        body = (
            f"<h2>Task <code>{task['id']}</code> {_badge(task['status'])}</h2>"
            f"<p><strong>Type:</strong> <code>{task['task_type']}</code> "
            f"&middot; <strong>Signature:</strong> <code>{task['payload_signature']}</code> "
            f"&middot; <strong>Attempts:</strong> {task['attempts']} / "
            f"{task['max_attempts']}</p>"
            f"{diagnose_html}"
            "<h2>Event ledger</h2>"
            f"{events_html}"
            "<h2>Interventions</h2>"
            f'<div id="interventions-list">{interventions_html}</div>'
            f"{form_html}"
        )
        return _layout(f"Task {task_id}", body)

    @app.post("/_partial/task/{task_id}/interventions", response_class=HTMLResponse)
    def html_partial_post_intervention(
        task_id: str,
        kind: str = Form(...),
        posted_by_agent_id: str = Form(...),
        hint: str | None = Form(None),
    ) -> str:
        """Form-encoded intervention POST that returns the refreshed
        interventions table fragment. Used by the inline form on the
        task-detail page via HTMX.

        If ``kind == 'inject_hint'`` and a ``hint`` is provided, we
        wrap it as ``payload_json={"hint": "..."}`` for the substrate
        row. Otherwise ``payload_json`` is null.
        """
        if kind not in INTERVENTION_KINDS:
            raise HTTPException(
                status_code=400,
                detail=f"kind must be one of {INTERVENTION_KINDS}",
            )
        with _conn(db_path) as conn:
            task = conn.execute(
                "SELECT id FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise HTTPException(status_code=404, detail=f"task {task_id} not found")

            payload_json: str | None = None
            if kind == "inject_hint" and hint:
                import json as _json
                payload_json = _json.dumps({"hint": hint})

            _post_intervention(
                conn,
                task_id=task_id,
                kind=kind,
                payload_json=payload_json,
                posted_by_agent_id=posted_by_agent_id,
            )
            cur = conn.execute(
                "SELECT id, kind, payload_json, posted_by_agent_id, posted_at, honored_at "
                "FROM interventions WHERE task_id = ? ORDER BY posted_at ASC",
                (task_id,),
            )
            interventions = _rows_to_dicts(cur, cur.fetchall())
        return _render_interventions(interventions)

    @app.get("/dlq", response_class=HTMLResponse)
    def html_dlq() -> str:
        with _conn(db_path) as conn:
            tasks = _dlq_tasks(conn, limit=50)
        if not tasks:
            body = (
                '<div class="empty"><strong>DLQ is empty.</strong> No tasks '
                "have failed beyond their <code>max_attempts</code> or been "
                "marked <code>unclaimable</code> yet. When they do, full "
                "forensic state (stack trace, prompt payload at failure, "
                "token count) lands in <code>task_events</code> and surfaces "
                "here — click through to a row to see the time-travel ledger."
                "</div>"
            )
        else:
            rows = "".join(
                f"<tr id='dlq-row-{t['id']}'>"
                f"<td><a href='/tasks/{t['id']}'><code>{t['id'][:12]}</code></a></td>"
                f"<td><code>{t['task_type']}</code></td>"
                f"<td>{_badge(t['status'])}</td>"
                f"<td>{t['attempts']}</td>"
                f"<td>{t['claimed_by'] or '—'}</td>"
                f"<td>"
                f'<button class="replay-btn" '
                f'hx-post="/_partial/dlq/replay/{t["id"]}" '
                f'hx-target="#dlq-row-{t["id"]}" hx-swap="outerHTML" '
                f"hx-confirm=\"Replay this task? A fresh task will be queued "
                f"with parent_id pointing back at this one.\">"
                f"Replay</button>"
                f"</td>"
                f"</tr>"
                for t in tasks
            )
            body = (
                '<div class="table-wrap">'
                "<table><thead><tr><th>id</th><th>type</th>"
                "<th>status</th><th>attempts</th><th>last claimer</th>"
                "<th></th></tr></thead>"
                f"<tbody>{rows}</tbody></table></div>"
            )
        return _layout("DLQ", body)

    @app.post("/_partial/dlq/replay/{task_id}", response_class=HTMLResponse)
    def html_partial_dlq_replay(task_id: str) -> str:
        """Form-friendly DLQ replay. Returns a single replacement <tr>
        for HTMX to swap into the DLQ table, showing the original row
        muted-out with the new task_id linked.
        """
        from bounty_board.dlq import DLQ
        from bounty_board.queue import Queue

        q = Queue(db_path)
        try:
            try:
                new_id = DLQ(q).replay(task_id)
            except ValueError as e:
                row = q._conn.execute(
                    "SELECT id FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    raise HTTPException(
                        status_code=404, detail=f"task {task_id} not found"
                    ) from e
                raise HTTPException(status_code=400, detail=str(e)) from e
        finally:
            q.close()

        return (
            f"<tr id='dlq-row-{task_id}' class='replayed-row'>"
            f"<td><a href='/tasks/{task_id}'><code>{task_id[:12]}</code></a></td>"
            f"<td colspan='4'>"
            f"<em>Replayed → <a href='/tasks/{new_id}'><code>{new_id[:12]}</code></a></em>"
            f"</td>"
            f"<td></td>"
            f"</tr>"
        )

    @app.get("/api/patches")
    def api_patches(
        payload_signature: str | None = Query(None),
        status: str | None = Query(None),
        limit: int = Query(100, ge=1, le=500),
    ) -> list[dict[str, Any]]:
        with _conn(db_path) as conn:
            return _patches(
                conn,
                payload_signature=payload_signature,
                status=status,
                limit=limit,
            )

    @app.get("/patches", response_class=HTMLResponse)
    def html_patches(
        signature: str | None = Query(None),
        status: str | None = Query(None),
    ) -> str:
        with _conn(db_path) as conn:
            patches = _patches(
                conn,
                payload_signature=signature,
                status=status,
                limit=100,
            )

        # Inline filter form (GET to /patches with query params).
        filter_form = (
            '<form class="intervene-form" method="get" action="/patches">'
            '<div class="row">'
            '<label for="p-sig">Signature</label>'
            f'<input id="p-sig" type="text" name="signature" '
            f'value="{signature or ""}" placeholder="e.g. summarize_pr">'
            '<label for="p-status">Status</label>'
            '<select id="p-status" name="status">'
            '<option value="">(any)</option>'
            f'<option value="candidate"'
            f'{" selected" if status == "candidate" else ""}>candidate</option>'
            f'<option value="canonical"'
            f'{" selected" if status == "canonical" else ""}>canonical</option>'
            f'<option value="retired"'
            f'{" selected" if status == "retired" else ""}>retired</option>'
            '</select>'
            '<button type="submit">Filter</button>'
            '</div>'
            '<div class="hint">'
            "<strong>Candidate</strong> patches apply only to the proposing "
            "agent's own replays. At <code>n_successes &ge; 3</code> they "
            "auto-promote to <strong>canonical</strong> and apply to every "
            "agent on this signature. <strong>Retired</strong> patches were "
            "superseded or proven harmful and are no longer applied."
            "</div>"
            "</form>"
        )

        if not patches:
            list_html = (
                '<div class="empty"><strong>No patches yet.</strong> Patches '
                "are proposed by agents during self-diagnostic replay "
                "(Option D). When an agent fails and reads its own "
                "trajectory, the diagnose surface emits a "
                "<code>proposed_patch</code>; that becomes a candidate row "
                "here. Successful replays auto-promote candidates to "
                "canonical."
                "</div>"
            )
        else:
            rows = ""
            for p in patches:
                xform_pretty = p["transformer_json"]
                try:
                    xform_pretty = json.dumps(
                        json.loads(p["transformer_json"]), indent=2,
                    )
                except (json.JSONDecodeError, TypeError):
                    pass
                rows += (
                    "<tr>"
                    f"<td>{p['id']}</td>"
                    f"<td><code>{p['payload_signature']}</code></td>"
                    f"<td>{_badge(p['status'])}</td>"
                    f"<td><div class='xform'>{xform_pretty}</div></td>"
                    f"<td>{p['n_successes']}</td>"
                    f"<td>{p['n_failures']}</td>"
                    f"<td>{p['proposed_by_agent_id']}</td>"
                    f"</tr>"
                )
            list_html = (
                '<div class="table-wrap">'
                "<table><thead><tr>"
                "<th>id</th><th>signature</th><th>status</th>"
                "<th>transformer</th><th>successes</th><th>failures</th>"
                "<th>proposed by</th>"
                "</tr></thead>"
                f"<tbody>{rows}</tbody></table></div>"
            )

        body = f"{filter_form}<h2>Patches</h2>{list_html}"
        return _layout("Patches", body)

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        with _conn(db_path) as conn:
            row = conn.execute(
                "SELECT value FROM _meta WHERE key='schema_version'"
            ).fetchone()
            schema_version = int(row[0]) if row else 0
        return JSONResponse({"ok": True, "schema_version": schema_version})

    return app
