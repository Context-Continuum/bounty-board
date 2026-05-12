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
<nav><a href="/">Dashboard</a><a href="/dlq">DLQ</a></nav>
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
            tasks = _recent_tasks(conn, limit=15)
            events = _events_since(conn, since_id=0, limit=20)
        # Reverse events so newest-on-top in the dashboard
        events = list(reversed(events))
        body = (
            "<h2>Queue depth</h2>"
            f'<div hx-get="/_partial/depth" hx-trigger="every 2s" '
            f'hx-swap="innerHTML">{_render_depth(depth)}</div>'
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
        interventions_html = _render_interventions(data["interventions"])
        form_html = _render_intervene_form(task["id"])
        body = (
            f"<h2>Task <code>{task['id']}</code> {_badge(task['status'])}</h2>"
            f"<p><strong>Type:</strong> <code>{task['task_type']}</code> "
            f"&middot; <strong>Signature:</strong> <code>{task['payload_signature']}</code> "
            f"&middot; <strong>Attempts:</strong> {task['attempts']} / "
            f"{task['max_attempts']}</p>"
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
                f"<tr><td><a href='/tasks/{t['id']}'><code>{t['id'][:12]}</code></a></td>"
                f"<td><code>{t['task_type']}</code></td>"
                f"<td>{_badge(t['status'])}</td>"
                f"<td>{t['attempts']}</td>"
                f"<td>{t['claimed_by'] or '—'}</td></tr>"
                for t in tasks
            )
            body = (
                '<div class="table-wrap">'
                "<table><thead><tr><th>id</th><th>type</th>"
                "<th>status</th><th>attempts</th><th>last claimer</th>"
                "</tr></thead>"
                f"<tbody>{rows}</tbody></table></div>"
            )
        return _layout("DLQ", body)

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        with _conn(db_path) as conn:
            row = conn.execute(
                "SELECT value FROM _meta WHERE key='schema_version'"
            ).fetchone()
            schema_version = int(row[0]) if row else 0
        return JSONResponse({"ok": True, "schema_version": schema_version})

    return app
