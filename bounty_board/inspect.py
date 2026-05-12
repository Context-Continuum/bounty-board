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

from fastapi import FastAPI, HTTPException, Query
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
<title>{title} — Bounty Board</title>
<script src="{HTMX_CDN}"></script>
<style>
  body {{ font-family: -apple-system, system-ui, sans-serif; max-width: 980px;
         margin: 1.5rem auto; padding: 0 1rem; color: #1a1a1a; }}
  h1 {{ font-size: 1.5rem; margin: 0 0 1rem; }}
  h2 {{ font-size: 1.05rem; margin: 1.25rem 0 0.5rem; color: #444; }}
  nav a {{ margin-right: 1rem; text-decoration: none; color: #2a5bd7; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
  th, td {{ padding: 0.35rem 0.5rem; text-align: left; border-bottom: 1px solid #eee; }}
  th {{ font-weight: 600; color: #666; background: #fafafa; }}
  .badge {{ display: inline-block; padding: 0.1rem 0.4rem; border-radius: 3px;
           font-size: 0.75rem; font-weight: 600; }}
  .b-queued    {{ background: #eef; color: #225; }}
  .b-claimed   {{ background: #ffe9b3; color: #6b4500; }}
  .b-processing{{ background: #b3e5ff; color: #003b66; }}
  .b-done      {{ background: #c7f0c7; color: #145214; }}
  .b-failed    {{ background: #ffd0d0; color: #6b0000; }}
  .b-unclaimable {{ background: #ddd; color: #444; }}
  .depth-row {{ display: flex; gap: 0.8rem; flex-wrap: wrap; font-size: 0.9rem; }}
  .depth-row .cell {{ background: #fafafa; padding: 0.5rem 0.8rem; border-radius: 4px;
                      border: 1px solid #eee; }}
  .depth-row .n {{ font-weight: 600; font-size: 1.05rem; margin-left: 0.4rem; }}
  code {{ font-size: 0.82rem; background: #f4f4f4; padding: 0.05rem 0.3rem;
         border-radius: 2px; }}
  .empty {{ color: #888; font-style: italic; padding: 0.5rem 0; }}
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
        return '<p class="empty">No tasks yet — the queue is empty.</p>'
    rows = "".join(
        f"<tr><td><a href='/tasks/{t['id']}'><code>{t['id']}</code></a></td>"
        f"<td><code>{t['task_type']}</code></td>"
        f"<td><code>{t['payload_signature']}</code></td>"
        f"<td>{_badge(t['status'])}</td>"
        f"<td>{t['attempts']}</td>"
        f"<td>{t['claimed_by'] or '—'}</td></tr>"
        for t in tasks
    )
    return (
        "<table><thead><tr><th>id</th><th>type</th><th>signature</th>"
        "<th>status</th><th>attempts</th><th>claimed_by</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def _render_events(events: list[dict[str, Any]]) -> str:
    if not events:
        return '<p class="empty">No events yet.</p>'
    rows = "".join(
        f"<tr><td>{e['id']}</td>"
        f"<td><a href='/tasks/{e['task_id']}'><code>{e['task_id']}</code></a></td>"
        f"<td><code>{e['event_kind']}</code></td>"
        f"<td>{e['agent_id'] or '—'}</td>"
        f"<td>{e['token_count']}</td></tr>"
        for e in events
    )
    return (
        "<table><thead><tr><th>event id</th><th>task</th><th>kind</th>"
        "<th>agent</th><th>tokens</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
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
                f"<p class='empty'>Task <code>{task_id}</code> not found.</p>",
            )
        task = data["task"]
        events_html = _render_events(data["events"])
        interventions = data["interventions"]
        if interventions:
            irows = "".join(
                f"<tr><td>{i['kind']}</td><td>{i['posted_by_agent_id']}</td>"
                f"<td>{'honored' if i['honored_at'] else 'pending'}</td></tr>"
                for i in interventions
            )
            interventions_html = (
                "<table><thead><tr><th>kind</th><th>posted by</th>"
                "<th>state</th></tr></thead>"
                f"<tbody>{irows}</tbody></table>"
            )
        else:
            interventions_html = '<p class="empty">No interventions posted.</p>'
        body = (
            f"<h2>Task <code>{task['id']}</code> {_badge(task['status'])}</h2>"
            f"<p><strong>Type:</strong> <code>{task['task_type']}</code> "
            f"&middot; <strong>Signature:</strong> <code>{task['payload_signature']}</code> "
            f"&middot; <strong>Attempts:</strong> {task['attempts']} / "
            f"{task['max_attempts']}</p>"
            "<h2>Event ledger</h2>"
            f"{events_html}"
            "<h2>Interventions</h2>"
            f"{interventions_html}"
        )
        return _layout(f"Task {task_id}", body)

    @app.get("/dlq", response_class=HTMLResponse)
    def html_dlq() -> str:
        with _conn(db_path) as conn:
            tasks = _dlq_tasks(conn, limit=50)
        if not tasks:
            body = '<p class="empty">DLQ is empty.</p>'
        else:
            rows = "".join(
                f"<tr><td><a href='/tasks/{t['id']}'><code>{t['id']}</code></a></td>"
                f"<td><code>{t['task_type']}</code></td>"
                f"<td>{_badge(t['status'])}</td>"
                f"<td>{t['attempts']}</td>"
                f"<td>{t['claimed_by'] or '—'}</td></tr>"
                for t in tasks
            )
            body = (
                "<table><thead><tr><th>id</th><th>type</th>"
                "<th>status</th><th>attempts</th><th>last claimer</th>"
                "</tr></thead>"
                f"<tbody>{rows}</tbody></table>"
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
