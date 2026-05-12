"""Tests for ``bounty_board.inspect`` — the /inspect dashboard substrate.

The dashboard is read-only against the V1 schema. Tests pre-populate
``tasks`` + ``task_events`` + ``interventions`` via raw SQL (no claim-path
code needed) and verify each endpoint surfaces the rows correctly.

This is intentional substrate-discipline: /inspect is a view, not a
participant. It works against the schema regardless of which code path
wrote the rows — Mac/B's claim-path PRs will populate the same rows my
tests fixture sets up, and the dashboard will render identically.
"""
from __future__ import annotations

import json
import sys
import time

import pytest

# /inspect uses PEP 604 union syntax (`str | None`) in FastAPI route
# signatures; pydantic/FastAPI evaluate those at runtime. On Python
# <3.10 the runtime eval raises TypeError at collection time, surfacing
# as a wall of confusing pytest errors. pyproject.toml pins
# requires-python = >=3.11, but give contributors on older Pythons a
# clean single-line skip with a clear message rather than the wall.
if sys.version_info < (3, 10):  # noqa: UP036 — defensive against contributors on older Python
    pytest.skip(
        "bounty_board.inspect requires Python 3.10+ for PEP 604 union "
        "syntax (`str | None`) used in FastAPI route signatures. "
        "pyproject.toml requires-python = '>=3.11'.",
        allow_module_level=True,
    )

# fastapi is an optional dependency; skip cleanly when not installed.
fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from bounty_board._meta import open_db  # noqa: E402
from bounty_board.inspect import create_app  # noqa: E402


def _seed_task(conn, task_id: str, **overrides):
    defaults = {
        "payload_json": '{"prompt": "hello"}',
        "task_type": "echo",
        "payload_signature": "sig:echo:v1",
        "priority": 0,
        "status": "queued",
        "claimed_by": None,
        "claimed_at": None,
        "completed_at": None,
        "attempts": 0,
        "max_attempts": 3,
        "parent_id": None,
        "created_at": time.time(),
    }
    defaults.update(overrides)
    conn.execute(
        "INSERT INTO tasks (id, payload_json, task_type, payload_signature, "
        "priority, status, claimed_by, claimed_at, completed_at, attempts, "
        "max_attempts, parent_id, created_at) "
        "VALUES (:id, :payload_json, :task_type, :payload_signature, :priority, "
        ":status, :claimed_by, :claimed_at, :completed_at, :attempts, "
        ":max_attempts, :parent_id, :created_at)",
        {"id": task_id, **defaults},
    )
    conn.commit()


def _seed_event(conn, task_id: str, kind: str, **overrides):
    defaults = {
        "ts": time.time(),
        "agent_id": "Win/Claude",
        "payload_json": None,
        "token_count": 0,
    }
    defaults.update(overrides)
    conn.execute(
        "INSERT INTO task_events (task_id, event_kind, ts, agent_id, "
        "payload_json, token_count) VALUES (?, ?, ?, ?, ?, ?)",
        (
            task_id,
            kind,
            defaults["ts"],
            defaults["agent_id"],
            defaults["payload_json"],
            defaults["token_count"],
        ),
    )
    conn.commit()


@pytest.fixture
def client(tmp_path):
    """Fresh queue + FastAPI TestClient per test."""
    db_path = tmp_path / "v.bounty.db"
    # Force schema migration to run.
    conn = open_db(db_path)
    conn.close()
    app = create_app(db_path)
    with TestClient(app) as c:
        # Expose the db path for tests that need to seed directly.
        c.db_path = db_path
        yield c


# ---------------------------------------------------------------------------
# JSON API
# ---------------------------------------------------------------------------


def test_queue_depth_empty(client):
    r = client.get("/api/queue/depth")
    assert r.status_code == 200
    assert r.json() == {
        "queued": 0,
        "claimed": 0,
        "processing": 0,
        "done": 0,
        "failed": 0,
        "unclaimable": 0,
    }


def test_queue_depth_counts_by_status(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t1", status="queued")
    _seed_task(conn, "t2", status="queued")
    _seed_task(conn, "t3", status="done")
    _seed_task(conn, "t4", status="failed")
    conn.close()

    r = client.get("/api/queue/depth")
    body = r.json()
    assert body["queued"] == 2
    assert body["done"] == 1
    assert body["failed"] == 1
    assert body["processing"] == 0


def test_recent_tasks_returns_newest_first(client):
    conn = open_db(client.db_path)
    now = time.time()
    _seed_task(conn, "older", created_at=now - 100)
    _seed_task(conn, "newer", created_at=now)
    conn.close()

    r = client.get("/api/tasks?limit=10")
    assert r.status_code == 200
    ids = [t["id"] for t in r.json()]
    assert ids == ["newer", "older"]


def test_task_detail_404_when_missing(client):
    r = client.get("/api/tasks/nope")
    assert r.status_code == 404


def test_task_detail_returns_events_and_interventions(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t1", status="failed", attempts=2)
    _seed_event(conn, "t1", "claim")
    _seed_event(conn, "t1", "fail", payload_json='{"stack": "Traceback..."}')
    # Direct insert into interventions to exercise the read path
    conn.execute(
        "INSERT INTO interventions (task_id, kind, payload_json, "
        "posted_by_agent_id, posted_at) VALUES (?, ?, ?, ?, ?)",
        ("t1", "inject_hint", '{"hint": "X"}', "Win/Claude", time.time()),
    )
    conn.commit()
    conn.close()

    r = client.get("/api/tasks/t1")
    assert r.status_code == 200
    body = r.json()
    assert body["task"]["id"] == "t1"
    assert body["task"]["status"] == "failed"
    assert len(body["events"]) == 2
    assert [e["event_kind"] for e in body["events"]] == ["claim", "fail"]
    assert len(body["interventions"]) == 1
    assert body["interventions"][0]["kind"] == "inject_hint"


def test_events_polling_cursor_filters_by_since(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t1")
    _seed_event(conn, "t1", "claim")
    _seed_event(conn, "t1", "process_step")
    _seed_event(conn, "t1", "complete")
    conn.close()

    r1 = client.get("/api/events?since=0")
    events = r1.json()
    assert len(events) == 3
    last_id = events[-1]["id"]

    # Polling-cursor: no new events since last cursor
    r2 = client.get(f"/api/events?since={last_id}")
    assert r2.json() == []

    # Insert one more, cursor moves
    conn = open_db(client.db_path)
    _seed_event(conn, "t1", "diagnose", payload_json='{"hypothesis": "X"}')
    conn.close()

    r3 = client.get(f"/api/events?since={last_id}")
    assert len(r3.json()) == 1
    assert r3.json()[0]["event_kind"] == "diagnose"


def test_events_filter_by_kind(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t1")
    _seed_event(conn, "t1", "claim")
    _seed_event(conn, "t1", "diagnose")
    _seed_event(conn, "t1", "claim")
    conn.close()

    r = client.get("/api/events?event_kind=diagnose")
    body = r.json()
    assert len(body) == 1
    assert body[0]["event_kind"] == "diagnose"


def test_dlq_returns_failed_and_unclaimable(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t1", status="failed", completed_at=time.time())
    _seed_task(conn, "t2", status="unclaimable", completed_at=time.time())
    _seed_task(conn, "t3", status="done", completed_at=time.time())
    _seed_task(conn, "t4", status="queued")
    conn.close()

    r = client.get("/api/dlq")
    ids = {t["id"] for t in r.json()}
    assert ids == {"t1", "t2"}


def test_post_intervention_writes_row(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t1", status="processing")
    conn.close()

    r = client.post(
        "/api/interventions",
        json={
            "task_id": "t1",
            "kind": "inject_hint",
            "payload_json": '{"hint": "check edge case Y"}',
            "posted_by_agent_id": "Win/Claude",
        },
    )
    assert r.status_code == 200, r.text
    iid = r.json()["id"]
    assert iid >= 1

    # Verify the row landed
    conn = open_db(client.db_path)
    row = conn.execute(
        "SELECT task_id, kind, posted_by_agent_id FROM interventions WHERE id=?",
        (iid,),
    ).fetchone()
    assert row == ("t1", "inject_hint", "Win/Claude")
    conn.close()


def test_post_intervention_404_when_task_missing(client):
    r = client.post(
        "/api/interventions",
        json={
            "task_id": "nope",
            "kind": "cancel",
            "posted_by_agent_id": "Win/Claude",
        },
    )
    assert r.status_code == 404


def test_post_intervention_400_when_required_fields_missing(client):
    r = client.post("/api/interventions", json={"task_id": "t1"})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# HTML routes
# ---------------------------------------------------------------------------


def test_dashboard_renders_empty(client):
    r = client.get("/")
    assert r.status_code == 200
    body = r.text
    assert "Dashboard" in body
    assert "Queue depth" in body
    # HTMX polling endpoints are wired into the html
    assert 'hx-get="/_partial/depth"' in body
    assert 'hx-get="/_partial/events"' in body


def test_dashboard_renders_seeded_data(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t-render", status="claimed", claimed_by="Mac/Claude-B")
    _seed_event(conn, "t-render", "claim", agent_id="Mac/Claude-B")
    conn.close()

    r = client.get("/")
    body = r.text
    assert "t-render" in body
    assert "Mac/Claude-B" in body
    assert "claimed" in body


def test_partial_depth_returns_fragment_not_layout(client):
    r = client.get("/_partial/depth")
    assert r.status_code == 200
    # Fragment should NOT include the layout chrome
    assert "<!doctype html>" not in r.text.lower()
    assert "depth-row" in r.text


def test_task_detail_html_renders(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t-detail", status="failed")
    _seed_event(conn, "t-detail", "fail", payload_json=json.dumps({"stack": "Boom"}))
    conn.close()

    r = client.get("/tasks/t-detail")
    body = r.text
    assert "t-detail" in body
    assert "Event ledger" in body
    assert "fail" in body
    assert "failed" in body


def test_dlq_html_empty(client):
    r = client.get("/dlq")
    body = r.text
    assert "DLQ is empty" in body


def test_dlq_html_renders_failed_tasks(client):
    conn = open_db(client.db_path)
    _seed_task(conn, "t-dlq", status="failed", attempts=3, completed_at=time.time())
    conn.close()

    r = client.get("/dlq")
    body = r.text
    assert "t-dlq" in body
    assert "failed" in body


def test_healthz_reports_schema_version(client):
    r = client.get("/healthz")
    body = r.json()
    assert body["ok"] is True
    assert body["schema_version"] == 1


# ---------------------------------------------------------------------------
# Polish: responsive layout, empty-state UX, animation classes
# ---------------------------------------------------------------------------


def test_layout_has_responsive_viewport(client):
    """Mobile-responsive layout — viewport meta is set."""
    r = client.get("/")
    body = r.text
    assert 'name="viewport"' in body
    assert "width=device-width" in body


def test_layout_includes_htmx_swap_animation(client):
    """CSS keyframe for fade-in on HTMX swaps is in the layout."""
    r = client.get("/")
    body = r.text
    assert "@keyframes htmx-swap-fade" in body


def test_tables_wrapped_in_horizontal_scroll(client):
    """Tables are wrapped in a `.table-wrap` div so narrow viewports
    can horizontally scroll instead of breaking layout."""
    conn = open_db(client.db_path)
    _seed_task(conn, "t-wrap")
    _seed_event(conn, "t-wrap", "claim")
    conn.close()
    r = client.get("/")
    assert 'class="table-wrap"' in r.text


def test_dashboard_empty_state_is_helpful_not_terse(client):
    """Empty-state messaging on the dashboard guides the contributor
    toward populating a queue (e.g. via the demo script)."""
    r = client.get("/")
    body = r.text
    # Helpful onboarding pointer present
    assert "bounty_board.demo" in body or "Queue(" in body


def test_dlq_empty_state_explains_the_dlq(client):
    """DLQ empty state isn't just 'DLQ is empty' — it explains what
    happens when failures land here so first-time viewers understand
    the substrate."""
    r = client.get("/dlq")
    body = r.text
    assert "DLQ is empty" in body
    # Educational copy about the time-travel ledger
    assert "task_events" in body or "forensic" in body


def test_task_detail_empty_interventions_explains_the_substrate(client):
    """Task detail with no interventions explains what interventions
    are + how to post one — not just 'No interventions posted.'"""
    conn = open_db(client.db_path)
    _seed_task(conn, "t-detail-no-iv")
    conn.close()
    r = client.get("/tasks/t-detail-no-iv")
    body = r.text
    assert "No interventions posted" in body
    # Pointer to the API surface for posting one
    assert "/api/interventions" in body or "POST" in body


def test_404_task_detail_renders_layout(client):
    """A 404 task detail renders the full layout (nav, css, etc.),
    not just a bare 'not found' line."""
    r = client.get("/tasks/does-not-exist")
    body = r.text
    assert r.status_code == 200  # we render as 200 with an HTML body
    assert "Dashboard" in body  # nav is present
    assert "not found" in body
    # Layout chrome present
    assert "<!doctype html>" in body.lower()
