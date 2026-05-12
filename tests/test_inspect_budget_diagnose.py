"""Tests for /inspect budget panel + diagnose event rich rendering.

Budget config lives in _meta rows (budget.limit_tokens, budget.window_seconds,
budget.policy). Spend is SUM(task_events.token_count) at read time.
Diagnose events carry {hypothesis, proposed_patch, confidence} in
payload_json; /inspect renders them as cards on task-detail.
"""
from __future__ import annotations

import json
import sys
import time

import pytest

if sys.version_info < (3, 10):  # noqa: UP036
    pytest.skip("requires Python 3.10+", allow_module_level=True)

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from bounty_board._meta import open_db  # noqa: E402
from bounty_board.inspect import create_app  # noqa: E402


def _set_meta(conn, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


def _seed_task(conn, task_id: str, **overrides):
    defaults = {
        "payload_json": "{}", "task_type": "echo", "payload_signature": "sig",
        "priority": 0, "status": "queued", "claimed_by": None,
        "claimed_at": None, "completed_at": None, "attempts": 0,
        "max_attempts": 3, "parent_id": None, "created_at": time.time(),
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
    defaults = {"ts": time.time(), "agent_id": "agent_a",
                "payload_json": None, "token_count": 0}
    defaults.update(overrides)
    conn.execute(
        "INSERT INTO task_events (task_id, event_kind, ts, agent_id, "
        "payload_json, token_count) VALUES (?, ?, ?, ?, ?, ?)",
        (task_id, kind, defaults["ts"], defaults["agent_id"],
         defaults["payload_json"], defaults["token_count"]),
    )
    conn.commit()


@pytest.fixture
def client(tmp_path):
    db_path = tmp_path / "bd.bounty.db"
    conn = open_db(db_path)
    conn.close()
    app = create_app(db_path)
    with TestClient(app) as c:
        c.db_path = db_path
        yield c


# ---------------------------------------------------------------------------
# Budget — JSON API
# ---------------------------------------------------------------------------


def test_api_budget_unconfigured(client):
    r = client.get("/api/budget")
    body = r.json()
    assert body["configured"] is False
    assert body["limit_tokens"] == 0
    assert body["current_spend"] == 0


def test_api_budget_configured_no_spend(client):
    conn = open_db(client.db_path)
    _set_meta(conn, "budget.limit_tokens", "10000")
    _set_meta(conn, "budget.policy", "refuse_claim")
    conn.close()

    r = client.get("/api/budget")
    body = r.json()
    assert body["configured"] is True
    assert body["limit_tokens"] == 10000
    assert body["current_spend"] == 0
    assert body["pct_used"] == 0.0
    assert body["policy"] == "refuse_claim"


def test_api_budget_sums_token_counts(client):
    conn = open_db(client.db_path)
    _set_meta(conn, "budget.limit_tokens", "1000")
    _seed_task(conn, "t1")
    _seed_event(conn, "t1", "complete", token_count=300)
    _seed_event(conn, "t1", "complete", token_count=500)
    conn.close()

    r = client.get("/api/budget")
    body = r.json()
    assert body["current_spend"] == 800
    assert body["pct_used"] == 80.0


def test_api_budget_rolling_window(client):
    """Window-bounded budget excludes spend older than window_seconds."""
    conn = open_db(client.db_path)
    _set_meta(conn, "budget.limit_tokens", "1000")
    _set_meta(conn, "budget.window_seconds", "60")
    _seed_task(conn, "t1")
    old = time.time() - 3600  # 1h ago, way past 60s window
    fresh = time.time()
    _seed_event(conn, "t1", "complete", ts=old, token_count=500)
    _seed_event(conn, "t1", "complete", ts=fresh, token_count=200)
    conn.close()

    r = client.get("/api/budget")
    body = r.json()
    # Only the fresh 200 counts; the 500-old is outside the window
    assert body["current_spend"] == 200
    assert body["window_seconds"] == 60.0


# ---------------------------------------------------------------------------
# Budget — HTML
# ---------------------------------------------------------------------------


def test_dashboard_renders_budget_panel(client):
    r = client.get("/")
    body = r.text
    assert "Token budget" in body
    assert "budget-panel" in body
    # HTMX wire for live refresh
    assert 'hx-get="/_partial/budget"' in body


def test_budget_panel_unconfigured_state(client):
    r = client.get("/_partial/budget")
    body = r.text
    assert "not configured" in body


def test_budget_panel_renders_bar_with_pct(client):
    conn = open_db(client.db_path)
    _set_meta(conn, "budget.limit_tokens", "1000")
    _set_meta(conn, "budget.policy", "soft")
    _seed_task(conn, "t1")
    _seed_event(conn, "t1", "complete", token_count=450)
    conn.close()

    r = client.get("/_partial/budget")
    body = r.text
    assert "45.0%" in body
    assert "budget-bar-fill" in body
    assert "soft" in body
    assert "1,000" in body  # limit formatted with thousands separator


def test_budget_panel_warn_color_at_80pct(client):
    conn = open_db(client.db_path)
    _set_meta(conn, "budget.limit_tokens", "100")
    _seed_task(conn, "t1")
    _seed_event(conn, "t1", "complete", token_count=85)
    conn.close()

    r = client.get("/_partial/budget")
    body = r.text
    assert "bf-warn" in body  # warn color class


def test_budget_panel_over_color_at_100pct(client):
    conn = open_db(client.db_path)
    _set_meta(conn, "budget.limit_tokens", "100")
    _seed_task(conn, "t1")
    _seed_event(conn, "t1", "complete", token_count=120)
    conn.close()

    r = client.get("/_partial/budget")
    body = r.text
    assert "bf-over" in body


# ---------------------------------------------------------------------------
# Diagnose event rich rendering on task detail
# ---------------------------------------------------------------------------


def test_task_detail_renders_diagnose_card(client):
    """When a task has a diagnose event, render it as a rich card
    above the generic event ledger."""
    conn = open_db(client.db_path)
    _seed_task(conn, "t-diag", status="failed")
    _seed_event(conn, "t-diag", "claim", agent_id="agent_a")
    _seed_event(conn, "t-diag", "fail", agent_id="agent_a",
                payload_json='{"stack": "boom"}', token_count=100)
    diag = {
        "hypothesis": "Payload exceeded the model's context window.",
        "proposed_patch": {
            "kind": "truncate_field",
            "args": {"field": "text", "max_chars": 8000},
        },
        "confidence": 0.85,
    }
    _seed_event(conn, "t-diag", "diagnose", agent_id="agent_a",
                payload_json=json.dumps(diag))
    conn.close()

    r = client.get("/tasks/t-diag")
    body = r.text
    assert "Self-diagnosis" in body
    assert "diagnose-card" in body
    assert "Payload exceeded the model" in body
    assert "85%" in body  # confidence percentage
    assert "truncate_field" in body  # patch transformer surfaced
    assert "agent_a" in body


def test_task_detail_handles_diagnose_with_no_patch(client):
    """Diagnose with proposed_patch=null still renders hypothesis +
    confidence; just skips the patch block."""
    conn = open_db(client.db_path)
    _seed_task(conn, "t-nopat")
    _seed_event(conn, "t-nopat", "diagnose",
                payload_json=json.dumps({
                    "hypothesis": "Unknown failure mode.",
                    "proposed_patch": None,
                    "confidence": 0.2,
                }))
    conn.close()

    r = client.get("/tasks/t-nopat")
    body = r.text
    assert "Unknown failure mode" in body
    assert "20%" in body
    # No <pre class="patch-json"> tag because no patch (CSS selector
    # 'pre.patch-json' is in the layout but the rendered <pre> isn't)
    assert '<pre class="patch-json">' not in body


def test_task_detail_no_diagnose_section_when_absent(client):
    """Task with no diagnose events doesn't render the self-diagnosis
    section header at all (no empty card)."""
    conn = open_db(client.db_path)
    _seed_task(conn, "t-plain")
    _seed_event(conn, "t-plain", "claim")
    _seed_event(conn, "t-plain", "complete", token_count=42)
    conn.close()

    r = client.get("/tasks/t-plain")
    body = r.text
    assert "Self-diagnosis" not in body
    # Distinguish the rendered card from the CSS selector in <style>
    assert '<div class="diagnose-card">' not in body


def test_task_detail_renders_multiple_diagnose_cards(client):
    """A task can carry multiple diagnose events across retry cycles;
    render each as its own card."""
    conn = open_db(client.db_path)
    _seed_task(conn, "t-multi")
    _seed_event(conn, "t-multi", "diagnose",
                payload_json=json.dumps({
                    "hypothesis": "First guess: payload too long.",
                    "proposed_patch": None,
                    "confidence": 0.4,
                }))
    _seed_event(conn, "t-multi", "diagnose",
                payload_json=json.dumps({
                    "hypothesis": "Refined: missing context in system prompt.",
                    "proposed_patch": {"kind": "prepend_system_msg",
                                       "args": {"text": "..."}},
                    "confidence": 0.78,
                }))
    conn.close()

    r = client.get("/tasks/t-multi")
    body = r.text
    # Count opening tags of rendered cards (not CSS selector mentions)
    assert body.count('<div class="diagnose-card">') == 2
    assert "First guess" in body
    assert "Refined" in body
    assert "prepend_system_msg" in body
