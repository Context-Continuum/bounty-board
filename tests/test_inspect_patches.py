"""Tests for /inspect patches surface (API + HTML page).

The patches table is written by bounty_board.patches (Mac/B's #19).
/inspect just renders the rows; no new write paths here.
"""
from __future__ import annotations

import sys
import time

import pytest

if sys.version_info < (3, 10):  # noqa: UP036 — defensive for older Python
    pytest.skip(
        "bounty_board.inspect requires Python 3.10+ for PEP 604 union syntax.",
        allow_module_level=True,
    )

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from bounty_board._meta import open_db  # noqa: E402
from bounty_board.inspect import create_app  # noqa: E402


def _seed_patch(conn, **overrides):
    defaults = {
        "payload_signature": "summarize_pr",
        "transformer_json": '{"kind": "prepend_system_msg", "args": {"text": "be brief"}}',
        "status": "candidate",
        "n_successes": 0,
        "n_failures": 0,
        "proposed_by_agent_id": "agent_a",
        "proposed_at": time.time(),
        "promoted_at": None,
    }
    defaults.update(overrides)
    cur = conn.execute(
        "INSERT INTO patches "
        "(payload_signature, transformer_json, status, n_successes, n_failures, "
        "proposed_by_agent_id, proposed_at, promoted_at) "
        "VALUES (:payload_signature, :transformer_json, :status, :n_successes, "
        ":n_failures, :proposed_by_agent_id, :proposed_at, :promoted_at) "
        "RETURNING id",
        defaults,
    )
    pid = cur.fetchone()[0]
    conn.commit()
    return pid


@pytest.fixture
def client(tmp_path):
    db_path = tmp_path / "p.bounty.db"
    conn = open_db(db_path)
    conn.close()
    app = create_app(db_path)
    with TestClient(app) as c:
        c.db_path = db_path
        yield c


# ---------------------------------------------------------------------------
# JSON API
# ---------------------------------------------------------------------------


def test_api_patches_empty(client):
    r = client.get("/api/patches")
    assert r.status_code == 200
    assert r.json() == []


def test_api_patches_lists_rows(client):
    conn = open_db(client.db_path)
    _seed_patch(conn, payload_signature="alpha")
    _seed_patch(conn, payload_signature="beta", status="canonical",
                n_successes=5, promoted_at=time.time())
    conn.close()

    r = client.get("/api/patches")
    body = r.json()
    assert len(body) == 2
    sigs = {p["payload_signature"] for p in body}
    assert sigs == {"alpha", "beta"}


def test_api_patches_filter_by_signature(client):
    conn = open_db(client.db_path)
    _seed_patch(conn, payload_signature="alpha")
    _seed_patch(conn, payload_signature="beta")
    conn.close()

    r = client.get("/api/patches?payload_signature=alpha")
    body = r.json()
    assert len(body) == 1
    assert body[0]["payload_signature"] == "alpha"


def test_api_patches_filter_by_status(client):
    conn = open_db(client.db_path)
    _seed_patch(conn, status="candidate")
    _seed_patch(conn, status="canonical", n_successes=3, promoted_at=time.time())
    _seed_patch(conn, status="retired")
    conn.close()

    r = client.get("/api/patches?status=canonical")
    body = r.json()
    assert len(body) == 1
    assert body[0]["status"] == "canonical"


# ---------------------------------------------------------------------------
# HTML page
# ---------------------------------------------------------------------------


def test_html_patches_empty_state(client):
    r = client.get("/patches")
    body = r.text
    assert "No patches yet" in body
    assert "Option D" in body or "self-diagnostic" in body
    # Filter form is present
    assert 'name="signature"' in body
    assert 'name="status"' in body


def test_html_patches_renders_rows(client):
    conn = open_db(client.db_path)
    _seed_patch(conn, payload_signature="summarize_pr", status="canonical",
                n_successes=7, promoted_at=time.time())
    _seed_patch(conn, payload_signature="review_diff", status="candidate")
    conn.close()

    r = client.get("/patches")
    body = r.text
    assert "summarize_pr" in body
    assert "review_diff" in body
    assert "canonical" in body
    assert "candidate" in body
    # The transformer JSON is rendered (pretty-printed)
    assert "prepend_system_msg" in body
    # Counts surface
    assert ">7<" in body or "n_successes" in body or "7" in body


def test_html_patches_filter_form_round_trip(client):
    """Submitting the filter form reflects current values back in
    the rendered <input>/<select>."""
    conn = open_db(client.db_path)
    _seed_patch(conn, payload_signature="alpha", status="canonical",
                promoted_at=time.time())
    _seed_patch(conn, payload_signature="beta", status="candidate")
    conn.close()

    r = client.get("/patches?signature=alpha&status=canonical")
    body = r.text
    # Pre-populated form fields
    assert 'value="alpha"' in body
    # Status select has "canonical" selected
    assert 'value="canonical" selected' in body
    # Only the matching row rendered
    assert "alpha" in body
    # Beta is filtered out
    assert ">beta<" not in body


def test_html_patches_status_badge_classes(client):
    """Each status renders its badge class for proper styling."""
    conn = open_db(client.db_path)
    _seed_patch(conn, payload_signature="x", status="candidate")
    _seed_patch(conn, payload_signature="y", status="canonical",
                promoted_at=time.time())
    _seed_patch(conn, payload_signature="z", status="retired")
    conn.close()

    r = client.get("/patches")
    body = r.text
    assert "b-candidate" in body
    assert "b-canonical" in body
    assert "b-retired" in body


def test_nav_includes_patches_link(client):
    r = client.get("/")
    body = r.text
    assert 'href="/patches"' in body


def test_patches_page_has_layout_chrome(client):
    r = client.get("/patches")
    body = r.text
    assert "<!doctype html>" in body.lower()
    assert "Dashboard" in body  # nav present
