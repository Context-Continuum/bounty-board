"""Tests for the lifecycle CLI subcommands (post / claim / complete / fail / decline).

Each verb maps to a Queue method but goes through process-level
boundaries — argparse parses, JSON serializes/deserializes, exit codes
signal status to the shell. These tests exercise the full CLI path.
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from bounty_board._cli import main as cli_main
from bounty_board._meta import open_db


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Fresh initialized queue file per test."""
    p = tmp_path / "lc.bounty.db"
    cli_main(["init", str(p)])
    return p


# ---------------------------------------------------------------------------
# post
# ---------------------------------------------------------------------------


def test_post_creates_task_and_returns_id(db_path: Path, capfd):
    capfd.readouterr()
    rc = cli_main([
        "post", str(db_path),
        "--type", "summarize",
        "--signature", "open",
        "--payload", '{"text": "hi"}',
    ])
    assert rc == 0

    out = capfd.readouterr().out
    parsed = json.loads(out)
    assert "task_id" in parsed
    task_id = parsed["task_id"]
    assert len(task_id) > 0

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT task_type, payload_signature, payload_json, priority, max_attempts "
        "FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    conn.close()
    assert row[0] == "summarize"
    assert row[1] == "open"
    assert json.loads(row[2]) == {"text": "hi"}
    assert row[3] == 0
    assert row[4] == 3


def test_post_signature_defaults_to_task_type(db_path: Path, capfd):
    capfd.readouterr()
    cli_main([
        "post", str(db_path),
        "--type", "echo",
        "--payload", "{}",
    ])
    task_id = json.loads(capfd.readouterr().out)["task_id"]

    conn = open_db(db_path)
    sig = conn.execute(
        "SELECT payload_signature FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()[0]
    conn.close()
    assert sig == "echo"


def test_post_priority_and_max_attempts(db_path: Path, capfd):
    capfd.readouterr()
    cli_main([
        "post", str(db_path),
        "--type", "urgent",
        "--signature", "open",
        "--payload", "{}",
        "--priority", "10",
        "--max-attempts", "5",
    ])
    task_id = json.loads(capfd.readouterr().out)["task_id"]

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT priority, max_attempts FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    conn.close()
    assert row == (10, 5)


def test_post_payload_stdin(db_path: Path, monkeypatch, capfd):
    capfd.readouterr()
    monkeypatch.setattr("sys.stdin", io.StringIO('{"from": "stdin"}'))
    cli_main([
        "post", str(db_path),
        "--type", "stdinned",
        "--signature", "open",
        "--payload-stdin",
    ])
    task_id = json.loads(capfd.readouterr().out)["task_id"]

    conn = open_db(db_path)
    payload = json.loads(
        conn.execute(
            "SELECT payload_json FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()[0]
    )
    conn.close()
    assert payload == {"from": "stdin"}


def test_post_missing_db_without_create_exits_1(tmp_path: Path, capfd):
    rc = cli_main([
        "post", str(tmp_path / "nope.db"),
        "--type", "x",
        "--signature", "open",
        "--payload", "{}",
    ])
    assert rc == 1
    err = capfd.readouterr().err
    assert "does not exist" in err
    assert "--create" in err


def test_post_with_create_flag_initializes(tmp_path: Path, capfd):
    db_path = tmp_path / "lazy.bounty.db"
    rc = cli_main([
        "post", str(db_path),
        "--type", "x",
        "--signature", "open",
        "--payload", "{}",
        "--create",
    ])
    assert rc == 0
    assert db_path.exists()


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------


def test_claim_returns_task_when_available(db_path: Path, capfd):
    capfd.readouterr()
    cli_main([
        "post", str(db_path),
        "--type", "thing",
        "--signature", "open",
        "--payload", '{"k": "v"}',
    ])
    capfd.readouterr()

    rc = cli_main(["claim", str(db_path), "--agent", "agent_a"])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["claimed"] is True
    assert parsed["task_type"] == "thing"
    assert parsed["payload"] == {"k": "v"}
    assert parsed["claimed_by"] == "agent_a"


def test_claim_no_work_exit_1_with_stderr(db_path: Path, capfd):
    rc = cli_main(["claim", str(db_path), "--agent", "alone"])
    assert rc == 1
    err = capfd.readouterr().err
    assert "no claimable task" in err


def test_claim_no_work_with_json_flag(db_path: Path, capfd):
    rc = cli_main(["claim", str(db_path), "--agent", "alone", "--json"])
    assert rc == 1
    out = capfd.readouterr().out
    parsed = json.loads(out)
    assert parsed == {"claimed": False}


def test_claim_respects_earned_capability(db_path: Path, capfd):
    """A task with a non-'open' signature is NOT claimable by a fresh
    agent (no track record)."""
    capfd.readouterr()
    cli_main([
        "post", str(db_path),
        "--type", "specialized",
        "--signature", "specialized",
        "--payload", "{}",
    ])
    capfd.readouterr()

    rc = cli_main(["claim", str(db_path), "--agent", "fresh", "--json"])
    assert rc == 1
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["claimed"] is False


# ---------------------------------------------------------------------------
# complete
# ---------------------------------------------------------------------------


def _post_and_claim(db_path: Path, agent: str, capfd) -> str:
    """Helper: post an open task + claim it; return task_id."""
    capfd.readouterr()
    cli_main([
        "post", str(db_path),
        "--type", "t", "--signature", "open", "--payload", "{}",
    ])
    capfd.readouterr()
    cli_main(["claim", str(db_path), "--agent", agent])
    return json.loads(capfd.readouterr().out)["task_id"]


def test_complete_marks_task_done(db_path: Path, capfd):
    task_id = _post_and_claim(db_path, "a", capfd)
    rc = cli_main([
        "complete", str(db_path),
        "--task", task_id, "--agent", "a",
        "--result", '{"ok": true}', "--tokens", "100",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed == {"task_id": task_id, "status": "done", "agent_id": "a"}

    conn = open_db(db_path)
    status = conn.execute(
        "SELECT status FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()[0]
    n_success = conn.execute(
        "SELECT success_n FROM agent_track_record WHERE agent_id = ?",
        ("a",),
    ).fetchone()[0]
    conn.close()
    assert status == "done"
    assert n_success == 1


def test_complete_missing_task_exits_1(db_path: Path, capfd):
    rc = cli_main([
        "complete", str(db_path),
        "--task", "ghost",
        "--agent", "a",
    ])
    assert rc == 1
    err = capfd.readouterr().err
    assert "no task with id" in err


# ---------------------------------------------------------------------------
# fail
# ---------------------------------------------------------------------------


def test_fail_under_max_attempts_requeues(db_path: Path, capfd):
    task_id = _post_and_claim(db_path, "a", capfd)
    rc = cli_main([
        "fail", str(db_path),
        "--task", task_id, "--agent", "a",
        "--stack", "boom",
        "--tokens", "50",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["task_id"] == task_id
    assert parsed["status"] == "queued"  # re-queued

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT status, attempts FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()
    conn.close()
    assert row[0] == "queued"
    assert row[1] == 1


def test_fail_at_max_attempts_parks(tmp_path: Path, capfd):
    db_path = tmp_path / "fail.bounty.db"
    cli_main([
        "post", str(db_path),
        "--type", "t", "--signature", "open", "--payload", "{}",
        "--max-attempts", "1",
        "--create",
    ])
    capfd.readouterr()
    cli_main(["claim", str(db_path), "--agent", "a"])
    task_id = json.loads(capfd.readouterr().out)["task_id"]

    rc = cli_main([
        "fail", str(db_path), "--task", task_id, "--agent", "a",
        "--stack", "boom",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["status"] == "failed"


def test_fail_stack_from_stdin(db_path: Path, monkeypatch, capfd):
    task_id = _post_and_claim(db_path, "a", capfd)
    monkeypatch.setattr("sys.stdin", io.StringIO("Traceback... full stack here"))
    rc = cli_main([
        "fail", str(db_path),
        "--task", task_id, "--agent", "a",
        "--stack-stdin",
    ])
    assert rc == 0
    capfd.readouterr()  # drain stdout

    conn = open_db(db_path)
    payload = conn.execute(
        "SELECT payload_json FROM task_events WHERE task_id = ? AND event_kind = 'fail'",
        (task_id,),
    ).fetchone()[0]
    conn.close()
    parsed = json.loads(payload)
    assert "Traceback" in parsed["stack"]


# ---------------------------------------------------------------------------
# decline
# ---------------------------------------------------------------------------


def test_decline_requeues_and_bumps_decline_n(db_path: Path, capfd):
    task_id = _post_and_claim(db_path, "a", capfd)
    rc = cli_main([
        "decline", str(db_path),
        "--task", task_id, "--agent", "a",
        "--reason", "topic_outside_my_capability",
    ])
    assert rc == 0
    parsed = json.loads(capfd.readouterr().out)
    assert parsed["status"] == "queued"
    assert parsed["reason"] == "topic_outside_my_capability"

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT decline_n, fail_n, success_n FROM agent_track_record "
        "WHERE agent_id = ?",
        ("a",),
    ).fetchone()
    conn.close()
    assert row == (1, 0, 0), "decline must increment only decline_n"


def test_decline_missing_task_exits_1(db_path: Path, capfd):
    rc = cli_main([
        "decline", str(db_path),
        "--task", "ghost", "--agent", "a", "--reason", "x",
    ])
    assert rc == 1


# ---------------------------------------------------------------------------
# end-to-end lifecycle smoke
# ---------------------------------------------------------------------------


def test_end_to_end_post_claim_complete(db_path: Path, capfd):
    """The README's quickstart story, via the CLI."""
    capfd.readouterr()

    cli_main([
        "post", str(db_path),
        "--type", "summarize",
        "--signature", "open",
        "--payload", '{"text": "hello world"}',
    ])
    task_id = json.loads(capfd.readouterr().out)["task_id"]

    cli_main(["claim", str(db_path), "--agent", "agent_42"])
    claimed = json.loads(capfd.readouterr().out)
    assert claimed["task_id"] == task_id
    assert claimed["payload"] == {"text": "hello world"}

    cli_main([
        "complete", str(db_path),
        "--task", task_id, "--agent", "agent_42",
        "--result", '{"summary": "hw"}',
        "--tokens", "42",
    ])
    capfd.readouterr()

    # status reports the final state
    cli_main(["status", str(db_path), "--json"])
    status = json.loads(capfd.readouterr().out)
    assert status["depth"]["done"] == 1
    assert status["depth"]["queued"] == 0
    assert status["total_events"] >= 2  # claim + complete
    assert status["known_agents"] == 1


def test_post_claim_decline_post_claim_with_different_agent(db_path: Path, capfd):
    """Bootstrap rule (a): 'open' tasks are claimable by ANY agent.
    After agent_a declines, agent_b can pick up the same task."""
    capfd.readouterr()

    cli_main([
        "post", str(db_path),
        "--type", "t", "--signature", "open", "--payload", "{}",
    ])
    task_id = json.loads(capfd.readouterr().out)["task_id"]

    cli_main(["claim", str(db_path), "--agent", "agent_a"])
    capfd.readouterr()

    cli_main([
        "decline", str(db_path),
        "--task", task_id, "--agent", "agent_a",
        "--reason", "busy",
    ])
    capfd.readouterr()

    rc = cli_main(["claim", str(db_path), "--agent", "agent_b"])
    assert rc == 0
    claimed = json.loads(capfd.readouterr().out)
    assert claimed["task_id"] == task_id
    assert claimed["claimed_by"] == "agent_b"
    # agent_a's decline_n incremented; agent_b should now be the claimant
    conn = open_db(db_path)
    tr = dict(
        conn.execute(
            "SELECT agent_id, decline_n FROM agent_track_record"
        ).fetchall()
    )
    conn.close()
    assert tr.get("agent_a", 0) == 1
