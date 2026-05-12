"""Tests for bounty_board.patches — #7 replay-time patch substrate.

Covers:
  * apply_transformer pure-function semantics (all V1 kinds + errors)
  * propose / get / list_by_signature lifecycle
  * record_outcome counter bumps
  * promote_eligible substrate sweep (threshold + idempotency)
  * find_applicable visibility rules (canonical-to-all,
    candidate-to-self)
  * retire + ordering invariants
  * PatchProposer / PatchPromoter ergonomic wrappers
  * End-to-end: propose → 3 successes → tick → another agent sees
    canonical

decision_id: cluster_brokerless_task_queue_pitch_v0
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from bounty_board import patches
from bounty_board.patches import (
    KNOWN_KINDS,
    PATCH_STATUSES,
    Patch,
    PatchPromoter,
    PatchProposer,
    UnknownTransformerKindError,
    apply_transformer,
)
from bounty_board.queue import Queue

# ─── apply_transformer (pure) ───────────────────────────────────────


def test_apply_prepend_system_msg_with_messages_list():
    payload = {"messages": [{"role": "user", "content": "hi"}]}
    out = apply_transformer(
        payload,
        {"kind": "prepend_system_msg", "args": {"text": "be careful"}},
    )
    assert out["messages"] == [
        {"role": "system", "content": "be careful"},
        {"role": "user", "content": "hi"},
    ]
    # Input not mutated.
    assert payload == {"messages": [{"role": "user", "content": "hi"}]}


def test_apply_prepend_system_msg_without_messages_falls_back_to_system_prompt():
    payload = {"prompt": "do the thing"}
    out = apply_transformer(
        payload,
        {"kind": "prepend_system_msg", "args": {"text": "be careful"}},
    )
    assert out["system_prompt"] == "be careful"
    assert out["prompt"] == "do the thing"


def test_apply_prepend_system_msg_with_non_list_messages_falls_back():
    # If "messages" is present but not a list, fallback path applies.
    payload = {"messages": "not-a-list"}
    out = apply_transformer(
        payload,
        {"kind": "prepend_system_msg", "args": {"text": "x"}},
    )
    assert out["system_prompt"] == "x"
    assert out["messages"] == "not-a-list"


def test_apply_truncate_field_shortens_long_string():
    payload = {"prompt": "a" * 5000}
    out = apply_transformer(
        payload,
        {"kind": "truncate_field",
         "args": {"field": "prompt", "max_chars": 100}},
    )
    assert len(out["prompt"]) == 100
    assert out["prompt"] == "a" * 100


def test_apply_truncate_field_noop_when_short():
    payload = {"prompt": "short"}
    out = apply_transformer(
        payload,
        {"kind": "truncate_field",
         "args": {"field": "prompt", "max_chars": 100}},
    )
    assert out["prompt"] == "short"


def test_apply_truncate_field_noop_when_field_absent():
    payload = {"other": "v"}
    out = apply_transformer(
        payload,
        {"kind": "truncate_field",
         "args": {"field": "prompt", "max_chars": 100}},
    )
    assert "prompt" not in out
    assert out["other"] == "v"


def test_apply_truncate_field_noop_when_field_not_string():
    payload = {"prompt": 42}
    out = apply_transformer(
        payload,
        {"kind": "truncate_field",
         "args": {"field": "prompt", "max_chars": 100}},
    )
    assert out["prompt"] == 42


def test_apply_swap_model_sets_model_field():
    payload = {"model": "gpt-3.5", "prompt": "hi"}
    out = apply_transformer(
        payload,
        {"kind": "swap_model", "args": {"to": "claude-sonnet"}},
    )
    assert out["model"] == "claude-sonnet"
    assert out["prompt"] == "hi"


def test_apply_swap_model_sets_when_absent():
    payload = {"prompt": "hi"}
    out = apply_transformer(
        payload,
        {"kind": "swap_model", "args": {"to": "claude-sonnet"}},
    )
    assert out["model"] == "claude-sonnet"


def test_apply_unknown_kind_raises():
    with pytest.raises(UnknownTransformerKindError):
        apply_transformer(
            {"x": 1}, {"kind": "do_a_dance", "args": {}},
        )


def test_apply_transformer_not_dict_raises():
    with pytest.raises(ValueError):
        apply_transformer({}, "not-a-dict")  # type: ignore[arg-type]


def test_apply_transformer_kind_not_str_raises():
    with pytest.raises(ValueError):
        apply_transformer({}, {"kind": 42, "args": {}})


def test_apply_transformer_args_not_dict_raises():
    with pytest.raises(ValueError):
        apply_transformer(
            {}, {"kind": "swap_model", "args": "to=claude"},
        )


def test_apply_prepend_system_msg_missing_text_raises():
    with pytest.raises(ValueError):
        apply_transformer({}, {"kind": "prepend_system_msg", "args": {}})


def test_apply_truncate_field_negative_max_chars_raises():
    with pytest.raises(ValueError):
        apply_transformer(
            {},
            {"kind": "truncate_field",
             "args": {"field": "x", "max_chars": -1}},
        )


def test_apply_truncate_field_missing_args_raises():
    with pytest.raises(ValueError):
        apply_transformer(
            {}, {"kind": "truncate_field", "args": {"field": "x"}},
        )


def test_apply_swap_model_missing_to_raises():
    with pytest.raises(ValueError):
        apply_transformer({}, {"kind": "swap_model", "args": {}})


def test_apply_does_not_mutate_nested_input():
    payload = {"messages": [{"role": "user", "content": "hi"}],
               "meta": {"trace": [1, 2, 3]}}
    out = apply_transformer(
        payload,
        {"kind": "prepend_system_msg", "args": {"text": "be safe"}},
    )
    # Mutate output and ensure input is independent.
    out["meta"]["trace"].append(99)
    assert payload["meta"]["trace"] == [1, 2, 3]


# ─── propose ────────────────────────────────────────────────────────


def test_propose_inserts_candidate_row(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "claude-sonnet"}},
        agent_id="a",
    )
    assert isinstance(pid, int)
    p = patches.get(q, pid)
    assert p is not None
    assert p.id == pid
    assert p.payload_signature == "sig:foo"
    assert p.transformer == {
        "kind": "swap_model", "args": {"to": "claude-sonnet"},
    }
    assert p.status == "candidate"
    assert p.is_candidate
    assert not p.is_canonical
    assert not p.is_retired
    assert p.n_successes == 0
    assert p.n_failures == 0
    assert p.proposed_by_agent_id == "a"
    assert p.promoted_at is None
    # proposed_at within last second
    assert abs(p.proposed_at - time.time()) < 2.0


def test_propose_rejects_unknown_kind(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(UnknownTransformerKindError):
        patches.propose(
            q, "sig:foo", {"kind": "bogus", "args": {}}, agent_id="a",
        )


def test_propose_rejects_malformed_transformer(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        patches.propose(
            q, "sig:foo",
            {"kind": "truncate_field", "args": {"field": "x"}},
            agent_id="a",
        )


def test_propose_two_candidates_same_signature_get_distinct_ids(
    tmp_path: Path,
):
    q = Queue(tmp_path / "q.db")
    p1 = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    p2 = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m2"}}, agent_id="b",
    )
    assert p1 != p2


# ─── get / list_by_signature ────────────────────────────────────────


def test_get_returns_none_for_missing(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    assert patches.get(q, 999) is None


def test_list_by_signature_returns_all(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m2"}}, agent_id="b",
    )
    patches.propose(
        q, "sig:other",
        {"kind": "swap_model", "args": {"to": "m3"}}, agent_id="a",
    )
    out = patches.list_by_signature(q, "sig:foo")
    assert len(out) == 2
    assert {p.transformer["args"]["to"] for p in out} == {"m1", "m2"}


def test_list_by_signature_filters_by_status(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid_cand = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    pid_canon = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m2"}}, agent_id="a",
    )
    # Manually flip one to canonical for the filter test.
    q._conn.execute(
        "UPDATE patches SET status = 'canonical' WHERE id = ?",
        (pid_canon,),
    )
    q._conn.commit()
    cands = patches.list_by_signature(q, "sig:foo", status="candidate")
    canons = patches.list_by_signature(q, "sig:foo", status="canonical")
    assert [p.id for p in cands] == [pid_cand]
    assert [p.id for p in canons] == [pid_canon]


def test_list_by_signature_rejects_bad_status(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        patches.list_by_signature(q, "sig:foo", status="banana")


def test_list_by_signature_orders_by_proposed_at(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    p1 = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    # Force a millisecond apart so sort order is unambiguous.
    time.sleep(0.01)
    p2 = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m2"}}, agent_id="a",
    )
    out = patches.list_by_signature(q, "sig:foo")
    assert [p.id for p in out] == [p1, p2]


# ─── record_outcome ─────────────────────────────────────────────────


def test_record_outcome_increments_successes(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    patches.record_outcome(q, pid, success=True)
    patches.record_outcome(q, pid, success=True)
    p = patches.get(q, pid)
    assert p is not None
    assert p.n_successes == 2
    assert p.n_failures == 0


def test_record_outcome_increments_failures(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    patches.record_outcome(q, pid, success=False)
    p = patches.get(q, pid)
    assert p is not None
    assert p.n_successes == 0
    assert p.n_failures == 1


def test_record_outcome_raises_on_missing(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        patches.record_outcome(q, 999, success=True)


# ─── promote_eligible ───────────────────────────────────────────────


def test_promote_eligible_promotes_at_threshold(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    for _ in range(3):
        patches.record_outcome(q, pid, success=True)
    n = patches.promote_eligible(q)
    assert n == 1
    p = patches.get(q, pid)
    assert p is not None
    assert p.status == "canonical"
    assert p.is_canonical
    assert p.promoted_at is not None


def test_promote_eligible_leaves_below_threshold(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    for _ in range(2):
        patches.record_outcome(q, pid, success=True)
    n = patches.promote_eligible(q)
    assert n == 0
    p = patches.get(q, pid)
    assert p is not None
    assert p.status == "candidate"


def test_promote_eligible_idempotent(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    for _ in range(3):
        patches.record_outcome(q, pid, success=True)
    assert patches.promote_eligible(q) == 1
    # Second tick promotes nothing — already canonical.
    assert patches.promote_eligible(q) == 0
    p = patches.get(q, pid)
    assert p is not None
    assert p.status == "canonical"


def test_promote_eligible_respects_threshold_parameter(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    patches.record_outcome(q, pid, success=True)
    # threshold=1 should promote a single-success patch
    assert patches.promote_eligible(q, threshold=1) == 1


def test_promote_eligible_rejects_zero_threshold(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        patches.promote_eligible(q, threshold=0)


def test_promote_eligible_only_touches_candidates(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    # Retired patch with high n_successes should NOT be promoted.
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    for _ in range(5):
        patches.record_outcome(q, pid, success=True)
    patches.retire(q, pid)
    n = patches.promote_eligible(q)
    assert n == 0
    p = patches.get(q, pid)
    assert p is not None
    assert p.status == "retired"


# ─── retire ─────────────────────────────────────────────────────────


def test_retire_marks_status(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    patches.retire(q, pid)
    p = patches.get(q, pid)
    assert p is not None
    assert p.status == "retired"
    assert p.is_retired


def test_retire_raises_on_missing(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    with pytest.raises(ValueError):
        patches.retire(q, 999)


# ─── find_applicable ────────────────────────────────────────────────


def test_find_applicable_returns_canonical_to_any_agent(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    for _ in range(3):
        patches.record_outcome(q, pid, success=True)
    patches.promote_eligible(q)
    # Agent b (different from proposer) sees the canonical.
    out = patches.find_applicable(q, "sig:foo", agent_id="b")
    assert len(out) == 1
    assert out[0].id == pid
    assert out[0].is_canonical


def test_find_applicable_returns_own_candidate(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    out = patches.find_applicable(q, "sig:foo", agent_id="a")
    assert [p.id for p in out] == [pid]


def test_find_applicable_excludes_other_agents_candidate(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    # Agent b should NOT see agent a's candidate.
    out = patches.find_applicable(q, "sig:foo", agent_id="b")
    assert out == []


def test_find_applicable_excludes_retired(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    patches.retire(q, pid)
    assert patches.find_applicable(q, "sig:foo", agent_id="a") == []


def test_find_applicable_excludes_other_signature(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    assert patches.find_applicable(q, "sig:bar", agent_id="a") == []


def test_find_applicable_orders_canonical_first(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    # Earlier candidate
    p1 = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    for _ in range(3):
        patches.record_outcome(q, p1, success=True)
    patches.promote_eligible(q)  # p1 now canonical
    # Later self-candidate
    time.sleep(0.01)
    p2 = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m2"}}, agent_id="a",
    )
    out = patches.find_applicable(q, "sig:foo", agent_id="a")
    # Canonical first, then own candidate.
    assert [p.id for p in out] == [p1, p2]
    assert out[0].is_canonical
    assert out[1].is_candidate


# ─── PatchProposer / PatchPromoter ──────────────────────────────────


def test_patch_proposer_propose_wraps_module(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    proposer = PatchProposer(q, agent_id="a")
    pid = proposer.propose(
        "sig:foo", {"kind": "swap_model", "args": {"to": "m1"}},
    )
    p = patches.get(q, pid)
    assert p is not None
    assert p.proposed_by_agent_id == "a"


def test_patch_promoter_tick_promotes_eligible(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    for _ in range(3):
        patches.record_outcome(q, pid, success=True)
    promoter = PatchPromoter(q)
    assert promoter.tick() == 1
    assert patches.get(q, pid).is_canonical  # type: ignore[union-attr]


def test_patch_promoter_respects_custom_threshold(tmp_path: Path):
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    patches.record_outcome(q, pid, success=True)
    promoter = PatchPromoter(q, threshold=1)
    assert promoter.tick() == 1
    p = patches.get(q, pid)
    assert p is not None
    assert p.is_canonical


# ─── module-level sanity ────────────────────────────────────────────


def test_known_kinds_exposed():
    # Anchor the V1 kind set so adding/removing a kind is a deliberate
    # API change visible in tests.
    assert set(KNOWN_KINDS) == {
        "prepend_system_msg", "truncate_field", "swap_model",
    }


def test_patch_statuses_match_schema():
    # Anchor the V1 status enum.
    assert set(PATCH_STATUSES) == {"candidate", "canonical", "retired"}


# ─── end-to-end ─────────────────────────────────────────────────────


def test_end_to_end_propose_promote_visible_to_other_agent(tmp_path: Path):
    """The canonical Option D #7 user story:

      1. Agent A diagnoses a failure, proposes a patch
      2. A's replays use the patch (find_applicable returns it for A)
      3. B sees nothing yet (candidate is scoped to A)
      4. The patch's replays succeed 3 times → record_outcome bumps
      5. PatchPromoter.tick() promotes A's candidate to canonical
      6. Now B's find_applicable returns the patch — it's canonical
         and applies to everyone claiming the signature
    """
    q = Queue(tmp_path / "q.db")
    proposer = PatchProposer(q, agent_id="A")
    promoter = PatchPromoter(q)

    # Step 1: A proposes
    pid = proposer.propose(
        "sig:long_prompt",
        {"kind": "truncate_field",
         "args": {"field": "prompt", "max_chars": 1000}},
    )

    # Step 2: A's view includes the candidate
    seen_by_a = patches.find_applicable(q, "sig:long_prompt", agent_id="A")
    assert [p.id for p in seen_by_a] == [pid]

    # Step 3: B's view is empty
    assert patches.find_applicable(
        q, "sig:long_prompt", agent_id="B",
    ) == []

    # Step 4: 3 successes
    for _ in range(3):
        patches.record_outcome(q, pid, success=True)

    # Step 5: promote
    assert promoter.tick() == 1

    # Step 6: B now sees it as canonical
    seen_by_b = patches.find_applicable(q, "sig:long_prompt", agent_id="B")
    assert len(seen_by_b) == 1
    assert seen_by_b[0].id == pid
    assert seen_by_b[0].is_canonical

    # Sanity: applying the canonical transformer truncates payloads
    long_payload = {"prompt": "x" * 5000}
    patched = apply_transformer(long_payload, seen_by_b[0].transformer)
    assert len(patched["prompt"]) == 1000


def test_patch_dataclass_is_read_only_view(tmp_path: Path):
    """A Patch is a hydrated snapshot — mutating it does NOT update
    the DB. This is the documented contract (lifecycle goes through
    module functions). Test that the snapshot is independent of
    subsequent record_outcome calls."""
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    snapshot = patches.get(q, pid)
    assert snapshot is not None
    assert snapshot.n_successes == 0
    patches.record_outcome(q, pid, success=True)
    # Snapshot didn't auto-refresh; fresh read shows the update.
    assert snapshot.n_successes == 0
    fresh = patches.get(q, pid)
    assert fresh is not None
    assert fresh.n_successes == 1


def test_patch_is_dataclass_instance(tmp_path: Path):
    """Defensive: confirm Patch is the dataclass we expose."""
    q = Queue(tmp_path / "q.db")
    pid = patches.propose(
        q, "sig:foo",
        {"kind": "swap_model", "args": {"to": "m1"}}, agent_id="a",
    )
    p = patches.get(q, pid)
    assert isinstance(p, Patch)
