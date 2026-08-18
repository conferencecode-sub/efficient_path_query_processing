import pytest

from recap_compiler.regex_frontend import NFA, compile_regex_to_nfa
from recap_compiler.transitions import (
    TransitionsRelation, build_transitions_relation, guard_against_ambiguity, is_ambiguous,
    to_dataframe, trivial_relation,
)


class FakeState:
    """Minimal stand-in for pyformlang's State (only `.value` is used by
    transitions.py), so Stage C can be unit-tested without depending on
    exactly what shape pyformlang happens to produce."""

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f"FakeState({self.value!r})"


def _relation_accepts(relation: TransitionsRelation, labels: list[str]) -> bool:
    current = {relation.q0}
    for label in labels:
        current = {to for (frm, to, lbl) in relation.rows if frm in current and lbl == label}
        if not current:
            return False
    return bool(current & relation.accepting_states)


def test_single_start_state_uses_its_id_as_q0():
    s0, s1, s2 = FakeState(0), FakeState(1), FakeState(2)
    nfa = NFA(
        states=frozenset({s0, s1, s2}),
        start_states=frozenset({s0}),
        accepting_states=frozenset({s2}),
        transitions=((s0, "a", s1), (s1, "b", s2)),
    )
    relation = build_transitions_relation(nfa)
    assert relation.q0 == 0
    assert relation.accepting_states == frozenset({2})
    assert set(relation.rows) == {(0, 1, "a"), (1, 2, "b")}


def test_multiple_start_states_synthesize_q0_with_unioned_transitions():
    s0, s1, s2, s3 = FakeState(0), FakeState(1), FakeState(2), FakeState(3)
    nfa = NFA(
        states=frozenset({s0, s1, s2, s3}),
        start_states=frozenset({s0, s1}),
        accepting_states=frozenset({s2, s3}),
        transitions=((s0, "a", s2), (s1, "b", s3)),
    )
    relation = build_transitions_relation(nfa)

    assert relation.q0 == 4  # fresh id, one past the 4 original states (0..3)
    # original transitions are kept, not replaced
    assert (0, 2, "a") in relation.rows
    assert (1, 3, "b") in relation.rows
    # q0 inherits the outgoing transitions of *every* original start state
    assert (4, 2, "a") in relation.rows
    assert (4, 3, "b") in relation.rows
    # neither original start state was itself accepting, so q0 isn't either
    assert relation.accepting_states == frozenset({2, 3})


def test_multiple_start_states_sharing_an_outgoing_transition_deduplicate():
    """Regression test for a real overcounting bug (found 2026-08-13 via
    experiments/q1_length_sweep, a pilot cross-validating Q1's path counts
    across independently-built engines): if two different original start
    states have an *identical* (to, label) outgoing transition, unioning
    them onto q0 used to append that (q0, to, label) row twice. Since
    Stage E/F's generated SQL joins `edges` against this relation, a
    duplicate row there makes the recursive CTE match the same real edge
    twice, multiplying the final path count -- this NFA shape is exactly
    what pyformlang produces for Q1's own `(a|b|c)+`-style regex, which is
    why the existing test suite (all single-start-state or non-overlapping
    cases) never caught it."""
    s0, s1, s2 = FakeState(0), FakeState(1), FakeState(2)
    nfa = NFA(
        states=frozenset({s0, s1, s2}),
        start_states=frozenset({s0, s1}),
        accepting_states=frozenset({s2}),
        # s0 and s1 both transition to s2 on 'a' -- same (to, label) pair.
        transitions=((s0, "a", s2), (s1, "a", s2)),
    )
    relation = build_transitions_relation(nfa)

    assert relation.q0 == 3
    assert relation.rows.count((3, 2, "a")) == 1  # not 2
    assert len(relation.rows) == len(set(relation.rows))  # no duplicates at all


def test_q0_accepts_when_a_start_state_already_accepts():
    s0, s1 = FakeState(0), FakeState(1)
    nfa = NFA(
        states=frozenset({s0, s1}),
        start_states=frozenset({s0, s1}),
        accepting_states=frozenset({s0}),
        transitions=((s0, "a", s1),),
    )
    relation = build_transitions_relation(nfa)
    assert relation.q0 == 2
    assert 0 in relation.accepting_states
    assert 2 in relation.accepting_states  # q0 accepts too: the empty string was accepted


def test_to_dataframe_columns_and_rows():
    s0, s1 = FakeState(0), FakeState(1)
    nfa = NFA(states=frozenset({s0, s1}), start_states=frozenset({s0}),
              accepting_states=frozenset({s1}), transitions=((s0, "a", s1),))
    relation = build_transitions_relation(nfa)
    df = to_dataframe(relation)
    assert list(df.columns) == ["from_state", "to_state", "label"]
    assert df.to_dict("records") == [{"from_state": 0, "to_state": 1, "label": "a"}]


def test_state_numbering_is_deterministic():
    s0, s1, s2 = FakeState(0), FakeState(1), FakeState(2)
    nfa = NFA(states=frozenset({s0, s1, s2}), start_states=frozenset({s0}),
              accepting_states=frozenset({s2}),
              transitions=((s0, "a", s1), (s1, "b", s2)))
    first = build_transitions_relation(nfa)
    second = build_transitions_relation(nfa)
    assert first == second


def test_trivial_relation_is_not_ambiguous():
    ambiguous, witness = is_ambiguous(trivial_relation())
    assert ambiguous is False
    assert witness is None


def test_simple_deterministic_relation_is_not_ambiguous():
    s0, s1, s2 = FakeState(0), FakeState(1), FakeState(2)
    nfa = NFA(states=frozenset({s0, s1, s2}), start_states=frozenset({s0}),
              accepting_states=frozenset({s2}), transitions=((s0, "a", s1), (s1, "b", s2)))
    relation = build_transitions_relation(nfa)
    ambiguous, witness = is_ambiguous(relation)
    assert ambiguous is False
    assert witness is None


def test_two_distinct_accepting_runs_for_one_string_is_detected():
    """Minimal hand-built ambiguous automaton: q0 --a--> {1, 2} (a genuine
    nondeterministic choice, not removed by build_transitions_relation's
    own q0-unioning dedup since q0 is *already* the only start state
    here), then both 1 and 2 --b--> 3 (accepting). The string "ab" has two
    distinct accepting runs (0,1,3) and (0,2,3) -- textbook ambiguity."""
    relation = TransitionsRelation(
        rows=((0, 1, "a"), (0, 2, "a"), (1, 3, "b"), (2, 3, "b")),
        q0=0, accepting_states=frozenset({3}),
    )
    ambiguous, witness = is_ambiguous(relation)
    assert ambiguous is True
    p, q = witness
    assert p != q
    assert {p, q} == {1, 2}


def test_q1_paper_regex_is_not_ambiguous_minimized_and_unminimized():
    """Regression test for the 2026-08-18 sanity check: minimize=True vs.
    minimize=False were confirmed to produce identical result counts (not
    just identical path sets) for Q1's actual paper regex, on real data.
    This is *why* that holds -- the relation is unambiguous either way, so
    no accepting run is ever double-counted."""
    regex = "(transfer|purchase|sale)+(phishing|scam)+"
    for minimize in (False, True):
        nfa = compile_regex_to_nfa(regex, minimize=minimize)
        relation = build_transitions_relation(nfa)
        ambiguous, witness = is_ambiguous(relation)
        assert ambiguous is False, f"Q1's regex should be unambiguous (minimize={minimize})"
        assert witness is None


def test_overlapping_alternation_branches_produce_a_detectably_ambiguous_relation():
    """Regression test for the 2026-08-18 finding: a label reachable via
    two overlapping alternation branches (here, 'purchase' appears in both
    halves of the outer union) makes the *unminimized* relation ambiguous
    -- confirmed empirically to overcount real query results (same
    physical-path set, higher row count) despite build_transitions_relation's
    own start-state dedup, since the overlap here isn't at the start
    state. minimize=True removes it (a DFA is always unambiguous)."""
    regex = "((transfer|purchase)|(purchase|sale))+"
    nfa_unminimized = compile_regex_to_nfa(regex, minimize=False)
    relation_unminimized = build_transitions_relation(nfa_unminimized)
    ambiguous, witness = is_ambiguous(relation_unminimized)
    assert ambiguous is True
    assert witness is not None

    nfa_minimized = compile_regex_to_nfa(regex, minimize=True)
    relation_minimized = build_transitions_relation(nfa_minimized)
    ambiguous_min, witness_min = is_ambiguous(relation_minimized)
    assert ambiguous_min is False
    assert witness_min is None


def test_guard_leaves_an_unambiguous_relation_unchanged():
    regex = "(transfer|purchase|sale)+(phishing|scam)+"
    nfa = compile_regex_to_nfa(regex, minimize=False)
    relation = build_transitions_relation(nfa)
    new_nfa, new_relation, message = guard_against_ambiguity(regex, nfa, relation)
    assert new_nfa is nfa
    assert new_relation == relation
    assert message is None


def test_guard_escalates_an_ambiguous_relation_and_warns():
    regex = "((transfer|purchase)|(purchase|sale))+"
    nfa = compile_regex_to_nfa(regex, minimize=False)
    relation = build_transitions_relation(nfa)
    ambiguous_before, _ = is_ambiguous(relation)
    assert ambiguous_before is True  # sanity: this regex really is ambiguous unminimized

    with pytest.warns(RuntimeWarning, match="ambiguous automaton"):
        new_nfa, new_relation, message = guard_against_ambiguity(regex, nfa, relation)

    assert message is not None
    assert "ambiguous automaton" in message
    assert "wavefront/segment" in message  # the compatibility caveat must survive into the message
    ambiguous_after, witness_after = is_ambiguous(new_relation)
    assert ambiguous_after is False
    assert witness_after is None
    # escalating actually changed something -- not a no-op relabeled as "fixed"
    assert new_relation != relation


def test_guard_never_fires_for_an_already_minimized_relation():
    """A minimized automaton is always a DFA, hence always unambiguous --
    the guard should be a pure no-op on it, never re-escalating."""
    regex = "((transfer|purchase)|(purchase|sale))+"
    nfa = compile_regex_to_nfa(regex, minimize=True)
    relation = build_transitions_relation(nfa)
    new_nfa, new_relation, message = guard_against_ambiguity(regex, nfa, relation)
    assert new_nfa is nfa
    assert new_relation == relation
    assert message is None


def test_domestic_foreign_end_to_end():
    # The paper's Q_B worked instantiation (Section 10 of compiler_reqs.md):
    # regex `Domestic+ Foreign`, T = {(1,2,Domestic),(2,2,Domestic),(2,3,Foreign)},
    # q0=1, Q_F={3} (up to renumbering -- pyformlang's construction doesn't
    # minimize to that exact 3-state shape, so this checks behavior instead).
    nfa = compile_regex_to_nfa("Domestic+ Foreign")
    relation = build_transitions_relation(nfa)

    assert isinstance(relation.q0, int)
    assert _relation_accepts(relation, ["Domestic", "Foreign"])
    assert _relation_accepts(relation, ["Domestic", "Domestic", "Domestic", "Foreign"])
    assert not _relation_accepts(relation, ["Foreign"])
    assert not _relation_accepts(relation, ["Domestic"])
    assert not _relation_accepts(relation, ["Domestic", "Foreign", "Domestic"])
