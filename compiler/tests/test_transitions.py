from recap_compiler.regex_frontend import NFA, compile_regex_to_nfa
from recap_compiler.transitions import TransitionsRelation, build_transitions_relation, to_dataframe


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
