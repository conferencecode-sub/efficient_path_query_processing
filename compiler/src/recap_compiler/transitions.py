"""Stage C: NFA -> transitions relation (FR-9, FR-10).

Renumbers the NFA's states to consecutive small integers and materializes
`T(from_state, to_state, label)`. When the NFA (as produced by Stage B) has
more than one start state -- pyformlang's construction does this routinely,
e.g. for `(a|b|c)+`, since each alternative starts its own epsilon-closure --
this stage synthesizes a single q0 by unioning the outgoing transitions of
every original start state, because the SQL template (Section E of the spec)
assumes exactly one q0. Original start states and their incoming transitions
are kept as-is: a start state can also be a mid-path state reached from
elsewhere, so it must stay reachable through its own edges too.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .regex_frontend import NFA


@dataclass(frozen=True)
class TransitionsRelation:
    rows: tuple  # tuple[tuple[int, int, str], ...] == (from_state, to_state, label)
    q0: int
    accepting_states: frozenset  # frozenset[int]


# Placeholder label value for a "no regex" query's trivial automaton (see
# `trivial_relation` and `ingestion.set_trivial_label_column`) -- not meant
# to collide with a real dataset's own label alphabet, just to give the
# single self-loop transition something to match against.
TRIVIAL_LABEL = "*"


def trivial_relation() -> TransitionsRelation:
    """A single-state NFA (`q0=0`, `accepting_states={0}`) with one
    self-loop on `TRIVIAL_LABEL` -- for a query with no label regex at
    all. Every edge's `label` must be set to `TRIVIAL_LABEL` for the
    self-loop to actually match every edge regardless of its real label
    (`ingestion.set_trivial_label_column` does this). The point of routing
    a "no regex" query through a real (if trivial) automaton, rather than
    a second code path in Stage E/F with no automaton, is that
    `build_standard_query`/`build_optimized_query` never need to know
    whether a query "really" has a regex -- there's exactly one shape of
    generated SQL, always."""
    return TransitionsRelation(rows=((0, 0, TRIVIAL_LABEL),), q0=0, accepting_states=frozenset({0}))


def build_transitions_relation(nfa: NFA) -> TransitionsRelation:
    # Sorted by str(value) for a deterministic state numbering (NFR-1): two
    # calls on the same NFA must produce the same T/q0/Q_F every time.
    ordered_states = sorted(nfa.states, key=lambda s: str(s.value))
    state_id = {state: i for i, state in enumerate(ordered_states)}

    rows = [(state_id[frm], state_id[to], label) for frm, label, to in nfa.transitions]
    accepting = {state_id[s] for s in nfa.accepting_states}

    if len(nfa.start_states) == 1:
        q0 = state_id[next(iter(nfa.start_states))]
    else:
        q0 = len(ordered_states)  # fresh id, one past the last real state
        for frm, label, to in nfa.transitions:
            if frm in nfa.start_states:
                rows.append((q0, state_id[to], label))
        if nfa.start_states & nfa.accepting_states:
            accepting.add(q0)  # some start state already accepts -> so does q0

    # Multiple original start states can share an identical (to, label)
    # outgoing transition -- unioning them onto q0 above then appends one
    # row per matching source, i.e. a real duplicate (from, to, label)
    # tuple, not just a repeated fact. A duplicate row here isn't inert:
    # the generated SQL joins the edges table against this relation, so a
    # duplicate transition row makes the recursive CTE match the same real
    # edge multiple times, multiplying the final path count. `dict.fromkeys`
    # dedupes while preserving first-occurrence order, keeping NFR-1's
    # determinism (the input order is itself already deterministic).
    rows = list(dict.fromkeys(rows))

    return TransitionsRelation(rows=tuple(rows), q0=q0, accepting_states=frozenset(accepting))


def to_dataframe(relation: TransitionsRelation) -> pd.DataFrame:
    """Materializes T(from_state, to_state, label) as a DataFrame, ready to
    register into DuckDB (FR-9)."""
    return pd.DataFrame(relation.rows, columns=["from_state", "to_state", "label"])
