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

    return TransitionsRelation(rows=tuple(rows), q0=q0, accepting_states=frozenset(accepting))


def to_dataframe(relation: TransitionsRelation) -> pd.DataFrame:
    """Materializes T(from_state, to_state, label) as a DataFrame, ready to
    register into DuckDB (FR-9)."""
    return pd.DataFrame(relation.rows, columns=["from_state", "to_state", "label"])
