"""Stage C: NFA -> transitions relation.

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

import warnings
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
    # Sorted by str(value) for a deterministic state numbering: two
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
    # dedupes while preserving first-occurrence order, keeping the
    # determinism above (the input order is itself already deterministic).
    rows = list(dict.fromkeys(rows))

    return TransitionsRelation(rows=tuple(rows), q0=q0, accepting_states=frozenset(accepting))


def is_ambiguous(relation: TransitionsRelation) -> tuple[bool, tuple[int, int] | None]:
    """Detects whether `relation` is an *ambiguous* automaton: some label
    string has more than one distinct accepting run through it. This is a
    property of the automaton as constructed, not of the regular language
    it recognizes -- the unique minimal DFA for any regular language is
    always unambiguous, so ambiguity here is purely an artifact of Stage B
    (Thompson's construction from a syntactically ambiguous regex, e.g. a
    label reachable via two overlapping alternation branches) that Stage C
    does not fully remove.

    Why this matters even when the *path set* found is correct: the
    generated SQL counts one output row per accepting run, not one per
    distinct physical path. An ambiguous relation can produce more than
    one row for the same physical path (one per accepting run), silently
    inflating the reported path *count* -- confirmed empirically:
    `(purchase|sale){1,3}phishing` and `((transfer|purchase)|(purchase|
    sale))+` both report the same physical-path set either way but a
    higher count when ambiguous, while `minimize=True` (which always
    produces a DFA, hence always unambiguous) reports the deduplicated
    count. The compiler's own `minimize=False` default already survives
    build_transitions_relation's q0-unioning dedup above for every regex
    checked in this project (including Q1's), so this is a residual risk
    for future/user-authored regexes, not a known-bad default.

    Standard product-automaton construction: (p, q) --a--> (p2, q2) iff
    p--a-->p2 and q--a-->q2 in `relation` (both components reading the same
    symbol). `relation` is ambiguous iff some pair (p, q) with p != q is
    both forward-reachable from a start pair (s1, s2) -- s1 == s2 allowed,
    since a single start state's own nondeterminism is a common source --
    and backward-reachable to a pair (f1, f2) with f1, f2 both accepting.
    Returns `(True, witness_pair)` if ambiguous, else `(False, None)`."""
    out: dict[int, dict[str, set[int]]] = {}
    for frm, to, label in relation.rows:
        out.setdefault(frm, {}).setdefault(label, set()).add(to)

    def product_successors(p, q):
        labels = set(out.get(p, {})) & set(out.get(q, {}))
        for label in labels:
            for p2 in out[p][label]:
                for q2 in out[q][label]:
                    yield (p2, q2)

    start_pairs = {(relation.q0, relation.q0)}
    forward = set(start_pairs)
    reverse_edges: dict[tuple[int, int], set[tuple[int, int]]] = {}
    queue = list(start_pairs)
    while queue:
        p, q = queue.pop()
        for p2, q2 in product_successors(p, q):
            reverse_edges.setdefault((p2, q2), set()).add((p, q))
            if (p2, q2) not in forward:
                forward.add((p2, q2))
                queue.append((p2, q2))

    accepting_pairs = {(f1, f2) for f1 in relation.accepting_states
                        for f2 in relation.accepting_states}
    backward = {pair for pair in accepting_pairs if pair in forward}
    queue = list(backward)
    while queue:
        node = queue.pop()
        for pred in reverse_edges.get(node, ()):
            if pred not in backward:
                backward.add(pred)
                queue.append(pred)

    for p, q in forward & backward:
        if p != q:
            return True, (p, q)
    return False, None


def guard_against_ambiguity(
    pattern: str, nfa: NFA, relation: TransitionsRelation,
) -> tuple[NFA, TransitionsRelation, str | None]:
    """Auto-escalation for the one case `is_ambiguous` exists to catch:
    `nfa`/`relation` were compiled from `pattern` with `minimize=False`
    (the compiler's required default) and turned out ambiguous, so this query's
    reported path *count* would silently overcount (one row per accepting
    run, not one per physical path -- see `is_ambiguous`'s own docstring).
    A minimized automaton is always a DFA, hence always unambiguous, so
    recompiling with `minimize=True` is a strict fix here, not a
    heuristic. Returns `(nfa, relation)` unchanged and `None` if `relation`
    is not ambiguous; otherwise returns the re-minimized `(nfa, relation)`
    plus a human-readable warning message (also emitted via `warnings.warn`
    for non-interactive callers -- callers with a UI, e.g. the workbench,
    should also display the returned message directly, since a Python
    warning alone is easy to miss outside a terminal).

    Deliberately does **not** run when the caller already asked for
    `minimize=True` -- there is nothing to escalate to, and calling this
    on an NFA the caller compiled with `minimize=False` on purpose (e.g.
    for wavefront/segment-planner compatibility) is the caller's
    call to make, not this function's."""
    ambiguous, witness = is_ambiguous(relation)
    if not ambiguous:
        return nfa, relation, None

    message = (
        f"Regex {pattern!r} compiles to an ambiguous automaton (states "
        f"{witness[0]} and {witness[1]} both reachable for the same input "
        "prefix and both able to reach acceptance from there): some "
        "accepted label sequence has more than one distinct accepting "
        "run, which silently overcounts this query's reported path total "
        "(one row per accepting run, not one per physical path) -- the "
        "set of matched paths itself stays correct, only the count is "
        "affected. Auto-escalating to a minimized automaton (a DFA is "
        "always unambiguous) to fix this. Note: minimizing changes the "
        "automaton's state structure, so if this query relies on "
        "wavefront/segment-style splitting at a specific NFA state, that "
        "seam may no longer exist after minimization -- recompile with "
        "minimize=True explicitly and re-derive the split point if so."
    )
    warnings.warn(message, RuntimeWarning, stacklevel=2)

    from .regex_frontend import compile_regex_to_nfa  # local import: avoids a
    # module-level cycle, since regex_frontend.py is Stage B and doesn't (and
    # shouldn't) need to know Stage C exists.

    minimized_nfa = compile_regex_to_nfa(pattern, minimize=True)
    minimized_relation = build_transitions_relation(minimized_nfa)
    return minimized_nfa, minimized_relation, message


def to_dataframe(relation: TransitionsRelation) -> pd.DataFrame:
    """Materializes T(from_state, to_state, label) as a DataFrame, ready to
    register into DuckDB."""
    return pd.DataFrame(relation.rows, columns=["from_state", "to_state", "label"])
