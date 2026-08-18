"""TCR8: Loan -[deposit]-> Account -[transfer|withdraw *1..3]-> Account,
time window on every edge, and a **growth-ratio** constraint on the
transfer/withdraw chain -- each edge's amount must exceed the previous
edge234's amount times `THRESHOLD` (not plain monotonicity: this is a
genuinely different negatively-stable shape from every other query in this
project, per `ldbc_finbench_transaction_impls/neo4j/queries/tcr-8.cypher`'s
`reduce(curr = head(amts), x IN tail(amts) | CASE WHEN (curr <> -1) AND
(x > curr*$threshold) THEN x ELSE -1 end) <> -1`).

The reference query's final step -- `GROUP BY dst, SUM(last edge's amount)`
across all accepted paths -- sits outside ReCAP's per-path
`is_viable_d_final` model (it's a cross-path aggregation, not a per-path
predicate), so it isn't expressed here; `run_tcr8.py` applies it as a plain
outer `GROUP BY` over ReCAP's raw path output instead. Everything that *is*
per-path (trail, window, growth-ratio) is fully early-filtered.

NFA: state 0 (start) -[deposit]-> 1 -[transfer|withdraw]-> 1 (self-loop,
accepting).
"""
from __future__ import annotations

from common import END_TIME, START_TIME
from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate
from recap_compiler.transitions import TransitionsRelation

NFA_RELATION = TransitionsRelation(
    rows=((0, 1, "deposit"), (1, 1, "transfer"), (1, 1, "withdraw")),
    q0=0,
    accepting_states=frozenset({1}),
)

THRESHOLD = 1.0


def tcr8_aggregate(threshold: float = THRESHOLD) -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_234_amount", "DOUBLE"),
        ),
        init_d="{edge_ids: [], last_234_amount: NULL}",
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "last_234_amount: CASE WHEN e.label IN ('transfer', 'withdraw') THEN e.amount "
            "ELSE D.last_234_amount END}"
        ),
        is_viable_d=(
            "NOT list_contains(D.edge_ids, e.edge_id) "
            f"AND e.timestamp_ms > {START_TIME} AND e.timestamp_ms < {END_TIME} "
            "AND (e.label NOT IN ('transfer', 'withdraw') "
            f"OR D.last_234_amount IS NULL OR e.amount > D.last_234_amount * {threshold})"
        ),
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
