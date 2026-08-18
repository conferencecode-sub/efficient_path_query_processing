"""TCR5: Person -[own]-> Account -[transfer*1..3]-> Account, ascending
timestamps on the transfer chain, time window on transfer edges only (the
`own` edge has no timestamp/window constraint at all in the reference
query -- `ldbc_finbench_transaction_impls/neo4j/queries/tcr-5.cypher`).

NFA: state 0 (start) -[own]-> 1 -[transfer]-> 2 (self-loop on transfer,
accepting). State 1 exists only to force exactly one `own` hop before any
transfer is allowed; nothing can accept there (0 transfers doesn't satisfy
FinBench's own `*1..3`, i.e. >=1 required).
"""
from __future__ import annotations

from common import END_TIME, START_TIME
from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate
from recap_compiler.transitions import TransitionsRelation

NFA_RELATION = TransitionsRelation(
    rows=((0, 1, "own"), (1, 2, "transfer"), (2, 2, "transfer")),
    q0=0,
    accepting_states=frozenset({2}),
)


def tcr5_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_transfer_time", "BIGINT"),
        ),
        init_d="{edge_ids: [], last_transfer_time: NULL}",
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "last_transfer_time: CASE WHEN e.label = 'transfer' THEN e.timestamp_ms "
            "ELSE D.last_transfer_time END}"
        ),
        is_viable_d=(
            "NOT list_contains(D.edge_ids, e.edge_id) "
            "AND (e.label != 'transfer' OR ("
            f"e.timestamp_ms > {START_TIME} AND e.timestamp_ms < {END_TIME} "
            "AND (D.last_transfer_time IS NULL OR e.timestamp_ms > D.last_transfer_time)))"
        ),
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
