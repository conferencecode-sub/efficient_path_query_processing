"""TCR1 ("Transaction Chain Repeating 1" in FinBench's own naming):
Account -[transfer*1..3]-> Account <-[signedInBy]- (blocked Medium), with
ascending timestamps on the transfer chain and a time window on every edge.
Source of truth: `ldbc_finbench_transaction_impls/neo4j/queries/tcr-1.cypher`
(fetched from the official repo this session, not guessed).

NFA: state 0 (start) -[transfer]-> 1 (self-loop on transfer) -[signedInBy]-> 2
(accepting, terminal -- no self-loop, matching the Cypher's own fixed
"...<-[signIn]-(medium)" single final hop with nothing after it).

Trail is enforced even though the reference Cypher never writes an explicit
check for it -- Cypher's variable-length pattern matching enforces
relationship-uniqueness within one pattern by default (confirmed for a
different FinBench-shaped query, Q2, earlier this project), so a no-repeat-
edge check is a faithful translation of what the reference query actually
does, not an addition.

`START_TIME`/`END_TIME` (see `common.py`) act as `$start_time`/`$end_time`;
window and monotonicity are both per-hop/negatively-stable, so everything
lives in `is_viable_d` -- `is_viable_d_final=TRUE`.
"""
from __future__ import annotations

from common import END_TIME, START_TIME
from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate
from recap_compiler.transitions import TransitionsRelation

NFA_RELATION = TransitionsRelation(
    rows=((0, 1, "transfer"), (1, 1, "transfer"), (1, 2, "signedInBy")),
    q0=0,
    accepting_states=frozenset({2}),
)


def tcr1_aggregate() -> SelectiveAggregate:
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
            f"AND e.timestamp_ms > {START_TIME} AND e.timestamp_ms < {END_TIME} "
            "AND (e.label != 'transfer' OR D.last_transfer_time IS NULL "
            "OR e.timestamp_ms > D.last_transfer_time)"
        ),
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
