"""E4 config 1: "regex-only" -- NFA-driven exploration with trail semantics
(every query in this paper enforces trail, per tab:queries' own caption),
but *no* property constraint at all, early or late. Isolates the automata-
as-join contribution in isolation from any property filtering.

`is_viable_d_final=TRUE` means every trail-respecting, regex-matching path
is accepted -- so this config's final result count and total intermediate
count are the same number, and that number is exactly what the paper calls
"the number of paths matching regex R with phi=true" (`tab:queries`'
methodology note, also `fig:intermediate_total_grid`'s caption for how
non-early-filtering systems' intermediate counts are defined).
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate


def q1_regex_only_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
        ),
        init_d="{edge_ids: []}",
        update_d="{edge_ids: list_append(D.edge_ids, e.edge_id)}",
        is_viable_d="NOT list_contains(D.edge_ids, e.edge_id)",
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
