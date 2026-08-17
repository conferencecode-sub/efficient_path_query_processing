"""Q3's real selective aggregate ("monotonic trail"), combining two of
Stage D's own FR-13 library entries into one aggregate -- there's no
"combine two library aggregates" helper in `selective_aggregate.py`, so
this composes their dictionary keys/bodies by hand, matching
`ReCAP/q3/recap_monotonic_trail_inline.py`'s own hardcoded SQL (the source
of truth for what Q3 actually checks):

  - trail: no edge id reused -- FR-13(ii) `trail_via_edge_ids`'s own shape.
  - strictly increasing edge weight -- FR-13(i) `adjacent_edge_predicate`'s
    own shape, with `comparator='>'` (strict) to match the old prototype's
    `e.weight > p.monotonicity_dictionary` exactly (not `>=`).

Both are per-hop-prunable (negatively stable) and enforced in `is_viable_d`
alone; `is_viable_d_final` is just `TRUE`, matching the old prototype's own
final `SELECT` (which adds no extra weight/trail check beyond what the
recursive term already enforced).
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate


def q3_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_weight", "DOUBLE"),
        ),
        init_d="{edge_ids: [], last_weight: NULL}",
        update_d="{edge_ids: list_append(D.edge_ids, e.edge_id), last_weight: e.weight}",
        is_viable_d=(
            "NOT list_contains(D.edge_ids, e.edge_id) "
            "AND (D.last_weight IS NULL OR e.weight > D.last_weight)"
        ),
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
