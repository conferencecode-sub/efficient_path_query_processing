"""Q1's real selective aggregate, hand-translated into the new compiler's
Definition-8 shape from `ReCAP/q1/duckdb_gen_recap.py`'s own hardcoded SQL
(the source of truth for what Q1 actually checks -- there's no existing
compiler-side Q1 entry, only the 3 generic library aggregates).

Q1's constraint, factored into per-hop-checkable pieces:
  - trail: no edge id reused (checked over the *whole* path, both the
    normal and fraud portions) -- same idea as the library's
    trail_via_edge_ids.
  - strictly increasing timestamps, again over the whole path -- the
    library's adjacent_edge_predicate with comparator='>'.
  - all edges share one location_region (first-seen region sticks).
  - bounded-range-style (same shape as the library's bounded_range)
    max-min risk_score <= 20, but scoped
    to only the *normal*-labelled prefix (transfer/purchase/sale), via a
    CASE on `e.label` -- deliberately keeps this factorized (no NFA-state
    dependency) since `e.label` is a real edge column, exactly like the
    original script's `CASE WHEN t.label IN (...)` branches.

Two checks are NOT early-pruned (put in `is_viable_d_final`, not
`is_viable_d`), because they're not negatively stable -- a path failing
them now could still pass after more edges:
  - total amount >= 1000 (a lower bound on a growing sum only gets easier
    to satisfy with more edges, never harder).
  - last normal risk_score >= 40 (a per-hop *current* value, not a
    monotone aggregate -- a later normal edge could still push it over).
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

NORMAL_LABELS = "'transfer', 'purchase', 'sale'"


def q1_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_timestamp_ms", "BIGINT"),
            DictionaryKey("region", "VARCHAR"),
            DictionaryKey("max_norm_score", "DOUBLE"),
            DictionaryKey("min_norm_score", "DOUBLE"),
            DictionaryKey("last_norm_score", "DOUBLE"),
            DictionaryKey("total_amount", "DOUBLE"),
        ),
        init_d=(
            "{edge_ids: [], last_timestamp_ms: NULL, region: NULL, "
            "max_norm_score: -1e308, min_norm_score: 1e308, "
            "last_norm_score: NULL, total_amount: 0.0}"
        ),
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "last_timestamp_ms: e.timestamp_ms, "
            "region: COALESCE(D.region, e.location_region), "
            f"max_norm_score: CASE WHEN e.label IN ({NORMAL_LABELS}) "
            "THEN GREATEST(D.max_norm_score, e.risk_score) ELSE D.max_norm_score END, "
            f"min_norm_score: CASE WHEN e.label IN ({NORMAL_LABELS}) "
            "THEN LEAST(D.min_norm_score, e.risk_score) ELSE D.min_norm_score END, "
            f"last_norm_score: CASE WHEN e.label IN ({NORMAL_LABELS}) "
            "THEN e.risk_score ELSE D.last_norm_score END, "
            "total_amount: D.total_amount + e.amount}"
        ),
        is_viable_d=(
            "NOT list_contains(D.edge_ids, e.edge_id) "
            "AND (D.last_timestamp_ms IS NULL OR e.timestamp_ms > D.last_timestamp_ms) "
            "AND (D.region IS NULL OR e.location_region = D.region) "
            f"AND (e.label NOT IN ({NORMAL_LABELS}) OR "
            "GREATEST(D.max_norm_score, e.risk_score) - LEAST(D.min_norm_score, e.risk_score) <= 20)"
        ),
        is_viable_d_final="D.total_amount >= 1000 AND D.last_norm_score >= 40",
        finalize_d="D",
        factorized=True,
    )
