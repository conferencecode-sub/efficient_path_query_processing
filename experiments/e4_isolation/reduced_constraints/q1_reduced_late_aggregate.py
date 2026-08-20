"""E4-reduced config 2: "regex + late property check" for the *reduced*
(milder) constraint set defined in `q1_reduced_early_aggregate.py` --
same 4 checks (trail, timestamp monotonicity, risk-range, final amount),
same thresholds, but every property constraint beyond trail is deferred
to `is_viable_d_final` instead of pruned per-hop. Mirrors the relationship
between the full aggregate's `q1_late_property_aggregate.py` and
`q1_aggregate_general.py` in the parent directory -- same state, different
is_viable_d/is_viable_d_final split.

Per `q1_late_property_aggregate.py`'s own reasoning (re-derived here for
the reduced set, not just copied):
  - `max_norm_score`/`min_norm_score` need no extra state: GREATEST/LEAST
    are monotonic as more edges fold in, so the *final* max-min range is
    always >= any intermediate range -- checking only at the end gives
    the same answer as checking at every (still-normal) hop. Still gated
    by the `e.label IN (...)` CASE here (factorized-style update, no
    NFA-state access) so fraud hops don't get folded into the range,
    matching the early version's own (0,1)/(1,1)-only placement.
  - `total_amount` is already a final-only check in the early version too
    (a growing sum can't be checked earlier), so no change here.
  - Monotonic timestamp is NOT recoverable from a single overwritten
    scalar (`last_timestamp_ms` only remembers the *current* value) --
    needs the sticky `timestamp_violated` flag (OR'd forward, never
    reset), same technique as the full aggregate's late version.
  - `region_violated` and the risk-gateway state (`last_norm_score`) are
    both absent here, since the reduced set drops region and the gateway
    entirely (see `q1_reduced_early_aggregate.py`'s docstring for why).
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

NORMAL_LABELS = "'transfer', 'purchase', 'sale'"
AMOUNT_THRESHOLD = 300  # must match q1_reduced_early_aggregate.py


def q1_reduced_late_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_timestamp_ms", "BIGINT"),
            DictionaryKey("max_norm_score", "DOUBLE"),
            DictionaryKey("min_norm_score", "DOUBLE"),
            DictionaryKey("total_amount", "DOUBLE"),
            DictionaryKey("timestamp_violated", "BOOLEAN"),
        ),
        init_d=(
            "{edge_ids: [], last_timestamp_ms: NULL, "
            "max_norm_score: -1e308, min_norm_score: 1e308, "
            "total_amount: 0.0, timestamp_violated: FALSE}"
        ),
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "last_timestamp_ms: e.timestamp_ms, "
            f"max_norm_score: CASE WHEN e.label IN ({NORMAL_LABELS}) "
            "THEN GREATEST(D.max_norm_score, e.risk_score) ELSE D.max_norm_score END, "
            f"min_norm_score: CASE WHEN e.label IN ({NORMAL_LABELS}) "
            "THEN LEAST(D.min_norm_score, e.risk_score) ELSE D.min_norm_score END, "
            "total_amount: D.total_amount + e.amount, "
            "timestamp_violated: D.timestamp_violated OR "
            "(D.last_timestamp_ms IS NOT NULL AND e.timestamp_ms <= D.last_timestamp_ms)}"
        ),
        is_viable_d="NOT list_contains(D.edge_ids, e.edge_id)",
        is_viable_d_final=(
            "NOT D.timestamp_violated "
            "AND (D.max_norm_score - D.min_norm_score <= 20) "
            f"AND D.total_amount >= {AMOUNT_THRESHOLD}"
        ),
        finalize_d="D",
        factorized=True,
    )
