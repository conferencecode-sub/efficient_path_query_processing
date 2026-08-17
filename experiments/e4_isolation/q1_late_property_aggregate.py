"""E4 config 2: "regex + late property check" -- the SOTA-style plan.
`update_d` tracks exactly the same state as `q1_aggregate.py` (unchanged),
but `is_viable_d` only enforces trail; every property constraint (monotonic
timestamp, region consistency, risk-range, the last-risk gateway, amount)
is deferred to `is_viable_d_final`, checked once after the whole path
(matching regex+trail) is already built -- exactly what Neo4j/Kùzu/DuckDB-
baseline do per `fig:performance_grid`'s own discussion ("all competitors
first generate candidate paths based solely on the label regex ... and then
post-process them with the property constraints").

**Two extra dictionary keys beyond `q1_aggregate.py`, not just a different
is_viable_d/is_viable_d_final split -- worked out by checking which checks
are recoverable from state alone, not assumed:**
  - `max_norm_score`/`min_norm_score` need no extra state: `GREATEST`/`LEAST`
    are monotonically non-decreasing/non-increasing as more edges are
    folded in, so the *final* max-min range is always >= any intermediate
    range -- checking it only at the end gives the exact same answer as
    checking it at every hop. (This is a general property of any
    negatively-stable bounded-range constraint, not specific to Q1.)
  - `last_norm_score` also needs no extra state: it's already only updated
    by normal-labeled edges (`update_d`'s own CASE), so its value at the end
    already correctly reflects "the last normal edge's risk score,"
    unaffected by whatever fraud edges follow.
  - **Monotonic timestamp and region consistency are NOT recoverable from a
    single overwritten scalar.** `last_timestamp_ms`/`region` each only
    remember the *current*/*first* value -- a path with timestamps
    [10, 5, 20] has a perfectly fine-looking final `last_timestamp_ms=20`
    despite violating monotonicity at hop 2, and a `region` that
    disagreement was silently overwritten past would leave `region` still
    looking non-null. Needs two new sticky boolean flags,
    `timestamp_violated`/`region_violated` (OR'd forward, never reset) --
    the same "collect a violated flag, check only at the end" technique
    used for the array-free SQLSolver rewrite of Q3's monotonic constraint
    in the earlier side-quest.
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

NORMAL_LABELS = "'transfer', 'purchase', 'sale'"


def q1_late_property_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_timestamp_ms", "BIGINT"),
            DictionaryKey("region", "VARCHAR"),
            DictionaryKey("max_norm_score", "DOUBLE"),
            DictionaryKey("min_norm_score", "DOUBLE"),
            DictionaryKey("last_norm_score", "DOUBLE"),
            DictionaryKey("total_amount", "DOUBLE"),
            DictionaryKey("timestamp_violated", "BOOLEAN"),
            DictionaryKey("region_violated", "BOOLEAN"),
        ),
        init_d=(
            "{edge_ids: [], last_timestamp_ms: NULL, region: NULL, "
            "max_norm_score: -1e308, min_norm_score: 1e308, "
            "last_norm_score: NULL, total_amount: 0.0, "
            "timestamp_violated: FALSE, region_violated: FALSE}"
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
            "total_amount: D.total_amount + e.amount, "
            "timestamp_violated: D.timestamp_violated OR "
            "(D.last_timestamp_ms IS NOT NULL AND e.timestamp_ms <= D.last_timestamp_ms), "
            "region_violated: D.region_violated OR "
            "(D.region IS NOT NULL AND e.location_region != D.region)}"
        ),
        is_viable_d="NOT list_contains(D.edge_ids, e.edge_id)",
        is_viable_d_final=(
            "NOT D.timestamp_violated AND NOT D.region_violated "
            "AND (D.max_norm_score - D.min_norm_score <= 20) "
            "AND D.total_amount >= 1000 AND D.last_norm_score >= 40"
        ),
        finalize_d="D",
        factorized=True,
    )
