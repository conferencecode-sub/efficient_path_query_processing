r"""Q1's constraint, re-authored as a General (non-factorized) selective
aggregate -- a direct test of `subsec:e5_handcrafted`'s own claim that the
factorized/`\CompilerOpt` gap (vs. Handcrafted/Split) is a *fixable*
limitation of the current generator, not a fundamental cost. `q1_aggregate`
(factorized=True) can't express "check the last-risk gateway only at the
state 1->2 transition" and has to defer it to `is_viable_d_final` -- evaluated
only after the entire fraud suffix has already been explored. Non-factorized
mode has `from_state`/`to_state` in scope, so it can place that same check
exactly where it becomes decidable: the (1,2) transition's own `is_viable_d`,
using `D.last_norm_score` as it stands *before* this hop -- which already
holds the last normal edge's risk score, set by the previous (0,1)/(1,1) hop,
so nothing else about the semantics changes.

**`update_d` is a single, uniform body across every pair -- not split by
state at all.** A first attempt here split it into a "normal" branch and a
"fraud" branch that froze `max_norm_score`/`min_norm_score`/`last_norm_score`
once past the transition, mirroring `q1_aggregate.py`'s own factorized
`CASE WHEN e.label IN (...)` guard -- but that guard exists there only
because a *factorized* body has no state to gate on and must reconstruct
the "normal-only" condition from `e.label` directly. The actual hand-written
source of truth, `ReCAP/q1/recap_gen_recap_inline.py`, does no such
freezing: it updates `max_risk`/`min_risk`/`last_risk` unconditionally on
*every* edge, fraud or not -- those fields are simply never read again once
past the transition (`is_viable_d`'s per-pair `CASE` only ever checks the
gateway at the exact (1,2) row, using the value as it stood *entering* that
row), so freezing them is pure wasted branching, not a semantic
requirement. Mapping the proven-fast hand-written shape directly, instead
of re-deriving an equivalent-but-more-branchy one, is exactly the point of
this file.

Every check's placement:
- (0,1)/(1,1) (still-normal hops): trail + timestamp + region + the
  risk-range bound -- unconditional here (no `e.label IN (...)` CASE
  needed; the NFA structure itself already guarantees these pairs only
  fire on normal-labelled edges).
- (1,2)/(2,2) (fraud hops): trail + timestamp + region only -- the
  risk-range bound doesn't apply on fraud edges, matching the factorized
  version's own `e.label NOT IN (...) OR ...` short-circuit.
- (1,2) additionally: the moved-up gateway check.
- `is_viable_d_final`: now just the amount bound -- the one check that
  really can't be pushed earlier (a growing sum only gets easier to
  satisfy with more edges, so it must stay a final check, per the
  docstring's own reasoning in `q1_aggregate.py`).
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

_TRAIL_TIME_REGION = (
    "NOT list_contains(D.edge_ids, e.edge_id) "
    "AND (D.last_timestamp_ms IS NULL OR e.timestamp_ms > D.last_timestamp_ms) "
    "AND (D.region IS NULL OR e.location_region = D.region)"
)

# Uniform across every pair, matching recap_gen_recap_inline.py's own
# unconditional update -- no normal/fraud branch.
_UPDATE = (
    "{edge_ids: list_append(D.edge_ids, e.edge_id), "
    "last_timestamp_ms: e.timestamp_ms, "
    "region: COALESCE(D.region, e.location_region), "
    "max_norm_score: GREATEST(D.max_norm_score, e.risk_score), "
    "min_norm_score: LEAST(D.min_norm_score, e.risk_score), "
    "last_norm_score: e.risk_score, "
    "total_amount: D.total_amount + e.amount}"
)


def q1_aggregate_general() -> SelectiveAggregate:
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
        update_d={
            (0, 1): _UPDATE,
            (1, 1): _UPDATE,
            (1, 2): _UPDATE,
            (2, 2): _UPDATE,
        },
        is_viable_d={
            (0, 1): f"{_TRAIL_TIME_REGION} AND "
                    "GREATEST(D.max_norm_score, e.risk_score) - LEAST(D.min_norm_score, e.risk_score) <= 20",
            (1, 1): f"{_TRAIL_TIME_REGION} AND "
                    "GREATEST(D.max_norm_score, e.risk_score) - LEAST(D.min_norm_score, e.risk_score) <= 20",
            # The moved-up gateway check: decidable right here, using D as
            # it stood before this hop (already the last normal edge's own
            # risk score) -- not deferred to is_viable_d_final anymore.
            (1, 2): f"{_TRAIL_TIME_REGION} AND D.last_norm_score >= 40",
            (2, 2): _TRAIL_TIME_REGION,
        },
        is_viable_d_final="D.total_amount >= 1000",
        finalize_d="D",
        factorized=False,
    )
