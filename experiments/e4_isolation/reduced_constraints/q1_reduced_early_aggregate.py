r"""E4-reduced config 3: "regex + early property filtering," but on a
*milder* constraint set than `q1_length_sweep/q1_aggregate_general.py`'s
full 6-check aggregate. That full aggregate is deliberately brutal --
result/regex-only-result ~= 1.6% at ell=2 (`e4_isolation/results/
e4_isolation.csv`) -- which makes it impossible to see how the E4 picture
(automata-exploration cost vs. early-filtering benefit) changes when
early filtering is only moderately selective instead of nearly a needle-
in-a-haystack. This file reduces the check set to land selectivity near
50% at ell=2, chosen empirically against the real Metaverse dataset
(see `run_e4_reduced.py`'s docstring for the sweep) rather than guessed.

**Kept (4 of the original 6 checks) vs. dropped (2), and why:**
  - **trail** (`edge_ids`) -- kept unconditionally per the task: dropping
    it would let edges repeat, changing what "a path" even means, not
    just its selectivity.
  - **timestamp monotonicity** (`last_timestamp_ms`, strictly increasing)
    -- kept at its original, unmodified definition. Empirically the
    single closest-to-50%-alone check on this dataset (51.07% of the
    1,408 regex-only paths at ell=2), so no threshold to tune here (it's
    a strict order relation, not a numeric bound).
  - **risk-range bound on normal hops** (`max_norm_score`/`min_norm_score`,
    max-min <= 20) -- kept at its original, unmodified bound. Empirically
    a near-total no-op *at ell=2 specifically* (0 additional paths pruned
    beyond timestamp alone -- two normal-labelled risk scores rarely spread
    > 20 apart), but it is NOT vacuous overall: at ell=3 it prunes
    9,132 -> 6,296 and at ell=4 it prunes 134,396 -> 60,304 beyond
    timestamp+amount alone (longer normal-hop runs accumulate a wider
    max-min spread). Kept specifically so the reduced set still has real
    per-hop pruning behavior at the longer lengths, not just at ell=2.
  - **final amount threshold** (`total_amount`, was >= 1000, now >= 300)
    -- kept but *loosened* (per the task's explicit suggestion to loosen
    thresholds rather than only drop checks). >= 1000 combined with
    timestamp alone was already down to 19.32% at ell=2; >= 300 combined
    with timestamp lands at 49.72%. Still necessarily a final-only check
    (a growing sum only gets easier to satisfy with more edges, so it
    can't be moved earlier -- same reasoning as the full aggregate).
  - **region match** -- DROPPED. Empirically the single most restrictive
    check alone (18.96% of regex-only paths survive region agreement
    alone) and, being a categorical equality rather than a numeric bound,
    it has no dial to loosen smoothly -- keeping it made hitting ~50%
    impossible without effectively disabling it, so it's dropped outright
    rather than kept-but-neutered.
  - **risk "gateway" at the (1,2) transition** (`last_norm_score` >= 40)
    -- DROPPED, per the task's own explicit suggestion. Empirically the
    second most restrictive single check (45.81% alone, but stacks badly
    with timestamp: 23.58% for timestamp+gateway together vs. 51.07% for
    timestamp alone), and, like the full aggregate's design, it only
    matters at the exact (1,2) NFA transition -- the property this file is
    supposed to be demonstrating (General mode's ability to push a
    transition-specific check earlier than `is_viable_d_final`) is already
    fully exercised by keeping the risk-range check gated to the (0,1)/
    (1,1) pair (normal hops only, absent on fraud hops), so dropping the
    gateway doesn't remove General mode's early-filtering advantage from
    the experiment, just one extra layer of it.

Every check's placement (same "push as early as decidable" structure as
the full aggregate, `q1_aggregate_general.py`):
  - (0,1)/(1,1) (still-normal hops): trail + timestamp + risk-range.
  - (1,2)/(2,2) (fraud hops): trail + timestamp only (risk-range doesn't
    apply to fraud edges, and the gateway that used to fire exactly at
    (1,2) has been dropped).
  - `is_viable_d_final`: just the amount bound (>= 300), same as the full
    aggregate's own "can't be pushed earlier" final check.
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

_TRAIL_TIME = (
    "NOT list_contains(D.edge_ids, e.edge_id) "
    "AND (D.last_timestamp_ms IS NULL OR e.timestamp_ms > D.last_timestamp_ms)"
)

# Uniform across every pair (same rationale as q1_aggregate_general.py:
# nothing reads max/min_norm_score again once past the (1,1)->(1,2)
# transition, so unconditionally updating them on fraud hops too is free).
_UPDATE = (
    "{edge_ids: list_append(D.edge_ids, e.edge_id), "
    "last_timestamp_ms: e.timestamp_ms, "
    "max_norm_score: GREATEST(D.max_norm_score, e.risk_score), "
    "min_norm_score: LEAST(D.min_norm_score, e.risk_score), "
    "total_amount: D.total_amount + e.amount}"
)

AMOUNT_THRESHOLD = 300  # loosened from the full aggregate's 1000


def q1_reduced_early_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_timestamp_ms", "BIGINT"),
            DictionaryKey("max_norm_score", "DOUBLE"),
            DictionaryKey("min_norm_score", "DOUBLE"),
            DictionaryKey("total_amount", "DOUBLE"),
        ),
        init_d=(
            "{edge_ids: [], last_timestamp_ms: NULL, "
            "max_norm_score: -1e308, min_norm_score: 1e308, total_amount: 0.0}"
        ),
        update_d={
            (0, 1): _UPDATE,
            (1, 1): _UPDATE,
            (1, 2): _UPDATE,
            (2, 2): _UPDATE,
        },
        is_viable_d={
            (0, 1): f"{_TRAIL_TIME} AND "
                    "GREATEST(D.max_norm_score, e.risk_score) - LEAST(D.min_norm_score, e.risk_score) <= 20",
            (1, 1): f"{_TRAIL_TIME} AND "
                    "GREATEST(D.max_norm_score, e.risk_score) - LEAST(D.min_norm_score, e.risk_score) <= 20",
            (1, 2): _TRAIL_TIME,
            (2, 2): _TRAIL_TIME,
        },
        is_viable_d_final=f"D.total_amount >= {AMOUNT_THRESHOLD}",
        finalize_d="D",
        factorized=False,
    )
