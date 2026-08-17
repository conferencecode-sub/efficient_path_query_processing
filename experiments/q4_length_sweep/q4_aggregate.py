"""Q4's selective aggregate ("max-min trail": a trail whose max and min
edge weight stay within a bound). Unlike Q1-Q3, there's no `ReCAP/q4/`
old-prototype to translate from -- this repo never had one (only
`ReCAP/q1`/`q2`/`q3` exist) -- so this is built directly from FR-13(iii)'s
own worked example (`bounded_range`), combined by hand with FR-13(ii)'s
`trail_via_edge_ids`, same composition pattern as q3_aggregate.py.

Bound picked to match `experiments/SOA-GDBMS/kuzu_run.py`'s own
`Q4_MAX_MIN_BOUND = 20` (fixed 2026-08-13 from a stale timestamp-scale
placeholder to something meaningful on this 0-100-range `weight` column,
the same one Q3 uses) -- so both engines check the same real constraint.

**2026-08-14 note:** figures.tex's actual Q4 (tab:queries) is a
*timestamp* cohesion check ("earliest and latest edge timestamp along the
path does not exceed two weeks"), not a generic 0-100 weight bound -- this
generic max-min-weight shape was originally built as a stand-in for the
toy 100-node dataset, which has no timestamp column. `bound` is now a
parameter (default unchanged at `MAX_MIN_BOUND=20`) so callers with a real
timestamp column can pass `q4_aggregate(bound=1_209_600_000)` (two weeks
in milliseconds) against an aliased `weight` column instead -- see
`run_new_compiler_ldbc100.py`.
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

MAX_MIN_BOUND = 20.0


def q4_aggregate(bound: float = MAX_MIN_BOUND) -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("max_weight", "DOUBLE"),
            DictionaryKey("min_weight", "DOUBLE"),
        ),
        init_d="{edge_ids: [], max_weight: -1e308, min_weight: 1e308}",
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "max_weight: GREATEST(D.max_weight, e.weight), "
            "min_weight: LEAST(D.min_weight, e.weight)}"
        ),
        is_viable_d=(
            "NOT list_contains(D.edge_ids, e.edge_id) "
            f"AND GREATEST(D.max_weight, e.weight) - LEAST(D.min_weight, e.weight) <= {bound}"
        ),
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
