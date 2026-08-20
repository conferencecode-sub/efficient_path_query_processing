"""Selective aggregate for a "product-of-weights" variant of the
scalability sweep over Datagen-7.7: a trail (no repeated edge) whose
running product of edge weight must stay `<= bound`.

Negatively stable because every edge weight in this dataset is >= 1.0
(confirmed range [1.0, 1.75342]): multiplying by a factor >= 1 can only
keep the running product the same or increase it, so once it exceeds
`bound`, no extension can bring it back under. This is a sharper
requirement than the sum aggregate's (which only needs weights positive) --
satisfied here specifically because the minimum weight is exactly 1.0, not
merely > 0.

Practical motivation: sum's mathematical floor at length L is L*1.0 (grows
with L, forcing a hard wall past some fixed length regardless of start
vertex/bound -- see sum_weight_aggregate.py's pass 2/3 history). Product's
floor is 1.0^L = 1.0 at every length, since an all-minimum-weight path
keeps a constant product regardless of length -- so a product bound does
not force a length-dependent wall the way sum does.
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

PRODUCT_WEIGHT_BOUND = 3.0


def product_weight_aggregate(bound: float = PRODUCT_WEIGHT_BOUND) -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("prod_weight", "DOUBLE"),
        ),
        init_d="{edge_ids: [], prod_weight: 1.0}",
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "prod_weight: D.prod_weight * e.weight}"
        ),
        is_viable_d=(
            "NOT list_contains(D.edge_ids, e.edge_id) "
            f"AND D.prod_weight * e.weight <= {bound}"
        ),
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
