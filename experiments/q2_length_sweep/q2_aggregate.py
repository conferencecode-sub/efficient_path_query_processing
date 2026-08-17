"""Q2's real selective aggregate ("two-color trail"), hand-translated into
the new compiler's Definition-8 shape from `ReCAP/q2/recap_two_color_trail_
inline.py`'s own hardcoded SQL (the source of truth for what Q2 actually
checks -- there's no existing compiler-side Q2 entry).

Q2 has no label regex at all (every edge in `ReCAP/simple_dataset/edges.csv`
has the same trivial label) -- it's a plain traversal with two constraints:
  - trail: no edge id reused (FR-13(ii)'s own `trail_via_edge_ids` shape).
  - "two-color": the path must contain at least one pair of *consecutive*
    edges sharing the same color. Unlike Q1's constraints, this is not a
    per-hop veto (a path missing the property so far isn't doomed -- a
    later hop could still supply it), so it's tracked via `update_d` and
    checked only in `is_viable_d_final`, not `is_viable_d`. `is_viable_d`
    here only carries the trail check.

`constraint_done` is written as `D.constraint_done OR (D.last_color IS NOT
NULL AND e.color = D.last_color)` rather than the inline script's CASE
form, specifically to avoid SQL's three-valued logic: `D.last_color =
e.color` is NULL (not FALSE) before the first hop sets a color, and `FALSE
OR NULL` is NULL, not FALSE -- the explicit `IS NOT NULL` guard keeps
`constraint_done` a real boolean at every step instead of ever going NULL.
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate


def q2_aggregate() -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("last_color", "VARCHAR"),
            DictionaryKey("constraint_done", "BOOLEAN"),
        ),
        init_d="{edge_ids: [], last_color: NULL, constraint_done: FALSE}",
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "last_color: e.color, "
            "constraint_done: D.constraint_done OR "
            "(D.last_color IS NOT NULL AND e.color = D.last_color)}"
        ),
        is_viable_d="NOT list_contains(D.edge_ids, e.edge_id)",
        is_viable_d_final="D.constraint_done",
        finalize_d="D",
        factorized=True,
    )
