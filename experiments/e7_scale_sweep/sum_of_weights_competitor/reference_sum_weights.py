"""A monolithic, hand-written recursive-CTE reference for the sum-of-weights
trail query, mirroring `experiments/e6_finbench_sf10/reference_baseline.py`'s
own "verify against a known-correct baseline before trusting the aggregate-
driven query at scale" discipline -- this is a plain DuckDB query, entirely
independent of `recap_compiler`'s Stage E/F code generation (no shared code
path with `sum_weight_aggregate.py`'s `is_viable_d`/`update_d` bodies beyond
both expressing the same query semantics by hand).

The bound is applied inline in the recursive term (`p.sum_weight + e.weight
<= bound`), not just at the final SELECT -- this is deliberate, not just an
optimization: `sum_weight_aggregate.py`'s docstring proves the two are
mathematically identical here (every edge weight is strictly positive, so
the running sum is non-decreasing and the final total is always the
maximum prefix sum), and applying it inline is also what keeps this
reference query itself safe to run at scale -- an *unfiltered* trail
enumeration over Datagen-7.7 from a real start vertex was confirmed
directly (during this experiment's own threshold selection) to blow up to
100+ GB resident memory by length 6-7, since trail state (a growing
edge-id list per row) compounds with an explosive unconstrained trail
count. Applying the bound inline keeps the candidate space bounded to only
what would actually be accepted, matching what a real early-filtering
engine (ReCAP) explores in the first place.
"""
from __future__ import annotations

import duckdb


def sum_weight_reference(conn: duckdb.DuckDBPyConnection, start_vertex: int, min_length: int,
                          max_length: int, bound: float) -> int:
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT CAST({start_vertex} AS BIGINT) AS v, CAST(0.0 AS DOUBLE) AS sum_weight,
               [] AS edge_ids, 0 AS path_length
        UNION ALL
        SELECT e.dst, p.sum_weight + e.weight, list_append(p.edge_ids, e.edge_id), p.path_length + 1
        FROM paths p
        JOIN edges e ON e.src = p.v
        WHERE p.path_length < {max_length}
          AND NOT list_contains(p.edge_ids, e.edge_id)
          AND p.sum_weight + e.weight <= {bound}
    )
    SELECT COUNT(*) FROM paths WHERE path_length BETWEEN {min_length} AND {max_length}
    """
    return conn.execute(query).fetchone()[0]
