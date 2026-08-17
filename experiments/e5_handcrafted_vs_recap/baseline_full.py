#!/usr/bin/env python3
"""E5 config 1 surrogate for sanity-checking only: a monolithic, single-
recursive-CTE version of Q1's FULL constraint set, seeded from one start
vertex -- structurally identical to `ReCAP/q1/recap_gen_recap_inline.py`
(same WHERE-clause shape, same per-transition `last_risk >= 40` gate),
just re-hosted on `common_full.py`'s loader so it can run in the same
process as `split_full.py` for a direct equivalence check.

**Not the actual E5 config-1 number** -- `run_e5.py` calls
`ReCAP/q1/recap_gen_recap_inline.py` directly for the real reported
"handcrafted" timing (avoiding two slightly-different re-implementations
diverging silently). This script exists purely to let `check_equivalence_
full.py` diff `split_full.py`'s output against a known-correct reference
without cross-process CSV plumbing.
"""
import time

import duckdb

from common_full import LAST_RISK_GATE, MIN_AMOUNT, RISK_RANGE_BOUND


def run(conn: duckdb.DuckDBPyConnection, min_length: int, max_length: int,
        start_vertex: int, result_table: str = "mono_full_results"):
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT {start_vertex} AS v, 0 AS state,
               NULL::DOUBLE AS max_risk, NULL::DOUBLE AS min_risk, NULL::DOUBLE AS last_risk,
               CAST(-99999 AS BIGINT) AS last_time, NULL::VARCHAR AS region,
               CAST(0.0 AS DOUBLE) AS amount, [] AS edge_ids, 0 AS path_length
        UNION ALL
        SELECT t.dst AS v, n.to_state AS state,
               GREATEST(p.max_risk, t.risk_score) AS max_risk,
               LEAST(p.min_risk, t.risk_score) AS min_risk,
               t.risk_score AS last_risk,
               t.timestamp_ms AS last_time,
               COALESCE(p.region, t.location_region) AS region,
               p.amount + t.amount AS amount,
               list_append(p.edge_ids, t.edge_id) AS edge_ids,
               p.path_length + 1 AS path_length
        FROM paths p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges n ON p.state = n.from_state AND t.label = n.label
        WHERE p.path_length < {max_length}
          AND (p.region IS NULL OR t.location_region = p.region)
          AND t.timestamp_ms > p.last_time
          AND NOT list_contains(p.edge_ids, t.edge_id)
          AND (CASE
                 WHEN p.state IN (0, 1) AND n.to_state = 1 THEN
                   p.max_risk IS NULL OR
                   GREATEST(p.max_risk, t.risk_score) - LEAST(p.min_risk, t.risk_score) <= {RISK_RANGE_BOUND}
                 WHEN p.state = 1 AND n.to_state = 2 THEN p.last_risk >= {LAST_RISK_GATE}
                 WHEN p.state = 2 AND n.to_state = 2 THEN TRUE
                 ELSE FALSE
               END)
    )
    SELECT v AS end_vertex, path_length, amount
    FROM paths
    WHERE state = 2 AND amount >= {MIN_AMOUNT} AND path_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {query}")
    wall_time = time.perf_counter() - t0
    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, wall_time
