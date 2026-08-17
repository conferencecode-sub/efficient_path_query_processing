#!/usr/bin/env python3
"""E5 config 3: Q1 (FULL constraint set) evaluated via a two-fragment split
at the NFA's rare-label seam (`(transfer|purchase|sale)+` -> `(phishing|scam)+`),
extending `alternative_explorations/navigation_experiment/naive_split.py`'s
technique (which only handled regex + monotonic-time) to every real Q1
constraint: trail, region, risk-range, the last-risk gateway, and amount.

**Constraint placement, worked out from `ReCAP/q1/recap_gen_recap_inline.py`'s
exact semantics (the source of truth), not guessed:**
  - trail, region, monotonic-time, risk-range (max-min<=20) are all
    per-hop-checkable and enforced inline within whichever fragment the hop
    belongs to.
  - **Trail needs no cross-fragment bookkeeping.** F1's edges are always
    normal-labeled, F2's are always fraud-labeled (the NFA structurally
    partitions the edge set by label), so no single edge_id can appear in
    both fragments of the same path -- each fragment enforces trail only
    against its own edge_ids, independently.
  - **region and amount DO need cross-fragment state**, carried on F1's
    boundary rows into F2: region must still hold for every fraud edge too
    (the original's `is_viable_d` checks it unconditionally, not just for
    normal edges), and amount keeps accumulating across both fragments.
  - **The `last_risk >= 40` gateway is the interesting one.** In the
    hand-inlined query it's checked in the recursive WHERE exactly at the
    state 1->2 transition edge (a per-hop check that happens to fire once
    per path). In the new compiler's `q1_aggregate.py` it's pushed into
    `is_viable_d_final` (checked only after the whole path, including the
    entire fraud suffix, is already built) -- because Q1's aggregate is
    `factorized=True`, which has no access to NFA state and so can't
    express "check only at this specific transition." The split naturally
    recovers the earlier, tighter timing for free: F1's own boundary rows
    *are* the state-1-about-to-transition point, so filtering
    `frag1_boundary WHERE last_risk >= 40` before ever generating F2 rows
    is exactly the hand-inlined query's own enforcement point, and prunes
    strictly more eagerly than the factorized optimized query does.
"""
import argparse
import time

import duckdb

from common_full import DEFAULT_EDGES, DEFAULT_NODES, LAST_RISK_GATE, MIN_AMOUNT, RISK_RANGE_BOUND, load_data


def run(conn: duckdb.DuckDBPyConnection, min_length: int, max_length: int,
        start_vertex: int, result_table: str = "split_full_results"):
    # Fragment 1: states {0,1}, from the single start vertex.
    frag1_query = f"""
    WITH RECURSIVE frag1 AS (
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
        FROM frag1 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges n ON p.state = n.from_state AND t.label = n.label
        WHERE n.to_state = 1  -- stay within the {{0,1}} fragment only
          AND p.path_length < {max_length} - 1  -- reserve room for >=1 fraud hop
          AND (p.region IS NULL OR t.location_region = p.region)
          AND t.timestamp_ms > p.last_time
          AND NOT list_contains(p.edge_ids, t.edge_id)
          AND (p.max_risk IS NULL OR
               GREATEST(p.max_risk, t.risk_score) - LEAST(p.min_risk, t.risk_score) <= {RISK_RANGE_BOUND})
    )
    SELECT v, region, last_time, amount, path_length AS prefix_length
    FROM frag1
    WHERE state = 1 AND last_risk >= {LAST_RISK_GATE}
    """
    conn.execute("DROP TABLE IF EXISTS frag1_boundary_full")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag1_boundary_full AS {frag1_query}")
    f1_wall = time.perf_counter() - t0
    boundary_rows = conn.execute("SELECT COUNT(*) FROM frag1_boundary_full").fetchone()[0]

    # Fragment 2: states {1,2}, seeded from F1's (already gate-filtered) boundary rows.
    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT v, 1 AS state, region, last_time, amount, [] AS edge_ids,
               prefix_length, 0 AS suffix_length
        FROM frag1_boundary_full
        UNION ALL
        SELECT t.dst AS v, n.to_state AS state,
               p.region AS region,  -- already established by F1; never changes in F2
               t.timestamp_ms AS last_time,
               p.amount + t.amount AS amount,
               list_append(p.edge_ids, t.edge_id) AS edge_ids,
               p.prefix_length AS prefix_length,
               p.suffix_length + 1 AS suffix_length
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges n ON p.state = n.from_state AND t.label = n.label
        WHERE n.to_state <> 1  -- fraud-labeled edges only (F1 already exhausted the {{0,1}}
                                -- fragment at every valid prefix depth)
          AND p.prefix_length + p.suffix_length < {max_length}
          AND (t.location_region = p.region)  -- still enforced on fraud edges too
          AND t.timestamp_ms > p.last_time
          AND NOT list_contains(p.edge_ids, t.edge_id)  -- F2's own trail, independent of F1's
                                                          -- (labels partition the edge set, so no
                                                          -- edge can appear in both fragments)
    )
    SELECT v AS end_vertex, prefix_length + suffix_length AS path_length, amount
    FROM frag2
    WHERE state = 2
      AND amount >= {MIN_AMOUNT}
      AND prefix_length + suffix_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {frag2_query}")
    f2_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f1_wall, f2_wall, boundary_rows


def main():
    parser = argparse.ArgumentParser(description="E5 config 3: Q1 (full constraints) via seam split")
    parser.add_argument('--nodes', default=DEFAULT_NODES)
    parser.add_argument('--edges', default=DEFAULT_EDGES)
    parser.add_argument('--min-length', type=int, default=2)
    parser.add_argument('--max-length', type=int, default=6)
    parser.add_argument('--start-vertex', type=int, default=383)
    args = parser.parse_args()

    conn = duckdb.connect(':memory:')
    load_data(conn, args.nodes, args.edges)

    count, f1_wall, f2_wall, boundary_rows = run(
        conn, args.min_length, args.max_length, args.start_vertex)
    total = f1_wall + f2_wall
    print(f"[split-full] max_length={args.max_length}: {count} accepted paths in {1000 * total:.2f}ms "
          f"(F1={1000 * f1_wall:.2f}ms producing {boundary_rows} gate-passing seam rows, "
          f"F2={1000 * f2_wall:.2f}ms)")


if __name__ == '__main__':
    main()
