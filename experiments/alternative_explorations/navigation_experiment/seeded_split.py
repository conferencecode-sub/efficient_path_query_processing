#!/usr/bin/env python3
"""Two-fragment seeded split + merge for Q1 (regex + monotonic-time only), all
vertices as start. This is plan (iii) in ../navigation_style_experiment.md.

Splits the NFA at the {0,1} / {1,2} seam (the transfer|purchase|sale ->
phishing|scam transition, the rare label in LG.csv). F1 explores states {0,1}
from every vertex and materializes its boundary rows at state 1 (nothing is
summarized away at the seam). F2 is seeded directly from those boundary rows
and explores states {1,2} to the accepting state, checking the seam's
monotonic-time constraint inline during its own recursion rather than at the
end.
"""
import argparse
import time

import duckdb

from common import DEFAULT_EDGES, DEFAULT_NODES, load_data


def run(conn: duckdb.DuckDBPyConnection, min_length: int, max_length: int,
        result_table: str = "split_results", start_vertex: int = None):
    # Fragment 1: states {0,1}, from every vertex (or just start_vertex, if given).
    node_filter = f"WHERE n.id = {start_vertex}" if start_vertex is not None else ""
    frag1_query = f"""
    WITH RECURSIVE frag1 AS (
        SELECT n.id AS start, n.id AS v, 0 AS state,
               CAST(-99999 AS BIGINT) AS last_time, 0 AS path_length
        FROM nodes n
        {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, t.timestamp_ms, p.path_length + 1
        FROM frag1 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state = 1
          AND p.path_length < {max_length} - 1  -- a prefix longer than max_length - 1
                                                 -- can never fit a >=1-hop suffix
          AND t.timestamp_ms > p.last_time
    )
    SELECT start, v, state, last_time, path_length AS prefix_length
    FROM frag1 WHERE state = 1
    """
    conn.execute("DROP TABLE IF EXISTS frag1_boundary")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag1_boundary AS {frag1_query}")
    f1_wall = time.perf_counter() - t0
    boundary_rows = conn.execute("SELECT COUNT(*) FROM frag1_boundary").fetchone()[0]

    # Seam join: hand F1's boundary rows to F2 as its seed. Materialized as its
    # own table (rather than folded into frag2's recursive base case) so this
    # step's cost -- the actual F1/F2 join -- can be timed separately from the
    # recursive expansion that follows it.
    conn.execute("DROP TABLE IF EXISTS frag2_seed")
    t0 = time.perf_counter()
    conn.execute("""
        CREATE TABLE frag2_seed AS
        SELECT start, v, state, last_time, prefix_length, 0 AS suffix_length
        FROM frag1_boundary
    """)
    join_wall = time.perf_counter() - t0

    # Fragment 2: states {1,2}, seeded from F1's boundary rows via frag2_seed.
    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT * FROM frag2_seed
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, t.timestamp_ms, p.prefix_length, p.suffix_length + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 1  -- F1 already enumerates every {0,1} prefix depth as a
                                 -- separate boundary row; F2 re-taking a 1->1 hop would
                                 -- double-count a walk F1 already produced at one prefix
                                 -- depth deeper
          AND p.prefix_length + p.suffix_length < {max_length}  -- prune by total length,
                                                                 -- not suffix length alone
          AND t.timestamp_ms > p.last_time
    )
    SELECT start, v AS end_vertex, prefix_length + suffix_length AS path_length, last_time
    FROM frag2
    WHERE state = 2
      AND prefix_length + suffix_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {frag2_query}")
    f2_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f1_wall, join_wall, f2_wall, boundary_rows


def main():
    parser = argparse.ArgumentParser(
        description="Seeded 2-fragment split + merge for Q1, regex + monotonic time only, all vertices")
    parser.add_argument('--nodes', default=DEFAULT_NODES)
    parser.add_argument('--edges', default=DEFAULT_EDGES)
    parser.add_argument('--min-length', type=int, default=2)
    parser.add_argument('--max-length', type=int, default=6)
    parser.add_argument('--start-vertex', type=int, default=None,
                         help="Restrict to a single start vertex instead of all vertices "
                              "(lets --max-length go deeper without the all-vertices blowup)")
    args = parser.parse_args()

    conn = duckdb.connect(':memory:')
    load_data(conn, args.nodes, args.edges)

    count, f1_wall, join_wall, f2_wall, boundary_rows = run(
        conn, args.min_length, args.max_length, start_vertex=args.start_vertex)
    total = f1_wall + join_wall + f2_wall
    print(f"[seeded-split] max_length={args.max_length}: {count} accepted paths in {1000 * total:.2f}ms "
          f"(F1={1000 * f1_wall:.2f}ms producing {boundary_rows} seam rows, "
          f"join={1000 * join_wall:.2f}ms, F2={1000 * f2_wall:.2f}ms)")

if __name__ == '__main__':
    main()