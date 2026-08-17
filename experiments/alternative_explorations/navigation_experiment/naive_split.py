#!/usr/bin/env python3
"""Two-fragment naive (unseeded) split + join for Q1 (regex + monotonic-time
only), all vertices as start. This is plan (ii) in ../navigation_style_experiment.md.

Unlike seeded_split.py, F2 does not know anything about F1's walks: it explores
states {1,2} independently from every vertex, remembering only its own first
and last vertex/timestamp. The cross-fragment monotonicity constraint (F1's
exit time must precede F2's entry time) is checked once, at a final join,
instead of inline on every hop. This exists to measure how much bigger F2's
intermediate result gets, and how much slower the overall query gets, when
seam-level filtering is deferred to the end rather than threaded through the
recursion.
"""
import argparse
import time

import duckdb

from common import DEFAULT_EDGES, DEFAULT_NODES, load_data


def run(conn: duckdb.DuckDBPyConnection, min_length: int, max_length: int,
        result_table: str = "naive_results", start_vertex: int = None):
    # Fragment 1: states {0,1}, from every vertex (or just start_vertex, if given).
    # Identical in shape to seeded_split.py's frag1 -- F1 itself isn't "naive",
    # only F2 is; --start-vertex only restricts where F1 starts.
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
          AND p.path_length < {max_length} - 1
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

    # Fragment 2: states {1,2}, unseeded -- starts from every vertex in the
    # dataset (a vertex with no transfer/purchase/sale-only outgoing edge simply
    # produces no rows on the first recursive hop, so this is a superset of the
    # true seam-vertex set, not a bug). Remembers first_v/first_time (frozen at
    # the very first hop) alongside the running v/last_time, since both ends are
    # needed for the join back to F1.
    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT n.id AS first_v, n.id AS v, 1 AS state,
               CAST(-99999 AS BIGINT) AS first_time, CAST(-99999 AS BIGINT) AS last_time,
               0 AS suffix_length
        FROM nodes n
        UNION ALL
        SELECT p.first_v, t.dst, nf.to_state,
               CASE WHEN p.suffix_length = 0 THEN t.timestamp_ms ELSE p.first_time END,
               t.timestamp_ms, p.suffix_length + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 1  -- stay inside the {{1,2}} fragment only
          AND p.suffix_length < {max_length} - 1  -- prefix needs >=1 hop of its own
          AND t.timestamp_ms > p.last_time
    )
    SELECT first_v, v, first_time, last_time, suffix_length
    FROM frag2 WHERE state = 2
    """
    conn.execute("DROP TABLE IF EXISTS frag2_unseeded")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag2_unseeded AS {frag2_query}")
    f2_wall = time.perf_counter() - t0
    frag2_rows = conn.execute("SELECT COUNT(*) FROM frag2_unseeded").fetchone()[0]

    # Merge: join on the seam vertex, with the cross-fragment monotonicity
    # check (F1's exit time < F2's entry time) as an explicit join predicate --
    # this is the check that seeded_split.py instead enforces inline on every
    # hop of F2's own recursion.
    join_query = f"""
    SELECT f1.start AS start, f2.v AS end_vertex,
           f1.prefix_length + f2.suffix_length AS path_length, f2.last_time AS last_time
    FROM frag1_boundary f1
    JOIN frag2_unseeded f2
      ON f1.v = f2.first_v
     AND f1.last_time < f2.first_time
    WHERE f1.prefix_length + f2.suffix_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {join_query}")
    join_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f1_wall, f2_wall, join_wall, boundary_rows, frag2_rows


def main():
    parser = argparse.ArgumentParser(
        description="Naive (unseeded) 2-fragment split + final join for Q1, regex + monotonic time only, all vertices")
    parser.add_argument('--nodes', default=DEFAULT_NODES)
    parser.add_argument('--edges', default=DEFAULT_EDGES)
    parser.add_argument('--min-length', type=int, default=2)
    parser.add_argument('--max-length', type=int, default=6)
    parser.add_argument('--start-vertex', type=int, default=None,
                         help="Restrict F1 to a single start vertex instead of all vertices "
                              "(F2 always explores from every vertex, unseeded, regardless)")
    args = parser.parse_args()

    conn = duckdb.connect(':memory:')
    load_data(conn, args.nodes, args.edges)

    count, f1_wall, f2_wall, join_wall, boundary_rows, frag2_rows = run(
        conn, args.min_length, args.max_length, start_vertex=args.start_vertex)
    total = f1_wall + f2_wall + join_wall
    print(f"[naive-split] max_length={args.max_length}: {count} accepted paths in {1000 * total:.2f}ms "
          f"(F1={1000 * f1_wall:.2f}ms producing {boundary_rows} seam rows, "
          f"F2={1000 * f2_wall:.2f}ms producing {frag2_rows} unseeded suffix rows, "
          f"join={1000 * join_wall:.2f}ms)")


if __name__ == '__main__':
    main()
