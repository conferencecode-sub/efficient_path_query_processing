#!/usr/bin/env python3
"""Baseline: the ordinary left-to-right ReCAP query for Q1, simplified to regex +
monotonic-time only, seeded from every vertex in the dataset (not one fixed
starter_node). This is plan (i) in ../navigation_style_experiment.md."""
import argparse
import time

import duckdb

from common import DEFAULT_EDGES, DEFAULT_NODES, load_data


def run(conn: duckdb.DuckDBPyConnection, min_length: int, max_length: int,
        result_table: str = "mono_results", start_vertex: int = None):
    node_filter = f"WHERE n.id = {start_vertex}" if start_vertex is not None else ""
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT n.id AS start, n.id AS v, 0 AS state,
               CAST(-99999 AS BIGINT) AS last_time, 0 AS path_length
        FROM nodes n
        {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, t.timestamp_ms, p.path_length + 1
        FROM paths p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE p.path_length < {max_length}
          AND t.timestamp_ms > p.last_time
    )
    SELECT start, v AS end_vertex, path_length, last_time
    FROM paths
    WHERE state = 2 AND path_length >= {min_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {query}")
    wall_time = time.perf_counter() - t0
    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, wall_time


def main():
    parser = argparse.ArgumentParser(
        description="Baseline monolithic (left-to-right) Q1, regex + monotonic time only, all vertices")
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

    count, wall_time = run(conn, args.min_length, args.max_length, start_vertex=args.start_vertex)
    print(f"[baseline] max_length={args.max_length}: {count} accepted paths in {1000 * wall_time:.2f}ms")


if __name__ == '__main__':
    main()
