"""Independent DuckDB reference for Q4 (max-min-timestamp trail) against the
real LDBC100 dataset, for cross-validating Kuzu/Neo4j/Memgraph.

`ReCAP/q4/duckdb_max_min_trail_inline.py` isn't reusable as-is here: it
requires NFA nodes/edges CSVs (Q4 has no regex -- single accepting state,
same "trivial label" pathway used everywhere else in this project), declares
`id`/`dst` as INTEGER (LDBC100's ids are BIGINT, up to ~4.4e13), and
hardcodes bound=20 for a 0-100 toy 'weight' column. This is a from-scratch,
independent implementation of the same semantics (trail + max-min range <=
bound), not a call into the compiler or into ReCAP-new -- deliberately, so
it serves as a genuine independent check, same role duckdb_two_color_trail
_inline.py / recap_monotonic_trail_inline.py played for Q2/Q3.

Dataset: experiments/datasets/ldbc100_engine_ready/edges.csv (edge_id, src,
dst, weight=epoch_ms(creation_date), color=placeholder, unused here).
Start vertex 24189256063073 (out-degree 18, the median over 389,944
vertices -- same choice as q4_length_sweep's ReCAP-new runner and the
Kuzu/Neo4j/Memgraph harnesses for this dataset).
"""
from __future__ import annotations

import argparse
import time

import duckdb

EDGES = "../datasets/ldbc100_engine_ready/edges.csv"
START_VERTEX = 24189256063073
TWO_WEEKS_MS = 1_209_600_000


def run(conn, min_len: int, max_len: int, starter: int, bound: int):
    query = f"""
        WITH RECURSIVE paths AS (
            SELECT
                CAST({starter} AS BIGINT) AS current_node,
                CAST([] AS BIGINT[]) AS edge_path,
                CAST([] AS DOUBLE[]) AS weights,
                0 AS path_length
            UNION ALL
            SELECT
                e.dst AS current_node,
                list_append(p.edge_path, e.edge_id) AS edge_path,
                list_append(p.weights, e.weight) AS weights,
                p.path_length + 1 AS path_length
            FROM paths p
            JOIN edges e ON e.src = p.current_node
            WHERE p.path_length < {max_len}
        )
        SELECT COUNT(*)
        FROM paths
        WHERE path_length >= {min_len}
          AND len(edge_path) = len(list_distinct(edge_path))
          AND list_max(weights) - list_min(weights) <= {bound}
    """
    start = time.time()
    result = conn.execute(query).fetchone()[0]
    elapsed = time.time() - start
    return result, elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-len", type=int, default=2)
    parser.add_argument("--max-len", type=int, default=3)
    parser.add_argument("--starter", type=int, default=START_VERTEX)
    parser.add_argument("--bound", type=int, default=TWO_WEEKS_MS)
    args = parser.parse_args()

    conn = duckdb.connect()
    conn.execute(
        f"CREATE TABLE edges AS SELECT edge_id, src::BIGINT AS src, dst::BIGINT AS dst, "
        f"weight::DOUBLE AS weight FROM read_csv('{EDGES}', header=true)")
    conn.execute("CREATE INDEX idx_edges_src ON edges(src)")

    for length in range(args.min_len, args.max_len + 1):
        result, elapsed = run(conn, args.min_len, length, args.starter, args.bound)
        print(f"length={length}: {result} paths, runtime={elapsed*1000:.2f}ms")


if __name__ == "__main__":
    main()
