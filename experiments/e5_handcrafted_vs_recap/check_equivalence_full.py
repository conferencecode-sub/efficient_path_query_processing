#!/usr/bin/env python3
"""Verifies `split_full.py` (E5 config 3) returns exactly the same result
set as `baseline_full.py` (a monolithic re-implementation of Q1's full
constraint set, structurally identical to `ReCAP/q1/recap_gen_recap_inline.py`)
over the same (min_length, max_length, start_vertex). A MISMATCH means a bug
in the split's constraint placement, not a research finding -- see
`split_full.py`'s docstring for why correctness is expected by construction.
"""
import argparse

import duckdb

import baseline_full
import split_full
from common_full import DEFAULT_EDGES, DEFAULT_NODES, load_data


def main():
    parser = argparse.ArgumentParser(description="Check baseline_full vs. split_full equivalence for Q1")
    parser.add_argument('--nodes', default=DEFAULT_NODES)
    parser.add_argument('--edges', default=DEFAULT_EDGES)
    parser.add_argument('--min-length', type=int, default=2)
    parser.add_argument('--max-length', type=int, default=6)
    parser.add_argument('--start-vertex', type=int, default=383)
    args = parser.parse_args()

    conn = duckdb.connect(':memory:')
    load_data(conn, args.nodes, args.edges)

    mono_count, mono_wall = baseline_full.run(
        conn, args.min_length, args.max_length, args.start_vertex, "mono_full_results")
    split_count, f1_wall, f2_wall, boundary_rows = split_full.run(
        conn, args.min_length, args.max_length, args.start_vertex, "split_full_results")

    # EXCEPT ALL (bag semantics) on (end_vertex, path_length, amount) -- amount
    # is included specifically so two distinct paths with the same endpoint/
    # length but different accumulated amounts aren't silently conflated.
    def except_all_counts(table_a: str, table_b: str):
        only_in_a = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM {table_a} EXCEPT ALL SELECT * FROM {table_b})"
        ).fetchone()[0]
        only_in_b = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM {table_b} EXCEPT ALL SELECT * FROM {table_a})"
        ).fetchone()[0]
        return only_in_a, only_in_b

    diff = except_all_counts("mono_full_results", "split_full_results")

    print(f"min_length={args.min_length}, max_length={args.max_length}, start_vertex={args.start_vertex}")
    print(f"  baseline (monolithic, full constraints): {mono_count} paths in {1000 * mono_wall:.2f}ms")
    print(f"  split (full constraints):                {split_count} paths in "
          f"{1000 * (f1_wall + f2_wall):.2f}ms (F1={1000*f1_wall:.2f}ms/{boundary_rows} seam rows, "
          f"F2={1000*f2_wall:.2f}ms)")

    if diff == (0, 0):
        print("  MATCH: result sets are identical.")
    else:
        print(f"  MISMATCH: {diff[0]} rows only in baseline, {diff[1]} rows only in split.")


if __name__ == '__main__':
    main()
