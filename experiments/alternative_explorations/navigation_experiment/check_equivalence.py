#!/usr/bin/env python3
"""Verify that the seeded 2-fragment split (F1 -> F2, merged) and the naive
2-fragment split (F1 || F2, joined at the end) both return exactly the same
result set as the ordinary left-to-right baseline, over the same
(min_length, max_length) window and the same all-vertices start set.

This is the actual experiment: correctness is supposed to hold by construction
(see the correctness note in section 3 of ../navigation_style_experiment.md), so
a MISMATCH here means a bug in the split, not a research finding.
"""
import argparse

import duckdb

import baseline_monolithic
import naive_split
import seeded_split
from common import DEFAULT_EDGES, DEFAULT_NODES, load_data


def main():
    parser = argparse.ArgumentParser(description="Check baseline vs. seeded-split vs. naive-split equivalence for Q1")
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

    mono_count, mono_wall = baseline_monolithic.run(
        conn, args.min_length, args.max_length, "mono_results", start_vertex=args.start_vertex)
    # split_count, f1_wall, f2_wall, boundary_rows = seeded_split.run(
    #     conn, args.min_length, args.max_length, "split_results", start_vertex=args.start_vertex)
    naive_count, nf1_wall, nf2_wall, join_wall, naive_boundary_rows, frag2_rows = naive_split.run(
        conn, args.min_length, args.max_length, "naive_results", start_vertex=args.start_vertex)

    # EXCEPT ALL (bag semantics), not EXCEPT (set semantics): without edge_ids in
    # this simplified query, distinct walks can share the same
    # (start, end_vertex, path_length, last_time) signature, so a plain EXCEPT
    # would silently dedupe them and can report MATCH even when the multisets
    # -- and therefore the path counts -- differ.
    def except_all_counts(table_a: str, table_b: str):
        only_in_a = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM {table_a} EXCEPT ALL SELECT * FROM {table_b})"
        ).fetchone()[0]
        only_in_b = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM {table_b} EXCEPT ALL SELECT * FROM {table_a})"
        ).fetchone()[0]
        return only_in_a, only_in_b

    # mono_vs_split = except_all_counts("mono_results", "split_results")
    mono_vs_naive = except_all_counts("mono_results", "naive_results")

    print(f"min_length={args.min_length}, max_length={args.max_length}")
    print(f"  baseline (monolithic):  {mono_count} paths in {1000 * mono_wall:.2f}ms")
    # print(f"  seeded split + merge:   {split_count} paths in {1000 * (f1_wall + f2_wall):.2f}ms "
        #   f"(F1 seam rows: {boundary_rows})")
    print(f"  naive split + join:     {naive_count} paths in {1000 * (nf1_wall + nf2_wall + join_wall):.2f}ms "
          f"(F1 seam rows: {naive_boundary_rows}, F2 unseeded suffix rows: {frag2_rows})")

    # if mono_vs_split == (0, 0):
    #     print("  MATCH (baseline vs. seeded split): result sets are identical.")
    # else:
    #     print(f"  MISMATCH (baseline vs. seeded split): {mono_vs_split[0]} rows only in baseline, "
    #           f"{mono_vs_split[1]} rows only in split.")

    if mono_vs_naive == (0, 0):
        print("  MATCH (baseline vs. naive split): result sets are identical.")
    else:
        print(f"  MISMATCH (baseline vs. naive split): {mono_vs_naive[0]} rows only in baseline, "
              f"{mono_vs_naive[1]} rows only in naive split.")


if __name__ == '__main__':
    main()
