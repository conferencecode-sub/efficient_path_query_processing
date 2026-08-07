#!/usr/bin/env python3
"""Verify that the seeded 2-fragment split (F1 -> F2, merged) returns exactly the
same result set as the ordinary left-to-right baseline, over the same
(min_length, max_length) window and the same all-vertices start set.

This is the actual experiment: correctness is supposed to hold by construction
(see the correctness note in section 3 of ../navigation_style_experiment.md), so
a MISMATCH here means a bug in the split, not a research finding.
"""
import argparse

import duckdb

import baseline_monolithic
import seeded_split
from common import DEFAULT_EDGES, DEFAULT_NODES, load_data


def main():
    parser = argparse.ArgumentParser(description="Check baseline vs. seeded-split equivalence for Q1")
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
    split_count, f1_wall, f2_wall, boundary_rows = seeded_split.run(
        conn, args.min_length, args.max_length, "split_results", start_vertex=args.start_vertex)

    # EXCEPT ALL (bag semantics), not EXCEPT (set semantics): without edge_ids in
    # this simplified query, distinct walks can share the same
    # (start, end_vertex, path_length, last_time) signature, so a plain EXCEPT
    # would silently dedupe them and can report MATCH even when the multisets
    # -- and therefore the path counts -- differ.
    only_in_mono = conn.execute(
        "SELECT COUNT(*) FROM (SELECT * FROM mono_results EXCEPT ALL SELECT * FROM split_results)"
    ).fetchone()[0]
    only_in_split = conn.execute(
        "SELECT COUNT(*) FROM (SELECT * FROM split_results EXCEPT ALL SELECT * FROM mono_results)"
    ).fetchone()[0]

    print(f"min_length={args.min_length}, max_length={args.max_length}")
    print(f"  baseline (monolithic):  {mono_count} paths in {1000 * mono_wall:.2f}ms")
    print(f"  seeded split + merge:   {split_count} paths in {1000 * (f1_wall + f2_wall):.2f}ms "
          f"(F1 seam rows: {boundary_rows})")
    if only_in_mono == 0 and only_in_split == 0:
        print("  MATCH: result sets are identical.")
    else:
        print(f"  MISMATCH: {only_in_mono} rows only in baseline, {only_in_split} rows only in split.")


if __name__ == '__main__':
    main()
