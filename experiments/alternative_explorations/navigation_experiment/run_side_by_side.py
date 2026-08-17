#!/usr/bin/env python3
"""Ad hoc driver: run baseline / naive-split / seeded-split at each path
length individually (not the cumulative min/max window check_equivalence.py
uses) so runtime and result counts can be shown side by side per length,
across one or more start vertices. With --csv-out, writes one combined CSV
(one row per (start_vertex, length)) to the given path.
"""
import argparse
import csv
import os
import time

import duckdb

import baseline_monolithic
import naive_split
import seeded_split
from common import DEFAULT_EDGES, DEFAULT_NODES, load_data

FIELDNAMES = [
    "start_vertex", "length",
    "mono_count", "mono_ms",
    "naive_count", "naive_ms", "naive_f1_ms", "naive_f2_ms", "naive_join_ms",
    "naive_boundary_rows", "naive_frag2_rows",
    "split_count", "split_ms", "split_f1_ms", "split_join_ms", "split_f2_ms",
    "split_boundary_rows",
    "mono_vs_naive_match", "mono_vs_split_match",
]


def run_one_vertex(conn, lengths, start_vertex):
    rows = []
    for ell in lengths:
        mono_count, mono_wall = baseline_monolithic.run(
            conn, ell, ell, "mono_results", start_vertex=start_vertex)
        naive_count, nf1_wall, nf2_wall, join_wall, naive_boundary_rows, frag2_rows = naive_split.run(
            conn, ell, ell, "naive_results", start_vertex=start_vertex)
        split_count, sf1_wall, sjoin_wall, sf2_wall, split_boundary_rows = seeded_split.run(
            conn, ell, ell, "split_results", start_vertex=start_vertex)

        def except_all(a, b):
            only_a = conn.execute(f"SELECT COUNT(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b})").fetchone()[0]
            only_b = conn.execute(f"SELECT COUNT(*) FROM (SELECT * FROM {b} EXCEPT ALL SELECT * FROM {a})").fetchone()[0]
            return only_a, only_b

        mono_vs_naive = except_all("mono_results", "naive_results")
        mono_vs_split = except_all("mono_results", "split_results")

        rows.append(dict(
            start_vertex=start_vertex, length=ell,
            mono_count=mono_count, mono_ms=1000 * mono_wall,
            naive_count=naive_count, naive_ms=1000 * (nf1_wall + nf2_wall + join_wall),
            naive_f1_ms=1000 * nf1_wall, naive_f2_ms=1000 * nf2_wall, naive_join_ms=1000 * join_wall,
            naive_boundary_rows=naive_boundary_rows, naive_frag2_rows=frag2_rows,
            split_count=split_count, split_ms=1000 * (sf1_wall + sjoin_wall + sf2_wall),
            split_f1_ms=1000 * sf1_wall, split_join_ms=1000 * sjoin_wall, split_f2_ms=1000 * sf2_wall,
            split_boundary_rows=split_boundary_rows,
            mono_vs_naive_match=(mono_vs_naive == (0, 0)),
            mono_vs_split_match=(mono_vs_split == (0, 0)),
        ))
    return rows


def print_rows(rows):
    print(f"{'len':>3} {'mono(#)':>8} {'mono(ms)':>10} | {'naive(#)':>9} {'naive(ms)':>10} {'F1/F2/join(ms)':>22} | "
          f"{'split(#)':>9} {'split(ms)':>10} {'F1/join/F2(ms)':>22} | match")
    for r in rows:
        match_str = f"naive={'OK' if r['mono_vs_naive_match'] else 'MISMATCH'} split={'OK' if r['mono_vs_split_match'] else 'MISMATCH'}"
        print(f"{r['length']:>3} {r['mono_count']:>8} {r['mono_ms']:>10.2f} | "
              f"{r['naive_count']:>9} {r['naive_ms']:>10.2f} "
              f"{r['naive_f1_ms']:>6.2f}/{r['naive_f2_ms']:>6.2f}/{r['naive_join_ms']:>6.2f} | "
              f"{r['split_count']:>9} {r['split_ms']:>10.2f} "
              f"{r['split_f1_ms']:>6.2f}/{r['split_join_ms']:>6.2f}/{r['split_f2_ms']:>6.2f} | {match_str}")

    print()
    print("Boundary/intermediate row counts:")
    for r in rows:
        print(f"  len={r['length']}: naive F1 seam rows={r['naive_boundary_rows']} F2 unseeded suffix rows={r['naive_frag2_rows']} "
              f"| seeded-split F1 seam rows={r['split_boundary_rows']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', default=DEFAULT_NODES)
    parser.add_argument('--edges', default=DEFAULT_EDGES)
    parser.add_argument('--lengths', type=int, nargs='+', default=[2, 3, 4])
    parser.add_argument('--start-vertices', type=int, nargs='+', default=[383])
    parser.add_argument('--csv-out', default=None,
                         help="Path to write a combined CSV (one row per start_vertex x length)")
    args = parser.parse_args()

    conn = duckdb.connect(':memory:')
    load_data(conn, args.nodes, args.edges)

    all_rows = []
    for sv in args.start_vertices:
        print(f"=== start_vertex={sv} ===")
        rows = run_one_vertex(conn, args.lengths, sv)
        print_rows(rows)
        print()
        all_rows.extend(rows)

    if args.csv_out:
        os.makedirs(os.path.dirname(args.csv_out), exist_ok=True)
        with open(args.csv_out, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"wrote {len(all_rows)} rows to {args.csv_out}")


if __name__ == '__main__':
    main()
