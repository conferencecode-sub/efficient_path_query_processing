#!/usr/bin/env python3
"""New WAVEGUIDE-style split experiment #1: SUM OF WEIGHTS (non-negative),
same regex/seam as the existing Phase 1 pilot.

Regex: (transfer|purchase|sale)+ (phishing|scam)+ -- identical NFA to
../navigation_experiment/common.py (states {0,1,2}, seam at 1->2, the rare
label in this dataset). Constraint: total edge `amount` along the path must
stay <= AMOUNT_BOUND. Negatively stable because every edge's `amount` is
strictly positive in this dataset (confirmed: min=0.01, no zero/negative
values) -- once the running sum exceeds the bound, no further non-negative
addition can bring it back down.

This isolates a genuinely different constraint *family* from the existing
pilot's monotonicity check: a distributive/compressible boundary state (the
seam only needs to carry one running total, and F2 can keep adding to it
directly -- no re-derivation from F1's own edges needed) vs. monotonicity's
scalar last-value comparison. The naive-split F2 has no way to apply the
bound at all during its own recursion (it doesn't know F1's prefix sum), so
it is entirely unconstrained by AMOUNT_BOUND until the final join -- a
cleaner, more extreme version of "deferring the check" than the monotonicity
pilot's F2, which was still bounded by suffix_length.
"""
import argparse
import csv
import os
import time

import duckdb

from common2 import DEFAULT_EDGES, DEFAULT_NODES, except_all, load_data

NFA_EDGES = [
    (0, 1, 'transfer'), (0, 1, 'purchase'), (0, 1, 'sale'),
    (1, 1, 'transfer'), (1, 1, 'purchase'), (1, 1, 'sale'),
    (1, 2, 'phishing'), (1, 2, 'scam'),
    (2, 2, 'phishing'), (2, 2, 'scam'),
]
AMOUNT_BOUND = 800.0
RESULTS_CSV = os.path.join(os.path.dirname(__file__), "results", "exp_sum_weights.csv")


def run_monolithic(conn, min_length, max_length, start_vertex, result_table="mono_results"):
    node_filter = f"WHERE n.id = {start_vertex}" if start_vertex is not None else ""
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT n.id AS start, n.id AS v, 0 AS state, CAST(0.0 AS DOUBLE) AS total_amount, 0 AS path_length
        FROM nodes n {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, p.total_amount + t.amount, p.path_length + 1
        FROM paths p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE p.path_length < {max_length}
          AND p.total_amount + t.amount <= {AMOUNT_BOUND}
    )
    SELECT start, v AS end_vertex, path_length, ROUND(total_amount, 6) AS total_amount
    FROM paths WHERE state = 2 AND path_length >= {min_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {query}")
    wall = time.perf_counter() - t0
    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, wall


def run_seeded_split(conn, min_length, max_length, start_vertex, result_table="split_results"):
    node_filter = f"WHERE n.id = {start_vertex}" if start_vertex is not None else ""
    frag1_query = f"""
    WITH RECURSIVE frag1 AS (
        SELECT n.id AS start, n.id AS v, 0 AS state, CAST(0.0 AS DOUBLE) AS total_amount, 0 AS path_length
        FROM nodes n {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, p.total_amount + t.amount, p.path_length + 1
        FROM frag1 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state = 1
          AND p.path_length < {max_length} - 1
          AND p.total_amount + t.amount <= {AMOUNT_BOUND}
    )
    SELECT start, v, state, total_amount, path_length AS prefix_length
    FROM frag1 WHERE state = 1
    """
    conn.execute("DROP TABLE IF EXISTS frag1_boundary")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag1_boundary AS {frag1_query}")
    f1_wall = time.perf_counter() - t0
    boundary_rows = conn.execute("SELECT COUNT(*) FROM frag1_boundary").fetchone()[0]

    conn.execute("DROP TABLE IF EXISTS frag2_seed")
    t0 = time.perf_counter()
    conn.execute("""
        CREATE TABLE frag2_seed AS
        SELECT start, v, state, total_amount, prefix_length, 0 AS suffix_length
        FROM frag1_boundary
    """)
    join_wall = time.perf_counter() - t0

    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT * FROM frag2_seed
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, p.total_amount + t.amount, p.prefix_length, p.suffix_length + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 1
          AND p.prefix_length + p.suffix_length < {max_length}
          AND p.total_amount + t.amount <= {AMOUNT_BOUND}
    )
    SELECT start, v AS end_vertex, prefix_length + suffix_length AS path_length, ROUND(total_amount, 6) AS total_amount
    FROM frag2
    WHERE state = 2 AND prefix_length + suffix_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {frag2_query}")
    f2_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f1_wall, join_wall, f2_wall, boundary_rows


def run_naive_split(conn, min_length, max_length, start_vertex, result_table="naive_results"):
    node_filter = f"WHERE n.id = {start_vertex}" if start_vertex is not None else ""
    frag1_query = f"""
    WITH RECURSIVE frag1 AS (
        SELECT n.id AS start, n.id AS v, 0 AS state, CAST(0.0 AS DOUBLE) AS total_amount, 0 AS path_length
        FROM nodes n {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, p.total_amount + t.amount, p.path_length + 1
        FROM frag1 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state = 1
          AND p.path_length < {max_length} - 1
          AND p.total_amount + t.amount <= {AMOUNT_BOUND}
    )
    SELECT start, v, state, total_amount, path_length AS prefix_length
    FROM frag1 WHERE state = 1
    """
    conn.execute("DROP TABLE IF EXISTS frag1_boundary")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag1_boundary AS {frag1_query}")
    f1_wall = time.perf_counter() - t0
    boundary_rows = conn.execute("SELECT COUNT(*) FROM frag1_boundary").fetchone()[0]

    # Naive F2: unseeded, from every vertex, tracking only its OWN increment
    # (suffix_amount) -- no AMOUNT_BOUND check at all here, since F2 has no
    # way to know F1's prefix sum. This is the point: unlike the monotonicity
    # pilot's naive F2 (still bounded by suffix_length alone), this F2 is
    # completely unconstrained by the property being tested until the join.
    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT n.id AS first_v, n.id AS v, 1 AS state, CAST(0.0 AS DOUBLE) AS suffix_amount, 0 AS suffix_length
        FROM nodes n
        UNION ALL
        SELECT p.first_v, t.dst, nf.to_state, p.suffix_amount + t.amount, p.suffix_length + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 1
          AND p.suffix_length < {max_length} - 1
    )
    SELECT first_v, v, suffix_amount, suffix_length
    FROM frag2 WHERE state = 2
    """
    conn.execute("DROP TABLE IF EXISTS frag2_unseeded")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag2_unseeded AS {frag2_query}")
    f2_wall = time.perf_counter() - t0
    frag2_rows = conn.execute("SELECT COUNT(*) FROM frag2_unseeded").fetchone()[0]

    join_query = f"""
    SELECT f1.start AS start, f2.v AS end_vertex,
           f1.prefix_length + f2.suffix_length AS path_length,
           ROUND(f1.total_amount + f2.suffix_amount, 6) AS total_amount
    FROM frag1_boundary f1
    JOIN frag2_unseeded f2 ON f1.v = f2.first_v
    WHERE f1.total_amount + f2.suffix_amount <= {AMOUNT_BOUND}
      AND f1.prefix_length + f2.suffix_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {join_query}")
    join_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f1_wall, f2_wall, join_wall, boundary_rows, frag2_rows


def run_reverse_seeded(conn, min_length, max_length, start_vertex, result_table="reverse_results"):
    """"F2 first" ordering: precompute F2 (states {1,2}) unseeded, from every
    vertex -- identical computation to run_naive_split's own frag2_unseeded,
    reused as-is. Then F1 (states {0,1}) is explored forward from
    start_vertex as usual, but with an inline EXISTS lookahead against the
    *precomputed* F2 table at every hop: a candidate row is only kept if some
    F2 completion exists that would bring the combined total under
    AMOUNT_BOUND within the remaining length budget. Valid to prune (not just
    filter the final answer) because the sum only grows and the remaining
    hop budget only shrinks as F1 extends, so a row infeasible now can never
    become feasible later -- unlike naive-split, which computes the same two
    fragments but defers this check entirely to a final join with no
    look-ahead pruning in either fragment."""
    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT n.id AS first_v, n.id AS v, 1 AS state, CAST(0.0 AS DOUBLE) AS suffix_amount, 0 AS suffix_length
        FROM nodes n
        UNION ALL
        SELECT p.first_v, t.dst, nf.to_state, p.suffix_amount + t.amount, p.suffix_length + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 1
          AND p.suffix_length < {max_length} - 1
    )
    SELECT first_v, v, suffix_amount, suffix_length
    FROM frag2 WHERE state = 2
    """
    conn.execute("DROP TABLE IF EXISTS frag2_precomputed")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag2_precomputed AS {frag2_query}")
    conn.execute("CREATE INDEX idx_frag2_precomputed ON frag2_precomputed(first_v)")
    f2_wall = time.perf_counter() - t0
    frag2_rows = conn.execute("SELECT COUNT(*) FROM frag2_precomputed").fetchone()[0]

    # IMPORTANT correctness note (found via a real miscount, not assumed):
    # the EXISTS lookahead below must filter the *outer* SELECT (which rows
    # get reported as boundary/seam candidates), never the recursive term's
    # own WHERE clause. Filtering the recursive term conflates two different
    # questions -- "is v a good seam *right now*" vs. "should this walk be
    # allowed to keep extending via more 1->1 hops" -- and they are not the
    # same: a vertex with no affordable completion *at this exact remaining
    # budget* can still lead, via further self-loop hops, to a *different*
    # vertex with a cheap completion at a *smaller* remaining budget. An
    # earlier version of this function applied the check inside the
    # recursive term and undercounted (2673 vs. the correct 3729 at ell=3,
    # confirmed via mono_results EXCEPT ALL reverse_results -- pure lost
    # rows, no spurious extras) by permanently dropping exactly those
    # mid-recursion rows instead of just declining to report them as seams.
    node_filter = f"WHERE n.id = {start_vertex}" if start_vertex is not None else ""
    frag1_query = f"""
    WITH RECURSIVE frag1 AS (
        SELECT n.id AS start, n.id AS v, 0 AS state, CAST(0.0 AS DOUBLE) AS total_amount, 0 AS path_length
        FROM nodes n {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, p.total_amount + t.amount, p.path_length + 1
        FROM frag1 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state = 1
          AND p.path_length < {max_length} - 1
          AND p.total_amount + t.amount <= {AMOUNT_BOUND}
    )
    SELECT start, v, state, total_amount, path_length AS prefix_length
    FROM frag1
    WHERE state = 1
      AND EXISTS (
            SELECT 1 FROM frag2_precomputed f2
            WHERE f2.first_v = v
              AND path_length + f2.suffix_length BETWEEN {min_length} AND {max_length}
              AND total_amount + f2.suffix_amount <= {AMOUNT_BOUND}
      )
    """
    conn.execute("DROP TABLE IF EXISTS frag1_pruned")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag1_pruned AS {frag1_query}")
    f1_wall = time.perf_counter() - t0
    boundary_rows = conn.execute("SELECT COUNT(*) FROM frag1_pruned").fetchone()[0]

    join_query = f"""
    SELECT f1.start AS start, f2.v AS end_vertex,
           f1.prefix_length + f2.suffix_length AS path_length,
           ROUND(f1.total_amount + f2.suffix_amount, 6) AS total_amount
    FROM frag1_pruned f1
    JOIN frag2_precomputed f2 ON f1.v = f2.first_v
    WHERE f1.total_amount + f2.suffix_amount <= {AMOUNT_BOUND}
      AND f1.prefix_length + f2.suffix_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {join_query}")
    join_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f2_wall, f1_wall, join_wall, frag2_rows, boundary_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-vertices', type=int, nargs='+', default=[383, 594, 592, 635])
    parser.add_argument('--lengths', type=int, nargs='+', default=[2, 3, 4, 5, 6])
    args = parser.parse_args()

    conn = duckdb.connect(':memory:')
    load_data(conn, NFA_EDGES)

    rows = []
    for sv in args.start_vertices:
        for ell in args.lengths:
            mono_count, mono_ms = run_monolithic(conn, ell, ell, sv)
            mono_ms *= 1000
            split_count, sf1, sjoin, sf2, split_boundary = run_seeded_split(conn, ell, ell, sv)
            split_ms = 1000 * (sf1 + sjoin + sf2)
            naive_count, nf1, nf2, njoin, naive_boundary, naive_frag2_rows = run_naive_split(conn, ell, ell, sv)
            naive_ms = 1000 * (nf1 + nf2 + njoin)

            mono_vs_split = except_all(conn, "mono_results", "split_results")
            mono_vs_naive = except_all(conn, "mono_results", "naive_results")

            row = dict(
                start_vertex=sv, length=ell,
                mono_count=mono_count, mono_ms=mono_ms,
                split_count=split_count, split_ms=split_ms,
                split_f1_ms=1000 * sf1, split_join_ms=1000 * sjoin, split_f2_ms=1000 * sf2,
                split_boundary_rows=split_boundary,
                naive_count=naive_count, naive_ms=naive_ms,
                naive_f1_ms=1000 * nf1, naive_f2_ms=1000 * nf2, naive_join_ms=1000 * njoin,
                naive_boundary_rows=naive_boundary, naive_frag2_rows=naive_frag2_rows,
                mono_vs_split_match=(mono_vs_split == (0, 0)),
                mono_vs_naive_match=(mono_vs_naive == (0, 0)),
            )
            rows.append(row)
            match_str = f"split={'OK' if row['mono_vs_split_match'] else 'MISMATCH'} naive={'OK' if row['mono_vs_naive_match'] else 'MISMATCH'}"
            print(f"sv={sv} len={ell}: mono={mono_count} ({mono_ms:.1f}ms) "
                  f"split={split_count} ({split_ms:.1f}ms, boundary={split_boundary}) "
                  f"naive={naive_count} ({naive_ms:.1f}ms, boundary={naive_boundary}, frag2={naive_frag2_rows}) "
                  f"| {match_str}")

    os.makedirs(os.path.dirname(RESULTS_CSV), exist_ok=True)
    with open(RESULTS_CSV, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {RESULTS_CSV}")
    n_mismatches = sum(1 for r in rows if not r['mono_vs_split_match'] or not r['mono_vs_naive_match'])
    print(f"mismatches: {n_mismatches} / {len(rows)}")


if __name__ == '__main__':
    main()
