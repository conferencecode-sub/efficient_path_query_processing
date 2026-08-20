#!/usr/bin/env python3
"""New WAVEGUIDE-style split experiment #2: MAX-MIN (bounded range), on the
MIRROR of the existing pilot's regex.

Regex: (phishing|scam)+ (transfer|purchase|sale)+ -- states {0,1,2}, seam at
1->2. This is Q1's own regex with the two segments swapped: the RARE label
(phishing|scam, ~8% of edges) is now the *prefix*, not the suffix. Every
prior split experiment in this project (the Phase 1 pilot, E5) splits at a
rare *suffix* -- this tests whether the seam-pruning benefit depends on
which side of the seam is rare.

Constraint: bounded range on risk_score, GREATEST(max_risk, e.risk_score) -
LEAST(min_risk, e.risk_score) <= RISK_BOUND -- Example 9 in the paper, the
same family as Q1's own real constraint (there scoped to the normal prefix
only; here applied across the whole path, isolated, with no other
constraint mixed in -- E5 found the full multi-constraint mix already
prunes F1 aggressively on its own, masking the seam-pruning signal).

Unlike the sum-of-weights experiment, GREATEST/LEAST are exact,
order-independent operations (no floating-point addition reordering across
the seam), so no rounding tolerance is needed in the equivalence check.
"""
import argparse
import csv
import os
import time

import duckdb

from common2 import DEFAULT_EDGES, DEFAULT_NODES, except_all, load_data

NFA_EDGES = [
    (0, 1, 'phishing'), (0, 1, 'scam'),
    (1, 1, 'phishing'), (1, 1, 'scam'),
    (1, 2, 'transfer'), (1, 2, 'purchase'), (1, 2, 'sale'),
    (2, 2, 'transfer'), (2, 2, 'purchase'), (2, 2, 'sale'),
]
RISK_BOUND = 20.0
RESULTS_CSV = os.path.join(os.path.dirname(__file__), "results", "exp_maxmin_mirrored.csv")

_INIT = "CAST(-1e308 AS DOUBLE) AS max_risk, CAST(1e308 AS DOUBLE) AS min_risk"
_STEP = "GREATEST(p.max_risk, t.risk_score), LEAST(p.min_risk, t.risk_score)"
_BOUND_CHECK = f"GREATEST(p.max_risk, t.risk_score) - LEAST(p.min_risk, t.risk_score) <= {RISK_BOUND}"


def run_monolithic(conn, min_length, max_length, start_vertex, result_table="mono_results"):
    node_filter = f"WHERE n.id = {start_vertex}" if start_vertex is not None else ""
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT n.id AS start, n.id AS v, 0 AS state, {_INIT}, 0 AS path_length
        FROM nodes n {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, {_STEP}, p.path_length + 1
        FROM paths p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE p.path_length < {max_length}
          AND {_BOUND_CHECK}
    )
    SELECT start, v AS end_vertex, path_length, max_risk, min_risk
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
        SELECT n.id AS start, n.id AS v, 0 AS state, {_INIT}, 0 AS path_length
        FROM nodes n {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, {_STEP}, p.path_length + 1
        FROM frag1 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state = 1
          AND p.path_length < {max_length} - 1
          AND {_BOUND_CHECK}
    )
    SELECT start, v, state, max_risk, min_risk, path_length AS prefix_length
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
        SELECT start, v, state, max_risk, min_risk, prefix_length, 0 AS suffix_length
        FROM frag1_boundary
    """)
    join_wall = time.perf_counter() - t0

    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT * FROM frag2_seed
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, {_STEP}, p.prefix_length, p.suffix_length + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 1
          AND p.prefix_length + p.suffix_length < {max_length}
          AND {_BOUND_CHECK}
    )
    SELECT start, v AS end_vertex, prefix_length + suffix_length AS path_length, max_risk, min_risk
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
        SELECT n.id AS start, n.id AS v, 0 AS state, {_INIT}, 0 AS path_length
        FROM nodes n {node_filter}
        UNION ALL
        SELECT p.start, t.dst, nf.to_state, {_STEP}, p.path_length + 1
        FROM frag1 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state = 1
          AND p.path_length < {max_length} - 1
          AND {_BOUND_CHECK}
    )
    SELECT start, v, state, max_risk, min_risk, path_length AS prefix_length
    FROM frag1 WHERE state = 1
    """
    conn.execute("DROP TABLE IF EXISTS frag1_boundary")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag1_boundary AS {frag1_query}")
    f1_wall = time.perf_counter() - t0
    boundary_rows = conn.execute("SELECT COUNT(*) FROM frag1_boundary").fetchone()[0]

    # Naive F2: unseeded, from every vertex, tracking its OWN (max,min) pair
    # starting fresh -- no RISK_BOUND check during its own recursion at all,
    # since it doesn't know F1's own max/min yet. Bound applied only at the
    # final join, combining both sides' extrema via GREATEST/LEAST.
    frag2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT n.id AS first_v, n.id AS v, 1 AS state, {_INIT}, 0 AS suffix_length
        FROM nodes n
        UNION ALL
        SELECT p.first_v, t.dst, nf.to_state, {_STEP}, p.suffix_length + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 1
          AND p.suffix_length < {max_length} - 1
    )
    SELECT first_v, v, max_risk AS suffix_max_risk, min_risk AS suffix_min_risk, suffix_length
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
           GREATEST(f1.max_risk, f2.suffix_max_risk) AS max_risk,
           LEAST(f1.min_risk, f2.suffix_min_risk) AS min_risk
    FROM frag1_boundary f1
    JOIN frag2_unseeded f2 ON f1.v = f2.first_v
    WHERE GREATEST(f1.max_risk, f2.suffix_max_risk) - LEAST(f1.min_risk, f2.suffix_min_risk) <= {RISK_BOUND}
      AND f1.prefix_length + f2.suffix_length BETWEEN {min_length} AND {max_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {join_query}")
    join_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f1_wall, f2_wall, join_wall, boundary_rows, frag2_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-vertices', type=int, nargs='+', default=[383, 594, 592, 635])
    parser.add_argument('--lengths', type=int, nargs='+', default=[2, 3, 4])
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
