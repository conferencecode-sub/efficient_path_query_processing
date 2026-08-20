#!/usr/bin/env python3
"""New WAVEGUIDE-style split experiment #3: a THREE-fragment, TWO-seam split,
explored middle-out: F2 first (independent), then F1 *backward* (seeded by
F2's own entry vertices, walked over the reversed graph), then F3 forward
(seeded by F2's exit boundary) -- exactly the paper's own deferred "genuine
directional reversal" stretch goal (navigation_style_experiment.md, section
4), now actually built, on a genuine multi-seam regex rather than Q1's
single-seam one.

Regex: (transfer|purchase)+ (sale)+ (phishing|scam)+ -- states {0,1,2,3},
q0=0, Q_F={3}. Real label counts in this dataset: transfer|purchase=47,065
(59.9%), sale=25,040 (31.9%), phishing|scam=6,495 (8.3%) -- three genuinely
different segment sizes, not a toy split.

Constraint: the same bounded-range-on-risk_score family as the mirrored
experiment (Example 9), applied across the whole path.

Middle-out plan:
  1. F2 (states {1,2}, `sale`-only): explored forward from *every* vertex,
     unseeded, producing (entry_v, exit_v, len2, max_risk2, min_risk2) rows.
  2. F1 (states {0,1}, `transfer|purchase`-only), walked BACKWARD: seeded by
     F2's own *distinct entry vertices* (not by a start vertex), over a
     reversed edge relation (src/dst swapped, `transfer|purchase` labels
     only). This discovers which vertices can reach each F2 entry point --
     a real vertex, seeded elsewhere in this project's own start-vertex
     methodology, is only checked for membership at the very end, not used
     to seed the walk itself (backward search doesn't know the eventual
     start vertex is 383 until it's found it).
  3. F3 (states {2,3}, `phishing|scam`-only), walked FORWARD as usual,
     seeded by F2's own exit boundary -- structurally identical to the
     existing two-fragment seeded_split.py, just one more hop down the chain.
  4. Merge: F1-backward rows ending at the real start vertex, joined to F2's
     own (entry,exit) rows, joined to F3's rows seeded from that exit --
     combined length and combined (max,min) risk range checked against the
     bound in one final predicate.

No lookahead-pruning trick is used anywhere here (the sum-of-weights
experiment in this same directory found that pruning a *recursive term* by
an EXISTS lookahead against a precomputed sibling fragment is unsound -- it
can drop a row that would still succeed via a longer continuation through a
different vertex). F1-backward is a plain backward-seeded fragment,
structurally symmetric to any other forward-seeded one -- safe by the same
argument that already-validated seeded_split.py is safe.
"""
import argparse
import csv
import os
import time

import duckdb
import pandas as pd

from common2 import DEFAULT_EDGES, DEFAULT_NODES, except_all

NFA_EDGES = [
    (0, 1, 'transfer'), (0, 1, 'purchase'),
    (1, 1, 'transfer'), (1, 1, 'purchase'),
    (1, 2, 'sale'),
    (2, 2, 'sale'),
    (2, 3, 'phishing'), (2, 3, 'scam'),
    (3, 3, 'phishing'), (3, 3, 'scam'),
]
RISK_BOUND = 40.0
RESULTS_CSV = os.path.join(os.path.dirname(__file__), "results", "exp_threeseg_maxmin.csv")

_INIT = "CAST(-1e308 AS DOUBLE) AS max_risk, CAST(1e308 AS DOUBLE) AS min_risk"
_STEP = "GREATEST(p.max_risk, t.risk_score), LEAST(p.min_risk, t.risk_score)"
_BOUND_CHECK = f"GREATEST(p.max_risk, t.risk_score) - LEAST(p.min_risk, t.risk_score) <= {RISK_BOUND}"


def load_data(conn, nodes_path=DEFAULT_NODES, edges_path=DEFAULT_EDGES):
    nodes_df = pd.read_csv(nodes_path)
    conn.register('nodes_df', nodes_df)
    conn.execute("DROP TABLE IF EXISTS nodes")
    conn.execute("CREATE TABLE nodes AS SELECT id FROM nodes_df")

    edges_df = pd.read_csv(edges_path)
    conn.register('edges_df', edges_df)
    conn.execute("DROP TABLE IF EXISTS edges")
    conn.execute(
        "CREATE TABLE edges AS SELECT edge_id, src, dst, label, timestamp_ms, amount, risk_score FROM edges_df")
    # Reversed relation for F1's backward walk: same rows, src/dst swapped,
    # filtered to segment 1's own labels only (transfer|purchase).
    conn.execute("DROP TABLE IF EXISTS edges_reversed_seg1")
    conn.execute("""
        CREATE TABLE edges_reversed_seg1 AS
        SELECT edge_id, dst AS src, src AS dst, label, timestamp_ms, amount, risk_score
        FROM edges WHERE label IN ('transfer', 'purchase')
    """)

    conn.execute("DROP TABLE IF EXISTS nfa_edges")
    conn.execute("CREATE TABLE nfa_edges(from_state INTEGER, to_state INTEGER, label VARCHAR)")
    conn.executemany("INSERT INTO nfa_edges VALUES (?, ?, ?)", NFA_EDGES)

    conn.execute("CREATE INDEX idx_edges_src ON edges(src)")
    conn.execute("CREATE INDEX idx_edges_rev_src ON edges_reversed_seg1(src)")
    conn.execute("CREATE INDEX idx_nfa ON nfa_edges(from_state, label)")


def run_monolithic(conn, min_length, max_length, start_vertex, result_table="mono_results"):
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT {start_vertex} AS v, 0 AS state, {_INIT}, 0 AS path_length
        UNION ALL
        SELECT t.dst, nf.to_state, {_STEP}, p.path_length + 1
        FROM paths p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE p.path_length < {max_length}
          AND {_BOUND_CHECK}
    )
    SELECT v AS end_vertex, path_length, max_risk, min_risk
    FROM paths WHERE state = 3 AND path_length >= {min_length}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {query}")
    wall = time.perf_counter() - t0
    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, wall


def run_middle_out(conn, min_length, max_length, start_vertex, result_table="middleout_results"):
    # --- Step 1: F2 (states {1,2}, sale-only), unseeded, every vertex. ---
    f2_query = f"""
    WITH RECURSIVE frag2 AS (
        SELECT n.id AS entry_v, n.id AS v, 1 AS state, {_INIT}, 0 AS len2
        FROM nodes n
        UNION ALL
        SELECT p.entry_v, t.dst, nf.to_state, {_STEP}, p.len2 + 1
        FROM frag2 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE t.label = 'sale'  -- state 1 also has a (1,1,transfer/purchase)
                                 -- self-loop in this NFA (segment 1's own
                                 -- continuation) -- filtering by from/to
                                 -- state range alone would wrongly follow
                                 -- it too; only 'sale' actually belongs to
                                 -- this fragment.
          AND p.len2 < {max_length} - 2  -- >=1 hop needed on each side
          AND {_BOUND_CHECK}
    )
    SELECT entry_v, v AS exit_v, len2, max_risk AS max_risk2, min_risk AS min_risk2
    FROM frag2 WHERE state = 2 AND len2 >= 1
    """
    conn.execute("DROP TABLE IF EXISTS frag2_table")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag2_table AS {f2_query}")
    conn.execute("CREATE INDEX idx_f2_entry ON frag2_table(entry_v)")
    f2_wall = time.perf_counter() - t0
    f2_rows = conn.execute("SELECT COUNT(*) FROM frag2_table").fetchone()[0]

    # --- Step 2: F1 (states {0,1}, transfer|purchase-only), walked
    # BACKWARD, seeded by F2's own *distinct* entry vertices (not by
    # start_vertex -- the backward search doesn't know it's looking for 383
    # until it finds it; that's checked only in the final SELECT below). ---
    f1_query = f"""
    WITH RECURSIVE frag1_backward AS (
        SELECT DISTINCT entry_v AS seg2_entry, entry_v AS v, {_INIT.replace('AS max_risk', 'AS max_risk').replace('AS min_risk', 'AS min_risk')}, 0 AS len1
        FROM frag2_table
        UNION ALL
        SELECT p.seg2_entry, t.dst, {_STEP}, p.len1 + 1
        FROM frag1_backward p
        JOIN edges_reversed_seg1 t ON p.v = t.src
        WHERE p.len1 < {max_length} - 1
          AND {_BOUND_CHECK}
    )
    SELECT seg2_entry, v AS real_start, len1, max_risk AS max_risk1, min_risk AS min_risk1
    FROM frag1_backward
    WHERE v = {start_vertex} AND len1 >= 1
    """
    conn.execute("DROP TABLE IF EXISTS frag1_table")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag1_table AS {f1_query}")
    f1_wall = time.perf_counter() - t0
    f1_rows = conn.execute("SELECT COUNT(*) FROM frag1_table").fetchone()[0]

    # --- Step 3: F3 (states {2,3}, phishing|scam-only), forward, seeded by
    # F2's own *distinct* exit vertices. ---
    f3_query = f"""
    WITH RECURSIVE frag3 AS (
        SELECT DISTINCT exit_v AS seg2_exit, exit_v AS v, 2 AS state, {_INIT}, 0 AS len3
        FROM frag2_table
        UNION ALL
        SELECT p.seg2_exit, t.dst, nf.to_state, {_STEP}, p.len3 + 1
        FROM frag3 p
        JOIN edges t ON p.v = t.src
        JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
        WHERE nf.to_state <> 2  -- exclude re-entering the sale segment (F2
                                 -- already enumerated every such depth as
                                 -- its own exit_v row -- same reasoning as
                                 -- seeded_split.py's `to_state <> 1` exclusion)
          AND p.len3 < {max_length} - 1
          AND {_BOUND_CHECK}
    )
    SELECT seg2_exit, v AS final_v, len3, max_risk AS max_risk3, min_risk AS min_risk3
    FROM frag3 WHERE state = 3 AND len3 >= 1
    """
    conn.execute("DROP TABLE IF EXISTS frag3_table")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE frag3_table AS {f3_query}")
    f3_wall = time.perf_counter() - t0
    f3_rows = conn.execute("SELECT COUNT(*) FROM frag3_table").fetchone()[0]

    # --- Step 4: merge all three fragments. ---
    merge_query = f"""
    SELECT f3.final_v AS end_vertex,
           f1.len1 + f2.len2 + f3.len3 AS path_length,
           GREATEST(f1.max_risk1, f2.max_risk2, f3.max_risk3) AS max_risk,
           LEAST(f1.min_risk1, f2.min_risk2, f3.min_risk3) AS min_risk
    FROM frag1_table f1
    JOIN frag2_table f2 ON f1.seg2_entry = f2.entry_v
    JOIN frag3_table f3 ON f2.exit_v = f3.seg2_exit
    WHERE f1.len1 + f2.len2 + f3.len3 BETWEEN {min_length} AND {max_length}
      AND GREATEST(f1.max_risk1, f2.max_risk2, f3.max_risk3)
        - LEAST(f1.min_risk1, f2.min_risk2, f3.min_risk3) <= {RISK_BOUND}
    """
    conn.execute(f"DROP TABLE IF EXISTS {result_table}")
    t0 = time.perf_counter()
    conn.execute(f"CREATE TABLE {result_table} AS {merge_query}")
    merge_wall = time.perf_counter() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    return count, f2_wall, f1_wall, f3_wall, merge_wall, f2_rows, f1_rows, f3_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-vertices', type=int, nargs='+', default=[383, 594, 592, 635])
    parser.add_argument('--lengths', type=int, nargs='+', default=[3, 4, 5])
    args = parser.parse_args()

    conn = duckdb.connect(':memory:')
    load_data(conn)

    rows = []
    for sv in args.start_vertices:
        for ell in args.lengths:
            mono_count, mono_wall = run_monolithic(conn, ell, ell, sv)
            mo_count, f2w, f1w, f3w, mergew, f2r, f1r, f3r = run_middle_out(conn, ell, ell, sv)
            mono_vs_mo = except_all(conn, "mono_results", "middleout_results")
            match = mono_vs_mo == (0, 0)
            mo_ms = 1000 * (f2w + f1w + f3w + mergew)
            row = dict(start_vertex=sv, length=ell,
                       mono_count=mono_count, mono_ms=1000 * mono_wall,
                       middleout_count=mo_count, middleout_ms=mo_ms,
                       f2_ms=1000 * f2w, f1_backward_ms=1000 * f1w, f3_ms=1000 * f3w, merge_ms=1000 * mergew,
                       f2_rows=f2r, f1_backward_rows=f1r, f3_rows=f3r,
                       match=match)
            rows.append(row)
            print(f"sv={sv} len={ell}: mono={mono_count} ({row['mono_ms']:.1f}ms)  "
                  f"middle-out={mo_count} ({mo_ms:.1f}ms; F2={1000*f2w:.1f}ms/{f2r}rows "
                  f"F1-bwd={1000*f1w:.1f}ms/{f1r}rows F3={1000*f3w:.1f}ms/{f3r}rows merge={1000*mergew:.1f}ms)  "
                  f"| {'OK' if match else 'MISMATCH'}")

    os.makedirs(os.path.dirname(RESULTS_CSV), exist_ok=True)
    with open(RESULTS_CSV, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {RESULTS_CSV}")
    print(f"mismatches: {sum(1 for r in rows if not r['match'])} / {len(rows)}")


if __name__ == '__main__':
    main()
