#!/usr/bin/env python3
"""Shared data loader for the three new WAVEGUIDE-style split experiments
(monotonicity-isolated on Q1's own regex already exists as the Phase 1 pilot
in ../navigation_experiment/ -- these three are genuinely new). Same dataset
as that pilot (LG.csv / LG_V.csv == experiments/datasets/metaverse's own
edges.csv, confirmed identical elsewhere in this project), loaded the same
way, but the NFA is passed in per experiment instead of hardcoded, since each
experiment uses a different regex shape."""
import os

import duckdb
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_NODES = os.path.join(_HERE, '..', '..', '..', 'ReCAP', 'simple_dataset', 'LG_V.csv')
DEFAULT_EDGES = os.path.join(_HERE, '..', '..', '..', 'ReCAP', 'simple_dataset', 'LG.csv')


def load_data(conn: duckdb.DuckDBPyConnection, nfa_edges: list[tuple[int, int, str]],
              nodes_path: str = DEFAULT_NODES, edges_path: str = DEFAULT_EDGES) -> None:
    nodes_df = pd.read_csv(nodes_path)
    conn.register('nodes_df', nodes_df)
    conn.execute("DROP TABLE IF EXISTS nodes")
    conn.execute("CREATE TABLE nodes AS SELECT id FROM nodes_df")

    edges_df = pd.read_csv(edges_path)
    conn.register('edges_df', edges_df)
    conn.execute("DROP TABLE IF EXISTS edges")
    conn.execute(
        "CREATE TABLE edges AS SELECT edge_id, src, dst, label, timestamp_ms, amount, risk_score "
        "FROM edges_df"
    )

    conn.execute("DROP TABLE IF EXISTS nfa_edges")
    conn.execute("CREATE TABLE nfa_edges(from_state INTEGER, to_state INTEGER, label VARCHAR)")
    conn.executemany("INSERT INTO nfa_edges VALUES (?, ?, ?)", nfa_edges)

    conn.execute("CREATE INDEX idx_edges_src ON edges(src)")
    conn.execute("CREATE INDEX idx_nfa ON nfa_edges(from_state, label)")


def except_all(conn, table_a: str, table_b: str) -> tuple[int, int]:
    """Bag-semantics diff (plain EXCEPT would silently hide a duplicate-row
    mismatch -- same reasoning as ../navigation_experiment/check_equivalence.py)."""
    only_a = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT * FROM {table_a} EXCEPT ALL SELECT * FROM {table_b})"
    ).fetchone()[0]
    only_b = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT * FROM {table_b} EXCEPT ALL SELECT * FROM {table_a})"
    ).fetchone()[0]
    return only_a, only_b
