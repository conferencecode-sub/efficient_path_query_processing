#!/usr/bin/env python3
"""Shared loader for E5 (handcrafted vs. ReCAP-Optimized vs. split), using
Q1's FULL constraint set (trail, region, risk-range, last-risk gateway,
amount) -- unlike `alternative_explorations/navigation_experiment/common.py`,
which is a deliberately simplified regex+monotonic-time-only cut for a
different (R4.O2 navigation-style) question.

Loads the real Metaverse dataset (`experiments/datasets/metaverse/`,
78,600 edges / 1,320 vertices, matching tab:realdata) with every column
Q1's aggregate needs, matching `ReCAP/q1/recap_gen_recap_inline.py`'s own
schema exactly (the source of truth for what Q1 checks).
"""
import os

import duckdb
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EDGES = os.path.join(_HERE, '..', 'datasets', 'metaverse', 'edges.csv')
DEFAULT_NODES = os.path.join(_HERE, '..', 'datasets', 'metaverse', 'nodes.csv')

# Q1's NFA: (transfer|purchase|sale)+ (phishing|scam)+, states {0,1,2}, q0=0, Q_F={2}.
NFA_EDGES = [
    (0, 1, 'transfer'), (0, 1, 'purchase'), (0, 1, 'sale'),
    (1, 1, 'transfer'), (1, 1, 'purchase'), (1, 1, 'sale'),
    (1, 2, 'phishing'), (1, 2, 'scam'),
    (2, 2, 'phishing'), (2, 2, 'scam'),
]

RISK_RANGE_BOUND = 20
LAST_RISK_GATE = 40
MIN_AMOUNT = 1000


def load_data(conn: duckdb.DuckDBPyConnection, nodes_path: str = DEFAULT_NODES,
              edges_path: str = DEFAULT_EDGES) -> None:
    nodes_df = pd.read_csv(nodes_path)
    conn.register('nodes_df', nodes_df)
    conn.execute("DROP TABLE IF EXISTS nodes")
    conn.execute("CREATE TABLE nodes AS SELECT id FROM nodes_df")

    edges_df = pd.read_csv(edges_path)
    conn.register('edges_df', edges_df)
    conn.execute("DROP TABLE IF EXISTS edges")
    conn.execute(
        "CREATE TABLE edges AS SELECT edge_id, src, dst, label, timestamp_ms, "
        "location_region, risk_score, amount FROM edges_df"
    )

    conn.execute("DROP TABLE IF EXISTS nfa_edges")
    conn.execute("CREATE TABLE nfa_edges(from_state INTEGER, to_state INTEGER, label VARCHAR)")
    conn.executemany("INSERT INTO nfa_edges VALUES (?, ?, ?)", NFA_EDGES)

    conn.execute("CREATE INDEX idx_edges_src ON edges(src)")
    conn.execute("CREATE INDEX idx_nfa ON nfa_edges(from_state, label)")
