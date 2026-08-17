#!/usr/bin/env python3
"""Shared data loader for the Phase 1 navigation-style experiment (Q1, simplified
to regex + monotonic-time only). See ../navigation_style_experiment.md."""
import os

import duckdb
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_NODES = os.path.join(_HERE, '..', '..', '..', 'ReCAP', 'simple_dataset', 'LG_V.csv')
DEFAULT_EDGES = os.path.join(_HERE, '..', '..', '..', 'ReCAP', 'simple_dataset', 'LG.csv')

# Q1's NFA: (transfer|purchase|sale)+ (phishing|scam)+, states {0,1,2}, q0=0, Q_F={2}.
# Hardcoded to match ReCAP/q1/recap_gen_recap_inline.py rather than read from
# simple_dataset/nfa.csv, which is a generic placeholder unrelated to Q1.
NFA_EDGES = [
    (0, 1, 'transfer'), (0, 1, 'purchase'), (0, 1, 'sale'),
    (1, 1, 'transfer'), (1, 1, 'purchase'), (1, 1, 'sale'),
    (1, 2, 'phishing'), (1, 2, 'scam'),
    (2, 2, 'phishing'), (2, 2, 'scam'),
]


def load_data(conn: duckdb.DuckDBPyConnection, nodes_path: str, edges_path: str) -> None:
    nodes_df = pd.read_csv(nodes_path)
    conn.register('nodes_df', nodes_df)
    conn.execute("DROP TABLE IF EXISTS nodes")
    conn.execute("CREATE TABLE nodes AS SELECT id FROM nodes_df")

    edges_df = pd.read_csv(edges_path)
    conn.register('edges_df', edges_df)
    conn.execute("DROP TABLE IF EXISTS edges")
    conn.execute(
        "CREATE TABLE edges AS SELECT edge_id, src, dst, label, timestamp_ms FROM edges_df"
    )

    conn.execute("DROP TABLE IF EXISTS nfa_edges")
    conn.execute("CREATE TABLE nfa_edges(from_state INTEGER, to_state INTEGER, label VARCHAR)")
    conn.executemany("INSERT INTO nfa_edges VALUES (?, ?, ?)", NFA_EDGES)

    conn.execute("CREATE INDEX idx_edges_src ON edges(src)")
    conn.execute("CREATE INDEX idx_nfa ON nfa_edges(from_state, label)")
