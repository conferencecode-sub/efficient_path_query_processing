#!/usr/bin/env python3
import duckdb
import pandas as pd
import json
import time
import argparse
from typing import Tuple
from collections import defaultdict

# ============================================================================
#  Main DuckDB Class
# ============================================================================

class DuckDBGeneralReCAPQuery:

    def __init__(self, db_path: str = ':memory:'):
        self.conn = duckdb.connect(db_path)
        self.register_udfs()

    def clean_array(self, result):
        if len(result) == 1:
            return result[0][0]
        return tuple(item[0] for item in result)

    def load_data(self, nodes_path: str, edges_path: str,
                  nfa_nodes_path: str, nfa_edges_path: str,
                  with_index: bool = True):
        print("Loading data files...")
        c = self.conn

        # ========== Load Graph Nodes ==========
        nodes_df = pd.read_csv(nodes_path)
        # Ensure columns are correct (id, name, label)
        if 'id' not in nodes_df.columns:
            # nodes_df.columns = ['id', 'name', 'label']
            nodes_df.columns = ['name', 'id']
        
        # Fill empty labels with empty string
        # nodes_df['label'] = nodes_df['label'].fillna('')
        
        self.conn.execute("DROP TABLE IF EXISTS nodes")
        # self.conn.execute("""
        #     CREATE TABLE nodes (
        #         id INTEGER PRIMARY KEY,
        #         name VARCHAR,
        #         label VARCHAR
        #     )
        # """)
        
        self.conn.execute("""
            CREATE TABLE nodes (
                name VARCHAR,
                id INTEGER PRIMARY KEY
            )
        """)
        self.conn.register('nodes_df', nodes_df)
        self.conn.execute("INSERT INTO nodes SELECT * FROM nodes_df")
        
        nfa_df = pd.read_csv(nfa_edges_path)
        if 'from' in nfa_df.columns and 'to' in nfa_df.columns:
            nfa_df = nfa_df.rename(columns={'from': 'from_state', 'to': 'to_state'})
        elif 'from_state' not in nfa_df.columns:
            nfa_df = nfa_df.rename(columns={
                nfa_df.columns[0]: 'from_state',
                nfa_df.columns[1]: 'to_state',
                nfa_df.columns[2]: 'label'
            })
        
        self.conn.execute("DROP TABLE IF EXISTS nfa_edges")
        self.conn.execute("""
            
            CREATE OR REPLACE TABLE nfa_edges AS
            SELECT * FROM (VALUES
                -- (transfer | purchase | sale)+
                (0, 1, 'transfer'),
                (0, 1, 'purchase'),
                (0, 1, 'sale'),
                (1, 1, 'transfer'),
                (1, 1, 'purchase'),
                (1, 1, 'sale'),
                -- transition: dense → fraud labels
                (1, 2, 'phishing'),
                (1, 2, 'scam'),
                -- (phishing | scam)+
                (2, 2, 'phishing'),
                (2, 2, 'scam')
            ) AS t(from_state, to_state, label);

        """)
        # self.conn.register('nfa_df', nfa_df)
        # self.conn.execute("INSERT INTO nfa_edges SELECT * FROM nfa_df")
        
        # Load edges
        edges_df = pd.read_csv(edges_path)
        if 'edge_id' not in edges_df.columns:
            edges_df['edge_id'] = range(len(edges_df))
        
        # Ensure column names match
        if 'from' in edges_df.columns and 'to' in edges_df.columns:
            edges_df = edges_df.rename(columns={'from': 'src', 'to': 'dst'})
            
        # edges_df = edges_df[['edge_id', 'src', 'dst', 'label', 'weight']]
        
        # edge_id,src,dst,post_id,weight,label,sentiment
        self.conn.register('edges_df', edges_df)
        self.conn.execute("DROP TABLE IF EXISTS edges")
        self.conn.execute("""
            CREATE TABLE edges AS ( SELECT * FROM edges_df )
        """)
        
        self.conn.execute("DROP TABLE IF EXISTS nfa_nodes")
        self.conn.execute("""
            CREATE TABLE nfa_nodes (
                id INTEGER,
                type VARCHAR
            )
        """)
        self.conn.register('nfa_nodes_df', pd.read_csv(nfa_nodes_path))
        self.conn.execute("INSERT INTO nfa_nodes SELECT * FROM nfa_nodes_df")
        
        self.conn.execute("CREATE INDEX idx_edges_src ON edges(src)")
        self.conn.execute("CREATE INDEX idx_nfa ON nfa_edges(from_state, label)")
        
        
        disable_optimizer = False
        if disable_optimizer:
            # self.conn.execute("SET disabled_optimizers = 'join_order,build_side_probe_side';")
            self.conn.execute("PRAGMA disable_optimizer;")

    def run_gen_recap_query(self, min_length: int, max_length: int) -> Tuple[int, float]:
        c = self.conn

        # graph_start_node   = self.clean_array(c.execute("SELECT id FROM nodes WHERE label = 'Start'").fetchall())
        recap_start_state  = self.clean_array(c.execute("SELECT id FROM nfa_nodes WHERE type = 'initial'").fetchall())
        accepting_states   = self.clean_array(c.execute("SELECT id FROM nfa_nodes WHERE type = 'accepting'").fetchall())

        # Format accepting states for IN clause
        if isinstance(accepting_states, tuple):
            accepting_states_sql = ", ".join(str(s) for s in accepting_states)
        else:
            accepting_states_sql = str(accepting_states)

        print("*" * 60)
        print(f"Running query: length [{min_length}, {max_length}]")
        
        query = f"""
        WITH RECURSIVE paths AS (
    -- Base case
            SELECT
                383 as v, 
                0 as state,
                []::DOUBLE[] AS risk_scores,
                []::DOUBLE[] AS norm_scores,
                []::DOUBLE[] AS fraud_scores,
                []::BIGINT[] AS timestamps,
                []::VARCHAR[] AS regions,
                []::DOUBLE[] AS amounts,
                []::BIGINT[] AS edge_ids,
                0 AS path_length
            
            UNION ALL
            
            -- Recursive case
            SELECT 
                t.dst AS v,
                n.to_state AS state,
                list_append(p.risk_scores, t.risk_score) AS risk_scores,
                CASE
                    WHEN t.label IN ('transfer', 'purchase', 'sale') THEN list_append(p.norm_scores, t.risk_score)
                    ELSE p.norm_scores
                END AS norm_scores,
                CASE
                    WHEN t.label IN ('phishing', 'scam') THEN list_append(p.fraud_scores, t.risk_score)
                    ELSE p.fraud_scores
                END AS fraud_scores,
                -- (transfer | purchase | sale)+ · (phishing | scam)
                list_append(p.timestamps, t.timestamp_ms) AS timestamps,
                list_append(p.regions, t.location_region) AS regions,
                list_append(p.amounts, t.amount) AS amounts,
                list_append(p.edge_ids, t.edge_id) AS edge_ids,
                p.path_length + 1 AS path_length
            FROM paths p
            JOIN edges t ON p.v = t.src
            JOIN nfa_edges n ON p.state = n.from_state and t.label = n.label 
            WHERE p.path_length < {max_length}
        )
        SELECT COUNT(*)
        FROM paths 
        WHERE state = 2
        AND path_length >= {min_length}
        AND len(list_distinct(regions)) = 1
        AND NOT list_contains(
                [timestamps[i] <= timestamps[i-1]
                FOR i IN range(2, len(timestamps)+1)],
                true
            )
        AND len(edge_ids) = len(list_distinct(edge_ids))
        -- total amount threshold
        AND list_sum(amounts) >= 1000
          -- risk score range over normal prefix
        AND list_max(norm_scores) - list_min(norm_scores) <= 20
        AND norm_scores[-1] >= 40
        """

        reset_udf_stats()
        t0 = time.perf_counter()
        result = c.execute(query).fetchone()
        wall_time = time.perf_counter() - t0

        print(f"  ✓ {result[0]} paths found in {1000*wall_time:.2f}ms (wall)")

        return result[0], wall_time


def main():
    parser = argparse.ArgumentParser(description='ReCAP Color+Trail UDF with profiling')
    parser.add_argument('--edges',    required=True, help='Path to edges CSV')
    parser.add_argument('--nodes',    required=True, help='Path to nodes CSV')
    parser.add_argument('--nfanodes', required=True, help='Path to NFA nodes CSV')
    parser.add_argument('--nfa',      required=True, help='Path to NFA edges CSV')
    args = parser.parse_args()

    recap = DuckDBGeneralReCAPQuery()

    print("-" * 50)
    print("Reading data from:", args.nodes)

    recap.load_data(args.nodes, args.edges, args.nfanodes, args.nfa, True)

    # print("-" * 50)
    for max_len in range(2,11):
        recap.run_gen_recap_query(2, max_len)

    print("-" * 60)


if __name__ == "__main__":
    main()