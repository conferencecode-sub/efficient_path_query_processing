#!/usr/bin/env python3
import duckdb
import pandas as pd
import numpy as np
import json
import time
import argparse
from typing import Dict, List, Optional, Tuple, Any

# ============================================================================
# Main ReCAP Class with Max-Min Trail Inline
# ============================================================================
class ReCAPInlineMaxMinTrailDB:
    """ReCAP implementation using max-min trail inline"""
    
    def __init__(self, db_path: str = ':memory:'):
        """Initialize DuckDB and register inline"""
        self.conn = duckdb.connect(db_path)
        
    def clean_array(self, result):
        if len(result) == 1:
            return (result[0][0])
        cleaned = ()
        for item in result:
            cleaned.append(item[0])
        return cleaned
    
    def load_data(self, nodes_path: str, edges_path: str, nfa_nodes_path: str, nfa_edges_path: str, with_index: bool = True):
        """Load edges and NFA data"""
        """Load all data including node tables"""
        print(f"Loading data files...")
        
        # ========== Load Graph Nodes ==========
        nodes_df = pd.read_csv(nodes_path)
        # Ensure columns are correct (id, name, label)
        if 'id' not in nodes_df.columns:
            nodes_df.columns = ['id', 'name', 'label']
        
        self.conn.execute("DROP TABLE IF EXISTS nodes")
        self.conn.execute("""
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                label VARCHAR
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
            CREATE TABLE nfa_edges (
                from_state INTEGER,
                to_state INTEGER,
                label VARCHAR
            )
        """)
        self.conn.register('nfa_df', nfa_df)
        self.conn.execute("INSERT INTO nfa_edges SELECT * FROM nfa_df")
        
        # Load edges
        edges_df = pd.read_csv(edges_path)
        if 'edge_id' not in edges_df.columns:
            edges_df['edge_id'] = range(len(edges_df))
        
        # Ensure column names match
        if 'from' in edges_df.columns and 'to' in edges_df.columns:
            edges_df = edges_df.rename(columns={'from': 'src', 'to': 'dst'})
            
        edges_df = edges_df[['src', 'dst', 'label', 'weight', 'color']]
        
        self.conn.execute("""
            CREATE TABLE edges (
                edge_id INTEGER,
                src INTEGER,
                dst INTEGER,
                label VARCHAR,
                weight DOUBLE,
                color VARCHAR
            )
        """)
        self.conn.register('edges_df', edges_df)
        self.conn.execute("""
            INSERT INTO edges
            SELECT ROW_NUMBER() OVER () - 1 AS edge_id,
                src, dst, label, weight, color
            FROM edges_df
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
        
    
    def run_with_inline_recap_max_min(self, 
                                    min_length,
                                     max_length, start_node) -> Tuple[int, float]:
        """
        Run ReCAP query using the combined monotonic trail UDF
        """
        
        # variables to define
            # graph start node
            # ReCAP Start states
            # ReCAP Accepting states
        # subquery to initialize the variables
    
        # query_start_node = f""" SELECT id FROM nodes WHERE label = 'Start' """
        query_nfa_no_label_init_state = f""" SELECT id FROM nfa_nodes WHERE type = 'initial' """
        query_nfa_no_label_accepting_states = f""" SELECT id FROM nfa_nodes WHERE type = 'accepting' """
        
        # graph_start_node = self.clean_array(self.conn.execute(query_start_node).fetchall())
        # print("Graph start node:", graph_start_node)
        recap_start_state_nfa = self.clean_array(self.conn.execute(query_nfa_no_label_init_state).fetchall())
        # print("NFA start state:", recap_start_state_nfa)
        accepting_states_nfa = self.clean_array(self.conn.execute(query_nfa_no_label_accepting_states).fetchall())
        # print("NFA accepting states:", accepting_states_nfa)
        
        print("*"*60)
        print("Proceeding to run query with parameters...")    

        query = f"""
        WITH RECURSIVE paths AS (
            SELECT
                CAST({start_node} AS BIGINT) AS current_node,
                {recap_start_state_nfa} as nfa_state,
                NULL::BIGINT AS min_weight,
                NULL::BIGINT AS max_weight,
                [] as edge_path,
                0 as path_length
            
            UNION ALL
            
            -- Recursive case: Update state with full edge tuple
            SELECT 
                CAST(e.dst AS BIGINT) AS current_node,
                n.to_state AS nfa_state,
                LEAST(p.min_weight, e.weight) AS min_weight,
                GREATEST(p.max_weight, e.weight) AS max_weight,
                list_append(p.edge_path, e.edge_id) as edge_path,
                p.path_length + 1 as path_length
                
            FROM paths p
            INNER JOIN edges e ON e.src = p.current_node
            INNER JOIN nfa_edges n ON n.from_state = p.nfa_state AND n.label = e.label
            WHERE ( GREATEST(p.max_weight, e.weight) - LEAST(p.min_weight, e.weight) <=  2592000/2) 
              AND NOT list_contains(p.edge_path, e.edge_id)
              AND p.path_length < {max_length}
        )
        SELECT COUNT(*)
        FROM paths 
        WHERE path_length >= {min_length} 
          AND nfa_state IN ({accepting_states_nfa})
        """
        start_time = time.time()
        result = self.conn.execute(query).fetchone()
        exec_time = time.time() - start_time
        
        print(f"  ✓ Query completed in {1000*exec_time:.4f}ms: {result[0]} paths found of length [{min_length}, {max_length}]")
        return result[0], exec_time
        
        # return result, exec_time

    
def main():
    parser = argparse.ArgumentParser(description='ReCAP Monotonic Trail UDF')
    parser.add_argument('--edges', required=True, help='Path to edges CSV')
    parser.add_argument('--nodes', required=True, help='Path to edges CSV')
    parser.add_argument('--nfanodes', required=True, help='Path to NFA CSV')
    parser.add_argument('--nfa', required=True, help='Path to NFA CSV')
    
    args = parser.parse_args()

    recap = ReCAPInlineMaxMinTrailDB()
    
    print("-"*50)
    print("Reading data from:", args.edges)
    
    # Load data
    recap.load_data(args.nodes, args.edges, args.nfanodes, args.nfa, True)
    
    start_nodes=[21990232602342, 15393162838270, 24189256232831, 15393162890992, 30786325874097, 28587302784377, 6597070144902, 30786325852906]
    
    print("\n" + "="*60)
    print("BENCHMARK RESULTS")
    print("="*60)
    
    for start_node in start_nodes:
        print(f"\n{'='*60}")
        print(f"Testing with start node: {start_node}")
        print(f"{'='*60}")
        results = []
        stop_early = False

        for length in range(2, 10 + 1):
            for runs in range(1, 4):
                print(f"\nRunning max-min trail UDF with length [{length}] and run {runs}...")
                metrics = recap.run_with_inline_recap_max_min(2, length, start_node)
                if metrics is None or metrics[0] == 0:
                    print("  No results returned, stopping early.")
                    stop_early = True
                    break
            if stop_early:
                break
        

    print("-"*60)

if __name__ == "__main__":
    main()