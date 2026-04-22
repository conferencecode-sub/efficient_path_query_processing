#!/usr/bin/env python3
import duckdb
import pandas as pd
import numpy as np
import json
import time
import argparse
from typing import Dict, List, Optional, Tuple, Any

# ============================================================================
# Main ReCAP (inline) Class with Monotonic Trail 
# ============================================================================
class RecapInlineMonotonicTrailDB:
    
    def __init__(self, db_path: str = ':memory:'):
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
        
    
    def run_with_inline_monotonic_trail(self, 
                                    min_length,
                                     max_length, start_node) -> Tuple[int, float]:
        """
        Run ReCAP query using the combined monotonic trail inline
        """
    
        query_nfa_no_label_init_state = f""" SELECT id FROM nfa_nodes WHERE type = 'initial' """
        query_nfa_no_label_accepting_states = f""" SELECT id FROM nfa_nodes WHERE type = 'accepting' """
        
        recap_start_state_nfa = self.clean_array(self.conn.execute(query_nfa_no_label_init_state).fetchall())
        # print("NFA start state:", recap_start_state_nfa)
        accepting_states_nfa = self.clean_array(self.conn.execute(query_nfa_no_label_accepting_states).fetchall())
        # print("NFA accepting states:", accepting_states_nfa)
        
        print("*"*60)
        print("Proceeding to run query with parameters...")        

        query = f"""
        WITH RECURSIVE paths AS (
            -- Base case: Initialize monotonic trail state
            SELECT 
                {start_node}              AS current_node,
                0 AS nfa_state,
                -1 as monotonicity_dictionary,
                CAST([] AS INTEGER[]) as trail_dictionary,  
                0 AS path_length

            UNION ALL

            SELECT
                e.dst AS current_node,
                n.to_state AS nfa_state,
                e.weight as monotonicity_dictionary,
                list_append(p.trail_dictionary, e.edge_id) as trail_dictionary,
                p.path_length + 1 AS path_length                                                            
            FROM paths p 
            INNER JOIN edges e ON e.src = p.current_node
            INNER JOIN nfa_edges n ON n.from_state = p.nfa_state AND n.label = e.label
            WHERE NOT list_contains(p.trail_dictionary, e.edge_id)
              AND (e.weight > p.monotonicity_dictionary)
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

    recap = RecapInlineMonotonicTrailDB()
    
    print("-"*50)
    print("Reading data from:", args.edges)
    
    # Load data
    recap.load_data(args.nodes, args.edges, args.nfanodes, args.nfa, True)
    
    start_nodes=[14485, 13689, 16177, 11863, 33412, 7412, 19197, 17148, 14974, 8271, 4498, 10308, 7460] #reddit
    
    print("\n" + "="*60)
    print("BENCHMARK RESULTS")
    print("="*60)
    
    for start_node in start_nodes:
        print(f"\n{'='*60}")
        print(f"Testing with start node: {start_node}")
        print(f"{'='*60}")
        results = []
        stop_early = False
        #  results.append(bench.run_q1_regex(MIN_LENGTH, MAX_LENGTH, WARMUP_RUNS, TIMED_RUNS))
    
        n = 10
        for length in range(2, n + 1):
            for runs in range(1, 4):
                print(f"\nRunning monotonic trail (ReCAP inline) with length [{length}] and run {runs}...")
                metrics = recap.run_with_inline_monotonic_trail(2, length, start_node)

    print("-"*60)

if __name__ == "__main__":
    main()