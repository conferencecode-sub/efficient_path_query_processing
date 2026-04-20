#!/usr/bin/env python3
import duckdb
import pandas as pd
import numpy as np
import json
import time
import argparse
from typing import Dict, List, Optional, Tuple, Any

### Design modification: Similar to the NFA "state/node" table, each ReCAP should also have a "start/node" table where a state is picked up as "Start" and "Accepting States"
###  

def recap_init_color() -> str:
    dictionary = {'current_color': None, 'satisfied_constraints': False}
    return json.dumps(dictionary)

# we take our old state, parse it, and update it with the new edge tuple values.
# we also need to update the NFA state here, but for simplicity, we will skip that part.
# in a real implementation, we would have a CASE statement to determine the next NFA state.
def recap_update_color(dictionary_json: str,
                                from_state: int,
                                to_state: int,
                                color: str) -> str:
    # Parse existing state
    dictionary = json.loads(dictionary_json)
    
    if dictionary['satisfied_constraints'] == True:
        return json.dumps(dictionary)
    
    else:
        if dictionary['current_color'] is None:
            dictionary['current_color'] = color
            return json.dumps(dictionary)
        else:
            if dictionary['current_color'] != color:
                dictionary['current_color'] = color
                return json.dumps(dictionary)
            else:
                dictionary['satisfied_constraints'] = True
                return json.dumps(dictionary)
        # Update last weight (for MONOTONIC constraint)
    return json.dumps(dictionary)

# we check if the new weight is greater than the last weight
# if not, we return False to indicate the path is invalid
def recap_is_valid_color(dictionary_json: str,
                                    from_state: int,
                                    to_state: int,
                                    color: str) -> bool:
    dictionary = json.loads(dictionary_json)
    
    return True
    
def recap_is_valid_final_color(dictionary_json: str) -> bool:
    dictionary = json.loads(dictionary_json)
    
    return dictionary["satisfied_constraints"]

# we finalize by checking if we are in an accepting state
# in this case it is trivial.
def recap_finalize_color(dictionary_json: str) -> str:
    dictionary = json.loads(dictionary_json)
    return str(dictionary["current_color"])

# ============================================================================
#           color Class
# ============================================================================ 


# ============================================================================
#           TRAIL Class
# ============================================================================ 
# simply extract the initial values and add them to our base case.
# then in the update function, we extract the values from the edge tuple and update our state
def recap_init_trail() -> str:
    dictionary = {'visited_edges': []}
    return json.dumps(dictionary)

# we take our old state, parse it, and update it with the new edge tuple values.
def recap_update_trail(dictionary_json: str,
                          from_state: int,
                          to_state: int,
                          edge_id: int) -> str:
    # Parse existing state
    dictionary = json.loads(dictionary_json)
    
    # Update visited edges (for ACYCLIC constraint)
    dictionary['visited_edges'].append(edge_id)
    return json.dumps(dictionary)

# we check if the edge has already been visited
# if so, we return False to indicate the path is invalid
def recap_is_valid_trail(dictionary_json: str,
                          from_state: int,
                          to_state: int,
                          edge_id: int) -> bool:
    # Parse state
    dictionary = json.loads(dictionary_json)
    # Check TRAIL constraint: edge must not be visited
    if edge_id in dictionary['visited_edges']:
        return False
    return True

# we finalize by checking if we are in an accepting state
# in this case it is trivial.
def recap_finalize_trail(dictionary_json: str) -> str:
    return "Trail"

# ============================================================================
#           TRAIL Class
# ============================================================================ 

# ============================================================================
# Main ReCAP DuckDB Class with 2-color Trail UDF
# ============================================================================
class ReCAPUDFTwoColorTrail:
    """ReCAP implementation using 2-color trail UDF"""
    
    def __init__(self, db_path: str = ':memory:'):
        """Initialize DuckDB and register UDFs"""
        self.conn = duckdb.connect(db_path)
        self.register_udfs()
    
    def register_udfs(self):
        """Register 2-color trail UDFs"""
        print("Registering 2-color Trail UDFs...")
        
        # Then functions for color
        self.conn.create_function(
            "recap_init_color", 
            recap_init_color,
            parameters=[],
            return_type='VARCHAR'
        )   
        
        self.conn.create_function(
            "recap_update_color",
            recap_update_color,
            parameters=['VARCHAR', 'BIGINT','BIGINT', 'VARCHAR'],
            return_type='VARCHAR'
        )
        
        self.conn.create_function(
            "recap_is_valid_color",
            recap_is_valid_color,
            parameters=['VARCHAR', 'BIGINT', 'BIGINT', 'VARCHAR'],
            return_type='BOOLEAN'
        )   
        
        self.conn.create_function(
            "recap_finalize_color",
            recap_finalize_color,
            parameters=['VARCHAR'],
            return_type='VARCHAR'
        )
        
        self.conn.create_function(
            "recap_is_valid_final_color",
            recap_is_valid_final_color,
            parameters=['VARCHAR'],
            return_type='BOOLEAN'
        )
        
         # Now the TRAIL functions
        self.conn.create_function(
            "recap_init_trail", 
            recap_init_trail,
            parameters=[],
            return_type='VARCHAR'
        )
        self.conn.create_function(
            "recap_update_trail",
            recap_update_trail,
            parameters=['VARCHAR', 'BIGINT', 'BIGINT', 'BIGINT'],
            return_type='VARCHAR'
        )
        self.conn.create_function(
            "recap_is_valid_trail",
            recap_is_valid_trail,
            parameters=['VARCHAR', 'BIGINT', 'BIGINT', 'BIGINT'],
            return_type='BOOLEAN'
        )
        self.conn.create_function(
            "recap_finalize_trail",
            recap_finalize_trail,
            parameters=['VARCHAR'],
            return_type='VARCHAR'
        )
        print("  ✓ Registered Monotonic Trail UDFs")
        
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
        
        # Fill empty labels with empty string
        nodes_df['label'] = nodes_df['label'].fillna('')
        
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
            
        edges_df = edges_df[['edge_id', 'src', 'dst', 'label', 'weight', 'color']]
        
        self.conn.execute("DROP TABLE IF EXISTS edges")
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
        self.conn.execute("INSERT INTO edges SELECT * FROM edges_df")
        
        self.conn.execute("DROP TABLE IF EXISTS nfa_nodes")
        self.conn.execute("""
            CREATE TABLE nfa_nodes (
                id INTEGER,
                type VARCHAR
            )
        """)
        self.conn.register('nfa_nodes_df', pd.read_csv(nfa_nodes_path))
        self.conn.execute("INSERT INTO nfa_nodes SELECT * FROM nfa_nodes_df")
        
    
    def run_with_pure_recap_color_trail_udf(self, 
                                    min_length,
                                     max_length) -> Tuple[int, float]:
        """
        Run ReCAP query using the combined monotonic trail UDF
        """
        
        # variables to define
            # graph start node
            # ReCAP Start states
            # ReCAP Accepting states
        # subquery to initialize the variables
    
        query_start_node = f""" SELECT id FROM nodes WHERE label = 'Start' """
        query_nfa_no_label_init_state = f""" SELECT id FROM nfa_nodes WHERE type = 'initial' """
        query_nfa_no_label_accepting_states = f""" SELECT id FROM nfa_nodes WHERE type = 'accepting' """
        
        graph_start_node = self.clean_array(self.conn.execute(query_start_node).fetchall())
        # print("Graph start node:", graph_start_node)
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
                {graph_start_node} as current_node,
                {recap_start_state_nfa} as nfa_state,
                recap_init_color() as color_dictionary,
                recap_init_trail() as trail_dictionary,
                0 as path_length
            
            UNION ALL
            
            -- Recursive case: Update state with full edge tuple
            SELECT 
                e.dst as current_node,
                n.to_state as nfa_state,
                recap_update_color(p.color_dictionary, n.from_state, n.to_state, e.color) as color_dictionary,
                recap_update_trail(p.trail_dictionary, n.from_state, n.to_state, e.edge_id) as trail_dictionary,
                p.path_length + 1 as path_length
                
            FROM paths p, edges e, nfa_edges n
            WHERE e.src = p.current_node 
              AND p.nfa_state = n.from_state AND n.label = e.label
              AND recap_is_valid_trail(p.trail_dictionary, n.from_state, n.to_state, e.edge_id)
              AND recap_is_valid_color(p.color_dictionary, n.from_state, n.to_state, e.color)
              AND p.path_length < {max_length}
        )
        SELECT COUNT(*)
        FROM paths 
        WHERE path_length >= {min_length} 
          AND nfa_state IN ({accepting_states_nfa})
          AND recap_is_valid_final_color(color_dictionary)
        """
          
        start_time = time.time()
        result = self.conn.execute(query).fetchone()
        exec_time = time.time() - start_time
        
        print(f"  ✓ Query completed in {1000*exec_time:.4f}ms: {result[0]} paths found of length [{min_length}, {max_length}]")
        
        # return result, exec_time
    
def main():
    parser = argparse.ArgumentParser(description='ReCAP Monotonic Trail UDF')
    parser.add_argument('--edges', required=True, help='Path to edges CSV')
    parser.add_argument('--nodes', required=True, help='Path to edges CSV')
    parser.add_argument('--nfanodes', required=True, help='Path to NFA CSV')
    parser.add_argument('--nfa', required=True, help='Path to NFA CSV')
    
    args = parser.parse_args()

    recap = ReCAPUDFTwoColorTrail()
    
    print("-"*50)
    print("Reading data from:", args.nodes)
    
    # Load data
    recap.load_data(args.nodes, args.edges, args.nfanodes, args.nfa, True)
    
    n = 6
    results_cons = []
    results = []
    for i in range(2, n+1):
        recap.run_with_pure_recap_color_trail_udf(2, i)

    print("-"*60)

if __name__ == "__main__":
    main()