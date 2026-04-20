#!/usr/bin/env python3
import duckdb
import pandas as pd
import json
import time
import argparse
from typing import Tuple
from collections import defaultdict

EDGE_STRUCT = duckdb.struct_type({
    'edge_id': 'BIGINT',
    'timestamp_ms': 'BIGINT',
    'hour_of_day': 'BIGINT',
    'src': 'BIGINT',
    'dst': 'BIGINT',
    'amount': 'DOUBLE',
    'label': 'VARCHAR',
    'location_region': 'VARCHAR',
    'ip_prefix': 'DOUBLE',
    'login_frequency': 'BIGINT',
    'session_duration': 'BIGINT',
    'purchase_pattern': 'VARCHAR',
    'age_group': 'VARCHAR',
    'risk_score': 'DOUBLE',
    'anomaly': 'VARCHAR'
})

def recap_init_properties() -> str:
    return json.dumps(
        {
            'last_time': -9999,
            'region': None,
            'max_risk': -99999,
            'min_risk': 99999,
            'amount': 0,
            'last_risk': None
        }
    )

def recap_update_properties(dictionary_json: str, from_state: int, to_state: int, edge_data: dict) -> str:

    d = json.loads(dictionary_json)

    # d['last_weight'] = edge_data['weight']
    d['last_time'] = edge_data['timestamp_ms']
    d['region'] = edge_data['location_region']
    d['max_risk'] = max(d['max_risk'], edge_data['risk_score'])
    d['min_risk'] = min(d['min_risk'], edge_data['risk_score'])
    d['last_risk'] = edge_data['risk_score']
    d['amount'] += edge_data['amount']

    result = json.dumps(d)
    return result

def recap_is_valid_properties(dictionary_json: str, from_state: int, to_state: int, edge_data: dict) -> bool:

    d = json.loads(dictionary_json)
    
    all_satisfy = True
    
    if d['region'] is not None:
        all_satisfy = all_satisfy and (edge_data['location_region'] == d['region'])
        
    all_satisfy = all_satisfy and (edge_data['timestamp_ms'] > d['last_time'])
    
    if from_state == 0 and to_state == 1:
        all_satisfy = all_satisfy and ((max(d['max_risk'], edge_data['risk_score']) - min(d['min_risk'], edge_data['risk_score']) <= 20)  or (d['max_risk'] == -99999))
    elif from_state == 1 and to_state == 1:
        all_satisfy = all_satisfy and ((max(d['max_risk'], edge_data['risk_score']) - min(d['min_risk'], edge_data['risk_score']) <= 20)  or (d['max_risk'] == -99999))
    elif from_state == 1 and to_state == 2:
        all_satisfy = all_satisfy and (d['last_risk'] is not None and d['last_risk'] >= 40)

    return all_satisfy


def recap_is_valid_final_properties(dictionary_json: str) -> bool:

    d = json.loads(dictionary_json)

    return d['amount'] >= 1000


def recap_finalize_properties(dictionary_json: str) -> str:

    return "Ok"



# ============================================================================
#  Trail UDFs
# ============================================================================

def recap_init_trail() -> str:
    return json.dumps({'visited_edges': []})


def recap_update_trail(dictionary_json: str, from_state: int, to_state: int, edge_id: int) -> str:

    d = json.loads(dictionary_json)

    d['visited_edges'].append(edge_id)

    result = json.dumps(d)
    return result


def recap_is_valid_trail(dictionary_json: str, from_state: int, to_state: int, edge_id: int) -> bool:

    d = json.loads(dictionary_json)

    result = edge_id not in d['visited_edges']
    return result


def recap_is_valid_final_trail(dictionary_json: str) -> bool:

    return True


def recap_finalize_trail(dictionary_json: str) -> str:
    # No JSON parsing needed here, but counted for completeness
    result = "Trail"
    return result

# ============================================================================
#  Main ReCAP Class
# ============================================================================

class UDFReCAPQuery:

    def __init__(self, db_path: str = ':memory:'):
        self.conn = duckdb.connect(db_path)
        self.register_udfs()

    def register_udfs(self):
        print("Registering Monotonic + Trail UDFs...")
        c = self.conn

        c.create_function("recap_init_properties",          recap_init_properties,          [],                                    'VARCHAR')
        c.create_function("recap_update_properties",         recap_update_properties,         ['VARCHAR','BIGINT','BIGINT',EDGE_STRUCT], 'VARCHAR')
        c.create_function("recap_is_valid_properties",       recap_is_valid_properties,       ['VARCHAR','BIGINT','BIGINT',EDGE_STRUCT], 'BOOLEAN')
        c.create_function("recap_finalize_properties",       recap_finalize_properties,       ['VARCHAR'],                           'VARCHAR')
        c.create_function("recap_is_valid_final_properties", recap_is_valid_final_properties, ['VARCHAR'],                           'BOOLEAN')

        c.create_function("recap_init_trail",     recap_init_trail,     [],                                    'VARCHAR')
        c.create_function("recap_update_trail",   recap_update_trail,   ['VARCHAR','BIGINT','BIGINT','BIGINT'], 'VARCHAR')
        c.create_function("recap_is_valid_trail", recap_is_valid_trail, ['VARCHAR','BIGINT','BIGINT','BIGINT'], 'BOOLEAN')
        c.create_function("recap_finalize_trail", recap_finalize_trail, ['VARCHAR'],                           'VARCHAR')
        c.create_function("recap_is_valid_final_trail", recap_is_valid_final_trail, ['VARCHAR'], 'BOOLEAN')

        print("  ✓ Registered Monotonic + Trail UDFs")

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

    def run_gen_recap_inline(self, min_length: int, max_length: int) -> Tuple[int, float]:
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
                recap_init_properties() as properties,
                recap_init_trail() as trail,
                0 as path_length
            
            UNION ALL
            
            -- Recursive case
            SELECT 
                t.dst as v,
                n.to_state as state,
                recap_update_properties(p.properties, n.from_state, n.to_state, t) as properties,
                recap_update_trail(p.trail, n.from_state, n.to_state, t.edge_id) as trail,
                p.path_length + 1 as path_length
            FROM paths p
            JOIN edges t ON p.v = t.src
            JOIN nfa_edges n ON p.state = n.from_state and t.label = n.label 
            WHERE p.path_length < {max_length} -- change
                AND recap_is_valid_properties(p.properties, n.from_state, n.to_state, t)
                AND recap_is_valid_trail(p.trail, n.from_state, n.to_state, t.edge_id)
        )
        SELECT COUNT(*)
        FROM paths 
        WHERE state = 2
        AND path_length >= {min_length}
        AND recap_is_valid_final_properties(properties)
        AND recap_is_valid_final_trail(trail)
        """
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

    recap = UDFReCAPQuery()

    print("-" * 50)
    print("Reading data from:", args.nodes)

    recap.load_data(args.nodes, args.edges, args.nfanodes, args.nfa, True)

    # print("-" * 50)
    for max_len in range(2,11):
        recap.run_gen_recap_inline(2, max_len)

    print("-" * 60)


if __name__ == "__main__":
    main()