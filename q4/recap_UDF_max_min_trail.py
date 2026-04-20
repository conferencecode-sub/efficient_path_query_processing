#!/usr/bin/env python3
import duckdb
import pandas as pd
import json
import time
import argparse
from typing import Tuple
from collections import defaultdict

def recap_init_max_min() -> str:
    dictionary = {'max_so_far': -float('inf'), 'min_so_far': float('inf')}
    return json.dumps(dictionary)


# we take our old state, parse it, and update it with the new edge tuple values.
# we also need to update the NFA state here, but for simplicity, we will skip that part.
# in a real implementation, we would have a CASE statement to determine the next NFA state.
def recap_update_max_min(dictionary_json: str,
                                from_state: int,
                                to_state: int,
                                weight: float) -> str:
    # Parse existing state
    dictionary = json.loads(dictionary_json)
    dictionary['min_so_far'] = min(dictionary['min_so_far'], weight)
    dictionary['max_so_far'] = max(dictionary['max_so_far'], weight)
    return json.dumps(dictionary)

# we check if the new weight is greater than the last weight
# if not, we return False to indicate the path is invalid
def recap_is_valid_max_min(dictionary_json: str,
                                    from_state: int,
                                    to_state: int,
                                    weight: float) -> bool:
    dictionary = json.loads(dictionary_json)
    up, down = max(dictionary['max_so_far'], weight), min(dictionary['min_so_far'], weight)
    if up - down < (2592000/2): # we want to ensure that the max and min are within a certain range of each other
    # if up - down < (0.1):
        return True
    return False
    
def recap_is_valid_final_max_min(dictionary_json: str) -> bool:
    dictionary = json.loads(dictionary_json)
    
    return True
# we finalize by checking if we are in an accepting state
# in this case it is trivial.
def recap_finalize_max_min(dictionary_json: str) -> str:
    dictionary = json.loads(dictionary_json)
    return f"Max: {dictionary['max_so_far']}, Min: {dictionary['min_so_far']}"




def recap_update_max_min(dictionary_json: str, from_state: int, to_state: int, weight: float) -> str:

    dictionary = json.loads(dictionary_json)
    
    dictionary['min_so_far'] = min(dictionary['min_so_far'], weight)
    dictionary['max_so_far'] = max(dictionary['max_so_far'], weight)

    result = json.dumps(dictionary)
    return result
    
    # we check if the new weight is greater than the last weight
# if not, we return False to indicate the path is invalid
def recap_is_valid_max_min(dictionary_json: str,
                                    from_state: int,
                                    to_state: int,
                                    weight: float) -> bool:


    dictionary = json.loads(dictionary_json)

    up, down = max(dictionary['max_so_far'], weight), min(dictionary['min_so_far'], weight)
    result = (up - down) < (2592000/2) # we want to ensure that the max and min are within a certain range of each other

    return result


def recap_is_valid_final_max_min(dictionary_json: str) -> bool:

    _ = json.loads(dictionary_json)
    return True

def recap_finalize_max_min(dictionary_json: str) -> str:


    dictionary = json.loads(dictionary_json)

    result = "Yes"
    return result



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


def recap_finalize_trail(dictionary_json: str) -> str:
    
    result = "Trail"

    return result

def recap_is_valid_final_trail(dictionary_json: str) -> bool:
    _ = json.loads(dictionary_json)

    return True

# ============================================================================
#  Main ReCAP Class
# ============================================================================

class ReCAPUDFMaxMinTrailDB:
    """ReCAP implementation using max-min trail UDFs with profiling."""

    def __init__(self, db_path: str = ':memory:'):
        self.conn = duckdb.connect(db_path)
        self.register_udfs()

    def register_udfs(self):
        print("Registering Monotonic + Trail UDFs...")
        c = self.conn

        c.create_function("recap_init_max_min",          recap_init_max_min,          [],                                    'VARCHAR')
        c.create_function("recap_update_max_min",         recap_update_max_min,         ['VARCHAR','BIGINT','BIGINT','DOUBLE'], 'VARCHAR')
        c.create_function("recap_is_valid_max_min",       recap_is_valid_max_min,       ['VARCHAR','BIGINT','BIGINT','DOUBLE'], 'BOOLEAN')
        c.create_function("recap_finalize_max_min",       recap_finalize_max_min,       ['VARCHAR'],                           'VARCHAR')
        c.create_function("recap_is_valid_final_max_min", recap_is_valid_final_max_min, ['VARCHAR'],                           'BOOLEAN')

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
            
        edges_df = edges_df[['edge_id', 'src', 'dst', 'label', 'weight']]
        
        self.conn.execute("DROP TABLE IF EXISTS edges")
        self.conn.execute("""
            CREATE TABLE edges (
                edge_id INTEGER,
                src INT64,
                dst INT64,
                label VARCHAR,
                weight DOUBLE
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
        
        self.conn.execute("CREATE INDEX idx_edges_src ON edges(src)")
        self.conn.execute("CREATE INDEX idx_nfa ON nfa_edges(from_state, label)")
        
        self.conn.execute("SET threads = 1")
        
    
    def run_with_max_min_udf(self, min_length: int, max_length: int) -> Tuple[int, float]:
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
            SELECT
                CAST(76367 AS BIGINT)               AS current_node,
                {recap_start_state} AS nfa_state,
                recap_init_max_min()  AS max_min_dictionary,
                recap_init_trail()  AS trail_dictionary,
                0                   AS path_length

            UNION ALL

            SELECT
                CAST(e.dst AS BIGINT)               AS current_node,
                n.to_state                         AS nfa_state,
                recap_update_max_min(p.max_min_dictionary, n.from_state, n.to_state, e.weight) as max_min_dictionary,
                recap_update_trail(p.trail_dictionary, n.from_state, n.to_state, e.edge_id) as trail_dictionary,
                p.path_length + 1                                                                  AS path_length
            FROM paths p 
            INNER JOIN edges e ON e.src = p.current_node
            INNER JOIN nfa_edges n ON p.nfa_state = n.from_state AND n.label = e.label
            WHERE recap_is_valid_max_min(p.max_min_dictionary, n.from_state, n.to_state, e.weight)
              AND recap_is_valid_trail(p.trail_dictionary, n.from_state, n.to_state, e.edge_id)
              AND p.path_length < {max_length}
        )
        SELECT COUNT(*)
        FROM paths
        WHERE path_length >= {min_length}
          AND nfa_state IN ({accepting_states_sql})
          AND recap_is_valid_final_max_min(max_min_dictionary)
          AND recap_is_valid_final_trail(trail_dictionary)
        """

        # reset_udf_stats()
        t0 = time.perf_counter()
        result = c.execute(query).fetchone()
        wall_time = time.perf_counter() - t0

        print(f"  ✓ {result[0]} paths found in {1000*wall_time:.2f}ms (wall)")

        # --- UDF profile breakdown ---
        # print_udf_profile()

        # --- DuckDB operator profile ---
        print("DuckDB EXPLAIN ANALYZE:")
        plan = c.execute(f"EXPLAIN ANALYZE {query}").fetchall()
        for row in plan:
            print(row[1])

        return result[0], wall_time


def main():
    parser = argparse.ArgumentParser(description='ReCAP Color+Trail UDF with profiling')
    parser.add_argument('--edges',    required=True, help='Path to edges CSV')
    parser.add_argument('--nodes',    required=True, help='Path to nodes CSV')
    parser.add_argument('--nfanodes', required=True, help='Path to NFA nodes CSV')
    parser.add_argument('--nfa',      required=True, help='Path to NFA edges CSV')
    parser.add_argument('--index',    required=True, help='Whether to create indexes')
    args = parser.parse_args()

    recap = ReCAPUDFMaxMinTrailDB()

    print("-" * 50)
    print("Reading data from:", args.nodes)
    print("Running Monotonic+Trail ReCAP", "with index" if args.index else "without index")

    recap.load_data(args.nodes, args.edges, args.nfanodes, args.nfa, args.index)

    # print("-" * 50)
    for max_len in range(2, 11):
       recap.run_with_max_min_udf(2, max_len)

    # print("-" * 60)


if __name__ == "__main__":
    main()