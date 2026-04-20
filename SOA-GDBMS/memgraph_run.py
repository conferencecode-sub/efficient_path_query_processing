#!/usr/bin/env python3
"""
Memgraph Benchmark Script
Creates database, imports data, runs queries, reports timing

Memgraph uses Cypher and is compatible with the neo4j Python driver.
"""

from neo4j import GraphDatabase
import pandas as pd
import statistics
import time

# ============================================================================
#                          CONFIGURATION
# ============================================================================

MEMGRAPH_URI = "bolt://localhost:7687"
MEMGRAPH_USER = ""                  # Memgraph default: no auth
MEMGRAPH_PASSWORD = ""

NODES_PATH = "path/to/nodes.csv"  # Update with actual path to nodes CSV
EDGES_PATH = "path/to/edges.csv"  # Update with actual path to edges CSV
FRESH_DB = not True                     # Clear existing data

WARMUP_RUNS = 1
TIMED_RUNS = 3
QUERY_TIMEOUT_S = 7200         # 2 hours in seconds

MIN_LENGTH = 2
MAX_LENGTH = 10

# Which queries to run: 'paths', 'trails', 'monotonic', 'same_color', 'all'
QUERY_TYPE = 'trail'

# ============================================================================
#                          BENCHMARK CLASS
# ============================================================================

class MemgraphBenchmark:
    def __init__(self, uri: str, user: str = "", password: str = ""):
        print(f"Connecting to Memgraph at {uri}...")
        
        # Memgraph often runs without auth
        if user and password:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        else:
            self.driver = GraphDatabase.driver(uri)
        
        self.driver.verify_connectivity()
        print("  ✓ Connected")
    
    def close(self):
        self.driver.close()
        print("\nDatabase connection closed.")
    
    def run(self, query: str, **params):
        with self.driver.session() as session:
            result = session.run(query, **params)
            return [record.data() for record in result]
    
    def clear_database(self):
        print("Clearing existing data...")
        self.run("MATCH (n) DETACH DELETE n")
        # Drop all indexes
        try:
            indexes = self.run("SHOW INDEX INFO")
            for idx in indexes:
                self.run(f"DROP INDEX ON :{idx.get('label', '')}({idx.get('property', '')})")
        except:
            pass
        print("  ✓ Database cleared")
    
    def create_indexes(self):
        print("Creating indexes...")
        # Memgraph index syntax
        self.run("CREATE INDEX ON :Node(id)")
        self.run("CREATE INDEX ON :Node(name)")
        print("  ✓ Indexes created")
    
    def load_data_batch(self, nodes_path: str, edges_path: str):
        print("Loading data (batch mode)...")
        
        # Load nodes
        nodes_df = pd.read_csv(nodes_path)
        if 'id' not in nodes_df.columns:
            nodes_df.columns = ['id', 'name']
        # nodes_df['label'] = nodes_df['label'].fillna('')
        
        # Convert to list of dicts, ensuring proper types
        nodes_data = []
        for _, row in nodes_df.iterrows():
            nodes_data.append({
                'id': int(row['id']),
                'name': str(row['name'])
                # 'label': str(row['label'])
            })
        
        start = time.time()
        with self.driver.session() as session:
            session.run("""
                UNWIND $nodes AS node
                CREATE (n:Node {id: node.id, name: node.name})
            """, nodes=nodes_data)
        node_time = (time.time() - start) * 1000
        print(f"  ✓ Loaded {len(nodes_df)} nodes in {node_time:.2f} ms")
        
        # Load edges
        edges_df = pd.read_csv(edges_path)
        if 'from' in edges_df.columns:
            edges_df = edges_df.rename(columns={'from': 'src', 'to': 'dst'})
        if 'edge_id' not in edges_df.columns:
            edges_df['edge_id'] = range(len(edges_df))
    
        # edge_id,src,dst,post_id,weight,label,sentiment
        # Convert to list of dicts with proper types
        # edge_id,timestamp_ms,hour_of_day,src,dst,amount,label,location_region,risk_score
        edges_data = []
        for _, row in edges_df.iterrows():
            edges_data.append({
                'src': int(row['src']),
                'dst': int(row['dst']),
                'edge_id': int(row['edge_id']),
                'timestamp_ms': int(row['timestamp_ms']),
                'hour_of_day': int(row['hour_of_day']),
                'amount': float(row['amount']),
                'label': str(row['label']),
                'location_region': str(row['location_region']),
                'risk_score': float(row['risk_score'])

            })
            
        
        # edge_id,src,dst,label,weight,color,original_edge_id
        start = time.time()
        with self.driver.session() as session:

            transfer_data = [e for e in edges_data if e['label'] == 'transfer']
            purchase_data = [e for e in edges_data if e['label'] == 'purchase']
            sale_data = [e for e in edges_data if e['label'] == 'sale']
            phishing_data = [e for e in edges_data if e['label'] == 'phishing']
            scam_data = [e for e in edges_data if e['label'] == 'scam']
            
            session.run("""
                UNWIND $edges AS row
                MATCH (s:Node {id: row.src}), (d:Node {id: row.dst})
                CREATE (s)-[:transfer {
                    edge_id: row.edge_id,
                    time: row.timestamp_ms,
                    region: row.location_region,
                    amount: row.amount,
                    risk_score: row.risk_score
                }]->(d)
            """, edges=transfer_data)
        
            session.run("""
                    UNWIND $edges AS row
                    MATCH (s:Node {id: row.src}), (d:Node {id: row.dst})
                    CREATE (s)-[:purchase {
                        edge_id: row.edge_id,
                        time: row.timestamp_ms,
                        region: row.location_region,
                        amount: row.amount,
                        risk_score: row.risk_score
                    }]->(d)
                """, edges=purchase_data)
            
            session.run("""
                    UNWIND $edges AS row
                    MATCH (s:Node {id: row.src}), (d:Node {id: row.dst})
                    CREATE (s)-[:sale {
                        edge_id: row.edge_id,
                        time: row.timestamp_ms,
                        region: row.location_region,
                        amount: row.amount,
                        risk_score: row.risk_score
                    }]->(d)
                """, edges=sale_data)
            
            session.run("""
                    UNWIND $edges AS row
                    MATCH (s:Node {id: row.src}), (d:Node {id: row.dst})
                    CREATE (s)-[:scam {
                        edge_id: row.edge_id,
                        time: row.timestamp_ms,
                        region: row.location_region,
                        amount: row.amount,
                        risk_score: row.risk_score
                    }]->(d)
                """, edges=scam_data)
            
            session.run("""
                    UNWIND $edges AS row
                    MATCH (s:Node {id: row.src}), (d:Node {id: row.dst})
                    CREATE (s)-[:phishing {
                        edge_id: row.edge_id,
                        time: row.timestamp_ms,
                        region: row.location_region,
                        amount: row.amount,
                        risk_score: row.risk_score
                    }]->(d)
                """, edges=phishing_data)
            
        edge_time = (time.time() - start) * 1000
        print(f"  ✓ Loaded {len(edges_df)} edges in {edge_time:.2f} ms")
    
    def set_query_timeout(self, timeout_s: int):
        # Memgraph does not have a built-in query timeout, but we can set a transaction timeout
        # This will apply to all subsequent queries in the session
        self.run("SET DATABASE SETTING 'query.timeout' TO '" + str(timeout_s) + "'")
        print(f"  ✓ Query timeout set to {timeout_s} seconds")
    
    def run_query(self, query: str, name: str, warmup: int, runs: int):
        print(f"\n{'='*60}")
        print(f"Running: {name}")
        print(f"{'='*60}")
        
        # Warmup
        print(f"  Warmup ({warmup} runs)...", end=" ", flush=True)
        for _ in range(warmup):
            self.run(query)
        print("done")
        
        # Timed runs
        times = []
        result_value = None
        
        for i in range(runs):
            start = time.time()
            result = self.run(query)
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
            print(f"  Run {i+1}/{runs}: {elapsed:.2f} ms (result: {result})")
            
            if result_value is None and result:
                result_value = list(result[0].values())[0] if result else None
        
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        median_ms = statistics.median(times)
        
        print(f"\n  Results:")
        print(f"    Value:   {result_value}")
        print(f"    Avg:     {avg_time:.2f} ms")
        print(f"    Min:     {min_time:.2f} ms")
        print(f"    Max:     {max_time:.2f} ms")
        print(f"    Median:  {median_ms:.2f} ms")
        
        return {'name': name, 'result': result_value, 'avg_ms': avg_time, 'min_ms': min_time, 'max_ms': max_time, 'median_ms': median_ms}
    
    def run_path_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        query = f"""
            MATCH (start:Node {{label: 'Start'}})-[e:Edge*{min_len}..{max_len}]->(end)
            RETURN count(*) AS cnt
        """
        return self.run_query(query, f"Paths [{min_len},{max_len}]", warmup, runs)
    
    def run_gen_recap_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        query = f"""
            MATCH p = (start:Node {{id: 383}})
                    -[normal: transfer|sale|purchase *1..{max_len}]->
                  (mid)
                    -[fraud: phishing|scam *1..{max_len}]->
                 (e)
            WHERE ALL(i IN range(0, size(relationships(p))-2) WHERE relationships(p)[i].time < relationships(p)[i+1].time)
            AND ALL(i IN range(0, size(relationships(p))-2) WHERE relationships(p)[i].region = relationships(p)[i+1].region)
            AND normal[size(normal)-1].risk_score >= 40.0
            AND reduce(total = 0.0, r IN relationships(p) | total + r.amount) >= 1000
            AND (  reduce(mx = 0.0,   r IN normal | CASE WHEN r.risk_score > mx THEN r.risk_score ELSE mx END)
            - reduce(mn = 100.0, r IN normal | CASE WHEN r.risk_score < mn THEN r.risk_score ELSE mn END)
            ) <= 20.0
            AND size(relationships(p)) <= {max_len}
            RETURN count(*) AS cnt
        """
        return self.run_query(query, f"Paths [{min_len},{max_len}]", warmup, runs)
    
    def run_trail_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        query = f"""
            MATCH path = (start:Node {{label: 'Start'}})-[e:Edge*{min_len}..{max_len}]->(end)
            RETURN count(path) AS cnt
        """
        return self.run_query(query, f"Trails [{min_len},{max_len}]", warmup, runs)
    
    def run_q3_max_min(self, min_len: int, max_len: int, warmup: int, runs: int):
        query = f"""
            MATCH p = (start:Node {{id: 320104}})-[e:Edge*{min_len}..{max_len}]->(end)
            WITH p, relationships(p) AS edges
            WITH [r IN edges | r.weight] AS weights
            WITH reduce(state = 99999999999, w IN weights |
                CASE WHEN w < state THEN w ELSE state END
            ) AS min_weight,
            reduce(state = -99999999999, w IN weights |
                CASE WHEN w > state THEN w ELSE state END
            ) AS max_weight
            WHERE max_weight - min_weight <= 2592000/2
            RETURN count(*)
        """
        return self.run_query(query, f"Trails [{min_len},{max_len}]", warmup, runs)
    
    def run_monotonic_trail_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        # Memgraph supports similar list comprehensions to Neo4j
        query = f"""
            MATCH path = (start:Node {{id: 6113}})-[e:Edge*{min_len}..{max_len}]->(end)
            WITH path, [r IN relationships(path) | r.weight] AS weights
            WITH reduce(state = -1, w IN weights |
                CASE WHEN w > state THEN w ELSE 99999999999 END
            ) AS is_increasing
            WHERE is_increasing != 99999999999
            RETURN count(*) AS cnt
        """
        return self.run_query(query, f"Monotonic Trails [{min_len},{max_len}]", warmup, runs)
    
    def run_same_color_trail_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        query = f"""
            MATCH path = (start:Node {{label: 'Start'}})-[e:Edge*{min_len}..{max_len}]->(end)
            WITH path, relationships(path) as rels
            WITH path, [r IN rels | r.color] as colors
            WHERE ANY(i IN range(0, size(colors)-2) WHERE colors[i] = colors[i+1])
            RETURN COUNT(*)
        """
        return self.run_query(query, f"Same-Color Trails [{min_len},{max_len}]", warmup, runs)


# ============================================================================
#                          MAIN
# ============================================================================

def main():
    bench = MemgraphBenchmark(MEMGRAPH_URI, MEMGRAPH_USER, MEMGRAPH_PASSWORD)
    
    try:
        if FRESH_DB:
            bench.clear_database()
            bench.create_indexes()
            bench.load_data_batch(NODES_PATH, EDGES_PATH)
        
        results = []
        bench.set_query_timeout(QUERY_TIMEOUT_S)
        
        print("\n" + "="*60)
        print("BENCHMARK RESULTS")
        print("="*60)
        
        for length in range(MIN_LENGTH, MAX_LENGTH + 1):
            results.append(bench.run_gen_recap_query(MIN_LENGTH, length, WARMUP_RUNS, TIMED_RUNS))
        
        # Summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"{'Query':<40} {'Result':<15} {'Avg (ms)':<15}")
        print("-"*70)
        for r in results:
            print(f"{r['name']:<40} {str(r['result']):<15} {r['avg_ms']:<15.2f}")
    
    finally:
        bench.close()


if __name__ == "__main__":
    main()