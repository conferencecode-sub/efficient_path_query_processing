#!/usr/bin/env python3
"""
Neo4j Benchmark Script
Creates database, imports data, runs queries, reports timing
"""

from neo4j import GraphDatabase
import pandas as pd
import time
import os
import statistics

# ============================================================================
#                          CONFIGURATION
# ============================================================================

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"         # Change this

NODES_PATH = "path/to/nodes.csv"  # Update with actual path to nodes CSV
EDGES_PATH = "path/to/edges.csv"  # Update with actual path to edges CSV
FRESH_DB = not True                     # Clear existing data

WARMUP_RUNS = 1
TIMED_RUNS = 3

QUERY_TIMEOUT = 2 * 60 * 60  # 2 hours in seconds

MIN_LENGTH = 2
MAX_LENGTH = 8

# Which queries to run: 'paths', 'trails', 'monotonic', 'same_color', 'all'
QUERY_TYPE = 'trails'

# ============================================================================
#                          BENCHMARK CLASS
# ============================================================================

class Neo4jBenchmark:
    def __init__(self, uri: str, user: str, password: str):
        print(f"Connecting to Neo4j at {uri}...")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.driver.verify_connectivity()
        print("  ✓ Connected")
    
    def close(self):
        self.driver.close()
        print("\nDatabase connection closed.")
    
    def _run(self, query: str, **params):
        """Execute a query and return results"""
        with self.driver.session() as session:
            result = session.run(query, timeout=QUERY_TIMEOUT, **params)
            return [record.data() for record in result]
    
    def _run_single(self, query: str, **params):
        """Execute a query and return single value"""
        with self.driver.session() as session:
            result = session.run(query, timeout=QUERY_TIMEOUT, **params)
            record = result.single()
            return record[0] if record else None

    def clear_database(self):
        """Delete all nodes and edges in batches"""
        print("Clearing database...")
        
        # Drop all indexes first
        indexes = self._run("SHOW INDEXES YIELD name RETURN name")
        for idx in indexes:
            try:
                self._run(f"DROP INDEX {idx['name']}")
            except:
                pass  # Some indexes can't be dropped
        
        # Delete in batches to avoid OOM
        total_deleted = 0
        # while True:
        result = self._run_single("""
            MATCH (n)
            CALL (n) {
            DETACH DELETE n
            } IN TRANSACTIONS 
        """)
        # print(result)
        if not result is None:
            # break
            total_deleted += result
            print(f"  Deleted {total_deleted} nodes...")
        
        print(f"  ✓ Cleared {total_deleted} nodes")

    def create_schema(self):
        """Create indexes for performance"""
        print("Creating indexes...")
        
        self._run("CREATE INDEX node_id IF NOT EXISTS FOR (n:Node) ON (n.id)")
        
        # Wait for index to be online
        time.sleep(1)
        print("  ✓ Indexes created")

    def load_data(self, nodes_path: str, edges_path: str):
        """Load data from CSV files"""
        print(f"Loading data...")
        
        # Read CSVs to get absolute paths and validate
        nodes_df = pd.read_csv(nodes_path)
        edges_df = pd.read_csv(edges_path)
        
        print(f"  Nodes: {len(nodes_df):,} rows")
        print(f"  Edges: {len(edges_df):,} rows")
        
        print(nodes_df.columns.tolist())
        print(edges_df.columns.tolist())
        
        # Get absolute paths for Neo4j
        nodes_abs = NODES_PATH.split("import")[-1].lstrip("/")
        edges_abs = EDGES_PATH.split("import")[-1].lstrip("/")
        
        # print(f"  Node CSV: {nodes_abs}")
        # print(f"  Edge CSV: {edges_abs}")
        
        # print(nodes_abs, edges_abs)

        # Load nodes in batches
        print("  Loading nodes...")
        start = time.time()
        
        self._run(f"""
            LOAD CSV WITH HEADERS FROM 'file:///{nodes_abs}' AS row
            CALL {{
                WITH row
                CREATE (n:Node {{
                    id: toInteger(row.id)
                }})
            }} IN TRANSACTIONS 
        """)
        
        node_time = (time.time() - start) * 1000
        print(f"    ✓ Nodes loaded in {node_time:.2f} ms")
        
        # Load edges in batches
        print("  Loading edges...")
        start = time.time()
        
        self._run(f"""
            LOAD CSV WITH HEADERS FROM 'file:///{edges_abs}' AS row
            CALL {{
                WITH row
                MATCH (s:Node {{id: toInteger(row.src)}})
                MATCH (d:Node {{id: toInteger(row.dst)}})
                CREATE (s)-[:EDGE {{
                    edge_id: toInteger(row.edge_id),
                    label: row.label,
                    weight: toFloat(row.weight)
                }}]->(d)
            }} IN TRANSACTIONS 
        """)
        
        edge_time = (time.time() - start) * 1000
        print(f"    ✓ Edges loaded in {edge_time:.2f} ms")
        
        print(f"  ✓ Total load time: {node_time + edge_time:.2f} ms")

    def run_query(self, name: str, query: str, warmup: int, runs: int):
        """Run a query with warmup and timing"""
        print(f"\nRunning: {name}")
        
        try:
        # Warmup
            for i in range(warmup):
                self._run_single(query)
                print(f"  Warmup {i+1}/{warmup}")
            
            # Timed runs
            times = []
            result = None
            for i in range(runs):
                start = time.time()
                result = self._run_single(query)
                elapsed = (time.time() - start) * 1000
                times.append(elapsed)
                print(f"  Run {i+1}/{runs}: {elapsed:.2f} ms (result: {result})")
            
            avg_ms = sum(times) / len(times)
            median_ms = statistics.median(times)
            print(f"  → Average: {avg_ms:.2f} ms")
            print(f"  → Median: {median_ms:.2f} ms")
            
            return {
                'name': name,
                'result': result,
                'avg_ms': avg_ms,
                'median_ms': median_ms,
                'times': times,
                'success': True
            }
    
        except Exception as e:
            error_msg = str(e).lower()
            if 'timeout' in error_msg or 'time' in error_msg:
                print(f"  ✗ Query TIMEOUT (limit: {QUERY_TIMEOUT}s)")
            else:
                print(f"  ✗ Query ERROR: {e}")
            
            return {
                'name': name,
                'result': -1,
                'avg_ms': -1,
                'median_ms': -1,
                'times': [],
                'success': False
            }

    # ========================================================================
    #                          QUERY DEFINITIONS
    # ========================================================================

    def run_relaxed_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        """Simple path query"""
        query = f"""
            MATCH p = (s WHERE s.id = 6113)-[*{min_len}..{max_len}]->(t)
            RETURN count(p)
        """
        return self.run_query(f"Relaxed Paths [{min_len}..{max_len}]", query, warmup, runs)

    def run_monotonic_trail_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        """Monotonically increasing weight trail"""
        query = f"""
            MATCH p = (s)-[r]->{min_len,max_len}(t)
            WITH p, [rel IN relationships(p) | rel.weight] AS weights
            WHERE all(i IN range(0, size(weights)-2) WHERE weights[i] < weights[i+1])
            RETURN count(p)
        """
        return self.run_query(f"Monotonic trails [{min_len}..{max_len}]", query, warmup, runs)

    def run_same_color_trail_query(self, min_len: int, max_len: int, warmup: int, runs: int):
        """All edges same color"""
        query = f"""
            MATCH p = (s WHERE s.id = 4515)-[r *{min_len}..{max_len}]-> (t)
            WITH p, [rel IN relationships(p) | rel.color] AS colors
            WHERE any(i IN range(0, size(colors)-2) WHERE colors[i] = colors[i+1])
            RETURN COUNT(*)
        """
        return self.run_query(f"Same color trails [{min_len}..{max_len}]", query, warmup, runs)

    def run_distinct_colors_query(self, min_len: int, max_len: int, num_colors: int, warmup: int, runs: int):
        """Exactly N distinct colors"""
        query = f"""
            MATCH p = (s WHERE s.id = 4515)-[r *{min_len}..{max_len}]->(t)
            WITH p, [rel IN relationships(p) | rel.color] AS colors
            UNWIND colors AS color
            WITH p, collect(DISTINCT color) AS distinctColors
            WHERE size(distinctColors) = {num_colors}
            RETURN count(p)
        """
        return self.run_query(f"Distinct {num_colors} colors [{min_len}..{max_len}]", query, warmup, runs)
    
    def run_monotonic_query(self, min_len: int, max_len: int, warmup: int, runs: int, start_node: int):
        query = f"""
            MATCH p = (s WHERE s.id = {start_node})-[*{min_len}..{max_len}]->(t)
            WITH p, relationships(p) AS edges
            WITH [r IN edges | r.weight] AS weights
            WITH reduce(state = -INF, w IN weights |
                CASE WHEN w > state THEN w ELSE INF END
            ) AS result
            WHERE result <> INF
            RETURN count(*)
        """
        return self.run_query(f"Monotonic growing paths [{min_len}..{max_len}]", query, warmup, runs)
    
    def run_q3_max_min(self, min_len: int, max_len: int, warmup: int, runs: int, start_node: int):
        query = f"""
            MATCH p = (s WHERE s.id = {start_node})-[*{min_len}..{max_len}]->(t)
            WITH p, relationships(p) AS edges
            WITH [r IN edges | r.weight] AS weights
            WITH reduce(state = INF, w IN weights |
                CASE WHEN w < state THEN w ELSE state END
            ) AS min_weight,
            reduce(state = -INF, w IN weights |
                CASE WHEN w > state THEN w ELSE state END
            ) AS max_weight
            WHERE max_weight - min_weight <= 2592000/2
            RETURN count(*)
        """
        return self.run_query(f"Max-min paths [{min_len}..{max_len}]", query, warmup, runs)
    
    def run_q1_regex(self, min_len: int, max_len: int, warmup: int, runs: int):
        query = f"""
                MATCH p = (s WHERE s.id = 383)
                -[normal:(transfer|sale|purchase)*1..{max_len}]->
                (mid)
                -[fraud:(phishing|scam)*1..{max_len}]->
                (e)
                WHERE ALL(i IN range(0, size(relationships(p))-2) WHERE relationships(p)[i].time < relationships(p)[i+1].time)
                AND ALL(i IN range(0, size(relationships(p))-2) WHERE relationships(p)[i].region = relationships(p)[i+1].region)
                AND normal[size(normal)-1].risk_score >= 40.0
                AND reduce(total = 0.0, r IN relationships(p) | total + r.amount) >= 1000
                AND (  reduce(mx = 0.0,   r IN normal | CASE WHEN r.risk_score > mx THEN r.risk_score ELSE mx END)
                - reduce(mn = 100.0, r IN normal | CASE WHEN r.risk_score < mn THEN r.risk_score ELSE mn END)
                ) <= 20.0
                AND size(relationships(p)) <= {max_len}
                RETURN COUNT(*)
        """
        return self.run_query(f"Max-min paths [{min_len}..{max_len}]", query, warmup, runs)


# ============================================================================
#                          MAIN
# ============================================================================

def main():
    # Initialize
    bench = Neo4jBenchmark(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
    
    start_nodes=[15839, 29256, 14485, 13689, 16177, 11863, 33412, 7412, 19197, 17148, 14974, 8271, 4498, 10308, 7460]
    
    try:
        # Clear and load data if fresh
        if FRESH_DB:
            bench.clear_database()
            bench.create_schema()
            bench.load_data(NODES_PATH, EDGES_PATH)
        
        # Run queries
        results = []
        
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
        
            for length in range(MIN_LENGTH, MAX_LENGTH + 1):
                if stop_early:
                    print(f"Skipping remaining lengths for node {start_node}")
                    break
                
                metrics = bench.run_monotonic_query(MIN_LENGTH, length, WARMUP_RUNS, TIMED_RUNS, start_node)
                results.append(metrics)
                
                # Stop if timeout or zero results
                if not metrics['success']:
                    print(f"Stopping at length {length} for node {start_node} due to timeout/error")
                    stop_early = True
                elif metrics['result'] == 0:
                    print(f"Stopping at length {length} for node {start_node} - no results found")
                    stop_early = True
            
            # Summary for this node
            print(f"\n{'='*60}")
            print(f"SUMMARY FOR NODE {start_node}")
            print(f"{'='*60}")
            print(f"{'Query':<40} {'Result':<15} {'Avg (ms)':<15}")
            print("-"*70)
            for r in results:
                result_str = str(r['result']) if r['success'] else "TIMEOUT/ERROR"
                avg_str = f"{r['avg_ms']:.2f}" if r['success'] else "N/A"
                print(f"{r['name']:<40} {result_str:<15} {avg_str:<15}")
    
    finally:
        bench.close()


if __name__ == "__main__":
    main()