#!/usr/bin/env python3
"""
Kuzu Benchmark Script
Creates database, imports data, runs queries, reports timing
"""

import kuzu
import pandas as pd
import time
import shutil
import os
import statistics

# ============================================================================
#                          CONFIGURATION
# ============================================================================

NODES_PATH = "path/to/nodes.csv"  # Update with actual path to nodes CSV
EDGES_PATH = "path/to/edges.csv"  # Update with actual path to edges CSV


DB_PATH = "./kuzu_db"
FRESH_DB = not True  # Delete existing db and start fresh

WARMUP_RUNS = 1          # Discarded warmup runs
TIMED_RUNS = 3      # Runs to average

MIN_LENGTH = 2
MAX_LENGTH = 10
EDGE_TYPES = ["transfer", "purchase", "sale", "phishing", "scam"]

# ============================================================================
#                          BENCHMARK CLASS
# ============================================================================

class KuzuBenchmark:
    def __init__(self, db_path: str, fresh: bool = True):
        self.db_path = db_path
        
        if fresh and os.path.exists(db_path):
            print(f"Removing existing database at {db_path}...")
            if os.path.isdir(db_path):
                shutil.rmtree(db_path)
            else:
                os.remove(db_path)
        
        print(f"Opening database at {db_path}...")
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)
        self.conn.set_query_timeout(7200000)  # 2 hours
        print("  ✓ Database connected")
    
    def create_schema(self):
        print("Creating schema...")
        
        self.conn.execute("""
           CREATE NODE TABLE IF NOT EXISTS Node(
                id INT,
                name STRING,
                PRIMARY KEY (id)
            )
        """)

        for etype in EDGE_TYPES:
            self.conn.execute(f"""
                CREATE REL TABLE {etype} (
                    FROM Node TO Node,
                    edge_id        INT64,
                    timestamp_ms   INT64,
                    hour_of_day    INT64,
                    amount         DOUBLE,
                    location_region STRING,
                    risk_score     DOUBLE
                )
            """)
        print("  ✓ Schema created")
    
    def load_data(self, nodes_path: str, edges_path: str):
        print(f"Loading data...")
        
        # Load nodes
        start = time.time()
        self.conn.execute(f"COPY Node FROM '{os.path.abspath(nodes_path)}' (header=true)")
        node_time = (time.time() - start) * 1000
        
        result = self.conn.execute("MATCH (n:Node) RETURN count(n)")
        node_count = result.get_next()[0]
        print(f"  ✓ Loaded {node_count} nodes in {node_time:.2f} ms")
        
        # Load edges - transform CSV for Kuzu format
        edges_df = pd.read_csv(edges_path)
        
        if 'from' in edges_df.columns:
            edges_df = edges_df.rename(columns={'from': 'src', 'to': 'dst'})
        
        if 'edge_id' not in edges_df.columns:
            edges_df['edge_id'] = range(len(edges_df))
        

        cols = ['src', 'dst', 'edge_id', 'timestamp_ms', 'hour_of_day', 'amount', 'label', 'risk_score']

        for etype in EDGE_TYPES:
            subset = edges_df[edges_df["label"] == etype][[
                "src", "dst", "edge_id", "timestamp_ms",
                "hour_of_day", "amount", "location_region", "risk_score"
            ]].rename(columns={"src": "src", "dst": "dst"})  # Kùzu requires 'from'/'to'
            if len(subset) == 0:
                print(f"  [{etype}] no edges, skipping")
                continue

            tmp = f"/tmp/{etype}.csv"
            subset.to_csv(tmp, index=False)

            self.conn.execute(f"""
                COPY {etype} FROM '{tmp}' (HEADER = TRUE)
            """)
            print(f"  [{etype}] {len(subset):,} edges loaded")
        
            os.remove(tmp)
    
    def run_query(self, query: str, name: str, warmup: int, runs: int):
        print(f"\n{'='*60}")
        print(f"Running: {name}")
        print(f"{'='*60}")
        
        # Warmup
        print(f"  Warmup ({warmup} runs)...", end=" ", flush=True)
        for _ in range(warmup):
            result = self.conn.execute(query)
            while result.has_next():
                result.get_next()
        print("done")
        
        # Timed runs
        times = []
        result_value = None
        
        for i in range(runs):
            start = time.time()
            result = self.conn.execute(query)
            
            rows = []
            while result.has_next():
                rows.append(result.get_next())
            
            # print(result)
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
            
            print(f"  Run {i+1}/{runs}: {elapsed:.2f} ms (result: {result})")
            
            if result_value is None and rows:
                result_value = rows[0][0] if len(rows) == 1 else len(rows)
                
        
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        median_value = statistics.median(times)
        
        print(f"\n  Results:")
        print(f"    Value:   {result_value}")
        print(f"    Avg:     {avg_time:.2f} ms")
        print(f"    Min:     {min_time:.2f} ms")
        print(f"    Max:     {max_time:.2f} ms")
        print(f"    Median:  {median_value:.2f} ms")
        
        return {'name': name, 'result': result_value, 'avg_ms': avg_time, 'min_ms': min_time, 'max_ms': max_time, 'median_ms': median_value}
    
    def run_q1_gen_recap_q(self, min_len: int, max_len: int, warmup: int, runs: int, starter_node: int):
        query = f"""
            MATCH path = (start:Node {{id: {starter_node}}})-[normal:transfer|sale|purchase *1..{max_len}]->
                (mid:Node)
                -[fraud: phishing|scam *1..{max_len}]->
                (e:Node)
            WITH path as path, normal as normal, properties(rels(normal), 'edge_id') AS normal_ids,
            fraud as fraud, properties(rels(fraud), 'edge_id') AS fraud_ids,
            list_reduce(
                    properties(rels(path), 'timestamp_ms'),
                            (acc, t) ->
                                CASE WHEN t > acc THEN t ELSE NULL END
                    ) AS ordered,
            list_reduce(
                    properties(rels(path), 'amount'),
                            (acc, a) ->
                                acc + a
                    ) AS total_amount,
            list_reduce(
                    properties(rels(path), 'location_region'),
                            (acc, r) ->
                               CASE WHEN acc = r THEN acc ELSE NULL END
                    ) AS same_region,
            list_reduce(
                    properties(rels(normal), 'risk_score'),
                            (acc, r) ->
                                CASE WHEN acc < r THEN acc ELSE r END
                    ) AS min_risk,
            list_reduce(
                    properties(rels(normal), 'risk_score'),
                            (acc, r) ->
                                CASE WHEN acc > r THEN acc ELSE r END
                    ) AS max_risk
            WHERE is_trail(path) 
                 AND size(normal_ids) + size(fraud_ids) <= {max_len}
                 AND ordered IS NOT NULL
                 AND total_amount >= 1000
                 AND same_region IS NOT NULL
                 AND max_risk - min_risk <= 20
                 AND properties(rels(normal), 'risk_score')[-1] >= 40.0
            RETURN count(*)
        """
        return self.run_query(query, f"General ReCAP Paths [{min_len},{max_len}]", warmup, runs)
    
    def run_q2_any_colors_query(self, min_len: int, max_len: int, warmup: int, runs: int, starter_node: int):
        query = f"""
            MATCH path = (start:Node {{label: {starter_node}}})-[e:Edge*{min_len}..{max_len}]->()
            WITH path as path, PROPERTIES(RELS(path), 'color') AS cs
            WITH cs as cs, RANGE(1, SIZE(cs)-1) AS idxs
            WHERE ANY(i IN idxs WHERE cs[i] = cs[i+1])
            RETURN COUNT(*);
        """
        return self.run_query(query, f"Any 2 color Full Paths [{min_len},{max_len}]", warmup, runs)
    
    def run_q3_monotonic_query(self, min_len: int, max_len: int, warmup: int, runs: int, start_node: int):
        query = f"""
            MATCH path = (start:Node {{id: {start_node}}})-[*{min_len}..{max_len}]->(t)
            WITH path as path,
            list_reduce(
                    properties(rels(path), 'weight'),
                            (acc, w) ->
                                CASE WHEN w > acc THEN w ELSE NULL END
                    ) AS result
            WHERE result IS NOT NULL AND is_trail(path) 
            RETURN COUNT(*);
        """
        return self.run_query(query, f"Monotonic growing paths [{min_len}..{max_len}]", warmup, runs)
    
    def run_q4_max_min_query(self, min_len: int, max_len: int, warmup: int, runs: int, starter_node: int):
        query = f"""
            MATCH path = (start:Node {{id: {starter_node}}})-[*{min_len}..{max_len}]->(t)
            WITH path as path,
            list_reduce(
                    properties(rels(path), 'weight'),
                            (acc, w) ->
                                CASE WHEN w > acc THEN w ELSE acc END
                    ) AS max_sf, 
            list_reduce(
                    properties(rels(path), 'weight'),
                            (acc, w) ->
                                CASE WHEN acc < w THEN acc ELSE w END
                    ) AS min_sf
            WHERE max_sf - min_sf <= 2592000/2 AND is_trail(path) 
            RETURN COUNT(*);
        """
        return self.run_query(query, f"Max-min paths [{min_len}..{max_len}]", warmup, runs)
    

# ============================================================================
#                          MAIN
# ============================================================================

def main():
    bench = KuzuBenchmark(db_path=DB_PATH, fresh=FRESH_DB)
    
    if FRESH_DB:
        bench.create_schema()
        bench.load_data(NODES_PATH, EDGES_PATH)
    
    results = []
    
    print("\n" + "="*60)
    print("BENCHMARK RESULTS")
    print("="*60)
    
    starter_node = 1 # change if needed
    for length in range(MIN_LENGTH, MAX_LENGTH + 1):
        results.append(bench.run_q1_gen_recap_q(MIN_LENGTH, length, WARMUP_RUNS, TIMED_RUNS, starter_node))
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"{'Query':<40} {'Result':<15} {'Avg (ms)':<15}")
    print("-"*70)
    for r in results:
        print(f"{r['name']:<40} {str(r['result']):<15} {r['avg_ms']:<15.2f}")


if __name__ == "__main__":
    main()