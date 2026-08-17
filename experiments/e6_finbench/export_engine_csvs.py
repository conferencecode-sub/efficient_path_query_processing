"""One-off export: materializes `common.load_data()`'s edges/nodes tables
to CSV for the graph-DBMS competitor comparison (Neo4j/Memgraph/Kùzu),
which need file-based bulk loading rather than DuckDB's in-process tables.
"""
import os

import duckdb

from common import load_data

_HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(_HERE, "..", "datasets", "finbench_sf0.1_engine_ready")

if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    conn = duckdb.connect()
    load_data(conn)
    conn.execute(f"COPY nodes TO '{OUT_DIR}/nodes.csv' (HEADER, DELIMITER ',')")
    conn.execute(f"COPY edges TO '{OUT_DIR}/edges.csv' (HEADER, DELIMITER ',')")
    n = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    e = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    print(f"wrote {n:,} nodes, {e:,} edges to {OUT_DIR}")
    print(conn.execute("SELECT label, COUNT(*) FROM edges GROUP BY label ORDER BY label").fetchall())
