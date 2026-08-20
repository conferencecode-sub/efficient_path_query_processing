"""Exports Datagen-7.7's bidirected graph (same loading convention as
`run_recap_sum_weights.py`/`run_q3_datagen77.py`: bidirect the raw `.e`
file, `ignore_errors=true` for the one truncated final row) to two plain
CSVs suitable for Memgraph's `LOAD CSV`: `nodes.csv` (one `id` column) and
`edges.csv` (`edge_id,src,dst,weight`). Written under this directory's own
`data_export/` (not committed as a permanent dataset copy -- deleted by
`load_memgraph.py` after a successful load) rather than `/tmp`, since
`/tmp` sits on this machine's small (~15GB free) root partition while
`/home` has terabytes free.
"""
from __future__ import annotations

import os

import duckdb

_HERE = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(_HERE, "..", "..", "datasets", "datagen7.7")
EDGES_RAW = os.path.join(DATASET_DIR, "edges.e")
EXPORT_DIR = os.path.join(_HERE, "data_export")


def main() -> None:
    os.makedirs(EXPORT_DIR, exist_ok=True)
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE raw_edges AS SELECT * FROM read_csv(?, delim=' ', header=false, ignore_errors=true, "
        "columns={'src': 'BIGINT', 'dst': 'BIGINT', 'weight': 'DOUBLE'})", [EDGES_RAW])
    conn.execute(
        "CREATE TABLE edges AS SELECT ROW_NUMBER() OVER () AS edge_id, src, dst, weight FROM "
        "(SELECT src, dst, weight FROM raw_edges UNION ALL SELECT dst, src, weight FROM raw_edges)")
    conn.execute(
        "CREATE TABLE nodes AS SELECT DISTINCT v AS id FROM "
        "(SELECT src AS v FROM edges UNION SELECT dst AS v FROM edges)")

    nodes_path = os.path.join(EXPORT_DIR, "nodes.csv")
    edges_path = os.path.join(EXPORT_DIR, "edges.csv")
    conn.execute(f"COPY nodes TO '{nodes_path}' (HEADER, DELIMITER ',')")
    conn.execute(f"COPY edges TO '{edges_path}' (HEADER, DELIMITER ',')")

    n_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    n_edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    print(f"wrote {n_nodes:,} nodes to {nodes_path}")
    print(f"wrote {n_edges:,} edges to {edges_path}")


if __name__ == "__main__":
    main()
