"""Loads Datagen-7.7's bidirected graph into the project's existing
Memgraph container (`memgraph`, bolt port 7688 -- same container
`experiments/e6_finbench_sf10/run_memgraph.py` uses, which notes it
"already held unrelated data from an earlier experiment"; `clear_database`
wipes it the same way).

Datagen-7.7 is far larger than FinBench SF10 (13.18M nodes / 53.8M edges
here vs. ~800K nodes there), so the row-by-row `UNWIND` + bolt-driver
batching those FinBench loaders use would be slow. Instead this uses
Memgraph's own `LOAD CSV`, run *inside* the container against files
`docker cp`'d in first (the container has no bind mount, and `/tmp` on
this host sits on a small ~15GB-free root partition, so
`export_for_memgraph.py` writes its CSVs under this directory's own
`data_export/` on `/home` instead, which has terabytes free) -- avoiding
both problems.
"""
from __future__ import annotations

import os
import subprocess
import time

from neo4j import GraphDatabase

_HERE = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(_HERE, "data_export")
CONTAINER = "memgraph"
CONTAINER_DIR = "/tmp/datagen77_import"
MEMGRAPH_URI = "bolt://localhost:7688"


def _docker_cp_in() -> None:
    subprocess.run(["docker", "exec", CONTAINER, "mkdir", "-p", CONTAINER_DIR], check=True)
    for fname in ("nodes.csv", "edges.csv"):
        src = os.path.join(EXPORT_DIR, fname)
        subprocess.run(["docker", "cp", src, f"{CONTAINER}:{CONTAINER_DIR}/{fname}"], check=True)
        print(f"  copied {fname} into container")


def clear_database(driver) -> None:
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


def load_data(driver) -> None:
    with driver.session() as session:
        session.run("CREATE INDEX ON :Node(id)")
        t0 = time.perf_counter()
        session.run(f"""
            LOAD CSV FROM "{CONTAINER_DIR}/nodes.csv" WITH HEADER AS row
            CREATE (:Node {{id: toInteger(row.id)}})
        """)
        print(f"  nodes loaded in {time.perf_counter() - t0:.1f}s")

        t0 = time.perf_counter()
        session.run(f"""
            LOAD CSV FROM "{CONTAINER_DIR}/edges.csv" WITH HEADER AS row
            MATCH (s:Node {{id: toInteger(row.src)}}), (d:Node {{id: toInteger(row.dst)}})
            CREATE (s)-[:LINK {{edge_id: toInteger(row.edge_id), weight: toFloat(row.weight)}}]->(d)
        """)
        print(f"  edges loaded in {time.perf_counter() - t0:.1f}s")


def main() -> None:
    print("Copying CSVs into the Memgraph container...")
    _docker_cp_in()

    print(f"Connecting to Memgraph at {MEMGRAPH_URI}...")
    driver = GraphDatabase.driver(MEMGRAPH_URI)
    driver.verify_connectivity()
    try:
        print("Clearing database...")
        clear_database(driver)
        print("Loading Datagen-7.7 (bidirected)...")
        load_data(driver)
        with driver.session() as session:
            n_nodes = session.run("MATCH (n:Node) RETURN count(n) AS c").single()["c"]
            n_edges = session.run("MATCH ()-[r:LINK]->() RETURN count(r) AS c").single()["c"]
        print(f"loaded: {n_nodes:,} nodes, {n_edges:,} edges")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
