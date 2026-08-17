#!/usr/bin/env python3
"""Neo4j benchmark runner -- loads a graph, runs one of the registered
queries over a length sweep, and reports runtime + intermediate-path count
+ peak memory (best-effort) per length. See `bench_common.py` for the
shared harness and `run_neo4j.sh` for a filled-in example invocation.

**Real bugs fixed here, not just reformatted.** The original script's
`main()` called `bench.run_monotonic_query(...)`, a method that doesn't
exist anywhere in the class -- it would `AttributeError` immediately.
Separately, its `load_data` created untyped `:EDGE` relationships (a
`label` *property*, no `time`/`region`/`amount`/`risk_score` properties at
all -- only `edge_id`/`label`/`weight` were ever set), while `run_q1_regex`
matched on typed relationships (`-[normal:(transfer|sale|purchase)*...]->`)
and referenced `.time`/`.region`/etc -- properties that were never loaded
and a relationship-typing convention the loader never created. Neither bug
is hypothetical: this script could not have produced a correct Q1 result
against any dataset as originally written. Fixed by adopting the schema
Memgraph's own script already gets right (typed relationships per label,
`time`/`region` aliased from `timestamp_ms`/`location_region`) -- Neo4j and
Memgraph both speak the same Cypher dialect closely enough that this
carries over directly.

**Query registry note.** As with the other two engines, only `q1` matches
the bundled `ReCAP/simple_dataset/LG.csv`/`LG_V.csv` schema. `q2`/`q3`/`q4`
are carried over from the original script's own weight/color-based
definitions (matching `ReCAP/q2`/`q3`'s DuckDB scripts), which need a
differently-shaped edges CSV not present in this repo -- refactored into
the same registry shape, not exercised here.
"""
from __future__ import annotations

import argparse
import os

import pandas as pd
from neo4j import GraphDatabase

import bench_common

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

Q1_EDGE_TYPES = ["transfer", "purchase", "sale", "phishing", "scam"]


# ============================================================================
#                          SCHEMA + DATA LOADING
# ============================================================================

def clear_database(driver) -> None:
    with driver.session() as session:
        session.run("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS")


def create_indexes(driver) -> None:
    with driver.session() as session:
        session.run("CREATE INDEX node_id IF NOT EXISTS FOR (n:Node) ON (n.id)")


def load_q1_schema_and_data(driver, nodes_path: str, edges_path: str) -> None:
    """Typed relationships per label, `time`/`region` aliased from
    `timestamp_ms`/`location_region` -- matches Q1's own WHERE clause and
    Memgraph's already-correct loader, unlike the original `load_data`."""
    nodes_df = pd.read_csv(nodes_path)
    edges_df = pd.read_csv(edges_path)
    if "edge_id" not in edges_df.columns:
        edges_df["edge_id"] = range(len(edges_df))

    with driver.session() as session:
        nodes_data = [{"id": int(row["id"])} for _, row in nodes_df.iterrows()]
        session.run("UNWIND $nodes AS n CREATE (:Node {id: n.id})", nodes=nodes_data)
        print(f"  loaded {len(nodes_data)} nodes")

        for etype in Q1_EDGE_TYPES:
            subset = edges_df[edges_df["label"] == etype]
            if subset.empty:
                print(f"  [{etype}] no edges, skipping")
                continue
            edges_data = [{
                "src": int(r["src"]), "dst": int(r["dst"]), "edge_id": int(r["edge_id"]),
                "time": int(r["timestamp_ms"]), "region": str(r["location_region"]),
                "amount": float(r["amount"]), "risk_score": float(r["risk_score"]),
            } for _, r in subset.iterrows()]
            session.run(f"""
                UNWIND $edges AS row
                MATCH (s:Node {{id: row.src}}), (d:Node {{id: row.dst}})
                CREATE (s)-[:{etype} {{
                    edge_id: row.edge_id, time: row.time, region: row.region,
                    amount: row.amount, risk_score: row.risk_score
                }}]->(d)
            """, edges=edges_data)
            print(f"  [{etype}] {len(edges_data):,} edges loaded")


_LOAD_BATCH_SIZE = 50_000  # a single unbatched UNWIND across LDBC100's 19.9M
# edges (one giant transaction, one giant Bolt parameter list) caused
# repeated 1-30s stop-the-world GC pauses in Neo4j until the driver's read
# eventually saw the connection as defunct -- fine at Bitcoin/Reddit scale
# (35K/286K edges), not at LDBC100 scale. Batching keeps each transaction
# and parameter list bounded regardless of dataset size.


def load_generic_schema_and_data(driver, nodes_path: str, edges_path: str) -> None:
    """Q2/Q3/Q4's schema: a single generic `Edge` relationship type
    carrying `weight`/`color` -- requires a differently-shaped edges CSV
    than the one bundled in this repo (see module docstring)."""
    nodes_df = pd.read_csv(nodes_path)
    edges_df = pd.read_csv(edges_path)
    if "edge_id" not in edges_df.columns:
        edges_df["edge_id"] = range(len(edges_df))

    with driver.session() as session:
        nodes_data = [{"id": int(row["id"])} for _, row in nodes_df.iterrows()]
        for i in range(0, len(nodes_data), _LOAD_BATCH_SIZE):
            batch = nodes_data[i:i + _LOAD_BATCH_SIZE]
            session.run("UNWIND $nodes AS n CREATE (:Node {id: n.id})", nodes=batch)
        print(f"  loaded {len(nodes_data):,} nodes")
        edges_data = [{
            "src": int(r["src"]), "dst": int(r["dst"]), "edge_id": int(r["edge_id"]),
            "weight": float(r["weight"]), "color": str(r["color"]),
        } for _, r in edges_df.iterrows()]
        for i in range(0, len(edges_data), _LOAD_BATCH_SIZE):
            batch = edges_data[i:i + _LOAD_BATCH_SIZE]
            session.run("""
                UNWIND $edges AS row
                MATCH (s:Node {id: row.src}), (d:Node {id: row.dst})
                CREATE (s)-[:Edge {edge_id: row.edge_id, weight: row.weight, color: row.color}]->(d)
            """, edges=batch)
            if (i // _LOAD_BATCH_SIZE) % 20 == 0:
                print(f"  ...{min(i + _LOAD_BATCH_SIZE, len(edges_data)):,}/{len(edges_data):,} edges loaded")
        print(f"  loaded {len(edges_data):,} edges")


# ============================================================================
#                          QUERY REGISTRY
# ============================================================================

def _q1_full(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s {{id: {starter}}})
            -[normal:transfer|sale|purchase*1..{max_len}]->(mid)
            -[fraud:phishing|scam*1..{max_len}]->(e)
        WHERE size(relationships(p)) <= {max_len}
        AND ALL(i IN range(0, size(relationships(p))-2)
                WHERE relationships(p)[i].time < relationships(p)[i+1].time)
        AND ALL(i IN range(0, size(relationships(p))-2)
                WHERE relationships(p)[i].region = relationships(p)[i+1].region)
        AND normal[size(normal)-1].risk_score >= 40.0
        AND reduce(total = 0.0, r IN relationships(p) | total + r.amount) >= 1000
        AND (reduce(mx = 0.0, r IN normal | CASE WHEN r.risk_score > mx THEN r.risk_score ELSE mx END)
             - reduce(mn = 100.0, r IN normal | CASE WHEN r.risk_score < mn THEN r.risk_score ELSE mn END)
            ) <= 20.0
        RETURN count(*) AS cnt
    """
    # min_len matches the original: only max_len bounds the pattern, no
    # explicit `size(relationships(p)) >= min_len` filter.


def _q1_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s {{id: {starter}}})
            -[normal:transfer|sale|purchase*1..{max_len}]->(mid)
            -[fraud:phishing|scam*1..{max_len}]->(e)
        WHERE size(relationships(p)) <= {max_len}
        RETURN count(*) AS cnt
    """


def _q2_full(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->()
        WITH p, [rel IN relationships(p) | rel.color] AS colors
        WHERE any(i IN range(0, size(colors)-2) WHERE colors[i] = colors[i+1])
        RETURN COUNT(*) AS cnt
    """


def _q2_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->()
        RETURN COUNT(*) AS cnt
    """


def _q3_full(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(t)
        WITH p, [rel IN relationships(p) | rel.weight] AS weights
        WHERE all(i IN range(0, size(weights)-2) WHERE weights[i] < weights[i+1])
        RETURN count(p) AS cnt
    """


def _q3_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(t)
        RETURN count(p) AS cnt
    """


Q4_MAX_MIN_BOUND = int(os.environ.get("Q4_MAX_MIN_BOUND", 20))  # was `2592000/2`
                        # (2,592,000s = 30 days) -- a timestamp-scale constant
                        # compared against 'weight' (0-100 range in the toy
                        # dataset), always trivially true. Same bug independently
                        # found in kuzu_run.py and the old-prototype q4 scripts --
                        # see experiments/q4_length_sweep/README.md. Fixed 2026-08-14
                        # to match q4_aggregate.py's own bound. Made env-var
                        # configurable (matching kuzu_run.py's own pattern) since
                        # LDBC100's 'weight' is epoch_ms(creation_date), needing
                        # Q4_MAX_MIN_BOUND=1209600000 (two weeks, ms) instead.


def _q4_full(starter: int, min_len: int, max_len: int) -> str:
    # The original used bare `INF`/`-INF` as the reduce seed -- not a valid
    # Cypher literal in Neo4j (unlike some SQL dialects), so the original
    # would likely fail to parse. Replaced with large numeric sentinels,
    # matching Memgraph's own (parseable) version of the same query.
    return f"""
        MATCH p = (s {{id: {starter}}})-[*{min_len}..{max_len}]->(t)
        WITH [r IN relationships(p) | r.weight] AS weights
        WITH reduce(state = 1e18, w IN weights | CASE WHEN w < state THEN w ELSE state END) AS min_weight,
             reduce(state = -1e18, w IN weights | CASE WHEN w > state THEN w ELSE state END) AS max_weight
        WHERE max_weight - min_weight <= {Q4_MAX_MIN_BOUND}
        RETURN count(*) AS cnt
    """


def _q4_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s {{id: {starter}}})-[*{min_len}..{max_len}]->(t)
        RETURN count(*) AS cnt
    """


QUERY_REGISTRY = {
    "q1": bench_common.QueryRegistryEntry(full=_q1_full, candidate_only=_q1_candidate_only),
    "q2": bench_common.QueryRegistryEntry(full=_q2_full, candidate_only=_q2_candidate_only),
    "q3": bench_common.QueryRegistryEntry(full=_q3_full, candidate_only=_q3_candidate_only),
    "q4": bench_common.QueryRegistryEntry(full=_q4_full, candidate_only=_q4_candidate_only),
}
GENERIC_SCHEMA_QUERIES = {"q2", "q3", "q4"}  # need the weight/color dataset shape, not Q1's


# ============================================================================
#                          MAIN
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Neo4j benchmark runner")
    bench_common.add_common_args(parser, query_choices=QUERY_REGISTRY)
    parser.add_argument("--uri", default=NEO4J_URI)
    parser.add_argument("--user", default=NEO4J_USER)
    parser.add_argument("--password", default=NEO4J_PASSWORD)
    args = parser.parse_args()

    print(f"Connecting to Neo4j at {args.uri}...")
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    driver.verify_connectivity()

    try:
        if args.fresh_db:
            clear_database(driver)
            create_indexes(driver)
            if args.query in GENERIC_SCHEMA_QUERIES:
                load_generic_schema_and_data(driver, args.nodes, args.edges)
            else:
                load_q1_schema_and_data(driver, args.nodes, args.edges)

        def execute_scalar(query: str):
            with driver.session() as session:
                result = session.run(query, timeout=args.timeout)
                return result.single()[0]

        memory_sampler = bench_common.make_memory_sampler(
            args.memory_source, container_name=args.container, process_name=args.process_name)

        results = bench_common.run_sweep(
            engine="neo4j", query_name=args.query, execute_scalar=execute_scalar,
            entry=QUERY_REGISTRY[args.query], starter=args.starter,
            min_len=args.min_len, max_len=args.max_len, warmup=args.warmup, runs=args.runs,
            memory_sampler=memory_sampler, csv_path=args.csv,
        )
        bench_common.print_summary(results)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
