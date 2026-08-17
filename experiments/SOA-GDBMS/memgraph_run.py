#!/usr/bin/env python3
"""Memgraph benchmark runner -- loads a graph, runs one of the registered
queries over a length sweep, and reports runtime + intermediate-path count
+ peak memory (best-effort) per length. Memgraph speaks Cypher over bolt,
so it uses the `neo4j` Python driver like `neo4j_run.py` does. See
`bench_common.py` for the shared harness and `run_memgraph.sh` for a
filled-in example invocation.

**Real bug fixed here, not just reformatted.** The original script's
`main()` called `bench.run_gen_recap_query(...)`, a method that doesn't
exist (the real method is `run_q1_gen_recap_query`) -- it would
`AttributeError` immediately. Unlike `neo4j_run.py`, this script's own
schema loader (typed relationships per label, `time`/`region` aliased from
`timestamp_ms`/`location_region`) already matched its Q1 query correctly --
that part carries over unchanged, just refactored into the registry/CLI
shape.

**Query registry note.** As with the other two engines, only `q1` matches
the bundled `ReCAP/simple_dataset/LG.csv`/`LG_V.csv` schema. `q2`/`q3`/`q4`
are carried over from the original script's own weight-based definitions
(matching `ReCAP/q2`/`q3`'s DuckDB scripts), which need a differently-shaped
edges CSV not present in this repo -- refactored into the same registry
shape, not exercised here.
"""
from __future__ import annotations

import argparse
import os

import pandas as pd
from neo4j import GraphDatabase

import bench_common

MEMGRAPH_URI = "bolt://localhost:7687"
MEMGRAPH_USER = ""
MEMGRAPH_PASSWORD = ""

Q1_EDGE_TYPES = ["transfer", "purchase", "sale", "phishing", "scam"]


# ============================================================================
#                          SCHEMA + DATA LOADING
# ============================================================================

def clear_database(driver) -> None:
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


def create_indexes(driver) -> None:
    with driver.session() as session:
        session.run("CREATE INDEX ON :Node(id)")


def load_q1_schema_and_data(driver, nodes_path: str, edges_path: str) -> None:
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


_LOAD_BATCH_SIZE = 50_000  # see neo4j_run.py's identical constant -- same
# unbatched-UNWIND-at-LDBC100-scale issue applies here too (one giant
# transaction across 19.9M edges), fixed the same way.


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


def set_query_timeout(driver, timeout_s: int) -> None:
    with driver.session() as session:
        session.run(f"SET DATABASE SETTING 'query.timeout' TO '{timeout_s}'")


# ============================================================================
#                          QUERY REGISTRY
# ============================================================================

def _q1_full(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s:Node {{id: {starter}}})
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
    # min_len matches the original: only max_len bounds the pattern.


def _q1_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s:Node {{id: {starter}}})
            -[normal:transfer|sale|purchase*1..{max_len}]->(mid)
            -[fraud:phishing|scam*1..{max_len}]->(e)
        WHERE size(relationships(p)) <= {max_len}
        RETURN count(*) AS cnt
    """


def _q2_full(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(end)
        WITH p, relationships(p) as rels
        WITH p, [r IN rels | r.color] as colors
        WHERE ANY(i IN range(0, size(colors)-2) WHERE colors[i] = colors[i+1])
        RETURN COUNT(*) AS cnt
    """


def _q2_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(end)
        RETURN COUNT(*) AS cnt
    """


def _q3_full(starter: int, min_len: int, max_len: int) -> str:
    # Deviates from the original script here, not just reformatted: the
    # original's `reduce(state = -1, w IN weights | CASE WHEN w > state
    # THEN w ELSE 99999999999 END)` doesn't actually test "every weight
    # greater than the previous one" -- `state` only ever tracks the
    # single last-seen weight, not a running comparison against the whole
    # prefix, so it doesn't detect a later decrease after an earlier
    # increase. Replaced with the same `all(...)` pairwise check
    # `neo4j_run.py`'s Q3 (and the original Neo4j script) already use
    # correctly. Moot for correctness today either way, since this query
    # needs the weight/color dataset this repo doesn't have -- but a
    # future run against a real one should get the right answer, not a
    # silently preserved bug.
    return f"""
        MATCH p = (s:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(end)
        WITH p, [r IN relationships(p) | r.weight] AS weights
        WHERE all(i IN range(0, size(weights)-2) WHERE weights[i] < weights[i+1])
        RETURN count(p) AS cnt
    """


def _q3_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(end)
        RETURN count(p) AS cnt
    """


Q4_MAX_MIN_BOUND = int(os.environ.get("Q4_MAX_MIN_BOUND", 20))  # was `2592000/2`
                        # (2,592,000s = 30 days) -- a timestamp-scale constant
                        # compared against 'weight' (0-100 range in the toy
                        # dataset), always trivially true. Same bug independently
                        # found in kuzu_run.py, neo4j_run.py, and the old-prototype
                        # q4 scripts -- see experiments/q4_length_sweep/README.md.
                        # Fixed 2026-08-14 to match q4_aggregate.py's own bound. Made
                        # env-var configurable (matching kuzu_run.py's own pattern)
                        # since LDBC100's 'weight' is epoch_ms(creation_date),
                        # needing Q4_MAX_MIN_BOUND=1209600000 (two weeks, ms) instead.


def _q4_full(starter: int, min_len: int, max_len: int) -> str:
    # Sentinels must safely bound every real weight value, not just the toy
    # dataset's 0-100 range: LDBC100's weight is epoch_ms(creation_date),
    # ~1.3-1.7e12 -- the previous 99999999999/-99999999999 (~1e11) sentinels
    # were *smaller* than every real weight, so the min-reduce's `w < state`
    # branch never fired and min_weight stayed pinned at the sentinel,
    # inflating max_weight - min_weight and silently zeroing every result
    # (found via a real 0-vs-422 discrepancy against Neo4j/Kuzu/reference on
    # LDBC100/Q4). Matches neo4j_run.py's own 1e18/-1e18 sentinels, which
    # don't have this problem since 1e18 safely exceeds any real timestamp.
    return f"""
        MATCH p = (s:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(end)
        WITH relationships(p) AS edges
        WITH [r IN edges | r.weight] AS weights
        WITH reduce(state = 1e18, w IN weights | CASE WHEN w < state THEN w ELSE state END) AS min_weight,
             reduce(state = -1e18, w IN weights | CASE WHEN w > state THEN w ELSE state END) AS max_weight
        WHERE max_weight - min_weight <= {Q4_MAX_MIN_BOUND}
        RETURN count(*) AS cnt
    """


def _q4_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH p = (s:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->(end)
        RETURN count(p) AS cnt
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
    parser = argparse.ArgumentParser(description="Memgraph benchmark runner")
    bench_common.add_common_args(parser, query_choices=QUERY_REGISTRY)
    parser.add_argument("--uri", default=MEMGRAPH_URI)
    parser.add_argument("--user", default=MEMGRAPH_USER)
    parser.add_argument("--password", default=MEMGRAPH_PASSWORD)
    args = parser.parse_args()

    print(f"Connecting to Memgraph at {args.uri}...")
    auth = (args.user, args.password) if args.user else None
    driver = GraphDatabase.driver(args.uri, auth=auth) if auth else GraphDatabase.driver(args.uri)
    driver.verify_connectivity()

    try:
        set_query_timeout(driver, args.timeout)

        if args.fresh_db:
            clear_database(driver)
            create_indexes(driver)
            if args.query in GENERIC_SCHEMA_QUERIES:
                load_generic_schema_and_data(driver, args.nodes, args.edges)
            else:
                load_q1_schema_and_data(driver, args.nodes, args.edges)

        def execute_scalar(query: str):
            with driver.session() as session:
                result = session.run(query)
                return result.single()[0]

        memory_sampler = bench_common.make_memory_sampler(
            args.memory_source, container_name=args.container, process_name=args.process_name)

        results = bench_common.run_sweep(
            engine="memgraph", query_name=args.query, execute_scalar=execute_scalar,
            entry=QUERY_REGISTRY[args.query], starter=args.starter,
            min_len=args.min_len, max_len=args.max_len, warmup=args.warmup, runs=args.runs,
            memory_sampler=memory_sampler, csv_path=args.csv,
        )
        bench_common.print_summary(results)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
