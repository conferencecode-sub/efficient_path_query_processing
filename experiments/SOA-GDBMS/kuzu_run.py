#!/usr/bin/env python3
"""Kùzu benchmark runner -- loads a graph, runs one of the registered
queries over a length sweep, and reports runtime + intermediate-path count
+ peak memory (best-effort) per length. See `bench_common.py` for the
shared harness and `run_kuzu.sh` for a filled-in example invocation.

**Query registry note (corrected 2026-08-13 -- the dataset claim below was
wrong).** `q1` (the paper's regex + selective-aggregate query) matches the
bundled `ReCAP/simple_dataset/LG.csv`/`LG_V.csv` schema (label-typed edges
with `amount`/`risk_score`/`location_region`/`timestamp_ms`). `q2`/`q3`
(two-color trail, monotonic trail) are carried over from `ReCAP/q2`/`q3`'s
own DuckDB scripts and use a *different*, simpler dataset shape (a single
generic edge type with `weight`/`color` columns) -- that dataset **is**
bundled here too (`ReCAP/simple_dataset/nodes.csv`/`edges.csv`, per
`ReCAP/q2/run_queries_py.sh`'s own "use the nodes/edges .csv files from
the dataset folder (not LG)" comment), so `q2`/`q3` are exercisable, not
just registry placeholders. `q4` (max-min) originally had no `ReCAP/q4/`
counterpart in this repo -- backfilled 2026-08-13 from the canonical
`~/ReCAP/q4/` (see `experiments/q4_length_sweep/README.md`),
which confirmed `Q4_MAX_MIN_BOUND` below independently of that backfill;
both now agree on the same bound.
"""
from __future__ import annotations

import argparse
import os
import shutil

import kuzu
import pandas as pd

import bench_common

DB_PATH = "./kuzu_db"

# Edge types for Q1's typed-relationship schema (one REL TABLE per label,
# matching Q1's own regex pattern -- Kùzu requires a real relationship
# *type* per label, not just a property, for a `-[x:transfer|sale|...]->`
# pattern to match anything).
Q1_EDGE_TYPES = ["transfer", "purchase", "sale", "phishing", "scam"]


# ============================================================================
#                          SCHEMA + DATA LOADING
# ============================================================================

def _fresh_database(db_path: str) -> tuple[kuzu.Database, kuzu.Connection]:
    if os.path.exists(db_path):
        print(f"Removing existing database at {db_path}...")
        shutil.rmtree(db_path) if os.path.isdir(db_path) else os.remove(db_path)
    print(f"Opening database at {db_path}...")
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)
    conn.set_query_timeout(7200000)
    return db, conn


def load_q1_schema_and_data(conn: kuzu.Connection, nodes_path: str, edges_path: str) -> None:
    """Q1's schema: `Node(id, name)` + one typed REL TABLE per edge label,
    each carrying the properties Q1's aggregate reads (`amount`,
    `risk_score`, `location_region`, `timestamp_ms`)."""
    conn.execute("""
       CREATE NODE TABLE IF NOT EXISTS Node(
            id INT, name STRING, PRIMARY KEY (id)
        )
    """)
    for etype in Q1_EDGE_TYPES:
        conn.execute(f"""
            CREATE REL TABLE {etype} (
                FROM Node TO Node,
                edge_id INT64, timestamp_ms INT64, hour_of_day INT64,
                amount DOUBLE, location_region STRING, risk_score DOUBLE
            )
        """)

    conn.execute(f"COPY Node FROM '{os.path.abspath(nodes_path)}' (header=true)")
    node_count = conn.execute("MATCH (n:Node) RETURN count(n)").get_next()[0]
    print(f"  loaded {node_count} nodes")

    edges_df = pd.read_csv(edges_path)
    if "edge_id" not in edges_df.columns:
        edges_df["edge_id"] = range(len(edges_df))
    if "from" in edges_df.columns:
        edges_df = edges_df.rename(columns={"from": "src", "to": "dst"})

    for etype in Q1_EDGE_TYPES:
        subset = edges_df[edges_df["label"] == etype][
            ["src", "dst", "edge_id", "timestamp_ms", "hour_of_day", "amount",
             "location_region", "risk_score"]]
        if subset.empty:
            print(f"  [{etype}] no edges, skipping")
            continue
        tmp_path = f"/tmp/kuzu_{etype}.csv"
        subset.to_csv(tmp_path, index=False)
        conn.execute(f"COPY {etype} FROM '{tmp_path}' (HEADER = TRUE)")
        print(f"  [{etype}] {len(subset):,} edges loaded")
        os.remove(tmp_path)


def load_generic_schema_and_data(conn: kuzu.Connection, nodes_path: str, edges_path: str) -> None:
    """Q2/Q3/Q4's schema: a single generic `Edge` REL TABLE carrying
    `weight`/`color`. Matches `ReCAP/simple_dataset/nodes.csv` +
    `edges.csv` (contrary to what an earlier version of this docstring
    claimed -- that dataset *is* bundled in this repo, see
    `ReCAP/q2/run_queries_py.sh`'s own "use the nodes/edges .csv files from
    the dataset folder (not LG)" comment)."""
    # id is INT64, not INT32: LDBC100's vertex ids run up to ~4.4e13, which
    # overflows INT32 (first hit running Q4 against ldbc100_engine_ready).
    # Strictly widens the column for every other dataset already using this
    # loader (Bitcoin/Reddit ids all fit in INT32 too) -- no behavior change.
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Node(id INT64, name STRING, PRIMARY KEY (id))")
    conn.execute("""
        CREATE REL TABLE IF NOT EXISTS Edge(
            FROM Node TO Node, edge_id INT64, weight DOUBLE, color STRING
        )
    """)
    nodes_df = pd.read_csv(nodes_path)
    tmp_nodes_path = "/tmp/kuzu_generic_nodes.csv"
    # `nodes.csv` has a third (`label`) column Kùzu's 2-column Node table
    # doesn't expect -- COPY is strict about column count, so this drops
    # it the same way the edges CSV below is already trimmed to just the
    # columns the target table declares.
    nodes_df[["id", "name"]].to_csv(tmp_nodes_path, index=False)
    conn.execute(f"COPY Node FROM '{tmp_nodes_path}' (header=true)")
    os.remove(tmp_nodes_path)

    edges_df = pd.read_csv(edges_path)
    if "edge_id" not in edges_df.columns:
        edges_df["edge_id"] = range(len(edges_df))
    tmp_path = "/tmp/kuzu_generic_edges.csv"
    edges_df[["src", "dst", "edge_id", "weight", "color"]].to_csv(tmp_path, index=False)
    conn.execute(f"COPY Edge FROM '{tmp_path}' (HEADER = TRUE)")
    os.remove(tmp_path)


# ============================================================================
#                          QUERY REGISTRY
# ============================================================================

def _q1_full(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[normal:transfer|sale|purchase *1..{max_len}]->
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
    # min_len is accepted for CLI/registry consistency with the other
    # engines/queries but, matching the original script, is not enforced
    # here (no `size(...) >= min_len` clause) -- only max_len bounds it.


def _q1_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    """Same regex pattern and length bound, no property/trail filter at
    all -- the "no early filtering" comparison point (E3/E4-style
    intermediate-cardinality reporting)."""
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[normal:transfer|sale|purchase *1..{max_len}]->
            (mid:Node)
            -[fraud: phishing|scam *1..{max_len}]->
            (e:Node)
        WHERE size(rels(normal)) + size(rels(fraud)) <= {max_len}
        RETURN count(*)
    """


def _q2_full(starter: int, min_len: int, max_len: int) -> str:
    # Deviates from the original script here, not just reformatted: the
    # original matched `(start:Node {label: {starter_node}})`, but `Node`
    # has no `label` property (only `id`/`name`) -- almost certainly a
    # copy-paste artifact from a differently-shaped dataset, since it would
    # never match any node and this query would silently always return 0.
    # Fixed to match on `id`, like every other query here does.
    # `is_trail(path)` added 2026-08-13 -- the original never checked it,
    # a real correctness gap (every other engine, old-prototype included,
    # requires the path be a trail). Doesn't avoid the segfault documented
    # in experiments/q2_length_sweep/README.md (confirmed: it crashes with
    # or without this clause, so it's a genuine Kùzu 0.11.2 engine bug in
    # the color-list processing below at ~12k+ rows, not a workaroundable
    # query issue) -- kept anyway since it's the correct query regardless.
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->()
        WHERE is_trail(path)
        WITH path as path, PROPERTIES(RELS(path), 'color') AS cs
        WITH cs as cs, RANGE(1, SIZE(cs)-1) AS idxs
        WHERE ANY(i IN idxs WHERE cs[i] = cs[i+1])
        RETURN COUNT(*)
    """


def _q2_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[e:Edge*{min_len}..{max_len}]->()
        RETURN COUNT(*)
    """


def _q3_full(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[*{min_len}..{max_len}]->(t)
        WITH path as path,
        list_reduce(
                properties(rels(path), 'weight'),
                        (acc, w) ->
                            CASE WHEN w > acc THEN w ELSE NULL END
                ) AS result
        WHERE result IS NOT NULL AND is_trail(path)
        RETURN COUNT(*)
    """


def _q3_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[*{min_len}..{max_len}]->(t)
        RETURN COUNT(*)
    """


Q4_MAX_MIN_BOUND = int(os.environ.get("Q4_MAX_MIN_BOUND", 20))  # matches q1's own
    # bounded_range(upper_bound=20) usage on the same 0-100-range 'weight' column q3 already
    # uses -- BUT for E2/E3's real LDBC100 rerun (2026-08-14), 'weight' is aliased to
    # epoch_ms(creationDate) (no real 0-100 weight column exists there), so the bound needs to
    # be in milliseconds too: Q4_MAX_MIN_BOUND=1209600000 (two weeks), matching the real Q4
    # semantics (tab:queries: "earliest and latest edge timestamp ... does not exceed two
    # weeks") already used for this dataset in q4_length_sweep/run_new_compiler.py.


def _q4_full(starter: int, min_len: int, max_len: int) -> str:
    # `WHERE max_sf - min_sf <= 2592000/2` (2,592,000s = 30 days) was here
    # originally -- a timestamp-scale constant against a 0-100-range
    # `weight` column, so it always trivially held (never actually
    # constrained anything, silently degrading this to an is_trail-only
    # query). Fixed 2026-08-13 to Q4_MAX_MIN_BOUND, on the same scale as
    # the data it's actually checking.
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[*{min_len}..{max_len}]->(t)
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
        WHERE max_sf - min_sf <= {Q4_MAX_MIN_BOUND} AND is_trail(path)
        RETURN COUNT(*)
    """


def _q4_candidate_only(starter: int, min_len: int, max_len: int) -> str:
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[*{min_len}..{max_len}]->(t)
        RETURN COUNT(*)
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
    parser = argparse.ArgumentParser(description="Kùzu benchmark runner")
    bench_common.add_common_args(parser, query_choices=QUERY_REGISTRY)
    args = parser.parse_args()

    db_path = DB_PATH
    if args.fresh_db:
        _, conn = _fresh_database(db_path)
        if args.query in GENERIC_SCHEMA_QUERIES:
            load_generic_schema_and_data(conn, args.nodes, args.edges)
        else:
            load_q1_schema_and_data(conn, args.nodes, args.edges)
    else:
        db = kuzu.Database(db_path)
        conn = kuzu.Connection(db)
        conn.set_query_timeout(args.timeout * 1000)

    def execute_scalar(query: str):
        result = conn.execute(query)
        return result.get_next()[0]

    memory_sampler = bench_common.make_memory_sampler(
        args.memory_source, container_name=args.container, process_name=args.process_name)

    results = bench_common.run_sweep(
        engine="kuzu", query_name=args.query, execute_scalar=execute_scalar,
        entry=QUERY_REGISTRY[args.query], starter=args.starter,
        min_len=args.min_len, max_len=args.max_len, warmup=args.warmup, runs=args.runs,
        memory_sampler=memory_sampler, csv_path=args.csv,
    )
    bench_common.print_summary(results)


if __name__ == "__main__":
    main()
