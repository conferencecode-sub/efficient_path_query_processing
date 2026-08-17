"""FinBench TCR1/TCR5/TCR8 competitor run: Neo4j.

Schema: one typed relationship per FinBench edge label (transfer, withdraw,
deposit, own, signedInBy), each carrying edge_id/timestamp_ms/amount
(amount unused/0 where the reference query never reads it -- own,
signedInBy, deposit -- but loaded uniformly for simplicity). Queries are
translated directly from `reference_baseline.py`'s exact recursive-CTE
semantics, not re-derived from prose:
  - trail semantics (no repeated edge) is Cypher's own default for a
    variable-length relationship pattern -- no explicit check needed, and
    edge_id is globally unique across labels anyway (assigned by a single
    ROW_NUMBER() OVER () in common.py's UNION ALL), so no two different
    relationship-type segments of a path could collide even in principle.
  - ascending-timestamp / growth-ratio checks via ALL(i IN range(...) ...)
    over the relevant chain segment only (matching each reference's own
    scoping -- e.g. TCR5's ascending check and time window apply only to
    the transfer chain, not the leading `own` edge).
"""
import csv
import os
import time

from neo4j import GraphDatabase

_HERE = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(_HERE, "..", "datasets", "finbench_sf0.1_engine_ready")
RESULTS_DIR = os.path.join(_HERE, "results")

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

START_TIME = 1_500_000_000_000
END_TIME = 1_700_000_000_000
THRESHOLD = 1.0
LENGTHS = (2, 3, 4, 5, 6, 7, 8)
TIMEOUT_S = 7200

EDGE_TYPES = ["transfer", "withdraw", "deposit", "own", "signedInBy"]

REFERENCE = {
    "tcr1": {2: 30, 3: 65, 4: 137, 5: 873, 6: 1107, 7: 1107, 8: 1107},
    "tcr5": {2: 549, 3: 1493, 4: 3677, 5: 5035, 6: 7532, 7: 9197, 8: 10183},
    "tcr8": {2: 775, 3: 6497, 4: 20027, 5: 46311, 6: 84847, 7: 138900, 8: 200855},
}

TCR1_START = 241505530017743994
TCR5_START = 911
TCR8_START = 292171025825661041


def clear_database(driver):
    with driver.session() as session:
        session.run("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS")


def load_data(driver):
    import pandas as pd

    nodes_df = pd.read_csv(os.path.join(DATASET_DIR, "nodes.csv"))
    edges_df = pd.read_csv(os.path.join(DATASET_DIR, "edges.csv"))

    with driver.session() as session:
        session.run("CREATE INDEX node_id IF NOT EXISTS FOR (n:Node) ON (n.id)")
        nodes_data = [{"id": int(r["id"])} for _, r in nodes_df.iterrows()]
        for i in range(0, len(nodes_data), 50_000):
            session.run("UNWIND $nodes AS n MERGE (:Node {id: n.id})", nodes=nodes_data[i:i + 50_000])
        print(f"  loaded {len(nodes_data):,} nodes")

        for etype in EDGE_TYPES:
            subset = edges_df[edges_df["label"] == etype]
            if subset.empty:
                continue
            edges_data = [{
                "src": int(r["src"]), "dst": int(r["dst"]), "edge_id": int(r["edge_id"]),
                "timestamp_ms": int(r["timestamp_ms"]),
                "amount": float(r["amount"]) if r["amount"] == r["amount"] else 0.0,
            } for _, r in subset.iterrows()]
            for i in range(0, len(edges_data), 50_000):
                batch = edges_data[i:i + 50_000]
                session.run(f"""
                    UNWIND $edges AS row
                    MATCH (s:Node {{id: row.src}}), (d:Node {{id: row.dst}})
                    CREATE (s)-[:{etype} {{
                        edge_id: row.edge_id, timestamp_ms: row.timestamp_ms, amount: row.amount
                    }}]->(d)
                """, edges=batch)
            print(f"  [{etype}] {len(edges_data):,} edges loaded")


def q_tcr1(starter, max_len):
    return f"""
        MATCH p = (start:Node {{id: {starter}}})-[t:transfer*1..{max_len - 1}]->(mid:Node)-[s:signedInBy]->(medium:Node)
        WHERE ALL(i IN range(0, size(t)-2) WHERE t[i].timestamp_ms < t[i+1].timestamp_ms)
          AND ALL(r IN t + [s] WHERE r.timestamp_ms > {START_TIME} AND r.timestamp_ms < {END_TIME})
        RETURN count(*) AS cnt
    """


def q_tcr5(starter, max_len):
    return f"""
        MATCH p = (start:Node {{id: {starter}}})-[o:own]->(acc:Node)-[t:transfer*1..{max_len - 1}]->(dst:Node)
        WHERE ALL(i IN range(0, size(t)-2) WHERE t[i].timestamp_ms < t[i+1].timestamp_ms)
          AND ALL(r IN t WHERE r.timestamp_ms > {START_TIME} AND r.timestamp_ms < {END_TIME})
        RETURN count(*) AS cnt
    """


def q_tcr8(starter, max_len):
    return f"""
        MATCH p = (start:Node {{id: {starter}}})-[d:deposit]->(acc:Node)-[c:transfer|withdraw*1..{max_len - 1}]->(dst:Node)
        WHERE ALL(i IN range(0, size(c)-2) WHERE c[i+1].amount > c[i].amount * {THRESHOLD})
          AND ALL(r IN c + [d] WHERE r.timestamp_ms > {START_TIME} AND r.timestamp_ms < {END_TIME})
        RETURN count(*) AS cnt
    """


QUERIES = {"tcr1": (q_tcr1, TCR1_START), "tcr5": (q_tcr5, TCR5_START), "tcr8": (q_tcr8, TCR8_START)}


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    print(f"Connecting to Neo4j at {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    try:
        print("Clearing database...")
        clear_database(driver)
        print("Loading FinBench SF0.1 data...")
        load_data(driver)

        for qname, (qfunc, starter) in QUERIES.items():
            rows = []
            for length in LENGTHS:
                query = qfunc(starter, length)
                t0 = time.perf_counter()
                try:
                    with driver.session() as session:
                        result = session.run(query, timeout=TIMEOUT_S)
                        cnt = result.single()[0]
                    wall_ms = (time.perf_counter() - t0) * 1000
                    expected = REFERENCE[qname][length]
                    match = (cnt == expected)
                    print(f"[neo4j] {qname} len={length}: result={cnt} expected={expected} "
                          f"match={match} time={wall_ms:.1f}ms")
                    rows.append({"length": length, "result": cnt, "reference_result": expected,
                                 "match": match, "runtime_ms": wall_ms, "error": ""})
                    if not match:
                        print(f"  MISMATCH -- stopping {qname} sweep for investigation")
                        break
                except Exception as exc:
                    print(f"[neo4j] {qname} len={length}: ERROR {exc}")
                    rows.append({"length": length, "result": "", "reference_result": REFERENCE[qname][length],
                                 "match": False, "runtime_ms": "", "error": str(exc)})
                    break
            csv_path = os.path.join(RESULTS_DIR, f"neo4j_{qname}.csv")
            with open(csv_path, "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["length", "result", "reference_result",
                                                          "match", "runtime_ms", "error"])
                writer.writeheader()
                writer.writerows(rows)
            print(f"wrote {len(rows)} rows to {csv_path}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
