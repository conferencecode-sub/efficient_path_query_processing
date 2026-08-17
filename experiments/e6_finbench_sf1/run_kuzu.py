"""FinBench SF1 TCR1/TCR5/TCR8 competitor run: Kùzu. Same query semantics
and 1-based-indexing workarounds as `experiments/e6_finbench/run_kuzu.py`
(SF0.1) -- see that file's docstring for the full reasoning, including the
combined-`ALL(...)`-predicate bug workaround (precompute each as a named
boolean in an intermediate `WITH`, don't combine two `ALL(...)` calls with
`AND` directly in one `WHERE`). Reference counts are read from
`results/tcr{1,5,8}.csv` (see `run_neo4j.py`'s docstring for why).
"""
import csv
import os
import shutil
import time

import kuzu

_HERE = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(_HERE, "..", "datasets", "finbench_sf1_engine_ready")
RESULTS_DIR = os.path.join(_HERE, "results")
DB_PATH = os.path.join(_HERE, "kuzu_finbench_db")

START_TIME = 1_500_000_000_000
END_TIME = 1_700_000_000_000
THRESHOLD = 1.0
LENGTHS = (2, 3, 4, 5, 6, 7, 8)
TIMEOUT_MS = 7200_000

EDGE_TYPES = ["transfer", "withdraw", "deposit", "own", "signedInBy"]

TCR1_START = 300615275126988483
TCR5_START = 13194139540693
TCR8_START = 302867074940670634


def _load_reference():
    reference = {}
    for qname in ("tcr1", "tcr5", "tcr8"):
        with open(os.path.join(RESULTS_DIR, f"{qname}.csv")) as fh:
            reference[qname] = {int(r["length"]): int(r["result"]) for r in csv.DictReader(fh)}
    return reference


REFERENCE = _load_reference()


def fresh_database():
    if os.path.isdir(DB_PATH):
        shutil.rmtree(DB_PATH)
    elif os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    db = kuzu.Database(DB_PATH)
    conn = kuzu.Connection(db)
    conn.set_query_timeout(TIMEOUT_MS)
    return db, conn


def load_data(conn):
    import pandas as pd

    conn.execute("CREATE NODE TABLE IF NOT EXISTS Node(id INT64, PRIMARY KEY (id))")
    for etype in EDGE_TYPES:
        conn.execute(f"""
            CREATE REL TABLE {etype} (
                FROM Node TO Node, edge_id INT64, timestamp_ms INT64, amount DOUBLE
            )
        """)

    nodes_df = pd.read_csv(os.path.join(DATASET_DIR, "nodes.csv"))
    tmp_nodes = "/tmp/kuzu_fb_sf1_nodes.csv"
    nodes_df[["id"]].to_csv(tmp_nodes, index=False)
    conn.execute(f"COPY Node FROM '{tmp_nodes}' (header=true)")
    os.remove(tmp_nodes)
    node_count = conn.execute("MATCH (n:Node) RETURN count(n)").get_next()[0]
    print(f"  loaded {node_count:,} nodes")

    edges_df = pd.read_csv(os.path.join(DATASET_DIR, "edges.csv"))
    for etype in EDGE_TYPES:
        subset = edges_df[edges_df["label"] == etype][["src", "dst", "edge_id", "timestamp_ms", "amount"]].copy()
        if subset.empty:
            continue
        subset["amount"] = subset["amount"].fillna(0.0)
        tmp_path = f"/tmp/kuzu_fb_sf1_{etype}.csv"
        subset.to_csv(tmp_path, index=False)
        conn.execute(f"COPY {etype} FROM '{tmp_path}' (HEADER = TRUE)")
        print(f"  [{etype}] {len(subset):,} edges loaded")
        os.remove(tmp_path)


def q_tcr1(starter, max_len):
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[t:transfer*1..{max_len - 1}]->(mid:Node)-[s:signedInBy]->(medium:Node)
        WITH properties(rels(t), 'timestamp_ms') AS tts, s AS s
        WITH tts AS tts,
             ALL(i IN range(1, size(tts)-1) WHERE tts[i] < tts[i+1]) AS ordered,
             ALL(r IN tts + [s.timestamp_ms] WHERE r > {START_TIME} AND r < {END_TIME}) AS windowed
        WHERE ordered AND windowed
        RETURN COUNT(*)
    """


def q_tcr5(starter, max_len):
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[o:own]->(midacc:Node)-[t:transfer*1..{max_len - 1}]->(dst:Node)
        WITH properties(rels(t), 'timestamp_ms') AS tts
        WITH tts AS tts,
             ALL(i IN range(1, size(tts)-1) WHERE tts[i] < tts[i+1]) AS ordered,
             ALL(r IN tts WHERE r > {START_TIME} AND r < {END_TIME}) AS windowed
        WHERE ordered AND windowed
        RETURN COUNT(*)
    """


def q_tcr8(starter, max_len):
    return f"""
        MATCH path = (start:Node {{id: {starter}}})-[d:deposit]->(midacc:Node)-[c:transfer|withdraw*1..{max_len - 1}]->(dst:Node)
        WITH properties(rels(c), 'amount') AS camts, properties(rels(c), 'timestamp_ms') AS ctts, d AS d
        WITH camts AS camts, ctts AS ctts, d AS d,
             ALL(i IN range(1, size(camts)-1) WHERE camts[i+1] > camts[i] * {THRESHOLD}) AS grown,
             ALL(r IN ctts + [d.timestamp_ms] WHERE r > {START_TIME} AND r < {END_TIME}) AS windowed
        WHERE grown AND windowed
        RETURN COUNT(*)
    """


QUERIES = {"tcr1": (q_tcr1, TCR1_START), "tcr5": (q_tcr5, TCR5_START), "tcr8": (q_tcr8, TCR8_START)}


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    print(f"Opening Kùzu database at {DB_PATH}...")
    db, conn = fresh_database()
    print("Loading FinBench SF1 data...")
    load_data(conn)

    for qname, (qfunc, starter) in QUERIES.items():
        rows = []
        for length in LENGTHS:
            query = qfunc(starter, length)
            t0 = time.perf_counter()
            try:
                result = conn.execute(query)
                cnt = result.get_next()[0]
                wall_ms = (time.perf_counter() - t0) * 1000
                expected = REFERENCE[qname][length]
                match = (cnt == expected)
                print(f"[kuzu] {qname} len={length}: result={cnt} expected={expected} "
                      f"match={match} time={wall_ms:.1f}ms", flush=True)
                rows.append({"length": length, "result": cnt, "reference_result": expected,
                             "match": match, "runtime_ms": wall_ms, "error": ""})
                if not match:
                    print(f"  MISMATCH -- stopping {qname} sweep for investigation")
                    break
            except Exception as exc:
                print(f"[kuzu] {qname} len={length}: ERROR {exc}", flush=True)
                rows.append({"length": length, "result": "", "reference_result": REFERENCE[qname][length],
                             "match": False, "runtime_ms": "", "error": str(exc)})
                break
        csv_path = os.path.join(RESULTS_DIR, f"kuzu_{qname}.csv")
        with open(csv_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["length", "result", "reference_result",
                                                      "match", "runtime_ms", "error"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"wrote {len(rows)} rows to {csv_path}")


if __name__ == "__main__":
    main()
