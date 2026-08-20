"""Runs exactly one Memgraph query for one length, in its own process --
so `run_memgraph_sum_weights.py` can enforce a hard 1800s (30 minute)
wall-clock timeout via `subprocess.run(..., timeout=...)` and kill this
process outright if it's exceeded, dropping the bolt connection (which
in turn lets Memgraph abort the in-flight query server-side) rather than
leaving a client blocked indefinitely inside a single long-lived process.

Cypher's own variable-length relationship pattern (`[r:LINK*MIN..L]`) is
already edge-simple (trail semantics: no relationship may repeat within
one matched path) by the openCypher path definition itself -- same reason
none of `experiments/e6_finbench_sf10/run_memgraph.py`'s own TCR queries
add an explicit "distinct edge" check. The running-sum bound is checked
via `reduce(...) <= bound` over the *whole* completed relationship list,
not per-hop -- `sum_weight_aggregate.py`'s docstring proves this is exactly
equivalent to ReCAP's own per-hop `is_viable_d` check here, since every
edge weight is strictly positive (the running sum is non-decreasing, so
the final total is always the maximum prefix sum).
"""
from __future__ import annotations

import sys
import time

from neo4j import GraphDatabase

START_VERTEX = 6597072984304  # pass 3: max out-degree (2084) vertex -- see sum_weight_aggregate.py
SUM_WEIGHT_BOUND = 6.0  # pass 3: tightest bound in the length-6-feasible range -- see sum_weight_aggregate.py
MIN_LENGTH = 2
MEMGRAPH_URI = "bolt://localhost:7688"


def main() -> None:
    length = int(sys.argv[1])
    query = f"""
        MATCH p = (start:Node {{id: {START_VERTEX}}})-[r:LINK*{MIN_LENGTH}..{length}]->(end:Node)
        WHERE reduce(s = 0.0, x IN r | s + x.weight) <= {SUM_WEIGHT_BOUND}
        RETURN count(*) AS cnt
    """
    driver = GraphDatabase.driver(MEMGRAPH_URI)
    try:
        t0 = time.perf_counter()
        with driver.session() as session:
            result = session.run(query)
            cnt = result.single()["cnt"]
        wall_ms = (time.perf_counter() - t0) * 1000
        print(f"{cnt},{wall_ms}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
