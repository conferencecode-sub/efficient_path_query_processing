"""Runs exactly one Memgraph query (product-of-weights variant) for one
length, in its own process -- same isolation/timeout rationale as
`query_memgraph_once.py` (sum variant). The bound is checked via
`reduce(...) <= bound` over the whole completed relationship list, base
1.0 and multiplying -- exactly equivalent to ReCAP's own per-hop
`is_viable_d` check since every edge weight here is >= 1.0 (see
`product_weight_aggregate.py`'s docstring for the full argument).
"""
from __future__ import annotations

import sys
import time

from neo4j import GraphDatabase

START_VERTEX = 6597072984304  # this dataset's own max out-degree (2084) vertex
PRODUCT_WEIGHT_BOUND = 1.05
MIN_LENGTH = 2
MEMGRAPH_URI = "bolt://localhost:7688"


def main() -> None:
    length = int(sys.argv[1])
    query = f"""
        MATCH p = (start:Node {{id: {START_VERTEX}}})-[r:LINK*{MIN_LENGTH}..{length}]->(end:Node)
        WHERE reduce(s = 1.0, x IN r | s * x.weight) <= {PRODUCT_WEIGHT_BOUND}
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
