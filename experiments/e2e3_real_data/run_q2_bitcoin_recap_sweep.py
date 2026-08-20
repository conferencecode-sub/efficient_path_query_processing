r"""Q2/Bitcoin optimized-compiler (Stage F) length sweep at start vertex 4515
(out-degree 49), requested directly rather than `q2_length_sweep`'s own
canonical median-degree vertex 3999 (out-degree 2) -- see that script's own
docstring for the methodology behind 3999. Not a replacement for it: this
is a separate, higher-degree data point, saved under its own filename so
it isn't confused with the canonical run.

Same "no regex" pathway (`set_trivial_label_column` + `trivial_relation()`)
and CSV convention as `run_q3_reddit_recap_sweep.py` / `run_q4_ldbc100_
recap_sweep.py`. Optimized variant only.
"""
from __future__ import annotations

import csv
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "q2_length_sweep"))

import duckdb  # noqa: E402

from q2_aggregate import q2_aggregate  # noqa: E402
from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions  # noqa: E402
from recap_compiler.transitions import trivial_relation  # noqa: E402

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets", "bitcoin")
EDGES = os.path.join(DATASET_DIR, "edges_with_colors.csv")
NODES = os.path.join(DATASET_DIR, "nodes.csv")
START_VERTEX = 4515
LENGTHS = (2, 3, 4, 5)
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q2_bitcoin_v4515.csv")


def main():
    rows = []
    for length in LENGTHS:
        conn = duckdb.connect()
        handle = load_graph(conn, EDGES, NODES)
        set_trivial_label_column(conn)
        aggregate = q2_aggregate()
        relation = trivial_relation()
        starts = select_start_vertices(handle, ids=[START_VERTEX])
        edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
        validate_selective_aggregate(aggregate, edge_columns=edge_columns)
        materialize_transitions(conn, relation)
        q = build_optimized_query(aggregate=aggregate, relation=relation,
                                   start_vertices=starts, length_bound=length)

        t0 = time.perf_counter()
        result = run_query(conn, q, result_shape="count")
        wall_s = time.perf_counter() - t0
        n_paths = result.rows[0][0]
        runtime_ms = result.telemetry.runtime_ms
        print(f"length={length}: {n_paths} paths, runtime={runtime_ms:.2f}ms "
              f"(wall {wall_s:.1f}s), intermediate={result.telemetry.intermediate_paths}",
              flush=True)
        rows.append({"engine": "recap-new-optimized", "length": length, "result": n_paths,
                      "runtime_ms": runtime_ms, "intermediate_paths": result.telemetry.intermediate_paths})
        conn.close()

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["engine", "length", "result", "runtime_ms", "intermediate_paths"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {CSV_PATH}")


if __name__ == "__main__":
    main()
