r"""Q4/LDBC100 optimized-compiler (Stage F) length sweep, same convention as
`run_q3_reddit_recap_sweep.py` -- checks whether `fig:intermediate_total_grid`'s
Q4 \ourabstraction series (68, 254, 1160, 4792, 16674, 49384, 128854, 299428,
627472) matches a real run against vertex 24189256063073 (out-degree 18,
median -- same choice as `q4_length_sweep/run_new_compiler.py` and
`reference_q4_ldbc100.py`).
"""
from __future__ import annotations

import csv
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "q4_length_sweep"))

import duckdb  # noqa: E402

from q4_aggregate import q4_aggregate  # noqa: E402
from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions  # noqa: E402
from recap_compiler.transitions import trivial_relation  # noqa: E402

EDGES_RAW = os.path.join(os.path.dirname(__file__), "..", "datasets", "ldbc100", "person_knows_person_0_0.csv")
START_VERTEX = 24189256063073
TWO_WEEKS_MS = 1_209_600_000
LENGTHS = (2, 3, 4, 5, 6, 7, 8)
MIN_LENGTH = 2
PER_POINT_BUDGET_S = 300
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q4_ldbc100.csv")


def _load_ldbc_edges(conn):
    conn.execute(
        "CREATE TABLE raw_edges AS SELECT * FROM read_csv(?, delim='|', header=false, skip=1, "
        "columns={'src': 'BIGINT', 'dst': 'BIGINT', 'creation_date': 'VARCHAR'})", [EDGES_RAW])
    conn.execute(
        "CREATE TABLE ldbc_edges AS SELECT src, dst, "
        "epoch_ms(CAST(creation_date AS TIMESTAMPTZ)) AS weight FROM raw_edges")
    return load_graph(conn, "ldbc_edges")


def main():
    rows = []
    for length in LENGTHS:
        conn = duckdb.connect()
        handle = _load_ldbc_edges(conn)
        set_trivial_label_column(conn)
        aggregate = q4_aggregate(bound=TWO_WEEKS_MS)
        relation = trivial_relation()
        starts = select_start_vertices(handle, ids=[START_VERTEX])
        edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
        validate_selective_aggregate(aggregate, edge_columns=edge_columns)
        materialize_transitions(conn, relation)
        q = build_optimized_query(aggregate=aggregate, relation=relation,
                                   start_vertices=starts, length_bound=length)
        q = SimpleNamespace(sql=f"SELECT * FROM ({q.sql}) t WHERE path_length >= {MIN_LENGTH}", cte=q.cte)

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
        if wall_s > PER_POINT_BUDGET_S:
            print(f"stopping: length={length} took {wall_s:.1f}s > {PER_POINT_BUDGET_S}s budget")
            break

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["engine", "length", "result", "runtime_ms", "intermediate_paths"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {CSV_PATH}")


if __name__ == "__main__":
    main()
