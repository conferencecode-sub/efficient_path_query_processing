r"""Extends `run_q3_reddit_recap.py` into a proper length sweep with a saved
CSV, matching the `q{1,2,4}_length_sweep/run_new_compiler.py` convention --
needed because `fig:performance_grid`'s own Q3/Reddit panel turned out to
have no compiler-run numbers saved anywhere in this repo (its plotted
`\ourabstraction` series was untraceable legacy data), per the 2026-08-18
audit. Only measures `\CompilerOpt` (Stage F) -- that's the one series this
figure actually needs; no Standard/UDF variants here.

Per this repo's "document and stop, don't force it" policy elsewhere: stops
the sweep once one length exceeds a generous per-point budget, rather than
guessing how far Reddit's own small-world cliff (already noted in
`e2e3_real_data/README.md` -- ~10.9M intermediate rows by ell=4) will let it
scale.
"""
from __future__ import annotations

import csv
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "q3_length_sweep"))

import duckdb  # noqa: E402

from q3_aggregate import q3_aggregate  # noqa: E402
from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions  # noqa: E402
from recap_compiler.transitions import trivial_relation  # noqa: E402

EDGES = os.path.join(os.path.dirname(__file__), "..", "datasets", "reddit", "edges.csv")
NODES = os.path.join(os.path.dirname(__file__), "..", "datasets", "reddit", "nodes.csv")
START_VERTEX = 31470
LENGTHS = (2, 3, 4, 5, 6, 7, 8, 9, 10)
MIN_LENGTH = 2
PER_POINT_BUDGET_S = 300
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q3_reddit.csv")


def main():
    rows = []
    for length in LENGTHS:
        conn = duckdb.connect()
        handle = load_graph(conn, EDGES, NODES)
        set_trivial_label_column(conn)
        aggregate = q3_aggregate()
        relation = trivial_relation()
        starts = select_start_vertices(handle, ids=[START_VERTEX])
        edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
        validate_selective_aggregate(aggregate, edge_columns=edge_columns, transitions=relation)
        materialize_transitions(conn, relation)
        q = build_optimized_query(aggregate=aggregate, relation=relation,
                                   start_vertices=starts, length_bound=length)
        q = type(q)(sql=f"SELECT * FROM ({q.sql}) t WHERE path_length >= {MIN_LENGTH}", cte=q.cte)

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
