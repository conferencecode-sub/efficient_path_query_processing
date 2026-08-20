"""DuckDB-without-ReCAP baseline for Q4 on the real LDBC100 dataset, at the
paper's confirmed start vertex (24189256063073, out-degree 18/median) --
no prior real-data run exists (the only "duckdb-baseline" row on disk for
Q4, results/old_prototype_q4.csv, uses the old toy dataset at start=9, AND
its own hand-written query never actually checks the max-min bound or
trail-disjointness at all -- confirmed by reading
ReCAP/q4/duckdb_max_min_trail_inline.py directly; not reused here).

Uses `q4_default_aggregate` (walk semantics, no early pruning, property
checked once in `is_viable_d_final`) through the same Stage F compiler
pipeline as `run_new_compiler.py`, which is what Section 4.2's default
construction reduces to -- functionally identical to a hand-written
non-ReCAP recursive query, and FR-22-verified to match `q4_aggregate`'s
early-filtered result set at every length tested below before trusting
the timing numbers.
"""
from __future__ import annotations

import csv
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.dirname(__file__))

import duckdb  # noqa: E402

from q4_aggregate import q4_aggregate, q4_default_aggregate  # noqa: E402
from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions, register_aggregate_macros  # noqa: E402
from recap_compiler.transitions import trivial_relation  # noqa: E402
from run_new_compiler import EDGES_RAW, START_VERTEX, TWO_WEEKS_MS, MIN_LENGTH, _load_ldbc_edges, _with_min_length  # noqa: E402

CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "duckdb_baseline_q4_real.csv")
LENGTHS = (2, 3, 4, 5)  # capped here, not just by BUDGET_S: length=6 was
                          # attempted separately and killed manually after
                          # ballooning to ~715GB RSS (94.7% of this shared
                          # machine's RAM, swap exhausted) well past 60
                          # minutes with no sign of finishing -- walk
                          # semantics with zero pruning at ~55-60x/hop
                          # branching is not just slow past ell=5, it's a
                          # real risk to other users on this machine.
BUDGET_S = 200  # stop once a single length exceeds this -- matches this
                 # project's own "document and stop" convention elsewhere


def _setup_default(length):
    aggregate = q4_default_aggregate(bound=TWO_WEEKS_MS)
    relation = trivial_relation()
    conn = duckdb.connect()
    handle = _load_ldbc_edges(conn)
    set_trivial_label_column(conn)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    query = _with_min_length(
        build_optimized_query(aggregate=aggregate, relation=relation,
                               start_vertices=starts, length_bound=length), MIN_LENGTH)
    return conn, query


def _reference_result(length):
    """The already-trusted early-filtered result, for a same-length FR-22
    style cross-check before trusting the default-construction number."""
    from run_new_compiler import _setup
    conn, _std, opt_q = _setup(length)
    n = run_query(conn, opt_q, result_shape="count").rows[0][0]
    conn.close()
    return n


def main():
    rows = []
    for length in LENGTHS:
        conn, query = _setup_default(length)
        start = time.time()
        try:
            # Materialize the recursion once, then derive both metrics from
            # it, rather than re-running the (expensive) recursive CTE twice.
            conn.execute(f"CREATE TEMP TABLE paths_materialized AS WITH RECURSIVE {query.cte} "
                         f"SELECT * FROM paths WHERE path_length >= {MIN_LENGTH}")
            runtime_ms = (time.time() - start) * 1000
            intermediate = conn.execute("SELECT count(*) FROM paths_materialized").fetchone()[0]
            # Stage F flattens D's keys into plain columns of `paths` (no
            # `D.` prefix) -- match `q4_default_aggregate`'s own dictionary
            # keys directly.
            count = conn.execute(
                "SELECT count(*) FROM paths_materialized WHERE "
                "len(edge_ids) = len(list_distinct(edge_ids)) "
                f"AND max_weight - min_weight <= {TWO_WEEKS_MS}").fetchone()[0]
        except Exception as exc:  # noqa: BLE001
            print(f"length={length}: FAILED ({exc})")
            conn.close()
            break
        conn.close()

        expected = _reference_result(length)
        match = (count == expected)
        print(f"length={length}: result={count} (expected {expected}, match={match}) "
              f"intermediate={intermediate} runtime={runtime_ms:.2f}ms")
        rows.append({"engine": "duckdb-baseline", "length": length, "result": count,
                     "intermediate_paths": intermediate,
                     "runtime_ms": runtime_ms, "match_early_filtered": match})

        if runtime_ms / 1000 > BUDGET_S:
            print(f"length={length} exceeded {BUDGET_S}s budget -- stopping")
            break

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["engine", "length", "result", "intermediate_paths",
                                                 "runtime_ms", "match_early_filtered"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
