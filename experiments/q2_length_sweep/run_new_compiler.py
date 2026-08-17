"""Sweeps Q2 (two-color trail, see q2_aggregate.py) over the new compiler's
Standard (Stage E) and Optimized (Stage F) pipelines, against the real
Bitcoin dataset now symlinked at `experiments/datasets/bitcoin/`
(5,882 nodes / 35,593 edges, matching tab:realdata exactly). Uses
`edges_with_colors.csv` rather than the plain `edges.csv`, since Q2's
aggregate needs a `color` column that only the colors variant has.

Q2 has no label regex at all, so this uses `ingestion.set_trivial_label_
column` + `transitions.trivial_relation()` (a single self-looping state
matching every edge) instead of `compile_regex_to_nfa`.

`START_VERTEX = 3999`: the paper's own methodology (Section "Experiments"
in figures.tex) picks **medium**-degree start vertices for Q2 (and Q4),
**low** for Q3, and **high** only for Q1 -- so this isn't the max-out-degree
vertex. Computed directly: out-degree distribution over this dataset's
4,814 vertices with outgoing edges has q25=1, median=2, q75=5; vertex 3999
has out-degree 2 (median), replacing an earlier version of this script that
mistakenly used the highest-out-degree vertex (763) for every query.

No explicit min_length filter is needed: `is_viable_d_final` checks
`D.constraint_done`, which can only become TRUE starting at the 2nd edge.

**Memory: per-variant subprocess isolation**, same rework and same reason
as `q1_length_sweep/run_new_compiler.py` (see that docstring) -- running
both variants on one shared connection made their memory numbers
identical/cumulative rather than each variant's own. FR-22 is checked once
per length on its own throwaway connection in the parent process.

Q2 has no early filtering on the two-color constraint (only the trail
check is in `is_viable_d`), so intermediate cardinality grows combinatorially
with `length_bound` regardless of Standard vs Optimized -- expect this to
get slow at higher lengths for a reason that has nothing to do with the
inlining optimization itself (see the paper's own Q2-Bitcoin panel in
fig:recap_performance_grid, where even Standard/Optimized both stop around
length_bound 4-5).
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import threading
import time

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "udf_variant"))

import q2_udf  # noqa: E402
from q2_aggregate import q2_aggregate  # noqa: E402
from register_udfs import register_aggregate_udfs  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import (  # noqa: E402
    build_standard_query, materialize_transitions, register_aggregate_macros,
)
from recap_compiler.transitions import trivial_relation  # noqa: E402

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets", "bitcoin")
EDGES = os.path.join(DATASET_DIR, "edges_with_colors.csv")
NODES = os.path.join(DATASET_DIR, "nodes.csv")
START_VERTEX = 3999  # median out-degree (2) in the real Bitcoin dataset -- see docstring
LENGTHS = (2, 3, 4, 5)  # length_bound=6 blew up to an intractable path count (>150s, killed) --
                         # growth from length_bound 4->5 was already 52x (52,508 -> 2,743,095),
                         # consistent with paths passing through a few high-out-degree hub vertices
                         # even though the start vertex itself is median-degree.
VARIANTS = ("standard", "optimized")
MACRO_MODES = ("sql", "python-udf")
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q2.csv")
CSV_PATH_UDF = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q2_udf.csv")

CSV_FIELDNAMES = ["engine", "length", "result", "runtime_ms", "intermediate_count_ms",
                   "peak_buffer_memory_mb", "peak_rss_mb"]


def _setup(length, macro_mode="sql"):
    aggregate = q2_aggregate()
    relation = trivial_relation()
    conn = duckdb.connect()
    handle = load_graph(conn, EDGES, NODES)
    set_trivial_label_column(conn)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    if macro_mode == "python-udf":
        register_aggregate_udfs(conn, init_d=q2_udf.init_d, update_d=q2_udf.update_d,
                                 is_viable_d=q2_udf.is_viable_d,
                                 is_viable_d_final=q2_udf.is_viable_d_final,
                                 finalize_d=q2_udf.finalize_d)
    else:
        register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    standard_query = build_standard_query(relation=relation, start_vertices=starts, length_bound=length)
    optimized_query = build_optimized_query(aggregate=aggregate, relation=relation,
                                              start_vertices=starts, length_bound=length)
    return conn, standard_query, optimized_query


FULL_SIGNATURE_VERIFY_MAX_LENGTH = 3  # beyond this, materializing full "paths" is too expensive
                                       # (Q2 has no early filtering -- intermediate cardinality
                                       # is already in the millions by length_bound=4, per the
                                       # paper's own fig:intermediate_total_grid). Falls back to
                                       # count-only equivalence, which FR-22 already established
                                       # exactly at every smaller length here and in q1's full sweep.


def _verify_fr22(length, macro_mode="sql") -> int:
    conn, standard_query, optimized_query = _setup(length, macro_mode=macro_mode)
    if length <= FULL_SIGNATURE_VERIFY_MAX_LENGTH:
        verify_standard = run_query(conn, standard_query, result_shape="paths")
        verify_optimized = run_query(conn, optimized_query, result_shape="paths")
        standard_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_standard.rows}
        optimized_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_optimized.rows}
        assert standard_signature == optimized_signature, \
            f"FR-22 violated at length_bound={length} macro_mode={macro_mode}!"
        path_count = len(verify_standard.rows)
    else:
        verify_standard = run_query(conn, standard_query, result_shape="count")
        verify_optimized = run_query(conn, optimized_query, result_shape="count")
        assert verify_standard.rows[0][0] == verify_optimized.rows[0][0], \
            f"FR-22 (count) violated at length_bound={length} macro_mode={macro_mode}!"
        path_count = verify_standard.rows[0][0]
    conn.close()
    return path_count


def _poll_peak_rss(sampler, stop_event, peak_holder, interval=0.01):
    while not stop_event.is_set():
        value = sampler()
        if value is not None and value > peak_holder[0]:
            peak_holder[0] = value
        stop_event.wait(interval)


def run_one(length: int, variant: str, macro_mode: str = "sql") -> dict:
    import bench_common

    conn, standard_query, optimized_query = _setup(length, macro_mode=macro_mode)
    query = standard_query if variant == "standard" else optimized_query
    engine = "recap-new-optimized" if variant == "optimized" else (
        "recap-new-standard-udf" if macro_mode == "python-udf" else "recap-new-standard")

    sampler = bench_common.PsutilSelfSampler()
    peak = [sampler() or 0.0]
    stop_event = threading.Event()
    poller = threading.Thread(target=_poll_peak_rss, args=(sampler, stop_event, peak), daemon=True)
    poller.start()
    try:
        result = run_query(conn, query, result_shape="count")
    finally:
        stop_event.set()
        poller.join(timeout=2)
    conn.close()

    return {
        "engine": engine, "length": length, "result": result.rows[0][0],
        "runtime_ms": result.telemetry.runtime_ms,
        "intermediate_count_ms": result.telemetry.intermediate_count_ms,
        "peak_buffer_memory_mb": result.telemetry.peak_buffer_memory_mb,
        "peak_rss_mb": peak[0],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--length", type=int)
    parser.add_argument("--variant", choices=VARIANTS)
    parser.add_argument("--macro-mode", choices=MACRO_MODES, default="sql")
    args = parser.parse_args()

    if args.length is not None and args.variant is not None:
        row = run_one(args.length, args.variant, macro_mode=args.macro_mode)
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerow(row)
        return

    rows = []
    udf_rows = []
    for length in LENGTHS:
        t0 = time.time()
        path_count = _verify_fr22(length, macro_mode="sql")
        udf_path_count = _verify_fr22(length, macro_mode="python-udf")
        assert path_count == udf_path_count, \
            f"python-udf standard disagrees with sql-macro standard at length_bound={length}!"
        for variant, macro_mode in (("standard", "sql"), ("standard", "python-udf"), ("optimized", "sql")):
            proc = subprocess.run(
                [sys.executable, __file__, "--length", str(length), "--variant", variant,
                 "--macro-mode", macro_mode],
                capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                print(proc.stdout)
                print(proc.stderr, file=sys.stderr)
                raise RuntimeError(f"length={length} variant={variant} macro_mode={macro_mode} "
                                    f"subprocess failed (exit {proc.returncode})")
            lines = proc.stdout.strip().splitlines()
            reader = csv.DictReader(lines)
            row = next(reader)
            row["length"] = int(row["length"])
            row["result"] = int(row["result"])
            row["runtime_ms"] = float(row["runtime_ms"])
            row["intermediate_count_ms"] = float(row["intermediate_count_ms"])
            row["peak_buffer_memory_mb"] = float(row["peak_buffer_memory_mb"])
            row["peak_rss_mb"] = float(row["peak_rss_mb"])
            (udf_rows if macro_mode == "python-udf" else rows).append(row)
            print(f"length_bound={length} {row['engine']}: {row['result']} paths, "
                  f"runtime={row['runtime_ms']:.2f}ms, "
                  f"buffer_mem={row['peak_buffer_memory_mb']:.1f}MB, "
                  f"rss={row['peak_rss_mb']:.1f}MB")
        elapsed = time.time() - t0
        print(f"  (length_bound={length} done in {elapsed:.1f}s wall, {path_count} paths verified FR-22)")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")

    with open(CSV_PATH_UDF, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(udf_rows)
    print(f"wrote {len(udf_rows)} rows to {CSV_PATH_UDF}")


if __name__ == "__main__":
    main()
