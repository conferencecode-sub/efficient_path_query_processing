"""Sweeps Q1 (paper's real constraint, see q1_aggregate.py) over the new
compiler's Standard (Stage E) and Optimized (Stage F) pipelines, at
length_bound in {2..10} (matching the paper's own fig:recap_performance_grid
Q1-Metaverse sweep), against the real Metaverse dataset now symlinked at
`experiments/datasets/metaverse/` (same file as the bundled
ReCAP/simple_dataset/LG.csv -- confirmed identical, 78,600 edges / 1,320
vertices, matching tab:realdata exactly).

`length_bound` here means the same thing as the old prototype's
`max_length` (max edges per path) -- see standard_sql.build_standard_query's
`WHERE p.path_length < length_bound` -- and Q1's own NFA structure (state
0->1->2, needs >=1 normal + >=1 fraud edge) means no explicit min-length
filter is needed: nothing can reach the accepting state at path_length < 2,
so `length_bound=k` alone already gives exactly "paths of length in [2,k]".

`compile_regex_to_nfa(..., minimize=True)`: FR-7 requires this to default
to False (preserving the NFA is what keeps ReCAP compatible with
wavefront/segment planners, R4.O2), but this pilot is measuring standard
bottom-up evaluation only -- exactly the case FR-7 names as fine to opt
into minimization for.

**Memory, 2026-08-14 rework: per-variant subprocess isolation.** The
original version of this script ran both `standard_result` and
`optimized_result` on the *same* DuckDB connection within one process,
so `peak_buffer_memory_mb` (DuckDB's internal buffer stat) and any
process-RSS reading would be identical/cumulative for both rows -- not a
real per-variant measurement. Fixed by giving each (length, variant) pair
its own subprocess (`--length N --variant {standard,optimized}`), so
`bench_common.PsutilSelfSampler`'s peak-RSS reading and DuckDB's own
buffer-memory stat are both genuinely scoped to *that* variant's query
alone, matching the isolation approach `run_old_prototype.py` already
uses for the same reason. Two memory numbers are reported per row:
`peak_buffer_memory_mb` (DuckDB's internal buffer manager -- excludes
Python/pandas/connection overhead) and `peak_rss_mb` (this process's own
peak resident memory, comparable to the old-prototype/Kùzu/Neo4j/Memgraph
numbers elsewhere in this repo).

FR-22 (standard == optimized result sets) is checked once per length in
the *parent* process (its own throwaway connection, not the timed
subprocesses), so the equivalence check never contaminates either
variant's isolated memory reading.
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

import q1_udf  # noqa: E402
from q1_aggregate import q1_aggregate  # noqa: E402
from register_udfs import register_aggregate_udfs  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.regex_frontend import compile_regex_to_nfa  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import (  # noqa: E402
    build_standard_query, materialize_transitions, register_aggregate_macros,
)
from recap_compiler.transitions import build_transitions_relation  # noqa: E402

DATASET = os.path.join(os.path.dirname(__file__), "..", "datasets", "metaverse", "edges.csv")
REGEX = "(transfer|purchase|sale)+(phishing|scam)+"
START_VERTEX = 383
LENGTHS = (2, 3, 4, 5, 6, 7, 8, 9, 10)
VARIANTS = ("standard", "optimized")
MACRO_MODES = ("sql", "python-udf")
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q1.csv")
CSV_PATH_UDF = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q1_udf.csv")

CSV_FIELDNAMES = ["engine", "length", "result", "runtime_ms", "intermediate_count_ms",
                   "peak_buffer_memory_mb", "peak_rss_mb"]


def _setup(length, macro_mode="sql"):
    aggregate = q1_aggregate()
    nfa = compile_regex_to_nfa(REGEX, minimize=True)
    relation = build_transitions_relation(nfa)
    conn = duckdb.connect()
    handle = load_graph(conn, DATASET)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    if macro_mode == "python-udf":
        register_aggregate_udfs(conn, init_d=q1_udf.init_d, update_d=q1_udf.update_d,
                                 is_viable_d=q1_udf.is_viable_d,
                                 is_viable_d_final=q1_udf.is_viable_d_final,
                                 finalize_d=q1_udf.finalize_d)
    else:
        register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    standard_query = build_standard_query(relation=relation, start_vertices=starts, length_bound=length)
    optimized_query = build_optimized_query(aggregate=aggregate, relation=relation,
                                              start_vertices=starts, length_bound=length)
    return conn, standard_query, optimized_query


def _verify_fr22(length, macro_mode="sql") -> int:
    """Checks standard == optimized result sets once, on its own throwaway
    connection -- never part of either variant's timed/memory-measured run."""
    conn, standard_query, optimized_query = _setup(length, macro_mode=macro_mode)
    verify_standard = run_query(conn, standard_query, result_shape="paths")
    verify_optimized = run_query(conn, optimized_query, result_shape="paths")
    standard_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_standard.rows}
    optimized_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_optimized.rows}
    assert standard_signature == optimized_signature, \
        f"FR-22 violated at length_bound={length} macro_mode={macro_mode}!"
    path_count = len(verify_standard.rows)
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
