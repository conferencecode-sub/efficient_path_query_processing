"""E7 scale check: sweeps Q4 (max-min-weight trail) over the new compiler's
Standard vs Optimized pipelines against Datagen-7.7
(`experiments/datasets/datagen7.7/`) -- see `run_q3_datagen77.py`'s
docstring for the dataset's own quirks (truncated final row, extreme
degree skew) that also apply here.

**Bound choice, real finding:** unlike LDBC100 (where Q4 uses a real
2-week timestamp window) or the toy dataset (0-100 weight range,
bound=20), Datagen-7.7's `weight` column is extremely narrow and skewed:
p10=p25=p50=1.0, p75=1.04, p90=1.29, max=1.75 -- most edges weigh exactly
1.0. `MAX_MIN_BOUND=0.1` (roughly 13% of the full [1.0, 1.75] span) is
used here; whether this is actually selective (vs. trivially satisfied by
paths that never touch the rare >1.1-weight edges) is reported empirically
in the README rather than assumed.

`START_VERTEX = 245996`: out-degree 2 -- this graph's own q75, used as the
best available "medium" anchor since q25 and median both collapse to 1
(see `run_q3_datagen77.py`'s docstring for the degree-skew finding).

Same `_with_min_length` wrapper as every other "no regex" query (Q3/Q4),
and the same per-variant subprocess memory isolation as every other runner.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "q4_length_sweep"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))

from q4_aggregate import q4_aggregate  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import (  # noqa: E402
    build_standard_query, materialize_transitions, register_aggregate_macros,
)
from recap_compiler.transitions import trivial_relation  # noqa: E402

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets", "datagen7.7")
EDGES_RAW = os.path.join(DATASET_DIR, "edges.e")
START_VERTEX = 245996  # out-degree 2 (this graph's own q75, the best available "medium" anchor)
MAX_MIN_BOUND = 0.1
MIN_LENGTH = 2
LENGTHS = (2, 3, 4, 5, 6)  # length_bound=7 didn't finish in a 500s budget -- growth 5->6 was
                            # already 43x (3.4M -> 147.4M paths), confirming bound=0.1 isn't very
                            # selective given how skewed this dataset's weight distribution is
                            # (most edges weigh ~1.0, so most paths trivially satisfy the bound)
VARIANTS = ("standard", "optimized")
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q4_datagen77.csv")

CSV_FIELDNAMES = ["engine", "length", "result", "runtime_ms", "intermediate_count_ms",
                   "peak_buffer_memory_mb", "peak_rss_mb"]


def _load_bidirected(conn):
    conn.execute(
        "CREATE TABLE raw_edges AS SELECT * FROM read_csv(?, delim=' ', header=false, ignore_errors=true, "
        "columns={'src': 'BIGINT', 'dst': 'BIGINT', 'weight': 'DOUBLE'})", [EDGES_RAW])
    conn.execute(
        "CREATE TABLE bidirected_edges AS "
        "SELECT src, dst, weight FROM raw_edges UNION ALL SELECT dst, src, weight FROM raw_edges")
    return load_graph(conn, "bidirected_edges")


def _with_min_length(query, min_length: int):
    return SimpleNamespace(sql=f"SELECT * FROM ({query.sql}) t WHERE path_length >= {min_length}",
                            cte=query.cte)


def _setup(length):
    aggregate = q4_aggregate(bound=MAX_MIN_BOUND)
    relation = trivial_relation()
    conn = duckdb.connect()
    handle = _load_bidirected(conn)
    set_trivial_label_column(conn)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    standard_query = _with_min_length(
        build_standard_query(relation=relation, start_vertices=starts, length_bound=length), MIN_LENGTH)
    optimized_query = _with_min_length(
        build_optimized_query(aggregate=aggregate, relation=relation,
                               start_vertices=starts, length_bound=length), MIN_LENGTH)
    return conn, standard_query, optimized_query


FULL_SIGNATURE_VERIFY_MAX_LENGTH = 3


def _verify_fr22(length) -> int:
    conn, standard_query, optimized_query = _setup(length)
    if length <= FULL_SIGNATURE_VERIFY_MAX_LENGTH:
        verify_standard = run_query(conn, standard_query, result_shape="paths")
        verify_optimized = run_query(conn, optimized_query, result_shape="paths")
        standard_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_standard.rows}
        optimized_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_optimized.rows}
        assert standard_signature == optimized_signature, f"FR-22 violated at length_bound={length}!"
        path_count = len(verify_standard.rows)
    else:
        verify_standard = run_query(conn, standard_query, result_shape="count")
        verify_optimized = run_query(conn, optimized_query, result_shape="count")
        assert verify_standard.rows[0][0] == verify_optimized.rows[0][0], \
            f"FR-22 (count) violated at length_bound={length}!"
        path_count = verify_standard.rows[0][0]
    conn.close()
    return path_count


def _poll_peak_rss(sampler, stop_event, peak_holder, interval=0.01):
    while not stop_event.is_set():
        value = sampler()
        if value is not None and value > peak_holder[0]:
            peak_holder[0] = value
        stop_event.wait(interval)


def run_one(length: int, variant: str) -> dict:
    import bench_common

    conn, standard_query, optimized_query = _setup(length)
    query = standard_query if variant == "standard" else optimized_query

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
        "engine": f"recap-new-{variant}", "length": length, "result": result.rows[0][0],
        "runtime_ms": result.telemetry.runtime_ms,
        "intermediate_count_ms": result.telemetry.intermediate_count_ms,
        "peak_buffer_memory_mb": result.telemetry.peak_buffer_memory_mb,
        "peak_rss_mb": peak[0],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--length", type=int)
    parser.add_argument("--variant", choices=VARIANTS)
    args = parser.parse_args()

    if args.length is not None and args.variant is not None:
        row = run_one(args.length, args.variant)
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerow(row)
        return

    rows = []
    for length in LENGTHS:
        t0 = time.time()
        path_count = _verify_fr22(length)
        for variant in VARIANTS:
            proc = subprocess.run(
                [sys.executable, __file__, "--length", str(length), "--variant", variant],
                capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                print(proc.stdout)
                print(proc.stderr, file=sys.stderr)
                raise RuntimeError(f"length={length} variant={variant} subprocess failed "
                                    f"(exit {proc.returncode})")
            lines = proc.stdout.strip().splitlines()
            reader = csv.DictReader(lines)
            row = next(reader)
            row["length"] = int(row["length"])
            row["result"] = int(row["result"])
            row["runtime_ms"] = float(row["runtime_ms"])
            row["intermediate_count_ms"] = float(row["intermediate_count_ms"])
            row["peak_buffer_memory_mb"] = float(row["peak_buffer_memory_mb"])
            row["peak_rss_mb"] = float(row["peak_rss_mb"])
            rows.append(row)
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


if __name__ == "__main__":
    main()
