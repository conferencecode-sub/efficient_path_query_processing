"""E7 scale check (see `experiments/new_experiments_checklist/
recap_experiments_requirements.md`): sweeps Q3 (monotonic trail) over the
new compiler's Standard vs Optimized pipelines against Datagen-7.7
(`experiments/datasets/datagen7.7/`), the larger-but-sparser dataset in
tab:realdata not yet exercised anywhere in this repo.

**Real data-quality finding, confirmed directly:** the raw `.e` file has
26,894,900 well-formed `src dst weight` lines and one final, truncated line
(`8796109644722 10995117360684`, missing its weight value and the
trailing newline) -- the file was evidently cut off mid-write during
extraction. `ignore_errors=true` on `read_csv` drops just that one row;
the other 26,894,900 bidirect to 53,789,800, matching tab:realdata's
"53.7M" edges exactly. Vertex count (13,180,508) matches "13.1M" directly,
no adjustment needed. `dataset.properties` claims 32,791,267 edges, which
doesn't match either the raw line count or the paper's own table --
apparently stale/wrong, not something this loader trusts.

**Real degree-distribution finding, relevant to `START_VERTEX` choice:**
unlike Datagen-7.6 (q25=22, median=61, q75=140) and LDBC100 (q25=5,
median=18, q75=57), this graph is extremely sparse and skewed: q25=1,
median=1, q75=2 (out of 10,933,040 distinct vertices with edges; 5,959,720
of them -- 55% -- have out-degree exactly 1; max out-degree is only 2,084).
The paper's own "low/medium/high out-degree quartile" methodology
degenerates here since q25 and median coincide -- there's no genuine
"low vs. medium" split available. Used degree=1 (matches q25/median
exactly) as the low-degree anchor for Q3, consistent with every other
query's use of the *true* low quartile.

Same custom Graphalytics loader pattern as `q3_length_sweep`'s Datagen-7.6
runner (bidirect + `ignore_errors=true`), same per-variant subprocess
memory isolation, same `_with_min_length` wrapper.
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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "q3_length_sweep"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))

from q3_aggregate import q3_aggregate  # noqa: E402

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
START_VERTEX = 3184  # out-degree 1 (q25 == median in this sparse graph) -- see docstring
MIN_LENGTH = 2
LENGTHS = (2, 3, 4, 5, 6, 7, 8, 9, 10)
VARIANTS = ("standard", "optimized")
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q3_datagen77.csv")

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
    aggregate = q3_aggregate()
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


def _verify_equivalence(length) -> int:
    conn, standard_query, optimized_query = _setup(length)
    if length <= FULL_SIGNATURE_VERIFY_MAX_LENGTH:
        verify_standard = run_query(conn, standard_query, result_shape="paths")
        verify_optimized = run_query(conn, optimized_query, result_shape="paths")
        standard_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_standard.rows}
        optimized_signature = {(v, q, path_length) for v, q, _d, path_length, _r in verify_optimized.rows}
        assert standard_signature == optimized_signature, f"standard/optimized equivalence violated at length_bound={length}!"
        path_count = len(verify_standard.rows)
    else:
        verify_standard = run_query(conn, standard_query, result_shape="count")
        verify_optimized = run_query(conn, optimized_query, result_shape="count")
        assert verify_standard.rows[0][0] == verify_optimized.rows[0][0], \
            f"standard/optimized count equivalence violated at length_bound={length}!"
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
        path_count = _verify_equivalence(length)
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
        print(f"  (length_bound={length} done in {elapsed:.1f}s wall, {path_count} paths verified equivalent)")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
