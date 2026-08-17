"""Sweeps Q4 (max-min *timestamp* trail -- see below) over the new
compiler's Standard (Stage E) and Optimized (Stage F) pipelines, against
the real LDBC100 dataset now symlinked at `experiments/datasets/ldbc100/`
(448,626 vertices / 19,941,198 edges, matching tab:realdata's
"448k / 19.9M" exactly -- already the correct directed edge count as
exported, no bidirecting needed: unlike Datagen-7.6/7.7's Graphalytics
export, LDBC's `person_knows_person` file is emitted as one directed row
per relationship instance and the paper's own count matches that row
count directly, confirmed by `wc -l`).

**Uses the paper's actual Q4 semantics, not the toy-dataset stand-in.**
`q4_aggregate.py`'s original hardcoded MAX_MIN_BOUND=20 was built for a
generic 0-100 "weight" column on the 100-node toy dataset, which has no
timestamp property -- but figures.tex's real Q4 (tab:queries) is "earliest
and latest edge timestamp along the path does not exceed two weeks", and
LDBC's `person_knows_person` has no "weight" column at all, only
`creationDate`. So this loader aliases `epoch_ms(creation_date)` AS
`weight` and calls `q4_aggregate(bound=1_209_600_000)` (two weeks, in
milliseconds) -- same bounded-range shape, now checking what the paper
actually specifies instead of an arbitrary placeholder range.

`START_VERTEX`: paper methodology picks **medium**-degree start vertices
for Q4 (see q2_length_sweep's own runner for the same rule re: Q2).
Computed directly: out-degree distribution over 389,944 vertices with
outgoing `knows` edges has q25=5, median=18, q75=57; vertex 24189256063073
has out-degree 18 (median).

Same "no regex" pathway + `_with_min_length` wrapper as Q3 (see that
runner's docstring for why it's needed), and the same per-variant
subprocess memory isolation as every other runner in this revision.
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
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "udf_variant"))

import q4_udf  # noqa: E402
from q4_aggregate import q4_aggregate  # noqa: E402
from register_udfs import register_aggregate_udfs  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import (  # noqa: E402
    build_standard_query, materialize_transitions, register_aggregate_macros,
)
from recap_compiler.transitions import trivial_relation  # noqa: E402

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets", "ldbc100")
EDGES_RAW = os.path.join(DATASET_DIR, "person_knows_person_0_0.csv")
START_VERTEX = 24189256063073  # out-degree 18 (median) -- see docstring
TWO_WEEKS_MS = 1_209_600_000
MIN_LENGTH = 2
LENGTHS = (2, 3, 4, 5, 6, 7, 8)  # length_bound=9 didn't finish in a 590s budget --
                                  # growth 7->8 was already 5.4x (10.7M -> 57.6M paths)
                                  # with buffer memory jumping to 10.3GB; capped here rather
                                  # than spending several more minutes for one more point.
VARIANTS = ("standard", "optimized")
MACRO_MODES = ("sql", "python-udf")
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q4.csv")
CSV_PATH_UDF = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q4_udf.csv")

CSV_FIELDNAMES = ["engine", "length", "result", "runtime_ms", "intermediate_count_ms",
                   "peak_buffer_memory_mb", "peak_rss_mb"]
_UDFS = q4_udf.make_udfs(bound=TWO_WEEKS_MS)


def _load_ldbc_edges(conn):
    """Loads the pipe-delimited LDBC export (header row present but with
    two duplicate 'Person_id' columns, so `header=false, skip=1` + explicit
    `columns=` is used instead of relying on DuckDB's name-based header
    matching). Aliases the ISO-8601 `creationDate` string as `weight`
    (epoch milliseconds) so it slots directly into `q4_aggregate`'s
    existing max-min-range shape."""
    conn.execute(
        "CREATE TABLE raw_edges AS SELECT * FROM read_csv(?, delim='|', header=false, skip=1, "
        "columns={'src': 'BIGINT', 'dst': 'BIGINT', 'creation_date': 'VARCHAR'})", [EDGES_RAW])
    conn.execute(
        "CREATE TABLE ldbc_edges AS SELECT src, dst, "
        "epoch_ms(CAST(creation_date AS TIMESTAMPTZ)) AS weight FROM raw_edges")
    return load_graph(conn, "ldbc_edges")


def _with_min_length(query, min_length: int):
    return SimpleNamespace(sql=f"SELECT * FROM ({query.sql}) t WHERE path_length >= {min_length}",
                            cte=query.cte)


def _setup(length, macro_mode="sql"):
    aggregate = q4_aggregate(bound=TWO_WEEKS_MS)
    relation = trivial_relation()
    conn = duckdb.connect()
    handle = _load_ldbc_edges(conn)
    set_trivial_label_column(conn)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    if macro_mode == "python-udf":
        register_aggregate_udfs(conn, init_d=_UDFS.init_d, update_d=_UDFS.update_d,
                                 is_viable_d=_UDFS.is_viable_d,
                                 is_viable_d_final=_UDFS.is_viable_d_final,
                                 finalize_d=_UDFS.finalize_d)
    else:
        register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    standard_query = _with_min_length(
        build_standard_query(relation=relation, start_vertices=starts, length_bound=length), MIN_LENGTH)
    optimized_query = _with_min_length(
        build_optimized_query(aggregate=aggregate, relation=relation,
                               start_vertices=starts, length_bound=length), MIN_LENGTH)
    return conn, standard_query, optimized_query


FULL_SIGNATURE_VERIFY_MAX_LENGTH = 3


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
