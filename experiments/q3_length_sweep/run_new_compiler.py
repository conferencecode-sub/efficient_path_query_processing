"""Sweeps Q3 (monotonic trail, see q3_aggregate.py) over the new compiler's
Standard (Stage E) and Optimized (Stage F) pipelines, against the real
Datagen-7.6 dataset now symlinked at `experiments/datasets/datagen7.6/`
(754,147 vertices / 42,162,988 undirected edges = 84,325,976 directed,
matching tab:realdata's "754k / 84.3M" exactly, confirmed by direct count).

**Custom loader, unlike Q1/Q2 (`load_graph` alone isn't enough):**
Datagen-7.6's `.e`/`.v` files are LDBC Graphalytics format, not the
`nodes.csv`/`edges.csv` convention -- space-delimited `src dst weight`, no
header, no `label` column, and the graph is *undirected*
(`dataset.properties`: `directed = false`), meaning each physical line is
one edge but represents traversal in both directions. `_load_bidirected`
below loads the raw file, then unions it with its own reversal (`src dst`
and `dst src`, same weight) before handing the result to `load_graph` as
an already-registered table name -- `_load_relation`'s own "not a CSV
path" branch does `SELECT * FROM <table>` in that case, confirmed via
`ingestion.py` directly rather than assumed.

`START_VERTEX`: the paper's own methodology picks **low**-degree start
vertices for Q3 specifically (high for Q1, medium for Q2/Q4) -- computed
directly: out-degree distribution over the bidirected graph has q25=22,
median=61, q75=140; the vertex chosen below has out-degree 22 (q25).

Q3 has no label regex, so this uses the same "no regex" pathway
(`set_trivial_label_column` + `trivial_relation()`) as Q2/Q4, and the same
`_with_min_length` wrapper Q3/Q4 both need (see their own aggregate/runner
docstrings for why: `is_viable_d_final=TRUE` over a trivial one-state
automaton doesn't structurally exclude 0/1-edge "paths" on its own).

**Memory: per-variant subprocess isolation**, same rework and same reason
as `q1_length_sweep/run_new_compiler.py`.
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

import q3_udf  # noqa: E402
from q3_aggregate import q3_aggregate  # noqa: E402
from register_udfs import register_aggregate_udfs  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import (  # noqa: E402
    build_standard_query, materialize_transitions, register_aggregate_macros,
)
from recap_compiler.transitions import trivial_relation  # noqa: E402

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets", "datagen7.6")
EDGES_RAW = os.path.join(DATASET_DIR, "edges.e")
START_VERTEX = 4398046568596  # out-degree 22 (q25, low) in the bidirected graph -- see docstring
MIN_LENGTH = 2
LENGTHS = (2, 3, 4)
VARIANTS = ("standard", "optimized")
MACRO_MODES = ("sql", "python-udf")
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q3.csv")
CSV_PATH_UDF = os.path.join(os.path.dirname(__file__), "results", "new_compiler_q3_udf.csv")

CSV_FIELDNAMES = ["engine", "length", "result", "runtime_ms", "intermediate_count_ms",
                   "peak_buffer_memory_mb", "peak_rss_mb"]


def _load_bidirected(conn):
    """Loads the raw Graphalytics `.e` file and unions it with its own
    reversal, since the graph is undirected but stored as one line per
    edge -- see module docstring."""
    conn.execute(
        "CREATE TABLE raw_edges AS SELECT * FROM read_csv(?, delim=' ', header=false, "
        "columns={'src': 'BIGINT', 'dst': 'BIGINT', 'weight': 'DOUBLE'})", [EDGES_RAW])
    conn.execute(
        "CREATE TABLE bidirected_edges AS "
        "SELECT src, dst, weight FROM raw_edges UNION ALL SELECT dst, src, weight FROM raw_edges")
    return load_graph(conn, "bidirected_edges")


def _with_min_length(query, min_length: int):
    return SimpleNamespace(sql=f"SELECT * FROM ({query.sql}) t WHERE path_length >= {min_length}",
                            cte=query.cte)


def _setup(length, macro_mode="sql"):
    aggregate = q3_aggregate()
    relation = trivial_relation()
    conn = duckdb.connect()
    handle = _load_bidirected(conn)
    set_trivial_label_column(conn)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    if macro_mode == "python-udf":
        register_aggregate_udfs(conn, init_d=q3_udf.init_d, update_d=q3_udf.update_d,
                                 is_viable_d=q3_udf.is_viable_d,
                                 is_viable_d_final=q3_udf.is_viable_d_final,
                                 finalize_d=q3_udf.finalize_d)
    else:
        register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    standard_query = _with_min_length(
        build_standard_query(relation=relation, start_vertices=starts, length_bound=length), MIN_LENGTH)
    optimized_query = _with_min_length(
        build_optimized_query(aggregate=aggregate, relation=relation,
                               start_vertices=starts, length_bound=length), MIN_LENGTH)
    return conn, standard_query, optimized_query


FULL_SIGNATURE_VERIFY_MAX_LENGTH = 3  # see q2_length_sweep's own runner for why beyond this,
                                       # count-only equivalence is used instead.


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
