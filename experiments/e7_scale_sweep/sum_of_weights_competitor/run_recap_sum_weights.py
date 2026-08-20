"""ReCAP (Stage F/optimized) side of the sum-of-weights scalability sweep
over Datagen-7.7. Same dataset loading convention as
`experiments/e7_scale_sweep/run_q3_datagen77.py`/`run_q4_datagen77.py`
(bidirect + `ignore_errors=true` for the one truncated final row), same
trivial single-state automaton (`recap_compiler.transitions.trivial_relation`,
no label regex), same `_with_min_length` wrapper, same per-length
subprocess isolation for a clean peak-RSS measurement.

Query: `sum_weight_aggregate.py`'s trail + running-sum-bound aggregate.
**Pass 4** (see that file's docstring for the full derivation, and passes
1-3's history): completely different vertex-search strategy this time --
`START_VERTEX = 2199025319463` was found by systematically searching for
a vertex whose *unconstrained* trail count itself grows gently (checked
directly, not assumed), rather than picking a vertex and hoping a bound
could tame explosive growth after the fact. It sits on an unusually long
(>= 8 hop, verified) unbranched pendant chain into this dataset's single
giant component, with genuinely flat unconstrained trail counts
(`2, 2, 3, 3, 4, 4, 3, 3` at lengths 2-8) -- safe by construction for any
length tested here, unlike every vertex tried in passes 1-3 (all of which
grew 15-190x per hop by length 4-5). `SUM_WEIGHT_BOUND = 6.7` is the
tightest value that still reaches length 6 for this vertex (length 5's
own achievable range has no variance to cut; length 6's does -- see
`sum_weight_aggregate.py`'s docstring, including why Q4-style max-min was
considered and rejected for this specific vertex).

**This pass is ReCAP-only -- Memgraph is skipped** (both the runner and
the reference-vs-Memgraph verification), per this pass's own brief: every
memory incident so far came from either an unfiltered diagnostic or from
Memgraph needing to fully enumerate before filtering, never from ReCAP's
own inline filtering, and this vertex's trail counts are tiny regardless
of engine, so a competitor comparison would not be informative this pass.

At every length this script still verifies ReCAP's own count against
`reference_sum_weights.py`'s independent, hand-written DuckDB recursive CTE
(not ReCAP's own generated SQL) before trusting the timing -- that
discipline is unchanged. Each length is run in its own subprocess with a
hard 1800s (30 minute) wall-clock timeout -- if ReCAP itself doesn't
finish a length within that budget, that is this script's own "document
and stop" point for that length (not exercised this pass: every length
here is tiny and fast by construction).
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "SOA-GDBMS"))

from reference_sum_weights import sum_weight_reference  # noqa: E402
from sum_weight_aggregate import SUM_WEIGHT_BOUND, sum_weight_aggregate  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions, register_aggregate_macros  # noqa: E402
from recap_compiler.transitions import trivial_relation  # noqa: E402

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "datagen7.7")
EDGES_RAW = os.path.join(DATASET_DIR, "edges.e")
START_VERTEX = 2199025319463  # gentle-growth pendant-chain vertex -- see sum_weight_aggregate.py's docstring (pass 4)
MIN_LENGTH = 2
LENGTHS = (2, 3, 4, 5, 6, 7)  # length 7 is a confirmed hard floor (min sum 7.497 > bound); see sum_weight_aggregate.py
PER_LENGTH_TIMEOUT_S = 1800  # 30 minutes -- "document and stop" if exceeded, per this experiment's brief
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "recap_sum_weights.csv")

CSV_FIELDNAMES = ["length", "result", "reference_result", "match", "runtime_ms",
                   "intermediate_count_ms", "peak_buffer_memory_mb", "peak_rss_mb", "error"]


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
    aggregate = sum_weight_aggregate()
    relation = trivial_relation()
    conn = duckdb.connect()
    handle = _load_bidirected(conn)
    set_trivial_label_column(conn)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    optimized_query = _with_min_length(
        build_optimized_query(aggregate=aggregate, relation=relation,
                               start_vertices=starts, length_bound=length), MIN_LENGTH)
    return conn, optimized_query


def _poll_peak_rss(sampler, stop_event, peak_holder, interval=0.01):
    while not stop_event.is_set():
        value = sampler()
        if value is not None and value > peak_holder[0]:
            peak_holder[0] = value
        stop_event.wait(interval)


def run_one(length: int) -> dict:
    import bench_common

    conn, optimized_query = _setup(length)

    sampler = bench_common.PsutilSelfSampler()
    peak = [sampler() or 0.0]
    stop_event = threading.Event()
    poller = threading.Thread(target=_poll_peak_rss, args=(sampler, stop_event, peak), daemon=True)
    poller.start()
    try:
        result = run_query(conn, optimized_query, result_shape="count")
    finally:
        stop_event.set()
        poller.join(timeout=2)

    recap_count = result.rows[0][0]
    reference_count = sum_weight_reference(conn, START_VERTEX, MIN_LENGTH, length, SUM_WEIGHT_BOUND)
    conn.close()

    return {
        "length": length, "result": recap_count, "reference_result": reference_count,
        "match": recap_count == reference_count,
        "runtime_ms": result.telemetry.runtime_ms,
        "intermediate_count_ms": result.telemetry.intermediate_count_ms,
        "peak_buffer_memory_mb": result.telemetry.peak_buffer_memory_mb,
        "peak_rss_mb": peak[0], "error": "",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--length", type=int)
    args = parser.parse_args()

    if args.length is not None:
        row = run_one(args.length)
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerow(row)
        return

    rows = []
    for length in LENGTHS:
        try:
            proc = subprocess.run(
                [sys.executable, __file__, "--length", str(length)],
                capture_output=True, text=True, check=False, timeout=PER_LENGTH_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            print(f"length={length}: TIMEOUT after {PER_LENGTH_TIMEOUT_S}s -- stopping sweep (document and stop)")
            rows.append({"length": length, "result": "", "reference_result": "", "match": False,
                         "runtime_ms": "", "intermediate_count_ms": "", "peak_buffer_memory_mb": "",
                         "peak_rss_mb": "", "error": f"timeout after {PER_LENGTH_TIMEOUT_S}s"})
            break
        if proc.returncode != 0:
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
            raise RuntimeError(f"length={length} subprocess failed (exit {proc.returncode})")
        lines = proc.stdout.strip().splitlines()
        reader = csv.DictReader(lines)
        row = next(reader)
        row["length"] = int(row["length"])
        row["result"] = int(row["result"])
        row["reference_result"] = int(row["reference_result"])
        row["match"] = row["match"] == "True"
        row["runtime_ms"] = float(row["runtime_ms"])
        row["intermediate_count_ms"] = float(row["intermediate_count_ms"])
        row["peak_buffer_memory_mb"] = float(row["peak_buffer_memory_mb"])
        row["peak_rss_mb"] = float(row["peak_rss_mb"])
        rows.append(row)
        print(f"length_bound={length}: recap={row['result']} reference={row['reference_result']} "
              f"match={row['match']} runtime={row['runtime_ms']:.2f}ms "
              f"buffer_mem={row['peak_buffer_memory_mb']:.1f}MB rss={row['peak_rss_mb']:.1f}MB")
        if not row["match"]:
            print(f"  MISMATCH at length={length} -- stopping sweep for investigation")
            break

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
