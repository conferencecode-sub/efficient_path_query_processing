"""E6/SF10 / TCR1: sweeps ReCAP (Stage F, optimized) over increasing
length_bound, verified against `reference_baseline.tcr1_reference` before
trusting any timing -- same structure as `experiments/e6_finbench/run_tcr1.py`
(SF0.1), just pointed at SF10 data with a start vertex re-derived for SF10's
own graph (SF0.1's `START_VERTEX` doesn't exist in this larger, independently
generated dataset). `length_bound` is total path length (transfer hops + the
final signedInBy hop), so FinBench's own "*1..3" cap corresponds to
length_bound=4 -- lengths beyond that are the "lift the cap" part of E6.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import threading

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))

from common import load_data  # noqa: E402
from reference_baseline import tcr1_reference  # noqa: E402
from tcr1_aggregate import NFA_RELATION, tcr1_aggregate  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions, register_aggregate_macros  # noqa: E402

# Re-derived for SF10 via the same methodology as SF0.1/SF1 ("highest
# transfer out-degree"): `SELECT src, COUNT(*) FROM edges WHERE
# label='transfer' GROUP BY src ORDER BY COUNT(*) DESC` against SF10's own
# loaded data -> out-degree 8693 (tied with one other vertex at the same
# degree). Verified non-zero (53,857 at length_bound=4) via tcr1_reference
# before trusting it, since SF1's own START_VERTEX turned out to not exist
# meaningfully in this independently-generated SF10 dataset (0 results at
# every length) -- each scale factor is a fresh random graph, not an
# extension of the smaller one.
START_VERTEX = 208575431643765159
MIN_LENGTH = 2  # TCR1's accepting state (after the mandatory signedInBy hop)
                 # is only reachable at path_length >= 2 anyway, but the raw
                 # recursive CTE also contains the length-0/length-1 prefix
                 # rows -- floor here so intermediate_paths is apples-to-apples
                 # with the competitor scripts' own candidate-only queries.
LENGTHS = (2, 3, 4, 5, 6, 7, 8)
TIMEOUT_S = 7200
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "tcr1.csv")
CSV_FIELDNAMES = ["length", "result", "reference_result", "runtime_ms",
                   "intermediate_paths", "peak_rss_mb", "error"]


def _setup(length):
    aggregate = tcr1_aggregate()
    conn = duckdb.connect()
    load_data(conn)
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, NFA_RELATION)
    query = build_optimized_query(aggregate=aggregate, relation=NFA_RELATION,
                                    start_vertices=[START_VERTEX], length_bound=length)
    return conn, query


def _poll_peak_rss(sampler, stop_event, peak_holder, interval=0.01):
    while not stop_event.is_set():
        value = sampler()
        if value is not None and value > peak_holder[0]:
            peak_holder[0] = value
        stop_event.wait(interval)


def run_one(length: int) -> dict:
    import bench_common

    conn, query = _setup(length)
    reference_result = tcr1_reference(conn, START_VERTEX, length)

    sampler = bench_common.PsutilSelfSampler()
    peak = [sampler() or 0.0]
    stop_event = threading.Event()
    poller = threading.Thread(target=_poll_peak_rss, args=(sampler, stop_event, peak), daemon=True)
    poller.start()
    try:
        result = run_query(conn, query, result_shape="count")
        intermediate = conn.execute(
            f"WITH RECURSIVE {query.cte} SELECT count(*) FROM paths "
            f"WHERE path_length >= {MIN_LENGTH}").fetchone()[0]
    finally:
        stop_event.set()
        poller.join(timeout=2)
    conn.close()

    assert result.rows[0][0] == reference_result, (
        f"length={length}: ReCAP={result.rows[0][0]} != reference={reference_result}")

    return {
        "length": length, "result": result.rows[0][0], "reference_result": reference_result,
        "runtime_ms": result.telemetry.runtime_ms, "intermediate_paths": intermediate,
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
            proc = subprocess.run([sys.executable, __file__, "--length", str(length)],
                                   capture_output=True, text=True, check=False, timeout=TIMEOUT_S)
        except subprocess.TimeoutExpired:
            print(f"length={length}: TIMEOUT after {TIMEOUT_S}s -- stopping sweep")
            rows.append({"length": length, "result": "", "reference_result": "",
                          "runtime_ms": "", "intermediate_paths": "", "peak_rss_mb": "",
                          "error": f"timeout after {TIMEOUT_S}s"})
            break
        if proc.returncode != 0:
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
            print(f"length={length}: subprocess failed (exit {proc.returncode}) -- stopping sweep")
            rows.append({"length": length, "result": "", "reference_result": "",
                          "runtime_ms": "", "intermediate_paths": "", "peak_rss_mb": "",
                          "error": f"exit {proc.returncode}"})
            break
        lines = proc.stdout.strip().splitlines()
        reader = csv.DictReader(lines)
        row = next(reader)
        row["length"] = int(row["length"])
        row["result"] = int(row["result"])
        row["reference_result"] = int(row["reference_result"])
        row["runtime_ms"] = float(row["runtime_ms"])
        row["intermediate_paths"] = int(row["intermediate_paths"])
        row["peak_rss_mb"] = float(row["peak_rss_mb"])
        rows.append(row)
        print(f"length={length}: {row['result']} paths (matches reference: "
              f"{row['result'] == row['reference_result']}), runtime={row['runtime_ms']:.2f}ms, "
              f"intermediate={row['intermediate_paths']}, rss={row['peak_rss_mb']:.1f}MB")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
