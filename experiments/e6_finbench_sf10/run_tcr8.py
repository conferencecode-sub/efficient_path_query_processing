"""E6/SF10 / TCR8: sweeps ReCAP (Stage F, optimized) over increasing
length_bound, verified against `reference_baseline.tcr8_reference`.
length_bound=4 (1 deposit + up to 3 transfer/withdraw) is FinBench's own
cap. Same structure as `experiments/e6_finbench/run_tcr8.py` (SF0.1), start
vertex re-derived for SF10's own graph.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import threading
from types import SimpleNamespace

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))

from common import load_data  # noqa: E402
from reference_baseline import tcr8_reference  # noqa: E402
from tcr8_aggregate import NFA_RELATION, THRESHOLD, tcr8_aggregate  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions, register_aggregate_macros  # noqa: E402

# Re-derived for SF10 via the same methodology as SF0.1/SF1 ("loan with the
# most deposit-reachable transfer/withdraw chains at length 2"): joining
# deposit->{transfer,withdraw} and grouping by the loan, against SF10's own
# loaded data -> 4,540 reachable (tied with one other vertex). Verified
# non-zero (501,419 at length_bound=4) via tcr8_reference before trusting
# it -- see run_tcr1.py for why re-deriving per scale factor is necessary.
START_VERTEX = 241226254064298435
MIN_LENGTH = 2  # same `_with_min_length` fix as SF0.1's run_tcr8.py -- TCR8's
                 # accepting state is reached right after the mandatory
                 # deposit hop and self-loops for repeated 234-hops, so
                 # nothing structurally excludes a bare deposit with zero
                 # 234-hops without this wrapper.
LENGTHS = (2, 3, 4, 5, 6, 7, 8)
TIMEOUT_S = 7200
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "tcr8.csv")
CSV_FIELDNAMES = ["length", "result", "reference_result", "runtime_ms", "peak_rss_mb", "error"]


def _with_min_length(query, min_length: int):
    return SimpleNamespace(sql=f"SELECT * FROM ({query.sql}) t WHERE path_length >= {min_length}",
                            cte=query.cte)


def _setup(length):
    aggregate = tcr8_aggregate()
    conn = duckdb.connect()
    load_data(conn)
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, NFA_RELATION)
    query = build_optimized_query(aggregate=aggregate, relation=NFA_RELATION,
                                    start_vertices=[START_VERTEX], length_bound=length)
    query = _with_min_length(query, MIN_LENGTH)
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
    reference_result = tcr8_reference(conn, START_VERTEX, length, threshold=THRESHOLD)

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

    assert result.rows[0][0] == reference_result, (
        f"length={length}: ReCAP={result.rows[0][0]} != reference={reference_result}")

    return {
        "length": length, "result": result.rows[0][0], "reference_result": reference_result,
        "runtime_ms": result.telemetry.runtime_ms, "peak_rss_mb": peak[0], "error": "",
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
                          "runtime_ms": "", "peak_rss_mb": "", "error": f"timeout after {TIMEOUT_S}s"})
            break
        if proc.returncode != 0:
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
            print(f"length={length}: subprocess failed (exit {proc.returncode}) -- stopping sweep")
            rows.append({"length": length, "result": "", "reference_result": "",
                          "runtime_ms": "", "peak_rss_mb": "", "error": f"exit {proc.returncode}"})
            break
        lines = proc.stdout.strip().splitlines()
        reader = csv.DictReader(lines)
        row = next(reader)
        row["length"] = int(row["length"])
        row["result"] = int(row["result"])
        row["reference_result"] = int(row["reference_result"])
        row["runtime_ms"] = float(row["runtime_ms"])
        row["peak_rss_mb"] = float(row["peak_rss_mb"])
        rows.append(row)
        print(f"length={length}: {row['result']} paths (matches reference: "
              f"{row['result'] == row['reference_result']}), runtime={row['runtime_ms']:.2f}ms, "
              f"rss={row['peak_rss_mb']:.1f}MB")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
