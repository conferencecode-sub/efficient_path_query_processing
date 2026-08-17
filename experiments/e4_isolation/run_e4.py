"""E4 (see `experiments/new_experiments_checklist/recap_experiments_requirements.md`):
isolates the automata-exploration contribution from the property-early-
filtering contribution, per R3.O1. Three configs on Q1 (the only query with
both a real regex and per-hop property constraints -- Q3 has no regex, see
the planning discussion this session), same real Metaverse dataset/start
vertex (383, high-degree, matching every other Q1 experiment) as E1/E5:

1. **regex-only** (`q1_regex_only_aggregate.py`) -- NFA + trail only, no
   property constraint at all.
2. **regex + late property check** (`q1_late_property_aggregate.py`) -- same
   state tracking as ReCAP's own aggregate, but every property constraint
   deferred to `is_viable_d_final` instead of pruning per-hop.
3. **regex + early property filtering** -- full ReCAP, i.e. the *existing*
   `q1_aggregate.py` from `q1_length_sweep/` (identical to the E1 rerun's
   `recap-new-optimized`, rerun here rather than reused so all three
   configs report `intermediate_paths` uniformly -- E1's own CSV didn't
   capture that field).

All three built via `build_optimized_query` (Stage F) only -- the
standard-vs-optimized axis is a separate, already-answered question (E1),
not what E4 is isolating. Reports both runtime and `telemetry.
intermediate_paths` (the recursive CTE's row count *before* the outer
`is_viable_d_final` filter -- exactly the paper's own "total candidate
paths" metric from `fig:intermediate_total_grid`).

Since configs 1 and 2 have no early property pruning, intermediate
cardinality can blow up combinatorially at higher `length_bound`, per the
paper's own point about Q1's `transfer|purchase|sale` self-loop covering
~92% of edges -- this script's `LENGTHS` starts conservative and is
extended only after confirming timing stays reasonable at each step.
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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "q1_length_sweep"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))

from q1_aggregate import q1_aggregate  # noqa: E402
from q1_late_property_aggregate import q1_late_property_aggregate  # noqa: E402
from q1_regex_only_aggregate import q1_regex_only_aggregate  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.regex_frontend import compile_regex_to_nfa  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions, register_aggregate_macros  # noqa: E402
from recap_compiler.transitions import build_transitions_relation  # noqa: E402

DATASET = os.path.join(os.path.dirname(__file__), "..", "datasets", "metaverse", "edges.csv")
REGEX = "(transfer|purchase|sale)+(phishing|scam)+"
START_VERTEX = 383
LENGTHS = (2, 3, 4)  # configs 1/2 (no early property pruning) grow ~50x per hop here
                      # (confirmed: 1,408 -> 69,500 -> 3,530,274 intermediate/accepting-state
                      # rows at ell=2/3/4, matching fig:intermediate_total_grid's own published
                      # numbers almost exactly) -- length_bound=5 would likely take many minutes
                      # and tens-to-hundreds of GB, consistent with why the paper's own
                      # fig:performance_grid/fig:intermediate_total_grid cap this axis at ell=4 too.
CONFIGS = {
    "1-regex-only": q1_regex_only_aggregate,
    "2-regex-late-property": q1_late_property_aggregate,
    "3-regex-early-property": q1_aggregate,
}
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "e4_isolation.csv")

CSV_FIELDNAMES = ["config", "length", "result", "intermediate_paths", "runtime_ms",
                   "peak_buffer_memory_mb", "peak_rss_mb"]


def _setup(config_name, length):
    aggregate = CONFIGS[config_name]()
    nfa = compile_regex_to_nfa(REGEX, minimize=True)
    relation = build_transitions_relation(nfa)
    conn = duckdb.connect()
    handle = load_graph(conn, DATASET)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    query = build_optimized_query(aggregate=aggregate, relation=relation,
                                    start_vertices=starts, length_bound=length)
    return conn, query


def _poll_peak_rss(sampler, stop_event, peak_holder, interval=0.01):
    while not stop_event.is_set():
        value = sampler()
        if value is not None and value > peak_holder[0]:
            peak_holder[0] = value
        stop_event.wait(interval)


def run_one(config_name: str, length: int) -> dict:
    import bench_common

    conn, query = _setup(config_name, length)
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
        "config": config_name, "length": length, "result": result.rows[0][0],
        "intermediate_paths": result.telemetry.intermediate_paths,
        "runtime_ms": result.telemetry.runtime_ms,
        "peak_buffer_memory_mb": result.telemetry.peak_buffer_memory_mb,
        "peak_rss_mb": peak[0],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", choices=list(CONFIGS))
    parser.add_argument("--length", type=int)
    args = parser.parse_args()

    if args.config is not None and args.length is not None:
        row = run_one(args.config, args.length)
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerow(row)
        return

    rows = []
    for length in LENGTHS:
        for config_name in CONFIGS:
            t0 = time.time()
            proc = subprocess.run(
                [sys.executable, __file__, "--config", config_name, "--length", str(length)],
                capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                print(proc.stdout)
                print(proc.stderr, file=sys.stderr)
                raise RuntimeError(f"config={config_name} length={length} subprocess failed "
                                    f"(exit {proc.returncode})")
            lines = proc.stdout.strip().splitlines()
            reader = csv.DictReader(lines)
            row = next(reader)
            row["length"] = int(row["length"])
            row["result"] = int(row["result"])
            row["intermediate_paths"] = int(row["intermediate_paths"])
            row["runtime_ms"] = float(row["runtime_ms"])
            row["peak_buffer_memory_mb"] = float(row["peak_buffer_memory_mb"])
            row["peak_rss_mb"] = float(row["peak_rss_mb"])
            rows.append(row)
            elapsed = time.time() - t0
            print(f"length={length} {config_name}: {row['result']} final / "
                  f"{row['intermediate_paths']} intermediate, "
                  f"runtime={row['runtime_ms']:.2f}ms, rss={row['peak_rss_mb']:.1f}MB "
                  f"({elapsed:.1f}s wall)")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
