"""E4-reduced: same 3-config isolation methodology as `../run_e4.py`
(automata/regex-exploration cost vs. early-property-filtering benefit on
Q1, real Metaverse dataset, start vertex 383, length_bound in {2,3,4}),
but config 3's property constraint is the *milder* 4-check reduced
aggregate from `q1_reduced_early_aggregate.py` instead of the full
6-check aggregate in `q1_length_sweep/q1_aggregate_general.py`.

Motivation: the full aggregate is deliberately brutal (result/regex-only
~= 1.6% at ell=2), so E4's original run can't show how the
exploration-cost-vs-filtering-benefit picture looks when early filtering
is only moderately selective. This variant's config 3 was tuned (see
`q1_reduced_early_aggregate.py`'s docstring for the empirical sweep
against the real dataset) to land near 50% selectivity at ell=2.

1. **regex-only** -- `q1_regex_only_aggregate` from `../` (`e4_isolation/`
   directly), completely unchanged, imported without copying: this
   config has no property constraint at all, so there is nothing to
   reduce.
2. **regex + late (reduced) property check** -- `q1_reduced_late_aggregate`
   (this directory): same 4 reduced checks as config 3, all deferred to
   `is_viable_d_final`.
3. **regex + early (reduced) property filtering** --
   `q1_reduced_early_aggregate` (this directory): General/non-factorized,
   same 4 reduced checks pushed as early as each is decidable.

Does NOT modify or overwrite anything under `experiments/e4_isolation/`
itself -- `../run_e4.py` and `../results/e4_isolation.csv` back the
paper's already-reviewed E4 section and are untouched. This script's
own output goes to `results/e4_isolation_reduced.csv` in this directory.

Verifies configs 2 and 3 return the same final `result` at every length
before trusting any timing number (they must, by construction -- same
checks, just different is_viable_d/is_viable_d_final placement, an
FR-22-style semantics-preserving equivalence) and raises loudly if they
ever disagree, rather than silently reporting a possibly-buggy number.
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

_HERE = os.path.dirname(__file__)
_E4_DIR = os.path.join(_HERE, "..")
sys.path.insert(0, os.path.join(_E4_DIR, "..", "..", "compiler", "src"))
sys.path.insert(0, _HERE)
sys.path.insert(0, _E4_DIR)
sys.path.insert(0, os.path.join(_E4_DIR, "..", "SOA-GDBMS"))

from q1_regex_only_aggregate import q1_regex_only_aggregate  # noqa: E402
from q1_reduced_late_aggregate import q1_reduced_late_aggregate  # noqa: E402
from q1_reduced_early_aggregate import q1_reduced_early_aggregate  # noqa: E402

from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.regex_frontend import compile_regex_to_nfa  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions, register_aggregate_macros  # noqa: E402
from recap_compiler.transitions import build_transitions_relation  # noqa: E402

DATASET = os.path.join(_E4_DIR, "..", "datasets", "metaverse", "edges.csv")
REGEX = "(transfer|purchase|sale)+(phishing|scam)+"
START_VERTEX = 383
LENGTHS = (2, 3, 4)  # same lengths as the original E4 run, for direct comparability
# Floor used for "intermediate_paths" per the task's own convention (matches
# tab:e4_isolation/Q4 figures elsewhere in this project): raw
# telemetry.intermediate_paths sums path lengths 0..length_bound, including
# trivial short prefixes that Q1's NFA can't possibly accept from (it needs
# >= 2 hops to reach its accepting state) -- so intermediate_paths here is
# instead computed directly as count(*) FROM paths WHERE path_length >= 2
# against the query's own recursive CTE, not taken from telemetry.
INTERMEDIATE_FLOOR_LENGTH = 2

CONFIGS = {
    "1-regex-only": q1_regex_only_aggregate,
    "2-regex-late-property-reduced": q1_reduced_late_aggregate,
    "3-regex-early-property-reduced": q1_reduced_early_aggregate,
}
CSV_PATH = os.path.join(_HERE, "results", "e4_isolation_reduced.csv")

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
        floored_intermediate = conn.execute(
            f"WITH RECURSIVE {query.cte} "
            f"SELECT count(*) FROM paths WHERE path_length >= {INTERMEDIATE_FLOOR_LENGTH}"
        ).fetchone()[0]
    finally:
        stop_event.set()
        poller.join(timeout=2)
    conn.close()

    return {
        "config": config_name, "length": length, "result": result.rows[0][0],
        "intermediate_paths": floored_intermediate,
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
    by_length_config = {}
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
            by_length_config[(length, config_name)] = row["result"]
            elapsed = time.time() - t0
            print(f"length={length} {config_name}: {row['result']} final / "
                  f"{row['intermediate_paths']} intermediate (>= {INTERMEDIATE_FLOOR_LENGTH} hops), "
                  f"runtime={row['runtime_ms']:.2f}ms, rss={row['peak_rss_mb']:.1f}MB "
                  f"({elapsed:.1f}s wall)")

    # FR-22-style semantics-preserving equivalence check: configs 2 and 3
    # implement the *same* reduced constraint set, just early vs. late
    # placement -- their final result must match at every length, or one
    # of the two aggregates has a bug, not just a timing difference worth
    # reporting.
    mismatches = []
    for length in LENGTHS:
        late = by_length_config[(length, "2-regex-late-property-reduced")]
        early = by_length_config[(length, "3-regex-early-property-reduced")]
        if late != early:
            mismatches.append((length, late, early))
    if mismatches:
        for length, late, early in mismatches:
            print(f"MISMATCH at length={length}: late-property result={late} "
                  f"!= early-property result={early}", file=sys.stderr)
        raise RuntimeError(
            "config 2 (late) and config 3 (early) reduced aggregates disagree on final "
            "result -- this must not happen for semantics-preserving early/late "
            "placement of the same checks; investigate before trusting any timing "
            f"numbers. Mismatches: {mismatches}")
    print("\nconfirmed: configs 2 and 3 agree on final result at every length "
          "(semantics-preserving early/late placement verified).")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
