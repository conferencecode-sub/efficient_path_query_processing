"""Memgraph side of the sum-of-weights scalability sweep over Datagen-7.7.
Same competitor (bolt port 7688, same `memgraph`/`memgraph-mage` container
`experiments/e6_finbench_sf10/run_memgraph.py` already uses) and the same
"increase length until it fails, document and stop" discipline as that
project's own existing GDBMS sweeps.

**Why Memgraph, not Neo4j or Kuzu (checked, not assumed):** Kuzu is ruled
out first -- it is consistently the weakest/riskiest engine in this
project's own real-data results (`experiments/e6_finbench_sf10/results/
kuzu_tcr{1,5,8}.csv`: OOMs earliest of the three engines at every scale
factor tested, e.g. TCR1 fails at length 7 while Neo4j/Memgraph both reach
length 8; `kuzu_slowness_explanation.md` documents a real, confirmed root
cause -- Kuzu's own recursive-join plan does a full, unfiltered
`Node`-table scan per query regardless of query selectivity). Between
Neo4j and Memgraph: `experiments/latex_template/figures.tex`'s own
`fig:performance_grid` (the closest precedent to *this* query family --
Q3/Q4 are both bounded-aggregate trail queries, same shape as this
experiment's own sum-of-weights query) shows Memgraph as the faster
competitor at the largest length reached on both real datasets plotted
there: Q3-Reddit at l=5 (2,977,664.7ms vs. Neo4j's 4,207,017.6ms) and
Q4-LDBC100 at l=5 (881,075.3ms vs. Neo4j's 928,776.4ms -- the case the
figure's own caption calls out as "the best/fastest competitor there").
FinBench SF10's own TCR1/5/8 results are more mixed (Memgraph faster on
TCR1, Neo4j faster on TCR5/TCR8), but the Q3/Q4-family precedent -- the
same query shape as this experiment -- is the more directly relevant
signal, so Memgraph is the pick here.

Reference counts are read from `results/recap_sum_weights.csv` (written by
`run_recap_sum_weights.py`, which already verified each of those counts
against `reference_sum_weights.py`'s independent hand-written DuckDB
query) -- same "read a pre-verified reference from CSV" convention as
`experiments/e6_finbench_sf10/run_memgraph.py`.

Each length is run in `query_memgraph_once.py`'s own subprocess with a
hard 1800s (30 minute) timeout; Memgraph's own process memory is polled
throughout via `docker stats` (`bench_common.DockerMemorySampler`) -- a
best-effort, non-blocking safety signal on top of the timeout itself,
since this is a shared machine.
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "SOA-GDBMS"))
from bench_common import DockerMemorySampler  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(_HERE, "results")
RECAP_CSV = os.path.join(RESULTS_DIR, "recap_sum_weights.csv")
CSV_PATH = os.path.join(RESULTS_DIR, "memgraph_sum_weights.csv")
CONTAINER = "memgraph"
PER_LENGTH_TIMEOUT_S = 1800  # 30 minutes -- "document and stop" if exceeded

CSV_FIELDNAMES = ["length", "result", "reference_result", "match", "runtime_ms", "peak_container_mem_mb", "error"]


def _load_reference() -> dict[int, int]:
    with open(RECAP_CSV) as fh:
        return {int(r["length"]): int(r["result"]) for r in csv.DictReader(fh) if r["result"] != ""}


def _poll_peak(sampler, stop_event, peak_holder, interval_s: float = 1.0) -> None:
    while not stop_event.is_set():
        value = sampler()
        if value is not None and value > peak_holder[0]:
            peak_holder[0] = value
        stop_event.wait(interval_s)


def run_one(length: int) -> dict:
    sampler = DockerMemorySampler(CONTAINER)
    peak = [sampler() or 0.0]
    stop_event = threading.Event()
    poller = threading.Thread(target=_poll_peak, args=(sampler, stop_event, peak), daemon=True)
    poller.start()
    try:
        proc = subprocess.run(
            [sys.executable, os.path.join(_HERE, "query_memgraph_once.py"), str(length)],
            capture_output=True, text=True, timeout=PER_LENGTH_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        stop_event.set()
        poller.join(timeout=2)
        return {"length": length, "result": "", "match": False,
                "runtime_ms": "", "peak_container_mem_mb": peak[0],
                "error": f"timeout after {PER_LENGTH_TIMEOUT_S}s"}
    finally:
        stop_event.set()
        poller.join(timeout=2)

    if proc.returncode != 0:
        return {"length": length, "result": "", "match": False,
                "runtime_ms": "", "peak_container_mem_mb": peak[0],
                "error": (proc.stderr or proc.stdout).strip()[-2000:]}

    cnt_str, wall_str = proc.stdout.strip().split(",")
    return {"length": length, "result": int(cnt_str), "runtime_ms": float(wall_str),
            "peak_container_mem_mb": peak[0], "error": ""}


def main() -> None:
    reference = _load_reference()
    rows = []
    for length in sorted(reference):
        row = run_one(length)
        row["reference_result"] = reference[length]
        if row["result"] != "":
            row["match"] = row["result"] == reference[length]
        row.setdefault("match", False)
        rows.append(row)

        if row["error"]:
            print(f"[memgraph] length={length}: ERROR/TIMEOUT: {row['error'][:300]}", flush=True)
            print(f"  stopping sweep -- document and stop at length={length}")
            break
        print(f"[memgraph] length={length}: result={row['result']} reference={row['reference_result']} "
              f"match={row['match']} runtime={row['runtime_ms']:.1f}ms "
              f"peak_container_mem={row['peak_container_mem_mb']:.1f}MB", flush=True)
        if not row["match"]:
            print(f"  MISMATCH at length={length} -- stopping sweep for investigation")
            break

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
