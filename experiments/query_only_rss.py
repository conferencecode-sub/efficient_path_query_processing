"""Query-only RSS: reuses each qN_length_sweep/run_new_compiler.py's own
`_setup`/`PsutilSelfSampler`/`_poll_peak_rss` verbatim (same dataset, same
methodology that produced e1_real_data_results.md's own peak_rss_mb), but
additionally reports the RSS baseline captured right after `_setup()`
(load + macro registration + materialize_transitions, no query run yet)
so `query_only_rss_mb = peak_rss_mb - baseline_rss_mb` can be computed --
per explicit user request, to update tab:recap_real_data with a memory
figure that excludes the one-time dataset-load cost.

One subprocess per (query, length, variant), matching the existing
scripts' own already-established isolation methodology exactly (each
_setup() call needs its own fresh process so peak RSS isn't contaminated
by a previous variant's own resident memory on the same process).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "compiler", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "SOA-GDBMS"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True, choices=["q1", "q2", "q3", "q4"])
    parser.add_argument("--length", type=int, required=True)
    parser.add_argument("--variant", required=True, choices=["standard", "optimized"])
    args = parser.parse_args()

    query_dir = os.path.join(os.path.dirname(__file__), f"{args.query}_length_sweep")
    sys.path.insert(0, query_dir)
    import bench_common
    from recap_compiler.execution import run_query

    import run_new_compiler as mod  # picks up query_dir's own module -- fresh subprocess per call
    conn, standard_query, optimized_query = mod._setup(args.length, macro_mode="sql")
    query = standard_query if args.variant == "standard" else optimized_query

    sampler = bench_common.PsutilSelfSampler()
    baseline_rss = sampler() or 0.0  # right after _setup() -- load done, no query run yet
    peak = [baseline_rss]
    stop_event = threading.Event()
    poller = threading.Thread(target=mod._poll_peak_rss, args=(sampler, stop_event, peak), daemon=True)
    poller.start()
    try:
        result = run_query(conn, query, result_shape="count")
    finally:
        stop_event.set()
        poller.join(timeout=2)
    conn.close()

    print(json.dumps({
        "query": args.query, "length": args.length, "variant": args.variant,
        "result": result.rows[0][0], "runtime_ms": result.telemetry.runtime_ms,
        "baseline_rss_mb": baseline_rss, "peak_rss_mb": peak[0],
        "query_only_rss_mb": peak[0] - baseline_rss,
    }))


if __name__ == "__main__":
    main()
