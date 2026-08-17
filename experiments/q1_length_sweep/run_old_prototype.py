"""Sweeps Q1 over the old prototype's three ReCAP/q1 variants -- DuckDB
baseline, ReCAP-inline, ReCAP-UDF -- at max_length in {2,3,4} (min_length=2
fixed, starter_node=383, matching every other Q1 experiment in this repo).

These scripts don't measure memory on their own, so this reuses
`experiments/SOA-GDBMS/bench_common.py`'s `PsutilSelfSampler`/`time_query`
(this process's own peak RSS while the query runs -- correct here since
DuckDB is embedded in this same process, exactly like the Kùzu runner).

**Isolation matters here, unlike the new-compiler runner's per-length
reconnect:** all three engines load the *same* ~78k-edge dataset into their
own DuckDB connection, and Python doesn't drop the previous engine's memory
just because the loop moved on. Running all three in one process would make
the second and third engines' peak-RSS numbers cumulative (include the
earlier engines' resident memory too), not their own. So `--engine all`
(the default) launches one fresh subprocess per engine via `--engine
<name>` rather than looping in-process -- each subprocess only ever loads
one engine's data, so its peak RSS is genuinely that engine's own.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ReCAP", "q1"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "SOA-GDBMS"))

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "ReCAP", "simple_dataset")
NODES = os.path.join(DATASET_DIR, "LG_V.csv")
EDGES = os.path.join(DATASET_DIR, "LG.csv")
NFA_NODES = os.path.join(DATASET_DIR, "nfa_nodes.csv")
NFA_EDGES = os.path.join(DATASET_DIR, "nfa.csv")

STARTER_NODE = 383
MIN_LENGTH = 2
LENGTHS = (2, 3, 4)
WARMUP, RUNS = 1, 3
CSV_PATH = os.path.join(os.path.dirname(__file__), "results", "old_prototype_q1.csv")
CSV_FIELDNAMES = ["engine", "length", "result", "median_ms", "peak_memory_mb"]

ENGINES = ("duckdb-baseline", "recap-inline", "recap-udf")


def _class_and_method(engine: str):
    if engine == "duckdb-baseline":
        from duckdb_gen_recap import DuckDBGeneralReCAPQuery
        return DuckDBGeneralReCAPQuery, "run_gen_recap_query"
    if engine == "recap-inline":
        from recap_gen_recap_inline import InlineReCAPQuery
        return InlineReCAPQuery, "run_gen_recap_inline"
    if engine == "recap-udf":
        from recap_gen_recap_UDF import UDFReCAPQuery
        return UDFReCAPQuery, "run_gen_recap_inline"
    raise ValueError(f"unknown engine {engine!r}")


def run_one_engine(engine: str) -> list[dict]:
    import bench_common

    cls, method_name = _class_and_method(engine)
    instance = cls()
    instance.load_data(NODES, EDGES, NFA_NODES, NFA_EDGES, True)
    run_method = getattr(instance, method_name)

    rows = []
    for length in LENGTHS:
        sampler = bench_common.PsutilSelfSampler()
        execute_scalar = lambda _q, _m=run_method, _l=length: _m(MIN_LENGTH, _l, STARTER_NODE)[0]
        result, times_ms, peak_mb = bench_common.time_query(
            execute_scalar, query=length, warmup=WARMUP, runs=RUNS, memory_sampler=sampler)
        median_ms = sorted(times_ms)[len(times_ms) // 2]
        print(f"  length=[{MIN_LENGTH},{length}]: {result} paths, "
              f"median={median_ms:.2f}ms, peak_rss={peak_mb:.1f}MB")
        rows.append({"engine": engine, "length": length, "result": result,
                     "median_ms": median_ms, "peak_memory_mb": peak_mb})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=ENGINES + ("all",), default="all")
    args = parser.parse_args()

    if args.engine != "all":
        rows = run_one_engine(args.engine)
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
        return

    all_rows: list[dict] = []
    for engine in ENGINES:
        print(f"\n{'=' * 60}\n{engine} (subprocess, isolated memory)\n{'=' * 60}")
        proc = subprocess.run(
            [sys.executable, __file__, "--engine", engine],
            capture_output=True, text=True, check=False)
        print(proc.stderr, end="")
        if proc.returncode != 0:
            print(proc.stdout)
            raise RuntimeError(f"{engine} subprocess failed (exit {proc.returncode})")
        lines = proc.stdout.strip().splitlines()
        header_idx = next(i for i, line in enumerate(lines) if line.startswith("engine,"))
        reader = csv.DictReader(lines[header_idx:])
        for row in reader:
            row["length"] = int(row["length"])
            row["result"] = int(row["result"])
            row["median_ms"] = float(row["median_ms"])
            row["peak_memory_mb"] = float(row["peak_memory_mb"])
            all_rows.append(row)
            print(f"  length=[{MIN_LENGTH},{row['length']}]: {row['result']} paths, "
                  f"median={row['median_ms']:.2f}ms, peak_rss={row['peak_memory_mb']:.1f}MB")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"\nwrote {len(all_rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
