"""Shared benchmark harness for the SOA-GDBMS scripts (neo4j_run.py,
kuzu_run.py, memgraph_run.py).

Each engine script only needs to supply: a callable that executes one query
and returns a single scalar (`execute_scalar`), and a small per-engine
`QUERY_REGISTRY` of hand-written Cypher (each entry a pair of query-builder
functions -- see `QueryRegistryEntry`). Everything else -- the CLI, the
warmup/timed-runs loop, memory sampling, and CSV output -- lives here, so
the three scripts stay in sync instead of each re-implementing (and
re-drifting) its own version of the same loop.

Metrics reported per (query, length) mirror `compiler/src/recap_compiler/
execution.py`'s `Telemetry` shape on purpose (`runtime_ms`,
`intermediate_paths`, `peak_buffer_memory_mb` there vs. `runtime_ms`,
`intermediate_paths`, `peak_memory_mb` here) so a results table can line up
ReCAP and SOTA-system rows directly.

**Intermediate paths, uniform across every engine.** None of Neo4j/Kùzu/
Memgraph expose a portable "rows before the final filter" number the way
DuckDB's own profiler does (which is what `execution.py` reads directly).
Instead, each `QUERY_REGISTRY` entry supplies a second, deliberately
*unfiltered* query -- the same variable-length/regex `MATCH` pattern with
no final property/trail predicate -- and its count is reported as
`intermediate_paths`. This is the same idea `execution.py` uses (count the
recursive CTE before the outer filter), generalized to work as two separate
queries since these engines don't expose that count any other way.

**Peak memory, best-effort, never a hard failure.** `--memory-source`
picks how (or whether) to sample memory while a query runs:
  - `psutil`  -- this process's own RSS, polled on a background thread.
                 Correct for an *embedded* engine (Kùzu) since the database
                 lives in the same process as this script.
  - `docker`  -- polls `docker stats --no-stream` for a named container.
                 For a containerized server (Memgraph/Neo4j run via Docker).
  - `local`   -- finds a local (non-Docker) server process by name
                 substring and polls its RSS via psutil.
  - `none`    -- memory is not measured; `peak_memory_mb` is reported as
                 `None` rather than a guessed or crashing value.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

import psutil

# A query-builder is `(starter, min_len, max_len) -> query_text`. The
# candidate-only builder may be `None` for a query where an honest
# "pattern only, no property/trail filter" variant doesn't make sense.
QueryBuilder = Callable[[int, int, int], str]


@dataclass(frozen=True)
class QueryRegistryEntry:
    full: QueryBuilder
    candidate_only: Optional[QueryBuilder] = None


@dataclass
class RunResult:
    engine: str
    query: str
    start: int
    length: int
    success: bool
    result: object = None
    median_ms: Optional[float] = None
    avg_ms: Optional[float] = None
    min_ms: Optional[float] = None
    max_ms: Optional[float] = None
    intermediate_paths: Optional[int] = None
    peak_memory_mb: Optional[float] = None
    error: Optional[str] = None

    def as_csv_row(self) -> dict:
        return {
            "engine": self.engine, "query": self.query, "start": self.start,
            "len": self.length, "success": int(self.success), "result": self.result,
            "median_ms": self.median_ms, "avg_ms": self.avg_ms,
            "min_ms": self.min_ms, "max_ms": self.max_ms,
            "intermediate_paths": self.intermediate_paths,
            "peak_memory_mb": self.peak_memory_mb, "error": self.error,
        }


CSV_FIELDNAMES = [
    "engine", "query", "start", "len", "success", "result", "median_ms",
    "avg_ms", "min_ms", "max_ms", "intermediate_paths", "peak_memory_mb", "error",
]


# ============================================================================
#                          MEMORY SAMPLERS
# ============================================================================

_MEM_UNIT_TO_MB = {"b": 1e-6, "kb": 1e-3, "kib": 1 / 1024, "mb": 1.0,
                    "mib": 1.0, "gb": 1000.0, "gib": 1024.0}


def _parse_mem_string(text: str) -> Optional[float]:
    """Parses a human memory string (`docker stats`'s `'123.4MiB'`) into MB."""
    match = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    if not match:
        return None
    value, unit = match.groups()
    factor = _MEM_UNIT_TO_MB.get(unit.lower())
    return None if factor is None else float(value) * factor


class PsutilSelfSampler:
    """Peak RSS of this process -- correct for an embedded engine (Kùzu),
    since the database lives in the same process as this script."""

    def __init__(self) -> None:
        self._process = psutil.Process(os.getpid())

    def __call__(self) -> Optional[float]:
        try:
            return self._process.memory_info().rss / 1e6
        except psutil.Error:
            return None


class DockerMemorySampler:
    """Peak memory of a named Docker container, via `docker stats`.

    Deliberately shells out to the `docker` CLI on every poll rather than
    reading a cgroup file directly: cgroup v1 vs v2 paths differ across
    docker/kernel versions, and that difference can't be verified without a
    live container to test against (not done this session -- see
    CHECKLIST.md/plan notes) -- `docker stats` is slower per-poll but far
    more portable, and a wrong hand-rolled path would fail silently."""

    def __init__(self, container_name: str) -> None:
        self.container_name = container_name

    def __call__(self) -> Optional[float]:
        try:
            out = subprocess.run(
                ["docker", "stats", self.container_name, "--no-stream", "--format", "{{.MemUsage}}"],
                capture_output=True, text=True, timeout=5,
            )
            if out.returncode != 0:
                return None
            used = out.stdout.split("/")[0].strip()
            return _parse_mem_string(used)
        except (subprocess.SubprocessError, OSError, ValueError):
            return None


class LocalProcessMemorySampler:
    """Peak RSS of a local (non-Docker) server process, found by matching
    `name_substring` against each running process's name."""

    def __init__(self, name_substring: str) -> None:
        self.name_substring = name_substring.lower()

    def _find_process(self):
        for proc in psutil.process_iter(["name"]):
            try:
                if self.name_substring in (proc.info["name"] or "").lower():
                    return proc
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None

    def __call__(self) -> Optional[float]:
        proc = self._find_process()
        if proc is None:
            return None
        try:
            return proc.memory_info().rss / 1e6
        except psutil.Error:
            return None


MemorySampler = Callable[[], Optional[float]]


def make_memory_sampler(source: str, *, container_name: Optional[str] = None,
                         process_name: Optional[str] = None) -> Optional[MemorySampler]:
    if source == "none":
        return None
    if source == "psutil":
        return PsutilSelfSampler()
    if source == "docker":
        if not container_name:
            print("warning: --memory-source docker requires --container; memory will not be measured")
            return None
        return DockerMemorySampler(container_name)
    if source == "local":
        if not process_name:
            print("warning: --memory-source local requires --process-name; memory will not be measured")
            return None
        return LocalProcessMemorySampler(process_name)
    raise ValueError(f"unknown --memory-source {source!r}")


# ============================================================================
#                          TIMING + SWEEP
# ============================================================================

def _poll_peak(sampler: MemorySampler, stop: threading.Event, peak: list, interval_s: float = 0.05) -> None:
    while not stop.is_set():
        value = sampler()
        if value is not None and value > peak[0]:
            peak[0] = value
        stop.wait(interval_s)


def time_query(execute_scalar: Callable[[str], object], query: str, *, warmup: int, runs: int,
               memory_sampler: Optional[MemorySampler] = None):
    """Runs `execute_scalar(query)` `warmup` times (discarded) then `runs`
    times (timed). Returns `(last_result, times_ms, peak_memory_mb)`."""
    for _ in range(warmup):
        execute_scalar(query)

    stop_event = None
    poller = None
    peak = [0.0]
    if memory_sampler is not None:
        stop_event = threading.Event()
        peak_holder = peak
        poller = threading.Thread(target=_poll_peak, args=(memory_sampler, stop_event, peak_holder), daemon=True)
        poller.start()

    result = None
    times_ms = []
    try:
        for _ in range(runs):
            t0 = time.perf_counter()
            result = execute_scalar(query)
            times_ms.append((time.perf_counter() - t0) * 1000)
    finally:
        if stop_event is not None:
            stop_event.set()
            poller.join(timeout=2)

    peak_mb = peak[0] or None
    return result, times_ms, peak_mb


def run_sweep(*, engine: str, query_name: str, execute_scalar: Callable[[str], object],
              entry: QueryRegistryEntry, starter: int, min_len: int, max_len: int,
              warmup: int, runs: int, memory_sampler: Optional[MemorySampler] = None,
              csv_path: Optional[str] = None) -> list:
    """Sweeps `length` from `min_len` to `max_len`, running both the full
    query and (if the registry entry supplies one) the candidate-only query
    at each length. Stops early on a query error or a zero-result length,
    matching the existing scripts' own stop-early convention. Writes a CSV
    row per length if `csv_path` is given."""
    results = []
    for length in range(min_len, max_len + 1):
        print(f"\n[{engine}] {query_name}  start={starter}  len=[{min_len},{length}]")
        try:
            result, times_ms, peak_mb = time_query(
                execute_scalar, entry.full(starter, min_len, length),
                warmup=warmup, runs=runs, memory_sampler=memory_sampler)

            intermediate = None
            if entry.candidate_only is not None:
                intermediate, _, _ = time_query(
                    execute_scalar, entry.candidate_only(starter, min_len, length), warmup=0, runs=1)

            run = RunResult(
                engine=engine, query=query_name, start=starter, length=length, success=True,
                result=result, median_ms=statistics.median(times_ms), avg_ms=statistics.mean(times_ms),
                min_ms=min(times_ms), max_ms=max(times_ms),
                intermediate_paths=intermediate, peak_memory_mb=peak_mb,
            )
            print(f"  result={run.result}  median={run.median_ms:.2f}ms  "
                  f"intermediate_paths={run.intermediate_paths}  peak_mem={run.peak_memory_mb}")
        except Exception as exc:  # noqa: BLE001 -- a query failure/timeout ends the sweep, not the process
            run = RunResult(engine=engine, query=query_name, start=starter, length=length,
                             success=False, error=str(exc))
            print(f"  ERROR: {exc}")
            results.append(run)
            print("  stopping sweep early due to error")
            break

        results.append(run)
        if run.result == 0:
            print("  no results at this length -- stopping sweep early")
            break

    if csv_path:
        write_csv(csv_path, results)
        print(f"\nwrote {len(results)} row(s) to {csv_path}")
    return results


def write_csv(path: str, results: list) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for run in results:
            writer.writerow(run.as_csv_row())


def print_summary(results: list) -> None:
    print("\n" + "=" * 78)
    print("SUMMARY")
    print("=" * 78)
    print(f"{'len':>5}{'result':>12}{'median_ms':>12}{'intermediate':>14}{'peak_mb':>10}")
    for run in results:
        if not run.success:
            print(f"{run.length:>5}{'ERROR':>12}")
            continue
        peak = f"{run.peak_memory_mb:.1f}" if run.peak_memory_mb is not None else "n/a"
        inter = run.intermediate_paths if run.intermediate_paths is not None else "n/a"
        print(f"{run.length:>5}{run.result:>12}{run.median_ms:>12.2f}{inter!s:>14}{peak:>10}")


# ============================================================================
#                          CLI
# ============================================================================

def add_common_args(parser: argparse.ArgumentParser, *, query_choices) -> None:
    parser.add_argument("--nodes", required=True, help="Path to nodes CSV")
    parser.add_argument("--edges", required=True, help="Path to edges CSV")
    parser.add_argument("--query", required=True, choices=sorted(query_choices),
                         help="Which registered query to run")
    parser.add_argument("--starter", type=int, required=True, help="Start vertex id")
    parser.add_argument("--min-len", type=int, default=2)
    parser.add_argument("--max-len", type=int, default=8)
    parser.add_argument("--warmup", type=int, default=1, help="Discarded warmup runs per length")
    parser.add_argument("--runs", type=int, default=3, help="Timed runs per length")
    parser.add_argument("--timeout", type=int, default=7200, help="Per-query timeout, in seconds")
    parser.add_argument("--memory-source", choices=["psutil", "docker", "local", "none"], default="none",
                         help="How to sample peak memory while a query runs (default: don't measure)")
    parser.add_argument("--container", default=None, help="Docker container name (--memory-source docker)")
    parser.add_argument("--process-name", default=None, help="Process name substring (--memory-source local)")
    parser.add_argument("--fresh-db", action="store_true", help="Clear/reload the database before running")
    parser.add_argument("--csv", default=None, help="Write per-length results to this CSV")
