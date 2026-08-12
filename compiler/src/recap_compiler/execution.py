"""Stage G: execution and results (FR-24..FR-26).

Runs a Stage E `StandardQuery` on DuckDB and shapes the output as full
paths, reached endpoints, or a count (FR-24) -- all three enumerate the same
viable paths internally (the same underlying query), so timing is
independent of output shape, per the paper's methodology. Telemetry (FR-26):
wall-clock time, the number of rows the recursive CTE actually produced
before the outer filter (the "intermediate paths explored" figure the paper
reports), and peak DuckDB buffer memory (2026-08-11, per user request).

**Peak memory caveat, stated plainly rather than glossed over:** DuckDB's
own query profiler (`PRAGMA enable_profiling='json'`) reports
`system_peak_buffer_memory` as a high-water mark for the whole connection's
lifetime, not reset per query -- verified empirically (running a small
query after a large one on the same connection still reports the large
one's peak). So this number is accurate for the common case of one fresh
`duckdb.connect()` per measured run (true of `demo_pipeline.py` and of the
webapp's own "Compile & run" click), but if two queries share one
connection (the webapp's FR-22 standard-vs-optimized comparison does this),
the second one's reported peak includes the first's memory too, not just
its own. No isolation trick (e.g. reconnecting between queries) is applied
here, since that would mean reloading the whole graph a second time --
not worth it for a diagnostic number.
"""
from __future__ import annotations

import json
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .errors import ExecutionError
from .optimizer import OptimizedQuery
from .standard_sql import StandardQuery

RESULT_SHAPES = {"paths", "endpoints", "count"}


@dataclass(frozen=True)
class Telemetry:
    intermediate_paths: int
    runtime_ms: float
    peak_buffer_memory_mb: float


@dataclass(frozen=True)
class QueryResult:
    rows: list[tuple]
    columns: list[str]
    telemetry: Telemetry
    sql: str  # FR-25: the generated SQL, exposed as an inspectable artifact


def run_query(conn: duckdb.DuckDBPyConnection, query: StandardQuery | OptimizedQuery, *,
              result_shape: str = "paths") -> QueryResult:
    """FR-24: executes `query.sql` and returns results in the requested
    shape. FR-25/FR-26: the SQL text and basic telemetry ride along on every
    call, not just on request, since they're cheap to keep. Accepts either
    Stage E's `StandardQuery` or Stage F's `OptimizedQuery` -- both expose
    the same `.sql`/`.cte` shape."""
    if result_shape not in RESULT_SHAPES:
        raise ExecutionError(
            f"unknown result_shape '{result_shape}', expected one of {sorted(RESULT_SHAPES)}")

    if result_shape == "count":
        wrapped = f"SELECT count(*) AS path_count FROM ({query.sql}) t"
    elif result_shape == "endpoints":
        wrapped = f"SELECT DISTINCT v AS endpoint FROM ({query.sql}) t"
    else:
        wrapped = query.sql

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as profile_file:
        profile_path = Path(profile_file.name)
    conn.execute("PRAGMA enable_profiling='json'")
    conn.execute(f"PRAGMA profiling_output='{profile_path.as_posix()}'")

    start = time.perf_counter()
    try:
        try:
            cursor = conn.execute(wrapped)
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            # Every row the recursive member ever produced, before the outer
            # filter -- not recoverable from `query.sql` alone since that's
            # already the post-filter query. Run while profiling is still
            # on, so the reported peak accounts for this query too, not
            # just the first one above.
            intermediate_paths = conn.execute(
                f"WITH RECURSIVE {query.cte} SELECT count(*) FROM paths").fetchone()[0]
        except duckdb.Error as exc:
            raise ExecutionError(f"DuckDB failed to run the generated query: {exc}") from exc
        runtime_ms = (time.perf_counter() - start) * 1000
        peak_buffer_memory_mb = json.loads(profile_path.read_text())["system_peak_buffer_memory"] / 1e6
    finally:
        # Always disable profiling and clean up the temp file, whether the
        # query above succeeded or raised -- otherwise a failed run leaves
        # this connection still pointed at a now-deleted profiling_output
        # path, breaking whatever query runs on it next.
        conn.execute("PRAGMA disable_profiling")
        profile_path.unlink(missing_ok=True)

    return QueryResult(
        rows=rows, columns=columns,
        telemetry=Telemetry(intermediate_paths=intermediate_paths, runtime_ms=runtime_ms,
                             peak_buffer_memory_mb=peak_buffer_memory_mb),
        sql=query.sql,
    )
