"""Stage G: execution and results (FR-24..FR-26).

Runs a Stage E `StandardQuery` on DuckDB and shapes the output as full
paths, reached endpoints, or a count (FR-24) -- all three enumerate the same
viable paths internally (the same underlying query), so timing is
independent of output shape, per the paper's methodology. Telemetry (FR-26)
is kept minimal for this first cut: wall-clock time and the number of rows
the recursive CTE actually produced before the outer filter, which is the
"intermediate paths explored" figure the paper reports. Peak memory isn't
measured yet (would need `EXPLAIN ANALYZE` output; left for later).
"""
from __future__ import annotations

import time
from dataclasses import dataclass

import duckdb

from .errors import ExecutionError
from .optimizer import OptimizedQuery
from .standard_sql import StandardQuery

RESULT_SHAPES = {"paths", "endpoints", "count"}


@dataclass(frozen=True)
class Telemetry:
    intermediate_paths: int
    runtime_ms: float


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

    start = time.perf_counter()
    try:
        cursor = conn.execute(wrapped)
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        # Every row the recursive member ever produced, before the outer
        # filter -- not recoverable from `query.sql` alone since that's
        # already the post-filter query.
        intermediate_paths = conn.execute(
            f"WITH RECURSIVE {query.cte} SELECT count(*) FROM paths").fetchone()[0]
    except duckdb.Error as exc:
        raise ExecutionError(f"DuckDB failed to run the generated query: {exc}") from exc
    runtime_ms = (time.perf_counter() - start) * 1000

    return QueryResult(
        rows=rows, columns=columns,
        telemetry=Telemetry(intermediate_paths=intermediate_paths, runtime_ms=runtime_ms),
        sql=query.sql,
    )
