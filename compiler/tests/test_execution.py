import duckdb
import pytest

from recap_compiler.errors import ExecutionError
from recap_compiler.execution import run_query
from recap_compiler.selective_aggregate import bounded_range
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import TransitionsRelation

LOOP_RELATION = TransitionsRelation(rows=((0, 0, "purchase"),), q0=0, accepting_states=frozenset({0}))


@pytest.fixture
def query_and_conn():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE edges(edge_id INT, src INT, dst INT, label TEXT, amount DOUBLE)")
    conn.execute(
        "INSERT INTO edges VALUES "
        "(1, 1, 2, 'purchase', 10.0), "
        "(2, 2, 3, 'purchase', 20.0), "
        "(3, 3, 4, 'purchase', 999.0)"
    )
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, LOOP_RELATION)
    query = build_standard_query(relation=LOOP_RELATION, start_vertices=[1], length_bound=4)
    return query, conn


def test_paths_shape_returns_full_rows_with_finalize_d_result(query_and_conn):
    query, conn = query_and_conn
    result = run_query(conn, query, result_shape="paths")
    assert result.columns == ["v", "q", "D", "path_length", "result"]
    assert {row[0] for row in result.rows} == {1, 2, 3}
    assert result.sql == query.sql  # the exact SQL that ran is exposed


def test_endpoints_shape_deduplicates_reached_vertices(query_and_conn):
    query, conn = query_and_conn
    result = run_query(conn, query, result_shape="endpoints")
    assert result.columns == ["endpoint"]
    assert sorted(row[0] for row in result.rows) == [1, 2, 3]


def test_count_shape_matches_paths_row_count(query_and_conn):
    query, conn = query_and_conn
    paths = run_query(conn, query, result_shape="paths")
    count = run_query(conn, query, result_shape="count")
    assert count.rows == [(len(paths.rows),)]


def test_telemetry_reports_positive_runtime():
    query, conn = _two_state_query()
    result = run_query(conn, query, result_shape="paths")
    assert result.telemetry.runtime_ms >= 0


def test_intermediate_paths_counts_rows_the_outer_filter_later_drops():
    # A non-accepting anchor state (q0=0, Q_F={1}) means the anchor row
    # itself is a real intermediate path that never appears in the final,
    # outer-filtered result -- this is exactly what the intermediate-paths
    # telemetry is supposed to surface, and it isn't recoverable by just counting the
    # final result set.
    query, conn = _two_state_query()
    result = run_query(conn, query, result_shape="paths")
    assert result.telemetry.intermediate_paths > len(result.rows)


def _two_state_query():
    relation = TransitionsRelation(
        rows=((0, 1, "purchase"), (1, 1, "purchase")), q0=0, accepting_states=frozenset({1}))
    conn = duckdb.connect()
    conn.execute("CREATE TABLE edges(edge_id INT, src INT, dst INT, label TEXT, amount DOUBLE)")
    conn.execute(
        "INSERT INTO edges VALUES "
        "(1, 1, 2, 'purchase', 10.0), "
        "(2, 2, 3, 'purchase', 20.0), "
        "(3, 3, 4, 'purchase', 999.0)"
    )
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    query = build_standard_query(relation=relation, start_vertices=[1], length_bound=4)
    return query, conn


def test_unknown_result_shape_raises_execution_error(query_and_conn):
    query, conn = query_and_conn
    with pytest.raises(ExecutionError, match="unknown result_shape"):
        run_query(conn, query, result_shape="bogus")


# --- peak_buffer_memory_mb (2026-08-11, per user request) ------------------

def test_telemetry_reports_positive_peak_memory(query_and_conn):
    query, conn = query_and_conn
    result = run_query(conn, query, result_shape="paths")
    assert result.telemetry.peak_buffer_memory_mb > 0


def test_a_failed_query_still_leaves_the_connection_usable(query_and_conn):
    # A real risk with toggling PRAGMA enable_profiling/profiling_output
    # around the query: if a failure skips disabling it, the connection is
    # left pointed at a (now-deleted) profiling_output path, and the *next*
    # query on it -- even an unrelated one -- fails too. Build a query with
    # a `.cte` that's valid SQL (so `wrapped` fails cleanly, not via a
    # Python-level exception before profiling even starts) but references a
    # nonexistent table, forcing the DuckDB-level failure path.
    query, conn = query_and_conn
    broken = type(query)(sql="SELECT * FROM does_not_exist", cte=query.cte,
                          transitions_table=query.transitions_table)
    with pytest.raises(ExecutionError):
        run_query(conn, broken, result_shape="paths")

    # The connection itself must still work afterward -- profiling state
    # from the failed call must not have leaked.
    result = run_query(conn, query, result_shape="paths")
    assert result.telemetry.peak_buffer_memory_mb > 0
