import duckdb
import pytest

from recap_compiler.errors import ExecutionError
from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate, bounded_range
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import TransitionsRelation

# A single-state loop NFA accepting any nonempty sequence of "purchase" edges
# -- the simplest possible non-trivial regex, T = {(0,0,purchase)}, q0=0,
# Q_F={0} (matches after at least one hop, but the outer filter doesn't
# distinguish "not yet moved" from "moved" -- both have q=0).
LOOP_RELATION = TransitionsRelation(rows=((0, 0, "purchase"),), q0=0, accepting_states=frozenset({0}))


@pytest.fixture
def conn():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE edges(edge_id INT, src INT, dst INT, label TEXT, amount DOUBLE)")
    connection.execute(
        "INSERT INTO edges VALUES "
        "(1, 1, 2, 'purchase', 10.0), "
        "(2, 2, 3, 'purchase', 20.0), "
        "(3, 3, 4, 'purchase', 999.0), "  # far outside any reasonable bounded_range -> pruned
        "(4, 4, 5, 'other', 5.0)"          # wrong label, never taken
    )
    return connection


def _run(conn, aggregate, *, start_vertices, length_bound):
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, LOOP_RELATION)
    query = build_standard_query(relation=LOOP_RELATION, start_vertices=start_vertices,
                                  length_bound=length_bound)
    return query


def test_anchor_seeds_every_start_vertex_with_no_undefined_columns(conn):
    """FR-16: the anchor is well-formed for every seed, not just one -- this
    is the defect class R4.O3 flagged (an unbound `s` in the base case)."""
    aggregate = SelectiveAggregate(
        dictionary_keys=(), init_d="NULL", update_d="D", is_viable_d="TRUE",
        is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    query = _run(conn, aggregate, start_vertices=[1, 4], length_bound=0)
    rows = conn.execute(query.sql).fetchall()
    starts_seen = {row[0] for row in rows}
    assert starts_seen == {1, 4}  # both seeds present, length_bound=0 means no hops taken (path_length starts at 0)


def test_bounded_range_aggregate_prunes_the_out_of_range_edge(conn):
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    query = _run(conn, aggregate, start_vertices=[1], length_bound=3)
    rows = conn.execute(query.sql).fetchall()
    reached = {row[0] for row in rows}
    # 1->2 (amount 10) stays within [max-min<=15]; 2->3 (amount 20) would
    # push the range to 20-10=10, still viable; vertex 4 requires amount 999
    # which blows the range past 15 and must be pruned.
    assert 4 not in reached
    assert {1, 2, 3} <= reached


def test_non_factorized_aggregate_generates_case_and_runs():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE edges(edge_id INT, src INT, dst INT, label TEXT, amount DOUBLE)")
    conn.execute("INSERT INTO edges VALUES (1, 1, 2, 'purchase', 10.0), (2, 2, 3, 'purchase', 999.0)")
    aggregate = SelectiveAggregate(
        dictionary_keys=(),
        init_d="NULL",
        update_d={(0, 0): "D"},
        is_viable_d={(0, 0): "e.amount <= 15.0"},
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=False,
    )
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, LOOP_RELATION)
    macro_def = conn.execute(
        "SELECT macro_definition FROM duckdb_functions() WHERE function_name='is_viable_d'"
    ).fetchone()[0]
    assert "CASE" in macro_def  # the CASE lives inside the macro, not the outer query text

    query = build_standard_query(relation=LOOP_RELATION, start_vertices=[1], length_bound=3)
    reached = {row[0] for row in conn.execute(query.sql).fetchall()}
    assert reached == {1, 2}  # the amount=999 hop to vertex 3 is pruned


def test_empty_start_vertices_raises_execution_error():
    with pytest.raises(ExecutionError, match="no start vertices"):
        build_standard_query(relation=LOOP_RELATION, start_vertices=[], length_bound=3)


def test_no_accepting_states_raises_execution_error():
    empty_accept = TransitionsRelation(rows=((0, 0, "purchase"),), q0=0, accepting_states=frozenset())
    with pytest.raises(ExecutionError, match="no accepting states"):
        build_standard_query(relation=empty_accept, start_vertices=[1], length_bound=3)
