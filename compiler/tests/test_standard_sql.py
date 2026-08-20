import duckdb
import pytest

from recap_compiler.errors import ExecutionError
from recap_compiler.ingestion import set_trivial_label_column
from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate, bounded_range
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import TransitionsRelation, trivial_relation

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
    """The anchor is well-formed for every seed, not just one -- guards
    against an unbound `s` in the base case."""
    aggregate = SelectiveAggregate(
        dictionary_keys=(), init_d="NULL", update_d="D", is_viable_d="TRUE",
        is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    query = _run(conn, aggregate, start_vertices=[1, 4], length_bound=0)
    rows = conn.execute(query.sql).fetchall()
    starts_seen = {row[0] for row in rows}
    assert starts_seen == {1, 4}  # both seeds present, length_bound=0 means no hops taken (path_length starts at 0)


def test_anchor_handles_vertex_ids_beyond_int32_range():
    """A small start-vertex literal (fits INT32) must not narrow the
    recursive CTE's `v` column to INTEGER when the graph's real vertex ids
    need BIGINT -- found on Datagen-7.7 (LDBC-style ids up to ~13 trillion)
    via a low-degree start vertex that happened to be small itself. DuckDB
    infers a VALUES clause's column type from the literal, so an unqualified
    `(3184)` anchor would type `v` as INTEGER and then fail to hold a
    recursively-reached BIGINT destination id."""
    conn = duckdb.connect()
    conn.execute("CREATE TABLE edges(edge_id BIGINT, src BIGINT, dst BIGINT, label TEXT)")
    huge_id = 13194146057717  # exceeds INT32's ~2.1 billion max
    conn.execute("INSERT INTO edges VALUES (1, 3184, ?, 'purchase')", [huge_id])

    aggregate = SelectiveAggregate(
        dictionary_keys=(), init_d="NULL", update_d="D", is_viable_d="TRUE",
        is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    query = _run(conn, aggregate, start_vertices=[3184], length_bound=1)
    rows = conn.execute(query.sql).fetchall()
    reached = {row[0] for row in rows}
    assert huge_id in reached


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


# --- a "no regex" query still goes through the same automaton-shaped SQL,
# just over transitions.trivial_relation()'s single self-looping state ------

def test_trivial_relation_explores_every_edge_regardless_of_its_original_label(conn):
    aggregate = SelectiveAggregate(
        dictionary_keys=(), init_d="NULL", update_d="D", is_viable_d="TRUE",
        is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    set_trivial_label_column(conn)  # overwrites the fixture's real 'purchase'/'other' labels
    relation = trivial_relation()
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    query = build_standard_query(relation=relation, start_vertices=[1], length_bound=3)
    # the fixture's vertex-4 edge has amount 999 and a real bounded_range
    # aggregate would prune it -- this aggregate never checks amount at all,
    # so every edge (regardless of its original label) is followed up to
    # the length bound, same as if there were no regex.
    reached = {row[0] for row in conn.execute(query.sql).fetchall()}
    assert reached == {1, 2, 3, 4}


def test_trivial_relation_query_uses_the_same_join_shape_as_a_real_regex(conn):
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    set_trivial_label_column(conn)
    relation = trivial_relation()
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    query = build_standard_query(relation=relation, start_vertices=[1], length_bound=3)
    assert "transitions" in query.sql  # not a special-cased no-automaton query
    assert query.transitions_table == "transitions"
    reached = {row[0] for row in conn.execute(query.sql).fetchall()}
    assert 4 not in reached  # amount 999 still pruned by the aggregate itself
