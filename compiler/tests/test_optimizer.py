import duckdb
import pytest

from recap_compiler.errors import ExecutionError, RefError, UnsupportedError
from recap_compiler.execution import run_query
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate, bounded_range
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import TransitionsRelation

LOOP_RELATION = TransitionsRelation(rows=((0, 0, "purchase"),), q0=0, accepting_states=frozenset({0}))
TWO_STATE_RELATION = TransitionsRelation(
    rows=((0, 1, "purchase"), (1, 1, "purchase")), q0=0, accepting_states=frozenset({1}))


def _conn_with_edges(rows):
    conn = duckdb.connect()
    conn.execute("CREATE TABLE edges(edge_id INT, src INT, dst INT, label TEXT, amount DOUBLE)")
    for row in rows:
        conn.execute("INSERT INTO edges VALUES (?, ?, ?, ?, ?)", row)
    return conn


def _both_queries(conn, aggregate, relation, *, start_vertices, length_bound):
    """Registers macros + transitions once, then builds both the standard
    (Stage E) and optimized (Stage F) queries against the same connection,
    so FR-22 equivalence tests are comparing the exact same inputs."""
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    standard = build_standard_query(relation=relation, start_vertices=start_vertices,
                                     length_bound=length_bound)
    optimized = build_optimized_query(aggregate=aggregate, relation=relation,
                                       start_vertices=start_vertices, length_bound=length_bound)
    return standard, optimized


def test_optimized_sql_has_no_macro_calls():
    conn = _conn_with_edges([(1, 1, 2, "purchase", 10.0)])
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    _, optimized = _both_queries(conn, aggregate, LOOP_RELATION, start_vertices=[1], length_bound=3)
    for macro_name in ("init_d(", "update_d(", "is_viable_d(", "is_viable_d_final(", "finalize_d("):
        assert macro_name not in optimized.sql


def test_optimized_query_prunes_the_same_way_as_standard_query():
    conn = _conn_with_edges([
        (1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 20.0), (3, 3, 4, "purchase", 999.0)])
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    standard, optimized = _both_queries(conn, aggregate, LOOP_RELATION,
                                         start_vertices=[1], length_bound=4)
    reached = {row[0] for row in conn.execute(optimized.sql).fetchall()}
    assert reached == {1, 2, 3}  # vertex 4 pruned, same as the standard query (see test_standard_sql.py)


# --- FR-22: results must match the standard query, not just "look similar" -

@pytest.mark.parametrize("length_bound", [1, 2, 3, 4])
def test_fr22_factorized_equivalence_across_length_bounds(length_bound):
    conn = _conn_with_edges([
        (1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 20.0),
        (3, 3, 4, "purchase", 999.0), (4, 1, 5, "purchase", 5.0)])
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    standard, optimized = _both_queries(conn, aggregate, LOOP_RELATION,
                                         start_vertices=[1], length_bound=length_bound)

    standard_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                      in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                       in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows


def test_fr22_non_factorized_equivalence():
    conn = _conn_with_edges([
        (1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 12.0), (3, 3, 4, "purchase", 999.0)])
    aggregate = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_amount", "DOUBLE"),),
        init_d="{last_amount: NULL}",
        update_d={(0, 1): "{last_amount: e.amount}", (1, 1): "{last_amount: e.amount}"},
        is_viable_d={(0, 1): "TRUE", (1, 1): "D.last_amount IS NULL OR e.amount - D.last_amount <= 5.0"},
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=False,
    )
    standard, optimized = _both_queries(conn, aggregate, TWO_STATE_RELATION,
                                         start_vertices=[1], length_bound=4)
    standard_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                      in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                       in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows
    assert (4, 1, 3) not in optimized_rows  # 999-12=987 > 5.0, pruned on both sides


def test_fr22_no_dictionary_keys_equivalence():
    conn = _conn_with_edges([(1, 1, 2, "purchase", 10.0)])
    aggregate = SelectiveAggregate(
        dictionary_keys=(), init_d="NULL", update_d="D", is_viable_d="TRUE",
        is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    standard, optimized = _both_queries(conn, aggregate, LOOP_RELATION,
                                         start_vertices=[1], length_bound=3)
    standard_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                      in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                       in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows == {(1, 0, 0), (2, 0, 1)}


def test_run_query_accepts_optimized_query_via_execution_module():
    conn = _conn_with_edges([(1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 999.0)])
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    materialize_transitions(conn, LOOP_RELATION)
    optimized = build_optimized_query(aggregate=aggregate, relation=LOOP_RELATION,
                                       start_vertices=[1], length_bound=3)
    result = run_query(conn, optimized, result_shape="count")
    assert result.rows == [(2,)]  # vertex 3 pruned


def test_non_struct_body_raises_unsupported_error():
    aggregate = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("x", "DOUBLE"),),
        init_d="some_udf_call()",  # not a struct literal -- not inlinable by this cut
        update_d="D",
        is_viable_d="TRUE",
        factorized=True,
    )
    with pytest.raises(UnsupportedError, match="struct literal"):
        build_optimized_query(aggregate=aggregate, relation=LOOP_RELATION,
                               start_vertices=[1], length_bound=3)


def test_empty_start_vertices_raises_execution_error():
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    with pytest.raises(ExecutionError, match="no start vertices"):
        build_optimized_query(aggregate=aggregate, relation=LOOP_RELATION,
                               start_vertices=[], length_bound=3)


# --- update_d completion: a partial struct must not crash, and must mean
# "leave this key unchanged" identically in both the standard and optimized
# queries (not just in whichever stage happens to be more lenient) --------

def test_partial_update_d_struct_does_not_crash_and_freezes_the_omitted_key():
    conn = _conn_with_edges([
        (1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 20.0), (3, 3, 4, "purchase", 999.0)])
    aggregate = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("max_amount", "DOUBLE"), DictionaryKey("min_amount", "DOUBLE")),
        init_d="{max_amount: -1e308, min_amount: 1e308}",
        update_d="{max_amount: GREATEST(D.max_amount, e.amount)}",  # min_amount deliberately omitted
        is_viable_d="D.max_amount <= 15.0",
        is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    standard, optimized = _both_queries(conn, aggregate, LOOP_RELATION,
                                         start_vertices=[1], length_bound=4)

    standard_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                      in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                       in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows == {(1, 0, 0), (2, 0, 1), (3, 0, 2)}

    d_by_vertex = {v: d for v, _q, d, _pl, _r in conn.execute(optimized.sql).fetchall()}
    assert d_by_vertex[3]["min_amount"] == 1e308  # never referenced by update_d -- frozen at init


def test_partial_update_d_struct_non_factorized_per_pair():
    conn = _conn_with_edges([(1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 999.0)])
    aggregate = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_amount", "DOUBLE"), DictionaryKey("hop_count", "BIGINT")),
        init_d="{last_amount: NULL, hop_count: 0}",
        # (0,1) only updates last_amount; (1,1) only updates hop_count -- each
        # pair omits a different key, both must default to pass-through.
        update_d={(0, 1): "{last_amount: e.amount}", (1, 1): "{hop_count: D.hop_count + 1}"},
        is_viable_d={(0, 1): "TRUE", (1, 1): "TRUE"},
        is_viable_d_final="TRUE", finalize_d="D", factorized=False,
    )
    standard, optimized = _both_queries(conn, aggregate, TWO_STATE_RELATION,
                                         start_vertices=[1], length_bound=3)
    standard_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                      in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                       in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows == {(2, 1, 1), (3, 1, 2)}

    d_by_vertex = {v: d for v, _q, d, _pl, _r in conn.execute(optimized.sql).fetchall()}
    assert d_by_vertex[2]["hop_count"] == 0     # (0,1) doesn't touch hop_count -- stays at init
    assert d_by_vertex[3]["last_amount"] == 10.0  # (1,1) doesn't touch last_amount -- carried from hop 1


def test_update_d_assignment_statement_form_matches_struct_literal_form():
    conn = _conn_with_edges([
        (1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 20.0), (3, 3, 4, "purchase", 999.0)])
    struct_aggregate = bounded_range(property="amount", upper_bound=15.0)
    assignment_aggregate = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("max_amount", "DOUBLE"), DictionaryKey("min_amount", "DOUBLE")),
        init_d="{max_amount: -1e308, min_amount: 1e308}",
        update_d=("D.max_amount = GREATEST(D.max_amount, e.amount); "
                   "D.min_amount = LEAST(D.min_amount, e.amount)"),
        is_viable_d="GREATEST(D.max_amount, e.amount) - LEAST(D.min_amount, e.amount) <= 15.0",
        is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )

    def signature(aggregate):
        _, optimized = _both_queries(conn, aggregate, LOOP_RELATION, start_vertices=[1], length_bound=4)
        return {(v, q, path_length) for v, q, _d, path_length, _r
                in conn.execute(optimized.sql).fetchall()}

    struct_rows = signature(struct_aggregate)
    assignment_rows = signature(assignment_aggregate)
    assert struct_rows == assignment_rows == {(1, 0, 0), (2, 0, 1), (3, 0, 2)}


def test_update_d_single_assignment_leaves_the_unmentioned_key_frozen():
    conn = _conn_with_edges([(1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 20.0)])
    aggregate = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("max_amount", "DOUBLE"), DictionaryKey("min_amount", "DOUBLE")),
        init_d="{max_amount: -1e308, min_amount: 1e308}",
        update_d="D.max_amount = GREATEST(D.max_amount, e.amount)",  # min_amount never assigned
        is_viable_d="TRUE", is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    standard, optimized = _both_queries(conn, aggregate, LOOP_RELATION, start_vertices=[1], length_bound=3)
    standard_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                      in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                       in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows == {(1, 0, 0), (2, 0, 1), (3, 0, 2)}

    d_by_vertex = {v: d for v, _q, d, _pl, _r in conn.execute(optimized.sql).fetchall()}
    assert d_by_vertex[3]["min_amount"] == 1e308  # never assigned -- frozen at init


def test_init_d_missing_declared_key_raises_ref_error():
    aggregate = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("max_amount", "DOUBLE"), DictionaryKey("min_amount", "DOUBLE")),
        init_d="{max_amount: -1e308}",  # min_amount missing -- no sensible pass-through at init
        update_d="{max_amount: D.max_amount, min_amount: D.min_amount}",
        is_viable_d="TRUE", is_viable_d_final="TRUE", finalize_d="D", factorized=True,
    )
    with pytest.raises(RefError, match="init_d does not initialize declared key.*min_amount"):
        build_optimized_query(aggregate=aggregate, relation=LOOP_RELATION,
                               start_vertices=[1], length_bound=3)
