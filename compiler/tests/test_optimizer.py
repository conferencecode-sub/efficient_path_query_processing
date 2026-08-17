import duckdb
import pytest

from recap_compiler.errors import ExecutionError, RefError, UnsupportedError
from recap_compiler.execution import run_query
from recap_compiler.ingestion import set_trivial_label_column
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate, adjacent_edge_predicate, bounded_range
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import TransitionsRelation, trivial_relation

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


def test_optimized_anchor_handles_vertex_ids_beyond_int32_range():
    """Same BIGINT-anchor regression as standard_sql's own test (found via
    Datagen-7.7's LDBC-style ids + a small low-degree start vertex) -- the
    optimized query builds its own separate anchor/VALUES clause, so it
    needs the same explicit `::BIGINT` cast, not just the standard one."""
    conn = duckdb.connect()
    conn.execute("CREATE TABLE edges(edge_id BIGINT, src BIGINT, dst BIGINT, label TEXT, amount DOUBLE)")
    huge_id = 13194146057717
    conn.execute("INSERT INTO edges VALUES (1, 3184, ?, 'purchase', 1.0)", [huge_id])

    aggregate = bounded_range(property="amount", upper_bound=100.0)
    _, optimized = _both_queries(conn, aggregate, LOOP_RELATION, start_vertices=[3184], length_bound=1)
    rows = conn.execute(optimized.sql).fetchall()
    reached = {row[0] for row in rows}
    assert huge_id in reached


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


# --- a "no regex" query still goes through the same automaton-shaped SQL,
# just over transitions.trivial_relation()'s single self-looping state ------

def test_fr22_trivial_relation_equivalence():
    """A regex-less query (`trivial_relation()`) still has to agree between
    Stage E and Stage F -- same FR-22 obligation, just with a single
    self-looping state instead of a real regex's NFA."""
    conn = _conn_with_edges([
        (1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 20.0),
        (3, 3, 4, "other", 999.0), (4, 1, 5, "other", 5.0)])
    set_trivial_label_column(conn)  # overwrites the real 'purchase'/'other' labels above
    relation = trivial_relation()
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    standard, optimized = _both_queries(conn, aggregate, relation, start_vertices=[1], length_bound=4)
    standard_rows = {(v, path_length) for v, _q, _d, path_length, _r in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, path_length) for v, _q, _d, path_length, _r in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows
    assert (4, 3) not in optimized_rows  # amount 999 blows the range, pruned on both sides
    assert (5, 1) in optimized_rows  # reached regardless of the edge's original label -- 'other' included too


def test_trivial_relation_query_still_has_a_q_column_and_a_transitions_join():
    conn = _conn_with_edges([(1, 1, 2, "purchase", 10.0)])
    set_trivial_label_column(conn)
    relation = trivial_relation()
    aggregate = bounded_range(property="amount", upper_bound=15.0)
    optimized = build_optimized_query(aggregate=aggregate, relation=relation,
                                       start_vertices=[1], length_bound=3)
    assert "AS q" in optimized.sql  # not a special-cased no-automaton query
    assert "transitions" in optimized.sql


def test_trivial_relation_supports_non_factorized_aggregates_too():
    """The whole point of routing "no regex" through a trivial automaton
    instead of a separate no-NFA code path: a non-factorized aggregate
    (defined per NFA transition pair) still has meaning here, since the
    trivial automaton has exactly one pair, (0, 0) -- no factorized-only
    restriction is needed."""
    conn = _conn_with_edges([(1, 1, 2, "purchase", 10.0), (2, 2, 3, "purchase", 999.0)])
    set_trivial_label_column(conn)
    relation = trivial_relation()
    aggregate = SelectiveAggregate(
        dictionary_keys=(), init_d="NULL", update_d={(0, 0): "D"}, is_viable_d={(0, 0): "e.amount <= 15.0"},
        is_viable_d_final="TRUE", finalize_d="D", factorized=False,
    )
    standard, optimized = _both_queries(conn, aggregate, relation, start_vertices=[1], length_bound=3)
    standard_rows = {row[0] for row in conn.execute(standard.sql).fetchall()}
    optimized_rows = {row[0] for row in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows == {1, 2}  # amount=999 hop pruned on both sides


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


# --- regression: a bare `NULL` in init_d must not make DuckDB infer a type
# too narrow for the real values update_d later produces. A real user hit
# this via the workbench: adjacent_edge_predicate(property="timestamp_ms")
# raised "Type INT64 ... can't be cast ... INT32" on real epoch-ms values,
# because init_d's untyped NULL got inferred as INTEGER for the anchor
# branch, independent of what the recursive branch would compute. Fixed by
# `typed_init_d` casting to the declared DictionaryKey.sql_type. -----------

def test_adjacent_edge_predicate_does_not_overflow_on_real_bigint_timestamps():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE edges(edge_id INT, src INT, dst INT, label TEXT, timestamp_ms BIGINT)")
    # The exact value class from the bug report: a real epoch-ms timestamp,
    # far beyond INT32's ~2.1 billion range.
    for row in [(1, 1, 2, "purchase", 1665251714000), (2, 2, 3, "purchase", 1665251715000),
                (3, 3, 4, "purchase", 1665251716000)]:
        conn.execute("INSERT INTO edges VALUES (?, ?, ?, ?, ?)", row)

    aggregate = adjacent_edge_predicate(property="timestamp_ms")
    standard, optimized = _both_queries(conn, aggregate, LOOP_RELATION,
                                         start_vertices=[1], length_bound=3)

    standard_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                      in conn.execute(standard.sql).fetchall()}
    optimized_rows = {(v, q, path_length) for v, q, _d, path_length, _r
                       in conn.execute(optimized.sql).fetchall()}
    assert standard_rows == optimized_rows == {(1, 0, 0), (2, 0, 1), (3, 0, 2), (4, 0, 3)}
