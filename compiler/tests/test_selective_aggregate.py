import pytest

from recap_compiler.errors import RefError
from recap_compiler.selective_aggregate import (
    DictionaryKey,
    SelectiveAggregate,
    adjacent_edge_predicate,
    bounded_range,
    generate_skeleton,
    trail_via_edge_ids,
    validate_selective_aggregate,
)
from recap_compiler.transitions import TransitionsRelation

# Q_B's worked instantiation (Section 10 of compiler_reqs.md): regex
# `Domestic+ Foreign` -> T = {(1,2,Domestic),(2,2,Domestic),(2,3,Foreign)}.
QB_RELATION = TransitionsRelation(
    rows=((1, 2, "Domestic"), (2, 2, "Domestic"), (2, 3, "Foreign")),
    q0=1, accepting_states=frozenset({3}))

EDGE_COLUMNS = {"src", "dst", "label", "amount", "time", "id"}


# --- FR-12: skeleton generation --------------------------------------------

def test_factorized_skeleton_has_no_case_and_lists_pairs():
    skeleton = generate_skeleton(QB_RELATION, factorized=True)
    assert skeleton.factorized is True
    assert skeleton.pairs == ((1, 2), (2, 2), (2, 3))
    assert "CASE" not in skeleton.update_d
    assert "CASE" not in skeleton.is_viable_d


def test_non_factorized_skeleton_has_one_branch_per_transition_pair():
    skeleton = generate_skeleton(QB_RELATION, factorized=False)
    assert skeleton.pairs == ((1, 2), (2, 2), (2, 3))
    for frm, to in skeleton.pairs:
        branch = f"WHEN from_state = {frm} AND to_state = {to} THEN"
        assert branch in skeleton.update_d
        assert branch in skeleton.is_viable_d
    assert skeleton.update_d.startswith("CASE")
    assert skeleton.update_d.rstrip().endswith("END")


# --- FR-13: library entries -------------------------------------------------

def test_adjacent_edge_predicate_library_entry_is_factorized_and_valid():
    agg = adjacent_edge_predicate(property="time")
    assert agg.factorized is True
    assert agg.dictionary_keys == (DictionaryKey("last_time", "DOUBLE"),)
    assert "D.last_time" in agg.is_viable_d
    validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_trail_via_edge_ids_library_entry_is_factorized_and_valid():
    agg = trail_via_edge_ids()
    assert agg.dictionary_keys == (DictionaryKey("edge_ids", "BIGINT[]"),)
    assert "list_contains(D.edge_ids" in agg.is_viable_d
    validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_bounded_range_library_entry_matches_worked_maxmin_example():
    agg = bounded_range(property="amount", upper_bound=89.55)
    assert {key.name for key in agg.dictionary_keys} == {"max_amount", "min_amount"}
    assert "89.55" in agg.is_viable_d
    assert "D.max_amount" in agg.is_viable_d and "D.min_amount" in agg.is_viable_d
    validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


# --- FR-14: reference validation --------------------------------------------

def test_init_d_may_not_reference_anything():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: D.last_time}",  # nothing is in scope inside init_d
        update_d="{last_time: e.time}",
        is_viable_d="TRUE",
        factorized=True,
    )
    with pytest.raises(RefError, match="takes no parameters"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_rejects_unknown_edge_column():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d="{last_time: e.nonexistent_column}",
        is_viable_d="TRUE",
        factorized=True,
    )
    with pytest.raises(RefError, match="unknown edge column 'e.nonexistent_column'"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_rejects_undeclared_dictionary_key():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d="{last_time: e.time}",
        is_viable_d="e.time >= D.last_time AND D.never_declared > 0",
        factorized=True,
    )
    with pytest.raises(RefError, match="undeclared dictionary key 'D.never_declared'"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_rejects_bare_identifier_that_is_not_D_or_a_state_variable():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d="{last_time: e.time}",
        is_viable_d="e.time >= last_time",  # bare key access no longer allowed
        factorized=True,
    )
    with pytest.raises(RefError, match="unknown identifier 'last_time'"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_rejects_state_variable_in_factorized_body():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d="{last_time: e.time}",
        is_viable_d="from_state = 1 AND e.time >= D.last_time",
        factorized=True,
    )
    with pytest.raises(RefError, match="factorized body"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_state_variables_are_allowed_in_non_factorized_body():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d={
            (1, 2): "{last_time: e.time}",
            (2, 2): "{last_time: e.time}",
            (2, 3): "{last_time: e.time}",
        },
        is_viable_d={
            (1, 2): "TRUE",
            (2, 2): "e.time - D.last_time <= 2",
            (2, 3): "from_state = 2 AND e.time - D.last_time <= 3",
        },
        factorized=False,
    )
    validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS, transitions=QB_RELATION)


def test_non_factorized_aggregate_missing_a_transition_branch_is_rejected():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d={(1, 2): "{last_time: e.time}", (2, 2): "{last_time: e.time}"},
        is_viable_d={(1, 2): "TRUE", (2, 2): "TRUE"},
        factorized=False,
    )
    with pytest.raises(RefError, match=r"missing a branch for transition pair"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS, transitions=QB_RELATION)


def test_finalize_d_and_is_viable_d_final_may_reference_D_and_declared_keys_only():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("max_amount", "DOUBLE"), DictionaryKey("min_amount", "DOUBLE")),
        init_d="{max_amount: -1e308, min_amount: 1e308}",
        update_d="{max_amount: GREATEST(D.max_amount, e.amount), min_amount: LEAST(D.min_amount, e.amount)}",
        is_viable_d="TRUE",
        is_viable_d_final="D.max_amount - D.min_amount <= 89.55",
        finalize_d="D",
    )
    validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_finalize_d_rejects_table_qualified_column_other_than_D():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d="{last_time: e.time}",
        is_viable_d="TRUE",
        finalize_d="e.time",
    )
    with pytest.raises(RefError, match="only 'D'"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)


def test_malformed_body_raises_ref_error_not_a_raw_parse_exception():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("last_time", "DOUBLE"),),
        init_d="{last_time: NULL}",
        update_d="{last_time: e.time}",
        is_viable_d="e.time >=",
        factorized=True,
    )
    with pytest.raises(RefError, match="could not parse"):
        validate_selective_aggregate(agg, edge_columns=EDGE_COLUMNS)
