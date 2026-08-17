import pytest

from recap_compiler.errors import RefError
from recap_compiler.selective_aggregate import (
    DictionaryKey,
    SelectiveAggregate,
    adjacent_edge_predicate,
    bounded_range,
    combine_library_aggregates,
    complete_update_d_body,
    generate_skeleton,
    normalize_update_d_body,
    trail_via_edge_ids,
    typed_init_d,
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


# --- FR-34: combining multiple library entries -------------------------------

def test_combine_library_aggregates_unions_keys_and_is_still_valid():
    # The checklist's own example: max-min and trail together.
    combined = combine_library_aggregates(
        bounded_range(property="amount", upper_bound=100.0), trail_via_edge_ids())
    assert {key.name for key in combined.dictionary_keys} == {"max_amount", "min_amount", "edge_ids"}
    assert combined.factorized is True
    validate_selective_aggregate(combined, edge_columns=EDGE_COLUMNS)


def test_combine_library_aggregates_conjoins_is_viable_d():
    combined = combine_library_aggregates(
        bounded_range(property="amount", upper_bound=100.0), trail_via_edge_ids())
    assert " AND " in combined.is_viable_d
    assert "D.max_amount" in combined.is_viable_d
    assert "list_contains(D.edge_ids" in combined.is_viable_d


def test_combine_library_aggregates_each_entry_keeps_its_own_update_d_logic():
    combined = combine_library_aggregates(
        bounded_range(property="amount", upper_bound=100.0), trail_via_edge_ids())
    assert "GREATEST(D.max_amount" in combined.update_d
    assert "list_append(D.edge_ids" in combined.update_d


def test_combine_library_aggregates_rejects_duplicate_key_name():
    with pytest.raises(RefError):
        combine_library_aggregates(
            adjacent_edge_predicate(property="time"), adjacent_edge_predicate(property="time"))


def test_combine_library_aggregates_rejects_fewer_than_two():
    with pytest.raises(RefError):
        combine_library_aggregates(trail_via_edge_ids())


def test_combine_library_aggregates_rejects_non_factorized():
    non_factorized = SelectiveAggregate(
        dictionary_keys=(), init_d="NULL",
        update_d={(1, 2): "D"}, is_viable_d={(1, 2): "TRUE"}, factorized=False)
    with pytest.raises(RefError):
        combine_library_aggregates(trail_via_edge_ids(), non_factorized)


def test_combine_library_aggregates_supports_three_entries():
    combined = combine_library_aggregates(
        bounded_range(property="amount", upper_bound=100.0),
        adjacent_edge_predicate(property="time"),
        trail_via_edge_ids())
    assert {key.name for key in combined.dictionary_keys} == {
        "max_amount", "min_amount", "last_time", "edge_ids"}
    validate_selective_aggregate(combined, edge_columns=EDGE_COLUMNS)


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


# --- complete_update_d_body: a partial struct defaults missing keys to
# passing their previous value through unchanged (`D.<key>`) -------------

def test_complete_update_d_body_fills_in_the_missing_key():
    completed = complete_update_d_body(
        "{max_amount: GREATEST(D.max_amount, e.amount)}", declared_keys=["max_amount", "min_amount"])
    assert "D.min_amount" in completed
    assert "GREATEST(D.max_amount, e.amount)" in completed


def test_complete_update_d_body_is_a_no_op_when_already_complete():
    body = "{max_amount: D.max_amount, min_amount: D.min_amount}"
    assert complete_update_d_body(body, declared_keys=["max_amount", "min_amount"]) == body


def test_complete_update_d_body_leaves_a_non_struct_body_unchanged():
    # bare "D" (pass everything through) isn't a struct literal -- nothing to complete.
    assert complete_update_d_body("D", declared_keys=["max_amount", "min_amount"]) == "D"


def test_complete_update_d_body_is_a_no_op_with_no_declared_keys():
    assert complete_update_d_body("{x: 1}", declared_keys=[]) == "{x: 1}"


# --- normalize_update_d_body: accepts either a struct literal or one or
# more "D.<key> = <expr>" assignment statements, converting both to the
# same completed-struct-literal form -------------------------------------

def test_normalize_update_d_body_converts_a_single_assignment():
    normalized = normalize_update_d_body(
        "D.max_amount = GREATEST(D.max_amount, e.amount)",
        declared_keys=["max_amount", "min_amount"])
    assert "GREATEST(D.max_amount, e.amount)" in normalized
    assert "D.min_amount" in normalized  # omitted key defaults to pass-through


def test_normalize_update_d_body_converts_multiple_semicolon_separated_assignments():
    normalized = normalize_update_d_body(
        "D.max_amount = GREATEST(D.max_amount, e.amount); D.min_amount = LEAST(D.min_amount, e.amount)",
        declared_keys=["max_amount", "min_amount"])
    assert "GREATEST(D.max_amount, e.amount)" in normalized
    assert "LEAST(D.min_amount, e.amount)" in normalized


def test_normalize_update_d_body_converts_one_assignment_per_line_with_no_semicolons():
    normalized = normalize_update_d_body(
        "D.max_amount = GREATEST(D.max_amount, e.amount)\n"
        "D.min_amount = LEAST(D.min_amount, e.amount)",
        declared_keys=["max_amount", "min_amount"])
    assert "GREATEST(D.max_amount, e.amount)" in normalized
    assert "LEAST(D.min_amount, e.amount)" in normalized


def test_normalize_update_d_body_line_separated_form_can_still_omit_a_key():
    normalized = normalize_update_d_body(
        "D.max_amount = GREATEST(D.max_amount, e.amount)\n\n"  # blank line between -- should be ignored
        "D.min_amount = LEAST(D.min_amount, e.amount)",
        declared_keys=["max_amount", "min_amount", "count"])
    assert "D.count" in normalized  # omitted key defaults to pass-through


def test_normalize_update_d_body_does_not_mangle_a_multiline_struct_literal():
    body = "{\n  max_amount: GREATEST(D.max_amount, e.amount),\n  min_amount: LEAST(D.min_amount, e.amount)\n}"
    normalized = normalize_update_d_body(body, declared_keys=["max_amount", "min_amount"])
    assert "GREATEST(D.max_amount, e.amount)" in normalized
    assert "LEAST(D.min_amount, e.amount)" in normalized


def test_normalize_update_d_body_still_handles_a_plain_struct_literal():
    body = "{max_amount: GREATEST(D.max_amount, e.amount)}"
    normalized = normalize_update_d_body(body, declared_keys=["max_amount", "min_amount"])
    assert "D.min_amount" in normalized  # same completion as complete_update_d_body


def test_normalize_update_d_body_still_passes_through_bare_D():
    assert normalize_update_d_body("D", declared_keys=["max_amount"]) == "D"


def test_normalize_update_d_body_rejects_assignment_to_undeclared_key():
    with pytest.raises(RefError, match="undeclared dictionary key 'D.bogus'"):
        normalize_update_d_body("D.bogus = e.amount", declared_keys=["max_amount"])


def test_normalize_update_d_body_rejects_a_non_assignment_statement_among_several():
    with pytest.raises(RefError, match="expected a struct literal"):
        normalize_update_d_body(
            "D.max_amount = e.amount; TRUE", declared_keys=["max_amount", "min_amount"])


# --- normalize_update_d_body: augmented assignment (+=/-=/*=//=), expanded
# to "D.<key> = D.<key> <op> <expr>" before parsing -- SQL has no augmented-
# assignment operator in any dialect, so this has to be a text rewrite, not
# a parse-tree one (a real user hit this via `D["total_amounts"] += e.amount`
# in the workbench; confirmed the error is identical for dot or bracket
# notation, so it's the operator that's unparseable) ----------------------

@pytest.mark.parametrize("op", ["+", "-", "*", "/"])
def test_normalize_update_d_body_expands_augmented_assignment(op):
    normalized = normalize_update_d_body(
        f"D.total_amount {op}= e.amount", declared_keys=["total_amount"])
    expected = normalize_update_d_body(
        f"D.total_amount = D.total_amount {op} (e.amount)", declared_keys=["total_amount"])
    assert normalized == expected


def test_normalize_update_d_body_augmented_assignment_omitted_key_defaults_to_pass_through():
    normalized = normalize_update_d_body(
        "D.total_amount += e.amount", declared_keys=["total_amount", "count"])
    assert "D.count" in normalized  # omitted key defaults to pass-through, same as plain "="


def test_normalize_update_d_body_augmented_assignment_semicolon_separated():
    normalized = normalize_update_d_body(
        "D.total_amount += e.amount; D.count += 1",
        declared_keys=["total_amount", "count"])
    assert "D.total_amount + (e.amount)" in normalized
    assert "D.count + (1)" in normalized


def test_normalize_update_d_body_augmented_assignment_mixed_with_plain_equals():
    normalized = normalize_update_d_body(
        "D.total_amount += e.amount\nD.region = e.region",
        declared_keys=["total_amount", "region"])
    assert "D.total_amount + (e.amount)" in normalized
    assert "e.region" in normalized


def test_normalize_update_d_body_augmented_assignment_rejects_undeclared_key():
    with pytest.raises(RefError, match="undeclared dictionary key 'D.bogus'"):
        normalize_update_d_body("D.bogus += e.amount", declared_keys=["total_amount"])


def test_normalize_update_d_body_bracket_notation_still_unsupported():
    """Scoped deliberately: only dot-notation augmented assignment was
    requested/implemented. Bracket notation (`D["key"] += expr`) fails the
    same way it always did -- this test pins that down so a future change
    doesn't accidentally start silently accepting it half-way."""
    with pytest.raises(RefError, match="could not parse expression body"):
        normalize_update_d_body('D["total_amount"] += e.amount', declared_keys=["total_amount"])


# --- typed_init_d: casts each field to its declared type, so DuckDB can't
# independently infer a too-narrow type for the anchor branch of the
# recursive CTE (a real bug: a bare `NULL` for a last_timestamp_ms key
# inferred as INTEGER, then overflowed once real BIGINT epoch-ms values
# flowed through the recursive term) --------------------------------------

def test_typed_init_d_casts_a_bare_null_to_the_declared_type():
    agg = adjacent_edge_predicate(property="timestamp_ms")
    typed = typed_init_d(agg)
    # sqlglot renders the struct key quoted here (e.g. {'last_timestamp_ms':
    # ...}), a cosmetically different but equally valid DuckDB struct-literal
    # form -- checked via the real end-to-end regression test
    # (test_adjacent_edge_predicate_does_not_overflow_on_real_bigint_timestamps
    # in test_optimizer.py) rather than asserting on exact string form here.
    assert "last_timestamp_ms" in typed
    assert "CAST(NULL AS DOUBLE)" in typed


def test_typed_init_d_casts_every_declared_key_independently():
    agg = bounded_range(property="amount", upper_bound=15.0)
    typed = typed_init_d(agg)
    assert typed.count("CAST(") == 2  # max_amount and min_amount both cast
    assert typed.count("AS DOUBLE") == 2


def test_typed_init_d_is_a_no_op_with_no_declared_keys():
    agg = SelectiveAggregate(dictionary_keys=(), init_d="NULL", update_d="D", is_viable_d="TRUE")
    assert typed_init_d(agg) == "NULL"


def test_typed_init_d_leaves_a_non_struct_body_unchanged():
    agg = SelectiveAggregate(
        dictionary_keys=(DictionaryKey("x", "DOUBLE"),), init_d="D", update_d="D", is_viable_d="TRUE")
    assert typed_init_d(agg) == "D"
