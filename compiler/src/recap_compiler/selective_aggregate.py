"""Stage D: selective-aggregate frontend + skeleton generation (FR-11..FR-14).

A selective aggregate (Definition 8) is up to five SQL expression bodies over
a dictionary `D`: `init_d()`, `update_d(D, from_state, to_state, e)`,
`is_viable_d(D, from_state, to_state, e)`, `is_viable_d_final(D)`,
`finalize_d(D)`. `D` is represented as a DuckDB struct literal keyed by the
declared dictionary keys (e.g. `{max_amt: ..., min_amt: ...}`), matching the
default JSON-column representation of FR-18 -- Stage F (not yet built) is
what flattens each key into its own column.

Every body is written as **literal SQL against Definition 8's own parameter
names**, because Stage E pastes these bodies verbatim as the bodies of
DuckDB SQL macros named after the functions themselves (no flattening or
inlining yet -- that's Stage F) -- so what's valid here is exactly what's
valid inside `CREATE MACRO update_d(D, from_state, to_state, e) AS <body>`:
  - `D.<key>` is a dictionary field (DuckDB struct field access -- confirmed
    empirically that a macro parameter typed as a struct supports `.field`
    the same way a table alias does); bare `D` alone is the whole struct.
  - `e.<column>` is an edge property (DuckDB treats a bare table alias
    passed into a macro as a STRUCT, so this also works unmodified).
  - bare `from_state`/`to_state` are the NFA state variables -- only
    meaningful in a non-factorized `update_d`/`is_viable_d` body, since a
    factorized body is a single expression applied regardless of state.
  - `init_d()` takes no parameters at all, so nothing (not even `D`) is in
    scope inside it.

`update_d`/`is_viable_d` are each either a single body (`factorized=True`)
or a dict keyed by the exact `(from_state, to_state)` pairs of a
`TransitionsRelation` (`factorized=False`), one branch per transition, per
FR-12.
"""
from __future__ import annotations

from dataclasses import dataclass

import sqlglot
import sqlglot.errors
from sqlglot import exp

from .errors import RefError
from .transitions import TransitionsRelation

TransitionPair = tuple[int, int]

DICT_ALIAS = "D"
EDGE_ALIAS = "e"


@dataclass(frozen=True)
class DictionaryKey:
    """One key of the selective aggregate's dictionary D (FR-14a)."""

    name: str
    sql_type: str


@dataclass(frozen=True)
class SelectiveAggregate:
    """A query author's (or library entry's) selective aggregate (FR-11)."""

    dictionary_keys: tuple[DictionaryKey, ...]
    init_d: str
    update_d: str | dict[TransitionPair, str]
    is_viable_d: str | dict[TransitionPair, str]
    is_viable_d_final: str = "TRUE"
    finalize_d: str = "D"
    factorized: bool = True


@dataclass(frozen=True)
class Skeleton:
    """FR-12: the CASE-statement skeleton for a non-factorized aggregate, or
    the single-body placeholder for a factorized one, over the transitions
    relation's actual `(from_state, to_state)` pairs."""

    factorized: bool
    pairs: tuple[TransitionPair, ...]
    update_d: str
    is_viable_d: str


def generate_skeleton(relation: TransitionsRelation, *, factorized: bool) -> Skeleton:
    """FR-12. For a factorized aggregate the author supplies one unconditional
    body each for `update_d`/`is_viable_d`, so no CASE is generated. For a
    non-factorized aggregate, a `CASE` skeleton is generated over every
    distinct `(from_state, to_state)` pair actually present in `relation`,
    so the author fills in only the transition-specific logic."""
    pairs = tuple(sorted({(frm, to) for frm, to, _label in relation.rows}))

    if factorized:
        return Skeleton(
            factorized=True, pairs=pairs,
            update_d="-- TODO: single body (factorized): D",
            is_viable_d="-- TODO: single body (factorized): TRUE",
        )

    def case_skeleton(default: str) -> str:
        branches = "\n".join(
            f"    WHEN from_state = {frm} AND to_state = {to} THEN -- TODO"
            for frm, to in pairs)
        return f"CASE\n{branches}\n    ELSE {default}\nEND"

    return Skeleton(
        factorized=False, pairs=pairs,
        update_d=case_skeleton("D"),
        is_viable_d=case_skeleton("TRUE"),
    )


def _referenced_columns(body: str, *, function_name: str, locus: str) -> list[exp.Column]:
    try:
        parsed = sqlglot.parse_one(body, read="duckdb")
    except sqlglot.errors.ParseError as exc:
        raise RefError(
            f"{function_name}: could not parse expression body: {exc}", locus=locus) from exc
    return list(parsed.find_all(exp.Column))


def _validate_init_d(body: str) -> None:
    """FR-14 for init_d(): it takes no parameters, so no column reference of
    any kind is in scope."""
    for column in _referenced_columns(body, function_name="init_d", locus="init_d"):
        raise RefError(
            f"init_d takes no parameters, so it may not reference "
            f"'{column.sql()}' (nothing is in scope)", locus=column.sql())


def _validate_dict_only_body(body: str, *, function_name: str, declared_keys: set[str]) -> None:
    """FR-14 for is_viable_d_final(D)/finalize_d(D): the only allowed
    references are `D` itself (the whole dictionary) or `D.<key>` for a
    declared dictionary key."""
    for column in _referenced_columns(body, function_name=function_name, locus=function_name):
        if column.table:
            if column.table != DICT_ALIAS:
                raise RefError(
                    f"{function_name} may not reference '{column.sql()}' "
                    f"(only '{DICT_ALIAS}', the dictionary, is in scope)",
                    locus=column.sql())
            if column.name not in declared_keys:
                raise RefError(
                    f"{function_name} references undeclared dictionary key "
                    f"'{DICT_ALIAS}.{column.name}'", locus=column.sql())
            continue
        if column.name != DICT_ALIAS:
            raise RefError(
                f"{function_name} references unknown identifier '{column.name}' "
                f"(only '{DICT_ALIAS}' or '{DICT_ALIAS}.<key>' is in scope)",
                locus=column.name)


def _validate_transition_body(body: str, *, function_name: str, declared_keys: set[str],
                               edge_columns: set[str], factorized: bool,
                               locus_suffix: str = "") -> None:
    """FR-14 for update_d/is_viable_d: `D.<key>` for a declared dictionary
    key, `e.<column>` for a real edge column, or (only when non-factorized)
    bare from_state/to_state."""
    locus = f"{function_name}{locus_suffix}"
    state_vars = set() if factorized else {"from_state", "to_state"}
    for column in _referenced_columns(body, function_name=function_name, locus=locus):
        if column.table == DICT_ALIAS:
            if column.name not in declared_keys:
                raise RefError(
                    f"{locus} references undeclared dictionary key "
                    f"'{DICT_ALIAS}.{column.name}'", locus=column.sql())
            continue
        if column.table == EDGE_ALIAS:
            if column.name not in edge_columns:
                raise RefError(
                    f"{locus} references unknown edge column "
                    f"'{EDGE_ALIAS}.{column.name}'", locus=column.sql())
            continue
        if column.table:
            raise RefError(
                f"{locus} references unknown table alias '{column.table}' "
                f"(only '{DICT_ALIAS}' and '{EDGE_ALIAS}' are in scope)",
                locus=column.sql())
        if column.name == DICT_ALIAS:
            continue
        if column.name in state_vars:
            continue
        if factorized and column.name in {"from_state", "to_state"}:
            raise RefError(
                f"{locus} references NFA state variable '{column.name}' in a "
                f"factorized body (factorized aggregates may not depend on "
                f"NFA state transitions)", locus=column.name)
        raise RefError(
            f"{locus} references unknown identifier '{column.name}' (expected "
            f"'{DICT_ALIAS}', '{DICT_ALIAS}.<key>', '{EDGE_ALIAS}.<column>', or a state variable)",
            locus=column.name)


def validate_selective_aggregate(aggregate: SelectiveAggregate, *,
                                  edge_columns: set[str],
                                  transitions: TransitionsRelation | None = None) -> None:
    """FR-14: validates every function body references only declared
    dictionary keys, real edge-schema columns, or (for a non-factorized
    update_d/is_viable_d) the NFA state variables. Raises RefError naming
    the offending identifier and function on the first violation found."""
    declared_keys = {key.name for key in aggregate.dictionary_keys}

    _validate_init_d(aggregate.init_d)
    _validate_dict_only_body(aggregate.is_viable_d_final, function_name="is_viable_d_final",
                              declared_keys=declared_keys)
    _validate_dict_only_body(aggregate.finalize_d, function_name="finalize_d",
                              declared_keys=declared_keys)

    for function_name, body in (("update_d", aggregate.update_d),
                                 ("is_viable_d", aggregate.is_viable_d)):
        if aggregate.factorized:
            if not isinstance(body, str):
                raise RefError(
                    f"{function_name} must be a single body for a factorized "
                    f"aggregate, got a per-transition mapping", locus=function_name)
            _validate_transition_body(
                body, function_name=function_name, declared_keys=declared_keys,
                edge_columns=edge_columns, factorized=True)
        else:
            if not isinstance(body, dict):
                raise RefError(
                    f"{function_name} must be a per-(from_state, to_state) mapping "
                    f"for a non-factorized aggregate, got a single body",
                    locus=function_name)
            if transitions is not None:
                expected_pairs = {(frm, to) for frm, to, _label in transitions.rows}
                missing = expected_pairs - set(body)
                if missing:
                    raise RefError(
                        f"{function_name} is missing a branch for transition "
                        f"pair(s) {sorted(missing)}", locus=function_name)
            for pair, pair_body in body.items():
                _validate_transition_body(
                    pair_body, function_name=function_name, declared_keys=declared_keys,
                    edge_columns=edge_columns, factorized=False, locus_suffix=f"[{pair}]")


# --- FR-13: library of pre-written selective aggregates -------------------

def adjacent_edge_predicate(*, property: str, comparator: str = ">=") -> SelectiveAggregate:
    """FR-13(i) / Example 7: an adjacent-edge predicate on a maintained last
    value, e.g. non-decreasing timestamps between consecutive edges
    (`comparator=">="`). Negatively stable because the predicate is checked
    on every hop and never re-examines an earlier edge."""
    last_key = f"last_{property}"
    return SelectiveAggregate(
        dictionary_keys=(DictionaryKey(last_key, "DOUBLE"),),
        init_d=f"{{{last_key}: NULL}}",
        update_d=f"{{{last_key}: e.{property}}}",
        is_viable_d=f"D.{last_key} IS NULL OR e.{property} {comparator} D.{last_key}",
        factorized=True,
    )


def trail_via_edge_ids(*, id_column: str = "id") -> SelectiveAggregate:
    """FR-13(ii) / Example 8: trail semantics via a maintained edge-id set --
    a path may not reuse an edge it has already traversed. Negatively stable
    because a repeated edge id can never become un-repeated by extension."""
    return SelectiveAggregate(
        dictionary_keys=(DictionaryKey("edge_ids", "BIGINT[]"),),
        init_d="{edge_ids: []}",
        update_d=f"{{edge_ids: list_append(D.edge_ids, e.{id_column})}}",
        is_viable_d=f"NOT list_contains(D.edge_ids, e.{id_column})",
        factorized=True,
    )


def bounded_range(*, property: str, upper_bound: float) -> SelectiveAggregate:
    """FR-13(iii) / Example 9: a bounded monotone/distributive aggregate,
    max(property) - min(property) <= U. Negatively stable because max-min is
    monotonically non-decreasing under extension, so once it exceeds U no
    extension can bring it back down."""
    max_key, min_key = f"max_{property}", f"min_{property}"
    return SelectiveAggregate(
        dictionary_keys=(DictionaryKey(max_key, "DOUBLE"), DictionaryKey(min_key, "DOUBLE")),
        init_d=f"{{{max_key}: -1e308, {min_key}: 1e308}}",
        update_d=(f"{{{max_key}: GREATEST(D.{max_key}, e.{property}), "
                   f"{min_key}: LEAST(D.{min_key}, e.{property})}}"),
        is_viable_d=(f"GREATEST(D.{max_key}, e.{property}) - "
                      f"LEAST(D.{min_key}, e.{property}) <= {upper_bound}"),
        factorized=True,
    )
