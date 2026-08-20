"""Stage F: optimizer -- dictionary flattening + function inlining.

Stage E pastes Stage D's five bodies verbatim as DuckDB macros and calls
them by name; this stage instead **rewrites** those same (already
reference-validated) bodies into plain SQL spliced directly into the query
text, with no macro layer and no struct-typed `D` column internally:

- **Flattening:** each dictionary key becomes its own typed column
  of `paths` instead of a field nested inside a struct `D`. This falls out
  of Stage D's own convention almost for free -- `init_d`/`update_d` are
  already written as a single struct literal `{key: expr, ...}`, so
  decomposing that literal by key (`_decompose_struct`) *is* the
  flattening.
- **Inlining:** `D.<key>` becomes a real column reference
  (`p.<key>` in the recursive member, bare `<key>` in the outer query);
  `from_state`/`to_state` become `p.q`/`t.to_state`; `e.<column>` is left
  alone. No macro call remains anywhere in the generated SQL.
- A factorized aggregate's bodies rewrite to plain expressions;
  a non-factorized aggregate's `is_viable_d` rewrites to a single combined
  boolean `CASE` over transition pairs (matching Stage E's macro body
  shape), but `update_d` rewrites to **one plain `CASE` per dictionary
  key**, not one combined struct-valued `CASE` -- a deliberate performance
  choice, not an oversight (see the note below).
- **Measured, not assumed: a combined struct-valued `CASE` for `update_d`
  is slower, not faster, than one flat `CASE` per key.** A 2026-08-18
  same-day revert: this file briefly built one struct-valued `CASE` per
  transition pair (mirroring `is_viable_d`'s own shape), wrapped in a
  subquery and destructured back into columns. Real experiment
  (`experiments/q1_length_sweep/run_general_vs_factorized.py`, Q1's real
  aggregate re-authored non-factorized, same NFA/dataset/start vertex as
  the factorized version, result counts confirmed identical at every
  ℓ=2..10) found this combined shape ran ~1.4-1.65x *slower* than the
  equivalent factorized query, even after moving a state-gated check
  earlier (the exact "fixable" scenario `subsec:e5_handcrafted` in the
  paper argues for) -- because the combined shape's own per-hop cost
  (constructing a full multi-key struct inside a `CASE`, subquery-wrapped,
  then destructured) is paid on *every* hop of the whole path, while the
  benefit of state-aware early pruning only fires at the one transition
  that needed it. One flat `CASE` per key avoids the struct/subquery
  machinery entirely -- each key is just its own scalar column, same as
  the factorized path, just with a `CASE` instead of one flat expression.
- **Semantics-preserving:** this is a correctness obligation, not
  just a performance option -- see `tests/test_optimizer.py`'s equivalence
  tests against Stage E's standard query on the same inputs.
- A body that isn't a struct literal where one is expected (the
  only "inlinable sublanguage" this cut supports) raises `UnsupportedError`
  naming the function, rather than silently producing wrong SQL. A real
  UDF-macro fallback isn't built yet -- every built-in library entry and
  every body this compiler has generated so far already fits the
  struct-literal convention, so this hasn't been needed in practice.
"""
from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from .errors import ExecutionError, RefError, UnsupportedError
from .selective_aggregate import (
    DICT_ALIAS,
    EDGE_ALIAS,
    SelectiveAggregate,
    TransitionPair,
    normalize_update_d_body,
    typed_init_d,
)
from .transitions import TransitionsRelation

RECURSIVE_STATE_MAP = {"from_state": "p.q", "to_state": "t.to_state"}


@dataclass(frozen=True)
class OptimizedQuery:
    """Same shape as `standard_sql.StandardQuery` (`.sql`/`.cte`), so
    `execution.run_query` accepts either without changes."""

    sql: str
    cte: str


def _parse(body: str) -> exp.Expression:
    return sqlglot.parse_one(body, read="duckdb")


def _decompose_struct(body: str) -> dict[str, exp.Expression]:
    """Splits a Stage D struct-literal body into one raw (not yet
    rewritten) expression per dictionary key."""
    tree = _parse(body)
    if not isinstance(tree, exp.Struct):
        raise UnsupportedError(
            f"expected a struct literal ({{key: expr, ...}}) to flatten, got: {body!r} "
            f"-- Stage F's inliner only supports Stage D's own struct-literal convention",
            locus=body)
    return {prop.this.name: prop.expression for prop in tree.expressions}


def _struct_literal_node(declared_keys: list[str], *, path_alias: str | None) -> exp.Expression:
    if not declared_keys:
        return exp.Null()
    return exp.Struct(expressions=[
        exp.PropertyEQ(this=exp.to_identifier(key), expression=exp.column(key, table=path_alias))
        for key in declared_keys
    ])


def _rewrite_node(node: exp.Expression, *, path_alias: str | None,
                   state_map: dict[str, str], declared_keys: list[str]) -> exp.Expression:
    """`D.<key>` -> `<path_alias.>key`; bare `D` -> the whole
    dictionary reconstructed as a struct from the flattened columns;
    `from_state`/`to_state` -> whatever `state_map` says; `e.<column>`
    unchanged."""
    def rewrite(child: exp.Expression) -> exp.Expression:
        if isinstance(child, exp.Column):
            if child.table == DICT_ALIAS:
                return exp.column(child.name, table=path_alias)
            if child.table == EDGE_ALIAS:
                return child
            if not child.table and child.name == DICT_ALIAS:
                return _struct_literal_node(declared_keys, path_alias=path_alias)
            if not child.table and child.name in state_map:
                return _parse(state_map[child.name])
        return child

    return node.copy().transform(rewrite)


def _rewrite_sql(body: str, **kwargs) -> str:
    return _rewrite_node(_parse(body), **kwargs).sql(dialect="duckdb")


def _flatten_update_d(aggregate: SelectiveAggregate, *, declared_keys: list[str]) -> dict[str, str]:
    """Returns one flattened+inlined SQL expression per dictionary key.
    `update_d` is normalized first (`normalize_update_d_body`) -- both to
    its struct-literal-with-every-key-covered form (a key a partial body
    leaves out flattens to "keep the previous column's value" instead of a
    `KeyError`) and, if it was written as one or more `D.<key> = <expr>`
    assignments, into the equivalent struct literal `_decompose_struct`
    already knows how to handle. The same normalization Stage E applies
    before pasting into a macro, so the two stages agree.

    Non-factorized: one flat `CASE` per key (see this module's docstring
    for why -- measured faster than one combined struct-valued `CASE`)."""
    if aggregate.factorized:
        normalized = normalize_update_d_body(aggregate.update_d, declared_keys=declared_keys)
        fields = _decompose_struct(normalized)
        return {
            key: _rewrite_node(fields[key], path_alias="p", state_map=RECURSIVE_STATE_MAP,
                                declared_keys=declared_keys).sql(dialect="duckdb")
            for key in declared_keys
        }

    per_pair_fields = {
        pair: _decompose_struct(normalize_update_d_body(body, declared_keys=declared_keys))
        for pair, body in aggregate.update_d.items()
    }
    result: dict[str, str] = {}
    for key in declared_keys:
        inlined_by_pair = {
            pair: _rewrite_node(per_pair_fields[pair][key], path_alias="p",
                                 state_map=RECURSIVE_STATE_MAP,
                                 declared_keys=declared_keys).sql(dialect="duckdb")
            for pair in per_pair_fields
        }
        distinct_bodies = set(inlined_by_pair.values())
        if len(distinct_bodies) == 1:
            # Every transition pair updates this key identically -- a real,
            # measured win, not just a simplification: dispatching on
            # from_state/to_state here is pure waste when nothing actually
            # varies by state, and it's paid on every hop of every path, so
            # it's worth detecting rather than always building the CASE.
            result[key] = next(iter(distinct_bodies))
        else:
            branch_lines = [f"        WHEN t.from_state = {frm} AND t.to_state = {to} THEN ({inlined})"
                             for (frm, to), inlined in sorted(inlined_by_pair.items())]
            result[key] = "CASE\n" + "\n".join(branch_lines) + f"\n        ELSE p.{key}\n    END"
    return result


def _flatten_is_viable_d(aggregate: SelectiveAggregate, *, declared_keys: list[str]) -> str:
    """Returns a single flattened+inlined boolean expression (a `CASE` over
    transition pairs when non-factorized)."""
    if aggregate.factorized:
        return _rewrite_sql(aggregate.is_viable_d, path_alias="p",
                             state_map=RECURSIVE_STATE_MAP, declared_keys=declared_keys)

    branch_lines = []
    for (frm, to), body in sorted(aggregate.is_viable_d.items()):
        inlined = _rewrite_sql(body, path_alias="p", state_map=RECURSIVE_STATE_MAP,
                                declared_keys=declared_keys)
        branch_lines.append(f"        WHEN t.from_state = {frm} AND t.to_state = {to} THEN ({inlined})")
    return "CASE\n" + "\n".join(branch_lines) + "\n        ELSE TRUE\n    END"


def build_optimized_query(*, aggregate: SelectiveAggregate, relation: TransitionsRelation,
                           start_vertices: list[int], length_bound: int,
                           edges_table: str = "edges",
                           transitions_table: str = "transitions") -> OptimizedQuery:
    """The flattened, inlined equivalent of `standard_sql.build_standard_query`
    for the same inputs -- same anchor-seeding fix, same join
    structure, but no `D` struct column and no macro calls internally.
    `D` is still reconstructed as a struct in the *output*
    columns (`D`/`result`) for a readable, apples-to-apples comparison
    against the standard query's output shape.

    A "no regex" query is not a separate code path here either -- see
    `standard_sql.build_standard_query`'s docstring; both go through
    `transitions.trivial_relation()` instead."""
    if not start_vertices:
        raise ExecutionError("no start vertices given; nothing to seed the anchor with")
    if not relation.accepting_states:
        raise ExecutionError("NFA has no accepting states; the query can never match")

    declared_keys = [key.name for key in aggregate.dictionary_keys]
    seed_values = ", ".join(f"({v}::BIGINT)" for v in sorted(start_vertices))
    accepting = ", ".join(str(q) for q in sorted(relation.accepting_states))

    init_fields = _decompose_struct(typed_init_d(aggregate)) if declared_keys else {}
    missing_init = [key for key in declared_keys if key not in init_fields]
    if missing_init:
        # Unlike update_d, there's no previous value to default a missing
        # key to at initialization -- this must be a real, reported error,
        # not a silent default (and not a raw KeyError on first use below).
        raise RefError(f"init_d does not initialize declared key(s): {missing_init}", locus="init_d")
    anchor_cols = "".join(
        f", ({init_fields[key].sql(dialect='duckdb')}) AS {key}" for key in declared_keys)

    update_exprs = _flatten_update_d(aggregate, declared_keys=declared_keys) if declared_keys else {}
    viable_expr = _flatten_is_viable_d(aggregate, declared_keys=declared_keys)
    update_cols = "".join(f", ({update_exprs[key]}) AS {key}" for key in declared_keys)

    d_struct_sql = _struct_literal_node(declared_keys, path_alias=None).sql(dialect="duckdb")
    viable_final_expr = _rewrite_sql(aggregate.is_viable_d_final, path_alias=None,
                                      state_map={}, declared_keys=declared_keys)
    finalize_expr = _rewrite_sql(aggregate.finalize_d, path_alias=None,
                                  state_map={}, declared_keys=declared_keys)

    cte = f"""paths AS (
    SELECT s.v AS v, {relation.q0} AS q{anchor_cols}, 0 AS path_length
    FROM (VALUES {seed_values}) AS s(v)
    UNION ALL
    SELECT e.dst AS v, t.to_state AS q{update_cols},
           p.path_length + 1 AS path_length
    FROM paths p
    JOIN {edges_table} e ON e.src = p.v
    JOIN {transitions_table} t ON t.from_state = p.q AND t.label = e.label
    WHERE p.path_length < {length_bound}
      AND ({viable_expr})
)""".strip()

    sql = (f"WITH RECURSIVE {cte}\n"
           f"SELECT v, q, {d_struct_sql} AS D, path_length, ({finalize_expr}) AS result\n"
           f"FROM paths\n"
           f"WHERE q IN ({accepting}) AND ({viable_final_expr})")

    return OptimizedQuery(sql=sql, cte=cte)
