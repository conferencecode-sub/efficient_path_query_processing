"""Stage E: standard ReCAP SQL generation (FR-15..FR-18).

No flattening or inlining yet -- that's Stage F, not built. Instead, the
selective aggregate's five bodies (Stage D) are pasted **verbatim** as the
bodies of DuckDB SQL macros named after Definition 8's own functions
(`init_d`, `update_d`, `is_viable_d`, `is_viable_d_final`, `finalize_d`), and
the generated recursive CTE just calls them by name. This works because a
DuckDB macro's struct-typed parameter supports `.field` access exactly like
a table alias does (confirmed empirically), so Stage D's `D.<key>`/
`e.<column>` convention needs no rewriting at all to become real SQL --
FR-18's default JSON/struct representation of `D` falls out for free.

For a non-factorized aggregate, `update_d`/`is_viable_d` are a dict of
per-`(from_state, to_state)` bodies (Stage D); those become a `CASE` over
the transition pair inside the macro body, matching FR-12/FR-21 (the
`CASE` a human author would otherwise have hand-written).
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .errors import ExecutionError
from .selective_aggregate import DICT_ALIAS, EDGE_ALIAS, SelectiveAggregate, TransitionPair
from .transitions import TransitionsRelation, to_dataframe

MACRO_SIGNATURES = {
    "init_d": (),
    "update_d": (DICT_ALIAS, "from_state", "to_state", EDGE_ALIAS),
    "is_viable_d": (DICT_ALIAS, "from_state", "to_state", EDGE_ALIAS),
    "is_viable_d_final": (DICT_ALIAS,),
    "finalize_d": (DICT_ALIAS,),
}


@dataclass(frozen=True)
class StandardQuery:
    """FR-25: the generated SQL is a first-class, inspectable artifact.
    `cte` is the reusable `paths AS (...)` fragment (no `WITH RECURSIVE`
    prefix, no outer `SELECT`) -- Stage G reuses it to separately count
    intermediate paths explored (FR-26) before the outer filter is applied,
    since that count isn't otherwise recoverable from `sql` alone."""

    sql: str
    cte: str
    transitions_table: str


def _case_expression(bodies: dict[TransitionPair, str], *, default: str) -> str:
    branches = "\n".join(
        f"        WHEN from_state = {frm} AND to_state = {to} THEN ({body})"
        for (frm, to), body in sorted(bodies.items()))
    return f"CASE\n{branches}\n        ELSE {default}\n    END"


def register_aggregate_macros(conn: duckdb.DuckDBPyConnection, aggregate: SelectiveAggregate) -> None:
    """Pastes each of the five (already FR-14-validated) function bodies
    into a `CREATE OR REPLACE MACRO`, exactly as authored."""
    conn.execute(f"CREATE OR REPLACE MACRO init_d() AS ({aggregate.init_d})")
    conn.execute(f"CREATE OR REPLACE MACRO is_viable_d_final(D) AS ({aggregate.is_viable_d_final})")
    conn.execute(f"CREATE OR REPLACE MACRO finalize_d(D) AS ({aggregate.finalize_d})")

    for name, body, default in (("update_d", aggregate.update_d, "D"),
                                 ("is_viable_d", aggregate.is_viable_d, "TRUE")):
        params = ", ".join(MACRO_SIGNATURES[name])
        expr = body if aggregate.factorized else _case_expression(body, default=default)
        conn.execute(f"CREATE OR REPLACE MACRO {name}({params}) AS ({expr})")


def materialize_transitions(conn: duckdb.DuckDBPyConnection, relation: TransitionsRelation,
                             *, table_name: str = "transitions") -> None:
    """FR-9's `T(from_state, to_state, label)` as a real DuckDB table, so the
    generated query can join against it by name."""
    df = to_dataframe(relation)
    conn.register("_transitions_df", df)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _transitions_df")
    conn.unregister("_transitions_df")


def build_standard_query(*, relation: TransitionsRelation, start_vertices: list[int],
                          length_bound: int, edges_table: str = "edges",
                          transitions_table: str = "transitions") -> StandardQuery:
    """FR-15..FR-18: the standard ReCAP query -- an anchor seeded from every
    start vertex (FR-16, fixing R4.O3's unbound-`s` defect: `s` is introduced
    via a `FROM` over a literal start-vertex relation, never a free
    variable), a recursive member joining `Paths ⋈ Edges ⋈ T`, and an outer
    query filtering on the accepting states and `is_viable_d_final`. Always
    produces the full-paths shape; Stage G wraps this for the
    endpoints/count shapes (FR-24), so result shaping stays out of SQL
    generation. `register_aggregate_macros`/`materialize_transitions` must
    have already been run on the same connection this SQL will execute
    against."""
    if not start_vertices:
        raise ExecutionError("no start vertices given; nothing to seed the anchor with")
    if not relation.accepting_states:
        raise ExecutionError("NFA has no accepting states; the query can never match")

    seed_values = ", ".join(f"({v})" for v in sorted(start_vertices))
    accepting = ", ".join(str(q) for q in sorted(relation.accepting_states))

    cte = f"""paths AS (
    SELECT s.v AS v, {relation.q0} AS q, init_d() AS D, 1 AS path_length
    FROM (VALUES {seed_values}) AS s(v)
    UNION ALL
    SELECT e.dst AS v, t.to_state AS q,
           update_d(p.D, p.q, t.to_state, e) AS D,
           p.path_length + 1 AS path_length
    FROM paths p
    JOIN {edges_table} e ON e.src = p.v
    JOIN {transitions_table} t ON t.from_state = p.q AND t.label = e.label
    WHERE p.path_length < {length_bound}
      AND is_viable_d(p.D, p.q, t.to_state, e)
)""".strip()

    sql = (f"WITH RECURSIVE {cte}\n"
           f"SELECT v, q, D, path_length, finalize_d(D) AS result\n"
           f"FROM paths\n"
           f"WHERE q IN ({accepting}) AND is_viable_d_final(D)")

    return StandardQuery(sql=sql, cte=cte, transitions_table=transitions_table)
