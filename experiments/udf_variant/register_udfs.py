"""Generic Python-UDF registration for the "udf-variant" ablation.

`standard_sql.build_standard_query` generates a recursive CTE that calls
exactly five fixed names (`init_d`, `update_d`, `is_viable_d`,
`is_viable_d_final`, `finalize_d`, per `standard_sql.MACRO_SIGNATURES`)
regardless of how they're registered -- `register_aggregate_macros`
installs them as DuckDB SQL macros with `D` as a native STRUCT; this
module installs the *same five names* as real Python UDFs instead, with
`D` as a JSON string (VARCHAR), matching the old hand-built prototype's
own convention (`ReCAP/q1/recap_sql_udfs.py`'s `py_*` functions) so this
reproduces the same overhead source (per-call UDF dispatch + JSON
marshalling) the paper's 150-346x number is attributed to. Stage F
(`build_optimized_query`) never calls these names at all, so it is
unaffected by which registration function is used.

The edge struct type is built dynamically from `DESCRIBE {edges_table}`
so this is generic across every query's own edge schema -- no
per-dataset special-casing.
"""
from __future__ import annotations

from typing import Callable

import duckdb


def register_aggregate_udfs(
    conn: duckdb.DuckDBPyConnection,
    *,
    init_d: Callable[[], str],
    update_d: Callable[[str, int, int, dict], str],
    is_viable_d: Callable[[str, int, int, dict], bool],
    is_viable_d_final: Callable[[str], bool],
    finalize_d: Callable[[str], str],
    edges_table: str = "edges",
) -> None:
    """Registers the five given Python callables under the fixed names
    the generated SQL calls, with `D` as VARCHAR (JSON) throughout."""
    columns = conn.execute(f"DESCRIBE {edges_table}").fetchall()
    edge_struct = duckdb.struct_type({name: col_type for name, col_type, *_ in columns})

    conn.create_function("init_d", init_d, [], "VARCHAR")
    conn.create_function("update_d", update_d, ["VARCHAR", "BIGINT", "BIGINT", edge_struct], "VARCHAR")
    conn.create_function("is_viable_d", is_viable_d, ["VARCHAR", "BIGINT", "BIGINT", edge_struct], "BOOLEAN")
    conn.create_function("is_viable_d_final", is_viable_d_final, ["VARCHAR"], "BOOLEAN")
    conn.create_function("finalize_d", finalize_d, ["VARCHAR"], "VARCHAR")
