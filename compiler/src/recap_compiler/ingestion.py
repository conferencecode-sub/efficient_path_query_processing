"""Stage A: data ingestion.

Loads a graph's edges (and optional vertices) into DuckDB tables, from a CSV
path or an already-registered DuckDB table name, and selects start vertices
by id list, property predicate, or out-degree quantile band.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import duckdb

from .errors import IngestionError
from .transitions import TRIVIAL_LABEL

REQUIRED_EDGE_COLUMNS = {"src", "dst"}
REQUIRED_VERTEX_COLUMN = "id"
EDGE_ID_COLUMN = "edge_id"  # matches selective_aggregate.trail_via_edge_ids's default id_column

_DEGREE_BAND_CONDITIONS = {
    "low": "d.out_degree < b.q25",
    "medium": "d.out_degree >= b.q25 AND d.out_degree <= b.q75",
    "high": "d.out_degree > b.q75",
}


@dataclass(frozen=True)
class GraphHandle:
    """Names of the DuckDB tables backing a loaded graph."""

    conn: duckdb.DuckDBPyConnection
    edges_table: str = "edges"
    nodes_table: str = "nodes"


def _is_csv_path(source: str) -> bool:
    return source.lower().endswith(".csv") or os.path.sep in source or os.path.exists(source)


def _load_relation(conn: duckdb.DuckDBPyConnection, source: str, table_name: str,
                    required_columns: set[str],
                    type_overrides: dict[str, str] | None) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    if _is_csv_path(source):
        if not os.path.exists(source):
            raise IngestionError(f"CSV file not found: {source}", locus=source)
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)", [source])
    else:
        try:
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {source}")
        except duckdb.Error as exc:
            raise IngestionError(
                f"could not read source table '{source}': {exc}", locus=source) from exc

    actual_columns = {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}
    missing = required_columns - actual_columns
    if missing:
        conn.execute(f"DROP TABLE {table_name}")
        raise IngestionError(
            f"missing required column(s) {sorted(missing)} in '{source}'", locus=source)

    # Column names are checked against the schema just read back from DuckDB
    # (not arbitrary external input) before being interpolated below.
    for column, new_type in (type_overrides or {}).items():
        if column not in actual_columns:
            raise IngestionError(
                f"cannot override type of unknown column '{column}'", locus=column)
        conn.execute(f'ALTER TABLE {table_name} ALTER COLUMN "{column}" TYPE {new_type}')


def _ensure_edge_id(conn: duckdb.DuckDBPyConnection, table_name: str = "edges") -> None:
    """Not every edges source has its own unique id column, but trail
    semantics (no repeated edge) need one to detect repeats. Adds a
    synthetic `edge_id` (row position, 0-based) if the loaded data doesn't
    already have one -- a no-op when it does, so a real id column is never
    overwritten."""
    columns = {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}
    if EDGE_ID_COLUMN not in columns:
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT row_number() OVER () - 1 AS {EDGE_ID_COLUMN}, * FROM {table_name}"
        )


def _replace_label_column(conn: duckdb.DuckDBPyConnection, table_name: str, label_expr_sql: str) -> None:
    """Shared by `set_label_column`/`set_trivial_label_column`: rebuilds
    `table_name` with a `label` column computed as `label_expr_sql`,
    dropping any pre-existing `label` column first so the two never
    collide into an ambiguous duplicate."""
    columns = [row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()]
    keep = [c for c in columns if c != "label"]
    select_list = ", ".join(f'"{c}"' for c in keep)
    conn.execute(
        f'CREATE OR REPLACE TABLE {table_name} AS '
        f'SELECT {select_list}, {label_expr_sql} AS label FROM {table_name}'
    )


def set_label_column(conn: duckdb.DuckDBPyConnection, column: str, *,
                      table_name: str = "edges") -> None:
    """Designates `column` as the regex-matching label column by deriving a
    `label` column from it (`CAST(column AS VARCHAR)`), for a graph whose
    label-carrying column isn't literally named `label` -- a regex query
    can be built over any string column this way, not just one with that
    exact name. `column` itself is left untouched (any aggregate reading
    `e.<column>` directly, or a second call choosing a different column,
    both keep working); a pre-existing `label` column, if any, is replaced
    rather than duplicated. A no-op if `column` already *is* `label`."""
    columns = {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}
    if column not in columns:
        raise IngestionError(f"cannot use unknown column '{column}' as the label column", locus=column)
    if column == "label":
        return
    _replace_label_column(conn, table_name, f'CAST("{column}" AS VARCHAR)')


def set_trivial_label_column(conn: duckdb.DuckDBPyConnection, *, table_name: str = "edges") -> None:
    """Sets every edge's `label` to the constant `transitions.TRIVIAL_LABEL`,
    for a "no regex" query: paired with `transitions.trivial_relation()`'s
    single-state self-loop on that same constant, every edge matches
    regardless of any real label it may have -- so a regex-less query is
    still built and run through the exact same Stage E/F automaton-based
    code path as a real regex, not a separate one. Real label values are
    irrelevant here by construction (the whole point is a query that
    doesn't filter by label), so a pre-existing `label` column is
    replaced, same as `set_label_column`."""
    _replace_label_column(conn, table_name, f"'{TRIVIAL_LABEL}'")


def load_graph(conn: duckdb.DuckDBPyConnection, edges_source: str,
               vertices_source: str | None = None,
               edge_type_overrides: dict[str, str] | None = None,
               vertex_type_overrides: dict[str, str] | None = None,
               label_column: str | None = None) -> GraphHandle:
    """Load edges (required src/dst + properties) and,
    optionally, vertices (required id + properties; inferred from edges when
    absent). `edges_source`/`vertices_source` may be a CSV path or the name
    of a table already registered on `conn`. Guarantees an `edge_id` column
    exists on `edges` (trail semantics need one), synthesizing
    one from row position if the source didn't already have it.

    `label` is no longer a required column -- a graph with no notion of
    "label" at all is a valid input (see `set_trivial_label_column` for
    running a "no regex" query over it). `label_column`, if given, is
    passed straight to `set_label_column` after loading -- a convenience
    for the common case of loading and designating a label column in one
    call."""
    _load_relation(conn, edges_source, "edges", REQUIRED_EDGE_COLUMNS, edge_type_overrides)
    _ensure_edge_id(conn)
    if label_column is not None:
        set_label_column(conn, label_column)

    if vertices_source is not None:
        _load_relation(conn, vertices_source, "nodes", {REQUIRED_VERTEX_COLUMN},
                        vertex_type_overrides)
    else:
        conn.execute("DROP TABLE IF EXISTS nodes")
        conn.execute(
            "CREATE TABLE nodes AS "
            "SELECT src AS id FROM edges UNION SELECT dst AS id FROM edges"
        )

    return GraphHandle(conn=conn)


def select_start_vertices(handle: GraphHandle, *, ids: list[int] | None = None,
                           predicate: str | None = None,
                           degree_band: str | None = None) -> list[int]:
    """Select start vertices by an explicit id list, a SQL predicate
    over vertex properties, or an out-degree quantile band (`'low'` <25%,
    `'medium'` 25-75%, `'high'` >75%). At most one of these may be given.

    **Amended (2026-08-17):** if none is given, every distinct `src`
    value in the Edges table is used (the all-vertices default) -- not
    every vertex in `nodes`, since a vertex with no outgoing edges has no
    path to explore from and would just be a pointless start."""
    conn = handle.conn
    given = [name for name, value in
             (("ids", ids), ("predicate", predicate), ("degree_band", degree_band))
             if value is not None]
    if len(given) > 1:
        raise IngestionError(
            "select_start_vertices accepts at most one of ids/predicate/degree_band, "
            f"got: {given}")

    if ids is not None:
        return sorted(ids)

    if not given:
        rows = conn.execute(
            f"SELECT DISTINCT src FROM {handle.edges_table} ORDER BY src"
        ).fetchall()
        return [row[0] for row in rows]

    if predicate is not None:
        rows = conn.execute(
            f"SELECT id FROM {handle.nodes_table} WHERE {predicate} ORDER BY id"
        ).fetchall()
        return [row[0] for row in rows]

    if degree_band not in _DEGREE_BAND_CONDITIONS:
        raise IngestionError(
            f"unknown degree_band '{degree_band}', expected one of "
            f"{sorted(_DEGREE_BAND_CONDITIONS)}")
    condition = _DEGREE_BAND_CONDITIONS[degree_band]
    query = f"""
    WITH degree AS (
        SELECT n.id AS id, COUNT(e.src) AS out_degree
        FROM {handle.nodes_table} n
        LEFT JOIN {handle.edges_table} e ON e.src = n.id
        GROUP BY n.id
    ),
    bounds AS (
        SELECT quantile_cont(out_degree, 0.25) AS q25,
               quantile_cont(out_degree, 0.75) AS q75
        FROM degree
    )
    SELECT d.id FROM degree d, bounds b WHERE {condition} ORDER BY d.id
    """
    return [row[0] for row in conn.execute(query).fetchall()]
