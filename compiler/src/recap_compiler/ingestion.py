"""Stage A: data ingestion (FR-1..FR-4).

Loads a graph's edges (and optional vertices) into DuckDB tables, from a CSV
path or an already-registered DuckDB table name, and selects start vertices
by id list, property predicate, or out-degree quantile band.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import duckdb

from .errors import IngestionError

REQUIRED_EDGE_COLUMNS = {"src", "dst", "label"}
REQUIRED_VERTEX_COLUMN = "id"

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


def load_graph(conn: duckdb.DuckDBPyConnection, edges_source: str,
               vertices_source: str | None = None,
               edge_type_overrides: dict[str, str] | None = None,
               vertex_type_overrides: dict[str, str] | None = None) -> GraphHandle:
    """FR-1..FR-3: load edges (required src/dst/label + properties) and,
    optionally, vertices (required id + properties; inferred from edges when
    absent). `edges_source`/`vertices_source` may be a CSV path or the name
    of a table already registered on `conn`."""
    _load_relation(conn, edges_source, "edges", REQUIRED_EDGE_COLUMNS, edge_type_overrides)

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
    """FR-4: select start vertices by exactly one of an explicit id list, a
    SQL predicate over vertex properties, or an out-degree quantile band
    (`'low'` <25%, `'medium'` 25-75%, `'high'` >75%)."""
    conn = handle.conn
    given = [name for name, value in
             (("ids", ids), ("predicate", predicate), ("degree_band", degree_band))
             if value is not None]
    if len(given) != 1:
        raise IngestionError(
            "select_start_vertices requires exactly one of ids/predicate/degree_band, "
            f"got: {given or 'none'}")

    if ids is not None:
        return sorted(ids)

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
