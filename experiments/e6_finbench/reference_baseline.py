"""A monolithic, hand-written recursive-CTE reference for each TCR query,
mirroring the official Cypher's exact semantics -- used only to cross-check
ReCAP's aggregate-driven queries at small length bounds before trusting them
at scale, same "verify against a known-correct baseline" discipline used
throughout this project (e.g. `e5_handcrafted_vs_recap/baseline_full.py`).
"""
from __future__ import annotations

import duckdb

from common import END_TIME, START_TIME


def tcr1_reference(conn: duckdb.DuckDBPyConnection, start_vertex: int, max_length: int) -> int:
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT CAST({start_vertex} AS BIGINT) AS v, 0 AS state, CAST(NULL AS BIGINT) AS last_transfer_time,
               [] AS edge_ids, 0 AS path_length
        UNION ALL
        SELECT e.dst AS v,
               CASE WHEN e.label = 'transfer' THEN 1 ELSE 2 END AS state,
               CASE WHEN e.label = 'transfer' THEN e.timestamp_ms ELSE p.last_transfer_time END,
               list_append(p.edge_ids, e.edge_id), p.path_length + 1
        FROM paths p
        JOIN edges e ON e.src = p.v
        WHERE p.path_length < {max_length}
          AND NOT list_contains(p.edge_ids, e.edge_id)
          AND e.timestamp_ms > {START_TIME} AND e.timestamp_ms < {END_TIME}
          AND ((p.state = 0 AND e.label = 'transfer')
               OR (p.state = 1 AND e.label = 'transfer' AND e.timestamp_ms > p.last_transfer_time)
               OR (p.state = 1 AND e.label = 'signedInBy'))
    )
    SELECT COUNT(*) FROM paths WHERE state = 2
    """
    return conn.execute(query).fetchone()[0]


def tcr5_reference(conn: duckdb.DuckDBPyConnection, start_vertex: int, max_length: int) -> int:
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT CAST({start_vertex} AS BIGINT) AS v, 0 AS state, CAST(NULL AS BIGINT) AS last_transfer_time,
               [] AS edge_ids, 0 AS path_length
        UNION ALL
        SELECT e.dst AS v,
               CASE WHEN p.state = 0 THEN 1 ELSE 2 END AS state,
               CASE WHEN e.label = 'transfer' THEN e.timestamp_ms ELSE p.last_transfer_time END,
               list_append(p.edge_ids, e.edge_id), p.path_length + 1
        FROM paths p
        JOIN edges e ON e.src = p.v
        WHERE p.path_length < {max_length}
          AND NOT list_contains(p.edge_ids, e.edge_id)
          AND ((p.state = 0 AND e.label = 'own')
               OR (p.state IN (1, 2) AND e.label = 'transfer'
                   AND e.timestamp_ms > {START_TIME} AND e.timestamp_ms < {END_TIME}
                   AND (p.last_transfer_time IS NULL OR e.timestamp_ms > p.last_transfer_time)))
    )
    SELECT COUNT(*) FROM paths WHERE state = 2
    """
    return conn.execute(query).fetchone()[0]


def tcr8_reference(conn: duckdb.DuckDBPyConnection, start_vertex: int, max_length: int, threshold: float) -> int:
    query = f"""
    WITH RECURSIVE paths AS (
        SELECT CAST({start_vertex} AS BIGINT) AS v, 0 AS state, CAST(NULL AS DOUBLE) AS last_234_amount,
               [] AS edge_ids, 0 AS path_length
        UNION ALL
        SELECT e.dst AS v,
               CASE WHEN p.state = 0 THEN 1 ELSE 2 END AS state,
               CASE WHEN e.label IN ('transfer', 'withdraw') THEN e.amount ELSE p.last_234_amount END,
               list_append(p.edge_ids, e.edge_id), p.path_length + 1
        FROM paths p
        JOIN edges e ON e.src = p.v
        WHERE p.path_length < {max_length}
          AND NOT list_contains(p.edge_ids, e.edge_id)
          AND ((p.state = 0 AND e.label = 'deposit'
                AND e.timestamp_ms > {START_TIME} AND e.timestamp_ms < {END_TIME})
               OR (p.state IN (1, 2) AND e.label IN ('transfer', 'withdraw')
                   AND e.timestamp_ms > {START_TIME} AND e.timestamp_ms < {END_TIME}
                   AND (p.last_234_amount IS NULL OR e.amount > p.last_234_amount * {threshold})))
    )
    SELECT COUNT(*) FROM paths WHERE state = 2
    """
    return conn.execute(query).fetchone()[0]
