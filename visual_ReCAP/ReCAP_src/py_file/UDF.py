#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

# BEGIN GENERATED FUNCTIONS
def init_d():
    return {
        "last_time": None,
        "last_domestic_day": None,
        "foreign_day": None,
        "edge_ids": []
    }

def update_d(D, P_nfa_state, T_to_state, Edge):
    if P_nfa_state == 0 and T_to_state == 1:
        # transition label: purchase
        return D
    if P_nfa_state == 1 and T_to_state == 1:
        # transition label: purchase
        return D
    return D

def is_viable_d(D, P_nfa_state, T_to_state, Edge):
    if P_nfa_state == 0 and T_to_state == 1:
        # transition label: purchase
        return True
    if P_nfa_state == 1 and T_to_state == 1:
        # transition label: purchase
        return True
    return True

def finalize_d(D):
    return {
        **D,
        "has_trail": len(D["edge_ids"]) == len(set(D["edge_ids"]))
    }

def is_viable_d_final(D):
    if not D["has_trail"]:
        return False
    if D["last_domestic_day"] is None or D["foreign_day"] is None:
        return True
    return abs(D["last_domestic_day"] - D["foreign_day"]) <= 3
# END GENERATED FUNCTIONS

EDGE_ACCESS_COLUMNS = [
    "edge_id",
    "timestamp_ms",
    "src",
    "dst",
    "amount",
    "label",
    "location_region",
    "purchase_pattern",
    "age_group",
    "risk_score",
    "anomaly",
]


def _to_json(value: Any) -> str:
    return json.dumps(value, default=str)


def recap_init_d() -> str:
    return _to_json(init_d())


def recap_update_d(d_json: str, from_state: int, to_state: int, edge_json: str) -> str:
    D = json.loads(d_json)
    Edge = json.loads(edge_json)
    updated = update_d(D, from_state, to_state, Edge)
    return _to_json(updated)


def recap_is_viable_d(d_json: str, from_state: int, to_state: int, edge_json: str) -> bool:
    D = json.loads(d_json)
    Edge = json.loads(edge_json)
    return bool(is_viable_d(D, from_state, to_state, Edge))


def recap_finalize_d(d_json: str) -> str:
    D = json.loads(d_json)
    finalized = finalize_d(D)
    return _to_json(finalized)


def recap_is_viable_d_final(d_json: str) -> bool:
    D = json.loads(d_json)
    return bool(is_viable_d_final(D))


class ReCAPRunner:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self.register_udfs()

    def register_udfs(self) -> None:
        self.conn.create_function(
            "recap_init_d",
            recap_init_d,
            parameters=[],
            return_type="VARCHAR",
        )
        self.conn.create_function(
            "recap_update_d",
            recap_update_d,
            parameters=["VARCHAR", "BIGINT", "BIGINT", "VARCHAR"],
            return_type="VARCHAR",
        )
        self.conn.create_function(
            "recap_is_viable_d",
            recap_is_viable_d,
            parameters=["VARCHAR", "BIGINT", "BIGINT", "VARCHAR"],
            return_type="BOOLEAN",
        )
        self.conn.create_function(
            "recap_finalize_d",
            recap_finalize_d,
            parameters=["VARCHAR"],
            return_type="VARCHAR",
        )
        self.conn.create_function(
            "recap_is_viable_d_final",
            recap_is_viable_d_final,
            parameters=["VARCHAR"],
            return_type="BOOLEAN",
        )

    def load_data(
        self,
        nodes_path: str,
        edges_path: str,
        nfa_nodes_path: str,
        nfa_edges_path: str,
    ) -> None:
        nodes_df = pd.read_csv(nodes_path)
        if "id" not in nodes_df.columns:
            nodes_df.columns = ["name", "id"]
        nodes_df = nodes_df[["name", "id"]]

        edges_df = pd.read_csv(edges_path)
        if "edge_id" not in edges_df.columns:
            edges_df["edge_id"] = range(1, len(edges_df) + 1)
        if "from" in edges_df.columns and "to" in edges_df.columns:
            edges_df = edges_df.rename(columns={"from": "src", "to": "dst"})

        def _edge_to_json(row: pd.Series) -> str:
            payload = {}
            for key, value in row.items():
                if key not in EDGE_ACCESS_COLUMNS:
                    continue
                if pd.isna(value):
                    continue
                payload[key] = value.item() if hasattr(value, "item") else value
            # Provide UI-friendly aliases expected by the editable Python examples.
            if "timestamp_ms" in payload and "time" not in payload:
                payload["time"] = payload["timestamp_ms"]
            if "hour_of_day" in payload and "day" not in payload:
                payload["day"] = payload["hour_of_day"]
            if "edge_id" in payload and "id" not in payload:
                payload["id"] = payload["edge_id"]
            return json.dumps(payload, default=str)

        edges_df["edge_json"] = edges_df.apply(_edge_to_json, axis=1)

        nfa_edges_df = pd.read_csv(nfa_edges_path)
        if "from" in nfa_edges_df.columns and "to" in nfa_edges_df.columns:
            nfa_edges_df = nfa_edges_df.rename(columns={"from": "from_state", "to": "to_state"})

        nfa_nodes_df = pd.read_csv(nfa_nodes_path)

        self.conn.execute("DROP TABLE IF EXISTS nodes")
        self.conn.execute("DROP TABLE IF EXISTS edges")
        self.conn.execute("DROP TABLE IF EXISTS nfa_edges")
        self.conn.execute("DROP TABLE IF EXISTS nfa_nodes")

        self.conn.register("nodes_df", nodes_df)
        self.conn.register("edges_df", edges_df)
        self.conn.register("nfa_edges_df", nfa_edges_df)
        self.conn.register("nfa_nodes_df", nfa_nodes_df)

        self.conn.execute("CREATE TABLE nodes AS SELECT * FROM nodes_df")
        self.conn.execute("CREATE TABLE edges AS SELECT * FROM edges_df")
        self.conn.execute("CREATE TABLE nfa_edges AS SELECT * FROM nfa_edges_df")
        self.conn.execute("CREATE TABLE nfa_nodes AS SELECT * FROM nfa_nodes_df")

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_src_label ON edges(src, label)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_nfa_from_label ON nfa_edges(from_state, label)")

    def run_query(self, start_node: int, min_length: int, max_length: int) -> int:
        initial_state = self.conn.execute(
            "SELECT id FROM nfa_nodes WHERE type = 'initial' LIMIT 1"
        ).fetchone()
        if not initial_state:
            raise ValueError("No initial state found in nfa_nodes.csv")

        accepting_states = [
            row[0]
            for row in self.conn.execute(
                "SELECT id FROM nfa_nodes WHERE type = 'accepting' ORDER BY id"
            ).fetchall()
        ]
        if not accepting_states:
            raise ValueError("No accepting states found in nfa_nodes.csv")

        placeholders = ",".join("?" for _ in accepting_states)
        query = f"""
        WITH RECURSIVE paths AS (
            SELECT
                ? AS current_node,
                ? AS nfa_state,
                recap_init_d() AS d,
                0 AS path_length

            UNION ALL

            SELECT
                e.dst AS current_node,
                n.to_state AS nfa_state,
                recap_update_d(p.d, p.nfa_state, n.to_state, e.edge_json) AS d,
                p.path_length + 1 AS path_length
            FROM paths p
            JOIN nfa_edges n
              ON p.nfa_state = n.from_state
            JOIN edges e
              ON e.src = p.current_node
             AND e.label = n.label
            WHERE p.path_length < ?
              AND recap_is_viable_d(p.d, p.nfa_state, n.to_state, e.edge_json)
        )
        SELECT COUNT(*)
        FROM paths
        WHERE path_length BETWEEN ? AND ?
          AND nfa_state IN ({placeholders})
          AND recap_is_viable_d_final(recap_finalize_d(d))
        """
        params = [start_node, initial_state[0], max_length, min_length, max_length, *accepting_states]
        return int(self.conn.execute(query, params).fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ReCAP with generated UDFs and NFA CSVs")
    parser.add_argument("--edges", required=True, help="Path to LG.csv")
    parser.add_argument("--nodes", required=True, help="Path to LG_V.csv")
    parser.add_argument("--nfanodes", required=True, help="Path to nfa_nodes.csv")
    parser.add_argument("--nfa", required=True, help="Path to nfa.csv")
    parser.add_argument("--start", required=True, type=int, help="Start node id")
    parser.add_argument("--min-length", required=True, type=int, help="Lower path bound")
    parser.add_argument("--max-length", required=True, type=int, help="Upper path bound")
    args = parser.parse_args()

    runner = ReCAPRunner()
    runner.load_data(args.nodes, args.edges, args.nfanodes, args.nfa)
    count = runner.run_query(args.start, args.min_length, args.max_length)
    print(f"Number of paths [{args.min_length}, {args.max_length}] from {args.start} are: {count}")


if __name__ == "__main__":
    main()
