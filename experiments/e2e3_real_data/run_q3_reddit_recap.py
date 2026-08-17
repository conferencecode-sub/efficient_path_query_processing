import os, sys, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "q3_length_sweep"))
import duckdb
from q3_aggregate import q3_aggregate
from recap_compiler.execution import run_query
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.selective_aggregate import validate_selective_aggregate
from recap_compiler.standard_sql import materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import trivial_relation
from types import SimpleNamespace

EDGES = "../datasets/reddit/edges.csv"
NODES = "../datasets/reddit/nodes.csv"
START_VERTEX = 31470

def with_min_length(q, m):
    return SimpleNamespace(sql=f"SELECT * FROM ({q.sql}) t WHERE path_length >= {m}", cte=q.cte)

for length in (2,3,4,5,6):
    conn = duckdb.connect()
    handle = load_graph(conn, EDGES, NODES)
    set_trivial_label_column(conn)
    aggregate = q3_aggregate()
    relation = trivial_relation()
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)
    q = build_optimized_query(aggregate=aggregate, relation=relation, start_vertices=starts, length_bound=length)
    q = with_min_length(q, 2)
    result = run_query(conn, q, result_shape="count")
    print(f"length={length}: {result.rows[0][0]} paths, runtime={result.telemetry.runtime_ms:.2f}ms, intermediate={result.telemetry.intermediate_paths}")
    conn.close()
