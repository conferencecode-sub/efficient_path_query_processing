"""Tests whether re-authoring Q1 as General (non-factorized) closes the
CompilerOpt-vs-Handcrafted/Split gap `subsec:e5_handcrafted` attributes to
the factorized generator deferring the risk-gateway check to
`is_viable_d_final` -- see `q1_aggregate_general.py`'s own docstring.
Both variants go through the exact same `build_optimized_query` (Stage F),
same NFA, same dataset/start vertex as `run_new_compiler.py`'s own sweep;
only the aggregate's factorized/non-factorized shape differs.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "compiler", "src"))
sys.path.insert(0, os.path.dirname(__file__))

import duckdb  # noqa: E402

from q1_aggregate import q1_aggregate  # noqa: E402
from q1_aggregate_general import q1_aggregate_general  # noqa: E402
from recap_compiler.execution import run_query  # noqa: E402
from recap_compiler.ingestion import load_graph, select_start_vertices  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.regex_frontend import compile_regex_to_nfa  # noqa: E402
from recap_compiler.selective_aggregate import validate_selective_aggregate  # noqa: E402
from recap_compiler.standard_sql import materialize_transitions  # noqa: E402
from recap_compiler.transitions import build_transitions_relation  # noqa: E402

DATASET = os.path.join(os.path.dirname(__file__), "..", "datasets", "metaverse", "edges.csv")
REGEX = "(transfer|purchase|sale)+(phishing|scam)+"
START_VERTEX = 383
LENGTHS = (2, 3, 4, 5, 6, 7, 8, 9, 10)


def _run(aggregate, length):
    conn = duckdb.connect()
    handle = load_graph(conn, DATASET)
    nfa = compile_regex_to_nfa(REGEX, minimize=True)
    relation = build_transitions_relation(nfa)
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    edge_columns = {r[0] for r in conn.execute("DESCRIBE edges").fetchall()}
    validate_selective_aggregate(aggregate, edge_columns=edge_columns, transitions=relation)
    materialize_transitions(conn, relation)
    q = build_optimized_query(aggregate=aggregate, relation=relation,
                               start_vertices=starts, length_bound=length)
    result = run_query(conn, q, result_shape="count")
    conn.close()
    return result.rows[0][0], result.telemetry.runtime_ms


def main():
    print(f"{'len':>3}  {'factorized_result':>17}  {'factorized_ms':>13}  "
          f"{'general_result':>14}  {'general_ms':>10}  {'ratio':>6}")
    for length in LENGTHS:
        fac_result, fac_ms = _run(q1_aggregate(), length)
        gen_result, gen_ms = _run(q1_aggregate_general(), length)
        assert fac_result == gen_result, (
            f"result mismatch at length={length}: factorized={fac_result} general={gen_result}")
        ratio = fac_ms / gen_ms
        print(f"{length:>3}  {fac_result:>17}  {fac_ms:>13.2f}  {gen_result:>14}  "
              f"{gen_ms:>10.2f}  {ratio:>5.2f}x")


if __name__ == "__main__":
    main()
