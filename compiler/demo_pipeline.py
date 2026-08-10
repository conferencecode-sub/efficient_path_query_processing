"""Runs the ReCAP compiler end to end (Stages A-G) on the sample dataset and
prints every intermediate artifact plus the actual DuckDB results -- both
the unoptimized (Stage E) and optimized (Stage F) queries, side by side, so
you can see both that they agree (FR-22) and how much Stage F's flattening
and inlining actually save.

Usage:
    cd compiler
    python3 demo_pipeline.py
"""
from __future__ import annotations

import os

import duckdb

from recap_compiler.execution import run_query
from recap_compiler.ingestion import load_graph, select_start_vertices
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.regex_frontend import compile_regex_to_nfa
from recap_compiler.selective_aggregate import bounded_range, validate_selective_aggregate
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import build_transitions_relation

DATASET = os.path.join(os.path.dirname(__file__), "..", "ReCAP", "simple_dataset", "LG.csv")
REGEX = "(transfer|purchase|sale)+(phishing|scam)+"  # the paper's Q1 query
START_VERTEX = 383  # matches the fixed starter_node used throughout the earlier navigation experiments
LENGTH_BOUND = 4    # kept small so the demo finishes in seconds -- see CHECKLIST.md's note on why
                     # this specific regex/dataset combination blows up fast at depth


def _section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    conn = duckdb.connect()

    _section("Stage A: ingestion")
    handle = load_graph(conn, DATASET)
    edge_columns = {row[0] for row in conn.execute("DESCRIBE edges").fetchall()}
    print(f"loaded {conn.execute('SELECT count(*) FROM edges').fetchone()[0]} edges, "
          f"{conn.execute('SELECT count(*) FROM nodes').fetchone()[0]} vertices")
    starts = select_start_vertices(handle, ids=[START_VERTEX])
    print(f"start vertex: {starts}")

    _section("Stage B: regex -> NFA")
    print(f"regex: {REGEX!r}")
    nfa = compile_regex_to_nfa(REGEX)
    print(f"{len(nfa.states)} states, {len(nfa.accepting_states)} accepting state(s)")

    _section("Stage C: NFA -> transitions relation T(from_state, to_state, label)")
    relation = build_transitions_relation(nfa)
    print(f"q0 = {relation.q0}, Q_F = {sorted(relation.accepting_states)}, "
          f"{len(relation.rows)} transition rows")

    _section("Stage D: selective-aggregate frontend")
    aggregate = bounded_range(property="amount", upper_bound=500.0)
    print("chosen library entry: FR-13(iii) bounded_range(property='amount', upper_bound=500.0)")
    print(f"  is_viable_d = {aggregate.is_viable_d}")
    validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    print("  FR-14 validation: PASSED")

    register_aggregate_macros(conn, aggregate)
    materialize_transitions(conn, relation)

    _section("Stage E: standard ReCAP SQL (functions pasted as DuckDB macros, called by name)")
    standard_query = build_standard_query(relation=relation, start_vertices=starts,
                                           length_bound=LENGTH_BOUND)
    print(standard_query.sql)

    _section("Stage F: optimized SQL (dictionary flattened to columns, macro calls inlined)")
    optimized_query = build_optimized_query(aggregate=aggregate, relation=relation,
                                              start_vertices=starts, length_bound=LENGTH_BOUND)
    print(optimized_query.sql)

    _section(f"Stage G: execution (length_bound={LENGTH_BOUND}) -- standard vs. optimized")
    standard_result = run_query(conn, standard_query, result_shape="paths")
    optimized_result = run_query(conn, optimized_query, result_shape="paths")

    standard_signature = {(v, q, path_length) for v, q, _d, path_length, _r in standard_result.rows}
    optimized_signature = {(v, q, path_length) for v, q, _d, path_length, _r in optimized_result.rows}
    assert standard_signature == optimized_signature, "FR-22 violated: results diverged!"

    print(f"standard  (Stage E, macros):  {len(standard_result.rows):>7} paths, "
          f"{standard_result.telemetry.runtime_ms:>7.1f} ms, "
          f"{standard_result.telemetry.intermediate_paths} intermediate paths explored")
    print(f"optimized (Stage F, inlined): {len(optimized_result.rows):>7} paths, "
          f"{optimized_result.telemetry.runtime_ms:>7.1f} ms, "
          f"{optimized_result.telemetry.intermediate_paths} intermediate paths explored")
    speedup = standard_result.telemetry.runtime_ms / optimized_result.telemetry.runtime_ms
    print(f"\nFR-22 check: PASSED (both queries found the exact same {len(standard_result.rows)} paths)")
    print(f"speedup: {speedup:.2f}x")

    print(f"\nsample optimized result row: {optimized_result.rows[0]}")


if __name__ == "__main__":
    main()
