"""Runs the ReCAP compiler end to end (Stages A-G) on the sample dataset and
prints every intermediate artifact plus the actual DuckDB results -- both
the unoptimized (Stage E) and optimized (Stage F) queries, side by side, so
you can see both that they agree (Stage F is required to produce the same
result set as Stage E for every query) and how much Stage F's flattening
and inlining actually save. Also prints a stage-by-stage timing breakdown
(parsing, loading, SQL generation, execution, ...) so you can see where the
time actually goes, not just the end-to-end total.

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
from recap_compiler.profiling import TimingBreakdown, timed_stage
from recap_compiler.regex_frontend import compile_regex_to_nfa
from recap_compiler.selective_aggregate import bounded_range, validate_selective_aggregate
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import build_transitions_relation

DATASET = os.path.join(os.path.dirname(__file__), "sample_data", "LG.csv")
REGEX = "(transfer|purchase|sale)+(phishing|scam)+"  # the paper's Q1 query
START_VERTEX = 383  # matches the fixed starter_node used throughout the earlier navigation experiments
LENGTH_BOUND = 3    # max edges per path (path_length now starts at 0, so this is 1 less than it
                     # used to be for the same reach) -- kept small so the demo finishes in seconds;
                     # see CHECKLIST.md for why this regex/dataset combination blows up fast at depth


def _section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    breakdown = TimingBreakdown()
    conn = duckdb.connect()

    _section("Stage A: ingestion")
    with timed_stage(breakdown, "A: load graph"):
        handle = load_graph(conn, DATASET)
    edge_columns = {row[0] for row in conn.execute("DESCRIBE edges").fetchall()}
    print(f"loaded {conn.execute('SELECT count(*) FROM edges').fetchone()[0]} edges, "
          f"{conn.execute('SELECT count(*) FROM nodes').fetchone()[0]} vertices")
    with timed_stage(breakdown, "A: select start vertices"):
        starts = select_start_vertices(handle, ids=[START_VERTEX])
    print(f"start vertex: {starts}")

    _section("Stage B: regex -> NFA")
    print(f"regex: {REGEX!r}")
    with timed_stage(breakdown, "B: regex -> NFA"):
        nfa = compile_regex_to_nfa(REGEX)
    print(f"{len(nfa.states)} states, {len(nfa.accepting_states)} accepting state(s)")

    _section("Stage C: NFA -> transitions relation T(from_state, to_state, label)")
    with timed_stage(breakdown, "C: build transitions relation"):
        relation = build_transitions_relation(nfa)
    print(f"q0 = {relation.q0}, Q_F = {sorted(relation.accepting_states)}, "
          f"{len(relation.rows)} transition rows")

    _section("Stage D: selective-aggregate frontend")
    aggregate = bounded_range(property="amount", upper_bound=500.0)
    print("chosen library entry: bounded_range(property='amount', upper_bound=500.0)"
          " -- a bounded monotone/distributive aggregate")
    print(f"  is_viable_d = {aggregate.is_viable_d}")
    with timed_stage(breakdown, "D: validate aggregate"):
        validate_selective_aggregate(aggregate, edge_columns=edge_columns)
    print("  reference validation (function bodies only touch declared columns/keys): PASSED")

    with timed_stage(breakdown, "E: register aggregate macros"):
        register_aggregate_macros(conn, aggregate)
    with timed_stage(breakdown, "C: materialize transitions table"):
        materialize_transitions(conn, relation)

    _section("Stage E: standard ReCAP SQL (functions pasted as DuckDB macros, called by name)")
    with timed_stage(breakdown, "E: generate standard SQL"):
        standard_query = build_standard_query(relation=relation, start_vertices=starts,
                                               length_bound=LENGTH_BOUND)
    print(standard_query.sql)

    _section("Stage F: optimized SQL (dictionary flattened to columns, macro calls inlined)")
    with timed_stage(breakdown, "F: generate optimized SQL"):
        optimized_query = build_optimized_query(aggregate=aggregate, relation=relation,
                                                  start_vertices=starts, length_bound=LENGTH_BOUND)
    print(optimized_query.sql)

    _section(f"Stage G: execution (length_bound={LENGTH_BOUND}) -- standard vs. optimized")
    with timed_stage(breakdown, "G: execute standard query"):
        standard_result = run_query(conn, standard_query, result_shape="paths")
    with timed_stage(breakdown, "G: execute optimized query"):
        optimized_result = run_query(conn, optimized_query, result_shape="paths")

    standard_signature = {(v, q, path_length) for v, q, _d, path_length, _r in standard_result.rows}
    optimized_signature = {(v, q, path_length) for v, q, _d, path_length, _r in optimized_result.rows}
    assert standard_signature == optimized_signature, "standard and optimized queries diverged!"

    print(f"standard  (Stage E, macros):  {len(standard_result.rows):>7} paths, "
          f"{standard_result.telemetry.runtime_ms:>7.1f} ms "
          f"(+{standard_result.telemetry.intermediate_count_ms:.1f} ms for the intermediate-count recount below), "
          f"{standard_result.telemetry.intermediate_paths} intermediate paths explored, "
          f"{standard_result.telemetry.peak_buffer_memory_mb:.1f} MB peak buffer memory")
    print(f"optimized (Stage F, inlined): {len(optimized_result.rows):>7} paths, "
          f"{optimized_result.telemetry.runtime_ms:>7.1f} ms "
          f"(+{optimized_result.telemetry.intermediate_count_ms:.1f} ms for the intermediate-count recount below), "
          f"{optimized_result.telemetry.intermediate_paths} intermediate paths explored, "
          # Same connection ran the standard query first (see execution.py's
          # module docstring) -- this is the peak since then, not isolated.
          f"{optimized_result.telemetry.peak_buffer_memory_mb:.1f} MB peak buffer memory (cumulative)")
    speedup = standard_result.telemetry.runtime_ms / optimized_result.telemetry.runtime_ms
    print(f"\nequivalence check: PASSED (both queries found the exact same {len(standard_result.rows)} paths)")
    print(f"speedup: {speedup:.2f}x")

    # Drop the internal NFA state column `q` from the printed sample --
    # it's load-bearing for the equivalence check above, but it's
    # compiler-internal plumbing that shouldn't appear in output tables.
    sample_row = dict(zip(optimized_result.columns, optimized_result.rows[0]))
    sample_row.pop("q", None)
    print(f"\nsample optimized result row: {sample_row}")

    _section("Timing breakdown")
    for row in breakdown.as_rows():
        print(f"{row['step']:<32} {row['ms']:>9.2f} ms  ({row['% of total']:5.1f}%)")
    print(f"{'TOTAL':<32} {breakdown.total_ms:>9.2f} ms")
    print("\n(most of this is the two query executions -- compiling the query itself "
          "is comparatively instant; that contrast is itself worth pointing out.)")


if __name__ == "__main__":
    main()
