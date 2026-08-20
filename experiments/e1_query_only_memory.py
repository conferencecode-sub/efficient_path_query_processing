r"""Query-only peak memory (excludes the one-time dataset-load cost) for
CompilerStd vs CompilerOpt, all four real datasets -- per explicit user
request, to update tab:recap_real_data's memory columns.

DuckDB's own buffer-memory stat is a monotonic high-water mark for the
whole connection's lifetime (documented in execution.py), so "just the
query" isn't really "load excluded" -- the loaded graph has to stay
resident for the query to run at all. What's actually isolatable is the
*incremental* memory the query adds on top of the resident graph: enable
profiling on a *fresh* connection *before* loading (confirmed empirically
that enabling profiling only after loading undercounts -- some load-time
allocations are already freed by then, so the "baseline" would be wrong),
checkpoint peak_buffer_memory right after loading as a baseline, then
checkpoint again after each length_bound's query, in increasing ell order
on the *same* connection -- since memory usage grows with ell for these
queries, each successive checkpoint's own delta from baseline is that
ell's own true incremental peak, not a carried-over smaller one. One
connection per (dataset, variant) rather than per (dataset, variant, ell),
so the big real datasets (Datagen-7.6, LDBC100) only get loaded twice
each. Reuses each query's own existing run_new_compiler.py loading
helpers directly (`_load_bidirected`, `_load_ldbc_edges`) rather than
re-deriving that logic, to guarantee this measures the exact same setup
that produced e1_real_data_results.md's own numbers.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "compiler", "src"))

import duckdb  # noqa: E402

from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column  # noqa: E402
from recap_compiler.optimizer import build_optimized_query  # noqa: E402
from recap_compiler.regex_frontend import compile_regex_to_nfa  # noqa: E402
from recap_compiler.standard_sql import build_standard_query, materialize_transitions  # noqa: E402
from recap_compiler.transitions import build_transitions_relation, trivial_relation  # noqa: E402


def _checkpoint_mb(conn, profpath) -> float:
    conn.execute("SELECT 1")  # forces a fresh profiling_output write reflecting current state
    return json.loads(open(profpath).read())["system_peak_buffer_memory"] / 1e6


def _fresh_profiled_connection():
    conn = duckdb.connect()
    profpath = tempfile.mktemp(suffix=".json")
    conn.execute("PRAGMA enable_profiling='json'")  # BEFORE any loading -- see module docstring
    conn.execute(f"PRAGMA profiling_output='{profpath}'")
    return conn, profpath


def run_case(name, *, load_fn, use_regex_relation, start_vertex, aggregate, lengths, min_length=None):
    print(f"=== {name} ===")
    results = {}
    for variant in ("standard", "optimized"):
        conn, profpath = _fresh_profiled_connection()
        handle = load_fn(conn)
        if use_regex_relation is not None:
            rel = use_regex_relation()
        else:
            set_trivial_label_column(conn)
            rel = trivial_relation()
        starts = select_start_vertices(handle, ids=[start_vertex])
        materialize_transitions(conn, rel)
        baseline_mb = _checkpoint_mb(conn, profpath)
        incremental = {}
        for length in lengths:
            if variant == "standard":
                from recap_compiler.standard_sql import register_aggregate_macros
                register_aggregate_macros(conn, aggregate)
                q = build_standard_query(relation=rel, start_vertices=starts, length_bound=length)
            else:
                q = build_optimized_query(aggregate=aggregate, relation=rel,
                                           start_vertices=starts, length_bound=length)
            sql = q.sql if min_length is None else f"SELECT * FROM ({q.sql}) t WHERE path_length >= {min_length}"
            conn.execute(f"SELECT count(*) FROM ({sql}) t").fetchall()
            incremental[length] = _checkpoint_mb(conn, profpath) - baseline_mb
        conn.close()
        results[variant] = (baseline_mb, incremental)
        print(f"  {variant}: baseline(load-only)={baseline_mb:.1f}MB")
        for length in lengths:
            print(f"    ell={length}: query-only={incremental[length]:.2f}MB")

    std_vals = results["standard"][1].values()
    opt_vals = results["optimized"][1].values()
    print(f"  std range: {min(std_vals):.2f}--{max(std_vals):.2f}MB")
    print(f"  opt range: {min(opt_vals):.2f}--{max(opt_vals):.2f}MB")
    return results


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"

    if which in ("q1", "all"):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "q1_length_sweep"))
        from q1_aggregate import q1_aggregate
        DATASET = os.path.join(os.path.dirname(__file__), "datasets", "metaverse", "edges.csv")

        def _load_q1(conn):
            return load_graph(conn, DATASET)

        run_case("Q1 Metaverse", load_fn=_load_q1,
                  use_regex_relation=lambda: build_transitions_relation(
                      compile_regex_to_nfa("(transfer|purchase|sale)+(phishing|scam)+", minimize=True)),
                  start_vertex=383, aggregate=q1_aggregate(), lengths=(2, 3, 4, 5, 6, 7, 8, 9, 10))

    if which in ("q2", "all"):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "q2_length_sweep"))
        from q2_aggregate import q2_aggregate
        EDGES = os.path.join(os.path.dirname(__file__), "datasets", "bitcoin", "edges_with_colors.csv")
        NODES = os.path.join(os.path.dirname(__file__), "datasets", "bitcoin", "nodes.csv")

        def _load_q2(conn):
            return load_graph(conn, EDGES, NODES)

        run_case("Q2 Bitcoin", load_fn=_load_q2, use_regex_relation=None,
                  start_vertex=3999, aggregate=q2_aggregate(), lengths=(2, 3, 4, 5))

    if which in ("q3", "all"):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "q3_length_sweep"))
        from q3_aggregate import q3_aggregate
        from run_new_compiler import _load_bidirected as _load_q3

        run_case("Q3 Datagen-7.6", load_fn=_load_q3, use_regex_relation=None,
                  start_vertex=4398046568596, aggregate=q3_aggregate(), lengths=(2, 3, 4), min_length=2)

    if which in ("q4", "all"):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "q4_length_sweep"))
        from q4_aggregate import q4_aggregate
        from run_new_compiler import _load_ldbc_edges as _load_q4, TWO_WEEKS_MS

        run_case("Q4 LDBC100", load_fn=_load_q4, use_regex_relation=None,
                  start_vertex=24189256063073, aggregate=q4_aggregate(bound=TWO_WEEKS_MS),
                  lengths=(2, 3, 4, 5, 6, 7, 8), min_length=2)
