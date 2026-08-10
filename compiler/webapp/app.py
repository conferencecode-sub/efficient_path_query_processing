"""Stage I: workbench UI (FR-32, FR-33) -- MVP scope.

A single-page Streamlit app wired directly to the existing A-G pipeline: no
dataset, query, or aggregate is hard-coded (FR-33) -- the bundled sample
graph is only a default, replaceable via the file uploader. Drives the
pipeline end to end (FR-32): load data -> enter regex -> pick a library
selective aggregate with its parameters -> compile -> run -> see the
generated SQL, results, and telemetry.

The regex (and the NFA/transitions table it produces, Stages B/C) live
*outside* the query form on purpose: Streamlit only re-runs a form's body
on submit, so a regex field inside the form wouldn't recompute the
automaton until the whole query ran -- exactly the "not updating" bug a
user hit in practice. Everything expensive (loading data, running the
query) stays inside the form so it doesn't refire on every keystroke.

Deliberately out of scope for this MVP (see CHECKLIST.md): live in-browser
editing of the generated CASE skeleton (FR-12's full authoring flow) --
this cut only exposes the three FR-13 library aggregates with their
parameters, not free-form skeleton authoring. The negative-stability check
(Section 13) and the LLM proposer (Module J) are both still unbuilt/
deferred, so neither has a UI hook here either.

Run with:
    cd compiler
    pip install -e '.[ui]'
    streamlit run webapp/app.py
"""
from __future__ import annotations

import os
import tempfile

import duckdb
import pandas as pd
import streamlit as st

from recap_compiler.errors import RecapCompilerError
from recap_compiler.execution import run_query
from recap_compiler.ingestion import load_graph, select_start_vertices
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.regex_frontend import compile_regex_to_nfa
from recap_compiler.selective_aggregate import (
    adjacent_edge_predicate,
    bounded_range,
    trail_via_edge_ids,
    validate_selective_aggregate,
)
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import build_transitions_relation

DEFAULT_DATASET = os.path.join(
    os.path.dirname(__file__), "..", "..", "ReCAP", "simple_dataset", "LG.csv")
DEGREE_BAND_CAP = 5  # keep the demo responsive; a degree band can return ~200 vertices


EDGE_PREVIEW_ROWS = 10

# Substrings of DuckDB type names (from DESCRIBE) that indicate a numeric
# column -- covers all int widths/signedness, floats, and DECIMAL(p,s).
_NUMERIC_TYPE_MARKERS = ("INT", "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC", "HUGEINT")


def _is_numeric_type(column_type: str) -> bool:
    return any(marker in column_type.upper() for marker in _NUMERIC_TYPE_MARKERS)


@st.cache_data(show_spinner=False)
def _probe_schema(csv_bytes: bytes | None, path: str):
    """Loads just enough to show column names/types, counts, a small edge
    preview, and (if there's a label column) the label alphabet, before the
    user commits to a full compile+run -- cached so retyping other widgets
    doesn't reload the graph every rerun. `load_graph` (Stage A) guarantees
    an `edge_id` column exists even if the source didn't have one, so the
    preview below always has a real identifier column to show."""
    conn = duckdb.connect()
    if csv_bytes is not None:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(csv_bytes)
            path = tmp.name
    handle = load_graph(conn, path)
    described = conn.execute("DESCRIBE edges").fetchall()
    columns = [row[0] for row in described]
    column_types = {row[0]: row[1] for row in described}
    n_edges = conn.execute("SELECT count(*) FROM edges").fetchone()[0]
    n_vertices = conn.execute("SELECT count(*) FROM nodes").fetchone()[0]
    preview = conn.execute(f"SELECT * FROM edges LIMIT {EDGE_PREVIEW_ROWS}").df()
    labels = None
    if "label" in columns:
        labels = [row[0] for row in
                  conn.execute("SELECT DISTINCT label FROM edges ORDER BY label").fetchall()]
    return columns, column_types, n_edges, n_vertices, preview, labels


def _signature_set(result, columns=("v", "q", "path_length")) -> set:
    idx = {name: i for i, name in enumerate(result.columns)}
    return {tuple(row[idx[c]] for c in columns) for row in result.rows}


def _expand_struct_columns(result) -> pd.DataFrame:
    """Cosmetic: the `D`/`result` columns come back as Python dicts (DuckDB
    structs); expand them into their own columns for a readable table
    instead of showing raw dict reprs."""
    df = pd.DataFrame(result.rows, columns=result.columns)
    for col in list(df.columns):
        if len(df) and isinstance(df[col].iloc[0], dict):
            expanded = pd.json_normalize(df[col]).add_prefix(f"{col}.")
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
    return df


def _friendly_error(exc: RecapCompilerError) -> None:
    locus = f" (at `{exc.locus}`)" if exc.locus else ""
    st.error(f"**[{exc.category}]** {exc.message}{locus}")


st.set_page_config(page_title="ReCAP Compiler", layout="wide")
st.title("ReCAP Compiler workbench")
st.caption("Path query = label regex + selective aggregate over a property graph, "
           "compiled to recursive SQL and run on DuckDB.")

with st.sidebar:
    st.header("1. Graph data")
    uploaded = st.file_uploader("Edges CSV (required columns: src, dst, label)", type="csv")
    dataset_label = uploaded.name if uploaded else "bundled sample: ReCAP/simple_dataset/LG.csv"
    st.caption(f"Using: {dataset_label}")

    try:
        csv_bytes = uploaded.getvalue() if uploaded else None
        edge_columns, column_types, n_edges, n_vertices, edge_preview, labels = _probe_schema(
            csv_bytes, DEFAULT_DATASET)
        st.success(f"{n_edges:,} edges, {n_vertices:,} vertices")
        st.caption(f"columns: {', '.join(edge_columns)}")
        if labels is not None:
            st.caption(f"label alphabet ({len(labels)}): {', '.join(map(str, labels))}")
    except RecapCompilerError as exc:
        _friendly_error(exc)
        st.stop()

st.subheader(f"Edge data (first {EDGE_PREVIEW_ROWS} rows)")
st.dataframe(edge_preview, height=250)

st.header("2. Label regex (Stages B/C: NFA + transitions, not shown -- can get large)")
regex = st.text_input("Label regex", value="(transfer|purchase|sale)+(phishing|scam)+")

try:
    nfa = compile_regex_to_nfa(regex)
    relation = build_transitions_relation(nfa)
except RecapCompilerError as exc:
    _friendly_error(exc)
    st.stop()

st.write(f"**{len(nfa.states)} states**, q0 = `{relation.q0}`, "
         f"accepting states = `{sorted(relation.accepting_states)}`, "
         f"**{len(relation.rows)} transitions**")

with st.form("query_form"):
    st.header("3. Start vertices and length bound")
    start_mode = st.radio("Start vertices", ["Specific vertex id", "Out-degree band"], horizontal=True)
    if start_mode == "Specific vertex id":
        start_vertex_id = st.number_input("Start vertex id", value=383, step=1)
        degree_band = None
    else:
        degree_band = st.selectbox("Out-degree band", ["low", "medium", "high"], index=2)
        start_vertex_id = None

    length_bound = st.number_input("Length bound", min_value=1, max_value=20, value=4, step=1,
                                    help="Kept small on purpose -- this regex's branching factor "
                                         "makes deep single-vertex runs expensive. See CHECKLIST.md.")

    st.header("4. Selective aggregate (FR-13 library)")
    non_property_columns = {"src", "dst", "label"}
    numeric_property_candidates = [
        c for c in edge_columns if c not in non_property_columns and _is_numeric_type(column_types[c])
    ]

    aggregate_kind = st.selectbox(
        "Aggregate",
        ["Bounded range (max - min <= U)", "Adjacent-edge predicate", "Trail (no repeated edges)"],
    )
    if aggregate_kind in ("Bounded range (max - min <= U)", "Adjacent-edge predicate"):
        if not numeric_property_candidates:
            st.warning("No numeric edge columns found -- this aggregate needs one "
                       "(GREATEST/LEAST/subtraction don't apply to text columns).")
            st.stop()
        agg_property = st.selectbox(
            "Property", numeric_property_candidates,
            help="Only numeric columns are offered -- this aggregate does arithmetic "
                 "(max/min/subtraction) on the property, which isn't meaningful for text.")
        if aggregate_kind == "Bounded range (max - min <= U)":
            upper_bound = st.number_input("Upper bound U", value=500.0)
        else:
            comparator = st.selectbox("Comparator (edge vs. last edge)", [">=", "<="], index=0)
    else:
        id_column = st.selectbox("Edge id column", edge_columns,
                                  index=edge_columns.index("edge_id") if "edge_id" in edge_columns else 0,
                                  help="Any column works here -- trail semantics only need "
                                       "equality, not order, so text ids are fine too.")

    compare_to_standard = st.checkbox(
        "Also run the unoptimized (Stage E) query, to check it agrees with the optimized one (FR-22)",
        value=True)

    submitted = st.form_submit_button("Compile & run", type="primary")

if not submitted:
    st.info("Fill in the query above and click **Compile & run**.")
    st.stop()

# --- compile ------------------------------------------------------------
try:
    if aggregate_kind == "Bounded range (max - min <= U)":
        aggregate = bounded_range(property=agg_property, upper_bound=upper_bound)
    elif aggregate_kind == "Adjacent-edge predicate":
        aggregate = adjacent_edge_predicate(property=agg_property, comparator=comparator)
    else:
        aggregate = trail_via_edge_ids(id_column=id_column)

    conn = duckdb.connect()
    if uploaded:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(uploaded.getvalue())
            dataset_path = tmp.name
    else:
        dataset_path = DEFAULT_DATASET
    handle = load_graph(conn, dataset_path)

    validate_selective_aggregate(aggregate, edge_columns=set(edge_columns))

    if start_vertex_id is not None:
        starts = select_start_vertices(handle, ids=[int(start_vertex_id)])
    else:
        starts = select_start_vertices(handle, degree_band=degree_band)
        if len(starts) > DEGREE_BAND_CAP:
            st.warning(f"'{degree_band}' band has {len(starts)} vertices; using the first "
                       f"{DEGREE_BAND_CAP} to keep this responsive.")
            starts = starts[:DEGREE_BAND_CAP]

    materialize_transitions(conn, relation)

except RecapCompilerError as exc:
    _friendly_error(exc)
    st.stop()

st.header("Results")
st.write(f"start vertices: {starts}")

col_std, col_opt = st.columns(2) if compare_to_standard else (st.container(), None)

try:
    optimized_query = build_optimized_query(aggregate=aggregate, relation=relation,
                                             start_vertices=starts, length_bound=int(length_bound))
    optimized_result = run_query(conn, optimized_query, result_shape="paths")

    if compare_to_standard:
        register_aggregate_macros(conn, aggregate)
        standard_query = build_standard_query(relation=relation, start_vertices=starts,
                                               length_bound=int(length_bound))
        standard_result = run_query(conn, standard_query, result_shape="paths")

except RecapCompilerError as exc:
    _friendly_error(exc)
    st.stop()

with col_opt if compare_to_standard else col_std:
    st.subheader("Optimized (Stage F)")
    st.code(optimized_query.sql, language="sql")
    st.metric("Paths found", f"{len(optimized_result.rows):,}")
    st.metric("Runtime", f"{optimized_result.telemetry.runtime_ms:.1f} ms")
    st.metric("Intermediate paths explored", f"{optimized_result.telemetry.intermediate_paths:,}")
    st.dataframe(_expand_struct_columns(optimized_result).head(200))

if compare_to_standard:
    with col_std:
        st.subheader("Standard (Stage E, unoptimized)")
        st.code(standard_query.sql, language="sql")
        st.metric("Paths found", f"{len(standard_result.rows):,}")
        st.metric("Runtime", f"{standard_result.telemetry.runtime_ms:.1f} ms")
        st.metric("Intermediate paths explored", f"{standard_result.telemetry.intermediate_paths:,}")
        st.dataframe(_expand_struct_columns(standard_result).head(200))

    st.divider()
    if _signature_set(optimized_result) == _signature_set(standard_result):
        speedup = standard_result.telemetry.runtime_ms / max(optimized_result.telemetry.runtime_ms, 1e-9)
        st.success(f"FR-22 check PASSED: both queries found the exact same "
                   f"{len(optimized_result.rows):,} paths. Speedup: {speedup:.2f}x.")
    else:
        st.error("FR-22 check FAILED: standard and optimized queries disagree -- this "
                 "would be a real compiler bug, please report it.")
