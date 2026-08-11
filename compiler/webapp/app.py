"""Stage I: workbench UI (FR-32, FR-33) -- MVP scope.

A single-page Streamlit app wired directly to the existing A-G pipeline: no
dataset, query, or aggregate is hard-coded (FR-33) -- the bundled sample
graph is only a default, replaceable via the file uploader. Drives the
pipeline end to end (FR-32): load data -> enter regex -> pick or author a
selective aggregate -> compile -> run -> see the generated SQL, results,
and telemetry.

Every widget that configures the query (regex, start vertices, aggregate
choice, custom aggregate bodies) is a plain, non-form widget, so the whole
layout reacts live as you type or click -- nothing needs a submit first to
become visible. This is deliberate, not an oversight: Streamlit's own
`st.form` docs say interacting with a widget inside a form "will do
nothing" visibly until the form is submitted, which is exactly the bug a
user hit when the regex field was briefly inside one (see CHECKLIST.md).
Only the actually expensive part -- loading data and running the query --
is gated behind a plain `st.button`, since that's the only part slow
enough (per the timing breakdown below) to be worth not re-running on
every keystroke.

Two ways to get a selective aggregate: pick one of the three FR-13 library
entries with its parameters, or author a **factorized** custom one (own
dictionary keys + five SQL bodies, validated live via FR-14). Non-
factorized (per-NFA-transition) authoring is deliberately out of scope --
a real regex can have 100+ transition pairs (Q1's does), so a one-text-box-
per-pair UI doesn't scale; see CHECKLIST.md. The negative-stability check
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

from recap_compiler.errors import RecapCompilerError, RefError
from recap_compiler.execution import run_query
from recap_compiler.ingestion import load_graph, select_start_vertices
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.profiling import TimingBreakdown, timed_stage
from recap_compiler.regex_frontend import compile_regex_to_nfa
from recap_compiler.selective_aggregate import (
    DictionaryKey,
    SelectiveAggregate,
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


def _infer_dictionary_keys(init_d_body: str) -> tuple[DictionaryKey, ...]:
    """Derives the dictionary's keys and types directly from `init_d`'s own
    struct literal, via DuckDB's own type system -- there's no separate
    table to keep in sync by hand; edit `init_d` and the tracked keys
    follow automatically (this is why: a user tried to drop a key from
    `init_d` while an independent keys table still declared it, and got a
    confusing "does not initialize declared key" error -- the table and
    init_d could disagree by construction. Removing the second source of
    truth removes the disagreement.) A non-struct `init_d` (e.g. bare
    `NULL`, for an aggregate that tracks nothing) infers zero keys."""
    conn = duckdb.connect()
    try:
        struct_type = conn.sql(f"SELECT ({init_d_body}) AS d").types[0]
    except duckdb.Error as exc:
        raise RefError(f"init_d: could not evaluate expression: {exc}", locus="init_d") from exc
    if struct_type.id != "struct":
        return ()
    return tuple(DictionaryKey(name=name, sql_type=str(child_type))
                 for name, child_type in struct_type.children)


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

starting_state = relation.q0
accepting_states = sorted(relation.accepting_states)
st.write(f"**{len(nfa.states)} states**, starting_state = `{starting_state}`, "
         f"accepting_states = `{accepting_states}`, "
         f"**{len(relation.rows)} transitions**")

st.header("3. Start vertices and length bound")
start_mode = st.radio("Start vertices", ["Specific vertex id", "Out-degree band"], horizontal=True)
if start_mode == "Specific vertex id":
    start_vertex_id = st.number_input("Start vertex id", value=383, step=1)
    degree_band = None
else:
    degree_band = st.selectbox("Out-degree band", ["low", "medium", "high"], index=2)
    start_vertex_id = None

length_bound = st.number_input("Length bound", min_value=0, max_value=20, value=3, step=1,
                                help="Max number of edges in a path (path_length starts at 0, "
                                     "at the start vertex, before any edge is taken). Kept small "
                                     "on purpose -- this regex's branching factor makes deep "
                                     "single-vertex runs expensive. See CHECKLIST.md.")

st.header("4. Selective aggregate")

non_property_columns = {"src", "dst", "label"}
numeric_property_candidates = [
    c for c in edge_columns if c not in non_property_columns and _is_numeric_type(column_types[c])
]
# Used to seed a *working* custom-aggregate example below, not just for the
# library picker -- prefer "amount" (the bundled dataset's own property)
# when present, so the default reads naturally, but fall back to whatever
# numeric column actually exists so the default still works on any upload.
_default_property = ("amount" if "amount" in numeric_property_candidates
                      else (numeric_property_candidates[0] if numeric_property_candidates else None))

aggregate_source = st.radio(
    "Aggregate source",
    ["Library aggregate (FR-13)", "Custom aggregate (factorized only)"],
    horizontal=True)

if aggregate_source == "Library aggregate (FR-13)":
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
else:
    st.caption("Factorized only: the body doesn't depend on NFA state, so there's one expression "
               "per function, not one per transition pair (Q1's regex alone has 100+ pairs -- see "
               "CHECKLIST.md for why per-transition editing isn't offered here).")
    st.caption("Convention: `D.<key>` for a dictionary field, `e.<column>` for an edge property. "
               "**Dictionary keys are inferred automatically from `init_d`'s own struct literal --"
               "** there's no separate table to keep in sync by hand. Edit `init_d`, and the "
               "tracked keys (shown below it) follow.")

    if _default_property is not None:
        _max_key, _min_key = f"max_{_default_property}", f"min_{_default_property}"
        _default_init_d = f"{{{_max_key}: -1e308, {_min_key}: 1e308}}"
        _default_update_d = (f"{{{_max_key}: GREATEST(D.{_max_key}, e.{_default_property}), "
                              f"{_min_key}: LEAST(D.{_min_key}, e.{_default_property})}}")
        _default_is_viable_d = (f"GREATEST(D.{_max_key}, e.{_default_property}) - "
                                 f"LEAST(D.{_min_key}, e.{_default_property}) <= 500.0")
        st.caption(f"Prefilled below: a working example equivalent to the library's "
                   f"`bounded_range(property='{_default_property}', upper_bound=500.0)` -- edit "
                   f"`init_d` and the other bodies together to build something else.")
    else:
        _default_init_d, _default_update_d, _default_is_viable_d = (
            "{my_key: 0.0}", "{my_key: D.my_key}", "TRUE")

    custom_init_d = st.text_area(
        "init_d()", value=_default_init_d, height=80,
        help="Nothing is in scope here (no D, no e) -- build the initial dictionary from "
             "literals/constants only. Its keys and their types (shown below) are inferred "
             "directly from this struct literal, e.g. `{k: 0.0}` declares one DOUBLE key `k`.")

    try:
        dictionary_keys = _infer_dictionary_keys(custom_init_d)
        if dictionary_keys:
            st.dataframe(pd.DataFrame([{"name": k.name, "sql_type": k.sql_type} for k in dictionary_keys]),
                         hide_index=True)
        else:
            st.caption("(`init_d` doesn't evaluate to a struct, so this aggregate tracks no "
                       "dictionary keys -- fine for an aggregate that only checks the path shape.)")
    except RecapCompilerError as exc:
        _friendly_error(exc)
        dictionary_keys = None  # fix init_d above before this can run

    custom_update_d = st.text_area(
        "update_d(D, e)", value=_default_update_d, height=80,
        help="Two accepted forms: a struct literal `{key: expr, ...}`, or one or more "
             "`D.<key> = <expr>` assignments (separate with `;` for more than one) -- e.g. "
             "`D.max_amount = GREATEST(D.max_amount, e.amount)`. Either way, you don't have to "
             "mention every key from init_d: leave one out and it automatically keeps its "
             "previous value unchanged instead of being removed from D.")
    custom_is_viable_d = st.text_area("is_viable_d(D, e)", value=_default_is_viable_d, height=80)
    custom_is_viable_d_final = st.text_area("is_viable_d_final(D)", value="TRUE", height=60)
    custom_finalize_d = st.text_area("finalize_d(D)", value="D", height=60)

compare_to_standard = st.checkbox(
    "Also run the unoptimized (Stage E) query, to check it agrees with the optimized one (FR-22)",
    value=True)

run_clicked = st.button("Compile & run", type="primary")

if not run_clicked:
    st.info("Fill in the query above and click **Compile & run**.")
    st.stop()

# --- compile ------------------------------------------------------------
# Timed from scratch here (even though the regex/NFA were already computed
# live above) so the breakdown shown at the end reflects the whole pipeline
# a fresh run would do, not just the parts gated behind this button.
breakdown = TimingBreakdown()
try:
    with timed_stage(breakdown, "B: regex -> NFA"):
        nfa = compile_regex_to_nfa(regex)
    with timed_stage(breakdown, "C: build transitions relation"):
        relation = build_transitions_relation(nfa)  # deterministic (NFR-1) -- same content as above

    if aggregate_source == "Library aggregate (FR-13)":
        if aggregate_kind == "Bounded range (max - min <= U)":
            aggregate = bounded_range(property=agg_property, upper_bound=upper_bound)
        elif aggregate_kind == "Adjacent-edge predicate":
            aggregate = adjacent_edge_predicate(property=agg_property, comparator=comparator)
        else:
            aggregate = trail_via_edge_ids(id_column=id_column)
    else:
        if dictionary_keys is None:
            st.error("Fix init_d above before running -- its keys couldn't be inferred.")
            st.stop()
        aggregate = SelectiveAggregate(
            dictionary_keys=dictionary_keys,
            init_d=custom_init_d,
            update_d=custom_update_d,
            is_viable_d=custom_is_viable_d,
            is_viable_d_final=custom_is_viable_d_final,
            finalize_d=custom_finalize_d,
            factorized=True,
        )

    conn = duckdb.connect()
    if uploaded:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(uploaded.getvalue())
            dataset_path = tmp.name
    else:
        dataset_path = DEFAULT_DATASET
    with timed_stage(breakdown, "A: load graph"):
        handle = load_graph(conn, dataset_path)

    with timed_stage(breakdown, "D: validate aggregate"):
        validate_selective_aggregate(aggregate, edge_columns=set(edge_columns))

    with timed_stage(breakdown, "A: select start vertices"):
        if start_vertex_id is not None:
            starts = select_start_vertices(handle, ids=[int(start_vertex_id)])
        else:
            starts = select_start_vertices(handle, degree_band=degree_band)
            if len(starts) > DEGREE_BAND_CAP:
                st.warning(f"'{degree_band}' band has {len(starts)} vertices; using the first "
                           f"{DEGREE_BAND_CAP} to keep this responsive.")
                starts = starts[:DEGREE_BAND_CAP]

    with timed_stage(breakdown, "C: materialize transitions table"):
        materialize_transitions(conn, relation)

except RecapCompilerError as exc:
    _friendly_error(exc)
    st.stop()

st.header("Results")
st.write(f"start vertices: {starts}")

col_std, col_opt = st.columns(2) if compare_to_standard else (st.container(), None)

try:
    with timed_stage(breakdown, "F: generate optimized SQL"):
        optimized_query = build_optimized_query(aggregate=aggregate, relation=relation,
                                                 start_vertices=starts, length_bound=int(length_bound))
    with timed_stage(breakdown, "G: execute optimized query"):
        optimized_result = run_query(conn, optimized_query, result_shape="paths")

    if compare_to_standard:
        with timed_stage(breakdown, "E: register aggregate macros"):
            register_aggregate_macros(conn, aggregate)
        with timed_stage(breakdown, "E: generate standard SQL"):
            standard_query = build_standard_query(relation=relation, start_vertices=starts,
                                                   length_bound=int(length_bound))
        with timed_stage(breakdown, "G: execute standard query"):
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

st.divider()
st.subheader("Timing breakdown")
st.caption("Every stage this run actually went through, from parsing the regex to executing "
           "the query. Usually most of the total is the query execution(s) -- everything "
           "before that (parsing, loading, validating, generating SQL) is comparatively instant.")
timing_df = pd.DataFrame(breakdown.as_rows())
st.bar_chart(timing_df.set_index("stage")["ms"])
st.dataframe(timing_df.style.format({"ms": "{:.2f}", "% of total": "{:.1f}%"}))
st.metric("Total", f"{breakdown.total_ms:.1f} ms")
