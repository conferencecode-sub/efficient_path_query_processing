"""Stage I: workbench UI (FR-32, FR-33) -- MVP scope.

A single-page Streamlit app wired directly to the existing A-G pipeline: no
dataset, query, or aggregate is hard-coded (FR-33) -- the bundled sample
graph is only a default, replaceable via the file uploader. Drives the
pipeline end to end (FR-32): load data -> optionally pick a label column and
enter a regex -> pick or author a selective aggregate -> compile -> run ->
see the generated SQL, results, and telemetry. A regex is no longer
mandatory: leaving the label-column picker on its no-regex option builds
the query over `transitions.trivial_relation()`'s single-state, self-
looping automaton instead of a real regex's NFA -- every edge is explored
regardless of label, filtered only by the aggregate, but through the exact
same Stage E/F code path as a real regex, not a separate one.

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
per-pair UI doesn't scale; see CHECKLIST.md. The negative-stability
verifier (Section 13) is still unbuilt/deferred and has no UI hook. An
LLM-assisted drafting panel (Module J) was built, live-tested, and removed
(2026-08-11, per user decision) -- see CHECKLIST.md for that history.

Run with:
    cd compiler
    pip install -e '.[ui]'
    streamlit run webapp/app.py
"""
from __future__ import annotations

import os
import random
import tempfile

import duckdb
import pandas as pd
import streamlit as st

from recap_compiler.errors import RecapCompilerError, RefError
from recap_compiler.execution import run_query
from recap_compiler.ingestion import load_graph, select_start_vertices, set_trivial_label_column
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.profiling import TimingBreakdown, timed_stage
from recap_compiler.regex_frontend import compile_regex_to_nfa
from recap_compiler.selective_aggregate import (
    DictionaryKey,
    SelectiveAggregate,
    adjacent_edge_predicate,
    bounded_range,
    combine_library_aggregates,
    trail_via_edge_ids,
    validate_selective_aggregate,
)
from recap_compiler.standard_sql import build_standard_query, materialize_transitions, register_aggregate_macros
from recap_compiler.transitions import build_transitions_relation, guard_against_ambiguity, trivial_relation

DEFAULT_DATASET = os.path.join(
    os.path.dirname(__file__), "..", "..", "ReCAP", "simple_dataset", "LG.csv")
MANY_START_VERTICES_CAP = 5  # keep the demo responsive; a degree band or the FR-4
                              # all-vertices default can return hundreds of vertices
EDGE_PREVIEW_ROWS = 10

# Substrings of DuckDB type names (from DESCRIBE) that indicate a numeric
# column -- covers all int widths/signedness, floats, and DECIMAL(p,s).
_NUMERIC_TYPE_MARKERS = ("INT", "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC", "HUGEINT")
# Substrings indicating a string-typed column -- offered as label-column
# candidates, since a label regex matches against text values.
_STRING_TYPE_MARKERS = ("VARCHAR", "TEXT", "STRING", "CHAR", "BLOB")
_NO_REGEX_OPTION = "(no regex -- explore every edge)"
_DEFAULT_REGEX = "(transfer|purchase|sale)+(phishing|scam)+"
# regex_frontend.py doesn't escape/quote label text yet (see its module
# docstring's "known limitation") -- a label containing any of these, or
# whitespace, would confuse the parser if spliced straight into a regex.
_REGEX_METACHARACTERS = set("|()*+?{}$.")

_INTERMEDIATE_PATHS_HELP = (
    "How many rows the recursive query actually produced before the final filter "
    "(reaching an accepting state AND passing is_viable_d_final) was applied -- every "
    "candidate path considered, viable or not. Comparing this between the optimized and "
    "standard queries shows how much the early-pruning check (is_viable_d) actually cut "
    "the search down, separate from how many paths end up in the final result. Computing "
    "this count re-runs the whole recursive CTE a second time (not recoverable from the "
    "main query's own result), which is why its own time is reported separately below "
    "rather than folded into Runtime.")
_PEAK_MEMORY_HELP = (
    "DuckDB's own peak buffer-memory usage for this connection (via PRAGMA "
    "enable_profiling), in MB. Useful for spotting when a deep length_bound is about to "
    "blow up before it actually OOMs (see CHECKLIST.md for how bad this regex/dataset "
    "combination can get). Not perfectly isolated per query -- it's a high-water mark for "
    "the whole connection, so when comparing standard vs. optimized (they share one "
    "connection), the second query's number includes the first's.")


def _is_numeric_type(column_type: str) -> bool:
    return any(marker in column_type.upper() for marker in _NUMERIC_TYPE_MARKERS)


def _is_string_type(column_type: str) -> bool:
    return any(marker in column_type.upper() for marker in _STRING_TYPE_MARKERS)


def _regex_token_for_label(value) -> str | None:
    """A label containing a regex metacharacter or whitespace (e.g. `North
    America`) needs `regex_frontend.py`'s double-quote syntax to be
    matched as one atomic token instead of being silently misparsed --
    quoted here, not just left bare. Returns `None` for a label
    containing a literal `"`, since quoting has no escape mechanism for
    that yet -- such values are excluded from the random example rather
    than emitting a regex that would fail to parse."""
    text = str(value)
    if not text or '"' in text:
        return None
    if any(ch in _REGEX_METACHARACTERS or ch.isspace() for ch in text):
        return f'"{text}"'
    return text


def _random_regex_from_alphabet(alphabet: list) -> str | None:
    """Builds a plausible example regex out of a label column's *actual*
    values -- e.g. `(purchase|sale)+("North America")+` -- instead of
    always prefilling the bundled dataset's own Q1 regex, which means
    nothing for a column from a different dataset entirely. Returns
    `None` if nothing usable remains (e.g. every value contains a literal
    `"`), so the caller can fall back to a plain default instead of
    generating a broken or empty regex."""
    safe = sorted({token for v in alphabet if (token := _regex_token_for_label(v)) is not None})
    if not safe:
        return None
    random.shuffle(safe)
    split = random.randint(1, min(3, len(safe)))
    first_group, rest = safe[:split], safe[split:]
    pattern = f"({'|'.join(first_group)})+"
    if rest:
        second_group = rest[:random.randint(1, min(2, len(rest)))]
        pattern += f"({'|'.join(second_group)})+"
    return pattern


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
    """Loads just enough to show column names/types, counts, and a small
    edge preview, before the user commits to a full compile+run -- cached
    so retyping other widgets doesn't reload the graph every rerun.
    `load_graph` (Stage A) guarantees an `edge_id` column exists even if
    the source didn't have one, so the preview below always has a real
    identifier column to show. No particular column is assumed to hold
    labels -- `label` is no longer a required (or even a special) column
    at this stage; see `_distinct_values` for the label-alphabet display,
    computed only for whichever column the user picks as the regex source."""
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
    return columns, column_types, n_edges, n_vertices, preview


@st.cache_data(show_spinner=False)
def _distinct_values(csv_bytes: bytes | None, path: str, column: str) -> list:
    """The regex alphabet for whichever column the user designates as the
    label source -- computed on demand for just that one column, not every
    column up front, since the label source is now a live user choice
    rather than a fixed 'label' column."""
    conn = duckdb.connect()
    if csv_bytes is not None:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(csv_bytes)
            path = tmp.name
    load_graph(conn, path)
    return [row[0] for row in
            conn.execute(f'SELECT DISTINCT "{column}" FROM edges ORDER BY "{column}"').fetchall()]


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
st.caption("Path query = an optional label regex + a selective aggregate over a property graph, "
           "compiled to recursive SQL and run on DuckDB.")

with st.sidebar:
    st.header("1. Graph data")
    uploaded = st.file_uploader("Edges CSV (required columns: src, dst)", type="csv")
    dataset_label = uploaded.name if uploaded else "bundled sample: ReCAP/simple_dataset/LG.csv"
    st.caption(f"Using: {dataset_label}")

    try:
        csv_bytes = uploaded.getvalue() if uploaded else None
        edge_columns, column_types, n_edges, n_vertices, edge_preview = _probe_schema(
            csv_bytes, DEFAULT_DATASET)
        st.success(f"{n_edges:,} edges, {n_vertices:,} vertices")
        st.caption(f"columns: {', '.join(edge_columns)}")
    except RecapCompilerError as exc:
        _friendly_error(exc)
        st.stop()

st.subheader(f"Edge data (first {EDGE_PREVIEW_ROWS} rows)")
st.dataframe(edge_preview, height=250)

st.header("2. Label regex (optional)")
st.caption("A selective aggregate doesn't inherently need a regex/NFA -- pick a label column here "
           "only if this query should also filter by a path through specific edge labels.")

string_columns = [c for c in edge_columns if c not in {"src", "dst", "edge_id"}
                  and _is_string_type(column_types[c])]
label_options = [_NO_REGEX_OPTION] + string_columns
default_index = label_options.index("label") if "label" in string_columns else 0
label_column = st.selectbox(
    "Label column", label_options, index=default_index,
    help="Any string column works, not just one literally named 'label' -- its distinct values "
         "become the regex alphabet. Leave the no-regex option selected to skip label matching "
         "entirely and explore every edge up to the length bound instead.")
use_regex = label_column != _NO_REGEX_OPTION

if use_regex:
    alphabet = _distinct_values(csv_bytes, DEFAULT_DATASET, label_column)
    st.caption(f"label alphabet from '{label_column}' ({len(alphabet)}): {', '.join(map(str, alphabet))}")
    default_regex = _random_regex_from_alphabet(alphabet) or _DEFAULT_REGEX
    # An explicit, column-scoped key (not including the random text itself)
    # keeps this widget's identity stable across reruns -- Streamlit only
    # applies `value=` the first time a given key appears, so retyping
    # elsewhere on the page never resets what the user has edited here, but
    # switching to a different label column (a genuinely new key) gets a
    # fresh random example drawn from that column's own alphabet.
    regex = st.text_input("Label regex", value=default_regex, key=f"label_regex::{label_column}")
    with st.expander("Regex syntax help (FR-36)"):
        st.markdown(
            "- `|` union -- `a|b` matches label `a` or label `b`\n"
            "- concatenation (write atoms next to each other) -- `ab` matches `a` then `b`\n"
            "- `*` zero or more -- `a*` matches zero or more `a` edges in a row\n"
            "- `+` one or more -- `a+` matches one or more `a` edges in a row\n"
            "- `?` optional -- `a?` matches zero or one `a` edge\n"
            "- `{m,n}` bounded repetition -- `a{2,3}` matches 2 or 3 `a` edges in a row\n"
            "- `\"...\"` quote a label containing a metacharacter or whitespace as one token "
            "-- e.g. `(\"North America\"|Asia)+`\n\n"
            f"Example from this dataset's own alphabet: `{default_regex}`")
    try:
        nfa = compile_regex_to_nfa(regex)
        relation = build_transitions_relation(nfa)
        nfa, relation, ambiguity_warning = guard_against_ambiguity(regex, nfa, relation)
        if ambiguity_warning:
            st.warning(ambiguity_warning)
    except RecapCompilerError as exc:
        _friendly_error(exc)
        st.stop()
else:
    st.caption("No label regex -- every edge up to the length bound will be explored, filtered "
               "only by the selective aggregate below.")
    regex = None
    relation = None

st.header("3. Start vertices and length bound")
start_mode = st.radio("Start vertices", ["Explicit vertex id(s)", "Out-degree band"], horizontal=True)
if start_mode == "Explicit vertex id(s)":
    start_vertex_ids_text = st.text_input(
        "Start vertex id(s)", value="383",
        help="One id, or several separated by `;` (e.g. `383;12;97`) (FR-37). Leave empty to "
             "start from every distinct src vertex in the Edges table instead (FR-4's "
             "all-vertices default).")
    degree_band = None
else:
    degree_band = st.selectbox("Out-degree band", ["low", "medium", "high"], index=2)
    start_vertex_ids_text = None

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

st.caption("Pick library aggregate(s), author a custom one, or both -- everything picked/authored "
           "below combines into a single query (FR-34): the union of dictionary keys and the "
           "conjunction of viability checks, so a path must satisfy all of them.")

selected_library_aggregates: list[SelectiveAggregate] = []

use_library = st.checkbox("Use library aggregate(s) (FR-13)", value=True, key="use_library")
if use_library:
    aggregate_kinds = st.multiselect(
        "Aggregate(s) -- pick more than one to combine them into one query (FR-34), "
        "e.g. bounded range + trail",
        ["Bounded range (max - min <= U)", "Adjacent-edge predicate", "Trail (no repeated edges)"],
        default=["Bounded range (max - min <= U)"], key="aggregate_kinds",
        help="Combining takes the union of the picked aggregates' dictionary keys and the "
             "conjunction of their viability checks -- a path must satisfy all of them. Picking "
             "the same kind twice isn't supported here (its dictionary keys would collide); use "
             "distinct properties across different kinds instead.")
    if not aggregate_kinds:
        st.warning("'Use library aggregate(s)' is checked but none are picked -- uncheck it, or "
                   "pick at least one below.")
    for kind in aggregate_kinds:
        st.markdown(f"**{kind}**")
        if kind in ("Bounded range (max - min <= U)", "Adjacent-edge predicate"):
            if not numeric_property_candidates:
                st.warning("No numeric edge columns found -- this aggregate needs one "
                           "(GREATEST/LEAST/subtraction don't apply to text columns).")
                st.stop()
            kind_property = st.selectbox(
                "Property", numeric_property_candidates, key=f"property::{kind}",
                help="Only numeric columns are offered -- this aggregate does arithmetic "
                     "(max/min/subtraction) on the property, which isn't meaningful for text.")
            if kind == "Bounded range (max - min <= U)":
                kind_upper_bound = st.number_input(
                    "Upper bound U", value=500.0, key=f"upper_bound::{kind}")
                selected_library_aggregates.append(
                    bounded_range(property=kind_property, upper_bound=kind_upper_bound))
            else:
                kind_comparator = st.selectbox(
                    "Comparator (edge vs. last edge)", [">=", "<="], index=0,
                    key=f"comparator::{kind}")
                selected_library_aggregates.append(
                    adjacent_edge_predicate(property=kind_property, comparator=kind_comparator))
        else:
            kind_id_column = st.selectbox(
                "Edge id column", edge_columns, key=f"id_column::{kind}",
                index=edge_columns.index("edge_id") if "edge_id" in edge_columns else 0,
                help="Any column works here -- trail semantics only need "
                     "equality, not order, so text ids are fine too.")
            selected_library_aggregates.append(trail_via_edge_ids(id_column=kind_id_column))

custom_aggregate: SelectiveAggregate | None = None
use_custom = st.checkbox("Author a custom aggregate", value=False, key="use_custom")
if use_custom:
    custom_mode = st.radio(
        "Authoring mode", ["Factorized", "General"], horizontal=True, key="custom_mode",
        help="**Factorized**: `update_d`/`is_viable_d` are each one expression, applied "
             "regardless of NFA state -- fine when the constraint doesn't depend on where "
             "the path is in the regex. **General**: `update_d`/`is_viable_d` may each "
             "differ per `(from_state, to_state)` transition pair (Figure 5's per-transition "
             "boxes), edited as a table below instead of one CASE-statement text block.")
    st.caption("Convention: `D.<key>` for a dictionary field, `e.<column>` for an edge property"
               + (", plus bare `from_state`/`to_state` in General mode" if custom_mode == "General" else "")
               + ". **Dictionary keys are inferred automatically from `init_d`'s own struct "
               "literal --** there's no separate table to keep in sync by hand. Edit `init_d`, "
               "and the tracked keys (shown below it) follow.")

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
    _default_is_viable_d_final = "TRUE"
    _default_finalize_d = "D"

    custom_init_d = st.text_area(
        "init_d()", value=_default_init_d, height=80, key="custom_init_d",
        help="**Role:** the dictionary's value at the anchor (path length 0), before any edge "
             "is taken. Nothing is in scope here (no `D`, no `e`) -- build it from "
             "literals/constants only. Its keys and their types (shown below) are inferred "
             "directly from this struct literal.\n\n"
             "**Examples:** `{last_time: NULL}` (one nullable DOUBLE key); "
             "`{max_amt: -1e308, min_amt: 1e308}` (two DOUBLE keys, seeded so the first real "
             "edge always widens the range).")

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

    custom_update_d_dict: dict[tuple[int, int], str] | None = None
    custom_is_viable_d_dict: dict[tuple[int, int], str] | None = None

    if custom_mode == "Factorized":
        custom_update_d = st.text_area(
            "update_d(D, e)", value=_default_update_d, height=80, key="custom_update_d",
            help="**Role:** how `D` changes when extending a path by one edge `e`. Two accepted "
                 "forms: a struct literal `{key: expr, ...}`, or one or more `D.<key> = <expr>` "
                 "assignments, one per line (or separated by `;` on one line). Either way, you "
                 "don't have to mention every key from `init_d`: leave one out and it "
                 "automatically keeps its previous value unchanged instead of being removed "
                 "from `D`. Accumulator-style assignments may also use `+=`/`-=`/`*=`//=` "
                 "(expanded to the equivalent `D.key = D.key <op> (expr)` before anything else "
                 "happens to it). Only `D.<key>` (dot notation) is recognized, not `D[\"key\"]`."
                 "\n\n**Examples:** `{last_time: e.time}` (struct form); "
                 "`D.total_amount += e.amount` (assignment form with augmented assignment).")
    else:
        # General (Figure 5): update_d/is_viable_d may differ per (from_state,
        # to_state) transition pair, edited as one table instead of a per-pair
        # text box each -- the scale problem that kept non-factorized authoring
        # out of the workbench until now. `relation` may be None (no regex
        # picked in Section 2); fall back to the same trivial_relation() Stage
        # A/G already use for a regex-less query, so the table still has
        # something to show (a single (0,0) row).
        _table_relation = relation if relation is not None else trivial_relation()
        _pairs = sorted({(frm, to) for frm, to, _label in _table_relation.rows})
        _labels_by_pair: dict[tuple[int, int], set[str]] = {}
        for _frm, _to, _label in _table_relation.rows:
            _labels_by_pair.setdefault((_frm, _to), set()).add(_label)

        if len(_pairs) > 50:
            st.warning(f"This automaton has {len(_pairs)} transition pairs -- editing all of "
                       "them here may be slow. A simpler/shorter regex produces fewer pairs.")

        st.caption(f"One row per `(from\\_state, to\\_state)` transition pair ({len(_pairs)} "
                   "total). Unedited rows default to `D` (unchanged) / `TRUE` (always viable) "
                   "-- edit only the rows that need real logic. `labels` is shown for context "
                   "and isn't itself editable.")
        _default_table = pd.DataFrame([
            {"from_state": frm, "to_state": to,
             "labels": ", ".join(sorted(_labels_by_pair[(frm, to)])),
             "update_d": "D", "is_viable_d": "TRUE"}
            for frm, to in _pairs
        ])
        _table_key = f"general_table::{regex if use_regex else '(no regex)'}"
        _edited_table = st.data_editor(
            _default_table, key=_table_key, hide_index=True, width="stretch",
            num_rows="fixed", disabled=["from_state", "to_state", "labels"])

        custom_update_d_dict = {}
        custom_is_viable_d_dict = {}
        for _, _row in _edited_table.iterrows():
            _pair = (int(_row["from_state"]), int(_row["to_state"]))
            custom_update_d_dict[_pair] = (_row["update_d"] or "D").strip() or "D"
            custom_is_viable_d_dict[_pair] = (_row["is_viable_d"] or "TRUE").strip() or "TRUE"

    with st.expander("Merge-function authoring box (FR-35, sketch only -- not run)"):
        st.caption(
            "FR-35: sketch how two fragments *both running the `update_d` above* would "
            "compose their dictionaries at a seam (e.g. for a split/wavefront-style plan, "
            "R4.O2 -- see FR-7 and Section 12 non-goal 3). Authoring aid only: nothing here "
            "is parsed, validated, or used by Compile & run below -- no split/merge execution "
            "plan is generated from it in this revision.")
        _merge_default_d = custom_init_d if dictionary_keys else "{last_time: NULL}"
        merge_d1 = st.text_area("D1", value=_merge_default_d, height=60, key="merge_d1",
                                 help="Sketch of the first fragment's dictionary -- same "
                                      "shape as this aggregate's own init_d by default, "
                                      "since both fragments run the same update_d above.")
        merge_d2 = st.text_area("D2", value=_merge_default_d, height=60, key="merge_d2",
                                 help="Sketch of the second fragment's dictionary.")
        if dictionary_keys:
            _merge_default_body = "{" + ", ".join(
                f"{k.name}: D1.{k.name}" for k in dictionary_keys) + "}"
        else:
            _merge_default_body = "D1"
        merge_function_body = st.text_area(
            "merge(D1, D2)", value=_merge_default_body, height=60, key="merge_function_body",
            help="Sketch of how D1 and D2 combine into one dictionary at the seam vertex. "
                 "Prefilled to just keep D1's value per key -- edit each key to whatever "
                 "combination makes sense for it (e.g. GREATEST/LEAST for a running "
                 "extremum, list_concat for a trail).")

    if custom_mode == "Factorized":
        custom_is_viable_d = st.text_area(
            "is_viable_d(D, e)", value=_default_is_viable_d, height=80, key="custom_is_viable_d",
            help="**Role:** the early-filtering check (Definition 8) -- a single Boolean "
                 "expression over the dictionary *before* this hop's `update_d` and the "
                 "candidate edge `e`. Returning `FALSE` prunes this extension immediately, "
                 "before it's ever added to the path.\n\n"
                 "**Examples:** `NOT list_contains(D.edge_ids, e.id)` (trail: reject a repeated "
                 "edge); `D.last_time IS NULL OR e.time >= D.last_time` (non-decreasing "
                 "timestamps).")
    # In General mode, is_viable_d is already captured per-row in the table above
    # (custom_is_viable_d_dict) -- no separate single-body widget to show here.

    custom_is_viable_d_final = st.text_area(
        "is_viable_d_final(D)", value=_default_is_viable_d_final, height=60,
        key="custom_is_viable_d_final",
        help="**Role:** the one-time check applied to a completed path's final `D`, in "
             "addition to `is_viable_d` having held at every hop -- e.g. a total that can "
             "only be evaluated once the path is done.\n\n"
             "**Examples:** `TRUE` (no additional final check, the default); "
             "`D.total_amount >= 1000` (require a minimum total only at the end).")
    custom_finalize_d = st.text_area(
        "finalize_d(D)", value=_default_finalize_d, height=60, key="custom_finalize_d",
        help="**Role:** what a matched path actually reports for `D` in the result set -- "
             "usually the whole dictionary, but it may project down to just the part worth "
             "returning.\n\n"
             "**Examples:** `D` (report the whole dictionary, the default); "
             "`D.edge_ids` (report only the trail, dropping any other tracked keys).")

    if dictionary_keys is not None:
        if custom_mode == "Factorized":
            custom_aggregate = SelectiveAggregate(
                dictionary_keys=dictionary_keys,
                init_d=custom_init_d,
                update_d=custom_update_d,
                is_viable_d=custom_is_viable_d,
                is_viable_d_final=custom_is_viable_d_final,
                finalize_d=custom_finalize_d,
                factorized=True,
            )
        else:
            custom_aggregate = SelectiveAggregate(
                dictionary_keys=dictionary_keys,
                init_d=custom_init_d,
                update_d=custom_update_d_dict,
                is_viable_d=custom_is_viable_d_dict,
                is_viable_d_final=custom_is_viable_d_final,
                finalize_d=custom_finalize_d,
                factorized=False,
            )

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
    if use_regex:
        with timed_stage(breakdown, "B: regex -> NFA"):
            nfa = compile_regex_to_nfa(regex)
        with timed_stage(breakdown, "C: build transitions relation"):
            relation = build_transitions_relation(nfa)  # deterministic (NFR-1) -- same content as above
        with timed_stage(breakdown, "C: ambiguity guard"):
            nfa, relation, ambiguity_warning = guard_against_ambiguity(regex, nfa, relation)
        if ambiguity_warning:
            st.warning(ambiguity_warning)
    else:
        # No regex chosen -- still a real (if trivial) automaton, so this
        # goes through the exact same Stage E/F code path as a real regex.
        relation = trivial_relation()

    if use_custom and custom_aggregate is None:
        st.error("Fix init_d above before running -- its keys couldn't be inferred.")
        st.stop()

    all_aggregates = list(selected_library_aggregates)
    if use_custom and custom_aggregate is not None:
        all_aggregates.append(custom_aggregate)

    if not all_aggregates:
        st.error("Pick at least one library aggregate, or check 'Author a custom aggregate', "
                 "before running.")
        st.stop()
    elif len(all_aggregates) == 1:
        aggregate = all_aggregates[0]
    else:
        # FR-34: more than one picked/authored above -> combine into one aggregate
        # (library + library, library + custom, or custom + custom all go through here).
        aggregate = combine_library_aggregates(*all_aggregates)

    conn = duckdb.connect()
    if uploaded:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(uploaded.getvalue())
            dataset_path = tmp.name
    else:
        dataset_path = DEFAULT_DATASET
    with timed_stage(breakdown, "A: load graph"):
        handle = load_graph(conn, dataset_path,
                             label_column=label_column if use_regex else None)
        if not use_regex:
            # Every edge must carry trivial_relation()'s constant label for
            # its self-loop to actually match every edge, regardless of any
            # real label the source data has.
            set_trivial_label_column(conn)

    with timed_stage(breakdown, "D: validate aggregate"):
        validate_selective_aggregate(aggregate, edge_columns=set(edge_columns), transitions=relation)

    with timed_stage(breakdown, "A: select start vertices"):
        if start_vertex_ids_text is not None:
            stripped_ids_text = start_vertex_ids_text.strip()
            if not stripped_ids_text:
                starts = select_start_vertices(handle)  # FR-4 all-vertices default
            else:
                try:
                    explicit_ids = [int(piece.strip()) for piece in stripped_ids_text.split(";")
                                     if piece.strip()]
                except ValueError:
                    st.error("Start vertex id(s) must be integers separated by `;` "
                             "(e.g. `383;12;97`).")
                    st.stop()
                starts = select_start_vertices(handle, ids=explicit_ids)
        else:
            starts = select_start_vertices(handle, degree_band=degree_band)
        if len(starts) > MANY_START_VERTICES_CAP:
            st.warning(f"{len(starts)} start vertices selected; using the first "
                       f"{MANY_START_VERTICES_CAP} to keep this responsive.")
            starts = starts[:MANY_START_VERTICES_CAP]

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
    st.metric("Intermediate paths explored", f"{optimized_result.telemetry.intermediate_paths:,}",
              help=_INTERMEDIATE_PATHS_HELP)
    st.caption(f"(+{optimized_result.telemetry.intermediate_count_ms:.1f} ms to compute that count, "
               f"not included in Runtime above)")
    st.metric("Peak DuckDB buffer memory", f"{optimized_result.telemetry.peak_buffer_memory_mb:,.1f} MB",
              help=_PEAK_MEMORY_HELP)
    st.dataframe(_expand_struct_columns(optimized_result).head(200))

if compare_to_standard:
    with col_std:
        st.subheader("Standard (Stage E, unoptimized)")
        st.code(standard_query.sql, language="sql")
        st.metric("Paths found", f"{len(standard_result.rows):,}")
        st.metric("Runtime", f"{standard_result.telemetry.runtime_ms:.1f} ms")
        st.metric("Intermediate paths explored", f"{standard_result.telemetry.intermediate_paths:,}",
                  help=_INTERMEDIATE_PATHS_HELP)
        st.caption(f"(+{standard_result.telemetry.intermediate_count_ms:.1f} ms to compute that count, "
                   f"not included in Runtime above)")
        st.metric("Peak DuckDB buffer memory", f"{standard_result.telemetry.peak_buffer_memory_mb:,.1f} MB",
                  help=_PEAK_MEMORY_HELP)
        st.caption("This connection ran the optimized query first, so this figure is the peak "
                   "since then, not isolated to just this query -- DuckDB's own profiler tracks "
                   "a high-water mark for the whole connection, not per query.")
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
