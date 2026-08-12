# ReCAP Compiler

Implementation of the compiler specified in
`../new_compiler_requirements/compiler_reqs.md` (that folder was
`../requirements/` until 2026-08-07 -- update any old links):
regex + selective aggregate + property graph -> optimized recursive SQL,
executed on DuckDB. See `CHECKLIST.md` for what's built so far, what's next,
and implementation notes/gotchas for whoever picks this up.

## Layout

```
compiler/
├── CHECKLIST.md              build progress, one row per pipeline stage
├── demo_pipeline.py           script: runs Stages A-G end to end, prints both
│                               the standard and optimized SQL plus real results
├── webapp/app.py              Stage I: Streamlit workbench UI over the same pipeline
├── pyproject.toml
├── src/recap_compiler/
│   ├── errors.py                    error taxonomy (Section 7 of the spec)
│   ├── ingestion.py                 Stage A: load graph, select start vertices
│   ├── regex_frontend.py            Stage B: regex -> epsilon-free NFA
│   ├── transitions.py               Stage C: NFA -> T(from_state,to_state,label), q0, Q_F
│   ├── selective_aggregate.py       Stage D: aggregate data model, CASE-skeleton
│   │                                 generation, FR-13 library entries, FR-14 validation
│   ├── standard_sql.py              Stage E: generates the WITH RECURSIVE query;
│   │                                 pastes Stage D's bodies as DuckDB macros
│   ├── optimizer.py                 Stage F: flattens D's dictionary keys into
│   │                                 real columns and inlines the macro bodies
│   │                                 directly into the query text
│   ├── execution.py                 Stage G: runs the query, shapes results
│   │                                 (paths/endpoints/count), basic telemetry
│   └── profiling.py                 diagnostic (not a spec stage): stage-by-stage
│                                     timing breakdown, used by the demo and the UI
└── tests/                     one test file per module above (webapp/app.py has
                                no automated tests yet -- see CHECKLIST.md)
```

## Setup

```bash
cd compiler
pip install -e '.[dev]'    # for running tests
pip install -e '.[ui]'     # for the Streamlit workbench (adds streamlit)
```

## Web UI (Stage I) -- the easiest way to demo this

```bash
cd compiler
pip install -e '.[ui]'
streamlit run webapp/app.py
```

This opens a browser tab: pick or upload an edges CSV (defaults to the
bundled sample dataset, nothing is hard-coded per FR-33). It shows a
10-row preview of the loaded edges and, if there's a `label` column, the
full label alphabet -- so you know what's actually in the data before
writing a regex against it. (Every edges table is guaranteed to have an
`edge_id` column, real or synthesized, per FR-13(ii)'s trail semantics --
see `CHECKLIST.md`.) Then type a label regex -- the NFA state/transition
count updates live as you type, before you've touched anything else (the
transitions table itself isn't shown, since it can get large fast). Then
choose start vertices, and then either pick one of the three FR-13 library
aggregates and its parameters, or switch to **custom aggregate** to author
your own: write `init_d`, and its dictionary keys and their types are
inferred automatically (via DuckDB's own type system) and shown live in a
read-only table right below it -- there's no separate keys table to keep
in sync by hand, so editing `init_d` is always enough. Then write
`update_d`/`is_viable_d`/`is_viable_d_final`/`finalize_d`. Everything
starts pre-filled with a genuinely working example (equivalent to the
library's `bounded_range` on whatever numeric column is actually
available) rather than a placeholder -- clicking **Compile & run** with
nothing edited works out of the box, and editing from a real example is
easier than editing from a comment. `update_d` accepts two forms: a struct
literal `{key: expr, ...}`, or one or more `D.<key> = <expr>` assignments
(`;`-separated for more than one) -- whichever you use, you don't need to
mention *every* key `init_d` declares: leave one out and it keeps its
previous value unchanged instead of being dropped from `D`. Set
a length bound and click **Compile & run**.
It shows the generated SQL (both the unoptimized Stage E and optimized
Stage F versions, if you leave the comparison checkbox on), the actual
DuckDB results, a pass/fail banner confirming the two queries agree
(FR-22), and a stage-by-stage timing breakdown (bar chart + table) covering
every step that run went through -- parsing the regex, loading the graph,
validating the aggregate, generating SQL, and executing each query. On the
bundled dataset, expect loading to dominate the "compile" side (~230ms,
real CSV I/O) while every other pre-execution step is a few milliseconds
or less -- the query executions themselves are what actually costs time.

Every widget that configures the query is a plain (non-form) widget, so
the whole form reacts live -- switching between library/custom, or between
aggregate kinds, updates immediately, with no need to click a button first
to see the right fields appear.

This is an MVP scope, not the full spec'd authoring flow -- see
`CHECKLIST.md`'s Stage I completion note for exactly what's in vs. out
(short version: custom aggregates are **factorized only** -- a body that
depends on NFA state needs one expression per transition pair, and a real
regex can have 100+ pairs, so that's deliberately not offered here yet).

## Seeing it run without a browser

`demo_pipeline.py` does the same end-to-end run as a plain script instead
of a UI -- loads the real sample dataset, compiles the paper's Q1 regex,
generates *both* the standard (Stage E) and optimized (Stage F) SQL for the
same aggregate, runs both on DuckDB, checks they return the exact same
paths (FR-22), and prints a timing breakdown covering every stage it went
through, not just the two queries' own runtimes:

```bash
cd compiler
python3 demo_pipeline.py
```

It uses a single start vertex and a small length bound on purpose (see
`CHECKLIST.md`'s note on why this specific query blows up fast at depth --
it's a property of the regex/dataset combination, not a bug). Note on the
timing comparison: don't expect a dramatic speedup -- on this dataset it's
around 1.1x, because DuckDB already inlines simple SQL macros itself at
bind time (confirmed via `EXPLAIN`), so Stage F's real, smaller win is
avoiding struct-field-access overhead on `D`, not removing a function call.
See `CHECKLIST.md`'s Stage F completion note for the full story.

To poke at a stage directly instead, e.g. to try a different regex, a
different FR-13 library entry, or a different start vertex/length bound:

```python
import duckdb
from recap_compiler.ingestion import load_graph, select_start_vertices
from recap_compiler.regex_frontend import compile_regex_to_nfa
from recap_compiler.transitions import build_transitions_relation
from recap_compiler.selective_aggregate import bounded_range, validate_selective_aggregate
from recap_compiler.standard_sql import materialize_transitions
from recap_compiler.optimizer import build_optimized_query
from recap_compiler.execution import run_query

conn = duckdb.connect()
handle = load_graph(conn, "../ReCAP/simple_dataset/LG.csv")
nfa = compile_regex_to_nfa("(transfer|purchase|sale)+(phishing|scam)+")
relation = build_transitions_relation(nfa)

aggregate = bounded_range(property="amount", upper_bound=500.0)
edge_columns = {row[0] for row in conn.execute("DESCRIBE edges").fetchall()}
validate_selective_aggregate(aggregate, edge_columns=edge_columns)  # raises RefError if invalid

materialize_transitions(conn, relation)
starts = select_start_vertices(handle, ids=[383])
query = build_optimized_query(aggregate=aggregate, relation=relation,
                               start_vertices=starts, length_bound=3)  # max edges per path
print(query.sql)  # the generated SQL, inspectable (FR-25)

result = run_query(conn, query, result_shape="paths")  # or "endpoints" / "count"
print(len(result.rows), result.telemetry)
```

## Running tests

```bash
cd compiler
python3 -m pytest tests/ -v
```

## Status

A (ingestion), B (regex frontend), C (NFA -> transitions relation), D
(selective-aggregate frontend), E (standard SQL generation), F (the
optimizer), G (execution), and I (workbench UI, including live factorized
custom-aggregate authoring) are all implemented (91 automated tests across
A-G plus the timing-breakdown utility; I has no automated tests yet, see
`CHECKLIST.md`) -- a graph + regex + selective aggregate (library or
custom) can be compiled
to both unoptimized and optimized SQL, actually run on DuckDB, and driven
either from a script or a browser UI today, with FR-22 equivalence checked
directly in tests, the demo script, and the UI. H (the negative-stability
verifier) remains intentionally deferred/optional per the spec. An optional
LLM-assisted aggregate authoring module (J) was built, live-tested, and
then removed per explicit decision -- see `CHECKLIST.md` for that history.
