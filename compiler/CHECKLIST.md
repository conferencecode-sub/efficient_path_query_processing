# ReCAP Compiler — Build Checklist

Tracks progress against `new_compiler_requirements/compiler_reqs.md` (repo
root; the `requirements/` folder was renamed to `new_compiler_requirements/`
on 2026-08-07). Updated as of **2026-08-10**. When an item is finished, fill
in its "Completed" date (don't backdate/guess -- use the date the tests
actually passed) and link the code + tests.

**2026-08-10: spec reconciliation resolved.** `new_compiler_requirements/`
briefly had two drafts. Decision: keep `compiler_reqs.md`'s existing
Section 13 framing for the negative-stability verifier (Module H stays a
stretch objective, SMT work deferred -- the FULL draft's un-demotion of it
was **not** adopted), but merge in the FULL draft's **Part II / Module J**
(LLM-assisted selective-aggregate authoring, FR-34..43, NFR-6..9) as a new
committed-but-optional part of `compiler_reqs.md`. `recap_compiler_requirements_FULL.md`
is now marked superseded in its own header; `compiler_reqs.md` is the single
source of truth again. Module J is an optional branch inside Stage D (a
proposer that drafts `is_viable_d`/`update_d` bodies into D's skeleton,
gated by a fail-safe negative-stability classification and the same
FR-14/FR-23 validation as manual input) -- added as its own row below,
**not started**, and explicitly not required for the mini end-to-end demo
(regex + aggregate + graph -> DuckDB results) that Stage E/G below target.

Build order rationale: A and B+C have no dependencies on the rest of the
pipeline and unblock everything downstream (D and E both need the
transitions relation from C; E needs an aggregate from D), so they're built
and tested first. F (the optimizer) is the paper's actual novel contribution
but needs E working as its correctness baseline (NFR-2 compares standard vs.
optimized output), so it comes after E/G. I (the workbench UI) comes last --
it's glue over an already-working pipeline.

## Pipeline stages

| # | Stage | Requirements | Status | Completed | Code | Tests |
|---|---|---|---|---|---|---|
| A | Data ingestion | FR-1..FR-4 | **Done** | 2026-08-06 (edge_id fix 2026-08-10) | `src/recap_compiler/ingestion.py` | `tests/test_ingestion.py` (13 cases) |
| B | Regex frontend (Thompson's construction) | FR-5..FR-8 | **Done** | 2026-08-06 | `src/recap_compiler/regex_frontend.py` | `tests/test_regex_frontend.py` (14 cases) |
| C | NFA -> transitions relation | FR-9, FR-10 | **Done** | 2026-08-06 | `src/recap_compiler/transitions.py` | `tests/test_transitions.py` (6 cases) |
| D | Selective-aggregate frontend + skeleton generation | FR-11..FR-14 | **Done** | 2026-08-10 | `src/recap_compiler/selective_aggregate.py` | `tests/test_selective_aggregate.py` (15 cases) |
| E | Standard ReCAP SQL generation | FR-15..FR-18 | **Done** (unoptimized -- see notes) | 2026-08-10 | `src/recap_compiler/standard_sql.py` | `tests/test_standard_sql.py` (5 cases) |
| G | Execution + telemetry | FR-24..FR-26 | **Done** (minimal telemetry) | 2026-08-10 | `src/recap_compiler/execution.py` | `tests/test_execution.py` (6 cases) |
| F | Optimizer: dictionary flattening + function inlining | FR-19..FR-23 | **Done** | 2026-08-10 | `src/recap_compiler/optimizer.py` | `tests/test_optimizer.py` (11 cases, incl. FR-22 equivalence) |
| I | Workbench orchestration | FR-32, FR-33 | **Done** (MVP scope -- see notes) | 2026-08-10 | `webapp/app.py` | manual/bare-mode smoke test only, no automated tests (see notes) |
| Section 7 | Error taxonomy (cross-cutting) | E-INPUT, E-REGEX now; E-REF/E-TYPE/E-UNSUPPORTED/E-EXEC land with D/F/G | **Scaffolded** | 2026-08-06 | `src/recap_compiler/errors.py` | exercised via A/B tests |
| H (stretch) | Negative-stability verifier | FR-27..FR-31 (Section 13, not committed) | Not started | -- | -- | -- |
| J (optional) | LLM-assisted selective-aggregate authoring | FR-34..FR-43, NFR-6..9 (Part II) | Not started | -- | -- | -- |

## Next up

Everything in the pipeline stages table is now done or explicitly deferred
(H stretch, J optional -- see the 2026-08-10 spec-reconciliation note
above). The natural next increments, none started:
- **Full FR-32 authoring flow**: today's workbench (below) only exposes the
  three FR-13 library aggregates with their parameters; it doesn't let the
  author edit the generated `CASE` skeleton in-browser the way the full
  spec describes. That needs a real code-editor widget plus live
  re-validation against FR-14 as the author types.
- **Vertices-file upload** (workbench currently always infers vertices from
  edges; FR-2's explicit vertices-file path isn't wired into the UI).
- **Automated tests for `webapp/app.py`** -- see the Stage I completion note
  for why this cut has none and what a real test would need.

## Completed: I (2026-08-10) -- Streamlit workbench, MVP scope

Built per the user's explicit request for "the actual interface," scoped
down to what's buildable same-day (confirmed with the user before
building): a single-page Streamlit app (`webapp/app.py`) wired directly to
the existing A-G pipeline, not a mock. Sidebar: upload an edges CSV or fall
back to the bundled sample dataset (FR-33 -- nothing hard-coded, the sample
is only a default), with a schema probe (cached via `st.cache_data`) so the
aggregate-parameter dropdowns are populated from the real columns. Below
the sidebar, **outside any form**: the regex field, plus the live NFA
summary and the actual `T(from_state, to_state, label)` transitions table
(via `transitions.to_dataframe`) for whatever regex is currently typed.
Then a form for the rest: start-vertex selection (explicit id or an
out-degree band, FR-4 -- a band is capped to `DEGREE_BAND_CAP=5` vertices
so a careless pick doesn't trigger the same blowup documented below), a
picker over the three FR-13 library aggregates with their real parameters,
length bound, and a toggle to also run the unoptimized (Stage E) query for
an FR-22 comparison. On submit: builds and runs the optimized (Stage F)
query (and the standard one if requested), shows both SQL texts, results
(struct columns expanded via `pd.json_normalize` for readability),
telemetry, and a pass/fail FR-22 banner comparing `(v, q, path_length)`
signatures between the two. Every `RecapCompilerError` is caught and shown
as `[CATEGORY] message (at locus)` instead of a raw traceback -- Section
7's error taxonomy made visible in the UI, not just in tests.

**Follow-up fix, same day:** the regex field was originally *inside* the
query form alongside start vertices/aggregate/length bound. User reported
that changing the regex and re-submitting still showed the previous
regex's NFA state count -- i.e. the automaton looked stale. Root cause:
Streamlit only re-runs a form's body (and only recomputes anything that
reads a form widget) on that form's own submit event; nothing inside a
form is live as you type, and there's no visible feedback that a field's
value even changed until submit. Confirmed the underlying pipeline itself
was never the problem (`compile_regex_to_nfa`/`build_transitions_relation`
correctly produce a different NFA/transition-count for `'transfer'` (2
states, 1 transition) vs. `'a|b|c'` (10 states, 16 transitions) vs. the
default Q1 regex (36 states, 103 transitions) when called directly) --
this was purely a UI-structure bug, not a compiler bug. Fixed by moving the
regex field, and the NFA/transitions-table display, **out of the form**
entirely, so every keystroke triggers a fresh rerun and a fresh
`compile_regex_to_nfa` call, visible immediately as the transitions table
below the field. Only the expensive part (loading data, running the actual
query) stays inside the form, gated behind explicit submit. General lesson
for any future Streamlit work here: **only put a widget inside `st.form`
if its value should stay inert until an explicit submit** -- anything the
user expects to see react live (a preview, a validation message, a derived
table) has to live outside the form, even if that means the form ends up
covering less of the page than originally planned.

**Follow-up, same day: dropped the transitions table, added an edge preview,
and fixed a real correctness gap it surfaced.** User asked to stop showing
the transitions table (can get large -- Q1's regex alone produces 103 rows)
and instead show ~10 rows of the loaded edge data, with two requirements:
(1) the edge table needs a reliable unique row identifier even if the
source data doesn't have one, since FR-13(ii)'s trail semantics
(`trail_via_edge_ids`) depend on one; (2) if a `label` column exists, show
the distinct label values so the user has the actual alphabet in front of
them while writing a regex.

(1) is a real gap in Stage A, not just a UI concern -- fixed there, not in
the UI, so every caller benefits: `ingestion.load_graph` now calls a new
`_ensure_edge_id`, which adds a synthetic `edge_id` (0-based row position)
via `CREATE OR REPLACE TABLE edges AS SELECT row_number() OVER () - 1 AS
edge_id, * FROM edges` if the loaded data doesn't already have an
`edge_id` column -- a no-op, real ids left untouched, when it does (the
bundled sample already has its own `edge_id`, so this is invisible for the
demo dataset but matters for anything a user uploads). Two new tests in
`test_ingestion.py` cover both branches (13 cases now, 70 passing total).
`webapp/app.py`'s cached schema probe (`_probe_schema`) now also returns a
10-row edge preview (`EDGE_PREVIEW_ROWS`) and, when a `label` column
exists, the sorted distinct label list -- both shown before the regex
field, replacing the removed transitions-table display. Re-verified via the
same bare-mode direct-execution technique (exit 0, no exceptions) plus a
standalone check against the real dataset confirming the alphabet
(`phishing, purchase, sale, scam, transfer`) and preview render correctly.

**Follow-up, same day: filter the property picker by actual column type,
not just by name.** The `bounded_range`/`adjacent_edge_predicate` property
dropdown originally offered every edge column except `src`/`dst`/`label`,
with no regard for type -- picking a text column (e.g. `location_region`,
`purchase_pattern`) would generate SQL that fails at runtime, since
`GREATEST`/`LEAST`/subtraction don't apply meaningfully to text. User's own
framing: "we cannot assume the column names... would have to select a
numerical column." Fixed by reading real DuckDB types from `DESCRIBE
edges` (`_probe_schema` now also returns a `column_types` dict) and
filtering to columns whose type matches `_is_numeric_type` (a substring
check against `INT`/`FLOAT`/`DOUBLE`/`DECIMAL`/`REAL`/`NUMERIC`/`HUGEINT`,
covering every DuckDB integer width/signedness plus `DECIMAL(p,s)`). If no
numeric column exists at all, the UI now warns and stops rather than
offering a selection that's guaranteed to fail. `trail_via_edge_ids`'s id
column is deliberately left unfiltered (equality-only semantics, so a text
id is fine). Verified directly against the real dataset: numeric candidates
correctly resolve to `amount, edge_id, hour_of_day, ip_prefix,
login_frequency, risk_score, session_duration, timestamp_ms`; text columns
(`age_group, anomaly, location_region, purchase_pattern`) correctly
excluded. As the user noted themselves, the real long-term fix is the
full FR-32 skeleton editor (any column, any expression, author's
responsibility) -- this is a stopgap for the pre-built-library-only MVP.

**Explicitly out of scope for this cut, not silently dropped:** in-browser
`CASE`-skeleton editing (FR-12's full authoring flow -- this cut only
exposes the three pre-written library aggregates), the negative-stability
check (Section 13, itself still a stretch objective) and the LLM proposer
(Module J, still unbuilt) both have no UI hook since neither exists yet,
and explicit vertices-file upload (edges-only, vertices always inferred).

**Testing note, for whoever touches this file next:** no browser-automation
tool was available this session, so this couldn't get a real click-through
test. What *was* done: (1) launched via `streamlit run` and confirmed the
server starts and serves HTTP 200 with no exceptions in the server log; (2)
ran `python3 webapp/app.py` directly (bypassing `streamlit run`) -- in this
"bare mode," Streamlit's widgets return their coded `value=`/`index=`
defaults and `st.stop()` is a no-op, so the *entire* script, including the
full default-configuration pipeline run (both the optimized and standard
query, real `load_graph`/`compile_regex_to_nfa`/`run_query` calls against
the real sample dataset), executed to completion with exit code 0 and zero
exceptions in the log. This is a real, if partial, correctness signal for
the non-widget logic -- it is *not* a substitute for actually clicking
through the file uploader, the radio/selectbox branches, or the
degree-band cap, none of which the default path exercises. If a browser
tool becomes available, use it here before trusting this UI further.

## Completed: F (2026-08-10) -- optimizer, and a real, verified finding about *why* it helps

FR-22 (semantics-preserving) is checked directly, not just argued: eleven
tests in `tests/test_optimizer.py` build both the standard (Stage E) and
optimized (Stage F) query for the same aggregate/relation/inputs and assert
identical `(v, q, path_length)` result sets, across several length bounds,
a non-factorized aggregate, and the zero-dictionary-keys edge case.

**Real finding, checked before writing it down (don't repeat the mistake of
asserting a performance story you haven't measured):** the original plan
was "inlining removes macro-call overhead." Measured on the real dataset
(Q1 regex, `bounded_range`, vertex 383, length_bound=4, 3 runs each),
Stage F was only about **1.1x faster** (~630ms standard vs. ~545ms
optimized) -- not the dramatic win "removes function-call overhead" would
predict. Checked why via `EXPLAIN` on the standard query: **none of the
five macro names appear anywhere in the physical plan.** DuckDB expands
scalar SQL macros at bind time, before physical planning -- there is no
per-call dispatch cost to remove in the first place. The real (smaller, but
genuine) win is that reading a native flattened column (`p.max_amount`) is
cheaper than extracting a field out of a struct-typed column
(`p.D.max_amount`) on every one of the ~1.4M intermediate rows -- FR-19's
actual justification is avoiding *struct-access* overhead, not avoiding
function-call overhead. This would likely matter much more for a `D`
represented as JSON (FR-18's literal default) rather than a native DuckDB
struct, since JSON field extraction is markedly more expensive than struct
field access -- not verified here, since Stage E already uses a struct;
worth checking if this ever gets revisited. **Corrected the two places
below and in `demo_pipeline.py` that had the wrong (unverified) claim.**

- `src/recap_compiler/optimizer.py` -- `_decompose_struct` (FR-19: splits a
  Stage D struct-literal body into one raw expression per key -- this *is*
  the flattening, made easy by Stage D's own struct-literal convention),
  `_rewrite_node`/`_rewrite_sql` (FR-20: `D.<key>`/bare `D`/`from_state`/
  `to_state` -> real column refs, via the same `sqlglot` AST approach as
  Stage D's validator), `_flatten_update_d`/`_flatten_is_viable_d` (FR-21:
  `CASE` over transition pairs when non-factorized), `build_optimized_query`
  (same shape as Stage E's `build_standard_query`, no macros, no `D` struct
  column internally). `OptimizedQuery` exposes the same `.sql`/`.cte` shape
  as `StandardQuery`, so `execution.run_query` accepts either unchanged.
- `demo_pipeline.py` now builds and runs *both* the standard and optimized
  query on the same connection, asserts their `(v, q, path_length)` result
  sets match (a live FR-22 check, not just a claim), and prints the timing
  comparison above.

## Completed: D, E, G (2026-08-10) -- the mini end-to-end demo works

Per the user's explicit request: no flattening/inlining yet (that's F,
still not started) and no LLM module (J, also not started) -- E instead
pastes each of Stage D's five function bodies **verbatim** as DuckDB
`CREATE MACRO` bodies, named after Definition 8's own functions, and the
generated recursive CTE calls them by name
(`is_viable_d(p.D, p.q, t.to_state, e)`, etc.). This works with zero
identifier rewriting because DuckDB treats a bare table alias passed into a
macro as a STRUCT, so `e.<column>`/`D.<key>` field access inside a macro
body behaves exactly like it would inside a real query -- confirmed
empirically (see the gotcha below) before writing any of Stage E's code,
which is also why Stage D's identifier convention changed from bare
`<key>` to `D.<key>` (a real macro parameter, `D`, needs real struct field
access, not a bare identifier Stage E would otherwise have to rewrite).

- `src/recap_compiler/selective_aggregate.py` (Stage D, convention updated) --
  `SelectiveAggregate`/`DictionaryKey`, `generate_skeleton` (FR-12),
  `validate_selective_aggregate` (FR-14), three FR-13 library entries.
- `src/recap_compiler/standard_sql.py` (Stage E) -- `register_aggregate_macros`,
  `materialize_transitions`, `build_standard_query` (FR-15..18; FR-16's
  anchor-seeding fix for R4.O3 is a `VALUES` relation, not a free variable).
- `src/recap_compiler/execution.py` (Stage G) -- `run_query` (FR-24: paths/
  endpoints/count shapes; FR-25: `.sql` on the result; FR-26: wall-clock +
  intermediate-paths-explored telemetry, via a separate `paths`-CTE-only
  count query since the final query's row count isn't the same number --
  see the module docstring).
- `demo_pipeline.py` (repo root of `compiler/`) runs Stages A-E+G end to end
  against the real sample dataset (Q1's regex, `bounded_range` on `amount`,
  start vertex 383, length bound 4) and prints the generated SQL plus real
  DuckDB results -- ~130K viable paths in ~1s. See the repo README for how
  to run it, and the point below for why the length bound is kept small.

## Notes and gotchas for whoever touches this next

- **pyformlang's `Regex` does not implement `+`, `?`, or `{m,n}`.** Verified
  directly against its source (`regex_objects.py`'s operator tables only list
  concatenation `.`, union `|`/`+` -- yes, `+` is a *union* alias there, not
  Kleene-plus -- Kleene star `*`, parens, and epsilon `$`/`epsilon`) and
  against `EpsilonNFA.accepts()`: `Regex("a+").to_epsilon_nfa().accepts(["a",
  "a"])` is `False`. It parses `+`/`?` without error, it just silently drops
  the operator. `regex_frontend.py` expands `+`, `?`, and `{m,n}` into
  pyformlang's native subset *before* parsing (see the module docstring for
  the exact rewrite rules). Any future change to that expansion needs a
  behavioral test via `.accepts()` or the `_accepts`-style simulator in
  `tests/test_regex_frontend.py` -- a passing parse is not evidence the
  operator did anything.
- **Multiple start states are the common case, not an edge case.** Any
  top-level alternation (e.g. `(transfer|purchase|sale)+ ...`, which is
  exactly Q1's real query) produces several epsilon-reachable start states
  after epsilon-removal. `transitions.py` (Stage C) synthesizes a single q0
  by unioning their outgoing transitions, per FR-9/FR-10 -- this is required,
  not optional, for the SQL template (Stage E) to have a single seed row.
- **Stage D's identifier convention, v2 (changed 2026-08-10 for Stage E):**
  every function body is one SQL expression; `D.<key>` is real DuckDB struct
  field access on the dictionary (bare `D` alone is the whole struct);
  `e.<column>` is an edge property; bare `from_state`/`to_state` are the NFA
  state variables, only in a non-factorized `update_d`/`is_viable_d` body.
  `D.<key>` replaced the original bare-`<key>` shorthand once Stage E's
  design was picked: Stage E pastes these bodies **verbatim** as the bodies
  of real `CREATE MACRO`s named `init_d`/`update_d`/etc. with parameters
  literally named `D`/`from_state`/`to_state`/`e` (Definition 8's own
  signatures), so whatever's valid inside the macro body has to already be
  real SQL against those parameter names -- no compiler-side rewrite step.
  `init_d()` takes no parameters at all, so nothing (not even `D`) is in
  scope inside it -- `_validate_init_d` rejects any reference. `sqlglot`'s
  AST still does all the FR-14 checking (`Column` nodes with
  `table=='D'`/`table=='e'`/bare); the struct-literal `{key: val}` syntax
  for constructing `D` still parses its keys as `Identifier`, not `Column`,
  so init_d/update_d bodies that *build* D are unaffected by this change --
  only bodies that *read from* D changed shape.
- **DuckDB treats a bare table alias passed into a macro as a STRUCT
  (confirmed empirically, not assumed):** `CREATE MACRO f(e) AS e.amount`
  then `SELECT f(e) FROM edges e` works, and struct/JSON-shaped macro
  parameters support `.field` access the same way. This is *why* Stage E
  can pass the whole candidate edge row (or `D`) as a single macro argument
  and have `e.<column>`/`D.<key>` just work, instead of the compiler
  needing to explode every column into its own macro parameter or rewrite
  identifiers into a different form.
- **This regex + dataset combination is deliberately expensive at depth --
  not a bug in E/G/F.** `(transfer|purchase|sale)+(phishing|scam)+` from a
  single start vertex has the same ~59x-branching blowup documented in
  `alternative_explorations/navigation_experiment/`: `length_bound=4` from
  vertex 383 runs in ~1s (129K results), but `length_bound=6` consumed 40GB+
  RAM and had to be killed, consistent with that experiment's own
  `max_length=6` findings. `demo_pipeline.py` deliberately uses
  `length_bound=4`.
- **DuckDB macro calls are *not* the performance cost they look like.**
  Confirmed via `EXPLAIN` that a scalar SQL macro's name never appears in
  the physical plan -- DuckDB expands it at bind time. So "Stage F removes
  macro-call overhead" is not the right performance story (see the Stage F
  completion note above for the real, measured one: ~1.1x from avoiding
  struct-field-access indirection on ~1.4M intermediate rows, not from
  removing a function call). If you're ever tempted to explain Stage F's
  speed to someone by saying "it removes function-call overhead," check
  `EXPLAIN` first -- it doesn't, for the reason above.
- **Known limitation, not yet handled:** edge labels containing regex
  metacharacters (`|()*+?{}$.` or whitespace) aren't escaped/quoted before
  being spliced into the pattern text, so such a label would confuse the
  parser. Not needed for the datasets exercised so far (`ReCAP/simple_dataset`);
  flag if a future dataset's labels need it.
