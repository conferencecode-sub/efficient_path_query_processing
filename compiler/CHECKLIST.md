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
| D | Selective-aggregate frontend + skeleton generation | FR-11..FR-14 | **Done** | 2026-08-10 | `src/recap_compiler/selective_aggregate.py` | `tests/test_selective_aggregate.py` (25 cases) |
| E | Standard ReCAP SQL generation | FR-15..FR-18 | **Done** (unoptimized -- see notes) | 2026-08-10 | `src/recap_compiler/standard_sql.py` | `tests/test_standard_sql.py` (5 cases; `update_d` normalization covered via `test_optimizer.py`'s FR-22 equivalence tests) |
| G | Execution + telemetry | FR-24..FR-26 | **Done** (minimal telemetry) | 2026-08-10 | `src/recap_compiler/execution.py` | `tests/test_execution.py` (6 cases) |
| F | Optimizer: dictionary flattening + function inlining | FR-19..FR-23 | **Done** | 2026-08-10 | `src/recap_compiler/optimizer.py` | `tests/test_optimizer.py` (16 cases, incl. FR-22 equivalence) |
| I | Workbench orchestration | FR-32, FR-33 | **Done** (MVP scope, now incl. factorized custom-aggregate authoring -- see notes) | 2026-08-10 | `webapp/app.py` | manual/bare-mode smoke test + a standalone script exercising the custom-aggregate code path directly (see notes); no automated UI tests |
| Section 7 | Error taxonomy (cross-cutting) | E-INPUT, E-REGEX now; E-REF/E-TYPE/E-UNSUPPORTED/E-EXEC land with D/F/G | **Scaffolded** | 2026-08-06 | `src/recap_compiler/errors.py` | exercised via A/B tests |
| -- | Stage-by-stage timing breakdown (not a spec item -- diagnostic/demo tool) | n/a | **Done** | 2026-08-10 | `src/recap_compiler/profiling.py` | `tests/test_profiling.py` (6 cases) |
| H (stretch) | Negative-stability verifier | FR-27..FR-31 (Section 13, not committed) | Not started | -- | -- | -- |
| J (optional) | LLM-assisted selective-aggregate authoring | FR-34..FR-43, NFR-6..9 (Part II) | Not started | -- | -- | -- |

## Next up

Everything in the pipeline stages table is now done or explicitly deferred
(H stretch, J optional -- see the 2026-08-10 spec-reconciliation note
above). The natural next increments, none started:
- **Non-factorized (per-transition) authoring in the UI.** User explicitly
  chose to skip this for now ("let's skip the nfa-state-dependent one") --
  a real regex can have 100+ transition pairs (Q1's has 103), so a
  one-text-box-per-pair UI doesn't scale as-is. Worth revisiting with a
  smarter design (e.g. only show pairs reachable from the chosen start
  vertices, or a real code-editor widget) rather than a plain form.
- **Vertices-file upload** (workbench currently always infers vertices from
  edges; FR-2's explicit vertices-file path isn't wired into the UI).
- **Automated tests for `webapp/app.py`** -- see the Stage I completion note
  for why this cut has none and what a real test would need.
- Module J (LLM-assisted authoring) now has a real place to plug into:
  the custom-aggregate editor's five text areas are exactly "the author's
  edit step" Module J's design assumes it drafts into (Part II, Section 1).

## Completed: `path_length` now starts at 0, not 1 (2026-08-10)

User asked for `path_length` to start at 0 "since we begin from a vertex
and not an edge" -- i.e. it should count *edges traversed*, not *vertices
visited*. Changed the anchor's literal in both `standard_sql.py` and
`optimizer.py` from `1 AS path_length` to `0 AS path_length` (the
recursive increment, `p.path_length + 1`, and the `WHERE p.path_length <
{length_bound}` guard are both unchanged text). **This is not purely
cosmetic -- it changes what `length_bound` means, on purpose:** since the
guard text didn't change, `length_bound` now directly equals the maximum
number of edges in any returned path (the standard graph-theoretic
definition of "path length"), instead of the old "max edges = length_bound
- 1" -- one hop deeper is now reachable for the same numeric `length_bound`
as before. This is a real behavior change, not just relabeling, and it's
the right one to make: the old off-by-one was confusing (`length_bound=4`
silently meaning "3 edges"), and the new version is exactly what the name
says.

**Caught the real consequence of this before shipping it, not after:**
re-ran `demo_pipeline.py` against the real dataset at the old
`LENGTH_BOUND=4` and it **timed out at 60s** -- because "one hop deeper"
at `length_bound=4` is now equivalent to the *old* `length_bound=5`, and
the earlier navigation-experiment findings already documented that this
exact regex/dataset combination (`~59x` branching in the "grow" state)
blows up between `length_bound=5` and `6` (old numbering) -- `length_bound=6`
old-numbering was already known to consume 40GB+ RAM. Fixed by lowering
`demo_pipeline.py`'s `LENGTH_BOUND` from `4` to `3` (new semantics) --
this reaches exactly the same 3 edges as the old default did, and
re-running confirmed byte-for-byte the same result: 129,377 paths, ~1s,
identical timing breakdown, sample rows' `path_length` values now correctly
0-indexed (e.g. a row previously labeled `path_length=3` now reads `2`).
Also lowered the workbench's `Length bound` widget default from `4` to `3`
and its `min_value` from `1` to `0` (since `0` is now a meaningful value --
"no edges, just the start vertex" -- not an invalid one), and updated its
help text to state the new "max edges" meaning directly.

Fixed six existing tests whose assertions hardcoded the old 1-indexed
`path_length` values (`test_standard_sql.py`,`test_optimizer.py`) --
recomputed each expected value by hand from the actual fixture graphs
(not just "subtract 1" -- confirmed for each one whether the reachable
*set* of vertices also changed now that one more hop is allowed, since
amount-based pruning and length-based pruning don't always cut off at the
same point). In every affected case, the amount-based `is_viable_d` check
already excluded the same vertices independent of the length bound, so
only the `path_length` *labels* needed correcting, not the reachable sets
-- but that had to be verified per test, not assumed. All 91 tests still
pass after the fixes.

## Minor: renamed `q0`/`accepting states` labels in the UI (2026-08-10)

Cosmetic only, no logic change: the NFA-summary line in `webapp/app.py`
(right below the regex field) said `q0 = ...` -- automata-theory jargon --
per user request, now reads `starting_state = ...` / `accepting_states =
...`. Internal names (`TransitionsRelation.q0`/`.accepting_states` in
`transitions.py`, used throughout `standard_sql.py`/`optimizer.py`) are
unchanged -- this only touches the one user-facing display line.

## Completed: dictionary keys are now inferred from `init_d`, not a separate table (2026-08-10)

Same session as the `update_d`-completion fix below, follow-on to it. User
hit the same underlying "two things must stay in sync" problem from a
different angle: after editing `init_d` to drop `min_amount`, they got
`[E-REF] init_d does not initialize declared key(s): ['min_amount']` --
correct behavior (that error is exactly what the previous fix added, on
purpose, since there's no sensible pass-through default at init time), but
the *cause* was the same design flaw: `webapp/app.py`'s dictionary-keys
table (`st.data_editor`) was an **independent, user-editable list**, not
derived from `init_d` -- so editing `init_d` alone could never be
"enough," no matter how the UI explained it. User's own diagnosis was
exactly right: "if I apply the changes in init_d, and remove min, it
should not appear in the table."

Fixed by removing the second source of truth instead of documenting
around it: `_infer_dictionary_keys(init_d_body)` derives both the key
*names* and their *SQL types* directly from `init_d`'s own struct literal,
by asking DuckDB itself -- `conn.sql(f"SELECT ({init_d_body}) AS d").types[0]`
returns a `DuckDBPyType` whose `.children` is exactly `[(name, type), ...]`
for a STRUCT (confirmed empirically before relying on it). A non-struct
`init_d` (bare `NULL`, etc.) infers zero keys, matching the existing
zero-dictionary-keys support. The dictionary-keys table is gone entirely;
the UI now shows a **read-only** table computed live from whatever
`init_d` currently says, right below the `init_d` text box, so the "I like
to keep track of the variables" visibility the user wanted is preserved --
it just can't go stale anymore, since it isn't independent input. The
inferred keys are computed once per rerun and reused directly at compile
time (no second inference pass, no duplicate-name guard needed either --
that was only ever a symptom of the table being independently editable).

**Consequence worth flagging, not yet hit but real:** type inference comes
from whatever literal `init_d` happens to write -- `{k: 0}` infers `k` as
`INTEGER`, not `DOUBLE`, which could surprise someone if `update_d` later
computes a `DOUBLE` for that key (DuckDB's recursive CTE generally
widens/casts this without complaint, but hasn't been stress-tested here).
Our own default custom-aggregate example already avoids this (`-1e308`/
`1e308` are float literals), and the zero-numeric-column fallback default
was changed from `{my_key: 0}` to `{my_key: 0.0}` for the same reason.
Worth a real test if this becomes a reported problem.

## Completed: `update_d` now also accepts `D.<key> = <expr>` assignment syntax (2026-08-10)

Same session, third round on `update_d` ergonomics. User tried exactly
this syntax unprompted (`"D.max_amount = GREATEST(D.max_amount, e.amount)"`)
and hit `[E-UNSUPPORTED] expected a struct literal ... to flatten` --
asked whether this was a DuckDB limitation. It wasn't: DuckDB has no
opinion here, the compiler had simply never implemented anything but the
struct-literal form (a choice explicitly flagged, and deliberately
deferred, in the previous `update_d`-completion note above). Given the
user reached for this syntax naturally on their own, twice now in spirit,
implemented it for real instead of continuing to explain around it.

Added `normalize_update_d_body(body, declared_keys)` to
`selective_aggregate.py`: accepts either the existing struct literal (and
delegates to `complete_update_d_body` unchanged) or one or more
`D.<key> = <expr>` assignment statements, `;`-separated -- parsed via
`sqlglot.parse` (not `parse_one`, which handles multi-statement input
ambiguously, silently keeping only the first statement in some cases --
confirmed empirically before relying on either function). Each assignment
statement must have a `D.`-qualified column on the left; an undeclared key
on the left raises `RefError` immediately (`update_d assigns undeclared
dictionary key ...`). Converts to the same completed-struct-literal
representation `complete_update_d_body` already produces, so a key left
unassigned defaults to pass-through, identically to the struct-literal
form. `standard_sql.register_aggregate_macros` (Stage E) and
`optimizer._flatten_update_d` (Stage F) both call `normalize_update_d_body`
now instead of `complete_update_d_body` directly -- same FR-22 reasoning
as before: pasting `D.key = expr` verbatim into a macro would be invalid
SQL (a boolean comparison, not a struct), so this conversion is load-
bearing for Stage E, not just a Stage F nicety.

**Fixed a related, pre-existing validation gap while wiring this in:**
`_referenced_columns` (Stage D's core validation primitive) used
`sqlglot.parse_one`, which -- per the same ambiguous multi-statement
behavior above -- could silently validate only the *first* of several
`;`-separated statements. Switched it to `sqlglot.parse` (list) and walk
every returned statement's columns. This was already a latent gap for
any multi-statement body (none existed before this feature), and a
happy side effect: since a `D.<key>` reference is validated as "must be
declared" regardless of whether it appears as the left or right side of
an `=`, the *existing* per-column validation logic already correctly
rejects an undeclared assignment target with no code changes beyond the
parser swap -- `normalize_update_d_body`'s own undeclared-key check is a
defense-in-depth backstop for callers that skip `validate_selective_aggregate`
(e.g. calling Stage E/F directly), not the primary gate.

8 new tests: 6 in `test_selective_aggregate.py` for `normalize_update_d_body`
itself (single assignment; multiple semicolon-separated assignments; still
handles a plain struct literal; still passes bare `D` through; rejects an
assignment to an undeclared key; rejects a non-assignment statement mixed
in with real ones), 2 in `test_optimizer.py` (the assignment form produces
identical results to the equivalent struct-literal form and to the library
`bounded_range`; a single assignment leaves the unmentioned key visibly
frozen at its init value). 91 tests passing total. Updated the workbench's
`update_d` help text to document both accepted forms.

## Completed: `update_d` no longer has to re-list every declared key (2026-08-10)

User asked two related things after using the new custom-aggregate editor:
whether dictionary fields could be read via `.` notation in `update_d` (they
already can -- `D.<key>` is the existing convention), and hit a real crash
trying to track only `max_amount` after dropping `min_amount` from
`update_d`'s struct literal while leaving it declared: a raw
`KeyError: 'min_amount'` traceback from `optimizer.py`, not a clean
compiler error. Root cause: `_flatten_update_d`/`_decompose_struct`
unconditionally assumed the struct literal covered every declared key --
dropping one wasn't a supported way to say "stop updating this field," it
was just an unhandled gap.

Decided against inventing a new `D.key = expr` assignment-statement syntax
(would need Stage E's macro-pasting to also start interpreting/
reconstructing `update_d` instead of pasting it verbatim, a bigger,
cross-stage change) in favor of a smaller fix that gets the same "easier"
result: **a key left out of the struct literal now defaults to passing its
previous value through unchanged** (`D.<key>`), instead of being silently
dropped or crashing. Added `complete_update_d_body(body, declared_keys)`
to `selective_aggregate.py` (Stage D) -- parses the body, and if it's a
struct literal missing any declared key, appends `key: D.key` for each one
via `sqlglot`'s AST `.append()` (confirmed via a quick check that DuckDB
accepts the resulting quoted-key struct syntax identically to bare keys);
returns the body **unchanged** (not just re-serialized) when nothing was
actually missing, and passes through non-struct bodies (bare `D`, or an
aggregate with no declared keys) untouched.

**Critical design point, not just a nice-to-have:** both Stage E
(`standard_sql.register_aggregate_macros`) and Stage F
(`optimizer._flatten_update_d`) now call `complete_update_d_body` **before**
doing anything else with `update_d` -- pasting a partial struct verbatim
into a macro (Stage E's old behavior) and completing-then-decomposing it
(Stage F) are not the same body, and doing only one of the two would have
silently broken FR-22 equivalence for exactly this case (the standard
query's `D` would be missing a field the optimized query's flattened
columns still tracked). Caught this while designing the fix, before
writing any code -- worth remembering as a general rule for this codebase:
**any change to how `update_d`/`is_viable_d`/etc. are interpreted must land
in both Stage E and Stage F identically, or write an equivalence test that
would have caught the divergence.**

Separately, `init_d` has the exact same shape of bug (`_decompose_struct`
then unconditional `fields[key]`) but *cannot* get the same fix -- there's
no previous value to default a missing key to at initialization. Turned
that into a clean `RefError` ("init_d does not initialize declared key(s):
[...]") instead of a raw `KeyError`.

7 new tests: 4 in `test_selective_aggregate.py` for `complete_update_d_body`
itself (fills a missing key; no-op when already complete; no-op on a
non-struct body; no-op with zero declared keys), 3 in `test_optimizer.py`
(a partial factorized `update_d` produces identical standard/optimized
results, with the omitted key visibly frozen at its init value; the same
for a non-factorized per-pair `update_d` where different pairs omit
different keys; `init_d` missing a key raises `RefError`, not `KeyError`).
Also updated the workbench's `update_d` help text to state the new
behavior explicitly, since the whole point was to make hand-writing
`update_d` easier.

## Completed: factorized custom-aggregate authoring in the UI (2026-08-10)

User confirmed starting with "the interactive end" (the skeleton-editing
UI, FR-32/FR-12) before Module J, explicitly scoped to skip non-factorized
(NFA-state-dependent) editing for the reason above. Added a live "Aggregate
source" radio (library vs. custom) to `webapp/app.py`'s aggregate section.
Custom mode: an editable dictionary-keys table (`st.data_editor`, add/
remove rows, `name`/`sql_type` columns) and five `st.text_area` widgets for
`init_d`/`update_d`/`is_viable_d`/`is_viable_d_final`/`finalize_d`. The
resulting `SelectiveAggregate` feeds into the exact same, unchanged Stage
E/F/G pipeline as a library aggregate -- no downstream code needed to
change, which is exactly the payoff of Stage D's factorized data model
already being a plain dataclass. Duplicate dictionary-key names are
rejected before validation with a clear message (real FR-14 validation
would otherwise fail confusingly on the duplicate struct key instead).

**Follow-up fix, same day: the first version's defaults were actively
misleading, and a user hit it immediately.** The initial defaults used
`generate_skeleton(relation, factorized=True)`'s placeholder text for
`update_d`/`is_viable_d` (a `-- TODO: single body (factorized): D`-style
SQL *comment*, not real SQL) and, separately, `init_d`'s placeholder
mentioned example key names `key1`/`key2` in a comment -- while the
dictionary-keys table defaulted to a single row named `example_key`. A
user copying the `key1` example into `update_d` got exactly the confusing
error this predicts: `[E-REF] update_d references undeclared dictionary
key 'D.key1'` -- correct behavior from FR-14's perspective (key1 really
wasn't declared), but a bad first experience, since nothing in the UI's
own defaults was internally consistent enough to just run. Fixed by
replacing the placeholder-comment approach entirely: the custom-aggregate
defaults are now a genuinely **working example**, dynamically built from
whatever numeric column is actually available (preferring `amount` when
present, since that's the bundled dataset's own property, falling back to
the first numeric column found so it still works on an upload without an
`amount` column) -- dictionary keys `max_<property>`/`min_<property>`, and
all five bodies filled in as a real, valid `bounded_range`-equivalent
aggregate. Clicking **Compile & run** with nothing edited now works and
returns the same result as the library `bounded_range` entry, rather than
being guaranteed to fail. `generate_skeleton` is no longer used by the UI
(the `-- TODO` comment style it produces is fine as a hint text artifact,
per FR-12's own framing, but was the wrong thing to use as directly-runnable
widget defaults) -- import removed accordingly. General lesson: **when a
default value can be run as-is, it should actually run** -- placeholder
comments invite exactly this kind of "user copies the example literally,
gets confused by an error that's technically correct" failure mode.
Verified with a standalone script (not the UI, since no browser tool is
available -- see the Stage I completion note) reproducing the app's exact
default-computation logic: `default_property` resolves to `amount` on the
bundled dataset, the generated bodies validate and run, and return the
identical 129,377 paths the library `bounded_range` entry does. Separately
re-confirmed (as in the original version of this note) that an
intentionally malformed body still raises a clean `RefError` rather than
crashing.

**Follow-up fix, same session: dropped `st.form` for the whole query
section, not just the regex field.** While wiring in the new "Aggregate
source" radio, checked whether a form's *internal* conditional widgets
(e.g. the existing aggregate-kind selectbox revealing different parameter
widgets) actually update live before submit, since the new radio has the
same shape of problem the regex field had. Checked Streamlit's own
`st.form` docstring rather than assume: "interacting with a widget inside
the form will do nothing" until submit -- confirming the *existing*
aggregate-kind conditional already had this same latent bug (just never
reported, unlike the regex one), and the new library/custom toggle would
inherit it. Since profiling showed only `load_graph` (~230ms) and the two
query executions (~900-1000ms each) are actually expensive -- see the
timing-breakdown completion note below -- there was no performance reason
to keep any of the configuration widgets inside a form. Removed
`st.form("query_form")` entirely; every widget that configures the query
(start vertices, length bound, aggregate source/kind/custom bodies) is now
a plain widget that reruns and re-renders live, and a plain `st.button`
("Compile & run") gates just the expensive load-and-execute block --
`st.button` has the same "only True on the exact rerun it was clicked"
semantics as `st.form_submit_button`, so the gating behavior is unchanged.
General lesson, worth remembering beyond this one fix: **`st.form` doesn't
make its own contents reactive to each other** -- it only batches
everything until one submit event. If anything inside a form needs to
show/hide based on another widget in the *same* form, that reveal won't
happen until submit either, which reads as a bug to a user encountering it
for the first time (as happened here, twice, before this fix).

## Completed: stage-by-stage timing breakdown (2026-08-10)

User asked, apropos of "what's next": "would it be possible to have a
running time breakdown? ... measure everything, from parsing, loading, the
actual SQL, etc." Added `src/recap_compiler/profiling.py` -- a small,
spec-independent utility (`TimingBreakdown` + a `timed_stage` context
manager) that either caller (the demo script, the workbench) wraps around
each pipeline step to build an ordered `[(stage_name, ms), ...]` list, with
`.total_ms` and `.as_rows()` (adds each stage's % of total) for display.
Deliberately kept separate from `execution.Telemetry` (FR-26, which only
measures the generated query's own execution) -- a query's
`Telemetry.runtime_ms` is folded in as one line of the wider breakdown, not
replaced by it. 6 new tests in `test_profiling.py` (76 passing total).

Wired into both `demo_pipeline.py` (prints a text table at the end) and
`webapp/app.py` (a bar chart + formatted table + total, after the FR-22
banner). Stages measured in both: regex -> NFA (B), build transitions
relation (C), load graph (A), validate aggregate (D), select start
vertices (A), materialize transitions table (C), register macros + generate
standard SQL (E), generate optimized SQL (F), execute each query (G). In
the UI, the regex/relation are deliberately re-timed *inside* the timed
run (even though they were already computed live above the form, per the
earlier fix) so the breakdown reflects what a fresh end-to-end run would
cost, not just the parts gated behind the submit button.

**What the numbers actually show, confirmed by running it:** on the real
dataset (Q1 regex, `bounded_range`, vertex 383, length_bound=4), loading
the graph is ~230ms (real CSV I/O -- `read_csv_auto` over 78,600 rows);
every other pre-execution stage (regex parsing, transitions relation,
validation, SQL generation) is low single-digit milliseconds or less; the
two query executions dominate at ~900-1000ms each, ~90%+ of the total.
This is worth stating plainly in any demo: compiling this query is
essentially free: the two-query FR-22 comparison's cost is the recursive
join itself, not the compiler's own overhead, which is exactly the
contrast the paper's own "we don't extend DuckDB, the win is early
filtering inside the query" framing would predict.

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
