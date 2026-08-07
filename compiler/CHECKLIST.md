# ReCAP Compiler — Build Checklist

Tracks progress against `requirements/compiler_reqs.md` (repo root). Updated
as of **2026-08-06**. When an item is finished, fill in its "Completed" date
(don't backdate/guess -- use the date the tests actually passed) and link the
code + tests.

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
| A | Data ingestion | FR-1..FR-4 | **Done** | 2026-08-06 | `src/recap_compiler/ingestion.py` | `tests/test_ingestion.py` (11 cases) |
| B | Regex frontend (Thompson's construction) | FR-5..FR-8 | **Done** | 2026-08-06 | `src/recap_compiler/regex_frontend.py` | `tests/test_regex_frontend.py` (14 cases) |
| C | NFA -> transitions relation | FR-9, FR-10 | **Done** | 2026-08-06 | `src/recap_compiler/transitions.py` | `tests/test_transitions.py` (6 cases) |
| D | Selective-aggregate frontend + skeleton generation | FR-11..FR-14 | Not started | -- | -- | -- |
| E | Standard ReCAP SQL generation | FR-15..FR-18 | Not started | -- | -- | -- |
| G | Execution + telemetry | FR-24..FR-26 | Not started | -- | -- | -- |
| F | Optimizer: dictionary flattening + function inlining | FR-19..FR-23 | Not started | -- | -- | -- |
| I | Workbench orchestration | FR-32, FR-33 | Not started | -- | -- | -- |
| Section 7 | Error taxonomy (cross-cutting) | E-INPUT, E-REGEX now; E-REF/E-TYPE/E-UNSUPPORTED/E-EXEC land with D/F/G | **Scaffolded** | 2026-08-06 | `src/recap_compiler/errors.py` | exercised via A/B tests |
| H (stretch) | Negative-stability verifier | FR-27..FR-31 (Section 13, not committed) | Not started | -- | -- | -- |

## Next up

**D: selective-aggregate frontend + skeleton generation.** Needs the schema
from A (column names/types available for FR-14 validation) and the
transitions relation from C (to generate the `CASE` skeleton over
`(from_state, to_state)` pairs per FR-12). Suggested test cases: skeleton
generation for a factorized vs. non-factorized aggregate (Q_B's differing
`is_viable_d` per transition is the spec's own worked example, Section 10);
each of the three library entries (FR-13: adjacent-edge predicate, trail via
edge-id set, bounded monotone aggregate); and FR-14's reference validation
(unknown column, undeclared dictionary key, state variable in a factorized
body -- one test per rejected case).

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
- **Known limitation, not yet handled:** edge labels containing regex
  metacharacters (`|()*+?{}$.` or whitespace) aren't escaped/quoted before
  being spliced into the pattern text, so such a label would confuse the
  parser. Not needed for the datasets exercised so far (`ReCAP/simple_dataset`);
  flag if a future dataset's labels need it.
