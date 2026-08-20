# Changelog — `compiler_reqs.md`

Log of substantive changes to the functional requirements spec. Each entry
names the source of the change and exactly which requirements were added or
amended, so the spec's own history stays auditable independent of git.

## 2026-08-17 — integrated `compiler/more_add_ons/checklist.txt`

Source: a UI/UX fixes checklist for the workbench, handed off by the query
author (`compiler/more_add_ons/checklist.txt`). All items were webapp-facing
requests; none required changing the compiler's core pipeline (stages A–G)
or its existing correctness obligations (semantics-preservation of the
optimizer). Mapping from checklist bullet to spec change:

| Checklist item | Spec change |
|---|---|
| "Maybe add a small example for the user to understand the symbols: \|, concatenation, {n,m}" | New requirement (Section I): regex input shall show inline operator help/examples. |
| "for starting vertex: ... separate them with a ;" | New requirement (Section I): start-vertex control accepts a `;`-separated id list, mapping to the existing explicit-id-list mode. |
| "If empty (or maybe as dropdown), have a start from everywhere (all unique src from the Edges table)" | Start-vertex requirement amended (Section A) to add a fourth, default, all-vertices mode. Section 6's `start` contract line amended to match. |
| "could we remove the factorized only label? Just say custom or something?" | New requirement (Section I), marked presentation-only: UI label changes from "factorized" to "custom"; the underlying factorized/non-factorized distinction is unchanged internally. |
| "Each function should have a mini description ... + 2 small examples" | New requirement (Section I): per-function description + ≥2 worked examples in the skeleton-editing UI. |
| "have a merge function box ... which takes two dictionaries as parameters (D1, D2)" | New requirement (Section D), explicitly scoped as an authoring aid **not** consumed by stages E/F in this revision — kept consistent with Section 12 non-goal 3 (compatible with, but not implementing, segment/wavefront planning). |
| "is there a way of doing multiple? ... How would I add both the max-min and trail?" | New requirement (Section D): combining multiple library entries into one selective aggregate, via union of dictionary keys (namespaced) and conjunction of viability predicates; conflicting keys rejected under the existing **E-REF** error class (Section 7). The library-entries requirement's own text amended with a pointer to this new one. |

**Not changed:** stages E/F (SQL generation, optimization), the optimizer's
semantics-preservation obligation, and Section 13 (verifier stretch
objectives) — none of the checklist items touched generated-SQL semantics,
only authoring-time UI and one start-vertex default.

## 2026-08-17 — workbench UI checklist items implemented in `compiler/`; merge-function box placement amended

Implemented all six in the actual webapp/compiler code (previously only
specified). See `compiler/CHECKLIST.md`'s dated entry for the full
implementation summary and how it was verified.

While implementing the merge-function box, the user asked whether it
could sit explicitly under `update_d` when the author is writing a custom
aggregate, rather than as a separate, always-visible section — a better
placement than what was specified. **Amended** accordingly: the box
now appears only in the custom-aggregate authoring flow, directly beneath
`update_d`, with its `D1`/`D2` defaults seeded from the author's own
`init_d` keys instead of a generic example.

## 2026-08-17 — General (non-factorized) authoring added and implemented in the workbench

Source: the user's own idea, grounded in the paper's Figure 5 and a
hand-drawn mockup they provided. New requirement (Section I): a Factorized/General
authoring-mode choice for
custom aggregates, General mode presenting `update_d`/`is_viable_d` as a
per-`(from_state, to_state)` table (the same skeleton already used for
factorized bodies, as a table
instead of a `CASE` block) rather than a single body each. Implemented
the same day -- see `compiler/CHECKLIST.md`'s dated entry for the design,
the real shared-code bug it surfaced and fixed (`normalize_update_d_body`
didn't expand bare `D` into a struct, breaking Stage F for *any* bare-`D`
update_d, factorized or not), and how it was verified.

LLM-assisted prefilling of this table (and the other four functions),
i.e. reviving Module J, is explicitly deferred to a future session at the
user's request -- not part of this requirement, not started.
