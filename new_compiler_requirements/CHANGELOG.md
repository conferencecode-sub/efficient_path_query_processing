# Changelog — `compiler_reqs.md`

Log of substantive changes to the functional requirements spec. Each entry
names the source of the change and exactly which requirements were added or
amended, so the spec's own history stays auditable independent of git.

## 2026-08-17 — integrated `compiler/more_add_ons/checklist.txt`

Source: a UI/UX fixes checklist for the workbench, handed off by the query
author (`compiler/more_add_ons/checklist.txt`). All items were webapp-facing
requests; none required changing the compiler's core pipeline (stages A–G)
or its existing correctness obligations (FR-22, NFR-2). Mapping from
checklist bullet to spec change:

| Checklist item | Spec change |
|---|---|
| "Maybe add a small example for the user to understand the symbols: \|, concatenation, {n,m}" | New **FR-36** (Section I): regex input shall show inline operator help/examples. |
| "for starting vertex: ... separate them with a ;" | New **FR-37** (Section I): start-vertex control accepts a `;`-separated id list, mapping to FR-4's explicit-id-list mode. |
| "If empty (or maybe as dropdown), have a start from everywhere (all unique src from the Edges table)" | **FR-4 amended** (Section A) to add a fourth, default, all-vertices mode. Section 6's `start` contract line amended to match. |
| "could we remove the factorized only label? Just say custom or something?" | New **FR-38** (Section I), marked presentation-only: UI label changes from "factorized" to "custom"; the underlying FR-12/FR-21 distinction is unchanged internally. |
| "Each function should have a mini description ... + 2 small examples" | New **FR-39** (Section I): per-function description + ≥2 worked examples in the skeleton-editing UI. |
| "have a merge function box ... which takes two dictionaries as parameters (D1, D2)" | New **FR-35** (Section D), explicitly scoped as an authoring aid **not** consumed by stages E/F in this revision — kept consistent with Section 12 non-goal 3 (compatible with, but not implementing, segment/wavefront planning) and FR-7. Added a traceability row (Section 11) linking it to R4.O2. |
| "is there a way of doing multiple? ... How would I add both the max-min and trail?" | New **FR-34** (Section D): combining multiple library entries (FR-13) into one selective aggregate, via union of dictionary keys (namespaced) and conjunction of viability predicates; conflicting keys rejected under the existing **E-REF** error class (Section 7). FR-13's own text amended with a pointer to FR-34. |

**Numbering note:** new committed requirements were numbered FR-34 onward,
continuing after FR-33 (the last committed requirement) rather than
FR-27, because FR-27..FR-31 are permanently reserved for the Section 13
stretch objectives (the negative-stability verifier) and are cross-referenced
by number from Sections 3, 4, 6, 8, 11, and 12 — renumbering them to make
room would have touched every one of those references for no benefit. This
follows the precedent already set by FR-32/FR-33, which were themselves
numbered after the reserved block for the same reason.

**Not changed:** stages E/F (SQL generation, optimization), FR-22/NFR-2
(semantics-preservation), and Section 13 (verifier stretch objectives) — none
of the checklist items touched generated-SQL semantics, only authoring-time
UI and one start-vertex default.
