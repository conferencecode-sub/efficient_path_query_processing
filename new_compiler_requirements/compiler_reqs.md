# ReCAP Compiler — Functional Requirements Specification

**Document status:** Internal functional-requirements spec for the ReCAP compiler. Functional requirements only; no implementation. "Mechanization notes" indicate where an existing library can discharge a non-novel step versus where custom code is required.

**Reconciliation note (2026-08-10):** a second draft, `recap_compiler_requirements_FULL.md`, was briefly circulated alongside this file and has since been deleted (it was never committed, so nothing was lost). Its Part I differed from this document only in un-demoting the negative-stability verifier (Module H) out of Section 13 back into the committed spec; that change was **not** adopted — Section 13 below (verifier as a stretch objective, SMT work deferred) remains the source of truth. Its **Part II — LLM-Assisted Selective-Aggregate Authoring (Module J)** was adopted, built, and live-tested as this document's own Part II, but has since been **removed** (2026-08-11, per explicit decision) along with its implementation — see the compiler's `CHECKLIST.md` for that history. This document no longer has a Part II; the compiler is Part I only.

---

## 1. Purpose and scope

The ReCAP compiler takes a path query — a label regex plus a selective aggregate over edge/vertex properties — together with a property graph, and produces an **executable, optimized recursive SQL query** that performs early filtering during path exploration, executes it on a relational engine (DuckDB as the reference target), and returns results.

This specification exists to ensure the compiler is a genuine, general artifact rather than a prototype that bakes in specific benchmark queries. The requirements below define a general, query-agnostic compiler whose behaviour is fully determined by its inputs, so that the generality claimed in the paper (Theorem 5.1, Listing 3) is realized in the artifact rather than asserted.

**In scope:** ingestion of arbitrary graph data; full regex support via Thompson's construction; generation of the standard ReCAP SQL (Listing 3); the optimization layer (dictionary flattening and function inlining, Section 6, previously "left for future work"); execution and result/telemetry reporting. A proof-of-concept negative-stability verifier is retained as a **stretch objective** (Section 13) rather than a committed requirement for this revision.

**Out of scope (Section 12):** automatic synthesis of selective aggregates from a raw declarative predicate; automatic detection of negative stability from an arbitrary user predicate; cost-based or wavefront path planning. These are named explicitly as non-goals to bound the revision.

---

## 2. Terminology

Terms follow the paper. **Selective aggregate**: the tuple `(D, init_d, update_d, finalize_d, is_viable_d, is_viable_d_final)`. **Factorized** ReCAP: the selective aggregate does not depend on NFA state transitions. **Early-filtering mode**: all five functions supplied. **Default mode**: only `is_viable_d_final` and `finalize_d` supplied; behaviour reduces to SOTA late filtering (Section 4.2). **Transitions relation** `T(from_state, to_state, label)`: the tabular NFA. **Dictionary flattening / function inlining**: the two semantics-preserving rewrites of Section 6.

---

## 3. Actors and modes

- **Query author** — supplies the regex, the selective aggregate (as SQL, per revision item 7), the start vertex/vertices, the length bound `ℓ`, and threshold parameters.
- **Data provider** — supplies the graph as CSV (or an existing DuckDB table).
- **System** — the compiler and its execution/telemetry harness.

The compiler operates in **default** or **early-filtering** mode (FR-20). A negative-stability check is described as a stretch objective (Section 13, FR-27..FR-31) and is not part of the committed pipeline for this revision.

---

## 4. System overview (pipeline)

```
                 ┌─────────────┐
Graph CSV ──────►│ A. Ingestion│──────────────► Edges/Vertices tables
                 └─────────────┘
Regex ──────────►┌─────────────┐   ┌───────────────────┐
                 │ B. Regex    │──►│ C. NFA→Transitions │──► T(from,to,label)
                 │  frontend   │   └───────────────────┘
                 │ (Thompson's)│
                 └─────────────┘
Selective   ────►┌─────────────┐
aggregate (SQL)  │ D. Sel-agg  │
+ params         │  frontend + │
                 │  skeleton   │
                 └─────────────┘
                        │
                        ▼
                 ┌─────────────┐   ┌──────────────┐   ┌────────────┐
                 │ E. Standard │──►│ F. Optimizer │──►│ G. Execute │──► results + telemetry
                 │ ReCAP SQL   │   │ (flatten,    │   │  on DuckDB │
                 │ gen         │   │  inline)     │   └────────────┘
                 └─────────────┘   └──────────────┘
```

Every stage's output is inspectable; the generated SQL is a first-class artifact (FR-25), which is what makes the compiler's generality checkable rather than asserted. A negative-stability verifier (Section 13, FR-27..FR-31) is an optional stretch stage that could sit between D and E; it is not part of the committed pipeline for this revision.

---

## 5. Functional requirements

### A. Data ingestion

**FR-1.** The system shall load a graph from a user-selected edges file, from a small bundled sample up to large real-world datasets, without code changes. The required edge schema is `src`, `dst`, `label`; all remaining columns are treated as edge properties addressable by name in the selective aggregate (e.g. `amount`, `time`).

**FR-2.** The system shall optionally load a vertices file with a required `id` column; remaining columns are vertex properties. When absent, vertices are inferred from the `src`/`dst` columns of the edges.

**FR-3.** The system shall support at least CSV input and shall accept an already-registered DuckDB table by name. Column types shall be inferred, with an option for the author to override the inferred type of any column.

**FR-4.** The system shall let the author select start vertices by explicit id list, by a predicate over vertex properties, or by out-degree quantile band (low `<25%`, medium `25–75%`, high `>75%`) to reproduce the paper's start-vertex methodology. **(Amended 2026-08-17.)** When no explicit selection is given, the system shall default to an **all-vertices** mode: every distinct `src` value in the Edges table is used as a start vertex.

> *Mechanization:* CSV ingestion and type inference are non-novel — use DuckDB's native `read_csv_auto` and load directly into the execution engine, avoiding a separate parsing layer. Out-degree banding is a single SQL aggregate. No custom parser required.

### B. Regex frontend (Thompson's construction)

**FR-5.** The system shall accept a regular expression over the edge-label alphabet supporting, at minimum, concatenation, union (`|`), Kleene star (`*`), Kleene plus (`+`), optional (`?`), and bounded repetition (`{m,n}`). This satisfies the paper's "any regex" claim rather than supporting only a narrow, hand-picked subset of regex operators.

**FR-6.** The system shall construct an ε-NFA from the regex via Thompson's construction, eliminate ε-transitions to yield an NFA, and expose the resulting states, initial state, and accepting-state set.

**FR-7.** The system shall not require determinization. The NFA (non-deterministic, ε-free) is retained deliberately, because non-determinism is handled natively by the recursive join (multiple transition rows join a path/edge pair) and because preserving the NFA is what keeps ReCAP compatible with wavefront-style planners. Determinization/minimization may be offered as an optional pass but shall not be the default.

**FR-8.** The system shall reject malformed regex with a precise diagnostic (offending position and expected token) rather than a stack trace (see Section 7).

> *Mechanization:* regex → ε-NFA → ε-removal is fully standard. Recommended: `pyformlang` (`Regex.to_epsilon_nfa()`, `.remove_epsilon()`, `.to_deterministic()`), which is designed for formal-language objects and exposes states/transitions directly. Alternatives: `automata-lib`, or the Rust `regex-automata` crate if a non-Python core is preferred. Implementing Thompson's directly is ~100 lines and acceptable, but delegating to a well-tested library reduces the risk of a subtle implementation bug. **This step is non-novel and should be explicitly credited as such in the paper.**

### C. NFA → transitions relation

**FR-9.** The system shall materialize the NFA's transition function as a relation `T(from_state, to_state, label)`, one row per `(q, q')` transition on a concrete label. This is the tabular form joined in Listing 3.

**FR-10.** The system shall record the initial state `q0` and accepting set `Q_F` as parameters to be emitted into the anchor and the outer query respectively.

### D. Selective-aggregate frontend and skeleton generation

**FR-11.** The system shall accept the selective aggregate as SQL (per revision item 7 — no Python UDFs in the shipped path). The unit of input is the body of each of the (up to) five functions over the signatures in Definition 8: `init_d()`, `update_d(D, q, q', E)`, `finalize_d(D)`, `is_viable_d(D, q, q', E)`, `is_viable_d_final(D)`.

**FR-12.** The system shall generate a **skeleton** for `is_viable_d` and `update_d` as a `CASE` statement over the NFA's actual `(from_state, to_state)` transition pairs, so the author fills in only the transition-specific logic. For a factorized aggregate the author may supply a single unconditional body and the system shall omit the `CASE`.

**FR-13.** The system shall provide a **library of pre-written selective aggregates** for the common negatively-stable patterns named in the paper: (i) adjacent-edge predicate on a maintained last value (Example 7); (ii) trail semantics via a maintained id set (Example 8); (iii) bounded monotone/distributive aggregate such as max−min ≤ U (Example 9). Library entries are parameterized by property name and threshold and are usable directly in factorized cases and as scaffolding in non-factorized ones, so an author reuses a tested pattern instead of hand-inlining the same logic into a bespoke CTE for every new query. Multiple library entries may be combined into a single query's selective aggregate per FR-34.

**FR-14.** The system shall validate that supplied function bodies reference only (a) the dictionary keys they declare, (b) columns present in the edge/vertex schema, and (c) the NFA state variables `q`, `q'`. Unknown references shall be reported per Section 7.

**FR-34 (added 2026-08-17).** The system shall support combining **multiple library entries** (FR-13) into a single selective aggregate for one query — e.g. enabling the bounded max−min aggregate (Example 9) and the trail aggregate (Example 8) simultaneously. Composition shall take the union of the entries' dictionary keys (each key namespaced to its originating entry to avoid collision) and the conjunction of their `is_viable_d`/`is_viable_d_final` predicates; `update_d` and `init_d` shall apply each entry's own logic to its own keys independently. If two composed entries declare conflicting dictionary keys under the same name, the system shall reject the combination per the **E-REF** class (Section 7), naming both entries and the conflicting key.

**FR-35 (added 2026-08-17, workbench-facing; authoring aid only; placement amended 2026-08-17; extended 2026-08-20 to a second sketch function).** The workbench shall provide a **merge-function** input box, taking two dictionary instances `(D1, D2)` as parameters, for the author to sketch how two fragments — both running the same `update_d` — would compose at a seam. This covers two functions, not one: `is_mergable_d(D1, D2)`, a Boolean predicate over the pair (the merge-time counterpart of `is_viable_d`) deciding whether the two fragments may be joined at all, and `merge_d(D1, D2)`, the actual dictionary produced when they are. The box shall appear directly beneath `update_d` in the custom-aggregate authoring flow (not as a separate, generally-visible section), and its `D1`/`D2` defaults shall be seeded from the same dictionary keys the author's own `init_d` declares, so the sketch is concretely about the aggregate just authored rather than a generic example; it is not shown when a library aggregate (FR-13/FR-34) is selected instead of a custom one. This is authoring scaffolding only: both functions are captured and displayed but are **not** consumed by the Standard SQL generation (E) or Optimization (F) stages in this revision — no split/merge execution plan is generated from either. This is consistent with Section 12 non-goal 3 (the compiler remains *compatible* with segment/wavefront-style planning per FR-7, but does not implement it); the box exists to make that compatibility tangible to the author, not to commit to executing merged plans.

> *Mechanization:* function-body validation is AST-level analysis of SQL expressions — use `sqlglot` to parse each body and resolve identifiers against the known schema and dictionary keys. Skeleton generation is templating over `T` and requires no external library. FR-34's key-namespacing and conflict check are a straightforward extension of the same identifier-resolution pass. FR-35's merge-function box (`is_mergable_d` and `merge_d`) needs no execution-side plumbing since neither is wired into E/F.

### E. Standard ReCAP SQL generation

**FR-15.** The system shall generate the standard ReCAP query of Listing 3: an anchor initializing `v = s`, `q = q0`, `D = init_d()`; a recursive member joining `Paths ⋈ Edges ⋈ Transitions` under `T.label = E.label AND P.q = T.from_state`, applying `is_viable_d` in the `WHERE`, and updating `D` with `update_d`; and an outer query selecting where `q ∈ Q_F AND is_viable_d_final(D)`.

**FR-16.** The generated anchor shall be **well-formed with respect to the SQL recursion standard**: the start vertex shall be introduced correctly (as a literal seed row or via `FROM` over a start-vertex relation), so that the base case has no undefined columns. This directly fixes a defect in Listings 1–2, where the base case referenced an unbound `s`.

**FR-17.** The system shall parameterize `s`, `q0`, `Q_F`, `ℓ`, and any threshold constants, so the same query template instantiates for any start vertex, length bound, and parameter setting without regeneration of structure.

**FR-18.** The system shall represent the dictionary `D` by default as a JSON column (Section 5, "ReCAP Definition and Semantics") when flattening is not applied.

### F. Optimization layer (dictionary flattening + function inlining)

**FR-19.** The system shall apply **dictionary flattening**: each dictionary key becomes a column of `Paths`. Scalar-typed keys become built-in typed columns (JSON eliminated for that key); list-typed keys become native array columns where the target supports them (DuckDB), otherwise remain JSON with one fewer level of nesting. Value type shall determine the encoding automatically.

**FR-20.** The system shall apply **function inlining**: each of the five functions is rewritten into SQL and its call sites in the template are replaced by its body, so that after flattening the query contains no UDF calls and no JSON operations (Figure 7). In default mode, only `is_viable_d_final` and `finalize_d` are non-trivial and inlining reduces `is_viable_d` to `TRUE` and `update_d` to a list append.

**FR-21.** For a factorized aggregate the inlined `CASE` expressions shall collapse to unconditional expressions; for a non-factorized aggregate the `CASE` over NFA transitions shall be preserved (as in Q_B's timestamp ranges).

**FR-22.** Both passes shall be **semantics-preserving**: the optimized query shall return the same result set as the standard query for every input. This is a correctness obligation (Section 9), not merely a performance option.

**FR-23.** When a supplied function cannot be expressed in the inlinable SQL-expression sublanguage, the system shall fall back to registering it as a DuckDB SQL macro/UDF and emit a warning naming the function and the reason, rather than failing.

> *Mechanization:* inlining is an AST rewrite over the generated SQL — `sqlglot` (parse → substitute → regenerate) or `libpg_query` for a Postgres-grammar AST. Flattening is a schema transform plus type-directed column projection; DuckDB array types cover the list-valued keys. The rewrite logic itself is the novel contribution of Section 6 and should be presented as such.

### G. Execution and results

**FR-24.** The system shall execute the generated query on DuckDB (reference target) and return results in one of three user-selected shapes: full paths (edge-id sequence via the trail state), reached endpoints, or a count. All three shall enumerate the same viable paths internally so that timing is independent of output shape (the paper's methodology).

**FR-25.** The system shall expose the generated SQL (standard and optimized) as an inspectable output artifact for every run.

**FR-26.** The system shall optionally report telemetry: total intermediate paths explored, wall-clock time, and peak memory, to support the intermediate-result comparisons of Section 7.4 and give a concrete memory-cost figure for the compiler's own execution.

> *Mechanization:* telemetry is available from DuckDB's `EXPLAIN ANALYZE` and profiling pragmas; no custom instrumentation of the engine is required (consistent with "we did not extend DuckDB").

### I. Workbench orchestration

**FR-32.** The workbench shall drive the pipeline end to end: select data and start vertices → enter regex → system generates NFA and skeleton → author edits skeleton (or picks a library aggregate) → system generates and runs optimized SQL → results and telemetry displayed. (A negative-stability check may be added as a stretch step per Section 13.) This is revision item 6.

**FR-33.** The workbench shall not hard-code any dataset, query, or example. A bundled sample dataset and example query may exist as defaults, but every input shall be replaceable through the UI without code changes. This keeps the workbench itself from silently becoming dataset- or query-specific over time.

**FR-36 (added 2026-08-17).** The regex input control shall display inline help covering, at minimum, the supported operators named in FR-5 (union `|`, concatenation, Kleene `*`/`+`, `?`, bounded repetition `{m,n}`) with a short example of each. The help shall be presented unobtrusively (e.g. a dismissible bubble or text below the input) so it does not require the author to leave the input to consult it, and shall not block entry of a regex while displayed.

**FR-37 (added 2026-08-17).** The start-vertex input control shall accept multiple explicit ids as a single string with ids separated by `;`, mapping to FR-4's "explicit id list" mode. An empty input shall map to FR-4's all-vertices default. Where feasible, the control may additionally offer a dropdown enumerating the degree-band option (FR-4) alongside explicit entry.

**FR-38 (added 2026-08-17, presentation only).** The workbench shall not expose the internal term "factorized" to the author when labeling aggregate-authoring modes; the UI label for the non-`CASE` (state-independent) mode shall use plain wording such as "custom" instead. This is a labeling change only — the underlying factorized/non-factorized distinction (FR-12, FR-21) and its effect on generated SQL are unchanged.

**FR-39 (added 2026-08-17).** For each of the five selective-aggregate functions (`init_d`, `update_d`, `finalize_d`, `is_viable_d`, `is_viable_d_final`), the skeleton-editing UI shall display a short description of the function's role (per Definition 8's signatures) and at least two worked examples, to guide an author unfamiliar with the selective-aggregate model in filling in the skeleton generated by FR-12.

**FR-40 (added 2026-08-17; own "minimize first" choice merged into FR-41's, 2026-08-20).** When authoring a custom aggregate, the workbench shall offer a **Factorized**/**General** authoring-mode choice (Figure 5). In Factorized mode, `update_d`/`is_viable_d` are each authored as the single body FR-12 already supports. In General mode, the workbench shall present `update_d`/`is_viable_d` as an editable table with one row per `(from_state, to_state)` pair of the compiled transitions relation (FR-12's own skeleton, presented as a table instead of a `CASE`-statement text block) -- each row defaulting to a pass-through body (`D` for `update_d`, `TRUE` for `is_viable_d`) so only pairs needing real logic require editing. `init_d`/`is_viable_d_final`/`finalize_d` are unaffected by this choice, since Definition 8 does not make them state-dependent. The workbench shall warn (not block) when the number of pairs is large. This table's own transition pairs come from whichever automaton FR-41's "minimize first" choice currently produces -- there is no second, independent minimize decision here.

**FR-41 (added 2026-08-20, workbench-facing; merged with FR-40's own "minimize first" choice on 2026-08-20).** Once the label regex (FR-5) parses successfully, the workbench shall display, immediately below the regex input: (a) a **single** "minimize the automaton first" choice -- shared by this preview, FR-40's own General-mode table, and the actual compiled query, rather than each deciding independently -- (b) a clear accepted signal, distinct from the silence that previously followed a valid regex, and (c) that automaton's transitions relation (state/pair counts, `q0`, accepting states, and a table of `(from_state, to_state, labels)`), reflecting whichever choice (a) is currently set to -- so the author sees what their regex actually compiled to without navigating to the aggregate section's own (edit-oriented) transitions table, and toggling the choice here updates that table too rather than requiring a second, possibly-inconsistent decision there.

**FR-42 (added 2026-08-20, workbench-facing).** The dictionary-key table the workbench shows beneath `init_d`, inferring each key's name and SQL type directly from `init_d`'s own struct literal, shall let the author override each key's *type* from a dropdown of common scalar types, in addition to whatever DuckDB itself inferred. An override is not cosmetic: the system already casts `init_d`'s anchor value to each key's declared type (FR-19's own flattening needs a consistent type for the anchor and the recursive term alike), so picking a wider type here is how an author fixes a too-narrow inference (e.g. a bare `NULL` inferring as INTEGER when the real values flowing through `update_d` are BIGINT epoch-millisecond timestamps) without resorting to a `CAST` inside `init_d` itself. Key *names* remain solely derived from `init_d`, matching the existing rationale for inferring keys from `init_d` rather than a separate table at all (a hand-kept keys list could silently disagree with `init_d` about which keys exist) -- only the type of an already-existing key is author-editable, so there is still exactly one source of truth for which keys exist, just not for their type once the author overrides it.

---

## 6. Input / output contracts

**Graph input.**
- Edges: `src`, `dst`, `label` (required) + arbitrary typed property columns.
- Vertices (optional): `id` (required) + property columns.

**Query input.**
- `regex`: string over the label alphabet (FR-5 grammar).
- `selective_aggregate`: up to five SQL function bodies, or a reference to a library entry with bound parameters.
- `start`: id list | vertex predicate | degree band | all (default when omitted — every distinct `src` in the Edges table; FR-4).
- `ell`: non-negative integer length bound.
- `params`: named constants referenced by the aggregate (e.g. `U`).

**Intermediate artifacts.**
- `T(from_state INT, to_state INT, label TEXT)`; `q0 INT`; `Q_F INT[]`.
- Skeleton SQL for `is_viable_d`/`update_d`.

**Outputs.**
- Result set in the chosen shape (paths | endpoints | count).
- Generated SQL (standard and optimized) as text.
- Telemetry record: `{intermediate_paths, runtime_ms, peak_mem}`.

*(Stretch, if Section 13 is pursued: verifier verdict `{status ∈ {PROVED, COUNTEREXAMPLE, UNKNOWN}, witness?}`.)*

---

## 7. Error handling and taxonomy

Every failure shall be reported with a category, the offending input location, and a human-readable message; the system shall not surface raw engine or solver stack traces to the author.

| Class | Examples | Detection stage | Behaviour |
|---|---|---|---|
| **E-INPUT** | missing `src`/`dst`/`label`; unreadable file; type-inference conflict | A. Ingestion | reject with column/row locus; offer type override |
| **E-REGEX** | unbalanced parenthesis; unknown operator; empty language | B. Regex frontend | reject with position and expected token |
| **E-REF** | aggregate references an unknown column, undeclared dictionary key, or a state variable in a factorized body | D. Frontend validation | reject naming the bad identifier |
| **E-TYPE** | viability predicate not Boolean; update returns wrong shape | D / E | reject naming the function and expected type |
| **E-UNSUPPORTED** | function outside the inlinable sublanguage | F. Optimizer | warn + fall back to UDF (FR-23), continue |
| **E-EXEC** | DuckDB runtime error; length bound causes resource exhaustion | G. Execution | report engine message, mapped to a friendly cause; preserve generated SQL |

*(Stretch, if Section 13 is pursued: a `V-UNKNOWN` class — solver timeout or unsupported theory — detected at the verifier stage, returning `UNKNOWN` and never a false `PROVED`.)*

---

## 8. Mechanization map (summary)

| Step | Novel? | Recommended existing component |
|---|---|---|
| CSV load + type inference | No | DuckDB `read_csv_auto` |
| Regex → ε-NFA → NFA | No | `pyformlang` (or `automata-lib`; or hand-rolled Thompson's) |
| Transitions materialization | No | plain SQL / DataFrame |
| Selective-aggregate parsing/validation | No | `sqlglot` AST |
| Standard ReCAP SQL emission | **Partly** (template is the contribution) | templating; `sqlglot` for well-formedness checks |
| Dictionary flattening + function inlining | **Yes** (Section 6) | `sqlglot`/`libpg_query` for the rewrite plumbing; logic is custom |
| Execution + telemetry | No | DuckDB engine + `EXPLAIN ANALYZE` |

The compiler's genuine contributions are the SQL template (E) and the two optimization rewrites (F). Everything else is deliberately delegated to standard components, which is a point worth stating plainly in the paper: the artifact is thin around a small, well-defined novel core, not a pile of query-specific scripts. (A negative-stability check, Section 13, would add a further proof-of-concept contribution — Z3 via its Python API, theories per property type — if pursued as a stretch objective.)

---

## 9. Non-functional and correctness requirements

**NFR-1 (determinism).** For fixed inputs the generated SQL and results shall be reproducible run to run.

**NFR-2 (optimization soundness).** Flattening and inlining (FR-19..FR-22) shall preserve the result set of the standard query. This should be argued in the paper and spot-checked in the artifact by comparing standard vs optimized output on every benchmark configuration.

*(NFR-3 and NFR-4, concerning verifier soundness and its adequacy obligation, are deferred with the verifier to Section 13 — they apply only if that stretch objective is pursued.)*

**NFR-5 (no engine modification).** The system shall run on a stock DuckDB build. This is a hard requirement: the compiler must not depend on any non-stock DuckDB behavior.

---

## 10. Worked instantiation (Q_B, for validation)

To make the contracts concrete, the spec shall be exercised end-to-end on Q_B:
- **regex** `Domestic+ Foreign` → ε-NFA → NFA with `T = {(1,2,Domestic), (2,2,Domestic), (2,3,Foreign)}`, `q0 = 1`, `Q_F = {3}`.
- **aggregate**: non-factorized; `D = {last_time, edge_ids}`; `is_viable_d` differs on the `2→2` (±2 days) vs `2→3` (±3 days) transitions; trail via `edge_ids`.
- **skeleton**: `CASE` over the three transition pairs, author fills the two range bodies.
- **generation**: standard SQL (Listing 3 form) → flattened (`last_time` typed, `edge_ids` array) → inlined (`CASE` retained for `is_viable_d`, dropped for `update_d`), matching Figure 7.

A passing run of stages A–G on this instantiation is the acceptance test for the compiler as a whole. *(Stretch, if Section 13 is pursued: the verifier should return `PROVED` for the monotonicity and trail sub-constraints, and `COUNTEREXAMPLE` with a witness for a synthetic lower-bound variant, reproducing the paper's claim that lower bounds are not negatively stable.)*

---

## 12. Explicit non-goals (scope boundary for this revision)

1. **Synthesis of a selective aggregate from a raw declarative predicate** (Problem B). The compiler consumes a selective aggregate; it does not invent one from `list_max(amounts) - list_min(amounts) <= U`.
2. **Automatic detection of negative stability from an arbitrary user predicate.** General detection is acknowledged as an open problem. The stretch objective in Section 13, if pursued, only checks a *supplied* incremental encoding (Problem A); it does not search for one.
3. **Cost-based, wavefront, or segment-based path planning.** The compiler targets the standard bottom-up recursive evaluation; it is designed to remain *compatible* with richer planners (FR-7) but does not implement them.
4. **Discharging the adequacy obligation (NFR-4, Section 13).**

Stating these keeps the revision's committed surface bounded and explicit, and reserves 1–2 as the spine of follow-on work rather than giving it away here.

---

## 13. Stretch objectives (not committed for this revision)

The items below describe a proof-of-concept negative-stability verifier. They are **not required** for this revision to be considered complete — Sections 1, 3, and 4 describe the committed pipeline without it. They are kept here, rather than deleted, because they sketch a partial, honest answer to an open question ("how would a system identify negatively stable constraints?") that could be picked up if time permits. Nothing in Sections 1–12 depends on this section being implemented.

### H. Negative-stability verifier (proof-of-concept module)

**FR-27.** Given a selective aggregate expressed as a state transformer — state `S`, update `u = update_d`, viability predicate `ok = is_viable_d`/`is_viable_d_final` — the system shall attempt to prove the single-step obligation: for all states `s ∈ S` and all candidate edges `e`, `¬ok(s) ⟹ ¬ok(u(s, e))`. A proof of this obligation establishes negative stability (prefix-closure of the satisfying set) without induction over path length, because the state summarizes every prefix.

**FR-28.** The verifier shall discharge the obligation by asking an SMT solver for the **negation** (`∃ s, e : ok is currently false but becomes true after update`) and interpreting `unsat` as *proved negatively stable* and `sat` as a *candidate counterexample* with a concrete witness.

**FR-29.** For a non-factorized aggregate the obligation shall be checked **per NFA transition** `(q, q')`, ranging over the finite transition set, so quantification stays over data values only.

**FR-30.** The verifier shall return exactly one of: `PROVED` (sound: the constraint is negatively stable), `COUNTEREXAMPLE` (a state/edge witness was found — see the soundness caveat below), or `UNKNOWN` (solver timeout or a construct outside the supported theories).

**FR-31.** In the workbench, a "check negative stability" action shall run the verifier over the current selective aggregate and surface the result and any witness next to the relevant transition.

> *Mechanization:* Z3 via its Python API. Theory selection by property type: LIA/LRA/LIRA for numeric amounts and timestamps; the array or finite-set fragment for `edge_ids` membership (trail); sequence/datatype theory only if list-shaped state is retained rather than flattened. Invariant inference (Problem B) would use CHC/Spacer or an inductive prover; that remains out of scope even if this stretch objective is pursued (Section 12).

**NFR-3 (verifier soundness, stated precisely).** `PROVED` is sound: an `unsat` result establishes negative stability. `COUNTEREXAMPLE` is a witness over *all* states, including states that may be unreachable in a real path exploration; it therefore means "not provable negatively stable by the single-step method," not necessarily "observably non-stable at runtime." This caveat shall be documented so no reader can mistake this for a claim of completeness.

**NFR-4 (adequacy obligation, acknowledged).** The verifier assumes the incremental triple `(S, u, ok)` faithfully computes the author's declarative predicate `φ` (i.e. `ok(fold(u, s0, xs)) = φ(xs)` for all lists). Discharging adequacy is an induction over lists and is **not** attempted by the PoC; it is stated as an assumption and flagged as future work. Naming this pre-empts the obvious "you only proved self-consistency of the incremental form" objection.

**If pursued:** this offers a partial, PoC-level answer to how negative stability could be identified. It checks a *supplied* incremental encoding rather than searching for one, so it does not fully resolve the automation question, but it would materially strengthen the honesty of the revision's automation story.
