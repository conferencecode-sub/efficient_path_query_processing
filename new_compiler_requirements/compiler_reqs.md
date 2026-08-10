# ReCAP Compiler — Functional Requirements Specification

**Document status:** Draft for the SIGMOD 2027 Round-2 revision. Functional requirements only; no implementation. "Mechanization notes" indicate where an existing library can discharge a non-novel step versus where custom code is required.

**Reconciliation note (2026-08-10):** a second draft, `recap_compiler_requirements_FULL.md`, was briefly circulated alongside this file and has since been deleted (it was never committed, so nothing was lost). Its Part I differed from this document only in un-demoting the negative-stability verifier (Module H) out of Section 13 back into the committed spec; that change was **not** adopted — Section 13 below (verifier as a stretch objective, SMT work deferred) remains the source of truth. Its **Part II — LLM-Assisted Selective-Aggregate Authoring (Module J)** *was* adopted and is appended below as this document's own Part II, unchanged.

---

## 1. Purpose and scope

The ReCAP compiler takes a path query — a label regex plus a selective aggregate over edge/vertex properties — together with a property graph, and produces an **executable, optimized recursive SQL query** that performs early filtering during path exploration, executes it on a relational engine (DuckDB as the reference target), and returns results.

This specification exists to close the gap the Round-2 reviews identified. R2 read the artifact as a "hard-coded prototype" that bakes in the four benchmark queries (R2.O1) and questioned whether the approach is genuinely automatable (R2.O3, R5.O3). The meta-review's crux (3) is "how versatile the compiler is to generate the approach automatically across many query shapes." The requirements below define a general, query-agnostic compiler whose behaviour is fully determined by its inputs, so that the generality claimed in the paper (Theorem 5.1, Listing 3) is realized in the artifact rather than asserted. A traceability matrix (Section 11) maps each requirement to the reviewer concern it serves.

**In scope:** ingestion of arbitrary graph data; full regex support via Thompson's construction; generation of the standard ReCAP SQL (Listing 3); the optimization layer (dictionary flattening and function inlining, Section 6, previously "left for future work"); execution and result/telemetry reporting; an optional LLM-assisted selective-aggregate authoring module (Part II, Module J). A proof-of-concept negative-stability verifier is retained as a **stretch objective** (Section 13) rather than a committed requirement for this revision — Module J's own safety case (Part II, NFR-6) explicitly leaves the *sound* version of that check (an SMT proof of the one-step obligation) as future work, so it does not depend on Section 13 being built.

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

Every stage's output is inspectable; the generated SQL is a first-class artifact (FR-25), which is the concrete rebuttal to R2.O1. A negative-stability verifier (Section 13, FR-27..FR-31) is an optional stretch stage that could sit between D and E; it is not part of the committed pipeline for this revision.

---

## 5. Functional requirements

### A. Data ingestion

**FR-1.** The system shall load a graph from a user-selected edges file. The required edge schema is `src`, `dst`, `label`; all remaining columns are treated as edge properties addressable by name in the selective aggregate (e.g. `amount`, `time`).

**FR-2.** The system shall optionally load a vertices file with a required `id` column; remaining columns are vertex properties. When absent, vertices are inferred from the `src`/`dst` columns of the edges.

**FR-3.** The system shall support at least CSV input and shall accept an already-registered DuckDB table by name. Column types shall be inferred, with an option for the author to override the inferred type of any column.

**FR-4.** The system shall let the author select start vertices by explicit id list, by a predicate over vertex properties, or by out-degree quantile band (low `<25%`, medium `25–75%`, high `>75%`) to reproduce the paper's start-vertex methodology.

> *Mechanization:* CSV ingestion and type inference are non-novel — use DuckDB's native `read_csv_auto` and load directly into the execution engine, avoiding a separate parsing layer. Out-degree banding is a single SQL aggregate. No custom parser required.

### B. Regex frontend (Thompson's construction)

**FR-5.** The system shall accept a regular expression over the edge-label alphabet supporting, at minimum, concatenation, union (`|`), Kleene star (`*`), Kleene plus (`+`), optional (`?`), and bounded repetition (`{m,n}`). This satisfies the paper's "any regex" claim and removes the artifact limitation both R3 and R5 flagged.

**FR-6.** The system shall construct an ε-NFA from the regex via Thompson's construction, eliminate ε-transitions to yield an NFA, and expose the resulting states, initial state, and accepting-state set.

**FR-7.** The system shall not require determinization. The NFA (non-deterministic, ε-free) is retained deliberately, because non-determinism is handled natively by the recursive join (multiple transition rows join a path/edge pair) and because preserving the NFA is what keeps ReCAP compatible with wavefront-style planners (R4.O2). Determinization/minimization may be offered as an optional pass but shall not be the default.

**FR-8.** The system shall reject malformed regex with a precise diagnostic (offending position and expected token) rather than a stack trace (see Section 7).

> *Mechanization:* regex → ε-NFA → ε-removal is fully standard. Recommended: `pyformlang` (`Regex.to_epsilon_nfa()`, `.remove_epsilon()`, `.to_deterministic()`), which is designed for formal-language objects and exposes states/transitions directly. Alternatives: `automata-lib`, or the Rust `regex-automata` crate if a non-Python core is preferred. Implementing Thompson's directly is ~100 lines and acceptable, but a library removes an avenue of reviewer doubt about correctness. **This step is non-novel and should be explicitly credited as such in the paper.**

### C. NFA → transitions relation

**FR-9.** The system shall materialize the NFA's transition function as a relation `T(from_state, to_state, label)`, one row per `(q, q')` transition on a concrete label. This is the tabular form joined in Listing 3.

**FR-10.** The system shall record the initial state `q0` and accepting set `Q_F` as parameters to be emitted into the anchor and the outer query respectively.

### D. Selective-aggregate frontend and skeleton generation

**FR-11.** The system shall accept the selective aggregate as SQL (per revision item 7 — no Python UDFs in the shipped path). The unit of input is the body of each of the (up to) five functions over the signatures in Definition 8: `init_d()`, `update_d(D, q, q', E)`, `finalize_d(D)`, `is_viable_d(D, q, q', E)`, `is_viable_d_final(D)`.

**FR-12.** The system shall generate a **skeleton** for `is_viable_d` and `update_d` as a `CASE` statement over the NFA's actual `(from_state, to_state)` transition pairs, so the author fills in only the transition-specific logic. For a factorized aggregate the author may supply a single unconditional body and the system shall omit the `CASE`.

**FR-13.** The system shall provide a **library of pre-written selective aggregates** for the common negatively-stable patterns named in the paper: (i) adjacent-edge predicate on a maintained last value (Example 7); (ii) trail semantics via a maintained id set (Example 8); (iii) bounded monotone/distributive aggregate such as max−min ≤ U (Example 9). Library entries are parameterized by property name and threshold and are usable directly in factorized cases and as scaffolding in non-factorized ones.

**FR-14.** The system shall validate that supplied function bodies reference only (a) the dictionary keys they declare, (b) columns present in the edge/vertex schema, and (c) the NFA state variables `q`, `q'`. Unknown references shall be reported per Section 7.

> *Mechanization:* function-body validation is AST-level analysis of SQL expressions — use `sqlglot` to parse each body and resolve identifiers against the known schema and dictionary keys. Skeleton generation is templating over `T` and requires no external library.

### E. Standard ReCAP SQL generation

**FR-15.** The system shall generate the standard ReCAP query of Listing 3: an anchor initializing `v = s`, `q = q0`, `D = init_d()`; a recursive member joining `Paths ⋈ Edges ⋈ Transitions` under `T.label = E.label AND P.q = T.from_state`, applying `is_viable_d` in the `WHERE`, and updating `D` with `update_d`; and an outer query selecting where `q ∈ Q_F AND is_viable_d_final(D)`.

**FR-16.** The generated anchor shall be **well-formed with respect to the SQL recursion standard**: the start vertex shall be introduced correctly (as a literal seed row or via `FROM` over a start-vertex relation), so that the base case has no undefined columns. This directly fixes the defect R4.O3 flagged in Listings 1–2, where the base case referenced an unbound `s`.

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

**FR-26.** The system shall optionally report telemetry: total intermediate paths explored, wall-clock time, and peak memory, to support R3.O3 (memory cost) and the intermediate-result comparisons of Section 7.4.

> *Mechanization:* telemetry is available from DuckDB's `EXPLAIN ANALYZE` and profiling pragmas; no custom instrumentation of the engine is required (consistent with "we did not extend DuckDB").

### I. Workbench orchestration

**FR-32.** The workbench shall drive the pipeline end to end: select data and start vertices → enter regex → system generates NFA and skeleton → author edits skeleton (or picks a library aggregate) → system generates and runs optimized SQL → results and telemetry displayed. (A negative-stability check may be added as a stretch step per Section 13.) This is revision item 6.

**FR-33.** The workbench shall not hard-code any dataset, query, or example. A bundled sample dataset and example query may exist as defaults, but every input shall be replaceable through the UI without code changes. This is the direct structural answer to R2.O1.

---

## 6. Input / output contracts

**Graph input.**
- Edges: `src`, `dst`, `label` (required) + arbitrary typed property columns.
- Vertices (optional): `id` (required) + property columns.

**Query input.**
- `regex`: string over the label alphabet (FR-5 grammar).
- `selective_aggregate`: up to five SQL function bodies, or a reference to a library entry with bound parameters.
- `start`: id list | vertex predicate | degree band.
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

**NFR-5 (no engine modification).** The system shall run on a stock DuckDB build. This is a hard requirement, both technically and rhetorically, given R2.O1.

---

## 10. Worked instantiation (Q_B, for validation)

To make the contracts concrete, the spec shall be exercised end-to-end on Q_B:
- **regex** `Domestic+ Foreign` → ε-NFA → NFA with `T = {(1,2,Domestic), (2,2,Domestic), (2,3,Foreign)}`, `q0 = 1`, `Q_F = {3}`.
- **aggregate**: non-factorized; `D = {last_time, edge_ids}`; `is_viable_d` differs on the `2→2` (±2 days) vs `2→3` (±3 days) transitions; trail via `edge_ids`.
- **skeleton**: `CASE` over the three transition pairs, author fills the two range bodies.
- **generation**: standard SQL (Listing 3 form) → flattened (`last_time` typed, `edge_ids` array) → inlined (`CASE` retained for `is_viable_d`, dropped for `update_d`), matching Figure 7.

A passing run of stages A–G on this instantiation is the acceptance test for the compiler as a whole. *(Stretch, if Section 13 is pursued: the verifier should return `PROVED` for the monotonicity and trail sub-constraints, and `COUNTEREXAMPLE` with a witness for a synthetic lower-bound variant, reproducing the paper's claim that lower bounds are not negatively stable.)*

---

## 11. Traceability to reviewer concerns

| Requirement(s) | Serves |
|---|---|
| FR-32, FR-33, FR-25 | R2.O1 (artifact is a general compiler, not hard-coded; architecture explicit) |
| FR-5..FR-8 | R3, R5 (full regex / Thompson's; removes the artifact's "simple regex only" limitation) |
| FR-16 | R4.O3 (fixes the malformed base case in Listings 1–2) |
| FR-7, and preserving the NFA | R4.O2 (compatibility with wavefront/segment planners) |
| FR-24, FR-26 | R3.O3 (path return + memory cost) |
| FR-4, FR-1 | R3.O2 (start-vertex methodology; larger-graph runs) |
| FR-13, FR-12 | R4.O1 (abstraction/library benefit over hand-inlined CTEs) |
| Section 8 map | meta-crux (3) (compiler versatility across query shapes) |
| Section 13 (stretch, if pursued) | R2.O3 / R5.O3 (how negative stability could be identified; automation boundary) — only partially served even if implemented, since it checks a supplied encoding rather than discovering one |

---

## 12. Explicit non-goals (scope boundary for this revision)

1. **Synthesis of a selective aggregate from a raw declarative predicate** (Problem B). The compiler consumes a selective aggregate; it does not invent one from `list_max(amounts) - list_min(amounts) <= U`.
2. **Automatic detection of negative stability from an arbitrary user predicate.** General detection is acknowledged as an open problem. The stretch objective in Section 13, if pursued, only checks a *supplied* incremental encoding (Problem A); it does not search for one.
3. **Cost-based, wavefront, or segment-based path planning.** The compiler targets the standard bottom-up recursive evaluation; it is designed to remain *compatible* with richer planners (FR-7) but does not implement them.
4. **Discharging the adequacy obligation (NFR-4, Section 13).**

Stating these keeps the revision's committed surface exactly equal to what the AE endorsed, and reserves 1–2 as the spine of follow-on work rather than giving it away here.

---

## 13. Stretch objectives (not committed for this revision)

The items below describe a proof-of-concept negative-stability verifier. They are **not required** for this revision to be considered complete — Sections 1, 3, and 4 describe the committed pipeline without it. They are kept here, rather than deleted, because they sketch a partial, honest answer to R2.O3/R5.O3 ("how would a system identify negatively stable constraints?") that could be picked up if time permits. Nothing in Sections 1–12 depends on this section being implemented.

### H. Negative-stability verifier (proof-of-concept module)

**FR-27.** Given a selective aggregate expressed as a state transformer — state `S`, update `u = update_d`, viability predicate `ok = is_viable_d`/`is_viable_d_final` — the system shall attempt to prove the single-step obligation: for all states `s ∈ S` and all candidate edges `e`, `¬ok(s) ⟹ ¬ok(u(s, e))`. A proof of this obligation establishes negative stability (prefix-closure of the satisfying set) without induction over path length, because the state summarizes every prefix.

**FR-28.** The verifier shall discharge the obligation by asking an SMT solver for the **negation** (`∃ s, e : ok is currently false but becomes true after update`) and interpreting `unsat` as *proved negatively stable* and `sat` as a *candidate counterexample* with a concrete witness.

**FR-29.** For a non-factorized aggregate the obligation shall be checked **per NFA transition** `(q, q')`, ranging over the finite transition set, so quantification stays over data values only.

**FR-30.** The verifier shall return exactly one of: `PROVED` (sound: the constraint is negatively stable), `COUNTEREXAMPLE` (a state/edge witness was found — see the soundness caveat below), or `UNKNOWN` (solver timeout or a construct outside the supported theories).

**FR-31.** In the workbench, a "check negative stability" action shall run the verifier over the current selective aggregate and surface the result and any witness next to the relevant transition.

> *Mechanization:* Z3 via its Python API. Theory selection by property type: LIA/LRA/LIRA for numeric amounts and timestamps; the array or finite-set fragment for `edge_ids` membership (trail); sequence/datatype theory only if list-shaped state is retained rather than flattened. Invariant inference (Problem B) would use CHC/Spacer or an inductive prover; that remains out of scope even if this stretch objective is pursued (Section 12).

**NFR-3 (verifier soundness, stated precisely).** `PROVED` is sound: an `unsat` result establishes negative stability. `COUNTEREXAMPLE` is a witness over *all* states, including states that may be unreachable in a real path exploration; it therefore means "not provable negatively stable by the single-step method," not necessarily "observably non-stable at runtime." This caveat shall be documented so no reviewer can read a false claim of completeness into it.

**NFR-4 (adequacy obligation, acknowledged).** The verifier assumes the incremental triple `(S, u, ok)` faithfully computes the author's declarative predicate `φ` (i.e. `ok(fold(u, s0, xs)) = φ(xs)` for all lists). Discharging adequacy is an induction over lists and is **not** attempted by the PoC; it is stated as an assumption and flagged as future work. Naming this pre-empts the obvious "you only proved self-consistency of the incremental form" objection.

**Traceability, if pursued:** this offers a partial, PoC-level answer to R2.O3 and R5.O3. It checks a *supplied* incremental encoding rather than searching for one, so it does not fully resolve the reviewers' concern, but it would materially strengthen the honesty of the revision's automation story.
---

# Part II — LLM-Assisted Selective-Aggregate Authoring (Optional Module)

*The following extends Modules A–I above. It is optional and peripheral: the compiler of Part I is deterministic and complete without it. Numbering continues (Module J, FR-34+; NFR-6+). The design invariant: the LLM drafts function bodies into the skeleton the compiler already emits; it never decides negative stability, never bypasses validation or user acceptance, and its worst case is a lost speedup, never a wrong answer.*

## 1. Where it fits in the pipeline

The proposer is an **optional branch inside Module D** (selective-aggregate frontend), between skeleton generation (FR-12) and the author's edit step. It does not sit on the path from an accepted aggregate to SQL; that path (Modules E/F/G) is unchanged and never sees LLM output directly.

```
            ┌──────────────────────── Module D ────────────────────────┐
            │                                                           │
 regex ─► NFA ─► CASE skeleton ──┬─────────────────────────► author edits ─► accepted
 (B/C)          (FR-12)          │                              ▲   aggregate
                                 │   ┌──────────────────────┐   │      │
                                 └──►│ J. LLM proposer      │───┘      │ (same as manual path)
                                     │  drafts body OR DEFER│          ▼
                                     └──────────┬───────────┘   validation (FR-14)
                                                │                      │
                                        (draft is inert;              ▼
                                         user must accept)     E. standard SQL gen
                                                               F. optimize ► G. execute
```

Two properties of this placement:

- **The draft is inert.** The proposer writes into an editable buffer, exactly like an IDE autocomplete suggestion. Nothing it produces is compiled or executed until the user accepts it, at which point it is indistinguishable from a hand-written body and flows through the identical validation and codegen.
- **The fallback is the default construction.** If the proposer declines (`DEFER`) or its draft fails validation and the user does not repair it, the compiler uses the default construction (Module D default mode): the constraint is checked in `is_viable_d_final` on complete paths, i.e. SOTA late filtering. This is the regret-free path (Observation 1 in the paper).

---

## 2. Functional requirements (Module J)

**FR-34.** The proposer shall accept as input: (a) the constraint to encode, as a natural-language description and/or a declarative SQL-style predicate over path properties; (b) the edge/vertex schema (column names and types) from Module A; (c) the NFA transition set `T` and the state semantics from Module C; (d) the list of dictionary keys already declared, if any.

**FR-35.** The proposer shall return a **structured** result containing, for each of the five selective-aggregate functions, either an SQL expression body or an explicit "leave default" marker, plus a required `negatively_stable ∈ {yes, no, unsure}` classification and a short rationale. The output format shall be machine-parseable (JSON) so it can be routed into the skeleton without free-form text handling.

**FR-36 (fail-safe classification).** When the proposer's classification is `no` or `unsure`, it shall emit `is_viable_d = TRUE` (no pruning) and place the full constraint in `is_viable_d_final`. The proposer shall be instructed and post-checked never to emit a pruning predicate in `is_viable_d` while classifying the constraint as anything other than `yes`. A proposal that violates this shall be rejected by the system before it reaches the user (treated as a malformed proposal, FR-40), not shown as a valid draft.

**FR-37 (structured target, not free-form).** For a non-factorized constraint, the proposer shall fill the per-transition branches of the CASE skeleton generated in FR-12, one body per `(from_state, to_state)` pair, rather than generating an unconstrained function. For a factorized constraint it shall fill a single unconditional body. The proposer never generates the surrounding query structure — only the function bodies.

**FR-38 (same validation as human input).** Every accepted proposal shall pass the identical Module-D validation as a hand-written aggregate (FR-14: references only declared keys, schema columns, and state variables; correct types) and the identical inlinability check (FR-23). The LLM path shares this gate; it has no privileged path.

**FR-39 (user is author of record).** The interface shall present the draft for review and editing and shall require an explicit accept action before the draft becomes the active aggregate. The system shall record that the aggregate was LLM-drafted (provenance), for reproducibility and for the artifact's transparency.

**FR-40 (malformed-proposal handling).** If the proposer returns unparseable output, references unknown columns/keys, produces non-inlinable code, or violates FR-36, the system shall discard the proposal, surface a diagnostic, and offer the empty skeleton (manual authoring) or the default construction. It shall never partially apply a malformed proposal.

**FR-41 (reproducibility controls, local open-weights model).** The proposer shall use a **local, open-weights** model rather than a hosted API, so the artifact is self-contained and anonymous. The system shall pin the exact model version **and numeric precision/quantization** (results differ across quantizations), use a fixed low sampling temperature by default, and ship the exact prompt templates together with the model weights (or a weights hash plus a download script). Each proposal shall be logged with model id, precision, prompt version, and inputs, so runs are bit-for-bit reproducible and reportable (supports §4 and NFR-8). The **specific model identity shall live in the artifact, not the submitted PDF**; the paper refers to it generically (e.g. "an open-weights instruction-tuned model, specified in the artifact") to avoid a de-anonymizing side-channel about the group's infrastructure.

**FR-42 (optional, off by default).** LLM assistance shall be an explicit opt-in in the workbench. With it disabled, the pipeline behaves exactly as Modules A–I specify. No dataset, query, or result shall depend on the proposer being invoked.

**FR-43 (advisory rationale and mini-example/witness).** In addition to the function bodies (FR-35), the proposer shall return a short natural-language `rationale` and a concrete `witness` that guides the user, subject to the classification-dependent rule below. The witness is **advisory only** and shall never gate compilation; the sole exception is FR-43(b), which the system may re-evaluate.

  (a) *For a `yes` (negatively stable) classification*, the witness is one illustrative path on which the incremental check behaves as intended. It is presented as guidance only and shall **not** be treated as evidence of correctness — a single example cannot establish the universal "holds on every extension." The UI shall not imply otherwise.

  (b) *For a `no`/`unsure` classification*, the proposer shall return a **prefix→extension counterexample**: a short path `p'` on which the constraint is false together with an extension `p ⊇ p'` on which it becomes true (e.g. amounts `[10]` with median `> X`, extended to `[10,1,1,1]` with median `≤ X`). Because this witness is a pair of concrete paths, the system **may re-evaluate the constraint on `p'` and `p`** and confirm the flip. A confirmed flip is a cheap, sound corroboration that the constraint is not negatively stable (justifying the safe deferral); a witness that fails to flip is discarded as unreliable and the classification is treated as `unsure`. This is the same object an SMT check would prove and is the natural precursor to that future check.

  The mini-example shall never be used to *promote* a constraint to prunable: no witness can turn a `no`/`unsure` into a `yes`. It may only corroborate deferral or serve as user guidance.

---

## 3. The prompt

The proposer uses a fixed system prompt (the abstraction contract and the safety rule) and a templated user prompt (the concrete constraint and context). The safety rule — *decline rather than guess at negative stability* — is encoded in the prompt itself and re-checked by the system (FR-36).

### 3.1 System prompt (fixed)

```
You draft the body of a ReCAP "selective aggregate": a small set of SQL
expressions that let a recursive path query filter doomed paths early.

A selective aggregate maintains a dictionary D of key-value pairs along a
path and defines up to five functions over the signatures:
  init_d()                         -> initial dictionary
  update_d(D, from_state, to_state, e) -> updated dictionary after edge e
  is_viable_d(D, from_state, to_state, e) -> BOOLEAN  (early-pruning check)
  is_viable_d_final(D)             -> BOOLEAN  (final check on complete paths)
  finalize_d(D)                    -> output value
Bodies are SQL expressions over: dictionary keys in D, the candidate edge's
columns (e.<column>), and the NFA state variables from_state / to_state.

CRITICAL RULE — negative stability:
  is_viable_d may prune a path ONLY IF the constraint is "negatively stable":
  once it is false on a path, it is false on EVERY extension of that path
  (e.g. monotone timestamps, a set-membership/trail check, or a monotone
  aggregate bound like max-min <= U). If a constraint is negatively stable,
  put its incremental check in is_viable_d. If it is NOT negatively stable,
  or if you are unsure (e.g. MEDIAN <= X, a lower bound like SUM >= X, or
  "there exist two adjacent equal edges"), you MUST set is_viable_d = TRUE
  and place the full check in is_viable_d_final. Never emit a pruning
  predicate you cannot justify as negatively stable. When in doubt, defer.

Also return a short rationale and a concrete witness:
  - If negatively_stable = "yes": give ONE illustrative example path. This
    is guidance only; it does NOT prove correctness.
  - If negatively_stable = "no" or "unsure": give a COUNTEREXAMPLE as a
    prefix path where the constraint is FALSE and an extension where it
    becomes TRUE (e.g. amounts [10] median>X, extended to [10,1,1,1]
    median<=X). This shows why early pruning would be unsound.
  A witness never makes a "no"/"unsure" constraint prunable.

Return ONLY JSON of the form:
  {
    "negatively_stable": "yes" | "no" | "unsure",
    "rationale": "<one or two sentences>",
    "witness": {
       "kind": "illustration" | "counterexample",
       "prefix": "<short path or property sequence, e.g. [10]>",
       "extension": "<extension path, only for counterexample, e.g. [10,1,1,1]>",
       "note": "<what to observe>"
    },
    "dictionary_keys": [ {"name": "...", "type": "...", "init": "<expr>"} ],
    "update_d": { "<from>-<to>": "<expr>", ... }   // or "default" if append-only,
    "is_viable_d": { "<from>-<to>": "<expr>", ... } // or "TRUE" when deferring,
    "is_viable_d_final": "<expr>",
    "finalize_d": "<expr>"    // or "default"
  }
Do not output anything except this JSON.
```

### 3.2 User prompt (templated)

```
CONSTRAINT (natural language): {nl_description}
CONSTRAINT (declarative, if available): {sql_predicate}

EDGE SCHEMA: {columns_with_types}
VERTEX SCHEMA (if used): {vertex_columns_with_types}

NFA TRANSITIONS (from_state, to_state, label):
{transition_rows}
ACCEPTING STATES: {accepting_states}

ALREADY-DECLARED DICTIONARY KEYS (if any): {existing_keys}

Draft the selective aggregate per the rules. If the constraint is not
negatively stable, defer it (is_viable_d = TRUE, full check in
is_viable_d_final) and set negatively_stable accordingly.
```

### 3.3 Worked example (the max–min bound, factorized)

Given `nl_description = "the difference between the largest and smallest edge amount on the path is at most U (U = 89.55)"`, `columns = amount DOUBLE, ...`, a single-state loop NFA, the expected proposal is:

```json
{
  "negatively_stable": "yes",
  "rationale": "max-min is monotonically non-decreasing under extension, so once it exceeds U no extension can satisfy it.",
  "witness": {
    "kind": "illustration",
    "prefix": "amounts [50, 60] -> gap 10 <= 89.55 (viable)",
    "note": "extending can only widen the gap; illustration only, not a proof"
  },
  "dictionary_keys": [
    {"name":"max_amt","type":"DOUBLE","init":"-1e308"},
    {"name":"min_amt","type":"DOUBLE","init":"1e308"}
  ],
  "update_d": "default_scalar: max_amt = GREATEST(max_amt, e.amount); min_amt = LEAST(min_amt, e.amount)",
  "is_viable_d": "GREATEST(max_amt, e.amount) - LEAST(min_amt, e.amount) <= 89.55",
  "is_viable_d_final": "TRUE",
  "finalize_d": "default"
}
```

And the **negative** control — `nl_description = "the median edge amount is at most X"` — must yield:

```json
{
  "negatively_stable": "no",
  "rationale": "median is not monotone under extension; a prefix above X can drop below X after more edges, so early pruning is unsound.",
  "witness": {
    "kind": "counterexample",
    "prefix": "[10]  (median 10 > X=5, constraint FALSE)",
    "extension": "[10,1,1,1]  (median 1 <= X=5, constraint TRUE)",
    "note": "constraint flips false->true under extension; system can re-evaluate to confirm"
  },
  "is_viable_d": "TRUE",
  "is_viable_d_final": "median(amounts) <= X",
  "...": "..."
}
```

Correctly producing the second case — *declining to prune* — is as important as the first and is measured directly (§4, EXP-2).

---

## 4. Experiments

The benchmark queries have known-correct, hand-written selective aggregates. That gives a **ground-truth oracle** (rare for LLM-rewrite work) against which drafts are scored by result-equivalence, not by human judgment.

**EXP-1 — Drafting accuracy (result-equivalence).** For each benchmark constraint with a known aggregate, sample the proposer `k` times; compile each proposal through the normal path; run on the test graphs; compare the returned answer set (both count and the actual path sets) against the ground-truth ReCAP. Report `pass@1` and `pass@k` per constraint family (adjacent-edge predicate, monotone/distributive aggregate, trail, non-factorized transition-dependent). *Success:* identical result sets.

**EXP-2 — Safe decline (knowing when NOT to prune).** Include constraints that are *not* negatively stable: `MEDIAN ≤ X`, a lower bound `SUM ≥ X` (the Q1 total-amount case), and Q2's "two adjacent equal-color edges." The correct behavior is to classify `no`/`unsure` and defer. Report: (i) decline precision/recall on these cases, and (ii) the headline safety number — the **false-pruning rate**: the fraction of non-NS constraints for which the proposer emitted a pruning `is_viable_d`. This is the dangerous event (silent result corruption if accepted unedited); FR-36 rejects such proposals before display, but the *rate* quantifies how often the LLM would have erred without the guard. Additionally report (iii) **counterexample validity**: for `no`/`unsure` classifications, the fraction whose prefix→extension witness (FR-43b), when re-evaluated by the system, actually flips false→true. High validity means the LLM's caution is *corroborated*, not merely asserted — a cheap, sound signal on the safety-critical decisions.

**EXP-3 — Silent-corruption rate under blind acceptance.** Combine EXP-1 failures and EXP-2 false-prunings into a single number: if a user accepted every draft unedited, on what fraction of constraints would results differ from ground truth? This directly motivates keeping the human (and, later, an SMT verifier) in the loop, and is the honest counterweight to the accuracy numbers.

**EXP-4 — Prompt-structure ablation (optional).** Compare accuracy with vs. without the NFA transitions supplied, and with vs. without the abstraction contract in the system prompt. Shows whether the compiler-provided structure (the skeleton, the transitions) actually improves proposals — i.e., that the scaffolding is load-bearing, not decorative.

**EXP-5 — Authoring-effort proxy (optional).** Report token- or AST-edit distance between the accepted body and the LLM draft, as a rough proxy for effort saved when the draft is close.

**Reporting requirements.** All numbers are reported against a pinned model snapshot, fixed temperature, and shipped prompt version (FR-41), stated explicitly, with the caveat that result-equivalence on a fixed benchmark is an empirical signal, not a correctness proof.

---

## 5. Non-functional / correctness requirements

**NFR-6 (fail-safe dominance).** No LLM outcome shall be able to produce a result set different from the deterministic ground truth *without* either passing validation (FR-38) and explicit user acceptance (FR-39), or being caught by FR-36/FR-40. The residual risk — a *plausible but unsound* pruning predicate that passes validation, is (wrongly) classified `yes`, and is accepted by an inattentive user — shall be stated explicitly in the paper as the known limitation, with the sound resolution (an SMT check of the one-step negative-stability obligation) named as future work.

**NFR-7 (no privileged path).** The LLM branch shall share every downstream gate with manual authoring. There shall be no code path by which an LLM proposal reaches execution that a typed, hand-written body could not.

**NFR-8 (reproducibility).** Given the pinned local open-weights model (version + precision), temperature, prompt version, and inputs, proposer behavior shall be reproducible for the reported experiments, and the artifact shall ship the weights (or hash + fetch script) and everything else needed to re-run them offline. The submitted PDF names the model only generically; the artifact carries the exact identity (FR-41). A local model is preferred here not only for anonymity but because a hosted API cannot be shipped, pinned, or guaranteed stable, so it could not satisfy this requirement.

**NFR-9 (scope containment).** The LLM subsection and its experiments shall occupy at most a short subsection plus one figure. If the feature grows to compete with the deterministic compiler for prominence, it is out of scope for this revision.

---

## 6. Error handling additions

| Class | Example | Stage | Behaviour |
|---|---|---|---|
| **L-PARSE** | proposer returns non-JSON or missing fields | J | discard, offer manual skeleton or default construction (FR-40) |
| **L-REF** | draft references an unknown column or undeclared key | J → FR-14 | reject draft, show diagnostic, keep buffer editable |
| **L-UNSAFE** | pruning `is_viable_d` emitted with classification ≠ `yes` | J → FR-36 | reject before display; never shown as a valid draft |
| **L-NONINLINE** | draft body outside the inlinable sublanguage | J → FR-23 | reject or fall back to UDF with warning |
| **L-API** | model timeout / API failure | J | report unavailability; pipeline proceeds with manual authoring or default construction |

---

## 7. Mechanization map additions

| Step | Novel? | Component |
|---|---|---|
| Prompt assembly from schema + transitions | No | templating over Module A/C outputs |
| LLM invocation | No | a pinned hosted or local model behind a fixed API; low temperature |
| Structured-output parsing | No | JSON schema validation |
| Draft validation | No (reused) | Module D FR-14 + FR-23 (`sqlglot`) — **shared gate** |
| Result-equivalence harness | Partly | run draft vs. ground-truth ReCAP on test graphs, compare answer sets (custom, small) |

The only genuinely new machinery is the result-equivalence harness for the experiments; everything correctness-bearing reuses the deterministic compiler's existing gates.

---

## 8. Traceability

| Requirement(s) | Serves |
|---|---|
| FR-34..FR-39, §1 placement | R2.O3 / R5.O3 (a concrete, bounded automation story for authoring the aggregate) |
| FR-36, NFR-6, EXP-2, EXP-3 | preserves the R4.O1 correctness-is-localized rebuttal; distinguishes ReCAP from weak-guarantee LLM-rewrite work |
| EXP-1, EXP-4 | evidence the abstraction's structure makes drafting tractable (meta-crux 3) |
| FR-41, NFR-8 | artifact/reproducibility expectations |
| §6 non-goals below | keeps the AE-locked scope intact |

---

## 9. Explicit non-goals (unchanged from the base spec, made specific to Module J)

1. **The LLM never decides negative stability.** Classification is advisory; pruning is gated on `yes` *and* validation *and* user acceptance, with deferral as the safe default. Automatic, *sound* detection of negative stability (e.g. via an SMT check, or verified synthesis in a proposer–verifier loop) is future work and is not built here.
2. **No formal correctness guarantee for LLM output.** Result-equivalence on the benchmark is the only claimed signal; it is empirical.
3. **Not central.** The deterministic compiler (Modules A–I) is the automation contribution; Module J is an optional convenience. The paper shall frame it as such.
