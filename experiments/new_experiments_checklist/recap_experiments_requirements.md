# ReCAP Revision — Experiments Requirements

**Purpose.** Specify every experiment the revised paper must contain: what exists and is kept, what's newly required, and the datasets/queries/metrics/protocol for each. Experiments are tiered by priority against the 13-page budget (one extra page over the 12-page original), because not all new figures fit — Tier 1 is acceptance-critical, Tier 2 is expected, Tier 3 is nice-to-have / appendix.

**Standing protocol (applies to all, from the current paper).** Real + synthetic graphs (Table `tab:realdata`); start vertices sampled from low/medium/high out-degree quartiles; each config run 5× after 1 warm-up, report median (runs within 5%); 2-hour timeout; path lengths `ℓ ∈ {2..10}`; queries report a *count* of result paths but plans are verified to enumerate paths first (no counting shortcut). New experiments inherit this unless stated otherwise.

---

## A. Existing experiments (keep; already in the paper)

**E0 — UDF-overhead breakdown** (Table `tab:udf-cost-breakdown`). Python-UDF baseline, UDF vs non-UDF time by `ℓ`. *Keep as-is;* motivates the optimization layer. Status: DONE.

**E1 — Optimization impact: Standard vs Optimized ReCAP** (Fig. `fig:recap_performance_grid`, Q1–Q4). Shows dictionary flattening + inlining give up to 346×. Status: DONE.

**E2 — Running-time vs SOTA** (Fig. `fig:performance_grid`, Q1–Q4 across Metaverse/Bitcoin/Reddit/LDBC100), vs DuckDB, System X, Neo4j, Memgraph, Kùzu. Status: DONE.

**E3 — Intermediate-result cardinality** (Fig. `fig:intermediate_total_grid`). Root-causes the runtime gap; shows the trail-vs-walk distinction is negligible here. Status: DONE.

> These four already discharge the *headline* speedup and the root-cause story. The new experiments below target specific open questions they do **not** yet answer.

---

## B. New experiments

### E4 — Isolation: automata-exploration vs property early-filtering  **[Tier 1]**
**Question it answers:** how much of the speedup is the NFA-as-a-join (regex exploration) versus the property early-filtering? Prior work ([13]) shows the NFA approach *alone* already helps; that contribution needs to be separated cleanly from ReCAP's own property pruning.
**Design.** Three configurations on the *same* query/graph, varying `ℓ`:
1. **Regex-only** — NFA-driven exploration, no property constraint (or constraint deferred to the end). Isolates the automata-as-join contribution.
2. **Regex + late property check** — the SOTA-style plan (default construction).
3. **Regex + early property filtering** — full ReCAP.
Report runtime *and* intermediate cardinality for each. The gap (1)→(3) attributable to early filtering is the headline; the (regex-only vs naive) gap credits the automata contribution honestly.
**Sweep two axes:** (a) regex selectivity — run on a non-trivial pattern at several selectivities; (b) path returned vs not returned (endpoints only vs full edge-id sequence), since returning paths changes the pruning/storage story.
**Metric.** Runtime (ms), intermediate paths. **Datasets.** Reuse Reddit + one LDBC scale. **Queries.** Q3 (pure monotonicity, cleanest isolation) + one multi-label-regex query.

### E5 — Handcrafted incremental CTE vs ReCAP  **[Tier 1 — the crux]**
**Question it answers:** is ReCAP just a "veneer" over hand-pushing the constraint into the `RECURSIVE` clause, or does the abstraction cost something real relative to hand-writing?
**Design.** Same queries/graphs, three plans:
1. **Handcrafted** — constraints inlined by hand in the recursive `WHERE` (the plan a hand-optimized rewrite already gets "for free").
2. **ReCAP-Optimized (forward)** — current paper.
3. **ReCAP + split/reverse** — ReCAP under a WAVEGUIDE-style planner that splits the regex at a selective label and may reverse a fragment (uses the preserved NFA).
**Expected result (state as hypothesis, then confirm):** (1)≈(2) — the abstraction costs ~nothing over hand-writing (defuses "veneer" on the cost axis); (3) > (1),(2) on queries with a *selective interior/rare label* — the win is only reachable because ReCAP preserved the NFA (the reach axis). Verify (1),(2),(3) return identical result sets (result-equivalence check).
**Metric.** Runtime; also report the split point chosen. **Datasets.** A graph + query with a genuinely selective interior label (construct one; see regex-realism note below). **Queries.** Q1 (has the `normal⁺·fraud⁺` structure with a natural split) + one synthetic rare-label query.
**Note (correctness companion, not an experiment):** the correctness theorem (two local obligations vs undecidable recursive-query equivalence) is the *non-performance* half of the answer to the "veneer" concern — it lives in the body/appendix, not here, but E5 is the performance evidence this concern calls for.

### E6 — End-to-end on a realistic workload (FinBench / LDBC)  **[Tier 1]**
**Question it answers:** do the target queries occur in a real benchmark, and does ReCAP help *overall query performance*, not just on hand-picked examples?
**Design.** Take LDBC **FinBench** transfer-path queries (TCR1/TCR5 use ascending-timestamp multi-hop transfer paths = negatively-stable monotonicity; TCR8 uses per-step amount thresholds = NS aggregate). FinBench caps paths at a small constant and its reference Cypher resorts to list-processing/`reduce` folds — exactly the pattern the paper targets. **Lift the cap to variable-length** and run end-to-end (query submission → full output), ReCAP vs the SOTA systems.
**Metric.** End-to-end runtime; report the reference-Cypher list-processing plan for contrast. **Datasets.** FinBench generator at ≥1 scale factor; LDBC SNB where a KNOWS-path query can be adapted.
**Deliverable framing:** "the queries are not synthetic — here are standardized benchmark queries, and ReCAP lifts their length restriction without sacrificing early filtering."

### E7 — Scale and memory  **[Tier 2]**
**Question it answers:** does ReCAP hold up on larger graphs, and what does it cost in memory when the viable-path state genuinely can't be collapsed?
**Design (two parts).**
- **Scale:** run the early-filtering queries (Q1/Q3/Q4) on **Datagen-7.7** (13.1M vertices / 53.7M edges, already in `tab:realdata` but not yet exercised for these) to answer "why only modest graphs?" — show ReCAP still scales where competitors time out.
- **Memory:** report peak memory for ReCAP vs a competitor, specifically in the **many-viable-paths regime** (full-path/trail state stored). Include at least one query where early filtering does *not* collapse the state (so storage is genuinely stressed).
**Metric.** Runtime + peak RSS (via DuckDB profiling / `/usr/bin/time`).

### E8 — `Q2` clarification (piggybacking without early filtering)  **[Tier 2]**
**Question it answers:** in Fig. 8/9, does "ReCAP" mean Optimized specifically, and how does Standard compare to competitors on Q2?
**Design.** No new runs likely needed; *presentation* fix — state that Fig. `fig:performance_grid` "ReCAP" = ReCAP-Optimized, and add the Standard-vs-competitors point for Q2 so the piggybacking speedup (last_color + completed in `is_viable_d_final`) is unambiguous. Status: mostly a caption/text fix.

---

## C. Regex realism (strengthens E4/E5)

The regex side needs to be non-trivial and not cherry-picked. Use **real query-log studies** to justify the *shape* of the label patterns (Kleene-star/plus, unions, concatenations) that make constraints list-based:

- Bonifati, Martens, Timm. *An Analytical Study of Large SPARQL Query Logs.* PVLDB 11(2), 2017.
- Bonifati, Martens, Timm. *Navigating the Maze of Wikidata Query Logs.* WWW 2019.

**Use (scope-limited):** cite these to argue that property-path patterns with transitive closure (the patterns that *force* list-based constraints, per §Problem) are common in practice, and pick the regex complexity/selectivity levels in E4/E5 to match the distribution these logs report (e.g., prevalence of `*`/`+`, typical alternation width). **Do not** claim to replay the logs — they are RPQ/label workloads without ReCAP's property constraints; they justify the *regex* dimension only. This grounds the pattern space in measured reality rather than a hand-picked example, and addresses the general concern about query-set breadth ("only 4 queries").

---

## D. Concern-to-experiment mapping

| Open question | Experiment(s) |
|---|---|
| Isolate automata exploration from property pruning; non-trivial regex; path returned or not | **E4** (+ regex realism, §C) |
| Modest graph sizes | **E7 scale** (Datagen-7.7) |
| Path return + memory; only 4 queries | **E7 memory** (+ E4 path-returned axis) |
| Why not hand-write into RECURSIVE / veneer concern | **E5** (perf) + correctness theorem (body, not an experiment) |
| Integration with richer query-plan strategies | **E5 config 3** (split/reverse) |
| Real workloads / overall performance | **E6** (FinBench/LDBC end-to-end) |
| Figure labeling / Q2 Standard vs competitors | **E8** (presentation) |

---

## E. Priority tiers vs the 13-page budget

The extra page cannot hold every new figure. Ranking:

- **Tier 1 (must land in body):** E4, E5, E6 — these three map onto the top-priority open questions from review. If only these fit, the revision still answers every one of them.
- **Tier 2 (body if space, else appendix):** E7 (scale + memory), E8 (presentation fix — nearly free).
- **Tier 3 (appendix / cut first):** extra regex-selectivity sweeps beyond the one non-trivial pattern in E4; additional Datagen-7.7 queries beyond one.

**Figure-space tactic.** E4 and E5 can likely share a single multi-panel figure (both are "3 configurations vs `ℓ`" plots), and E7-scale can extend the existing `fig:performance_grid` with a Datagen-7.7 panel rather than a new float. Reuse existing figure scaffolds (the `tikzpicture`/`pgfplots` grids already in the paper) to minimize page cost. E6 needs its own figure (it's a different workload) — budget for that one.

---

## F. Open dependencies (block "done")

1. Construct/confirm a graph+query with a genuinely **selective interior label** for E5 config 3 (else the split/reverse win can't be shown).
2. FinBench generator setup + choice of scale factor(s) for E6; confirm which TCR queries lift cleanly to variable length.
3. Memory-measurement harness (peak RSS) for E7.
4. Confirm the Q1 `region`/`amount` and Q4 constraint semantics match the paper text (needed so E4/E5 result-equivalence checks are meaningful).
