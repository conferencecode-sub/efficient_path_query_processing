# E5 — Handcrafted vs. ReCAP-Optimized vs. Split/Reverse (2026-08-14)

Per `experiments/new_experiments_checklist/recap_experiments_requirements.md`'s
E5 (R4's single ranked point: "if ReCAP shows better performance, that changes
the game" — is ReCAP just a veneer over hand-pushing the constraint into the
`RECURSIVE` clause?). Q1 on real Metaverse data (`experiments/datasets/metaverse/`),
start vertex 383 (high-degree, matching every other Q1 experiment in this repo),
ℓ ∈ {2..10}, min_length=2, all three configs using Q1's **full** constraint set
(trail, region, risk-range, the last-risk gateway, amount) — not the simplified
regex+monotonic-time-only cut `alternative_explorations/navigation_experiment/`
uses for a different (R4.O2 navigation-style) question.

## The three configs

1. **Handcrafted** — `ReCAP/q1/recap_gen_recap_inline.py` (the canonical
   hand-inlined baseline, run directly, not re-implemented) — the plan R4
   says the engine already gets "for free."
2. **ReCAP-Optimized** — the new compiler's Stage F output (`recap-new-optimized`
   rows from `q1_length_sweep/results/new_compiler_q1.csv`, the E1 rerun).
3. **Split (full constraints)** — new this session: `split_full.py`, extending
   `alternative_explorations/navigation_experiment/naive_split.py`'s
   seam-splitting technique (F1 = states {0,1}, F2 = states {1,2}, split at
   the `(transfer|purchase|sale)+` → `(phishing|scam)+` seam) to Q1's **full**
   constraint set, not just regex+monotonic-time.

**Correctness confirmed exactly** at every ℓ, matching E1's own path counts:
23/95/264/466/711/745/840/852/878 — cross-validated three independent ways
(`baseline_full.py` a monolithic full-constraint reimplementation used only
for the in-process equivalence check, `check_equivalence_full.py`'s `EXCEPT
ALL` diff against `split_full.py`, and the official hand-inlined script run
standalone).

## Constraint placement in the split (worked out from the hand-inlined query's
exact semantics, not guessed — see `split_full.py`'s docstring for the full reasoning)

- Trail, region, monotonic-time, risk-range are per-hop-checkable, enforced
  inline within whichever fragment the hop belongs to.
- **Trail needs no cross-fragment bookkeeping**: F1's edges are always
  normal-labeled, F2's always fraud-labeled (the NFA structurally partitions
  the edge set by label), so no edge_id can appear in both fragments of the
  same path.
- **Region and amount need cross-fragment state**, carried on F1's boundary
  rows into F2.
- **The `last_risk >= 40` gateway is the interesting one.** The hand-inlined
  query checks it in the recursive `WHERE` exactly at the state 1→2
  transition. The new compiler's `q1_aggregate.py` pushes it into
  `is_viable_d_final` (checked only after the *entire* path, including the
  whole fraud suffix, is built) — because Q1's aggregate is `factorized=True`,
  which has no access to NFA state and can't express "check only at this
  transition." **The split recovers the earlier, tighter timing for free**:
  F1's boundary rows *are* the state-1-about-to-transition point, so
  `frag1_boundary WHERE last_risk >= 40` is exactly the hand-inlined query's
  own enforcement point.

## Results (full data in `results/e5_q1_metaverse.csv`)

| ℓ | result | handcrafted ms | recap-optimized ms | split ms | optimized/handcrafted |
|---|---|---|---|---|---|
| 2 | 23 | 21.1 | 23.7 | 19.7 | 1.12x |
| 4 | 264 | 40.1 | 58.0 | 48.8 | 1.45x |
| 6 | 711 | 67.8 | 108.6 | 73.3 | 1.60x |
| 8 | 840 | 81.9 | 146.7 | 97.4 | 1.79x |
| 10 | 878 | 101.0 | 164.9 | 106.0 | 1.63x |

## Findings — reported honestly, including one that doesn't match the doc's hypothesis

**Handcrafted ≈ Split, both consistently beat ReCAP-Optimized.** Split tracks
handcrafted closely at every ℓ (ratio 0.9x–1.2x, no consistent direction) —
this is a real "compatibility, not superiority" result for *this* query,
matching the design doc's own stated framing. Both handcrafted and split beat
ReCAP-Optimized by a growing margin: 1.12x at ℓ=2, up to ~1.6-1.8x by ℓ=8-10.

**This does NOT match the requirements doc's stated hypothesis "(1)≈(2) —
the abstraction costs ~nothing over hand-writing."** For Q1 specifically, the
optimized/factorized query has a real, measurable, *growing* cost relative to
hand-inlining. Root cause is the same factorization gap described above: by
deferring the `last_risk >= 40` gate to `is_viable_d_final`, the optimized
query keeps exploring the entire fraud suffix for paths that a
state-aware check would have pruned at the seam — wasted work that grows with
ℓ, exactly matching the widening gap observed. This is a genuine, useful data
point for the R4 response, but it complicates the clean "(1)≈(2)" framing:
**the abstraction (Definition 8's `is_viable_d`/`is_viable_d_final` split)
can express the tighter timing — the split/reverse realization and the
hand-inlined query both do — but the specific factorized-only SQL generator
currently used for Q1 gives up some of that precision for structural
simplicity.** Worth deciding how to frame this in the paper: as an honest
limitation of the current factorized code generator (fixable, e.g. by
detecting single-transition-triggered final checks and allowing a
non-factorized fallback), or as evidence that hitting `(1)≈(2)` requires the
right encoding choice, which is itself part of ReCAP's story.

**The split's headline "reach" win from the earlier, simplified Phase 1
experiment doesn't reproduce at the same magnitude here — for a real, findable
reason, not a discrepancy.** `alternative_explorations/navigation_experiment/`
found a 5x speedup from splitting when Q1 was cut down to *only* regex +
monotonic-time (no region/risk-range pruning) — because with only weak
per-hop pruning, F1's un-split exploration wastes enormous work in the
`transfer|purchase|sale` self-loop before ever reaching the seam. With Q1's
**full** constraint set, region and risk-range already prune F1 aggressively
on their own (only 878 total accepted paths reachable even at ℓ=10), so
there's much less "wasted branching before the seam" left for splitting to
additionally remove. **Both results are valid and tell complementary halves
of the same story**: splitting's benefit is largest when it's the *only*
seam-aware pruning available; when ReCAP's own per-hop property filtering is
already strong, splitting instead pays for itself by recovering NFA-state-
aware precision (the `last_risk` timing) that a factorized single-recursion
encoding gives up, rather than by avoiding combinatorial blowup.

## Files

- `common_full.py` — loader, Q1's full NFA + real edge columns.
- `baseline_full.py` — monolithic full-constraint reimplementation, used only
  to give `check_equivalence_full.py` an in-process reference (the officially
  reported "handcrafted" numbers above come from running
  `ReCAP/q1/recap_gen_recap_inline.py` directly, not this file).
- `split_full.py` — the actual E5 config-3 implementation.
- `check_equivalence_full.py` — `EXCEPT ALL` diff, confirms MATCH at every ℓ
  tested (2 through 10).
- `results/e5_q1_metaverse.csv` — all three configs' numbers.

## Not done in this pass

- The doc's second query ("one synthetic rare-label query") — not built.
- Genuine directional *reversal* (traversing a fragment backward over reversed
  edges) — still explicitly out of scope, same as the original Phase 1 doc's
  own deferred stretch goal.
- Peak memory wasn't measured for these three configs (all are small,
  single-vertex, DuckDB-only runs — expected to be dominated by loading the
  78.6k-edge dataset, per Q1's own E1 rerun finding).
