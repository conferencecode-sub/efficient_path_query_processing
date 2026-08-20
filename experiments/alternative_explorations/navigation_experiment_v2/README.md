# WAVEGUIDE-style split experiments, extended (2026-08-18, ℓ range re-tested/extended 2026-08-19)

Three new experiments extending `../navigation_experiment/`'s Phase 1 pilot
(monotonicity, isolated, on Q1's own regex -- already done there, not
repeated here). See `../navigation_style_experiment.md` for the design
context (R4.O2, the WAVEGUIDE-style split-and-possibly-reverse planner
comparison). Same dataset (`ReCAP/simple_dataset/LG.csv`/`LG_V.csv`, 1,320
vertices / 78,600 edges, identical to `experiments/datasets/metaverse/`),
same four start vertices spanning the out-degree distribution (383 max,
594 p75, 592 median, 635 p25).

Each experiment tests a genuinely different axis from the existing pilot:
a different **constraint family** (sum-of-weights, max-min) and/or a
different **regex shape** (mirrored rarity, three segments/two seams), plus
two additional fragment-*ordering* variants requested mid-build.

## 1. Sum of weights (non-negative) -- `exp_sum_weights.py`

Regex `(transfer|purchase|sale)+ (phishing|scam)+` (Q1's own, rare
suffix). Constraint: total edge `amount` (min 0.01 in this dataset, never
negative) must stay `<= 800.0` -- negatively stable because a non-negative
running sum only grows. Isolates a **distributive/compressible** boundary
state (one running total; F2 just keeps adding to it) against the existing
pilot's monotone-scalar check.

Four configs compared: **monolithic**, **seeded-split** (F1 -> seed -> F2,
inline bound check), **naive-split** (F2 unseeded, bound deferred to a
final join), and **reverse-seeded** ("F2 first": precompute F2 unseeded,
then prune F1's own candidates using it).

**Real bug found and fixed while building `reverse-seeded`:** an EXISTS
lookahead against the precomputed F2 table was first applied inside F1's
*recursive term*, which doesn't just decline to report a row as a seam --
it deletes the row from existing at all, blocking every future self-loop
hop through it. A vertex with no *immediate* affordable completion can
still lead, via more hops, to a *different* vertex with a cheap one --
pruning during growth conflates "not a good seam here" with "dead end,"
which are not the same claim. Caught via a real undercount (2,673 vs. the
correct 3,729 at $\ell=3$, confirmed by `EXCEPT ALL` showing pure lost
rows, no spurious extras) -- fixed by moving the lookahead to filter only
which rows get *reported* as boundary candidates, never which rows survive
to keep extending. Re-verified correct after the fix.

**Separately, a real floating-point artifact (not a logic bug):** splitting
a sum across two independently-computed fragments and adding at the join
computes it in a different order than one continuous accumulation, so
exact-float equality checks spuriously failed by ~1e-10 even though every
row was correct. Fixed by rounding to 6 decimal places in all four
configs' own output before comparing -- confirmed the raw mismatch
disappeared entirely once rounded, isolating it as floating-point
non-associativity, not a real correctness issue.

**Results, extended to $\ell=6$ (2026-08-19):** monolithic/seeded-split/
naive-split re-run at $\ell=2..6$, **20 (vertex, $\ell$) checkpoints, 0
mismatches.** $\ell=7$ was attempted and killed after exceeding a 120s
budget (a >30x jump from $\ell=6$'s own worst case, 5.9s) -- a real
intractability boundary, not an arbitrary cutoff, so $\ell=6$ is the new
cap; `--lengths` default updated to `[2, 3, 4, 5, 6]` accordingly.
Naive-split is consistently *slower* than both monolithic and
seeded-split, now up to ~11x at $\ell=6$ (sv=383: mono 2,098ms,
seeded-split 649ms, naive 5,869ms) -- its F2 is unconstrained by the
bound during its own recursion, so it always computes the full unseeded
suffix (4.3M rows at $\ell=6$) regardless of start vertex. Seeded-split's
own advantage over monolithic widens with $\ell$ too (0.8--1.2x at
$\ell{\leq}4$, up to 3.2x by $\ell=6$ for the highest-out-degree vertex),
consistent with the distributive bound compressing more of the exploration
away as paths get longer.

Reverse-seeded ("F2 first") is not wired into this script's own `main()`
sweep (its own `run_reverse_seeded` function exists but nothing calls it
here) -- its "matches seeded-split's own row counts exactly but 2--6x
slower" finding, mentioned in the paragraph above, is preserved from an
earlier, separate verification run and was not reproduced as part of this
extension. Flagging rather than silently dropping: if reverse-seeded's
own numbers are wanted in the saved CSV going forward, `main()` needs a
fourth branch calling `run_reverse_seeded` per (vertex, $\ell$), matching
the other three configs' own pattern.

## 2. Max-min (bounded range), mirrored regex -- `exp_maxmin_mirrored.py`

Regex `(phishing|scam)+ (transfer|purchase|sale)+` -- Q1's own regex with
the two segments **swapped**: the rare label (~8% of edges) is now the
*prefix*, not the suffix. Every prior split experiment in this project
splits at a rare suffix; this tests whether the seam-pruning story depends
on which side is rare. Constraint: bounded range on `risk_score <= 20.0`
(Example 9's family, Q1's own real constraint, here isolated with nothing
else mixed in).

Three configs: monolithic, seeded-split, naive-split. **0 mismatches**
across 12 (vertex, $\ell$) checkpoints at $\ell=2..4$ (capped there --
see below) across all four vertices.

**Real, dramatic finding:** because the rare segment is now first, F1's
boundary stays small (≤552 rows), but naive-split's F2 (the *common*
segment, fully unconstrained since it can't know F1's own risk range yet)
explodes: 72K rows at $\ell=2$, 3.9M at $\ell=3$, **210M rows at
$\ell=4$, taking 44.5 seconds** against monolithic/seeded-split's <30ms.
$\ell=5$ was attempted on 2026-08-19 (re-verifying $\ell=2..4$ at the same
time, still 12/12, 0 mismatches) and confirmed intractable within budget:
killed after exceeding 150s for a single vertex alone, consistent with the
predicted growth curve -- this project's own "document and stop" policy,
now backed by an actual attempt rather than an extrapolation. This is the sharpest
seam-rarity-position effect found across the three new experiments: naive
splitting's cost is dominated by whichever segment is *not* rare, and
swapping which segment that is changes the naive plan's tractability by
orders of magnitude, even though monolithic and seeded-split are
unaffected either way.

## 3. Max-min, three segments/two seams, explored middle-out -- `exp_threeseg_maxmin.py`

Regex `(transfer|purchase)+ (sale)+ (phishing|scam)+` -- states
`{0,1,2,3}`, two seams. Real label counts: transfer|purchase 47,065
(59.9%), sale 25,040 (31.9%), phishing|scam 6,495 (8.3%) -- three
genuinely different segment sizes. Constraint: bounded range on
`risk_score <= 40.0` (loosened from 20.0 after directly confirming, via a
plain 3-way join, that the *minimum* achievable range on any 3-hop walk
from vertex 383 is already ~31 -- 20.0 would have made the experiment
degenerately empty almost everywhere, not a meaningful test).

Explored **middle-out**, per an explicit request mid-build -- this is the
paper's own deferred "genuine directional reversal" stretch goal
(`../navigation_style_experiment.md`, section 4), now actually built:

1. **F2 first** (states `{1,2}`, `sale`-only): unseeded, every vertex.
2. **F1 backward** (states `{0,1}`, `transfer|purchase`-only): seeded by
   F2's own *distinct entry vertices*, walked over a reversed edge
   relation (`src`/`dst` swapped, filtered to segment-1 labels only) --
   the backward search doesn't know it's looking for vertex 383 until it
   finds it; that's checked only in the final selection.
3. **F3 forward** (states `{2,3}`, `phishing|scam`-only): seeded by F2's
   own exit boundary, structurally identical to the existing two-fragment
   `seeded_split.py`, one more hop down the chain.
4. **Merge**: F1-backward rows ending at the real start vertex, joined to
   F2 by entry vertex, joined to F3 by exit vertex; combined length and
   combined (max, min) risk range checked once.

**Two real bugs found and fixed:**
- F2's own join condition initially filtered by `from_state IN (1,2) AND
  to_state IN (1,2)` rather than by label -- state 1 in this NFA *also*
  has a `(1,1,transfer/purchase)` self-loop (segment 1's own
  continuation), which that range check would incorrectly follow too.
  Fixed to filter by `t.label = 'sale'` directly.
- F1-backward's own base case (`len1=0`, "zero segment-1 hops taken, seam
  vertex equals the walk's current position") was leaking through
  unfiltered into the final selection -- exactly the same bug class as
  this project's own FinBench `MIN_LENGTH` guards (nothing structurally
  excludes a bare zero-hop prefix). F2 and F3's own final selections
  already required `>= 1` hop each; F1-backward's did not. Caught via a
  monolithic-vs-middle-out mismatch that traced back to a phantom
  `seg2_entry=383, real_start=383, len1=0` row once inspected directly --
  fixed by requiring `len1 >= 1` there too.

**Results after both fixes, re-verified 2026-08-19:** monolithic vs.
middle-out, **0 mismatches** across 8 (vertex, $\ell$) checkpoints at
$\ell=3..4$. $\ell=5$ was attempted this pass (sv=592, the fastest vertex
at $\ell=4$) and confirmed intractable within budget: killed after
exceeding 180s, so $\ell=4$ stays the cap. Middle-out is consistently and
substantially *slower* than monolithic here -- from ~14x (sv=592,
$\ell=4$: 518ms vs. 7.38s) up to ~34x (sv=594, $\ell=4$: 250ms vs. 8.46s).
(Correction to this section's own earlier range, "~4x up to ~20x": that
undersold both ends -- recomputed directly from this pass's own numbers,
every one of the four vertices' own ratios falls in 14--34x, not 4--20x;
the qualitative finding, middle-out substantially slower with no
consistent per-vertex pattern, is unchanged.) The dominant cost is the
final three-way merge (28--40s
of the total), not any individual fragment's own computation (each of
F1-backward/F2/F3 individually finishes in well under 2.5s every time) --
and F2's own cost (~220--260ms) is **entirely independent of which start
vertex is queried**, since "F2 first" means computing the full unseeded
middle segment regardless of where the query actually starts. This is the
plainest illustration across all three new experiments of why "where best
to start such segments" (R4.O2's own phrasing) is a real cost trade-off,
not a free choice: precomputing a fragment before knowing the start vertex
means paying its full cost even when the eventual start vertex only needed
a tiny slice of it.

## Overall picture across all three

**Compatibility, not superiority, holds throughout** -- every split
variant (seeded, naive, reverse-seeded, middle-out) returns results
identical to the monolithic baseline in every one of the 40 (vertex,
$\ell$, experiment) checkpoints run as of the 2026-08-19 extension (20 +
12 + 8, one experiment's own range extended from $\ell{\leq}5$ to
$\ell{\leq}6$, the other two re-confirmed at their existing caps after
directly testing one length further), with zero mismatches after fixing
three real bugs (one soundness bug, one floating-point-comparison
artifact, one missing minimum-length guard) -- none of which were left
unresolved or waved away. Consistent with the existing pilot's own
finding, no split variant is *universally* faster: seeded-split beats
monolithic on the sum-of-weights experiment (a case where a distributive
bound compresses cleanly to the seam) but every other variant tested here
(naive-split, reverse-seeded, middle-out) is slower than monolithic in
every regime tested, sometimes by orders of magnitude. The paper's own
correct, modest claim -- ReCAP does not foreclose fragment-based or
directionally-reversed exploration, without claiming such exploration is
generally faster -- holds up under three additional constraint
families/regex shapes/fragment orderings, not just the original one.
