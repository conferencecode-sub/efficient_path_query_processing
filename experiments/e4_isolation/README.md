# E4 — Isolating automata exploration from property early-filtering (2026-08-14)

Per `experiments/new_experiments_checklist/recap_experiments_requirements.md`'s
E4: separate the NFA-as-a-join contribution from ReCAP's
property early-filtering. **Q1 only**, decided with the user during planning:
the doc pairs this with Q3, but Q3 has no regex (trivial one-state automaton),
so there's nothing automata-related to isolate there — only Q1 has both a
real regex and per-hop property constraints. Real Metaverse data
(`experiments/datasets/metaverse/`), start vertex 383 (matching E1/E5), all
three configs built via `build_optimized_query` (Stage F) only — the
standard-vs-optimized axis is E1's separate, already-answered question.

## The three configs

1. **regex-only** (`q1_regex_only_aggregate.py`) — NFA + trail semantics
   only, no property constraint at all (`is_viable_d_final=TRUE`).
2. **regex + late property check** (`q1_late_property_aggregate.py`) —
   tracks exactly the same state as ReCAP's real aggregate (`update_d`
   unchanged), but `is_viable_d` only enforces trail; every property
   constraint (monotonic timestamp, region, risk-range, last-risk gateway,
   amount) is deferred to `is_viable_d_final`.
3. **regex + early property filtering** — full ReCAP, i.e. `q1_aggregate.py`
   unchanged (same query E1 already benchmarks as `recap-new-optimized`;
   rerun here so all three configs report `intermediate_paths` uniformly —
   E1's own CSV didn't capture that field).

**Design note on config 2** (worked out, not assumed): most of Q1's
property state is naturally "late-check-safe" for free — `GREATEST`/`LEAST`
are monotonic, so the *final* risk-range is always ≥ any intermediate one,
and `last_norm_score` is already only updated by normal-labelled edges. But
monotonic-timestamp and region-consistency are **not** recoverable from a
single overwritten scalar (a path with timestamps `[10, 5, 20]` has a
perfectly normal-looking final value despite violating monotonicity at hop
2), so config 2 needs two extra sticky boolean flags
(`timestamp_violated`/`region_violated`, OR'd forward) — the same
"collect-then-check-once" technique used in the SQLSolver side-quest's
array-free rewrite of Q3.

## Results (full data in `results/e4_isolation.csv`)

| ℓ | config | final result | intermediate rows | runtime |
|---|---|---|---|---|
| 2 | 1. regex-only | 1,408 | 15,934 | 15.5ms |
| 2 | 2. regex+late property | 23 | 15,934 | 26.7ms |
| 2 | 3. regex+early property (ReCAP) | 23 | 1,270 | 22.9ms |
| 3 | 1. regex-only | 69,500 | 790,814 | 155.3ms |
| 3 | 2. regex+late property | 95 | 790,814 | 267.9ms |
| 3 | 3. regex+early property (ReCAP) | 95 | 3,292 | 36.1ms |
| 4 | 1. regex-only | 3,530,274 | 35,384,382 | 6,486.8ms |
| 4 | 2. regex+late property | 264 | 35,384,382 | 12,297.2ms |
| 4 | 3. regex+early property (ReCAP) | 264 | 6,213 | 53.6ms |

Stopped at ℓ=4: growth is ~50x per hop for configs 1/2 (no early property
pruning), and ℓ=4 already took 6.5-12.3s and 5.3-8.4GB — consistent with why
the paper's own `fig:performance_grid`/`fig:intermediate_total_grid` cap this
same axis at ℓ=4 too.

## A strong, independent correctness validation, found along the way

Config 1's final result count (paths that reach the accepting NFA state,
with no property filter) matches the paper's **own already-published**
`fig:intermediate_total_grid` numbers for the trail-enforced-during-matching
column almost exactly: 1,408 / 69,500 / 3,530,274 at ℓ=2/3/4 (the paper
reports 1,408 / 69,500 / 3,530,274 for Neo4j/Memgraph, and 3,530,598 for the
walk-then-trail-after group — our number matches the *trail-during-matching*
figure exactly, which is the right one, since `is_viable_d` always checks
trail per-hop here, matching Neo4j/Memgraph's own approach rather than
Kùzu/SysX/DuckDB's walk-then-dedupe). This wasn't targeted or curated — it
fell out of building config 1 to answer a completely different (E4) question,
and is strong independent evidence that Stage F's SQL generation is doing
exactly what it should.

**Note on two different "intermediate" numbers, worth not conflating.**
`intermediate_paths` (the CSV column above) is `execution.py`'s own
telemetry: every row in the recursive CTE regardless of NFA state, i.e. every
partial-and-complete prefix explored — a ReCAP-internal execution-cost proxy.
Config 1's *final result* (1,408/69,500/3,530,274, filtered to the accepting
state only) is the one that matches the paper's own "total candidate paths
matching regex R with φ=true" definition. Both are reported since both are
useful, but they answer different questions (raw exploration volume vs.
"how many complete regex matches exist before any property filtering").

## What this isolates, precisely

- **Config 1 vs config 2** (identical `intermediate_paths`, 15,934/790,814/35,384,382
  at each ℓ — confirms both explore the exact same candidate set): the gap is
  entirely the *bookkeeping* cost of tracking Q1's full property state
  (7-9 dictionary keys vs. 1) without yet using it to prune anything — 1.7x
  at ℓ=2, growing to 1.9x by ℓ=4. This isolates the cost of state-tracking
  itself, independent of whether that state is ever used for early pruning.
- **Config 2 vs config 3** (same property semantics, same final answer
  23/95/264 at every ℓ, verified): the gap is the *pure early-filtering
  benefit*, holding automata exploration and state-tracking cost constant —
  1.2x at ℓ=2, widening to **230x by ℓ=4** (12,297ms → 53.6ms). Intermediate
  cardinality collapses by 4-5 orders of magnitude (35.4M → 6,213). This is
  the E4 headline: pushing the *same* property state into per-hop pruning,
  rather than deferring it, is worth up to 230x at just ℓ=4, and the gap is
  visibly still widening.
- **Config 1 vs config 3**: the combined effect of both automata-only
  exploration cost and the full early-filtering benefit — 121x at ℓ=4
  (6,486.8ms → 53.6ms).
