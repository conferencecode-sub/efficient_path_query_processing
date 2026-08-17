# E6 — End-to-end on LDBC FinBench (2026-08-14)

Per `experiments/new_experiments_checklist/recap_experiments_requirements.md`'s
E6, the last Tier-1 item: "the queries are not synthetic — here are
standardized benchmark queries, and ReCAP lifts their length restriction
without sacrificing early filtering." Real FinBench data, SF0.1 (see
`experiments/datasets/finbench_sf0.1/README.md` for how it was generated —
building the actual Spark-based generator, not a synthetic stand-in), and
the three queries the doc named: TCR1, TCR5, TCR8, fetched directly from
`ldbc/ldbc_finbench_transaction_impls`'s reference Cypher (not
paraphrased/reconstructed from memory).

## The three queries, and why they're the right ones for this paper

All three match the doc's own description exactly: capped at `*1..3` hops,
with a `reduce()`-fold in the reference Cypher that is functionally
identical to ReCAP's own negatively-stable per-hop constraints:

- **TCR1**: `Account -[transfer*1..3]-> Account <-[signIn]- Medium{isBlocked}`,
  ascending timestamps on the transfer chain, time window on every edge.
- **TCR5**: `Person -[own]-> Account -[transfer*1..3]-> Account`, same
  ascending-timestamp constraint (window only on the transfer edges, not `own`).
- **TCR8**: `Loan -[deposit]-> Account -[transfer|withdraw *1..3]-> Account`,
  but the per-hop constraint is a **growth-ratio** check
  (`amount > prev_amount * threshold`), not plain monotonicity — a new
  constraint shape for this project — plus a `GROUP BY dst, SUM(...)`
  aggregation across paths in the reference query's final step, which sits
  outside ReCAP's per-path aggregate model (not implemented here; see
  "Not done" below).

## Direction handling, worked out from the Cypher directly

`deposit` is `Loan -> Account` and `signIn` is `Medium -> Account`, but TCR1
needs to *arrive at* the medium as its last hop (a reverse traversal).
Rather than building general path reversal (explicitly out of scope, per
the E5 write-up's own deferred stretch goal), `common.py` materializes the
one reversed direction TCR1 actually needs as its own forward pseudo-edge,
`signedInBy` — pre-filtered to `medium.isBlocked = true` at construction
time, which is exactly equivalent to the Cypher's own node-label filter
(ReCAP's aggregates only see edge properties, so folding the vertex filter
into which edges exist avoids needing vertex-property support for this one
query).

## Real bugs found and fixed, in code written *this session*

1. **The reference verification query itself had a bug** (`reference_baseline.
   tcr8_reference`): its recursive term set `last_234_amount` unconditionally
   to `e.amount`, even for the deposit hop, so the *first* transfer/withdraw
   edge was incorrectly compared against the deposit's own amount instead of
   being exempt (the reduce-fold's own semantics: no predecessor to compare
   against on the first 234-edge). Caught because ReCAP (779) and the
   reference (319) disagreed at length=2, and manually joining deposit×234
   edges with no threshold at all gave 775 — closer to ReCAP, proving the
   *reference* was wrong, not ReCAP. Fixed with the same `CASE WHEN e.label
   IN ('transfer','withdraw') THEN e.amount ELSE ...` guard already present
   (correctly) in `tcr8_aggregate.py`.
2. **TCR8's ReCAP query over-counted by exactly the deposit out-degree**
   (779 vs. the *corrected* reference's 775 — a 4-row gap, matching the loan's
   4 deposit edges exactly). Root cause: TCR8's NFA accepting state (after
   `deposit`) is the *same* state the `transfer`/`withdraw` self-loop lives
   on, unlike TCR1/TCR5 (whose accepting state is distinct from "just did
   the mandatory prefix"), so nothing structurally excluded a bare deposit
   with zero 234-hops — the exact same bug class as Q3/Q4's own
   `_with_min_length` fix from earlier this session, here triggered by a
   different NFA shape. Fixed with the same wrapper (`WHERE path_length >= 2`).
3. Both `common.py` and `reference_baseline.py` hit the **same BIGINT-anchor
   issue found and fixed in the compiler itself for E7** (small start-vertex
   literal + a graph needing BIGINT-range ids -> DuckDB infers `INTEGER` for
   the recursive CTE's anchor column, then overflows) — except this time in
   hand-written reference SQL, confirming it's a general pitfall for anyone
   writing this kind of query by hand, not specific to the compiler.

All three queries' ReCAP output was verified against an independent
hand-written reference implementation (`reference_baseline.py`) at every
length tested, not just spot-checked.

## Results (full data in `results/tcr{1,5,8}.csv`)

| Query | ℓ=4 (FinBench's own cap) | ℓ=8 ("lifted") | ℓ=8 runtime |
|---|---|---|---|
| TCR1 | 137 | 1,107 (plateaus at ℓ=6) | 26.0ms |
| TCR5 | 3,677 | 10,183 | 27.7ms |
| TCR8 | 20,027 | 200,855 | 186.1ms |

All three stay comfortably fast well past FinBench's own `*1..3` restriction
— the headline the doc asked for ("ReCAP lifts their length restriction
without sacrificing early filtering") holds cleanly for all three queries at
this scale, with no engineering strain (no length-bound tuning, no timeouts,
no memory pressure — peak RSS stayed under ~1.1GB throughout, dominated by
loading the ~76MB dataset).

## Not done

- **No competitor comparison yet.** This round only benchmarked ReCAP itself
  (verified against a hand-written reference, not run head-to-head against
  Neo4j/Kùzu/Memgraph on this dataset). Given System X is explicitly out of
  scope and the other graph DBMS already struggle past ℓ=2-3 on this
  project's other queries, running them here would need the same
  transformation step (`transformation/transform.sh`) this session skipped
  (see `experiments/datasets/finbench_sf0.1/README.md`) plus real setup time
  per engine.
- **TCR8's outer `GROUP BY dst, SUM(...)` aggregation** (the reference
  query's actual final `RETURN` shape) isn't implemented — only the
  per-path filtering (trail, window, growth-ratio) that's the part actually
  relevant to ReCAP's early-filtering story.
- Only SF0.1 (smallest available scale factor) — no scale-up attempted.
