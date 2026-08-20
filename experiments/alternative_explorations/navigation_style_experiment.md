# Experiment: Is ReCAP compatible with fragment-based / non-forward exploration?

**Status:** design doc, not yet implemented. Written to turn an earlier
informal brainstorming note into something runnable and falsifiable.

## 1. The design question this answers

A real limitation worth addressing directly: ReCAP's incremental checks, as
defined, only work if paths are grown incrementally from the start of the
regex to the accepting state. Richer path-exploration strategies exist in
the RPQ literature that grow intermediate paths in segments, using data
statistics for cost-based optimization to decide where best to start such
segments. Could ReCAP's approach be integrated with such strategies, or does
its incremental-check design foreclose that?

The response to this concern already asserts compatibility and sketches a
three-way benchmark (forward ReCAP vs. hand-split-and-join vs. a
WAVEGUIDE-style split-and-possibly-reverse planner), and `compiler_reqs.md`
encodes the same position structurally: the compiler keeps the NFA
non-deterministic by default "because preserving the NFA is what keeps
ReCAP compatible with wavefront-style planners," and Section 12 non-goal #3
explicitly declines to *implement* such a planner — compatibility is
claimed, implementation is not. This experiment is the evidence for that
compatibility claim.

## 2. What we're actually claiming (and what we're not)

The earlier brainstorming note talks itself from an overreach into the
right-sized claim; that arc is the spec for this experiment, so it's worth
keeping explicit:

- **Claim A (rejected as the target):** "splitting a regex into fragments and
  merging always recovers the monolithic result, under stated general
  conditions." Nobody in the splitting literature (WAVEGUIDE included) proves
  this in general either — it isn't the bar this concern sets, and we won't
  hold ourselves to it.
- **Claim B (the actual target):** ReCAP does not foreclose fragment-based
  exploration. For a concrete split of a concrete query, a fragment traversed
  in either direction is itself a ReCAP (same five-function signature), and
  fragments compose through one extra function. We demonstrate this by
  construction on one query, check the composed result against the
  monolithic one, and measure whether seam-level filtering actually prunes.

We are **not** claiming splitting is generally *better* than forward
exploration — that's a planning question orthogonal to ReCAP, and overclaiming
it is exactly the kind of thing that invites a harder follow-up review.

## 3. Concrete instance: split Q1 at its rare label

This reuses what already exists in `ReCAP/q1/` and `ReCAP/simple_dataset/LG.csv`
(the Metaverse-style dataset, 1.3k vertices / 78.6k edges — matches Table 2 of
the paper) rather than inventing a new query or dataset.

**Regex (from `ReCAP/q1/recap_gen_recap_inline.py`):**
`(transfer | purchase | sale)+ (phishing | scam)+`, NFA states `{0,1,2}`,
`q0=0`, `Q_F={2}`, transitions:

```
(0,1,transfer) (0,1,purchase) (0,1,sale)
(1,1,transfer) (1,1,purchase) (1,1,sale)
(1,2,phishing) (1,2,scam)          <-- the rare-label seam
(2,2,phishing) (2,2,scam)
```

**Label distribution in `LG.csv`** (checked directly):

| label | count |
|---|---|
| sale | 25,040 |
| purchase | 24,940 |
| transfer | 22,125 |
| scam | 3,949 |
| phishing | 2,546 |

`transfer|purchase|sale` account for ~92% of edges, `phishing|scam` for ~8%.
This is exactly the "split at the rare interior label" setup WAVEGUIDE-style
planners target, and it's real in this dataset, not constructed for effect.

**Selective aggregate (unchanged from the existing query):** `max_risk,
min_risk, last_risk, last_time, region, amount, edge_ids`. Per the existing
inline SQL, monotonic timestamp and same-region constraints apply across the
*whole* path (not gated by NFA state); the risk-range bound applies only on
`0→1`/`1→1` transitions; `last_risk ≥ 40` is checked exactly once, on the
`1→2` transition; `amount ≥ 1000` and trail semantics (`edge_ids` disjoint)
apply globally.

### Fragment F1 — states `{0,1}`, forward from `s`

Identical to the existing recursive CTE, restricted to `n.to_state = 1`. Its
output rows *at* state 1, for every prefix length, are the seam candidates —
nothing is discarded, no summary compression happens here:

```sql
WITH RECURSIVE frag1 AS (
    SELECT {s} AS v, 0 AS state,
           NULL::DOUBLE AS max_risk, NULL::DOUBLE AS min_risk,
           NULL::VARCHAR AS region, CAST(-99999 AS BIGINT) AS last_time,
           0 AS amount, CAST([] AS BIGINT[]) AS edge_ids, 0 AS path_length
    UNION ALL
    SELECT t.dst, n.to_state,
           GREATEST(p.max_risk, t.risk_score), LEAST(p.min_risk, t.risk_score),
           t.location_region, t.timestamp_ms,
           p.amount + t.amount, list_append(p.edge_ids, t.edge_id), p.path_length + 1
    FROM frag1 p
    JOIN edges t ON p.v = t.src
    JOIN nfa_edges n ON p.state = n.from_state AND t.label = n.label
    WHERE n.to_state = 1
      AND p.path_length < {max_prefix_length}
      AND (t.location_region = p.region OR p.region IS NULL)
      AND t.timestamp_ms > p.last_time
      AND (GREATEST(p.max_risk, t.risk_score) - LEAST(p.min_risk, t.risk_score) <= 20
           OR p.max_risk IS NULL)
      AND NOT list_contains(p.edge_ids, t.edge_id)
)
SELECT * FROM frag1 WHERE state = 1   -- seam rows
```

### Fragment F2 — states `{1,2}`, seeded continuation from each seam row

This is the "merge function" from the conversation, but in its cheapest
honest form: because nothing was discarded at the seam (F1's boundary row
carries the *exact* `region`, `last_time`, `amount`, `edge_ids` — not a
lossy summary), F2 doesn't need to reconcile two independently-derived
summaries. It just needs an anchor that is *seeded* by F1's terminal row
instead of `init_d()`:

```sql
WITH RECURSIVE frag2 AS (
    SELECT b.v AS v, b.state AS state, b.region AS region, b.last_time AS last_time,
           b.amount AS amount, b.edge_ids AS edge_ids, 0 AS suffix_length
    FROM frag1_boundary b   -- = "SELECT * FROM frag1 WHERE state = 1"
    UNION ALL
    SELECT t.dst, n.to_state, p.region, t.timestamp_ms,
           p.amount + t.amount, list_append(p.edge_ids, t.edge_id), p.suffix_length + 1
    FROM frag2 p
    JOIN edges t ON p.v = t.src
    JOIN nfa_edges n ON p.state = n.from_state AND t.label = n.label
    WHERE p.suffix_length < {max_suffix_length}
      AND (t.location_region = p.region OR p.region IS NULL)
      AND t.timestamp_ms > p.last_time
      AND NOT list_contains(p.edge_ids, t.edge_id)
      AND (p.state <> 1 OR n.to_state <> 2 OR t.risk_score >= 40)   -- the seam check
)
SELECT v, amount, edge_ids FROM frag2
WHERE state = 2 AND amount >= 1000
```

**Why correctness here is nearly free (and why that's worth saying out loud):**
this is not a general "two summaries merge into one" theorem — it's the same
`update_d`/`is_viable_d` definitions, called by two smaller recursive CTEs
glued by a seed instead of one large recursive CTE. By induction on path
length, the F1-then-F2 composed dictionary equals the monolithic dictionary
at every step, because no information is compressed at the seam. That's a
weaker, cheaper case than the general merge theorem the LLM conversation
worried about (point 15 in the transcript) — worth stating in the writeup so
it isn't mistaken for a bigger proof obligation than it is.

### The three plans to benchmark

| Plan | What it does | Status |
|---|---|---|
| (i) monolithic | the existing `recap_gen_recap_inline.py` query, unmodified | already implemented |
| (ii) naive split | F1 boundary rows × an *unseeded* independent `(phishing\|scam)+` search from every distinct seam vertex, ignoring region/time/risk during exploration, filtering only at the end via a final join | new, small script |
| (iii) seeded split | F1 → F2 as above, seam-checked inline during F2's own recursion | new, small script |

Plan (ii) exists specifically to isolate the payoff of seam-level filtering:
it's what you get if you mechanically cut the regex without threading state
through, and it should materialize a much larger intermediate `frag2` before
the final filter than plan (iii) does inline.

**DuckDB won't do this for you.** A single `WITH RECURSIVE` is opaque to the
optimizer as one fixpoint computation; splitting it into two smaller
recursive CTEs glued by a seed is a genuinely different query plan, not
something to expect DuckDB to discover on its own. Worth confirming with
`EXPLAIN ANALYZE` on plan (i) as a sanity check, not an assumption.

### Success criteria

1. **Result equality.** Plans (i), (ii), (iii) return identical path counts
   (and ideally identical `edge_ids` sets) for every `(s, ℓ)` tested. For
   (i) vs (iii) this should hold *by construction* (see correctness note
   above) — if it doesn't, that's an implementation bug, not a surprising
   research finding. (ii) vs (iii) matching is the sanity check that the
   seam filter in (iii) drops/keeps exactly the same rows as filtering at the
   end.
2. **Seam pruning is real.** Intermediate row counts for `frag2` in plan
   (iii) should be materially smaller than plan (ii)'s unseeded
   `frag2`, at every suffix length.
3. **Compatibility, not superiority.** Report whether (iii) matches, beats,
   or loses to (i) on runtime — honestly, in both directions. The paper-worth
   sentence is "the abstraction doesn't foreclose this plan," not "this plan
   is always better." If (iii) beats (i) specifically when the interior label
   is rare and start vertices are high-degree, that's the strongest version
   of the claim available without overreaching.

## 4. Stretch phase: genuine directional reversal (optional, do not let it delay Phase 1)

Phase 1 above never actually reverses direction — both fragments walk
forward. It answers "does ReCAP foreclose segmentation," but not the sharper
half of the original concern (whether the matching direction itself could be
reversed). That needs a query with a **fixed end vertex** (Section 2 of the paper
explicitly lists "start vertex and end vertex given" as a supported variant,
but none of `q1/q2/q3` implement it), plus a genuinely mirrored aggregate.

Sketch, using a point-to-point variant of Q3 (monotonic trail, `s` to `t`):

- **Forward fragment**, from `s`: maintains `max_time` seen so far (running
  max of a strictly-increasing sequence is just its last value), forward
  `edge_ids`.
- **Backward fragment**, from `t`, over the *reverse* graph (`dst` as `src`):
  maintains `min_time` seen so far walking backward — this is the mirrored
  `is_viable_d`, flipping `>` to `<`, per Example 7's family (adjacent-edge
  predicates mirror mechanically; this is the case worth checking
  systematically).
- **Merge**, at a candidate meeting vertex `m`: `fwd.max_time < bwd.min_time`,
  plus `edge_ids` disjointness-then-union (trail semantics needs the *actual*
  sets on both sides for this — it cannot be compressed into a bounded
  summary the way the timestamp check can; not every constraint splits into
  bounded boundary state, and it's worth being honest about that).

This is a heavier build (new point-to-point query variant, a reversed-edge
table, a real two-sided merge with a stated correctness condition rather than
a free inductive argument) and should stay a stretch item — a
demonstration-plus-one-experiment answers this concern fully; don't let this
balloon into a second contribution.

## 5. Suggested layout, when ready to implement

```
alternative_explorations/
├── navigation_style_experiment.md          (this file)
└── navigation_experiment/
    ├── monolithic_q1.py        # thin wrapper around the existing q1 script
    ├── naive_split_q1.py       # plan (ii)
    ├── seeded_split_q1.py      # plan (iii)
    ├── check_equivalence.py    # runs all three, diffs result sets
    └── results/
```

Phase 2 (if pursued) would get its own `reversed_q3/` sibling directory with
its own reversed-edges table and merge script, kept separate so Phase 1's
results aren't blocked on it.

## 6. Extensions (2026-08-18): three more constraint families/regex shapes/orderings

`navigation_experiment_v2/` (see its own `README.md` for full results)
extends Phase 1 along axes the original pilot didn't cover: a distributive
constraint family (sum of weights) instead of a monotone one, a regex with
the rare label as the *prefix* instead of the suffix, a genuine
three-fragment/two-seam split, and -- per an explicit ask mid-build -- the
paper's own deferred "genuine directional reversal" stretch goal (section 4
above), actually built this time: precompute the middle fragment first,
walk the first fragment *backward* over a reversed edge relation, walk the
last fragment forward as usual, then merge all three.

All three new experiments confirm the same compatibility (not superiority)
claim Phase 1 already established, with zero mismatches after finding and
fixing three real bugs along the way (an unsound recursive-term lookahead
prune, a floating-point non-associativity artifact in the equivalence
check, and a missing minimum-hop guard on a backward-seeded fragment's base
case -- the same bug class as this project's own FinBench `MIN_LENGTH`
guards). One new, sharp finding: naive-split's cost is dominated by
whichever segment is *not* rare, so mirroring which segment is rare changes
naive-split's tractability by orders of magnitude (72K rows at $\ell=2$ to
210M at $\ell=4$) even though monolithic and seeded-split are unaffected.
