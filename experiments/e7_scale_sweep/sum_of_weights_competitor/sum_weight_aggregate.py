"""Selective aggregate for the new "sum-of-weights" scalability sweep over
Datagen-7.7 (`experiments/datasets/datagen7.7/`) -- a trail (no repeated
edge) whose running sum of edge weight must stay `<= bound`.

Combines FR-13(ii)'s `trail_via_edge_ids` shape (no repeated edge) with a
running-sum bound analogous to FR-13(iii)'s `bounded_range`, but summed
rather than max-min. Negatively stable/monotone because every edge weight
in this dataset is strictly positive -- confirmed directly via DuckDB
before writing this file: `SELECT min(weight), max(weight), count(*) FILTER
(weight <= 0 OR weight IS NULL) FROM edges.e` gives range exactly
`[1.0, 1.75342]` with zero non-positive/null values -- so once the running
sum exceeds `bound`, no further extension (which can only add a positive
weight) can bring it back under. Same family as this project's Q4
(max-min-weight bound) and `experiments/alternative_explorations/
navigation_experiment_v2/exp_sum_weights.py`'s own sum-of-amount pilot (a
different, smaller dataset -- not reused here, per this experiment's own
instructions).

Composed by hand (same pattern as `q3_aggregate.py`/`q4_aggregate.py`):
`is_viable_d` checks trail-disjointness AND the running-sum bound inline
(`D.sum_weight + e.weight <= bound`), matching `update_d`'s own computation
of the *next* sum -- every other library aggregate here recomputes the
post-hop value inline in `is_viable_d` rather than referencing a separate
"new D".

**Why per-hop early filtering is provably equivalent to a single final
check here** (real finding, not assumed): since every edge weight is
strictly positive, the running sum is non-decreasing hop by hop, so the
final total is always the *maximum* prefix sum along the path. Checking
`sum <= bound` at every hop (as `is_viable_d` does) therefore accepts
exactly the same trails as checking it once on the completed path's total
weight -- the two are mathematically identical, not just similar. This is
what licenses using a single-final-predicate reference query (a plain
`reduce(...) <= bound` over the whole relationship list, e.g. in a Cypher
competitor query) to cross-check ReCAP's own per-hop-filtered result: any
disagreement would mean a real bug, not a semantics mismatch.

**Pass 1 (superseded, kept for context):** the first version of this
experiment deliberately picked a *low* out-degree start vertex (one with
an extra hop of low-degree "runway" before reaching the graph's giant
component, `START_VERTEX = 6597080337802`) with `SUM_WEIGHT_BOUND = 8.0`,
and pushed the sweep out to length 7 (length 8 was a real, confirmed
structural wall -- every start vertex tried reaches ~97% of the
10,933,040-vertex giant component within 6-7 hops, and any bound loose
enough to satisfy length 8's floor already exploded resident memory past
70-150GB by length 7, regardless of which of 4 start vertices or which of
6 bounds tried). That bound turned out to be essentially non-selective
until length 6 (0.35% cut) and only modestly selective at length 7
(32.5% cut) -- real, but "noise" through most of the swept range.

**Pass 2 (current): a high-fan-out start vertex + a tight bound, so
selectivity is visible from low length instead of only at the tail.**
`START_VERTEX = 6597072984304` is this dataset's own maximum-out-degree
vertex (bidirected out-degree 2084 -- the opposite choice from pass 1),
giving a large, rich unconstrained trail count from length 2 onward
(37,773 at length 2; 7,236,871 at length 3 -- vs. pass 1's 3 and 55).

Picking a bound that is *both* selective at low length *and* survives
several lengths runs into a hard mathematical wall here, checked directly
rather than assumed: a length-L trail's sum is always in
`[L*1.0, L*max_weight]` (every edge weighs >= 1.0), so length 2's own
achievable range is `[2.0, 3.338]` and length 3's is `[3.0, 5.056]` (both
measured directly against this vertex). Any bound `< 3.0` forces length 3
to exactly zero (below its own floor); any bound `>= 3.338` is not
selective at length 2 at all (above its own max). That leaves only the
narrow window `bound in (3.0, 3.338)` for "length 2 shows *any* real cut
and length 3 stays nonzero" -- and even inside that window, length 2's
own trail-weight distribution turns out to be concentrated well below its
max, so the achievable cut at length 2 is thin no matter which value in
that window is picked (measured directly: `bound=3.0` cuts only 2.0% of
length 2's trails, `bound=3.3` cuts only 0.06%). `bound=4.0` (or higher)
would let length 4 stay nonzero too, but is not selective at *either*
length 2 or 3 (its own max already exceeds both their own maxes) --
strictly worse for this experiment's actual goal (visible selectivity by
length 2-3), so it was not chosen despite reaching one length further.

`SUM_WEIGHT_BOUND = 3.0` was picked as the value in that window that
maximizes length 3's own cut (measured: 3.0 -> 30.0% of length 3's trails
survive, i.e. a real 70% cut, vs. 3.1 -> 47.4% survive, 3.2 -> 57.6%
survive, 3.3 -> 64.9% survive -- 3.0 is the strongest real cut available
while length 3 still stays nonzero), at the cost of length 2's own cut
being thin (98.0% of length 2's trails still pass -- a real but modest
2.0% cut, not zero). Length 4 is a hard, confirmed-directly floor at this
bound (every 4-edge trail's sum is >= 4.0 > 3.0, mathematically
unsatisfiable) -- checked empirically too (returns exactly 0, safely, in
just over a second), not merely assumed. The sweep therefore only reaches
length 3 before hitting a real wall -- shorter than pass 1's length 7,
because this pass optimizes for a different, explicitly requested
property (early, visible selectivity) that trades off directly against
sweep length for this dataset's own narrow weight distribution.

**Pass 3 (current): same vertex, bound raised to try to reach length 6 --
found, empirically, to be mutually exclusive with "selective by length
3-4" for this specific vertex, and length 5+ turns out to be a genuine
memory-safety wall, not just a selectivity problem.**

Requiring `bound >= 6.0` (so length 6 isn't forced to zero -- the same
hard per-edge->=1.0 floor as before) directly conflicts with "selective at
length 3-4" *mathematically*, not just as a tuning tradeoff: this
vertex's own achievable-sum ranges, measured directly, are `[2.0, 3.338]`
at length 2 and `[3.0, 5.056]` at length 3 -- both maximums are already
below 6.0, so *any* bound >= 6.0 is automatically 100% non-selective at
lengths 2 and 3 (every achievable trail already satisfies it). Length 4's
range is `[4.0, 6.499]` -- the only length where a bound in `[6.0, 6.499)`
could possibly cut anything -- but its own distribution is measured to be
99.84% saturated by sum=6.0 already (244,627,445 of 245,030,158 total
length-4 trails have sum <= 6.0; only 402,713, 0.16%, sit in the
(6.0, 6.499] sliver that any valid bound could still exclude). So no
choice of bound in the required range shows more than a fractional cut at
length 4 either -- a hard, measured fact about this vertex's own weight
distribution, not a failure to search hard enough for the right value.

Length 5 is worse than merely non-selective: it is a genuine,
directly-confirmed memory-safety wall for this vertex, independent of
which bound is used. A real attempt at length 5 with the tightest bound
in the valid range (`bound=6.0`) drove resident memory to ~392.8GB within
a single ~5-second polling interval before a safety watchdog killed it --
this project's most severe near-incident so far, caught in time. Root
cause quantified directly (not just modeled): of the 244,627,445 length-4
trails, 4,647,201 (1.9%) end back at the start vertex itself -- trail
semantics forbid reusing a specific edge, not revisiting a vertex, and
this hub has 2084 edges, so each of those ~4.65M rows has ~2080 still-
unused edges available to extend through. One more hop therefore expands
into tens of billions of candidate join rows *before* any weight filter
gets a chance to prune -- a property of this vertex's own revisitability
under trail semantics, which would hit any engine computing this query
(ReCAP or Memgraph alike) identically, not an artifact of the ad hoc
DuckDB probe that surfaced it.

Net result: for this exact vertex, `bound=6.0` (the tightest value in the
range required for length-6 feasibility) is used, but the sweep only
safely and honestly reaches **length 4** -- not length 6 -- because length
5 is a confirmed hazard, not merely an untested one.

**Pass 4 (current): completely different search strategy -- look for a
start vertex whose *unconstrained* (no weight bound at all) trail count
itself grows gently, checked directly before picking a bound, rather than
picking a vertex first and hoping a bound could tame it.** Also:
Memgraph is dropped this pass (ReCAP-only, for speed and because every
memory incident so far came from either an unfiltered diagnostic or from
Memgraph's own full-enumeration-before-filtering cost, never from ReCAP's
own inline filtering).

Every vertex tried in passes 1-3 (low out-degree, out-degree 5/8/10/15/20/
30, and the max-out-degree 2084 hub) showed the same qualitative pattern:
growth ratios of roughly 15-190x per hop by length 4-5, because this
dataset's giant component (10,933,040 of ~10.9M vertices with edges --
confirmed directly via full connected-components labeling, iterative
min-label propagation on the whole graph, not sampling) envelops the
overwhelming majority of vertices, reachable from nearly any start point
within 6-7 hops. The *actual* connected-component size distribution
(computed exactly, not estimated) is sharply bimodal: 157,383 components
of size 2-5, 80 of size 6-20, and otherwise nothing until the single
giant component (size 10,615,778) -- there is no "medium" tier to hide a
richer-but-still-gentle vertex in. The true small components (size <= 20)
are all either isolated pairs or tiny stars/paths, too small to give a
meaningful trail count at any length.

The escape hatch: a vertex belonging to the giant component but reached
via an unusually long *pendant chain* (a run of degree-1/degree-2
vertices with no branching) delays entry into the dense, explosive part
of the graph -- exactly the mechanism pass 1 used for one extra hop of
"runway". Systematically searching for this (a deterministic forward walk
from every degree-1 vertex, through the one non-backtracking neighbor at
each degree-2 node, stopping at the first real branch point -- safe by
construction, since it is a single forced path, never a trail
enumeration) over 400,000 sampled degree-1 vertices found only 29 with an
unbranched run of >= 6 hops -- confirming genuinely gentle vertices are
rare, not just hard to find by chance. Of those 29, all but one are
composed entirely of weight-1.0 edges (no selectivity possible on any
bound, sum or max-min alike, since every achievable trail has identical
weight statistics). `START_VERTEX = 2199025319463` is the one exception:
its single outgoing edge weighs 1.49708 (not 1.0), giving real, checked
weight variance to filter on.

Verified directly (not assumed) via the same recursive-CTE probe used in
every prior pass, including the actual per-hop bound applied inline (not
just the unconstrained count): unconstrained trail counts at lengths 2-8
are `2, 2, 3, 3, 4, 4, 3, 3` -- essentially flat, not combinatorial, safe
by construction for any length tested (including 8, two past the
originally-requested 6). Achievable sums cluster in two families per
length depending on how many times the single 1.49708-weight edge is
used (trail semantics permit reusing that *specific vertex*, just not
that literal edge, and there is a distinct reverse edge_id for the return
trip): e.g. at length 6, three of four trails sum to 6.49708 and one
sums to 6.99416.

**A real, considered alternative: switching to Q4-style `max(weight) -
min(weight) <= bound`** (this project's own `bounded_range`/`q4_aggregate`
shape, already used against this exact dataset in
`experiments/e7_scale_sweep/run_q4_datagen77.py`) was checked directly
for this vertex, since it doesn't have sum's hard `bound >= length *
min_weight` floor. It turns out to be *worse* here, not better: every
trail from this vertex necessarily includes the one 1.49708-weight edge
(forced -- it's the vertex's only outgoing edge) and otherwise only
weight-1.0 edges, so `max - min` is exactly `0.49708` for *every single
trail regardless of length or how many times the expensive edge is
reused* -- reusing an edge changes the running *sum* but never changes
which two values are the max/min. Sum-of-weights is the aggregate that
actually has variance to exploit for this specific vertex; max-min would
give a single constant value with no selectivity possible at all. Kept
sum-of-weights for this pass.

Picking `bound`: since the achievable range only has two distinct values
at each *even* length (odd lengths 3/5/7 each have exactly one achievable
sum, no variance to cut), and sum's own floor (`length * 1.0`, offset by
the forced 0.49708 extra from the mandatory first edge) means a bound
loose enough to stay feasible at length 5 (`bound >= 5.49708`) is already
non-selective at lengths 2-5 (their own maxes, up to 5.49708, are all
covered), the only length left where such a bound can still cut anything
is length 6 itself (range `[6.49708, 6.99416]`). `SUM_WEIGHT_BOUND = 6.7`
sits in that window: lengths 2-5 pass 100% (trivially, matching every
prior pass's own low-length behavior), length 6 keeps exactly 3 of the
4 achievable trails (a real, verified 25% cut), and length 7 is a
confirmed, verified-empirically hard floor (`min sum at length 7 =
7.49708 > 6.7`, returns exactly 0). This safely exceeds the relaxed
length-5 target by one length, with the sweep's only real selectivity
concentrated at the last length it reaches -- an honest result given how
little variance this particular (necessarily gentle, necessarily sparse)
vertex offers, not a richer story dressed up to look better than it is.
"""
from __future__ import annotations

from recap_compiler.selective_aggregate import DictionaryKey, SelectiveAggregate

SUM_WEIGHT_BOUND = 6.7


def sum_weight_aggregate(bound: float = SUM_WEIGHT_BOUND) -> SelectiveAggregate:
    return SelectiveAggregate(
        dictionary_keys=(
            DictionaryKey("edge_ids", "BIGINT[]"),
            DictionaryKey("sum_weight", "DOUBLE"),
        ),
        init_d="{edge_ids: [], sum_weight: 0.0}",
        update_d=(
            "{edge_ids: list_append(D.edge_ids, e.edge_id), "
            "sum_weight: D.sum_weight + e.weight}"
        ),
        is_viable_d=(
            "NOT list_contains(D.edge_ids, e.edge_id) "
            f"AND D.sum_weight + e.weight <= {bound}"
        ),
        is_viable_d_final="TRUE",
        finalize_d="D",
        factorized=True,
    )
