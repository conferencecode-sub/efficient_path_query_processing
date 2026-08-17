# Why is the split faster than the monolithic baseline?

Both `seeded_split.py` and `naive_split.py` return exactly the same result set
as `baseline_monolithic.py` (verified by `check_equivalence.py`), but run
noticeably faster — e.g. at `--min-length 2 --max-length 3` (all vertices):
baseline 7080ms vs. seeded split 1145ms vs. naive split 513ms. This isn't a
clever query-plan trick; it's one specific, large piece of wasted work that
the monolithic query does and the split fragments don't.

## The culprit: a state-blind length bound

The monolithic query's recursion condition is:

```sql
WHERE p.path_length < {max_length}
  AND t.timestamp_ms > p.last_time
```

This decides whether to keep growing a row *purely from its length* — it
never checks whether the row's current NFA state still has any chance of
reaching the accepting state within the remaining budget. So it treats
"extend a state-1 walk one more hop" and "extend an already-accepted walk one
more hop" identically.

`seeded_split.py` / `naive_split.py`'s F1 (states `{0,1}`) instead bounds
itself one hop tighter:

```sql
WHERE p.path_length < {max_length} - 1
```

reserving room for the mandatory ≥1-hop suffix into state 2. That one-hop
difference turns out to matter enormously, because of how lopsided Q1's NFA
branching is.

## Measured breakdown, `max_length = 3`, all vertices

The monolithic baseline's full internal recursive table (before the final
`WHERE state = 2` filter), grouped by state and depth:

```
state=0, depth=0:          1,320
state=1, depth=1:         72,105
state=1, depth=2:      1,930,315
state=2, depth=2:        173,373
state=1, depth=3:     34,400,163   <-- 86% of all 39,923,297 rows materialized
state=2, depth=3:      3,346,021
```

**`state=1, depth=3` is pure waste.** At `max_length=3`, a walk still in
state 1 after 3 hops (hasn't taken a `phishing`/`scam` edge yet) can never be
accepted — the final filter requires `state=2`, and there's no length budget
left for even one more hop. All 34.4M of those rows get computed, joined, and
materialized, then discarded by the final filter, contributing nothing to the
result.

`transfer|purchase|sale` cover ~92% of edges with a self-loop at state 1
(avg out-degree ≈59.5 per vertex overall), so each hop of state-1 growth
branches roughly 55x. That single avoided hop — F1 stops at `max_length - 1`
instead of `max_length` — is worth more than the entire rest of the
computation combined: 34.4M of 39.9M total rows, i.e. bigger than everything
else the query does put together.

A secondary, much smaller effect: F1's recursive join also filters
`WHERE nf.to_state = 1` inline, so it never even materializes the `to_state=2`
branch alongside the self-loop at any depth (that branch is F2's job,
computed separately, over a much smaller table). This trims a few more
percent off every depth level but is not the main story — the depth-3
dead-end layer above is.

## The general point

Splitting isn't winning by doing the same work more cleverly — it wins
because giving each fragment its own *state-aware* length bound
(`max_length - 1` instead of `max_length`) lets it recognize "this branch is
now doomed" one full hop earlier than the state-blind monolithic query can,
and at this branching factor, that one hop is where almost all the cost
lives. This is a length-budget instance of exactly the "prune doomed paths
early via negatively-stable constraints" idea the ReCAP paper is already
built around (see `../../new_compiler_requirements/compiler_reqs.md`) — the split
recovers it "for free" just by giving each fragment a tighter base case, with
no new machinery required.

## Minimal worked example (3 walks, traced by hand)

The 86%/82% dead-end-layer numbers above are easy to believe but hard to
*see* — they're aggregates over millions of rows. Here is the same
mechanism on a graph small enough to enumerate every walk by hand.

**Toy graph and NFA**, mirroring Q1's shape exactly (a self-looping
"normal" state feeding into a self-looping "accepting" state):

```
graph:  s --a--> m --a--> m (self-loop)     nfa:  0 --a--> 1
                  \--b--> t --b--> t (self-loop)         1 --a--> 1
                                                          1 --b--> 2
                                                          2 --b--> 2
```

Query: all walks from `s` of length **exactly 3**, ending in state 2.

**Every length-3 walk that exists** (there are only three, verified by
running the actual monolithic query from `baseline_monolithic.py` against
this 4-edge graph):

| Walk | Final state | Verdict |
|---|---|---|
| `s -> m -> m -> m` | 1 | **dead end** — zero hops left, can never take `b` |
| `s -> m -> m -> t` | 2 | accepted |
| `s -> m -> t -> t` | 2 | accepted |

**Why the monolithic baseline wastes work on the dead end.** Its
recursion condition is just `path_length < 3` — it has no idea that a
walk sitting in state 1 with zero hops remaining is already hopeless, so
it takes the `a` self-loop a third time, producing `[s,m,m,m]`, and only
discovers it's useless when the final `WHERE state = 2` filter throws it
away at the very end. One wasted walk out of three here (33%); in the
real Q1 data, where the self-loop branches ~55-60x per hop instead of
just once, the same dead-end layer is 82-86% of everything computed (see
above).

**Why the split never generates it.** F1 is bounded by
`path_length < max_length - 1` (here, `< 2`) instead of `< max_length`.
Concretely: F1 produces the boundary row `[s,m]` (len 1), then
`[s,m,m]` (len 2) -- and stops, because `2 < 2` is false. It is not
*filtered out* afterward; `[s,m,m,m]` is structurally impossible for F1
to reach at all, since F1's own recursive step never fires a third time.

F2 then continues from those two boundary rows, and its own recursive
step only allows `to_state != 1` -- it is not offered the option of
repeating the `a` self-loop:

| F1 boundary row | F2 continuation | Result |
|---|---|---|
| `[s,m]` (len 1) | +2 hops via `b`, `b` | `[s,m,t,t]`, state 2 -- accepted |
| `[s,m,m]` (len 2) | +1 hop via `b` | `[s,m,m,t]`, state 2 -- accepted |

Both accepted answers are recovered exactly, and the dead branch was
never even a candidate for F2 to explore, because "take `a` again" isn't
an action its query offers. Verified directly (not just argued) by
running both the toy F1 query and, per boundary row, the toy F2 query in
DuckDB -- output matched this table exactly.

That is the entire trick, at every scale: the monolithic loop cannot tell
a live branch from a doomed one, so it blindly re-explores the doomed one
right up to the length limit; the split's F1 boundary is drawn one hop
short specifically so the doomed extension is never on the table, and F2
is barred from re-taking the self-loop that would recreate it.

## Is this a different query plan?

No -- and that itself is informative. Running `EXPLAIN` (not
`EXPLAIN ANALYZE`, to avoid paying for execution) on the real Q1
monolithic query and on F1's query, both at `max_length = 4`,
`start_vertex = 383`, produces the **same physical operator tree** in
both cases: `REC_CTE` -> `HASH_JOIN` (edges) -> `HASH_JOIN` (nfa_edges)
-> `FILTER` -> `PROJECTION` -> `UNGROUPED_AGGREGATE`. Same join
algorithms, same scan types, no special "state-pruning" operator
invented for the split version.

Two details from the actual `EXPLAIN` output are worth calling out:

1. **DuckDB's own cardinality estimate is identical for both queries:
   ~78,600,001 rows.** The optimizer has no idea F1's version will
   produce dramatically fewer rows than the monolithic one -- it emits
   the same generic guess for "some recursive CTE" regardless of what's
   inside the loop. This is direct evidence that recursive CTEs are
   opaque to DuckDB's cost-based optimizer: it isn't reasoning about the
   loop's behavior, so it cannot discover the tighter bound on its own.
2. **The only differences in the compiled plan are exactly the two
   things written into the SQL text**, nothing planner-derived: the
   `nfa_edges` scan gets a pushed-down `Filters: to_state=1` (5 rows vs.
   10 -- trivially cheap either way, not the real story), and the
   recursion's own termination `FILTER` reads `path_length < 3` for F1
   vs. `path_length < 4` for the monolithic query. That one-hop
   difference in the loop's own stopping condition is the entire
   mechanism; join order, join type, and scan strategy are all
   unchanged.

**Why this has to be hand-written rather than discovered by the
optimizer:** pushing a predicate through a *recursive* fixpoint boundary
would require proving that no future iteration could ever produce a row
satisfying `state = 2` -- exactly the NFA-reachability reasoning ("can
this walk still reach the accepting state within the remaining hop
budget?") that a generic relational engine has no representation for.
`state` is just an opaque integer column to DuckDB; it does not know it
encodes automaton reachability. The only way to get that pruning is to
write it into the recursion's own boundary condition by hand, which is
what splitting into F1/F2 does. There is one plan *shape*; the human
supplies a tighter stopping predicate the optimizer could never have
derived on its own. This matches this project's own `navigation_style_experiment.md`,
which independently asserts "a single `WITH RECURSIVE` is opaque to the
optimizer as one fixpoint computation" -- here confirmed directly from
`EXPLAIN` output rather than just argued.

## Sanity check across multiple start vertices

Re-running the full 3-way comparison (baseline / naive split / seeded
split) at four start vertices spanning the out-degree distribution --
383 (max, out-degree 232), 594 (p75, 87), 592 (median, 58), 635 (p25,
29) -- at $\ell = 2..6$ (see `results/three_way_comparison.csv`, 20
rows) confirms this is not an artifact of one particular start vertex:
every one of the 20 (vertex, length) combinations has
`mono_vs_naive_match = True` and `mono_vs_split_match = True`, with zero
mismatches. The dead-end-layer effect and the resulting speedup both
hold throughout, though the *magnitude* of the speedup and which split
plan wins varies with the vertex's own branching structure (see
`README.md` and the CSV for the per-vertex numbers).

## Reproducing the breakdown

```python
import duckdb
from common import DEFAULT_EDGES, DEFAULT_NODES, load_data

conn = duckdb.connect(':memory:')
load_data(conn, DEFAULT_NODES, DEFAULT_EDGES)

max_length = 3
query = f"""
WITH RECURSIVE paths AS (
    SELECT n.id AS start, n.id AS v, 0 AS state,
           CAST(-99999 AS BIGINT) AS last_time, 0 AS path_length
    FROM nodes n
    UNION ALL
    SELECT p.start, t.dst, nf.to_state, t.timestamp_ms, p.path_length + 1
    FROM paths p
    JOIN edges t ON p.v = t.src
    JOIN nfa_edges nf ON p.state = nf.from_state AND t.label = nf.label
    WHERE p.path_length < {max_length}
      AND t.timestamp_ms > p.last_time
)
SELECT state, path_length, COUNT(*) FROM paths GROUP BY state, path_length ORDER BY path_length, state
"""
for row in conn.execute(query).fetchall():
    print(row)
```
