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
built around (see `../../requirements/compiler_reqs.md`) — the split
recovers it "for free" just by giving each fragment a tighter base case, with
no new machinery required.

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
