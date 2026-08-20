# Q4 length sweep pilot (2026-08-13)

Same shape as `experiments/q1-q3_length_sweep/`, for Q4 ("max-min trail": a
trail whose max and min edge weight stay within a bound). Same bundled
generic dataset as Q2/Q3, `starter_node=9`, `min_length=2`,
`max_length`/`length_bound` in `{2, 3, 4}`.

## Old-prototype backfill (2026-08-13, later same day)

Q4 originally shipped without an old-prototype comparison: `ReCAP/q4/`
doesn't exist anywhere under `ReCAP_Compiler` -- only `q1`/`q2`/`q3` do,
despite `ReCAP/README.md`'s table listing all four queries. The initial
decision (asked of the user at the time) was to run only ReCAP-new and
Kùzu for Q4.

That turned out to be based on incomplete information: `ReCAP_Compiler/
ReCAP/` is a **copy** of a separate, actively-maintained canonical repo at
`~/ReCAP` (real git history, commits like "fixed starter
nodes") -- and that canonical repo *does* have a real `q4/` with all three
variants. The copy inside `ReCAP_Compiler` was apparently made before `q4/`
was added upstream. Backfilled by copying `~/ReCAP/q4/*.py`
into `ReCAP_Compiler/ReCAP/q4/` and applying the same bug-fixing pass as
Q1-Q3 (see below) -- Q4 now has the full "same crew" comparison too. (That
nested copy has since been relocated out of this repo entirely, to avoid
duplicating the canonical `~/ReCAP` -- this section is historical record of
how `q4/` was originally backfilled, not a currently-valid path.)

`q4_aggregate.py` (the new-compiler side) is built directly from the
bounded-range library aggregate in `new_compiler_requirements/compiler_reqs.md`
(`bounded_range`'s own worked example), combined by hand with the trail
library aggregate's `trail_via_edge_ids` --
same manual-composition pattern as `q3_aggregate.py`, since there's no
"combine two library aggregates" helper in `selective_aggregate.py`. Its
`MAX_MIN_BOUND = 20.0` was picked independently of the old prototype, then
used to fix the old prototype's own broken bound (see below) so all
engines check the same real constraint.

## Engines and results

All engines agree exactly on path counts at every length (202 / 1360 / 7517).

| engine | k=2 runtime / mem | k=3 runtime / mem | k=4 runtime / mem |
|---|---|---|---|
| duckdb-baseline (no early filtering) | 8.2 ms / 159 MB | 18.5 ms / 175 MB | 225.4 ms / 272 MB |
| recap-inline (old prototype) | 6.8 ms / 158 MB | 7.8 ms / 166 MB | 12.5 ms / 175 MB |
| recap-udf (old prototype) | 15.3 ms / 158 MB | 57.9 ms / 172 MB | 289.7 ms / 190 MB |
| recap-new, Standard | 10.3 ms / 10.1 MB* | 13.4 ms / 12.2 MB* | 20.2 ms / 12.8 MB* |
| recap-new, Optimized | 7.3 ms / 10.1 MB* | 9.5 ms / 12.2 MB* | 14.8 ms / 12.8 MB* |
| kuzu | 12.1 ms / 1,564 MB | 55.8 ms / 1,739 MB | 653.2 ms / 2,102 MB |
| neo4j | 292.0 ms / 14,914 MB | 320.1 ms / 14,914 MB | 1,005.6 ms / 14,914 MB |
| memgraph | 1.8 ms / 955 MB | 30.0 ms / 950 MB | 691.4 ms / 941 MB |

\* DuckDB internal buffer memory, not process RSS -- not directly
comparable to the psutil-RSS numbers in other rows (same caveat as
Q1-Q3's READMEs).

**Neo4j and Memgraph both work cleanly at every length here too (added
2026-08-14)**, both had the same stale `2592000/2` bound bug as `kuzu_run.py`
and the old prototype (see below), and both use `Q4_MAX_MIN_BOUND = 20`
after the fix, matching every other engine.

No Kùzu crash here, unlike Q2 -- same as Q3, all lengths 2-4 run cleanly.

## Real bugs found and fixed in the backfilled old prototype

1. **`duckdb_max_min_trail_inline.py`'s baseline was missing its own
   advertised constraint entirely** -- same class of bug as Q3's baseline:
   the final `SELECT` checked only `path_length`/`nfa_state`, no trail
   check, no max-min-weight check. Fixed by adding
   `len(edge_path) = len(list_distinct(edge_path))` and
   `list_max(weights) - list_min(weights) <= 20` to the final filter.
2. **`recap_max_min_trail_inline.py` and `recap_max_min_trail_UDF.py` both
   had `WHERE ... <= 2592000/2`** (2,592,000s = 30 days) comparing against
   max-min **weight** (0-100 range in this dataset) -- a timestamp-scale
   constant on the wrong column's scale, so the constraint always trivially
   held (silently degraded to "trail only"). This is the *original* source
   of the same bug independently found and fixed in `kuzu_run.py`'s never-
   before-run `_q4_full` earlier the same day -- confirms it was a
   genuine, pre-existing bug in the canonical prototype, not something
   introduced by whoever wrote the Kùzu translation. Fixed both
   occurrences to `<= 20`, matching `q4_aggregate.py`'s own bound (the UDF
   file had this exact constant duplicated in two places -- the file
   defines `recap_update_max_min`/`recap_is_valid_max_min`/etc. *twice*,
   and only the second, later definition is actually registered/active in
   Python, but both were fixed for clarity since a stale, wrong-looking
   duplicate sitting right above the real one is its own footgun).
3. **`recap_max_min_trail_UDF.py`'s benchmark method ran a second,
   diagnostic `EXPLAIN ANALYZE {query}` execution after already running
   the query once** -- doesn't corrupt the method's own returned
   `(result, wall_time)` tuple (computed before the diagnostic block), but
   this pilot's outer `bench_common.time_query` wrapper times the whole
   method call, so the extra execution would have silently inflated every
   recorded median for this engine alone. Removed (same class of issue as
   the new compiler's own `intermediate_count_ms` double-execution bug
   found earlier this session, just in someone else's benchmarking code
   instead of ours).
4. All three scripts' hardcoded starter vertices don't fit this 100-node
   dataset (`duckdb_max_min_trail_inline.py`: `starter_node=1`, in range
   but arbitrary; `recap_max_min_trail_inline.py`: a `start_nodes` list of
   huge unrelated ids like `21990232602342`; `recap_max_min_trail_UDF.py`:
   `start_node=76367`) -- this pilot passes `starter_node=9` directly.
5. **`neo4j_run.py` and `memgraph_run.py`'s own `_q4_full` had the exact
   same `2592000/2` bound bug** as `kuzu_run.py` and the old prototype
   (item 2 above) -- confirms it propagated from the same original source
   into every hand-written translation. Fixed both to `Q4_MAX_MIN_BOUND =
   20`, matching everything else.

## Reproduce

```bash
./run_all.sh              # everything below, in sequence
./run_new_compiler.sh     # writes results/new_compiler_q4.csv
./run_old_prototype.sh    # writes results/old_prototype_q4.csv (subprocess per engine)
./run_kuzu.sh [max_len]   # writes results/kuzu_q4.csv; defaults to max_len=4
./run_neo4j.sh [max_len]  # writes results/neo4j_q4.csv; defaults to max_len=4
./run_memgraph.sh [max_len]  # writes results/memgraph_q4.csv; defaults to max_len=4
```

## E1 rerun on real LDBC100 data, with memory -- and a real semantics fix (2026-08-14)

`run_new_compiler.py` rewritten with a custom loader for LDBC100's pipe-
delimited `person_knows_person_0_0.csv` (`experiments/datasets/ldbc100/`;
19,941,198 rows, matching tab:realdata's "19.9M" directly -- unlike
Datagen-7.6/7.7, this file is already the correct directed edge count as
exported, no bidirecting needed). Start vertex is a genuinely **medium**-
degree one (out-degree 18, the median over 389,944 vertices), per the
paper's methodology.

**Also fixed a real query-semantics gap while wiring this up.**
`q4_aggregate.py`'s `MAX_MIN_BOUND=20` max-min-*weight* check was a
stand-in built for the toy 100-node dataset (which has a generic 0-100
`weight` column but no timestamps) -- but figures.tex's actual Q4
(`tab:queries`) is "earliest and latest edge timestamp along the path does
not exceed two weeks," and LDBC's `person_knows_person` has no `weight`
column at all, only `creationDate`. `q4_aggregate()` now takes an optional
`bound` parameter (default unchanged), and this loader aliases
`epoch_ms(creation_date)` as `weight` and passes `bound=1_209_600_000`
(two weeks, in ms) -- same bounded-range shape, now checking what the
paper actually specifies instead of an arbitrary placeholder range.

| ℓ | result | standard runtime | standard rss | optimized runtime | optimized rss |
|---|---|---|---|---|---|
| 2 | 422 | 61.8ms | 4,114MB | 37.8ms | 4,087MB |
| 4 | 38,444 | 121.7ms | 4,114MB | 90.6ms | 4,078MB |
| 6 | 1,818,120 | 472.4ms | 4,173MB | 433.9ms | 4,089MB |
| 7 | 10,704,627 | 2,319.0ms | 4,239MB | 2,146.8ms | 4,228MB |
| 8 | 57,615,956 | 13,520.4ms | 11,052MB | 12,408.7ms | 11,049MB |

Full data in `results/new_compiler_q4.csv`. Reached ℓ=8 (closer to the
paper's own ℓ≤10 range than any other real-data query in this rerun,
since the 2-week cohesion window is far more restrictive than Q2/Q3's
constraints) -- stopped there because ℓ=9 didn't complete in a 590s
budget after ℓ=7->8 alone jumped 5.4x with buffer memory jumping from
3.3GB to 10.3GB. RSS is flat through ℓ=7 (dominated by loading the
19.9M-edge graph, like Q3) and only visibly grows at ℓ=8 once
intermediate cardinality itself gets large enough to matter.
