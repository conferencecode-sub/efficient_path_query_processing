# Q3 length sweep pilot (2026-08-13)

Same shape as `experiments/q1_length_sweep/`/`q2_length_sweep/`, for Q3
("monotonic trail": a trail whose edge weights strictly increase along the
path). Same bundled generic dataset as Q2 --
`ReCAP/simple_dataset/nodes.csv`/`edges.csv` -- `starter_node=9`
(highest out-degree, 29, same vertex as Q2 for comparability),
`min_length=2`, `max_length`/`length_bound` in `{2, 3, 4}`.

## Engines and results

All engines agree exactly on path counts at every length (253 / 2054 / 10488).

| engine | k=2 runtime / mem | k=3 runtime / mem | k=4 runtime / mem |
|---|---|---|---|
| duckdb-baseline (no early filtering) | 8.6 ms / 159 MB | 22.2 ms / 170 MB | 286.2 ms / 278 MB |
| recap-inline (old prototype) | 6.0 ms / 158 MB | 7.2 ms / 166 MB | 10.6 ms / 176 MB |
| recap-udf (old prototype) | 15.1 ms / 157 MB | 64.9 ms / 173 MB | 366.7 ms / 194 MB |
| recap-new, Standard | 8.7 ms / 9.6 MB* | 11.8 ms / 11.9 MB* | 18.2 ms / 12.5 MB* |
| recap-new, Optimized | 6.5 ms / 9.6 MB* | 8.8 ms / 11.9 MB* | 14.4 ms / 12.5 MB* |
| kuzu | 9.4 ms / 1,521 MB | 42.2 ms / 1,642 MB | 522.5 ms / 1,936 MB |
| neo4j | 291.8 ms / 14,912 MB | 320.0 ms / 14,912 MB | 879.0 ms / 14,913 MB |
| memgraph | 1.5 ms / 943 MB | 22.7 ms / 938 MB | 494.8 ms / 935 MB |

\* DuckDB internal buffer memory, not process RSS -- not directly
comparable to the psutil-RSS numbers in other rows (same caveat as Q1/Q2's
READMEs).

**Neo4j and Memgraph both work cleanly at every length here too (added
2026-08-14)** -- no crash on any engine for this query. Memgraph is again
the fastest non-DuckDB engine at every length.

`recap-new-optimized` (6.5/8.8/14.4ms) is very close to `recap-inline`'s
(6.0/7.2/10.6ms) -- the smallest gap of the three queries so far, likely
because Q3's aggregate needs no `e.label IN (...)` string check at all
(unlike Q1) and no existence-tracking boolean (unlike Q2) -- just two
straightforward per-hop comparisons.

**Kùzu works cleanly here at every length, unlike Q2** -- no crash. Q3's
Cypher (`kuzu_run.py`'s existing `_q3_full`) uses a `list_reduce`
short-circuit (`CASE WHEN w > acc THEN w ELSE NULL END`) rather than Q2's
`PROPERTIES(RELS(path),'color')` + `RANGE` + `ANY` chain -- apparently
what avoids the engine bug documented in `../q2_length_sweep/README.md`.

## Real correctness bugs found and fixed (2026-08-13)

1. **`duckdb_monotonic_trail_inline.py`'s baseline was missing its own
   advertised constraint entirely** -- the final `SELECT` checked only
   `path_length`/`nfa_state`, with no trail check and no monotonic-weight
   check at all (worse than Q1/Q2's baselines, which did check both, just
   only at the end). This wasn't "no early filtering" -- it was
   structurally incapable of returning a correct count. Fixed by adding
   the missing `len(edge_ids) = len(list_distinct(edge_ids))` and a
   list-comprehension strictly-increasing check on `weights`, matching
   Q1's own baseline idiom for the same kind of check.
2. **`recap_monotonic_trail_UDF.py`'s `return` was commented out**, same
   as every Q1/Q2 old-prototype script -- uncommented.
   (`recap_monotonic_trail_inline.py` already had a working, if
   redundant, `return` -- no fix needed there.)
3. **All three scripts' hardcoded starter vertices are out of range or
   from a different dataset** (`duckdb_monotonic_trail_inline.py`:
   `starter_node=1`, in range but arbitrary; `recap_monotonic_trail_
   inline.py`: a `start_nodes` list of Reddit-dataset ids like `14485`;
   `recap_monotonic_trail_UDF.py`: `start_node=320104`) -- none
   exist in this 100-node dataset. This pilot passes `starter_node=9`
   directly instead of using each script's own `main()`.

## New-compiler-specific finding: an explicit min_length filter is required here

Unlike Q1 (NFA structure) and Q2 (`constraint_done`'s own semantics), Q3's
aggregate has `is_viable_d_final = TRUE` (both constraints are fully
enforced by `is_viable_d` already) and a trivial one-state automaton that's
both start and accepting. Nothing structurally excludes the 0-edge anchor
or a 1-edge extension -- neither the trail nor the strictly-increasing
check can fail with 0 or 1 edges. First run over-counted by exactly 30 at
every length (1 zero-edge "path" + 29 one-edge paths, one per out-edge of
vertex 9). Fixed in `run_new_compiler.py` via `_with_min_length`, which
wraps `query.sql` in an outer `WHERE path_length >= 2` before timing/
counting (a plain `SimpleNamespace(sql=..., cte=...)` works with
`run_query` since it only ever reads those two attributes). Confirmed via
FR-22 and by matching every other engine's count exactly after the fix.

## Reproduce

```bash
./run_all.sh              # everything below, in sequence
./run_new_compiler.sh     # writes results/new_compiler_q3.csv
./run_old_prototype.sh    # writes results/old_prototype_q3.csv (subprocess per engine)
./run_kuzu.sh [max_len]   # writes results/kuzu_q3.csv; defaults to max_len=4 (no crash here)
./run_neo4j.sh [max_len]  # writes results/neo4j_q3.csv; defaults to max_len=4
./run_memgraph.sh [max_len]  # writes results/memgraph_q3.csv; defaults to max_len=4
```

## E1 rerun on real Datagen-7.6 data, with memory (2026-08-14)

`run_new_compiler.py` rewritten with a **new custom loader**, since
Datagen-7.6 (`experiments/datasets/datagen7.6/`) isn't in the
`nodes.csv`/`edges.csv` convention -- it's LDBC Graphalytics format
(`.e`/`.v`, space-delimited `src dst weight`, no header). The graph is
undirected (`dataset.properties`: `directed = false`) but stored as one
line per edge, so `_load_bidirected` unions the raw file with its own
reversal before handing it to `load_graph` -- confirmed this reproduces
the paper's own directed-edge count exactly: 42,162,988 raw lines ->
84,325,976 after bidirecting, matching tab:realdata's "84.3M" precisely.
Start vertex switched to a genuinely **low**-degree one (out-degree 22,
the q25 point over 754,147 vertices) per the paper's own methodology (low
for Q3, medium for Q2/Q4, high only for Q1) -- same subprocess-isolated
memory approach as Q1/Q2.

| ℓ | result | standard runtime | standard rss | optimized runtime | optimized rss |
|---|---|---|---|---|---|
| 2 | 6,435 | 57.6ms | 19,863MB | 52.4ms | 19,344MB |
| 3 | 374,725 | 427.1ms | 18,047MB | 339.2ms | 19,533MB |
| 4 | 18,744,888 | 3,002.5ms | 17,875MB | 2,517.2ms | 17,608MB |

Full data in `results/new_compiler_q3.csv`. Stopped at ℓ=4: growth per hop
is already ~55-58x despite the strictly-increasing-weight constraint being
fully early-filtered (`is_viable_d`, negatively stable) -- at that rate
ℓ=5 would likely be close to a billion intermediate rows, well beyond what
seemed worth spending on a single pilot point. **RSS here (~18-20GB) is
overwhelmingly the cost of loading and bidirecting the full 84.3M-edge
graph into DuckDB, not query state** -- it's nearly identical across ℓ and
across Standard/Optimized, unlike Q1/Q2 where RSS visibly tracks the
query's own growth. This is expected and correct (not a red flag): the
paper's own protocol measures on this same server-scale hardware (768GB
RAM), and both variants pay the identical loading cost, so it doesn't bias
the standard-vs-optimized comparison either way.
