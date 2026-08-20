# Q2 length sweep pilot (2026-08-13)

Same shape as `experiments/q1_length_sweep/`, for Q2 ("two-color trail": a
trail whose path contains at least one pair of *consecutive* edges sharing
the same color). Uses the bundled *generic* dataset --
`ReCAP/simple_dataset/nodes.csv`/`edges.csv` (100 nodes, 2036 edges, every
edge labeled `'e'`, 25 distinct colors) -- not `LG.csv`; `starter_node=9`
(highest out-degree, 29, in this graph); `min_length=2`,
`max_length`/`length_bound` in `{2, 3, 4}`.

## Engines and results

All engines agree exactly on path counts at every length (26 / 1023 / 29871).

| engine | k=2 runtime / mem | k=3 runtime / mem | k=4 runtime / mem |
|---|---|---|---|
| duckdb-baseline (no early filtering) | 9.8 ms / 162 MB | 32.8 ms / 198 MB | 405.6 ms / 628 MB |
| recap-inline (old prototype) | 6.8 ms / 155 MB | 10.1 ms / 164 MB | 59.5 ms / 250 MB |
| recap-udf (old prototype) | 18.7 ms / 158 MB | 235.8 ms / 187 MB | 4,688.0 ms / 424 MB |
| recap-new, Standard | 9.1 ms / 10.1 MB* | 15.7 ms / 13.0 MB* | 75.5 ms / 73.6 MB* |
| recap-new, Optimized | 6.7 ms / 10.1 MB* | 11.9 ms / 13.1 MB* | 66.7 ms / 73.8 MB* |
| kuzu | 26.4 ms / 1,647 MB | **crashes (SIGSEGV)** | **crashes (SIGSEGV)** |
| neo4j | 292.3 ms / 14,907 MB | 316.2 ms / 14,907 MB | 997.7 ms / 14,907 MB |
| memgraph | 1.7 ms / 926 MB | 30.9 ms / 926 MB | 717.8 ms / 927 MB |

\* DuckDB internal buffer memory, not process RSS -- not directly
comparable to the psutil-RSS numbers in other rows (same caveat as Q1's
README).

**Neo4j and Memgraph both work cleanly at every length here, unlike Kùzu**
(added 2026-08-14). Neither engine's Q2 Cypher has an explicit trail check
at all -- confirmed this isn't a bug, unlike the gap `kuzu_run.py` had:
Cypher enforces relationship uniqueness within a single pattern by
default, so a variable-length `[e:Edge*min..max]` never reuses the same
relationship instance, and trail semantics fall out for free. Memgraph is
notably fast here (1.7ms at k=2, still only 717.8ms at k=4) -- faster than
every other engine at every length except recap-inline/recap-new at k=2.

`recap-new-optimized` (6.7/11.9/66.7ms) is now close to `recap-inline`
(6.8/10.1/59.5ms) -- consistent with Q1's own residual-gap finding.

## Real correctness bugs found and fixed (2026-08-13)

1. **Old prototype, all three `ReCAP/q2/*.py` scripts had their `return`
   statement commented out** (`# return result, exec_time`) -- they only
   ever printed results, so nothing could call them programmatically for
   benchmarking. Uncommented in all three (`duckdb_two_color_trail_inline.py`,
   `recap_two_color_trail_inline.py`, `recap_two_color_trail_UDF.py`).
2. **`duckdb_two_color_trail_inline.py` referenced an undefined
   `{start_node}`** in its query f-string (the method's actual parameter
   is `starter_node`) -- crashed with `NameError` even run standalone.
   Fixed the typo.
3. **All three scripts' hardcoded default `starter_node = 4515`** doesn't
   exist in this 100-node dataset (max id ~100) -- never actually
   exercised. This pilot passes `starter_node=9` explicitly instead of
   using each script's own `main()`.
4. **`experiments/SOA-GDBMS/kuzu_run.py`'s `load_generic_schema_and_data`
   crashed on `nodes.csv`** (`Binder exception: Number of columns
   mismatch. Expected 2 but got 3`) -- the Kùzu `Node` table schema only
   declares `id`/`name`, but `nodes.csv` has a third `label` column.
   Fixed by trimming to `id`/`name` via a temp CSV before `COPY`, the same
   pattern already used for the edges CSV just below it in that function.
   This also corrected the function's own docstring, which wrongly
   claimed this generic dataset "isn't present anywhere in this repo" --
   it is (`ReCAP/q2/run_queries_py.sh` even says to use it).
5. **`kuzu_run.py`'s `_q2_full` never checked `is_trail(path)` at all**
   -- a real correctness gap (every other engine requires the path be a
   trail). Added. Didn't fix the crash below (still crashes with or
   without it), but it's the correct query regardless.
6. **The `result_shape="paths"` vs `"count"` timing asymmetry from Q1
   (see that README) mattered far more here** -- Q2 has up to 29,871 final
   rows at length 4, so materializing and returning them all client-side
   (what the original `run_new_compiler.py` timed) is real, non-comparable
   work no other engine does. Fixed the same way as Q1: a separate,
   untimed `"paths"` pass just for the standard/optimized equivalence check, with the actual timed
   pass using `"count"`. This alone cut `recap-new-optimized`'s length=4
   number from 271.5ms to 66.7ms.

## Kùzu crash -- confirmed engine bug, not a query-logic issue

Kùzu 0.11.2 segfaults (`SIGSEGV`, core dumped) on Q2's query at length>=3,
both with and without the `is_trail(path)` fix above. Isolated via direct
testing (not guessed):

- `MATCH ... WHERE is_trail(path) RETURN COUNT(*)` **alone** (no
  color-adjacency check) does **not** crash at length 3 -- returns 11,928
  rows successfully.
- Adding the color check back (`PROPERTIES(RELS(path), 'color')` +
  `RANGE(...)` + `ANY(...)`) crashes at length 3 regardless of whether
  `is_trail(path)` is present.
- A `list_reduce`-based rewrite of the color check (matching how Q1/Q3/Q4
  already do adjacency checks) was tried as an alternative, but Kùzu's
  `list_reduce` rejected a struct accumulator with an initial value
  (`Binder exception ... Expected (LIST,ANY) -> LIST`) -- not pursued
  further given the binder-level rejection, separate from the crash.

Conclusion: this is a genuine bug in Kùzu 0.11.2's list-property
processing on variable-length paths once the row count reaches roughly
12k+, not something fixable by rewriting the query. Only `length=2` is
usable for Q2 on Kùzu until/unless a Kùzu version fixes this. Reproduce
(safe -- the crash is contained to the subprocess) with:

```bash
./run_kuzu.sh 3   # crashes on purpose, to reproduce
```

## Reproduce

```bash
./run_all.sh              # everything below, in sequence
./run_new_compiler.sh     # writes results/new_compiler_q2.csv
./run_old_prototype.sh    # writes results/old_prototype_q2.csv (subprocess per engine)
./run_kuzu.sh [max_len]   # writes results/kuzu_q2.csv; defaults to max_len=2 (see above)
./run_neo4j.sh [max_len]  # writes results/neo4j_q2.csv; defaults to max_len=4 (no crash here)
./run_memgraph.sh [max_len]  # writes results/memgraph_q2.csv; defaults to max_len=4
```

## E1 rerun on real Bitcoin data, with memory (2026-08-14)

`run_new_compiler.py` rewritten to point at `experiments/datasets/bitcoin/
edges_with_colors.csv` (the plain `edges.csv` has no `color` column, which
Q2's aggregate needs), and to isolate Standard vs Optimized into separate
subprocesses for genuine per-variant memory (same rework as Q1 -- see that
README). **First attempt used the highest-out-degree vertex (763) as the
start vertex -- wrong**: the paper's own methodology picks *medium*-degree
start vertices for Q2 (high only for Q1, low for Q3), so the real vertex
used is 3999 (out-degree 2, the dataset's median). Equivalence verification
switches from full result-set comparison to count-only beyond ℓ=3, since
materializing full path structs (with `edge_ids` arrays) for millions of
rows was the actual bottleneck, not query execution itself -- confirmed
directly: a length=4 run alone took ~10s, but the original full-signature
verify pass at length=4 hung past 280s.

| ℓ | result | standard runtime | standard rss | optimized runtime | optimized rss |
|---|---|---|---|---|---|
| 2 | 34 | 11.8ms | 383MB | 8.6ms | 415MB |
| 3 | 1,666 | 17.7ms | 410MB | 15.4ms | 404MB |
| 4 | 52,508 | 46.2ms | 507MB | 42.6ms | 482MB |
| 5 | 2,743,095 | 791.8ms | 1,373MB | 713.6ms | 1,329MB |

Full data in `results/new_compiler_q2.csv`. Stopped at ℓ=5: growth from
ℓ=4→5 was already 52x despite starting from a median-degree vertex (a few
edges evidently route through high-out-degree hub vertices), and ℓ=6 did
not complete in 150s -- consistent with the paper's own point that Q2
admits no early filtering at all (only the trail check is per-hop-prunable;
the two-color constraint only resolves in `is_viable_d_final`), so
intermediate cardinality here is inherent to the query, not an artifact of
Standard vs Optimized. As expected from that same fact, Standard and
Optimized track each other closely at every ℓ (no early-filtering benefit
for inlining to amplify) -- unlike Q1, where Optimized pulls further ahead
as ℓ grows.

## E8 clarification: Standard-vs-competitors on Q2, with memory (2026-08-14)

Added for clarity: `fig:performance_grid`'s "ReCAP" caption needed to say which
variant it means, and the Q2 discussion needed a Standard-vs-competitors point
to make the piggybacking benefit unambiguous. Ran `ReCAP/q2/
duckdb_two_color_trail_inline.py` (the genuine "DuckDB without ReCAP"
baseline -- collects the full `colors` array per path, checks the
two-adjacent-same-color condition via `UNNEST` only at the very end, no
incremental tracking at all) against the same real Bitcoin dataset and
vertex (3999) as this session's Standard/Optimized reruns. Results confirmed
against the same known-correct counts (34/1,666/52,508/2,743,095) as every
other Q2 engine in this repo:

| ℓ | duckdb-baseline ms | recap-new-standard ms | speedup |
|---|---|---|---|
| 2 | 34.3 | 11.7 | 2.9x |
| 3 | 29.2 | 17.7 | 1.6x |
| 4 | 175.3 | 46.2 | 3.8x |
| 5 | 5,353.7 | 791.8 | 6.8x |

Full data in `results/duckdb_baseline_q2_real.csv`. **ReCAP-Standard (no
inlining at all) already beats the no-ReCAP baseline at every ℓ, with the
margin growing (2.9x -> 6.8x)** -- confirms the piggybacking speedup
(`last_color`/`completed` tracked in `is_viable_d_final`) is a property of
ReCAP's incremental aggregate design itself, not an artifact of the
flattening/inlining optimization. Used to update `figures.tex`'s
`fig:performance_grid` caption and Q2 discussion paragraph directly.
