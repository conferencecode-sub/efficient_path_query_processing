# Q1 length sweep pilot (2026-08-13)

A small pilot run of Q1 (paper's regex + selective-aggregate query) across
every engine currently runnable in this repo, over the bundled
`ReCAP/simple_dataset/LG.csv`/`LG_V.csv`, `starter_node=383`,
`min_length=2`, `max_length`/`length_bound` in `{2, 3, 4}`. Not one of the
tiered E-experiments in `experiments/new_experiments_checklist/
recap_experiments_requirements.md` -- a scoped-down smoke test to validate
the comparison harness itself before scaling to the real E2/E7 protocol
(bigger graphs, `ℓ ∈ {2..10}`, Neo4j/Memgraph via Docker).

## Engines

| Script | Engine(s) | Memory metric |
|---|---|---|
| `run_new_compiler.py` | `compiler/`'s Standard (Stage E) + Optimized (Stage F) | DuckDB `system_peak_buffer_memory` (internal buffer-manager stat, **not** process RSS) |
| `run_old_prototype.py` | `ReCAP/q1`'s DuckDB baseline, ReCAP-inline, ReCAP-UDF | this process's peak RSS (`psutil`), isolated per engine via one subprocess each |
| `run_kuzu.sh` | Kùzu (`experiments/SOA-GDBMS/kuzu_run.py --query q1`) | this process's peak RSS (`psutil`) |
| `run_neo4j.sh` | Neo4j (local install, `~/neo4j/neo4j-community-5.24.0`) | the Neo4j JVM process's own RSS (`psutil`, matched by process name `java`) |
| `run_memgraph.sh` | Memgraph (docker container `memgraph`, bolt on host port **7688**, not 7687) | the container's own memory (`docker stats`) |

**The two memory metrics are not directly comparable units** -- DuckDB's
internal buffer accounting excludes Python/pandas/connection overhead that
process RSS includes, so the new-compiler numbers will always look smaller
than a same-magnitude process-RSS number for reasons that have nothing to
do with which engine is actually more efficient.

## Results

All engines that completed agree exactly on path counts at every length
(23 / 95 / 264) -- a real cross-engine correctness check, not just a
runtime comparison.

| engine | k=2 result | k=2 runtime | k=2 mem | k=3 result | k=3 runtime | k=3 mem | k=4 result | k=4 runtime | k=4 mem |
|---|---|---|---|---|---|---|---|---|---|
| duckdb-baseline (no early filtering) | 23 | 27.2 ms | 228 MB | 95 | 794 ms | 594 MB | 264 | 44,738 ms | 17,528 MB |
| recap-inline (old prototype) | 23 | 17.5 ms | 221 MB | 95 | 25.3 ms | 226 MB | 264 | 34.9 ms | 244 MB |
| recap-udf (old prototype) | 23 | 252 ms | 200 MB | 95 | 968 ms | 206 MB | 264 | 2,380 ms | 218 MB |
| recap-new-standard | 23 | 41.5 ms | 34.3 MB* | 95 | 63.8 ms | 42.4 MB* | 264 | 99.7 ms | 54.4 MB* |
| recap-new-optimized | 23 | 21.1 ms | 34.3 MB* | 95 | 32.0 ms | 42.4 MB* | 264 | 54.0 ms | 54.4 MB* |
| kuzu | 23 | 7,725 ms | 2,021 MB | -- | didn't finish in reasonable time | -- | -- | didn't finish | -- |
| neo4j | 23 | 388.7 ms | 14,886 MB | -- | ~14.3s (single-run, not part of the 4-run sweep below) | -- | -- | didn't finish in 180s | -- |
| memgraph | 23 | 361.0 ms | 1,068 MB | -- | ~71.4s (single-run) | -- | -- | not attempted (worse than Neo4j at k=3 already) | -- |

\* DuckDB internal buffer memory, not process RSS -- see metric caveat above.

**Neo4j/Memgraph, added 2026-08-14, both cap out at length=2 for the same
reason Kùzu does (no early filtering + this regex/dataset's ~59x-per-hop
branching), just less dramatically -- confirmed directly rather than
assumed:** a single (non-sweep) execution of the length=3 query took
~14.3s on Neo4j and ~71.4s on Memgraph; length=4 didn't complete in 180s on
Neo4j and wasn't attempted on Memgraph (already the slower of the two at
k=3). Interesting cross-engine contrast worth noting: Kùzu handles Q1's
own blowup far better than either of these (the full 2-4 sweep in
`run_kuzu.sh` completes in seconds) but crashes outright on Q2's
differently-shaped query (see `../q2_length_sweep/README.md`) -- no single
engine is uniformly better or worse at "no early filtering," it depends
on the specific query shape and each engine's own execution strategy.

**recap-new numbers above are with `compile_regex_to_nfa(..., minimize=True)` and the
`execution.py` telemetry fix, both added 2026-08-13 -- see next section.**
Before both fixes, `recap-new-optimized` reported 41/66/104ms (2-3x higher):
half of that gap was `runtime_ms` silently including a second, diagnostic-only
re-execution of the whole recursive CTE; the rest was the *unminimized*
36-state/98-transition automaton (vs. the minimal 3-state/10-transition one
below) forcing every hop's join against a much bigger transitions table.

With both fixes, `recap-new-optimized` (21.1/32.0/54.0ms) is now only
~1.2-1.55x slower than `recap-inline` (17.5/25.3/34.9ms) -- a real, residual,
*structural* gap, not a bug: Q1's aggregate here is deliberately factorized
(automaton-state-agnostic, per Stage D's convention), so it has to check
`e.label IN ('transfer','purchase','sale')` as a string-membership test 4
times per row to know whether an edge is on the normal or fraud side of the
pattern. `recap-inline`'s hand-written query gets that distinction for free
by dispatching on integer NFA state in a `CASE` it already needed for other
reasons -- cheaper than a string `IN` check, and needed only once per row.

Kùzu's own Cypher formulation of Q1 (`experiments/SOA-GDBMS/kuzu_run.py`)
checks trail/monotonicity/region/range only *after* fully materializing
each candidate path (`list_reduce` over the whole match), so it has the
same "no early filtering" character as the DuckDB baseline -- confirmed by
killing it after 3+ minutes and 3.7GB+ RSS still climbing on just the
*filtered* query at length 3 (never mind the unfiltered candidate-only
diagnostic, which is worse). Reproduce with:

```bash
cd ../SOA-GDBMS
python3 kuzu_run.py --nodes ../../ReCAP/simple_dataset/LG_V.csv \
  --edges ../../ReCAP/simple_dataset/LG.csv --query q1 --starter 383 \
  --min-len 2 --max-len 2 --memory-source psutil --fresh-db
```

## Two real bugs found and fixed while building this

1. **New compiler, Stage C (`transitions.py`) -- duplicate transition
   rows caused overcounting.** Before the fix, `recap-new-standard`/
   `-optimized` returned 63/262/733 paths instead of 23/95/264. Root cause
   and fix: see `[[project_recap_compiler_revision]]` memory / git history
   on `compiler/src/recap_compiler/transitions.py`. Caught only because
   this pilot cross-validated result *counts* across independently-built
   engines -- the existing 117-test suite never exercised a regex whose
   NFA has multiple start states sharing an outgoing target (true of Q1's
   own `(a|b|c)+`-shaped pattern).
2. **Old prototype -- dead `self.register_udfs()`/`reset_udf_stats()`
   calls crashed `duckdb_gen_recap.py` and `recap_gen_recap_inline.py`
   outright**, even run standalone via their own CLI. Removed (leftover
   copy-paste from the UDF variant; neither name was ever defined).

## Two more fixes, made answering "why is recap-new slower?" (2026-08-13)

3. **`execution.py`'s `Telemetry.runtime_ms` silently included a second
   query execution.** Computing `intermediate_paths` re-runs the entire
   recursive CTE a second time (there's no other way to recover "rows
   before the final filter"), and the old code folded that second
   execution's time into the same `runtime_ms` used for "how long did the
   query take" -- so every reported recap-new runtime was roughly double
   the real cost of the query a caller actually asked for. Fixed by adding
   a separate `Telemetry.intermediate_count_ms` field; `runtime_ms` now
   times only the main query. `demo_pipeline.py` and the webapp both
   updated to show the recount time separately instead of silently.
4. **Stage B's regex compiler never minimized the NFA, even though it's
   safe as an opt-in.** `compile_regex_to_nfa` now takes
   `minimize: bool = False` -- default unchanged (non-minimization stays
   the default, since preserving the raw NFA is what keeps ReCAP compatible
   with wavefront/segment-style planners), but this pilot passes
   `minimize=True` since it's only measuring
   standard bottom-up evaluation. Confirmed via `nfa.is_equivalent_to(...)`
   that minimizing doesn't change the language, and for Q1's own regex it
   collapses the automaton from 36 states/98 transitions down to exactly
   3 states/10 transitions -- the same shape as the old prototype's
   hand-designed automaton (pyformlang's `.minimize()` determinizes
   internally; no separate `.to_deterministic()` call needed).

## Reproduce

```bash
./run_all.sh              # everything below, in sequence
# or individually:
./run_new_compiler.sh     # writes results/new_compiler_q1.csv
./run_old_prototype.sh    # writes results/old_prototype_q1.csv (subprocess per engine;
                           #   duckdb-baseline's length=4 case alone takes ~45s/~17.5GB RSS)
./run_kuzu.sh [max_len]   # writes results/kuzu_q1.csv; defaults to max_len=2 -- see the
                           #   script's own comment for why 3/4 aren't run by default
./run_neo4j.sh [max_len]  # writes results/neo4j_q1.csv; defaults to max_len=2. Requires
                           #   a running local Neo4j (~/neo4j/neo4j-community-5.24.0) --
                           #   see experiments/SOA-GDBMS/run_neo4j.sh for start/stop
./run_memgraph.sh [max_len]  # writes results/memgraph_q1.csv; defaults to max_len=2.
                           #   Requires `docker start memgraph` first (bolt on host
                           #   port 7688, not 7687 -- Neo4j already owns that one)
```

Each `.sh` is a thin wrapper (`cd` to this directory, run the matching
`.py`, or invoke `experiments/SOA-GDBMS/kuzu_run.py` with this pilot's
fixed settings) -- run them directly rather than the `.py` files so
relative paths resolve regardless of your current working directory.

`results/kuzu_q1.csv` has a different (wider) column set than the other
two CSVs -- it's written directly by `bench_common.write_csv` (engine,
query, start, len, success, result, median_ms, avg_ms, min_ms, max_ms,
intermediate_paths, peak_memory_mb, error), not hand-rolled like the other
two scripts' output.

## E1 rerun on real Metaverse data, with memory (2026-08-14)

`run_new_compiler.py` rewritten to (1) point at the real dataset
(`experiments/datasets/metaverse/`, symlinked to the same `LG.csv`/`LG_V.csv`
this pilot already used -- no change in data, just a canonical path), (2)
sweep the paper's own `ℓ ∈ {2..10}` range for Q1-Metaverse instead of
`{2,3,4}`, and (3) isolate Standard vs Optimized into separate subprocesses
so each variant's memory reading (`peak_buffer_memory_mb`: DuckDB's
internal buffer stat; `peak_rss_mb`: this process's own peak RSS via
`psutil`) is genuinely that variant's own, not shared/cumulative as the
original single-connection version made them.

| ℓ | result | standard runtime | standard rss | optimized runtime | optimized rss |
|---|---|---|---|---|---|
| 2 | 23 | 46.7ms | 488MB | 23.7ms | 472MB |
| 4 | 264 | 100.0ms | 527MB | 58.0ms | 508MB |
| 6 | 711 | 189.4ms | 607MB | 108.6ms | 588MB |
| 8 | 840 | 251.4ms | 622MB | 146.7ms | 632MB |
| 10 | 878 | 336.2ms | 660MB | 164.9ms | 534MB |

Full data in `results/new_compiler_q1.csv`. Standard and optimized verified
to produce exactly equal results at every ℓ. Optimized is consistently ~1.4-2x faster than Standard here --
**far short of the paper's own 152x/346x for this same optimization**,
because the new compiler's "Standard" (Stage E, DuckDB SQL macros) has
nowhere near the overhead of the paper's actual Standard implementation
(Python UDFs, one interpreter round-trip + JSON parse per function call
per row) -- this was already established on the toy dataset in the
original version of this pilot and holds at real scale too. RSS is fairly
flat across ℓ (undoubtedly dominated by loading the ~78k-edge dataset
itself, not by query state, since Q1's own intermediate cardinality stays
under 1,000 rows even at ℓ=10 per `fig:recap_performance_grid`'s own
"is_viable_d" pruning).
