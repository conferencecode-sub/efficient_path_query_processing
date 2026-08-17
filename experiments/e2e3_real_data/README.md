# E2/E3 rerun on real data — status (checkpointed 2026-08-14)

Rerunning the competitor comparison (E2: runtime vs. Kùzu/Neo4j/Memgraph/DuckDB;
E3: intermediate cardinality) against the real datasets consolidated earlier
this session, instead of the toy/generic dataset the original pilots used.
Order: smallest dataset first (Bitcoin → Reddit → LDBC100). **2-hour timeout
per query; a competitor is dropped from further lengths the moment it times
out or errors at some ℓ** (per explicit user instruction).

Stopped mid-run by explicit user request (about to shut down the machine
this all runs on) — see `project_recap_compiler_revision.md` (memory) for
the full checkpoint. Short version:

## Bitcoin / Q2 — done

All of DuckDB-baseline, Neo4j, and Memgraph agree exactly at every length
through ℓ=6: 34 / 1,666 / 52,508 / 2,743,095 / 115,732,791. Runtimes at ℓ=6
(within the 2h budget): DuckDB 245s, Memgraph 565s, Neo4j 616s. **Kùzu
excluded** — a real, newly-found engine bug (a `WITH ... AS cs` list
variable silently returns empty once referenced twice in the same clause),
worse than the already-known crash-at-scale, confirmed via direct
isolation. See `results/*_q2_real.csv`.

## Reddit / Q3 — partial

Start vertex 31470 (out-degree 1, true low-degree per the paper's own
methodology). Reference, ReCAP-new, and Kùzu all agree through ℓ=4:
17 / 20 / 36. Real finding: intermediate cardinality cliffs from ~39.5K
(ℓ=3) to ~10.9M (ℓ=4) despite the low-degree start — Reddit's small-world
structure puts a hub within a few hops regardless. Kùzu self-reported an
out-of-memory error at ℓ=5 and stopped cleanly (results through ℓ=4 saved).
**Neo4j and Memgraph were killed mid-ℓ=5 by explicit request — nothing from
either is saved** (`bench_common.run_sweep` only writes its CSV once, at
the end of the whole sweep), so both need a full rerun from ℓ=2, not a
resume. Exact rerun commands are in the memory checkpoint.

## LDBC100 / Q4 — not started

The dataset most likely to actually be intractable (19.9M edges, vs.
Bitcoin's 35K and Reddit's 286K) — treat with real caution given how
severe Reddit's own cliff already was on a much smaller graph. Loader
prep (`experiments/datasets/ldbc100_engine_ready/edges.csv`, weight aliased
to `epoch_ms(creation_date)`) is already done; `kuzu_run.py`'s
`Q4_MAX_MIN_BOUND` needs `Q4_MAX_MIN_BOUND=1209600000` (two weeks, ms) set
as an env var for this dataset specifically.
