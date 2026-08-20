# Cross-system result-agreement report

Replays saved result CSVs under `experiments/` and checks that every system
reporting a result count for the same query/dataset/length agrees exactly.
No database server is contacted; this is a replay of prior runs, not a rerun.

## SOA-GDBMS

- Systems compared: kuzu, memgraph, neo4j
- Checkpoints (query/dataset/length points with >=2 systems reporting): 5
- Agreeing: 4
- Disagreeing: 1

**Mismatches:**
  - key=('q2', '2'): kuzu=0, memgraph=34, neo4j=34

## e2e3_real_data

- Systems compared: duckdb-baseline, kuzu, memgraph, neo4j, recap-new-optimized, recap-new-standard, recap-new-standard-udf
- Checkpoints (query/dataset/length points with >=2 systems reporting): 17
- Agreeing: 17
- Disagreeing: 0
- Checkpoints where at least one system errored/timed out (excluded from mismatch count, reported for completeness): 3

## e4_isolation

- Systems compared: 2-regex-late-property, 3-regex-early-property
- Checkpoints (query/dataset/length points with >=2 systems reporting): 3
- Agreeing: 3
- Disagreeing: 0

## e5_handcrafted_vs_recap

- Systems compared: handcrafted, recap-optimized, split
- Checkpoints (query/dataset/length points with >=2 systems reporting): 9
- Agreeing: 9
- Disagreeing: 0

## e6_finbench

- Systems compared: independent-reference, independent-reference (embedded in kuzu run), independent-reference (embedded in memgraph run), independent-reference (embedded in neo4j run), kuzu, memgraph, neo4j, recap
- Checkpoints (query/dataset/length points with >=2 systems reporting): 21
- Agreeing: 21
- Disagreeing: 0

## e6_finbench_sf1

- Systems compared: independent-reference, independent-reference (embedded in kuzu run), independent-reference (embedded in memgraph run), independent-reference (embedded in neo4j run), kuzu, memgraph, neo4j, recap
- Checkpoints (query/dataset/length points with >=2 systems reporting): 21
- Agreeing: 21
- Disagreeing: 0
- Checkpoints where at least one system errored/timed out (excluded from mismatch count, reported for completeness): 1

## e6_finbench_sf10

- Systems compared: independent-reference, independent-reference (embedded in kuzu run), independent-reference (embedded in memgraph run), independent-reference (embedded in neo4j run), kuzu, memgraph, neo4j, recap
- Checkpoints (query/dataset/length points with >=2 systems reporting): 21
- Agreeing: 21
- Disagreeing: 0
- Checkpoints where at least one system errored/timed out (excluded from mismatch count, reported for completeness): 1

## e7_scale_sweep

- Systems compared: recap-new-optimized, recap-new-standard
- Checkpoints (query/dataset/length points with >=2 systems reporting): 14
- Agreeing: 14
- Disagreeing: 0

## navigation_experiment

- Systems compared: monolithic, naive-split, seam-aware-split
- Checkpoints (query/dataset/length points with >=2 systems reporting): 20
- Agreeing: 20
- Disagreeing: 0

## q1_length_sweep

- Systems compared: duckdb-baseline, kuzu, memgraph, neo4j, recap-inline, recap-new-optimized, recap-new-standard, recap-udf
- Checkpoints (query/dataset/length points with >=2 systems reporting): 9
- Agreeing: 9
- Disagreeing: 0

## q2_length_sweep

- Systems compared: duckdb-baseline, kuzu, memgraph, neo4j, recap-inline, recap-udf
- Checkpoints (query/dataset/length points with >=2 systems reporting): 3
- Agreeing: 3
- Disagreeing: 0

**Same-engine-label conflicts** (two different source files used the same system label at this checkpoint and disagree -- a data-hygiene issue in the CSVs themselves, kept separate from the cross-system mismatch count above):
  - key=('q2', '2'), engine=duckdb-baseline: 34@q2_length_sweep/results/duckdb_baseline_q2_real.csv, 26@q2_length_sweep/results/old_prototype_q2.csv
  - key=('q2', '3'), engine=duckdb-baseline: 1666@q2_length_sweep/results/duckdb_baseline_q2_real.csv, 1023@q2_length_sweep/results/old_prototype_q2.csv
  - key=('q2', '4'), engine=duckdb-baseline: 52508@q2_length_sweep/results/duckdb_baseline_q2_real.csv, 29871@q2_length_sweep/results/old_prototype_q2.csv

## q3_length_sweep

- Systems compared: duckdb-baseline, kuzu, memgraph, neo4j, recap-inline, recap-udf
- Checkpoints (query/dataset/length points with >=2 systems reporting): 3
- Agreeing: 3
- Disagreeing: 0

## q4_length_sweep

- Systems compared: duckdb-baseline, kuzu, memgraph, neo4j, recap-inline, recap-udf
- Checkpoints (query/dataset/length points with >=2 systems reporting): 3
- Agreeing: 3
- Disagreeing: 0

## Files excluded: no counterpart to verify against

These saved result files could not be cross-checked because nothing else on disk
was run against the same dataset/start vertex -- listed here for transparency
rather than silently dropped:

  - `q3_length_sweep/results/new_compiler_q3.csv`: uses the Datagen-7.6 dataset (start=4398046568596); no other saved result on disk uses this exact dataset+start vertex to cross-check against (e2e3_real_data's Q3 is Reddit, start=31470 -- a different experiment).
  - `q3_length_sweep/results/new_compiler_q3_udf.csv`: same reason as new_compiler_q3.csv (Datagen-7.6, start=4398046568596).

## e4_isolation: excluded metric (by design)

Config `1-regex-only` measures the *unfiltered* automaton-exploration count, a
deliberately different metric from configs 2/3's filtered final answer -- not
included in the agreement check above. Values for reference:
  - length=2: 1408
  - length=3: 69500
  - length=4: 3530274

## Summary

- Total checkpoints checked: 149
- Total mismatches: 1
- Total same-engine-label conflicts (data hygiene, not a cross-system mismatch): 3
- Files excluded as unverifiable (no counterpart on disk): 2

**Mismatches found -- see detail above before trusting any cross-system claim.**
