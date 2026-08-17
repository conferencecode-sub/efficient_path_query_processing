#!/usr/bin/env bash
# Runs Q1 on the old prototype's three ReCAP/q1 variants -- DuckDB baseline,
# ReCAP-inline, ReCAP-UDF -- at max_length in {2,3,4}, min_length=2,
# starter_node=383. Each engine runs in its own subprocess (see
# run_old_prototype.py's own docstring) so peak-RSS memory readings aren't
# contaminated by an earlier engine's still-resident data. See README.md.
#
# Warning: duckdb-baseline (no early filtering) takes ~45s and ~17.5GB RSS
# at length=4 on this dataset -- that's the point being demonstrated, not a
# hang, but don't be surprised by it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

python3 run_old_prototype.py

echo "Completed -- results in results/old_prototype_q1.csv"
