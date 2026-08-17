#!/usr/bin/env bash
# Runs Q2 on the old prototype's three ReCAP/q2 variants -- DuckDB baseline,
# ReCAP-inline, ReCAP-UDF -- at max_length in {2,3,4}, min_length=2,
# starter_node=9. Each engine runs in its own subprocess so peak-RSS memory
# readings aren't contaminated by an earlier engine's still-resident data.
# See README.md.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

python3 run_old_prototype.py

echo "Completed -- results in results/old_prototype_q2.csv"
