#!/usr/bin/env bash
# Runs Q4 on the old prototype's three ReCAP/q4 variants -- DuckDB baseline,
# ReCAP-inline, ReCAP-UDF -- at max_length in {2,3,4}, min_length=2,
# starter_node=9. Backfilled 2026-08-13 from ~/ReCAP/q4/ (see
# README.md for why this repo's own ReCAP/ copy never had a q4/ folder).
# Each engine runs in its own subprocess so peak-RSS memory readings aren't
# contaminated by an earlier engine's still-resident data.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

python3 run_old_prototype.py

echo "Completed -- results in results/old_prototype_q4.csv"
