#!/usr/bin/env bash
# Runs Q2 (two-color trail) on the new compiler (Stage E Standard + Stage F
# Optimized) at length_bound in {2,3,4}, starter_node=9, over the bundled
# generic nodes.csv/edges.csv (not LG). See README.md.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

python3 run_new_compiler.py

echo "Completed -- results in results/new_compiler_q2.csv"
