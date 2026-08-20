#!/usr/bin/env bash
# Runs Q1 on the new compiler (Stage E Standard + Stage F Optimized) at
# length_bound in {2,3,4}, starter_node=383, over the bundled LG.csv/LG_V.csv.
# Uses compile_regex_to_nfa(..., minimize=True) (opt-in, since this
# pilot only measures standard bottom-up evaluation) and checks
# standard == optimized results at every length. See README.md.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

python3 run_new_compiler.py

echo "Completed -- results in results/new_compiler_q1.csv"
