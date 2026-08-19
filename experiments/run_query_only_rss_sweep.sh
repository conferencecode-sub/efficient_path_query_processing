#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

declare -A LENGTHS
LENGTHS[q1]="2 3 4 5 6 7 8 9 10"
LENGTHS[q2]="2 3 4 5"
LENGTHS[q3]="2 3 4"
LENGTHS[q4]="2 3 4 5 6 7 8"

QUERY="$1"
OUT="results_query_only_rss_${QUERY}.jsonl"
: > "$OUT"
for length in ${LENGTHS[$QUERY]}; do
    for variant in standard optimized; do
        echo "=== ${QUERY} length=${length} variant=${variant} ===" >&2
        python3 query_only_rss.py --query "$QUERY" --length "$length" --variant "$variant" | tee -a "$OUT"
    done
done
echo "wrote $OUT"
