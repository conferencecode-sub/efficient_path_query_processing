#!/usr/bin/env bash
# Runs one engine script across a length range, one length per invocation
# (--min-len X --max-len X), each wrapped in a real OS-level `timeout` --
# unlike Kùzu's own conn.set_query_timeout(), the Neo4j/Memgraph bolt
# drivers have no built-in per-query cancellation, so a stuck query would
# otherwise hang the whole sweep indefinitely. Stops calling higher lengths
# the moment one length times out (exit 124) or errors, per the user's
# explicit "if it times out at ell=3, stop performing experiments with said
# competitor" instruction -- matches the paper's own 2-hour-per-run
# protocol (`--timeout 7200` below).
#
# Usage: sweep_with_timeout.sh <engine_script.py> <csv_out_prefix> <min_len> <max_len> [extra args...]
set -u
SCRIPT="$1"; CSV_PREFIX="$2"; MIN_LEN="$3"; MAX_LEN="$4"
shift 4

for len in $(seq "$MIN_LEN" "$MAX_LEN"); do
    echo "=== length=$len ==="
    timeout 7200 python3 -u "$SCRIPT" "$@" --min-len "$MIN_LEN" --max-len "$len" \
        --timeout 7200 --csv "${CSV_PREFIX}_len${len}.csv"
    status=$?
    if [ $status -eq 124 ]; then
        echo "TIMEOUT at length=$len (2h) -- stopping sweep for this engine/query/dataset."
        break
    elif [ $status -ne 0 ]; then
        echo "ERROR at length=$len (exit $status) -- stopping sweep for this engine/query/dataset."
        break
    fi
done
