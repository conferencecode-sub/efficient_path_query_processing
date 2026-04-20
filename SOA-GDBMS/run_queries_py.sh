
# #!/usr/bin/env bash

GDB_BIN_PY_COMMAND="python3 path/to/gdbms"  # Update with actual path to GDBMS binary

output_dir="results/"
mkdir -p "$output_dir"

$GDB_BIN_PY_COMMAND > "${output_dir}/output.log" 2>&1

echo "Completed - results in $output_dir"
# done

