#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SF="${1:-0.01}"
DATA_DIR="${SCRIPT_DIR}/Data"

echo "=== TPC-H Parquet Data Generator via DuckDB ==="
echo "Scale Factor : ${SF}"
echo "Target Dir   : ${DATA_DIR}"

if ! command -v duckdb &> /dev/null; then
    echo "Error: duckdb command not found in PATH." >&2
    exit 1
fi

mkdir -p "${DATA_DIR}"

duckdb -dark-mode -c "
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${SF});
EXPORT DATABASE '${DATA_DIR}' (FORMAT PARQUET);
"

echo "=== Generation Complete ==="
ls -lh "${DATA_DIR}"/*.parquet
