#!/usr/bin/env bash
# Generates TPC-H data as Parquet (via DuckDB's tpch extension) for the TPC-H benchmarks.
# Usage: ./generate_tpch_data.sh [scale-factor]   (default 1; output: Data/tpch-sf<scale-factor>)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SF="${1:-1}"
DATA_DIR="${SCRIPT_DIR}/Data/tpch-sf${SF}"

if ! command -v duckdb &> /dev/null; then
    echo "Error: duckdb command not found in PATH." >&2
    exit 1
fi

mkdir -p "${DATA_DIR}"
duckdb -c "
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${SF});
EXPORT DATABASE '${DATA_DIR}' (FORMAT PARQUET);
"

ls -lh "${DATA_DIR}"/*.parquet
