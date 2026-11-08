#!/usr/bin/env bash
# Generates TPC-DS data as Parquet (via DuckDB's tpcds extension) for the TPC-DS benchmarks.
# Usage: ./generate_tpcds_data.sh [scale-factor]   (default 2; output: Data/tpcds-sf<scale-factor>)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SF="${1:-2}"
DATA_DIR="${SCRIPT_DIR}/Data/tpcds-sf${SF}"

if ! command -v duckdb &> /dev/null; then
    echo "Error: duckdb command not found in PATH." >&2
    exit 1
fi

mkdir -p "${DATA_DIR}"
duckdb -c "
INSTALL tpcds;
LOAD tpcds;
CALL dsdgen(sf=${SF});
EXPORT DATABASE '${DATA_DIR}' (FORMAT PARQUET);
"

ls -lh "${DATA_DIR}"/*.parquet
