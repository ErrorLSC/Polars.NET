"""Reads the dataset cases of ParquetDatasetInteropTests with Polars: argv[1] is the cases JSON, argv[2] the output JSON.

Each case: {"inputs": [...], "row_index": name|null, "file_column": name|null, "n_rows": int|null} and optionally
"hive": true|false|null, "parse_dates": bool (default true), "filter": [column, "==", value], "format": "ipc" (scan_ipc;
default scan_parquet, also for IpcDatasetInteropTests); the output per case is
{"rows": rows rendered as strings joined by '|' (Python's str, None as ""), "columns": names, "types": dtypes} or
{"error": the message}.
"""
import json
import sys

import polars as pl

cases = json.load(open(sys.argv[1]))
out = {}
for name, case in cases.items():
    try:
        inputs = case["inputs"]
        scan = pl.scan_ipc if case.get("format") == "ipc" else pl.scan_parquet
        lf = scan(inputs if len(inputs) > 1 else inputs[0], row_index_name=case["row_index"],
                             include_file_paths=case["file_column"], hive_partitioning=case.get("hive", False),
                             try_parse_hive_dates=case.get("parse_dates", True))
        if case.get("filter"):
            column, op, value = case["filter"]
            assert op == "=="
            lf = lf.filter(pl.col(column) == value)
        if case["n_rows"] is not None:
            lf = lf.head(case["n_rows"])
        df = lf.collect()
        out[name] = {"rows": ["|".join("" if v is None else str(v) for v in row) for row in df.iter_rows()],
                     "columns": df.columns, "types": [str(t) for t in df.dtypes]}
    except Exception as e:  # noqa: BLE001 - the error is compared
        out[name] = {"error": f"{type(e).__name__}: {e}"}
json.dump(out, open(sys.argv[2], "w"))
