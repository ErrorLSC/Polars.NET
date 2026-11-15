"""Reads the dataset cases of ParquetDatasetInteropTests with Polars: argv[1] is the cases JSON, argv[2] the output JSON.

Each case: {"inputs": [...], "row_index": name|null, "file_column": name|null, "n_rows": int|null}; the output per case
is {"files": [...] } with "rows": rows rendered as strings joined by '|', or "error": the message.
"""
import json
import sys

import polars as pl

cases = json.load(open(sys.argv[1]))
out = {}
for name, case in cases.items():
    try:
        inputs = case["inputs"]
        lf = pl.scan_parquet(inputs if len(inputs) > 1 else inputs[0], row_index_name=case["row_index"],
                             include_file_paths=case["file_column"], n_rows=case["n_rows"], hive_partitioning=False)
        df = lf.collect()
        out[name] = {"rows": ["|".join("" if v is None else str(v) for v in row) for row in df.iter_rows()],
                     "columns": df.columns}
    except Exception as e:  # noqa: BLE001 - the error is compared
        out[name] = {"error": f"{type(e).__name__}: {e}"}
json.dump(out, open(sys.argv[2], "w"))
