"""Reads IPC datasets as pyarrow (pyarrow.dataset, format="ipc") and Python Polars (scan_ipc) read them, for
IpcDatasetInteropTests: argv[1] is the cases JSON, argv[2] the output JSON.

Each case: {"inputs": [paths, in order]} (several files, or one directory read whole); the output per case is, per
engine ("pyarrow", "polars"), {"rows": rows of canonical values (../arrow_canonical.py) joined by '|', "columns":
names} or {"error": the message}.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.dataset as ds  # noqa: E402

from arrow_canonical import render  # noqa: E402


def rows(table):
    table = table.combine_chunks()
    columns = [render(column.chunk(0)) if column.num_chunks else [] for column in table.columns]
    return ["|".join(column[i] for column in columns) for i in range(table.num_rows)]


def pyarrow_read(inputs):
    source = inputs[0] if len(inputs) == 1 and os.path.isdir(inputs[0]) else inputs
    return ds.dataset(source, format="ipc").to_table()


def polars_read(inputs):
    source = inputs[0] + "/**/*" if len(inputs) == 1 and os.path.isdir(inputs[0]) else inputs
    return pl.scan_ipc(source).collect().to_arrow()


def main():
    cases = json.load(open(sys.argv[1]))
    out = {}
    for name, case in cases.items():
        out[name] = {}
        for engine, read in (("pyarrow", pyarrow_read), ("polars", polars_read)):
            try:
                table = read(case["inputs"])
                out[name][engine] = {"rows": rows(table), "columns": table.column_names}
            except BaseException as e:  # noqa: BLE001 - the error is compared (Polars panics raise PanicException)
                out[name][engine] = {"error": f"{type(e).__name__}: {e}"[:500]}
    json.dump(out, open(sys.argv[2], "w"))


if __name__ == "__main__":
    pa.set_cpu_count(1)
    main()
