"""Reads IPC datasets as pyarrow (pyarrow.dataset, format="ipc") and Python Polars (scan_ipc) read them, and file by
file (pyarrow ipc.open_file / open_stream, Polars read_ipc / read_ipc_stream, the rows concatenated: neither engine
reads datasets of streams), for IpcDatasetInteropTests: argv[1] is the cases JSON, argv[2] the output JSON.

Each case: {"inputs": [paths, in order]} (several files, or one directory read whole); the output per case is, per
engine ("pyarrow", "polars", "pyarrow_files", "polars_files"), {"rows": rows of canonical values
(../arrow_canonical.py) joined by '|', "columns": names} or {"error": the message}.

`dataset_oracle.py streams <source dir> <target dir>` writes each *.arrow file of the source as a stream (pyarrow).
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


def paths(inputs):
    if len(inputs) == 1 and os.path.isdir(inputs[0]):
        return sorted(os.path.join(inputs[0], n) for n in os.listdir(inputs[0]))
    return inputs


def is_file(path):
    with open(path, "rb") as f:
        return f.read(6) == b"ARROW1"


def pyarrow_files(inputs):
    import pyarrow.ipc as ipc
    tables = [(ipc.open_file(p) if is_file(p) else ipc.open_stream(p)).read_all() for p in paths(inputs)]
    return tables


def polars_files(inputs):
    return [(pl.read_ipc(p) if is_file(p) else pl.read_ipc_stream(p)).to_arrow() for p in paths(inputs)]


def streams(source, target):
    import pyarrow.ipc as ipc
    os.makedirs(target, exist_ok=True)
    for name in sorted(os.listdir(source)):
        if not name.endswith(".arrow"):
            continue
        table = ipc.open_file(os.path.join(source, name)).read_all()
        with ipc.new_stream(os.path.join(target, name + "s"), table.schema) as writer:
            for batch in table.to_batches():
                writer.write_batch(batch)


def main():
    if sys.argv[1] == "streams":
        streams(sys.argv[2], sys.argv[3])
        return
    cases = json.load(open(sys.argv[1]))
    out = {}
    for name, case in cases.items():
        out[name] = {}
        for engine, read in (("pyarrow", pyarrow_read), ("polars", polars_read), ("pyarrow_files", pyarrow_files), ("polars_files", polars_files)):
            try:
                tables = read(case["inputs"])
                tables = tables if isinstance(tables, list) else [tables]
                out[name][engine] = {"rows": [r for t in tables for r in rows(t)], "columns": tables[0].column_names if tables else []}
            except BaseException as e:  # noqa: BLE001 - the error is compared (Polars panics raise PanicException)
                out[name][engine] = {"error": f"{type(e).__name__}: {e}"[:500]}
    json.dump(out, open(sys.argv[2], "w"))


if __name__ == "__main__":
    pa.set_cpu_count(1)
    main()
