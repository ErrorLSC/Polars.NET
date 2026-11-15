"""Reads the Parquet files ParquetWriterInteropTests wrote with pyarrow, DuckDB and Python Polars.

Usage: python -I writer_interop.py <dir>

- Every <name>.parquet is read whole by each reader and saved as <name>.<reader>.arrow (Arrow IPC) for the C# test
  to compare with the values written. A reader that cannot read the whole file reads it column by column; the columns
  it cannot read are listed in errors.json ({file, reader, column, error}) for the test to check against what is known.
- sorted.parquet and sorted_nonnull.parquet (sorted columns over several row groups; pyarrow prunes no row group
  holding a null) are filtered at values from its columns, by each reader with
  its statistics (pushdown) and in memory: the row counts must agree, or the bounds skipped rows. Mismatches are
  printed and fail the script.
"""
import decimal
import glob
import json
import os
import sys

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

directory = sys.argv[1]
errors = []


def save(table, path):
    table = table.unify_dictionaries().combine_chunks()  # an IPC file holds one dictionary per field
    with pa.OSFile(path, "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def readers(path):
    return {
        "pyarrow": lambda cols: pq.read_table(path, columns=cols),
        "duckdb": lambda cols: duckdb.sql(f"select {', '.join(map(quote, cols))} from read_parquet('{path}')").to_arrow_table(),
        "polars": lambda cols: pl.read_parquet(path, columns=cols).to_arrow(),
    }


for path in sorted(glob.glob(os.path.join(directory, "*.parquet"))):
    name = os.path.basename(path)[: -len(".parquet")]
    columns = pq.ParquetFile(path).schema_arrow.names
    for reader, read in readers(path).items():
        try:
            table = read(columns)
        except BaseException:  # Polars panics are BaseExceptions
            arrays, names = [], []
            for column in columns:
                try:
                    part = read([column])
                    arrays.append(part.column(0))
                    names.append(column)
                except BaseException as e:  # noqa: BLE001 - recorded for the test
                    errors.append({"file": name, "reader": reader, "column": column, "error": str(e).splitlines()[0][:300]})
            table = pa.table(arrays, names=names)
        save(table, os.path.join(directory, f"{name}.{reader}.arrow"))

with open(os.path.join(directory, "errors.json"), "w") as out:
    json.dump(errors, out, indent=1)

# ----------------------------------------------------------------------
# Filters at the bounds: with statistics against in memory
# ----------------------------------------------------------------------
mismatches = []
for sorted_path in [os.path.join(directory, n) for n in ("sorted.parquet", "sorted_nonnull.parquet")]:
    if not os.path.exists(sorted_path):
        continue
    full = pq.read_table(sorted_path)
    in_memory = full  # DuckDB's in-memory baseline scans this table by name
    fragment = next(iter(ds.dataset(sorted_path, format="parquet").get_fragments()))
    frame = pl.from_arrow(full)
    ops = {"=": pc.equal, "<": pc.less, "<=": pc.less_equal, ">": pc.greater, ">=": pc.greater_equal}
    expressions = {"=": lambda f, v: f == v, "<": lambda f, v: f < v, "<=": lambda f, v: f <= v, ">": lambda f, v: f > v, ">=": lambda f, v: f >= v}
    polars_ops = {"=": "__eq__", "<": "__lt__", "<=": "__le__", ">": "__gt__", ">=": "__ge__"}
    for column in ["i32", "u32", "u64", "f64", "dec30", "dec5", "str", "day"]:
        values = [v for v in full.column(column).to_pylist() if v is not None and v == v]
        distinct = sorted(set(values))
        picks = {distinct[i] for i in [0, 1, len(distinct) // 8, len(distinct) // 4, len(distinct) // 2, -2, -1]}
        if column == "f64":
            picks |= {0.0, -0.0}
        for value in sorted(picks):
            scalar = pa.scalar(value, type=full.schema.field(column).type)
            for op, compute in ops.items():
                # Each reader against itself in memory (no statistics): their NaN orders differ (DuckDB and Polars put
                # NaN above every number, Arrow compute compares it false).
                arrow_count = pc.sum(pc.cast(pc.fill_null(compute(full.column(column), scalar), False), pa.int64())).as_py() or 0
                condition = getattr(pl.col(column), polars_ops[op])(pl.lit(value, dtype=frame.schema[column]))
                checks = {
                    # Row groups kept by their statistics (pyarrow's pruning), then filtered.
                    "pyarrow": (sum(f.to_table(columns=[column], filter=expressions[op](pc.field(column), scalar)).num_rows
                                    for f in fragment.split_by_row_group(expressions[op](pc.field(column), scalar))), arrow_count),
                    "duckdb": (duckdb.execute(f"select count(*) from read_parquet('{sorted_path}') where {quote(column)} {op} ?", [value]).fetchone()[0],
                               duckdb.execute(f"select count(*) from in_memory where {quote(column)} {op} ?", [value]).fetchone()[0]),
                    "polars": (pl.scan_parquet(sorted_path).filter(condition).select(pl.len()).collect().item(), frame.filter(condition).height),
                }
                for reader, (count, expected) in checks.items():
                    if count != expected:
                        mismatches.append(f"{reader}: {os.path.basename(sorted_path)} {column} {op} {value!r}: {count} rows, {expected} in memory")

for m in mismatches:
    print("MISMATCH", m)
print("interop done" if not mismatches else "interop FAILED")
sys.exit(1 if mismatches else 0)
