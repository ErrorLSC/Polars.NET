"""Reads the Parquet files ParquetWriterInteropTests wrote with pyarrow, DuckDB and Python Polars.

Usage: python -I writer_interop.py <dir>

- For every <name>.source.arrow, pyarrow writes the same table and the two footers are compared column by column
  (schema shape, levels, value and null counts, bounds).
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


# ----------------------------------------------------------------------
# Footers against pyarrow's for the same table (<name>.source.arrow): schema shape, levels, value and null counts, bounds
# ----------------------------------------------------------------------
footer_mismatches = []
for source in sorted(glob.glob(os.path.join(directory, "*.source.arrow"))):
    name = os.path.basename(source)[: -len(".source.arrow")]
    with pa.memory_map(source) as stream:
        table = pa.ipc.open_file(stream).read_all()
    reference = os.path.join(directory, f"{name}.pyarrow-reference")
    pq.write_table(table, reference, row_group_size=1 << 30)
    ours, theirs = pq.ParquetFile(os.path.join(directory, f"{name}.parquet")), pq.ParquetFile(reference)
    if ours.metadata.num_columns != theirs.metadata.num_columns:
        footer_mismatches.append(f"{name}: {ours.metadata.num_columns} leaf columns, pyarrow {theirs.metadata.num_columns}")
        continue
    for i in range(ours.metadata.num_columns):
        a, b = ours.schema.column(i), theirs.schema.column(i)
        for what in ["path", "physical_type", "logical_type", "converted_type", "max_definition_level", "max_repetition_level"]:
            if str(getattr(a, what)) != str(getattr(b, what)):
                footer_mismatches.append(f"{name} {a.path}: {what} {getattr(a, what)} vs pyarrow {getattr(b, what)}")
        sa, sb = ours.metadata.row_group(0).column(i).statistics, theirs.metadata.row_group(0).column(i).statistics
        ca, cb = ours.metadata.row_group(0).column(i), theirs.metadata.row_group(0).column(i)
        if ca.num_values != cb.num_values:
            footer_mismatches.append(f"{name} {a.path}: num_values {ca.num_values} vs pyarrow {cb.num_values}")
        if sa.null_count != sb.null_count:
            footer_mismatches.append(f"{name} {a.path}: null_count {sa.null_count} vs pyarrow {sb.null_count}")
        if sa.has_min_max != sb.has_min_max or (sa.has_min_max and (sa.min != sb.min or sa.max != sb.max)):
            footer_mismatches.append(f"{name} {a.path}: bounds {sa.min if sa.has_min_max else None}..{sa.max if sa.has_min_max else None} "
                                     f"vs pyarrow {sb.min if sb.has_min_max else None}..{sb.max if sb.has_min_max else None}")
for m in footer_mismatches:
    print("FOOTER", m, flush=True)

# Field ids where the writer put them: every named field, the list element and the map key and value included.
ids_path = os.path.join(directory, "field_ids.parquet")
if os.path.exists(ids_path):
    found = {}
    def walk(field, prefix):
        key = prefix + field.name
        if field.metadata and b"PARQUET:field_id" in field.metadata:
            found[key] = int(field.metadata[b"PARQUET:field_id"])
        t = field.type
        if pa.types.is_map(t):
            walk(t.key_field, key + "."); walk(t.item_field, key + ".")
        elif pa.types.is_list(t) or pa.types.is_large_list(t):
            walk(t.value_field, key + ".")
        elif pa.types.is_struct(t):
            for k in range(t.num_fields):
                walk(t.field(k), key + ".")
    for f in pq.read_schema(ids_path):
        walk(f, "")
    wanted = {"li": 2, "li.element": 3, "st": 4, "st.a": 5, "mp": 6, "mp.key": 7, "mp.value": 8, "x": 9}
    if found != wanted:
        footer_mismatches.append(f"field ids read by pyarrow {found}, written {wanted}")
        print("FOOTER", footer_mismatches[-1], flush=True)

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
print("interop done" if not mismatches and not footer_mismatches else "interop FAILED")
sys.exit(1 if mismatches or footer_mismatches else 0)
