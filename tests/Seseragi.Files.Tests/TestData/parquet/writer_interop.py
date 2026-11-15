"""Reads the Parquet files ParquetWriterInteropTests wrote with pyarrow, DuckDB and Python Polars.

Usage: python -I writer_interop.py <dir>

- For every <name>.source.arrow, pyarrow writes the same table and the two footers are compared column by column
  (schema shape, levels, value and null counts, bounds), and so are the page indexes (pyarrow writing one with the
  page rows of <name>.pages, when present): page locations' rows, null pages and counts, bounds, boundary order,
  level histograms where we write them, value bytes) and the chunks' size statistics; for the columns <name>.bloom
  lists, the Bloom filters (both sized for the chunk's distinct values, pyarrow's ndv only an upper bound: the same bits), and DuckDB's parquet_bloom_probe must
  rule out no value of the column.
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
import struct
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
# Compact Thrift, enough to read footers and page indexes: a struct is {field id: value}, a list a Python list.
# ----------------------------------------------------------------------
class Thrift:
    def __init__(self, data, pos=0):
        self.d, self.p = data, pos

    def varint(self):
        shift = result = 0
        while True:
            b = self.d[self.p]
            self.p += 1
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, t):
        if t in (1, 2):
            b = self.d[self.p]
            self.p += 1
            return b == 1
        if t == 3:
            self.p += 1
            return self.d[self.p - 1]
        if t in (4, 5, 6):
            return self.zigzag()
        if t == 7:
            self.p += 8
            return struct.unpack("<d", self.d[self.p - 8:self.p])[0]
        if t == 8:
            n = self.varint()
            self.p += n
            return bytes(self.d[self.p - n:self.p])
        if t in (9, 10):
            head = self.d[self.p]
            self.p += 1
            n, et = head >> 4, head & 0x0F
            if n == 15:
                n = self.varint()
            return [self.value(et) for _ in range(n)]
        if t == 12:
            return self.struct()
        raise ValueError(f"thrift type {t}")

    def struct(self):
        fields, last = {}, 0
        while True:
            head = self.d[self.p]
            self.p += 1
            if head == 0:
                return fields
            t, delta = head & 0x0F, head >> 4
            fid = last + delta if delta else self.zigzag()
            last = fid
            fields[fid] = (t == 1) if t in (1, 2) else self.value(t)


def footer(path):
    data = open(path, "rb").read()
    n = struct.unpack("<i", data[-8:-4])[0]
    return data, Thrift(data, len(data) - 8 - n).struct()


def bloom_filters(path):
    """Per leaf of row group 0: (header, bitset) of its Bloom filter, or None."""
    data, meta = footer(path)
    out = []
    for chunk in meta[4][0][1]:
        offset = chunk[3].get(14)
        if offset is None:
            out.append(None)
            continue
        reader = Thrift(data, offset)
        header = reader.struct()
        out.append((header, data[reader.p:reader.p + header[1]]))
    return out


def page_indexes(path):
    """Per leaf of row group 0: (column index or None, offset index or None, size statistics or None)."""
    data, meta = footer(path)
    out = []
    for chunk in meta[4][0][1]:
        ci = Thrift(data, chunk[6]).struct() if 6 in chunk else None
        oi = Thrift(data, chunk[4]).struct() if 4 in chunk else None
        out.append((ci, oi, chunk[3].get(16)))
    return out


# ----------------------------------------------------------------------
# Footers against pyarrow's for the same table (<name>.source.arrow): schema shape, levels, value and null counts, bounds
# ----------------------------------------------------------------------
footer_mismatches = []
for source in sorted(glob.glob(os.path.join(directory, "*.source.arrow"))):
    name = os.path.basename(source)[: -len(".source.arrow")]
    with pa.memory_map(source) as stream:
        table = pa.ipc.open_file(stream).read_all()
    reference = os.path.join(directory, f"{name}.pyarrow-reference")
    pages_file = os.path.join(directory, f"{name}.pages")
    page_rows = int(open(pages_file).read()) if os.path.exists(pages_file) else None
    bloom_file = os.path.join(directory, f"{name}.bloom")
    bloomed = open(bloom_file).read().split(",") if os.path.exists(bloom_file) else []
    pq.write_table(table, reference, row_group_size=1 << 30, write_page_index=page_rows is not None,
                   **({"max_rows_per_page": page_rows} if page_rows else {}),
                   **({"bloom_filter_options": {c: {"ndv": 1 << 20, "fpp": 0.01} for c in bloomed}} if bloomed else {}))
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
        # Raw (physical) bounds: logical ones overflow Python's types at the extremes (dates of ±2^31 days).
        if sa.has_min_max != sb.has_min_max or (sa.has_min_max and (sa.min_raw != sb.min_raw or sa.max_raw != sb.max_raw)):
            footer_mismatches.append(f"{name} {a.path}: bounds {sa.min_raw if sa.has_min_max else None}..{sa.max_raw if sa.has_min_max else None} "
                                     f"vs pyarrow {sb.min_raw if sb.has_min_max else None}..{sb.max_raw if sb.has_min_max else None}")
    if bloomed:
        ours_path = os.path.join(directory, f"{name}.parquet")
        for i, (a, b) in enumerate(zip(bloom_filters(ours_path), bloom_filters(reference))):
            path = ours.schema.column(i).path
            if (a is not None) != (path in bloomed) or (b is not None) != (path in bloomed):
                footer_mismatches.append(f"{name} {path}: Bloom filter {a is not None} (pyarrow {b is not None}), asked for {path in bloomed}")
            elif a is not None and (a[0] != b[0] or a[1] != b[1]):
                footer_mismatches.append(f"{name} {path}: Bloom filter header {a[0]} vs pyarrow {b[0]}, bits {'equal' if a[1] == b[1] else 'differ'}")
        # DuckDB's probe must not rule out a value the column holds (and should rule out most it does not).
        written = pq.read_table(ours_path)
        for column in [c for c in bloomed if c in ("i32", "i64", "str")]:  # dates of ±2^31 days overflow Python
            values = sorted({v for v in written.column(column).to_pylist() if v is not None}, key=str)[:40]
            literal = (lambda v: "'" + v.replace("'", "''") + "'") if column == "str" else str
            for v in values:
                excluded = duckdb.sql(f"select bloom_filter_excludes from parquet_bloom_probe('{ours_path}', '{column}', {literal(v)})").fetchall()
                if any(e for (e,) in excluded):
                    footer_mismatches.append(f"{name} {column}: DuckDB's Bloom probe rules out {v!r}, which the column holds")
            if column in ("i32", "i64"):
                absent = [x for x in range(10_000_000, 10_000_200) if x not in set(written.column(column).to_pylist())]
                ruled = sum(any(e for (e,) in duckdb.sql(f"select bloom_filter_excludes from parquet_bloom_probe('{ours_path}', '{column}', {x})").fetchall()) for x in absent)
                if ruled < len(absent) * 0.9:
                    footer_mismatches.append(f"{name} {column}: DuckDB's Bloom probe rules out only {ruled} of {len(absent)} absent values")
    if page_rows is None:
        continue
    # Page indexes and size statistics: pyarrow writes every histogram; we write those that say more than the null
    # count (repetition levels below lists, definition levels deeper than one), so only ours are compared.
    for i, ((ci, oi, ss), (pci, poi, pss)) in enumerate(zip(page_indexes(os.path.join(directory, f"{name}.parquet")), page_indexes(reference))):
        path, max_def, max_rep = ours.schema.column(i).path, ours.schema.column(i).max_definition_level, ours.schema.column(i).max_repetition_level
        def differ(what, a, b):
            if a != b:
                footer_mismatches.append(f"{name} {path}: {what} {a!r:.200} vs pyarrow {b!r:.200}")
        if oi is None or poi is None:
            differ("offset index", oi is not None, poi is not None)
            continue
        differ("page first rows", [p[3] for p in oi[1]], [p[3] for p in poi[1]])
        differ("page value bytes", oi.get(2), poi.get(2))
        differ("column index", ci is not None, pci is not None)
        if ci is not None and pci is not None:
            for fid, what in [(1, "null pages"), (2, "page lower bounds"), (3, "page upper bounds"), (4, "boundary order"), (5, "page null counts")]:
                differ(what, ci.get(fid), pci.get(fid))
            differ("page repetition histograms", ci.get(6), pci.get(6) if max_rep > 0 else None)
            differ("page definition histograms", ci.get(7), pci.get(7) if max_def > 1 else None)
        ss, pss = ss or {}, pss or {}
        differ("value bytes", ss.get(1), pss.get(1))
        differ("repetition histogram", ss.get(2), pss.get(2) if max_rep > 0 else None)
        differ("definition histogram", ss.get(3), pss.get(3) if max_def > 1 else None)

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
