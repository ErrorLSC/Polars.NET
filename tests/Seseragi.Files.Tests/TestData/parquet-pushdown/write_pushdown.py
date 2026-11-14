"""Writes the pushdown test files and expected.json: python -I write_pushdown.py (pyarrow 26 and DuckDB 2.0).

types.parquet  3 row groups of 4 rows over many types; row group 0 holds small values, 1 middle, 2 large (row group 1
               holds NaN in f64; n is null in all of row group 2), so filters can be checked to keep exactly the row
               groups that hold matching rows.
lie_*.parquet  3 row groups x 10 rows (i int64 0..29, s string s00..s29) whose statistics of row group 0 LIE (they say
               100..200 and x..y) in one place each: the typed or the legacy fields, with or without column orders, and
               with old writers. A reader that prunes by the lying piece returns fewer rows than the truth.
               expected.json records how many rows pyarrow returns for each file and filter: the statistics rules
               pyarrow follows (Arrow C++), which ours follow too.
flat_v1_pages.parquet / flat_v2_pages.parquet  flat columns in many small v1 / v2 pages.
nested_v2_pages.parquet  lists and structs in many small v2 pages, for skipping pages by their headers.
nulls_unknown.parquet  n int64 with nulls, its null counts removed (absent is not zero).
"""
import json
import os
import subprocess
import sys
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
sys.dont_write_bytecode = True
import thrift_edit as te  # noqa: E402


def types():
    def rows(lo):  # 4 rows of each column, starting at lo (0, 1, 2 per row group)
        return lo * 4, lo * 4 + 4

    cols = {name: [] for name in ["i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "f16", "d9", "d18",
                                  "d30", "b", "s", "dt", "ts_ms", "ts_us", "ts_ns", "ts_utc", "t_ms", "t_us", "t_ns", "n",
                                  "st", "li"]}
    for g in range(3):
        for k in range(4):
            v = g * 10 + k  # 0..3, 10..13, 20..23
            cols["i8"].append(v - 15)
            cols["i16"].append(v * 100 - 1500)
            cols["i32"].append(v * 1000 - 15000)
            cols["i64"].append(v * 10**12 - 15 * 10**12)
            cols["u8"].append(v * 10)
            cols["u16"].append(v * 2000)
            cols["u32"].append(v if g < 2 else 2**31 + v)          # above the signed range in row group 2
            cols["u64"].append(v if g < 2 else 2**63 + v)
            cols["f32"].append(v - 10.5)
            cols["f64"].append(float("nan") if (g, k) == (1, 2) else v - 10.25)
            cols["f16"].append(v - 10.5)
            cols["d9"].append(Decimal(v * 100 - 1000) / 100)       # -10.00 .. 13.00 by 1
            cols["d18"].append(Decimal(v) - Decimal("10.0001"))
            cols["d30"].append(Decimal(v * 10**20) / 1000)
            cols["b"].append(g == 1 or (g == 2 and k % 2 == 0))
            cols["s"].append(f"s{v:02d}" + ("é" if g == 2 else ""))
            cols["dt"].append(date(2020, 1, 1 + v))
            cols["ts_ms"].append(datetime(2020, 1, 1, 0, 0, v, 500_000))
            cols["ts_us"].append(datetime(2020, 1, 1, 0, 0, v, 123_456))
            cols["ts_ns"].append(datetime(2020, 1, 1, 0, 0, v))
            cols["ts_utc"].append(datetime(2020, 1, 1, v, tzinfo=timezone.utc))
            cols["t_ms"].append(time(0, 0, v, 250_000))
            cols["t_us"].append(time(0, 0, v, 125))
            cols["t_ns"].append(time(0, v, 0))
            cols["n"].append(None if g == 2 else (v if k != 1 else None))
            cols["st"].append({"a": v, "b": f"b{v:02d}"})
            cols["li"].append([v, v + 100])
    schema = pa.schema([
        ("i8", pa.int8()), ("i16", pa.int16()), ("i32", pa.int32()), ("i64", pa.int64()),
        ("u8", pa.uint8()), ("u16", pa.uint16()), ("u32", pa.uint32()), ("u64", pa.uint64()),
        ("f32", pa.float32()), ("f64", pa.float64()), ("f16", pa.float16()),
        ("d9", pa.decimal128(9, 2)), ("d18", pa.decimal128(18, 4)), ("d30", pa.decimal128(30, 3)),
        ("b", pa.bool_()), ("s", pa.string()), ("dt", pa.date32()),
        ("ts_ms", pa.timestamp("ms")), ("ts_us", pa.timestamp("us")), ("ts_ns", pa.timestamp("ns")),
        ("ts_utc", pa.timestamp("us", tz="UTC")),
        ("t_ms", pa.time32("ms")), ("t_us", pa.time64("us")), ("t_ns", pa.time64("ns")),
        ("n", pa.int64()), ("st", pa.struct([("a", pa.int32()), ("b", pa.string())])), ("li", pa.list_(pa.int32())),
    ])
    table = pa.table({k: v for k, v in cols.items()}, schema=schema)
    pq.write_table(table, os.path.join(here, "types.parquet"), row_group_size=4, compression="none", store_decimal_as_integer=True)


BASE = os.path.join(here, "base.parquet")
I, S = 0, 1
I100, I200 = (100).to_bytes(8, "little", signed=True), (200).to_bytes(8, "little", signed=True)


def lies():
    t = pa.table({"i": pa.array(range(30), pa.int64()), "s": [f"s{n:02d}" for n in range(30)]})
    pq.write_table(t, BASE, row_group_size=10, compression="none", use_dictionary=False)

    def stats(f, c):
        return te.get(f.column_meta(0, c), 12)

    def typed(f):
        for c, lo, hi in ((I, I100, I200), (S, b"x", b"y")):
            te.put(stats(f, c), 6, te.BINARY, lo)
            te.put(stats(f, c), 5, te.BINARY, hi)

    def legacy(f):
        for c, lo, hi in ((I, I100, I200), (S, b"x", b"y")):
            s = stats(f, c)
            te.drop(s, 5)
            te.drop(s, 6)
            te.put(s, 2, te.BINARY, lo)
            te.put(s, 1, te.BINARY, hi)

    def no_orders(f):
        te.drop(f.meta, 7)

    def created_by(text):
        def edit(f):
            if text is None:
                te.drop(f.meta, 6)
            else:
                te.put(f.meta, 6, te.BINARY, text.encode())
        return edit

    variants = {
        "lie_typed": [typed],
        "lie_typed_noorders": [typed, no_orders],
        "lie_legacy": [legacy],
        "lie_legacy_noorders": [legacy, no_orders],
        "lie_legacy_mr17": [legacy, no_orders, created_by("parquet-mr version 1.7.0 (build abc)")],
        "lie_legacy_nocreatedby": [legacy, no_orders, created_by(None)],
    }
    for name, edits in variants.items():
        f = te.ParquetFile(BASE)
        for e in edits:
            e(f)
        f.save(os.path.join(here, name + ".parquet"))
    return ["base"] + list(variants)


def unknown_null_counts():
    """nulls_unknown.parquet: n int64 with a null in each of 3 row groups, every null_count removed."""
    t = pa.table({"n": pa.array([None, 1, 2, 3, None, 11, 12, 13, None, 21, 22, 23], pa.int64())})
    path = os.path.join(here, "nulls_unknown.parquet")
    pq.write_table(t, path, row_group_size=4, compression="none")
    f = te.ParquetFile(path)
    for g in range(3):
        te.drop(te.get(f.column_meta(g, 0), 12), 3)
    f.save(path)


def flat_pages():
    """flat_v1_pages.parquet, flat_v2_pages.parquet: flat columns (one dictionary-encoded, nulls) in many small pages."""
    n = 600
    t = pa.table({
        "id": pa.array(range(n), pa.int64()),
        "x": pa.array([None if i % 7 == 0 else i * 0.5 for i in range(n)], pa.float64()),
        "cat": pa.array([None if i % 9 == 0 else f"c{i % 10}" for i in range(n)], pa.string()),
        "b": pa.array([i % 3 == 0 for i in range(n)], pa.bool_()),
    })
    for version in ("1.0", "2.0"):
        pq.write_table(t, os.path.join(here, f"flat_v{version[0]}_pages.parquet"), row_group_size=250, data_page_version=version,
                       data_page_size=128, write_batch_size=16, compression="snappy", use_dictionary=["cat"])


def nested_v2_pages():
    """nested_v2_pages.parquet: lists, structs and a flat column in many small v2 pages (headers give their rows)."""
    n = 600
    t = pa.table({
        "id": pa.array(range(n), pa.int64()),
        "li": pa.array([None if i % 13 == 0 else list(range(i % 5)) for i in range(n)], pa.list_(pa.int32())),
        "st": pa.array([None if i % 11 == 0 else {"a": i, "s": f"v{i}"} for i in range(n)],
                       pa.struct([("a", pa.int64()), ("s", pa.string())])),
        "ls": pa.array([[{"x": i, "y": [i, i + 1]}] * (i % 3) for i in range(n)],
                       pa.list_(pa.struct([("x", pa.int32()), ("y", pa.list_(pa.int64()))]))),
    })
    pq.write_table(t, os.path.join(here, "nested_v2_pages.parquet"), row_group_size=250, data_page_version="2.0",
                   data_page_size=128, write_batch_size=16, compression="snappy")


FILTERS = {"i<5": [("i", "<", 5)], "s<s05": [("s", "<", "s05")], "i=3": [("i", "=", 3)], "s=s03": [("s", "=", "s03")]}

CHILD = r'''
import json, sys
import pyarrow.parquet as pq
print(pq.read_table(sys.argv[1], filters=json.loads(sys.argv[2])).num_rows)
'''


def main():
    types()
    unknown_null_counts()
    nested_v2_pages()
    flat_pages()
    expected = {}
    for name in lies():
        for fname, flt in FILTERS.items():
            out = subprocess.run([sys.executable, "-I", "-c", CHILD, os.path.join(here, name + ".parquet"), json.dumps(flt)],
                                 capture_output=True, text=True, check=True)
            expected[f"{name} {fname}"] = int(out.stdout.strip())
    with open(os.path.join(here, "expected.json"), "w") as f:
        json.dump({"pyarrow": pa.__version__, "rows": expected}, f, indent=1, sort_keys=True)
        f.write("\n")
    print(json.dumps(expected, indent=1))


main()
