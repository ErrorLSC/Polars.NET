# make_write_fixtures.py: what Python Polars writes, for JsonWriterOracleTests. written.arrow holds columns of every
# type Polars writes to JSON, edge values and nulls included (as Polars hands them to Arrow: string views, large lists,
# dictionaries); written.ndjson and written.json are Polars' write_ndjson and write_json of it. Int128 is not in the
# Arrow file (Polars writes a 128-bit integer type other Arrow readers refuse): its expectations are in the tests.
# Run: ~/.cache/seseragi/excel-venv/bin/python -I make_write_fixtures.py  (Polars 2.1)
import datetime as dt, decimal, os
import polars as pl

here = os.path.dirname(os.path.abspath(__file__))
rows = 12

def col(values, dtype=None):
    values = (values * rows)[:rows] if len(values) < rows else values[:rows]
    return pl.Series(values, dtype=dtype, strict=False)

floats = [0.0, -0.0, 0.1 + 0.2, 1e-5, 9.99e-6, 1e-6, 1e15, 9999999999999998.0, 1e16, 1.5e16, 1.7976931348623157e308, 5e-324]
f32 = [1.1, 3.4e38, 1e-7, 1e-5, 16777216.0, -0.0, 0.1, 1e16, None, 2.5, 7.0, 1.4e-45]
strings = ["", "plain", "quote\" back\\ slash/", "\t\n\r\b\f", "".join(chr(c) for c in range(0x20)), "\x7f  ",
           "é😀中", "<>&'", None, "a" * 300, "\\u0041", "end"]
ms = [0, 1, -1, 1_700_000_000_123, -86_400_000, 1_000, 253_402_300_799_999, -62_135_596_800_000, None, 5, 120, 1_500]
ns = [0, 1, -1, 1_700_000_000_123_456_789, 1_000, 1_000_000, 999_999_999, -999_999_999, None, 123_000_000, 123_456_000, 86_400_000_000_000]
zoned = [dt.datetime(2024, 1, 2, 3, 4, 5), dt.datetime(2024, 7, 2, 3, 4, 5), dt.datetime(2024, 3, 10, 6, 59, 59),
         dt.datetime(2024, 3, 10, 7, 0, 0), dt.datetime(2024, 11, 3, 5, 30), dt.datetime(2024, 11, 3, 6, 30),
         dt.datetime(1969, 12, 31, 23, 59, 59), dt.datetime(1, 1, 1), dt.datetime(9999, 12, 31, 23, 59, 59), None,
         dt.datetime(1900, 1, 1), dt.datetime(2038, 1, 19, 3, 14, 8)]

def zone(unit, tz):
    # Nanoseconds reach 1677 to 2262 only.
    values = [v.replace(year=1700) if v and unit == "ns" and v.year < 1700 else v.replace(year=2200) if v and unit == "ns" and v.year > 2200 else v for v in zoned]
    return pl.Series(values, dtype=pl.Datetime(unit, "UTC")).dt.convert_time_zone(tz)

df = pl.DataFrame({
    "i8": col([-128, 127, 0, None], pl.Int8), "i16": col([-32768, 32767, None], pl.Int16),
    "i32": col([-2**31, 2**31 - 1, None], pl.Int32), "i64": col([-2**63, 2**63 - 1, 0, None], pl.Int64),
    "u8": col([0, 255, None], pl.UInt8), "u16": col([65535, None], pl.UInt16), "u32": col([2**32 - 1, None], pl.UInt32),
    "u64": col([0, 2**64 - 1, None], pl.UInt64),
    "f64": col(floats, pl.Float64), "f64_special": col([float("nan"), float("inf"), float("-inf"), None, 1.0, -2.5], pl.Float64),
    "f32": col(f32, pl.Float32), "bool": col([True, False, None], pl.Boolean),
    "s": col(strings, pl.String), "cat": col(["a", "b", None, "a"], pl.Categorical), "enum": col(["x", "y", None], pl.Enum(["x", "y"])),
    "date": col([dt.date(1, 1, 1), dt.date(9999, 12, 31), dt.date(1969, 12, 31), dt.date(1970, 1, 1), None, dt.date(2024, 2, 29)], pl.Date),
    "dt_ms": col(ms, pl.Int64).cast(pl.Datetime("ms")), "dt_us": col([v * 1000 if v is not None else None for v in ms], pl.Int64).cast(pl.Datetime("us")),
    "dt_ns": col(ns, pl.Int64).cast(pl.Datetime("ns")),
    "utc_us": zone("us", "UTC"), "tokyo_ms": zone("ms", "Asia/Tokyo"), "ny_us": zone("us", "America/New_York"),
    "kolkata_ns": zone("ns", "Asia/Kolkata"), "lord_howe_us": zone("us", "Australia/Lord_Howe"),
    "dur_ms": col(ms, pl.Int64).cast(pl.Duration("ms")), "dur_us": col([v * 1000 if v is not None else None for v in ms], pl.Int64).cast(pl.Duration("us")),
    "dur_ns": col(ns, pl.Int64).cast(pl.Duration("ns")),
    "time": col([0, 1, 1_000, 1_000_000, 86_399_999_999_999, 43_200_000_000_000, None, 123_456_789], pl.Int64).cast(pl.Time),
    "dec": col([decimal.Decimal("-1.50"), decimal.Decimal("0.00"), decimal.Decimal("123456789.12"), None, decimal.Decimal("0.01")], pl.Decimal(12, 2)),
    "dec0": col([decimal.Decimal("-5"), decimal.Decimal("0"), None, decimal.Decimal("99999")], pl.Decimal(5, 0)),
    "dec38": col([decimal.Decimal("1.0000000000000000000000000000000000001"), decimal.Decimal("-0.0000000000000000000000000000000000001"), None], pl.Decimal(38, 37)),
    "list": col([[1, None, 3], [], None, [0]], pl.List(pl.Int64)),
    "list_s": col([["a", None], None, [], ["\"q\""]], pl.List(pl.String)),
    "nested": col([[[1], [], None], None, [[None]]], pl.List(pl.List(pl.Int32))),
    "array": col([[1, 2], None, [None, 4]], pl.Array(pl.Int16, 2)),
    "struct": col([{"x": 1, "y": "a", "z": [1.5]}, None, {"x": None, "y": None, "z": None}, {"x": 2, "y": "b", "z": []}],
                  pl.Struct({"x": pl.Int64, "y": pl.String, "z": pl.List(pl.Float64)})),
    "deep": col([{"a": {"b": {"c": [{"d": 1}]}}}, None, {"a": None}], pl.Struct({"a": pl.Struct({"b": pl.Struct({"c": pl.List(pl.Struct({"d": pl.Int8}))})})})),
    "null": pl.Series([None] * rows, dtype=pl.Null),
    "name \"esc\"\n\t\\é": col([1, None], pl.Int64),
})
df.write_ipc(os.path.join(here, "written.arrow"), compression="uncompressed")
df.write_ndjson(os.path.join(here, "written.ndjson"))
df.write_json(os.path.join(here, "written.json"))
# Empty frames: no rows.
df.clear().write_ipc(os.path.join(here, "written-empty.arrow"), compression="uncompressed")
df.clear().write_ndjson(os.path.join(here, "written-empty.ndjson"))
df.clear().write_json(os.path.join(here, "written-empty.json"))
print(df.schema)
