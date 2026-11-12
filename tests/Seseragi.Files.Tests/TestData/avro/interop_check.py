"""Checks Avro files written by the C# writer with the reference readers (fastavro, Python polars).

Usage: python -I interop_check.py <wide.avro> <polars.avro> [maps.avro]
  wide.avro    AvroWriteTests.Wide(0, 300), snappy: every type; checked with fastavro on raw (non-logical) values.
  polars.avro  PolarsSubset(): the types the Python polars reader supports; checked with polars.read_avro.
  maps.avro    AvroInteropTests.Maps(): maps with non-string keys (arrays of key/value records, logicalType map);
               checked with fastavro, and with DuckDB when it is installed.
Exits non-zero with a message on the first mismatch. Needs fastavro, cramjam and polars.
"""
import decimal
import datetime as dt
import sys

import fastavro
import fastavro.read
import polars as pl

failures = []


def check(what, actual, expected):
    if actual != expected:
        failures.append(f"{what}: expected {expected!r}, got {actual!r}")


def maybe(k, v):
    return None if k % 5 == 0 else v


def be_decimal_bytes(unscaled, width):
    raw = unscaled.to_bytes(width, "big", signed=True)
    sign = b"\xff" if unscaled < 0 else b"\x00"
    while len(raw) > 1 and raw[:1] == sign and ((raw[1] ^ sign[0]) & 0x80) == 0:
        raw = raw[1:]
    return raw


# ---- wide.avro with fastavro, logical types disabled so raw encodings are compared
fastavro.read.LOGICAL_READERS.clear()
with open(sys.argv[1], "rb") as f:
    r = fastavro.reader(f)
    check("codec", r.codec, "snappy")
    rows = list(r)
check("row count", len(rows), 300)
for k, row in enumerate(rows):
    check(f"i8[{k}]", row["i8"], maybe(k, k % 256 - 128))
    check(f"i16[{k}]", row["i16"], k - 30000)
    check(f"i32[{k}]", row["i32"], k * 7 - 1)
    check(f"i64[{k}]", row["i64"], maybe(k, k * 1_000_000_000_007))
    check(f"u32[{k}]", row["u32"], 2**32 - 1 - k)
    check(f"u64[{k}]", row["u64"], 2**63 - 1 - k)
    check(f"f16[{k}]", row["f16"], k / 8.0)
    check(f"b[{k}]", row["b"], k % 3 == 0)
    check(f"s[{k}]", row["s"], None if k % 4 == 0 else f"s{k}✓")
    check(f"ls[{k}]", row["ls"], f"large{k}")
    sv = None if k % 7 == 0 else (f"v{k}" if k % 2 == 0 else f"a longer view string that is not inlined {k}")
    check(f"sv[{k}]", row["sv"], sv)
    check(f"bin[{k}]", row["bin"], bytes([k & 255, (k >> 8) & 255]))
    check(f"fsb[{k}]", row["fsb"], k.to_bytes(4, "little"))
    check(f"dec[{k}]", row["dec"], be_decimal_bytes((k - 500) * 1125, 16))
    check(f"dec256[{k}]", row["dec256"], be_decimal_bytes((k - 500) * 123456789, 32))
    check(f"date[{k}]", row["date"], k - 1000)
    check(f"tms[{k}]", row["tms"], k * 1000 + 7)
    check(f"tus[{k}]", row["tus"], k * 1_000_000 + 7)
    check(f"ts_utc[{k}]", row["ts_utc"], 1_600_000_000_000_000 + k)
    check(f"ts_ns_local[{k}]", row["ts_ns_local"], maybe(k, 1_600_000_000_000_000_000 + k))
    check(f"dur[{k}]", row["dur"], k * -1000)
    lst = None if k % 6 == 0 else [None if j == 1 else k * 10 + j for j in range(k % 4)]
    check(f"list[{k}]", row["list"], lst)
    point = None if k % 9 == 0 else {"a": k, "b": None if k % 2 == 0 else f"p{k}"}
    check(f"point[{k}]", row["point"], point)
    check(f"tags[{k}]", row["tags"], {f"k{j}": (None if j == 2 else k + j) for j in range(k % 3)})
    check(f"color[{k}]", row["color"], maybe(k, ["red", "green", "blue"][k % 3]))

# ---- polars.avro with the Python polars reader
df = pl.read_avro(sys.argv[2])
expected = pl.DataFrame(
    {
        "i": [1, None, -3],
        "l": [10**12, 2, None],
        "d": [0.5, None, -2.25],
        "f": [1.5, 2.0, None],
        "b": [True, False, None],
        "s": ["a", None, "雷"],
        "bin": [b"\x00", b"", None],
        "date": [dt.date(2024, 1, 2), None, dt.date(1969, 12, 31)],
        "ts": [dt.datetime(2024, 1, 2, 3, 4, 5, 6), None, dt.datetime(1970, 1, 1)],
        "tsz": [dt.datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=dt.timezone.utc), None, dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)],
        # Asia/Tokyo is kept in a schema attribute that other readers ignore: the same instants, in UTC.
        "tsk": [dt.datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=dt.timezone.utc), None, dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)],
        "dec": [decimal.Decimal("1.25"), None, decimal.Decimal("-3.50")],
        "list": [[1, None], None, []],
        "st": [{"x": 1, "y": "p"}, None, {"x": 3, "y": None}],
    },
    schema={
        "i": pl.Int32, "l": pl.Int64, "d": pl.Float64, "f": pl.Float32, "b": pl.Boolean, "s": pl.String,
        "bin": pl.Binary, "date": pl.Date, "ts": pl.Datetime("us"), "tsz": pl.Datetime("us", "UTC"), "tsk": pl.Datetime("us", "UTC"),
        "dec": pl.Decimal(10, 2), "list": pl.List(pl.Int64), "st": pl.Struct({"x": pl.Int32, "y": pl.String}),
    },
)
check("polars schema", dict(df.schema), dict(expected.schema))
if dict(df.schema) == dict(expected.schema):
    check("polars rows", df.to_dicts(), expected.to_dicts())

# ---- maps.avro: non-string map keys as arrays of key/value records
if len(sys.argv) > 3:
    with open(sys.argv[3], "rb") as f:
        r = fastavro.reader(f)
        m_schema = [x for x in r.writer_schema["fields"] if x["name"] == "m"][0]["type"][1]
        check("maps logicalType", (m_schema["type"], m_schema.get("logicalType")), ("array", "map"))
        rows = list(r)
    check("maps fastavro m", [row["m"] for row in rows],
          [[{"key": 1, "value": "a"}, {"key": 2, "value": None}], None, []])
    check("maps fastavro big", [row["big"] for row in rows],
          [[{"key": 5, "value": 6}], [{"key": -(2**63), "value": 0}], []])
    try:
        import duckdb
    except ImportError:
        duckdb = None
    if duckdb is not None:
        con = duckdb.connect()
        con.execute("INSTALL avro; LOAD avro;")
        types = {name: t for name, t, *_ in con.execute("DESCRIBE SELECT * FROM read_avro(?)", [sys.argv[3]]).fetchall()}
        check("maps duckdb types", (types["m"], types["big"]), ("MAP(INTEGER, VARCHAR)", "MAP(BIGINT, BIGINT)"))
        check("maps duckdb rows", con.execute("SELECT m, big FROM read_avro(?)", [sys.argv[3]]).fetchall(),
              [({1: "a", 2: None}, {5: 6}), (None, {-(2**63): 0}), ({}, {})])

if failures:
    print("\n".join(failures[:20]))
    sys.exit(1)
print("interop ok")
