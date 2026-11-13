"""Writes the flat type matrix: every physical and logical type pyarrow, DuckDB and Polars write, under each
encoding, page version and codec they offer (no DELTA_* or BYTE_STREAM_SPLIT: those come later).

Usage: python -I write_matrix.py <output directory>
"""
import datetime as dt
import decimal
import sys
import uuid

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
N = 300


def nullable(values, every=7):
    return [None if i % every == 3 else v for i, v in enumerate(values)]


def ints(lo, hi):
    span = hi - lo
    return [lo + (i * 2654435761) % (span + 1) if i % 11 else (lo if i % 2 else hi) for i in range(N)]


def floats():
    special = [0.0, -0.0, float("inf"), float("-inf"), float("nan"), 1.5e-300, -2.5e300]
    return [special[i % len(special)] if i % 5 == 0 else (i - 150) / 7.0 for i in range(N)]


def strings():
    words = ["", "a", "héllo", "日本語", "emoji 🎉", "x" * 40, "tab\tnew\nline", "\u0000nul"]
    return [words[i % len(words)] + (str(i) if i % 3 else "") for i in range(N)]


def decimals(precision, scale):
    top = 10 ** precision - 1
    ctx = decimal.Context(prec=100)
    return [decimal.Decimal(((i * 7919 ** 5) % (top + 1)) * (-1 if i % 2 else 1)).scaleb(-scale, ctx) if i % 13 else decimal.Decimal(top).scaleb(-scale, ctx) for i in range(N)]


base = dt.datetime(2024, 2, 29, 12, 34, 56, 789012)
stamps = [base + dt.timedelta(days=(i - 150) * 397, microseconds=i * 1001) for i in range(N)]
stamps[0] = dt.datetime(1, 1, 1)
stamps[1] = dt.datetime(9999, 12, 31, 23, 59, 59, 999999)

table = pa.table({
    "bool": pa.array(nullable([i % 3 == 0 for i in range(N)]), pa.bool_()),
    "bool_required": pa.array([i % 2 == 0 for i in range(N)], pa.bool_()),
    "i8": pa.array(nullable(ints(-128, 127)), pa.int8()),
    "i16": pa.array(nullable(ints(-32768, 32767)), pa.int16()),
    "i32": pa.array(nullable(ints(-2**31, 2**31 - 1)), pa.int32()),
    "i32_required": pa.array(ints(-2**31, 2**31 - 1), pa.int32()),
    "i64": pa.array(nullable(ints(-2**63, 2**63 - 1)), pa.int64()),
    "u8": pa.array(nullable(ints(0, 255)), pa.uint8()),
    "u16": pa.array(nullable(ints(0, 65535)), pa.uint16()),
    "u32": pa.array(nullable(ints(0, 2**32 - 1)), pa.uint32()),
    "u64": pa.array(nullable(ints(0, 2**64 - 1)), pa.uint64()),
    "f16": pa.array(nullable(floats()), pa.float32()).cast(pa.float16(), safe=False),
    "f32": pa.array(nullable(floats()), pa.float32()),
    "f64": pa.array(nullable(floats()), pa.float64()),
    "dec_9_2": pa.array(nullable(decimals(9, 2)), pa.decimal128(9, 2)),
    "dec_18_4": pa.array(nullable(decimals(18, 4)), pa.decimal128(18, 4)),
    "dec_38_10": pa.array(nullable(decimals(38, 10)), pa.decimal128(38, 10)),
    "dec_50_5": pa.array(nullable(decimals(50, 5)), pa.decimal256(50, 5)),
    "date": pa.array(nullable([dt.date(1, 1, 1) if i == 0 else (dt.date(1970, 1, 1) + dt.timedelta(days=(i - 150) * 401)) for i in range(N)]), pa.date32()),
    "time_ms": pa.array(nullable([(i * 287_123) % 86_400_000 for i in range(N)]), pa.time32("ms")),
    "time_us": pa.array(nullable([(i * 287_123_457) % 86_400_000_000 for i in range(N)]), pa.time64("us")),
    "time_ns": pa.array(nullable([(i * 287_123_457_311) % 86_400_000_000_000 for i in range(N)]), pa.time64("ns")),
    "ts_ms": pa.array(nullable(stamps), pa.timestamp("ms")),
    "ts_us": pa.array(nullable(stamps), pa.timestamp("us")),
    "ts_ns": pa.array(nullable(stamps[2:] + stamps[2:4]), pa.timestamp("ns")),
    "ts_us_utc": pa.array(nullable(stamps), pa.timestamp("us", "UTC")),
    "ts_ms_tokyo": pa.array(nullable(stamps), pa.timestamp("ms", "Asia/Tokyo")),
    "dur_us": pa.array(nullable([dt.timedelta(microseconds=(i - 150) * 123_456_789) for i in range(N)]), pa.duration("us")),
    "str": pa.array(nullable(strings()), pa.string()),
    "str_required": pa.array(strings(), pa.string()),
    "large_str": pa.array(nullable(strings()), pa.large_string()),
    "bin": pa.array(nullable([bytes([i % 256]) * (i % 9) for i in range(N)]), pa.binary()),
    "fixed": pa.array(nullable([bytes([(i + k) % 256 for k in range(5)]) for i in range(N)]), pa.binary(5)),
    "uuid": pa.array(nullable([uuid.UUID(int=i * 0x1234567890ABCDEF1234567).bytes for i in range(N)]), pa.binary(16)).cast(pa.uuid()),
    "json": pa.array(nullable(['{"a":%d}' % i for i in range(N)]), pa.string()).cast(pa.json_()),
    "dict_str": pa.array(nullable([["red", "green", "blue"][i % 3] for i in range(N)])).dictionary_encode(),
    "all_null": pa.nulls(N, pa.int32()),
    "null_type": pa.nulls(N),
})


def write(name, t=table, **kw):
    pq.write_table(t, f"{out}/{name}.parquet", **kw)


# pyarrow: encodings, page versions, codecs, page and row group sizes, checksums, page index
write("pa_plain_v1", use_dictionary=False, data_page_version="1.0", compression="none")
write("pa_dict_v1_snappy", use_dictionary=True, data_page_version="1.0", compression="snappy")
write("pa_dict_v2_zstd", use_dictionary=True, data_page_version="2.0", compression="zstd")
write("pa_plain_v2_gzip", use_dictionary=False, data_page_version="2.0", compression="gzip")
write("pa_plain_v1_brotli", use_dictionary=False, compression="brotli")
write("pa_plain_v2_lz4raw", use_dictionary=False, data_page_version="2.0", compression="lz4")
write("pa_pages_rowgroups", use_dictionary=True, data_page_size=256, row_group_size=70, write_page_checksum=True,
      write_page_index=True, compression="snappy", dictionary_pagesize_limit=64)
write("pa_rle_bool", t=table.select(["bool", "bool_required", "i32"]), use_dictionary=False, column_encoding={"bool": "RLE", "bool_required": "RLE", "i32": "PLAIN"})
write("pa_int96", use_deprecated_int96_timestamps=True)
write("pa_no_arrow_schema", store_schema=False)
write("pa_format_1_0", version="1.0", compression="snappy")
write("pa_decimal_as_int", store_decimal_as_integer=True)
write("pa_empty", t=table.slice(0, 0))
write("pa_one_row", t=table.slice(5, 1))

# DuckDB: its own type names, codecs and parquet-cpp-free writer
con = duckdb.connect()
con.execute("""
CREATE TABLE t AS SELECT
  i % 3 = 0 AS b, (i % 256 - 128)::TINYINT AS i8, (i * 37 % 65536 - 32768)::SMALLINT AS i16,
  (i::HUGEINT * 2654435761 % 4294967296 - 2147483648)::INTEGER AS i32, (i::HUGEINT * 6364136223846793005 % 18446744073709551616 - 9223372036854775808)::BIGINT AS i64,
  (i % 256)::UTINYINT AS u8, (i * 37 % 65536)::USMALLINT AS u16, (i::HUGEINT * 2654435761 % 4294967296)::UINTEGER AS u32,
  (i::HUGEINT * 11400714819323198485 % 18446744073709551616)::UBIGINT AS u64, (i / 7.0)::FLOAT AS f32, (i / 7.0)::DOUBLE AS f64,
  (i * 1.25)::DECIMAL(4, 1) AS d4, (i * 123.45)::DECIMAL(9, 2) AS d9, (i * 12345.678)::DECIMAL(18, 3) AS d18,
  (i * 1234567890123.45678)::DECIMAL(38, 5) AS d38, DATE '2024-02-29' + (i * 37)::INTEGER AS date,
  TIME '00:00:00' + INTERVAL (i * 287) SECOND AS time, TIMESTAMP '2024-02-29 12:34:56.789012' + INTERVAL (i * 97) HOUR AS ts,
  (TIMESTAMP '2024-02-29 12:34:56' + INTERVAL (i) DAY)::TIMESTAMP_MS AS ts_ms,
  (TIMESTAMP '2024-02-29 12:34:56' + INTERVAL (i) DAY)::TIMESTAMP_NS AS ts_ns,
  (TIMESTAMPTZ '2024-02-29 12:34:56+00' + INTERVAL (i) MINUTE) AS tstz,
  INTERVAL (i) DAY + INTERVAL (i * 3) MONTH + INTERVAL (i * 7) MILLISECOND AS ival,
  'v' || i::VARCHAR AS s, ('\\x' || lpad(to_hex(i % 256), 2, '0'))::BLOB AS blob,
  ('00000000-0000-0000-0000-' || lpad(i::VARCHAR, 12, '0'))::UUID AS id,
  (['red', 'green', 'blue'][i % 3 + 1])::ENUM('red', 'green', 'blue') AS e
FROM range(300) r(i)
""")
con.execute("UPDATE t SET i32 = NULL, s = NULL, d38 = NULL, ts = NULL WHERE i8 % 7 = 0")
for codec in ["uncompressed", "snappy", "gzip", "zstd", "lz4", "brotli"]:
    con.execute(f"COPY t TO '{out}/duckdb_{codec}.parquet' (FORMAT parquet, COMPRESSION {codec})")

# Polars: its writer (statistics, dictionary pages), Categorical and Enum, nanosecond times, codecs
df = pl.from_arrow(table.drop(["f16", "dec_50_5", "uuid", "json", "null_type", "dict_str", "fixed", "ts_ms_tokyo"])).with_columns(
    pl.Series("cat", [["red", "green", "blue"][i % 3] for i in range(N)], pl.Categorical),
    pl.Series("enum", [["red", "green", "blue"][i % 3] for i in range(N)], pl.Enum(["red", "green", "blue"])),
)
df.write_parquet(f"{out}/polars_zstd.parquet")
df.write_parquet(f"{out}/polars_lz4.parquet", compression="lz4", row_group_size=100, data_page_size=512)
df.write_parquet(f"{out}/polars_uncompressed.parquet", compression="uncompressed", statistics=False)
print("written")
