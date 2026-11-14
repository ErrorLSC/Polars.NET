"""Writes the encoding matrix: DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT
over every type pyarrow allows them for, page versions v1 and v2, codecs, small pages; and DuckDB's v2 writer (its own
choice of these encodings). The table is the flat matrix's (write_matrix.py). Spark's parquet-mr v2 writer: see
write_encodings_spark.py.

Usage: python -I write_encodings.py <output directory>
"""
import importlib.util
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
# The flat matrix's table, without writing its files: load the module's definitions only.
spec = importlib.util.spec_from_file_location("matrix", os.path.join(os.path.dirname(__file__), "write_matrix.py"))
source = open(spec.origin).read().split("def write(")[0]
matrix = {"__name__": "matrix", "sys": sys}
sys_argv, sys.argv = sys.argv, [spec.origin, out]
exec(compile(source, spec.origin, "exec"), matrix)
sys.argv = sys_argv
table = matrix["table"]

# The columns each encoding takes (pyarrow refuses the others).
delta_ints = ["i8", "i16", "i32", "i32_required", "i64", "u8", "u16", "u32", "u64", "date", "time_ms", "time_us", "time_ns",
              "ts_ms", "ts_us", "ts_ns", "ts_us_utc", "dur_us"]
delta_strings = ["str", "str_required", "large_str", "bin", "json"]
delta_fixed = ["fixed", "uuid", "dec_38_10", "dec_50_5", "f16"]
split = ["f16", "f32", "f64", "i32", "i32_required", "i64", "u32", "u64", "fixed", "uuid", "dec_38_10"]


def write(name, columns, encoding, **kw):
    t = table.select(columns)
    pq.write_table(t, f"{out}/{name}.parquet", use_dictionary=False, column_encoding={c: encoding for c in columns}, **kw)


for version, codec in [("1.0", "none"), ("2.0", "snappy"), ("2.0", "zstd"), ("1.0", "zstd")]:
    v = "v1" if version == "1.0" else "v2"
    write(f"enc_dbp_{v}_{codec}", delta_ints, "DELTA_BINARY_PACKED", data_page_version=version, compression=codec)
    write(f"enc_dlba_{v}_{codec}", delta_strings, "DELTA_LENGTH_BYTE_ARRAY", data_page_version=version, compression=codec)
    write(f"enc_dba_{v}_{codec}", delta_strings + delta_fixed, "DELTA_BYTE_ARRAY", data_page_version=version, compression=codec)
    write(f"enc_bss_{v}_{codec}", split, "BYTE_STREAM_SPLIT", data_page_version=version, compression=codec)

# Decimals stored as integers take DELTA_BINARY_PACKED and BYTE_STREAM_SPLIT as INT32 / INT64.
for encoding, short in [("DELTA_BINARY_PACKED", "dbp"), ("BYTE_STREAM_SPLIT", "bss")]:
    write(f"enc_{short}_decimal_ints", ["dec_9_2", "dec_18_4"], encoding, store_decimal_as_integer=True, compression="snappy")

# Many small pages and row groups: blocks and miniblocks cut by pages, streams per page, prefixes restarting per page.
for encoding, short, columns in [("DELTA_BINARY_PACKED", "dbp", delta_ints), ("DELTA_BYTE_ARRAY", "dba", delta_strings + delta_fixed),
                                 ("DELTA_LENGTH_BYTE_ARRAY", "dlba", delta_strings), ("BYTE_STREAM_SPLIT", "bss", split)]:
    write(f"enc_{short}_small_pages", columns, encoding, data_page_size=200, row_group_size=110, data_page_version="2.0",
          compression="snappy", write_page_index=True)

# DuckDB's v2 writer picks these encodings by itself (and writes INT32 deltas 33 bits wide). pyarrow cannot read the
# result, so DuckDB is the oracle: the UUID column is left out (DuckDB renders it as text, pyarrow as bytes).
con = duckdb.connect()
con.execute(f"CREATE TABLE t AS SELECT * EXCLUDE (null_type, all_null, uuid) FROM read_parquet('{out}/pa_plain_v1.parquet')")
for codec in ["uncompressed", "zstd"]:
    con.execute(f"COPY t TO '{out}/enc_duckdb_v2_{codec}.parquet' (FORMAT parquet, PARQUET_VERSION V2, COMPRESSION {codec})")
print("written")
