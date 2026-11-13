"""Writes the probe table with pyarrow, polars and duckdb into out/."""
import datetime as dt, decimal, sys
import pyarrow as pa, pyarrow.parquet as pq, polars as pl, duckdb

out = sys.argv[1]
n = 3
utc = [dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone.utc), None, dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)]
naive = [x.replace(tzinfo=None) if x else None for x in utc]
cats = ["red", "green", "red"]
t = pa.table({
    "ts_utc": pa.array(utc, pa.timestamp("us", "UTC")),
    "ts_tokyo": pa.array(utc, pa.timestamp("us", "Asia/Tokyo")),
    "ts_naive": pa.array(naive, pa.timestamp("us")),
    "ts_ns": pa.array(naive, pa.timestamp("ns")),
    "ts_ms": pa.array(naive, pa.timestamp("ms")),
    "dur_us": pa.array([dt.timedelta(seconds=90), None, dt.timedelta(days=-1)], pa.duration("us")),
    "date": pa.array([dt.date(2024, 1, 2), None, dt.date(1, 1, 1)], pa.date32()),
    "time_us": pa.array([dt.time(1, 2, 3, 4), None, dt.time(0)], pa.time64("us")),
    "dec": pa.array([decimal.Decimal("1.23"), None, decimal.Decimal("-9.99")], pa.decimal128(5, 2)),
    "u32": pa.array([1, None, 4294967295], pa.uint32()),
    "large_str": pa.array(["a", None, "ccc"], pa.large_string()),
    "dict_str": pa.array(cats).dictionary_encode(),
    "list_i": pa.array([[1, 2], None, []], pa.list_(pa.int32())),
    "large_list": pa.array([[1], None, [2, 3]], pa.large_list(pa.int64())),
    "fsl": pa.array([[1.0, 2.0], None, [3.0, 4.0]], pa.list_(pa.float32(), 2)),
    "map": pa.array([[("k", 1)], None, []], pa.map_(pa.string(), pa.int32())),
    "struct": pa.array([{"x": 1, "y": "a"}, None, {"x": None, "y": "c"}], pa.struct([("x", pa.int32()), ("y", pa.string())])),
})
pq.write_table(t, f"{out}/pyarrow.parquet")
pq.write_table(t, f"{out}/pyarrow_int96.parquet", use_deprecated_int96_timestamps=True)
try:
    pq.write_table(t, f"{out}/pyarrow_v1.parquet", version="1.0")
except Exception as e:
    print("pyarrow v1:", e)

df = pl.from_arrow(t.drop(["dict_str"])).with_columns(pl.Series("dict_str", cats, pl.Categorical), pl.Series("enum_str", cats, pl.Enum(["red", "green"])))
df.write_parquet(f"{out}/polars.parquet")

con = duckdb.connect()
# DuckDB stores durations as INTERVAL and refuses negative ones.
dt_ok = t.drop(["dict_str", "fsl", "large_list", "ts_tokyo"])
dt_ok = dt_ok.set_column(dt_ok.schema.get_field_index("dur_us"), "dur_us", pa.array([dt.timedelta(seconds=90), None, dt.timedelta(days=1)], pa.duration("us")))
con.register("t", dt_ok)
con.execute(f"COPY (SELECT *, 'x'::VARCHAR AS plain_str FROM t) TO '{out}/duckdb.parquet' (FORMAT parquet)")
print("written")
