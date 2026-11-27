"""Map fixtures for the Polars 2.2 Map dtype tests (run with Python Polars 2.2 + pyarrow, `python -I`).

basic.parquet / basic.arrow   Polars-written map[str, i64]: entries, a null map, an empty map, a null value.
nested.arrow                  Polars-written nested shapes: list[map], struct{map}, map[str, list], map[str, map], map[i32, struct].
duplicates.arrow / .parquet   pyarrow-written map<string, int64> with a duplicate key ([x:1, y:2, x:5]).
"""
import sys
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

out = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent)


def kv(*pairs):
    return [{"key": k, "value": v} for k, v in pairs]


basic = pl.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "m": [kv(("a", 1), ("b", 2)), None, [], kv(("c", None))],
    },
    schema={"id": pl.Int64, "m": pl.List(pl.Struct({"key": pl.String, "value": pl.Int64}))},
).with_columns(pl.col("m").list.to_map())
assert basic.schema["m"] == pl.Map(pl.String, pl.Int64), basic.schema
basic.write_parquet(out / "basic.parquet")
basic.write_ipc(out / "basic.arrow")

entries = pl.List(pl.Struct({"key": pl.String, "value": pl.Int64}))
nested = pl.DataFrame(
    {
        "lm": [[kv(("a", 1)), kv(("b", 2), ("c", 3))], None],
        "sm": [{"name": "x", "m": kv(("k", 10))}, {"name": "y", "m": None}],
        "ml": [kv(("p", [1, 2]), ("q", None)), kv(("r", []))],
        "mm": [kv(("outer", kv(("inner", 7)))), []],
        "ms": [kv((1, {"u": "one", "v": 1.5})), kv((2, {"u": None, "v": None}))],
    },
    schema={
        "lm": pl.List(entries),
        "sm": pl.Struct({"name": pl.String, "m": entries}),
        "ml": pl.List(pl.Struct({"key": pl.String, "value": pl.List(pl.Int64)})),
        "mm": pl.List(pl.Struct({"key": pl.String, "value": entries})),
        "ms": pl.List(pl.Struct({"key": pl.Int32, "value": pl.Struct({"u": pl.String, "v": pl.Float64})})),
    },
)
nested = nested.with_columns(
    pl.col("lm").list.eval(pl.element().list.to_map()),
    pl.struct(pl.col("sm").struct.field("name"), pl.col("sm").struct.field("m").list.to_map()).alias("sm"),
    pl.col("ml").list.to_map(),
    pl.col("mm").list.eval(pl.struct(pl.element().struct.field("key"), pl.element().struct.field("value").list.to_map())).list.to_map(),
    pl.col("ms").list.to_map(),
)
print(nested.schema)
nested.write_ipc(out / "nested.arrow")

map_type = pa.map_(pa.string(), pa.int64())
dup = pa.table(
    {
        "id": pa.array([1, 2], pa.int64()),
        "m": pa.array([[("x", 1), ("y", 2), ("x", 5)], [("z", 9)]], map_type),
    }
)
with pa.OSFile(str(out / "duplicates.arrow"), "wb") as sink, pa.ipc.new_file(sink, dup.schema) as writer:
    writer.write_table(dup)
pq.write_table(dup, out / "duplicates.parquet")

# What Polars 2.2 itself makes of them (oracle values the tests assert).
for name in ["basic.parquet", "basic.arrow", "nested.arrow", "duplicates.arrow", "duplicates.parquet"]:
    df = pl.read_parquet(out / name) if name.endswith(".parquet") else pl.read_ipc(out / name)
    print(name, df.schema)
    print(df)
