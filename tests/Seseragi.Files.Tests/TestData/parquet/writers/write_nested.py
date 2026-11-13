"""Writes the nested type matrix: lists, maps and structs nested in each other, with nulls and empties at every
level, from pyarrow (pages v1 and v2, dictionaries, small pages so records cross page boundaries), DuckDB and Polars.

Usage: python -I write_nested.py <output directory>
"""
import sys

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
N = 400


def pick(i, *choices):
    return choices[i % len(choices)]


ints = [None if i % 9 == 4 else pick(i, [], [1], [1, None, 3], list(range(i % 7))) for i in range(N)]
nested = [None if i % 11 == 5 else pick(i, [], [[]], [None], [["a", None], [], ["bcd"]], [[str(i)] * (i % 4)]) for i in range(N)]
point = pa.struct([("x", pa.int32()), ("y", pa.float64()), ("label", pa.string())])
structs = [None if i % 6 == 1 else {"x": None if i % 5 == 0 else i, "y": i / 3, "label": pick(i, None, "p", "q")} for i in range(N)]
list_of_structs = [None if i % 8 == 2 else pick(i, [], [None], [{"x": i, "y": None, "label": "z"}, None], [{"x": 1, "y": 2.0, "label": None}] * (i % 3)) for i in range(N)]
inner = pa.struct([("tags", pa.list_(pa.string())), ("score", pa.int64())])
struct_with_list = [None if i % 7 == 3 else {"tags": None if i % 4 == 0 else [pick(i, "x", "y")] * (i % 3), "score": i} for i in range(N)]
maps = [None if i % 10 == 7 else pick(i, [], [("k", 1)], [("a", None), ("b", 2)], [(f"k{j}", j) for j in range(i % 5)]) for i in range(N)]
map_of_lists = [None if i % 13 == 6 else pick(i, [], [("a", [1, 2])], [("b", None), ("c", [])]) for i in range(N)]
fixed = [None if i % 9 == 2 else [float(i), None if i % 4 == 0 else -1.5, 0.25] for i in range(N)]
deep = [None if i % 17 == 9 else pick(i, [], [{"m": [("q", [{"v": [1, None]}])]}], [{"m": None}]) for i in range(N)]

table = pa.table({
    "id": pa.array(range(N), pa.int32()),
    "ints": pa.array(ints, pa.list_(pa.int32())),
    "ints_required_items": pa.array([v if v is None else [x if x is not None else 0 for x in v] for v in ints], pa.list_(pa.field("item", pa.int32(), nullable=False))),
    "nested": pa.array(nested, pa.list_(pa.list_(pa.string()))),
    "point": pa.array(structs, point),
    "points": pa.array(list_of_structs, pa.list_(point)),
    "with_list": pa.array(struct_with_list, inner),
    "map": pa.array(maps, pa.map_(pa.string(), pa.int32())),
    "map_lists": pa.array(map_of_lists, pa.map_(pa.string(), pa.list_(pa.int64()))),
    "fixed": pa.array(fixed, pa.list_(pa.float64(), 3)),
    "deep": pa.array(deep, pa.list_(pa.struct([("m", pa.map_(pa.string(), pa.list_(pa.struct([("v", pa.list_(pa.int16()))]))))]))),
})
pq.write_table(table, f"{out}/pa_nested_v1.parquet", data_page_version="1.0", use_dictionary=False)
pq.write_table(table, f"{out}/pa_nested_v2_dict.parquet", data_page_version="2.0", use_dictionary=True, compression="zstd")
pq.write_table(table, f"{out}/pa_nested_small_pages.parquet", data_page_size=64, row_group_size=150, write_batch_size=7,
               compression="snappy", write_page_checksum=True)
pq.write_table(table, f"{out}/pa_nested_no_arrow_schema.parquet", store_schema=False)

con = duckdb.connect()
con.execute("""
CREATE TABLE t AS SELECT
  i AS id,
  CASE WHEN i % 9 = 4 THEN NULL ELSE list_transform(range(i % 6), lambda x: CASE WHEN x = 2 THEN NULL ELSE x::INTEGER END) END AS ints,
  CASE WHEN i % 11 = 5 THEN NULL ELSE [list_transform(range(i % 3), lambda x: 's' || x::VARCHAR), []] END AS nested,
  CASE WHEN i % 6 = 1 THEN NULL ELSE {'x': CASE WHEN i % 5 = 0 THEN NULL ELSE i END, 'label': 'p' || i::VARCHAR} END AS point,
  CASE WHEN i % 8 = 2 THEN NULL ELSE [{'x': i, 'tags': ['a', NULL]}, NULL] END AS points,
  CASE WHEN i % 10 = 7 THEN NULL ELSE MAP {'k': i, 'z': NULL} END AS m
FROM range(300) r(i)
""")
for codec in ["snappy", "zstd"]:
    con.execute(f"COPY t TO '{out}/duckdb_nested_{codec}.parquet' (FORMAT parquet, COMPRESSION {codec})")

df = pl.from_arrow(table.select(["id", "ints", "nested", "point", "points", "with_list", "fixed"]))
df.write_parquet(f"{out}/polars_nested.parquet")
df.write_parquet(f"{out}/polars_nested_small.parquet", row_group_size=120, data_page_size=128, compression="lz4")
print("written")
