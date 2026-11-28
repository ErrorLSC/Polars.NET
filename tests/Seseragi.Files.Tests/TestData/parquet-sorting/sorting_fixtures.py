"""Writes the sorting_columns fixtures with pyarrow (26): run from this folder with a Python that has pyarrow.

struct_first.parquet  s{x, y}, a, b, l (list): declared a ASC NULLS LAST, b DESC NULLS FIRST (leaf ordinals 2, 3:
                      pyarrow counts column chunks), two row groups whose a values follow each other strictly.
bad_index.parquet     a declaration naming column chunk 9 of 2.
list_leaf.parquet     a declaration naming the element of a list.
nan_bounds.parquet    d ASC (NaN last) in row groups 1, NaN | 2, 3: pyarrow's bounds leave NaN out (1..1), so only the
                      NaN count can tell the first row group's last row is NaN.
"""
import pyarrow as pa
import pyarrow.parquet as pq

t = pa.table({
    "s": [{"x": 9, "y": 1}, {"x": 8, "y": 2}, {"x": 7, "y": 3}, {"x": 6, "y": 4}],
    "a": [1, 2, 3, 4],
    "b": [None, 5, None, 3],
    "l": [[1], [2], [3], [4]],
})
keys = pq.SortingColumn.from_ordering(t.schema, [("a", "ascending")], null_placement="at_end") \
    + pq.SortingColumn.from_ordering(t.schema, [("b", "descending")], null_placement="at_start")
assert [k.column_index for k in keys] == [2, 3], keys
pq.write_table(t, "struct_first.parquet", sorting_columns=keys, row_group_size=2)

u = pa.table({"a": [1, 2], "b": [3, 4]})
pq.write_table(u, "bad_index.parquet", sorting_columns=[pq.SortingColumn(9)])
pq.write_table(t, "list_leaf.parquet", sorting_columns=[pq.SortingColumn(4)])

v = pa.table({"d": pa.array([1.0, float("nan"), 2.0, 3.0])})
pq.write_table(v, "nan_bounds.parquet", sorting_columns=[pq.SortingColumn(0)], row_group_size=2)

for f in ["struct_first.parquet", "bad_index.parquet", "list_leaf.parquet", "nan_bounds.parquet"]:
    m = pq.ParquetFile(f).metadata
    print(f, [m.row_group(i).sorting_columns for i in range(m.num_row_groups)], [m.row_group(i).column(0).statistics for i in range(m.num_row_groups)][:1])
