"""Prints what pyarrow and Polars read of a file's sorting_columns, as JSON: python -I read_sorting.py FILE

pyarrow: each row group's SortingColumn list; polars: the sorted flag of each top-level column after read_parquet.
"""
import json
import sys

import polars as pl
import pyarrow.parquet as pq

path = sys.argv[1]
m = pq.ParquetFile(path).metadata
groups = [[[s.column_index, s.descending, s.nulls_first] for s in m.row_group(i).sorting_columns] for i in range(m.num_row_groups)]
frame = pl.read_parquet(path)
flags = {c: frame[c].flags["SORTED_ASC"] for c in frame.columns if frame[c].dtype.is_integer()}
print(json.dumps({"pyarrow": groups, "polars": flags}))
