Parquet test files.

- `apache/`: the Apache Parquet testing repository (see `apache/README.md`).
- `writers/`: one table written by pyarrow, Polars and DuckDB (`write_py.py`) and by Spark with delta-spark
  (`write_spark.py`: default INT96 timestamps, TIMESTAMP_MICROS, and a Delta table's data file). Each script takes
  the output directory as its argument.
- `footers.json`: every footer here as pyarrow reads it, written by `parquet_footer_check.py` (run it again after
  adding files). The footer tests compare against it.
