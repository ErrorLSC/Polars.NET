Parquet test files.

- `apache/`: the Apache Parquet testing repository (see `apache/README.md`).
- `writers/`: one table written by pyarrow, Polars and DuckDB (`write_py.py`) and by Spark with delta-spark
  (`write_spark.py`: default INT96 timestamps, TIMESTAMP_MICROS, and a Delta table's data file). Each script takes
  the output directory as its argument.
- `matrix/`: the flat type matrix (`writers/write_matrix.py`): pyarrow encodings, page versions, codecs and options,
  DuckDB in each codec, Polars.
- `nested/`: the nested type matrix (`writers/write_nested.py`: pyarrow, DuckDB, Polars) and Spark files
  (`writers/write_checkpoint.py`: Delta checkpoints, classic and a V2 sidecar, and a shredded VARIANT column).
- `values.json`: every file's columns as pyarrow and DuckDB read them (types, counts, a hash of the values in a
  canonical form), written by `parquet_check.py`. The reader tests compare against it.
- `footers.json`: every footer here as pyarrow reads it, written by `parquet_footer_check.py` (run it again after
  adding files). The footer tests compare against it.
