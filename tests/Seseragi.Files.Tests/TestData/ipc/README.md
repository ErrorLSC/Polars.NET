Arrow IPC test files (file format, stream format, Feather v2).

- `apache/`: gold and fuzz files from the Apache Arrow testing repository (see `apache/README.md`).
- `pyarrow/`: written by `writers/write_pyarrow.py`: every type pyarrow writes (primitives, decimals 32 to 256,
  temporals, intervals, strings and views with several variadic buffers, list, large list, list view, fixed-size
  list, struct, map, unions, run-end encoding, extension types), dictionaries (every index type, ordered, nested,
  all-null, deltas in both formats, replacements in the stream format, unified), custom metadata at schema, field and
  message level, empty and zero batches, a stream without its end-of-stream marker, metadata V4, the pre-0.15
  framing, LZ4 and ZSTD, many batches, Feather v2 and a Feather v1 file (not an IPC file).
- `polars/`: written by `writers/write_polars.py`: one frame of every Polars type at the newest and the oldest
  compatibility level, uncompressed, LZ4 and ZSTD, in both formats; `int128.arrow` holds Int128 and UInt128, which
  Polars writes as Arrow `Int` of bit width 128.
  Each generator takes the output directory as its argument and fails when a file lacks the feature it is named for.
- `ipc_meta.py`: an independent reader of the framing and FlatBuffer metadata (used by the generators and the
  oracle).
- `arrow_canonical.py`: the canonical text form of values (a superset of `../parquet/parquet_check.py`'s).
- `values.json`: every file as pyarrow (with full validation) and Python Polars read it: per column the type, counts
  and a hash of the canonical values; per batch its rows and a hash. Written by `ipc_check.py`.
- `messages.json`: every file's framing and metadata as `ipc_meta.py` reads it. Written by `ipc_check.py`.
  Run `ipc_check.py` again after adding files.
