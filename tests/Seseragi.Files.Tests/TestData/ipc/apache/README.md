Test files from the Apache Arrow testing repository (apache/arrow-testing), Apache License 2.0 (`LICENSE.txt`), taken
at commit 5a26d35785ff8761e56d79eebff971f6f737e18c.

- `integration/`: the IPC integration gold files of `data/arrow-ipc-stream/integration`, each table in both formats
  (`.arrow_file`, `.stream`). Taken: `0.14.1` (metadata V4, the pre-0.15 framing without the continuation marker),
  `0.17.1` (unions in V4, with a validity buffer), `1.0.0-littleendian` (without the two large decimal files,
  `cpp-21.0.0` has them), `2.0.0-compression` (LZ4 frame and ZSTD, buffers stored raw), `4.0.0-shareddict` (two
  fields share one dictionary id), `cpp-21.0.0` (every type of the current format, views, list views, run-end
  encoding, decimal32/64), and four small files of `1.0.0-bigendian` (a reader that refuses big-endian data must
  say so). The golden JSON is kept only for the interval files (`generated_interval.json.gz`): pyarrow cannot hand
  out year-month and day-time interval arrays, so the oracle reads those values from it.
- `fuzz/stream`, `fuzz/file`: the crash reproducers of `data/arrow-ipc-stream` and `data/arrow-ipc-file`
  (clusterfuzz and libFuzzer finds against the C++ reader). A reader must refuse or read them without crashing.
