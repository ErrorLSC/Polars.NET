# Test tables

These tables are copied from the test data of delta-kernel-rs
(https://github.com/delta-io/delta-kernel-rs, `kernel/tests/data`, Apache License 2.0). They were written by
Spark or Databricks, so they show what the kernel must read from other engines.

| Table | What it has |
| --- | --- |
| `table-with-dv-small` | ten rows, rows 0 and 9 deleted through a deletion vector |
| `with-short-dv` | two files, one with a deletion vector, in a single commit |
| `with_checkpoint_no_last_checkpoint` | a classic checkpoint, no `_last_checkpoint` |
| `app-txn-checkpoint` | a classic checkpoint holding `txn` actions, partitioned |
| `v1-multi-part-struct-stats-only` | a multi-part checkpoint |
| `v2-parquet-sidecars-struct-stats-only` | a V2 Parquet checkpoint with sidecars |
| `v2-json-sidecars-struct-stats-only` | a V2 JSON checkpoint with sidecars |

## Delta Acceptance Tests

The folder `dat/` holds the reader cases of the Delta Acceptance Tests (https://github.com/delta-incubator/dat).
It is not in the repository; the tests that need it pass without checking anything when it is missing. Fetch it with:

```sh
mkdir -p tests/Seseragi.Delta.Tests/TestData/dat
curl -sfL https://github.com/delta-incubator/dat/releases/download/v0.0.3/deltalake-dat-v0.0.3.tar.gz \
  | tar -xz -C tests/Seseragi.Delta.Tests/TestData/dat
```
