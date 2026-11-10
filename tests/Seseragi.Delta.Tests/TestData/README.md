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
