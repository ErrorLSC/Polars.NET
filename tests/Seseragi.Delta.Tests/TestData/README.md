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

## Change data feed tables

The folder `cdf/` holds six tables from the same delta-kernel-rs test data (unpacked from its `cdf-table-*.tar.zst`
archives): tables with the change data feed on, written by Spark, with inserts, updates, deletes, `cdc` files,
partitions and deletion vectors.

Some are hand-made and not self-consistent, so tests must not replay them to the final table:
`cdf-table-simple` and `cdf-table-update-ops` have commits with `cdc` actions but no `add`, and
`cdf-table-update-ops` writes the change types `update_pre` / `update_post` instead of the protocol's
`update_preimage` / `update_postimage`. `cdf-table-with-dv` has an inline deletion vector whose last Z85 group is
above 32 bits; Delta's reference decoder keeps the low 32 bits.
