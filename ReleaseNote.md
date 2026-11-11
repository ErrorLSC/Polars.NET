# Polars.NET 1.2.0 Release Note

![icon](assets/icon_lite.png)

Polars.NET 1.2.0

For full architectural guides, API references, and benchmarks, please visit documentation site:
**[Polars.NET Documentation Site](https://errorlsc.github.io/Polars.NET)**

---

## Delta Lake: managed kernel

Delta Lake now runs on Seseragi.Lakehouse, a managed implementation of the Delta protocol, instead of the native `delta-rs` bindings. The log, checkpoints, deletion vectors, column mapping, type widening, in-commit timestamps and the change data feed are handled in .NET; the Polars engine reads and writes the Parquet data files. Behaviour is checked against Apache Spark with delta-spark 4.1, in both directions. The native library drops `delta-rs` and its dependencies.

### New

* Writes stream: `WriteDelta` / `SinkDelta` no longer collect the frame. Files are split by partition, `maxRowsPerFile` and size.
* Tables with the change data feed can be written (writes, deletes, merges and restores feed it). Previously writers failed with `Unsupported table features required`.
* Deletion vectors follow the protocol and are read and written by Spark and Databricks.
* `AddFeature` and `SetTableProperties` follow Spark's `ALTER TABLE ... SET TBLPROPERTIES`: properties that turn a feature on bring it into the protocol, `delta.feature.<name>` adds a feature, `delta.minReaderVersion` / `delta.minWriterVersion` raise the versions. Column mapping can be turned on (`none` to `name`), and in-commit timestamps for an existing table.
* The table properties `delta.dataSkippingNumIndexedCols` and `delta.dataSkippingStatsColumns` choose the columns that get statistics.
* Delete, merge and optimize are retried as a whole when another writer's commit conflicts with them (`POLARS_DELTA_MAX_RETRIES`, 5 by default); files of a failed attempt are removed.
* On S3, Azure Blob Storage and Google Cloud Storage, the kernel's own file access (log, checkpoints, deletion vectors, vacuum listings) goes through managed clients. They read the same configuration keys and environment variables (`AWS_*`, `AZURE_*`, `GOOGLE_*`, `~/.aws/credentials`, gcloud's application default credentials) and choose credentials in the same order as before: static keys, SAS and bearer tokens, service account keys, web identity (EKS), container credentials (ECS, EKS Pod Identity), the instance metadata services of AWS, Azure (managed identity) and Google Cloud, Microsoft Entra ID client secrets and workload identity, the Azure CLI, gcloud user logins, and credentials vended by Unity Catalog. S3 server-side encryption (including customer-provided keys) and requester pays are sent as before. Setups the managed clients do not implement (proxies, `allow_invalid_certificates`, S3 Express, checksums, conditional puts other than ETags, Microsoft Fabric, `https://` Azure URLs) keep using the native object store. `POLARS_NET_OBJECT_STORE=native` always uses the native store; `managed` makes a setup the managed clients cannot serve an error instead of a fallback.
* Unity Catalog tables on Google Cloud Storage use the OAuth token the catalog vends (`google_oauth_token`), for scans and writes as well. It used to be ignored, so access needed a service account or the metadata server of a GCP machine.

### Behaviour changes

* **Versions:** creating a table is one commit, as in Spark. The first write to a new table is version 0 (it used to be version 1, after an empty `CREATE TABLE` version 0). Existing tables keep their history.
* **MERGE follows Spark:** a source row with a null key matches nothing (was an error); several source rows matching one target row are an error only when that row is updated or deleted; source values are cast to the table's column types (keys needed identical types).
* **Schema evolution follows Spark:** an append with `canEvolve` adds the new columns and never drops existing ones (it used to replace the whole schema); an overwrite with `canEvolve` replaces the schema and the partitioning, unpartitioned when `partitionBy` is not given.
* **Change data feed:** readable only for versions where `delta.enableChangeDataFeed` was on (a range that starts earlier is refused); `_commit_timestamp` is `Datetime(us, UTC)` from the protocol's commit time, `_commit_version` is `Int64`.
* **Time travel by time** uses the protocol's commit time: the log file's modification time, or the in-commit timestamp. A time without an offset is UTC.
* **`AddFeature`** of `deletionVectors`, `changeDataFeed` or `appendOnly` also sets the property that turns it on (`delta.enableDeletionVectors`, ...), unless the table sets it already. `appendOnly` therefore makes the table append-only.
* **`SetTableProperties(raiseIfNotExists: true)`** refuses unknown `delta.` properties only; other keys are stored. `delta.constraints.*` is refused (check constraints come with their own operation later).
* **Optimize:** partition filters may name partition columns only, Z-order columns may not be partition columns (as Spark).
* **Unity Catalog** scans take the schema from the Delta log.
* **Deletion vectors written by earlier Polars.NET versions** did not follow the protocol and are refused with a hint: run `Delta.Optimize` with the earlier version first, so that no file carries a deletion vector.
* `History` renames an operation parameter or metric that collides with a top-level column (a RESTORE's `version` becomes `operationParameters.version`).
* `DeltaTableFeatures.IcebergCompatV1` was removed (it never named a protocol feature); `TimestampWithoutTimezone` now maps to `timestampNtz`.

### Ignored parameters (removed in 2.0)

* `SinkDelta` / `WriteDelta` / catalog sinks: `includeKeys` (partition columns are never written into data files), `statistics` (always written), `compatLevel`, `mkdir` (directories are always created).
* `ScanDelta` / `ReadDelta` / catalog scans: `glob`, `hivePartitioning`, `hivePartitionSchema`, `tryParseHiveDates` (partition values come from the log). `schema` now selects and casts the table's columns; an unknown column is an error.
* `approxBytesPerFile` and `delta.targetFileSize` (128 MB when unset; byte counts or sizes such as `64mb`) are measured as the engine estimates rows in memory: files on disk are smaller.
