# Delta Lake: Compatibility & Known Limitations

Polars.NET reads and writes Delta tables with its own managed kernel, checked against Apache Spark 4.2 with delta-spark 4.4 in both directions. This page lists the table features it supports, what it cannot do yet, and the few places where it deliberately behaves differently from Spark. The features themselves are described in [Delta Lake & Modern Lakehouse Storage](/articles/delta) and [Advanced Table Features](/articles/delta-advanced).

---

## 1. Supported Table Features

Protocol versions up to reader 3 and writer 7 are supported.

| Feature | Read | Write |
| --- | --- | --- |
| Deletion vectors (`deletionVectors`) | ✓ | ✓ |
| Change data feed (`changeDataFeed`) | ✓ | ✓ |
| Column mapping, by name and by id (`columnMapping`) | ✓ | ✓ |
| Timestamps without time zone (`timestampNtz`) | ✓ | ✓ |
| Type widening (`typeWidening`, `typeWidening-preview`) | ✓ | ✓ |
| V2 checkpoints (`v2Checkpoint`) | ✓ | ✓ |
| In-commit timestamps (`inCommitTimestamp`) | ✓ | ✓ |
| Domain metadata (`domainMetadata`) | ✓ | ✓ |
| Vacuum protocol check (`vacuumProtocolCheck`) | ✓ | ✓ |
| Append-only tables (`appendOnly`) | ✓ | ✓ |
| Identity columns (`identityColumns`) | ✓ | ✓ |
| Row tracking (`rowTracking`) | ✓ | ✓ |
| Liquid clustering (`clustering`) | ✓ | ✓ |
| Checkpoint protection (`checkpointProtection`, left by dropping a reader feature) | ✓ | ✓ |
| Variant columns (`variantType`) | ✓ through our own patch (see below) | — |
| Column invariants, check constraints, generated columns | ✓ | Tables that use them are read only (see below) |

A table that needs a feature not listed here is refused with an error naming the feature, rather than read or written wrongly.

---

## 2. Not Supported Yet

* **Map columns cannot be written.** Tables with map columns (made by Spark or Databricks) are read, but the engine has no map type: it would write maps as Parquet lists, which Spark then cannot read. Appends of map data, and updates, merges, optimize and deletes that rewrite files of such tables, are refused with an error, as are dropping deletion vectors (when a file has one), type widening (when the schema records type changes) and column mapping, refused before anything is committed. Deletes that only add deletion vectors work. This is lifted once the engine writes Parquet maps.
* **Variant columns are read through our own patch, at a cost.** The Parquet engine (Polars, up to its latest release) refuses every file that annotates a column as VARIANT, which every variant column Spark and Databricks write carries ([pola-rs/polars#28627](https://github.com/pola-rs/polars/issues/28627)). Polars.NET works around it with a patch of its own, not a feature of the engine: each data file of a table with variant columns is **read whole into memory**, the annotation is removed from its footer, and the engine reads the patched copy. Scans of such tables therefore do **no ranged reads and no column pruning**: every byte of every file selected is read, even for a query that touches one column or takes a few rows (`Head`). The files are read **when the scan is created** (`ScanDelta`), not when it is collected, and **stay in memory until the frame is released**, so the memory a scan needs is the size of the files it selects (all of the version read, less what the log statistics skip). A warning is written to standard error (and `Trace`) the first time a process scans such a table. Variant columns are read as they are stored, a struct of two binaries (`value`, `metadata`, the Parquet Variant encoding), or with `variantAsJson: true` (`ScanDelta`, `ReadDelta`, `ScanCatalogTable`) as JSON text, rendered as Spark's `to_json` renders them (timestamps in UTC); tables with them are read only. Decoding variants to JSON is Polars.NET's own and not part of the patch. Tables without variant columns are not affected. The patch is removed once the engine reads VARIANT.
* **SQL expressions.** Column invariants, check constraints and generated columns hold SQL expressions the kernel does not evaluate yet, so tables that use them are read but not written. Check constraints cannot be added, and `delta.constraints.*` properties are refused.
* **Table features not implemented:** variant shredding (`variantShredding`), column defaults (`allowColumnDefaults`), collations, Iceberg compatibility / UniForm (`icebergCompatV1`, `icebergCompatV2`), and catalog-managed tables.
* **Table changes not offered:** `ALTER COLUMN … DROP NOT NULL`, column comments and column positions. Column mapping is turned on with `delta.columnMapping.mode` (`none` to `name`) and off with `Delta.DropFeature`, not by setting the mode.
* **History truncation:** `DROP FEATURE … TRUNCATE HISTORY` is not offered, so `checkpointProtection` cannot be dropped.
* **Purging dropped columns:** Spark's `REORG … APPLY (PURGE)`, which dropping deletion vectors runs, also rewrites files that still hold the data of dropped columns; Polars.NET rewrites only the files with deletion vectors.
* **Statistics of arrays and maps:** data files get no statistics for array and map columns (their null counts cannot be read from a Parquet footer), so filters on them do not skip files.
* **Legacy deletion vectors:** deletion vectors written by Polars.NET before 1.2.0 did not follow the protocol and are refused with a hint: run `Delta.Optimize` with the earlier version first.

---

## 3. Differences from Spark

These differences are deliberate: each avoids a state Spark can leave a table in, or saves work.

* **Dropping a struct that holds a clustering column is refused.** Spark refuses dropping a clustering column but allows dropping the struct that holds it, after which its later `ALTER TABLE` changes on the table fail.
* **Turning on row tracking for an existing table is one commit** (Spark takes three: protocol, backfill, property), so no reader sees the table half done. Lifting `delta.rowTrackingSuspended` and turning tracking on can also be one change, and a table with both set (which Spark cannot repair) is repaired by setting either to false.
* **Identity values of one write are consecutive.** Spark leaves gaps between its tasks.
* **Optimize clusters a table into one cube per run.** Spark splits very large runs into cubes of about 150 GiB.
* **Checkpoints store timestamps as 64-bit microseconds.** Spark writes the `timestamp` columns of a checkpoint's typed statistics as legacy INT96 values; Polars.NET writes the standard INT64 microsecond encoding, which Spark and other readers read the same.
* **Writes can create clustered tables.** `WriteDelta` / `SinkDelta` with `clusterBy` create a clustered table; Spark's `DataFrameWriter.clusterBy` (open-source Delta) does not.
* **Restore keeps checkpoint protection.** Restoring a version from before a feature was dropped keeps `delta.requireCheckpointProtectionBeforeVersion`; Spark's restore loses it, after which a log cleanup may remove the history protected checkpoints still need.
* **Dropping column mapping from a clustered table renames the clustering columns.** The clustering domain names its columns by physical name; Spark leaves the old names in place, after which its own `OPTIMIZE` and `DESCRIBE DETAIL` fail on the table. Polars.NET records the columns under the names the files now hold.
