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
| Catalog-managed tables (`catalogManaged`) | ✓ through Unity Catalog | ✓ through Unity Catalog, created with `CreateCatalogTable` |
| Column invariants, check constraints, generated columns | ✓ | Tables that use them are read only (see below) |

A table that needs a feature not listed here is refused with an error naming the feature, rather than read or written wrongly.

---

## 2. Not Supported Yet

* **Map columns cannot be written.** Tables with map columns (made by Spark or Databricks) are read, but the engine has no map type: it would write maps as Parquet lists, which Spark then cannot read. Appends of map data, and updates, merges, optimize and deletes that rewrite files of such tables, are refused with an error, as are dropping deletion vectors (when a file has one), type widening (when the schema records type changes) and column mapping, refused before anything is committed. Deletes that only add deletion vectors work. This is lifted once the engine writes Parquet maps.
* **Variant columns are read through our own patch, at a cost.** The Parquet engine (Polars, up to its latest release) refuses every file that annotates a column as VARIANT, which every variant column Spark and Databricks write carries ([pola-rs/polars#28627](https://github.com/pola-rs/polars/issues/28627)). Polars.NET works around it with a patch of its own, not a feature of the engine: each data file of a table with variant columns is read whole into memory, the annotation is removed from its footer, and the engine reads the patched copy. Tables without variant columns are not affected, and the patch is removed once the engine reads VARIANT. What it costs:
  * **No ranged reads:** every byte of every file the scan selects is read from storage, even for a query that touches one column or takes a few rows (`Head`). The engine still decodes only the columns and row groups it needs.
  * **Read early, held long:** the files are read when the scan is created (`ScanDelta`), not when it is collected, and stay in memory until the frame is released. The memory a scan holds is the size of the files it selects as stored (compressed): the whole version read, less what the log statistics skip.
  * **A warning** is written to standard error (and `Trace`) the first time a process scans such a table.

  Variant columns are read as stored, a struct of two binaries (`value`, `metadata`, the Parquet Variant encoding), or with `variantAsJson: true` (`ScanDelta`, `ReadDelta`, `ScanCatalogTable`) as JSON text, rendered as Spark's `to_json` renders them; decoding to JSON is Polars.NET's own, not part of the patch. Tables with variant columns are read only. Variants holding a time or a nanosecond timestamp (types Spark 4.2 does not write yet) cannot be read as JSON.
* **SQL expressions.** Column invariants, check constraints and generated columns hold SQL expressions the kernel does not evaluate yet, so tables that use them are read but not written. Check constraints cannot be added, and `delta.constraints.*` properties are refused.
* **Table features not implemented:** variant shredding (`variantShredding-preview`), column defaults (`allowColumnDefaults`), collations, and Iceberg compatibility / UniForm (`icebergCompatV1`, `icebergCompatV2`).
* **Catalog-managed tables** are read and written through Unity Catalog only (verified against open-source Unity Catalog 0.7 and Delta Spark 4.4; Databricks documents external access to these tables, but it has not been verified there). Creating them on a cloud store (`CreateCatalogTable`, or writing to a table that does not exist) uses the credentials the catalog vends for the staged table (renewed through the catalog while the rows are written, as Spark's connector renews them), which has been verified on local storage only. By their path, and for `OptimizeCatalogTable`, `DeltaVacuum`, `RenameCatalogColumn` and `DropFeature`, they are refused, as Delta Spark refuses them. Turning on the change data feed is refused by Unity Catalog 0.7 itself (for Spark too).
* **Turning column mapping off:** column mapping is turned on with `delta.columnMapping.mode` (`none` to `name`) and off with `Delta.DropFeature`, not by setting the mode.
* **Statistics of arrays and maps:** data files get no statistics for array and map columns (their null counts cannot be read from a Parquet footer), so filters on them do not skip files.
* **A file holds at least 122,880 rows when writes split by size.** The engine splits files by size only in whole row groups, and a row group holds 122,880 rows unless `rowGroupSize` says otherwise. When rows are wide in memory (more than about 1 KB at the default 128 MB target: embedding vectors, long text, a few hundred columns), files are larger than `delta.targetFileSize`. Appends can pass a smaller `rowGroupSize` to `WriteDelta` / `SinkDelta` (`targetFileSize` divided by the row width in memory); updates, deletes and merges have no such setting, and `Delta.Optimize` does not split large files.
* **Version checksum files (`<version>.crc`) are not written.** They are optional in the Delta protocol: Spark and Databricks compute what they would hold when a table has none, so tables written here are read and written by them as usual, only without that shortcut when they load a version.
* **Legacy deletion vectors:** deletion vectors written by Polars.NET before 1.2.0 did not follow the protocol and are refused with a hint: run `Delta.Optimize` with the earlier version first.

---

## 3. Differences from Spark

These differences are deliberate: each avoids a state Spark can leave a table in, or saves work.

* **Dropping a struct that holds a clustering column is refused.** Spark refuses dropping a clustering column but allows dropping the struct that holds it, after which its later `ALTER TABLE` changes on the table fail.
* **Turning on row tracking for an existing table is one commit** (Spark takes three: protocol, backfill, property), so no reader sees the table half done. Lifting `delta.rowTrackingSuspended` and turning tracking on can also be one change, and a table with both set (which Spark cannot repair) is repaired by setting either to false.
* **Identity values of one write are consecutive.** Spark leaves gaps between its tasks.
* **Checkpoints store timestamps as 64-bit microseconds.** Spark writes the `timestamp` columns of a checkpoint's typed statistics as legacy INT96 values; Polars.NET writes the standard INT64 microsecond encoding, which Spark and other readers read the same.
* **Writes can create clustered tables.** `WriteDelta` / `SinkDelta` with `clusterBy` create a clustered table; Spark's `DataFrameWriter.clusterBy` (open-source Delta) does not.
* **Restore keeps checkpoint protection.** Restoring a version from before a feature was dropped keeps `delta.requireCheckpointProtectionBeforeVersion`; Spark's restore loses it, after which a log cleanup may remove the history protected checkpoints still need.
* **Dropping column mapping from a clustered table renames the clustering columns.** The clustering domain names its columns by physical name; Spark leaves the old names in place, after which its own `OPTIMIZE` and `DESCRIBE DETAIL` fail on the table. Polars.NET records the columns under the names the files now hold.
* **Timestamps inside variants are rendered as JSON in UTC.** Spark's `to_json` renders them in the session time zone; Polars.NET has no session time zone, and UTC is what its other timestamps use.
