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
| Variant columns (`variantType`) | ✓ (see below) | — |
| Catalog-managed tables (`catalogManaged`) | ✓ through Unity Catalog | ✓ through Unity Catalog, created with `CreateCatalogTable` |
| Column invariants, check constraints, generated columns | ✓ | Tables that use them are read only (see below) |

A table that needs a feature not listed here is refused with an error naming the feature, rather than read or written wrongly.

---

## 2. Not Supported Yet

* **Map columns are not created here.** The engine has no map type: it holds a map as a list of key/value structs, so such a column written to a new table, or added to one by schema evolution, becomes an array of structs. Tables with map columns made by Spark or Databricks are read and written, their maps as Parquet maps: appends and merges take a list of two-field structs as the table's map (by position, whatever the fields' names), and updates, deletes, optimize and feature drops rewrite them. With `POLARS_NET_DELTA_SINK=native` (the engine's own Parquet sink, which writes maps as lists Spark cannot read) these writes are refused before anything is committed; deletes that only add deletion vectors work either way.
* **Variant columns are read by Polars.NET's own Parquet reader.** The Parquet engine (Polars, up to its latest release) refuses every file that annotates a column as VARIANT, which every variant column Spark and Databricks write carries ([pola-rs/polars#28627](https://github.com/pola-rs/polars/issues/28627)). The data files of a table with variant columns are therefore read by Seseragi.Files, Polars.NET's managed Parquet reader, which hands the rows to the engine as they are read: only the columns and leading rows a query uses, by ranged reads. Tables without variant columns are read by the engine as before. What differs from the engine's own scan: the filter of a DataFrame query is not pushed into the files (the log statistics still skip whole files; inside a file, row groups are not skipped by their statistics and the filter runs after the read; the filter of a LINQ query does skip row groups and pages and leaves out rows as they are read), and on the streaming engine the columns read are first held in memory (`Collect(Engine.Streaming)`) or written to a temporary file (sinks, see [Memory and the streaming engine](/articles/interop)). Variant columns are read as stored, a struct of two binaries (`value`, `metadata`, the Parquet Variant encoding), or with `variantAsJson: true` (`ScanDelta`, `ReadDelta`, `ScanCatalogTable`) as JSON text, rendered as Spark's `to_json` renders them. Tables with variant columns are read only. Times and nanosecond timestamps inside variants (types Spark does not write, other writers such as Iceberg and DuckDB do) are rendered in the same style, as DuckDB renders them: `"12:33:54.12"`, `"2024-11-07 12:33:54.123456789"`.
* **SQL expressions.** Column invariants, check constraints and generated columns hold SQL expressions the kernel does not evaluate yet, so tables that use them are read but not written. Check constraints cannot be added, and `delta.constraints.*` properties are refused.
* **Table features not implemented:** variant shredding (`variantShredding-preview`), column defaults (`allowColumnDefaults`), collations, and Iceberg compatibility / UniForm (`icebergCompatV1`, `icebergCompatV2`).
* **Catalog-managed tables** are read and written through Unity Catalog only (verified against open-source Unity Catalog 0.7 and Delta Spark 4.4; Databricks documents external access to these tables, but it has not been verified there). Creating them on a cloud store (`CreateCatalogTable`, or writing to a table that does not exist) uses the credentials the catalog vends for the staged table (renewed through the catalog while the rows are written, as Spark's connector renews them), which has been verified on local storage only. By their path, and for `OptimizeCatalogTable`, `PurgeCatalogTable`, `DeltaVacuum`, `RenameCatalogColumn` and `DropFeature`, they are refused, as Delta Spark refuses them. Turning on the change data feed is refused by Unity Catalog 0.7 itself (for Spark too).
* **Turning column mapping off:** column mapping is turned on with `delta.columnMapping.mode` (`none` to `name`) and off with `Delta.DropFeature`, not by setting the mode.
* **Statistics of arrays and maps:** data files get no statistics for array and map columns (their null counts cannot be read from a Parquet footer), so filters on them do not skip files.
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
* **Array elements and map keys and values have no position to move.** `Delta.MoveColumn("items.element")` is refused; Spark commits such a move and changes nothing.
* **Under checkpoint protection, log cleanup removes the protected history only all at once.** Spark also cuts part of it when a checkpoint already sits where it cuts; Polars.NET keeps it until the cleanup reaches the protected version, so the log may hold more history than Spark would keep (time travel to it keeps working).
* **Timestamps inside variants are rendered as JSON in UTC.** Spark's `to_json` renders them in the session time zone; Polars.NET has no session time zone, and UTC is what its other timestamps use.
