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
| Column invariants, check constraints, generated columns, column defaults (`allowColumnDefaults`) | ✓ | ✓ (see below) |

A table that needs a feature not listed here is refused with an error naming the feature, rather than read or written wrongly.

---

## 2. Not Supported Yet

* **Map columns are not created here.** The engine has no map type: it holds a map as a list of key/value structs, so such a column written to a new table, or added to one by schema evolution, becomes an array of structs. Tables with map columns made by Spark or Databricks are read and written, their maps as Parquet maps: appends and merges take a list of two-field structs as the table's map (by position, whatever the fields' names), and updates, deletes, optimize and feature drops rewrite them.
* **Variant columns are read by Polars.NET's own Parquet reader.** The Parquet engine (Polars, up to its latest release) refuses every file that annotates a column as VARIANT, which every variant column Spark and Databricks write carries ([pola-rs/polars#28627](https://github.com/pola-rs/polars/issues/28627)). The data files of a table with variant columns are therefore read by Seseragi.Files, Polars.NET's managed Parquet reader, which hands the rows to the engine as they are read: only the columns and leading rows a query uses, by ranged reads. Tables without variant columns are read by the engine as before. What differs from the engine's own scan: the filter of a DataFrame query is not pushed into the files (the log statistics still skip whole files; inside a file, row groups are not skipped by their statistics and the filter runs after the read; the filter of a LINQ query does skip row groups and pages and leaves out rows as they are read), and on the streaming engine the columns read are first held in memory (`Collect(Engine.Streaming)`) or written to a temporary file (sinks, see [Memory and the streaming engine](/articles/interop)). Variant columns are read as stored, a struct of two binaries (`value`, `metadata`, the Parquet Variant encoding), or with `variantAsJson: true` (`ScanDelta`, `ReadDelta`, `ScanCatalogTable`) as JSON text, rendered as Spark's `to_json` renders them. Tables with variant columns are read only. Times and nanosecond timestamps inside variants (types Spark does not write, other writers such as Iceberg and DuckDB do) are rendered in the same style, as DuckDB renders them: `"12:33:54.12"`, `"2024-11-07 12:33:54.123456789"`.
* **SQL expressions of check constraints, column invariants, generated columns and column defaults** are evaluated as Spark 4 evaluates them (ANSI mode: an integer overflow, a division by zero or a cast that does not fit fails the write), over the functions and operators such tables use: comparisons, `AND` / `OR` / `NOT`, `CASE`, `IN`, `LIKE` / `RLIKE`, arithmetic, `CAST`, string functions (`length`, `upper`, `substring`, `concat`, `trim`, `lpad`, ...), date parts, `date_format`, `trunc` and `date_trunc`. An expression outside them, or one Spark evaluates in a way the engine cannot reproduce (a text column read as a number or a date, a floating value cast to a decimal or to text, decimal arithmetic that may need more than 38 digits, a Java-only regular expression construct), refuses the write before any file is written, naming the expression and why. Timestamps are read in `Delta.SessionTimeZone` (UTC by default), as Spark reads them in its session time zone. Expressions are declared with `Delta.AddConstraint`, `Delta.SetColumnDefault` and `CreateTable` (see [Advanced Table Features](/articles/delta-advanced)), and checked when declared so that every declared expression can be evaluated; `delta.constraints.*` table properties are refused.
* **Table features not implemented:** variant shredding (`variantShredding-preview`), collations, and Iceberg compatibility / UniForm (`icebergCompatV1`, `icebergCompatV2`).
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
* **A constraint violation names the expression as the table stores it.** Spark's message shows its own rendering of the expression (`s <> 'bad'` as `NOT (s = 'bad')`); the values of the row's columns are written as Spark writes them. When a row violates several constraints, which one is named is not defined (in Spark neither).
* **A merge gives inserted rows the defaults of the columns its source lacks.** Spark refuses such an insert.
* **Check constraints are stored as written.** Spark stores its own token rendering of the expression (`a > -5` as `a > - 5`), which can change its meaning (`'it''s'` becomes `'it' 's'`, read back as `its`); Spark reads the text as written all the same.
* **Declarations bring their features.** `CreateTable` with column defaults and `Delta.SetColumnDefault` add the `allowColumnDefaults` feature (Spark asks for `delta.feature.allowColumnDefaults` first), and `Delta.AddConstraint` the `checkConstraints` feature, as Spark does for constraints.
* **What Spark accepts but cannot then write is refused when declared.** A check constraint that is not deterministic (`rand() < 2`, `d <= current_date()`), reads a subquery, or has an empty name; a default on a generated column (Spark's `ALTER COLUMN … SET DEFAULT` accepts one, its `CREATE TABLE` refuses it); a `delta.constraints.*` property given to `CreateTable` (Spark stores it unchecked).
* **UNSET TBLPROPERTIES refuses what it cannot do safely.** A property the table does not have is refused unless `ifExists` (Spark skips it and commits anyway). `delta.columnMapping.mode` is refused, pointing to `Delta.DropFeature("columnMapping")` (Spark silently rewrites the whole table to remove column mapping); `delta.constraints.*` points to `Delta.DropConstraint` (Spark drops the constraint unchecked); `delta.feature.*` and the protocol versions point to `Delta.DropFeature` (Spark commits a change that changes nothing); the properties the table keeps itself (column mapping's highest id, row tracking's column names, in-commit timestamps' enablement) are refused.
* **`ReplaceTable(orCreate: true)` replaces any definition.** Spark's `CREATE OR REPLACE TABLE delta.`path`` refuses a schema or partitioning that differs from the table's; its `REPLACE TABLE` does not, and neither does Polars.NET.
* **Converting refuses UINT64 columns.** Spark reads them as `DECIMAL(20,0)`, a type the engine cannot read such a file as; the other unsigned integers are widened as Spark widens them.
* **A clone stays on its source's store.** `Delta.Clone` needs the source and the clone on one store (both local, or the same bucket), reached with the same credentials.
* **Dropping what is not there commits nothing.** `Delta.DropConstraint(ifExists: true)` on a missing constraint and `Delta.DropColumnDefault` on a column without a default make no commit; Spark commits an empty change.
* **Timestamps inside variants are rendered as JSON in UTC.** Spark's `to_json` renders them in the session time zone; Polars.NET renders them in UTC, as its other timestamps (`Delta.SessionTimeZone` applies to the SQL expressions of check constraints, generated columns and column defaults only).
