# Delta Lake: Compatibility & Known Limitations

Polars.NET reads and writes Delta tables with its own managed kernel, checked against Apache Spark with delta-spark 4.1 in both directions. This page lists the table features it supports, what it cannot do yet, and the few places where it deliberately behaves differently from Spark. The features themselves are described in [Delta Lake & Modern Lakehouse Storage](/articles/delta) and [Advanced Table Features](/articles/delta-advanced).

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
| Column invariants, check constraints, generated columns | ✓ | Tables that use them are read only (see below) |

A table that needs a feature not listed here is refused with an error naming the feature, rather than read or written wrongly.

---

## 2. Not Supported Yet

* **Map columns cannot be written.** Tables with map columns (made by Spark or Databricks) are read, but the engine has no map type: it would write maps as Parquet lists, which Spark then cannot read. Appends of map data, and updates, merges, optimize and deletes that rewrite files of such tables, are refused with an error. Deletes that only add deletion vectors work. This is lifted once the engine writes Parquet maps.
* **SQL expressions.** Column invariants, check constraints and generated columns hold SQL expressions the kernel does not evaluate yet, so tables that use them are read but not written. Check constraints cannot be added, and `delta.constraints.*` properties are refused.
* **Table features not implemented:** variant columns (`variantType`; such tables cannot be read), column defaults (`allowColumnDefaults`), collations, Iceberg compatibility / UniForm (`icebergCompatV1`, `icebergCompatV2`), and catalog-managed tables.
* **Table changes not offered:** dropping a table feature (`DROP FEATURE`), `ALTER COLUMN … DROP NOT NULL`, column comments and column positions. Column mapping can be turned on (`none` to `name`) but not off.
* **Statistics of arrays and maps:** data files get no statistics for array and map columns (their null counts cannot be read from a Parquet footer), so filters on them do not skip files.
* **Legacy deletion vectors:** deletion vectors written by Polars.NET before 1.2.0 did not follow the protocol and are refused with a hint: run `Delta.Optimize` with the earlier version first.

---

## 3. Differences from Spark

These differences are deliberate: each avoids a state Spark can leave a table in, or saves work.

* **Dropping a struct that holds a clustering column is refused.** Spark refuses dropping a clustering column but allows dropping the struct that holds it, after which its later `ALTER TABLE` changes on the table fail.
* **Turning on row tracking for an existing table is one commit** (Spark takes three: protocol, backfill, property), so no reader sees the table half done. Lifting `delta.rowTrackingSuspended` and turning tracking on can also be one change, and a table with both set (which Spark cannot repair) is repaired by setting either to false.
* **Identity values of one write are consecutive.** Spark leaves gaps between its tasks.
* **Optimize clusters a table into one cube per run.** Spark splits very large runs into cubes of about 150 GiB.
* **Writes can create clustered tables.** `WriteDelta` / `SinkDelta` with `clusterBy` create a clustered table; Spark's `DataFrameWriter.clusterBy` (open-source Delta) does not.
