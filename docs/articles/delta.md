# Delta Lake & Modern Lakehouse Storage

[Delta Lake](https://delta.io/) is an open storage format that brings **ACID transactions**, **Time Travel**, **metadata versioning**, and **high-throughput MERGE/Upsert** capabilities on top of Apache Parquet data files.

Traditional .NET applications accessing Delta Lake often rely on external JVM bridges, Apache Spark clusters, or REST services. **Polars.NET implements the Delta protocol in managed code** (the Seseragi kernel): it reads and commits the transaction log itself and lets the Polars engine read and write the Parquet data files, so you can read, write, time-travel, and maintain Delta Lake tables within a single .NET process without any JVM dependencies. Its behaviour is checked against Apache Spark with delta-spark, in both directions: tables either one writes, the other reads and writes.

---

## 1. Reading, Writing & Partitioning

Polars.NET supports both Eager (`DataFrame`) and Lazy (`LazyFrame`) modes for Delta Lake tables:
* **Reading**: Resolves the table from the `_delta_log` (checkpoints, deletion vectors, column mapping, type widening) and scans its files; LINQ queries also skip files by their statistics.
* **Writing**: Streams the rows into Parquet files (nothing is collected in memory), split by partition and by size, and commits them as one ACID transaction. Statistics for data skipping come from the files' footers.

> [!NOTE] File sizes
> `approxBytesPerFile` and the table's `delta.targetFileSize` (128 MB when unset) are measured the way the engine estimates rows **in memory**. Files on disk are smaller, being compressed: often several times smaller.

<div class="pl-pipeline">
  <div class="pl-step">
    <div class="pl-step-badge">1. Memory Pipeline</div>
    <div class="pl-step-name">DataFrame / LazyFrame</div>
    <div class="pl-step-desc">Columnar batch processing</div>
  </div>
  <div class="pl-arrow">&rarr;</div>
  <div class="pl-step">
    <div class="pl-step-badge">2. Columnar Disk Write</div>
    <div class="pl-step-name">Parquet Data Files</div>
    <div class="pl-step-desc">Hive-style directory partitioning</div>
  </div>
  <div class="pl-arrow">&rarr;</div>
  <div class="pl-step">
    <div class="pl-step-badge">3. Atomic Commit</div>
    <div class="pl-step-name">_delta_log/*.json</div>
    <div class="pl-step-desc">ACID transaction commit & version bump</div>
  </div>
</div>

::: code-group

```csharp [C# (Fluent)]
using Polars.CSharp;
using static Polars.CSharp.Polars;

var df = DataFrame.FromColumns(new
{
    Id = new[] { 1, 2, 3, 4 },
    Device = new[] { "A", "A", "B", "B" },
    Reading = new[] { 45.2, 78.0, 22.5, 91.3 }
});

// 1. Atomically overwrite a Delta table with Hive-style directory partitioning
df.WriteDelta(
    path: "./lake/devices",
    partitionBy: Col("Device"),
    mode: DeltaSaveMode.Overwrite,
    mkdir: true
);

// 2. Lazily scan the Delta table (with automatic partition pruning)
var lf = LazyFrame.ScanDelta("./lake/devices");
var collected = lf.Filter(Col("Device") == "A").Collect();
```

```fsharp [F# (Pipeline)]
open Polars.FSharp

let df =
    DataFrame.create [
        pl.series "Id" [ 1; 2; 3; 4 ]
        pl.series "Device" [ "A"; "A"; "B"; "B" ]
        pl.series "Reading" [ 45.2; 78.0; 22.5; 91.3 ]
    ]

// 1. Write partitioned Delta table
df.WriteDelta(
    path = "./lake/devices",
    partitionBy = pl.cs.byName [ "Device" ],
    mode = DeltaSaveMode.Overwrite,
    mkdir = true
)

// 2. Scan and filter with partition pruning
let collected =
    LazyFrame.ScanDelta "./lake/devices"
    |> LazyFrame.filter (pl.col "Device" .== pl.lit "A")
    |> LazyFrame.collect
```

:::

---

## 2. Time Travel & Point-in-Time Auditing

Every commit to a Delta table records a discrete snapshot in the transaction log; the first write to a new table is version 0. You can query any prior snapshot by **Version** or by ISO-8601 **Timestamp** (a time without an offset is UTC; a commit's time is the time its log file was written, or its in-commit timestamp when the table has them):

::: code-group

```csharp [C# (Fluent)]
// 1. Query by table commit version (e.g., Version 1 snapshot)
var v1Df = LazyFrame.ScanDelta("./lake/devices", version: 1UL).Collect();

// 2. Query point-in-time snapshot by timestamp
var snapshotDf = LazyFrame.ScanDelta(
    "./lake/devices",
    datetime: "2026-02-09T12:00:00Z"
).Collect();
```

```fsharp [F# (Pipeline)]
// 1. Query Version 1 snapshot
let v1Df = LazyFrame.ScanDelta("./lake/devices", version = 1UL).Collect()

// 2. Query point-in-time snapshot
let snapshotDf =
    LazyFrame.ScanDelta("./lake/devices", datetime = "2026-02-09T12:00:00Z").Collect()
```

:::

### Live Execution Output: Time Travel (Version 1 Snapshot)

<!--@include: ../snippets/delta_time_travel.html-->

---

## 3. Transaction History & Restoration (`History` & `Restore`)

Using `Delta.History(path)`, you can retrieve a DataFrame detailing table operations (`WRITE`, `MERGE`, `VACUUM`), execution modes, and engine versions.

If erroneous data is committed, `Delta.Restore` rolls back the table to any valid prior state:

::: code-group

```csharp [C# (Fluent)]
// 1. Inspect audit trail and commit provenance
var historyDf = Delta.History("./lake/devices");

// 2. Roll back table to Version 1 (records a new legitimate Restore commit)
ulong newVersion = Delta.Restore("./lake/devices", version: 1UL);
```

```fsharp [F# (Pipeline)]
// 1. Inspect table provenance
let historyDf = Delta.History "./lake/devices"

// 2. Roll back to Version 1
let newVersion = Delta.Restore("./lake/devices", version = 1UL)
```

:::

### Live Execution Output: ACID Commit History Table (`Delta.History()`)

<!--@include: ../snippets/delta_history.html-->

---

## 4. Compaction & File Hygiene (`Optimize` & `Vacuum`)

Frequent incremental writes create many small Parquet files. This fragmentation degrades read performance.

Polars.NET provides native table maintenance operations:
* **Bin-packing**: Merges small files into target sizes (e.g. 128 MB), and materializes deletion vectors.
* **Z-Order Clustering**: Constructs multidimensional Space-Filling curves across high-cardinality filter columns to accelerate Parquet data skipping. As in Spark, partition filters may name partition columns only, and Z-order columns may not be partition columns.
* **Vacuum**: Purges obsolete data files older than the configured retention threshold.

::: code-group

```csharp [C# (Fluent)]
// 1. Bin-pack small files and apply Z-Order clustering on Id and Device
long compactedFiles = Delta.Optimize(
    path: "./lake/devices",
    targetSizeMb: 128,
    zOrderColumns: new[] { "Id", "Device" }
);

// 2. Purge unreferenced files older than 7 days (168 hours)
long deletedFiles = Delta.Vacuum(
    path: "./lake/devices",
    retentionHours: 168,
    enforceRetention: true
);
```

```fsharp [F# (Pipeline)]
// 1. Optimize layout and cluster with Z-Order
let compactedFiles =
    Delta.Optimize(
        path = "./lake/devices",
        targetSizeMb = 128L,
        zOrderColumns = [| "Id"; "Device" |]
    )

// 2. Vacuum orphaned data files
let deletedFiles =
    Delta.Vacuum(
        path = "./lake/devices",
        retentionHours = 168,
        enforceRetention = true
    )
```

:::

---

## 5. Change Data Feed (CDC)

When `DeltaTableProperties.EnableChangeDataFeed` is enabled, row-level mutations are recorded in the Delta log: writes, deletes, merges and restores feed it. Consumers can subscribe to these mutations incrementally without scanning the full table.

> [!NOTE] Reading the feed
> As in Spark, the feed can be read only for versions at which it was enabled; a range that starts earlier is refused. `_commit_version` is `Int64` and `_commit_timestamp` is `Datetime(us, UTC)`.

::: code-group

```csharp [C# (Fluent)]
// Enable Change Data Feed on the table
Delta.SetTableProperties("./lake/devices", new Dictionary<string, string>
{
    [DeltaTableProperties.EnableChangeDataFeed] = "true"
});

// Stream row-level mutations between Version 2 and Version 5
var cdcStream = Delta.ReadChangeDataFeed(
    path: "./lake/devices",
    startVersion: 2,
    endVersion: 5
);

// Results include _change_type ('insert', 'update_preimage', 'update_postimage', 'delete')
var changes = cdcStream.Collect();
```

```fsharp [F# (Pipeline)]
// Enable Change Data Feed on the table
Delta.SetTableProperties("./lake/devices", [ DeltaTableProperties.EnableChangeDataFeed, "true" ])

// Stream incremental CDC changes
let cdcStream =
    Delta.ReadChangeDataFeed(
        path = "./lake/devices",
        startVersion = 2L,
        endVersion = 5L
    )

let changes = cdcStream.Collect()
```

:::

---

## 6. Order-Preserving SQL MERGE (Upsert & Deletion Vectors)

In complex ETL pipelines, out-of-order evaluations of matched and non-matched conditions can cause unexpected data overwrites.

Polars.NET provides `MergeDeltaOrdered`, ensuring strict evaluation matching ANSI SQL MERGE semantics. As in Spark: a source row with a null key matches nothing, several source rows matching one target row are an error only when that row is updated or deleted, and source values are cast to the table's column types:

::: code-group

```csharp [C# (Fluent)]
var sourceBatch = DataFrame.FromColumns(new
{
    Id = new[] { 2, 4, 5 },
    Device = new[] { "A", "B", "C" },
    Reading = new[] { 85.0, 95.5, 33.1 },
    IsDeleted = new[] { false, true, false }
});

// Chain merge actions in strict evaluation order
sourceBatch.MergeDeltaOrdered(
    path: "./lake/devices",
    mergeKeys: new[] { "Id" },
    canEvolve: true // Enable automatic schema evolution
)
// 1. Delete rows marked as deleted in the source
.WhenMatchedDelete(m => m.Source("IsDeleted"))

// 2. Update matched rows when the incoming reading is newer
.WhenMatchedUpdate(m => m.Source("Reading") > m.Target("Reading"))

// 3. Insert non-matching rows from the source
.WhenNotMatchedInsert()

// 4. Atomically commit the transaction
.Execute();
```

```fsharp [F# (Pipeline)]
let sourceBatch =
    DataFrame.create [
        pl.series "Id" [ 2; 4; 5 ]
        pl.series "Device" [ "A"; "B"; "C" ]
        pl.series "Reading" [ 85.0; 95.5; 33.1 ]
        pl.series "IsDeleted" [ false; true; false ]
    ]

sourceBatch.MergeDeltaOrdered("./lake/devices", [| "Id" |], canEvolve = true)
    .WhenMatchedDelete(Delta.Source("IsDeleted") .== pl.lit true)
    .WhenMatchedUpdate(Delta.Source("Reading") .> Delta.Target("Reading"))
    .WhenNotMatchedInsert()
    .Execute()
```

:::

### Deletion Vectors & Merge-on-Read (MoR)

> [!TIP] High-Throughput Updates and Deletions
> By default, Delta Lake uses **Copy-on-Write (CoW)**: updating or deleting a single row requires rewriting the entire containing Parquet file.
> 
> When Deletion Vectors are enabled via `Delta.AddFeature(path, DeltaTableFeatures.DeletionVectors)` (which also sets `delta.enableDeletionVectors`), Polars.NET switches to **Merge-on-Read (MoR)**. Deleted rows are recorded in lightweight bitmap vector files without rewriting underlying Parquet files. During subsequent `Delta.Optimize` operations, affected partitions are compacted and rewritten cleanly in the background.

### Update

`Delta.Update` runs SQL's `UPDATE table SET col = expr WHERE predicate`. Each value is evaluated over the row before the update and cast to the column's type; columns not named keep their values, and a null predicate updates every row. Identity columns cannot be updated. The commit is an `UPDATE` in the history.

To update target rows from a frame by key, `UpdateDelta` is a merge with only a `WHEN MATCHED UPDATE` clause: unmatched rows of the frame are ignored, nothing is inserted. A condition can compare both sides: `m.Source(name)` is the frame's column, `m.Target(name)` the table's (`Delta.Source` / `Delta.Target` in F#).

::: code-group

```csharp [C# (Fluent)]
// Double the reading of device 2 only
Delta.Update("./lake/devices", Polars.Col("Id") == 2,
    new Dictionary<string, Expr> { ["Reading"] = Polars.Col("Reading") * 2.0 });

// Update matched rows by key; unmatched rows are not inserted
sourceBatch.UpdateDelta("./lake/devices", mergeKeys: new[] { "Id" });

// Only where the incoming reading is higher than the stored one
sourceBatch.UpdateDelta("./lake/devices", new[] { "Id" }, m => m.Source("Reading") > m.Target("Reading"));
```

```fsharp [F# (Pipeline)]
// Double the reading of device 2 only
Delta.Update(
    "./lake/devices",
    [ "Reading", pl.col "Reading" * pl.lit 2.0 ],
    predicate = (pl.col "Id" .== pl.lit 2L))

// Update matched rows by key; unmatched rows are not inserted
sourceBatch.UpdateDelta("./lake/devices", [ "Id" ])

// Only where the incoming reading is higher than the stored one
sourceBatch.UpdateDelta("./lake/devices", [ "Id" ], condition = (Delta.Source "Reading" .> Delta.Target "Reading"))
```

:::

---

## 7. Identity Columns & Row Tracking

`Delta.CreateTable` creates an empty table, as Spark's `CREATE TABLE`: schema, partition columns, table properties and identity columns. An identity column is an `Int64` column whose values writes generate, `start`, `start + step`, … after the highest value given so far. With `generatedAlways` (the default) writes may not bring values for it; otherwise (GENERATED BY DEFAULT) rows that bring a value keep it. Identity columns are never updated.

With `DeltaTableProperties.EnableRowTracking`, every row keeps a stable row id through updates, merges and optimize; `ScanDelta` / `ReadDelta` return it, and the version that last changed the row, under the names given (Spark's `_metadata.row_id` and `_metadata.row_commit_version`). Set on an existing table, the property gives the rows already there their ids.

`DeltaTableProperties.RowTrackingSuspended` stops giving ids to new files, as Spark's `delta.rowTrackingSuspended`; it cannot be true while row tracking is on, so set `EnableRowTracking` to false in the same `SetTableProperties` call. Setting it back to false (with `EnableRowTracking` true in the same call, if wanted) resumes the ids after the highest one given, and gives the files written meanwhile theirs.

::: code-group

```csharp [C# (Fluent)]
Delta.CreateTable(
    "./lake/orders",
    new PolarsSchema().Add("order_id", DataType.Int64).Add("item", DataType.String),
    properties: new() { [DeltaTableProperties.EnableRowTracking] = "true" },
    identityColumns: [new DeltaIdentityColumn("order_id", Start: 1000, Step: 1)]);

// order_id is generated: 1000, 1001, ...
using var items = DataFrame.FromColumns(new { item = new[] { "pen", "ink" } });
items.WriteDelta("./lake/orders", mode: DeltaSaveMode.Append);

var withIds = DataFrame.ReadDelta("./lake/orders", rowIdColumn: "row_id", rowCommitVersionColumn: "row_version");
```

```fsharp [F# (Pipeline)]
Delta.CreateTable(
    "./lake/orders",
    PolarsSchema([ "order_id", DataType.Int64; "item", DataType.String ]),
    properties = [ DeltaTableProperties.EnableRowTracking, "true" ],
    identityColumns = [ DeltaIdentityColumn("order_id", start = 1000L) ])

let items = DataFrame.create [| Series.create("item", [ "pen"; "ink" ]) |]
items.WriteDelta("./lake/orders", mode = DeltaSaveMode.Append)

let withIds = DataFrame.ReadDelta("./lake/orders", rowIdColumn = "row_id", rowCommitVersionColumn = "row_version")
```

:::

---

## 8. Concurrency

Writers coordinate through the log alone: a commit is a put-if-absent of the next log file. When another writer committed first, a commit that does not conflict with it lands after it; a delete, update, merge or optimize that does conflict is planned again from the new table state and retried, up to `POLARS_DELTA_MAX_RETRIES` times (5 by default). Files written by a failed attempt are removed again.

