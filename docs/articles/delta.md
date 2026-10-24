# Delta Lake & Modern Lakehouse Storage

[Delta Lake](https://delta.io/) is an open storage format that brings **ACID transactions**, **Time Travel**, **metadata versioning**, and **high-throughput MERGE/Upsert** capabilities on top of Apache Parquet data files.

Traditional .NET applications accessing Delta Lake often rely on external JVM bridges, Apache Spark clusters, or REST services. **Polars.NET integrates native Rust `delta-rs` bindings**, allowing you to read, write, time-travel, and maintain Delta Lake tables directly within a single .NET process without any JVM dependencies.

---

## 1. Reading, Writing & Partitioning

Polars.NET supports both Eager (`DataFrame`) and Lazy (`LazyFrame`) modes for Delta Lake tables:
* **Reading**: Automatically parses the `_delta_log` transaction protocol and pushes down partition filters.
* **Writing**: Atomically writes Parquet files and commits ACID transactions to the Delta transaction log.

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
    partitionBy = pl.col "Device",
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

Every commit to a Delta table records a discrete snapshot in the transaction log. You can query any prior snapshot by **Version** or by ISO-8601 **Timestamp**:

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
let newVersion = Delta.Restore("./lake/devices", version: 1UL)
```

:::

### Live Execution Output: ACID Commit History Table (`Delta.History()`)

<!--@include: ../snippets/delta_history.html-->

---

## 4. Compaction & File Hygiene (`Optimize` & `Vacuum`)

Frequent incremental writes create many small Parquet files. This fragmentation degrades read performance.

Polars.NET provides native table maintenance operations:
* **Bin-packing**: Merges small files into target sizes (e.g. 128 MB).
* **Z-Order Clustering**: Constructs multidimensional Space-Filling curves across high-cardinality filter columns to accelerate Parquet data skipping.
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

When `DeltaTableProperties.EnableChangeDataFeed` is enabled, row-level mutations are recorded in the Delta log. Consumers can subscribe to these mutations incrementally without scanning the full table:

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

Polars.NET provides `MergeDeltaOrdered`, ensuring strict evaluation matching ANSI SQL MERGE semantics:

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
.WhenMatchedDelete(Delta.Source("IsDeleted") == true)

// 2. Update matched rows when the incoming reading is newer
.WhenMatchedUpdate(Delta.Source("Reading") > Delta.Target("Reading"))

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
> When Deletion Vectors are enabled via `Delta.AddFeature(path, DeltaTableFeatures.DeletionVectors)`, Polars.NET switches to **Merge-on-Read (MoR)**. Deleted rows are recorded in lightweight bitmap vector files without rewriting underlying Parquet files. During subsequent `Delta.Optimize` operations, affected partitions are compacted and rewritten cleanly in the background.
