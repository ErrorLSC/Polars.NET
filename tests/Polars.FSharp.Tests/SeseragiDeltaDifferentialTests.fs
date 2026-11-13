/// Differential tests of the managed Delta kernel: tables are written by the current engine, then every version
/// is read both by the engine and by the kernel, and the two must agree.
module Polars.FSharp.Tests.SeseragiDeltaDifferentialTests

open System
open System.IO
open System.Text.Json
open Xunit
open Polars.FSharp
open Kaminari.Seseragi.Lakehouse.Delta
open Kaminari.Seseragi.Lakehouse.Engine

let private numRecords (stats: string) =
    Assert.False(isNull stats, "the data file has no statistics")
    use doc = JsonDocument.Parse stats
    doc.RootElement.GetProperty("numRecords").GetInt64()

/// The number of live rows the kernel's snapshot describes: records of each file minus its deleted rows.
let private liveRows (snapshot: Snapshot) =
    snapshot.Files
    |> Seq.sumBy (fun f ->
        let deleted = if isNull f.DeletionVector then 0L else f.DeletionVector.Cardinality
        numRecords f.Stats - deleted)

/// Checks every version of the table: row count, column names and partition columns agree with the engine,
/// and at the latest version every listed data file exists.
let private assertKernelAgrees (path: string) (partitionColumns: string list) =
    let fs = LocalFileSystem path
    let latest = Snapshot.Load fs

    for v in 0L .. latest.Version do
        let snapshot = Snapshot.Load(fs, Nullable v)
        use expected = LazyFrame.ScanDelta(path, version = uint64 v).Collect()

        Assert.Equal(v, snapshot.Version)
        Assert.Equal(expected.Height, liveRows snapshot)
        Assert.Equal<string list>(
            expected.ColumnNames |> Seq.sort |> List.ofSeq,
            snapshot.Schema.Fields |> Seq.map (fun f -> f.Name) |> Seq.sort |> List.ofSeq)
        Assert.Equal<string list>(partitionColumns, List.ofSeq snapshot.PartitionColumns)

        // Row for row, the scan built from the kernel's snapshot gives what the engine's own reader gives.
        // (Not where the current engine wrote a deletion vector: the kernel refuses those by design.)
        if snapshot.Files |> Seq.forall (fun f -> isNull f.DeletionVector) then
            use viaKernel = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan(path, Nullable v))).Collect()
            let columns = expected.ColumnNames |> Seq.map pl.col |> List.ofSeq
            use left = viaKernel.Select(columns).Sort(columns)
            use right = expected.Sort(columns)
            Assert.True(left.Equals(right), $"version {v} differs\nkernel:\n{left}\nengine:\n{right}")

    for file in latest.Files do
        Assert.True(File.Exists(Path.Combine(path, file.DecodedPath)), $"missing data file {file.DecodedPath}")
        // Deletion vector files are not checked: the current engine stores them at a location the protocol does
        // not define (a path built from the table URL), and the kernel resolves the path the protocol defines.
        if not (isNull file.DeletionVector) && not file.DeletionVector.IsInline then
            Assert.StartsWith("deletion_vector_", Path.GetFileName(file.DeletionVector.FilePath()))

    latest

/// A table's storage that records the checkpoint files read.
type private CheckpointCounting(inner: IFileSystem) =
    member val Reads = System.Collections.Concurrent.ConcurrentBag<string>() with get

    interface IFileSystem with
        member _.List(directory, startFrom) = inner.List(directory, startFrom)
        member this.ReadAll path =
            if path.Contains ".checkpoint." || path.Contains "_sidecars/" then this.Reads.Add path
            inner.ReadAll path
        member this.ReadRange(path, offset, length) =
            if path.Contains ".checkpoint." || path.Contains "_sidecars/" then this.Reads.Add path
            inner.ReadRange(path, offset, length)

let private fileKeys (snapshot: Snapshot) =
    snapshot.Files
    |> Seq.map (fun f -> f.Path, (if isNull f.DeletionVector then "" else f.DeletionVector.UniqueId), f.Size, f.Stats)
    |> Seq.sort
    |> List.ofSeq

let private withTable (name: string) (body: string -> unit) =
    let path = Path.Combine(Path.GetTempPath(), $"seseragi_diff_{name}_{Guid.NewGuid():N}")
    try
        body path
    finally
        if Directory.Exists path then Directory.Delete(path, true)

let private frame (ids: int list) (region: string) =
    DataFrame.create [|
        Series.create("id", ids)
        Series.create("region", ids |> List.map (fun _ -> region))
        Series.create("price", ids |> List.map (fun i -> float i * 1.5))
    |]

[<Fact>]
let ``Kernel agrees after overwrite, append and overwrite`` () =
    withTable "append" (fun path ->
        (frame [1; 2; 3] "east").WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)
        (frame [4; 5] "west").WriteDelta(path, mode = DeltaSaveMode.Append)
        (frame [6] "north").WriteDelta(path, mode = DeltaSaveMode.Append)
        (frame [7; 8] "south").WriteDelta(path, mode = DeltaSaveMode.Overwrite)

        let latest = assertKernelAgrees path []
        Assert.Equal(2L, liveRows latest))

[<Fact>]
let ``Kernel agrees on a partitioned table`` () =
    withTable "partitioned" (fun path ->
        let both = (frame [1; 2; 3] "east").VStack(frame [4; 5] "west")
        both.WriteDelta(path, partitionBy = pl.cs.byName ["region"], mode = DeltaSaveMode.Overwrite, mkdir = true)
        (frame [6] "east").WriteDelta(path, partitionBy = pl.cs.byName ["region"], mode = DeltaSaveMode.Append)

        let latest = assertKernelAgrees path ["region"]
        let regions = latest.Files |> Seq.map (fun f -> f.PartitionValues["region"]) |> Seq.distinct |> Seq.sort |> List.ofSeq
        Assert.Equal<string list>(["east"; "west"], regions))

[<Fact>]
let ``Kernel agrees after deletes with and without deletion vectors`` () =
    withTable "delete" (fun path ->
        (frame [1 .. 10] "east").WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)
        Delta.Delete(path, pl.col "id" .< pl.lit 3)

        Delta.AddFeature(path, DeltaTableFeatures.DeletionVectors)
        Delta.Delete(path, pl.col "id" .< pl.lit 5)
        Delta.Delete(path, pl.col "id" .> pl.lit 8)

        let latest = assertKernelAgrees path []
        Assert.Equal(4L, liveRows latest)
        Assert.Contains(latest.Files, fun f -> not (isNull f.DeletionVector))
        Assert.Contains("deletionVectors", latest.Protocol.ReaderFeatures))

[<Fact>]
let ``Kernel agrees after merge and optimize`` () =
    withTable "merge" (fun path ->
        (frame [1; 2; 3] "east").WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)
        (frame [4; 5] "east").WriteDelta(path, mode = DeltaSaveMode.Append)

        let source =
            DataFrame.create [|
                Series.create("id", [2; 3; 9])
                Series.create("region", ["west"; "west"; "west"])
                Series.create("price", [100.0; 0.5; 9.0])
            |]
        source.MergeDeltaOrdered(path, mergeKeys = ["id"])
              .WhenMatchedUpdate(Delta.Source "price" .> Delta.Target "price")
              .WhenNotMatchedInsert(pl.lit true)
              .Execute()

        Delta.Optimize(path, targetSizeMb = 128L) |> ignore

        let latest = assertKernelAgrees path []
        Assert.Equal(6L, liveRows latest))

[<Fact>]
let ``Optimize leaves no deletion vectors behind`` () =
    // The kernel refuses the deletion vectors the current engine writes; users are told to optimize first.
    // This holds only if optimize really rewrites every file that carries one.
    withTable "purge" (fun path ->
        (frame [1 .. 10] "east").WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)
        Delta.AddFeature(path, DeltaTableFeatures.DeletionVectors)
        Delta.Delete(path, pl.col "id" .< pl.lit 4)
        Assert.Contains((Snapshot.Load(LocalFileSystem path)).Files, fun f -> not (isNull f.DeletionVector))

        Delta.Optimize(path, targetSizeMb = 128L) |> ignore

        let latest = assertKernelAgrees path []
        Assert.Equal(7L, liveRows latest)
        Assert.DoesNotContain(latest.Files, fun f -> not (isNull f.DeletionVector)))

let private sampleTable (name: string) =
    let rec up (dir: DirectoryInfo) =
        if isNull dir then failwith "repository root not found"
        elif File.Exists(Path.Combine(dir.FullName, "Polars.NET.slnx")) then dir.FullName
        else up dir.Parent
    Path.Combine(up (DirectoryInfo AppContext.BaseDirectory), "tests", "Seseragi.Lakehouse.Tests", "TestData", name)

/// Live rows counted from the data files themselves, for tables whose log carries no numRecords.
let private liveRowsFromFiles (path: string) (fs: IFileSystem) (snapshot: Snapshot) =
    snapshot.Files
    |> Seq.sumBy (fun f ->
        use df = LazyFrame.ScanParquet(Path.Combine(path, f.DecodedPath)).Select([ pl.len () ]).Collect()
        let rows = Convert.ToInt64(df.Row(0).[0])
        let deleted =
            if isNull f.DeletionVector then 0L
            else Kaminari.Seseragi.Lakehouse.Delta.Actions.DeletionVector.Read(fs, f.DeletionVector).Cardinality
        rows - deleted)

/// Tables written by Spark or Databricks, read through their checkpoints.
/// expectedRows: the live row count when known; -1 to compare with the engine's own reader; -2 when the sample
/// ships without its data files.
[<Theory>]
[<InlineData("with_checkpoint_no_last_checkpoint", true, -2L)>]
[<InlineData("app-txn-checkpoint", true, -1L)>]
[<InlineData("v1-multi-part-struct-stats-only", true, -1L)>]
[<InlineData("v2-parquet-sidecars-struct-stats-only", true, -1L)>]
[<InlineData("v2-json-sidecars-struct-stats-only", true, -1L)>]
// The current engine cannot read deletion vectors written by other engines, so these two have fixed counts.
[<InlineData("table-with-dv-small", false, 8L)>]
[<InlineData("with-short-dv", false, 7L)>]
let ``Kernel reads tables other engines wrote`` (name: string, hasCheckpoint: bool, expectedRows: int64) =
    let path = sampleTable name
    let fs = CheckpointCounting(LocalFileSystem path)
    let snapshot = Snapshot.Load fs

    Assert.Equal(hasCheckpoint, fs.Reads.Count > 0)

    // The same state must come out of the JSON commits alone.
    let fromCommits = Snapshot.Load(fs, Nullable(), false)
    let keys (s: Snapshot) =
        s.Files
        |> Seq.map (fun f ->
            f.Path,
            (if isNull f.DeletionVector then "" else f.DeletionVector.UniqueId),
            f.PartitionValues |> Seq.sortBy (fun kv -> kv.Key) |> Seq.map (fun kv -> $"{kv.Key}={kv.Value}") |> String.concat ",")
        |> Seq.sort
        |> List.ofSeq
    Assert.Equal(fromCommits.Version, snapshot.Version)
    Assert.Equal<(string * string * string) list>(keys fromCommits, keys snapshot)
    Assert.Equal(fromCommits.Metadata.SchemaString, snapshot.Metadata.SchemaString)
    Assert.Equal<string seq>(fromCommits.Protocol.ReaderFeatures, snapshot.Protocol.ReaderFeatures)
    Assert.Equal<string list>(
        fromCommits.AppTransactions |> Seq.map (fun kv -> $"{kv.Key}={kv.Value.Version}") |> Seq.sort |> List.ofSeq,
        snapshot.AppTransactions |> Seq.map (fun kv -> $"{kv.Key}={kv.Value.Version}") |> Seq.sort |> List.ofSeq)

    // Statistics survive the checkpoint, including one that keeps them only as a struct (stats_parsed).
    let recordsOf (s: Snapshot) =
        s.Files
        |> Seq.filter (fun f -> not (isNull f.Stats))
        |> Seq.map (fun f -> f.Path, f.ParsedStats.NumRecords)
        |> Map.ofSeq
    let committed = recordsOf fromCommits
    let checkpointed = recordsOf snapshot
    for KeyValue(file, records) in committed do
        Assert.True(checkpointed.ContainsKey file, $"{file} lost its statistics in the checkpoint")
        Assert.Equal(records, checkpointed.[file])

    if expectedRows = -1L then
        use expected = LazyFrame.ScanDelta(path).Collect()
        Assert.Equal(expected.Height, liveRowsFromFiles path fs snapshot)
    elif expectedRows >= 0L then
        Assert.Equal(expectedRows, liveRowsFromFiles path fs snapshot)

// ---------------------------------------------------------------------------------------------------------------
// Delta Acceptance Tests, reader cases. The data is fetched on demand (TestData/README.md); without it the
// theory has no rows.
// ---------------------------------------------------------------------------------------------------------------

let private datRoot = Path.Combine(sampleTable "dat", "out", "reader_tests", "generated")

let datCases () : seq<obj[]> =
    if not (Directory.Exists datRoot) then Seq.empty
    else
        seq {
            for caseDir in Directory.GetDirectories datRoot |> Array.sort do
                for expectedDir in Directory.GetDirectories(Path.Combine(caseDir, "expected")) |> Array.sort do
                    yield [| box (Path.GetFileName caseDir); box (Path.GetFileName expectedDir) |]
        }

[<Theory>]
[<MemberData(nameof datCases)>]
let ``Delta Acceptance Tests reader case`` (caseName: string, expectedName: string) =
    let tablePath = Path.Combine(datRoot, caseName, "delta")
    let expectedDir = Path.Combine(datRoot, caseName, "expected", expectedName)
    use metadata = JsonDocument.Parse(File.ReadAllText(Path.Combine(expectedDir, "table_version_metadata.json")))
    let expectedVersion = metadata.RootElement.GetProperty("version").GetInt64()

    let fs = LocalFileSystem tablePath
    let snapshot =
        if expectedName = "latest" then Snapshot.Load fs
        else Snapshot.Load(fs, Nullable expectedVersion)

    Assert.Equal(expectedVersion, snapshot.Version)
    Assert.Equal(metadata.RootElement.GetProperty("min_reader_version").GetInt32(), snapshot.Protocol.MinReaderVersion)
    Assert.Equal(metadata.RootElement.GetProperty("min_writer_version").GetInt32(), snapshot.Protocol.MinWriterVersion)
    Assert.Equal<string list>(
        [ for p in metadata.RootElement.GetProperty("properties").EnumerateObject() -> $"{p.Name}={p.Value.GetString()}" ] |> List.sort,
        [ for kv in snapshot.Metadata.Configuration -> $"{kv.Key}={kv.Value}" ] |> List.sort)

    let content = Path.Combine(expectedDir, "table_content")
    let expectedRows =
        Directory.GetFiles(content, "*.parquet")
        |> Array.sumBy (fun f ->
            use df = LazyFrame.ScanParquet(f).Select([ pl.len () ]).Collect()
            Convert.ToInt64(df.Row(0).[0]))
    Assert.Equal(expectedRows, liveRowsFromFiles tablePath fs snapshot)

/// The whole road: the kernel resolves the snapshot, the engine scans what it lists, and the rows must be the
/// ones the Delta Acceptance Tests expect.
[<Theory>]
[<MemberData(nameof datCases)>]
let ``Delta Acceptance Tests content through the kernel scan`` (caseName: string, expectedName: string) =
    let tablePath = Path.Combine(datRoot, caseName, "delta")
    let expectedDir = Path.Combine(datRoot, caseName, "expected", expectedName)
    use metadata = JsonDocument.Parse(File.ReadAllText(Path.Combine(expectedDir, "table_version_metadata.json")))
    let version = if expectedName = "latest" then Nullable() else Nullable(metadata.RootElement.GetProperty("version").GetInt64())

    use actual = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan(tablePath, version))).Collect()
    use expectedRaw = LazyFrame.ScanParquet(Path.Combine(expectedDir, "table_content", "*.parquet")).Collect()

    Assert.Equal<string list>(expectedRaw.ColumnNames |> Seq.sort |> List.ofSeq, actual.ColumnNames |> Seq.sort |> List.ofSeq)
    Assert.Equal(expectedRaw.Height, actual.Height)

    // Order rows by the plain columns, then compare everything.
    let snapshot = Snapshot.Load(LocalFileSystem tablePath, version)
    let sortColumns =
        snapshot.Schema.Fields
        |> Seq.filter (fun f -> f.Type :? Schema.PrimitiveType && f.Type.ToString() <> "binary" || f.Type :? Schema.DecimalType)
        |> Seq.map (fun f -> pl.col f.Name)
        |> List.ofSeq
    let columns = actual.ColumnNames |> Seq.map pl.col |> List.ofSeq
    use expected = expectedRaw.Select(columns).Sort sortColumns
    use sorted = actual.Sort sortColumns
    Assert.True(sorted.Equals expected, $"content differs\nactual:\n{sorted}\nexpected:\n{expected}")

/// Files written before the table schema widened a column or added a nested field must still read.
[<Fact>]
let ``Kernel scan reads files narrower than the table schema`` () =
    withTable "widening" (fun path ->
        Directory.CreateDirectory(Path.Combine(path, "_delta_log")) |> ignore
        // An old file: id is a 32-bit integer, score a 32-bit float, extra a column the table no longer has.
        let old =
            DataFrame.create [|
                Series.create("id", [1; 2])
                Series.create("score", [1.5f; 2.5f])
                Series.create("extra", ["x"; "y"])
            |]
        old.WriteParquet(Path.Combine(path, "old.parquet")) |> ignore
        // A new file with the current types and the column added later.
        let recent =
            DataFrame.create [|
                Series.create("id", [3L])
                Series.create("score", [3.5])
                Series.create("note", ["new"])
            |]
        recent.WriteParquet(Path.Combine(path, "new.parquet")) |> ignore

        let schema = """{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"note\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"""
        File.WriteAllLines(
            Path.Combine(path, "_delta_log", "00000000000000000000.json"),
            [ """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""
              sprintf """{"metaData":{"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"%s","partitionColumns":[],"configuration":{}}}""" schema
              """{"add":{"path":"old.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"""
              """{"add":{"path":"new.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}""" ])

        use df = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect().Sort("id")

        Assert.Equal<string list>(["id"; "score"; "note"], List.ofSeq df.ColumnNames)
        Assert.Equal(3L, df.Height)
        Assert.Equal("3", df.Select(pl.col "id").Row(2).[0].ToString())
        Assert.Equal("2.5", df.Select(pl.col "score").Row(1).[0].ToString())
        Assert.Equal("new", df.Select(pl.col "note").Row(2).[0].ToString())
        Assert.Equal("i64", df.Schema.["id"].ToString())
        Assert.Equal("f64", df.Schema.["score"].ToString()))

/// The object-store file system must behave like the plain local one. Run on a local path here, where the two
/// can be compared; on a cloud URL it is the same code with another store behind it.
[<Theory>]
[<InlineData("v2-parquet-sidecars-struct-stats-only")>]
[<InlineData("with-short-dv")>]
[<InlineData("app-txn-checkpoint")>]
let ``Object store file system agrees with the local one`` (name: string) =
    let path = sampleTable name
    let local = LocalFileSystem path
    use store = new Polars.NET.Core.Delta.ObjectStoreFileSystem(path)
    let fs = store :> IFileSystem

    for dir in [ "_delta_log"; "_delta_log/_sidecars"; ""; "no_such_directory" ] do
        let expected = local.List dir |> Seq.map (fun f -> f.Path, f.Size) |> List.ofSeq
        let actual = fs.List dir |> Seq.map (fun f -> f.Path, f.Size) |> List.ofSeq
        Assert.Equal<(string * int64) list>(expected, actual)
        for a, b in Seq.zip (local.List dir) (fs.List dir) do
            Assert.True(abs (a.LastModified - b.LastModified).TotalSeconds < 1.0, $"modification time of {a.Path}")

    // Listing from an offset: a version prefix, an exact file name (included), and inside a subdirectory.
    let logEntries = local.List "_delta_log"
    let exactName = Path.GetFileName(logEntries.[logEntries.Count - 1].Path)
    for dir, from in [ "_delta_log", "00000000000000000003"; "_delta_log", exactName; "_delta_log/_sidecars", "0"; "_delta_log", "zzz" ] do
        Assert.Equal<string list>(
            local.List(dir, from) |> Seq.map (fun f -> f.Path) |> List.ofSeq,
            fs.List(dir, from) |> Seq.map (fun f -> f.Path) |> List.ofSeq)
    Assert.Contains(fs.List("_delta_log", exactName), fun f -> f.Path = "_delta_log/" + exactName)

    let first = (local.List "_delta_log").[0].Path
    let whole = local.ReadAll first
    Assert.Equal<byte[]>(whole, fs.ReadAll first)
    // An absolute URI, as the log may hold for files outside the table.
    let absolute = Uri(Path.Combine(path, first)).AbsoluteUri
    Assert.Equal<byte[]>(whole, fs.ReadAll absolute)
    Assert.Equal<byte[]>(whole, local.ReadAll absolute)
    Assert.Equal<byte[]>(local.ReadRange(first, 5L, 20), fs.ReadRange(first, 5L, 20))
    Assert.Equal<byte[]>(local.ReadRange(first, int64 whole.Length - 3L, 50), fs.ReadRange(first, int64 whole.Length - 3L, 50))
    Assert.Empty(fs.ReadRange(first, int64 whole.Length + 10L, 5))
    Assert.Null(fs.ReadAll "_delta_log/missing.json")
    Assert.Null(fs.ReadRange("missing/missing.bin", 0L, 4))

    // The same snapshot comes out, and deletion vectors read through range requests.
    let viaLocal = Snapshot.Load local
    let viaStore = Snapshot.Load fs
    Assert.Equal(viaLocal.Version, viaStore.Version)
    Assert.Equal<string list>(
        viaLocal.Files |> Seq.map (fun f -> f.Path) |> Seq.sort |> List.ofSeq,
        viaStore.Files |> Seq.map (fun f -> f.Path) |> Seq.sort |> List.ofSeq)
    for file in viaStore.Files do
        if not (isNull file.DeletionVector) then
            Assert.Equal<int64[]>(
                Kaminari.Seseragi.Lakehouse.Delta.Actions.DeletionVector.Read(local, file.DeletionVector).DeletedRows,
                Kaminari.Seseragi.Lakehouse.Delta.Actions.DeletionVector.Read(fs, file.DeletionVector).DeletedRows)

/// A table given as a URL takes the object-store road end to end: log, checkpoint, deletion vectors and data.
[<Theory>]
[<InlineData("with-short-dv", 7L)>]
[<InlineData("v2-parquet-sidecars-struct-stats-only", -1L)>]
let ``Kernel scan of a table URL goes through the object store`` (name: string, expectedRows: int64) =
    let path = sampleTable name
    use viaUrl = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan("file://" + path))).Collect()
    use viaPath = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect()

    if expectedRows >= 0L then Assert.Equal(expectedRows, viaUrl.Height)
    let columns = viaPath.ColumnNames |> Seq.map pl.col |> List.ofSeq
    use left = viaUrl.Sort(columns)
    use right = viaPath.Sort(columns)
    Assert.True(left.Equals(right), $"url:\n{left}\npath:\n{right}")

// ---------------------------------------------------------------------------------------------------------------
// Change data feed
// ---------------------------------------------------------------------------------------------------------------

let private rowKeys (df: DataFrame) (columns: string list) =
    use projected = df.Select(columns |> List.map pl.col)
    [ for i in 0 .. int projected.Height - 1 ->
        projected.Row(i) |> Seq.map (fun v -> if isNull v then "<null>" else string v) |> String.concat "|" ]

/// Replaying the feed from version 0 must rebuild the table: every insert and update_postimage adds a row,
/// every delete and update_preimage takes one away, and what is left is the table at the end version.
/// cdf-table-simple and cdf-table-update-ops are left out on purpose: they are hand-made, with commits that
/// carry cdc actions but no add, so the tables are not self-consistent.
[<Theory>]
[<InlineData("cdf-table-partitioned")>]
[<InlineData("cdf-table-non-partitioned")>]
[<InlineData("cdf-table-with-dv")>]
[<InlineData("cdf-table-with-cdc-and-dvs")>]
let ``Change feed replays to the table`` (name: string) =
    let path = Path.Combine(sampleTable "cdf", name)
    use changes = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, 0L))).Collect()
    use final = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect()

    let tableColumns = List.ofSeq final.ColumnNames
    Assert.Equal<string list>(
        tableColumns @ [ "_change_type"; "_commit_version"; "_commit_timestamp" ],
        List.ofSeq changes.ColumnNames)
    Assert.True(changes.Height > 0L)

    let net = Collections.Generic.Dictionary<string, int>()
    let keys = rowKeys changes tableColumns
    let kinds = rowKeys changes [ "_change_type" ]
    for key, kind in List.zip keys kinds do
        let sign =
            match kind with
            | "insert" | "update_postimage" -> 1
            | "delete" | "update_preimage" -> -1
            | other -> failwith $"unexpected change type {other}"
        net.[key] <- (match net.TryGetValue key with | true, n -> n | _ -> 0) + sign

    let replayed = [ for kv in net do if kv.Value <> 0 then yield kv.Key, kv.Value ] |> List.sort
    let actual = rowKeys final tableColumns |> List.countBy id |> List.sort
    Assert.Equal<(string * int) list>(actual, replayed)

    // Versions never decrease down the frame, and each has one timestamp.
    let versions = rowKeys changes [ "_commit_version" ] |> List.map int64
    Assert.Equal<int64 list>(List.sort versions, versions)
    let stamps = rowKeys changes [ "_commit_version"; "_commit_timestamp" ] |> List.distinct
    Assert.Equal(versions |> List.distinct |> List.length, stamps.Length)

[<Fact>]
let ``Change feed of a range, by time, and when it is empty`` () =
    let path = Path.Combine(sampleTable "cdf", "cdf-table-simple")
    let fs = LocalFileSystem path
    let timeline = Log.CommitTimeline.Load fs

    use all = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, 0L))).Collect()
    use lastOnly = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, timeline.LatestVersion, Nullable timeline.LatestVersion))).Collect()
    Assert.True(lastOnly.Height > 0L && lastOnly.Height < all.Height)
    Assert.Equal<string list>([ string timeline.LatestVersion ], rowKeys lastOnly [ "_commit_version" ] |> List.distinct)

    // The same commit selected by its time.
    let at = timeline.TimestampOf timeline.LatestVersion
    use byTime = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, at, Nullable at))).Collect()
    Assert.Equal(lastOnly.Height, byTime.Height)

    // After the last commit there is nothing, but the columns are still there.
    use empty = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, at.AddDays 1.0))).Collect()
    Assert.Equal(0L, empty.Height)
    Assert.Equal<string list>(List.ofSeq all.ColumnNames, List.ofSeq empty.ColumnNames)

/// cdf-table-update-ops writes update_pre and update_post (as Delta Spark's own tests do), not the protocol's
/// update_preimage and update_postimage. The reader passes the column through as written, like delta-kernel-rs.
[<Fact>]
let ``Change feed passes cdc change types through as written`` () =
    let path = Path.Combine(sampleTable "cdf", "cdf-table-update-ops")
    use changes = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, 0L))).Collect()
    let expected =
        [ for id in 0 .. 9 -> $"{id}|insert|0" ]
        @ [ for id in 20 .. 24 -> $"{id}|update_pre|1" ]
        @ [ for id in 30 .. 34 -> $"{id}|update_post|2" ]
    Assert.Equal<string list>(List.sort expected, rowKeys changes [ "id"; "_change_type"; "_commit_version" ] |> List.sort)

[<Fact>]
let ``Change feed needs the feature unless waived`` () =
    withTable "cdf_own" (fun path ->
        (frame [1; 2; 3] "east").WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)
        (frame [4] "west").WriteDelta(path, mode = DeltaSaveMode.Append)
        Delta.Delete(path, pl.col "id" .< pl.lit 3)

        let ex = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () ->
            Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, 0L) |> ignore)
        Assert.Contains("not enabled", ex.Message)
        // The public API is the kernel: it refuses too (the old engine read such a table).
        let latest = (Snapshot.Load(LocalFileSystem path)).Version
        Assert.ThrowsAny<Exception>(fun () -> Delta.ReadChangeDataFeed(path, 0L, latest).Collect() |> ignore) |> ignore

        // Waived: inserts and deletes from the files the commits add and remove.
        use viaKernel = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, 0L, requireEnabled = false))).Collect()
        let columns = [ "id"; "_change_type"; "_commit_version" ]
        Assert.Contains("2|delete|2", rowKeys viaKernel columns |> List.map (fun k -> k.Replace(" ", ""))))

// ---------------------------------------------------------------------------------------------------------------
// File skipping from LINQ filters (Samidare IR -> kernel predicate, through IFilterAwareSource)
// ---------------------------------------------------------------------------------------------------------------

type KernelRow = { id: int; region: string; price: float }

/// A LINQ query over the table read by the kernel; `scanned` receives the number of data files of each scan.
let private kernelQuery (path: string) (scanned: ResizeArray<int>) =
    let source = Polars.NET.Linq.Backend.Polars.DeltaKernelSource(path, Nullable(), Action<int>(scanned.Add))
    Polars.NET.Linq.PolarsQuery<KernelRow>(source, FSharpRowCursorMaterializer(), Polars.NET.Linq.QueryLanguage.FSharp)

let private assertScanned (expected: int) (scanned: ResizeArray<int>) =
    Assert.NotEmpty scanned
    Assert.All(scanned, fun n -> Assert.Equal(expected, n))
    scanned.Clear()

[<Fact>]
let ``LINQ filters reach the kernel and skip files`` () =
    withTable "skip" (fun path ->
        (frame [ 1..10 ] "east").WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)
        (frame [ 11..20 ] "west").WriteDelta(path, mode = DeltaSaveMode.Append)
        (frame [ 21..30 ] "north").WriteDelta(path, mode = DeltaSaveMode.Append)
        Assert.Equal(3, (Snapshot.Load(LocalFileSystem path)).Files.Count)

        let scanned = ResizeArray<int>()
        let q = kernelQuery path scanned
        let ids (rows: KernelRow seq) = rows |> Seq.map (fun r -> r.id) |> Seq.sort |> List.ofSeq

        // One file holds ids above 25.
        Assert.Equal<int list>([ 26..30 ], query { for r in q do where (r.id > 25); select r } |> ids)
        assertScanned 1 scanned

        // A disjunction over two files.
        Assert.Equal<int list>([ 1; 2; 29; 30 ], query { for r in q do where (r.id < 3 || r.id > 28); select r } |> ids)
        assertScanned 2 scanned

        // Nothing matches: no file is read, and the result is empty.
        Assert.Empty(query { for r in q do where (r.id > 100); select r } |> List.ofSeq)
        assertScanned 0 scanned

        // A condition the kernel cannot read: every file is scanned, the rows are still filtered.
        Assert.Equal<int list>([ 11..20 ], query { for r in q do where (r.region.ToUpper() = "WEST"); select r } |> ids)
        assertScanned 3 scanned

        // A floating comparison: the frontend's NaN guard lets it through.
        Assert.Equal<int list>([ 27..30 ], query { for r in q do where (r.price > 40.0); select r } |> ids)
        assertScanned 1 scanned

        // No filter at all.
        Assert.Equal(30, query { for r in q do select r } |> Seq.length)
        assertScanned 3 scanned)

// ---------------------------------------------------------------------------------------------------------------
// Column mapping inside nested types
// ---------------------------------------------------------------------------------------------------------------

[<Fact>]
let ``Nested column mapping gets logical names back at every depth`` () =
    withTable "nested_cm" (fun path ->
        let logDir = Path.Combine(path, "_delta_log")
        Directory.CreateDirectory logDir |> ignore
        let W = Polars.NET.Core.Wrappers.PolarsWrapper.Alias
        let col (n: string) = Polars.NET.Core.Wrappers.PolarsWrapper.Col n
        let asStruct (fields: Polars.NET.Core.ExprHandle list) = Polars.NET.Core.Wrappers.PolarsWrapper.AsStruct(Array.ofList fields)
        let oneElementList e = Polars.NET.Core.Wrappers.PolarsWrapper.ConcatList [| e |]

        // The data file, under physical names: col-s = struct{col-x, col-l: list<struct{col-z}>}, null in row 3;
        // col-m = map<string, struct{col-v}> (a list of key/value entries).
        use source = DataFrame.create [| Series.create("id", [ 1; 2; 3 ]); Series.create("x", [ 10; 20; 30 ]); Series.create("z", [ "a"; "b"; "c" ]) |]
        let s =
            Polars.NET.Core.Wrappers.PolarsWrapper.IfElse(
                Polars.NET.Core.Wrappers.PolarsWrapper.Eq(col "id", Polars.NET.Core.Wrappers.PolarsWrapper.Lit 3),
                Polars.NET.Core.Wrappers.PolarsWrapper.LitNull(),
                asStruct [ W(col "x", "col-x"); W(oneElementList (asStruct [ W(col "z", "col-z") ]), "col-l") ])
        let m = oneElementList (asStruct [ W(col "z", "key"); W(asStruct [ W(col "x", "col-v") ], "value") ])
        let lf = Polars.NET.Core.Wrappers.PolarsWrapper.LazySelect(source.Lazy().CloneHandle(), [| W(col "id", "col-id"); W(s, "col-s"); W(m, "col-m") |])
        use physical = new DataFrame(Polars.NET.Core.Wrappers.PolarsWrapper.LazyCollect(lf, Polars.NET.Core.Enums.PlEngine.Auto))
        let dataFile = Path.Combine(path, "part-0.parquet")
        physical.WriteParquet dataFile |> ignore

        let field name physicalName id (typeJson: string) =
            $"{{\"name\":\"{name}\",\"type\":{typeJson},\"nullable\":true,\"metadata\":{{\"delta.columnMapping.id\":{id},\"delta.columnMapping.physicalName\":\"{physicalName}\"}}}}"
        let structOf (fields: string list) = $"{{\"type\":\"struct\",\"fields\":[{String.Join(',', fields)}]}}"
        let element = structOf [ field "z" "col-z" 5 "\"string\"" ]
        let mapValue = structOf [ field "v" "col-v" 7 "\"integer\"" ]
        let schema =
            structOf [
                field "id" "col-id" 1 "\"integer\""
                field "s" "col-s" 2 (structOf [
                    field "x" "col-x" 3 "\"integer\""
                    field "l" "col-l" 4 $"{{\"type\":\"array\",\"elementType\":{element},\"containsNull\":true}}" ])
                field "m" "col-m" 6 $"{{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{mapValue},\"valueContainsNull\":true}}" ]
        let metadata =
            JsonSerializer.Serialize(
                {| metaData =
                    {| id = "nested-cm"; format = {| provider = "parquet"; options = dict [] |}; schemaString = schema
                       partitionColumns = Array.empty<string>
                       configuration = dict [ "delta.columnMapping.mode", "name"; "delta.columnMapping.maxColumnId", "7" ]
                       createdTime = 1 |} |})
        let size = FileInfo(dataFile).Length
        File.WriteAllLines(
            Path.Combine(logDir, "00000000000000000000.json"),
            [| """{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}"""
               metadata
               $"{{\"add\":{{\"path\":\"part-0.parquet\",\"partitionValues\":{{}},\"size\":{size},\"modificationTime\":1,\"dataChange\":true,\"stats\":\"{{\\\"numRecords\\\":3}}\"}}}}" |])

        use table = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect()
        use sorted = table.Sort(pl.col "id")
        use json = Polars.NET.Core.Wrappers.PolarsWrapper.WriteJsonNativeMemory(sorted.Handle, Polars.NET.Core.Enums.PlJsonFormat.JsonLines)
        let lines = Text.Encoding.UTF8.GetString(json.Memory.Span).Split('\n', StringSplitOptions.RemoveEmptyEntries)
        Assert.Equal<string[]>(
            [| """{"id":1,"s":{"x":10,"l":[{"z":"a"}]},"m":[{"key":"a","value":{"v":10}}]}"""
               """{"id":2,"s":{"x":20,"l":[{"z":"b"}]},"m":[{"key":"b","value":{"v":20}}]}"""
               """{"id":3,"s":null,"m":[{"key":"c","value":{"v":30}}]}""" |],
            lines))

// ---------------------------------------------------------------------------------------------------------------
// Commits written by the kernel
// ---------------------------------------------------------------------------------------------------------------

/// Writes the rows of one partition as a data file and returns the add action for it.
let private dataFile (tableRoot: string) (ids: int list) (region: string) =
    let relative = $"region={region}/part-{Guid.NewGuid():N}.parquet"
    let full = Path.Combine(tableRoot, relative)
    Directory.CreateDirectory(Path.GetDirectoryName full) |> ignore
    use df = frame ids region
    use data = df.Select([ pl.col "id"; pl.col "price" ])
    data.WriteParquet full |> ignore
    Actions.AddAction(
        Path = relative,
        PartitionValues = readOnlyDict [ ("region", region) ],
        Size = FileInfo(full).Length,
        ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Stats = $"{{\"numRecords\":{ids.Length}}}")

[<Fact>]
let ``Tables the kernel commits are read by the engine at every version`` () =
    withTable "kernel_commit" (fun path ->
        let fs = LocalFileSystem path
        let schema =
            """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"region","type":"string","nullable":true,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}}]}"""
        let metadata =
            Actions.MetadataAction(Id = Guid.NewGuid().ToString(), SchemaString = schema, PartitionColumns = [| "region" |], CreatedTime = Nullable 1L)
        let protocol = Actions.ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2)

        let east = dataFile path [ 1; 2; 3 ] "east"
        Assert.Equal(0L, Transaction.Create(fs, protocol, metadata).Add(east).Commit())
        Assert.Equal(1L, Transaction.Start(fs, Snapshot.Load fs, "WRITE").Add(dataFile path [ 4; 5 ] "west").Commit())
        let remove = Actions.RemoveAction(Path = east.Path, DeletionTimestamp = Nullable(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))
        Assert.Equal(2L, Transaction.Start(fs, Snapshot.Load fs, "DELETE").Add(remove).Commit())

        let expected = [ [ 1; 2; 3 ]; [ 1; 2; 3; 4; 5 ]; [ 4; 5 ] ]
        for version, ids in List.indexed expected do
            use viaEngine = LazyFrame.ScanDelta(path, version = uint64 version).Collect()
            use sorted = viaEngine.Sort(pl.col "id")
            Assert.Equal<int list>(ids, sorted.["id"].ToArray<int>() |> List.ofArray)
            Assert.Equal<string list>([ "id"; "region"; "price" ] |> List.sort, viaEngine.ColumnNames |> Seq.sort |> List.ofSeq)
        assertKernelAgrees path [ "region" ] |> ignore)

[<Fact>]
let ``Concurrent kernel commits on the local disk: appends all land, one per version`` () =
    withTable "kernel_race" (fun path ->
        let fs = LocalFileSystem path
        let schema = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"region","type":"string","nullable":true,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}}]}"""
        let metadata = Actions.MetadataAction(Id = "race", SchemaString = schema, PartitionColumns = [| "region" |])
        Transaction.Create(fs, Actions.ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2), metadata).Commit() |> ignore
        let read = Snapshot.Load fs

        // Without retries exactly one of the writers of version 1 wins.
        let attempts =
            [| for i in 0..7 -> dataFile path [ i ] "east" |]
            |> Array.Parallel.map (fun add ->
                try
                    Transaction.Start(LocalFileSystem path, read, "WRITE").Add(add).Commit(maxRetries = 0) |> ignore
                    1
                with :? DeltaCommitConflictException -> 0)
        Assert.Equal(1, Array.sum attempts)

        // With retries every blind append lands, each in its own version.
        let versions =
            [| for i in 10..17 -> dataFile path [ i ] "west" |]
            |> Array.Parallel.map (fun add -> Transaction.Start(LocalFileSystem path, read, "WRITE").Add(add).Commit())
        Assert.Equal<int64 list>([ 2L .. 9L ], versions |> Array.sort |> List.ofArray)
        let latest = Snapshot.Load fs
        Assert.Equal(9L, latest.Version)
        Assert.Equal(9, latest.Files.Count)
        // No temporary file is left in the log.
        Assert.Empty(Directory.GetFiles(Path.Combine(path, "_delta_log"), "*.tmp", SearchOption.AllDirectories))
        Assert.Equal(10, Directory.GetFiles(Path.Combine(path, "_delta_log")).Length)
        assertKernelAgrees path [ "region" ] |> ignore)

// ---------------------------------------------------------------------------------------------------------------
// Data written through the kernel
// ---------------------------------------------------------------------------------------------------------------

let private kernelWrite (path: string) (df: DataFrame) (mode: Polars.NET.Core.Delta.KernelWriteMode) (partitionBy: string[]) =
    Polars.NET.Core.Delta.DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), mode, partitionBy)

let private typedFrame (ids: int list) (region: string) =
    DataFrame.create [|
        Series.create("id", ids)
        Series.create("region", ids |> List.map (fun _ -> region))
        Series.create("price", ids |> List.map (fun i -> float i * 1.5))
        Series.create("day", ids |> List.map (fun i -> DateOnly(2024, 1, i)))
        Series.create("at", ids |> List.map (fun i -> DateTimeOffset(2024, 1, i, 8, 30, 0, 123, TimeSpan.Zero).AddTicks(4560L)))
        Series.create("amount", ids |> List.map (fun i -> decimal i + 0.25m))
        Series.create("flag", ids |> List.map (fun i -> i % 2 = 0))
        Series.create("note", ids |> List.map (fun i -> if i = 2 then null else String('x', i * 20)))
    |]

[<Fact>]
let ``Kernel writes tables the engine reads: create, append, overwrite`` () =
    withTable "kernel_write" (fun path ->
        use first = typedFrame [ 1; 2; 3 ] "east"
        use second = typedFrame [ 4; 5 ] "west"
        use third = typedFrame [ 6 ] "north"
        Assert.Equal(0L, kernelWrite path first Polars.NET.Core.Delta.KernelWriteMode.ErrorIfExists [| "region" |])
        Assert.Equal(1L, kernelWrite path second Polars.NET.Core.Delta.KernelWriteMode.Append null)
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () ->
            kernelWrite path third Polars.NET.Core.Delta.KernelWriteMode.ErrorIfExists null |> ignore) |> ignore
        Assert.Equal(2L, kernelWrite path third Polars.NET.Core.Delta.KernelWriteMode.Overwrite null)

        // The engine reads every version with the values written, types included.
        for version, expected in [ 0, [ first ]; 1, [ first; second ]; 2, [ third ] ] do
            use viaEngine = LazyFrame.ScanDelta(path, version = uint64 version).Collect()
            let columns = first.ColumnNames |> Seq.map pl.col |> List.ofSeq
            use actual = viaEngine.Select(columns).Sort(pl.col "id")
            use wanted = DataFrame.ConcatVertical(expected |> Seq.map (fun d -> d.Clone())).Sort(pl.col "id")
            Assert.True(actual.Equals(wanted), $"version {version}\nengine:\n{actual}\nwritten:\n{wanted}")
        let latest = assertKernelAgrees path [ "region" ]

        // Statistics let the kernel skip files.
        let v1 = Snapshot.Load(LocalFileSystem path, Nullable 1L)
        Assert.All(v1.Files, fun f -> Assert.NotNull f.ParsedStats)
        let matching = v1.FilesMatching(Kaminari.Seseragi.Lakehouse.Skipping.Predicate.Gt("id", 3L))
        Assert.Equal<string list>([ "west" ], matching |> Seq.map (fun f -> f.PartitionValues.["region"]) |> List.ofSeq)
        let byTime = v1.FilesMatching(Kaminari.Seseragi.Lakehouse.Skipping.Predicate.Lt("at", DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero)))
        Assert.Equal(1, byTime.Count)
        // A string longer than the statistics keep has a minimum but no maximum.
        let east = v1.Files |> Seq.find (fun f -> f.PartitionValues.["region"] = "east")
        use stats = JsonDocument.Parse east.Stats
        Assert.Equal(1L, stats.RootElement.GetProperty("nullCount").GetProperty("note").GetInt64())
        Assert.Equal(String('x', 20), stats.RootElement.GetProperty("minValues").GetProperty("note").GetString())
        Assert.False(stats.RootElement.GetProperty("maxValues").TryGetProperty("note", ref Unchecked.defaultof<JsonElement>))
        Assert.Equal("2024-01-03T08:30:00.123Z", stats.RootElement.GetProperty("maxValues").GetProperty("at").GetString())
        Assert.Equal(2L, latest.Version))

[<Fact>]
let ``Kernel writes awkward partition values and reads them back`` () =
    withTable "kernel_partition_values" (fun path ->
        let regions = [ "a b"; "x/y"; "c:d"; "50%"; "k=v"; "東京"; null; "" ]
        use df =
            DataFrame.create [|
                Series.create("id", [ 1 .. regions.Length ])
                Series.create("region", regions)
            |]
        kernelWrite path df Polars.NET.Core.Delta.KernelWriteMode.ErrorIfExists [| "region" |] |> ignore

        // The current engine cannot read these back: it decodes file paths twice (a defect of the old Rust code;
        // Spark reads the same table correctly, see the Spark interop test). So only the kernel reads here.
        use viaKernel = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        // An empty string partition is stored as null, as Spark does.
        let expected = regions |> List.map (fun r -> if String.IsNullOrEmpty r then null else r)
        Assert.Equal<string list>(expected, viaKernel.["region"].ToArray<string>() |> List.ofArray)
        // Log text is plain UTF-8, not escaped.
        Assert.Contains("東京", File.ReadAllText(Path.Combine(path, "_delta_log", "00000000000000000000.json")))
        Assert.True(Directory.Exists(Path.Combine(path, "region=__HIVE_DEFAULT_PARTITION__")))
        Assert.True(Directory.Exists(Path.Combine(path, "region=x%2Fy"))))

[<Fact>]
let ``Naive timestamps give a timestamp_ntz table`` () =
    withTable "kernel_ntz" (fun path ->
        use df = DataFrame.create [| Series.create("at", [ DateTime(2024, 5, 6, 7, 8, 9) ]) |]
        kernelWrite path df Polars.NET.Core.Delta.KernelWriteMode.ErrorIfExists null |> ignore
        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.Equal(3, snapshot.Protocol.MinReaderVersion)
        Assert.Contains("timestampNtz", snapshot.Protocol.ReaderFeatures)
        Assert.Equal("timestamp_ntz", (snapshot.Schema.Field "at").Type.ToString())
        use back = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect()
        Assert.Equal(DateTime(2024, 5, 6, 7, 8, 9), back.["at"].ToArray<DateTime>().[0]))

// ---------------------------------------------------------------------------------------------------------------
// Checkpoints written by the kernel
// ---------------------------------------------------------------------------------------------------------------

[<Fact>]
let ``Kernel checkpoints give the state of the log, and other readers use them`` () =
    withTable "kernel_checkpoint" (fun path ->
        let write (df: DataFrame) mode =
            Polars.NET.Core.Delta.DeltaKernelWrite.Write(
                path, df.Lazy().CloneHandle(), mode, [| "region" |], null, readOnlyDict [ "delta.checkpointInterval", "4" ]) |> ignore
        use first = typedFrame [ 1; 2 ] "east"
        write first Polars.NET.Core.Delta.KernelWriteMode.ErrorIfExists
        for i in 3..10 do
            use more = typedFrame [ i ] (if i % 2 = 0 then "east" else "west")
            write more Polars.NET.Core.Delta.KernelWriteMode.Append
        // Version 9 removes everything (tombstones), version 10 adds again.
        use last = typedFrame [ 20; 21 ] "north"
        write last Polars.NET.Core.Delta.KernelWriteMode.Overwrite
        use again = typedFrame [ 22 ] "south"
        write again Polars.NET.Core.Delta.KernelWriteMode.Append

        let logDir = Path.Combine(path, "_delta_log")
        let checkpoints = Directory.GetFiles(logDir, "*.checkpoint.parquet") |> Array.map Path.GetFileName |> Array.sort
        Assert.Equal<string[]>([| "00000000000000000004.checkpoint.parquet"; "00000000000000000008.checkpoint.parquet" |], checkpoints)
        use hint = JsonDocument.Parse(File.ReadAllText(Path.Combine(logDir, "_last_checkpoint")))
        Assert.Equal(8L, hint.RootElement.GetProperty("version").GetInt64())

        // Through the checkpoint or from the commits alone, every version has the same state.
        let fs = CheckpointCounting(LocalFileSystem path)
        for v in [ 4L; 5L; 8L; 10L ] do
            let viaCheckpoint = Snapshot.Load(fs, Nullable v)
            let fromCommits = Snapshot.Load(fs, Nullable v, false)
            let files (s: Snapshot) = s.Files |> Seq.map (fun f -> f.Path, f.Stats, f.Size, f.PartitionValues.["region"]) |> Seq.sort |> List.ofSeq
            Assert.Equal<(string * string * int64 * string) list>(files fromCommits, files viaCheckpoint)
            let tombstones (s: Snapshot) = s.Tombstones |> Seq.map (fun r -> r.Path) |> Seq.sort |> List.ofSeq
            Assert.Equal<string list>(tombstones fromCommits, tombstones viaCheckpoint)
            Assert.Equal(fromCommits.Metadata.SchemaString, viaCheckpoint.Metadata.SchemaString)
            Assert.Equal<string list>(List.ofSeq fromCommits.Metadata.Configuration.Keys, List.ofSeq viaCheckpoint.Metadata.Configuration.Keys)
        Assert.True(fs.Reads.Count > 0)

        // The current engine reads the checkpoint with its own Parquet reader, and gets the same rows.
        assertKernelAgrees path [ "region" ] |> ignore

        // With the commits before the checkpoint gone (as log cleanup leaves it), the checkpoint alone is the table.
        for v in 0..8 do File.Delete(Path.Combine(logDir, $"{v:D20}.json"))
        use viaKernel = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        Assert.Equal<int list>([ 20; 21; 22 ], viaKernel.["id"].ToArray<int>() |> List.ofArray))

[<Fact>]
let ``Kernel cleans up the expired log after a checkpoint, and the table stays readable`` () =
    withTable "kernel_cleanup" (fun path ->
        // The log retention may not be shorter than the deleted file retention (Spark): both go to zero.
        let properties =
            readOnlyDict [ "delta.checkpointInterval", "3"; "delta.logRetentionDuration", "interval 0 seconds"; "delta.deletedFileRetentionDuration", "interval 0 seconds" ]
        let write (df: DataFrame) mode =
            Polars.NET.Core.Delta.DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), mode, null, null, properties) |> ignore
        use first = frame [ 1 ] "east"
        write first Polars.NET.Core.Delta.KernelWriteMode.ErrorIfExists
        for i in 2..6 do
            use more = frame [ i ] "east"
            write more Polars.NET.Core.Delta.KernelWriteMode.Append
        // The cleanup's cutoff is rounded down to midnight UTC (Spark), so versions 0-5 are made two days old.
        for file in Directory.GetFiles(Path.Combine(path, "_delta_log")) do
            File.SetLastWriteTimeUtc(file, DateTime.UtcNow.AddDays -2.0)
        for i in 7..8 do
            use more = frame [ i ] "east"
            write more Polars.NET.Core.Delta.KernelWriteMode.Append

        // Version 6 is the newest checkpoint; with no retention everything before it went.
        let log = Directory.GetFiles(Path.Combine(path, "_delta_log")) |> Array.map Path.GetFileName |> Array.sort
        Assert.Equal<string[]>(
            [| "00000000000000000006.checkpoint.parquet"; "00000000000000000006.json"; "00000000000000000007.json"; "_last_checkpoint" |],
            log)

        use latest = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        Assert.Equal<int list>([ 1 .. 8 ], latest.["id"].ToArray<int>() |> List.ofArray)
        use atSix = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.Scan(path, Nullable 6L))).Collect()
        Assert.Equal(7L, atSix.Height)
        // A version whose log is gone cannot be read any more.
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () ->
            Polars.NET.Core.Delta.DeltaKernelScan.Scan(path, Nullable 2L) |> ignore) |> ignore
        // The current engine reads the cleaned table too.
        use viaEngine = LazyFrame.ScanDelta(path).Collect()
        Assert.Equal(8L, viaEngine.Height))
