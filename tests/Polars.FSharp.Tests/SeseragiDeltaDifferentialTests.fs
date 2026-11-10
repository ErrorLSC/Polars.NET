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

/// A Parquet reader for the kernel built on the engine: scan the file, write its rows as JSON lines.
type private EngineParquetReader(tablePath: string) =
    member val Reads = ResizeArray<string>() with get

    interface IParquetReader with
        member this.ReadAsJsonLines(path: string) =
            this.Reads.Add path
            let temp = Path.Combine(Path.GetTempPath(), $"seseragi_rows_{Guid.NewGuid():N}.ndjson")
            try
                LazyFrame.ScanParquet(Path.Combine(tablePath, path)).SinkNdJson temp
                File.ReadAllBytes temp
            finally
                if File.Exists temp then File.Delete temp

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

/// Tables written by Spark or Databricks, read through their checkpoints with the engine as Parquet reader.
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
    let fs = LocalFileSystem path
    let reader = EngineParquetReader path
    let snapshot = Snapshot.Load(fs, parquet = reader)

    Assert.Equal(hasCheckpoint, reader.Reads.Count > 0)

    // The same state must come out of the JSON commits alone.
    let fromCommits = Snapshot.Load fs
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
    let reader = EngineParquetReader tablePath
    let snapshot =
        if expectedName = "latest" then Snapshot.Load(fs, parquet = reader)
        else Snapshot.Load(fs, Nullable expectedVersion, reader)

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
    let snapshot = Snapshot.Load(LocalFileSystem tablePath, version, EngineParquetReader tablePath)
    let sortColumns =
        snapshot.Schema.Fields
        |> Seq.filter (fun f -> (f.Type :? Schema.PrimitiveType && f.Type.ToString() <> "binary") || f.Type :? Schema.DecimalType)
        |> Seq.map (fun f -> pl.col f.Name)
        |> List.ofSeq
    let columns = actual.ColumnNames |> Seq.map pl.col |> List.ofSeq
    use expected = expectedRaw.Select(columns).Sort(sortColumns)
    use sorted = actual.Sort(sortColumns)
    Assert.True(sorted.Equals(expected), $"content differs\nactual:\n{sorted}\nexpected:\n{expected}")

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

    let from = "00000000000000000003"
    Assert.Equal<string list>(
        local.List("_delta_log", from) |> Seq.map (fun f -> f.Path) |> List.ofSeq,
        fs.List("_delta_log", from) |> Seq.map (fun f -> f.Path) |> List.ofSeq)

    let first = (local.List "_delta_log").[0].Path
    let whole = local.ReadAll first
    Assert.Equal<byte[]>(whole, fs.ReadAll first)
    Assert.Equal<byte[]>(local.ReadRange(first, 5L, 20), fs.ReadRange(first, 5L, 20))
    Assert.Equal<byte[]>(local.ReadRange(first, int64 whole.Length - 3L, 50), fs.ReadRange(first, int64 whole.Length - 3L, 50))
    Assert.Empty(fs.ReadRange(first, int64 whole.Length + 10L, 5))
    Assert.Null(fs.ReadAll "_delta_log/missing.json")
    Assert.Null(fs.ReadRange("missing/missing.bin", 0L, 4))

    // The same snapshot comes out, and deletion vectors read through range requests.
    let reader = EngineParquetReader path
    let viaLocal = Snapshot.Load(local, parquet = reader)
    let viaStore = Snapshot.Load(fs, parquet = reader)
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
let ``Change feed needs the feature unless waived, and then matches the engine`` () =
    withTable "cdf_own" (fun path ->
        (frame [1; 2; 3] "east").WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)
        (frame [4] "west").WriteDelta(path, mode = DeltaSaveMode.Append)
        Delta.Delete(path, pl.col "id" .< pl.lit 3)

        let ex = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () ->
            Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, 0L) |> ignore)
        Assert.Contains("not enabled", ex.Message)

        let latest = (Snapshot.Load(LocalFileSystem path)).Version
        use viaKernel = (new LazyFrame(Polars.NET.Core.Delta.DeltaKernelScan.ReadChangeFeed(path, 0L, requireEnabled = false))).Collect()
        use viaEngine = Delta.ReadChangeDataFeed(path, 0L, latest).Collect()

        let columns = [ "id"; "region"; "price"; "_change_type"; "_commit_version" ]
        Assert.Equal<string list>(rowKeys viaEngine columns |> List.sort, rowKeys viaKernel columns |> List.sort))

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
