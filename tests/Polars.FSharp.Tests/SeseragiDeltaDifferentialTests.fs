/// Differential tests of the managed Delta kernel: tables are written by the current engine, then every version
/// is read both by the engine and by the kernel, and the two must agree.
module Polars.FSharp.Tests.SeseragiDeltaDifferentialTests

open System
open System.IO
open System.Text.Json
open Xunit
open Polars.FSharp
open Kaminari.Seseragi.Delta
open Kaminari.Seseragi.Delta.Engine

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
    Path.Combine(up (DirectoryInfo AppContext.BaseDirectory), "tests", "Seseragi.Delta.Tests", "TestData", name)

/// Live rows counted from the data files themselves, for tables whose log carries no numRecords.
let private liveRowsFromFiles (path: string) (fs: IFileSystem) (snapshot: Snapshot) =
    snapshot.Files
    |> Seq.sumBy (fun f ->
        use df = LazyFrame.ScanParquet(Path.Combine(path, f.DecodedPath)).Select([ pl.len () ]).Collect()
        let rows = Convert.ToInt64(df.Row(0).[0])
        let deleted =
            if isNull f.DeletionVector then 0L
            else Kaminari.Seseragi.Delta.Actions.DeletionVector.Read(fs, f.DeletionVector).Cardinality
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
