/// Operations on Delta tables through the managed kernel (Phase 3): delete, and the write-side checks they share.
module Polars.FSharp.Tests.SeseragiDeltaOperationTests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open Xunit
open Polars.FSharp
open Polars.NET.Core.Delta
open Kaminari.Seseragi.Lakehouse.Delta
open Kaminari.Seseragi.Lakehouse.Delta.Actions
open Kaminari.Seseragi.Lakehouse.Engine

let private withTable (name: string) (body: string -> unit) =
    let path = Path.Combine(Path.GetTempPath(), $"seseragi_ops_{name}_{Guid.NewGuid():N}")
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

let private config (pairs: (string * string) list) =
    let d = Dictionary<string, string>()
    for k, v in pairs do d[k] <- v
    d :> IReadOnlyDictionary<string, string>

let private create (path: string) (df: DataFrame) (partitionBy: string[]) (properties: (string * string) list) =
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists, partitionBy, configuration = config properties)
    |> ignore

let private append (path: string) (df: DataFrame) =
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), KernelWriteMode.Append) |> ignore

let private delete (path: string) (predicate: Expr) =
    DeltaKernelDelete.Delete(path, predicate.CloneHandle())

let private ids (path: string) =
    use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
    df.["id"].ToArray<int>() |> List.ofArray

let private commitInfo (path: string) (version: int64) =
    let line =
        File.ReadLines(Path.Combine(path, "_delta_log", $"{version:D20}.json"))
        |> Seq.find (fun l -> l.StartsWith("{\"commitInfo\"", StringComparison.Ordinal))
    JsonDocument.Parse(line).RootElement.GetProperty("commitInfo").Clone()

let private metric (info: JsonElement) (name: string) =
    Int64.Parse(info.GetProperty("operationMetrics").GetProperty(name).GetString())

let private dvOn = [ TableFeatures.EnableDeletionVectorsKey, "true" ]
let private cdfOn = [ ChangeFeed.EnableKey, "true" ]

[<Fact>]
let ``Delete rewrites files on a table without deletion vectors`` () =
    withTable "delete_rewrite" (fun path ->
        use first = frame [ 1 .. 5 ] "east"
        use second = frame [ 6 .. 10 ] "west"
        create path first [||] []
        append path second

        let result = delete path (pl.col "id" .< pl.lit 3)

        Assert.Equal(Nullable 2L, result.Version)
        Assert.Equal(2L, result.DeletedRows)
        Assert.Equal(1, result.RewrittenFiles)
        Assert.Equal(0, result.DeletionVectorsAdded)
        Assert.Equal<int list>([ 3 .. 10 ], ids path)
        let info = commitInfo path 2L
        Assert.Equal("DELETE", info.GetProperty("operation").GetString())
        Assert.Equal(2L, metric info "numDeletedRows")
        Assert.Equal(3L, metric info "numCopiedRows")
        Assert.False(info.GetProperty("isBlindAppend").GetBoolean())
        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.Equal(2, snapshot.Files.Count)
        Assert.All(snapshot.Files, fun f -> Assert.Null f.DeletionVector)

        // The current engine reads the result too.
        use viaEngine = LazyFrame.ScanDelta(path).Collect()
        Assert.Equal(8L, viaEngine.Height))

[<Fact>]
let ``Delete removes whole files without rewriting, and a miss commits nothing`` () =
    withTable "delete_whole" (fun path ->
        use first = frame [ 1 .. 5 ] "east"
        use second = frame [ 6 .. 10 ] "west"
        create path first [||] []
        append path second

        let miss = delete path (pl.col "id" .> pl.lit 100)
        Assert.False(miss.Version.HasValue)
        Assert.False(File.Exists(Path.Combine(path, "_delta_log", $"{2:D20}.json")))

        let result = delete path (pl.col "region" .== pl.lit "east")
        Assert.Equal(Nullable 2L, result.Version)
        Assert.Equal((5L, 1, 0, 0), (result.DeletedRows, result.RemovedFiles, result.AddedFiles, result.RewrittenFiles))
        Assert.Equal<int list>([ 6 .. 10 ], ids path)

        // No predicate: every row, by removing every file.
        let all = DeltaKernelDelete.Delete(path, null)
        Assert.Equal(5L, all.DeletedRows)
        Assert.Empty(Snapshot.Load(LocalFileSystem path).Files)
        Assert.Empty(ids path))

[<Fact>]
let ``Delete marks rows in deletion vectors, merging with earlier ones`` () =
    withTable "delete_dv" (fun path ->
        use df = frame [ 1 .. 10 ] "east"
        create path df [||] dvOn
        let fs = LocalFileSystem path
        let created = Snapshot.Load fs
        Assert.Contains(TableFeatures.DeletionVectors, created.Protocol.ReaderFeatures)
        Assert.Contains(TableFeatures.DeletionVectors, created.Protocol.WriterFeatures)
        let original = Assert.Single created.Files

        let r1 = delete path (pl.col "id" .< pl.lit 4)
        Assert.Equal((3L, 1, 0), (r1.DeletedRows, r1.DeletionVectorsAdded, r1.RewrittenFiles))
        let r2 = delete path ((pl.col "id" .> pl.lit 8) .|| (pl.col "id" .== pl.lit 2)) // 2 is already gone
        Assert.Equal(2L, r2.DeletedRows)
        Assert.Equal<int list>([ 4 .. 8 ], ids path)

        let file = Assert.Single(Snapshot.Load(fs).Files)
        Assert.Equal(original.Path, file.Path)
        Assert.Equal(5L, file.DeletionVector.Cardinality)
        Assert.Equal<int64[]>([| 0L; 1L; 2L; 8L; 9L |], DeletionVector.Read(fs, file.DeletionVector).DeletedRows)
        Assert.False(file.ParsedStats.TightBounds)
        Assert.Equal(Nullable 10L, file.ParsedStats.NumRecords)
        // The second delete removed the file with the first vector and added it back with the merged one.
        let info = commitInfo path 2L
        Assert.Equal(1L, metric info "numDeletionVectorsRemoved")
        Assert.Single(Directory.GetFiles(path, "deletion_vector_*.bin") |> Array.filter (fun f -> f.Contains(file.DeletionVector.FilePath()))) |> ignore

        // Deleting the remaining rows removes the file.
        let r3 = delete path (pl.col "id" .< pl.lit 100)
        Assert.Equal((5L, 1, 0), (r3.DeletedRows, r3.RemovedFiles, r3.DeletionVectorsAdded))
        Assert.Empty(Snapshot.Load(fs).Files))

[<Fact>]
let ``Delete keeps rows where the predicate is null`` () =
    withTable "delete_null" (fun path ->
        use df =
            DataFrame.create [|
                Series.create("id", [ 1; 2; 3 ])
                Series.create("v", [ Nullable 1; Nullable(); Nullable 3 ])
            |]
        create path df [||] []
        let result = delete path (pl.col "v" .> pl.lit 1)
        Assert.Equal(1L, result.DeletedRows)
        Assert.Equal<int list>([ 1; 2 ], ids path))

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``Delete writes change data on a partitioned change feed table`` (withVectors: bool) =
    withTable "delete_cdf" (fun path ->
        use east = frame [ 1 .. 4 ] "east"
        use west = frame [ 5 .. 8 ] "west"
        use both = east.VStack west
        create path both [| "region" |] (cdfOn @ (if withVectors then dvOn else []))

        let result = delete path ((pl.col "id" .== pl.lit 2) .|| (pl.col "id" .>= pl.lit 7))
        Assert.Equal(3L, result.DeletedRows)
        Assert.Equal(2, result.ChangeFiles)
        Assert.Equal((if withVectors then 2 else 0), result.DeletionVectorsAdded)
        Assert.Equal<int list>([ 1; 3; 4; 5; 6 ], ids path)

        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.All(Directory.GetFiles(Path.Combine(path, "_change_data"), "*.parquet", SearchOption.AllDirectories), fun f ->
            Assert.Contains("region=", f))

        use changes = (new LazyFrame(DeltaKernelScan.ReadChangeFeed(path, 1L))).Collect().Sort(pl.col "id")
        Assert.Equal(3L, changes.Height)
        Assert.Equal<int list>([ 2; 7; 8 ], changes.["id"].ToArray<int>() |> List.ofArray)
        Assert.Equal<string list>([ "east"; "west"; "west" ], changes.["region"].ToArray<string>() |> List.ofArray)
        Assert.All(changes.["_change_type"].ToArray<string>(), fun t -> Assert.Equal("delete", t))
        Assert.All(changes.["_commit_version"].ToArray<int64>(), fun v -> Assert.Equal(1L, v))
        ignore snapshot)

[<Fact>]
let ``Delete on an append-only table is refused`` () =
    withTable "delete_append_only" (fun path ->
        use df = frame [ 1; 2 ] "east"
        create path df [||] [ TableFeatures.AppendOnlyKey, "true" ]
        let e = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> delete path (pl.col "id" .== pl.lit 1) |> ignore)
        Assert.Contains("append-only", e.Message)
        Assert.Equal<int list>([ 1; 2 ], ids path))

[<Fact>]
let ``Writes reject nulls in NOT NULL columns, nested fields included`` () =
    withTable "not_null" (fun path ->
        // A table whose schema declares id NOT NULL and s.k NOT NULL inside a nullable struct.
        let schema =
            """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},"""
            + """{"name":"s","type":{"type":"struct","fields":[{"name":"k","type":"string","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}}]}"""
        let fs = LocalFileSystem path
        let metadata = MetadataAction(Id = Guid.NewGuid().ToString(), SchemaString = schema)
        Transaction.Create(fs, ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2), metadata).Commit() |> ignore

        let rows (ids: Nullable<int> list) (keys: string list) (structNull: bool list) =
            use df =
                DataFrame.create [|
                    Series.create("id", ids)
                    Series.create("k", keys)
                    Series.create("isNull", structNull)
                |]
            df.Select(
                [ pl.col "id"
                  pl.ifElse (pl.col "isNull") (pl.litNull ()) (pl.asStruct [ pl.col "k" ]) |> pl.alias "s" ])

        // A null struct may hold anything; a present struct needs k.
        use ok = rows [ Nullable 1; Nullable 2 ] [ "a"; null ] [ false; true ]
        append path ok
        use nullId = rows [ Nullable(); Nullable 3 ] [ "a"; "b" ] [ false; false ]
        let e1 = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> append path nullId)
        Assert.Contains("NOT NULL: id", e1.Message)
        use nullKey = rows [ Nullable 4 ] [ null ] [ false ]
        let e2 = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> append path nullKey)
        Assert.Contains("s.k", e2.Message)
        Assert.Equal(1L, Snapshot.Load(fs).Version))
