/// Reading options of the kernel scan (what the public ScanDelta takes).
module Polars.FSharp.Tests.SeseragiScanOptionsTests

open System
open System.IO
open Xunit
open Polars.FSharp
open Polars.NET.Core.Delta
open Polars.NET.Core.Wrappers
open Kaminari.Seseragi.Lakehouse.Delta
open Kaminari.Seseragi.Lakehouse.Engine

let private withTable (name: string) (body: string -> unit) =
    let path = Path.Combine(Path.GetTempPath(), $"seseragi_scanopts_{name}_{Guid.NewGuid():N}")
    try
        body path
    finally
        if Directory.Exists path then Directory.Delete(path, true)

/// 10 rows in two partitions, then a delete with deletion vectors: one file has a vector.
let private table (path: string) =
    let config = Collections.Generic.Dictionary<string, string>()
    config[TableFeatures.EnableDeletionVectorsKey] <- "true"
    use df =
        DataFrame.create [|
            Series.create("id", [ 1 .. 10 ])
            Series.create("region", [ for i in 1 .. 10 -> if i % 2 = 0 then "east" else "west" ])
        |]
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists, [| "region" |], configuration = config) |> ignore
    DeltaKernelDelete.Delete(path, (pl.col "id" .== pl.lit 4).CloneHandle()) |> ignore
    Assert.Contains((Snapshot.Load(LocalFileSystem path)).Files, fun f -> not (isNull f.DeletionVector))

let private scan (path: string) (options: KernelScanOptions) =
    (new LazyFrame(DeltaKernelScan.Scan(path, options = options))).Collect()

[<Fact>]
let ``Row limit, row index and path column`` () =
    withTable "basic" (fun path ->
        table path
        use limited = scan path (KernelScanOptions(NRows = Nullable 3UL))
        Assert.Equal(3L, limited.Height)

        use indexed = scan path (KernelScanOptions(RowIndexName = "idx", RowIndexOffset = 10u))
        Assert.Equal(9L, indexed.Height)
        Assert.Equal("idx", indexed.Columns.[0])
        Assert.Equal<int64 list>([ 10L .. 18L ], indexed.["idx"].ToArray<uint32>() |> Array.map int64 |> Array.sort |> List.ofArray)

        use withPath = scan path (KernelScanOptions(IncludePathColumn = "file"))
        Assert.Equal(9L, withPath.Height)
        let files = withPath.["file"].ToArray<string>()
        Assert.All(files, fun f -> Assert.True(File.Exists f, f))
        // Each row's file is the one of its partition.
        let regions = withPath.["region"].ToArray<string>()
        Assert.All(Array.zip files regions, fun (f, r) -> Assert.Contains($"region={r}", f)))

[<Fact>]
let ``A schema selects and casts the table's columns, and refuses unknown ones`` () =
    withTable "schema" (fun path ->
        table path
        let wanted = PolarsSchema([ "region", DataType.String; "id", DataType.Int64 ])
        use df = scan path (KernelScanOptions(Schema = wanted.Handle))
        Assert.Equal<string list>([ "region"; "id" ], df.Columns |> List.ofSeq)
        Assert.Equal(DataType.Int64, df.["id"].DataType)
        Assert.Equal(9L, df.Height)
        let unknown = PolarsSchema([ "nope", DataType.Int64 ])
        Assert.ThrowsAny<Exception>(fun () -> scan path (KernelScanOptions(Schema = unknown.Handle)) |> ignore) |> ignore)

[<Fact>]
let ``An empty table keeps the path column, and engine options pass through`` () =
    withTable "empty" (fun path ->
        Directory.CreateDirectory path |> ignore
        use empty = DataFrame.create [| Series.create("id", ([]: int list)) |]
        DeltaKernelWrite.Write(path, empty.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists) |> ignore
        use df = scan path (KernelScanOptions(IncludePathColumn = "file", RowIndexName = "i"))
        Assert.Equal<string list>([ "i"; "id"; "file" ], df.Columns |> List.ofSeq)
        Assert.Equal(0L, df.Height)

        let other = Path.Combine(path, "t")
        table other
        use tuned = scan other (KernelScanOptions(Parallel = Polars.NET.Core.Enums.PlParallelStrategy.None, LowMemory = true, UseStatistics = false, Rechunk = true, Cache = false))
        Assert.Equal(9L, tuned.Height))

[<Fact>]
let ``Time travel times parse as ISO-8601, UTC without an offset`` () =
    Assert.Equal(DateTimeOffset(2026, 2, 9, 12, 0, 0, TimeSpan.Zero), DeltaKernelScan.ParseAsOf "2026-02-09T12:00:00Z")
    Assert.Equal(DateTimeOffset(2026, 2, 9, 12, 0, 0, TimeSpan.Zero), DeltaKernelScan.ParseAsOf "2026-02-09T12:00:00")
    Assert.Equal(DateTimeOffset(2026, 2, 9, 3, 0, 0, TimeSpan.Zero), (DeltaKernelScan.ParseAsOf "2026-02-09T12:00:00+09:00").ToUniversalTime())
    Assert.ThrowsAny<ArgumentException>(fun () -> DeltaKernelScan.ParseAsOf "yesterday" |> ignore) |> ignore
