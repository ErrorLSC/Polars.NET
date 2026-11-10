/// Kernel writes through the engine's streaming partitioned sink: file splitting, empty frames, failures,
/// column mapping, codecs and the Ignore mode.
module Polars.FSharp.Tests.SeseragiStreamingWriteTests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open Xunit
open Polars.FSharp
open Polars.NET.Core.Delta
open Polars.NET.Core.Enums
open Kaminari.Seseragi.Lakehouse.Delta
open Kaminari.Seseragi.Lakehouse.Delta.Actions
open Kaminari.Seseragi.Lakehouse.Engine
open Kaminari.Seseragi.Lakehouse.Parquet

let private withTable (name: string) (body: string -> unit) =
    let path = Path.Combine(Path.GetTempPath(), $"seseragi_stream_{name}_{Guid.NewGuid():N}")
    try
        body path
    finally
        if Directory.Exists path then Directory.Delete(path, true)

let private frame (ids: int list) =
    DataFrame.create [|
        Series.create("id", ids)
        Series.create("region", ids |> List.map (fun i -> if i % 2 = 0 then "east" else "west"))
        Series.create("price", ids |> List.map (fun i -> float i * 1.5))
    |]

let private write (path: string) (df: DataFrame) (mode: KernelWriteMode) (partitionBy: string[]) (options: KernelWriteOptions) (properties: (string * string) list) =
    let config = Dictionary<string, string>()
    for k, v in properties do config[k] <- v
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), mode, partitionBy, configuration = config, options = options)

let private snapshot (path: string) = Snapshot.Load(LocalFileSystem path)

let private ids (path: string) =
    use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
    df.["id"].ToArray<int>() |> List.ofArray

let private numRecords (add: AddAction) =
    JsonDocument.Parse(add.Stats).RootElement.GetProperty("numRecords").GetInt64()

let private dataFiles (path: string) =
    Directory.GetFiles(path, "*.parquet", SearchOption.AllDirectories)
    |> Array.filter (fun f -> not (f.Contains "_delta_log"))

[<Fact>]
let ``Rows are split into files by row count and size, per partition`` () =
    withTable "split" (fun path ->
        use df = frame [ 1 .. 20 ]
        write path df KernelWriteMode.ErrorIfExists [| "region" |] (KernelWriteOptions(MaxRowsPerFile = 3UL)) [] |> ignore
        let files = (snapshot path).Files
        // 10 rows per partition, at most 3 per file: 4 files each.
        Assert.Equal(8, files.Count)
        Assert.All(files, fun f -> Assert.True(numRecords f <= 3L))
        Assert.Equal(20L, files |> Seq.sumBy numRecords)
        for region in [ "east"; "west" ] do
            Assert.Equal(4, files |> Seq.filter (fun f -> f.PartitionValues["region"] = region) |> Seq.length)
        Assert.Equal<int list>([ 1 .. 20 ], ids path)

        // A small in-memory budget splits too (a file takes at least one sink morsel of rows, so enough rows).
        let small = Path.Combine(path, "small")
        use big = frame [ 1 .. 1_000_000 ]
        write small big KernelWriteMode.ErrorIfExists [||] (KernelWriteOptions(ApproxBytesPerFile = Nullable 2_000_000UL)) [] |> ignore
        Assert.True((snapshot small).Files.Count > 1)
        Assert.Equal(1_000_000L, (snapshot small).Files |> Seq.sumBy numRecords))

[<Fact>]
let ``The table's target file size is the default size`` () =
    withTable "target" (fun path ->
        use big = frame [ 1 .. 1_000_000 ]
        write path big KernelWriteMode.ErrorIfExists [||] KernelWriteOptions.Default [ ("delta.targetFileSize", "2000000") ] |> ignore
        Assert.True((snapshot path).Files.Count > 1)
        // Without the property one file holds all of it.
        let plain = Path.Combine(path, "plain")
        write plain big KernelWriteMode.ErrorIfExists [||] KernelWriteOptions.Default [] |> ignore
        Assert.Equal(1, (snapshot plain).Files.Count))

[<Fact>]
let ``Empty frames create, append and overwrite without data files`` () =
    withTable "empty" (fun path ->
        use empty = (frame [ 1 ]).Filter(pl.col "id" .> pl.lit 100)
        Assert.Equal(0L, write path empty KernelWriteMode.ErrorIfExists [| "region" |] KernelWriteOptions.Default [])
        Assert.Empty((snapshot path).Files)
        Assert.Equal("region", (snapshot path).PartitionColumns |> Seq.exactlyOne)

        use some = frame [ 1; 2 ]
        write path some KernelWriteMode.Append null KernelWriteOptions.Default [] |> ignore
        Assert.Equal(2L, write path empty KernelWriteMode.Append null KernelWriteOptions.Default [])
        Assert.Equal<int list>([ 1; 2 ], ids path)

        write path empty KernelWriteMode.Overwrite null KernelWriteOptions.Default [] |> ignore
        Assert.Empty((snapshot path).Files)
        Assert.Empty(ids path))

[<Fact>]
let ``A write that fails after its files are written leaves no files`` () =
    withTable "fail" (fun path ->
        // A table with a NOT NULL column, created by the kernel's transaction directly.
        let schema = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"region","type":"string","nullable":true,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}}]}"""
        Directory.CreateDirectory path |> ignore
        Transaction.Create(LocalFileSystem path, ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2), MetadataAction(Id = "t", SchemaString = schema)).Commit() |> ignore

        use bad =
            DataFrame.create [|
                Series.create("id", [ Some 1; None; Some 3 ])
                Series.create("region", [ "a"; "b"; "c" ])
                Series.create("price", [ 1.0; 2.0; 3.0 ])
            |]
        let error = Assert.ThrowsAny<Exception>(fun () -> write path bad KernelWriteMode.Append null (KernelWriteOptions(MaxRowsPerFile = 1UL)) [] |> ignore)
        Assert.Contains("NOT NULL", error.Message)
        Assert.Empty(dataFiles path)
        Assert.Equal(0L, (snapshot path).Version)

        // A NOT NULL partition column with a null value fails before any file.
        let partitioned = Path.Combine(path, "p")
        let schemaP = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"region","type":"string","nullable":false,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}}]}"""
        Directory.CreateDirectory partitioned |> ignore
        Transaction.Create(LocalFileSystem partitioned, ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2), MetadataAction(Id = "p", SchemaString = schemaP, PartitionColumns = [| "region" |])).Commit() |> ignore
        use nullRegion =
            DataFrame.create [|
                Series.create("id", [ 1; 2 ])
                Series.create("region", [ Some "a"; None ])
                Series.create("price", [ 1.0; 2.0 ])
            |]
        Assert.ThrowsAny<Exception>(fun () -> write partitioned nullRegion KernelWriteMode.Append null KernelWriteOptions.Default [] |> ignore) |> ignore
        Assert.Empty(dataFiles partitioned))

[<Fact>]
let ``Column-mapped tables get files with physical column names`` () =
    withTable "mapped" (fun path ->
        use df = frame [ 1 .. 6 ]
        write path df KernelWriteMode.ErrorIfExists [| "region" |] KernelWriteOptions.Default [ ("delta.columnMapping.mode", "name") ] |> ignore
        use more = frame [ 7; 8 ]
        write path more KernelWriteMode.Append null KernelWriteOptions.Default [] |> ignore
        let table = snapshot path
        Assert.Equal<int list>([ 1 .. 8 ], ids path)
        let file = table.Files |> Seq.head
        let footer = ParquetMetadata.FromFile(ReadOnlySpan(File.ReadAllBytes(Path.Combine(path, Uri.UnescapeDataString file.Path))))
        let names = footer.Schema |> Seq.skip 1 |> Seq.map (fun e -> e.Name) |> List.ofSeq
        Assert.All(names, fun n -> Assert.StartsWith("col-", n))
        Assert.Equal(2, names.Length) // the partition column is not in the file
        Assert.True(file.PartitionValues.Keys |> Seq.forall (fun k -> k.StartsWith("col-", StringComparison.Ordinal)))
        // Statistics under physical names.
        Assert.Contains(table.Schema.Field("id").PhysicalName, file.Stats))

[<Fact>]
let ``File names follow the codec, and the Ignore mode writes nothing to an existing table`` () =
    withTable "codec" (fun path ->
        use df = frame [ 1; 2; 3 ]
        write path df KernelWriteMode.ErrorIfExists [||] (KernelWriteOptions(Compression = PlParquetCompression.ZSTD)) [] |> ignore
        Assert.All((snapshot path).Files, fun f -> Assert.EndsWith(".zstd.parquet", f.Path))

        use other = frame [ 9 ]
        Assert.Equal(0L, write path other KernelWriteMode.Ignore null KernelWriteOptions.Default [])
        Assert.Equal<int list>([ 1; 2; 3 ], ids path)
        // On a new table, Ignore writes.
        let fresh = Path.Combine(path, "fresh")
        Assert.Equal(0L, write fresh other KernelWriteMode.Ignore null KernelWriteOptions.Default [])
        Assert.Equal<int list>([ 9 ], ids fresh))

[<Fact>]
let ``NOT NULL fields inside structs are checked on the rows`` () =
    withTable "nested_notnull" (fun path ->
        let schema = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"s","type":{"type":"struct","fields":[{"name":"k","type":"integer","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}}]}"""
        Directory.CreateDirectory path |> ignore
        Transaction.Create(LocalFileSystem path, ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2), MetadataAction(Id = "n", SchemaString = schema)).Commit() |> ignore
        use raw = DataFrame.create [| Series.create("id", [ 1; 2; 3 ]); Series.create("k", [ Some 1; None; Some 3 ]); Series.create("isNull", [ false; true; false ]) |]
        // A null struct is fine even though its NOT NULL field is then null too.
        use fine = raw.Select([ pl.col "id"; pl.ifElse (pl.col "isNull") (pl.litNull ()) (pl.asStruct [ pl.col "k" ]) |> pl.alias "s" ])
        write path fine KernelWriteMode.Append null KernelWriteOptions.Default [] |> ignore
        Assert.Equal<int list>([ 1; 2; 3 ], ids path)
        use bad = raw.Select([ pl.col "id"; pl.asStruct [ pl.col "k" ] |> pl.alias "s" ])
        Assert.ThrowsAny<Exception>(fun () -> write path bad KernelWriteMode.Append null KernelWriteOptions.Default [] |> ignore) |> ignore)
