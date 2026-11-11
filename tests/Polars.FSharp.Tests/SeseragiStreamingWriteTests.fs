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

[<Fact>]
let ``Statistics follow the table's data skipping properties`` () =
    withTable "stats_props" (fun path ->
        let statsOf (p: string) = JsonDocument.Parse((snapshot p).Files |> Seq.head |> fun f -> f.Stats).RootElement
        let names (e: JsonElement) (section: string) =
            let mutable v = Unchecked.defaultof<JsonElement>
            if e.TryGetProperty(section, &v) then v.EnumerateObject() |> Seq.map (fun p -> p.Name) |> List.ofSeq else []
        use df = frame [ 1; 2; 3 ]
        let one = Path.Combine(path, "one")
        write one df KernelWriteMode.ErrorIfExists [||] KernelWriteOptions.Default [ (FileStatistics.NumIndexedColsKey, "1") ] |> ignore
        Assert.Equal<string list>([ "id" ], names (statsOf one) "minValues")
        Assert.Equal<string list>([ "id" ], names (statsOf one) "nullCount")
        let named = Path.Combine(path, "named")
        write named df KernelWriteMode.ErrorIfExists [||] KernelWriteOptions.Default [ (FileStatistics.StatsColumnsKey, "price") ] |> ignore
        Assert.Equal<string list>([ "price" ], names (statsOf named) "maxValues")
        let none = Path.Combine(path, "none")
        write none df KernelWriteMode.ErrorIfExists [||] KernelWriteOptions.Default [ (FileStatistics.NumIndexedColsKey, "0") ] |> ignore
        Assert.Equal(1, (statsOf none).EnumerateObject() |> Seq.length) // numRecords only
        // A delete rewriting files keeps to the selection too.
        DeltaKernelDelete.Delete(one, (pl.col "id" .== pl.lit 2).CloneHandle()) |> ignore
        Assert.All((snapshot one).Files, fun f -> Assert.DoesNotContain("price", f.Stats)))

[<Fact>]
let ``NOT NULL struct and array columns pass when clean and fail with a null`` () =
    withTable "notnull_nested_top" (fun path ->
        let schema = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"s","type":{"type":"struct","fields":[{"name":"k","type":"integer","nullable":true,"metadata":{}}]},"nullable":false,"metadata":{}},{"name":"arr","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":false,"metadata":{}}]}"""
        Directory.CreateDirectory path |> ignore
        Transaction.Create(LocalFileSystem path, ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2), MetadataAction(Id = "t", SchemaString = schema)).Commit() |> ignore
        use raw = DataFrame.create [| Series.create("id", [ 1; 2 ]); Series.create("k", [ Some 1; None ]); Series.create("isNull", [ false; true ]) |]
        use clean = raw.Select([ pl.col "id"; pl.asStruct [ pl.col "k" ] |> pl.alias "s"; (pl.col "id").Implode().Over(pl.col "id") |> pl.alias "arr" ])
        write path clean KernelWriteMode.Append null KernelWriteOptions.Default [] |> ignore
        Assert.Equal<int list>([ 1; 2 ], ids path)
        use nullStruct = raw.Select([ pl.col "id"; pl.ifElse (pl.col "isNull") (pl.litNull ()) (pl.asStruct [ pl.col "k" ]) |> pl.alias "s"; (pl.col "id").Implode().Over(pl.col "id") |> pl.alias "arr" ])
        Assert.ThrowsAny<Exception>(fun () -> write path nullStruct KernelWriteMode.Append null KernelWriteOptions.Default [] |> ignore) |> ignore
        Assert.Equal<int list>([ 1; 2 ], ids path))

[<Fact>]
let ``String bounds of multi-byte text bound the real values in UTF-8 order`` () =
    withTable "utf8_bounds" (fun path ->
        let values = [ String.replicate 40 "漢"; "a" + String.replicate 30 "😀"; String.replicate 70 "é"; "z" + String(char 0x7FF, 50); "中文" ]
        use df = DataFrame.create [| Series.create("id", [ 1 .. values.Length ]); Series.create("s", values) |]
        write path df KernelWriteMode.ErrorIfExists [||] KernelWriteOptions.Default [] |> ignore
        let stats = JsonDocument.Parse((snapshot path).Files |> Seq.exactlyOne |> fun f -> f.Stats).RootElement
        let utf8 (s: string) = Text.Encoding.UTF8.GetBytes s
        let compare (a: string) (b: string) = (ReadOnlySpan(utf8 a)).SequenceCompareTo(ReadOnlySpan(utf8 b))
        let realMin = values |> List.minBy utf8 |> fun _ -> values |> List.reduce (fun a b -> if compare a b <= 0 then a else b)
        let realMax = values |> List.reduce (fun a b -> if compare a b >= 0 then a else b)
        let mutable v = Unchecked.defaultof<JsonElement>
        if stats.GetProperty("minValues").TryGetProperty("s", &v) then
            Assert.True(compare (v.GetString()) realMin <= 0, $"min {v.GetString()} above {realMin}")
        if stats.GetProperty("maxValues").TryGetProperty("s", &v) then
            Assert.True(compare (v.GetString()) realMax >= 0, $"max {v.GetString()} below {realMax}")
        // The table reads back whole.
        use back = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        Assert.Equal<string list>(values, back.["s"].ToArray<string>() |> List.ofArray))

[<Fact>]
let ``Thousands of partitions stream in one write`` () =
    withTable "many_partitions" (fun path ->
        let n = 3000
        use df = DataFrame.create [| Series.create("id", [ 1 .. n ]); Series.create("key", [ for i in 1 .. n -> $"k{i % 2000}" ]) |]
        let sw = Diagnostics.Stopwatch.StartNew()
        write path df KernelWriteMode.ErrorIfExists [| "key" |] KernelWriteOptions.Default [] |> ignore
        sw.Stop()
        let table = snapshot path
        Assert.Equal(2000, table.Files.Count)
        Assert.Equal(int64 n, table.Files |> Seq.sumBy numRecords)
        printfn "2000 partitions written in %d ms" sw.ElapsedMilliseconds)

[<Fact>]
let ``The target file size property takes byte counts and sizes, and refuses anything else`` () =
    let size (v: string) =
        let d = Dictionary<string, string>()
        d["delta.targetFileSize"] <- v
        DeltaKernelWrite.TargetFileSize d
    Assert.Equal(134217728UL, size "134217728")
    Assert.Equal(64UL * 1024UL * 1024UL, size "64mb")
    Assert.Equal(64UL * 1024UL * 1024UL, size " 64 MB ")
    Assert.Equal(1UL <<< 30, size "1g")
    Assert.Equal(128UL * 1024UL * 1024UL, DeltaKernelWrite.TargetFileSize null)
    for bad in [ "abc"; "-5"; "0"; "12 parsecs"; "" ] do
        Assert.ThrowsAny<Exception>(fun () -> size bad |> ignore) |> ignore

[<Fact>]
let ``Deletes and updates stream their new files, split by the table's target file size`` () =
    withTable "dml_split" (fun path ->
        use big = frame [ 1 .. 1_000_000 ]
        // One file (no size limit for this write) in a table whose target file size is small.
        write path big KernelWriteMode.ErrorIfExists [||] (KernelWriteOptions(ApproxBytesPerFile = Nullable 0UL)) [ ("delta.targetFileSize", "2000000") ] |> ignore
        Assert.Equal(1, (snapshot path).Files.Count)

        // The rewritten file's kept rows go to several files.
        DeltaKernelDelete.Delete(path, (pl.col "id" .== pl.lit 7).CloneHandle()) |> ignore
        let afterDelete = (snapshot path).Files
        Assert.True(afterDelete.Count > 1, $"{afterDelete.Count} files")
        Assert.Equal(999_999L, afterDelete |> Seq.sumBy numRecords)
        Assert.All(afterDelete, fun f -> Assert.Matches(@"^part-\d{5}-[0-9a-f-]{36}-c000\.snappy\.parquet$", f.Path))

        // So do the updated rows.
        DeltaKernelUpdate.Update(path, null, ResizeArray [ struct ("price", (pl.lit 0.0).CloneHandle()) ]) |> ignore
        let afterUpdate = (snapshot path).Files
        Assert.True(afterUpdate.Count > 1, $"{afterUpdate.Count} files")
        Assert.Equal(999_999L, afterUpdate |> Seq.sumBy numRecords)
        use back = (new LazyFrame(DeltaKernelScan.Scan path)).Select([ (pl.col "price").Max() |> pl.alias "m"; pl.len () |> pl.alias "n" ]).Collect()
        Assert.Equal(0.0, back.["m"].ToArray<float>().[0])
        Assert.Equal(999_999L, back.["n"].Cast(DataType.Int64).ToArray<int64>().[0]))

[<Fact>]
let ``Change data files are streamed under _change_data, per partition`` () =
    withTable "cdc_files" (fun path ->
        use df = frame [ 1 .. 10 ]
        write path df KernelWriteMode.ErrorIfExists [| "region" |] KernelWriteOptions.Default [ ("delta.enableChangeDataFeed", "true") ] |> ignore
        DeltaKernelDelete.Delete(path, (pl.col "id" .<= pl.lit 4).CloneHandle()) |> ignore
        let cdc = Directory.GetFiles(Path.Combine(path, "_change_data"), "*.parquet", SearchOption.AllDirectories)
        Assert.Equal(2, cdc.Length)
        Assert.All(cdc, fun f -> Assert.Matches(@"region=(east|west)[/\\]cdc-\d{5}-[0-9a-f-]{36}\.c000\.snappy\.parquet$", f))
        Assert.Equal<int list>([ 5 .. 10 ], ids path))
