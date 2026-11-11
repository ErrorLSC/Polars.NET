/// Delta statistics from Parquet footers (the managed footer reader), checked against the statistics the kernel
/// computes from the rows themselves.
module Polars.FSharp.Tests.SeseragiFooterStatisticsTests

open System
open System.Text.Json
open Xunit
open Polars.FSharp
open Polars.NET.Core.Delta
open Polars.NET.Core.Enums
open Polars.NET.Core.Wrappers
open Kaminari.Seseragi.Lakehouse.Delta
open Kaminari.Seseragi.Lakehouse.Delta.Schema
open Kaminari.Seseragi.Lakehouse.Parquet

/// Two JSON values equal: numbers by value, everything else exactly.
let rec private same (a: JsonElement) (b: JsonElement) =
    match a.ValueKind, b.ValueKind with
    | JsonValueKind.Number, JsonValueKind.Number ->
        (match a.TryGetDecimal(), b.TryGetDecimal() with
         | (true, x), (true, y) -> x = y
         | _ -> a.GetDouble() = b.GetDouble())
    | JsonValueKind.Object, JsonValueKind.Object ->
        let names (e: JsonElement) = e.EnumerateObject() |> Seq.map (fun p -> p.Name) |> Set.ofSeq
        names a = names b && (names a |> Set.forall (fun n -> same (a.GetProperty n) (b.GetProperty n)))
    | JsonValueKind.String, JsonValueKind.String -> a.GetString() = b.GetString()
    | _ -> a.GetRawText() = b.GetRawText()

/// The statistics both ways for the rows of a frame written with the given row group size.
let private bothWays (df: DataFrame) (rowGroupSize: int) =
    let incoming = DeltaKernelWrite.SchemaOf(df.Handle)
    use normalized = DeltaKernelWrite.Normalize(df.Handle, incoming)
    let fields = incoming.Fields |> Array.ofSeq
    let fromRows = DeltaKernelWrite.Statistics(normalized, fields)
    let bytes =
        PolarsWrapper.SinkParquetMemory(
            PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame normalized),
            PlParquetCompression.Snappy, -1, true, rowGroupSize, 0, 0, true)
    let metadata = ParquetMetadata.FromFile(ReadOnlySpan bytes)
    fromRows, FileStatistics.FromParquet(metadata, fields), metadata

let private assertSame (fromRows: string) (fromFooter: string) =
    use a = JsonDocument.Parse fromRows
    use b = JsonDocument.Parse fromFooter
    Assert.True(same a.RootElement b.RootElement, $"from rows:   {fromRows}\nfrom footer: {fromFooter}")

let private sample () =
    let n = 10
    let ids = [ 1 .. n ]
    let df =
        DataFrame.create [|
            Series.create("i32", ids)
            Series.create("i64", ids |> List.map (fun i -> int64 i * -1_000_000_000_000L))
            Series.create("f64", ids |> List.map (fun i -> if i = 3 then -0.0 elif i = 7 then 1e300 else float i / 3.0))
            Series.create("f32", ids |> List.map (fun i -> float32 i * 1.1f))
            Series.create("dec", ids |> List.map (fun i -> decimal i * 1.25m - 3m))
            Series.create("day", ids |> List.map (fun i -> DateOnly(1969, 12, 25).AddDays(i)))
            Series.create("at", ids |> List.map (fun i -> DateTimeOffset(1969, 12, 31, 23, 59, 59, 998, TimeSpan.Zero).AddTicks(int64 i * 4567L)))
            Series.create("naive", ids |> List.map (fun i -> DateTime(2024, 2, 29, 12, 0, 0).AddTicks(int64 i * 15L)))
            Series.create("s", ids |> List.map (fun i -> if i = 2 then None else Some(String('é', i * 4))))
            Series.create("allNull", ids |> List.map (fun _ -> (None: int option)))
            Series.create("flag", ids |> List.map (fun i -> i % 2 = 0))
            Series.create("emoji", ids |> List.map (fun i -> String.replicate i "😀"))
        |]
    let withNested = df.WithColumns([ pl.asStruct [ pl.col "i32"; pl.col "s" ] |> pl.alias "nested" ])
    df.Dispose()
    withNested

[<Fact>]
let ``Footer statistics equal the statistics of the rows, with one or several row groups`` () =
    use df = sample ()
    for rowGroupSize in [ 0; 3; 1 ] do
        let fromRows, fromFooter, metadata = bothWays df rowGroupSize
        if rowGroupSize > 0 then Assert.True(metadata.RowGroups.Count > 1)
        // The engine writes a long string maximum cut and bumped (a valid upper bound, Parquet's rule) where the
        // row statistics leave it out: check it bounds the real maximum, then compare the rest.
        let footer = Nodes.JsonNode.Parse(fromFooter).AsObject()
        // The struct "nested" holds i32 and s: its fields' statistics nest under it and equal theirs (as Spark).
        for section in [ "minValues"; "maxValues"; "nullCount" ] do
            let values = footer[section].AsObject()
            let nested = values["nested"].AsObject()
            for field in [ "i32"; "s" ] do
                Assert.Equal(values[field].ToJsonString(), nested[field].ToJsonString())
            values.Remove "nested" |> ignore
        let maxValues = footer["maxValues"].AsObject()
        let maxS = maxValues["s"].GetValue<string>()
        Assert.True(String.CompareOrdinal(maxS, String('é', 40)) > 0, maxS)
        maxValues.Remove "s" |> ignore
        assertSame fromRows (footer.ToJsonString())

[<Fact>]
let ``Footer statistics of decimals of every width`` () =
    for precision, scale in [ (5, 2); (15, 3); (30, 4); (38, 0) ] do
        use raw = DataFrame.create [| Series.create("d", [ 1.5m; -2.25m; 0m; 123.0m ]) |]
        use df = raw.WithColumns([ pl.col("d").Cast(DataType.Decimal(precision, scale)) |> pl.alias "d" ])
        let fromRows, fromFooter, _ = bothWays df 2
        assertSame fromRows fromFooter

[<Fact>]
let ``Footer statistics leave out NaN and infinite bounds`` () =
    use df = DataFrame.create [| Series.create("x", [ 1.0; nan; 2.0 ]); Series.create("y", [ 1.0; infinity; 2.0 ]) |]
    let _, fromFooter, _ = bothWays df 0
    use doc = JsonDocument.Parse fromFooter
    let has (section: string) (column: string) =
        let mutable v = Unchecked.defaultof<JsonElement>
        doc.RootElement.GetProperty(section).TryGetProperty(column, &v)
    Assert.False(has "maxValues" "y")
    Assert.True(has "minValues" "y")
    // NaN in the data: the footer bound (if any) excludes it, or there is none; never a NaN bound.
    Assert.DoesNotContain("NaN", fromFooter)

[<Fact>]
let ``Footers of files the engine writes parse, and a damaged footer is refused`` () =
    use df = sample ()
    let _, _, metadata = bothWays df 4
    Assert.Equal(10L, metadata.RowCount)
    Assert.Equal(10L, metadata.RowGroups |> Seq.sumBy (fun g -> g.RowCount))
    Assert.Equal(1L, (FileStatistics.NullCounts metadata).["s"])
    Assert.Throws<IO.InvalidDataException>(fun () -> ParquetMetadata.FromFile(ReadOnlySpan "PAR1 not really PAR1"B) |> ignore) |> ignore
