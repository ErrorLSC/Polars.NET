namespace Polars.FSharp.Tests

open System.IO
open System.IO.Pipelines
open Xunit
open Polars.FSharp

/// Smoke tests for the F# Parquet / CSV Stream and PipeWriter writers. Semantics are covered in Polars.CSharp.Tests (StreamWriteTests).
type StreamWriteParquetCsvTests() =

    let sample () =
        DataFrame.create [
            Series.create("id", [1; 2; 3; 4; 5])
            Series.create("name", ["a"; "b"; "c"; "d"; "e"])
        ]

    [<Fact>]
    member _.``Parquet: Stream and async Stream match memory output`` () =
        use df = sample ()
        let expected = df.WriteParquetMemory()

        use ms = new MemoryStream()
        df.WriteParquet(ms) |> ignore
        Assert.Equal<byte[]>(expected, ms.ToArray())

        use ms2 = new MemoryStream()
        df.WriteParquetAsync(ms2).GetAwaiter().GetResult()
        Assert.Equal<byte[]>(expected, ms2.ToArray())

    [<Fact>]
    member _.``Parquet: sink to Stream and write to PipeWriter match memory output`` () =
        use df = sample ()
        let expected = df.Lazy().SinkParquetMemory()

        use ms = new MemoryStream()
        df.Lazy().SinkParquet(ms)
        Assert.Equal<byte[]>(expected, ms.ToArray())

        let pipe = Pipe()
        use collected = new MemoryStream()
        let reading = pipe.Reader.CopyToAsync(collected)
        df.WriteParquetAsync(pipe.Writer).GetAwaiter().GetResult()
        pipe.Writer.Complete()
        reading.GetAwaiter().GetResult()
        Assert.Equal<byte[]>(expected, collected.ToArray())

    [<Fact>]
    member _.``CSV: Stream and async Stream match memory output`` () =
        use df = sample ()
        let expected = df.WriteCsvMemory()

        use ms = new MemoryStream()
        df.WriteCsv(ms) |> ignore
        Assert.Equal<byte[]>(expected, ms.ToArray())

        use ms2 = new MemoryStream()
        df.WriteCsvAsync(ms2).GetAwaiter().GetResult()
        Assert.Equal<byte[]>(expected, ms2.ToArray())

    [<Fact>]
    member _.``CSV: sink to Stream and write to PipeWriter match memory output`` () =
        use df = sample ()
        let expected = df.Lazy().SinkCsvMemory()

        use ms = new MemoryStream()
        df.Lazy().SinkCsv(ms)
        Assert.Equal<byte[]>(expected, ms.ToArray())

        let pipe = Pipe()
        use collected = new MemoryStream()
        let reading = pipe.Reader.CopyToAsync(collected)
        df.WriteCsvAsync(pipe.Writer).GetAwaiter().GetResult()
        pipe.Writer.Complete()
        reading.GetAwaiter().GetResult()
        Assert.Equal<byte[]>(expected, collected.ToArray())
