namespace Polars.FSharp.Tests

open System.IO
open System.IO.Pipelines
open Xunit
open Polars.FSharp

/// Smoke tests for the F# JSON / Avro Stream / PipeWriter writers. Semantics are covered in Polars.CSharp.Tests (StreamWriteTests).
type StreamWriteJsonAvroTests() =

    let sample () =
        DataFrame.create [
            Series.create("id", [1; 2; 3; 4; 5])
            Series.create("name", ["a"; "b"; "c"; "d"; "e"])
        ]

    [<Fact>]
    member _.``JSON: DataFrame WriteJson to Stream round-trips`` () =
        use df = sample ()

        use ms = new MemoryStream()
        df.WriteJson(ms) |> ignore
        use back = DataFrame.ReadJson(ms.ToArray())
        Assert.Equal(5L, back.Height)

    [<Fact>]
    member _.``JSON: DataFrame WriteJsonAsync to PipeWriter round-trips`` () =
        use df = sample ()

        let pipe = Pipe()
        use collected = new MemoryStream()
        let reading = pipe.Reader.CopyToAsync(collected)
        df.WriteJsonAsync(pipe.Writer).GetAwaiter().GetResult()
        pipe.Writer.Complete()
        reading.GetAwaiter().GetResult()
        use back = DataFrame.ReadJson(collected.ToArray())
        Assert.Equal(5L, back.Height)

    [<Fact>]
    member _.``JSON: LazyFrame SinkJson to Stream matches memory sink`` () =
        use df = sample ()
        let expected = df.Lazy().SinkJsonMemory()

        use ms = new MemoryStream()
        df.Lazy().SinkJson(ms)
        Assert.Equal<byte[]>(expected, ms.ToArray())

        use ms2 = new MemoryStream()
        df.Lazy().SinkJsonAsync(ms2).GetAwaiter().GetResult()
        Assert.Equal<byte[]>(expected, ms2.ToArray())

    [<Fact>]
    member _.``Avro: DataFrame WriteAvro to Stream and async Stream round-trip`` () =
        use df = sample ()

        use ms = new MemoryStream()
        df.WriteAvro(ms) |> ignore
        use back = DataFrame.ReadAvro(ms.ToArray())
        Assert.Equal(5L, back.Height)

        use ms2 = new MemoryStream()
        df.WriteAvroAsync(ms2).GetAwaiter().GetResult()
        use back2 = DataFrame.ReadAvro(ms2.ToArray())
        Assert.Equal(5L, back2.Height)

    [<Fact>]
    member _.``Avro: DataFrame WriteAvroAsync to PipeWriter round-trips`` () =
        use df = sample ()

        let pipe = Pipe()
        use collected = new MemoryStream()
        let reading = pipe.Reader.CopyToAsync(collected)
        df.WriteAvroAsync(pipe.Writer).GetAwaiter().GetResult()
        pipe.Writer.Complete()
        reading.GetAwaiter().GetResult()
        use back = DataFrame.ReadAvro(collected.ToArray())
        Assert.Equal(5L, back.Height)
