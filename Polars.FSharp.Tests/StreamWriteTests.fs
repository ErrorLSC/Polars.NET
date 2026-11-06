namespace Polars.FSharp.Tests

open System.IO
open System.IO.Pipelines
open Xunit
open Polars.FSharp

/// Smoke tests for the F# Stream / PipeWriter writers. Semantics are covered in Polars.CSharp.Tests (StreamWriteTests).
type StreamWriteTests() =

    let sample () =
        DataFrame.create [
            Series.create("id", [1; 2; 3; 4; 5])
            Series.create("name", ["a"; "b"; "c"; "d"; "e"])
        ]

    [<Fact>]
    member _.``IPC stream: Stream and async Stream match memory output`` () =
        use df = sample ()
        let expected = df.WriteIpcStreamMemory(None, None)

        use ms = new MemoryStream()
        df.WriteIpcStream(ms) |> ignore
        Assert.Equal<byte[]>(expected, ms.ToArray())

        use ms2 = new MemoryStream()
        df.WriteIpcStreamAsync(ms2).GetAwaiter().GetResult()
        Assert.Equal<byte[]>(expected, ms2.ToArray())

    [<Fact>]
    member _.``IPC: sink to Stream and write to PipeWriter round-trip`` () =
        use df = sample ()

        use ms = new MemoryStream()
        df.Lazy().SinkIpc(ms)
        use back = DataFrame.ReadIpc(ms.ToArray())
        Assert.Equal(5L, back.Height)

        let pipe = Pipe()
        use collected = new MemoryStream()
        let reading = pipe.Reader.CopyToAsync(collected)
        df.WriteIpcAsync(pipe.Writer).GetAwaiter().GetResult()
        pipe.Writer.Complete()
        reading.GetAwaiter().GetResult()
        use back2 = DataFrame.ReadIpc(collected.ToArray())
        Assert.Equal(5L, back2.Height)
