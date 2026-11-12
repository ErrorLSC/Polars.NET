namespace Polars.FSharp.Tests

open System
open System.IO
open Xunit
open Polars.FSharp

/// Smoke tests for the F# Avro API on the managed reader / writer. Semantics are covered in Polars.CSharp.Tests
/// (AvroManagedTests) and Seseragi.Files.Tests.
type AvroManagedTests() =

    let sample () =
        DataFrame.create [
            Series.create("id", [1L .. 1000L])
            Series.create("name", [ for i in 1 .. 1000 -> $"n{i}" ])
        ]

    let withTempFile (f: string -> unit) =
        let path = Path.Combine(Path.GetTempPath(), $"fs-avro-{Guid.NewGuid():N}.avro")
        try f path finally File.Delete path

    [<Fact>]
    member _.``ReadAvro with and without paralleling gives the same frame`` () =
        withTempFile (fun path ->
            use df = sample ()
            df.WriteAvro(path, AvroCompression.Zstandard) |> ignore
            use a = DataFrame.ReadAvro(path, paralleling = false)
            use b = DataFrame.ReadAvro(path)
            Assert.True(df.Equals a)
            Assert.True(a.Equals b))

    [<Fact>]
    member _.``ScanAvro filters and SinkAvro writes the result`` () =
        withTempFile (fun source ->
            withTempFile (fun target ->
                use df = sample ()
                df.WriteAvro(source) |> ignore
                use lf = LazyFrame.ScanAvro(source, columns = [ "id"; "name" ])
                lf.Filter(pl.col "id" .> pl.lit 990L).SinkAvro(target, AvroCompression.Snappy)
                use back = DataFrame.ReadAvro(target)
                Assert.Equal(10L, back.Height)))

    [<Fact>]
    member _.``ReadAvro reads a stream block by block`` () =
        use df = sample ()
        use ms = new MemoryStream(df.WriteAvroMemory(AvroCompression.Deflate))
        use back = DataFrame.ReadAvro(ms, nRows = 5UL)
        Assert.Equal(5L, back.Height)
