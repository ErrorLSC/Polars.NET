namespace Polars.FSharp.Tests

open System
open System.IO
open Xunit
open Polars.FSharp
open Kaminari.Seseragi.Files.Parquet

/// The F# twins of the engine's Parquet options: encryption, scan casts, extra columns and key-value metadata.
type ParquetEngineOptionsTests() =
    let dir = Path.Combine(Path.GetTempPath(), $"polars_fs_parquet_options_{Guid.NewGuid():N}")
    do Directory.CreateDirectory dir |> ignore
    let temp name = Path.Combine(dir, name)
    let k16 = "0123456789012345"B
    let colKey = "1234567890123450"B

    interface IDisposable with
        member _.Dispose() = Directory.Delete(dir, true)

    [<Fact>]
    member _.``Encrypted writes round-trip with a column key and an AAD prefix`` () =
        use df = DataFrame.create [ Series.create ("id", [| 1L; 2L; 3L |]); Series.create ("secret", [| "a"; "b"; "c" |]) ]
        let keys = Map [ "secret", colKey ]
        df.WriteParquet(temp "enc.parquet", encryption = ParquetEncryption(k16, columnKeys = keys, aadPrefix = "t"B)) |> ignore

        use back = DataFrame.ReadParquet(temp "enc.parquet", decryption = ParquetDecryption(k16, columnKeys = keys, aadPrefix = "t"B))
        Assert.True(df.Equals back)
        // Without the column key the other column still reads; the AAD prefix was not stored, so it is needed.
        use ids = DataFrame.ReadParquet(temp "enc.parquet", columns = [ "id" ], decryption = ParquetDecryption(k16, aadPrefix = "t"B))
        Assert.Equal<int64[]>([| 1L; 2L; 3L |], ids.["id"].ToArray<int64>())
        Assert.ThrowsAny<exn>(fun () -> DataFrame.ReadParquet(temp "enc.parquet", decryption = ParquetDecryption(k16, columnKeys = keys)) |> ignore) |> ignore
        Assert.ThrowsAny<exn>(fun () -> DataFrame.ReadParquet(temp "enc.parquet") |> ignore) |> ignore

    [<Fact>]
    member _.``Memory and stream sinks encrypt`` () =
        use df = DataFrame.create [ Series.create ("v", [| 1; 2 |]) ]
        let bytes = df.WriteParquetMemory(encryption = ParquetEncryption k16)
        use back = DataFrame.ReadParquet(bytes, decryption = ParquetDecryption k16)
        Assert.True(df.Equals back)
        use stream = new MemoryStream()
        df.WriteParquetAsync(stream, encryption = ParquetEncryption(k16, plaintextFooter = true)).Wait()
        use fromStream = DataFrame.ReadParquet(stream.ToArray(), decryption = ParquetDecryption k16)
        Assert.True(df.Equals fromStream)

    [<Fact>]
    member _.``Every sink encrypts`` () =
        use df = DataFrame.create [ Series.create ("v", [| 1L; 2L; 3L |]); Series.create ("g", [| "a"; "b"; "a" |]) ]
        let encryption = ParquetEncryption(k16, plaintextFooter = true)
        let decryption = ParquetDecryption k16
        let check (what: string) (read: ParquetDecryption option -> DataFrame) =
            Assert.ThrowsAny<exn>(fun () -> (read None).Dispose()) |> ignore
            use back = read (Some decryption)
            Assert.True(back.Height = 3, what)
        let fromBytes (bytes: byte[]) (d: ParquetDecryption option) =
            match d with
            | Some d -> DataFrame.ReadParquet(bytes, decryption = d)
            | None -> DataFrame.ReadParquet bytes
        let fromPath (path: string) (d: ParquetDecryption option) =
            match d with
            | Some d -> DataFrame.ReadParquet(path, decryption = d)
            | None -> DataFrame.ReadParquet path

        df.WriteParquet(temp "file.parquet", encryption = encryption) |> ignore
        check "file" (fromPath (temp "file.parquet"))
        (use byG = pl.cs.byName [ "g" ]
         df.WriteParquetPartitioned(temp "parts", byG, encryption = encryption) |> ignore)
        check "partitioned" (fromPath (Path.Combine(temp "parts", "**", "*.parquet")))
        check "memory" (fromBytes (df.WriteParquetMemory(encryption = encryption)))
        (use native = df.WriteParquetNativeMemory(encryption = encryption)
         check "native memory" (fromBytes (native.GetSpan().ToArray())))
        (use stream = new MemoryStream()
         df.WriteParquet(stream, encryption = encryption) |> ignore
         check "stream" (fromBytes (stream.ToArray())))
        (use stream = new MemoryStream()
         df.WriteParquetAsync(stream, encryption = encryption).Wait()
         check "async stream" (fromBytes (stream.ToArray())))
        (let pipe = System.IO.Pipelines.Pipe()
         use sink = new MemoryStream()
         let reading = pipe.Reader.CopyToAsync sink
         df.WriteParquetAsync(pipe.Writer, encryption = encryption).Wait()
         pipe.Writer.Complete()
         reading.Wait()
         check "pipe" (fromBytes (sink.ToArray())))
        df.Lazy().SinkParquet(temp "lazy.parquet", encryption = encryption)
        check "lazy sink" (fromPath (temp "lazy.parquet"))

    [<Fact>]
    member _.``Scan casts and extra columns follow the options`` () =
        (DataFrame.create [ Series.create ("v", [| 1; 2 |]) ]).WriteParquet(temp "a.parquet") |> ignore
        (DataFrame.create [ Series.create ("v", [| 3L |]); Series.create ("x", [| "extra" |]) ]).WriteParquet(temp "b.parquet") |> ignore
        let glob = Path.Combine(dir, "*.parquet")
        use schema = new PolarsSchema([ "v", DataType.Int64 ])

        Assert.ThrowsAny<exn>(fun () -> DataFrame.ReadParquet(glob, schema = schema, ignoreExtraColumns = true) |> ignore) |> ignore
        use df =
            DataFrame.ReadParquet(glob, schema = new PolarsSchema([ "v", DataType.Int64 ]), ignoreExtraColumns = true,
                                  castOptions = ScanCastOptions(integers = IntegerCasts.Upcast))
        Assert.Equal<int64[]>([| 1L; 2L; 3L |], df.["v"].ToArray<int64>())

    [<Fact>]
    member _.``Key-value metadata and statistics are written by default`` () =
        use df = DataFrame.create [ Series.create ("v", [| 5L; 1L |]) ]
        df.WriteParquet(temp "meta.parquet", metadata = Map [ "owner", "team-a" ]) |> ignore
        use file = ParquetFile.Open(temp "meta.parquet")
        Assert.Contains(file.Metadata.KeyValueMetadata, fun p -> p.Key = "owner" && p.Value = "team-a")
        let stats = file.Metadata.RowGroups[0].Columns[0].Statistics
        Assert.Equal(1L, BitConverter.ToInt64(stats.Min, 0))
        Assert.Equal(5L, BitConverter.ToInt64(stats.Max, 0))

    [<Fact>]
    member _.``Engine key lengths are checked with a clear message`` () =
        use df = DataFrame.create [ Series.create ("v", [| 1 |]) ]
        let e = Assert.Throws<ArgumentException>(fun () -> df.WriteParquetMemory(encryption = ParquetEncryption(Array.zeroCreate 24)) |> ignore)
        Assert.Contains("the footer key is 24 bytes", e.Message)
