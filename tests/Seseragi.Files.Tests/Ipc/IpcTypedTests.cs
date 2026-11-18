using System.Security.Cryptography;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// <see cref="IpcWriter{T}"/> and <see cref="IpcReader{T}"/>: rows written as <see cref="ArrowRowBuilder{T}"/> lays
/// them out come back, in both formats and with every compression; the output depends on the rows alone; a
/// predicate is evaluated by the reader.
/// </summary>
public sealed class IpcTypedTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-ipc-typed").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    public static TheoryData<string> Compressions => ["none", "lz4", "zstd"];

    private static IIpcCompression Compression(string name) => name switch
    {
        "lz4" => Lz4FrameCompression.Instance,
        "zstd" => new ZstandardCompression(3),
        _ => NoCompression.Instance,
    };

    [Theory]
    [MemberData(nameof(Compressions))]
    public void Rows_ComeBackThroughTheTypedReader(string compression)
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(40_000);
        string path = Path("trades.arrow");
        IpcWriter.WriteRows(path, rows, new IpcWriteOptions { Compression = Compression(compression) });
        using (IpcFile file = IpcFile.Open(path))
            Assert.Equal((40_000 + IpcWriter<Trade>.BatchRows - 1) / IpcWriter<Trade>.BatchRows, file.BatchCount);
        foreach (int parallelism in (int[])[1, 8])
        {
            List<Trade> read = IpcFile.ReadList<Trade>(path, options: new IpcReadOptions { MaxDegreeOfParallelism = parallelism });
            Assert.Equal(rows.Length, read.Count);
            for (int i = 0; i < rows.Length; i++) Assert.True(ParquetWriterOfTTests.Same(rows[i], read[i]), $"row {i}: wrote {rows[i]}, read {read[i]}");
        }
    }

    [Fact]
    public void Streams_AndMaps_AndBigIntegers_RoundTrip()
    {
        Priced[] priced =
        [
            new(1, new Dictionary<string, decimal> { ["a"] = 1.5m, ["b"] = 2m }, new Venues(new() { [7] = "x", [8] = null }), [new() { ["p"] = 1 }]),
            new(2, null, null, null),
            new(3, new Dictionary<string, decimal>(), new Venues([]), [[]]),
        ];
        using var memory = new MemoryStream();
        IpcWriter.WriteRows(memory, priced, new IpcWriteOptions { Format = IpcFormat.Stream, LeaveOpen = true });
        memory.Position = 0;
        using (IpcStreamReader reader = IpcStreamReader.Open(memory))
            Assert.Equal(3, reader.ReadBatches().Sum(b => { using (b) return b.Length; }));

        string path = Path("priced.arrow");
        IpcWriter.WriteRows(path, priced);
        List<Priced> read = IpcFile.ReadList<Priced>(path);
        Assert.Equal(3, read.Count);
        Assert.Equal(priced[0].Prices, read[0].Prices);
        Assert.Equal(priced[0].Venues!.Codes, read[0].Venues!.Codes);
        Assert.Equal(priced[0].Tiers![0], read[0].Tiers![0]);
        Assert.Null(read[1].Prices);
        Assert.Empty(read[2].Venues!.Codes);

        Int128 max = Int128.MaxValue / 1000;
        Counter[] counters = [new(max, UInt128.Parse("99999999999999999999999999999999999999"), -max), new(-5, null, null)];
        IpcWriter.WriteRows(Path("counters.arrow"), counters);
        Assert.Equal(counters, IpcFile.ReadList<Counter>(Path("counters.arrow")));
    }

    /// <summary>The output depends on the rows alone: arrays, lists, lazy sequences, async, row by row, any thread count.</summary>
    [Theory]
    [InlineData(IpcFormat.File)]
    [InlineData(IpcFormat.Stream)]
    public async Task TheOutput_DependsOnTheRowsAlone(IpcFormat format)
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(50_000);
        var options = new IpcWriteOptions { Format = format, Compression = Lz4FrameCompression.Instance };
        string Hash(string name) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path(name))));

        IpcWriter.WriteRows(Path("array"), rows, options);
        IpcWriter.WriteRows(Path("list"), rows.ToList(), options);
        IpcWriter.WriteRows(Path("lazy"), rows.Select(r => r), options);
        IpcWriter.WriteRows(Path("one-thread"), rows, options with { MaxDegreeOfParallelism = 1 });
        await IpcWriter.WriteRowsAsync(Path("async"), rows.ToAsyncEnumerable(), options);
        using (var writer = IpcWriter<Trade>.Create(Path("by-row"), options))
        {
            foreach (Trade row in rows) writer.WriteRow(row);
            Assert.Equal(rows.Length, writer.RowCount);
        }
        await using (var writer = IpcWriter<Trade>.Create(Path("mixed"), options))
        {
            writer.WriteRows(rows.AsSpan(0, 7).ToArray());
            await writer.WriteRowsAsync(rows.Skip(7).Take(20_000));
            foreach (Trade row in rows.Skip(20_007).Take(3)) writer.WriteRow(row);
            await writer.WriteRowsAsync(rows.Skip(20_010).ToAsyncEnumerable());
        }
        string expected = Hash("array");
        Assert.All(["list", "lazy", "one-thread", "async", "by-row", "mixed"], name => Assert.Equal(expected, Hash(name)));
    }

    [Fact]
    public void APredicate_IsEvaluatedByTheReader()
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(30_000);
        string path = Path("trades.arrow");
        IpcWriter.WriteRows(path, rows);
        using IpcFile file = IpcFile.Open(path);
        foreach (int parallelism in (int[])[1, 8])
        {
            var options = new IpcReadOptions { MaxDegreeOfParallelism = parallelism };
            using (IpcReader<Trade> reader = file.OpenReader<Trade>(t => (t.Id < 10 || t.Id >= 29_000) && t.Note != null, options))
            {
                Assert.Equal(rows.Where(t => (t.Id < 10 || t.Id >= 29_000) && t.Note != null).Select(t => t.Id), reader.ReadRows().Select(t => t.Id));
            }
            using (IpcReader<Trade> reader = file.OpenReader<Trade>(t => t.Symbol == "SYM5" && t.Fee > 100, options))
            {
                Assert.True(reader.FiltersExactly);
                Assert.True(reader.Inner.FiltersExactly);
                Assert.Equal(rows.Where(t => t.Symbol == "SYM5" && t.Fee > 100).Select(t => t.Id), reader.ReadRows().Select(t => t.Id));
            }
            // A row limit counts the rows delivered.
            using (IpcReader<Trade> reader = file.OpenReader<Trade>(t => t.Flag, options with { MaxRows = 20_000 }))
                Assert.Equal(rows.Where(t => t.Flag).Take(20_000).Select(t => t.Id), reader.ReadRows().Select(t => t.Id));
        }
        // Only the members' columns are read.
        using IpcReader<Narrow> narrow = file.OpenReader<Narrow>();
        Assert.Equal(["Id", "Symbol"], narrow.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void FiltersExactly_SaysWhetherTheOptionsFilterIsEvaluatedToo()
    {
        string path = Path("trades.arrow");
        IpcWriter.WriteRows(path, ParquetWriterOfTTests.Trades(1000));
        using IpcFile file = IpcFile.Open(path);
        ScalarExpr betweenColumns = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("Id"), ScalarExpr.NewColumn("Fee"));
        ScalarExpr evaluated = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("Id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(50)));
        foreach (var (filter, rowsFiltered, exact) in (ValueTuple<ScalarExpr, bool, bool>[])[(betweenColumns, true, false), (betweenColumns, false, false), (evaluated, false, true)])
        {
            var options = new IpcReadOptions { Filter = filter, FilterRows = rowsFiltered };
            using (IpcReader<Narrow> reader = file.OpenReader<Narrow>(r => r.Id < 100, options)) Assert.Equal(exact, reader.FiltersExactly);
            using (IpcReader<Narrow> reader = file.OpenReader<Narrow>(null, options)) Assert.Equal(exact && rowsFiltered, reader.FiltersExactly);
        }
        using (IpcReader<Narrow> reader = file.OpenReader<Narrow>(r => r.Id < 100, new IpcReadOptions { Filter = evaluated }))
            Assert.Equal(50, reader.ReadRows().Count());
    }

    /// <summary>
    /// Batches read back (their string-view dictionaries decoded by the reader) are written again, in both formats, and
    /// again: the writer leaves the caller's arrays alone (it once disposed a dictionary it compared).
    /// </summary>
    [Theory]
    [InlineData(IpcFormat.File)]
    [InlineData(IpcFormat.Stream)]
    public void BatchesRead_AreWrittenAgain(IpcFormat format)
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(40_000);
        string path = Path("trades.arrow"), again = Path("again.arrow");
        IpcWriter.WriteRows(path, rows);
        using (IpcFile file = IpcFile.Open(path))
        using (IpcReader reader = file.OpenReader(new IpcReadOptions { MemoryMap = IpcMemoryMap.Off }))
        {
            List<Apache.Arrow.RecordBatch> batches = [.. reader.ReadBatches()];
            try
            {
                IpcWriter.Write(Path("first.arrow"), reader.Schema, batches, new IpcWriteOptions { Format = format });
                IpcWriter.Write(again, reader.Schema, batches, new IpcWriteOptions { Format = format });
                Assert.Equal(File.ReadAllBytes(Path("first.arrow")), File.ReadAllBytes(again));
            }
            finally
            {
                foreach (Apache.Arrow.RecordBatch batch in batches) batch.Dispose();
            }
        }
        List<Trade> read = format == IpcFormat.File ? IpcFile.ReadList<Trade>(again) : [];
        if (format == IpcFormat.File)
        {
            Assert.Equal(rows.Length, read.Count);
            for (int i = 0; i < rows.Length; i++) Assert.True(ParquetWriterOfTTests.Same(rows[i], read[i]), $"row {i}");
        }
        else
        {
            using IpcStreamReader stream = IpcStreamReader.Open(File.OpenRead(again));
            Assert.Equal(rows.Length, stream.ReadBatches().Sum(b => { using (b) return b.Length; }));
        }
    }

    [Fact]
    public void AFailedWrite_DeletesThePartialFile()
    {
        string path = Path("failed.arrow");
        Leg?[] rows = [new Leg(1, "a"), null];
        Assert.Throws<ArgumentException>(() => IpcWriter.WriteRows(path, rows));
        Assert.False(File.Exists(path));
    }

    public sealed record Narrow(long Id, string Symbol);
}
