using System.Security.Cryptography;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetWriter{T}"/>: rows written as <see cref="ArrowRowBuilder{T}"/> lays them out come back through
/// <see cref="ParquetReader{T}"/>; the file depends on the rows alone (not on how they were passed, nor on the threads).
/// </summary>
public sealed class ParquetWriterOfTTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-typed").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    [Fact]
    public void Rows_ComeBackThroughTheTypedReader()
    {
        Trade[] rows = Trades(40_000);
        string path = Path("trades.parquet");
        ParquetWriter.WriteRows(path, rows, new ParquetWriteOptions { RowGroupRows = 15_000 });

        using var file = ParquetFile.Open(path);
        Assert.Equal(40_000, file.Metadata.RowCount);
        Assert.Equal([15_000L, 15_000L, 10_000L], file.Metadata.RowGroups.Select(g => g.RowCount));
        List<Trade> read = ParquetFile.ReadList<Trade>(path);
        Assert.Equal(rows.Length, read.Count);
        for (int i = 0; i < rows.Length; i++) Assert.True(Same(rows[i], read[i]), $"row {i}: wrote {rows[i]}, read {read[i]}");
    }

    [Fact]
    public async Task TheFile_DependsOnTheRowsAlone()
    {
        Trade[] rows = Trades(50_000);
        var options = new ParquetWriteOptions { RowGroupRows = 20_000 };
        string Hash(string name) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path(name))));

        ParquetWriter.WriteRows(Path("array.parquet"), rows, options);
        ParquetWriter.WriteRows(Path("list.parquet"), rows.ToList(), options);
        ParquetWriter.WriteRows(Path("lazy.parquet"), rows.Select(r => r), options);
        ParquetWriter.WriteRows(Path("one-thread.parquet"), rows, options with { MaxDegreeOfParallelism = 1 });
        await ParquetWriter.WriteRowsAsync(Path("async.parquet"), rows.ToAsyncEnumerable(), options);
        using (var writer = ParquetWriter<Trade>.Create(Path("by-row.parquet"), options))
        {
            foreach (Trade row in rows) writer.WriteRow(row);
            Assert.Equal(rows.Length, writer.RowCount);
        }
        await using (var writer = ParquetWriter<Trade>.Create(Path("mixed.parquet"), options))
        {
            writer.WriteRows(rows.AsSpan(0, 7).ToArray());
            await writer.WriteRowsAsync(rows.Skip(7).Take(20_000));
            foreach (Trade row in rows.Skip(20_007).Take(3)) writer.WriteRow(row);
            await writer.WriteRowsAsync(rows.Skip(20_010).ToAsyncEnumerable());
        }
        // The batches the builder makes, written as Arrow by the caller, one thread.
        using (var builder = new ArrowRowBuilder<Trade>())
        using (var writer = ParquetWriter.Create(Path("arrow.parquet"), ArrowRowBuilder<Trade>.Schema, options with { MaxDegreeOfParallelism = 1 }))
        {
            foreach (Trade[] chunk in rows.Chunk(ParquetWriter<Trade>.BatchRows))
            {
                builder.AppendRange(chunk);
                using Apache.Arrow.RecordBatch batch = builder.ExportBatch();
                writer.Write(batch);
            }
        }

        string expected = Hash("array.parquet");
        Assert.All(["list", "lazy", "one-thread", "async", "by-row", "mixed", "arrow"], name => Assert.Equal(expected, Hash($"{name}.parquet")));
    }

    [Fact]
    public void RowGroupsClosedOnBytes_DoNotDependOnTheThreads()
    {
        Trade[] rows = Trades(120_000);
        var options = new ParquetWriteOptions { RowGroupBytes = 1 << 20 };
        string Hash(string name) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path(name))));
        ParquetWriter.WriteRows(Path("bytes-1.parquet"), rows, options with { MaxDegreeOfParallelism = 1 });
        ParquetWriter.WriteRows(Path("bytes-8.parquet"), rows, options with { MaxDegreeOfParallelism = 8 });
        ParquetWriter.WriteRows(Path("bytes-lazy.parquet"), rows.Select(r => r), options with { MaxDegreeOfParallelism = 8 });
        using (var file = ParquetFile.Open(Path("bytes-1.parquet"))) Assert.True(file.Metadata.RowGroups.Count > 2);
        Assert.Equal(Hash("bytes-1.parquet"), Hash("bytes-8.parquet"));
        Assert.Equal(Hash("bytes-1.parquet"), Hash("bytes-lazy.parquet"));
    }

    [Fact]
    public async Task EndRowGroup_WritesTheRowsGatheredSoFar()
    {
        Trade[] rows = Trades(150);
        using (var writer = ParquetWriter<Trade>.Create(Path("groups.parquet")))
        {
            writer.WriteRows(rows[..100]);
            writer.EndRowGroup();
            Assert.True(writer.BytesWritten > 0);
            writer.WriteRows(rows[100..120]);
            await writer.EndRowGroupAsync();
            writer.WriteRows(rows[120..]);
            writer.Complete();
            Assert.Equal(150, writer.Metadata.RowCount);
        }
        using var file = ParquetFile.Open(Path("groups.parquet"));
        Assert.Equal([100L, 20L, 30L], file.Metadata.RowGroups.Select(g => g.RowCount));
    }

    [Fact]
    public void NullRows_AreRefused_TheWriterGoesOn()
    {
        string path = Path("nulls.parquet");
        using (var writer = ParquetWriter<Trade>.Create(path))
        {
            writer.WriteRows(Trades(3));
            var e = Assert.Throws<ArgumentException>(() => writer.WriteRow(null!));
            Assert.Contains("Row 3", e.Message);
            writer.WriteRow(Trades(1)[0]);
        }
        Assert.Equal(4, ParquetFile.ReadList<Trade>(path).Count);
    }

    [Fact]
    public void InParallel_ANullRow_StopsWhereOneThreadStops()
    {
        Trade[] rows = Trades(100_000);
        rows[70_000] = null!;
        string path = Path("parallel-null.parquet");
        using (var writer = ParquetWriter<Trade>.Create(path, new ParquetWriteOptions { MaxDegreeOfParallelism = 8 }))
        {
            writer.WriteRow(rows[1]); // an open batch first
            var e = Assert.Throws<ArgumentException>(() => writer.WriteRows(rows));
            Assert.Contains("Row 70001", e.Message);
            Assert.Equal(70_001, writer.RowCount);
        }
        List<Trade> read = ParquetFile.ReadList<Trade>(path);
        Assert.Equal(70_001, read.Count);
        Assert.True(Same(rows[69_999], read[^1]));
    }

    [Fact]
    public void InParallel_TheFirstRefusedValue_AbortsTheFile()
    {
        Trade[] rows = Trades(200_000);
        rows[150_000] = rows[150_000] with { Price = 1m / 3 };
        rows[40_000] = rows[40_000] with { Side = (Side)9 };
        string path = Path("parallel-refused.parquet");
        var writer = ParquetWriter<Trade>.Create(path, new ParquetWriteOptions { MaxDegreeOfParallelism = 8 });
        Assert.Throws<ArgumentException>(() => writer.WriteRows(rows)); // the undefined Side, before the decimal
        Assert.Throws<InvalidOperationException>(() => writer.WriteRow(rows[0]));
        writer.Dispose();
        Assert.ThrowsAny<Exception>(() => ParquetFile.Open(path));
    }

    [Fact]
    public void ARefusedValue_AbortsTheFile()
    {
        string path = Path("refused.parquet");
        var writer = ParquetWriter<Trade>.Create(path);
        writer.WriteRows(Trades(10));
        Assert.Throws<OverflowException>(() => writer.WriteRow(Trades(1)[0] with { Price = 1m / 3 }));
        Assert.Throws<InvalidOperationException>(() => writer.WriteRow(Trades(1)[0]));
        Assert.Throws<InvalidOperationException>(() => writer.Complete());
        writer.Dispose();
        Assert.ThrowsAny<Exception>(() => ParquetFile.Open(path)); // no footer
    }

    [Fact]
    public void AFailingSource_DeletesThePartialFile()
    {
        string path = Path("failing.parquet");
        IEnumerable<Trade> Failing()
        {
            foreach (Trade row in Trades(20_000)) yield return row;
            throw new InvalidDataException("source failed");
        }
        Assert.Throws<InvalidDataException>(() => ParquetWriter.WriteRows(path, Failing()));
        Assert.False(File.Exists(path));
    }

    [Fact]
    public void Dictionaries_AreWrittenAsMaps_AndReadBack()
    {
        string path = Path("maps.parquet");
        Priced[] rows =
        [
            new(1, new Dictionary<string, decimal> { ["a"] = 1.5m, ["b"] = 2m }, new Venues(new() { [7] = "x", [8] = null }), [new() { ["p"] = 1 }]),
            new(2, null, null, null),
            new(3, new Dictionary<string, decimal>(), new Venues([]), [[]]),
        ];
        ParquetWriter.WriteRows(path, rows);

        using (var file = ParquetFile.Open(path))
        {
            var thrift = file.Metadata.Thrift!.Schema;
            Assert.Equal(["Prices", "Codes", "element"], thrift.Where(e => e.LogicalType?.Kind == Kaminari.Seseragi.Files.Parquet.Metadata.LogicalTypeKind.Map).Select(e => e.Name));
            // The stored Arrow schema says maps too.
            string stored = file.Metadata.Thrift!.KeyValueMetadata!.Single(kv => kv.Key == "ARROW:schema").Value!;
            using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(new MemoryStream(Convert.FromBase64String(stored)));
            Apache.Arrow.Schema arrow = reader.Schema;
            Assert.IsType<Apache.Arrow.Types.MapType>(arrow.GetFieldByName("Prices").DataType);
            var tiers = Assert.IsType<Apache.Arrow.Types.LargeListType>(arrow.GetFieldByName("Tiers").DataType);
            Assert.IsType<Apache.Arrow.Types.MapType>(tiers.ValueDataType);
        }
        List<Priced> read = ParquetFile.ReadList<Priced>(path);
        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].Prices, read[i].Prices);
            Assert.Equal(rows[i].Venues?.Codes, read[i].Venues?.Codes);
            Assert.Equal(rows[i].Tiers, read[i].Tiers);
        }
    }

    [Fact]
    public void WideIntegers_AreDecimals_AndComeBack()
    {
        string path = Path("wide.parquet");
        Int128 max = Int128.Parse("99999999999999999999999999999999999999"); // 10^38 - 1
        Counter[] rows = [new(max, UInt128.Parse("99999999999999999999999999999999999999"), -max), new(-5, null, null)];
        ParquetWriter.WriteRows(path, rows);
        using (var file = ParquetFile.Open(path))
        {
            var big = file.Metadata.Thrift!.Schema[1];
            Assert.Equal((38, 0, 16), (big.Precision, big.Scale, big.TypeLength));
        }
        List<Counter> read = ParquetFile.ReadList<Counter>(path);
        Assert.Equal(rows, read);
    }

    /// <summary>
    /// Decimals of up to twenty integer digits fit decimal(38, 18), and come back although their unscaled values pass
    /// .NET's 96 bits at scale 18 (the fraction's trailing zeros dropped on the way).
    /// </summary>
    [Fact]
    public void LargeDecimals_ComeBack()
    {
        string path = Path("large-decimals.parquet");
        Amount[] rows =
        [
            new(1, 12345678901234567890.123456789m), new(2, -99999999999999999999.99m), new(3, 79228162514.264337593543950335m),
            new(4, 0.000000000000000001m), new(5, decimal.Round(decimal.MaxValue / 1e9m)), new(6, null),
        ];
        ParquetWriter.WriteRows(path, rows);
        Assert.Equal(rows, ParquetFile.ReadList<Amount>(path));
    }

    [Theory]
    [InlineData("100000000000000000000000000000000000000", false)]  // 10^38: 39 digits
    [InlineData("-100000000000000000000000000000000000000", false)]
    [InlineData("170141183460469231731687303715884105728", true)]   // 2^127 (unsigned)
    [InlineData("340282366920938463463374607431768211455", true)]   // UInt128.MaxValue
    public void WideIntegers_OfMoreThan38Digits_AreRefused(string value, bool unsigned)
    {
        Counter row = unsigned ? new(0, UInt128.Parse(value), null) : new(Int128.Parse(value), null, null);
        var e = Assert.Throws<NotSupportedException>(() => ParquetWriter.WriteRows(Path("refused.parquet"), [row]));
        Assert.Contains("Guid or byte[]", e.Message);
        Assert.False(File.Exists(Path("refused.parquet")));
    }

    [Fact]
    public void Scalars_AreOneValueColumn()
    {
        string path = Path("scalars.parquet");
        ParquetWriter.WriteRows(path, [1, 2, 3]);
        using var file = ParquetFile.Open(path);
        Assert.Equal(["value"], file.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void ColumnOptions_NameTheMembers()
    {
        string path = Path("bloom.parquet");
        ParquetWriter.WriteRows(path, Trades(1000), new ParquetWriteOptions
        {
            Columns = new Dictionary<string, ParquetColumnOptions> { ["Symbol"] = new() { BloomFilter = new() } },
        });
        using var file = ParquetFile.Open(path);
        Assert.NotNull(file.Metadata.Thrift!.RowGroups[0].Columns[1].MetaData!.BloomFilterOffset);
    }

    internal static bool Same(Trade a, Trade b) =>
        a with { Tags = null, Legs = null } == b with { Tags = null, Legs = null }
        && (a.Tags ?? []).SequenceEqual(b.Tags ?? []) && (a.Tags is null) == (b.Tags is null)
        && (a.Legs ?? []).SequenceEqual(b.Legs ?? []) && (a.Legs is null) == (b.Legs is null);

    internal static Trade[] Trades(int count) => [.. Enumerable.Range(0, count).Select(i => new Trade(
        i,
        $"SYM{i % 97}",
        i % 7 == 0 ? null : $"note {i} " + new string('n', i % 30),
        Math.Round(i * 1.25m, 2),
        i % 5 == 0 ? null : i * 0.5,
        new DateTime(2020, 1, 1).AddSeconds(i * 37),
        DateOnly.FromDayNumber(738_000 + i % 1000),
        (Side)(i % 3),
        i % 11 == 0 ? null : (Side)((i + 1) % 3),
        i % 2 == 0,
        new Guid(i, 1, 2, [3, 4, 5, 6, 7, 8, 9, 10]),
        i % 13 == 0 ? null : [.. Enumerable.Range(0, i % 4).Select(t => $"t{t}")],
        i % 17 == 0 ? null : [new Leg(i, "a"), new Leg(-i, null)]))];
}

public enum Side { Buy, Sell, Hold }

public sealed record Venues(Dictionary<int, string?> Codes);

public sealed record Counter(Int128 Big, UInt128? Unsigned, Int128? Maybe);

public sealed record Amount(int Id, decimal? Value);

public sealed record Priced(int Id, IReadOnlyDictionary<string, decimal>? Prices, Venues? Venues, List<Dictionary<string, int>>? Tiers);

public sealed record Leg(int Qty, string? Venue);

public sealed record Trade(
    long Id, string Symbol, string? Note, decimal Price, double? Fee, DateTime At, DateOnly Day, Side Side, Side? Hint, bool Flag,
    Guid Ref, List<string>? Tags, List<Leg>? Legs);
