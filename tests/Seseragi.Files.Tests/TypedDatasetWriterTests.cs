using Apache.Arrow;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests;

/// <summary>
/// <see cref="ParquetDatasetWriter{T}"/> and <see cref="IpcDatasetWriter{T}"/>: rows back through the typed dataset
/// readers (partition columns typed by <c>PartitionSchema</c>), keys of every member type a path holds, the same files
/// whatever way the rows come, refused rows giving the write up, maps as Parquet MAP groups.
/// </summary>
public sealed class TypedDatasetWriterTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-typed-dataset-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    public static TheoryData<string> Formats => ["parquet", "ipc", "ipcstream"];

    private static DatasetWriteOptions Options(string format) => format switch
    {
        "parquet" => new ParquetDatasetWriteOptions(),
        "ipc" => new IpcDatasetWriteOptions(),
        _ => new IpcDatasetWriteOptions { Writer = new IpcWriteOptions { Format = IpcFormat.Stream } },
    };

    private static DatasetWriteResult<DatasetWrittenFile> WriteRows<T>(string format, string path, IEnumerable<T> rows, DatasetWriteOptions options)
    {
        if (format == "parquet")
        {
            DatasetWriteResult<ParquetWrittenFile> written = ParquetDatasetWriter.WriteRows(path, rows, (ParquetDatasetWriteOptions)options);
            return new DatasetWriteResult<DatasetWrittenFile>(written.WriteId, written.Files);
        }
        return IpcDatasetWriter.WriteRows(path, rows, (IpcDatasetWriteOptions)options);
    }

    private static List<T> ReadList<T>(string format, string path, Schema? partitions) => format == "parquet"
        ? ParquetDataset.ReadList<T>(path, datasetOptions: new ParquetDatasetOptions { PartitionSchema = partitions })
        : IpcDataset.ReadList<T>(path, datasetOptions: new IpcDatasetOptions { PartitionSchema = partitions });

    private static Schema? PartitionSchema<T>(string format, string path, DatasetWriteOptions options)
    {
        if (format == "parquet")
        {
            using var parquet = ParquetDatasetWriter<T>.Create(path, (ParquetDatasetWriteOptions)options with { ExistingData = ExistingData.Append });
            return parquet.PartitionSchema;
        }
        using var ipc = IpcDatasetWriter<T>.Create(path, (IpcDatasetWriteOptions)options with { ExistingData = ExistingData.Append });
        return ipc.PartitionSchema;
    }

    private static Deal[] Deals(int count) =>
    [
        .. Enumerable.Range(0, count).Select(i => new Deal(
            i,
            i % 11 == 0 ? null : $"S{i % 97}",
            (Desk)(i % 3),
            2020 + i % 4,
            i * 0.25m,
            new DateOnly(2024, 1, 1).AddDays(i % 400),
            new TimeOnly(i % 24, i % 60, i % 60).Add(TimeSpan.FromTicks(i % 10_000 * 10)),
            new DateTime(2024, 1, 1).AddTicks(i * 10_000_010L),
            i % 13 == 0 ? double.NaN : i / 3.0,
            i % 5 == 0 ? null : new Dictionary<string, int> { ["a"] = i, ["b"] = -i },
            i % 7 == 0 ? null : [i, i + 1])),
    ];

    /// <summary>A trade as text (maps and lists by their content).</summary>
    private static string Text(Deal t) =>
        $"{t.Id}|{t.Symbol}|{t.Desk}|{t.Year}|{t.Price:G29}|{t.Day}|{t.At:HH:mm:ss.fffffff}|{t.When:O}|{t.X}|" +
        $"{(t.Tags is null ? "-" : string.Join(",", t.Tags.OrderBy(p => p.Key).Select(p => $"{p.Key}={p.Value}")))}|" +
        $"{(t.Lots is null ? "-" : string.Join(",", t.Lots))}";

    private static List<string> Texts(IEnumerable<Deal> trades) => [.. trades.OrderBy(t => t.Id).Select(Text)];

    [Theory]
    [MemberData(nameof(Formats))]
    public void Rows_ComeBack_WithoutPartitions(string format)
    {
        Deal[] trades = Deals(3 * 16_384 + 123);
        string path = Path.Combine(_root, "flat");
        DatasetWriteResult<DatasetWrittenFile> written = WriteRows(format, path, trades, Options(format) with { MaxRowsPerFile = 20_000 });
        Assert.Equal(3, written.Files.Count);
        Assert.Equal(trades.Length, written.Files.Sum(f => f.Rows));
        Assert.Equal(Texts(trades), Texts(ReadList<Deal>(format, path, null)));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void Rows_ComeBack_ByPartitions(string format)
    {
        Deal[] trades = Deals(40_000);
        string path = Path.Combine(_root, "parts");
        DatasetWriteOptions options = Options(format) with { PartitionBy = [nameof(Deal.Desk), nameof(Deal.Year)] };
        DatasetWriteResult<DatasetWrittenFile> written = WriteRows(format, path, trades, options);
        Assert.Equal(12, written.Files.Count);
        Assert.Contains(written.Files, f => f.Path.Replace('\\', '/').Contains("/Desk=London/Year=2021/"));
        Schema partitions = PartitionSchema<Deal>(format, path, options)!;
        // An enum's partition column is its member names.
        Assert.Equal(["Desk: StringView", "Year: Int32"], partitions.FieldsList.Select(f => $"{f.Name}: {f.DataType.TypeId}"));
        Assert.Equal(Texts(trades), Texts(ReadList<Deal>(format, path, partitions)));
    }

    /// <summary>Keys of every member type a path holds come back through the typed readers.</summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public void Keys_OfEveryType_ComeBack(string format)
    {
        Check<bool>([true, false]);
        Check<sbyte>([-3, 0, 7]);
        Check<short>([-300, 12]);
        Check<int>([int.MinValue, 0, int.MaxValue]);
        Check<long>([long.MinValue, -1, long.MaxValue]);
        Check<byte>([0, 255]);
        Check<ushort>([0, 65535]);
        Check<uint>([0, uint.MaxValue]);
        Check<ulong>([0, ulong.MaxValue]);
        Check<float>([1.5f, -0.25f, float.NaN]);
        Check<double>([0.1, 1e20, -3.25, double.NaN, double.PositiveInfinity]);
        Check<decimal>([1.5m, -0.000000000000000001m, 12345678901234567890.123456789m]);
        Check<Int128>([-(Int128)long.MaxValue * 1000, 0, (Int128)long.MaxValue * 1000]);
        Check<UInt128>([0, (UInt128)ulong.MaxValue * 1000]);
        Check<string?>(["a", "b c", "x/y", "p=q", "50%", "é", null, "", "C:\\x", "[x]"]);
        Check<char>(['a', '/', '=', '%']);
        Check<DateOnly>([new(1970, 1, 1), new(2024, 2, 29), new(1, 1, 1)]);
        Check<TimeOnly>([new(0, 0), new(23, 59, 59, 999, 999), new TimeOnly(12, 30).Add(TimeSpan.FromTicks(7))]);
        Check<DateTime>([new(2024, 1, 2, 3, 4, 5), new DateTime(2024, 1, 2, 3, 4, 5).AddTicks(1230)]);
        Check<DateTimeOffset>([new(2024, 1, 2, 3, 4, 5, TimeSpan.Zero), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(10)]);
        Check<Desk>([Desk.Tokyo, Desk.NewYork]);
        Check<int?>([1, null, 3]);

        void Check<TKey>(TKey[] keys)
        {
            KeyRow<TKey>[] rows = [.. Enumerable.Range(0, 30).Select(i => new KeyRow<TKey>(i, keys[i % keys.Length]))];
            string path = Path.Combine(_root, $"keys-{typeof(TKey).Name}-{Nullable.GetUnderlyingType(typeof(TKey))?.Name}");
            DatasetWriteOptions options = Options(format) with { PartitionBy = ["K"] };
            WriteRows(format, path, rows, options);
            Assert.Equal(keys.Distinct().Count(), Directory.GetDirectories(path).Length);
            Schema partitions = PartitionSchema<KeyRow<TKey>>(format, path, options)!;
            Assert.Equal(rows, ReadList<KeyRow<TKey>>(format, path, partitions).OrderBy(r => r.Id));
        }
    }

    /// <summary>The files depend on the rows alone: an array on several threads, a list, a lazy sequence, rows one by one, asynchronously.</summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task Files_AreTheSame_HoweverTheRowsCome(string format)
    {
        Deal[] trades = Deals(5 * 65_536 + 777);
        List<string> ways = ["array", "array1", "list", "lazy", "rows", "async", "asyncEnumerable"];
        foreach (string way in ways)
        {
            string path = Path.Combine(_root, way);
            DatasetWriteOptions options = Options(format) with
            {
                PartitionBy = [nameof(Deal.Year)],
                FileName = static n => $"f-{n.Index}{n.Extension}",
                MaxDegreeOfParallelism = way == "array1" ? 1 : 4,
            };
            if (format == "parquet")
            {
                await using var writer = await ParquetDatasetWriter<Deal>.CreateAsync(path, (ParquetDatasetWriteOptions)options);
                await Feed(way, writer.WriteRows, writer.WriteRow, writer.WriteRowsAsync, writer.WriteRowsAsync);
                await writer.CompleteAsync();
            }
            else
            {
                await using var writer = await IpcDatasetWriter<Deal>.CreateAsync(path, (IpcDatasetWriteOptions)options);
                await Feed(way, writer.WriteRows, writer.WriteRow, writer.WriteRowsAsync, writer.WriteRowsAsync);
                Assert.Equal(trades.Length, writer.RowCount);
                await writer.CompleteAsync();
            }
        }
        Dictionary<string, byte[]> first = Files(ways[0]);
        Assert.Equal(4, first.Count);
        foreach (string way in ways.Skip(1))
        {
            Dictionary<string, byte[]> files = Files(way);
            Assert.Equal(first.Keys.Order(), files.Keys.Order());
            foreach ((string name, byte[] bytes) in first) Assert.True(bytes.AsSpan().SequenceEqual(files[name]), $"{way}: {name}");
        }

        async Task Feed(string way, Action<IEnumerable<Deal>> rows, Action<Deal> row,
            Func<IEnumerable<Deal>, CancellationToken, ValueTask> rowsAsync, Func<IAsyncEnumerable<Deal>, CancellationToken, ValueTask> asyncRows)
        {
            switch (way)
            {
                case "array" or "array1":
                    rows(trades[..5]); // an open batch first: the array's rows then fill it
                    rows(trades[5..]);
                    break;
                case "list":
                    rows([.. trades]);
                    break;
                case "lazy":
                    rows(trades.Select(static t => t));
                    break;
                case "rows":
                    foreach (Deal t in trades) row(t);
                    break;
                case "async":
                    await rowsAsync(trades, default);
                    break;
                default:
                    await asyncRows(Async(trades), default);
                    break;
            }
        }

        Dictionary<string, byte[]> Files(string way)
        {
            string path = Path.Combine(_root, way);
            return Directory.GetFiles(path, "*", SearchOption.AllDirectories).ToDictionary(f => Path.GetRelativePath(path, f), File.ReadAllBytes);
        }
    }

    private static async IAsyncEnumerable<T> Async<T>(IEnumerable<T> rows)
    {
        foreach (T row in rows)
        {
            yield return row;
            if (row is Deal { Id: 1000 }) await Task.Yield();
        }
    }

    /// <summary>A row the layout refuses gives the write up: the files published are deleted, later calls throw.</summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public void ARefusedRow_GivesTheWriteUp(string format)
    {
        foreach (string way in (string[])["array", "rows"])
        {
            Deal[] trades = Deals(5 * 16_384);
            trades[^100] = trades[^100] with { Price = decimal.MaxValue }; // beyond decimal(38, 18)
            string path = Path.Combine(_root, $"refused-{way}");
            DatasetWriteOptions options = Options(format) with { MaxRowsPerFile = 10_000, PartitionBy = [nameof(Deal.Year)] };
            using IDisposable writer = format == "parquet"
                ? ParquetDatasetWriter<Deal>.Create(path, (ParquetDatasetWriteOptions)options)
                : IpcDatasetWriter<Deal>.Create(path, (IpcDatasetWriteOptions)options);
            Action<IEnumerable<Deal>> write = writer switch
            {
                ParquetDatasetWriter<Deal> p => way == "array" ? p.WriteRows : rows => { foreach (Deal t in rows) p.WriteRow(t); },
                IpcDatasetWriter<Deal> i => way == "array" ? i.WriteRows : rows => { foreach (Deal t in rows) i.WriteRow(t); },
                _ => throw new InvalidOperationException(),
            };
            Assert.ThrowsAny<Exception>(() => write(trades));
            Assert.Empty(Directory.Exists(path) ? Directory.GetFiles(path, "*", SearchOption.AllDirectories) : []);
            Assert.Throws<InvalidOperationException>(() => write(trades[..1]));
            Assert.Throws<InvalidOperationException>(() => _ = writer is ParquetDatasetWriter<Deal> p ? p.Complete().Files.Count : ((IpcDatasetWriter<Deal>)writer).Complete().Files.Count);
        }
    }

    [Fact]
    public void ARefusedRow_ThatLeavesFiles_NamesThem()
    {
        Deal[] trades = Deals(4 * 16_384);
        trades[^1] = trades[^1] with { Price = decimal.MaxValue };
        var store = new UndeletableStore(new LocalFileStore(_root), "part-00000");
        var options = new ParquetDatasetWriteOptions { Store = store, MaxRowsPerFile = 10_000 };
        var left = Assert.Throws<DatasetWriteException>(() => ParquetDatasetWriter.WriteRows("left", trades, options));
        Assert.NotNull(left.InnerException);
        Assert.IsNotType<DatasetWriteException>(left.InnerException);
        Assert.Contains("part-00000", Assert.Single(left.Remaining));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void ANullRow_IsRefusedAlone(string format)
    {
        string path = Path.Combine(_root, "nulls");
        Deal[] trades = Deals(100);
        if (format == "parquet")
        {
            using var writer = ParquetDatasetWriter<Deal>.Create(path, (ParquetDatasetWriteOptions)Options(format));
            writer.WriteRows(trades[..50]);
            Assert.Throws<ArgumentException>(() => writer.WriteRow(null!));
            writer.WriteRows(trades[50..]);
            Assert.Equal(100, writer.Complete().Files.Sum(f => f.Rows));
        }
        else
        {
            using var writer = IpcDatasetWriter<Deal>.Create(path, (IpcDatasetWriteOptions)Options(format));
            writer.WriteRows(trades[..50]);
            Assert.Throws<ArgumentException>(() => writer.WriteRow(null!));
            writer.WriteRows(trades[50..]);
            Assert.Equal(100, writer.Complete().Files.Sum(f => f.Rows));
        }
        Assert.Equal(Texts(trades), Texts(ReadList<Deal>(format, path, null)));
    }

    /// <summary>A cancelled stream of rows stops at its next row: the rows taken before are all written (none lost on the way).</summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task Cancelling_LosesNoRowTaken(string format)
    {
        string path = Path.Combine(_root, "cancelled");
        Deal[] trades = Deals(200_000);
        using var cancel = new CancellationTokenSource();
        async IAsyncEnumerable<Deal> Rows([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
        {
            foreach (Deal t in trades)
            {
                if (t.Id == 150_000) cancel.Cancel();
                token.ThrowIfCancellationRequested();
                yield return t;
                if (t.Id % 20_000 == 0) await Task.Yield();
            }
        }
        DatasetWriteOptions options = Options(format) with { PartitionBy = [nameof(Deal.Year)] };
        long taken, written;
        if (format == "parquet")
        {
            await using var writer = await ParquetDatasetWriter<Deal>.CreateAsync(path, (ParquetDatasetWriteOptions)options);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await writer.WriteRowsAsync(Rows(), cancel.Token));
            taken = writer.RowCount;
            written = (await writer.CompleteAsync()).Files.Sum(f => f.Rows);
        }
        else
        {
            await using var writer = await IpcDatasetWriter<Deal>.CreateAsync(path, (IpcDatasetWriteOptions)options);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await writer.WriteRowsAsync(Rows(), cancel.Token));
            taken = writer.RowCount;
            written = (await writer.CompleteAsync()).Files.Sum(f => f.Rows);
        }
        Assert.InRange(taken, 100_000, 150_001);
        Assert.Equal(taken, written);
        Assert.Equal(Texts(trades.Take((int)taken)), Texts(ReadList<Deal>(format, path, PartitionSchema<Deal>(format, path, options))));

        // An array under a token cancelled already: whatever the call does, every row it took is written.
        string array = Path.Combine(_root, "cancelled-array");
        if (format == "parquet")
        {
            await using var writer = await ParquetDatasetWriter<Deal>.CreateAsync(array, (ParquetDatasetWriteOptions)options);
            try { await writer.WriteRowsAsync(trades, cancel.Token); } catch (OperationCanceledException) { }
            (taken, written) = (writer.RowCount, (await writer.CompleteAsync()).Files.Sum(f => f.Rows));
        }
        else
        {
            await using var writer = await IpcDatasetWriter<Deal>.CreateAsync(array, (IpcDatasetWriteOptions)options);
            try { await writer.WriteRowsAsync(trades, cancel.Token); } catch (OperationCanceledException) { }
            (taken, written) = (writer.RowCount, (await writer.CompleteAsync()).Files.Sum(f => f.Rows));
        }
        Assert.Equal(taken, written);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void DisposeOrAbort_WithoutComplete_LeavesNothing(string format)
    {
        foreach (bool abort in (bool[])[false, true])
        {
            string path = Path.Combine(_root, $"given-up-{abort}");
            DatasetWriteOptions options = Options(format) with { MaxRowsPerFile = 1000 };
            if (format == "parquet")
            {
                using var writer = ParquetDatasetWriter<Deal>.Create(path, (ParquetDatasetWriteOptions)options);
                writer.WriteRows(Deals(40_000));
                if (abort) writer.Abort();
            }
            else
            {
                using var writer = IpcDatasetWriter<Deal>.Create(path, (IpcDatasetWriteOptions)options);
                writer.WriteRows(Deals(40_000));
                if (abort) writer.Abort();
            }
            Assert.Empty(Directory.Exists(path) ? Directory.GetFiles(path, "*", SearchOption.AllDirectories) : []);
        }
    }

    /// <summary>Files cut by size on disk, and files there already refused, kept or replaced, as the record-batch writers do.</summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public void FilesCutBySize_AndExistingData(string format)
    {
        Deal[] trades = Deals(200_000);
        string sized = Path.Combine(_root, "sized");
        DatasetWriteResult<DatasetWrittenFile> cut = WriteRows(format, sized, trades, Options(format) with { ApproxBytesPerFile = 2L << 20 });
        Assert.True(cut.Files.Count >= 3, $"{cut.Files.Count} files");
        Assert.All(cut.Files.SkipLast(1), f => Assert.InRange(f.Bytes, 1L << 20, 3L << 20));
        Assert.Equal(Texts(trades), Texts(ReadList<Deal>(format, sized, null)));

        string path = Path.Combine(_root, "existing");
        DatasetWriteOptions options = Options(format) with { PartitionBy = [nameof(Deal.Year)] };
        Deal[] first = trades[..1000], second = [.. trades[1000..2000].Where(t => t.Year == 2021)];
        WriteRows(format, path, first, options);
        Assert.Throws<IOException>(() => WriteRows(format, path, second, options)); // ExistingData.Error
        Schema partitions = PartitionSchema<Deal>(format, path, options)!;
        WriteRows(format, path, second, options with { ExistingData = ExistingData.Append });
        Assert.Equal(Texts([.. first, .. second]), Texts(ReadList<Deal>(format, path, partitions)));
        // Replacing: the partition written (2021) holds the new rows alone, the others stay.
        WriteRows(format, path, second, options with { ExistingData = ExistingData.ReplacePartitions });
        Assert.Equal(Texts([.. first.Where(t => t.Year != 2021), .. second]), Texts(ReadList<Deal>(format, path, partitions)));
    }

    [Fact]
    public void PartitionColumns_AreChecked()
    {
        string path = Path.Combine(_root, "bad");
        Assert.Throws<ArgumentException>(() => ParquetDatasetWriter<Ticketed>.Create(path, new ParquetDatasetWriteOptions { PartitionBy = ["Missing"] }));
        // A Guid is bytes to the layout: a path cannot hold it.
        Assert.Throws<ArgumentException>(() => ParquetDatasetWriter<Ticketed>.Create(path, new ParquetDatasetWriteOptions { PartitionBy = [nameof(Ticketed.Key)] }));
        Assert.Throws<ArgumentException>(() => IpcDatasetWriter<Ticketed>.Create(path, new IpcDatasetWriteOptions { PartitionBy = [nameof(Ticketed.Lots)] }));
        Assert.False(Directory.Exists(path));
        using var unpartitioned = ParquetDatasetWriter<Ticketed>.Create(path);
        Assert.Null(unpartitioned.PartitionSchema);
    }

    [Fact]
    public void Dictionaries_AreParquetMaps()
    {
        string path = Path.Combine(_root, "maps");
        DatasetWriteResult<ParquetWrittenFile> written = ParquetDatasetWriter.WriteRows(path, Deals(100), new ParquetDatasetWriteOptions());
        using ParquetFile file = ParquetFile.Open(Assert.Single(written.Files).Path);
        Assert.Equal(["Tags"], file.Metadata.Thrift!.Schema.Where(e => e.LogicalType?.Kind == LogicalTypeKind.Map).Select(e => e.Name));
    }

    /// <summary>A local store whose deletes of some paths fail.</summary>
    private sealed class UndeletableStore(IWritableFileStore inner, string undeletable) : IWritableFileStore
    {
        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) => inner.OpenAsync(file, cancellationToken);

        public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) => inner.OpenAsync(path, cancellationToken);

        public FileWriteStream OpenWrite(string path, bool createOnly) => inner.OpenWrite(path, createOnly);

        public ValueTask<bool> DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            path.Contains(undeletable) ? throw new IOException("permission denied") : inner.DeleteAsync(path, cancellationToken);
    }
}

public enum Desk { Tokyo, London, NewYork }

public sealed record Deal(long Id, string? Symbol, Desk Desk, int Year, decimal Price, DateOnly Day, TimeOnly At, DateTime When, double X,
    Dictionary<string, int>? Tags, List<int>? Lots);

public sealed record KeyRow<TKey>(int Id, TKey K);

public sealed record Ticketed(int Id, Guid Key, List<int> Lots);
