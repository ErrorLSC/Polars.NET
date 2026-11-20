using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Parquet;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests;

/// <summary>
/// Hive-partitioned dataset writes: values rendered as Polars renders them (its outputs, probed with Polars 2.1, are the
/// expectations), escaped as Hive does, an empty string and a null apart; rows back from the directories; partition
/// columns out of the files unless asked; partitions replaced alone; IPC runs gathered into record batches.
/// </summary>
public sealed class DatasetPartitionTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-dataset-partitions-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    public static TheoryData<string> Formats => ["parquet", "ipc", "ipcstream"];

    /// <summary>Floats as Polars 2.1 casts them to strings (-0.0 aside: one partition with 0.0, rendered so).</summary>
    [Theory]
    [InlineData(0.1, "0.1")]
    [InlineData(1.5, "1.5")]
    [InlineData(-0.0, "0.0")]
    [InlineData(double.NaN, "NaN")]
    [InlineData(double.PositiveInfinity, "inf")]
    [InlineData(double.NegativeInfinity, "-inf")]
    [InlineData(1e20, "1e+20")]
    [InlineData(1e-7, "1e-7")]
    [InlineData(-1e-7, "-1e-7")]
    [InlineData(2.5e-5, "0.000025")]
    [InlineData(1e-5, "0.00001")]
    [InlineData(9.99e-5, "0.0000999")]
    [InlineData(1.5e-6, "1.5e-6")]
    [InlineData(1e-6, "1e-6")]
    [InlineData(123456789.0, "123456789.0")]
    [InlineData(1e15, "1000000000000000.0")]
    [InlineData(9.9e15, "9900000000000000.0")]
    [InlineData(9999999999999998.0, "9999999999999998.0")]
    [InlineData(1e16, "1e+16")]
    [InlineData(-1e16, "-1e+16")]
    [InlineData(1.2345e16, "1.2345e+16")]
    [InlineData(123456789012345.6, "123456789012345.6")]
    [InlineData(1.0 / 3, "0.3333333333333333")]
    [InlineData(5e-324, "5e-324")]
    [InlineData(double.MaxValue, "1.7976931348623157e+308")]
    [InlineData(-123.25, "-123.25")]
    [InlineData(12345678.9, "12345678.9")]
    [InlineData(100.0, "100.0")]
    [InlineData(0.001, "0.001")]
    public void Doubles_RenderAsPolarsCastsThem(double value, string expected) => Assert.Equal(expected, HivePaths.Float(value, single: false));

    [Theory]
    [InlineData(0.1f, "0.1")]
    [InlineData(1.5f, "1.5")]
    [InlineData(16777216f, "16777216.0")]
    [InlineData(1e20f, "1e+20")]
    [InlineData(1e-5f, "0.00001")]
    [InlineData(1e16f, "1e+16")]
    [InlineData(1e7f, "10000000.0")]
    [InlineData(float.MaxValue, "3.4028235e+38")]
    [InlineData(1e-40f, "1e-40")]
    public void Floats_RenderAsPolarsCastsThem(float value, string expected) => Assert.Equal(expected, HivePaths.Float(value, single: true));

    private static IArrowArray Timestamps(TimeUnit unit, string? zone, params long[] values)
    {
        var data = new ArrayData(new TimestampType(unit, zone), values.Length, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()).ToArray())]);
        return ArrowArrayFactory.BuildArray(data);
    }

    /// <summary>Values of every type a path holds, as Polars renders them (times keep their fraction: Polars drops it).</summary>
    [Fact]
    public void Values_RenderAsPolarsCastsThem()
    {
        long at = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero).ToUnixTimeSeconds();
        (IArrowArray Array, string?[] Expected)[] cases =
        [
            (new Int8Array.Builder().Append(-5).Append(0).Append(127).Build(), ["-5", "0", "127"]),
            (new Int64Array.Builder().Append(123456789012).AppendNull().Build(), ["123456789012", null]),
            (new UInt64Array.Builder().Append(ulong.MaxValue).Build(), ["18446744073709551615"]),
            (new BooleanArray.Builder().Append(true).Append(false).Build(), ["true", "false"]),
            (new Date32Array.Builder().Append(new DateTime(2024, 1, 2)).Append(new DateTime(1900, 1, 1)).Append(new DateTime(1, 1, 1)).Build(),
                ["2024-01-02", "1900-01-01", "0001-01-01"]),
            (Timestamps(TimeUnit.Millisecond, null, at * 1000, at * 1000 + 123), ["2024-01-02 03:04:05.000", "2024-01-02 03:04:05.123"]),
            (Timestamps(TimeUnit.Microsecond, null, at * 1_000_000, at * 1_000_000 + 6, (at - 11045) * 1_000_000),
                ["2024-01-02 03:04:05.000000", "2024-01-02 03:04:05.000006", "2024-01-02 00:00:00.000000"]),
            (Timestamps(TimeUnit.Nanosecond, null, at * 1_000_000_000 + 6000), ["2024-01-02 03:04:05.000006000"]),
            (Timestamps(TimeUnit.Microsecond, "UTC", at * 1_000_000), ["2024-01-02 03:04:05.000000+00:00"]),
            (Timestamps(TimeUnit.Microsecond, "Asia/Tokyo", (at - 9 * 3600) * 1_000_000), ["2024-01-02 03:04:05.000000+09:00"]),
            (new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond)).Append(0).Append(11045_000_000).Append(11045_123_456).Build(),
                ["00:00:00", "03:04:05", "03:04:05.123456"]),
            (new Decimal128Array.Builder(new Decimal128Type(10, 2)).Append(1.50m).Append(-0.05m).Append(0.00m).Build(), ["1.50", "-0.05", "0.00"]),
            (new Decimal128Array.Builder(new Decimal128Type(10, 0)).Append(12m).Append(-3m).Build(), ["12", "-3"]),
            (new StringArray.Builder().Append("a").Append("").AppendNull().Append("a/b").Build(), ["a", "", null, "a/b"]),
            (new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false), new Int32Array.Builder().Append(1).Append(0).AppendNull().Build(),
                new StringArray.Builder().Append("x").Append("y").Build()), ["y", "x", null]),
        ];
        foreach (var (array, expected) in cases)
        {
            Assert.True(HivePaths.Renders(array.Data.DataType), array.Data.DataType.Name);
            Assert.Equal(expected, Enumerable.Range(0, array.Length).Select(i => HivePaths.Render(array, i)));
        }
        Assert.False(HivePaths.Renders(BinaryType.Default));
        Assert.False(HivePaths.Renders(DurationType.Microsecond));
        Assert.False(HivePaths.Renders(new ListType(Int32Type.Default)));
    }

    [Fact]
    public void Directories_AreEscapedAsHiveEscapesThem()
    {
        Assert.Equal("k=a%2Fb/", HivePaths.Directory(["k"], ["a/b"]));
        Assert.Equal("k=/", HivePaths.Directory(["k"], [""]));
        Assert.Equal("k=__HIVE_DEFAULT_PARTITION__/", HivePaths.Directory(["k"], [null]));
        Assert.Equal("a%3Db=x y é%25%3A%5C%3F%23%27%2A%22%7B%5B%5D%5E%09/n=1/", HivePaths.Directory(["a=b", "n"], ["x y é%:\\?#'*\"{[]^\t", "1"]));
    }

    private static readonly Schema TheSchema = new(
    [
        new Field("id", Int64Type.Default, false),
        new Field("k", StringType.Default, true),
        new Field("n", Int32Type.Default, true),
        new Field("d", Date32Type.Default, true),
    ], null);

    private static readonly string?[] Keys = ["a", "b c", "x/y", "p=q", "50%", "é", null, "", "C:\\x", "q?#"];

    private static RecordBatch Batch(long first, int rows) => new(TheSchema,
    [
        new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build(),
        new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => Keys[(first + i) % Keys.Length])).Build(),
        N(Enumerable.Range(0, rows).Select(i => (first + i) % 3 == 2 ? (int?)null : (int)((first + i) % 3))),
        new Date32Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => new DateTime(2024, 1, 1).AddDays((first + i) % 5))).Build(),
    ], rows);

    private static Int32Array N(IEnumerable<int?> values)
    {
        var builder = new Int32Array.Builder();
        foreach (int? value in values)
        {
            if (value is int v) builder.Append(v);
            else builder.AppendNull();
        }
        return builder.Build();
    }

    private static List<RecordBatch> Batches(params int[] sizes)
    {
        List<RecordBatch> batches = [];
        long first = 0;
        foreach (int size in sizes)
        {
            batches.Add(Batch(first, size));
            first += size;
        }
        return batches;
    }

    private static DatasetWriteOptions Options(string format) => format switch
    {
        "parquet" => new ParquetDatasetWriteOptions(),
        "ipc" => new IpcDatasetWriteOptions(),
        _ => new IpcDatasetWriteOptions { Writer = new IpcWriteOptions { Format = IpcFormat.Stream } },
    };

    private static List<DatasetWrittenFile> Write(string path, IEnumerable<RecordBatch> batches, DatasetWriteOptions options) => options is ParquetDatasetWriteOptions parquet
        ? [.. ParquetDatasetWriter.Write(path, TheSchema, batches, parquet).Files]
        : [.. IpcDatasetWriter.Write(path, TheSchema, batches, (IpcDatasetWriteOptions)options).Files];

    /// <summary>Every row (columns by name, in <paramref name="columns"/>' order) of the dataset read back with the partition schema given, sorted.</summary>
    private static List<string> Read(string format, string path, Schema partitions, string[] columns)
    {
        List<string> rows = [];
        IEnumerable<RecordBatch> batches;
        IDisposable dataset, reader;
        if (format == "parquet")
        {
            var parquet = ParquetDataset.Open(path, new ParquetDatasetOptions { PartitionSchema = partitions });
            var r = parquet.OpenReader();
            (dataset, reader, batches) = (parquet, r, r.ReadBatches());
        }
        else
        {
            var ipc = IpcDataset.Open(path, new IpcDatasetOptions { PartitionSchema = partitions });
            var r = ipc.OpenReader();
            (dataset, reader, batches) = (ipc, r, r.ReadBatches());
        }
        using (dataset)
        using (reader)
        {
            foreach (RecordBatch batch in batches)
            {
                int[] at = [.. columns.Select(c => batch.Schema.GetFieldIndex(c))];
                rows.AddRange(Enumerable.Range(0, batch.Length).Select(r => string.Join("|", at.Select(c => CanonicalRender.Value(batch.Column(c), r)))));
            }
        }
        return [.. rows.Order(StringComparer.Ordinal)];
    }

    private static List<string> Expected(IEnumerable<RecordBatch> batches, string[] columns)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
        {
            int[] at = [.. columns.Select(c => batch.Schema.GetFieldIndex(c))];
            rows.AddRange(Enumerable.Range(0, batch.Length).Select(r => string.Join("|", at.Select(c => CanonicalRender.Value(batch.Column(c), r)))));
        }
        return [.. rows.Order(StringComparer.Ordinal)];
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void Partitions_ComeBackFromTheirDirectories(string format)
    {
        List<RecordBatch> batches = Batches(997, 3, 1500);
        string path = Path.Combine(_root, "p");
        List<DatasetWrittenFile> files = Write(path, batches, Options(format) with { PartitionBy = ["k", "n"], MaxRowsPerFile = 40 });
        // An empty string and a null are partitions apart; every key is one directory level, in order.
        string[] directories = [.. files.Select(f => Path.GetRelativePath(path, Path.GetDirectoryName(f.Path)!).Replace('\\', '/')).Distinct().Order(StringComparer.Ordinal)];
        Assert.Contains("k=/n=0", directories);
        Assert.Contains("k=__HIVE_DEFAULT_PARTITION__/n=__HIVE_DEFAULT_PARTITION__", directories);
        Assert.Contains("k=x%2Fy/n=1", directories);
        Assert.Contains("k=C%3A%5Cx/n=1", directories);
        Assert.Equal(Keys.Length * 3, directories.Length);
        Assert.All(files, f => Assert.Equal(f.PartitionValues["k"] is { } k ? HivePaths.Escape(k) : HivePaths.NullValue,
            Path.GetFileName(Path.GetDirectoryName(Path.GetDirectoryName(f.Path)!))![2..]));
        Assert.All(files, f => Assert.InRange(f.Rows, 1, 40));
        // The partition columns are in the paths, not in the files.
        var partitions = new Schema([TheSchema.GetFieldByIndex(1), TheSchema.GetFieldByIndex(2)], null);
        string[] columns = ["id", "k", "n", "d"];
        Assert.Equal(Expected(batches, columns), Read(format, path, partitions, columns));
        if (format == "parquet")
        {
            using var file = ParquetFile.Open(files[0].Path);
            Assert.Equal(["id", "d"], file.Schema.FieldsList.Select(f => f.Name));
        }
        else
        {
            using IpcFile file = IpcFile.Open(files[0].Path);
            Assert.Equal(["id", "d"], file.Schema.FieldsList.Select(f => f.Name));
        }
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void PartitionColumns_GoIntoTheFiles_WhenAsked(string format)
    {
        string path = Path.Combine(_root, "inc");
        List<DatasetWrittenFile> files = Write(path, Batches(50), Options(format) with { PartitionBy = ["d"], IncludePartitionColumns = true });
        Assert.Equal(5, files.Count);
        Assert.All(files, f => Assert.StartsWith("d=2024-01-0", Path.GetFileName(Path.GetDirectoryName(f.Path))));
        using IDisposable file = format == "parquet" ? ParquetFile.Open(files[0].Path) : IpcFile.Open(files[0].Path);
        Schema schema = file is ParquetFile p ? p.Schema : ((IpcFile)file).Schema;
        Assert.Equal(["id", "k", "n", "d"], schema.FieldsList.Select(f => f.Name));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void ReplacingPartitions_LeavesThePartitionsNotWritten(string format)
    {
        string path = Path.Combine(_root, "r");
        Write(path, Batches(300), Options(format) with { PartitionBy = ["n"], MaxRowsPerFile = 30 });
        File.WriteAllText(Path.Combine(path, "n=0", "_old.txt"), "old");
        // New rows of partition n=1 only: n=0 and the null partition stay; n=1's old files go.
        var fresh = new RecordBatch(TheSchema, [.. Enumerable.Range(0, 4).Select(c => Batch(1000, 6).Column(c))], 6);
        List<RecordBatch> ones = [Filter(fresh, r => r % 3 == 1)];
        Write(path, ones, Options(format) with { PartitionBy = ["n"], ExistingData = ExistingData.ReplacePartitions });
        Assert.Single(Directory.GetFiles(Path.Combine(path, "n=1")));
        Assert.Equal(5, Directory.GetFiles(Path.Combine(path, "n=0")).Length); // four data files and the note
        File.Delete(Path.Combine(path, "n=0", "_old.txt")); // a directory read alone holds files of one extension
        var partitions = new Schema([TheSchema.GetFieldByIndex(2)], null);
        List<string> rows = Read(format, path, partitions, ["id", "n"]);
        Assert.Equal(200 + 2, rows.Count);
        Assert.Contains("1000|1", rows);
        Assert.Contains("1003|1", rows);
        Assert.DoesNotContain("1|1", rows);
    }

    private static RecordBatch Filter(RecordBatch batch, Func<long, bool> keep)
    {
        int[] rows = [.. Enumerable.Range(0, batch.Length).Where(r => keep(((Int64Array)batch.Column(0)).GetValue(r)!.Value))];
        return Kaminari.Sazanami.Arrow.Compute.ArrowTake.Take(batch, rows);
    }

    [Theory]
    [InlineData("ipc")]
    [InlineData("ipcstream")]
    public void IpcRuns_AreGatheredIntoRecordBatches(string format)
    {
        // A hundred batches of 30 rows over three partitions: each partition's 10-row runs gathered into one batch.
        List<RecordBatch> batches = [.. Enumerable.Range(0, 100).Select(b => Batch(b * 30, 30))];
        string path = Path.Combine(_root, "g");
        List<DatasetWrittenFile> files = Write(path, batches, Options(format) with { PartitionBy = ["n"] });
        Assert.Equal(3, files.Count);
        foreach (DatasetWrittenFile written in files)
        {
            using IpcFile file = IpcFile.Open(written.Path);
            Assert.Equal(1, file.BatchCount);
        }
        // A batch of enough rows goes as it is: without partitions, one big batch and the small ones gathered after it.
        string whole = Path.Combine(_root, "w");
        List<DatasetWrittenFile> one = Write(whole, [Batch(0, IpcFileFormat.BatchRows + 5), Batch(0, 7), Batch(0, 8)], Options(format));
        using IpcFile big = IpcFile.Open(Assert.Single(one).Path);
        Assert.Equal(2, big.BatchCount);
        Assert.Equal(Expected(batches, ["id", "k", "n", "d"]), Read(format, path, new Schema([TheSchema.GetFieldByIndex(2)], null), ["id", "k", "n", "d"]));
    }

    /// <summary>Keys of every type a path holds come back through the dataset readers, typed by the partition schema.</summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public void TypedKeys_ComeBackThroughTheReaders(string format)
    {
        const int n = 40;
        string?[] strings = ["a", "b c", "x/y", "p=q", "50%", "é", null, "", "C:\\x", "q?#", "it's", "[x]", "a^b", "tab\tx", "col:on", "st*r", "quo\"te", "{y}"];
        double[] doubles = [0.1, 1.5, double.NaN, 1e20, 1e-7, 2.5e-5, 123456789.0, double.PositiveInfinity, -3.25, 1e16, -0.0];
        long at = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero).ToUnixTimeSeconds();
        long[] instants = [.. Enumerable.Range(0, n).Select(i => (at + i % 4 * 3601) * 1_000_000 + i % 2 * 7)];
        IArrowArray[] keys =
        [
            new StringArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => strings[i % strings.Length])).Build(),
            new Int64Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => (long)(i % 5) * 123456789012 - 7)).Build(),
            new Int16Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => (short)(i % 7 - 3))).Build(),
            new DoubleArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => doubles[i % doubles.Length])).Build(),
            new BooleanArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => i % 3 == 0)).Build(),
            new Date32Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => new DateTime(2024, 1, 1).AddDays(i % 4 * 400))).Build(),
            new Decimal128Array.Builder(new Decimal128Type(10, 2)).AppendRange(Enumerable.Range(0, n).Select(i => (i % 4 - 2) * 1.25m)).Build(),
            Timestamps(TimeUnit.Microsecond, null, instants),
            Timestamps(TimeUnit.Microsecond, "UTC", instants),
            Timestamps(TimeUnit.Microsecond, "Asia/Tokyo", instants),
            new Time64Array.Builder(new Time64Type(TimeUnit.Nanosecond)).AppendRange(Enumerable.Range(0, n).Select(i => i % 4 * 3_600_000_000_123L + i % 3)).Build(),
            new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond)).AppendRange(Enumerable.Range(0, n).Select(i => i % 5 * 86_399_999_999L / 4)).Build(),
            new Time32Array.Builder(new Time32Type(TimeUnit.Millisecond)).AppendRange(Enumerable.Range(0, n).Select(i => i % 2 * 45_296_789)).Build(),
            new Time32Array.Builder(new Time32Type(TimeUnit.Second)).AppendRange(Enumerable.Range(0, n).Select(i => i % 2 * 86_399)).Build(),
        ];
        foreach (IArrowArray key in keys)
        {
            var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("k", key.Data.DataType, true)], null);
            var batch = new RecordBatch(schema, [new Int64Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => (long)i)).Build(), key], n);
            string path = Path.Combine(_root, $"typed-{format}-{System.Array.IndexOf(keys, key)}");
            if (format == "parquet") ParquetDatasetWriter.Write(path, schema, [batch], new ParquetDatasetWriteOptions { PartitionBy = ["k"] });
            else IpcDatasetWriter.Write(path, schema, [batch], (IpcDatasetWriteOptions)(Options(format) with { PartitionBy = ["k"] }));
            // -0.0 is one partition with 0.0: it comes back as 0.0.
            List<string> expected = [.. Enumerable.Range(0, n).Select(i => $"{i}|{(key is DoubleArray d && d.GetValue(i) == 0 ? CanonicalRender.Value(new DoubleArray.Builder().Append(0.0).Build(), 0) : CanonicalRender.Value(key, i))}").Order(StringComparer.Ordinal)];
            Assert.Equal(expected, Read(format, path, new Schema([schema.GetFieldByIndex(1)], null), ["id", "k"]));
        }
    }

    [Fact]
    public void PartitionColumns_AreChecked()
    {
        string path = Path.Combine(_root, "bad");
        Assert.Throws<ArgumentException>(() => Write(path, Batches(5), new ParquetDatasetWriteOptions { PartitionBy = ["missing"] }));
        Assert.Throws<ArgumentException>(() => Write(path, Batches(5), new ParquetDatasetWriteOptions { PartitionBy = ["k", "k"] }));
        var binary = new Schema([new Field("b", BinaryType.Default, true), new Field("v", Int32Type.Default, true)], null);
        Assert.Throws<ArgumentException>(() => ParquetDatasetWriter.Create(path, binary, new ParquetDatasetWriteOptions { PartitionBy = ["b"] }));
        var single = new Schema([new Field("k", StringType.Default, true)], null);
        Assert.Throws<ArgumentException>(() => IpcDatasetWriter.Create(path, single, new IpcDatasetWriteOptions { PartitionBy = ["k"] }));
        Assert.False(Directory.Exists(path));
    }

    [Fact]
    public void ManyPartitions_UnderATinyBudget_StillComeBack()
    {
        // 997 partitions (the id itself), a budget that holds next to nothing: row groups end and files close early.
        List<RecordBatch> batches = Batches(997, 997, 997);
        string path = Path.Combine(_root, "many");
        var options = new ParquetDatasetWriteOptions { PartitionBy = ["id"], MemoryBudget = 1 << 20, IncludePartitionColumns = true };
        List<DatasetWrittenFile> files = [.. ParquetDatasetWriter.Write(path, TheSchema, batches.Select(b => Filter(b, id => id < 997 || true)), options).Files];
        Assert.True(files.Count >= 2991, $"{files.Count} files");
        Assert.Equal(Expected(batches, ["id", "k", "n", "d"]), Read("parquet", path, new Schema([TheSchema.GetFieldByIndex(0)], null), ["id", "k", "n", "d"]));
    }
}
