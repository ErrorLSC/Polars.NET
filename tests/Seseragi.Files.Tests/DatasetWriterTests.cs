using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Parquet;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests;

/// <summary>
/// <see cref="ParquetDatasetWriter"/> and <see cref="IpcDatasetWriter"/> without partitions: rows back in order from
/// files cut by rows and size, files there already refused, kept or replaced (after the new ones are published),
/// failed writes undone (what cannot be deleted listed), names, options the dataset decides.
/// </summary>
public sealed class DatasetWriterTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-dataset-writer-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    public static TheoryData<string> Formats => ["parquet", "ipc", "ipcstream"];

    private static readonly Schema TheSchema = new(
        [new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, true), new Field("x", DoubleType.Default, true)], null);

    private static RecordBatch Batch(long first, int rows) => new(TheSchema,
    [
        new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build(),
        new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => (first + i) % 7 == 0 ? null : $"name-{first + i}")).Build(),
        new DoubleArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => (first + i) * 0.5)).Build(),
    ], rows);

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

    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
            rows.AddRange(Enumerable.Range(0, batch.Length).Select(r => string.Join("|", Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), r)))));
        return rows;
    }

    private static DatasetWriteOptions Options(string format) => format switch
    {
        "parquet" => new ParquetDatasetWriteOptions(),
        "ipc" => new IpcDatasetWriteOptions(),
        _ => new IpcDatasetWriteOptions { Writer = new IpcWriteOptions { Format = IpcFormat.Stream } },
    };

    private static string Extension(string format) => format switch { "parquet" => ".parquet", "ipc" => ".arrow", _ => ".arrows" };

    /// <summary>A write's files (path, rows, bytes) and id, through either format's writer.</summary>
    private static (string WriteId, List<DatasetWrittenFile> Files) Write(string format, string path, IEnumerable<RecordBatch> batches, DatasetWriteOptions options)
    {
        if (options is ParquetDatasetWriteOptions parquet)
        {
            DatasetWriteResult<ParquetWrittenFile> result = ParquetDatasetWriter.Write(path, TheSchema, batches, parquet);
            return (result.WriteId, [.. result.Files]);
        }
        DatasetWriteResult<DatasetWrittenFile> ipc = IpcDatasetWriter.Write(path, TheSchema, batches, (IpcDatasetWriteOptions)options);
        return (ipc.WriteId, [.. ipc.Files]);
    }

    /// <summary>The dataset read back (files in path order).</summary>
    private static List<string> Read(string format, string path, IFileStore? store = null)
    {
        if (format == "parquet")
        {
            using ParquetDataset dataset = ParquetDataset.Open(path, new ParquetDatasetOptions { Store = store });
            using ParquetDatasetReader reader = dataset.OpenReader();
            return Rows(reader.ReadBatches());
        }
        using IpcDataset ipc = IpcDataset.Open(path, new IpcDatasetOptions { Store = store });
        using IpcDatasetReader ipcReader = ipc.OpenReader();
        return Rows(ipcReader.ReadBatches());
    }

    private List<string> FilesOnDisk() =>
        [.. Directory.GetFiles(_root, "*", SearchOption.AllDirectories).Select(f => Path.GetRelativePath(_root, f).Replace('\\', '/')).Order(StringComparer.Ordinal)];

    [Theory]
    [MemberData(nameof(Formats))]
    public void Rows_ComeBackInOrder_FromFilesCutByRows(string format)
    {
        List<RecordBatch> batches = Batches(2500, 1, 4000, 0, 1999);
        string path = Path.Combine(_root, "out");
        var (id, files) = Write(format, path, batches, Options(format) with { MaxRowsPerFile = 1000 });
        // 8500 rows: eight full files, the 2500-row batch across three of them, then 500.
        Assert.Equal([1000L, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 500], files.Select(f => f.Rows));
        Assert.Equal(Enumerable.Range(0, 9).Select(i => $"{path}/part-{i:D5}-{id}{Extension(format)}"), files.Select(f => f.Path));
        Assert.All(files, f => Assert.Equal(new FileInfo(f.Path).Length, f.Bytes));
        Assert.Equal(Rows(batches), Read(format, path));
        Assert.Equal(files.Count, FilesOnDisk().Count); // no temporary file left
        if (format == "parquet")
        {
            ParquetDatasetWriter.Write(Path.Combine(_root, "meta"), TheSchema, batches, new ParquetDatasetWriteOptions { MaxRowsPerFile = 5000 })
                .Files.ToList().ForEach(f => Assert.Equal(f.Rows, f.Metadata.RowCount));
        }
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void FilesCutBySize_StayNearTheTarget(string format)
    {
        List<RecordBatch> batches = Batches(30_000, 30_000, 30_000);
        string path = Path.Combine(_root, "sized");
        const long target = 256 << 10;
        // Parquet measures its open row group as plainly encoded: small row groups keep the measure near the file's.
        DatasetWriteOptions options = format == "parquet" ? new ParquetDatasetWriteOptions { Writer = new ParquetWriteOptions { RowGroupRows = 2048 } } : Options(format);
        var (_, files) = Write(format, path, batches, options with { ApproxBytesPerFile = target });
        Assert.True(files.Count >= 4, $"{files.Count} files");
        Assert.All(files.SkipLast(1), f => Assert.InRange(f.Bytes, target / 2, target * 5 / 4));
        Assert.Equal(90_000, files.Sum(f => f.Rows));
        Assert.Equal(Rows(batches), Read(format, path));
    }

    /// <summary>
    /// A file written alone takes its rows as batches of its own (row groups encoded side by side): the caller's batches
    /// (native memory here) disposed as soon as each write returns still come back whole.
    /// </summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public void BatchesDisposedAfterEachWrite_ComeBackWhole(string format)
    {
        string path = Path.Combine(_root, "owned");
        List<string> expected = [];
        DatasetWriteOptions options = format == "parquet"
            ? new ParquetDatasetWriteOptions { Writer = new ParquetWriteOptions { RowGroupRows = 1000 } }
            : Options(format);
        void Each(Action<RecordBatch> write)
        {
            for (int b = 0; b < 20; b++)
            {
                using RecordBatch managed = Batch(b * 3000L, 3000);
                // A copy in native memory, freed when disposed (not left to the collector).
                RecordBatch native = Kaminari.Sazanami.Arrow.Compute.ArrowTake.Take(managed, [.. Enumerable.Range(0, 3000)]);
                expected.AddRange(Rows([native]));
                write(native);
                native.Dispose();
            }
        }
        if (options is ParquetDatasetWriteOptions parquet)
        {
            using ParquetDatasetWriter writer = ParquetDatasetWriter.Create(path, TheSchema, parquet);
            Each(writer.Write);
            writer.Complete();
        }
        else
        {
            using IpcDatasetWriter writer = IpcDatasetWriter.Create(path, TheSchema, (IpcDatasetWriteOptions)options);
            Each(writer.Write);
            writer.Complete();
        }
        Assert.Equal(expected, Read(format, path));
    }

    [Fact]
    public void NoRows_NoFile()
    {
        DatasetWriteResult<ParquetWrittenFile> result = ParquetDatasetWriter.Write(Path.Combine(_root, "empty"), TheSchema, Batches(0, 0));
        Assert.Empty(result.Files);
        Assert.Empty(FilesOnDisk());
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void FilesThere_FailTheWrite_ByDefault(string format)
    {
        string path = Path.Combine(_root, "out", "sales");
        Directory.CreateDirectory(Path.Combine(_root, "out", "sales2"));
        File.WriteAllText(Path.Combine(_root, "out", "sales2", "x.txt"), "a sibling prefix is not the dataset's");
        Write(format, path, Batches(10), Options(format)); // the directory did not exist
        var error = Assert.Throws<IOException>(() => Write(format, path, Batches(10), Options(format)));
        Assert.Contains("holds files already", error.Message);
        Assert.Equal(2, FilesOnDisk().Count); // the file written and the sibling's: nothing more

        var unlisted = new UnlistedStore(new LocalFileStore(_root));
        Assert.Throws<NotSupportedException>(() => Write(format, "unlisted", Batches(10), Options(format) with { Store = unlisted }));
        Write(format, "unlisted", Batches(10), Options(format) with { Store = unlisted, ExistingData = ExistingData.Append });
        Assert.Throws<NotSupportedException>(() => Write(format, "unlisted", Batches(10), Options(format) with { Store = unlisted, ExistingData = ExistingData.ReplacePartitions }));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void Append_AddsFilesBeside_ThoseThere(string format)
    {
        string path = Path.Combine(_root, "log");
        var (first, _) = Write(format, path, Batches(100), Options(format));
        var (second, files) = Write(format, path, [Batch(100, 50)], Options(format) with { ExistingData = ExistingData.Append });
        Assert.NotEqual(first, second);
        Assert.Single(files);
        Assert.Equal(Rows(Batches(150)).Order(StringComparer.Ordinal), Read(format, path).Order(StringComparer.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void Replacing_DeletesTheOldFiles_OnceTheNewArePublished(string format)
    {
        string path = Path.Combine(_root, "t");
        Write(format, path, Batches(300), Options(format) with { MaxRowsPerFile = 100 });
        Directory.CreateDirectory(Path.Combine(path, "sub"));
        File.WriteAllText(Path.Combine(path, "sub", "_meta.txt"), "old");

        // A write that fails leaves the old files.
        var faults = new FaultStore(new LocalFileStore()) { FailWrite = (p, at) => p.Contains("part-00001") && at > 0 };
        Assert.Throws<IOException>(() => Write(format, path, Batches(500), Options(format) with
        {
            Store = faults, ExistingData = ExistingData.ReplacePartitions, MaxRowsPerFile = 100,
        }));
        Assert.Equal(4, Directory.GetFiles(path, "*", SearchOption.AllDirectories).Length);
        Assert.Equal(Rows(Batches(300)), Read(format, path, new FilteredStore(new LocalFileStore(), Extension(format))));

        var (_, files) = Write(format, path, [Batch(1000, 20)], Options(format) with { ExistingData = ExistingData.ReplacePartitions });
        Assert.Equal(files.Select(f => f.Path.Replace('\\', '/')), Directory.GetFiles(path, "*", SearchOption.AllDirectories).Select(f => f.Replace('\\', '/')));
        Assert.Equal(Rows([Batch(1000, 20)]), Read(format, path));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void AFailedWrite_LeavesNothing(string format)
    {
        // The third file's stream fails midway: the two published are deleted, the open one given up.
        var faults = new FaultStore(new LocalFileStore(_root)) { FailWrite = (p, at) => p.Contains("part-00002") && at > 100 };
        var error = Assert.Throws<IOException>(() => Write(format, "f", Batches(250, 250), Options(format) with { Store = faults, MaxRowsPerFile = 200 }));
        Assert.Equal("disk full", error.Message);
        Assert.Empty(FilesOnDisk()); // hidden temporary files too
        Assert.Equal(2, faults.Deleted.Count);

        // A published file that cannot be deleted is named.
        var stubborn = new FaultStore(new LocalFileStore(_root))
        {
            FailWrite = (p, at) => p.Contains("part-00002") && at > 100,
            FailDelete = p => p.Contains("part-00000"),
        };
        var left = Assert.Throws<DatasetWriteException>(() => Write(format, "g", Batches(500), Options(format) with { Store = stubborn, MaxRowsPerFile = 200 }));
        Assert.Equal("disk full", left.InnerException!.Message);
        string remaining = Assert.Single(left.Remaining);
        Assert.Contains("part-00000", remaining);
        Assert.Equal([remaining.Replace('\\', '/')], FilesOnDisk());
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void Abort_AndDisposeUncompleted_GiveTheWriteUp(string format)
    {
        foreach (bool abort in (bool[])[true, false])
        {
            string path = Path.Combine(_root, abort ? "a" : "d");
            DatasetWriteOptions options = Options(format) with { MaxRowsPerFile = 10 };
            if (options is ParquetDatasetWriteOptions parquet)
            {
                using ParquetDatasetWriter writer = ParquetDatasetWriter.Create(path, TheSchema, parquet);
                writer.Write(Batch(0, 35));
                if (abort) writer.Abort();
            }
            else
            {
                using IpcDatasetWriter writer = IpcDatasetWriter.Create(path, TheSchema, (IpcDatasetWriteOptions)options);
                writer.Write(Batch(0, 35));
                if (abort) writer.Abort();
            }
            Assert.Empty(FilesOnDisk());
        }
    }

    [Fact]
    public void ABatchOfOtherColumns_IsRefused_AndTheWriteGoesOn()
    {
        string path = Path.Combine(_root, "s");
        using ParquetDatasetWriter writer = ParquetDatasetWriter.Create(path, TheSchema);
        writer.Write(Batch(0, 5));
        var other = new Schema([new Field("id", Int32Type.Default, false)], null);
        var error = Assert.Throws<ArgumentException>(() => writer.Write(new RecordBatch(other, [new Int32Array.Builder().Append(1).Build()], 1)));
        Assert.Contains("are not the dataset's", error.Message);
        // The same names, a column of another type.
        var retyped = new Schema([new Field("id", Int32Type.Default, false), TheSchema.GetFieldByIndex(1), TheSchema.GetFieldByIndex(2)], null);
        using RecordBatch five = Batch(0, 1);
        Assert.Throws<ArgumentException>(() => writer.Write(new RecordBatch(retyped, [new Int32Array.Builder().Append(1).Build(), five.Column(1), five.Column(2)], 1)));
        writer.Write(Batch(5, 5));
        Assert.Single(writer.Complete().Files);
        Assert.Equal(Rows(Batches(10)), Read("parquet", path));
        Assert.Throws<InvalidOperationException>(() => writer.Write(Batch(10, 1)));
    }

    [Fact]
    public void OptionsTheDatasetDecides_AreRefused()
    {
        string path = Path.Combine(_root, "o");
        Assert.Throws<ArgumentException>(() => ParquetDatasetWriter.Create(path, TheSchema,
            new ParquetDatasetWriteOptions { Writer = new ParquetWriteOptions { BatchOwnership = BatchOwnership.Writer } }));
        Assert.Throws<ArgumentException>(() => IpcDatasetWriter.Create(path, TheSchema,
            new IpcDatasetWriteOptions { Writer = new IpcWriteOptions { BatchOwnership = BatchOwnership.Writer } }));
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetDatasetWriter.Create(path, TheSchema, new ParquetDatasetWriteOptions { MaxRowsPerFile = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => IpcDatasetWriter.Create(path, TheSchema, new IpcDatasetWriteOptions { ApproxBytesPerFile = -1 }));
        Assert.Empty(FilesOnDisk());
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void FilesAreNamed_ByTheCallback(string format)
    {
        string path = Path.Combine(_root, "n");
        var (id, files) = Write(format, path, Batches(25), Options(format) with
        {
            MaxRowsPerFile = 10,
            FileName = n => $"chunk-{n.Index}-{n.WriteId[..8]}{n.Extension}",
        });
        Assert.Equal(Enumerable.Range(0, 3).Select(i => $"{path}/chunk-{i}-{id[..8]}{Extension(format)}"), files.Select(f => f.Path));
        Assert.Throws<InvalidOperationException>(() => Write(format, Path.Combine(_root, "bad"), Batches(5), Options(format) with { FileName = _ => "a/b" }));
        Assert.Equal(3, FilesOnDisk().Count);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task AsyncWrites_ComeBackTheSame(string format)
    {
        string path = Path.Combine(_root, "async");
        List<RecordBatch> batches = Batches(700, 300);
        static async IAsyncEnumerable<RecordBatch> Each(List<RecordBatch> batches)
        {
            foreach (RecordBatch batch in batches)
            {
                await Task.Yield();
                yield return batch;
            }
        }
        int files = format == "parquet"
            ? (await ParquetDatasetWriter.WriteAsync(path, TheSchema, Each(batches), new ParquetDatasetWriteOptions { MaxRowsPerFile = 400 })).Files.Count
            : (await IpcDatasetWriter.WriteAsync(path, TheSchema, Each(batches), (IpcDatasetWriteOptions)(Options(format) with { MaxRowsPerFile = 400 }))).Files.Count;
        Assert.Equal(3, files);
        Assert.Equal(Rows(batches), Read(format, path));
    }

    /// <summary>A store that cannot list its files.</summary>
    private sealed class UnlistedStore(IWritableFileStore inner) : IWritableFileStore
    {
        public bool CanList => false;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) => inner.OpenAsync(file, cancellationToken);

        public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) => inner.OpenAsync(path, cancellationToken);

        public FileWriteStream OpenWrite(string path, bool createOnly) => inner.OpenWrite(path, createOnly);

        public ValueTask<bool> DeleteAsync(string path, CancellationToken cancellationToken = default) => inner.DeleteAsync(path, cancellationToken);
    }

    /// <summary>A store listing only the files of one extension (reading a dataset beside other files).</summary>
    private sealed class FilteredStore(IWritableFileStore inner, string extension) : IFileStore, IDirectoryAware
    {
        public bool IsDirectory(string path) => ((IDirectoryAware)inner).IsDirectory(path);

        public bool CanList => true;

        public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (StoredFile file in inner.ListAsync(prefix, recursive, cancellationToken))
            {
                if (file.Path.EndsWith(extension, StringComparison.Ordinal)) yield return file;
            }
        }

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) => inner.OpenAsync(file, cancellationToken);

        public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) => inner.OpenAsync(path, cancellationToken);
    }

    /// <summary>A store whose streams fail past some bytes, and whose deletes fail, for chosen paths.</summary>
    private sealed class FaultStore(IWritableFileStore inner) : IWritableFileStore
    {
        /// <summary>Whether a write to a path fails, by the bytes written before it.</summary>
        public Func<string, long, bool>? FailWrite { get; init; }

        public Func<string, bool>? FailDelete { get; init; }

        public List<string> Deleted { get; } = [];

        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) => inner.OpenAsync(file, cancellationToken);

        public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) => inner.OpenAsync(path, cancellationToken);

        public FileWriteStream OpenWrite(string path, bool createOnly) => new Failing(inner.OpenWrite(path, createOnly), path, FailWrite);

        public ValueTask<bool> DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            if (FailDelete?.Invoke(path) == true) throw new IOException("permission denied");
            lock (Deleted) Deleted.Add(path);
            return inner.DeleteAsync(path, cancellationToken);
        }

        private sealed class Failing(FileWriteStream stream, string path, Func<string, long, bool>? fail) : FileWriteStream
        {
            private long _written;

            protected override void WriteCore(ReadOnlySpan<byte> buffer)
            {
                if (fail?.Invoke(path, _written) == true) throw new IOException("disk full");
                stream.Write(buffer);
                _written += buffer.Length;
            }

            protected override bool CommitCore() => stream.Commit();

            protected override void Abort() => stream.Dispose();

            protected override void Release() => stream.Dispose();
        }
    }
}
