using Kaminari.Seseragi.Files;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// <see cref="FileSystemStore"/>: a file system as a store of datasets — URLs below its root handed over relative,
/// listings at any depth where the file system can, files opened by their ends and read at the version first seen.
/// </summary>
public sealed class FileSystemStoreTests : IDisposable
{
    private const string Root = "mem://bucket";

    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-fsstore-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static byte[] File(long first, int rows)
    {
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true)], null);
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build();
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = Math.Max(1, rows / 3) }))
        {
            writer.Write(new RecordBatch(schema, [ids], rows));
            writer.Complete();
        }
        return stream.ToArray();
    }

    private static List<long> Ids(ParquetDataset dataset, int threads = 4)
    {
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        List<long> ids = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
        }
        return ids;
    }

    [Fact]
    public void ADatasetBelowTheRoot_IsListedAndRead()
    {
        var fs = new LocalFileSystem(_dir);
        fs.Overwrite("data/a b.parquet", File(0, 100));
        fs.Overwrite("data/sub/c%20d.parquet", File(100, 100));
        fs.Overwrite("data/notes.txt", "x"u8.ToArray());
        var store = new FileSystemStore(fs, Root + "/");
        Assert.Equal(Root, store.Root);
        using var all = ParquetDataset.Open($"{Root}/data/**/*.parquet", new ParquetDatasetOptions { Store = store });
        Assert.Equal([$"{Root}/data/a b.parquet", $"{Root}/data/sub/c%20d.parquet"], all.Files.Select(f => f.Path));
        Assert.Equal(Enumerable.Range(0, 200).Select(i => (long)i), Ids(all));
        using var top = ParquetDataset.Open($"{Root}/data/*.parquet", new ParquetDatasetOptions { Store = store });
        Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), Ids(top, 1));
        // A file named alone is opened by its end; one outside the root is refused.
        using var one = ParquetDataset.Open($"{Root}/data/sub/c%20d.parquet", new ParquetDatasetOptions { Store = store });
        Assert.Equal(100, Ids(one).Count);
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("mem://other/x.parquet", new ParquetDatasetOptions { Store = store }));
        Assert.Null(store.StatAsync($"{Root}/data/missing.parquet").AsTask().Result);
        Assert.Equal(new FileInfo(Path.Combine(_dir, "data", "a b.parquet")).Length, store.StatAsync($"{Root}/data/a b.parquet").AsTask().Result!.Value.Size);
    }

    [Fact]
    public void AFileSystemListingOneDirectoryAtATime_CannotListRecursively()
    {
        var local = new LocalFileSystem(_dir);
        local.Overwrite("d/a.parquet", File(0, 10));
        var store = new FileSystemStore(new OneLevel(local), Root);
        using var top = ParquetDataset.Open($"{Root}/d/*.parquet", new ParquetDatasetOptions { Store = store });
        Assert.Equal(10, Ids(top).Count);
        Assert.Throws<NotSupportedException>(() => ParquetDataset.Open($"{Root}/d/**/*.parquet", new ParquetDatasetOptions { Store = store }));
        // Without tails, a file named alone (no size) cannot be opened.
        Assert.Throws<NotSupportedException>(() => ParquetDataset.Open($"{Root}/d/a.parquet", new ParquetDatasetOptions { Store = store }));
    }

    /// <summary>Asynchronously, a file is read at the version its end was read at: a file replaced since fails, naming it.</summary>
    [Fact]
    public void Versions_AreRequired_OfLaterReads()
    {
        var fs = new Versioned(new LocalFileSystem(_dir));
        fs.Overwrite("v/a.parquet", File(0, 20_000));
        fs.Overwrite("v/b.parquet", File(20_000, 20_000));
        var store = new FileSystemStore(fs, Root);
        using var dataset = ParquetDataset.Open($"{Root}/v/*.parquet", new ParquetDatasetOptions { Store = store });
        Assert.Equal(40_000, Ids(dataset).Count);
        Assert.True(fs.VersionedReads > 0);
        fs.Overwrite("v/b.parquet", File(70_000, 20_000));
        var error = Assert.ThrowsAny<IOException>(() => Ids(dataset));
        Assert.Contains($"{Root}/v/b.parquet", error.Message);
        Assert.Contains("changed", error.Message);
    }

    /// <summary>A file opened by its end serves the end from memory, and requires that end's version of later reads.</summary>
    [Fact]
    public async Task AFileOpenedByItsEnd_ServesItFromMemory_AndRequiresItsVersion()
    {
        var fs = new Versioned(new LocalFileSystem(_dir));
        byte[] bytes = File(0, 50_000);
        Assert.True(bytes.Length > FileSystemStore.TailRead);
        fs.Overwrite("f.parquet", bytes);
        var store = new FileSystemStore(fs, Root);
        IRandomAccess source = (await store.OpenAsync($"{Root}/f.parquet"))!;
        Assert.Equal(bytes.Length, source.Length);
        byte[] end = new byte[100];
        Assert.Equal(100, await source.ReadAtAsync(bytes.Length - 100, end, default));
        Assert.Equal(bytes[^100..], end);
        Assert.Equal(0, fs.RangeReads); // from the end read when opened
        fs.Overwrite("f.parquet", bytes); // another version
        var error = await Assert.ThrowsAnyAsync<IOException>(async () => await source.ReadAtAsync(0, new byte[100], default));
        Assert.Contains("changed", error.Message);
    }

    /// <summary>A file system with only the read contract (one directory at a time, no tails).</summary>
    private sealed class OneLevel(IFileSystem inner) : IFileSystem
    {
        public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null) => inner.List(directory, startFrom);

        public bool TryReadAll(string path, System.Buffers.IBufferWriter<byte> into) => inner.TryReadAll(path, into);

        public int ReadRange(string path, long offset, Span<byte> destination) => inner.ReadRange(path, offset, destination);
    }

    /// <summary>A local file system with versions (a counter per write), read asynchronously, conditions checked.</summary>
    private sealed class Versioned(LocalFileSystem inner) : IWritableFileSystem, IAsyncFileSystem
    {
        private readonly Dictionary<string, int> _versions = [];
        private int _versionedReads, _rangeReads;

        public int RangeReads => Volatile.Read(ref _rangeReads);

        public int VersionedReads => Volatile.Read(ref _versionedReads);

        private string Version(string path)
        {
            lock (_versions) return _versions.GetValueOrDefault(path).ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null) => inner.List(directory, startFrom);

        public IReadOnlyList<FileEntry> ListAll(string directory) => inner.ListAll(directory);

        public bool TryReadAll(string path, System.Buffers.IBufferWriter<byte> into) => inner.TryReadAll(path, into);

        public int ReadRange(string path, long offset, Span<byte> destination) => inner.ReadRange(path, offset, destination);

        public bool TryCreate(string path, ReadOnlyMemory<byte> content) => inner.TryCreate(path, content);

        public void Overwrite(string path, ReadOnlyMemory<byte> content)
        {
            inner.Overwrite(path, content);
            lock (_versions) _versions[path] = _versions.GetValueOrDefault(path) + 1;
        }

        public bool Delete(string path) => inner.Delete(path);

        public ValueTask<FileRead?> ReadRangeAsync(string path, long offset, Memory<byte> destination, string? version, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _rangeReads);
            if (version is not null)
            {
                Interlocked.Increment(ref _versionedReads);
                if (version != Version(path)) throw new IOException("the file changed while it was read");
            }
            if (inner.ReadRange(path, offset, destination.Length) is not { } bytes) return ValueTask.FromResult<FileRead?>(null);
            bytes.CopyTo(destination);
            return ValueTask.FromResult<FileRead?>(new FileRead(bytes.Length, Version(path)));
        }

        public ValueTask<FileTail?> ReadTailAsync(string path, int length, CancellationToken cancellationToken) =>
            ValueTask.FromResult(inner.ReadTail(path, length) is { } tail ? tail with { Version = Version(path) } : (FileTail?)null);

        public ValueTask<IReadOnlyList<FileEntry>> ListAllAsync(string directory, CancellationToken cancellationToken) =>
            ValueTask.FromResult(inner.ListAll(directory));
    }
}
