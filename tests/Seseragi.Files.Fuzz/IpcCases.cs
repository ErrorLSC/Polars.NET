using System.Buffers.Binary;
using Apache.Arrow;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Fuzz;

/// <summary>The cases of one fuzzed format.</summary>
internal interface IFuzzCases
{
    Outcome Run(long seed, long n);

    /// <summary>Whether case <paramref name="n"/> reads a mutated corpus file (which <see cref="FileOf"/> gives).</summary>
    bool HasFile(long n);

    byte[] FileOf(long seed, long n, out string source);

    /// <summary>The extension of a saved file.</summary>
    string Extension { get; }
}

/// <summary>
/// The Arrow IPC cases: three in four are a corpus file (TestData/ipc: files and streams of pyarrow, Polars and the
/// Arrow integration gold files) with a few mutations aimed in part at message starts and the footer, read with random
/// options (threads, memory mapping through a temporary file, columns, row limits, row filters, UTF-8 checks, batches
/// read at random); of the others, half write random columns of every type (flat, or nested with dictionaries and
/// run-end encoding at every level) in random slices with random options (format, compression, threads) and read them
/// back, every value compared, and half read a dataset of corpus files (see <see cref="ReadDataset"/>). Any input either
/// reads to the end or is refused with <see cref="IpcFormatException"/>.
/// </summary>
internal sealed class IpcCases(IReadOnlyList<string> corpus) : IFuzzCases
{
    private readonly Dictionary<string, (byte[] Bytes, long[] Hot, int Footer)> _files = [];

    public string Extension => ".arrow";

    public bool HasFile(long n) => n % 4 != 3;

    public Outcome Run(long seed, long n)
    {
        Random random = Cases.RandomFor(seed, n);
        try
        {
            return HasFile(n) ? ReadFile(Mutate(random, out _), random) : n / 4 % 2 == 0 ? WriteAndRead(random) : ReadDataset(random);
        }
        catch (Exception e)
        {
            return Outcome.From(e);
        }
    }

    public byte[] FileOf(long seed, long n, out string source) => Mutate(Cases.RandomFor(seed, n), out source);

    private byte[] Mutate(Random random, out string source)
    {
        source = corpus[random.Next(corpus.Count)];
        var (original, hot, footer) = Load(source);
        return Mutations.Apply(random, original, hot, footer, r => Load(corpus[r.Next(corpus.Count)]).Bytes);
    }

    /// <summary>A corpus file, its message starts (after each continuation marker) and, for a file, its footer's length.</summary>
    private (byte[] Bytes, long[] Hot, int Footer) Load(string path)
    {
        if (_files.TryGetValue(path, out var known)) return known;
        byte[] bytes = File.ReadAllBytes(path);
        List<long> hot = [];
        ReadOnlySpan<byte> marker = [0xFF, 0xFF, 0xFF, 0xFF];
        for (int i = bytes.AsSpan().IndexOf(marker); i >= 0;)
        {
            hot.Add(i + 4);
            int next = bytes.AsSpan(i + 4).IndexOf(marker);
            i = next < 0 ? -1 : i + 4 + next;
        }
        int footer = bytes.Length >= 16 && bytes.AsSpan(bytes.Length - 6).SequenceEqual("ARROW1"u8)
            ? Math.Clamp(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 10)), 0, bytes.Length - 16)
            : 0;
        return _files[path] = (bytes, [.. hot], footer);
    }

    private static Outcome ReadFile(byte[] bytes, Random random)
    {
        string? temporary = null;
        try
        {
            var options = new IpcReadOptions
            {
                MaxDegreeOfParallelism = random.Next(3) switch { 0 => 1, 1 => 2, _ => 4 },
                MemoryMap = (IpcMemoryMap)random.Next(3),
                ValidateUtf8 = random.Next(3) == 0,
                MaxRows = random.Next(8) == 0 ? random.Next(0, 5000) : null,
                ParallelDecompressFrom = random.Next(2) == 0 ? 0 : 1 << 20,
            };
            // Files, and half the streams (scanned, then read at offsets as files are), through IpcFile.
            bool isFile = bytes.AsSpan().StartsWith("ARROW1"u8) || random.Next(2) == 0;
            if (isFile)
            {
                IpcFile file;
                if (random.Next(4) == 0)
                {
                    // Through a mapping: bodies read in place (not guarded, unlike the native blocks).
                    temporary = Path.Combine(Path.GetTempPath(), $"seseragi-ipc-fuzz-{Environment.ProcessId}.arrow");
                    File.WriteAllBytes(temporary, bytes);
                    file = IpcFile.Open(temporary);
                }
                else file = IpcFile.Open(bytes);
                using (file)
                {
                    options = Choose(random, options, file.Schema);
                    using IpcReader reader = file.OpenReader(options);
                    if (random.Next(4) == 0 && file.BatchCount > 0)
                    {
                        for (int k = 0; k < 3; k++) reader.ReadBatch(random.Next(file.BatchCount)).Dispose();
                    }
                    int stop = random.Next(4) == 0 ? random.Next(4) : int.MaxValue; // a consumer that stops early
                    foreach (RecordBatch batch in reader.ReadBatches())
                    {
                        batch.Dispose();
                        if (--stop < 0) break;
                    }
                }
                return Outcome.Read;
            }
            Stream stream = random.Next(2) == 0 ? new MemoryStream(bytes) : new NonSeekable(new MemoryStream(bytes));
            IpcReadOptions plain = options;
            using (IpcStreamReader probe = IpcStreamReader.Open(new MemoryStream(bytes), plain with { MaxDegreeOfParallelism = 1 }))
                options = Choose(random, options, probe.Schema);
            using (IpcStreamReader reader = IpcStreamReader.Open(stream, options))
            {
                int stop = random.Next(4) == 0 ? random.Next(4) : int.MaxValue;
                while (reader.ReadNext() is { } batch)
                {
                    batch.Dispose();
                    if (--stop < 0) break;
                }
            }
            return Outcome.Read;
        }
        catch (IpcFormatException e)
        {
            return Outcome.Refused(e);
        }
        catch (NotSupportedException e) when (e.Message.Contains("cannot be filtered"))
        {
            return Outcome.Refused(e); // a column the row filter cannot select: refused on open, by design
        }
        finally
        {
            if (temporary is not null) File.Delete(temporary);
        }
    }

    /// <summary>Random columns and a random row filter over a schema.</summary>
    private static IpcReadOptions Choose(Random random, IpcReadOptions options, Schema schema)
    {
        string[] names = [.. schema.FieldsList.Select(f => f.Name).Distinct()];
        if (random.Next(3) == 0 && names.Length > 1) options = options with { Columns = [.. names.Where(_ => random.Next(2) == 0)] };
        if (random.Next(3) == 0) options = options with { Filter = Cases.RandomFilter(random, schema), FilterRows = random.Next(4) != 0 };
        return options;
    }

    private static readonly IIpcCompression[] Codecs = [NoCompression.Instance, Lz4FrameCompression.Instance, new ZstandardCompression(1), new ZstandardCompression(9)];

    /// <summary>Random columns written in random slices with random options and read back with random options: every value must come back.</summary>
    private static Outcome WriteAndRead(Random random)
    {
        bool nested = random.Next(2) == 0;
        int rows = random.Next(random.Next(2) == 0 ? 50 : 3000);
        RecordBatch batch;
        string[][] expected;
        int skip;
        if (nested)
        {
            (batch, expected) = ParquetNestedData.Batch(random.Next(), rows, 1 + random.Next(6), 1 + random.Next(4));
            skip = rows == 0 ? 0 : random.Next(rows);
            expected = [.. expected.Select(c => c[skip..])];
        }
        else
        {
            RecordBatch wide = ParquetWriteData.Wide(random.Next(), rows);
            int[] columns = [.. Enumerable.Range(0, wide.ColumnCount).Where(_ => random.Next(3) == 0).DefaultIfEmpty(random.Next(wide.ColumnCount))];
            batch = new RecordBatch(new Schema([.. columns.Select(c => wide.Schema.FieldsList[c])], null), columns.Select(wide.Column), rows);
            skip = 0;
            expected = [.. Enumerable.Range(0, batch.ColumnCount).Select(c => ParquetWriteData.Canonical(batch.Column(c)))];
        }
        var options = new IpcWriteOptions
        {
            Format = (IpcFormat)random.Next(2),
            Compression = Codecs[random.Next(Codecs.Length)],
            MaxDegreeOfParallelism = random.Next(3) == 0 ? 1 : 1 + random.Next(8),
            LeaveOpen = true,
            // The writer taking the batch: it compresses several side by side (slices would share the batch's buffers: given whole).
            BatchOwnership = skip == 0 && random.Next(3) == 0 ? BatchOwnership.Writer : BatchOwnership.Caller,
            InFlightBytes = random.Next(2) == 0 ? 1 : 512L << 20,
            // Compressed record batches written buffer by buffer as they are compressed (the output is seekable).
            StreamBodyFrom = random.Next(2) == 0 ? 0 : long.MaxValue,
        };
        using var stream = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(stream, batch.Schema, options))
        {
            if (options.BatchOwnership == BatchOwnership.Writer) writer.Write(batch); // taken: the writer disposes it
            else
            {
                for (int at = skip; at < rows;)
                {
                    int take = Math.Min(rows - at, random.Next(2) == 0 ? rows : random.Next(1, 400));
                    writer.Write(batch.Slice(at, take));
                    at += take;
                }
            }
        }
        var read = new IpcReadOptions
        {
            MaxDegreeOfParallelism = random.Next(2) == 0 ? 1 : 4,
            ParallelDecompressFrom = random.Next(2) == 0 ? 0 : 1 << 20,
            ValidateUtf8 = true,
        };
        List<RecordBatch> batches = [];
        try
        {
            stream.Position = 0;
            if (options.Format == IpcFormat.File)
            {
                using IpcFile file = IpcFile.Open(stream.ToArray());
                using IpcReader reader = file.OpenReader(read);
                batches.AddRange(reader.ReadBatches());
            }
            else
            {
                using IpcStreamReader reader = IpcStreamReader.Open(stream, read, leaveOpen: true);
                batches.AddRange(reader.ReadBatches());
            }
            for (int c = 0; c < expected.Length; c++)
            {
                string[] got = nested
                    ? [.. batches.SelectMany(b => ParquetNestedData.Canonical(b.Column(c)))]
                    : [.. batches.SelectMany(b => ParquetWriteData.Canonical(b.Column(c)))];
                if (!expected[c].SequenceEqual(got))
                    return new Outcome(true, "Written IPC values do not come back",
                        $"column {c}, {options.Format}, {options.Compression}, {options.BatchOwnership}");
            }
            return Outcome.Read;
        }
        finally
        {
            foreach (RecordBatch b in batches) b.Dispose();
        }
    }

    // ---- datasets ---------------------------------------------------------------------------------------------------

    /// <summary>
    /// Sets of corpus files that make datasets: each directory of TestData/ipc/datasets (types, layouts and dictionaries
    /// that differ), pyarrow's files of one schema in several codecs, Polars' frames at two levels, the two big files.
    /// </summary>
    private List<string[]> Families()
    {
        if (_families is not null) return _families;
        string[] files = [.. corpus.Where(f => Path.GetExtension(f) is ".arrow" or ".feather" or ".arrows" or ".stream" or ".arrow_file")];
        List<string[]> families = [.. files.Where(f => Path.GetFileName(Path.GetDirectoryName(Path.GetDirectoryName(f))) == "datasets")
            .GroupBy(Path.GetDirectoryName).Select(g => g.ToArray())];
        string[] Named(string folder, params string[] names) => [.. files.Where(f => Path.GetFileName(Path.GetDirectoryName(f)) == folder && names.Contains(Path.GetFileNameWithoutExtension(f)))];
        families.Add(Named("pyarrow", "lz4", "zstd", "zstd_level19", "many_batches", "many_batches_lz4"));
        families.Add([.. files.Where(f => Path.GetFileName(Path.GetDirectoryName(f)) == "polars" && Path.GetFileName(f).StartsWith("frame_", StringComparison.Ordinal))]);
        families.Add([.. files.Where(f => Path.GetFileName(f) == "big_lz4.arrow")]);
        // Streams: Polars' frames, pyarrow's codecs, and both formats of one frame side by side.
        families.Add([.. files.Where(f => Path.GetFileName(Path.GetDirectoryName(f)) == "polars" && Path.GetExtension(f) == ".arrows")]);
        families.Add(Named("pyarrow", "lz4", "zstd", "many_batches").Concat(files.Where(f => Path.GetFileName(f) is "lz4.arrows" or "zstd.arrows")).ToArray());
        families.Add([.. files.Where(f => Path.GetFileName(f).StartsWith("frame_zstd", StringComparison.Ordinal) || Path.GetFileName(f).StartsWith("frame_oldest", StringComparison.Ordinal))]);
        return _families = [.. families.Where(f => f.Length > 0)];
    }

    private List<string[]>? _families;

    /// <summary>
    /// A dataset of 1 to 6 corpus files (one family, or any files; one in four of them mutated) in memory, flat or in
    /// Hive partitions, with random dataset options (schema mode, missing columns, lossy casts, unified dictionaries,
    /// the file path) and read options (columns, a filter over files and partitions, row limit, row index), read twice:
    /// file after file on one thread, and through the window on four threads from a store that counts as remote,
    /// synchronously or not. Both must refuse alike or read the same rows (an error of a file past a row limit never
    /// surfaces in either).
    /// </summary>
    private Outcome ReadDataset(Random random)
    {
        List<string[]> families = Families();
        string[] pool = random.Next(4) == 0 ? [.. corpus.Where(f => Path.GetExtension(f) is ".arrow" or ".feather" or ".arrows" or ".stream" or ".arrow_file")] : families[random.Next(families.Count)];
        int count = 1 + random.Next(6);
        var memory = new MemoryFileStore();
        bool hive = random.Next(2) == 0;
        for (int i = 0; i < count; i++)
        {
            string source = pool[random.Next(pool.Length)];
            var (bytes, hot, footer) = Load(source);
            if (random.Next(4) == 0) bytes = Mutations.Apply(random, bytes, hot, footer, r => Load(corpus[r.Next(corpus.Count)]).Bytes);
            memory.Add(hive ? $"d/part_k={random.Next(3)}/f{i}.arrow" : $"d/f{i}.arrow", bytes);
        }
        var datasetOptions = new IpcDatasetOptions
        {
            SchemaMode = random.Next(2) == 0 ? DatasetSchemaMode.FirstFile : DatasetSchemaMode.UnionByName,
            AllowMissingColumns = random.Next(3) == 0,
            AllowLossyCasts = random.Next(3) == 0,
            UnifyDictionaries = random.Next(3) == 0,
            FilePathColumn = random.Next(4) == 0 ? "__file" : null,
            MaxFilesInFlight = random.Next(3) == 0 ? 1 + random.Next(4) : null,
        };
        int seed = random.Next();
        bool async = random.Next(2) == 0;
        var (one, oneRows) = ReadDatasetOnce(new Random(seed), memory, datasetOptions, threads: 1, remote: false, async: false);
        var (many, manyRows) = ReadDatasetOnce(new Random(seed), memory, datasetOptions, threads: 4, remote: true, async);
        if (one.Finding) return one;
        if (many.Finding) return many;
        if (one.Signature != many.Signature)
            return new Outcome(true, "A dataset read differs on one thread and in the window", $"one thread: {one.Signature} {one.Detail}; window: {many.Signature} {many.Detail}");
        if (oneRows is not null && !oneRows.SequenceEqual(manyRows!))
        {
            int at = Enumerable.Range(0, Math.Min(oneRows.Count, manyRows!.Count)).FirstOrDefault(i => oneRows[i] != manyRows[i], Math.Min(oneRows.Count, manyRows.Count));
            return new Outcome(true, "A dataset's rows differ on one thread and in the window",
                $"{oneRows.Count} vs {manyRows.Count} rows; first difference at {at}: {(at < oneRows.Count ? oneRows[at] : "-")} vs {(at < manyRows.Count ? manyRows[at] : "-")}");
        }
        return one;
    }

    /// <summary>One reading of the dataset (its read options drawn from <paramref name="random"/>, the same for both readings): the outcome and the rows read.</summary>
    private static (Outcome Outcome, List<string>? Rows) ReadDatasetOnce(Random random, MemoryFileStore memory, IpcDatasetOptions options, int threads, bool remote, bool async)
    {
        try
        {
            using var dataset = IpcDataset.Open("d/", options with { Store = remote ? new RemoteMemory(memory) : memory });
            Schema schema = dataset.Schema;
            string[] names = [.. schema.FieldsList.Select(f => f.Name)];
            var read = new IpcReadOptions
            {
                MaxDegreeOfParallelism = threads,
                Columns = random.Next(3) == 0 && names.Length > 1 ? [.. names.Where(_ => random.Next(2) == 0)] : null,
                Filter = random.Next(3) == 0 ? Cases.RandomFilter(random, schema) : null,
                FilterRows = random.Next(4) != 0,
                MaxRows = random.Next(5) == 0 ? random.Next(0, 3000) : null,
                RowIndexColumn = random.Next(4) == 0 ? "__row" : null,
                ValidateUtf8 = random.Next(3) == 0,
            };
            using IpcDatasetReader reader = dataset.OpenReader(read);
            List<string> rows = [];
            void Add(RecordBatch batch)
            {
                using (batch)
                {
                    for (int i = 0; i < batch.Length; i++)
                        rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), i))));
                }
            }
            if (async)
            {
                IAsyncEnumerator<RecordBatch> batches = reader.ReadBatchesAsync().GetAsyncEnumerator();
                try
                {
                    while (batches.MoveNextAsync().AsTask().GetAwaiter().GetResult()) Add(batches.Current);
                }
                finally
                {
                    batches.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            }
            else foreach (RecordBatch batch in reader.ReadBatches()) Add(batch);
            return (Outcome.Read, rows);
        }
        catch (IpcFormatException e)
        {
            return (Outcome.Refused(e), null);
        }
        catch (NotSupportedException e) when (e.Message.Contains("cannot be filtered") || e.Message.Contains("cannot be unified"))
        {
            return (Outcome.Refused(e), null);
        }
        catch (Exception e)
        {
            return (Outcome.From(e), null);
        }
    }

    /// <summary>A store over memory that a dataset takes for a remote one (its files read through the window, requests bounded).</summary>
    private sealed class RemoteMemory(MemoryFileStore inner) : IFileStore
    {
        public bool CanList => true;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) => inner.OpenAsync(file, cancellationToken);

        public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) => inner.OpenAsync(path, cancellationToken);
    }

    /// <summary>A stream that cannot seek (a socket, a pipe).</summary>
    private sealed class NonSeekable(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => inner.Read(buffer);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer, cancellationToken);
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
