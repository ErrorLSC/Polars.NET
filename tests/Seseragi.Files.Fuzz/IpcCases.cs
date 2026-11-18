using System.Buffers.Binary;
using Apache.Arrow;
using Kaminari.Seseragi.Files;
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
/// read at random); the fourth writes random columns of every type (flat, or nested with dictionaries and run-end
/// encoding at every level) in random slices with random options (format, compression, threads) and reads them back,
/// every value compared. Any input either reads to the end or is refused with <see cref="IpcFormatException"/>.
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
            return HasFile(n) ? ReadFile(Mutate(random, out _), random) : WriteAndRead(random);
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
            bool isFile = bytes.AsSpan().StartsWith("ARROW1"u8);
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
        };
        using var stream = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(stream, batch.Schema, options))
        {
            for (int at = skip; at < rows;)
            {
                int take = Math.Min(rows - at, random.Next(2) == 0 ? rows : random.Next(1, 400));
                writer.Write(batch.Slice(at, take));
                at += take;
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
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                string[] got = nested
                    ? [.. batches.SelectMany(b => ParquetNestedData.Canonical(b.Column(c)))]
                    : [.. batches.SelectMany(b => ParquetWriteData.Canonical(b.Column(c)))];
                if (!expected[c].SequenceEqual(got))
                    return new Outcome(true, "Written IPC values do not come back",
                        $"column {batch.Schema.FieldsList[c].Name} ({batch.Schema.FieldsList[c].DataType.Name}), {options.Format}, {options.Compression}");
            }
            return Outcome.Read;
        }
        finally
        {
            foreach (RecordBatch b in batches) b.Dispose();
        }
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
