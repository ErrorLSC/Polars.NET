using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// Data in the stream format opened as an <see cref="IpcFile"/> (its messages scanned, its batches read at offsets):
/// every stream of the corpus reads as <see cref="IpcStreamReader"/> reads it (itself checked against pyarrow and
/// Polars), on one thread and many, mapped or not, from memory, asynchronously; batches read at random see the
/// dictionaries sent before them (replacements and deltas); rows are numbered from the scan; a stream cut anywhere
/// reads as the stream reader reads it or is refused alike; a stream without its end-of-stream marker reads whole.
/// </summary>
public sealed class IpcStreamFileTests
{
    private static string[] StreamNames => [.. IpcValueTests.Names.Where(n => !IpcValueTests.IsFile(n))];

    public static TheoryData<string> Streams => [.. StreamNames];

    private static string PathOf(string name) => Path.Combine(IpcValueTests.Root, name);

    private static List<List<string>[]> Read(IpcFile file, IpcReadOptions options)
    {
        using IpcReader reader = file.OpenReader(options);
        List<List<string>[]> batches = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) batches.Add(Render(batch));
        }
        return batches;
    }

    private static async Task<List<List<string>[]>> ReadAsync(IpcFile file, IpcReadOptions options)
    {
        using IpcReader reader = file.OpenReader(options);
        List<List<string>[]> batches = [];
        await foreach (RecordBatch batch in reader.ReadBatchesAsync())
        {
            using (batch) batches.Add(Render(batch));
        }
        return batches;
    }

    private static List<string>[] Render(RecordBatch batch) =>
        [.. Enumerable.Range(0, batch.ColumnCount).Select(c => Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(c), i)).ToList())];

    [Fact]
    public void TheCorpus_HoldsStreams()
    {
        Assert.True(StreamNames.Length >= 30, $"{StreamNames.Length} streams");
        Assert.Contains("pyarrow/dictionary_replacement.arrows", StreamNames);
        Assert.Contains("pyarrow/no_eos.arrows", StreamNames);
        Assert.Contains("pyarrow/legacy.arrows", StreamNames);
    }

    [Theory]
    [MemberData(nameof(Streams))]
    public async Task AStream_ReadsAsTheStreamReaderReadsIt(string name)
    {
        if (IpcValueTests.Refused.TryGetValue(name, out string? reason))
        {
            Assert.Contains(reason, Assert.ThrowsAny<IpcFormatException>(() => Read(IpcFile.Open(PathOf(name)), new IpcReadOptions())).Message);
            return;
        }
        var (schema, expected) = IpcValueTests.Read(name, new IpcReadOptions());
        using (IpcFile file = IpcFile.Open(PathOf(name)))
        {
            Assert.True(file.IsStream);
            Assert.Equal(expected.Count, file.BatchCount);
            Assert.Equal(schema.FieldsList.Select(f => f.Name), file.Schema.FieldsList.Select(f => f.Name));
            Assert.Empty(file.FileMetadata);
            foreach (IpcReadOptions way in (IpcReadOptions[])[new() { MaxDegreeOfParallelism = 1, MemoryMap = IpcMemoryMap.Off },
                         new() { MaxDegreeOfParallelism = 1 }, new() { MaxDegreeOfParallelism = 4 }, new() { MaxDegreeOfParallelism = 4, MemoryMap = IpcMemoryMap.Off }])
            {
                Assert.Equal(expected, Read(file, way));
            }
            Assert.Equal(expected, await ReadAsync(file, new IpcReadOptions { MaxDegreeOfParallelism = 4 }));
        }
        using (IpcFile memory = IpcFile.Open(File.ReadAllBytes(PathOf(name)))) Assert.Equal(expected, Read(memory, new IpcReadOptions { MaxDegreeOfParallelism = 4 }));
        await using (IpcFile opened = await IpcFile.OpenAsync(PathOf(name))) Assert.Equal(expected, await ReadAsync(opened, new IpcReadOptions()));
    }

    /// <summary>Batches read one by one in reverse order, by one reader and by a reader each: each with the dictionaries sent before it.</summary>
    [Theory]
    [InlineData("pyarrow/dictionary_replacement.arrows")]
    [InlineData("pyarrow/dictionary_delta.arrows")]
    [InlineData("pyarrow/dictionary.arrows")]
    [InlineData("pyarrow/many_batches.arrow")]
    public async Task BatchesReadAtRandom_SeeTheirDictionaries(string name)
    {
        string path = PathOf(name);
        if (IpcValueTests.IsFile(name)) return;
        var (_, expected) = IpcValueTests.Read(name, new IpcReadOptions());
        using IpcFile file = IpcFile.Open(path);
        using (IpcReader shared = file.OpenReader())
        {
            for (int i = file.BatchCount - 1; i >= 0; i--)
            {
                using RecordBatch batch = shared.ReadBatch(i);
                Assert.Equal(expected[i], Render(batch));
            }
        }
        for (int i = file.BatchCount - 1; i >= 0; i--)
        {
            using RecordBatch batch = await file.ReadBatchAsync(i);
            Assert.Equal(expected[i], Render(batch));
        }
        // Numbered from the scan: batch i's first row is the rows of the batches before it.
        using IpcReader numbered = file.OpenReader(new IpcReadOptions { RowIndexColumn = "__row" });
        long before = 0;
        for (int i = 0; i < file.BatchCount; i++)
        {
            using RecordBatch batch = numbered.ReadBatch(i);
            if (batch.Length > 0) Assert.Equal(before, ((Int64Array)batch.Column(0)).GetValue(0));
            before += expected[i].Length == 0 ? 0 : expected[i][0].Count;
        }
    }

    /// <summary>
    /// A stream that replaces its dictionary before every batch: read in order (the dictionaries of batches delivered
    /// let go), then at random backwards by the same reader (its dictionaries applied again from the start).
    /// </summary>
    [Fact]
    public void BatchesReadAtRandom_AfterAReadInOrder_SeeTheirDictionaries()
    {
        var type = new Apache.Arrow.Types.DictionaryType(Apache.Arrow.Types.Int32Type.Default, Apache.Arrow.Types.StringType.Default, false);
        var schema = new Schema([new Field("d", type, true)], null);
        using var bytes = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(bytes, schema, new IpcWriteOptions { Format = IpcFormat.Stream }))
        {
            for (int b = 0; b < 12; b++)
            {
                var entries = new StringArray.Builder().AppendRange(Enumerable.Range(0, 4).Select(i => $"b{b}v{i}")).Build();
                using var batch = new RecordBatch(schema, [new DictionaryArray(type, new Int32Array.Builder().AppendRange([3, 0, 2]).Build(), entries)], 3);
                writer.Write(batch);
            }
            writer.Complete();
        }
        using IpcFile file = IpcFile.Open(bytes.ToArray());
        foreach (int threads in (int[])[1, 4])
        {
            using IpcReader reader = file.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
            int n = 0;
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) Assert.Equal([$"u:{Hex($"b{n}v3")}", $"u:{Hex($"b{n}v0")}", $"u:{Hex($"b{n}v2")}"], Render(batch)[0]);
                n++;
            }
            for (int b = 11; b >= 0; b -= 3)
            {
                using RecordBatch batch = reader.ReadBatch(b);
                Assert.Equal($"u:{Hex($"b{b}v3")}", Render(batch)[0][0]);
            }
        }

        static string Hex(string s) => Convert.ToHexStringLower(System.Text.Encoding.UTF8.GetBytes(s));
    }

    /// <summary>
    /// A stream cut at every point reads as the stream reader reads it: the batches before a cut at a message's end (no
    /// end-of-stream marker needed), or refused when the cut falls inside a message (the stream reader then fails too).
    /// </summary>
    [Theory]
    [InlineData("pyarrow/dictionary_replacement.arrows")]
    [InlineData("pyarrow/primitives.arrows")]
    [InlineData("pyarrow/legacy.arrows")]
    public void AStreamCutAnywhere_ReadsOrIsRefused_AsTheStreamReader(string name)
    {
        byte[] whole = File.ReadAllBytes(PathOf(name));
        int boundaries = 0, refused = 0;
        for (int cut = 0; cut <= whole.Length; cut += cut < 2000 ? 1 : 97)
        {
            byte[] part = whole[..cut];
            List<List<string>[]>? theirs = null, ours = null;
            try
            {
                using IpcStreamReader stream = IpcStreamReader.Open(new MemoryStream(part));
                List<List<string>[]> read = [];
                foreach (RecordBatch batch in stream.ReadBatches())
                {
                    using (batch) read.Add(Render(batch));
                }
                theirs = read; // read to the end (a failure midway is a refusal)
            }
            catch (IpcFormatException)
            {
            }
            try
            {
                using IpcFile file = IpcFile.Open(part);
                ours = Read(file, new IpcReadOptions());
            }
            catch (IpcFormatException)
            {
            }
            if (theirs is null) refused++;
            else boundaries++;
            Assert.True(theirs is null == ours is null, $"cut at {cut}: stream reader {(theirs is null ? "refuses" : "reads")}, IpcFile {(ours is null ? "refuses" : "reads")}");
            if (theirs is not null) Assert.Equal(theirs, ours);
        }
        Assert.True(boundaries >= 1 && refused > 100, $"{boundaries} cuts read, {refused} refused");
    }

    /// <summary>
    /// A stream's scan is the same whichever way its bytes are looked at: through a local file's mapping, read a stretch
    /// at a time (a local file not to be mapped, a store), or in place in memory; for every stream of the corpus whole
    /// and cut (inside a message, or at a third), and a generated one many stretches long whose dictionary is replaced
    /// as it goes; refusals say the same. Every file scanned with mapping allowed was mapped, and unmapped with its source.
    /// </summary>
    [Fact]
    public async Task TheScan_IsTheSame_MappedReadOrInMemory()
    {
        List<byte[]> cases = [Generated()];
        foreach (string name in StreamNames)
        {
            byte[] whole = File.ReadAllBytes(PathOf(name));
            foreach (int cut in (int[])[whole.Length, whole.Length - 1, whole.Length / 3]) cases.Add(whole[..cut]);
        }
        string dir = Directory.CreateTempSubdirectory("seseragi-ipc-scan-").FullName;
        try
        {
            int refused = 0;
            for (int c = 0; c < cases.Count; c++)
            {
                byte[] bytes = cases[c];
                bool useAsync = c % 2 == 0;
                string path = Path.Combine(dir, $"{c}.arrows");
                await File.WriteAllBytesAsync(path, bytes);
                string expected = await Scan(new MemoryRandomAccess(bytes), useAsync);
                if (expected.StartsWith("refused", StringComparison.Ordinal)) refused++;
                Assert.Equal(expected, await Scan(new Parquet.ParquetPruningTests.RecordingSource(bytes), useAsync));
                using (var unmapped = new RandomAccessSource(File.OpenRead(path), leaveOpen: false, copy: null, mappable: true))
                {
                    Assert.Equal(expected, await Scan(unmapped, useAsync, map: false));
                    Assert.False(unmapped.IsMapped);
                }
                Kaminari.Sazanami.Buffers.MappedFile? map;
                using (var mapped = new RandomAccessSource(File.OpenRead(path), leaveOpen: false, copy: null, mappable: true))
                {
                    Assert.Equal(expected, await Scan(mapped, useAsync));
                    Assert.True(mapped.IsMapped);
                    map = mapped.Map(out _);
                }
                Assert.False(map!.TryAcquire()); // unmapped with its source: the scan let go of its hold
            }
            Assert.InRange(refused, 1, cases.Count - 2);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        // The scan as bytes, or the refusal's message.
        static async Task<string> Scan(IRandomAccess source, bool useAsync, bool map = true)
        {
            try
            {
                return Convert.ToHexString((await IpcStreamIndex.ScanAsync(source, map, useAsync, default)).Write());
            }
            catch (IpcFormatException e)
            {
                return $"refused: {e.Message}";
            }
        }

        // 400 batches of 500 rows (about 3 MB: many 64 KiB and 1 MiB stretches), the dictionary replaced every 50.
        static byte[] Generated()
        {
            var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
            var schema = new Schema([new Field("i", Int64Type.Default, true), new Field("s", StringType.Default, true), new Field("d", type, true)], null);
            using var bytes = new MemoryStream();
            using (IpcWriter writer = IpcWriter.Create(bytes, schema, new IpcWriteOptions { Format = IpcFormat.Stream }))
            {
                for (int b = 0; b < 400; b++)
                {
                    var entries = new StringArray.Builder().AppendRange(Enumerable.Range(0, 3).Select(i => $"e{b / 50}_{i}")).Build();
                    using var batch = new RecordBatch(schema,
                    [
                        new Int64Array.Builder().AppendRange(Enumerable.Range(b * 500, 500).Select(i => (long)i)).Build(),
                        new StringArray.Builder().AppendRange(Enumerable.Range(0, 500).Select(i => $"row {b}:{i}")).Build(),
                        new DictionaryArray(type, new Int32Array.Builder().AppendRange(Enumerable.Range(0, 500).Select(i => i % 3)).Build(), entries),
                    ], 500);
                    writer.Write(batch);
                }
                writer.Complete();
            }
            return bytes.ToArray();
        }
    }

    [Fact]
    public void BatchMetadata_AndRowCounts_ComeFromTheStream()
    {
        using IpcFile file = IpcFile.Open(PathOf("pyarrow/metadata.arrows"));
        using IpcStreamReader stream = IpcStreamReader.Open(File.OpenRead(PathOf("pyarrow/metadata.arrows")));
        for (int i = 0; stream.ReadNext() is { } batch; i++)
        {
            using (batch) Assert.Equal(stream.BatchMetadata, file.GetBatchMetadata(i));
        }
    }
}
