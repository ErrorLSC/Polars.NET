using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Compression;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>The IPC readers' API: lifetimes, metadata, random access, errors, and Apache.Arrow's reader as a second oracle.</summary>
public class IpcApiTests
{
    private static string Path(string name) => System.IO.Path.Combine(IpcValueTests.Root, name);

    private static NativeMemoryOwner Owner(IArrowArray array)
    {
        foreach (ArrowBuffer buffer in array.Data.Buffers)
        {
            if (MemoryMarshal.TryGetMemoryManager(buffer.Memory, out NativeBufferMemory? memory)) return memory.Owner;
        }
        throw new InvalidOperationException("No native buffer.");
    }

    [Theory]
    [InlineData(IpcMemoryMap.On)]
    [InlineData(IpcMemoryMap.Off)]
    public void Batches_OutliveTheirFileAndReader_AndFreeTheirMemoryWhenDisposed(IpcMemoryMap map)
    {
        RecordBatch batch;
        List<string> expected;
        using (IpcFile file = IpcFile.Open(Path("pyarrow/primitives.arrow")))
        using (IpcReader reader = file.OpenReader(new IpcReadOptions { MemoryMap = map }))
        {
            batch = reader.ReadBatch(0);
            expected = [.. Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(3), i))];
        }
        NativeMemoryOwner owner = Owner(batch.Column(3));
        Assert.False(owner.IsFreed);
        Assert.Equal(expected, Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(3), i)));
        batch.Dispose();
        Assert.True(owner.IsFreed);
    }

    /// <summary>Compressed batches are decompressed out of the file's mapping by default (Auto, as On); Off reads them.</summary>
    [Theory]
    [InlineData("pyarrow/lz4.arrow", IpcMemoryMap.Auto, true)]
    [InlineData("pyarrow/zstd.arrow", IpcMemoryMap.Auto, true)]
    [InlineData("polars/frame_lz4.arrow", IpcMemoryMap.Auto, true)]
    [InlineData("pyarrow/lz4.arrow", IpcMemoryMap.On, true)]
    [InlineData("pyarrow/lz4.arrow", IpcMemoryMap.Off, false)]
    [InlineData("pyarrow/primitives.arrow", IpcMemoryMap.Auto, true)]
    public void CompressedBatches_AreReadFromTheMapping_ByDefault(string name, IpcMemoryMap map, bool mapped)
    {
        using IpcFile file = IpcFile.Open(Path(name));
        using IpcReader reader = file.OpenReader(new IpcReadOptions { MemoryMap = map });
        using RecordBatch batch = reader.ReadBatch(0);
        int column = Enumerable.Range(0, batch.ColumnCount).First(c => batch.Column(c) is not NullArray && batch.Column(c).Data.Buffers.Any(b => !b.IsEmpty));
        Assert.Equal(mapped, Assert.IsType<IpcBodyMemory>(Owner(batch.Column(column))).IsMapped);
    }

    /// <summary>
    /// A mapped file's messages (record batches, dictionaries, the row counts the row index needs) are parsed in the
    /// mapping, as its bodies are read from it: reading asks nothing of the source; unmapped, every message is read.
    /// </summary>
    [Theory]
    [InlineData("pyarrow/dictionary.arrow", 1)]
    [InlineData("pyarrow/dictionary.arrow", 4)]
    [InlineData("pyarrow/dictionary_replacement.arrows", 1)]
    [InlineData("pyarrow/dictionary_replacement.arrows", 4)]
    [InlineData("pyarrow/many_batches.arrow", 4)]
    [InlineData("pyarrow/primitives.arrows", 1)]
    public void MappedMessages_AreParsedInPlace_WithoutReads(string name, int threads)
    {
        foreach (IpcMemoryMap map in (IpcMemoryMap[])[IpcMemoryMap.Auto, IpcMemoryMap.Off])
        {
            using IpcFile file = IpcFile.Open(Path(name));
            var source = (Kaminari.Seseragi.Files.IO.RandomAccessSource)file.Source;
            int before = source.Reads;
            using IpcReader reader = file.OpenReader(new IpcReadOptions { MemoryMap = map, MaxDegreeOfParallelism = threads, RowIndexColumn = "i", RowIndexOffset = 5 });
            List<string> rows = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) rows.AddRange(Enumerable.Range(0, batch.Length).Select(r => string.Join("|", Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), r)))));
            }
            // A batch read at random: its row index counts the rows before it (a file's from their messages).
            using IpcReader again = file.OpenReader(new IpcReadOptions { MemoryMap = map, RowIndexColumn = "i", RowIndexOffset = 5 });
            List<string> tail;
            using (RecordBatch last = again.ReadBatch(file.BatchCount - 1))
                tail = [.. Enumerable.Range(0, last.Length).Select(r => string.Join("|", Enumerable.Range(0, last.ColumnCount).Select(c => CanonicalRender.Value(last.Column(c), r))))];
            int reads = source.Reads - before;
            if (map == IpcMemoryMap.Off) Assert.True(reads > 0, $"{name}: unmapped, {reads} reads");
            else Assert.Equal(0, reads);
            List<string> expected = Expected(name, threads);
            Assert.Equal(expected, rows);
            Assert.Equal(expected[^tail.Count..], tail);
        }

        static List<string> Expected(string name, int threads)
        {
            using IpcFile file = IpcFile.Open(File.OpenRead(Path(name)));
            using IpcReader reader = file.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads, RowIndexColumn = "i", RowIndexOffset = 5 });
            List<string> rows = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) rows.AddRange(Enumerable.Range(0, batch.Length).Select(r => string.Join("|", Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), r)))));
            }
            Assert.NotEmpty(rows);
            return rows;
        }
    }

    /// <summary>A message whose block reaches past the mapped file is a format error, as an early end is unmapped.</summary>
    [Fact]
    public void AMappedMessage_PastTheFile_IsAFormatError()
    {
        string path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"ipc-message-past-{Guid.NewGuid():N}.arrows");
        try
        {
            File.Copy(Path("pyarrow/primitives.arrows"), path);
            using IpcFile file = IpcFile.Open(path);
            int batches = file.BatchCount; // scans the stream whole: the blocks are known
            Assert.True(batches > 0);
            var source = (Kaminari.Seseragi.Files.IO.RandomAccessSource)file.Source;
            IpcBlock last = file.BatchBlock(batches - 1);
            var map = source.Map(out long start)!; // the source's mapping (it keeps the hold)
            IpcBlock past = last with { Offset = map.Length - last.MetadataLength + 1 };
            var e = Assert.Throws<IpcFormatException>(() => IpcIO.Sync(IpcIO.MessageAsync(source, map, start, past, useAsync: false, default)));
            Assert.Contains("reaches past the file", e.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Dictionaries_AreKeptByTheReaderAndEveryBatch_AndFreedAfterTheLast()
    {
        IpcFile file = IpcFile.Open(Path("pyarrow/dictionary.arrow"));
        IpcReader reader = file.OpenReader(new IpcReadOptions { Columns = ["dict_int8"] });
        RecordBatch first = reader.ReadBatch(0), second = reader.ReadBatch(1);
        NativeMemoryOwner dictionary = Owner(((DictionaryArray)first.Column(0)).Dictionary);
        Assert.Same(dictionary, Owner(((DictionaryArray)second.Column(0)).Dictionary));
        reader.Dispose();
        file.Dispose();
        first.Dispose();
        Assert.False(dictionary.IsFreed);
        Assert.StartsWith("u:", CanonicalRender.Value(second.Column(0), 0));
        second.Dispose();
        Assert.True(dictionary.IsFreed);
    }

    [Fact]
    public void ReadBatch_ReadsAnyBatchOnItsOwn()
    {
        var (_, all) = IpcValueTests.Read("pyarrow/many_batches.arrow", new IpcReadOptions());
        using IpcFile file = IpcFile.Open(Path("pyarrow/many_batches.arrow"));
        Assert.Equal(16, file.BatchCount);
        foreach (int index in (int[])[7, 0, 15, 3])
        {
            using RecordBatch batch = file.ReadBatch(index);
            Assert.Equal(all[index][1], Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(1), i)));
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => file.ReadBatch(16));
    }

    [Fact]
    public void Metadata_IsReadAtEveryLevel()
    {
        using IpcFile file = IpcFile.Open(Path("pyarrow/metadata.arrow"));
        Assert.Equal("schema v", file.Schema.Metadata["schema:k"]);
        Assert.Equal("", file.Schema.Metadata["empty"]);
        Assert.Equal("field v", file.Schema.FieldsList[0].Metadata["field:k"]);
        Assert.Equal("çödé", file.Schema.FieldsList[0].Metadata["ünï"]);
        Assert.False(file.Schema.FieldsList[0].IsNullable);
        Assert.Equal("inner v", ((Apache.Arrow.Types.StructType)file.Schema.FieldsList[1].DataType).Fields[0].Metadata["inner:k"]);
        Assert.Equal("first", file.GetBatchMetadata(0)["batch:k"]);
        Assert.Empty(file.GetBatchMetadata(1));
        Assert.Equal("2", file.GetBatchMetadata(2)["n"]);

        using var stream = IpcStreamReader.Open(File.OpenRead(Path("pyarrow/metadata.arrows")));
        Assert.Equal("schema v", stream.Schema.Metadata["schema:k"]);
        string[] seen = [.. stream.ReadBatches().Select(b =>
        {
            b.Dispose();
            return stream.BatchMetadata.TryGetValue("batch:k", out string? v) ? v : "-";
        })];
        Assert.Equal(["first", "-", "third"], seen);
    }

    [Fact]
    public void Formats_AreToldApart()
    {
        // Streams open as files too (their messages scanned); the file format is told by its magic.
        using (IpcFile stream = IpcFile.Open(Path("pyarrow/primitives.arrows"))) Assert.True(stream.IsStream);
        using (IpcFile file = IpcFile.Open(Path("pyarrow/primitives.arrow"))) Assert.False(file.IsStream);
        Assert.Contains("an Arrow IPC file, not a stream", Assert.Throws<IpcFormatException>(() => IpcStreamReader.Open(File.OpenRead(Path("pyarrow/primitives.arrow")))).Message);
        Assert.Contains("Feather v1", Assert.Throws<IpcFormatException>(() => IpcFile.Open(Path("pyarrow/feather_v1.feather"))).Message);
        Assert.Contains("Feather v1", Assert.Throws<IpcFormatException>(() => IpcStreamReader.Open(File.OpenRead(Path("pyarrow/feather_v1.feather")))).Message);
        Assert.Contains("not an Arrow IPC file", Assert.Throws<IpcFormatException>(() => IpcFile.Open(new byte[100])).Message);
        Assert.Contains("is empty", Assert.Throws<IpcFormatException>(() => IpcStreamReader.Open(new MemoryStream())).Message);
    }

    [Fact]
    public void DamagedFooters_AreRefused()
    {
        byte[] data = File.ReadAllBytes(Path("pyarrow/primitives.arrow"));
        Assert.Throws<IpcFormatException>(() => IpcFile.Open(data[..^20]));
        byte[] longFooter = (byte[])data.Clone();
        BitConverter.TryWriteBytes(longFooter.AsSpan(longFooter.Length - 10), int.MaxValue);
        Assert.Contains("does not fit", Assert.Throws<IpcFormatException>(() => IpcFile.Open(longFooter)).Message);
        byte[] noHead = (byte[])data.Clone();
        noHead[0] = (byte)'X';
        Assert.Contains("does not start with ARROW1", Assert.Throws<IpcFormatException>(() => IpcFile.Open(noHead)).Message);
        // A stream cut inside a body.
        byte[] stream = File.ReadAllBytes(Path("pyarrow/primitives.arrows"));
        using var reader = IpcStreamReader.Open(new MemoryStream(stream[..(stream.Length / 2)]));
        Assert.Contains("ends inside", Assert.Throws<IpcFormatException>(() => reader.ReadBatches().ToList()).Message);
    }

    [Fact]
    public void Options_AreChecked()
    {
        using IpcFile file = IpcFile.Open(Path("pyarrow/primitives.arrow"));
        Assert.Contains("no column 'nope'", Assert.Throws<ArgumentException>(() => file.OpenReader(new IpcReadOptions { Columns = ["i8", "nope"] })).Message);
        Assert.Throws<ArgumentOutOfRangeException>(() => file.OpenReader(new IpcReadOptions { MaxRows = -1 }));
        using IpcReader none = file.OpenReader(new IpcReadOptions { MaxRows = 0 });
        Assert.Empty(none.ReadBatches());
    }

    [Fact]
    public async Task ArrowStream_HandsOutEachBatchUntilTheNext()
    {
        using IpcFile file = IpcFile.Open(Path("pyarrow/many_batches.arrow"));
        using IpcReader reader = file.OpenReader();
        using IArrowArrayStream stream = reader.ReadArrowStream();
        int batches = 0;
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            Assert.Equal(100, batch.Length);
            batches++;
        }
        Assert.Equal(16, batches);
    }

    /// <summary>
    /// Apache.Arrow's own reader (no compression codecs) reads the uncompressed files alike, except where it cannot read
    /// a type; the files it fails on are listed.
    /// </summary>
    [Fact]
    public void ApacheReader_ReadsTheUncompressedFilesAlike()
    {
        var json = IpcValueTests.Oracle.Value.RootElement.GetProperty("files");
        var messages = System.Text.Json.JsonDocument.Parse(File.ReadAllBytes(System.IO.Path.Combine(IpcValueTests.Root, "messages.json"))).RootElement.GetProperty("files");
        List<string> apacheFails = [], compared = [], differ = [];
        foreach (var entry in json.EnumerateObject())
        {
            string name = entry.Name;
            if (name.StartsWith("apache/fuzz/") || name.Contains("bigendian") || name.EndsWith("feather_v1.feather")) continue;
            if (messages.GetProperty(name).GetProperty("messages").EnumerateArray().Any(m => m.TryGetProperty("compression", out _))) continue;
            List<List<string>> theirs;
            try
            {
                theirs = ReadApache(name);
            }
            catch (Exception)
            {
                apacheFails.Add(name);
                continue;
            }
            var (_, ours) = IpcValueTests.Read(name, new IpcReadOptions());
            List<List<string>> flat = [.. Enumerable.Range(0, theirs.Count).Select(c => ours.SelectMany(b => b[c]).ToList())];
            compared.Add(name);
            if (!flat.Zip(theirs).All(p => p.First.SequenceEqual(p.Second))) differ.Add(name);
        }
        Assert.Empty(differ);
        Assert.True(compared.Count > 100, $"only {compared.Count} files compared");
        Assert.Equal(ApacheCannotRead, apacheFails.Order());
    }

    /// <summary>The uncompressed test files Apache.Arrow 23's reader fails on (we read them all).</summary>
    private static readonly string[] ApacheCannotRead =
    [
        "polars/int128.arrow",              // Arrow Int of 128 bits (Polars' Int128/UInt128), not an Arrow type
        "pyarrow/dictionary_delta.arrows",  // delta dictionaries in a stream
    ];

    private static List<List<string>> ReadApache(string name)
    {
        using var stream = File.OpenRead(Path(name));
        using ArrowStreamReader reader = IpcValueTests.IsFile(name) ? new ArrowFileReader(stream) : new ArrowStreamReader(stream);
        List<List<string>> columns = [];
        while (reader.ReadNextRecordBatch() is { } batch)
        {
            using (batch)
            {
                while (columns.Count < batch.ColumnCount) columns.Add([]);
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    for (int i = 0; i < batch.Length; i++) columns[c].Add(CanonicalRender.Value(batch.Column(c), i));
                }
            }
        }
        return columns;
    }

    [Fact]
    public void Lz4Frame_RefusesDamage_AndReadsWhatItWasGiven()
    {
        // A frame from a file Polars wrote (linked blocks, block and content checksums): the largest buffer of its batch,
        // located through the footer block and the buffer list of messages.json.
        string name = "polars/big_lz4.arrow";
        byte[] file = File.ReadAllBytes(Path(name));
        var described = System.Text.Json.JsonDocument.Parse(File.ReadAllBytes(System.IO.Path.Combine(IpcValueTests.Root, "messages.json"))).RootElement
            .GetProperty("files").GetProperty(name);
        var block = described.GetProperty("footer").GetProperty("recordBatches")[0];
        long body = block[0].GetInt64() + block[1].GetInt64();
        var message = described.GetProperty("messages")[0];
        int largest = Enumerable.Range(0, message.GetProperty("buffers").GetArrayLength())
            .MaxBy(i => message.GetProperty("prefixes")[i].ValueKind == System.Text.Json.JsonValueKind.Number ? message.GetProperty("prefixes")[i].GetInt64() : 0);
        long offset = message.GetProperty("buffers")[largest][0].GetInt64(), size = message.GetProperty("buffers")[largest][1].GetInt64();
        long expanded = message.GetProperty("prefixes")[largest].GetInt64();
        Assert.True(expanded > 1 << 16, "a frame of several blocks");
        byte[] frame = file[(int)(body + offset + 8)..(int)(body + offset + size)];
        byte[] output = new byte[expanded];
        Assert.Equal(expanded, Lz4Frame.Decompress(frame, output));
        var random = new Random(5);
        for (int round = 0; round < 2000; round++)
        {
            byte[] damaged = (byte[])frame.Clone();
            damaged[random.Next(damaged.Length)] ^= (byte)(1 << random.Next(8));
            Exception? e = Record.Exception(() => Lz4Frame.Decompress(damaged.AsSpan(0, random.Next(4) == 0 ? random.Next(damaged.Length) : damaged.Length), new byte[expanded]));
            if (e is not null) Assert.IsType<InvalidDataException>(e);
        }

        // Damage the block checksums let through (each recomputed) is caught by the content checksum, taken block by
        // block as the frame is decoded: in the first block, a later one, and the checksum itself.
        Assert.Equal(0x40, frame[4] & 0xC0);
        Assert.Equal(0x10 | 0x04, frame[4] & 0x14); // block and content checksums
        List<(int Data, int Size)> blocks = [];
        for (int ip = 7; ;)
        {
            int word = BitConverter.ToInt32(frame, ip);
            if (word == 0) break;
            int length = word & 0x7FFFFFFF;
            blocks.Add((ip + 4, length));
            ip += 4 + length + 4;
        }
        Assert.True(blocks.Count > 2);
        int caught = 0;
        foreach (var (data, length) in (ValueTuple<int, int>[])[blocks[0], blocks[^2]])
        {
            for (int round = 0; round < 200; round++)
            {
                byte[] damaged = (byte[])frame.Clone();
                damaged[data + random.Next(length)] ^= (byte)(1 << random.Next(8));
                System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(damaged.AsSpan(data + length),
                    System.IO.Hashing.XxHash32.HashToUInt32(damaged.AsSpan(data, length)));
                byte[] written = new byte[expanded];
                Exception? e = Record.Exception(() => Lz4Frame.Decompress(damaged, written));
                if (e is null)
                {
                    // Read: only when the damage left the output as it was (a match copying equal bytes, say).
                    Assert.True(written.AsSpan().SequenceEqual(output), $"damage at {round} read as valid with other output");
                    continue;
                }
                Assert.IsType<InvalidDataException>(e);
                if (e.Message.Contains("content checksum")) caught++;
            }
        }
        Assert.True(caught > 50, $"{caught} caught by the content checksum");
        byte[] wrongSum = (byte[])frame.Clone();
        wrongSum[^1] ^= 1;
        Assert.Contains("content checksum", Assert.Throws<InvalidDataException>(() => Lz4Frame.Decompress(wrongSum, new byte[expanded])).Message);
    }
}
