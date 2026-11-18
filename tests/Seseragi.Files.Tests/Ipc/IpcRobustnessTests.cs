using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// Damaged and invalid IPC data is refused with <see cref="IpcFormatException"/>, never read as garbage nor crashing:
/// the crash reproducers of the Arrow C++ fuzzers, random damage to valid files, and arrays that break each rule the
/// reader checks (written with Apache.Arrow's writer, which does not check them).
/// </summary>
public class IpcRobustnessTests
{
    public static TheoryData<string> FuzzFiles => [.. IpcValueTests.Oracle.Value.RootElement.GetProperty("files").EnumerateObject()
        .Select(p => p.Name).Where(n => n.StartsWith("apache/fuzz/"))];

    /// <summary>The fuzz files pyarrow reads cleanly: read here too, value for value.</summary>
    private static readonly string[] ReadCleanly =
    [
        "apache/fuzz/file/clusterfuzz-testcase-arrow-ipc-file-fuzz-6051391008473088",
        "apache/fuzz/file/clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-6088759971217408",
        "apache/fuzz/file/clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-6295340960776192",
        "apache/fuzz/stream/clusterfuzz-testcase-minimized-arrow-ipc-stream-fuzz-5718685113384960",
    ];

    /// <summary>Of those, the ones refused here by design, with the reason: big-endian data (pyarrow swaps the bytes), a
    /// malformed schema (a LargeUtf8 field with a child, which pyarrow ignores).</summary>
    private static readonly Dictionary<string, string> RefusedByDesign = new()
    {
        ["apache/fuzz/file/clusterfuzz-testcase-arrow-ipc-file-fuzz-6051391008473088"] = "Big-endian",
        ["apache/fuzz/stream/clusterfuzz-testcase-minimized-arrow-ipc-stream-fuzz-5718685113384960"] = "has 1 children; 0 expected",
    };

    [Theory]
    [MemberData(nameof(FuzzFiles))]
    public void FuzzFile_IsRefusedOrRead(string name)
    {
        byte[] data = File.ReadAllBytes(Path.Combine(IpcValueTests.Root, name));
        JsonElement pyarrow = IpcValueTests.Oracle.Value.RootElement.GetProperty("files").GetProperty(name).GetProperty("pyarrow");
        Assert.Equal(ReadCleanly.Contains(name), !pyarrow.TryGetProperty("error", out _) && !pyarrow.TryGetProperty("invalid", out _));
        foreach (bool seekable in (bool[])[true, false])
        {
            List<string>[]? columns = null;
            Exception? e = Record.Exception(() => columns = ReadAny(data, seekable));
            if (!ReadCleanly.Contains(name))
            {
                if (e is not null) Assert.True(e is IpcFormatException, $"{name}: {e.GetType().Name}: {e.Message}");
                continue;
            }
            if (RefusedByDesign.TryGetValue(name, out string? reason))
            {
                Assert.Contains(reason, Assert.IsType<IpcFormatException>(e).Message);
                continue;
            }
            // What pyarrow reads cleanly is read here, value for value.
            Assert.Null(e);
            JsonElement[] expected = [.. pyarrow.GetProperty("columns").EnumerateArray()];
            Assert.Equal(expected.Length, columns!.Length);
            for (int c = 0; c < expected.Length; c++) Assert.Equal(expected[c].GetProperty("hash").GetString(), CanonicalRender.Hash(columns[c]));
        }
    }

    /// <summary>Reads data of either format, rendering every value; returns the rendered columns. A stream is read
    /// through a stream that cannot seek unless <paramref name="seekable"/> (forged lengths are then not bounded by
    /// the stream's length).</summary>
    internal static List<string>[] ReadAny(byte[] data, bool seekable = true)
    {
        List<List<string>> columns = [];
        IEnumerable<RecordBatch> batches;
        IDisposable owner;
        Schema schema;
        if (data.Length >= 6 && data.AsSpan(0, 6).SequenceEqual("ARROW1"u8))
        {
            var file = IpcFile.Open(data);
            IpcReader reader = file.OpenReader();
            (batches, schema, owner) = (reader.ReadBatches(), reader.Schema, new Both(reader, file));
        }
        else
        {
            IpcStreamReader reader = IpcStreamReader.Open(seekable ? new MemoryStream(data) : new IpcValueTests.NonSeekable(new MemoryStream(data)));
            (batches, schema, owner) = (reader.ReadBatches(), reader.Schema, reader);
        }
        using (owner)
        {
            for (int c = 0; c < schema.FieldsList.Count; c++) columns.Add([]);
            foreach (RecordBatch batch in batches)
            {
                using (batch)
                {
                    for (int c = 0; c < batch.ColumnCount; c++)
                    {
                        for (int i = 0; i < batch.Length; i++) columns[c].Add(CanonicalRender.Value(batch.Column(c), i));
                    }
                }
            }
        }
        return [.. columns];
    }

    private sealed class Both(IDisposable a, IDisposable b) : IDisposable
    {
        public void Dispose()
        {
            a.Dispose();
            b.Dispose();
        }
    }

    /// <summary>Valid files with bytes changed at random: read (every value rendered) or refused, never anything else.</summary>
    [Theory]
    [InlineData("pyarrow/primitives.arrow")]
    [InlineData("pyarrow/strings.arrows")]
    [InlineData("pyarrow/nested.arrow")]
    [InlineData("pyarrow/unions.arrow")]
    [InlineData("pyarrow/ree.arrows")]
    [InlineData("pyarrow/dictionary_delta.arrows")]
    [InlineData("pyarrow/dictionary.arrow")]
    [InlineData("pyarrow/lz4.arrow")]
    [InlineData("pyarrow/zstd.arrows")]
    [InlineData("polars/frame_lz4.arrow")]
    [InlineData("polars/frame.arrows")]
    [InlineData("apache/integration/cpp-21.0.0/generated_list_view.arrow_file")]
    [InlineData("apache/integration/cpp-21.0.0/generated_binary_view.stream")]
    [InlineData("apache/integration/0.14.1/generated_nested.stream")]
    public void DamagedFile_IsRefusedOrRead(string name)
    {
        byte[] original = File.ReadAllBytes(Path.Combine(IpcValueTests.Root, name));
        var random = new Random(name.GetHashCode(StringComparison.Ordinal) & 0xFFFF);
        for (int round = 0; round < 300; round++)
        {
            byte[] damaged = (byte[])original.Clone();
            int changes = 1 + random.Next(4);
            for (int k = 0; k < changes; k++)
            {
                int at = random.Next(damaged.Length);
                damaged[at] = random.Next(3) switch
                {
                    0 => (byte)(damaged[at] ^ (1 << random.Next(8))),
                    1 => (byte)random.Next(256),
                    _ => (byte)(damaged[at] + random.Next(-2, 3)),
                };
            }
            if (random.Next(10) == 0) damaged = damaged[..random.Next(damaged.Length)];
            Exception? e = Record.Exception(() => ReadAny(damaged, seekable: round % 2 == 0));
            if (e is not null && e is not IpcFormatException) Assert.Fail($"{name}, round {round}: {e.GetType().Name}: {e.Message}\n{e.StackTrace}");
        }
    }

    // ------------------------------------------------------------------
    // Arrays that break each rule (Apache.Arrow's writer does not check them)
    // ------------------------------------------------------------------

    private static ArrowBuffer Ints(params int[] values) => new(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()).ToArray());

    private static ArrowBuffer Longs(params long[] values) => new(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()).ToArray());

    private static ArrowBuffer Bytes(params byte[] values) => new(values);

    /// <summary>Writes one batch of <paramref name="data"/> as an IPC stream with Apache.Arrow's writer and reads it back here.</summary>
    private static IpcFormatException Refuse(ArrayData data, string name = "c")
    {
        IArrowArray array = ArrowArrayFactory.BuildArray(data);
        var schema = new Schema([new Field(name, data.DataType, true)], null);
        using var batch = new RecordBatch(schema, [array], data.Length);
        var bytes = new MemoryStream();
        using (var writer = new ArrowStreamWriter(bytes, schema, leaveOpen: true)) writer.WriteRecordBatch(batch);
        return Assert.Throws<IpcFormatException>(() => ReadAny(bytes.ToArray()));
    }

    [Fact]
    public void StringOffsets_MustNotDecrease()
    {
        var e = Refuse(new ArrayData(StringType.Default, 3, 0, 0, [ArrowBuffer.Empty, Ints(0, 3, 1, 4), Bytes("abcd"u8.ToArray())]));
        Assert.Contains("offsets decrease", e.Message);
    }

    [Fact]
    public void StringOffsets_MustStayInTheData()
    {
        var e = Refuse(new ArrayData(StringType.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(0, 2, 9), Bytes("abcd"u8.ToArray())]));
        Assert.Contains("past its data", e.Message);
    }

    [Fact]
    public void StringOffsets_MustStartAtZeroOrMore()
    {
        // Apache.Arrow's writer rebases offsets: a valid stream is written, then its first offset patched to -1.
        var data = new ArrayData(StringType.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(0, 2, 3), Bytes("abcd"u8.ToArray())]);
        var schema = new Schema([new Field("c", StringType.Default, true)], null);
        using var batch = new RecordBatch(schema, [ArrowArrayFactory.BuildArray(data)], 2);
        var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true)) writer.WriteRecordBatch(batch);
        byte[] bytes = stream.ToArray();
        int at = bytes.AsSpan().IndexOf((ReadOnlySpan<byte>)[0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]);
        Assert.True(at > 0);
        bytes.AsSpan(at, 4).Fill(0xFF);
        Assert.Contains("first offset", Assert.Throws<IpcFormatException>(() => ReadAny(bytes)).Message);
    }

    [Fact]
    public void LargeListOffsets_MustStayInTheChild()
    {
        var child = new ArrayData(Int32Type.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(1, 2)]);
        var e = Refuse(new ArrayData(new LargeListType(Int32Type.Default), 2, 0, 0, [ArrowBuffer.Empty, Longs(0, 1, 5)], [child]));
        Assert.Contains("past its data", e.Message);
    }

    [Fact]
    public void NullCount_MustMatchTheBitmap()
    {
        var e = Refuse(new ArrayData(Int32Type.Default, 4, 1, 0, [Bytes(0b1111), Ints(1, 2, 3, 4)]));
        Assert.Contains("does not match its validity bitmap", e.Message);
    }

    [Fact]
    public void Values_MustCoverTheLength()
    {
        var e = Refuse(new ArrayData(Int64Type.Default, 4, 0, 0, [ArrowBuffer.Empty, Longs(1, 2)]));
        Assert.Contains("are needed", e.Message);
    }

    [Fact]
    public void DictionaryIndices_MustBeInTheDictionary()
    {
        var dictionary = new ArrayData(StringType.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(0, 1, 2), Bytes("ab"u8.ToArray())]);
        var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var e = Refuse(new ArrayData(type, 3, 0, 0, [ArrowBuffer.Empty, Ints(0, 1, 2)], [], dictionary));
        Assert.Contains("outside its 2 values", e.Message);
        // A null slot's index is not looked at.
        var nullSlot = new ArrayData(type, 3, 1, 0, [Bytes(0b011), Ints(0, 1, 99)], [], dictionary);
        IArrowArray array = ArrowArrayFactory.BuildArray(nullSlot);
        var schema = new Schema([new Field("c", type, true)], null);
        using var batch = new RecordBatch(schema, [array], 3);
        var bytes = new MemoryStream();
        using (var writer = new ArrowStreamWriter(bytes, schema, leaveOpen: true)) writer.WriteRecordBatch(batch);
        Assert.Equal(["u:61", "u:62", "null"], ReadAny(bytes.ToArray())[0]);
    }

    private static ArrowBuffer View(int length, string inline) // a view of up to 12 bytes, inline
    {
        byte[] view = new byte[16];
        BitConverter.TryWriteBytes(view, length);
        System.Text.Encoding.ASCII.GetBytes(inline).CopyTo(view, 4);
        return new ArrowBuffer(view);
    }

    private static ArrowBuffer View(int length, string prefix, int buffer, int offset)
    {
        byte[] view = new byte[16];
        BitConverter.TryWriteBytes(view, length);
        System.Text.Encoding.ASCII.GetBytes(prefix).CopyTo(view, 4);
        BitConverter.TryWriteBytes(view.AsSpan(8), buffer);
        BitConverter.TryWriteBytes(view.AsSpan(12), offset);
        return new ArrowBuffer(view);
    }

    [Fact]
    public void Views_MustPointIntoTheirBuffers()
    {
        ArrowBuffer data = Bytes("a long string value here"u8.ToArray());
        Assert.Contains("refers to data buffer 3", Refuse(new ArrayData(StringViewType.Default, 1, 0, 0, [ArrowBuffer.Empty, View(20, "a lo", 3, 0), data])).Message);
        Assert.Contains("reaches past data buffer", Refuse(new ArrayData(StringViewType.Default, 1, 0, 0, [ArrowBuffer.Empty, View(20, "a lo", 0, 10), data])).Message);
        Assert.Contains("prefix differs", Refuse(new ArrayData(StringViewType.Default, 1, 0, 0, [ArrowBuffer.Empty, View(20, "xxxx", 0, 0), data])).Message);
        Assert.Contains("has length -3", Refuse(new ArrayData(StringViewType.Default, 1, 0, 0, [ArrowBuffer.Empty, View(-3, ""), data])).Message);
    }

    [Fact]
    public void ListViews_MustStayInTheChild()
    {
        var child = new ArrayData(Int32Type.Default, 3, 0, 0, [ArrowBuffer.Empty, Ints(1, 2, 3)]);
        var type = new ListViewType(Int32Type.Default);
        Assert.Contains("reaches past its 3 items", Refuse(new ArrayData(type, 2, 0, 0, [ArrowBuffer.Empty, Ints(0, 2), Ints(1, 2)], [child])).Message);
        Assert.Contains("has size -1", Refuse(new ArrayData(type, 2, 0, 0, [ArrowBuffer.Empty, Ints(0, 1), Ints(1, -1)], [child])).Message);
    }

    [Fact]
    public void UnionTypeIds_MustBeDeclared_AndDenseOffsetsInTheirChild()
    {
        var ints = new ArrayData(Int32Type.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(1, 2)]);
        var sparse = new UnionType([new Field("i", Int32Type.Default, true)], [5], UnionMode.Sparse);
        Assert.Contains("type id 6", Refuse(new ArrayData(sparse, 2, 0, 0, [Bytes(5, 6)], [ints])).Message);
        var dense = new UnionType([new Field("i", Int32Type.Default, true)], [5], UnionMode.Dense);
        Assert.Contains("offset 2 in a child of 2", Refuse(new ArrayData(dense, 2, 0, 0, [Bytes(5, 5), Ints(0, 2)], [ints])).Message);
    }

    [Fact]
    public void RunEnds_MustIncrease()
    {
        var type = new RunEndEncodedType(Int32Type.Default, Int64Type.Default);
        var values = new ArrayData(Int64Type.Default, 2, 0, 0, [ArrowBuffer.Empty, Longs(7, 8)]);
        Assert.Contains("do not increase", Refuse(new ArrayData(type, 4, 0, 0, [], [new ArrayData(Int32Type.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(3, 3)]), values])).Message);
        Assert.Contains("before its 4 rows", Refuse(new ArrayData(type, 4, 0, 0, [], [new ArrayData(Int32Type.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(1, 3)]), values])).Message);
        Assert.Contains("first run ends at 0", Refuse(new ArrayData(type, 4, 0, 0, [], [new ArrayData(Int32Type.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(0, 4)]), values])).Message);
    }

    [Fact]
    public void StructChildren_MustCoverTheLength()
    {
        var child = new ArrayData(Int32Type.Default, 1, 0, 0, [ArrowBuffer.Empty, Ints(1)]);
        var type = new StructType([new Field("a", Int32Type.Default, true)]);
        Assert.Contains("a struct child of 1 values for 2", Refuse(new ArrayData(type, 2, 0, 0, [ArrowBuffer.Empty], [child])).Message);
    }

    [Fact]
    public void MapKeys_MustNotBeNull()
    {
        var keys = new ArrayData(StringType.Default, 1, 1, 0, [Bytes(0), Ints(0, 0), ArrowBuffer.Empty]);
        var items = new ArrayData(Int32Type.Default, 1, 0, 0, [ArrowBuffer.Empty, Ints(1)]);
        var type = new MapType(StringType.Default, Int32Type.Default);
        var entries = new ArrayData(type.KeyValueType, 1, 0, 0, [ArrowBuffer.Empty], [keys, items]);
        Assert.Contains("a map key is null", Refuse(new ArrayData(type, 1, 0, 0, [ArrowBuffer.Empty, Ints(0, 1)], [entries])).Message);
    }

    [Fact]
    public void Utf8_IsCheckedOnlyWhenAsked()
    {
        var data = new ArrayData(StringType.Default, 1, 0, 0, [ArrowBuffer.Empty, Ints(0, 2), Bytes(0xC3, 0x28)]);
        var schema = new Schema([new Field("c", StringType.Default, true)], null);
        using var batch = new RecordBatch(schema, [ArrowArrayFactory.BuildArray(data)], 1);
        var bytes = new MemoryStream();
        using (var writer = new ArrowStreamWriter(bytes, schema, leaveOpen: true)) writer.WriteRecordBatch(batch);
        using (var reader = IpcStreamReader.Open(new MemoryStream(bytes.ToArray())))
        using (RecordBatch read = reader.ReadNext()!) Assert.Equal("u:c328", CanonicalRender.Value(read.Column(0), 0));
        using var strict = IpcStreamReader.Open(new MemoryStream(bytes.ToArray()), new IpcReadOptions { ValidateUtf8 = true });
        Assert.Contains("not UTF-8", Assert.Throws<IpcFormatException>(() => strict.ReadNext()).Message);
    }
}

/// <summary>The decoder's own checks, on headers and bodies made by hand.</summary>
public class IpcDecoderTests
{
    private static Kaminari.Sazanami.Arrow.Ipc.IpcField Int32Field => new(new Field("a", Int32Type.Default, true), null, [], Kaminari.Sazanami.Arrow.Ipc.IpcConversion.None);

    [Fact]
    public void ABatch_MustHoldExactlyTheNodesAndBuffersItsSchemaTakes()
    {
        Kaminari.Sazanami.Arrow.Ipc.IpcRecordBatch Header(int nodes, int buffers) => new(1, [.. Enumerable.Repeat(new Kaminari.Sazanami.Arrow.Ipc.IpcFieldNode(1, 0), nodes)],
            [.. Enumerable.Repeat(new Kaminari.Sazanami.Arrow.Ipc.IpcBuffer(0, 8), buffers)], Kaminari.Sazanami.Arrow.Ipc.IpcCompression.None, []);
        Kaminari.Sazanami.Arrow.Ipc.IpcBatchDecoder.Plan([Int32Field], null, Header(1, 2), Kaminari.Sazanami.Arrow.Ipc.IpcMetadataVersion.V5, 8);
        Assert.Contains("its schema takes", Assert.Throws<InvalidDataException>(() => Kaminari.Sazanami.Arrow.Ipc.IpcBatchDecoder.Plan([Int32Field], null, Header(2, 2), Kaminari.Sazanami.Arrow.Ipc.IpcMetadataVersion.V5, 8)).Message);
        Assert.Contains("its schema takes", Assert.Throws<InvalidDataException>(() => Kaminari.Sazanami.Arrow.Ipc.IpcBatchDecoder.Plan([Int32Field], null, Header(1, 3), Kaminari.Sazanami.Arrow.Ipc.IpcMetadataVersion.V5, 8)).Message);
        Assert.Contains("too few buffers", Assert.Throws<InvalidDataException>(() => Kaminari.Sazanami.Arrow.Ipc.IpcBatchDecoder.Plan([Int32Field], null, Header(1, 1), Kaminari.Sazanami.Arrow.Ipc.IpcMetadataVersion.V5, 8)).Message);
        Assert.Contains("lies outside", Assert.Throws<InvalidDataException>(() => Kaminari.Sazanami.Arrow.Ipc.IpcBatchDecoder.Plan([Int32Field], null, Header(1, 2), Kaminari.Sazanami.Arrow.Ipc.IpcMetadataVersion.V5, 4)).Message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public unsafe void ACompressedBuffer_MustExpandToItsDeclaredLength(bool zstd)
    {
        var codec = zstd ? Kaminari.Sazanami.Arrow.Ipc.IpcCompression.Zstd : Kaminari.Sazanami.Arrow.Ipc.IpcCompression.Lz4Frame;
        byte[] plain = [.. Enumerable.Range(0, 64).Select(i => (byte)(i % 7))];
        // A frame of the codec, from a file of it, is not needed: Zstandard frames are made here; for LZ4 a frame of stored blocks.
        byte[] frame = codec == Kaminari.Sazanami.Arrow.Ipc.IpcCompression.Zstd ? Zstd(plain) : StoredLz4Frame(plain);
        foreach (long declared in (long[])[64, 65, 100])
        {
            byte[] buffer = [.. BitConverter.GetBytes(declared), .. frame];
            var memory = new Kaminari.Sazanami.Arrow.Ipc.IpcBodyMemory();
            byte* p = memory.Allocate(buffer.Length);
            buffer.CopyTo(new Span<byte>(p, buffer.Length));
            var body = new Kaminari.Sazanami.Arrow.Ipc.IpcBody(memory, buffer.Length, [new(0, (nint)p, buffer.Length)]);
            if (declared == 64) Assert.Equal(64, body.Slice(new(0, buffer.Length), codec, "c").Length);
            else Assert.Throws<InvalidDataException>(() => body.Slice(new(0, buffer.Length), codec, "c"));
            memory.Sealed();
        }
    }

    private static byte[] Zstd(byte[] plain)
    {
        byte[] output = new byte[Kaminari.Sazanami.Compression.ZstdBlock.MaxCompressedLength(plain.Length)];
        return output[..Kaminari.Sazanami.Compression.ZstdBlock.Compress(plain, output)];
    }

    /// <summary>An LZ4 frame (no checksums, no content size) of one stored block.</summary>
    private static byte[] StoredLz4Frame(byte[] plain)
    {
        byte[] descriptor = [0x60, 0x40]; // version 1, independent blocks; 64 KiB blocks
        byte check = (byte)(System.IO.Hashing.XxHash32.HashToUInt32(descriptor) >> 8);
        return [0x04, 0x22, 0x4D, 0x18, .. descriptor, check, .. BitConverter.GetBytes(plain.Length | int.MinValue), .. plain, 0, 0, 0, 0];
    }
}

/// <summary>The decisions on edge cases of the format, on data made by hand: late dictionaries, V4 unions, replacements in files, sizes, alignment, projection.</summary>
public class IpcEdgeCaseTests
{
    /// <summary>The messages of a stream: each prefix, metadata and body as one piece.</summary>
    private static List<byte[]> Messages(byte[] stream)
    {
        List<byte[]> messages = [];
        int at = 0;
        while (at + 8 <= stream.Length)
        {
            int length = BitConverter.ToInt32(stream, at + 4);
            if (length == 0) break;
            var message = Kaminari.Sazanami.Arrow.Ipc.IpcMetadata.ParseMessage(stream.AsSpan(at + 8, length));
            int end = at + 8 + length + (int)message.BodyLength;
            messages.Add(stream[at..end]);
            at = end;
        }
        return messages;
    }

    private static byte[] Write(Schema schema, params RecordBatch[] batches)
    {
        var bytes = new MemoryStream();
        using (var writer = new ArrowStreamWriter(bytes, schema, leaveOpen: true))
        {
            foreach (RecordBatch batch in batches) writer.WriteRecordBatch(batch);
        }
        return bytes.ToArray();
    }

    private static ArrowBuffer Ints(params int[] values) => new(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()).ToArray());

    [Fact]
    public void AStreamDictionary_MayComeAfterAnAllNullBatch_ButNotAfterOneThatUsesIt()
    {
        var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var schema = new Schema([new Field("d", type, true)], null);
        var dictionary = new ArrayData(StringType.Default, 2, 0, 0, [ArrowBuffer.Empty, Ints(0, 1, 2), new ArrowBuffer("ab"u8.ToArray())]);
        using var allNull = new RecordBatch(schema, [ArrowArrayFactory.BuildArray(new ArrayData(type, 2, 2, 0, [new ArrowBuffer(new byte[1]), Ints(0, 0)], [], dictionary))], 2);
        using var used = new RecordBatch(schema, [ArrowArrayFactory.BuildArray(new ArrayData(type, 2, 0, 0, [ArrowBuffer.Empty, Ints(1, 0)], [], dictionary))], 2);
        List<byte[]> m = Messages(Write(schema, allNull, used)); // schema, dictionary, batch, batch
        Assert.Equal(4, m.Count);
        byte[] late = [.. m[0], .. m[2], .. m[1], .. m[3]];   // the dictionary after the all-null batch
        Assert.Equal(["null", "null", "u:62", "u:61"], IpcRobustnessTests.ReadAny(late)[0]);
        byte[] early = [.. m[0], .. m[3], .. m[1]];             // a batch using it before it comes
        Assert.Contains("used before it is defined", Assert.Throws<IpcFormatException>(() => IpcRobustnessTests.ReadAny(early)).Message);
    }

    [Fact]
    public void AFile_RefusesASecondDictionaryOfAnId_UnlessADelta()
    {
        // pyarrow's replacement stream, its dictionary messages applied as a file's would be.
        byte[] stream = File.ReadAllBytes(Path.Combine(IpcValueTests.Root, "pyarrow/dictionary_replacement.arrows"));
        List<byte[]> m = Messages(stream);
        var schema = Kaminari.Sazanami.Arrow.Ipc.IpcMetadata.ParseMessage(m[0].AsSpan(8, BitConverter.ToInt32(m[0], 4))).Schema!;
        using var dictionaries = new Kaminari.Sazanami.Arrow.Ipc.IpcDictionaries();
        int applied = 0;
        foreach (byte[] piece in m)
        {
            int length = BitConverter.ToInt32(piece, 4);
            var message = Kaminari.Sazanami.Arrow.Ipc.IpcMetadata.ParseMessage(piece.AsSpan(8, length));
            if (message.Kind != Kaminari.Sazanami.Arrow.Ipc.IpcMessageKind.DictionaryBatch) continue;
            var body = Body(piece[(8 + length)..]);
            if (applied++ == 0) Kaminari.Seseragi.Files.Ipc.IpcIO.Dictionary(schema, message, body, dictionaries, message.Version, false, replacements: false);
            else
            {
                Assert.Contains("the file format does not allow", Assert.Throws<IpcFormatException>(() =>
                    Kaminari.Seseragi.Files.Ipc.IpcIO.Dictionary(schema, message, body, dictionaries, message.Version, false, replacements: false)).Message);
                break;
            }
        }
        Assert.Equal(2, applied);
    }

    private static unsafe Kaminari.Sazanami.Arrow.Ipc.IpcBody Body(byte[] bytes, int shift = 0)
    {
        var memory = new Kaminari.Sazanami.Arrow.Ipc.IpcBodyMemory();
        byte* p = memory.Allocate(bytes.Length + shift) + shift;
        bytes.CopyTo(new Span<byte>(p, bytes.Length));
        return new Kaminari.Sazanami.Arrow.Ipc.IpcBody(memory, bytes.Length, [new(0, (nint)p, bytes.Length)]);
    }

    [Fact]
    public void AV4Union_WithNullsOfItsOwn_IsRefused()
    {
        var union = new UnionType([new Field("i", Int32Type.Default, true)], [0], UnionMode.Sparse);
        var field = new Kaminari.Sazanami.Arrow.Ipc.IpcField(new Field("u", union, true), null,
            [new(new Field("i", Int32Type.Default, true), null, [], Kaminari.Sazanami.Arrow.Ipc.IpcConversion.None)], Kaminari.Sazanami.Arrow.Ipc.IpcConversion.None);
        // Buffers: union validity, type ids, child validity, child values (8 bytes each, at 0, 8, 16, 24).
        byte[] bytes = new byte[32];
        bytes[0] = 0b10;
        BitConverter.TryWriteBytes(bytes.AsSpan(24), 7);
        Kaminari.Sazanami.Arrow.Ipc.IpcRecordBatch Header(long nulls) => new(2, [new(2, nulls), new(2, 0)],
            [new(0, 8), new(8, 2), new(16, 0), new(24, 8)], Kaminari.Sazanami.Arrow.Ipc.IpcCompression.None, []);
        using var dictionaries = new Kaminari.Sazanami.Arrow.Ipc.IpcDictionaries();
        var body = Body(bytes);
        var ok = Kaminari.Sazanami.Arrow.Ipc.IpcBatchDecoder.Decode([field], null, Header(0), Kaminari.Sazanami.Arrow.Ipc.IpcMetadataVersion.V4, body, dictionaries, false);
        Assert.Equal(2, ok[0]!.Length);
        Assert.Contains("top-level nulls", Assert.Throws<InvalidDataException>(() =>
            Kaminari.Sazanami.Arrow.Ipc.IpcBatchDecoder.Decode([field], null, Header(1), Kaminari.Sazanami.Arrow.Ipc.IpcMetadataVersion.V4, body, dictionaries, false)).Message);
        body.Memory.Sealed();
    }

    [Fact]
    public unsafe void ABufferOver2GiB_IsRefused_AndAMisalignedOneCopied()
    {
        var memory = new Kaminari.Sazanami.Arrow.Ipc.IpcBodyMemory();
        byte* p = memory.Allocate(64);
        // A segment that claims more than is there: the size check comes before any byte is touched.
        var huge = new Kaminari.Sazanami.Arrow.Ipc.IpcBody(memory, 3L << 30, [new(0, (nint)p, 3L << 30)]);
        Assert.Contains("over 2 GiB", Assert.Throws<InvalidDataException>(() => huge.Slice(new(0, 3L << 30), Kaminari.Sazanami.Arrow.Ipc.IpcCompression.None, "c")).Message);
        for (int i = 0; i < 32; i++) p[i] = (byte)i;
        var body = new Kaminari.Sazanami.Arrow.Ipc.IpcBody(memory, 32, [new(0, (nint)p, 32)]);
        var aligned = body.Slice(new(8, 8), Kaminari.Sazanami.Arrow.Ipc.IpcCompression.None, "c");
        Assert.Equal((nint)(p + 8), aligned.Pointer);
        var misaligned = body.Slice(new(3, 8), Kaminari.Sazanami.Arrow.Ipc.IpcCompression.None, "c");
        Assert.NotEqual((nint)(p + 3), misaligned.Pointer);
        Assert.Equal(0, misaligned.Pointer & 7);
        Assert.Equal(new byte[] { 3, 4, 5, 6, 7, 8, 9, 10 }, misaligned.Span.ToArray());
        memory.Sealed();
    }

    [Fact]
    public void UnselectedColumns_AreNotChecked()
    {
        // Column b has decreasing string offsets: reading it fails; reading only a does not look at it.
        var schema = new Schema([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)], null);
        var bad = new ArrayData(StringType.Default, 3, 0, 0, [ArrowBuffer.Empty, Ints(0, 3, 1, 4), new ArrowBuffer("abcd"u8.ToArray())]);
        using var batch = new RecordBatch(schema, [ArrowArrayFactory.BuildArray(new ArrayData(Int32Type.Default, 3, 0, 0, [ArrowBuffer.Empty, Ints(1, 2, 3)])),
            ArrowArrayFactory.BuildArray(bad)], 3);
        byte[] stream = Write(schema, batch);
        var file = new MemoryStream();
        using (var writer = new ArrowFileWriter(file, schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }
        var only = new IpcReadOptions { Columns = ["a"] };

        using (var reader = IpcStreamReader.Open(new MemoryStream(stream), only))
        using (RecordBatch read = reader.ReadNext()!) Assert.Equal("2", CanonicalRender.Value(read.Column(0), 1));
        using (var whole = IpcStreamReader.Open(new MemoryStream(stream))) Assert.Throws<IpcFormatException>(() => whole.ReadNext());

        using IpcFile ipc = IpcFile.Open(file.ToArray());
        using (RecordBatch read = ipc.ReadBatch(0, only)) Assert.Equal("3", CanonicalRender.Value(read.Column(0), 2));
        Assert.Throws<IpcFormatException>(() => ipc.ReadBatch(0));
    }
}
