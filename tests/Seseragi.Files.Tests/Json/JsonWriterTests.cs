using System.Buffers;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Json;
using static Seseragi.Files.Tests.Json.JsonWriterOracleTests;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// <see cref="JsonWriter"/>: types Polars does not write (read from their expected text with a given schema, written
/// back to the same text), every output, the bytes whatever the threads, pieces, batches and ownership, compression,
/// failures; and the reader's memory and text entry points.
/// </summary>
public class JsonWriterTests
{
    private static Field F(string name, IArrowType type, IEnumerable<KeyValuePair<string, string>>? metadata = null) => new(name, type, true, metadata);

    // The text read with the schema and written back.
    private static string RoundTrip(string text, Schema schema, JsonFormat format = JsonFormat.Lines)
    {
        using JsonReader reader = JsonReader.Parse(text, new JsonReadOptions { Schema = schema, Format = format });
        var batches = reader.ReadBatches().ToList();
        try { return JsonWriter.WriteString(schema, batches, new JsonWriteOptions { Format = format }); }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    // ---------------------------------------------------------------- values

    [Fact]
    public void TypesPolarsDoesNotWrite_AreWrittenAsTheyAreRead()
    {
        var schema = new Schema([
            F("s", StringType.Default), F("ls", LargeStringType.Default), F("b", BinaryType.Default), F("lb", LargeBinaryType.Default),
            F("bv", BinaryViewType.Default), F("d64", Date64Type.Default), F("t32s", new Time32Type(TimeUnit.Second)),
            F("t32ms", new Time32Type(TimeUnit.Millisecond)), F("t64us", new Time64Type(TimeUnit.Microsecond)),
            F("ts_s", new TimestampType(TimeUnit.Second, (string?)null)), F("fixed", new TimestampType(TimeUnit.Millisecond, "+05:45")),
            F("west", new TimestampType(TimeUnit.Microsecond, "-03:30")), F("dur_s", DurationType.Second),
            F("h", HalfFloatType.Default), F("i128", new Decimal128Type(38, 0), [new("kaminari:int128", "i128")]),
            F("u128", new Decimal128Type(38, 0), [new("kaminari:int128", "u128")]),
            F("map", new MapType(StringType.Default, Int64Type.Default)),
            F("dict", new DictionaryType(Int32Type.Default, StringType.Default, false)),
            F("fl", new FixedSizeListType(Int64Type.Default, 2)), F("ll", new LargeListType(new ListType(Int8Type.Default))),
            F("st", new StructType([F("a", BooleanType.Default), F("m", new MapType(StringType.Default, StringType.Default))])),
        ], null);
        string text = """
            {"s":"a\"b\\\u0001\u001f","ls":"x","b":"AAEC","lb":"","bv":"/w==","d64":"2024-02-29","t32s":"23:59:59","t32ms":"00:00:00.123","t64us":"12:00:00.000001","ts_s":"1969-12-31 23:59:59","fixed":"2024-01-02T03:04:05.500+05:45","west":"2024-01-02T03:04:05-03:30","dur_s":"-PT5S","h":1.5,"i128":-170141183460469231731687303715884105728,"u128":340282366920938463463374607431768211455,"map":{"a":1,"b":null},"dict":"x","fl":[1,null],"ll":[[1],null,[]],"st":{"a":true,"m":{"k":"v"}}}
            {"s":null,"ls":null,"b":null,"lb":null,"bv":null,"d64":null,"t32s":null,"t32ms":null,"t64us":null,"ts_s":null,"fixed":null,"west":null,"dur_s":"PT90S","h":null,"i128":0,"u128":null,"map":{},"dict":null,"fl":null,"ll":null,"st":null}

            """.Replace("\r", "");
        Assert.Equal(text, RoundTrip(text, schema));
        Assert.Equal("[" + string.Join(",", text.Split('\n', StringSplitOptions.RemoveEmptyEntries)) + "]",
            RoundTrip("[" + string.Join(",", text.Split('\n', StringSplitOptions.RemoveEmptyEntries)) + "]", schema, JsonFormat.Document));
    }

    [Fact]
    public void FixedBinaries_ListViews_AndNegativeScales_AreWritten()
    {
        byte[] bytes = [0, 1, 2, 255, 254, 253];
        var fixedBinary = new Apache.Arrow.Arrays.FixedSizeBinaryArray(new ArrayData(new FixedSizeBinaryType(3), 2, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
        var items = new ArrayData(Int32Type.Default, 4, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(Bytes<int>([7, 8, 9, 10]))]);
        var views = new ListViewArray(new ArrayData(new ListViewType(Int32Type.Default), 2, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(Bytes<int>([2, 0])), new ArrowBuffer(Bytes<int>([2, 1]))], [items]));
        var decimals = new Decimal128Array(new ArrayData(new Decimal128Type(5, -2), 2, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(Bytes<Int128>([123, -4]))]));
        var schema = new Schema([F("fb", new FixedSizeBinaryType(3)), F("lv", new ListViewType(Int32Type.Default)), F("d", new Decimal128Type(5, -2))], null);
        using var batch = new RecordBatch(schema, [fixedBinary, views, decimals], 2);
        Assert.Equal("""
            {"fb":"AAEC","lv":[9,10],"d":"12300"}
            {"fb":"//79","lv":[7],"d":"-400"}

            """.Replace("\r", ""), JsonWriter.WriteString(schema, [batch]));
    }

    private static byte[] Bytes<T>(T[] values) where T : unmanaged => System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()).ToArray();

    [Fact]
    public void Floats_AreShortest_InDecimalNotation_FromAHundredThousandthTo1e16()
    {
        double[] values = [0.1 + 0.2, 1e-5, 9.99e-6, 1e15, 9999999999999998, 1e16, -1.5e-300, 100, -0.0, 0, 123.456, 1e21];
        string[] expected = ["0.30000000000000004", "0.00001", "9.99e-6", "1000000000000000.0", "9999999999999998.0", "1e+16", "-1.5e-300",
            "100.0", "-0.0", "0.0", "123.456", "1e+21"];
        var schema = new Schema([F("f", DoubleType.Default)], null);
        using var batch = new RecordBatch(schema, [new DoubleArray.Builder().AppendRange(values).Build(Kaminari.Sazanami.Arrow.Builders.PooledArrowAllocator.Instance)], values.Length);
        Assert.Equal(expected.Select(e => $"{{\"f\":{e}}}"), JsonWriter.WriteString(schema, [batch]).Split('\n', StringSplitOptions.RemoveEmptyEntries));
    }

    // ---------------------------------------------------------------- outputs

    [Fact]
    public async Task EveryOutput_WritesTheSameText()
    {
        var (schema, batches) = ReadArrow("written.arrow");
        try
        {
            byte[] expected = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "TestData", "json", "written.ndjson"));
            var buffer = new ArrayBufferWriter<byte>();
            JsonWriter.Write(buffer, schema, batches);
            AssertSameText(expected, buffer.WrittenSpan.ToArray());
            var text = new StringWriter();
            JsonWriter.Write(text, schema, batches);
            Assert.Equal(Encoding.UTF8.GetString(expected), text.ToString());
            Assert.Equal(Encoding.UTF8.GetString(expected), JsonWriter.WriteString(schema, batches));
            string path = Path.Combine(Path.GetTempPath(), $"json_{Guid.NewGuid():N}.ndjson");
            try
            {
                JsonWriter.Write(path, schema, batches);
                AssertSameText(expected, File.ReadAllBytes(path));
                await JsonWriter.WriteAsync(path, schema, batches.ToAsyncEnumerable());
                AssertSameText(expected, File.ReadAllBytes(path));
            }
            finally { File.Delete(path); }
            var stream = new MemoryStream();
            await JsonWriter.WriteAsync(stream, schema, batches.ToAsyncEnumerable(), new JsonWriteOptions { LeaveOpen = true });
            AssertSameText(expected, stream.ToArray());
            var asyncText = new StringWriter();
            await JsonWriter.WriteAsync(asyncText, schema, batches.ToAsyncEnumerable());
            Assert.Equal(Encoding.UTF8.GetString(expected), asyncText.ToString());
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    // Everything a pipe's reader gets until the pipe is completed (or the reader stops after `stopAfter` bytes).
    private static async Task<byte[]> ReadPipeAsync(PipeReader reader, long stopAfter = long.MaxValue)
    {
        var all = new MemoryStream();
        while (true)
        {
            ReadResult result = await reader.ReadAsync();
            foreach (ReadOnlyMemory<byte> segment in result.Buffer) all.Write(segment.Span);
            reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted || all.Length >= stopAfter) break;
        }
        await reader.CompleteAsync();
        return all.ToArray();
    }

    [Fact]
    public async Task Pipe_IsFlushedAsItGoes_WaitsForItsReader_AndStaysOpen()
    {
        var (schema, batches) = ReadArrow("written.arrow");
        try
        {
            byte[] expected = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "TestData", "json", "written.ndjson"));
            var options = new JsonWriteOptions { PieceBytes = 300, MaxDegreeOfParallelism = 3 };
            foreach (bool useAsync in (bool[])[true, false])
            {
                var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 1024, resumeWriterThreshold: 512, useSynchronizationContext: false));
                Task write = useAsync
                    ? JsonWriter.WriteAsync(pipe.Writer, schema, batches.ToAsyncEnumerable(), options)
                    : Task.Run(() => JsonWriter.Write(pipe.Writer, schema, batches, options));
                // Nothing read: the writer waits for the reader.
                await Task.Delay(200);
                Assert.False(write.IsCompleted);
                Task<byte[]> read = ReadPipeAsync(pipe.Reader);
                await write;
                // Not completed by the writer: the caller ends the pipe.
                await pipe.Writer.WriteAsync("x"u8.ToArray());
                await pipe.Writer.CompleteAsync();
                AssertSameText([.. expected, (byte)'x'], await read);
            }
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    [Fact]
    public async Task Pipe_WhoseReaderStops_FailsTheWriter()
    {
        var (schema, batches) = ReadArrow("written.arrow");
        try
        {
            var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 1024, resumeWriterThreshold: 512, useSynchronizationContext: false));
            Task<byte[]> read = ReadPipeAsync(pipe.Reader, stopAfter: 2000);
            await Assert.ThrowsAsync<IOException>(() => JsonWriter.WriteAsync(pipe.Writer, schema, batches.ToAsyncEnumerable(), new JsonWriteOptions { PieceBytes = 300 }));
            await read;
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    [Theory]
    [InlineData(JsonFormat.Lines)]
    [InlineData(JsonFormat.Document)]
    public void TheBytes_DoNotDependOnThreads_Pieces_Batches_OrOwnership(JsonFormat format)
    {
        var (schema, whole) = ReadArrow("written.arrow");
        byte[] expected;
        try { expected = Encoding.UTF8.GetBytes(JsonWriter.WriteString(schema, whole, new JsonWriteOptions { Format = format })); }
        finally { whole.ForEach(b => b.Dispose()); }
        foreach (int piece in (int[])[1, 300, 1 << 20])
            foreach (int threads in (int[])[1, 4])
                foreach (BatchOwnership ownership in (BatchOwnership[])[BatchOwnership.Caller, BatchOwnership.Writer])
                    foreach (int slice in (int[])[1, 5, 1000])
                    {
                        var (_, batches) = ReadArrow("written.arrow");
                        // Slices of the batches (rows at offsets in every array); the originals stay with the test.
                        List<RecordBatch> slices = [.. batches.SelectMany(b => Enumerable.Range(0, (b.Length + slice - 1) / slice)
                            .Select(k => b.Slice(k * slice, Math.Min(slice, b.Length - k * slice))))];
                        try
                        {
                            var options = new JsonWriteOptions { Format = format, PieceBytes = piece, MaxDegreeOfParallelism = threads, InFlightBytes = 2000 };
                            using var output = new MemoryStream();
                            using (JsonWriter writer = JsonWriter.Create(output, schema, options with { LeaveOpen = true }))
                            {
                                foreach (RecordBatch s in slices) writer.Write(s);
                            }
                            AssertSameText(expected, output.ToArray());
                            if (ownership == BatchOwnership.Writer)
                            {
                                // The writer takes fresh batches and disposes them.
                                var (_, owned) = ReadArrow("written.arrow");
                                using var owned2 = new MemoryStream();
                                JsonWriter.Write(owned2, schema, owned, options with { BatchOwnership = BatchOwnership.Writer });
                                AssertSameText(expected, owned2.ToArray());
                            }
                        }
                        finally { batches.ForEach(b => b.Dispose()); }
                    }
    }

    // ---------------------------------------------------------------- compression

    [Theory]
    [InlineData(JsonCompression.Gzip, JsonFormat.Lines)]
    [InlineData(JsonCompression.Gzip, JsonFormat.Document)]
    [InlineData(JsonCompression.Zstd, JsonFormat.Lines)]
    [InlineData(JsonCompression.Zstd, JsonFormat.Document)]
    public void CompressedOutput_DecompressesToTheText_AndReadsBack(JsonCompression compression, JsonFormat format)
    {
        var (schema, batches) = ReadArrow("written.arrow");
        try
        {
            string text = JsonWriter.WriteString(schema, batches, new JsonWriteOptions { Format = format });
            using var output = new MemoryStream();
            JsonWriter.Write(output, schema, batches, new JsonWriteOptions { Format = format, Compression = compression, PieceBytes = 500, MaxDegreeOfParallelism = 3 });
            byte[] packed = output.ToArray();
            using Stream unpacked = compression == JsonCompression.Gzip
                ? new GZipStream(new MemoryStream(packed), CompressionMode.Decompress)
                : new ZstdSharp.DecompressionStream(new MemoryStream(packed));
            using var plain = new MemoryStream();
            unpacked.CopyTo(plain);
            Assert.Equal(text, Encoding.UTF8.GetString(plain.ToArray()));
            // The reader decompresses the pieces as one input.
            var options = new JsonReadOptions { Format = format, InferSchemaLength = null };
            using var fromPacked = JsonReader.Open(new MemoryStream(packed), options);
            using var fromText = JsonReader.Parse(text, options);
            Assert.Equal(ArrowRender.Rows(fromText.ReadBatches().ToList()), ArrowRender.Rows(fromPacked.ReadBatches().ToList()));
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    /// <summary>No rows, compressed: one empty member (or frame), so that the output is compressed data still.</summary>
    [Theory]
    [InlineData(JsonCompression.Gzip, JsonFormat.Lines, "")]
    [InlineData(JsonCompression.Zstd, JsonFormat.Lines, "")]
    [InlineData(JsonCompression.Gzip, JsonFormat.Document, "[]")]
    [InlineData(JsonCompression.None, JsonFormat.Lines, "")]
    public void NoRows_Compressed_IsAnEmptyMember(JsonCompression compression, JsonFormat format, string text)
    {
        var schema = new Schema([F("a", Int64Type.Default)], null);
        using var output = new MemoryStream();
        JsonWriter.Write(output, schema, [], new JsonWriteOptions { Format = format, Compression = compression });
        byte[] packed = output.ToArray();
        Assert.Equal(compression == JsonCompression.None && text.Length == 0, packed.Length == 0);
        using Stream unpacked = compression switch
        {
            JsonCompression.Gzip => new GZipStream(new MemoryStream(packed), CompressionMode.Decompress),
            JsonCompression.Zstd => new ZstdSharp.DecompressionStream(new MemoryStream(packed)),
            _ => new MemoryStream(packed),
        };
        using var plain = new MemoryStream();
        unpacked.CopyTo(plain);
        Assert.Equal(text, Encoding.UTF8.GetString(plain.ToArray()));
    }

    [Fact]
    public void TextOutputs_RefuseCompression_AndLevelsAreChecked()
    {
        var schema = new Schema([F("a", Int64Type.Default)], null);
        Assert.Throws<ArgumentException>(() => JsonWriter.Create(new StringWriter(), schema, new JsonWriteOptions { Compression = JsonCompression.Gzip }));
        Assert.Throws<ArgumentException>(() => JsonWriter.WriteString(schema, [], new JsonWriteOptions { Compression = JsonCompression.Zstd }));
        Assert.Throws<ArgumentOutOfRangeException>(() => JsonWriter.Create(new MemoryStream(), schema, new JsonWriteOptions { Compression = JsonCompression.Gzip, CompressionLevel = 10 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => JsonWriter.Create(new MemoryStream(), schema, new JsonWriteOptions { CompressionLevel = 3 }));
    }

    // ---------------------------------------------------------------- failures

    [Fact]
    public void TypesJsonCannotHold_AreRefusedAtCreate()
    {
        foreach (IArrowType type in (IArrowType[])[new Decimal256Type(40, 2), new IntervalType(IntervalUnit.DayTime), new MapType(Int32Type.Default, Int32Type.Default),
            new TimestampType(TimeUnit.Second, "Not/AZone"), new StructType([F("x", new Decimal256Type(40, 2))])])
            Assert.Throws<NotSupportedException>(() => JsonWriter.Create(new MemoryStream(), new Schema([F("c", type)], null)));
    }

    [Fact]
    public void StringsThatAreNotUtf8_FailAtTheirRow_AndTheWriterCannotComplete()
    {
        var schema = new Schema([F("s", StringType.Default)], null);
        byte[] data = [.. "okfine"u8, 0xFF];
        var strings = new StringArray(new ArrayData(StringType.Default, 3, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(Bytes<int>([0, 2, 6, 7])), new ArrowBuffer(data)]));
        using var batch = new RecordBatch(schema, [strings], 3);
        // The failure is the call's, every time (not a race with the writes behind).
        for (int i = 0; i < 300; i++)
        {
            using var output = new MemoryStream();
            var writer = JsonWriter.Create(output, schema, new JsonWriteOptions { PieceBytes = 1, MaxDegreeOfParallelism = 1 + i % 4 });
            var e = Assert.Throws<JsonValueException>(() => writer.Write(batch));
            Assert.Equal("Row 2: Column 's': a string that is not UTF-8.", e.Message);
            Assert.Throws<InvalidOperationException>(() => writer.Complete());
            writer.Dispose();
        }
    }

    [Fact]
    public void BatchesOfAnotherSchema_AreRefused()
    {
        var schema = new Schema([F("a", Int64Type.Default)], null);
        using var other = new RecordBatch(new Schema([F("a", Int32Type.Default)], null), [new Int32Array.Builder().Append(1).Build(Kaminari.Sazanami.Arrow.Builders.PooledArrowAllocator.Instance)], 1);
        using var writer = JsonWriter.Create(new MemoryStream(), schema);
        Assert.Throws<ArgumentException>(() => writer.Write(other));
    }

    [Fact]
    public void Abort_LeavesTheDocumentOpen_EmptyOutputsAreAsPolarsWritesThem()
    {
        var schema = new Schema([F("a", Int64Type.Default)], null);
        using var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build(Kaminari.Sazanami.Arrow.Builders.PooledArrowAllocator.Instance)], 1);
        using var output = new MemoryStream();
        using (JsonWriter writer = JsonWriter.Create(output, schema, new JsonWriteOptions { Format = JsonFormat.Document, LeaveOpen = true }))
        {
            writer.Write(batch);
            writer.Flush(); // rows still on their way when aborting are dropped: these are written
            writer.Abort();
            Assert.Throws<InvalidOperationException>(() => writer.Complete());
        }
        Assert.Equal("[{\"a\":1}", Encoding.UTF8.GetString(output.ToArray()));
        Assert.Equal("[]", JsonWriter.WriteString(schema, [], new JsonWriteOptions { Format = JsonFormat.Document }));
        Assert.Equal("", JsonWriter.WriteString(schema, []));
        Assert.Equal("{}\n{}\n", JsonWriter.WriteString(new Schema([], null), [new RecordBatch(new Schema([], null), [], 2)]));
    }

    /// <summary>A stream that takes asynchronous writes only (as some network bodies do).</summary>
    private sealed class AsyncOnly : Stream
    {
        private readonly MemoryStream _bytes = new();

        public byte[] ToArray() => _bytes.ToArray();
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new InvalidOperationException("Synchronous writes are not allowed.");
        public override void Flush() => throw new InvalidOperationException("Synchronous flushes are not allowed.");

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _bytes.Write(buffer.Span);
            return default;
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [Fact]
    public async Task AsynchronousWriting_WritesAsynchronouslyBehind_FlushHandsTheOutputOver()
    {
        var (schema, batches) = ReadArrow("written.arrow");
        try
        {
            string expected = JsonWriter.WriteString(schema, batches);
            var output = new AsyncOnly();
            await using (JsonWriter writer = JsonWriter.Create(output, schema, new JsonWriteOptions { LeaveOpen = true, PieceBytes = 100 }))
            {
                await writer.WriteAsync(batches[0]);
                await writer.FlushAsync();
                Assert.Equal(expected, Encoding.UTF8.GetString(output.ToArray()));
                await writer.CompleteAsync();
            }
            var buffer = new ArrayBufferWriter<byte>();
            using (JsonWriter writer = JsonWriter.Create(buffer, schema))
            {
                writer.Write(batches[0]);
                writer.Flush();
                Assert.Equal(expected, Encoding.UTF8.GetString(buffer.WrittenSpan));
            }
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    [Fact]
    public void AFile_TakesThousandsOfPiecesOfABatch_InOneGatheringWrite()
    {
        // More pieces than one vectored write takes (IOV_MAX is 1024): every byte lands, in order.
        var schema = new Schema([F("i", Int64Type.Default)], null);
        var builder = new Int64Array.Builder();
        for (long v = 0; v < 5000; v++) builder.Append(v * 1_000_003);
        using var batch = new RecordBatch(schema, [builder.Build(Kaminari.Sazanami.Arrow.Builders.PooledArrowAllocator.Instance)], 5000);
        string expected = JsonWriter.WriteString(schema, [batch]);
        string path = Path.Combine(Path.GetTempPath(), $"json_{Guid.NewGuid():N}.ndjson");
        try
        {
            JsonWriter.Write(path, schema, [batch], new JsonWriteOptions { PieceBytes = 1, MaxDegreeOfParallelism = 4 });
            Assert.Equal(expected, File.ReadAllText(path));
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public void Decimals32And64_AreWrittenAsDecimals()
    {
        var d32 = new Decimal32Array(new ArrayData(new Decimal32Type(9, 2), 2, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(Bytes<int>([-150, 7]))]));
        var d64 = new Decimal64Array(new ArrayData(new Decimal64Type(18, 3), 2, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(Bytes<long>([123_456_789_012, -1]))]));
        var schema = new Schema([F("a", new Decimal32Type(9, 2)), F("b", new Decimal64Type(18, 3))], null);
        using var batch = new RecordBatch(schema, [d32, d64], 2);
        Assert.Equal("{\"a\":\"-1.50\",\"b\":\"123456789.012\"}\n{\"a\":\"0.07\",\"b\":\"-0.001\"}\n", JsonWriter.WriteString(schema, [batch]));
    }

    // ---------------------------------------------------------------- reading memory and text

    [Fact]
    public void TheReader_ReadsMemoryInPlace_AndText()
    {
        const string text = "[{\"a\":1,\"s\":\"é😀\"},{\"a\":2,\"s\":null}]";
        var options = new JsonReadOptions { Format = JsonFormat.Document, UnitBytes = 5 };
        using (var reader = JsonReader.Open(Encoding.UTF8.GetBytes(text).AsMemory(), options))
            Assert.Equal(["1 | \"é😀\"", "2 | null"], ArrowRender.Rows(reader.ReadBatches().ToList()));
        using (var reader = JsonReader.Parse(text, options))
            Assert.Equal(["1 | \"é😀\"", "2 | null"], ArrowRender.Rows(reader.ReadBatches().ToList()));
        using (var reader = JsonReader.Parse(text.AsSpan(1, text.Length - 2).ToString().Insert(0, "[") + "]", options))
            Assert.Equal(2, reader.ReadBatches().Sum(b => { using (b) return b.Length; }));
        var e = Assert.Throws<JsonFormatException>(() => JsonReader.Parse("{\"s\":\"" + '\uD800' + "\"}"));
        Assert.Contains("unpaired surrogate at character 6", e.Message);
    }
}

/// <summary><see cref="JsonWriter{T}"/>: rows of records written as their fields, nested records as objects.</summary>
public class JsonTypedWriterTests
{
    [Fact]
    public void Records_AreWrittenField_ByField()
    {
        Order[] orders = [new(1, "ab\"c", [new(2.5m, Kind.Big)], new DateOnly(2024, 2, 29), null), new(2, null, [], new DateOnly(1969, 12, 31), 7)];
        string expected = """
            {"Id":1,"Name":"ab\"c","Lines":[{"Price":"2.500000000000000000","Kind":"Big"}],"Day":"2024-02-29","Count":null}
            {"Id":2,"Name":null,"Lines":[],"Day":"1969-12-31","Count":7}

            """.Replace("\r", "");
        Assert.Equal(expected, JsonWriter.WriteString(orders));
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        JsonWriter.WriteRows(buffer, orders.AsEnumerable().Select(o => o), new JsonWriteOptions { MaxDegreeOfParallelism = 1 });
        Assert.Equal(expected, System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan));
        using var text = new StringWriter();
        using (var writer = JsonWriter<Order>.Create(text, new JsonWriteOptions { Format = JsonFormat.Document, LeaveOpen = true }))
        {
            foreach (Order o in orders) writer.WriteRow(o);
            writer.Complete();
        }
        Assert.Equal("[" + string.Join(",", expected.Split('\n', StringSplitOptions.RemoveEmptyEntries)) + "]", text.ToString());
    }

    [Fact]
    public async Task ManyRows_FromManyThreads_KeepTheirOrder()
    {
        Order[] orders = [.. Enumerable.Range(0, 100_000).Select(i => new Order(i, $"n{i}", [], new DateOnly(2000, 1, 1).AddDays(i % 1000), i % 3 == 0 ? null : i))];
        string one = JsonWriter.WriteString(orders, new JsonWriteOptions { MaxDegreeOfParallelism = 1 });
        string many = JsonWriter.WriteString(orders, new JsonWriteOptions { MaxDegreeOfParallelism = 8 });
        Assert.Equal(one, many);
        string path = Path.Combine(Path.GetTempPath(), $"json_{Guid.NewGuid():N}.ndjson");
        try
        {
            await JsonWriter.WriteRowsAsync(path, orders.ToAsyncEnumerable());
            Assert.Equal(one, File.ReadAllText(path));
        }
        finally { File.Delete(path); }
    }

    public enum Kind { Small, Big }

    public sealed record Line(decimal Price, Kind Kind);

    public sealed record Order(long Id, string? Name, List<Line> Lines, DateOnly Day, int? Count);
}
