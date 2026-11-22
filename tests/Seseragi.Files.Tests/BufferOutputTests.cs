using System.Buffers;
using System.IO.Pipelines;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;
using Kaminari.Seseragi.Files.Excel;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests;

/// <summary>
/// Every writer into an <see cref="IBufferWriter{T}"/> (each way in: created, the static writes, typed rows) writes what
/// it writes into a stream that cannot seek, read back from memory; a pipe is flushed as it goes, waits for its reader
/// and stays open. Workbooks and Avro files read from memory read as they do from their path.
/// </summary>
public class BufferOutputTests
{
    public sealed record Row(long Id, string? Name, double Score);

    private static readonly Row[] Rows = [new(1, "a", 0.5), new(2, null, -1), new(3, "ccc", 1e300)];

    private static RecordBatch Batch() => TestBatches.Batch(
        ("id", Fixed<long>(Int64Type.Default, [1, null, 3, 4]), true),
        ("name", Strings(["a", "bb", null, "dddd"]), true),
        ("score", Fixed<double>(DoubleType.Default, [0.5, -1, null, 1e300]), true));

    // A stream that only writes forward: the writers write into it as into a buffer writer.
    private sealed class OneWayStream : MemoryStream
    {
        public override bool CanSeek => false;

        public override long Position { get => base.Position; set => throw new NotSupportedException(); }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    }

    private static async Task<byte[]> IntoBuffer(Func<IBufferWriter<byte>, Task> write)
    {
        var buffer = new ArrayBufferWriter<byte>();
        await write(buffer);
        return buffer.WrittenSpan.ToArray();
    }

    private static async Task<byte[]> IntoStream(Func<Stream, Task> write)
    {
        var stream = new OneWayStream();
        await write(stream);
        return stream.ToArray();
    }

    private static Task Done(Action write)
    {
        write();
        return Task.CompletedTask;
    }

    // ---------------------------------------------------------------- every writer

    // How a format is written into a stream (the reference) and every way into a buffer writer, and read back.
    private sealed record Case(Func<Stream, Task> Reference, Func<IBufferWriter<byte>, Task>[] Ways, Func<byte[], List<string>> Read, bool Deterministic);

    public static TheoryData<string> Formats => ["parquet", "ipc", "ipc-compressed", "avro", "excel", "json"];

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task EveryWayIntoABufferWriter_WritesWhatAOneWayStreamGets(string format)
    {
        using RecordBatch batch = Batch();
        Schema schema = batch.Schema;
        RecordBatch[] batches = [batch];
        var ipc = new IpcWriteOptions { LeaveOpen = true };
        if (format == "ipc-compressed") ipc = ipc with { Compression = new ZstandardCompression(), StreamBodyFrom = 0 };
        Case c = format switch
        {
            "parquet" => new(s => Done(() => ParquetWriter.Write(s, schema, batches, new() { LeaveOpen = true })), [
                b => Done(() => ParquetWriter.Write(b, schema, batches)),
                b => ParquetWriter.WriteAsync(b, schema, batches.ToAsyncEnumerable()),
                b => Done(() =>
                {
                    using ParquetWriter writer = ParquetWriter.Create(b, schema);
                    writer.Write(batch);
                    writer.Complete();
                }),
            ], ReadParquet, true),
            "ipc" or "ipc-compressed" => new(s => Done(() => IpcWriter.Write(s, schema, batches, ipc)), [
                b => Done(() => IpcWriter.Write(b, schema, batches, ipc)),
                b => IpcWriter.WriteAsync(b, schema, batches.ToAsyncEnumerable(), ipc),
                b => Done(() =>
                {
                    using IpcWriter writer = IpcWriter.Create(b, schema, ipc);
                    writer.Write(batch);
                    writer.Complete();
                }),
            ], ReadIpc, true),
            "avro" => new(s => Done(() => AvroWriter.Write(s, schema, batches, new() { LeaveOpen = true })), [
                b => Done(() => AvroWriter.Write(b, schema, batches)),
                b => AvroWriter.WriteAsync(b, schema, batches.ToAsyncEnumerable()),
                b => Done(() =>
                {
                    using AvroWriter writer = AvroWriter.Create(b, schema);
                    writer.Write(batch);
                    writer.Complete();
                }),
            ], ReadAvro, false), // a random sync marker
            "excel" => new(s => Done(() => ExcelWriter.Write(s, schema, batches, new() { LeaveOpen = true })), [
                b => Done(() => ExcelWriter.Write(b, schema, batches)),
                b => ExcelWriter.WriteAsync(b, schema, batches.ToAsyncEnumerable()),
                b => Done(() =>
                {
                    using ExcelWriter writer = ExcelWriter.Create(b, schema);
                    writer.Write(batch);
                    writer.Complete();
                }),
            ], ReadExcel, false), // entries stamped with the time
            "json" => new(s => Done(() => JsonWriter.Write(s, schema, batches, new() { LeaveOpen = true })), [
                b => Done(() => JsonWriter.Write(b, schema, batches)),
                b => JsonWriter.WriteAsync(b, schema, batches.ToAsyncEnumerable()),
            ], ReadJson, true),
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        byte[] expected = await IntoStream(c.Reference);
        List<string> rows = c.Read(expected);
        Assert.Equal(4, rows.Count);
        foreach (var way in c.Ways)
        {
            byte[] written = await IntoBuffer(way);
            if (c.Deterministic) Assert.Equal(expected, written);
            Assert.Equal(rows, c.Read(written));
        }
    }

    [Fact]
    public async Task CompressedIpc_IntoABufferWriter_IsTheFileASeekableStreamGets()
    {
        // A seekable stream takes a compressed body as it is compressed, written back into for its length; a buffer
        // writer takes it once compressed whole: the same bytes.
        using RecordBatch batch = Batch();
        var options = new IpcWriteOptions { Compression = new ZstandardCompression(), StreamBodyFrom = 0, LeaveOpen = true };
        using var seekable = new MemoryStream();
        IpcWriter.Write(seekable, batch.Schema, [batch], options);
        Assert.Equal(seekable.ToArray(), await IntoBuffer(b => Done(() => IpcWriter.Write(b, batch.Schema, [batch], options))));
    }

    [Fact]
    public async Task TypedRows_IntoABufferWriter_WriteWhatAStreamGets()
    {
        Assert.Equal(await IntoStream(s => Done(() => ParquetWriter.WriteRows(s, Rows, new() { LeaveOpen = true }))),
            await IntoBuffer(b => Done(() => ParquetWriter.WriteRows(b, Rows))));
        Assert.Equal(await IntoStream(s => ParquetWriter.WriteRowsAsync(s, Rows.ToAsyncEnumerable(), new() { LeaveOpen = true })),
            await IntoBuffer(b => ParquetWriter.WriteRowsAsync(b, Rows.ToAsyncEnumerable())));
        Assert.Equal(await IntoStream(s => Done(() => ParquetWriter.WriteRows(s, Rows, new() { LeaveOpen = true }))), await IntoBuffer(b => Done(() =>
        {
            using ParquetWriter<Row> writer = ParquetWriter<Row>.Create(b);
            writer.WriteRows(Rows);
            writer.Complete();
        })));
        Assert.Equal(await IntoStream(s => Done(() => IpcWriter.WriteRows(s, Rows, new() { LeaveOpen = true }))),
            await IntoBuffer(b => Done(() => IpcWriter.WriteRows(b, Rows))));
        Assert.Equal(await IntoStream(s => IpcWriter.WriteRowsAsync(s, Rows.ToAsyncEnumerable(), new() { LeaveOpen = true })),
            await IntoBuffer(b => IpcWriter.WriteRowsAsync(b, Rows.ToAsyncEnumerable())));
        Assert.Equal(await IntoStream(s => Done(() => IpcWriter.WriteRows(s, Rows, new() { LeaveOpen = true }))), await IntoBuffer(b => Done(() =>
        {
            using IpcWriter<Row> writer = IpcWriter<Row>.Create(b);
            writer.WriteRows(Rows);
            writer.Complete();
        })));
        Assert.Equal(await IntoStream(s => JsonWriter.WriteRowsAsync(s, Rows.ToAsyncEnumerable(), new() { LeaveOpen = true })),
            await IntoBuffer(b => JsonWriter.WriteRowsAsync(b, Rows.ToAsyncEnumerable())));
    }

    [Fact]
    public void ANullBufferWriter_IsRefused()
    {
        Schema schema = Batch().Schema;
        Assert.Throws<ArgumentNullException>("output", () => ParquetWriter.Create((IBufferWriter<byte>)null!, schema));
        Assert.Throws<ArgumentNullException>("output", () => IpcWriter.Create((IBufferWriter<byte>)null!, schema));
        Assert.Throws<ArgumentNullException>("output", () => AvroWriter.Create((IBufferWriter<byte>)null!, schema));
        Assert.Throws<ArgumentNullException>("output", () => ExcelWriter.Create((IBufferWriter<byte>)null!, schema));
    }

    private static List<string> ReadParquet(byte[] bytes)
    {
        using ParquetFile file = ParquetFile.Open(bytes);
        using ParquetReader reader = file.OpenReader();
        return Render(reader.ReadBatches());
    }

    private static List<string> ReadIpc(byte[] bytes)
    {
        using IpcFile file = IpcFile.Open(bytes);
        using IpcReader reader = file.OpenReader();
        return Render(reader.ReadBatches());
    }

    private static List<string> ReadAvro(byte[] bytes)
    {
        using AvroReader reader = AvroReader.Open(bytes);
        return Render(reader.ReadBatches());
    }

    private static List<string> ReadExcel(byte[] bytes)
    {
        using ExcelReader reader = ExcelReader.Open(bytes);
        return Render(reader.ReadBatches());
    }

    private static List<string> ReadJson(byte[] bytes)
    {
        using JsonReader reader = JsonReader.Open(bytes);
        return Render(reader.ReadBatches());
    }

    // The rows of batches read, each batch disposed.
    private static List<string> Render(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
            using (batch)
                rows.AddRange(ArrowRender.Rows(batch));
        return rows;
    }

    // ---------------------------------------------------------------- pipes

    private static Pipe SmallPipe() => new(new PipeOptions(pauseWriterThreshold: 1024, resumeWriterThreshold: 512, useSynchronizationContext: false));

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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task APipe_WaitsForItsReader_AndStaysOpen(bool useAsync)
    {
        using RecordBatch batch = Batch();
        byte[] expected = await IntoStream(s => Done(() => ParquetWriter.Write(s, batch.Schema, [batch], new() { LeaveOpen = true })));
        Pipe pipe = SmallPipe();
        Task write = useAsync
            ? ParquetWriter.WriteAsync(pipe.Writer, batch.Schema, ((RecordBatch[])[batch]).ToAsyncEnumerable())
            : Task.Run(() => ParquetWriter.Write(pipe.Writer, batch.Schema, [batch]));
        // Nothing read: the writer waits for the reader.
        await Task.Delay(200);
        Assert.False(write.IsCompleted);
        Task<byte[]> read = ReadPipeAsync(pipe.Reader);
        await write;
        // Not completed by the writer: the caller ends the pipe.
        await pipe.Writer.WriteAsync("x"u8.ToArray());
        await pipe.Writer.CompleteAsync();
        byte[] withMore = [.. expected, (byte)'x'];
        Assert.Equal(withMore, await read);
    }

    [Fact]
    public async Task APipeWhoseReaderStops_FailsTheWriter()
    {
        using RecordBatch batch = Batch();
        Pipe pipe = SmallPipe();
        // Gone before the first flush (a reader that stops after reading races the writer's last flush).
        await pipe.Reader.CompleteAsync();
        await Assert.ThrowsAsync<IOException>(() => ParquetWriter.WriteAsync(pipe.Writer, batch.Schema, ((RecordBatch[])[batch]).ToAsyncEnumerable()));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task APipe_IsFlushedOnceAMebibyteIsWritten(bool useAsync)
    {
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 64 << 20, resumeWriterThreshold: 32 << 20, useSynchronizationContext: false));
        var stream = BufferWriterStream.Over(pipe.Writer);
        byte[] chunk = new byte[100_000];
        for (int k = 0; k < 11; k++)
        {
            if (useAsync) await stream.WriteAsync(chunk);
            else stream.Write(chunk, 0, chunk.Length);
        }
        // 1.1 MB written: the first mebibyte (and the write that crossed it) is the reader's without a flush.
        Assert.True(pipe.Reader.TryRead(out ReadResult result));
        Assert.Equal(1_100_000, result.Buffer.Length);
        pipe.Reader.AdvanceTo(result.Buffer.End);
        stream.Write(chunk, 0, chunk.Length);
        Assert.False(pipe.Reader.TryRead(out _));
        stream.Flush();
        Assert.True(pipe.Reader.TryRead(out result));
        Assert.Equal(100_000, result.Buffer.Length);
        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    // ---------------------------------------------------------------- memory inputs

    public static TheoryData<string> Workbooks => [.. Directory.EnumerateFiles(Path.Combine(AppContext.BaseDirectory, "TestData", "excel"), "*.*", SearchOption.AllDirectories)
        .Where(p => Path.GetExtension(p) is ".xlsx" or ".xlsb" or ".xls" or ".ods" or ".fods")
        .Select(p => Path.GetRelativePath(Path.Combine(AppContext.BaseDirectory, "TestData", "excel"), p))
        .Order()];

    [Theory]
    [MemberData(nameof(Workbooks))]
    public void AWorkbookInMemory_ReadsAsFromItsPath(string name)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "excel", name);
        byte[] bytes = File.ReadAllBytes(path);
        Assert.Equal(Outcome(() => Sheets(ExcelWorkbook.Open(path))), Outcome(() => Sheets(ExcelWorkbook.Open(bytes))));
        Assert.Equal(Outcome(() => Render(ExcelReader.Open(path))), Outcome(() => Render(ExcelReader.Open(bytes))));

        static List<string> Sheets(ExcelWorkbook workbook)
        {
            using (workbook) return [.. workbook.Sheets.Select(s => s.Name)];
        }

        static List<string> Render(ExcelReader reader)
        {
            using (reader) return BufferOutputTests.Render(reader.ReadBatches());
        }
    }

    public static TheoryData<string> AvroFiles => [.. Directory.EnumerateFiles(Path.Combine(AppContext.BaseDirectory, "TestData", "avro"), "*.avro", SearchOption.AllDirectories)
        .Select(p => Path.GetRelativePath(Path.Combine(AppContext.BaseDirectory, "TestData", "avro"), p))
        .Order()];

    [Theory]
    [MemberData(nameof(AvroFiles))]
    public void AnAvroFileInMemory_ReadsAsFromItsPath(string name)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "avro", name);
        byte[] bytes = File.ReadAllBytes(path);
        Assert.Equal(Outcome(() => Render(AvroReader.Open(path))), Outcome(() => Render(AvroReader.Open(bytes))));

        static List<string> Render(AvroReader reader)
        {
            using (reader) return BufferOutputTests.Render(reader.ReadBatches());
        }
    }

    // What reading gives: the rows, or the kind of failure and its message.
    private static List<string> Outcome(Func<List<string>> read)
    {
        try { return read(); }
        catch (Exception e) { return [$"{e.GetType().Name}: {e.Message}"]; }
    }
}
