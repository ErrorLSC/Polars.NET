using System.IO.Pipes;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// Parallel reading (<see cref="IpcReadOptions.MaxDegreeOfParallelism"/>): batches decoded side by side, the compressed
/// buffers of a batch decompressed side by side, a stream read ahead by a task of its own; the batches are those one
/// thread reads, in order, and failures surface where one thread meets them.
/// </summary>
public class IpcParallelTests
{
    private static string Path(string name) => System.IO.Path.Combine(IpcValueTests.Root, name);

    private static readonly IpcReadOptions OneThread = new() { MaxDegreeOfParallelism = 1 };

    /// <summary>Every buffer of a compressed batch decompressed side by side, however small the batch.</summary>
    private static readonly IpcReadOptions Many = new() { MaxDegreeOfParallelism = 8, ParallelDecompressFrom = 0 };

    public static TheoryData<string> Readable => [.. IpcValueTests.Names.Where(n => !IpcValueTests.Refused.ContainsKey(n))];

    /// <summary>many_batches.arrow's 16 batches rewritten as a stream (LZ4).</summary>
    internal static byte[] ManyBatchesStream()
    {
        using IpcFile file = IpcFile.Open(Path("pyarrow/many_batches.arrow"));
        using IpcReader reader = file.OpenReader(OneThread);
        using var memory = new MemoryStream();
        IpcWriter.Write(memory, reader.Schema, reader.ReadBatches(), new IpcWriteOptions { Format = IpcFormat.Stream, Compression = Lz4FrameCompression.Instance, LeaveOpen = true });
        return memory.ToArray();
    }

    [Theory]
    [MemberData(nameof(Readable))]
    public void EveryFile_ReadsAsOnOneThread(string name)
    {
        var (schema, expected) = IpcValueTests.Read(name, OneThread);
        foreach (IpcReadOptions options in (IpcReadOptions[])[Many, Many with { MemoryMap = IpcMemoryMap.Off }, Many with { MemoryMap = IpcMemoryMap.On }])
        {
            var (s, batches) = IpcValueTests.Read(name, options);
            Assert.Equal(schema.ToString(), s.ToString());
            Assert.Equal(expected.Count, batches.Count);
            for (int b = 0; b < batches.Count; b++) Assert.Equal(expected[b], batches[b]);
            var (_, fromMemory) = IpcValueTests.Read(name, options, fromMemory: true);
            Assert.Equal(expected.SelectMany(b => b), fromMemory.SelectMany(b => b));
        }
        // A file opened from a seekable stream; a stream read ahead within a byte budget of one batch at a time.
        if (IpcValueTests.IsFile(name))
        {
            using IpcFile file = IpcFile.Open(new MemoryStream(File.ReadAllBytes(Path(name))));
            using IpcReader reader = file.OpenReader(Many);
            List<List<string>[]> batches = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) batches.Add([.. Enumerable.Range(0, batch.ColumnCount).Select(c => Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(c), i)).ToList())]);
            }
            Assert.Equal(expected.SelectMany(b => b), batches.SelectMany(b => b));
        }
        else
        {
            var (_, tight) = IpcValueTests.Read(name, Many with { InFlightBytes = 1 });
            Assert.Equal(expected.SelectMany(b => b), tight.SelectMany(b => b));
        }
    }

    [Theory]
    [InlineData("pyarrow/many_batches.arrow")]
    [InlineData("pyarrow/many_batches_lz4.arrow")]
    [InlineData("pyarrow/dictionary_delta.arrows")]
    public void RowLimits_CutWhereOneThreadCuts(string name)
    {
        var (_, all) = IpcValueTests.Read(name, OneThread);
        long total = all.Sum(b => (long)b[0].Count);
        foreach (long limit in (long[])[0, 1, total / 3, total / 2 + 1, total - 1, total, total + 5])
        {
            var (_, one) = IpcValueTests.Read(name, OneThread with { MaxRows = limit });
            var (_, many) = IpcValueTests.Read(name, Many with { MaxRows = limit });
            Assert.Equal(Math.Min(limit, total), many.Sum(b => (long)b[0].Count));
            Assert.Equal(one.SelectMany(b => b[0]), many.SelectMany(b => b[0]));
        }
    }

    [Fact]
    public async Task AsyncReads_GiveTheSameBatches()
    {
        foreach (string name in (string[])["pyarrow/many_batches.arrow", "pyarrow/zstd.arrow", "pyarrow/lz4.arrow"])
        {
            var (_, expected) = IpcValueTests.Read(name, OneThread);
            await using IpcFile file = await IpcFile.OpenAsync(Path(name));
            using IpcReader reader = file.OpenReader(Many);
            List<string> read = [];
            await foreach (RecordBatch batch in reader.ReadBatchesAsync())
            {
                using (batch) read.AddRange(Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(0), i)));
            }
            Assert.Equal(expected.SelectMany(b => b[0]), read);
        }
    }

    /// <summary>A consumer that stops early leaves nothing running: the batches decoded ahead are freed, the reader disposes cleanly.</summary>
    [Fact]
    public void StoppingEarly_FreesTheBatchesDecodedAhead()
    {
        using IpcFile file = IpcFile.Open(Path("pyarrow/many_batches.arrow"));
        using IpcReader reader = file.OpenReader(Many with { MemoryMap = IpcMemoryMap.On });
        for (int round = 0; round < 3; round++)
        {
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                batch.Dispose();
                if (round == 0) break;
            }
        }
        // The reader disposed in the middle of an enumeration: the window is drained, the enumeration then refused.
        using IEnumerator<RecordBatch> batches = reader.ReadBatches().GetEnumerator();
        Assert.True(batches.MoveNext());
        batches.Current.Dispose();
        reader.Dispose();
        Assert.Throws<ObjectDisposedException>(() => batches.MoveNext());
    }

    /// <summary>
    /// A stream read ahead never waits for later messages to deliver a batch: a writer that sends batch n + 1 only
    /// after the reader has batch n does not stall it.
    /// </summary>
    [Fact]
    public async Task ALiveStream_DeliversEachBatchBeforeTheNextIsWritten()
    {
        var schema = new Schema([new Field("n", Int64Type.Default, false)], null);
        using var server = new AnonymousPipeServerStream(PipeDirection.Out);
        using var client = new AnonymousPipeClientStream(PipeDirection.In, server.ClientSafePipeHandle);
        var delivered = new SemaphoreSlim(0);
        const int Batches = 5;
        Task writing = Task.Run(async () =>
        {
            using IpcWriter writer = IpcWriter.Create(server, schema, new IpcWriteOptions { Format = IpcFormat.Stream, LeaveOpen = true });
            for (int b = 0; b < Batches; b++)
            {
                using RecordBatch batch = new(schema, [new Int64Array.Builder().Append(b).Build()], 1);
                writer.Write(batch);
                server.Flush();
                Assert.True(await delivered.WaitAsync(TimeSpan.FromSeconds(20)), $"batch {b} was not delivered");
            }
            writer.Complete();
            server.Dispose();
        });
        await using IpcStreamReader reader = await IpcStreamReader.OpenAsync(client, new IpcReadOptions { MaxDegreeOfParallelism = 4 }, leaveOpen: true);
        List<long> read = [];
        await foreach (RecordBatch batch in reader.ReadBatchesAsync().WithCancellation(new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token))
        {
            using (batch) read.Add(((Int64Array)batch.Column(0)).GetValue(0)!.Value);
            delivered.Release();
        }
        await writing.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal([0L, 1, 2, 3, 4], read);
    }

    /// <summary>A stream damaged in its middle gives the batches before the damage, then fails, read ahead as on one thread.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ADamagedStream_FailsAfterTheBatchesBeforeTheDamage(bool seekable)
    {
        byte[] bytes = ManyBatchesStream();
        byte[] cut = bytes[..(bytes.Length * 2 / 3)];
        int one = Count(OneThread, out Exception? e1), many = Count(Many, out Exception? e2);
        Assert.Equal(one, many);
        Assert.True(one > 0);
        Assert.IsType<IpcFormatException>(e1);
        Assert.IsType<IpcFormatException>(e2);
        Assert.Equal(e1!.Message, e2!.Message);

        int Count(IpcReadOptions options, out Exception? error)
        {
            Stream stream = seekable ? new MemoryStream(cut) : new IpcValueTests.NonSeekable(new MemoryStream(cut));
            using IpcStreamReader reader = IpcStreamReader.Open(stream, options);
            int n = 0;
            error = null;
            try
            {
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    batch.Dispose();
                    n++;
                }
            }
            catch (Exception e)
            {
                error = e;
            }
            return n;
        }
    }

    /// <summary>Disposing a stream reader that reads ahead stops it; the batches delivered stay valid.</summary>
    [Fact]
    public void DisposingAStreamReaderReadingAhead_KeepsTheBatchesDelivered()
    {
        var (_, expected) = IpcValueTests.Read("pyarrow/many_batches.arrow", OneThread);
        RecordBatch first;
        using (IpcStreamReader reader = IpcStreamReader.Open(new MemoryStream(ManyBatchesStream()), Many))
        {
            first = reader.ReadNext()!;
        }
        using (first) Assert.Equal(expected[0][0], Enumerable.Range(0, first.Length).Select(i => CanonicalRender.Value(first.Column(0), i)));
    }

    /// <summary>
    /// A stream whose reads ignore cancellation (stdin, a FIFO opened synchronously) and block once its bytes run out:
    /// disposing a reader that reads it ahead returns at once, the read in progress finishing in the background.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DisposingAReaderWhoseStreamBlocks_DoesNotWait(bool leaveOpen)
    {
        byte[] bytes = ManyBatchesStream();
        using var blocking = new Blocking(bytes[..(bytes.Length / 6)]); // a couple of batches, then a read that blocks
        var reader = IpcStreamReader.Open(blocking, Many, leaveOpen);
        reader.ReadNext()!.Dispose();
        Assert.True(blocking.Blocked.Wait(TimeSpan.FromSeconds(10)), "the reader never blocked in a read");
        Task disposing = Task.Run(reader.Dispose);
        Assert.True(await Task.WhenAny(disposing, Task.Delay(TimeSpan.FromSeconds(10))) == disposing, "Dispose waited for a blocked read");
        blocking.Release();
    }

    /// <summary>
    /// A stream left open is the caller's again once the reader is disposed: no read of the reader's is still going on,
    /// however far ahead it read (reopened from the start, it gives the same first batch every time).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AStreamLeftOpen_IsTheCallersAgainAfterDispose(bool slow)
    {
        using var memory = new MemoryStream(ManyBatchesStream());
        Stream stream = slow ? new Slow(memory) : memory;
        string? first = null;
        for (int round = 0; round < (slow ? 20 : 50); round++)
        {
            memory.Position = 0;
            using (IpcStreamReader reader = IpcStreamReader.Open(stream, Many, leaveOpen: true))
            {
                using RecordBatch batch = reader.ReadNext()!;
                string text = string.Join(",", Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(0), i)));
                first ??= text;
                Assert.Equal(first, text);
            }
        }
    }

    /// <summary>Reads that take a moment and ignore cancellation once started (a pipe read synchronously).</summary>
    private sealed class Slow(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            Thread.Sleep(3);
            return inner.Read(buffer);
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>A cancellation token covers a call's compression, not the write behind it: cancelled after the call, the output still completes.</summary>
    [Fact]
    public async Task ATokenCancelledAfterWriteAsync_DoesNotFaultTheWriter()
    {
        var schema = new Schema([new Field("n", Int64Type.Default, false)], null);
        using var memory = new MemoryStream();
        await using (IpcWriter writer = IpcWriter.Create(memory, schema, new IpcWriteOptions { Compression = Lz4FrameCompression.Instance, LeaveOpen = true }))
        {
            for (int b = 0; b < 5; b++)
            {
                using var cancel = new CancellationTokenSource();
                var builder = new Int64Array.Builder();
                for (int i = 0; i < 200_000; i++) builder.Append(i);
                using RecordBatch batch = new(schema, [builder.Build()], 200_000);
                await writer.WriteAsync(batch, cancel.Token);
                cancel.Cancel();
            }
            await writer.CompleteAsync();
        }
        using IpcFile file = IpcFile.Open(memory.ToArray());
        Assert.Equal(5, file.BatchCount);
    }

    /// <summary>
    /// A read in progress that ends soon (within the reader's wait) is waited for: once Dispose returns, no read of the
    /// reader's is running on the stream left open.
    /// </summary>
    [Fact]
    public void DisposingAReaderWhoseReadEndsSoon_WaitsForIt()
    {
        byte[] bytes = ManyBatchesStream();
        using var blocking = new Blocking(bytes[..(bytes.Length / 6)], TimeSpan.FromMilliseconds(300));
        var reader = IpcStreamReader.Open(blocking, Many, leaveOpen: true);
        reader.ReadNext()!.Dispose();
        Assert.True(blocking.Blocked.Wait(TimeSpan.FromSeconds(10)), "the reader never blocked in a read");
        reader.Dispose();
        Assert.Equal(0, blocking.Reading);
    }

    /// <summary>
    /// Gives its bytes, then blocks every read until released, or for <c>ending</c> when given (then reports the end);
    /// ignores cancellation.
    /// </summary>
    private sealed class Blocking(byte[] bytes, TimeSpan? ending = null) : Stream
    {
        private readonly ManualResetEventSlim _released = new();
        private int _at, _reading;

        /// <summary>Reads in progress.</summary>
        public int Reading => Volatile.Read(ref _reading);

        /// <summary>Set once a read blocks.</summary>
        public ManualResetEventSlim Blocked { get; } = new();

        public void Release() => _released.Set();

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            if (_at == bytes.Length)
            {
                Interlocked.Increment(ref _reading);
                Blocked.Set();
                if (ending is { } wait) _released.Wait(wait);
                else _released.Wait();
                Interlocked.Decrement(ref _reading);
                return 0;
            }
            int n = Math.Min(buffer.Length, bytes.Length - _at);
            bytes.AsSpan(_at, n).CopyTo(buffer);
            _at += n;
            return n;
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            _released.Set();
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// With compression a batch is written behind, while the next is compressed: the caller's batch may go once Write
    /// returns, and a stream that fails surfaces from the next call; the writer is then faulted and disposes cleanly.
    /// </summary>
    [Fact]
    public void BatchesWrittenBehind_FailOnTheNextCall()
    {
        var schema = new Schema([new Field("n", Int64Type.Default, false)], null);
        RecordBatch Make(int b)
        {
            var builder = new Int64Array.Builder();
            for (int i = 0; i < 100_000; i++) builder.Append(b * 100_000L + i);
            return new RecordBatch(schema, [builder.Build()], 100_000);
        }

        // Batches disposed as soon as Write returns: the output is still theirs.
        using (var memory = new MemoryStream())
        {
            using (IpcWriter writer = IpcWriter.Create(memory, schema, new IpcWriteOptions { Compression = Lz4FrameCompression.Instance, LeaveOpen = true }))
            {
                for (int b = 0; b < 6; b++)
                {
                    using RecordBatch batch = Make(b);
                    writer.Write(batch);
                }
            }
            using IpcFile file = IpcFile.Open(memory.ToArray());
            using IpcReader reader = file.OpenReader(OneThread);
            Assert.Equal(Enumerable.Range(0, 600_000).Select(i => (long)i), reader.ReadBatches().SelectMany(b => { using (b) return ((Int64Array)b.Column(0)).Values.ToArray(); }));
        }

        // A stream failing during the first batch's write: the second Write (or Complete) throws, the writer is faulted.
        var failing = new Failing(limit: 4096);
        var w = IpcWriter.Create(failing, schema, new IpcWriteOptions { Compression = Lz4FrameCompression.Instance });
        using (RecordBatch first = Make(0)) w.Write(first);
        Exception e = Record.Exception(() =>
        {
            using RecordBatch second = Make(1);
            w.Write(second);
            w.Complete();
        });
        Assert.IsType<IOException>(e);
        Assert.Throws<InvalidOperationException>(() => w.Complete());
        w.Dispose();
        Assert.True(failing.Disposed);
    }

    /// <summary>A stream that fails once more than <c>limit</c> bytes are written.</summary>
    private sealed class Failing(int limit) : Stream
    {
        private long _written;

        public bool Disposed { get; private set; }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            _written += buffer.Length;
            if (_written > limit) throw new IOException("The disk is full.");
        }

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }

    /// <summary>The metadata of each batch's message comes with that batch, however far ahead the reader is.</summary>
    [Fact]
    public void BatchMetadata_ComesWithItsBatch()
    {
        var schema = new Schema([new Field("n", Int32Type.Default, false)], null);
        using var memory = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(memory, schema, new IpcWriteOptions { Format = IpcFormat.Stream, LeaveOpen = true }))
        {
            for (int b = 0; b < 20; b++)
            {
                using RecordBatch batch = new(schema, [new Int32Array.Builder().Append(b).Build()], 1);
                writer.Write(batch, new Dictionary<string, string> { ["b"] = b.ToString() });
            }
        }
        memory.Position = 0;
        using IpcStreamReader reader = IpcStreamReader.Open(memory, Many);
        int n = 0;
        while (reader.ReadNext() is { } batch)
        {
            using (batch) Assert.Equal(((Int32Array)batch.Column(0)).GetValue(0)!.Value.ToString(), reader.BatchMetadata["b"]);
            n++;
        }
        Assert.Equal(20, n);
    }
}
