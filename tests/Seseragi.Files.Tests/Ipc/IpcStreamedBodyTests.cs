using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// A compressed record batch written to a seekable output buffer by buffer while the rest is still compressed, its
/// metadata last, over a placeholder: the bytes are those of the batch written once compressed, whatever the codec,
/// threads, ownership, output or its start; a failure in the middle of the body faults the writer, Dispose returns
/// and every batch the writer took is freed.
/// </summary>
public class IpcStreamedBodyTests
{
    private const long Always = 0, Never = long.MaxValue;

    public static TheoryData<string> Files => [.. IpcWriterTests.Names];

    private static readonly (IpcFormat Format, IIpcCompression Codec, int Frames)[] Ways =
    [
        (IpcFormat.File, Lz4FrameCompression.Instance, 32 << 20),
        (IpcFormat.Stream, new ZstandardCompression(), 4096), // several frames per buffer
        (IpcFormat.File, new ZstandardCompression(), 32 << 20),
    ];

    /// <summary>Every corpus batch streamed (any size) gives the bytes written whole: every layout's metadata fits its placeholder.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void CorpusFiles_Streamed_GiveTheSameBytes(string name)
    {
        foreach (var (format, codec, frames) in Ways)
        {
            if (name == "pyarrow/dictionary_replacement.arrows" && format == IpcFormat.File) continue; // refused by the file format
            var options = new IpcWriteOptions { Format = format, Compression = codec, ZstdFrameBytes = frames, MaxDegreeOfParallelism = 1, StreamBodyFrom = Never };
            byte[] expected = IpcWriterTests.Rewrite(name, options);
            var streamed = options with { StreamBodyFrom = Always };
            Assert.Equal(expected, IpcWriterTests.Rewrite(name, streamed));
            Assert.Equal(expected, IpcWriterTests.Rewrite(name, streamed with { MaxDegreeOfParallelism = 16 }));
            Assert.Equal(expected, IpcOwnershipTests.Rewrite(name, streamed with { MaxDegreeOfParallelism = 16, BatchOwnership = BatchOwnership.Writer }));
            Assert.Equal(expected, IpcOwnershipTests.Rewrite(name, streamed with { MaxDegreeOfParallelism = 16, BatchOwnership = BatchOwnership.Writer, InFlightBytes = 1 }));
        }
    }

    /// <summary>Custom metadata on a streamed batch's message: the placeholder has the same shape, the bytes are the same.</summary>
    [Fact]
    public void BatchMetadata_GivesTheSameBytes_Streamed()
    {
        Dictionary<string, string> metadata = new() { ["origin"] = "test", ["note"] = new string('x', 77) };
        byte[] Write(IpcWriteOptions options)
        {
            using RecordBatch batch = Large(3);
            using var memory = new MemoryStream();
            using (IpcWriter writer = IpcWriter.Create(memory, batch.Schema, options with { LeaveOpen = true }))
            {
                writer.Write(batch, metadata);
                writer.Write(batch);
            }
            return memory.ToArray();
        }
        var options = new IpcWriteOptions { Compression = Lz4FrameCompression.Instance, MaxDegreeOfParallelism = 8, StreamBodyFrom = Never };
        byte[] expected = Write(options);
        Assert.Equal(expected, Write(options with { StreamBodyFrom = Always }));
        using IpcFile file = IpcFile.Open(expected);
        Assert.Equal("test", file.GetBatchMetadata(0)!["origin"]);
    }

    /// <summary>Random columns of every type (dictionaries first, incompressible buffers stored as they are), over 8 MiB.</summary>
    private static RecordBatch Large(int seed) => ParquetWriteData.Wide(seed, 60_000);

    /// <summary>Writes batches made by <paramref name="make"/> (taken by the writer or disposed here) and checks what it counted.</summary>
    private static void Write(Stream output, Func<int, RecordBatch> make, int count, IpcWriteOptions options)
    {
        long start = output.CanSeek ? output.Position : 0;
        Schema schema;
        using (RecordBatch probe = make(0)) schema = probe.Schema;
        using IpcWriter writer = IpcWriter.Create(output, schema, options with { LeaveOpen = true });
        for (int b = 0; b < count; b++)
        {
            RecordBatch batch = make(b);
            writer.Write(batch);
            if (options.BatchOwnership == BatchOwnership.Caller) batch.Dispose();
        }
        writer.Complete();
        if (output.CanSeek)
        {
            Assert.Equal(output.Length - start, writer.BytesWritten);
            Assert.Equal(output.Length, output.Position);
        }
    }

    private static byte[] Bytes(Func<int, RecordBatch> make, int count, IpcWriteOptions options)
    {
        using var memory = new MemoryStream();
        Write(memory, make, count, options);
        return memory.ToArray();
    }

    [Theory]
    [InlineData("lz4", 1)]
    [InlineData("lz4", 16)]
    [InlineData("zstd", 4)]
    public void LargeBatches_GiveTheSameBytes_OnEveryOutput(string codec, int parallelism)
    {
        var options = new IpcWriteOptions
        {
            Format = IpcFormat.Stream, // the dictionaries change from batch to batch
            Compression = codec == "lz4" ? Lz4FrameCompression.Instance : new ZstandardCompression(),
            ZstdFrameBytes = 1 << 20,
            MaxDegreeOfParallelism = parallelism,
            StreamBodyFrom = Never,
        };
        byte[] expected = Bytes(Make, 3, options);
        Assert.True(expected.Length > 3 * (8 << 20), "each batch is over the threshold");
        var streamed = options with { StreamBodyFrom = 8L << 20 };
        Assert.Equal(expected, Bytes(Make, 3, streamed));
        Assert.Equal(expected, Bytes(Make, 3, streamed with { BatchOwnership = BatchOwnership.Writer }));
        Assert.Equal(expected, Bytes(Make, 3, streamed with { BatchOwnership = BatchOwnership.Writer, MaxDegreeOfParallelism = 16, InFlightBytes = 1 }));

        // An output that does not start at 0.
        using (var shifted = new MemoryStream())
        {
            shifted.Write("preamble"u8);
            Write(shifted, Make, 3, streamed);
            Assert.Equal(expected, shifted.ToArray()[8..]);
        }

        // Not seekable: written whole (the bytes do not depend on it).
        using (var plain = new MemoryStream())
        {
            Write(new WriteOnly(plain), Make, 3, streamed);
            Assert.Equal(expected, plain.ToArray());
        }

        // A file.
        string path = Path.Combine(Path.GetTempPath(), $"ipc-streamed-{Guid.NewGuid():N}.arrows");
        try
        {
            using (IpcWriter writer = IpcWriter.Create(path, Large(0).Schema, streamed))
            {
                for (int b = 0; b < 3; b++)
                {
                    using RecordBatch batch = Make(b);
                    writer.Write(batch);
                }
            }
            Assert.Equal(expected, File.ReadAllBytes(path));
        }
        finally
        {
            File.Delete(path);
        }

        static RecordBatch Make(int b) => Large(100 + b);
    }

    [Theory]
    [InlineData(BatchOwnership.Caller)]
    [InlineData(BatchOwnership.Writer)]
    public async Task LargeBatches_WrittenAsync_GiveTheSameBytes(BatchOwnership ownership)
    {
        var options = new IpcWriteOptions { Compression = Lz4FrameCompression.Instance, MaxDegreeOfParallelism = 8, StreamBodyFrom = Never };
        using RecordBatch first = Large(7);
        byte[] expected = Bytes(_ => Large(7), 1, options);
        string path = Path.Combine(Path.GetTempPath(), $"ipc-streamed-{Guid.NewGuid():N}.arrow");
        try
        {
            await using (IpcWriter writer = IpcWriter.Create(path, first.Schema, options with { StreamBodyFrom = Always, BatchOwnership = ownership }))
            {
                await writer.WriteAsync(Large(7));
                await writer.CompleteAsync();
                Assert.Equal(expected.Length, writer.BytesWritten);
            }
            Assert.Equal(expected, await File.ReadAllBytesAsync(path));
            var memory = new MemoryStream();
            await IpcWriter.WriteAsync(memory, first.Schema, One(), options with { StreamBodyFrom = Always, BatchOwnership = ownership });
            Assert.Equal(expected, memory.ToArray());
        }
        finally
        {
            File.Delete(path);
        }

        async IAsyncEnumerable<RecordBatch> One()
        {
            await Task.Yield();
            RecordBatch batch = Large(7);
            yield return batch;
            if (ownership == BatchOwnership.Caller) batch.Dispose(); // written by now
        }
    }

    /// <summary>The stream fails in the middle of a streamed body: the failure is thrown, the writer is faulted, Dispose returns.</summary>
    [Theory]
    [InlineData(BatchOwnership.Caller, 1)]
    [InlineData(BatchOwnership.Caller, 16)]
    [InlineData(BatchOwnership.Writer, 16)]
    public void AFailureMidBody_FaultsTheWriter(BatchOwnership ownership, int parallelism)
    {
        var options = new IpcWriteOptions { Compression = Lz4FrameCompression.Instance, MaxDegreeOfParallelism = parallelism, StreamBodyFrom = Always,
            BatchOwnership = ownership };
        long length = Bytes(_ => Large(5), 1, options with { BatchOwnership = BatchOwnership.Caller }).Length;
        foreach (long limit in (long[])[4096, length / 3, length * 2 / 3])
        {
            using RecordBatch schemaOf = Large(5);
            var writer = IpcWriter.Create(new FailingSeekable(limit), schemaOf.Schema, options);
            RecordBatch batch = Large(5);
            Exception? error = Record.Exception(() =>
            {
                writer.Write(batch);
                writer.Complete();
            });
            Assert.IsType<IOException>(error);
            Assert.Throws<InvalidOperationException>(() => writer.Complete());
            writer.Dispose();
            if (ownership == BatchOwnership.Caller) batch.Dispose();
        }
    }

    /// <summary>Batches taken by the writer are freed once written, and on a failure in the middle of a streamed body.</summary>
    [Fact]
    public void BatchesTaken_AreFreed_WhenAStreamedBodyFails()
    {
        var options = new IpcWriteOptions { BatchOwnership = BatchOwnership.Writer, Compression = Lz4FrameCompression.Instance, MaxDegreeOfParallelism = 4,
            StreamBodyFrom = Always };
        var (schema, batches) = IpcOwnershipTests.Batches(8);
        NativeMemoryOwner[] owners = [.. batches.Select(b => IpcOwnershipTests.Owner(b.Column(0)))];
        var writer = IpcWriter.Create(new FailingSeekable(2048), schema, options);
        int handed = 0;
        Exception? error = Record.Exception(() =>
        {
            foreach (RecordBatch batch in batches)
            {
                handed++;
                writer.Write(batch);
            }
            writer.Complete();
        });
        Assert.IsType<IOException>(error);
        writer.Dispose();
        Assert.All(owners[..handed], o => Assert.True(o.IsFreed));
        foreach (RecordBatch batch in batches.Skip(handed)) batch.Dispose();

        // Written whole: freed, and the file reads.
        (schema, batches) = IpcOwnershipTests.Batches(8);
        owners = [.. batches.Select(b => IpcOwnershipTests.Owner(b.Column(0)))];
        using var memory = new MemoryStream();
        using (IpcWriter ok = IpcWriter.Create(memory, schema, options with { LeaveOpen = true }))
        {
            foreach (RecordBatch batch in batches) ok.Write(batch);
            ok.Complete();
            Assert.All(owners, o => Assert.True(o.IsFreed));
        }
        using IpcFile file = IpcFile.Open(memory.ToArray());
        Assert.Equal(8, file.BatchCount);
    }

    /// <summary>
    /// The caller's batches are written behind, one at a time: when Write returns, the batch before is written (else
    /// compressed batches would pile up in memory behind a slow output).
    /// </summary>
    [Fact]
    public void CallerWrites_LeaveOneBatchBehind_AtMost()
    {
        var options = new IpcWriteOptions { Format = IpcFormat.Stream, Compression = Lz4FrameCompression.Instance, MaxDegreeOfParallelism = 4,
            StreamBodyFrom = Always };
        var (schema, batches) = IpcOwnershipTests.Batches(5);
        try
        {
            // Where the first k batches end: written without delay, then completed (the stream format ends with 8 bytes).
            long[] ends = new long[batches.Count + 1];
            for (int k = 0; k <= batches.Count; k++)
            {
                using var writer = IpcWriter.Create(new MemoryStream(), schema, options);
                foreach (RecordBatch batch in batches.Take(k)) writer.Write(batch);
                writer.Complete();
                ends[k] = writer.BytesWritten - 8;
            }
            using (var slow = IpcWriter.Create(new SlowSeekable(), schema, options))
            {
                for (int k = 0; k < batches.Count; k++)
                {
                    slow.Write(batches[k]);
                    if (k > 0) Assert.True(slow.BytesWritten >= ends[k], $"batch {k - 1} is still on its way when Write({k}) returns");
                }
            }
        }
        finally
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }

    /// <summary>A seekable stream that takes 5 ms per write.</summary>
    private sealed class SlowSeekable : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count)
        {
            Thread.Sleep(5);
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer) => Write(buffer.ToArray(), 0, buffer.Length);
    }

    /// <summary>A seekable stream that fails once more than <c>limit</c> bytes are written.</summary>
    private sealed class FailingSeekable(long limit) : MemoryStream
    {
        private long _written;

        public override void Write(byte[] buffer, int offset, int count)
        {
            _written += count;
            if (_written > limit) throw new IOException("The disk is full.");
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer) => Write(buffer.ToArray(), 0, buffer.Length);
    }

    /// <summary>A stream that can only be written to, forward (a socket, a pipe).</summary>
    private sealed class WriteOnly(Stream inner) : Stream
    {
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);
    }
}
