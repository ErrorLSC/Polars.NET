using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// <see cref="BatchOwnership.Writer"/>: the writer takes the batches, compresses several side by side and writes them in
/// order: the bytes are those of <see cref="BatchOwnership.Caller"/> whatever the threads and the memory allowed ahead,
/// and every batch taken is freed, once written or on any failure.
/// </summary>
public class IpcOwnershipTests
{
    private static string Path(string name) => System.IO.Path.Combine(IpcValueTests.Root, name);

    public static TheoryData<string> Files => [.. IpcWriterTests.Names];

    /// <summary>Writes a corpus file's batches again: handed over (Writer), or disposed by us after each Write (Caller).</summary>
    internal static byte[] Rewrite(string name, IpcWriteOptions options)
    {
        var output = new MemoryStream();
        bool owned = options.BatchOwnership == BatchOwnership.Writer;
        IEnumerable<RecordBatch> batches;
        Schema schema;
        IDisposable source;
        if (IpcValueTests.IsFile(name))
        {
            var file = IpcFile.Open(Path(name));
            IpcReader reader = file.OpenReader(new IpcReadOptions { MemoryMap = IpcMemoryMap.Off });
            (schema, batches, source) = (reader.Schema, reader.ReadBatches(), new Both(reader, file));
        }
        else
        {
            IpcStreamReader reader = IpcStreamReader.Open(File.OpenRead(Path(name)));
            (schema, batches, source) = (reader.Schema, reader.ReadBatches(), reader);
        }
        using (source)
        using (IpcWriter writer = IpcWriter.Create(output, schema, options with { LeaveOpen = true }))
        {
            foreach (RecordBatch batch in batches)
            {
                writer.Write(batch);
                if (!owned) batch.Dispose();
            }
        }
        return output.ToArray();
    }

    private sealed class Both(IDisposable a, IDisposable b) : IDisposable
    {
        public void Dispose()
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void TheBytes_DoNotDependOnOwnershipNorThreads(string name)
    {
        (IpcFormat Format, IIpcCompression Codec, int Frames)[] ways =
        [
            (IpcFormat.File, NoCompression.Instance, 32 << 20),
            (IpcFormat.Stream, Lz4FrameCompression.Instance, 32 << 20),
            (IpcFormat.File, new ZstandardCompression(), 4096),
        ];
        foreach (var (format, codec, frames) in ways)
        {
            if (name == "pyarrow/dictionary_replacement.arrows" && format == IpcFormat.File) continue; // refused by the file format
            var options = new IpcWriteOptions { Format = format, Compression = codec, ZstdFrameBytes = frames };
            byte[] expected = Rewrite(name, options with { MaxDegreeOfParallelism = 1 });
            foreach (IpcWriteOptions owned in (IpcWriteOptions[])
                [
                    options with { BatchOwnership = BatchOwnership.Writer, MaxDegreeOfParallelism = 1 },
                    options with { BatchOwnership = BatchOwnership.Writer, MaxDegreeOfParallelism = 16 },
                    options with { BatchOwnership = BatchOwnership.Writer, MaxDegreeOfParallelism = 16, InFlightBytes = 1 }, // one batch at a time
                ])
            {
                Assert.Equal(expected, Rewrite(name, owned));
            }
        }
    }

    internal static NativeMemoryOwner Owner(IArrowArray array)
    {
        foreach (ArrowBuffer buffer in array.Data.Buffers)
        {
            if (MemoryMarshal.TryGetMemoryManager(buffer.Memory, out NativeBufferMemory? memory)) return memory.Owner;
        }
        throw new InvalidOperationException("No native buffer.");
    }

    /// <summary>Batches of one column read into native memory of their own: what was taken is seen freed.</summary>
    internal static (Schema Schema, List<RecordBatch> Batches) Batches(int count)
    {
        using IpcFile file = IpcFile.Open(Path("pyarrow/many_batches.arrow"));
        using IpcReader reader = file.OpenReader(new IpcReadOptions { Columns = ["i64"], MemoryMap = IpcMemoryMap.Off, MaxDegreeOfParallelism = 1 });
        return (reader.Schema, [.. reader.ReadBatches().Take(count)]);
    }

    [Fact]
    public void BatchesTaken_AreFreedOnceWritten_AndOnEveryFailure()
    {
        var options = new IpcWriteOptions { BatchOwnership = BatchOwnership.Writer, Compression = Lz4FrameCompression.Instance, MaxDegreeOfParallelism = 4 };

        // Written: freed by Complete at the latest.
        var (schema, batches) = Batches(6);
        NativeMemoryOwner[] owners = [.. batches.Select(b => Owner(b.Column(0)))];
        using (var memory = new MemoryStream())
        {
            using (IpcWriter writer = IpcWriter.Create(memory, schema, options with { LeaveOpen = true }))
            {
                foreach (RecordBatch batch in batches) writer.Write(batch);
                writer.Complete();
                Assert.All(owners, o => Assert.True(o.IsFreed, "written"));
            }
            using IpcFile file = IpcFile.Open(memory.ToArray());
            Assert.Equal(6, file.BatchCount);
        }

        // Refused (another schema): freed, nothing written; the writer stays usable.
        (schema, batches) = Batches(2);
        owners = [.. batches.Select(b => Owner(b.Column(0)))];
        var other = new Schema([new Field("x", Int32Type.Default, false), new Field("y", Int32Type.Default, false)], null);
        using (IpcWriter writer = IpcWriter.Create(new MemoryStream(), other, options))
        {
            Assert.Throws<ArgumentException>(() => writer.Write(batches[0]));
            Assert.True(owners[0].IsFreed);
        }
        batches[1].Dispose();

        // A stream failing: every batch taken is freed by Dispose, written or not.
        (schema, batches) = Batches(8);
        owners = [.. batches.Select(b => Owner(b.Column(0)))];
        var writerOfFailing = IpcWriter.Create(new FailingStream(2048), schema, options);
        int handed = 0; // a Write that throws has taken its batch; those after it stay ours
        Exception? error = Record.Exception(() =>
        {
            foreach (RecordBatch batch in batches)
            {
                handed++;
                writerOfFailing.Write(batch);
            }
            writerOfFailing.Complete();
        });
        Assert.IsType<IOException>(error);
        writerOfFailing.Dispose();
        Assert.All(owners[..handed], o => Assert.True(o.IsFreed, "a failing stream"));
        Assert.All(owners[handed..], o => Assert.False(o.IsFreed));
        foreach (RecordBatch batch in batches.Skip(handed)) batch.Dispose();

        // Aborted with batches on their way: freed, not completed.
        (schema, batches) = Batches(8);
        owners = [.. batches.Select(b => Owner(b.Column(0)))];
        var memoryOfAborted = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(memoryOfAborted, schema, options with { LeaveOpen = true }))
        {
            foreach (RecordBatch batch in batches) writer.Write(batch);
            writer.Abort();
        }
        Assert.All(owners, o => Assert.True(o.IsFreed, "aborted"));
        Assert.ThrowsAny<IpcFormatException>(() => IpcFile.Open(memoryOfAborted.ToArray()));
    }

    /// <summary>Typed rows through the writer that owns their batches: the bytes of one thread, with any memory ahead.</summary>
    [Fact]
    public void TypedRows_GiveTheSameBytes_WhateverTheMemoryAhead()
    {
        var rows = Seseragi.Files.Tests.Parquet.ParquetWriterOfTTests.Trades(60_000);
        byte[] Write(IpcWriteOptions options)
        {
            using var memory = new MemoryStream();
            IpcWriter.WriteRows(memory, rows, options with { LeaveOpen = true });
            return memory.ToArray();
        }
        var lz4 = new IpcWriteOptions { Compression = Lz4FrameCompression.Instance };
        byte[] expected = Write(lz4 with { MaxDegreeOfParallelism = 1 });
        Assert.Equal(expected, Write(lz4 with { MaxDegreeOfParallelism = 16 }));
        Assert.Equal(expected, Write(lz4 with { MaxDegreeOfParallelism = 16, InFlightBytes = 1 }));
    }

    /// <summary>A stream that fails once more than <c>limit</c> bytes are written.</summary>
    private sealed class FailingStream(int limit) : Stream
    {
        private long _written;

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
    }
}
