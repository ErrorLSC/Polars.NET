using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Compression;

namespace Sazanami.Tests;

/// <summary>
/// Bodies handed over buffer by buffer as they are compressed: each buffer is the one compressed whole, and a failed
/// (here cancelled) compression fails every message and every buffer still waited for, so that no writer waits forever.
/// </summary>
public unsafe class IpcBodyStreamTests
{
    private static readonly byte[][] Data = Make();

    private static byte[][] Make()
    {
        var random = new Random(5);
        byte[][] data = [[], [.. Enumerable.Range(0, 300_000).Select(i => (byte)(i % 251))], new byte[200_000], new byte[70_000], [],
            [.. Enumerable.Range(0, 2_500_003).Select(i => (byte)(i % 7 == 0 ? random.Next() : i / 1000))], // several units of several blocks
            new byte[6 << 20]]; // more than a chunk: each thread moves on to new chunks
        random.NextBytes(data[2]); // stored as it is
        random.NextBytes(data[6]);
        return data;
    }

    private static IpcOutgoingMessage Message()
    {
        var body = new IpcEncodedBody();
        foreach (byte[] d in Data)
        {
            byte* p = body.Make(Math.Max(1, d.Length));
            d.CopyTo(new Span<byte>(p, d.Length));
            body.Buffers.Add(new IpcPiece((nint)p, d.Length));
        }
        return new IpcOutgoingMessage(body, 1, IpcCompression.Lz4Frame, false, static (_, _) => new byte[24]);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(16)]
    public void StreamedBuffers_AreThoseCompressedWhole(int parallelism)
    {
        using IpcOutgoingMessage whole = Message();
        whole.Compress(0, 1, IpcBatchEncoder.ZstdFrameBytes);
        using IpcOutgoingMessage streamed = Message();
        streamed.StreamBody();
        streamed.Compress(0, parallelism, IpcBatchEncoder.ZstdFrameBytes, detach: true);
        Assert.True(streamed.Compressed.IsCompletedSuccessfully);
        Assert.Equal(whole.Layout, streamed.Layout);
        Assert.Equal(streamed.Placeholder(0).Length, IpcMessageEncoder.Frame(new byte[24], 0).Length);
        for (int i = 0; i < Data.Length; i++)
        {
            IpcStreamedBuffer buffer = streamed.Streamed!.Ready(i).Result;
            IpcPiece expected = whole.Body!.Buffers[i];
            Assert.Equal((expected.Prefix, expected.Prefixed, expected.Size), (buffer.Piece.Prefix, buffer.Piece.Prefixed, buffer.Piece.Size));
            Assert.Equal(expected.Span.ToArray(), Bytes(buffer));
            if (buffer.Piece.Prefix == -1) Assert.NotEqual(0, buffer.Owned); // detached: a copy of ours
            streamed.Streamed.Release(i);
        }
        Assert.Equal(0, streamed.Streamed!.LiveChunks); // every unit written (or not used) and every packer done: all back in the pool
    }

    /// <summary>A streamed buffer's bytes as written.</summary>
    private static byte[] Bytes(IpcStreamedBuffer buffer)
    {
        if (buffer.Pieces is null) return buffer.Piece.Span.ToArray();
        List<byte> bytes = [];
        if (buffer.Framed) bytes.AddRange(IpcBodyStream.Lz4FrameHeader);
        foreach (IpcChunkPiece piece in buffer.Pieces) bytes.AddRange(new ReadOnlySpan<byte>((void*)piece.Pointer, piece.Length));
        if (buffer.Framed) bytes.AddRange(IpcBodyStream.Lz4FrameEnd);
        return [.. bytes];
    }

    [Fact]
    public void AFailedCompression_FailsEveryMessageAndBufferWaitedFor()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        // The streamed message fails while compressed: its empty buffers were handed over before any work, the rest fail.
        using (IpcOutgoingMessage alone = Message())
        {
            alone.StreamBody();
            Assert.ThrowsAny<Exception>(() => IpcOutgoingMessage.CompressAll([alone], (0, 4, IpcBatchEncoder.ZstdFrameBytes), false, cancelled.Token));
            Assert.True(alone.Compressed.IsFaulted);
            for (int i = 0; i < Data.Length; i++) Assert.Equal(Data[i].Length == 0, alone.Streamed!.Ready(i).IsCompletedSuccessfully);
            for (int i = 0; i < Data.Length; i++) Assert.True(alone.Streamed!.Ready(i).IsCompleted, $"buffer {i} is still waited for");
        }

        // A message before it fails: it is never compressed, and fails whole.
        using IpcOutgoingMessage first = Message(), second = Message();
        second.StreamBody();
        Assert.ThrowsAny<Exception>(() => IpcOutgoingMessage.CompressAll([first, second], (0, 4, IpcBatchEncoder.ZstdFrameBytes), false, cancelled.Token));
        Assert.True(first.Compressed.IsFaulted);
        Assert.True(second.Compressed.IsFaulted);
        for (int i = 0; i < Data.Length; i++) Assert.True(second.Streamed!.Ready(i).IsFaulted, $"buffer {i} is still waited for");
    }
}
