using System.IO.Pipelines;
using Polars.NET.Core;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Tests for writing / sinking into a .NET <see cref="Stream"/> (sync and async) via <see cref="NativeStreamWriter"/>.
/// Format-specific tests live in <c>StreamWriteTests.{Format}.cs</c>.
/// </summary>
public partial class StreamWriteTests
{
    private static DataFrame SampleFrame() => DataFrame.FromColumns(new
    {
        id = new[] { 1, 2, 3, 4, 5 },
        name = new[] { "Alice", "Bob", null, "David", "Eve" },
        score = new[] { 1.5, 2.5, 3.5, 4.5, 5.5 },
    });

    /// <summary>~ several MB of output, so it spans many native chunks.</summary>
    private static DataFrame LargeFrame(int rows = 300_000) => DataFrame.FromColumns(new
    {
        id = Enumerable.Range(0, rows).ToArray(),
        text = Enumerable.Range(0, rows).Select(i => $"row-{i:D8}").ToArray(),
    });

    // ---------------- Test streams ----------------

    /// <summary>Records writes; optionally slow (to exercise backpressure) or failing after N bytes.</summary>
    private sealed class RecordingStream(bool allowSyncWrites = true, long failAfterBytes = long.MaxValue, int asyncDelayMs = 0)
        : MemoryStream
    {
        public int WriteCalls;
        public int FlushCalls;

        private void Check(int count)
        {
            if (Length + count > failAfterBytes) throw new IOException("boom: destination failed");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!allowSyncWrites) throw new InvalidOperationException("Synchronous operations are disallowed.");
            Check(count);
            WriteCalls++;
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (!allowSyncWrites) throw new InvalidOperationException("Synchronous operations are disallowed.");
            Check(buffer.Length);
            WriteCalls++;
            byte[] copy = buffer.ToArray();
            base.Write(copy, 0, copy.Length);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (asyncDelayMs > 0) await Task.Delay(asyncDelayMs, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            Check(buffer.Length);
            WriteCalls++;
            // Not base.Write(span): MemoryStream subclasses route that back to the (blocked) sync overload.
            byte[] copy = buffer.ToArray();
            base.Write(copy, 0, copy.Length);
        }

        public override void Flush()
        {
            if (!allowSyncWrites) throw new InvalidOperationException("Synchronous operations are disallowed.");
            FlushCalls++;
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            FlushCalls++;
            return Task.CompletedTask;
        }
    }

    // ---------------- IPC ----------------

    [Fact]
    public void WriteIpcStream_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteIpcStream(ms, IpcCompression.LZ4);

        Assert.Equal(df.WriteIpcStreamMemory(IpcCompression.LZ4), ms.ToArray());
        using var back = DataFrame.ReadIpcStream(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task WriteIpcStreamAsync_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.WriteIpcStreamAsync(ms, IpcCompression.LZ4);

        Assert.Equal(df.WriteIpcStreamMemory(IpcCompression.LZ4), ms.ToArray());
        Assert.Equal(1, ms.FlushCalls);
    }

    [Fact]
    public void WriteIpc_ToStream_RoundTrip()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteIpc(ms);

        ms.Position = 0;
        using var back = DataFrame.ReadIpc(ms);
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task WriteIpcAsync_ToStream_RoundTrip()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.WriteIpcAsync(ms);

        using var back = DataFrame.ReadIpc(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void SinkIpc_ToStream_RoundTrip()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.Lazy().Filter(Col("id") > 2).SinkIpc(ms);

        using var back = DataFrame.ReadIpc(ms.ToArray());
        Assert.Equal(3, back.Height);
        Assert.Equal(3, back.GetValue<int>(0, "id"));
    }

    [Fact]
    public async Task SinkIpcAsync_ToStream_RoundTrip()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.Lazy().Filter(Col("id") > 2).SinkIpcAsync(ms);

        using var back = DataFrame.ReadIpc(ms.ToArray());
        Assert.Equal(3, back.Height);
    }

    // ---------------- Streaming / chunking ----------------

    [Fact]
    public void LargeOutput_IsWrittenInMultipleChunks()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream();

        df.WriteIpcStream(ms);

        Assert.True(ms.WriteCalls > 1, $"expected multiple chunks, got {ms.WriteCalls}");
        using var back = DataFrame.ReadIpcStream(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task LargeOutput_Async_WithSlowDestination_Backpressure()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream(allowSyncWrites: false, asyncDelayMs: 1);

        await df.Lazy().SinkIpcAsync(ms);

        Assert.True(ms.WriteCalls > 1);
        using var back = DataFrame.ReadIpc(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    // ---------------- Errors ----------------

    [Fact]
    public void DestinationFailure_PropagatesOriginalException()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream(failAfterBytes: 100_000);

        var ex = Assert.Throws<IOException>(() => df.WriteIpcStream(ms));
        Assert.Equal("boom: destination failed", ex.Message);

        // Lazy sink path (streaming engine, worker threads) as well.
        using var ms2 = new RecordingStream(failAfterBytes: 100_000);
        var ex2 = Assert.Throws<IOException>(() => df.Lazy().SinkIpc(ms2));
        Assert.Equal("boom: destination failed", ex2.Message);
    }

    [Fact]
    public async Task DestinationFailure_Async_PropagatesOriginalException()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream(allowSyncWrites: false, failAfterBytes: 100_000);

        var ex = await Assert.ThrowsAsync<IOException>(() => df.Lazy().SinkIpcAsync(ms));
        Assert.Equal("boom: destination failed", ex.Message);
    }

    [Fact]
    public void QueryFailure_PropagatesPolarsError()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        Assert.ThrowsAny<Exception>(() => df.Lazy().Select(Col("does_not_exist")).SinkIpc(ms));
    }

    [Fact]
    public async Task QueryFailure_Async_PropagatesPolarsError()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        await Assert.ThrowsAnyAsync<Exception>(() => df.Lazy().Select(Col("does_not_exist")).SinkIpcAsync(ms));
    }

    [Fact]
    public void NonWritableStream_Throws()
    {
        using var df = SampleFrame();
        using var ro = new MemoryStream([], writable: false);

        Assert.Throws<ArgumentException>(() => df.WriteIpcStream(ro));
    }

    [Fact]
    public async Task Cancellation_Async_Throws()
    {
        using var df = LargeFrame();
        using var cts = new CancellationTokenSource();
        using var ms = new RecordingStream(allowSyncWrites: false, asyncDelayMs: 5);

        cts.CancelAfter(20);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => df.Lazy().SinkIpcAsync(ms, cancellationToken: cts.Token));

        // Already-cancelled token: fails fast.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => df.WriteIpcStreamAsync(ms, cancellationToken: cts.Token));
    }

    [Fact]
    public void StreamIsNotDisposed()
    {
        using var df = SampleFrame();
        var ms = new MemoryStream();

        df.WriteIpcStream(ms);
        df.WriteIpcStream(ms); // still usable

        Assert.True(ms.CanWrite);
        ms.Dispose();
    }

    // ---------------- PipeWriter ----------------

    /// <summary>Runs <paramref name="write"/> against a Pipe and collects everything the reader receives.</summary>
    private static async Task<byte[]> CollectFromPipeAsync(Func<PipeWriter, Task> write, long pauseThreshold = 64 * 1024)
    {
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: pauseThreshold, resumeWriterThreshold: pauseThreshold / 2));
        using var ms = new MemoryStream();
        var reading = pipe.Reader.CopyToAsync(ms);

        await write(pipe.Writer);
        await pipe.Writer.CompleteAsync();
        await reading;
        return ms.ToArray();
    }

    [Fact]
    public async Task WriteIpcStreamAsync_ToPipeWriter_MatchesMemory()
    {
        using var df = SampleFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.WriteIpcStreamAsync(w, IpcCompression.LZ4));

        Assert.Equal(df.WriteIpcStreamMemory(IpcCompression.LZ4), bytes);
    }

    [Fact]
    public async Task SinkIpcAsync_ToPipeWriter_LargeOutput_Backpressure()
    {
        using var df = LargeFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.Lazy().SinkIpcAsync(w), pauseThreshold: 16 * 1024);

        using var back = DataFrame.ReadIpc(bytes);
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task WriteIpcAsync_ToPipeWriter_ReaderGone_Throws()
    {
        using var df = LargeFrame();
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 16 * 1024, resumeWriterThreshold: 8 * 1024));
        await pipe.Reader.CompleteAsync(); // e.g. client disconnected

        await Assert.ThrowsAsync<IOException>(() => df.WriteIpcAsync(pipe.Writer));
    }
}
