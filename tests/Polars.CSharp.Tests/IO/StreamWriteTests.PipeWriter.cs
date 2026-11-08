using System.IO.Pipelines;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// PipeWriter edge cases: backpressure, cancellation, reader shutdown, writer ownership.
/// Per-format happy paths live in the other <c>StreamWriteTests.*.cs</c> files.
/// </summary>
public partial class StreamWriteTests
{
    private static Pipe SmallPipe(long pause = 16 * 1024) =>
        new(new PipeOptions(pauseWriterThreshold: pause, resumeWriterThreshold: pause / 2, useSynchronizationContext: false));

    [Fact]
    public async Task PipeWriter_IsFlushedButNotCompleted_CallerCanKeepWriting()
    {
        using var df = SampleFrame();
        var pipe = SmallPipe(1024 * 1024);

        await df.WriteIpcStreamAsync(pipe.Writer);

        // Writer still usable: append a trailer, then complete ourselves.
        byte[] trailer = [0xDE, 0xAD, 0xBE, 0xEF];
        await pipe.Writer.WriteAsync(trailer);
        await pipe.Writer.CompleteAsync();

        using var ms = new MemoryStream();
        await pipe.Reader.CopyToAsync(ms);
        byte[] all = ms.ToArray();

        byte[] expected = df.WriteIpcStreamMemory();
        Assert.Equal(expected, all[..expected.Length]);
        Assert.Equal(trailer, all[expected.Length..]);
    }

    [Fact]
    public async Task PipeWriter_SequentialWritesIntoSameWriter()
    {
        using var df = SampleFrame();
        byte[] one = df.WriteParquetMemory();

        byte[] bytes = await CollectFromPipeAsync(async w =>
        {
            await df.WriteParquetAsync(w);
            await df.Lazy().SinkParquetAsync(w);
        });

        Assert.Equal(one.Length * 2, bytes.Length);
        Assert.Equal(one, bytes[..one.Length]);
        Assert.Equal(one, bytes[one.Length..]);
    }

    [Fact]
    public async Task PipeWriter_ConcurrentWritersIntoSeparatePipes()
    {
        using var df = LargeFrame(50_000);
        byte[] expected = df.WriteIpcStreamMemory();

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ =>
            CollectFromPipeAsync(w => df.WriteIpcStreamAsync(w))));

        foreach (var r in results)
            Assert.Equal(expected, r);
    }

    [Fact]
    public async Task PipeWriter_SlowReader_BackpressureIsRespected()
    {
        using var df = LargeFrame();
        const long pause = 16 * 1024;
        var pipe = SmallPipe(pause);

        long maxUnread = 0;
        using var ms = new MemoryStream();
        var reading = Task.Run(async () =>
        {
            while (true)
            {
                var result = await pipe.Reader.ReadAsync();
                maxUnread = Math.Max(maxUnread, result.Buffer.Length);
                foreach (var seg in result.Buffer) ms.Write(seg.Span);
                pipe.Reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted) break;
                await Task.Delay(1);
            }
            await pipe.Reader.CompleteAsync();
        });

        await df.Lazy().SinkIpcAsync(pipe.Writer);
        await pipe.Writer.CompleteAsync();
        await reading;

        using var back = DataFrame.ReadIpc(ms.ToArray());
        Assert.True(df.Equals(back));
        // Native callbacks are capped at 64 KB (large record batches are split) and the writer flushes
        // after each one, so unread data stays bounded by one chunk above the pause threshold.
        Assert.True(maxUnread <= pause + 64 * 1024 + 16 * 1024, $"unread grew to {maxUnread} bytes");
    }

    [Fact]
    public async Task PipeWriter_CancelWhileBlockedOnBackpressure_Throws()
    {
        using var df = LargeFrame();
        var pipe = SmallPipe(); // nobody reads: the writer blocks once 16 KB are pending
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var write = df.Lazy().SinkIpcAsync(pipe.Writer, cancellationToken: cts.Token);
        var finished = await Task.WhenAny(write, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.Same(write, finished); // did not hang
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write);
    }

    [Fact]
    public async Task PipeWriter_CancelPendingFlush_Throws()
    {
        using var df = LargeFrame();
        var pipe = SmallPipe();

        var write = df.WriteIpcStreamAsync(pipe.Writer);
        await Task.Delay(100); // let the writer block on backpressure
        pipe.Writer.CancelPendingFlush();

        var finished = await Task.WhenAny(write, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(write, finished);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write);
    }

    [Fact]
    public async Task PipeWriter_ReaderCompletesMidStream_ThrowsIOException()
    {
        using var df = LargeFrame();
        var pipe = SmallPipe();

        var reading = Task.Run(async () =>
        {
            long total = 0;
            while (total < 64 * 1024)
            {
                var result = await pipe.Reader.ReadAsync();
                total += result.Buffer.Length;
                pipe.Reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted) break;
            }
            await pipe.Reader.CompleteAsync(); // e.g. client disconnected
        });

        await Assert.ThrowsAsync<IOException>(() => df.Lazy().SinkParquetAsync(pipe.Writer));
        await reading;
    }

    [Fact]
    public async Task PipeWriter_ReaderCompletesWithError_Throws()
    {
        using var df = LargeFrame();
        var pipe = SmallPipe();
        var readerError = new InvalidOperationException("reader blew up");

        var reading = Task.Run(async () =>
        {
            var result = await pipe.Reader.ReadAsync();
            pipe.Reader.AdvanceTo(result.Buffer.End);
            await pipe.Reader.CompleteAsync(readerError);
        });

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => df.WriteIpcStreamAsync(pipe.Writer));
        Assert.True(ex is IOException || ReferenceEquals(ex, readerError), $"unexpected {ex.GetType()}: {ex.Message}");
        await reading;
    }

    [Fact]
    public async Task PipeWriter_QueryFailure_PropagatesAndWriterStaysUsable()
    {
        using var df = SampleFrame();
        var pipe = SmallPipe(1024 * 1024);

        await Assert.ThrowsAnyAsync<Exception>(() => df.Lazy().Select(Col("does_not_exist")).SinkIpcAsync(pipe.Writer));

        // Not completed by the failed write: the caller decides how to end the response.
        await pipe.Writer.WriteAsync(new byte[] { 1 });
        await pipe.Writer.CompleteAsync();
    }

    [Fact]
    public async Task PipeWriter_AlreadyCancelledToken_FailsFast()
    {
        using var df = SampleFrame();
        var pipe = SmallPipe();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => df.WriteIpcStreamAsync(pipe.Writer, cancellationToken: cts.Token));
        Assert.False(pipe.Reader.TryRead(out _)); // nothing written
    }
}
