using System.Buffers;
using System.IO.Pipelines;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// A write-only stream into a buffer writer, for the writers that write streams. A <see cref="PipeWriter"/> is flushed
/// once a mebibyte is written since its last flush, and on <see cref="Flush"/>, so writing waits while its reader is
/// behind; it is never completed here (the pipe stays the caller's). Disposing the stream leaves the output as it is.
/// </summary>
internal sealed class BufferWriterStream(IBufferWriter<byte> output) : Stream
{
    private const int FlushEvery = 1 << 20;
    private readonly PipeWriter? _pipe = output as PipeWriter;
    private long _written, _unflushed;

    /// <summary>The stream into <paramref name="output"/> (checked for null).</summary>
    public static BufferWriterStream Over(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        return new(output);
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    /// <summary>The bytes written so far.</summary>
    public override long Position
    {
        get => _written;
        set => throw new NotSupportedException();
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        Copy(buffer);
        if (_pipe is not null && _unflushed >= FlushEvery) Flush();
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void WriteByte(byte value) => Write([value]);

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Copy(buffer.Span);
        if (_pipe is not null && _unflushed >= FlushEvery) await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush()
    {
        if (_pipe is null) return;
        _unflushed = 0;
        PipeFlush.FlushAsync(_pipe, useAsync: false, default).GetAwaiter().GetResult();
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_pipe is null) return;
        _unflushed = 0;
        await PipeFlush.FlushAsync(_pipe, useAsync: true, cancellationToken).ConfigureAwait(false);
    }

    private void Copy(ReadOnlySpan<byte> rest)
    {
        _written += rest.Length;
        _unflushed += rest.Length;
        while (!rest.IsEmpty)
        {
            // A pipe hands out segments of its pool's size; other outputs grow once to take the whole write.
            Span<byte> span = output.GetSpan(_pipe is null ? rest.Length : Math.Min(rest.Length, 1 << 16));
            int n = Math.Min(span.Length, rest.Length);
            rest[..n].CopyTo(span);
            output.Advance(n);
            rest = rest[n..];
        }
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();
}

/// <summary>A pipe's flush, the way every writer reports it: a reader that stopped is an <see cref="IOException"/>.</summary>
internal static class PipeFlush
{
    public static async ValueTask FlushAsync(PipeWriter pipe, bool useAsync, CancellationToken ct)
    {
        ValueTask<FlushResult> flush = pipe.FlushAsync(ct);
        FlushResult result = flush.IsCompletedSuccessfully ? flush.Result
            : useAsync ? await flush.ConfigureAwait(false)
            : flush.AsTask().GetAwaiter().GetResult();
        if (result.IsCanceled) throw new OperationCanceledException("The pipe's flush was canceled.", ct);
        if (result.IsCompleted) throw new IOException("The destination stopped accepting data.");
    }
}
