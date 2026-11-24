using System.Buffers;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Text.Unicode;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.IO.Writing;

/// <summary>
/// Where a text writer's bytes (UTF-8, or an encoding's) go, in order: a file, a stream, a buffer writer (a pipe), a
/// text writer, or a string. Bytes come in whole rows, so a text output never sees a character cut in two.
/// </summary>
internal abstract class ByteSink
{
    /// <summary>Whether the output is text (no compression).</summary>
    public virtual bool IsText => false;

    public abstract ValueTask WriteAsync(ReadOnlyMemory<byte> utf8, bool useAsync, CancellationToken ct);

    /// <summary>Once every byte is written.</summary>
    public virtual ValueTask CompleteAsync(bool useAsync, CancellationToken ct) => default;

    /// <summary>Closes what the sink owns (whether or not it was completed).</summary>
    public virtual ValueTask CloseAsync(bool useAsync) => default;
}

/// <summary>A file of the writer's own: each batch's pieces written at once (one gathering write), in order.</summary>
internal sealed class FileSink(string path) : ByteSink
{
    private readonly Microsoft.Win32.SafeHandles.SafeFileHandle _file = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.None);
    private long _length;

    public void WriteAll(IReadOnlyList<ReadOnlyMemory<byte>> buffers, long bytes)
    {
        RandomAccess.Write(_file, buffers, _length);
        _length += bytes;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> utf8, bool useAsync, CancellationToken ct)
    {
        RandomAccess.Write(_file, utf8.Span, _length);
        _length += utf8.Length;
        return default;
    }

    public override ValueTask CloseAsync(bool useAsync)
    {
        _file.Dispose();
        return default;
    }
}

internal sealed class StreamSink(Stream stream, bool leaveOpen) : ByteSink
{
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> utf8, bool useAsync, CancellationToken ct)
    {
        if (useAsync) await stream.WriteAsync(utf8, ct).ConfigureAwait(false);
        else stream.Write(utf8.Span);
    }

    public override async ValueTask CompleteAsync(bool useAsync, CancellationToken ct)
    {
        if (useAsync) await stream.FlushAsync(ct).ConfigureAwait(false);
        else stream.Flush();
    }

    public override async ValueTask CloseAsync(bool useAsync)
    {
        if (leaveOpen) return;
        if (useAsync) await stream.DisposeAsync().ConfigureAwait(false);
        else stream.Dispose();
    }
}

internal sealed class BufferSink(IBufferWriter<byte> output) : ByteSink
{
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> utf8, bool useAsync, CancellationToken ct)
    {
        ReadOnlySpan<byte> rest = utf8.Span;
        while (!rest.IsEmpty)
        {
            Span<byte> span = output.GetSpan(Math.Min(rest.Length, 1 << 20));
            int n = Math.Min(span.Length, rest.Length);
            rest[..n].CopyTo(span);
            output.Advance(n);
            rest = rest[n..];
        }
        return default;
    }
}

/// <summary>
/// A pipe: flushed after every piece, so the writer waits while the reader is behind (the pipe stays the caller's: it
/// is not completed). A reader that stops is an <see cref="IOException"/>.
/// </summary>
internal sealed class PipeSink(PipeWriter pipe) : ByteSink
{
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> utf8, bool useAsync, CancellationToken ct)
    {
        ReadOnlySpan<byte> rest = utf8.Span;
        while (!rest.IsEmpty)
        {
            Span<byte> span = pipe.GetSpan(Math.Min(rest.Length, 1 << 16));
            int n = Math.Min(span.Length, rest.Length);
            rest[..n].CopyTo(span);
            pipe.Advance(n);
            rest = rest[n..];
        }
        await PipeFlush.FlushAsync(pipe, useAsync, ct).ConfigureAwait(false);
    }

    public override ValueTask CompleteAsync(bool useAsync, CancellationToken ct) => PipeFlush.FlushAsync(pipe, useAsync, ct);
}

/// <summary>Text: UTF-8 rows decoded to UTF-16 a piece at a time.</summary>
internal sealed class TextSink(TextWriter writer, bool leaveOpen) : ByteSink
{
    public override bool IsText => true;

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> utf8, bool useAsync, CancellationToken ct)
    {
        char[] chars = ArrayPool<char>.Shared.Rent(Math.Max(1, utf8.Length));
        try
        {
            Utf8.ToUtf16(utf8.Span, chars, out _, out int written);
            if (useAsync) await writer.WriteAsync(chars.AsMemory(0, written), ct).ConfigureAwait(false);
            else writer.Write(chars, 0, written);
        }
        finally { ArrayPool<char>.Shared.Return(chars); }
    }

    public override async ValueTask CompleteAsync(bool useAsync, CancellationToken ct)
    {
        if (useAsync) await writer.FlushAsync(ct).ConfigureAwait(false);
        else writer.Flush();
    }

    public override async ValueTask CloseAsync(bool useAsync)
    {
        if (leaveOpen) return;
        if (useAsync) await writer.DisposeAsync().ConfigureAwait(false);
        else writer.Dispose();
    }
}

/// <summary>A string: the UTF-8 kept in native memory, decoded once complete.</summary>
internal sealed class StringSink : ByteSink
{
    private readonly NativeBufferWriter _bytes = new(1 << 16);

    public override bool IsText => true;

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> utf8, bool useAsync, CancellationToken ct)
    {
        utf8.Span.CopyTo(_bytes.GetSpan(utf8.Length));
        _bytes.Advance(utf8.Length);
        return default;
    }

    /// <summary>The text written (once complete); the bytes are freed.</summary>
    public string TakeText()
    {
        try { return System.Text.Encoding.UTF8.GetString(_bytes.WrittenSpan); }
        finally { _bytes.Dispose(); }
    }
}

/// <summary>
/// Bytes encoded (or compressed) into native memory, or the failure encoding them met; once written, the buffer is
/// handed back (to be used again) or freed.
/// </summary>
internal sealed class EncodedPiece(NativeBufferWriter? bytes, Exception? error, Action<NativeBufferWriter>? giveBack = null) : IDisposable
{
    public ReadOnlyMemory<byte> Bytes => bytes?.WrittenMemory ?? default;

    public Exception? Error => error;

    public void Dispose()
    {
        if (bytes is null) return;
        if (giveBack is null) bytes.Dispose();
        else giveBack(bytes);
    }

    /// <summary>
    /// <paramref name="raw"/> as one gzip member or Zstandard frame of its own (the raw bytes disposed), or as it is.
    /// </summary>
    public static NativeBufferWriter Compress(NativeBufferWriter raw, PieceCompression compression, int? level)
    {
        if (compression == PieceCompression.None) return raw;
        try
        {
            var packed = new NativeBufferWriter(Math.Max(64, raw.WrittenCount / 4));
            try
            {
                if (compression == PieceCompression.Zstd)
                {
                    using var compressor = new ZstdSharp.Compressor(level ?? 3);
                    int bound = ZstdSharp.Compressor.GetCompressBound(raw.WrittenCount);
                    int written = compressor.Wrap(raw.WrittenSpan, packed.GetSpan(bound));
                    packed.Advance(written);
                }
                else
                {
                    using var buffer = new NativeByteBuffer(Math.Max(64, raw.WrittenCount / 4), pooled: true);
                    var stream = new NativeByteBufferStream(buffer);
                    using (var gzip = new GZipStream(stream, GzipLevel(level), leaveOpen: true)) gzip.Write(raw.WrittenSpan);
                    ReadOnlySpan<byte> member = buffer.Span[..(int)stream.Length];
                    member.CopyTo(packed.GetSpan(member.Length));
                    packed.Advance(member.Length);
                }
                return packed;
            }
            catch
            {
                packed.Dispose();
                throw;
            }
        }
        finally { raw.Dispose(); }
    }

    // gzip's levels 0–9 as .NET's (whose library version has no numeric levels): store, fastest, default, smallest.
    private static CompressionLevel GzipLevel(int? level) => level switch
    {
        null => CompressionLevel.Optimal,
        0 => CompressionLevel.NoCompression,
        <= 3 => CompressionLevel.Fastest,
        <= 6 => CompressionLevel.Optimal,
        _ => CompressionLevel.SmallestSize,
    };
}

/// <summary>The compression of the pieces a writer writes: each a gzip member or Zstandard frame of its own.</summary>
internal enum PieceCompression
{
    None,
    Gzip,
    Zstd,
}
