using Kaminari.Sazanami.Helpers;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>Copies a stream that cannot seek into native memory, for formats read at offsets.</summary>
internal static class NativeCopy
{
    /// <summary>
    /// Copies the rest of <paramref name="stream"/> (disposed afterwards unless <paramref name="leaveOpen"/>) and
    /// returns a seekable stream over the copy, with the copy to release once done. At most 2 GB fit.
    /// </summary>
    public static async ValueTask<(UnmanagedMemoryStream Stream, NativeByteBuffer Copy)> CopyAsync(
        Stream stream, bool leaveOpen, bool useAsync, CancellationToken ct)
    {
        var copy = new NativeByteBuffer(1 << 20);
        try
        {
            var sink = new NativeByteBufferStream(copy);
            if (useAsync) await stream.CopyToAsync(sink, ct).ConfigureAwait(false);
            else stream.CopyTo(sink);
            if (!leaveOpen)
            {
                if (useAsync) await stream.DisposeAsync().ConfigureAwait(false);
                else stream.Dispose();
            }
            return (FromCopy(copy, sink.Length), copy);
        }
        catch
        {
            copy.Release();
            throw;
        }

        static unsafe UnmanagedMemoryStream FromCopy(NativeByteBuffer copy, long length) => new((byte*)copy.Pin().Pointer, length);
    }
}
