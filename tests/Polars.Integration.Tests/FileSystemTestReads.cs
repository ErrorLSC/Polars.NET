using System.Buffers;
using Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>
/// The byte-array reads of <see cref="IFileSystem"/> as they were, for test assertions only: the kernel reads into
/// native memory (<see cref="IFileSystem.TryReadAll"/>, <see cref="IFileSystem.ReadRange"/>).
/// </summary>
internal static class FileSystemTestReads
{
    /// <summary>A whole file, or null when it does not exist.</summary>
    public static byte[]? ReadAll(this IFileSystem fileSystem, string path)
    {
        var bytes = new ArrayBufferWriter<byte>();
        return fileSystem.TryReadAll(path, bytes) ? bytes.WrittenSpan.ToArray() : null;
    }

    /// <summary>Up to <paramref name="length"/> bytes from <paramref name="offset"/>, or null when the file does not exist.</summary>
    public static byte[]? ReadRange(this IFileSystem fileSystem, string path, long offset, int length)
    {
        var bytes = new byte[length];
        int read = fileSystem.ReadRange(path, offset, bytes);
        return read < 0 ? null : bytes[..read];
    }

    /// <summary>A whole object of a store client, or null when there is none.</summary>
    public static byte[]? Get(this Kaminari.Seseragi.Lakehouse.Storage.ObjectStoreClient client, string key)
    {
        var bytes = new ArrayBufferWriter<byte>();
        return client.Get(key, bytes) ? bytes.WrittenSpan.ToArray() : null;
    }
}
