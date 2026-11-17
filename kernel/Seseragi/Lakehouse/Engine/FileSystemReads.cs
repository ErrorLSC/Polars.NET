using System.Buffers;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>A file read whole into native memory (<see cref="FileSystemReads.ReadFile"/>): valid until disposed.</summary>
public sealed class FileBytes : IDisposable
{
    private NativeBufferWriter? _bytes = new();

    internal IBufferWriter<byte> Writer => _bytes ?? throw new ObjectDisposedException(nameof(FileBytes));

    /// <summary>The file's bytes.</summary>
    public ReadOnlySpan<byte> Span => (_bytes ?? throw new ObjectDisposedException(nameof(FileBytes))).WrittenSpan;

    /// <summary>The file's bytes, as memory (valid until disposed).</summary>
    public ReadOnlyMemory<byte> Memory => (_bytes ?? throw new ObjectDisposedException(nameof(FileBytes))).WrittenMemory;

    /// <summary>The file's size.</summary>
    public int Length => _bytes?.WrittenCount ?? 0;

    /// <summary>Returns the memory to the pool.</summary>
    public void Dispose() => Interlocked.Exchange(ref _bytes, null)?.Dispose();
}

/// <summary>Reads on <see cref="IFileSystem"/> beyond its two primitives.</summary>
public static class FileSystemReads
{
    /// <summary>A whole file in native memory (dispose it), or null when it does not exist.</summary>
    public static FileBytes? ReadFile(this IFileSystem fileSystem, string path)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        var bytes = new FileBytes();
        try
        {
            if (fileSystem.TryReadAll(path, bytes.Writer)) return bytes;
        }
        catch
        {
            bytes.Dispose();
            throw;
        }
        bytes.Dispose();
        return null;
    }

    /// <summary>Whether a file exists (one byte read).</summary>
    public static bool Exists(this IFileSystem fileSystem, string path)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        Span<byte> one = stackalloc byte[1];
        return fileSystem.ReadRange(path, 0, one) >= 0;
    }
}
