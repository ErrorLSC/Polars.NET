using System.Buffers;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>A file as a listing reports it.</summary>
/// <param name="Path">The path relative to the table root, with <c>/</c> separators.</param>
/// <param name="Size">The size in bytes.</param>
/// <param name="LastModified">The modification time.</param>
public readonly record struct FileEntry(string Path, long Size, DateTimeOffset LastModified);

/// <summary>
/// The storage a table lives on, rooted at the table directory. Paths are relative to that root and use
/// <c>/</c> separators. This is what an engine implements to let the kernel read a table.
/// </summary>
/// <remarks>
/// A path given to <see cref="TryReadAll"/> or <see cref="ReadRange"/> may also be an absolute URI (such as
/// <c>s3://bucket/dir/file</c> or <c>file:///dir/file</c>), because the log may point outside the table: data files
/// of a shallow clone, deletion vectors of storage type <c>p</c>, sidecars of a V2 checkpoint. A file system reads
/// such a file when it can reach it and throws otherwise.
/// </remarks>
public interface IFileSystem
{
    /// <summary>
    /// Lists the files directly inside <paramref name="directory"/> (not its subdirectories), in ordinal order
    /// of their names. With <paramref name="startFrom"/> only files whose name is not less than it are returned.
    /// A directory that does not exist lists as empty.
    /// </summary>
    IReadOnlyList<FileEntry> List(string directory, string? startFrom = null);

    /// <summary>
    /// Appends a whole file to <paramref name="into"/> (the caller's buffer: native memory, as a rule); false, nothing
    /// written, when it does not exist. <see cref="FileSystemReads.ReadFile"/> reads into a buffer of its own.
    /// </summary>
    bool TryReadAll(string path, IBufferWriter<byte> into);

    /// <summary>
    /// Reads the bytes of a file from <paramref name="offset"/> into <paramref name="destination"/>: all it holds, fewer
    /// only when the file ends first. Returns the bytes read, or -1 when the file does not exist.
    /// </summary>
    int ReadRange(string path, long offset, Span<byte> destination);
}
