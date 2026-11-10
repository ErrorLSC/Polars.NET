namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>A file as a listing reports it.</summary>
/// <param name="Path">The path relative to the table root, with <c>/</c> separators.</param>
/// <param name="Size">The size in bytes.</param>
/// <param name="LastModified">The modification time.</param>
public readonly record struct FileEntry(string Path, long Size, DateTimeOffset LastModified);

/// <summary>
/// The storage a table lives on, rooted at the table directory. All paths are relative to that root and use
/// <c>/</c> separators. This is what an engine implements to let the kernel read a table.
/// </summary>
public interface IFileSystem
{
    /// <summary>
    /// Lists the files directly inside <paramref name="directory"/> (not its subdirectories), in ordinal order
    /// of their names. With <paramref name="startFrom"/> only files whose name is not less than it are returned.
    /// A directory that does not exist lists as empty.
    /// </summary>
    IReadOnlyList<FileEntry> List(string directory, string? startFrom = null);

    /// <summary>Reads a whole file, or returns null when it does not exist.</summary>
    byte[]? ReadAll(string path);

    /// <summary>
    /// Reads <paramref name="length"/> bytes of a file starting at <paramref name="offset"/>, or returns null when
    /// the file does not exist. Fewer bytes are returned only when the file ends first.
    /// </summary>
    byte[]? ReadRange(string path, long offset, int length);
}
