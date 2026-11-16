namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>
/// Storage a table can be written to. Writes go to table-relative paths. A reader sees a file either not at all
/// or with its whole content, never a part of it.
/// </summary>
public interface IWritableFileSystem : IFileSystem
{
    /// <summary>
    /// Creates a file only when none exists at the path, atomically against other writers (put-if-absent): of two
    /// writers creating the same path, exactly one succeeds. Returns false when the file already exists. Commits to
    /// a table log rely on this.
    /// </summary>
    bool TryCreate(string path, ReadOnlyMemory<byte> content);

    /// <summary>Writes a file, replacing the one at the path if there is one.</summary>
    void Overwrite(string path, ReadOnlyMemory<byte> content);

    /// <summary>
    /// Opens a file for writing as a stream, for content too large to hold at once: the file appears, whole, when the
    /// stream is committed (<see cref="FileWriteStream.Commit"/>), and not at all when it is disposed uncommitted. With
    /// <paramref name="createOnly"/>, the commit creates the file only when none exists, atomically against other
    /// writers (as <see cref="TryCreate"/>); otherwise it replaces any file at the path. By default the bytes are held
    /// in memory and written with one call at commit; stores that can write in pieces override it.
    /// </summary>
    FileWriteStream OpenWrite(string path, bool createOnly) => FileWriteStream.Buffered(this, path, createOnly);

    /// <summary>Deletes a file; returns false when there was none.</summary>
    bool Delete(string path);

    /// <summary>
    /// Lists every file under <paramref name="directory"/> at any depth, paths relative to the table root, in
    /// ordinal order. A directory that does not exist lists as empty. Vacuum uses it to find files the log no
    /// longer references.
    /// </summary>
    IReadOnlyList<FileEntry> ListAll(string directory);

    /// <summary>
    /// Every directory under <paramref name="directory"/> at any depth, relative to the table root. Stores without
    /// real directories (object stores) have none.
    /// </summary>
    IReadOnlyList<string> ListDirectories(string directory) => [];

    /// <summary>Deletes a directory when it is empty; returns whether it did. Stores without real directories never do.</summary>
    bool DeleteDirectoryIfEmpty(string directory) => false;

    /// <summary>
    /// Creates a directory (with its parents) where the store has real directories; stores without them (object
    /// stores) have none to create.
    /// </summary>
    void CreateDirectory(string directory) { }
}
