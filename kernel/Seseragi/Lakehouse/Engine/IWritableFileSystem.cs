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

    /// <summary>Deletes a file; returns false when there was none.</summary>
    bool Delete(string path);
}
