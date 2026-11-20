namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// A store files can be written to, as well as listed and read (an <see cref="IFileStore"/>): what dataset writers write
/// through. A file appears whole or not at all: its bytes go to a <see cref="FileWriteStream"/>, published by
/// <see cref="FileWriteStream.Commit"/>, given up when the stream is disposed uncommitted. Paths are the store's own, as
/// for reading.
/// </summary>
public interface IWritableFileStore : IFileStore
{
    /// <summary>
    /// Opens a file for writing (directories, where the store has any, are created as needed). With
    /// <paramref name="createOnly"/> the commit creates the file only when none exists, atomically against other writers
    /// (it returns false otherwise, the existing file untouched); without, the commit replaces any file at the path.
    /// </summary>
    FileWriteStream OpenWrite(string path, bool createOnly);

    /// <summary>Deletes a file; false when there was none.</summary>
    ValueTask<bool> DeleteAsync(string path, CancellationToken cancellationToken = default);
}
