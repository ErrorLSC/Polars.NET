namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>The end of a file and the file's size, as one read gives them.</summary>
/// <param name="Bytes">The last bytes of the file (all of it when it is shorter than asked).</param>
/// <param name="Size">The file's size.</param>
public readonly record struct FileTail(byte[] Bytes, long Size);

/// <summary>
/// A file system that reads the end of a file without knowing its size: Parquet footers of freshly written files
/// are read so, without listing their directories for sizes.
/// </summary>
public interface ITailReadable
{
    /// <summary>
    /// Whether <see cref="ReadTail"/> can be used (a wrapper or a dispatching file system may say no; callers then
    /// list for sizes and read ranges).
    /// </summary>
    bool CanReadTails { get; }

    /// <summary>
    /// The last <paramref name="length"/> bytes of a file (all of them when it is shorter) with its size, or null
    /// when it does not exist. Safe to call from several threads at once.
    /// </summary>
    FileTail? ReadTail(string path, int length);
}
