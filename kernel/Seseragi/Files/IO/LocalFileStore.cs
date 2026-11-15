using System.Runtime.CompilerServices;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// The local file system as an <see cref="IFileStore"/>. Relative paths are taken from <paramref name="root"/> (the
/// current directory when null) and listed with the spelling given: listing <c>data</c> gives <c>data/a.parquet</c>.
/// Hidden files are listed like any other. Files are opened for reading at offsets (and may be read through a memory
/// mapping, as files opened by path).
/// </summary>
/// <param name="root">The directory relative paths start from; null for the current directory.</param>
public sealed class LocalFileStore(string? root = null) : IFileStore, IDirectoryAware
{
    private static readonly EnumerationOptions TopLevel = Options(recursive: false);
    private static readonly EnumerationOptions Everything = Options(recursive: true);

    private static EnumerationOptions Options(bool recursive) => new()
    {
        RecurseSubdirectories = recursive,
        AttributesToSkip = 0, // hidden files (a leading '.' on Unix) and system files are read like any other
        IgnoreInaccessible = false,
        MatchType = MatchType.Simple,
    };

    /// <summary>The directory relative paths start from; null for the current directory.</summary>
    public string? Root => root;

    /// <inheritdoc/>
    public bool CanList => true;

    private string Resolve(string path) => root is null ? path : Path.Combine(root, path); // an absolute path stays as it is

    /// <inheritdoc/>
    public bool IsDirectory(string path) => Directory.Exists(Resolve(path.Length == 0 ? "." : path));

    /// <inheritdoc/>
    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        var directory = new DirectoryInfo(Resolve(prefix.Length == 0 ? "." : prefix));
        if (!directory.Exists) yield break;
        string full = directory.FullName;
        string lead = prefix.Length == 0 || Path.EndsInDirectorySeparator(prefix) ? prefix : prefix + Path.DirectorySeparatorChar;
        foreach (FileInfo file in directory.EnumerateFiles("*", recursive ? Everything : TopLevel))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new StoredFile(lead + Path.GetRelativePath(full, file.FullName), file.Length, file.LastWriteTimeUtc);
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        var file = new FileInfo(Resolve(path));
        return ValueTask.FromResult<StoredFile?>(file.Exists ? new StoredFile(path, file.Length, file.LastWriteTimeUtc) : null);
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default)
    {
        try
        {
            return ValueTask.FromResult<IRandomAccess>(Open(file.Path));
        }
        catch (DirectoryNotFoundException e)
        {
            throw new FileNotFoundException($"'{file.Path}' does not exist.", file.Path, e);
        }
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        try
        {
            return ValueTask.FromResult<IRandomAccess?>(Open(path));
        }
        catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException)
        {
            return ValueTask.FromResult<IRandomAccess?>(null);
        }
    }

    private RandomAccessSource Open(string path)
    {
        var stream = new FileStream(Resolve(path), FileMode.Open, FileAccess.Read, FileShare.Read, 1, FileOptions.RandomAccess);
        return new RandomAccessSource(stream, leaveOpen: false, copy: null, mappable: true);
    }
}
