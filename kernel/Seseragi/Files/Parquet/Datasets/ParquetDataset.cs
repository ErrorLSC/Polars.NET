using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Parquet files read as one table: files named, directories (every file below them), globs (<c>*</c>, <c>?</c>,
/// <c>[...]</c>, <c>**</c> as a whole segment), or a list of files known beforehand, from the local file system or a
/// store (<see cref="ParquetDatasetOptions.Store"/>). The schema is the first file's; the other files must hold its
/// columns (with the same types; columns they have beyond it are not read).
/// </summary>
/// <remarks>
/// Paths are expanded as dataframe engines expand them: each glob's or directory's files in path order, inputs in the order
/// given; directories and empty files are left out of listings; files of a directory given alone must share their
/// extension; hidden files and files starting with '_' are read unless <see cref="ParquetDatasetOptions.IgnorePrefixes"/>
/// says otherwise. A dataset's columns are nullable: its files may declare them differently.
/// </remarks>
public sealed class ParquetDataset : IDisposable, IAsyncDisposable
{
    private static readonly LocalFileStore LocalFiles = new();

    private readonly ParquetMetadata?[] _footers;

    private ParquetDataset(IFileStore store, ParquetDatasetOptions options, ParquetDatasetFile[] files, ParquetMetadata first, Apache.Arrow.Schema fileSchema,
        Partitions? partitions)
    {
        Store = store;
        Options = options;
        Files = files;
        _footers = new ParquetMetadata?[files.Length];
        _footers[0] = first;
        FileSchema = fileSchema;
        Partitions = partitions;
        // The files' columns (a partition column of the same name in its place: the path's value wins), then the
        // other partition columns, then the file path.
        List<Field> fields = [.. fileSchema.FieldsList.Select(f => partitions?.IndexOf(f.Name) is int p and >= 0
            ? PartitionField(p, views: true)
            : f.IsNullable ? f : new Field(f.Name, f.DataType, nullable: true, f.Metadata))];
        for (int p = 0; p < (partitions?.Fields.Length ?? 0); p++)
        {
            if (fileSchema.GetFieldIndex(partitions!.Fields[p].Name) < 0) fields.Add(PartitionField(p, views: true));
        }
        if (options.FilePathColumn is { } path)
        {
            if (fields.Any(f => f.Name == path)) throw new ArgumentException($"The file path column '{path}' has the name of a column of the files.", nameof(options));
            fields.Add(new Field(path, StringViewType.Default, nullable: false));
        }
        Schema = new Apache.Arrow.Schema(fields, fileSchema.Metadata);
        PartitionSchema = partitions is null ? null : new Apache.Arrow.Schema([.. Enumerable.Range(0, partitions.Fields.Length).Select(p => PartitionField(p, views: true))], null);
    }

    /// <summary>The partition values of the files (one row per file), when the dataset is partitioned.</summary>
    internal Partitions? Partitions { get; }

    /// <summary>
    /// The partition columns, in the order of the keys, as read with default options: the type given in
    /// <see cref="ParquetDatasetOptions.PartitionSchema"/>, else the type inferred (strings in the view layout unless
    /// <see cref="ParquetReadOptions.ViewTypes"/> is off); null when the dataset is not partitioned.
    /// </summary>
    public Apache.Arrow.Schema? PartitionSchema { get; }

    /// <summary>Partition column <paramref name="index"/> as batches hold it: inferred strings follow the reader's layout.</summary>
    internal Field PartitionField(int index, bool views)
    {
        Field field = Partitions!.Fields[index];
        // As given, unless the given type was a temporal one read as strings.
        bool given = Options.PartitionSchema?.GetFieldByName(field.Name) is { } declared && declared.DataType.TypeId == field.DataType.TypeId;
        return field.DataType is StringType && !given && views ? new Field(field.Name, StringViewType.Default, nullable: true) : field;
    }

    /// <summary>The store the files are read from.</summary>
    internal IFileStore Store { get; }

    internal ParquetDatasetOptions Options { get; }

    /// <summary>The first file's schema, read with default options.</summary>
    internal Apache.Arrow.Schema FileSchema { get; }

    /// <summary>
    /// The schema read with default options: the first file's columns (nullable), then the file path column if asked
    /// (in the view layout; <see cref="ParquetReadOptions.ViewTypes"/> off makes it a plain string).
    /// </summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>The files, in the order read.</summary>
    public IReadOnlyList<ParquetDatasetFile> Files { get; }

    /// <summary>The footer of file <paramref name="index"/>, when read before (by this dataset or given).</summary>
    internal ParquetMetadata? Footer(int index) => Files[index].Metadata ?? Volatile.Read(ref _footers[index]);

    internal void KeepFooter(int index, ParquetMetadata metadata) => Volatile.Write(ref _footers[index], metadata);

    /// <summary>Opens the dataset at a path, a directory or a glob.</summary>
    /// <exception cref="FileNotFoundException">No file is found.</exception>
    /// <exception cref="ParquetFormatException">The first file is not a Parquet file.</exception>
    public static ParquetDataset Open(string pathOrGlob, ParquetDatasetOptions? options = null) =>
        Sync(OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: false, default));

    /// <summary>Opens the dataset of several paths, directories or globs, read in the order given.</summary>
    /// <inheritdoc cref="Open(string, ParquetDatasetOptions?)"/>
    public static ParquetDataset Open(IEnumerable<string> pathsOrGlobs, ParquetDatasetOptions? options = null) =>
        Sync(OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: false, default));

    /// <summary>Opens the dataset of files known beforehand (no listing): read in the order given.</summary>
    /// <inheritdoc cref="Open(string, ParquetDatasetOptions?)"/>
    public static ParquetDataset Open(IEnumerable<ParquetDatasetFile> files, ParquetDatasetOptions? options = null) =>
        Sync(OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: false, default));

    /// <inheritdoc cref="Open(string, ParquetDatasetOptions?)"/>
    public static ValueTask<ParquetDataset> OpenAsync(string pathOrGlob, ParquetDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{string}, ParquetDatasetOptions?)"/>
    public static ValueTask<ParquetDataset> OpenAsync(IEnumerable<string> pathsOrGlobs, ParquetDatasetOptions? options = null,
        CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{ParquetDatasetFile}, ParquetDatasetOptions?)"/>
    public static ValueTask<ParquetDataset> OpenAsync(IEnumerable<ParquetDatasetFile> files, ParquetDatasetOptions? options = null,
        CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: true, cancellationToken);

    /// <summary>Opens a reader of the dataset's batches (see <see cref="ParquetDatasetReader"/> for how the options apply).</summary>
    /// <exception cref="ArgumentException">The options select rows or row groups by number (per file: <see cref="ParquetDatasetFile.Rows"/>), name columns the first file lacks, or are out of range.</exception>
    public ParquetDatasetReader OpenReader(ParquetReadOptions? options = null) => new(this, options ?? new ParquetReadOptions());

    /// <summary>Nothing is held open between reads; kept for stores that will hold connections.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    internal static T Sync<T>(ValueTask<T> task) => task.IsCompleted ? task.Result : task.AsTask().GetAwaiter().GetResult();

    private static async ValueTask<ParquetDataset> OpenCoreAsync(string[] inputs, ParquetDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new ParquetDatasetOptions();
        options.Validate();
        if (inputs.Length == 0) throw new ArgumentException("No path is given.", nameof(inputs));
        IFileStore store = options.Store ?? LocalFiles;
        foreach (string input in inputs)
        {
            if (string.IsNullOrEmpty(input)) throw new ArgumentException("A path is empty.", nameof(inputs));
            if (options.Store is null && input.Contains("://", StringComparison.Ordinal))
                throw new ArgumentException($"'{input}' is a URL: give the store that reads it (ParquetDatasetOptions.Store).", nameof(inputs));
        }
        List<ParquetDatasetFile> files = [];
        int hiveStart = int.MaxValue;
        bool directory = false;
        foreach (string input in inputs)
        {
            (bool isDirectory, int start) = await ExpandAsync(store, input, options, files, alone: inputs.Length == 1, ct).ConfigureAwait(false);
            hiveStart = Math.Min(hiveStart, start);
            directory |= isDirectory;
        }
        if (files.Count == 0) throw new FileNotFoundException($"No file is found at {string.Join(", ", inputs.Select(i => $"'{i}'"))}.");
        // Partitions from the paths: when asked, or for a directory given alone (the default).
        bool hive = options.Partitioning == HivePartitioning.On || options.Partitioning == HivePartitioning.Auto && inputs.Length == 1 && directory;
        return await FromFilesAsync(store, options, [.. files], hive ? hiveStart : null, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<ParquetDataset> OpenCoreAsync(ParquetDatasetFile[] files, ParquetDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new ParquetDatasetOptions();
        options.Validate();
        if (files.Length == 0) throw new ArgumentException("No file is given.", nameof(files));
        foreach (ParquetDatasetFile file in files)
        {
            ArgumentNullException.ThrowIfNull(file, nameof(files));
            if (string.IsNullOrEmpty(file.Path)) throw new ArgumentException("A file has no path.", nameof(files));
            if (file.Size < 0) throw new ArgumentException($"'{file.Path}' has a negative size.", nameof(files));
        }
        return await FromFilesAsync(options.Store ?? LocalFiles, options, files, options.Partitioning == HivePartitioning.On ? 0 : null, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The dataset of the files: the first one's footer is read for the schema; partitions from the values given with
    /// the files, else from the paths after <paramref name="hiveStart"/> when it is set.
    /// </summary>
    private static async ValueTask<ParquetDataset> FromFilesAsync(IFileStore store, ParquetDatasetOptions options, ParquetDatasetFile[] files, int? hiveStart,
        bool useAsync, CancellationToken ct)
    {
        await using ParquetFile first = await OpenFileAsync(store, files[0], files[0].Metadata, useAsync, ct).ConfigureAwait(false);
        Apache.Arrow.Schema schema;
        try
        {
            schema = first.Schema;
        }
        catch (ParquetFormatException e)
        {
            throw InFile(files[0].Path, e);
        }
        Partitions? partitions;
        if (files.Any(f => f.PartitionValues is not null))
        {
            Apache.Arrow.Schema given = options.PartitionSchema
                ?? throw new ArgumentException("Files are given with partition values: give their types too (ParquetDatasetOptions.PartitionSchema).", nameof(files));
            partitions = Partitions.FromValues(files, given);
        }
        else partitions = hiveStart is int start && options.Partitioning != HivePartitioning.Off
            ? Partitions.FromHive([.. files.Select(f => f.Path)], start, options.PartitionSchema, schema, options.ParseHiveDates)
            : null;
        return new ParquetDataset(store, options, files, first.Metadata, schema, partitions);
    }

    /// <summary>Opens a file of the store (owned by the <see cref="ParquetFile"/>); errors name the file.</summary>
    internal static async ValueTask<ParquetFile> OpenFileAsync(IFileStore store, ParquetDatasetFile file, ParquetMetadata? footer, bool useAsync, CancellationToken ct)
    {
        IRandomAccess source = file.Size is long size
            ? await store.OpenAsync(new StoredFile(file.Path, size), ct).ConfigureAwait(false)
            : await store.OpenAsync(file.Path, ct).ConfigureAwait(false) ?? throw new FileNotFoundException($"'{file.Path}' does not exist.", file.Path);
        try
        {
            return await ParquetFile.OpenOwnedAsync(source, footer, useAsync, ct).ConfigureAwait(false);
        }
        catch (ParquetFormatException e)
        {
            throw InFile(file.Path, e);
        }
    }

    /// <summary>The error with the file it is about.</summary>
    internal static ParquetFormatException InFile(string path, ParquetFormatException e) => new($"{path}: {e.Message}", e);

    /// <summary>
    /// Adds the files <paramref name="input"/> names: itself, the files below a directory, or a glob's matches. Tells
    /// whether it was a directory, and where Hive keys start in the paths (after a directory; everywhere for others).
    /// </summary>
    private static async ValueTask<(bool Directory, int HiveStart)> ExpandAsync(IFileStore store, string input, ParquetDatasetOptions options, List<ParquetDatasetFile> files, bool alone,
        CancellationToken ct)
    {
        bool local = store is LocalFileStore;
        // Local paths may use the platform's separator; patterns and listings are matched with '/'.
        string path = local && Path.DirectorySeparatorChar != '/' ? input.Replace(Path.DirectorySeparatorChar, '/') : input;
        int start = files.Count;
        if (options.Glob && GlobPattern.IsGlob(path) && !IsHttp(path))
        {
            GlobPattern glob = GlobPattern.Parse(path);
            if (!store.CanList) throw new NotSupportedException($"'{input}' is a glob, but the store cannot list its files.");
            await foreach (StoredFile file in store.ListAsync(glob.Prefix, glob.Recursive, ct).ConfigureAwait(false))
            {
                string listed = local && Path.DirectorySeparatorChar != '/' ? file.Path.Replace(Path.DirectorySeparatorChar, '/') : file.Path;
                if (file.Size > 0 && listed.StartsWith(glob.Prefix, StringComparison.Ordinal) && glob.IsMatch(listed.AsSpan(glob.Prefix.Length)) && Kept(file.Path, options))
                    files.Add(new ParquetDatasetFile(file.Path, file.Size));
            }
        }
        else if (path.EndsWith('/') || store is IDirectoryAware directories && directories.IsDirectory(input))
        {
            if (!store.CanList) throw new NotSupportedException($"'{input}' is a directory, but the store cannot list its files.");
            await foreach (StoredFile file in store.ListAsync(input, recursive: true, ct).ConfigureAwait(false))
            {
                if (file.Size > 0 && Kept(file.Path, options)) files.Add(new ParquetDatasetFile(file.Path, file.Size));
            }
            if (alone) SameExtension(input, files);
            files.Sort(start, files.Count - start, ByPath.Instance);
            return (true, input.Length);
        }
        else
        {
            if (Kept(input, options)) files.Add(new ParquetDatasetFile(input));
            return (false, 0);
        }
        files.Sort(start, files.Count - start, ByPath.Instance);
        return (false, 0);
    }

    /// <summary>HTTP(S) URLs are never globs: their '?' starts a query (as in signed URLs).</summary>
    private static bool IsHttp(string path) =>
        path.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || path.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether a file's name starts with none of the prefixes to leave out.</summary>
    private static bool Kept(string path, ParquetDatasetOptions options)
    {
        if (options.IgnorePrefixes is not { Count: > 0 } prefixes) return true;
        string name = path[(path.AsSpan().LastIndexOfAny('/', Path.DirectorySeparatorChar) + 1)..];
        return !prefixes.Any(p => name.StartsWith(p, StringComparison.Ordinal));
    }

    /// <summary>The files of a directory given alone must share an extension: a glob says which to read otherwise.</summary>
    private static void SameExtension(string directory, List<ParquetDatasetFile> files)
    {
        string? first = null;
        foreach (ParquetDatasetFile file in files)
        {
            string extension = Path.GetExtension(file.Path);
            if (first is null) first = extension;
            else if (extension != first)
                throw new InvalidOperationException($"The directory '{directory}' holds files of different extensions ('{first}', '{extension}' in '{file.Path}'): "
                    + "give a glob that says which to read (such as 'dir/**/*.parquet').");
        }
    }

    private sealed class ByPath : IComparer<ParquetDatasetFile>
    {
        public static readonly ByPath Instance = new();

        public int Compare(ParquetDatasetFile? x, ParquetDatasetFile? y) => string.CompareOrdinal(x!.Path, y!.Path);
    }
}
