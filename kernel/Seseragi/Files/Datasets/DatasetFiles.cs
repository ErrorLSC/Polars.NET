using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// What datasets of every format share before their files are read: the store of the inputs, the files the inputs
/// name (paths, directories, globs), the partitions (from paths or given), the columns of every file united by name.
/// </summary>
/// <remarks>
/// Paths are expanded as dataframe engines expand them: each glob's or directory's files in path order, inputs in the
/// order given; directories and empty files are left out of listings; files of a directory given alone must share their
/// extension; hidden files and files starting with '_' are read unless <see cref="DatasetOptions.IgnorePrefixes"/> says
/// otherwise.
/// </remarks>
internal static class DatasetFiles
{
    private static readonly LocalFileStore LocalFiles = new();

    private static readonly Lazy<HttpFileStore> DefaultHttp = new(() => new HttpFileStore());

    private static readonly Lazy<HuggingFaceFileStore> DefaultHuggingFace = new(() => new HuggingFaceFileStore());

    internal static T Sync<T>(ValueTask<T> task) => task.IsCompleted ? task.Result : task.AsTask().GetAwaiter().GetResult();

    /// <summary>The files the inputs name, in order; where Hive keys start in their paths (null: no partitions from paths).</summary>
    /// <exception cref="FileNotFoundException">No file is found.</exception>
    public static async ValueTask<(IFileStore Store, List<(string Path, long? Size)> Files, int? HiveStart)> ExpandAsync(string[] inputs, DatasetOptions options,
        CancellationToken ct)
    {
        if (inputs.Length == 0) throw new ArgumentException("No path is given.", nameof(inputs));
        foreach (string input in inputs)
        {
            if (string.IsNullOrEmpty(input)) throw new ArgumentException("A path is empty.", nameof(inputs));
        }
        IFileStore store = options.Store ?? StoreOf(inputs, options);
        List<(string Path, long? Size)> files = [];
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
        return (store, files, hive ? hiveStart : null);
    }

    /// <summary>Checks files given (no listing); their store, and where Hive keys start (all of the path, when asked).</summary>
    public static (IFileStore Store, int? HiveStart) Given(IReadOnlyList<DatasetFile> files, DatasetOptions options)
    {
        if (files.Count == 0) throw new ArgumentException("No file is given.", nameof(files));
        foreach (DatasetFile file in files)
        {
            ArgumentNullException.ThrowIfNull(file, nameof(files));
            if (string.IsNullOrEmpty(file.Path)) throw new ArgumentException("A file has no path.", nameof(files));
            if (file.Size < 0) throw new ArgumentException($"'{file.Path}' has a negative size.", nameof(files));
        }
        return (options.Store ?? StoreOf([.. files.Select(f => f.Path)], options), options.Partitioning == HivePartitioning.On ? 0 : null);
    }

    /// <summary>
    /// The partitions of the files: from the values given with them, else from the paths after
    /// <paramref name="hiveStart"/> when it is set (typed by <paramref name="files"/>' columns where they have them).
    /// </summary>
    public static Partitions? PartitionsOf(IReadOnlyList<DatasetFile> files, DatasetOptions options, int? hiveStart, Schema fileSchema)
    {
        if (files.Any(f => f.PartitionValues is not null))
        {
            Schema given = options.PartitionSchema
                ?? throw new ArgumentException($"Files are given with partition values: give their types too ({options.Name}.PartitionSchema).", nameof(files));
            return Partitions.FromValues(files, given);
        }
        return hiveStart is int start && options.Partitioning != HivePartitioning.Off
            ? Partitions.FromHive([.. files.Select(f => f.Path)], start, options.PartitionSchema, fileSchema, options.ParseHiveDates, options.CaseInsensitiveNames)
            : null;
    }

    /// <summary>
    /// The files' columns united by name: first appearance order, each of the narrowest type holding the files', nullable.
    /// </summary>
    /// <param name="files">Each file's path and columns.</param>
    /// <param name="options">The dataset's options (lossy conversions; their name in messages).</param>
    /// <param name="error">The format's error for a message (naming the file).</param>
    /// <param name="common">The narrowest type two types convert to (null: none), lossy conversions allowed or not; null for <see cref="ArrowWidening.Common"/>.</param>
    public static Schema Union(IReadOnlyList<(string Path, Schema Schema)> files, DatasetOptions options, Func<string, Exception> error,
        Func<IArrowType, IArrowType, bool, IArrowType?>? common = null)
    {
        common ??= ArrowWidening.Common;
        List<Field> fields = [];
        List<string> origins = [];
        Dictionary<string, int> index = new(StringComparer.Ordinal);
        foreach (var (path, given) in files)
        {
            // Ignoring case, each file's fields spelled as the columns so far (its own spelling for new ones).
            Schema schema = given;
            if (options.CaseInsensitiveNames)
            {
                if (ArrowNames.CaseTwins(given.FieldsList) is var (level, first, second))
                    throw error($"{path}: the file's {(level.Length == 0 ? "columns" : $"fields of '{level}'")} '{first}' and '{second}' differ in case alone: with CaseInsensitiveNames they cannot be told apart.");
                schema = ArrowNames.Respell(given, fields, (at, a, b, t) => error(TwiceMessage(path, at, a, b, t)));
            }
            foreach (Field field in schema.FieldsList)
            {
                if (!index.TryGetValue(field.Name, out int at))
                {
                    index[field.Name] = fields.Count;
                    fields.Add(field.IsNullable ? field : new Field(field.Name, field.DataType, nullable: true, field.Metadata));
                    origins.Add(path);
                    continue;
                }
                IArrowType current = fields[at].DataType;
                if (common(current, field.DataType, options.AllowLossyCasts) is not { } united)
                {
                    string hint = !options.AllowLossyCasts && common(current, field.DataType, true) is not null
                        ? $" without a lossy conversion ({options.Name}.AllowLossyCasts allows them)" : "";
                    throw error($"{path}: column '{field.Name}' is {ArrowTypes.Describe(field.DataType)} in this file but "
                        + $"{ArrowTypes.Describe(current)} in '{origins[at]}': no type holds both{hint}.");
                }
                fields[at] = new Field(field.Name, united, nullable: true, fields[at].Metadata);
            }
        }
        return new Schema(fields, null);
    }

    /// <summary>The error of two fields of a file matching one of the dataset's ignoring case, neither exactly.</summary>
    internal static string TwiceMessage(string path, string level, string first, string second, string target) =>
        $"{path}: the file's {(level.Length == 0 ? "columns" : $"fields of '{level}'")} '{first}' and '{second}' both match the dataset's '{target}' ignoring case "
        + "(CaseInsensitiveNames): rename one of them, or read the files with exact names.";

    /// <summary>
    /// Checks, ignoring case, the columns of a dataset that opens (<see cref="DatasetOptions.CaseInsensitiveNames"/>):
    /// none at one level may differ from another in case alone (the files could not be matched to them).
    /// </summary>
    internal static void CheckNames(DatasetOptions options, Schema columns, Func<string, Exception> error)
    {
        if (options.CaseInsensitiveNames && ArrowNames.CaseTwins(columns.FieldsList) is var (level, first, second))
            throw error($"The dataset's {(level.Length == 0 ? "columns" : $"fields of '{level}'")} '{first}' and '{second}' differ in case alone: "
                + "with CaseInsensitiveNames they cannot be told apart (rename one, or match exact names).");
    }

    /// <summary>
    /// The store of inputs given without one: the local file system for paths, a shared <see cref="HttpFileStore"/>
    /// for http(s) URLs, a shared <see cref="HuggingFaceFileStore"/> for hf:// ones; one store for all of them.
    /// </summary>
    /// <exception cref="ArgumentException">A URL of another scheme (its store must be given), or inputs of several stores.</exception>
    private static IFileStore StoreOf(IReadOnlyList<string> inputs, DatasetOptions options)
    {
        IFileStore? chosen = null;
        string? first = null;
        foreach (string input in inputs)
        {
            int end = input.IndexOf("://", StringComparison.Ordinal);
            string? scheme = end > 0 ? input[..end].ToLowerInvariant() : null;
            IFileStore store = scheme switch
            {
                null => LocalFiles,
                "http" or "https" => DefaultHttp.Value,
                "hf" => DefaultHuggingFace.Value,
                _ => throw new ArgumentException($"'{input}' is a {scheme}:// URL: give the store that reads it ({options.Name}.Store), "
                    + $"such as CloudFileStore.Open(\"{scheme}://…\") of Kaminari.Seseragi.Lakehouse.", nameof(inputs)),
            };
            if (chosen is null)
            {
                chosen = store;
                first = input;
            }
            else if (!ReferenceEquals(chosen, store))
                throw new ArgumentException($"'{first}' and '{input}' are read from different stores: a dataset reads from one.", nameof(inputs));
        }
        return chosen ?? LocalFiles;
    }

    /// <summary>
    /// Adds the files <paramref name="input"/> names: itself, the files below a directory, or a glob's matches. Tells
    /// whether it was a directory, and where Hive keys start in the paths (after a directory; everywhere for others).
    /// </summary>
    private static async ValueTask<(bool Directory, int HiveStart)> ExpandAsync(IFileStore store, string input, DatasetOptions options,
        List<(string Path, long? Size)> files, bool alone, CancellationToken ct)
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
                    files.Add((file.Path, file.Size));
            }
        }
        else if (path.EndsWith('/') || store is IDirectoryAware directories && directories.IsDirectory(input))
        {
            if (!store.CanList) throw new NotSupportedException($"'{input}' is a directory, but the store cannot list its files.");
            await foreach (StoredFile file in store.ListAsync(input, recursive: true, ct).ConfigureAwait(false))
            {
                if (file.Size > 0 && Kept(file.Path, options)) files.Add((file.Path, file.Size));
            }
            if (alone) SameExtension(input, files);
            files.Sort(start, files.Count - start, ByPath.Instance);
            return (true, input.Length);
        }
        else
        {
            if (Kept(input, options)) files.Add((input, null));
            return (false, 0);
        }
        files.Sort(start, files.Count - start, ByPath.Instance);
        return (false, 0);
    }

    /// <summary>HTTP(S) URLs are never globs: their '?' starts a query (as in signed URLs).</summary>
    private static bool IsHttp(string path) =>
        path.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || path.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether a file's name starts with none of the prefixes to leave out.</summary>
    private static bool Kept(string path, DatasetOptions options)
    {
        if (options.IgnorePrefixes is not { Count: > 0 } prefixes) return true;
        string name = path[(path.AsSpan().LastIndexOfAny('/', Path.DirectorySeparatorChar) + 1)..];
        return !prefixes.Any(p => name.StartsWith(p, StringComparison.Ordinal));
    }

    /// <summary>
    /// The files of a directory given alone must share an extension (a compressed file's being the one before its
    /// compression's: <c>a.jsonl.gz</c> is a <c>.jsonl</c>): a glob says which to read otherwise.
    /// </summary>
    private static void SameExtension(string directory, List<(string Path, long? Size)> files)
    {
        string? first = null;
        foreach (var (path, _) in files)
        {
            string extension = Extension(path);
            if (first is null) first = extension;
            else if (extension != first)
                throw new InvalidOperationException($"The directory '{directory}' holds files of different extensions ('{first}', '{extension}' in '{path}'): "
                    + $"give a glob that says which to read (such as '{directory.TrimEnd('/')}/**/*{first}').");
        }
    }

    // A file's extension, a compression's suffix (.gz, .zst) passed over.
    private static string Extension(string path)
    {
        string extension = Path.GetExtension(path);
        return extension is ".gz" or ".zst" ? Path.GetExtension(path[..^extension.Length]) : extension;
    }

    private sealed class ByPath : IComparer<(string Path, long? Size)>
    {
        public static readonly ByPath Instance = new();

        public int Compare((string Path, long? Size) x, (string Path, long? Size) y) => string.CompareOrdinal(x.Path, y.Path);
    }

    /// <summary>A store's failure (a network's, a disk's) with the file it is about.</summary>
    public static IOException InFile(string path, IOException e) => new($"{path}: {e.Message}", e);

    /// <summary>Whether a store's failure does not name the file yet (a missing file's does).</summary>
    public static bool Unnamed(IOException e, string path) => e is not FileNotFoundException && !e.Message.StartsWith(path + ": ", StringComparison.Ordinal);
}
