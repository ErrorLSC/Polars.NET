using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Csv.Reading;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// Delimited text files (CSV, TSV, …) read as one table: files named, directories (every file below them), globs
/// (<c>*</c>, <c>?</c>, <c>[...]</c>, <c>**</c> as a whole segment), or a list of files known beforehand, from the local
/// file system or a store (<see cref="DatasetOptions.Store"/>); gzip, zlib and zstd files are decompressed as they are
/// read. Every file is written alike (<see cref="CsvDatasetOptions.Format"/>: its header, skips and comments are its
/// own). The columns are given, or inferred from the first records of the first files, united by name
/// (<see cref="CsvDatasetOptions"/>). Partition columns come from <c>key=value</c> directories
/// (<see cref="DatasetOptions.Partitioning"/>).
/// </summary>
/// <remarks>
/// Paths are expanded as dataframe engines expand them (see <see cref="DatasetOptions"/>; empty files are left out of
/// listings). A dataset's columns are nullable. Partition strings and the file path column are string views.
/// </remarks>
public sealed class CsvDataset : IDisposable, IAsyncDisposable
{
    /// <summary>How many files' first records are inferred at once.</summary>
    internal const int HeadsAtOnce = 16;

    private CsvDataset(IFileStore store, CsvDatasetOptions options, CsvDatasetFile[] files, FileVersions versions, Schema fileSchema, int inferredFrom,
        Partitions? partitions)
    {
        Store = store;
        Versions = versions;
        Options = options;
        Files = files;
        FileSchema = fileSchema;
        InferredFrom = inferredFrom;
        Partitions = partitions;
        // The files' columns (a partition column of the same name in its place: the path's value wins), then the
        // other partition columns, then the file path.
        List<Field> fields = [.. fileSchema.FieldsList.Select(f => partitions?.IndexOf(f.Name) is int p and >= 0 ? PartitionField(p, views: true) : f)];
        for (int p = 0; p < (partitions?.Fields.Length ?? 0); p++)
        {
            if (fileSchema.GetFieldIndex(partitions!.Fields[p].Name) < 0) fields.Add(PartitionField(p, views: true));
        }
        if (options.FilePathColumn is { } path)
        {
            if (fields.Any(f => f.Name == path)) throw new ArgumentException($"The file path column '{path}' has the name of a column of the files.", nameof(options));
            fields.Add(new Field(path, StringViewType.Default, nullable: false));
        }
        Schema = new Schema(fields, fileSchema.Metadata);
        PartitionSchema = partitions is null ? null : new Schema([.. Enumerable.Range(0, partitions.Fields.Length).Select(p => PartitionField(p, views: true))], null);
    }

    /// <summary>The versions the files were first opened at (reads ask for them: see <see cref="FileVersions"/>).</summary>
    internal FileVersions Versions { get; }

    /// <summary>The partition values of the files (one row per file), when the dataset is partitioned.</summary>
    internal Partitions? Partitions { get; }

    /// <summary>
    /// The partition columns, in the order of the keys: the type given in <see cref="DatasetOptions.PartitionSchema"/>,
    /// else the type inferred (strings as views); null when the dataset is not partitioned.
    /// </summary>
    public Schema? PartitionSchema { get; }

    /// <summary>Partition column <paramref name="index"/> as batches hold it: inferred strings as views when <paramref name="views"/>.</summary>
    internal Field PartitionField(int index, bool views)
    {
        Field field = Partitions!.Fields[index];
        bool given = Options.PartitionSchema?.GetFieldByName(field.Name) is { } declared && declared.DataType.TypeId == field.DataType.TypeId;
        return field.DataType is StringType && !given && views ? new Field(field.Name, StringViewType.Default, nullable: true) : field;
    }

    /// <summary>The store the files are read from.</summary>
    internal IFileStore Store { get; }

    internal CsvDatasetOptions Options { get; }

    /// <summary>The dataset's columns from files (given or inferred; nullable).</summary>
    internal Schema FileSchema { get; }

    /// <summary>The records per file the columns were inferred from (0 when given): errors then suggest overrides.</summary>
    internal int InferredFrom { get; }

    /// <summary>
    /// The schema of the batches read with default options: the columns from files (given, or inferred), then the
    /// partition columns the files do not have, then the file path column if asked.
    /// </summary>
    public Schema Schema { get; }

    /// <summary>The files, in the order read.</summary>
    public IReadOnlyList<CsvDatasetFile> Files { get; }

    /// <summary>Opens the dataset at a path, a directory or a glob.</summary>
    /// <exception cref="FileNotFoundException">No file is found.</exception>
    /// <exception cref="CsvFormatException">A file the columns are inferred from cannot be read, or its columns are not the dataset's (the message names it).</exception>
    public static CsvDataset Open(string pathOrGlob, CsvDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: false, default));

    /// <summary>Opens the dataset of several paths, directories or globs, read in the order given.</summary>
    /// <inheritdoc cref="Open(string, CsvDatasetOptions?)"/>
    public static CsvDataset Open(IEnumerable<string> pathsOrGlobs, CsvDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: false, default));

    /// <summary>Opens the dataset of files known beforehand (no listing): read in the order given.</summary>
    /// <inheritdoc cref="Open(string, CsvDatasetOptions?)"/>
    public static CsvDataset Open(IEnumerable<CsvDatasetFile> files, CsvDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: false, default));

    /// <inheritdoc cref="Open(string, CsvDatasetOptions?)"/>
    public static ValueTask<CsvDataset> OpenAsync(string pathOrGlob, CsvDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{string}, CsvDatasetOptions?)"/>
    public static ValueTask<CsvDataset> OpenAsync(IEnumerable<string> pathsOrGlobs, CsvDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{CsvDatasetFile}, CsvDatasetOptions?)"/>
    public static ValueTask<CsvDataset> OpenAsync(IEnumerable<CsvDatasetFile> files, CsvDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: true, cancellationToken);

    /// <summary>
    /// Opens a reader of the dataset's batches (see <see cref="CsvDatasetReader"/> for how the options apply: the
    /// dataset's options decide how the files are written and the columns).
    /// </summary>
    /// <exception cref="ArgumentException">The options give a schema, name columns the dataset lacks, or are out of range.</exception>
    public CsvDatasetReader OpenReader(CsvReadOptions? options = null) => new(this, options ?? new CsvReadOptions());

    /// <summary>Nothing is held open between reads.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async ValueTask<CsvDataset> OpenCoreAsync(string[] inputs, CsvDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new CsvDatasetOptions();
        options.Validate();
        var (store, files, hiveStart) = await DatasetFiles.ExpandAsync(inputs, options, ct).ConfigureAwait(false);
        return await FromFilesAsync(store, options, [.. files.Select(f => new CsvDatasetFile(f.Path, f.Size))], hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<CsvDataset> OpenCoreAsync(CsvDatasetFile[] files, CsvDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new CsvDatasetOptions();
        options.Validate();
        var (store, hiveStart) = DatasetFiles.Given(files, options);
        return await FromFilesAsync(store, options, files, hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The dataset of the files: the columns given, or inferred from the heads of the first files (every file's with
    /// UnionByName); partitions from the values given with the files, else from the paths after
    /// <paramref name="hiveStart"/> when set.
    /// </summary>
    private static async ValueTask<CsvDataset> FromFilesAsync(IFileStore store, CsvDatasetOptions options, CsvDatasetFile[] files, int? hiveStart,
        bool useAsync, CancellationToken ct)
    {
        var versions = new FileVersions(files.Length);
        Schema schema;
        int inferredFrom = 0;
        if (options.Schema is { } declared)
            schema = new Schema([.. declared.FieldsList.Select(f => f.IsNullable ? f : new Field(f.Name, f.DataType, nullable: true, f.Metadata))], declared.Metadata);
        else
        {
            int count = options.SchemaMode == DatasetSchemaMode.UnionByName ? files.Length : Math.Min(files.Length, options.InferSchemaFiles);
            CsvHead[] heads = await HeadsAsync(store, options, files, versions, count, useAsync, ct).ConfigureAwait(false);
            try
            {
                schema = United(heads, files, options);
            }
            finally
            {
                foreach (CsvHead head in heads) head.Dispose();
            }
            inferredFrom = options.Format.InferSchemaLength ?? int.MaxValue;
        }
        Partitions? partitions = DatasetFiles.PartitionsOf(files, options, hiveStart, schema);
        return new CsvDataset(store, options, files, versions, schema, inferredFrom, partitions);
    }

    /// <summary>
    /// The heads of the first <paramref name="count"/> files, read side by side (in order with one); a failure is the
    /// first file's that fails, as if they were read in order.
    /// </summary>
    private static async ValueTask<CsvHead[]> HeadsAsync(IFileStore store, CsvDatasetOptions options, CsvDatasetFile[] files, FileVersions versions, int count,
        bool useAsync, CancellationToken ct)
    {
        var heads = new CsvHead?[count];
        try
        {
            if (count == 1) heads[0] = await HeadAsync(store, options, files, versions, 0, useAsync, ct).ConfigureAwait(false);
            else
            {
                var failures = new Exception?[count];
                await Parallel.ForEachAsync(Enumerable.Range(0, count), new ParallelOptions { MaxDegreeOfParallelism = HeadsAtOnce, CancellationToken = ct },
                    async (i, token) =>
                    {
                        try
                        {
                            heads[i] = await HeadAsync(store, options, files, versions, i, useAsync: true, token).ConfigureAwait(false);
                        }
                        catch (Exception e) when (e is not OperationCanceledException || !token.IsCancellationRequested)
                        {
                            failures[i] = e;
                        }
                    }).ConfigureAwait(false);
                if (failures.FirstOrDefault(f => f is not null) is { } first) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(first);
            }
            return heads!;
        }
        catch
        {
            foreach (CsvHead? head in heads) head?.Dispose();
            throw;
        }
    }

    // A file's head; errors name the file.
    private static async ValueTask<CsvHead> HeadAsync(IFileStore store, CsvDatasetOptions options, CsvDatasetFile[] files, FileVersions versions, int index,
        bool useAsync, CancellationToken ct)
    {
        CsvDatasetFile file = files[index];
        IRandomAccess source = await OpenSourceAsync(store, file, versions, index, ct).ConfigureAwait(false);
        try
        {
            return await CsvReader.HeadAsync(source, options.Format, mappable: true, useAsync, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (Named(file.Path, e) is { } named)
        {
            throw named;
        }
        finally
        {
            versions.Note(index, source); // the version read (a store learns it reading)
            await Close(source).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The columns of the heads, in file order, as if the files were one: the first file's columns, every other file
    /// holding the same ones (by name: an extra one is left out with IgnoreExtraColumns), or with AllowMissingColumns
    /// (or UnionByName) every file's in the order first seen; their values' kinds united; overrides applied.
    /// </summary>
    private static Schema United(CsvHead[] heads, CsvDatasetFile[] files, CsvDatasetOptions options)
    {
        bool byName = options.AllowMissingColumns || options.SchemaMode == DatasetSchemaMode.UnionByName;
        List<string> names = [];
        HashSet<string> known = new(StringComparer.Ordinal);
        CsvInference? root = null;
        Dictionary<string, int> rootColumn = new(StringComparer.Ordinal);
        for (int i = 0; i < heads.Length; i++)
        {
            CsvHead head = heads[i];
            if (i == 0) foreach (string name in head.Names) Add(name);
            else if (byName)
            {
                foreach (string name in head.Names) Add(name);
            }
            else
            {
                if (names.FirstOrDefault(n => !head.Names.Contains(n)) is { } lacking)
                    throw new CsvFormatException($"{files[i].Path}: the file lacks the column '{lacking}' of the dataset's first file " +
                        $"({string.Join(", ", names)}): give AllowMissingColumns = true to read it as nulls.");
                if (!options.IgnoreExtraColumns && head.Names.FirstOrDefault(n => !known.Contains(n)) is { } extra)
                    throw new CsvFormatException($"{files[i].Path}: the file's column '{extra}' is not one of the dataset's first file " +
                        $"({string.Join(", ", names)}): give IgnoreExtraColumns = true to leave it out, or AllowMissingColumns = true to unite the files' columns.");
            }
            if (head.Inference is not { } inference) continue;
            if (root is null)
            {
                root = inference;
                for (int c = 0; c < head.Names.Length; c++) rootColumn[head.Names[c]] = c;
                continue;
            }
            for (int c = 0; c < head.Names.Length; c++)
            {
                if (!known.Contains(head.Names[c])) continue;
                if (!rootColumn.TryGetValue(head.Names[c], out int r))
                {
                    r = root.Columns;
                    root.Widen(r + 1);
                    rootColumn[head.Names[c]] = r;
                }
                root.Absorb(r, inference, c);
            }
            root.AbsorbRecords(inference);
        }
        if (names.Count == 0) throw new CsvFormatException("The dataset's files hold no columns to infer (give a schema).");
        CsvReadOptions format = options.Format;
        IArrowType text = format.ViewTypes ? StringViewType.Default : StringType.Default;
        Dictionary<string, Field> inferred = new(StringComparer.Ordinal);
        if (root is not null)
        {
            string[] rootNames = new string[root.Columns];
            foreach ((string name, int c) in rootColumn) rootNames[c] = name;
            foreach (Field field in root.Finish(rootNames, format.ViewTypes).Fields) inferred[field.Name] = field;
        }
        Field[] fields = [.. names.Select(n => inferred.TryGetValue(n, out Field? f) ? f : new Field(n, text, true))];
        if (format.SchemaOverrides is { } overrides)
        {
            var unknown = overrides.Keys.Where(k => !known.Contains(k)).ToList();
            if (unknown.Count > 0) throw new ArgumentException($"The inferred columns have none named {string.Join(", ", unknown)}.", nameof(options));
            fields = [.. fields.Select(f => overrides.TryGetValue(f.Name, out IArrowType? t) ? new Field(f.Name, t, true) : f)];
        }
        return new Schema(fields, null);

        void Add(string name)
        {
            if (known.Add(name)) names.Add(name);
        }
    }

    /// <summary>
    /// Opens file <paramref name="index"/> of the store: at the version it was first read at (<paramref name="versions"/>,
    /// noted by the caller once read), else at its size from the listing when known; errors name the file.
    /// </summary>
    internal static async ValueTask<IRandomAccess> OpenSourceAsync(IFileStore store, CsvDatasetFile file, FileVersions versions, int index, CancellationToken ct)
    {
        try
        {
            return versions.Get(index) is { } version ? await store.OpenAsync(version with { Path = file.Path }, ct).ConfigureAwait(false)
                : file.Size is long size ? await store.OpenAsync(new StoredFile(file.Path, size), ct).ConfigureAwait(false)
                : await store.OpenAsync(file.Path, ct).ConfigureAwait(false) ?? throw new FileNotFoundException($"'{file.Path}' does not exist.", file.Path);
        }
        catch (IOException e) when (DatasetFiles.Unnamed(e, file.Path))
        {
            throw DatasetFiles.InFile(file.Path, e);
        }
    }

    /// <summary>Closes a source a store opened (when it holds something).</summary>
    internal static async ValueTask Close(IRandomAccess source)
    {
        if (source is IAsyncDisposable asyncDisposable) await asyncDisposable.DisposeAsync().ConfigureAwait(false);
        else (source as IDisposable)?.Dispose();
    }

    /// <summary>The error with the file it is about (null when it is not one to name: it names it already, or is another kind).</summary>
    internal static Exception? Named(string path, Exception e) => e switch
    {
        CsvFormatException f when !f.Message.StartsWith(path + ": ", StringComparison.Ordinal) => new CsvFormatException($"{path}: {f.Message}", f.Row, f.Line, f),
        CsvValueException v when !v.Message.StartsWith(path + ": ", StringComparison.Ordinal) => new CsvValueException($"{path}: {v.Message}", v.Column, v.Row, v.Line, v.Value),
        IOException io when DatasetFiles.Unnamed(io, path) => DatasetFiles.InFile(path, io),
        _ => null,
    };
}
