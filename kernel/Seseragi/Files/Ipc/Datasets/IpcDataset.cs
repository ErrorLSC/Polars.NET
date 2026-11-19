using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Compute;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Arrow IPC files (<c>.arrow</c>, Feather v2) read as one table: files named, directories (every file below them),
/// globs (<c>*</c>, <c>?</c>, <c>[...]</c>, <c>**</c> as a whole segment), or a list of files known beforehand, from the
/// local file system or a store (<see cref="DatasetOptions.Store"/>). The columns are the first file's, every file's
/// united (<see cref="DatasetSchemaMode"/>), or given (<see cref="DatasetOptions.Schema"/>); files' columns convert to
/// the dataset's types by lossless widenings (<see cref="IpcDatasetReader"/>), columns they have beyond them are not
/// read. Partition columns come from <c>key=value</c> directories (<see cref="DatasetOptions.Partitioning"/>).
/// </summary>
/// <remarks>
/// Paths are expanded as dataframe engines expand them (see <see cref="DatasetOptions"/>). A dataset's columns are
/// nullable: its files may declare them differently. Partition strings and the file path column are string views.
/// </remarks>
public sealed class IpcDataset : IDisposable, IAsyncDisposable
{
    /// <summary>How many footers are read at once (a dataset united by name reads every one when it opens).</summary>
    internal const int FootersAtOnce = 16;

    private IpcDataset(IFileStore store, IpcDatasetOptions options, IpcDatasetFile[] files, KeptFooters kept, Schema fileSchema, Partitions? partitions)
    {
        Store = store;
        Options = options;
        Files = files;
        Footers = kept;
        // One dictionary per column for the dataset: every dictionary indexed by Int32.
        if (options.UnifyDictionaries) fileSchema = DictionaryUnifier.Unified(fileSchema);
        FileSchema = fileSchema;
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
        // As given, unless the given type was a temporal one read as strings.
        bool given = Options.PartitionSchema?.GetFieldByName(field.Name) is { } declared && declared.DataType.TypeId == field.DataType.TypeId;
        return field.DataType is StringType && !given && views ? new Field(field.Name, StringViewType.Default, nullable: true) : field;
    }

    /// <summary>The store the files are read from.</summary>
    internal IFileStore Store { get; }

    internal IpcDatasetOptions Options { get; }

    /// <summary>The dataset's columns from files (the first file's, united or given; nullable).</summary>
    internal Schema FileSchema { get; }

    /// <summary>
    /// The schema of the batches read with default options: the columns from files (the first file's, every file's
    /// united, or the given ones; nullable), then the partition columns the files do not have, then the file path
    /// column if asked.
    /// </summary>
    public Schema Schema { get; }

    /// <summary>The files, in the order read.</summary>
    public IReadOnlyList<IpcDatasetFile> Files { get; }

    /// <summary>The footers kept between reads (of files on stores: see <see cref="KeptFooters"/>).</summary>
    internal KeptFooters Footers { get; }

    /// <summary>Opens the dataset at a path, a directory or a glob.</summary>
    /// <exception cref="FileNotFoundException">No file is found.</exception>
    /// <exception cref="IpcFormatException">
    /// The first file (every file, united by name) is not an Arrow IPC file, or (united) no type holds a column's types in two files.
    /// </exception>
    public static IpcDataset Open(string pathOrGlob, IpcDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: false, default));

    /// <summary>Opens the dataset of several paths, directories or globs, read in the order given.</summary>
    /// <inheritdoc cref="Open(string, IpcDatasetOptions?)"/>
    public static IpcDataset Open(IEnumerable<string> pathsOrGlobs, IpcDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: false, default));

    /// <summary>Opens the dataset of files known beforehand (no listing): read in the order given.</summary>
    /// <inheritdoc cref="Open(string, IpcDatasetOptions?)"/>
    public static IpcDataset Open(IEnumerable<IpcDatasetFile> files, IpcDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: false, default));

    /// <inheritdoc cref="Open(string, IpcDatasetOptions?)"/>
    public static ValueTask<IpcDataset> OpenAsync(string pathOrGlob, IpcDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{string}, IpcDatasetOptions?)"/>
    public static ValueTask<IpcDataset> OpenAsync(IEnumerable<string> pathsOrGlobs, IpcDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{IpcDatasetFile}, IpcDatasetOptions?)"/>
    public static ValueTask<IpcDataset> OpenAsync(IEnumerable<IpcDatasetFile> files, IpcDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: true, cancellationToken);

    /// <summary>Opens a reader of the dataset's batches (see <see cref="IpcDatasetReader"/> for how the options apply).</summary>
    /// <exception cref="ArgumentException">The options name columns the dataset lacks, or are out of range.</exception>
    public IpcDatasetReader OpenReader(IpcReadOptions? options = null) => new(this, options ?? new IpcReadOptions());

    /// <summary>
    /// Opens a reader of the dataset's rows as <typeparamref name="T"/>, of the columns its members map (columns from
    /// files and partition columns), filtered by <paramref name="where"/> (pushed into the read: conjuncts over
    /// partition members decide which files are read; see <see cref="IpcDatasetReader{T}"/>).
    /// </summary>
    /// <exception cref="ArgumentException">The dataset has no column for a member, or the options are out of range.</exception>
    /// <exception cref="NotSupportedException">A conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    public IpcDatasetReader<T> OpenReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where = null,
        IpcReadOptions? options = null) => new(this, where, options);

    /// <summary>Reads the rows of the dataset at a path, a directory or a glob as <typeparamref name="T"/> (see <see cref="IpcDatasetReader{T}"/>).</summary>
    /// <inheritdoc cref="OpenReader{T}(Expression{Func{T, bool}}?, IpcReadOptions?)"/>
    public static List<T> ReadList<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string pathOrGlob, Expression<Func<T, bool>>? where = null,
        IpcDatasetOptions? datasetOptions = null, IpcReadOptions? readOptions = null)
    {
        using IpcDataset dataset = Open(pathOrGlob, datasetOptions);
        using IpcDatasetReader<T> reader = dataset.OpenReader(where, readOptions);
        return reader.ReadList();
    }

    /// <inheritdoc cref="ReadList{T}(string, Expression{Func{T, bool}}?, IpcDatasetOptions?, IpcReadOptions?)"/>
    public static async Task<List<T>> ReadListAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string pathOrGlob,
        Expression<Func<T, bool>>? where = null, IpcDatasetOptions? datasetOptions = null, IpcReadOptions? readOptions = null,
        CancellationToken cancellationToken = default)
    {
        await using IpcDataset dataset = await OpenAsync(pathOrGlob, datasetOptions, cancellationToken).ConfigureAwait(false);
        await using IpcDatasetReader<T> reader = dataset.OpenReader(where, readOptions);
        return await reader.ReadListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The columns a filter reads: the columns from files (a partition column of the same name in its place), then the
    /// other partition columns.
    /// </summary>
    internal Schema FilterSchema()
    {
        List<Field> fields = [.. FileSchema.FieldsList.Select(f => Partitions?.IndexOf(f.Name) is int p and >= 0 ? PartitionField(p, views: true) : f)];
        for (int p = 0; p < (Partitions?.Fields.Length ?? 0); p++)
        {
            if (FileSchema.GetFieldIndex(Partitions!.Fields[p].Name) < 0) fields.Add(PartitionField(p, views: true));
        }
        return new Schema(fields, null);
    }

    /// <summary>Nothing is held open between reads; kept for stores that will hold connections.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async ValueTask<IpcDataset> OpenCoreAsync(string[] inputs, IpcDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new IpcDatasetOptions();
        options.Validate();
        var (store, files, hiveStart) = await DatasetFiles.ExpandAsync(inputs, options, ct).ConfigureAwait(false);
        return await FromFilesAsync(store, options, [.. files.Select(f => new IpcDatasetFile(f.Path, f.Size))], hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<IpcDataset> OpenCoreAsync(IpcDatasetFile[] files, IpcDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new IpcDatasetOptions();
        options.Validate();
        var (store, hiveStart) = DatasetFiles.Given(files, options);
        return await FromFilesAsync(store, options, files, hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The dataset of the files: the first one's footer read for the schema (every one's, united by name; none, given);
    /// partitions from the values given with the files, else from the paths after <paramref name="hiveStart"/> when set.
    /// </summary>
    private static async ValueTask<IpcDataset> FromFilesAsync(IFileStore store, IpcDatasetOptions options, IpcDatasetFile[] files, int? hiveStart,
        bool useAsync, CancellationToken ct)
    {
        var kept = new KeptFooters(files.Length);
        Schema schema;
        if (options.Schema is { } declared) schema = Nullable(declared); // no file read
        else if (options.SchemaMode == DatasetSchemaMode.UnionByName)
        {
            var schemas = new Schema[files.Length];
            await Parallel.ForEachAsync(Enumerable.Range(0, files.Length), new ParallelOptions { MaxDegreeOfParallelism = FootersAtOnce, CancellationToken = ct },
                async (i, token) =>
                {
                    await using IpcFile file = await OpenFileAsync(store, files[i], kept, i, useAsync: true, token).ConfigureAwait(false);
                    schemas[i] = file.Schema;
                }).ConfigureAwait(false);
            schema = DatasetFiles.Union([.. files.Select((f, i) => (f.Path, schemas[i]))], options, message => new IpcFormatException(message), IpcDatasetTypes.Common);
        }
        else
        {
            await using IpcFile first = await OpenFileAsync(store, files[0], kept, 0, useAsync, ct).ConfigureAwait(false);
            schema = Nullable(first.Schema);
        }
        if (schema.FieldsList.GroupBy(f => f.Name, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1) is { } twice)
            throw new IpcFormatException($"{files[0].Path}: column '{twice.Key}' appears {twice.Count()} times: a dataset's columns have names of their own "
                + "(read the file with IpcFile instead).");
        Partitions? partitions = DatasetFiles.PartitionsOf(files, options, hiveStart, schema);
        return new IpcDataset(store, options, files, kept, schema, partitions);

        static Schema Nullable(Schema schema) =>
            new([.. schema.FieldsList.Select(f => f.IsNullable ? f : new Field(f.Name, f.DataType, nullable: true, f.Metadata))], schema.Metadata);
    }

    /// <summary>
    /// Opens file <paramref name="index"/> of the store (owned by the <see cref="IpcFile"/>); errors name the file. Its
    /// footer comes from <paramref name="kept"/> when kept there (with its size and version: no request), else it is read
    /// and kept when that is worth it (see <see cref="KeptFooters"/>).
    /// </summary>
    internal static async ValueTask<IpcFile> OpenFileAsync(IFileStore store, IpcDatasetFile file, KeptFooters kept, int index, bool useAsync, CancellationToken ct)
    {
        ReadOnlyMemory<byte>? footer = kept.Get(index);
        StoredFile? known = footer is not null && kept.Version(index) is { } version ? version with { Path = file.Path }
            : file.Size is long size ? new StoredFile(file.Path, size) : null;
        IRandomAccess source;
        try
        {
            source = known is { } stored
                ? await store.OpenAsync(stored, ct).ConfigureAwait(false)
                : await store.OpenAsync(file.Path, ct).ConfigureAwait(false) ?? throw new FileNotFoundException($"'{file.Path}' does not exist.", file.Path);
        }
        catch (IOException e) when (DatasetFiles.Unnamed(e, file.Path))
        {
            throw DatasetFiles.InFile(file.Path, e);
        }
        FooterRead? keep = footer is null && KeptFooters.Worth(source, IpcFile.TailGuess) ? bytes => kept.Keep(index, bytes, (source as IStoredSource)?.Stored) : null;
        try
        {
            return await IpcFile.OpenOwnedAsync(source, footer, keep, useAsync, ct).ConfigureAwait(false);
        }
        catch (IpcFormatException e)
        {
            throw InFile(file.Path, e);
        }
        catch (IOException e) when (DatasetFiles.Unnamed(e, file.Path))
        {
            throw DatasetFiles.InFile(file.Path, e);
        }
    }

    /// <summary>The error with the file it is about.</summary>
    internal static IpcFormatException InFile(string path, IpcFormatException e) => new($"{path}: {e.Message}", e);
}
