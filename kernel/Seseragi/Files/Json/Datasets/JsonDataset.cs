using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json.Reading;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// JSON files (newline-delimited, or documents: <see cref="JsonDatasetOptions.Format"/>) read as one table: files named,
/// directories (every file below them), globs (<c>*</c>, <c>?</c>, <c>[...]</c>, <c>**</c> as a whole segment), or a list
/// of files known beforehand, from the local file system or a store (<see cref="DatasetOptions.Store"/>); gzip, zlib and
/// zstd files are decompressed as they are read. The columns are given, or inferred from the first file's records or
/// every file's, united (<see cref="JsonDatasetOptions"/>). Partition columns come from <c>key=value</c> directories
/// (<see cref="DatasetOptions.Partitioning"/>).
/// </summary>
/// <remarks>
/// Paths are expanded as dataframe engines expand them (see <see cref="DatasetOptions"/>). A dataset's columns are
/// nullable. Partition strings and the file path column are string views.
/// </remarks>
public sealed class JsonDataset : IDisposable, IAsyncDisposable
{
    /// <summary>How many files' first records are inferred at once (a dataset united by name reads every file's when it opens).</summary>
    internal const int HeadsAtOnce = 16;

    private JsonDataset(IFileStore store, JsonDatasetOptions options, JsonDatasetFile[] files, Schema fileSchema, bool inferred, Partitions? partitions)
    {
        Store = store;
        Options = options;
        Files = files;
        FileSchema = fileSchema;
        Inferred = inferred;
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
        bool given = Options.PartitionSchema?.GetFieldByName(field.Name) is { } declared && declared.DataType.TypeId == field.DataType.TypeId;
        return field.DataType is StringType && !given && views ? new Field(field.Name, StringViewType.Default, nullable: true) : field;
    }

    /// <summary>The store the files are read from.</summary>
    internal IFileStore Store { get; }

    internal JsonDatasetOptions Options { get; }

    /// <summary>The dataset's columns from files (given or inferred; nullable).</summary>
    internal Schema FileSchema { get; }

    /// <summary>Whether the columns were inferred (a document's field they lack is then an error, as in one file).</summary>
    internal bool Inferred { get; }

    /// <summary>
    /// The schema of the batches read with default options: the columns from files (given, or inferred), then the
    /// partition columns the files do not have, then the file path column if asked.
    /// </summary>
    public Schema Schema { get; }

    /// <summary>The files, in the order read.</summary>
    public IReadOnlyList<JsonDatasetFile> Files { get; }

    /// <summary>Opens the dataset at a path, a directory or a glob.</summary>
    /// <exception cref="FileNotFoundException">No file is found.</exception>
    /// <exception cref="JsonFormatException">A file the columns are inferred from is not JSON of the layout read (the message names it).</exception>
    /// <exception cref="JsonValueException">The records the columns are inferred from hold values no type holds together.</exception>
    public static JsonDataset Open(string pathOrGlob, JsonDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: false, default));

    /// <summary>Opens the dataset of several paths, directories or globs, read in the order given.</summary>
    /// <inheritdoc cref="Open(string, JsonDatasetOptions?)"/>
    public static JsonDataset Open(IEnumerable<string> pathsOrGlobs, JsonDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: false, default));

    /// <summary>Opens the dataset of files known beforehand (no listing): read in the order given.</summary>
    /// <inheritdoc cref="Open(string, JsonDatasetOptions?)"/>
    public static JsonDataset Open(IEnumerable<JsonDatasetFile> files, JsonDatasetOptions? options = null) =>
        DatasetFiles.Sync(OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: false, default));

    /// <inheritdoc cref="Open(string, JsonDatasetOptions?)"/>
    public static ValueTask<JsonDataset> OpenAsync(string pathOrGlob, JsonDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([pathOrGlob ?? throw new ArgumentNullException(nameof(pathOrGlob))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{string}, JsonDatasetOptions?)"/>
    public static ValueTask<JsonDataset> OpenAsync(IEnumerable<string> pathsOrGlobs, JsonDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. pathsOrGlobs ?? throw new ArgumentNullException(nameof(pathsOrGlobs))], options, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IEnumerable{JsonDatasetFile}, JsonDatasetOptions?)"/>
    public static ValueTask<JsonDataset> OpenAsync(IEnumerable<JsonDatasetFile> files, JsonDatasetOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync([.. files ?? throw new ArgumentNullException(nameof(files))], options, useAsync: true, cancellationToken);

    /// <summary>
    /// Opens a reader of the dataset's batches (see <see cref="JsonDatasetReader"/> for how the options apply: the
    /// dataset's options decide the layout and the columns).
    /// </summary>
    /// <exception cref="ArgumentException">The options give a schema, name columns the dataset lacks, or are out of range.</exception>
    public JsonDatasetReader OpenReader(JsonReadOptions? options = null) => new(this, options ?? new JsonReadOptions());

    /// <summary>
    /// Opens a reader of the dataset's rows as <typeparamref name="T"/>, of the columns its members map (columns from
    /// files and partition columns), filtered by <paramref name="where"/> (pushed into the read: conjuncts over
    /// partition members decide which files are read; see <see cref="JsonDatasetReader{T}"/>).
    /// </summary>
    /// <exception cref="ArgumentException">The dataset has no column for a member, or the options are out of range.</exception>
    /// <exception cref="NotSupportedException">A conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    public JsonDatasetReader<T> OpenReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where = null,
        JsonReadOptions? options = null) => new(this, where, options);

    /// <summary>Reads the rows of the dataset at a path, a directory or a glob as <typeparamref name="T"/> (see <see cref="JsonDatasetReader{T}"/>).</summary>
    /// <inheritdoc cref="OpenReader{T}(Expression{Func{T, bool}}?, JsonReadOptions?)"/>
    public static List<T> ReadList<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string pathOrGlob, Expression<Func<T, bool>>? where = null,
        JsonDatasetOptions? datasetOptions = null, JsonReadOptions? readOptions = null)
    {
        using JsonDataset dataset = Open(pathOrGlob, datasetOptions);
        using JsonDatasetReader<T> reader = dataset.OpenReader(where, readOptions);
        return reader.ReadList();
    }

    /// <inheritdoc cref="ReadList{T}(string, Expression{Func{T, bool}}?, JsonDatasetOptions?, JsonReadOptions?)"/>
    public static async Task<List<T>> ReadListAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string pathOrGlob,
        Expression<Func<T, bool>>? where = null, JsonDatasetOptions? datasetOptions = null, JsonReadOptions? readOptions = null,
        CancellationToken cancellationToken = default)
    {
        await using JsonDataset dataset = await OpenAsync(pathOrGlob, datasetOptions, cancellationToken).ConfigureAwait(false);
        await using JsonDatasetReader<T> reader = dataset.OpenReader(where, readOptions);
        return await reader.ReadListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The columns a filter reads: the columns from files (a partition column of the same name in its place), then the
    /// other partition columns, then the file path column.
    /// </summary>
    internal Schema FilterSchema()
    {
        List<Field> fields = [.. FileSchema.FieldsList.Select(f => Partitions?.IndexOf(f.Name) is int p and >= 0 ? PartitionField(p, views: true) : f)];
        for (int p = 0; p < (Partitions?.Fields.Length ?? 0); p++)
        {
            if (FileSchema.GetFieldIndex(Partitions!.Fields[p].Name) < 0) fields.Add(PartitionField(p, views: true));
        }
        if (Options.FilePathColumn is { } file && !fields.Any(f => f.Name == file)) fields.Add(new Field(file, StringViewType.Default, nullable: false));
        return new Schema(fields, null);
    }

    /// <summary>Nothing is held open between reads.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async ValueTask<JsonDataset> OpenCoreAsync(string[] inputs, JsonDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new JsonDatasetOptions();
        options.Validate();
        var (store, files, hiveStart) = await DatasetFiles.ExpandAsync(inputs, options, ct).ConfigureAwait(false);
        return await FromFilesAsync(store, options, [.. files.Select(f => new JsonDatasetFile(f.Path, f.Size))], hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<JsonDataset> OpenCoreAsync(JsonDatasetFile[] files, JsonDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new JsonDatasetOptions();
        options.Validate();
        var (store, hiveStart) = DatasetFiles.Given(files, options);
        return await FromFilesAsync(store, options, files, hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The dataset of the files: the columns given, or inferred (the first file holding records, or every file's first
    /// records, united in file order); partitions from the values given with the files, else from the paths after
    /// <paramref name="hiveStart"/> when set.
    /// </summary>
    private static async ValueTask<JsonDataset> FromFilesAsync(IFileStore store, JsonDatasetOptions options, JsonDatasetFile[] files, int? hiveStart,
        bool useAsync, CancellationToken ct)
    {
        Schema schema;
        bool inferred = options.Schema is null;
        if (options.Schema is { } declared) schema = Nullable(declared);
        else
        {
            using JsonShape root = options.SchemaMode == DatasetSchemaMode.UnionByName
                ? await UnitedAsync(store, options, files, ct).ConfigureAwait(false)
                : await FirstAsync(store, options, files, useAsync, ct).ConfigureAwait(false);
            if (root.Records == 0 && options.Format == JsonFormat.Lines)
                throw new JsonFormatException("The dataset's files hold no records to infer the columns from (give a schema).");
            schema = new Schema(root.Columns(options.ViewTypes), null);
            if (options.SchemaOverrides is { } overrides)
            {
                var unknown = overrides.Keys.Where(k => schema.GetFieldIndex(k) < 0).ToList();
                if (unknown.Count > 0) throw new ArgumentException($"The inferred columns have none named {string.Join(", ", unknown)}.", nameof(options));
                schema = new Schema([.. schema.FieldsList.Select(f => overrides.TryGetValue(f.Name, out IArrowType? t) ? new Field(f.Name, t, true) : f)], null);
            }
        }
        Partitions? partitions = DatasetFiles.PartitionsOf(files, options, hiveStart, schema);
        return new JsonDataset(store, options, files, schema, inferred, partitions);

        static Schema Nullable(Schema schema) =>
            new([.. schema.FieldsList.Select(f => f.IsNullable ? f : new Field(f.Name, f.DataType, nullable: true, f.Metadata))], schema.Metadata);
    }

    // The first records of the first file that holds any (a document of no element, or a file of no line, is passed over).
    private static async ValueTask<JsonShape> FirstAsync(IFileStore store, JsonDatasetOptions options, JsonDatasetFile[] files, bool useAsync, CancellationToken ct)
    {
        var root = new JsonShape("");
        try
        {
            for (int i = 0; i < files.Length && root.Records == 0; i++)
                await HeadAsync(root, store, options, files[i], useAsync, ct).ConfigureAwait(false);
            return root;
        }
        catch
        {
            root.Dispose();
            throw;
        }
    }

    // Every file's first records, taken in side by side, united in file order.
    private static async ValueTask<JsonShape> UnitedAsync(IFileStore store, JsonDatasetOptions options, JsonDatasetFile[] files, CancellationToken ct)
    {
        var heads = new JsonShape?[files.Length];
        try
        {
            await Parallel.ForEachAsync(Enumerable.Range(0, files.Length), new ParallelOptions { MaxDegreeOfParallelism = HeadsAtOnce, CancellationToken = ct },
                async (i, token) =>
                {
                    var head = new JsonShape("");
                    heads[i] = head;
                    await HeadAsync(head, store, options, files[i], useAsync: true, token).ConfigureAwait(false);
                }).ConfigureAwait(false);
            var root = new JsonShape("");
            foreach (JsonShape? head in heads) root.Merge(head!);
            return root;
        }
        finally
        {
            foreach (JsonShape? head in heads) head?.Dispose();
        }
    }

    // A file's first records taken into a shape; errors name the file.
    private static async ValueTask HeadAsync(JsonShape root, IFileStore store, JsonDatasetOptions options, JsonDatasetFile file, bool useAsync, CancellationToken ct)
    {
        IRandomAccess source = await OpenSourceAsync(store, file, ct).ConfigureAwait(false);
        try
        {
            using JsonInput input = await JsonReader.InputAsync(source, options.Format, map: true, JsonInput.DefaultUnitBytes, threads: 1, useAsync, ct).ConfigureAwait(false);
            await JsonReader.ObserveAsync(root, input, options.Format == JsonFormat.Document, options.InferSchemaLength, useAsync, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (Named(file.Path, e) is { } named)
        {
            throw named;
        }
        finally
        {
            await Close(source).ConfigureAwait(false);
        }
    }

    /// <summary>Opens a file of the store (its size from the listing when known); errors name the file.</summary>
    internal static async ValueTask<IRandomAccess> OpenSourceAsync(IFileStore store, JsonDatasetFile file, CancellationToken ct)
    {
        try
        {
            return file.Size is long size
                ? await store.OpenAsync(new StoredFile(file.Path, size), ct).ConfigureAwait(false)
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
        JsonFormatException f when !f.Message.StartsWith(path + ": ", StringComparison.Ordinal) => new JsonFormatException($"{path}: {f.Message}", f),
        JsonValueException v when !v.Message.StartsWith(path + ": ", StringComparison.Ordinal) => new JsonValueException($"{path}: {v.Message}"),
        IOException io when DatasetFiles.Unnamed(io, path) => DatasetFiles.InFile(path, io),
        _ => null,
    };
}
