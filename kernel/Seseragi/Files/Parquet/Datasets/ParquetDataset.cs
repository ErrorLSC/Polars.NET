using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Compute;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Parquet files read as one table: files named, directories (every file below them), globs (<c>*</c>, <c>?</c>,
/// <c>[...]</c>, <c>**</c> as a whole segment), or a list of files known beforehand, from the local file system or a
/// store (<see cref="DatasetOptions.Store"/>). The columns are the first file's, every file's united
/// (<see cref="DatasetSchemaMode"/>), or given (<see cref="DatasetOptions.Schema"/>); files' columns convert
/// to the dataset's types by lossless widenings (<see cref="ParquetDatasetReader"/>), columns they have beyond them
/// are not read.
/// </summary>
/// <remarks>
/// Paths are expanded as dataframe engines expand them: each glob's or directory's files in path order, inputs in the order
/// given; directories and empty files are left out of listings; files of a directory given alone must share their
/// extension; hidden files and files starting with '_' are read unless <see cref="DatasetOptions.IgnorePrefixes"/>
/// says otherwise. A dataset's columns are nullable: its files may declare them differently.
/// </remarks>
public sealed class ParquetDataset : IDisposable, IAsyncDisposable
{
    private static readonly ParquetReadOptions DefaultRead = new();

    /// <summary>How many footers are read at once (a dataset united by name reads every one when it opens).</summary>
    internal const int FootersAtOnce = 16;

    /// <summary>The first file's footer, when the dataset's columns are that file's (kept: the schema came from it).</summary>
    private readonly ParquetMetadata? _first;

    /// <summary>How the files are read by field id (<see cref="ParquetFieldMatching.ByFieldId"/>); null by name.</summary>
    internal FileProjection? Projection { get; }

    private ParquetDataset(IFileStore store, ParquetDatasetOptions options, ParquetDatasetFile[] files, KeptFooters kept, FooterInterner interner,
        ParquetMetadata? first, Apache.Arrow.Schema fileSchema, Partitions? partitions)
    {
        Store = store;
        Options = options;
        Files = files;
        Footers = kept;
        Interner = interner;
        _first = first;
        // One dictionary per column for the dataset: read with Int32 indices, delivered with the given schema's (or Int32).
        if (options.UnifyDictionaries) fileSchema = DictionaryUnifier.Unified(fileSchema);
        FileSchema = fileSchema;
        if (options.UnifyDictionaries && options.Schema is { } declared) fileSchema = DictionaryUnifier.Declared(fileSchema, declared);
        Partitions = partitions;
        if (options.FieldMatching == ParquetFieldMatching.ByFieldId)
            Projection = new FieldIdProjection(options.Schema!, options.NameMapping, name => partitions?.IndexOf(name) >= 0 || name == options.FilePathColumn);
        else if (options.CaseInsensitiveNames)
            Projection = new NameProjection(FileSchema, name => partitions?.IndexOf(name) >= 0 || name == options.FilePathColumn);
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
        if (options.FileRowIndexColumn is { } position)
        {
            if (fields.Any(f => f.Name == position)) throw new ArgumentException($"The file row index column '{position}' has the name of a column of the files.", nameof(options));
            fields.Add(new Field(position, Int64Type.Default, nullable: false));
        }
        Schema = new Apache.Arrow.Schema(fields, fileSchema.Metadata);
        DatasetFiles.CheckNames(options, Schema, message => new ArgumentException(message, nameof(options)));
        PartitionSchema = partitions is null ? null : new Apache.Arrow.Schema([.. Enumerable.Range(0, partitions.Fields.Length).Select(p => PartitionField(p, views: true))], null);
    }

    /// <summary>The partition values of the files (one row per file), when the dataset is partitioned.</summary>
    internal Partitions? Partitions { get; }

    /// <summary>
    /// The partition columns, in the order of the keys, as read with default options: the type given in
    /// <see cref="DatasetOptions.PartitionSchema"/>, else the type inferred (strings in the view layout unless
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

    /// <summary>The dataset's columns from files (the first file's, united or given), read with default options.</summary>
    internal Apache.Arrow.Schema FileSchema { get; }

    /// <summary>
    /// The schema read with default options: the columns from files (the first file's, every file's united, or the
    /// given ones; nullable), then the partition columns the files do not have, then the file path column if asked
    /// (strings in the view layout; <see cref="ParquetReadOptions.ViewTypes"/> off makes them plain strings).
    /// </summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// The dataset's columns from files as a reader with <paramref name="options"/> reads them (their layouts and
    /// types follow the options): the given schema, the first file's, or every file's united.
    /// </summary>
    /// <exception cref="ParquetFormatException">A file's schema is damaged, or (united) no type holds a column's types in two files.</exception>
    internal Apache.Arrow.Schema ColumnsFor(ParquetReadOptions options)
    {
        Apache.Arrow.Schema columns = ColumnsOf(options);
        return Options.UnifyDictionaries ? DictionaryUnifier.Unified(columns) : columns;
    }

    private Apache.Arrow.Schema ColumnsOf(ParquetReadOptions options)
    {
        if (Options.Schema is { } given) return Layout(given, options.ViewTypes);
        if (Options.SchemaMode == DatasetSchemaMode.UnionByName)
        {
            // United once with default options when the dataset opened; again only for other layouts.
            if (options.ViewTypes == DefaultRead.ViewTypes && options.BinaryAsString == DefaultRead.BinaryAsString && options.Int96Unit == DefaultRead.Int96Unit
                && options.Int96Timezone == DefaultRead.Int96Timezone)
                return FileSchema;
            var footers = new ParquetMetadata?[Files.Count];
            for (int i = 0; i < footers.Length; i++) footers[i] = Footer(i);
            ReadFootersAsync(Store, [.. Files], footers, Footers, Interner, Options.Decryption, default).AsTask().GetAwaiter().GetResult(); // those not kept
            return Union(Files, footers!, options, Options);
        }
        return SchemaOf(Files[0].Path, Footer(0)!, options);
    }

    /// <summary>
    /// The columns a filter reads, as a reader with <paramref name="options"/> filters them: the columns from files (a
    /// partition column of the same name in its place), then the other partition columns, then the file path column.
    /// </summary>
    internal Apache.Arrow.Schema FilterSchema(ParquetReadOptions options)
    {
        Apache.Arrow.Schema columns = ColumnsFor(options);
        List<Field> fields = [.. columns.FieldsList.Select(f => Partitions?.IndexOf(f.Name) is int p and >= 0 ? PartitionField(p, options.ViewTypes) : f)];
        for (int p = 0; p < (Partitions?.Fields.Length ?? 0); p++)
        {
            if (columns.GetFieldIndex(Partitions!.Fields[p].Name) < 0) fields.Add(PartitionField(p, options.ViewTypes));
        }
        if (Options.FilePathColumn is { } file && !fields.Any(f => f.Name == file))
            fields.Add(new Field(file, options.ViewTypes ? StringViewType.Default : StringType.Default, nullable: false));
        if (Options.FileRowIndexColumn is { } position && !fields.Any(f => f.Name == position)) fields.Add(new Field(position, Int64Type.Default, nullable: false));
        return new Apache.Arrow.Schema(fields, null);
    }

    /// <summary>A file's schema as a reader with <paramref name="options"/> reads it.</summary>
    internal static Apache.Arrow.Schema SchemaOf(string path, ParquetMetadata footer, ParquetReadOptions options)
    {
        try
        {
            return SchemaShape.Of(footer.Thrift!).Mapping(options).Schema;
        }
        catch (ParquetFormatException e)
        {
            throw InFile(path, e);
        }
    }

    /// <summary>The files' columns united by name: first appearance order, each of the narrowest type holding the files'.</summary>
    private static Apache.Arrow.Schema Union(IReadOnlyList<ParquetDatasetFile> files, ParquetMetadata[] footers, ParquetReadOptions options, ParquetDatasetOptions dataset) =>
        DatasetFiles.Union([.. files.Select((f, i) => (f.Path, SchemaOf(f.Path, footers[i], options)))], dataset, message => new ParquetFormatException(message));

    /// <summary>
    /// A schema's strings and binaries (nested ones too) in the layout a reader returns: views, or plain offsets
    /// (dictionaries' values always plain).
    /// </summary>
    internal static Apache.Arrow.Schema Layout(Apache.Arrow.Schema schema, bool views) =>
        new([.. schema.FieldsList.Select(f => Retyped(f, Layout(f.DataType, views)))], schema.Metadata);

    private static IArrowType Layout(IArrowType type, bool views) => type switch
    {
        StringType or LargeStringType or StringViewType => views ? StringViewType.Default : StringType.Default,
        BinaryType or LargeBinaryType or BinaryViewType => views ? BinaryViewType.Default : BinaryType.Default,
        DictionaryType d => new DictionaryType(d.IndexType, Layout(d.ValueType, views: false), d.Ordered), // a reader's dictionaries hold plain values
        StructType s => new StructType([.. s.Fields.Select(f => Retyped(f, Layout(f.DataType, views)))]),
        ListType l => new ListType(Retyped(l.ValueField, Layout(l.ValueDataType, views))),
        LargeListType l => new ListType(Retyped(l.ValueField, Layout(l.ValueDataType, views))),
        FixedSizeListType l => new FixedSizeListType(Retyped(l.ValueField, Layout(l.ValueDataType, views)), l.ListSize),
        MapType m => new MapType(Retyped(m.KeyField, Layout(m.KeyField.DataType, views)), Retyped(m.ValueField, Layout(m.ValueField.DataType, views)), m.KeySorted),
        _ => type,
    };

    private static Field Retyped(Field field, IArrowType type) => new(field.Name, type, field.IsNullable, field.Metadata);

    /// <summary>The files, in the order read.</summary>
    public IReadOnlyList<ParquetDatasetFile> Files { get; }

    /// <summary>The footer of file <paramref name="index"/>, when given, kept (see <see cref="KeptFooters"/>), or the schema's.</summary>
    internal ParquetMetadata? Footer(int index) =>
        Files[index].Metadata ?? (index == 0 ? _first : null) ?? (Footers.Get(index) is { } kept ? ParquetMetadata.ParseFooter(kept.Span, Interner) : null);

    /// <summary>The footers kept between reads.</summary>
    internal KeptFooters Footers { get; }

    /// <summary>What the files' footers share (their schemas), read once.</summary>
    internal FooterInterner Interner { get; }

    /// <summary>Opens the dataset at a path, a directory or a glob.</summary>
    /// <exception cref="FileNotFoundException">No file is found.</exception>
    /// <exception cref="ParquetFormatException">
    /// The first file (every file, united by name) is not a Parquet file, or (united) no type holds a column's types in two files.
    /// </exception>
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
    /// <exception cref="ArgumentException">The options select rows or row groups by number (per file: <see cref="ParquetDatasetFile.Rows"/>), name columns the dataset lacks, or are out of range.</exception>
    public ParquetDatasetReader OpenReader(ParquetReadOptions? options = null) => new(this, options ?? new ParquetReadOptions());

    /// <summary>
    /// Opens a reader of the dataset's rows as <typeparamref name="T"/>, of the columns its members map (columns from
    /// files and partition columns), filtered by <paramref name="where"/> (pushed into the read: conjuncts over
    /// partition members decide which files are read; see <see cref="ParquetDatasetReader{T}"/>).
    /// </summary>
    /// <exception cref="ArgumentException">The dataset has no column for a member, or the options are out of range.</exception>
    /// <exception cref="NotSupportedException">A conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    public ParquetDatasetReader<T> OpenReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where = null,
        ParquetReadOptions? options = null) => new(this, where, options);

    /// <summary>Reads the rows of the dataset at a path, a directory or a glob as <typeparamref name="T"/> (see <see cref="ParquetDatasetReader{T}"/>).</summary>
    /// <inheritdoc cref="OpenReader{T}(Expression{Func{T, bool}}?, ParquetReadOptions?)"/>
    public static List<T> ReadList<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string pathOrGlob, Expression<Func<T, bool>>? where = null,
        ParquetDatasetOptions? datasetOptions = null, ParquetReadOptions? readOptions = null)
    {
        using ParquetDataset dataset = Open(pathOrGlob, datasetOptions);
        using ParquetDatasetReader<T> reader = dataset.OpenReader(where, readOptions);
        return reader.ReadList();
    }

    /// <inheritdoc cref="ReadList{T}(string, Expression{Func{T, bool}}?, ParquetDatasetOptions?, ParquetReadOptions?)"/>
    public static async Task<List<T>> ReadListAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string pathOrGlob,
        Expression<Func<T, bool>>? where = null, ParquetDatasetOptions? datasetOptions = null, ParquetReadOptions? readOptions = null,
        CancellationToken cancellationToken = default)
    {
        await using ParquetDataset dataset = await OpenAsync(pathOrGlob, datasetOptions, cancellationToken).ConfigureAwait(false);
        await using ParquetDatasetReader<T> reader = dataset.OpenReader(where, readOptions);
        return await reader.ReadListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Nothing is held open between reads; kept for stores that will hold connections.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    internal static T Sync<T>(ValueTask<T> task) => DatasetFiles.Sync(task);

    private static async ValueTask<ParquetDataset> OpenCoreAsync(string[] inputs, ParquetDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new ParquetDatasetOptions();
        options.Validate();
        var (store, files, hiveStart) = await DatasetFiles.ExpandAsync(inputs, options, ct).ConfigureAwait(false);
        return await FromFilesAsync(store, options, [.. files.Select(f => new ParquetDatasetFile(f.Path, f.Size))], hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<ParquetDataset> OpenCoreAsync(ParquetDatasetFile[] files, ParquetDatasetOptions? options, bool useAsync, CancellationToken ct)
    {
        options ??= new ParquetDatasetOptions();
        options.Validate();
        var (store, hiveStart) = DatasetFiles.Given(files, options);
        return await FromFilesAsync(store, options, files, hiveStart, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The dataset of the files: the first one's footer is read for the schema; partitions from the values given with
    /// the files, else from the paths after <paramref name="hiveStart"/> when it is set.
    /// </summary>
    private static async ValueTask<ParquetDataset> FromFilesAsync(IFileStore store, ParquetDatasetOptions options, ParquetDatasetFile[] files, int? hiveStart,
        bool useAsync, CancellationToken ct)
    {
        var kept = new KeptFooters(files.Length);
        var interner = new FooterInterner();
        ParquetMetadata? firstFooter = null;
        Apache.Arrow.Schema schema;
        if (options.Schema is { } declared) schema = Layout(declared, views: true); // no file read
        else if (options.SchemaMode == DatasetSchemaMode.UnionByName)
        {
            var footers = new ParquetMetadata?[files.Length];
            await ReadFootersAsync(store, files, footers, kept, interner, options.Decryption, ct).ConfigureAwait(false);
            schema = Union(files, footers!, new ParquetReadOptions(), options);
        }
        else
        {
            // Kept as the other files' footers are (with its version, when worth it): reading it opens it without a request.
            await using ParquetFile first = await OpenFileAsync(store, files[0], files[0].Metadata, options.Decryption, useAsync, ct, kept, 0, interner).ConfigureAwait(false);
            firstFooter = first.Metadata;
            schema = SchemaOf(files[0].Path, first.Metadata, new ParquetReadOptions());
        }
        Partitions? partitions = DatasetFiles.PartitionsOf(files, options, hiveStart, schema);
        DatasetShape.CheckDefaults(options, partitions, schema, files);
        return new ParquetDataset(store, options, files, kept, interner, firstFooter, schema, partitions);
    }

    /// <summary>
    /// Reads the footers of the files (those not given or already there; of those <paramref name="needed"/> marks when
    /// given), <see cref="FootersAtOnce"/> at a time.
    /// </summary>
    internal static async ValueTask ReadFootersAsync(IFileStore store, ParquetDatasetFile[] files, ParquetMetadata?[] footers, KeptFooters kept, FooterInterner interner,
        ParquetDecryptionOptions? decryption, CancellationToken ct, IReadOnlyList<bool>? needed = null)
    {
        await Parallel.ForEachAsync(Enumerable.Range(0, files.Length), new ParallelOptions { MaxDegreeOfParallelism = FootersAtOnce, CancellationToken = ct },
            async (i, token) =>
            {
                if (footers[i] is not null || needed?[i] == false) return;
                if (files[i].Metadata is { } known)
                {
                    footers[i] = known;
                    return;
                }
                await using ParquetFile file = await OpenFileAsync(store, files[i], null, decryption, useAsync: true, token, kept, i, interner).ConfigureAwait(false);
                footers[i] = file.Metadata;
            }).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a file of the store (owned by the <see cref="ParquetFile"/>); errors name the file. A footer read is kept
    /// in <paramref name="kept"/> as file <paramref name="index"/> when that is worth it (see <see cref="KeptFooters"/>),
    /// and read sharing with <paramref name="interner"/>.
    /// </summary>
    internal static async ValueTask<ParquetFile> OpenFileAsync(IFileStore store, ParquetDatasetFile file, ParquetMetadata? footer, ParquetDecryptionOptions? decryption,
        bool useAsync, CancellationToken ct,
        KeptFooters? kept = null, int index = 0, FooterInterner? interner = null)
    {
        // A kept footer comes with the file's size and version: opened without a request, read at that version.
        StoredFile? known = footer is not null && kept?.Version(index) is { } version ? version with { Path = file.Path }
            : file.Size is long size ? new StoredFile(file.Path, size) : null;
        IRandomAccess source;
        try
        {
            source = known is { } stored
                ? await store.OpenAsync(stored, ct).ConfigureAwait(false)
                : await store.OpenAsync(file.Path, ct).ConfigureAwait(false) ?? throw new FileNotFoundException($"'{file.Path}' does not exist.", file.Path);
        }
        catch (IOException e) when (Unnamed(e, file.Path))
        {
            throw InFile(file.Path, e);
        }
        FooterRead? keep = footer is null && kept is not null && KeptFooters.Worth(source, ParquetMetadata.TailGuess) ? bytes => kept.Keep(index, bytes, (source as IStoredSource)?.Stored) : null;
        try
        {
            return await ParquetFile.OpenOwnedAsync(source, footer, decryption, useAsync, ct, keep, interner).ConfigureAwait(false);
        }
        catch (ParquetFormatException e)
        {
            throw InFile(file.Path, e);
        }
        catch (IOException e) when (Unnamed(e, file.Path))
        {
            throw InFile(file.Path, e);
        }
    }

    /// <summary>The error with the file it is about.</summary>
    internal static ParquetFormatException InFile(string path, ParquetFormatException e) => new($"{path}: {e.Message}", e);

    /// <summary>A store's failure (a network's, a disk's) with the file it is about.</summary>
    internal static IOException InFile(string path, IOException e) => DatasetFiles.InFile(path, e);

    /// <summary>Whether a store's failure does not name the file yet (a missing file's does).</summary>
    internal static bool Unnamed(IOException e, string path) => DatasetFiles.Unnamed(e, path);
}
