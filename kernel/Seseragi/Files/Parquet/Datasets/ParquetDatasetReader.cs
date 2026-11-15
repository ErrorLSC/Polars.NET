using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Reads a dataset's files one after another, each as <see cref="ParquetFile.OpenReader(ParquetReadOptions?)"/> does
/// with the options given, into batches of one schema: batches never span files. The footers of the next files are
/// read while a file is read.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="ParquetReadOptions.Columns"/> and <see cref="ParquetReadOptions.ColumnPaths"/> name the dataset's
/// columns (all of the first file's when both are null); every file must have them, with the same types (the
/// nullability may differ); other columns of a file are not read. Batches hold them in the first file's order,
/// whatever the order in each file.</item>
/// <item><see cref="ParquetReadOptions.RowIndexColumn"/> numbers the rows across the files, in the order listed, as if
/// they were one (the numbers of each file follow the previous files' row counts, rows not read included): the
/// first column, as Polars puts it.</item>
/// <item><see cref="ParquetReadOptions.MaxRows"/> counts the rows delivered across the files.</item>
/// <item><see cref="ParquetReadOptions.Filter"/> prunes (and with <see cref="ParquetReadOptions.FilterRows"/>
/// filters) every file.</item>
/// <item><see cref="ParquetReadOptions.RowGroups"/> and <see cref="ParquetReadOptions.Rows"/> number per file: they
/// are refused (rows are selected per file by <see cref="ParquetDatasetFile.Rows"/>).</item>
/// </list>
/// Errors in a file name it.
/// </remarks>
public sealed class ParquetDatasetReader : IDisposable, IAsyncDisposable
{
    /// <summary>How many files ahead of the one read are opened (their footers read) at once.</summary>
    private const int FilesAhead = 16;

    private readonly ParquetDataset _dataset;
    private readonly ParquetReadOptions _options;
    private readonly string[] _roots;
    private readonly Field[] _fields;

    internal ParquetDatasetReader(ParquetDataset dataset, ParquetReadOptions options)
    {
        options.Validate();
        if (options.RowGroups is not null)
            throw new ArgumentException("Row groups are numbered per file: a dataset reads them all (select rows per file with ParquetDatasetFile.Rows).", nameof(options));
        if (options.Rows is not null)
            throw new ArgumentException("Rows are numbered per file: select them with ParquetDatasetFile.Rows.", nameof(options));
        _dataset = dataset;
        _options = options.Columns is null && options.ColumnPaths is null
            ? options with { Columns = [.. dataset.FileSchema.FieldsList.Select(f => f.Name)] }
            : options;
        _roots = [.. (_options.Columns ?? []).Concat((_options.ColumnPaths ?? []).Where(p => p is { Length: > 0 }).Select(p => p[0])).Distinct()];
        foreach (string root in _roots)
        {
            if (dataset.FileSchema.GetFieldIndex(root) < 0)
                throw new ArgumentException($"The dataset has no column '{root}' (the columns are the first file's).", nameof(options));
        }

        // The fields read, as the first file gives them.
        using ParquetFile first = ParquetDataset.Sync(OpenAsync(0, useAsync: false, default));
        using ParquetReader reader = Reader(first, 0, 0, null);
        FiltersExactly = reader.FiltersExactly;
        _fields = [.. reader.Schema.FieldsList.Skip(options.RowIndexColumn is null ? 0 : 1)];
        List<Field> fields = [.. _fields.Select(f => f.IsNullable ? f : new Field(f.Name, f.DataType, nullable: true, f.Metadata))];
        if (options.RowIndexColumn is { } index) fields.Insert(0, reader.Schema.FieldsList[0]);
        if (dataset.Options.FilePathColumn is { } path)
        {
            if (fields.Any(f => f.Name == path)) throw new ArgumentException($"The file path column '{path}' has the name of another column read.", nameof(options));
            fields.Add(new Field(path, options.ViewTypes ? StringViewType.Default : StringType.Default, nullable: false));
        }
        Schema = new Apache.Arrow.Schema(fields, reader.Schema.Metadata);
    }

    /// <summary>The schema of the batches: the row index if asked, the columns read (nullable), the file path if asked.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// Whether, with <see cref="ParquetReadOptions.FilterRows"/>, the batches hold exactly the rows the filter keeps
    /// (as the first file's reader tells: the files share their columns' types).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>Reads the batches, file after file.</summary>
    /// <exception cref="ParquetFormatException">A file is damaged, is not a Parquet file, or lacks a column read (the message names it).</exception>
    /// <exception cref="FileNotFoundException">A file named does not exist.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        using var ahead = new Ahead(this, default);
        long? remaining = _options.MaxRows;
        long before = 0;
        for (int i = 0; i < _dataset.Files.Count && remaining != 0; i++)
        {
            ParquetFile file = ahead.Take(i).GetAwaiter().GetResult();
            try
            {
                using ParquetReader reader = Reader(file, i, before, remaining);
                using IEnumerator<RecordBatch> batches = reader.ReadBatches().GetEnumerator();
                int[] order = Order(reader.Schema, i);
                while (Next(batches, i) is { } batch)
                {
                    remaining -= batch.Length;
                    yield return Complete(batch, order, i);
                }
                before += RowCount(file);
            }
            finally
            {
                file.Dispose();
            }
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var ahead = new Ahead(this, cancellationToken);
        long? remaining = _options.MaxRows;
        long before = 0;
        for (int i = 0; i < _dataset.Files.Count && remaining != 0; i++)
        {
            ParquetFile file = await ahead.Take(i).ConfigureAwait(false);
            try
            {
                await using ParquetReader reader = Reader(file, i, before, remaining);
                await using IAsyncEnumerator<RecordBatch> batches = reader.ReadBatchesAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
                int[] order = Order(reader.Schema, i);
                while (await NextAsync(batches, i).ConfigureAwait(false) is { } batch)
                {
                    remaining -= batch.Length;
                    yield return Complete(batch, order, i);
                }
                before += RowCount(file);
            }
            finally
            {
                await file.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Reads the batches as an Arrow stream (each batch disposed when the next is read).</summary>
    public Apache.Arrow.Ipc.IArrowArrayStream ReadArrowStream() => new ParquetReader.OwningBatchStream(Schema, ReadBatches());

    /// <summary>Nothing is held between reads.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private string PathOf(int file) => _dataset.Files[file].Path;

    /// <summary>Opens a file (its footer from before when there is one, kept for the next reads otherwise).</summary>
    private async ValueTask<ParquetFile> OpenAsync(int index, bool useAsync, CancellationToken ct)
    {
        ParquetFile file = await ParquetDataset.OpenFileAsync(_dataset.Store, _dataset.Files[index], _dataset.Footer(index), useAsync, ct).ConfigureAwait(false);
        _dataset.KeepFooter(index, file.Metadata);
        return file;
    }

    /// <summary>The reader of a file: its rows, numbered after <paramref name="before"/> rows, up to <paramref name="remaining"/>.</summary>
    private ParquetReader Reader(ParquetFile file, int index, long before, long? remaining)
    {
        Apache.Arrow.Schema schema;
        try
        {
            schema = file.Schema;
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
        foreach (string root in _roots)
        {
            if (schema.GetFieldIndex(root) < 0)
                throw new ParquetFormatException($"{PathOf(index)}: the file has no column '{root}', which the dataset reads (the first file's columns).");
        }
        try
        {
            return file.OpenReader(_options with { Rows = _dataset.Files[index].Rows, RowIndexOffset = before, MaxRows = remaining });
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>Where each field of the dataset is among a file's columns (files may order them differently); the types must agree.</summary>
    private int[] Order(Apache.Arrow.Schema read, int index)
    {
        int skip = _options.RowIndexColumn is null ? 0 : 1;
        var order = new int[_fields.Length];
        for (int f = 0; f < _fields.Length; f++)
        {
            int at = read.GetFieldIndex(_fields[f].Name);
            IArrowType type = read.FieldsList[at].DataType;
            if (!ArrowTypes.SameType(type, _fields[f].DataType))
                throw new ParquetFormatException($"{PathOf(index)}: column '{_fields[f].Name}' is {Describe(type)} in this file but {Describe(_fields[f].DataType)} "
                    + "in the first file.");
            order[f] = at;
        }
        return skip == 0 ? order : [0, .. order];

        static string Describe(IArrowType type) => type is DictionaryType d ? $"a dictionary of {d.ValueType.Name}" : type.Name;
    }

    /// <summary>A file's batch in the dataset's schema: columns in its order, the file path appended.</summary>
    private RecordBatch Complete(RecordBatch batch, int[] order, int index)
    {
        IArrowArray[] columns = [.. order.Select(batch.Column)];
        if (_dataset.Options.FilePathColumn is not null) columns = [.. columns, ConstantArrays.String(PathOf(index), batch.Length, _options.ViewTypes)];
        return new RecordBatch(Schema, columns, batch.Length);
    }

    /// <summary>A file's row count as its reader numbers rows (the row groups' counts).</summary>
    private static long RowCount(ParquetFile file) => file.Thrift.RowGroups.Sum(g => Math.Max(0, g.NumRows));

    private RecordBatch? Next(IEnumerator<RecordBatch> batches, int index)
    {
        try
        {
            return batches.MoveNext() ? batches.Current : null;
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    private async ValueTask<RecordBatch?> NextAsync(IAsyncEnumerator<RecordBatch> batches, int index)
    {
        try
        {
            return await batches.MoveNextAsync().ConfigureAwait(false) ? batches.Current : null;
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>Files opened ahead of the one read (up to <see cref="FilesAhead"/>); those not taken are disposed once opened.</summary>
    private sealed class Ahead(ParquetDatasetReader owner, CancellationToken ct) : IDisposable
    {
        private readonly Dictionary<int, Task<ParquetFile>> _started = [];
        private int _next;

        public Task<ParquetFile> Take(int index)
        {
            int last = Math.Min(index + FilesAhead, owner._dataset.Files.Count);
            for (; _next < last; _next++)
            {
                int k = _next;
                _started[k] = Task.Run(() => owner.OpenAsync(k, useAsync: true, ct).AsTask(), ct);
            }
            _started.Remove(index, out Task<ParquetFile>? task);
            return task!;
        }

        public void Dispose()
        {
            foreach (Task<ParquetFile> task in _started.Values)
                task.ContinueWith(static t =>
                {
                    if (t.IsCompletedSuccessfully) t.Result.Dispose();
                    else _ = t.Exception; // observed: a file not read need not open
                }, CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            _started.Clear();
        }
    }
}
