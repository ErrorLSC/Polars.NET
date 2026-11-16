using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Filters;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Microsoft.FSharp.Core;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Reads a dataset's files one after another, each as <see cref="ParquetFile.OpenReader(ParquetReadOptions?)"/> does
/// with the options given, into batches of one schema: batches never span files. The footers of the next files are
/// read while a file is read.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="ParquetReadOptions.Columns"/> and <see cref="ParquetReadOptions.ColumnPaths"/> name the dataset's
/// columns (all of them when both are null): the files' columns (every file must have those read, with the same types;
/// the nullability may differ) and partition columns. Batches hold the files' columns in the first file's order
/// (a partition column of the same name in its place, holding the path's value), then the other partition columns,
/// then the file path.</item>
/// <item><see cref="ParquetReadOptions.Filter"/>: its conjuncts over partition columns only decide which files are
/// read (exactly, on the files' partition values); the others prune (and with <see cref="ParquetReadOptions.FilterRows"/>
/// filter) every file read, partition columns replaced by the file's values.</item>
/// <item><see cref="ParquetReadOptions.RowIndexColumn"/> numbers the rows across the files, in the order listed, as if
/// they were one (the numbers of each file follow the previous files' row counts, rows of files left out included):
/// the first column.</item>
/// <item><see cref="ParquetReadOptions.MaxRows"/> counts the rows delivered across the files.</item>
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
    private readonly Column[] _layout;
    private readonly bool[] _read;
    private readonly ScalarExpr? _fileFilter;
    private readonly ScalarExpr[] _mixed;

    /// <summary>A column of the batches after the row index: a file column by name, or a partition column by index.</summary>
    private readonly record struct Column(Field Field, int Partition);

    internal ParquetDatasetReader(ParquetDataset dataset, ParquetReadOptions options)
    {
        options.Validate();
        if (options.RowGroups is not null)
            throw new ArgumentException("Row groups are numbered per file: a dataset reads them all (select rows per file with ParquetDatasetFile.Rows).", nameof(options));
        if (options.Rows is not null)
            throw new ArgumentException("Rows are numbered per file: select them with ParquetDatasetFile.Rows.", nameof(options));
        _dataset = dataset;
        Partitions? partitions = dataset.Partitions;
        bool IsPartition(string name) => partitions?.IndexOf(name) >= 0;

        // The columns selected: of the files (not shadowed by a partition column), and partition columns.
        bool all = options.Columns is null && options.ColumnPaths is null;
        foreach (string name in options.Columns ?? [])
        {
            if (!IsPartition(name) && dataset.FileSchema.GetFieldIndex(name) < 0)
                throw new ArgumentException($"The dataset has no column '{name}' (the columns are the first file's and the partitions').", nameof(options));
        }
        foreach (string[] selected in options.ColumnPaths ?? [])
        {
            if (selected is { Length: > 0 } && IsPartition(selected[0]))
                throw new ArgumentException($"'{selected[0]}' is a partition column: it has no fields to select.", nameof(options));
        }
        string[] fileColumns = all
            ? [.. dataset.FileSchema.FieldsList.Select(f => f.Name).Where(n => !IsPartition(n))]
            : [.. (options.Columns ?? []).Where(n => !IsPartition(n))];
        bool[] partitionsRead = [.. Enumerable.Range(0, partitions?.Fields.Length ?? 0).Select(p => all || options.Columns?.Contains(partitions!.Fields[p].Name) == true)];
        _options = options.Columns is null && options.ColumnPaths is not null ? options : options with { Columns = fileColumns };
        _roots = [.. (_options.Columns ?? []).Concat((_options.ColumnPaths ?? []).Where(p => p is { Length: > 0 }).Select(p => p[0])).Distinct()];
        foreach (string root in _roots)
        {
            if (dataset.FileSchema.GetFieldIndex(root) < 0)
                throw new ArgumentException($"The dataset has no column '{root}' (the columns are the first file's).", nameof(options));
        }

        // The filter: conjuncts over partition columns only decide which files are read; the others go to the files,
        // those over both with the partition columns replaced by each file's values.
        _read = [.. Enumerable.Repeat(true, dataset.Files.Count)];
        bool exact = true;
        List<ScalarExpr> onFiles = [], mixed = [];
        if (options.Filter is { } filter)
        {
            List<ScalarExpr> onPartitions = [];
            foreach (ScalarExpr conjunct in FilterShapes.conjuncts(filter))
            {
                string[] columns = [.. ScalarExprOps.collectColumns(conjunct)];
                int inPartitions = columns.Count(IsPartition);
                (inPartitions == 0 ? onFiles : inPartitions == columns.Length ? onPartitions : mixed).Add(conjunct);
            }
            if (onPartitions.Count > 0) exact = Prune(partitions!, onPartitions.Aggregate(And), _read);
        }
        _fileFilter = onFiles.Count == 0 ? null : onFiles.Aggregate(And);
        _mixed = [.. mixed];

        // The fields read, as the first file gives them.
        using ParquetFile first = ParquetDataset.Sync(OpenAsync(0, useAsync: false, default));
        using ParquetReader reader = Reader(first, 0, 0, null);
        FiltersExactly = reader.FiltersExactly && exact;
        if (FiltersExactly && _mixed.Length > 0)
        {
            // Each file reads its own filter (its partition values put in): exact only if every file's is, lowered
            // over the columns as the first file maps them (the files share their columns' types).
            var (_, mapped) = ArrowMapping.Map(first.Tree, first.Thrift, _options);
            var columns = new Apache.Arrow.Schema([.. mapped.Select(c => c.Field)], null);
            for (int i = 0; i < _read.Length && FiltersExactly; i++)
            {
                if (!_read[i]) continue;
                var (fileFilter, complete) = FileFilter(i);
                FiltersExactly = complete && (fileFilter is null || FilterLowering.lower(columns, fileFilter).Exact);
            }
        }
        Field[] read = [.. reader.Schema.FieldsList.Skip(options.RowIndexColumn is null ? 0 : 1)];
        List<Column> layout = [];
        foreach (Field field in dataset.FileSchema.FieldsList)
        {
            int p = partitions?.IndexOf(field.Name) ?? -1;
            if (p >= 0)
            {
                if (partitionsRead[p]) layout.Add(new Column(dataset.PartitionField(p, options.ViewTypes), p));
            }
            else if (read.FirstOrDefault(f => f.Name == field.Name) is { } got)
                layout.Add(new Column(got.IsNullable ? got : new Field(got.Name, got.DataType, nullable: true, got.Metadata), -1));
        }
        for (int p = 0; p < partitionsRead.Length; p++)
        {
            if (partitionsRead[p] && dataset.FileSchema.GetFieldIndex(partitions!.Fields[p].Name) < 0)
                layout.Add(new Column(dataset.PartitionField(p, options.ViewTypes), p));
        }
        _layout = [.. layout];
        List<Field> fields = [.. _layout.Select(c => c.Field)];
        if (options.RowIndexColumn is { } index)
        {
            if (fields.Any(f => f.Name == index)) throw new ArgumentException($"The row index column '{index}' has the name of another column read.", nameof(options));
            fields.Insert(0, reader.Schema.FieldsList[0]);
        }
        if (dataset.Options.FilePathColumn is { } path)
        {
            if (fields.Any(f => f.Name == path)) throw new ArgumentException($"The file path column '{path}' has the name of another column read.", nameof(options));
            fields.Add(new Field(path, options.ViewTypes ? StringViewType.Default : StringType.Default, nullable: false));
        }
        Schema = new Apache.Arrow.Schema(fields, reader.Schema.Metadata);

        static ScalarExpr And(ScalarExpr a, ScalarExpr b) => ScalarExpr.NewBinary(BinaryOp.And, a, b);
    }

    /// <summary>The schema of the batches: the row index if asked, the columns read (nullable), the file path if asked.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// Whether, with <see cref="ParquetReadOptions.FilterRows"/>, the batches hold exactly the rows the filter keeps
    /// (as the first file's reader tells, the files sharing their columns' types, and the conjuncts over partition
    /// columns decided exactly).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The files a filter over partition columns leaves (for tests).</summary>
    internal IReadOnlyList<bool> FilesRead => _read;

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
            if (!_read[i] && _options.RowIndexColumn is null) continue;
            ParquetFile file = ahead.Take(i).GetAwaiter().GetResult();
            try
            {
                if (_read[i])
                {
                    using ParquetReader reader = Reader(file, i, before, remaining);
                    using IEnumerator<RecordBatch> batches = reader.ReadBatches().GetEnumerator();
                    int[] order = Order(reader.Schema, i);
                    while (Next(batches, i) is { } batch)
                    {
                        remaining -= batch.Length;
                        yield return Complete(batch, order, i);
                    }
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
            if (!_read[i] && _options.RowIndexColumn is null) continue;
            ParquetFile file = await ahead.Take(i).ConfigureAwait(false);
            try
            {
                if (_read[i])
                {
                    await using ParquetReader reader = Reader(file, i, before, remaining);
                    await using IAsyncEnumerator<RecordBatch> batches = reader.ReadBatchesAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
                    int[] order = Order(reader.Schema, i);
                    while (await NextAsync(batches, i).ConfigureAwait(false) is { } batch)
                    {
                        remaining -= batch.Length;
                        yield return Complete(batch, order, i);
                    }
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

    /// <summary>
    /// Clears <paramref name="read"/> for the files whose partition values the filter rules out (false or null), as
    /// the IR's semantics evaluate it; whether every conjunct could be decided (those that cannot keep every file).
    /// </summary>
    private static bool Prune(Partitions partitions, ScalarExpr filter, bool[] read)
    {
        var schema = new Apache.Arrow.Schema(partitions.Fields, null);
        LoweredFilter lowered = FilterLowering.lower(schema, filter);
        if (OptionModule.IsNone(lowered.Program)) return lowered.Exact;
        FilterProgram program = lowered.Program.Value;
        IArrowArray[] columns = [.. program.Columns.Select(c => partitions.Columns[partitions.IndexOf(c.Path[0])])];
        var selected = new ulong[(read.Length + 63) / 64];
        program.Evaluate(columns, read.Length, selected);
        for (int i = 0; i < read.Length; i++) read[i] = (selected[i / 64] >> (i % 64) & 1) != 0;
        return lowered.Exact;
    }

    /// <summary>
    /// The filter a file reads: the conjuncts over its columns, those over partition columns too with its values;
    /// whether every conjunct could take the file's values (one with a null value or a value of no literal form is
    /// left out: the file's rows are then filtered less).
    /// </summary>
    private (ScalarExpr? Filter, bool Complete) FileFilter(int index)
    {
        if (_mixed.Length == 0) return (_fileFilter, true);
        Partitions partitions = _dataset.Partitions!;
        ScalarExpr? filter = _fileFilter;
        bool all = true;
        foreach (ScalarExpr conjunct in _mixed)
        {
            bool complete = true;
            ScalarExpr substituted = ScalarExprOps.substitute(FuncConvert.FromFunc((string name) =>
            {
                int p = partitions.IndexOf(name);
                if (p < 0) return FSharpOption<ScalarExpr>.None;
                if (Literal(partitions.Columns[p], index) is not { } literal)
                {
                    complete = false;
                    return FSharpOption<ScalarExpr>.None;
                }
                return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewLiteral(literal));
            }), conjunct);
            // A value without a literal form: the conjunct only filters less (FiltersExactly says so).
            if (!complete)
            {
                all = false;
                continue;
            }
            // Comparisons between the values put in are decided, so that what is left reads the file's columns only.
            substituted = FilterShapes.foldConstants(substituted);
            if (substituted is ScalarExpr.Literal { value: LiteralValue.Bool { Item: true } }) continue;
            filter = filter is null ? substituted : ScalarExpr.NewBinary(BinaryOp.And, filter, substituted);
        }
        return (filter, all);
    }

    /// <summary>A partition value as an IR literal; null for a null value or a type without one.</summary>
    private static LiteralValue? Literal(IArrowArray column, int index) => column.IsNull(index) ? null : column switch
    {
        BooleanArray a => LiteralValue.NewBool(a.GetValue(index)!.Value),
        Int8Array a => LiteralValue.NewInt8(a.GetValue(index)!.Value),
        Int16Array a => LiteralValue.NewInt16(a.GetValue(index)!.Value),
        Int32Array a => LiteralValue.NewInt32(a.GetValue(index)!.Value),
        Int64Array a => LiteralValue.NewInt64(a.GetValue(index)!.Value),
        UInt8Array a => LiteralValue.NewUInt8(a.GetValue(index)!.Value),
        UInt16Array a => LiteralValue.NewUInt16(a.GetValue(index)!.Value),
        UInt32Array a => LiteralValue.NewUInt32(a.GetValue(index)!.Value),
        UInt64Array a => LiteralValue.NewUInt64(a.GetValue(index)!.Value),
        FloatArray a => LiteralValue.NewFloat32(a.GetValue(index)!.Value),
        DoubleArray a => LiteralValue.NewFloat64(a.GetValue(index)!.Value),
        Date32Array a => LiteralValue.NewDate(a.GetDateOnly(index)!.Value),
        StringArray a => LiteralValue.NewString(a.GetString(index)),
        StringViewArray a => LiteralValue.NewString(a.GetString(index)),
        _ => null,
    };

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
            return file.OpenReader(_options with
            {
                Rows = _dataset.Files[index].Rows,
                RowIndexOffset = before,
                MaxRows = remaining,
                Filter = FileFilter(index).Filter,
            });
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>Where each file column of the batches is among a file's columns (files may order them differently); the types must agree.</summary>
    private int[] Order(Apache.Arrow.Schema read, int index)
    {
        int skip = _options.RowIndexColumn is null ? 0 : 1;
        var order = new int[_layout.Length];
        for (int c = 0; c < _layout.Length; c++)
        {
            if (_layout[c].Partition >= 0)
            {
                order[c] = -1;
                continue;
            }
            Field expected = _layout[c].Field;
            int at = read.GetFieldIndex(expected.Name);
            IArrowType type = read.FieldsList[at].DataType;
            if (!ArrowTypes.SameType(type, expected.DataType))
                throw new ParquetFormatException($"{PathOf(index)}: column '{expected.Name}' is {Describe(type)} in this file but {Describe(expected.DataType)} "
                    + "in the first file.");
            order[c] = at;
        }
        return skip == 0 ? order : [0, .. order];

        static string Describe(IArrowType type) => type is DictionaryType d ? $"a dictionary of {d.ValueType.Name}" : type.Name;
    }

    /// <summary>A file's batch in the dataset's schema: its columns in order, partition values and the file path added.</summary>
    private RecordBatch Complete(RecordBatch batch, int[] order, int index)
    {
        int skip = order.Length - _layout.Length;
        var columns = new IArrowArray[Schema.FieldsList.Count];
        if (skip == 1) columns[0] = batch.Column(0);
        for (int c = 0; c < _layout.Length; c++)
        {
            columns[skip + c] = _layout[c].Partition is int p and >= 0
                ? Constant(_dataset.Partitions!.Columns[p], index, batch.Length, _layout[c].Field.DataType)
                : batch.Column(order[skip + c]);
        }
        if (_dataset.Options.FilePathColumn is not null) columns[^1] = ConstantArrays.String(PathOf(index), batch.Length, _options.ViewTypes);
        return new RecordBatch(Schema, columns, batch.Length);

        static IArrowArray Constant(IArrowArray values, int file, int length, IArrowType type)
        {
            // Inferred strings are kept as plain strings and repeated in the reader's layout.
            if (type is StringViewType && values is StringArray strings)
                return strings.IsNull(file)
                    ? ConstantArrays.Repeat(new StringViewArray.Builder().AppendNull().Build(), 0, length)
                    : ConstantArrays.String(strings.GetString(file), length, view: true);
            return ConstantArrays.Repeat(values, file, length);
        }
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

    /// <summary>
    /// Files opened ahead of the one read (up to <see cref="FilesAhead"/> of those to open: the files read, and the
    /// others when their row counts are needed); those not taken are disposed once opened.
    /// </summary>
    private sealed class Ahead(ParquetDatasetReader owner, CancellationToken ct) : IDisposable
    {
        private readonly Dictionary<int, Task<ParquetFile>> _started = [];
        private int _next;

        private bool Needed(int index) => owner._read[index] || owner._options.RowIndexColumn is not null;

        /// <summary>The file at <paramref name="index"/> (files are taken in order), the next ones started meanwhile.</summary>
        public Task<ParquetFile> Take(int index)
        {
            int count = owner._dataset.Files.Count;
            _next = Math.Max(_next, index);
            for (; _next < count && _started.Count < FilesAhead; _next++)
            {
                if (!Needed(_next)) continue;
                int file = _next;
                _started[file] = Task.Run(() => owner.OpenAsync(file, useAsync: true, ct).AsTask(), ct);
            }
            return _started.Remove(index, out Task<ParquetFile>? task) ? task : Task.Run(() => owner.OpenAsync(index, useAsync: true, ct).AsTask(), ct);
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
