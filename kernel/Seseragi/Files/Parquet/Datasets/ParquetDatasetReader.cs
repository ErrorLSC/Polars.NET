using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Microsoft.FSharp.Core;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Reads a dataset's files in order, each as <see cref="ParquetFile.OpenReader(ParquetReadOptions?)"/> does with the
/// options given, into batches of one schema: batches never span files. With
/// <see cref="ParquetReadOptions.MaxDegreeOfParallelism"/> above 1 the row groups of several files are decoded side by
/// side ahead of the consumer, on that many threads and within one reader's in-flight memory, files opened and
/// planned ahead (<see cref="ParquetDatasetOptions.MaxFilesInFlight"/>), requests to a remote store bounded apart
/// (<see cref="ParquetDatasetOptions.MaxRequestsInFlight"/>, those of the files read first let in first); with 1, from
/// local files and memory file after file on the calling thread, from other stores (whose requests wait on the
/// network) as with several threads, on one.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="ParquetReadOptions.Columns"/> and <see cref="ParquetReadOptions.ColumnPaths"/> name the dataset's
/// columns (all of them when both are null): columns from files and partition columns. Batches hold the columns from
/// files in the dataset's order (a partition column of the same name in its place, holding the path's value), then
/// the other partition columns, then the file path.</item>
/// <item>A file's column converts to the dataset's type: as it is when the types agree (nullability aside); else by a
/// lossless widening (wider integers, floats holding the integers, decimals of no fewer digits, finer time units, dates
/// into timestamps, struct fields matched by name — those the file has beyond the dataset's left out —, the elements
/// of lists and maps, dictionaries into their values and back); by an approximating conversion only with
/// <see cref="ParquetDatasetOptions.AllowLossyCasts"/>; else the read fails naming the file and the column. A column
/// or struct field a file lacks is an error, or nulls with <see cref="ParquetDatasetOptions.AllowMissingColumns"/>
/// (always when united by name).</item>
/// <item><see cref="ParquetReadOptions.Filter"/>: its conjuncts over partition columns only decide which files are
/// read (exactly, on the files' partition values); the others prune (and with <see cref="ParquetReadOptions.FilterRows"/>
/// filter) every file read, partition columns replaced by the file's values and columns the file lacks by nulls.
/// Conjuncts over a column a file's values convert into lossily, or with struct fields it lacks, do not prune that
/// file; where the file's own filtering cannot be exact, the dataset filters its rows after the conversion, so that
/// <see cref="FiltersExactly"/> holds for every file.</item>
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
    /// <summary>
    /// How many units (row groups) beyond the one the consumer waits for are decoded at once, per thread. Measured
    /// (16 threads; 2000 small files, 64 mid-sized, 4 large, 60 partitioned): 1, 2, 4 and 8 read as fast; 1 holds the
    /// least memory.
    /// </summary>
    internal const int UnitsAheadPerThread = 1;

    /// <summary>
    /// How many units are fetched and decoded at once ahead of the consumer, per thread, from a remote store: their
    /// requests wait on the network. Measured (16 threads, 64 files of 2 MB, 4 of 33 MB, stores 6 and 23 ms away): 4
    /// read 5–30% faster than 1.
    /// </summary>
    internal const int RemoteUnitsAheadPerThread = 4;

    /// <summary>
    /// The fewest units ahead from a remote store, whatever the threads: with one thread, 16 units ahead read 60
    /// partitioned files of 425 KB 3.5–5 times as fast as 1 (stores 6, 23 and 170 ms away).
    /// </summary>
    internal const int RemoteMinUnitsAhead = 16;

    /// <summary>From how many uncompressed bytes a unit decodes its columns side by side.</summary>
    internal const long ParallelColumnsFrom = 8L << 20;

    /// <summary>The fewest files open at once (read, or opened ahead) when <see cref="ParquetDatasetOptions.MaxFilesInFlight"/> is not set.</summary>
    internal const int MinFilesAhead = 16;

    /// <summary>
    /// The requests to a remote store at once when <see cref="ParquetDatasetOptions.MaxRequestsInFlight"/> is not set
    /// (provisional). Measured on 2000 files of 52 KB, 16 threads, from one home link: the best number differs by the
    /// store's distance (16–32 at 6 ms, 64 at 23 ms, 128 or more at 170 ms), and many more requests at once lose
    /// packets on the way, so that a few requests take hundreds of milliseconds.
    /// </summary>
    internal const int RemoteRequests = 64;

    /// <summary>
    /// The files open at once from a remote store when <see cref="ParquetDatasetOptions.MaxFilesInFlight"/> is not set:
    /// many, so that the files after one whose request is slow keep being read while the consumer waits for it (each
    /// holds its footer, and a small file its bytes).
    /// </summary>
    internal const int RemoteFilesHeld = 1024;

    private readonly ParquetDataset _dataset;
    private readonly ParquetReadOptions _options;
    private readonly bool _allowMissing;
    private readonly ArrowConversionRules _rules;
    private readonly Dictionary<string, Field> _columns;   // the dataset's columns from files, whole, as these options read them
    private readonly Target[] _targets;                   // the columns from files selected, in the dataset's order
    private readonly Column[] _layout;
    private readonly bool[] _read;
    private readonly ScalarExpr? _fileFilter;
    private readonly ScalarExpr[] _mixed;
    private readonly ScalarExpr? _rowFilter;              // what the rows delivered must pass (the conjuncts not decided per file)
    private readonly FilterProgram? _program;             // _rowFilter as the dataset applies it after converting (exact)
    private readonly string[] _filterRoots;               // the columns from files the row filter reads

    /// <summary>A column from files selected: whole, or the struct fields on its paths; its type in the batches.</summary>
    private sealed record Target(string Name, string[][]? Paths, Field Field);

    /// <summary>A column of the batches after the row index: a selected column from files, or a partition column.</summary>
    private readonly record struct Column(Field Field, int Target, int Partition);

    internal ParquetDatasetReader(ParquetDataset dataset, ParquetReadOptions options)
    {
        options.Validate();
        if (options.RowGroups is not null)
            throw new ArgumentException("Row groups are numbered per file: a dataset reads them all (select rows per file with ParquetDatasetFile.Rows).", nameof(options));
        if (options.Rows is not null)
            throw new ArgumentException("Rows are numbered per file: select them with ParquetDatasetFile.Rows.", nameof(options));
        if (options.Filter is { } given) options = options with { Filter = FilterShapes.canonical(given) };
        _dataset = dataset;
        _options = options;
        _allowMissing = dataset.Options.AllowMissingColumns || dataset.Options.Schema is null && dataset.Options.SchemaMode == DatasetSchemaMode.UnionByName;
        _rules = new ArrowConversionRules(dataset.Options.AllowLossyCasts, _allowMissing);
        Partitions? partitions = dataset.Partitions;
        bool IsPartition(string name) => partitions?.IndexOf(name) >= 0;

        Apache.Arrow.Schema columns = dataset.ColumnsFor(options);
        _columns = columns.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);

        // The columns selected: from files (not shadowed by a partition column), and partition columns.
        bool all = options.Columns is null && options.ColumnPaths is null;
        foreach (string name in options.Columns ?? [])
        {
            if (!IsPartition(name) && !_columns.ContainsKey(name))
                throw new ArgumentException($"The dataset has no column '{name}' (the columns are those of the files and the partitions).", nameof(options));
        }
        Dictionary<string, List<string[]>> paths = new(StringComparer.Ordinal);
        foreach (string[] selected in options.ColumnPaths ?? [])
        {
            if (selected is null || selected.Length == 0) throw new ArgumentException("A column path is empty.", nameof(options));
            if (IsPartition(selected[0])) throw new ArgumentException($"'{selected[0]}' is a partition column: it has no fields to select.", nameof(options));
            if (!_columns.ContainsKey(selected[0])) throw new ArgumentException($"The dataset has no column '{selected[0]}'.", nameof(options));
            (paths.TryGetValue(selected[0], out var list) ? list : paths[selected[0]] = []).Add(selected[1..]);
        }
        List<Target> targets = [];
        foreach (Field field in columns.FieldsList)
        {
            if (IsPartition(field.Name)) continue;
            Field nullable = field.IsNullable ? field : new Field(field.Name, field.DataType, nullable: true, field.Metadata);
            if (all || options.Columns?.Contains(field.Name) == true) targets.Add(new Target(field.Name, null, nullable));
            else if (paths.TryGetValue(field.Name, out var below))
                targets.Add(new Target(field.Name, [.. below], new Field(field.Name, Prune(field.DataType, below, field.Name), nullable: true, field.Metadata)));
        }
        _targets = [.. targets];
        bool[] partitionsRead = [.. Enumerable.Range(0, partitions?.Fields.Length ?? 0).Select(p => all || options.Columns?.Contains(partitions!.Fields[p].Name) == true)];

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
                string[] names = [.. ScalarExprOps.collectColumns(conjunct)];
                int inPartitions = names.Count(IsPartition);
                (inPartitions == 0 ? onFiles : inPartitions == names.Length ? onPartitions : mixed).Add(conjunct);
            }
            if (onPartitions.Count > 0) exact = Prune(partitions!, onPartitions.Aggregate(And), _read);
        }
        _fileFilter = onFiles.Count == 0 ? null : onFiles.Aggregate(And);
        _mixed = [.. mixed];
        _rowFilter = onFiles.Count + mixed.Count == 0 ? null : onFiles.Concat(mixed).Aggregate(And);

        // Exact when the filter lowers exactly over the dataset's types (each file's rows are filtered again after
        // conversion where its own filtering is not exact).
        _filterRoots = [];
        FiltersExactly = options.FilterRows && exact;
        if (options.FilterRows && _rowFilter is { } rows)
        {
            LoweredFilter lowered = FilterLowering.lower(dataset.FilterSchema(options), rows);
            FiltersExactly &= lowered.Exact;
            if (FiltersExactly && OptionModule.IsSome(lowered.Program))
            {
                _program = lowered.Program.Value;
                _filterRoots = [.. _program.Columns.Select(c => c.Path[0]).Where(n => !IsPartition(n)).Distinct()];
            }
        }

        List<Column> layout = [];
        foreach (Field field in columns.FieldsList)
        {
            int p = partitions?.IndexOf(field.Name) ?? -1;
            if (p >= 0)
            {
                if (partitionsRead[p]) layout.Add(new Column(dataset.PartitionField(p, options.ViewTypes), -1, p));
            }
            else if (System.Array.FindIndex(_targets, t => t.Name == field.Name) is int t and >= 0) layout.Add(new Column(_targets[t].Field, t, -1));
        }
        for (int p = 0; p < partitionsRead.Length; p++)
        {
            if (partitionsRead[p] && !_columns.ContainsKey(partitions!.Fields[p].Name))
                layout.Add(new Column(dataset.PartitionField(p, options.ViewTypes), -1, p));
        }
        _layout = [.. layout];
        List<Field> output = [.. _layout.Select(c => c.Field)];
        if (options.RowIndexColumn is { } index)
        {
            if (output.Any(f => f.Name == index)) throw new ArgumentException($"The row index column '{index}' has the name of another column read.", nameof(options));
            output.Insert(0, new Field(index, Int64Type.Default, nullable: false));
        }
        if (dataset.Options.FilePathColumn is { } path)
        {
            if (output.Any(f => f.Name == path)) throw new ArgumentException($"The file path column '{path}' has the name of another column read.", nameof(options));
            output.Add(new Field(path, options.ViewTypes ? StringViewType.Default : StringType.Default, nullable: false));
        }
        Schema = new Apache.Arrow.Schema(output, columns.Metadata);

        static ScalarExpr And(ScalarExpr a, ScalarExpr b) => ScalarExpr.NewBinary(BinaryOp.And, a, b);
    }

    /// <summary>How many units are decoded at once ahead of the consumer.</summary>
    private int UnitsAhead => Remote
        ? Math.Max(RemoteMinUnitsAhead, Math.Max(1, _options.MaxDegreeOfParallelism) * RemoteUnitsAheadPerThread)
        : Math.Max(1, _options.MaxDegreeOfParallelism) * UnitsAheadPerThread;

    /// <summary>Whether the files come from a store whose requests wait on the network (not local files or memory).</summary>
    private bool Remote => _dataset.Store is not (LocalFileStore or MemoryFileStore);

    /// <summary>
    /// How many files are open at once (read, planned, or opened ahead): as the options say; else, from local files
    /// and memory, enough to feed the units ahead (a small file is one unit), and from other stores
    /// <see cref="RemoteFilesHeld"/> (their requests bounded apart, see <see cref="RequestsInFlight"/>).
    /// </summary>
    private int FilesInFlight => _dataset.Options.MaxFilesInFlight
        ?? (Remote ? RemoteFilesHeld : Math.Max(MinFilesAhead, UnitsAhead + 1));

    /// <summary>
    /// How many requests to the store run at once (files opened, row groups fetched), null when not bounded: as the
    /// options say, else <see cref="RemoteRequests"/> from a remote store; never more than the files open at once
    /// when the options bound those.
    /// </summary>
    private int? RequestsInFlight =>
        (_dataset.Options.MaxRequestsInFlight ?? (Remote ? RemoteRequests : null)) is int requests
            ? Math.Min(requests, _dataset.Options.MaxFilesInFlight ?? int.MaxValue)
            : null;

    /// <summary>A gate for <see cref="RequestsInFlight"/>, null when not bounded.</summary>
    private RequestGate? NewRequestGate() => RequestsInFlight is int requests ? new RequestGate(requests) : null;

    /// <summary>The most requests to the store at once in the last read (0 when not bounded; for tests).</summary>
    internal int PeakRequests { get; private set; }

    /// <summary>The most decodes running at once in the last windowed read (see <see cref="ReadBudget.PeakDecoding"/>; for tests).</summary>
    internal int PeakDecoding { get; private set; }

    /// <summary>The schema of the batches: the row index if asked, the columns read (nullable), the file path if asked.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// Whether, with <see cref="ParquetReadOptions.FilterRows"/>, the batches hold exactly the rows the filter keeps:
    /// when the filter lowers exactly over the dataset's types and its conjuncts over partition columns are decided
    /// exactly (files whose own filtering is not exact are filtered again after their columns convert).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The files a filter over partition columns leaves (for tests).</summary>
    internal IReadOnlyList<bool> FilesRead => _read;

    /// <summary>
    /// The most uncompressed bytes and units decoded at once ahead of the consumer, and files open at once, in the last
    /// windowed read (for tests).
    /// </summary>
    internal (long Bytes, int Units, int Files) PeakInFlight { get; private set; }

    /// <summary>The files whose rows the dataset filtered after converting them (for tests).</summary>
    internal int FilesFilteredAfter => _filteredAfter;

    private int _filteredAfter;

    /// <summary>
    /// Reads the batches, in file order. With <see cref="ParquetReadOptions.MaxDegreeOfParallelism"/> above 1 the row
    /// groups of several files are decoded side by side on that many threads (and within the in-flight memory of
    /// one reader), ahead of the consumer; with 1, from local files and memory file after file on the calling thread,
    /// from other stores fetched ahead in the same way and decoded on one thread.
    /// </summary>
    /// <exception cref="ParquetFormatException">
    /// A file is damaged, is not a Parquet file, lacks a column read, holds a column of a type that does not convert
    /// to the dataset's, or a value beyond the range of the dataset's type (the message names the file). It surfaces
    /// after the rows of the files before it.
    /// </exception>
    /// <exception cref="FileNotFoundException">A file named does not exist.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        if (_options.MaxDegreeOfParallelism > 1 || Remote)
        {
            foreach (RecordBatch batch in ReadWindowed()) yield return batch;
            yield break;
        }
        RequestGate? gate = NewRequestGate();
        using var ahead = new Ahead(this, FilesInFlight, gate, default); // the file taken counts among them
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
                    using FilePlan plan = Plan(file, i, before, remaining);
                    using IEnumerator<RecordBatch> batches = plan.Reader.ReadBatches().GetEnumerator();
                    while (remaining != 0 && Next(batches, i) is { } batch)
                    {
                        if (Convert(batch, plan, i) is { } done) yield return Cut(done, ref remaining);
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
        if (_options.MaxDegreeOfParallelism > 1 || Remote)
        {
            await foreach (RecordBatch batch in ReadWindowedAsync(cancellationToken).ConfigureAwait(false)) yield return batch;
            yield break;
        }
        RequestGate? gate = NewRequestGate();
        using var ahead = new Ahead(this, FilesInFlight, gate, cancellationToken);
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
                    using FilePlan plan = Plan(file, i, before, remaining);
                    await using IAsyncEnumerator<RecordBatch> batches = plan.Reader.ReadBatchesAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
                    while (remaining != 0 && await NextAsync(batches, i).ConfigureAwait(false) is { } batch)
                    {
                        if (Convert(batch, plan, i) is { } done) yield return Cut(done, ref remaining);
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

    /// <summary>The files' units through the window, the batches of each in order, cut to the rows asked.</summary>
    private IEnumerable<RecordBatch> ReadWindowed()
    {
        using var window = new UnitWindow(this, default);
        long? remaining = _options.MaxRows;
        while (remaining != 0 && window.NextAsync().GetAwaiter().GetResult() is { } batches)
        {
            int i = 0;
            try
            {
                for (; i < batches.Length && remaining != 0; i++)
                {
                    RecordBatch batch = batches[i];
                    batches[i] = null!;
                    yield return Cut(batch, ref remaining);
                }
            }
            finally
            {
                for (; i < batches.Length; i++) batches[i]?.Dispose();
            }
        }
    }

    /// <inheritdoc cref="ReadWindowed"/>
    private async IAsyncEnumerable<RecordBatch> ReadWindowedAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var window = new UnitWindow(this, cancellationToken);
        long? remaining = _options.MaxRows;
        while (remaining != 0 && await window.NextAsync().ConfigureAwait(false) is { } batches)
        {
            int i = 0;
            try
            {
                for (; i < batches.Length && remaining != 0; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    RecordBatch batch = batches[i];
                    batches[i] = null!;
                    yield return Cut(batch, ref remaining);
                }
            }
            finally
            {
                for (; i < batches.Length; i++) batches[i]?.Dispose();
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

    // ---- one file -------------------------------------------------------------------------------------------------

    /// <summary>
    /// How a file is read: its reader, and per column of the batches (and per column the dataset's filter reads
    /// after converting) the reader's column it comes from (-1: the file lacks it, nulls) and the conversion.
    /// </summary>
    private sealed class FilePlan(ParquetReader reader, (int Source, ArrowConversion Conversion)[] outputs,
        (int Source, ArrowConversion Conversion)[] filtered, bool filterAfter) : IDisposable
    {
        public ParquetReader Reader { get; } = reader;

        /// <summary>Per selected column from files (as <see cref="_targets"/>).</summary>
        public (int Source, ArrowConversion Conversion)[] Outputs { get; } = outputs;

        /// <summary>Per column from files the dataset's filter reads (as <see cref="_filterRoots"/>), when it filters this file.</summary>
        public (int Source, ArrowConversion Conversion)[] Filtered { get; } = filtered;

        /// <summary>Whether the dataset filters the file's rows after converting them.</summary>
        public bool FilterAfter { get; } = filterAfter;

        public void Dispose() => Reader.Dispose();
    }

    /// <summary>
    /// The plan of a file: its rows numbered after <paramref name="before"/> rows, up to <paramref name="remaining"/>;
    /// its reader on the dataset's threads and buffers when <paramref name="shared"/> is given.
    /// </summary>
    private FilePlan Plan(ParquetFile file, int index, long before, long? remaining, ReadBudget? shared = null)
    {
        string path = PathOf(index);
        ShapePlan shape = ShapePlanOf(file, path);
        var (fileFilter, complete) = FileFilter(index, shape.Missing, shape.Dropped);

        bool after = FiltersExactly && _program is not null && !complete;
        ParquetReader reader = Open(file, index, before, after ? null : remaining, shape, fileFilter, after, shared);
        if (FiltersExactly && _program is not null && !after && !reader.FiltersExactly)
        {
            // The file's filtering is not exact: read again with what the dataset's filter needs.
            reader.Dispose();
            after = true;
            reader = Open(file, index, before, null, shape, fileFilter, after, shared);
        }
        try
        {
            int variant = after ? 1 : 0;
            Converted converted = Volatile.Read(ref shape.Converted[variant]) ?? Conversions(reader.Schema, after, path);
            Volatile.Write(ref shape.Converted[variant], converted); // the same for every file of the shape
            if (after) Interlocked.Increment(ref _filteredAfter);
            return new FilePlan(reader, converted.Outputs, converted.Filtered, after);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    /// <summary>How the columns of a file's batches (<paramref name="read"/>) convert to the dataset's.</summary>
    private Converted Conversions(Apache.Arrow.Schema read, bool after, string path)
    {
        int skip = _options.RowIndexColumn is null ? 0 : 1;
        var outputs = new (int, ArrowConversion)[_targets.Length];
        for (int t = 0; t < _targets.Length; t++)
            outputs[t] = Conversion(read, skip, _targets[t].Name, _targets[t].Field.DataType, path);
        var filtered = after ? new (int, ArrowConversion)[_filterRoots.Length] : [];
        for (int f = 0; f < filtered.Length; f++) filtered[f] = Conversion(read, skip, _filterRoots[f], _columns[_filterRoots[f]].DataType, path);
        return new Converted(outputs, filtered);
    }

    /// <summary>The conversions of a file's columns: per selected column, and per column the dataset's filter reads.</summary>
    private sealed record Converted((int Source, ArrowConversion Conversion)[] Outputs, (int Source, ArrowConversion Conversion)[] Filtered);

    /// <summary>
    /// What the files of one schema shape read, and how their columns convert: the same for each of them, so made by the
    /// first one and shared (the file filter, which holds the file's partition values, is the file's own).
    /// </summary>
    private sealed class ShapePlan
    {
        /// <summary>The file's columns as these options read them, by name.</summary>
        public required Dictionary<string, Field> Has { get; init; }

        /// <summary>The columns the row filter reads that the files lack (nulls), and those whose conjuncts are left out.</summary>
        public required HashSet<string> Missing { get; init; }

        public required HashSet<string> Dropped { get; init; }

        /// <summary>The reader's column lists: the columns read, and (second) with the columns the dataset's filter reads.</summary>
        public required (IReadOnlyList<string> Columns, IReadOnlyList<string[]>? Paths)[] Lists { get; init; }

        /// <summary>The conversions of the readers of either list, once a file made them.</summary>
        public readonly Converted?[] Converted = new Converted?[2];

        /// <summary>What the readers of either list select from the files' columns, once a file's reader selected it.</summary>
        public readonly ParquetReader.Selection?[] Selections = new ParquetReader.Selection?[2];
    }

    /// <summary>The most shapes planned (files of other shapes plan for themselves).</summary>
    private const int MaxShapePlans = 64;

    private readonly object _plansLock = new();
    private readonly Dictionary<SchemaShape, ShapePlan> _plans = new(ReferenceEqualityComparer.Instance);

    /// <summary>The plan of the file's shape: made once per shape (errors name the file that made it).</summary>
    private ShapePlan ShapePlanOf(ParquetFile file, string path)
    {
        SchemaShape key = file.Shape;
        lock (_plansLock)
        {
            if (_plans.TryGetValue(key, out ShapePlan? known)) return known;
        }
        ShapePlan made = MakeShapePlan(file, path);
        lock (_plansLock)
        {
            if (_plans.TryGetValue(key, out ShapePlan? known)) return known;
            if (_plans.Count < MaxShapePlans) _plans.Add(key, made);
        }
        return made;
    }

    private ShapePlan MakeShapePlan(ParquetFile file, string path)
    {
        Dictionary<string, Field> has;
        try
        {
            has = file.Mapping(_options).Schema.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(path, e);
        }

        // What the file's reader reads: whole columns and paths (those the file has).
        HashSet<string> whole = new(StringComparer.Ordinal);
        List<string[]> paths = [];
        foreach (Target target in _targets)
        {
            if (!has.TryGetValue(target.Name, out Field? field))
            {
                if (!_allowMissing) throw Missing(path, target.Name, filter: false);
                continue;
            }
            if (target.Paths is null)
            {
                whole.Add(target.Name);
                continue;
            }
            string[][] kept = [.. target.Paths.Where(p => Exists(field.DataType, p, 0))];
            if (kept.Length < target.Paths.Length && !_allowMissing)
                throw Missing(path, string.Join('.', [target.Name, .. target.Paths.First(p => !Exists(field.DataType, p, 0))]), filter: false);
            // Every field selected missing: the column is read whole (its rows' nulls), its fields then left out.
            if (kept.Length == 0) whole.Add(target.Name);
            else paths.AddRange(kept.Select(p => (string[])[target.Name, .. p]));
        }

        // The filter the file reads: columns it lacks as nulls; conjuncts over columns whose values convert (lossily, or
        // with struct fields it lacks) are left out, the file then filtered after converting.
        HashSet<string> missing = new(StringComparer.Ordinal), dropped = new(StringComparer.Ordinal);
        if (_rowFilter is not null)
        {
            foreach (string root in ScalarExprOps.collectColumns(_rowFilter))
            {
                if (!_columns.TryGetValue(root, out Field? target) || _dataset.Partitions?.IndexOf(root) >= 0) continue;
                if (!has.TryGetValue(root, out Field? field))
                {
                    if (!_allowMissing) throw Missing(path, root, filter: true);
                    missing.Add(root);
                }
                else if (!ArrowWidening.CanConvert(field.DataType, target.DataType, default)) dropped.Add(root);
            }
        }

        // With the dataset filtering after converting, the reader reads the columns the filter reads too, whole.
        HashSet<string> withFilter = new(whole, StringComparer.Ordinal);
        foreach (string root in _filterRoots)
        {
            if (has.ContainsKey(root)) withFilter.Add(root);
        }
        List<string[]> pathsWithFilter = [.. paths.Where(p => !withFilter.Contains(p[0]))];
        return new ShapePlan
        {
            Has = has,
            Missing = missing,
            Dropped = dropped,
            Lists = [([.. whole], paths.Count == 0 ? null : paths), ([.. withFilter], pathsWithFilter.Count == 0 ? null : pathsWithFilter)],
        };
    }

    /// <summary>The file's reader: the columns read, and with <paramref name="after"/> the columns the dataset's filter reads, whole.</summary>
    private ParquetReader Open(ParquetFile file, int index, long before, long? remaining, ShapePlan shape, ScalarExpr? filter, bool after, ReadBudget? shared)
    {
        int variant = after ? 1 : 0;
        var (columns, paths) = shape.Lists[variant];
        try
        {
            var reader = new ParquetReader(file, _options with
            {
                Columns = columns,
                ColumnPaths = paths,
                Rows = _dataset.Files[index].Rows,
                RowIndexOffset = before,
                MaxRows = remaining,
                Filter = filter,
            }, shared, Volatile.Read(ref shape.Selections[variant])) { RequestOrder = index };
            Volatile.Write(ref shape.Selections[variant], reader.ColumnSelection); // the same for every file of the shape
            return reader;
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>
    /// Where a column comes from in the reader's batches (-1 when the file lacks it: nulls) and how it converts to
    /// <paramref name="target"/>; a conversion the rules refuse is an error naming the file, the column and what allows it.
    /// </summary>
    private (int Source, ArrowConversion Conversion) Conversion(Apache.Arrow.Schema read, int skip, string name, IArrowType target, string path)
    {
        int at = -1;
        for (int c = skip; c < read.FieldsList.Count; c++)
        {
            if (read.FieldsList[c].Name == name) at = c;
        }
        if (at < 0) return (-1, new NullsConversion(target));
        IArrowType type = read.FieldsList[at].DataType;
        if (ArrowWidening.TryCompile(type, target, _rules, name, out ArrowConversion? conversion, out ArrowConversionError? error)) return (at, conversion);
        string hint = error.Problem switch
        {
            ArrowConversionProblem.Lossy => " (ParquetDatasetOptions.AllowLossyCasts allows lossy conversions)",
            ArrowConversionProblem.MissingField => " (ParquetDatasetOptions.AllowMissingColumns reads missing columns and fields as nulls)",
            _ when _dataset.Options.Schema is null && _dataset.Options.SchemaMode == DatasetSchemaMode.FirstFile && ArrowWidening.CanConvert(target, type, _rules) =>
                " (the dataset's types are the first file's: SchemaMode.UnionByName or a given Schema reads wider types in later files)",
            _ => "",
        };
        string what = error.Problem == ArrowConversionProblem.MissingField
            ? $"the file has no field '{error.Path}', which the dataset reads"
            : $"'{error.Path}': {error.Message} (the dataset's type)";
        throw new ParquetFormatException($"{path}: {what}{hint}.");
    }

    private static ParquetFormatException Missing(string path, string column, bool filter) =>
        new($"{path}: the file has no column '{column}', which the dataset {(filter ? "filters on" : "reads")} "
            + "(ParquetDatasetOptions.AllowMissingColumns reads missing columns and fields as nulls).");

    /// <summary>Whether a path of field names (through lists, and maps by key and value) leads to a field of the type.</summary>
    private static bool Exists(IArrowType type, string[] path, int at)
    {
        if (at == path.Length) return true;
        return type switch
        {
            StructType s => s.GetFieldIndex(path[at], StringComparer.Ordinal) is int i and >= 0 && Exists(s.Fields[i].DataType, path, at + 1),
            ListType l => Exists(l.ValueDataType, path, at),
            FixedSizeListType l => Exists(l.ValueDataType, path, at),
            MapType m => path[at] switch
            {
                "key" => Exists(m.KeyField.DataType, path, at + 1),
                "value" => Exists(m.ValueField.DataType, path, at + 1),
                _ => false,
            },
            _ => false,
        };
    }

    /// <summary>A type with only the struct fields on the paths, as a file's reader prunes a column it reads by paths.</summary>
    /// <exception cref="ArgumentException">A path names a field the type does not have.</exception>
    private static IArrowType Prune(IArrowType type, IReadOnlyList<string[]> paths, string where)
    {
        if (paths.Count == 0 || paths.Any(p => p.Length == 0)) return type;
        switch (type)
        {
            case StructType s:
                foreach (string[] p in paths)
                {
                    if (s.GetFieldIndex(p[0], StringComparer.Ordinal) < 0) throw new ArgumentException($"'{where}' has no field '{p[0]}'.");
                }
                return new StructType([.. s.Fields
                    .Where(f => paths.Any(p => p[0] == f.Name))
                    .Select(f => new Field(f.Name, Prune(f.DataType, [.. paths.Where(p => p[0] == f.Name).Select(p => p[1..])], $"{where}.{f.Name}"), f.IsNullable, f.Metadata))]);
            case ListType l:
                return new ListType(new Field(l.ValueField.Name, Prune(l.ValueDataType, paths, where), l.ValueField.IsNullable, l.ValueField.Metadata));
            case FixedSizeListType l:
                return new FixedSizeListType(new Field(l.ValueField.Name, Prune(l.ValueDataType, paths, where), l.ValueField.IsNullable, l.ValueField.Metadata), l.ListSize);
            case MapType m:
            {
                foreach (string[] p in paths)
                {
                    if (p[0] is not ("key" or "value")) throw new ArgumentException($"'{where}' is a map: paths go through 'key' or 'value', not '{p[0]}'.");
                }
                string[][] values = [.. paths.Where(p => p[0] == "value").Select(p => p[1..])];
                Field value = values.Length == 0 ? m.ValueField
                    : new Field(m.ValueField.Name, Prune(m.ValueField.DataType, values, $"{where}.value"), m.ValueField.IsNullable, m.ValueField.Metadata);
                return new MapType(m.KeyField, value, m.KeySorted);
            }
            default:
                throw new ArgumentException($"'{where}' is not a struct, list or map: no field below it can be selected.");
        }
    }

    /// <summary>
    /// A file's batch in the dataset's schema: its columns converted and in order, partition values and the file
    /// path added; filtered by the dataset when the plan says so. Null when no row is left. Safe on any thread.
    /// </summary>
    private RecordBatch? Convert(RecordBatch batch, FilePlan plan, int index)
    {
        int length = batch.Length, skip = _options.RowIndexColumn is null ? 0 : 1;
        var columns = new IArrowArray[Schema.FieldsList.Count];
        var used = new bool[batch.ColumnCount]; // the batch's columns handed on as they are
        IArrowArray[] filterArrays = [];
        try
        {
            try
            {
                if (skip == 1)
                {
                    columns[0] = batch.Column(0);
                    used[0] = true;
                }
                for (int c = 0; c < _layout.Length; c++)
                {
                    columns[skip + c] = _layout[c].Partition is int p and >= 0
                        ? Constant(_dataset.Partitions!.Columns[p], index, length, _layout[c].Field.DataType)
                        : Convert(batch, plan.Outputs[_layout[c].Target], length, used);
                }
                if (_dataset.Options.FilePathColumn is not null) columns[^1] = ConstantArrays.String(PathOf(index), length, _options.ViewTypes);
                if (plan.FilterAfter) filterArrays = [.. plan.Filtered.Select(f => Convert(batch, f, length, used))];
            }
            catch (OverflowException e)
            {
                throw new ParquetFormatException($"{PathOf(index)}: {e.Message}", e);
            }
            finally
            {
                for (int c = 0; c < used.Length; c++)
                {
                    if (!used[c]) batch.Column(c).Dispose();
                }
            }

            if (plan.FilterAfter)
            {
                // The dataset's filter over the converted columns (and the file's partition values).
                FilterProgram program = _program!;
                var arguments = new IArrowArray[program.Columns.Count];
                List<IArrowArray> constants = [];
                for (int a = 0; a < arguments.Length; a++)
                {
                    FilterColumn column = program.Columns[a];
                    int f = System.Array.IndexOf(_filterRoots, column.Path[0]);
                    IArrowArray root;
                    if (f >= 0) root = filterArrays[f];
                    else
                    {
                        int p = _dataset.Partitions!.IndexOf(column.Path[0]);
                        root = Constant(_dataset.Partitions.Columns[p], index, length, _dataset.PartitionField(p, _options.ViewTypes).DataType);
                        constants.Add(root);
                    }
                    arguments[a] = Descend(root, column.Path);
                }
                var mask = new ulong[Masks.Words(length)];
                int kept;
                try
                {
                    kept = program.Evaluate(arguments, length, mask);
                }
                finally
                {
                    foreach (IArrowArray constant in constants) constant.Dispose();
                }
                if (kept < length)
                {
                    for (int c = 0; c < columns.Length; c++)
                    {
                        IArrowArray all = columns[c];
                        columns[c] = ArrowFilter.Filter(all, mask);
                        if (!filterArrays.Contains(all)) all.Dispose(); // those are disposed below
                    }
                    length = kept;
                }
            }
        }
        catch
        {
            foreach (IArrowArray? column in columns) column?.Dispose();
            throw;
        }
        finally
        {
            foreach (IArrowArray array in filterArrays)
            {
                if (!columns.Contains(array)) array.Dispose();
            }
        }

        if (length == 0)
        {
            foreach (IArrowArray column in columns) column.Dispose();
            return null;
        }
        return new RecordBatch(Schema, columns, length);

        static IArrowArray Constant(IArrowArray values, int file, int length, IArrowType type)
        {
            // Inferred strings are kept as plain strings and repeated in the reader's layout.
            if (type is StringViewType && values is StringArray strings)
                return strings.IsNull(file)
                    ? ConstantArrays.Repeat(new StringViewArray.Builder().AppendNull().Build(PooledArrowAllocator.Instance), 0, length)
                    : ConstantArrays.String(strings.GetString(file), length, view: true);
            return ConstantArrays.Repeat(values, file, length);
        }
    }

    /// <summary>A batch cut to the <paramref name="remaining"/> rows still to deliver (over its memory: disposing the cut releases it all).</summary>
    private RecordBatch Cut(RecordBatch batch, ref long? remaining)
    {
        if (remaining is not long left) return batch;
        if (batch.Length < left)
        {
            remaining = left - batch.Length;
            return batch;
        }
        remaining = 0;
        return batch.Length == left ? batch : new RecordBatch(Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, (int)left))], (int)left);
    }

    /// <summary>A column of the batch converted (the batch's own when nothing changes, marked used), or nulls when the file lacks it.</summary>
    private static IArrowArray Convert(RecordBatch batch, (int Source, ArrowConversion Conversion) plan, int length, bool[] used)
    {
        if (plan.Source < 0) return NullArrays.Of(plan.Conversion.Target, length);
        IArrowArray source = batch.Column(plan.Source);
        if (plan.Conversion.IsIdentity)
        {
            // A column handed on twice (selected and filtered on) is the same array.
            used[plan.Source] = true;
            return source;
        }
        return plan.Conversion.Convert(source);
    }

    /// <summary>The struct field a filter column names below its top-level column.</summary>
    private static IArrowArray Descend(IArrowArray column, string[] path)
    {
        IArrowArray current = column;
        for (int i = 1; i < path.Length; i++)
        {
            var parent = (StructArray)current;
            current = parent.Fields[((StructType)parent.Data.DataType).GetFieldIndex(path[i])];
        }
        return current;
    }

    // ---- filters --------------------------------------------------------------------------------------------------

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
    /// The filter a file reads: the conjuncts over its columns, those over partition columns too with its values,
    /// columns it lacks (<paramref name="missing"/>) as nulls; conjuncts over <paramref name="dropped"/> columns are
    /// left out. Whether every conjunct is in (one left out, or one with a partition value of no literal form, makes
    /// the file's rows filtered less).
    /// </summary>
    private (ScalarExpr? Filter, bool Complete) FileFilter(int index, HashSet<string> missing, HashSet<string> dropped)
    {
        if (_mixed.Length == 0 && missing.Count == 0 && dropped.Count == 0) return (_fileFilter, true);
        Partitions? partitions = _dataset.Partitions;
        ScalarExpr? filter = null;
        bool all = true;
        IEnumerable<ScalarExpr> conjuncts = _fileFilter is null ? _mixed : FilterShapes.conjuncts(_fileFilter).Concat(_mixed);
        foreach (ScalarExpr conjunct in conjuncts)
        {
            bool complete = true, changed = false;
            ScalarExpr substituted = ScalarExprOps.substitute(FuncConvert.FromFunc((string name) =>
            {
                if (dropped.Contains(name))
                {
                    complete = false;
                    return FSharpOption<ScalarExpr>.None;
                }
                if (missing.Contains(name))
                {
                    changed = true;
                    return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewLiteral(LiteralValue.NewNull(LogicalTypes.Null)));
                }
                int p = partitions?.IndexOf(name) ?? -1;
                if (p < 0) return FSharpOption<ScalarExpr>.None;
                if (Literal(partitions!.Columns[p], index) is not { } literal)
                {
                    complete = false;
                    return FSharpOption<ScalarExpr>.None;
                }
                changed = true;
                return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewLiteral(literal));
            }), conjunct);
            // A conjunct left out only filters less (the dataset filters after where it promised exact rows).
            if (!complete)
            {
                all = false;
                continue;
            }
            // Comparisons between the values put in are decided, so that what is left reads the file's columns only.
            if (changed) substituted = FilterShapes.foldConstants(substituted);
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

    // ---- files ----------------------------------------------------------------------------------------------------

    /// <summary>Opens a file (its footer from before when there is one, kept for the next reads when worth it).</summary>
    private async ValueTask<ParquetFile> OpenAsync(int index, bool useAsync, CancellationToken ct)
    {
        return await ParquetDataset.OpenFileAsync(_dataset.Store, _dataset.Files[index], _dataset.Footer(index), useAsync, ct, _dataset.Footers, index, _dataset.Interner).ConfigureAwait(false);
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
        catch (IOException e) when (ParquetDataset.Unnamed(e, PathOf(index)))
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
        catch (IOException e) when (ParquetDataset.Unnamed(e, PathOf(index)))
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>A file the window reads: its plan and file, released once its last unit is taken.</summary>
    private sealed class FileSlot(int index, ParquetFile? file, FilePlan? plan, int units) : IDisposable
    {
        private int _taken;
        private bool _disposed;

        public int Index => index;

        public FilePlan Plan => plan!;

        public int Units => units;

        /// <summary>One more unit taken; true when it was the last (the slot is then disposed).</summary>
        public bool Taken()
        {
            if (++_taken < units) return false;
            Dispose();
            return true;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            plan?.Dispose();
            file?.Dispose();
        }
    }

    /// <summary>A unit of the window: a file's row group, decoded and converted by its task once started.</summary>
    private sealed class Unit(FileSlot file, int index, long bytes)
    {
        public FileSlot File => file;

        public int Index => index;

        public long Bytes => bytes;

        public Task<RecordBatch[]>? Task { get; set; }
    }

    /// <summary>
    /// The units of the dataset's files (row groups with their rows) in output order, decoded ahead of the consumer
    /// on one scheduler: the unit the consumer waits for always starts; the others start up to
    /// <see cref="UnitsAhead"/> beyond it while the uncompressed bytes in flight stay within one reader's budget.
    /// Files are opened ahead and planned side by side (a file's row numbers follow the earlier files' counts, known
    /// from their footers); their units join the window in file order as their plans complete. A file's failure
    /// surfaces when the consumer reaches it. Disposing the window stops the work, waits for what runs, and frees
    /// what was decoded and not taken, then the files.
    /// </summary>
    private sealed class UnitWindow : IDisposable
    {
        private readonly ParquetDatasetReader _owner;
        private readonly ReadBudget _budget;
        private readonly Ahead _ahead;
        private readonly CancellationTokenSource _cancel;
        private readonly object _lock = new();
        private readonly List<Unit> _units = [];
        private readonly List<Task<FileSlot>> _plans = [];   // the files planned or planning, in order (null once joined)
        private readonly HashSet<FileSlot> _live = [];        // the files joined whose units are not all taken
        private readonly long? _maxRows;                     // the rows to plan, when no row is filtered
        private Task<long> _before = Task.FromResult(0L);   // the rows of the files before the next one planned
        private int _nextFile, _integrated, _head, _started;
        private long _inFlight, _plannedRows, _peakBytes;
        private int _peakUnits, _peakFiles;
        private bool _disposed;

        public UnitWindow(ParquetDatasetReader owner, CancellationToken cancellationToken)
        {
            _owner = owner;
            _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            int threads = Math.Max(1, owner._options.MaxDegreeOfParallelism);
            int leaves = Math.Max(1, owner._targets.Sum(t => Leaves(t.Field.DataType)) + owner._filterRoots.Length);
            _budget = new ReadBudget(threads, owner._options.InFlightBytes, (2 * leaves + 4) * owner.UnitsAhead, writesPastCaches: true,
                owner.NewRequestGate());
            _ahead = new Ahead(owner, 0, _budget.Requests, _cancel.Token); // files open as they are planned (counted below)
            // MaxRows bounds the planning only when no row is filtered (the consumer cuts in any case).
            _maxRows = !owner._options.FilterRows || owner._options.Filter is null ? owner._options.MaxRows : null;
        }

        /// <summary>The next unit's batches (converted), in order; null when every unit was taken.</summary>
        public async Task<RecordBatch[]?> NextAsync()
        {
            while (true)
            {
                Task<FileSlot> waiting;
                lock (_lock)
                {
                    Fill();
                    if (_head < _units.Count) break;
                    if (_integrated == _plans.Count) return null; // every file planned and taken
                    waiting = _plans[_integrated];
                }
                await waiting.ConfigureAwait(false); // a failure to open or plan surfaces here, in order
            }
            Unit unit;
            lock (_lock) unit = _units[_head];
            RecordBatch[] batches = await unit.Task!.ConfigureAwait(false);
            lock (_lock)
            {
                unit.Task = null;
                _inFlight -= unit.Bytes;
                _units[_head++] = null!;
                if (unit.File.Taken()) _live.Remove(unit.File);
            }
            return batches;
        }

        /// <summary>
        /// Under the lock: joins the plans completed in order, starts the units the window allows, and starts planning
        /// more files while the units known ahead run short.
        /// </summary>
        private void Fill()
        {
            if (_disposed) return;
            while (_integrated < _plans.Count && _plans[_integrated].IsCompletedSuccessfully)
            {
                FileSlot slot = _plans[_integrated].Result;
                _plans[_integrated++] = null!;
                if (slot.Units == 0)
                {
                    slot.Dispose();
                    continue;
                }
                _live.Add(slot);
                for (int u = 0; u < slot.Units; u++)
                {
                    _units.Add(new Unit(slot, u, slot.Plan.Reader.UnitBytes(u)));
                    _plannedRows += slot.Plan.Reader.UnitRows(u);
                }
            }
            int ahead = _owner.UnitsAhead;
            while (_started < _units.Count && (_started == _head || _started - _head < ahead && _inFlight + _units[_started].Bytes <= _budget.Bytes))
            {
                Unit unit = _units[_started++];
                _inFlight += unit.Bytes;
                CancellationToken token = _cancel.Token;
                // Small units decode their columns one after another, units side by side on the scheduler; large
                // ones (few at a time), and the last units when fewer than the threads are left, their columns side
                // by side.
                bool parallel = unit.Bytes >= ParallelColumnsFrom
                    || _units.Count - _started < _budget.Threads && _integrated == _plans.Count && _nextFile >= _owner._dataset.Files.Count;
                _peakBytes = Math.Max(_peakBytes, _inFlight);
                _peakUnits = Math.Max(_peakUnits, _started - _head);
                unit.Task = parallel
                    ? Task.Run(() => _owner.DecodeAsync(unit.File, unit.Index, parallel: true, token), token)
                    : Task.Factory.StartNew(() => _owner.DecodeAsync(unit.File, unit.Index, parallel: false, token), token,
                        TaskCreationOptions.DenyChildAttach, _budget.Schedulers.ConcurrentScheduler).Unwrap();
            }
            // Files open: those planning or planned and not joined, and those joined with units not taken.
            while (_plans.Count - _integrated + _live.Count < _owner.FilesInFlight && _units.Count - _head < ahead && !(_plannedRows >= _maxRows)
                   && Next() is int file)
            {
                _plans.Add(Plan(file));
                _peakFiles = Math.Max(_peakFiles, _plans.Count - _integrated + _live.Count);
            }
        }

        /// <summary>The next file to plan (read, or counted for row numbers), past those left out; null when none is left.</summary>
        private int? Next()
        {
            ParquetDatasetReader owner = _owner;
            for (; _nextFile < owner._dataset.Files.Count; _nextFile++)
            {
                if (owner._read[_nextFile] || owner._options.RowIndexColumn is not null) return _nextFile++;
            }
            return null;
        }

        /// <summary>Plans a file on the thread pool: opened ahead (in order), numbered after the files before it.</summary>
        private Task<FileSlot> Plan(int index)
        {
            Task<FileSlot> planning = Planning(index);
            // Once planned, its units may start (when it is next in order) and more files may be planned.
            planning.ContinueWith(_ =>
            {
                lock (_lock) Fill();
            }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
            return planning;
        }

        private Task<FileSlot> Planning(int index)
        {
            Task<ParquetFile> opening = _ahead.Take(index);
            Task<long> previous = _before;
            var after = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            _before = after.Task;
            CancellationToken token = _cancel.Token;
            ParquetDatasetReader owner = _owner;
            // Started without the token: every planning runs and settles the row count the next file awaits (a
            // planning cancelled before it ran would leave the next waiting).
            return Task.Run(async () =>
            {
                ParquetFile? file = null;
                FilePlan? plan = null;
                try
                {
                    file = await opening.ConfigureAwait(false);
                    long before = await previous.ConfigureAwait(false);
                    after.TrySetResult(before + RowCount(file));
                    token.ThrowIfCancellationRequested();
                    if (!owner._read[index])
                    {
                        file.Dispose();
                        return new FileSlot(index, null, null, 0);
                    }
                    plan = owner.Plan(file, index, before, _maxRows, _budget);
                    int units;
                    try
                    {
                        // With a filter, planning may read page indexes and Bloom filters: requests too, a place held
                        // only while it runs (not while waiting for the previous file above, which may wait in turn
                        // for a place).
                        RequestGate? gate = owner._options.Filter is null ? null : _budget.Requests;
                        using RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(index, token).ConfigureAwait(false);
                        units = await plan.Reader.PlanUnitsAsync(useAsync: true, token).ConfigureAwait(false);
                    }
                    catch (ParquetFormatException e)
                    {
                        throw ParquetDataset.InFile(owner.PathOf(index), e);
                    }
                    catch (IOException e) when (ParquetDataset.Unnamed(e, owner.PathOf(index)))
                    {
                        throw ParquetDataset.InFile(owner.PathOf(index), e);
                    }
                    return new FileSlot(index, file, plan, units);
                }
                catch (Exception e)
                {
                    after.TrySetException(e); // the files after this one fail with it (when reached)
                    plan?.Dispose();
                    file?.Dispose();
                    throw;
                }
            });
        }

        public void Dispose()
        {
            Task<FileSlot>[] plans;
            FileSlot[] live;
            lock (_lock)
            {
                _disposed = true;
                _cancel.Cancel();
                plans = [.. _plans.Skip(_integrated)];
                live = [.. _live];
            }
            foreach (Task<FileSlot> plan in plans)
            {
                try
                {
                    plan.GetAwaiter().GetResult();
                }
                catch
                {
                    // abandoned: its error no longer matters
                }
            }
            for (int u = _head; u < _units.Count; u++)
            {
                if (_units[u]?.Task is not { } task) continue;
                try
                {
                    foreach (RecordBatch batch in task.GetAwaiter().GetResult()) batch?.Dispose();
                }
                catch
                {
                    // abandoned
                }
            }
            foreach (Task<FileSlot> plan in plans)
            {
                if (plan.IsCompletedSuccessfully) plan.Result.Dispose();
            }
            foreach (FileSlot slot in live) slot.Dispose();
            _ahead.Dispose();
            _budget.Dispose();
            _cancel.Dispose();
            _owner.PeakInFlight = (_peakBytes, _peakUnits, _peakFiles);
            _owner.PeakRequests = _budget.Requests?.PeakInside ?? 0;
            _owner.PeakDecoding = _budget.PeakDecoding;
        }

        /// <summary>The leaves of a type (a struct's fields, a list's elements, a map's keys and values).</summary>
        private static int Leaves(IArrowType type) => type switch
        {
            StructType s => s.Fields.Sum(f => Leaves(f.DataType)),
            ListType l => Leaves(l.ValueDataType),
            FixedSizeListType l => Leaves(l.ValueDataType),
            MapType m => Leaves(m.KeyField.DataType) + Leaves(m.ValueField.DataType),
            _ => 1,
        };
    }

    /// <summary>Decodes a unit of a file and converts its batches (on the window's threads); errors name the file.</summary>
    private async Task<RecordBatch[]> DecodeAsync(FileSlot slot, int unit, bool parallel, CancellationToken cancellationToken)
    {
        RecordBatch[] decoded;
        try
        {
            decoded = await slot.Plan.Reader.DecodeUnitAsync(unit, parallel, cancellationToken).ConfigureAwait(false);
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(slot.Index), e);
        }
        catch (IOException e) when (ParquetDataset.Unnamed(e, PathOf(slot.Index)))
        {
            throw ParquetDataset.InFile(PathOf(slot.Index), e);
        }
        List<RecordBatch> converted = [];
        int b = 0;
        try
        {
            for (; b < decoded.Length; b++)
            {
                RecordBatch batch = decoded[b];
                decoded[b] = null!;
                if (Convert(batch, slot.Plan, slot.Index) is { } done) converted.Add(done);
            }
            return [.. converted];
        }
        catch
        {
            for (; b < decoded.Length; b++) decoded[b]?.Dispose();
            foreach (RecordBatch batch in converted) batch.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Files opened ahead of the one taken (up to <paramref name="count"/> of those to open: the files read, and the
    /// others when their row counts are needed); those not taken are disposed once opened. Taken in order, by one
    /// caller at a time. Disposing it cancels the opens still running (a store that hangs does not hold them).
    /// </summary>
    /// <summary>
    /// Opens the files ahead of the one taken, up to <c>count</c> not taken yet, each open a request through
    /// <c>gate</c> when there is one (the lower files first).
    /// </summary>
    private sealed class Ahead(ParquetDatasetReader owner, int count, RequestGate? gate, CancellationToken cancellationToken) : IDisposable
    {
        private readonly Dictionary<int, Task<ParquetFile>> _started = [];
        private readonly CancellationTokenSource _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        private int _next;

        private CancellationToken Token => _cancel.Token;

        private bool Needed(int index) => owner._read[index] || owner._options.RowIndexColumn is not null;

        /// <summary>The file at <paramref name="index"/> (files are taken in order), the next ones started meanwhile.</summary>
        public Task<ParquetFile> Take(int index)
        {
            int files = owner._dataset.Files.Count;
            _next = Math.Max(_next, index);
            for (; _next < files && _started.Count < count; _next++)
            {
                if (!Needed(_next)) continue;
                int file = _next;
                CancellationToken token = Token;
                _started[file] = Open(file, token);
            }
            CancellationToken taken = Token;
            return _started.Remove(index, out Task<ParquetFile>? task) ? task : Open(index, taken);
        }

        /// <summary>
        /// Opens a file on the thread pool. Through the gate, it queues here, as files are taken (in order), not on the
        /// thread pool, which may start later files first; it then waits there, whatever the token, so that a place
        /// given is given back.
        /// </summary>
        private Task<ParquetFile> Open(int file, CancellationToken token)
        {
            if (gate is null) return Task.Run(() => owner.OpenAsync(file, useAsync: true, token).AsTask(), token);
            ValueTask<RequestGate.Lease> entering = gate.EnterAsync(file, token);
            return Task.Run(async () =>
            {
                using RequestGate.Lease lease = await entering.ConfigureAwait(false);
                return await owner.OpenAsync(file, useAsync: true, token).ConfigureAwait(false);
            }, CancellationToken.None);
        }

        public void Dispose()
        {
            _cancel.Cancel();
            Task<ParquetFile>[] pending = [.. _started.Values];
            foreach (Task<ParquetFile> task in pending)
                task.ContinueWith(static t =>
                {
                    if (t.IsCompletedSuccessfully) t.Result.Dispose();
                    else _ = t.Exception; // observed: a file not read need not open
                }, CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            _started.Clear();
            // Disposed once the opens it cancelled are done with its token (those taken were awaited by their takers).
            CancellationTokenSource cancel = _cancel;
            Task.WhenAll(pending).ContinueWith(_ => cancel.Dispose(), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }
}

