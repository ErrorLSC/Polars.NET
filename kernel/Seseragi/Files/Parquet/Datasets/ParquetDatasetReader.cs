using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Reads a dataset's files in order, each as <see cref="ParquetFile.OpenReader(ParquetReadOptions?)"/> does with the
/// options given, into batches of one schema: batches never span files. With
/// <see cref="ParquetReadOptions.MaxDegreeOfParallelism"/> above 1 the row groups of several files are decoded side by
/// side ahead of the consumer, on that many threads and within one reader's in-flight memory, files opened and
/// planned ahead (<see cref="DatasetOptions.MaxFilesInFlight"/>), requests to a remote store bounded apart
/// (<see cref="DatasetOptions.MaxRequestsInFlight"/>, those of the files read first let in first); with 1, from
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
/// <see cref="DatasetOptions.AllowLossyCasts"/>; else the read fails naming the file and the column. A column
/// or struct field a file lacks is an error, or nulls with <see cref="DatasetOptions.AllowMissingColumns"/>
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

    /// <inheritdoc cref="DatasetLimits.RemoteUnitsAheadPerThread"/>
    internal const int RemoteUnitsAheadPerThread = DatasetLimits.RemoteUnitsAheadPerThread;

    /// <inheritdoc cref="DatasetLimits.RemoteMinUnitsAhead"/>
    internal const int RemoteMinUnitsAhead = DatasetLimits.RemoteMinUnitsAhead;

    /// <summary>From how many uncompressed bytes a unit decodes its columns side by side.</summary>
    internal const long ParallelColumnsFrom = 8L << 20;

    /// <inheritdoc cref="DatasetLimits.MinFilesAhead"/>
    internal const int MinFilesAhead = DatasetLimits.MinFilesAhead;

    /// <inheritdoc cref="DatasetLimits.RemoteRequests"/>
    internal const int RemoteRequests = DatasetLimits.RemoteRequests;

    /// <inheritdoc cref="DatasetLimits.RemoteFilesHeld"/>
    internal const int RemoteFilesHeld = DatasetLimits.RemoteFilesHeld;

    private readonly ParquetDataset _dataset;
    private readonly ParquetReadOptions _options;
    private readonly DatasetShape _shape;                 // the columns, the filter's split and the batches' conversion
    private readonly bool[] _read;                        // the files a filter over partition columns leaves

    internal ParquetDatasetReader(ParquetDataset dataset, ParquetReadOptions options)
    {
        options.Validate();
        if (options.RowGroups is not null)
            throw new ArgumentException("Row groups are numbered per file: a dataset reads them all (select rows per file with ParquetDatasetFile.Rows).", nameof(options));
        if (options.Rows is not null)
            throw new ArgumentException("Rows are numbered per file: select them with ParquetDatasetFile.Rows.", nameof(options));
        if (options.Filter is { } given) options = options with { Filter = FilterShapes.normalize(given) };
        _dataset = dataset;
        _options = options;
        _shape = new DatasetShape(dataset.Options, dataset.Partitions, dataset.PartitionField, dataset.ColumnsFor(options), dataset.Files.Count,
            options.Columns, options.ColumnPaths, options.Filter, options.FilterRows, options.RowIndexColumn, options.ViewTypes,
            PathOf, static (message, inner) => new ParquetFormatException(message, inner));
        _read = _shape.Read;
        Schema = _shape.Output;
        FiltersExactly = _shape.FiltersExactly;
    }

    /// <summary>How many units are decoded at once ahead of the consumer.</summary>
    private int UnitsAhead => DatasetLimits.UnitsAhead(_options.MaxDegreeOfParallelism, Remote);

    /// <summary>Whether the files come from a store whose requests wait on the network (not local files or memory).</summary>
    private bool Remote => DatasetLimits.Remote(_dataset.Store);

    /// <inheritdoc cref="DatasetLimits.FilesInFlight"/>
    private int FilesInFlight => DatasetLimits.FilesInFlight(_dataset.Options, Remote, UnitsAhead);

    /// <inheritdoc cref="DatasetLimits.RequestsInFlight"/>
    private int? RequestsInFlight => DatasetLimits.RequestsInFlight(_dataset.Options, Remote);

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
        using DictionaryUnifier? unifier = _shape.NewUnifier(); // per reading: values numbered as they come
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
                        if (_shape.Convert(batch, plan.Converted, plan.FilterAfter, i) is { } done) yield return _shape.Cut(done, ref remaining, unifier);
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
        using DictionaryUnifier? unifier = _shape.NewUnifier(); // per reading: values numbered as they come
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
                        if (_shape.Convert(batch, plan.Converted, plan.FilterAfter, i) is { } done) yield return _shape.Cut(done, ref remaining, unifier);
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
        var units = new Units(this);
        var window = new OrderedUnits<FileSlot, UnitBatches>(units, units.Limits, default);
        try
        {
            long? remaining = _options.MaxRows;
            using DictionaryUnifier? unifier = _shape.NewUnifier(); // per reading: values numbered as they come
            while (remaining != 0 && window.NextAsync().GetAwaiter().GetResult() is { } unit)
            {
                RecordBatch?[] batches = unit.Batches;
                int i = 0;
                try
                {
                    for (; i < batches.Length && remaining != 0; i++)
                    {
                        RecordBatch batch = batches[i]!;
                        batches[i] = null;
                        yield return _shape.Cut(batch, ref remaining, unifier);
                    }
                }
                finally
                {
                    unit.Dispose(); // the batches not delivered
                }
            }
        }
        finally
        {
            window.Dispose();
            Ended(units, window);
        }
    }

    /// <summary>A windowed read is over: its threads and opens let go, its peaks kept (for tests).</summary>
    private void Ended(Units units, OrderedUnits<FileSlot, UnitBatches> window)
    {
        units.Dispose();
        PeakInFlight = window.Peak;
        PeakRequests = units.Budget.Requests?.PeakInside ?? 0;
        PeakDecoding = units.Budget.PeakDecoding;
    }

    /// <inheritdoc cref="ReadWindowed"/>
    private async IAsyncEnumerable<RecordBatch> ReadWindowedAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var units = new Units(this);
        var window = new OrderedUnits<FileSlot, UnitBatches>(units, units.Limits, cancellationToken);
        try
        {
            long? remaining = _options.MaxRows;
            using DictionaryUnifier? unifier = _shape.NewUnifier(); // per reading: values numbered as they come
            while (remaining != 0 && await window.NextAsync().ConfigureAwait(false) is { } unit)
            {
                RecordBatch?[] batches = unit.Batches;
                int i = 0;
                try
                {
                    for (; i < batches.Length && remaining != 0; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        RecordBatch batch = batches[i]!;
                        batches[i] = null;
                        yield return _shape.Cut(batch, ref remaining, unifier);
                    }
                }
                finally
                {
                    unit.Dispose(); // the batches not delivered
                }
            }
        }
        finally
        {
            await window.DisposeAsync().ConfigureAwait(false);
            Ended(units, window);
        }
    }

    /// <summary>Reads the batches as an Arrow stream (each batch disposed when the next is read).</summary>
    public Apache.Arrow.Ipc.IArrowArrayStream ReadArrowStream() => new ParquetReader.OwningBatchStream(Schema, ReadBatches());

    /// <summary>
    /// The number of rows <see cref="ReadBatches"/> delivers. Without a filter over the files' columns (one over
    /// partition columns only decides which files are read) it comes from the footers (the row groups' counts, or the
    /// rows <see cref="ParquetDatasetFile.Rows"/> selects), up to <see cref="ParquetReadOptions.MaxRows"/>: no data is
    /// read. Otherwise the rows are read and counted, the filter's columns only.
    /// </summary>
    /// <exception cref="ParquetFormatException">A file is damaged or is not a Parquet file (the message names it).</exception>
    /// <exception cref="FileNotFoundException">A file named does not exist.</exception>
    public long CountRows() => CountRowsAsync(useAsync: false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="CountRows"/>
    public ValueTask<long> CountRowsAsync(CancellationToken cancellationToken = default) => CountRowsAsync(useAsync: true, cancellationToken);

    private async ValueTask<long> CountRowsAsync(bool useAsync, CancellationToken cancellationToken)
    {
        if (_shape.RowFilter is { } filter) return await CountReadAsync(filter, useAsync, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<ParquetDatasetFile> files = _dataset.Files;
        ParquetDatasetFile[] all = [.. files];
        var footers = new ParquetMetadata?[files.Count];
        long rows = 0;
        // Without a limit, every footer at once (FootersAtOnce in flight); with one, in file order a window at a time:
        // a limit reached reads no footer of the files after it.
        int window = _options.MaxRows is null ? Math.Max(1, files.Count) : ParquetDataset.FootersAtOnce;
        for (int start = 0; start < files.Count && !(rows >= _options.MaxRows); start += window)
        {
            int end = Math.Min(files.Count, start + window);
            bool[] needed = new bool[files.Count];
            for (int i = start; i < end; i++)
            {
                if (_read[i] && (footers[i] = _dataset.Footer(i)) is null) needed[i] = true;
            }
            if (needed.Contains(true))
            {
                ValueTask reading = ParquetDataset.ReadFootersAsync(_dataset.Store, all, footers, _dataset.Footers, _dataset.Interner, _dataset.Options.Decryption,
                    cancellationToken, needed);
                if (useAsync) await reading.ConfigureAwait(false);
                else reading.AsTask().GetAwaiter().GetResult();
            }
            for (int i = start; i < end; i++)
            {
                if (!_read[i]) continue;
                long total = RowCount(footers[i]!);
                if (files[i].Rows is { } selected)
                {
                    if (selected.End > total)
                        throw new ArgumentOutOfRangeException("options", $"{PathOf(i)}: the rows selected reach row {selected.End - 1}; the file has {total} rows.");
                    total = selected.RowCount;
                }
                rows += total;
            }
        }
        return _options.MaxRows is long most ? Math.Min(rows, most) : rows;

        // As a file's reader numbers rows (its row groups' counts).
        static long RowCount(ParquetMetadata footer) =>
            footer.Thrift is { } thrift ? thrift.RowGroups.Sum(g => Math.Max(0, g.NumRows)) : footer.RowGroups.Sum(g => Math.Max(0, g.RowCount));
    }

    /// <summary>The rows a read delivers, counted by reading one column the filter reads (late materialization reads no other).</summary>
    private async ValueTask<long> CountReadAsync(ScalarExpr filter, bool useAsync, CancellationToken cancellationToken)
    {
        string? column = ScalarExprOps.collectColumns(filter).FirstOrDefault(FromFiles)
            ?? _dataset.FileSchema.FieldsList.Select(f => f.Name).FirstOrDefault(FromFiles);
        var reader = new ParquetDatasetReader(_dataset, _options with { Columns = column is null ? null : [column], ColumnPaths = null, RowIndexColumn = null });
        long rows = 0;
        if (useAsync)
        {
            await foreach (RecordBatch batch in reader.ReadBatchesAsync(cancellationToken).ConfigureAwait(false)) rows += Counted(batch);
        }
        else
        {
            foreach (RecordBatch batch in reader.ReadBatches()) rows += Counted(batch);
        }
        return rows;

        bool FromFiles(string name) => _shape.Columns.ContainsKey(name) && !(_dataset.Partitions?.IndexOf(name) >= 0);

        static long Counted(RecordBatch batch)
        {
            using (batch) return batch.Length;
        }
    }

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
    private sealed class FilePlan(ParquetReader reader, DatasetShape.Converted converted, bool filterAfter) : IDisposable
    {
        public ParquetReader Reader { get; } = reader;

        /// <summary>How the reader's columns convert: per selected column from files, per column the dataset's filter reads.</summary>
        public DatasetShape.Converted Converted { get; } = converted;

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
        var (fileFilter, complete) = _shape.FileFilter(index, shape.Missing, shape.Dropped, shape.Has);

        bool after = FiltersExactly && _shape.FiltersAfter && !complete;
        ParquetReader reader = Open(file, index, before, after ? null : remaining, shape, fileFilter, after, shared);
        if (FiltersExactly && _shape.FiltersAfter && !after && !reader.FiltersExactly)
        {
            // The file's filtering is not exact: read again with what the dataset's filter needs.
            reader.Dispose();
            after = true;
            reader = Open(file, index, before, null, shape, fileFilter, after, shared);
        }
        try
        {
            int variant = after ? 1 : 0;
            DatasetShape.Converted converted = Volatile.Read(ref shape.Converted[variant]) ?? _shape.Conversions(reader.Schema, after, path);
            Volatile.Write(ref shape.Converted[variant], converted); // the same for every file of the shape
            if (after) Interlocked.Increment(ref _filteredAfter);
            return new FilePlan(reader, converted, after);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

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
        public readonly DatasetShape.Converted?[] Converted = new DatasetShape.Converted?[2];

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

        DatasetShape.FileColumns columns = _shape.ColumnsOf(has, path);
        return new ShapePlan { Has = has, Missing = columns.Missing, Dropped = columns.Dropped, Lists = columns.Lists };
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

    // ---- files ----------------------------------------------------------------------------------------------------

    /// <summary>Opens a file (its footer from before when there is one, kept for the next reads when worth it).</summary>
    private async ValueTask<ParquetFile> OpenAsync(int index, bool useAsync, CancellationToken ct)
    {
        return await ParquetDataset.OpenFileAsync(_dataset.Store, _dataset.Files[index], _dataset.Footer(index), _dataset.Options.Decryption, useAsync, ct, _dataset.Footers, index, _dataset.Interner).ConfigureAwait(false);
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

    /// <summary>A file the window reads (its units: its row groups to read), freed once its last unit is taken.</summary>
    private sealed class FileSlot(int index, ParquetFile? file, FilePlan? plan, int units) : UnitGroup(index)
    {
        public FilePlan Plan => plan!;

        public override (int Found, bool Over) Progress => (units, true);

        public override long UnitBytes(int unit) => plan!.Reader.UnitBytes(unit);

        public override long UnitRows(int unit) => plan!.Reader.UnitRows(unit);

        protected override void Free()
        {
            plan?.Dispose();
            file?.Dispose();
        }
    }

    /// <summary>A unit's batches, converted: those the consumer has not taken (still in the array) are freed with it.</summary>
    private sealed class UnitBatches(RecordBatch?[] batches) : IDisposable
    {
        public RecordBatch?[] Batches => batches;

        public void Dispose()
        {
            for (int i = 0; i < batches.Length; i++)
            {
                batches[i]?.Dispose();
                batches[i] = null;
            }
        }
    }

    /// <summary>
    /// The dataset's files for the window (<see cref="OrderedUnits{TGroup, TResult}"/>): opened ahead (in order) and
    /// planned side by side (a file's row numbers follow the earlier files' counts, known from their footers), their
    /// row groups decoded on one scheduler within one reader's budget (the window's bytes), up to
    /// <see cref="UnitsAhead"/> beyond the one the consumer waits for. Planning stops once the row groups planned hold
    /// the rows asked for, when no row is filtered.
    /// </summary>
    private sealed class Units : IUnitSource<FileSlot, UnitBatches>, IDisposable
    {
        private readonly ParquetDatasetReader _owner;
        private readonly long? _maxRows;                     // the rows to plan, when no row is filtered
        private Ahead? _ahead;                               // made by the first plan, with the window's token
        private Task<long> _before = Task.FromResult(0L);   // the rows of the files before the next one planned
        private int _nextFile;

        public Units(ParquetDatasetReader owner)
        {
            _owner = owner;
            int threads = Math.Max(1, owner._options.MaxDegreeOfParallelism);
            int leaves = Math.Max(1, owner._shape.Leaves);
            Budget = new ReadBudget(threads, owner._options.InFlightBytes, (2 * leaves + 4) * owner.UnitsAhead, writesPastCaches: true,
                owner.NewRequestGate());
            // MaxRows bounds the planning only when no row is filtered (the consumer cuts in any case).
            _maxRows = !owner._options.FilterRows || owner._options.Filter is null ? owner._options.MaxRows : null;
            Limits = new OrderedUnitsLimits(owner.UnitsAhead, Budget.Bytes, owner.FilesInFlight, _maxRows, FewUnits: Budget.Threads);
        }

        public ReadBudget Budget { get; }

        public OrderedUnitsLimits Limits { get; }

        public bool GroupsLeft => _nextFile < _owner._dataset.Files.Count;

        /// <summary>The next file to plan (read, or counted for row numbers), past those left out.</summary>
        public int? NextGroup()
        {
            ParquetDatasetReader owner = _owner;
            for (; _nextFile < owner._dataset.Files.Count; _nextFile++)
            {
                if (owner._read[_nextFile] || owner._options.RowIndexColumn is not null) return _nextFile++;
            }
            return null;
        }

        /// <summary>Plans a file on the thread pool: opened ahead (in order), numbered after the files before it.</summary>
        public Task<FileSlot> Plan(int index, CancellationToken token)
        {
            _ahead ??= new Ahead(_owner, 0, Budget.Requests, token); // files open as they are planned (counted by the window)
            Task<ParquetFile> opening = _ahead.Take(index);
            Task<long> previous = _before;
            var after = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            _before = after.Task;
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
                    plan = owner.Plan(file, index, before, _maxRows, Budget);
                    int units;
                    try
                    {
                        // With a filter, planning may read page indexes and Bloom filters: requests too, a place held
                        // only while it runs (not while waiting for the previous file above, which may wait in turn
                        // for a place).
                        RequestGate? gate = owner._options.Filter is null ? null : Budget.Requests;
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

        /// <summary>
        /// Small units decode their columns one after another, units side by side on the scheduler; large ones (few at
        /// a time), and the last units when fewer than the threads are left, their columns side by side.
        /// </summary>
        public Task<UnitBatches> Start(FileSlot slot, int unit, bool last, CancellationToken token)
        {
            ParquetDatasetReader owner = _owner;
            bool parallel = slot.UnitBytes(unit) >= ParallelColumnsFrom || last;
            return parallel
                ? Task.Run(() => owner.DecodeAsync(slot, unit, parallel: true, token), token)
                : Task.Factory.StartNew(() => owner.DecodeAsync(slot, unit, parallel: false, token), token,
                    TaskCreationOptions.DenyChildAttach, Budget.Schedulers.ConcurrentScheduler).Unwrap();
        }

        public UnitBatches Empty(FileSlot slot) => new([]);

        public bool Keeps(UnitBatches result) => false;

        /// <summary>Once the window is disposed: the opens ahead cancelled, the threads and buffers let go.</summary>
        public void Dispose()
        {
            _ahead?.Dispose();
            Budget.Dispose();
        }
    }

    /// <summary>Decodes a unit of a file and converts its batches (on the window's threads); errors name the file.</summary>
    private async Task<UnitBatches> DecodeAsync(FileSlot slot, int unit, bool parallel, CancellationToken cancellationToken)
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
                if (_shape.Convert(batch, slot.Plan.Converted, slot.Plan.FilterAfter, slot.Index) is { } done) converted.Add(done);
            }
            return new UnitBatches([.. converted]);
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

