using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Files.Csv.Reading;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// Reads a dataset's files in order, each as <see cref="CsvReader"/> reads a file, into batches of one schema (batches
/// never span files): the files' chunks of records decoded side by side ahead of the consumer, files opened ahead (their
/// headers read: <see cref="DatasetOptions.MaxFilesInFlight"/>), requests to a remote store bounded apart
/// (<see cref="DatasetOptions.MaxRequestsInFlight"/>, those of the files read first let in first). A compressed file (or a
/// stream-like source) is read in order, its chunks found as it is read.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>The dataset's options decide how the files are written and the columns: the read options'
/// <see cref="CsvReadOptions.Schema"/> and <see cref="CsvReadOptions.SchemaOverrides"/> must be null; what describes the
/// files (separator, quote, header, skips, null values, encoding, inference …) is the dataset's
/// <see cref="CsvDatasetOptions.Format"/>, the read options' is not used.</item>
/// <item><see cref="CsvReadOptions.Columns"/> names the dataset's columns (all of them when null): columns from files
/// and partition columns. Batches hold the columns from files in the dataset's order (a partition column of the same
/// name in its place, holding the path's value), then the other partition columns, then the file path.</item>
/// <item><see cref="CsvReadOptions.Filter"/>: its conjuncts over partition columns only decide which files are read
/// (exactly, on the files' partition values: the others are not opened); with <see cref="CsvReadOptions.FilterRows"/> the
/// others filter the rows of every file read as they are decoded, partition columns replaced by the file's values.</item>
/// <item><see cref="CsvReadOptions.RowIndexColumn"/> numbers the rows across the files from
/// <see cref="CsvReadOptions.RowIndexOffset"/>, in the order listed, as if they were one (the numbers of each file follow
/// the previous files' records, those of files left out included: their records are read and counted, so an error in
/// them fails the read): the first column.</item>
/// <item><see cref="CsvReadOptions.MaxRows"/> counts the rows delivered across the files.</item>
/// </list>
/// Errors in a file name it, and the row and line in it.
/// </remarks>
public sealed class CsvDatasetReader : IDisposable, IAsyncDisposable
{
    private readonly CsvDataset _dataset;
    private readonly CsvReadOptions _options;
    private readonly DatasetShape _shape; // the columns, the filter's split, partition values and the file path added
    private readonly bool[] _read;        // the files a filter over partition columns leaves
    private readonly bool _indexed;       // a row index is asked: files left out are counted
    private int _filteredAfter;

    internal CsvDatasetReader(CsvDataset dataset, CsvReadOptions options)
    {
        if (options.Schema is not null || options.SchemaOverrides is not null)
            throw new ArgumentException("The dataset's options decide the columns: give the read options no schema (nor schema overrides).", nameof(options));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BatchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxDegreeOfParallelism);
        if (options.MaxRows is < 0) throw new ArgumentOutOfRangeException(nameof(options), "MaxRows cannot be negative.");
        if (options.RowIndexOffset < 0) throw new ArgumentOutOfRangeException(nameof(options), "RowIndexOffset cannot be negative.");
        if (options.Filter is { } given) options = options with { Filter = FilterShapes.normalize(given) };
        _dataset = dataset;
        _options = options;
        _indexed = options.RowIndexColumn is not null;
        _shape = new DatasetShape(dataset.Options, dataset.Partitions, dataset.PartitionField, dataset.FileSchema, dataset.Files.Count,
            options.Columns, null, options.Filter, options.FilterRows, options.RowIndexColumn, views: true,
            PathOf, static (message, inner) => new CsvFormatException(message, inner));
        _read = _shape.Read;
        Schema = _shape.Output;
        FiltersExactly = _shape.FiltersExactly;
    }

    /// <summary>The schema of the batches: the columns read (nullable), the file path if asked.</summary>
    public Schema Schema { get; }

    /// <summary>
    /// Whether, with <see cref="CsvReadOptions.FilterRows"/>, the batches hold exactly the rows the filter keeps: when the
    /// filter lowers exactly over the dataset's types and its conjuncts over partition columns are decided exactly
    /// (files whose own filtering is not exact are filtered again once their partition values are added).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The files a filter over partition columns leaves (for tests).</summary>
    internal IReadOnlyList<bool> FilesRead => _read;

    /// <summary>The files whose rows the dataset filtered after decoding them (for tests).</summary>
    internal int FilesFilteredAfter => Volatile.Read(ref _filteredAfter);

    /// <summary>The most requests to the store at once in the last read (0 when not bounded; for tests).</summary>
    internal int PeakRequests { get; private set; }

    /// <summary>The most bytes and chunks read ahead at once, and files open at once, in the last read (for tests).</summary>
    internal (long Bytes, int Units, int Files) PeakInFlight { get; private set; }

    /// <summary>Reads the batches, in file order.</summary>
    /// <exception cref="CsvFormatException">A file cannot be read as the dataset's files are written, or its columns are not the dataset's (the message names it and the line).</exception>
    /// <exception cref="CsvValueException">A value its column's type cannot hold (the message names the file, the row and the line).</exception>
    /// <exception cref="FileNotFoundException">A file named does not exist.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        IAsyncEnumerator<RecordBatch> batches = ReadCoreAsync(useAsync: false, default).GetAsyncEnumerator();
        try
        {
            while (DatasetFiles.Sync(batches.MoveNextAsync())) yield return batches.Current;
        }
        finally
        {
            DatasetFiles.Sync(Done(batches.DisposeAsync()));
        }

        static async ValueTask<bool> Done(ValueTask disposing)
        {
            await disposing.ConfigureAwait(false);
            return true;
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public IAsyncEnumerable<RecordBatch> ReadBatchesAsync(CancellationToken cancellationToken = default) => ReadCoreAsync(useAsync: true, cancellationToken);

    /// <summary>Nothing is held between reads.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private string PathOf(int file) => _dataset.Files[file].Path;

    private bool Remote => DatasetLimits.Remote(_dataset.Store);

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(bool useAsync, [EnumeratorCancellation] CancellationToken ct)
    {
        RequestGate? gate = DatasetLimits.RequestsInFlight(_dataset.Options, Remote) is int requests ? new RequestGate(requests) : null;
        int ahead = DatasetLimits.UnitsAhead(_options.MaxDegreeOfParallelism, Remote);
        long bytes = 2L * ahead * _options.UnitBytes;
        var window = new OrderedUnits<FileSlot, Output>(new Units(this, gate),
            new OrderedUnitsLimits(ahead, bytes, DatasetLimits.FilesInFlight(_dataset.Options, Remote, ahead)), ct);
        try
        {
            long? left = _options.MaxRows;
            int file = -1;
            CsvReader.Delivery? delivery = null;
            long before = _options.RowIndexOffset; // the row number of the file's first record (the records of the files before it)
            while (left != 0 && await window.NextAsync().ConfigureAwait(false) is { } taken)
            {
                try
                {
                    if (taken.File != file)
                    {
                        before += delivery?.Records ?? 0;
                        file = taken.File;
                        // Rows cut by the file's reader only when no rows are dropped after converting (else by the dataset).
                        delivery = taken.Slot?.Reader.Deliver(taken.Slot.State.After || left is null ? long.MaxValue : left.Value, before);
                    }
                    if (taken.Unit is not { } unit || delivery is null) continue;
                    FileState state = taken.Slot!.State;
                    try
                    {
                        unit = await delivery.Check(taken.Index, unit, useAsync, ct, taken.Bytes).ConfigureAwait(false);
                    }
                    catch (Exception e) when (InFile(file, e) is var named && !ReferenceEquals(named, e))
                    {
                        throw named;
                    }
                    bool raw = taken.Redo(unit, delivery.Redone);
                    if (unit.End) continue;
                    while (left != 0 && delivery.Next(unit, out RecordBatch? next))
                    {
                        RecordBatch? batch = next;
                        if (state.CountOnly)
                        {
                            batch!.Dispose();
                            continue;
                        }
                        if (raw) batch = _shape.Convert(batch!, state.Converted!, state.After, file);
                        if (batch is null) continue;
                        yield return _shape.Cut(batch, ref left, null);
                    }
                    delivery.Done(unit);
                }
                finally
                {
                    taken.Dispose();
                }
            }
        }
        finally
        {
            await window.DisposeAsync().ConfigureAwait(false);
            (PeakInFlight, PeakRequests) = (window.Peak, gate?.PeakInside ?? 0);
        }
    }

    // ---- one file -------------------------------------------------------------------------------------------------

    /// <summary>
    /// What a file's reader decided once its header was read: whether it is only counted (left out by a filter over
    /// partition columns while a row index is asked), whether its rows are filtered after converting, how its batches
    /// convert to the dataset's.
    /// </summary>
    private sealed class FileState
    {
        public bool CountOnly { get; init; }

        public bool After { get; set; }

        public DatasetShape.Converted? Converted { get; set; }
    }

    /// <summary>Whether file <paramref name="index"/> is read: its rows delivered, or (a row index asked) its records counted.</summary>
    private bool Needed(int index) => _read[index] || _indexed;

    /// <summary>
    /// The options file <paramref name="index"/> is read with: the dataset's way of writing it, this read's batches and
    /// threads; its columns and filter decided by its names (<see cref="Resolve"/>).
    /// </summary>
    private CsvReadOptions FileOptions(int index, FileState state, bool forceAfter) => _dataset.Options.Format with
    {
        Schema = null,
        SchemaOverrides = null,
        Columns = null,
        Filter = null,
        FilterRows = _options.FilterRows && !state.CountOnly,
        MaxRows = null,
        RowIndexColumn = state.CountOnly ? null : _options.RowIndexColumn,
        RowIndexOffset = 0,
        IgnoreErrors = _options.IgnoreErrors,
        BatchSize = _options.BatchSize,
        MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism,
        MemoryMap = _options.MemoryMap,
        UnitBytes = _options.UnitBytes,
        Dataset = new CsvDatasetColumns(_dataset.FileSchema, _dataset.Options.IgnoreExtraColumns, _dataset.InferredFrom,
            names => Resolve(index, names, state, forceAfter), ByPosition: !_dataset.Options.Format.HasHeader && _dataset.Options.Schema is not null),
    };

    /// <summary>
    /// The columns file <paramref name="index"/> reads and its row filter, from its names: the columns selected it has
    /// (with the dataset's filter after converting, those the filter reads too), the filter with its partition values
    /// and the columns it lacks put in (a dataset's column it lacks is an error unless AllowMissingColumns). A file
    /// only counted reads no column.
    /// </summary>
    private (IReadOnlyList<string> Columns, ScalarExpr? Filter) Resolve(int index, string[] names, FileState state, bool forceAfter)
    {
        if (state.CountOnly) return ([], null);
        Dictionary<string, Field> has = new(StringComparer.Ordinal);
        foreach (string name in names)
        {
            if (_dataset.FileSchema.GetFieldByName(name) is { } field) has.TryAdd(name, field);
        }
        DatasetShape.FileColumns columns = _shape.ColumnsOf(has, PathOf(index));
        var (fileFilter, complete) = _shape.FileFilter(index, columns.Missing, columns.Dropped, has);
        state.After = forceAfter || (FiltersExactly && _shape.FiltersAfter && !complete);
        return (columns.Lists[state.After ? 1 : 0].Columns, _options.FilterRows ? fileFilter : null);
    }

    /// <summary>A unit's error placed in its file.</summary>
    private Exception InFile(int file, Exception e) => CsvDataset.Named(PathOf(file), e) ?? e;

    /// <summary>
    /// What a unit gives the consumer: its decoded records (<see cref="Unit"/>; the batches already the dataset's unless
    /// decoded again by the consumer), with its bytes kept until it is taken (the file's formats may ask for it decoded
    /// again), or nothing for a file with no unit.
    /// </summary>
    private sealed class Output(FileSlot? slot, int file, CsvUnit? unit, TextUnitBytes? bytes, int index) : IDisposable
    {
        private CsvUnit? _unit = unit;
        private bool _raw;

        public int File => file;

        public FileSlot? Slot => slot;

        public int Index => index;

        public CsvUnit? Unit => _unit;

        /// <summary>The unit's bytes (null for a file with no unit).</summary>
        public TextUnitBytes? Bytes => bytes;

        /// <summary>
        /// The unit as checked (<paramref name="redone"/>: decoded again by the consumer, the one before dropped); true when
        /// its batches are still the file's (to convert as they are delivered).
        /// </summary>
        public bool Redo(CsvUnit checkedUnit, bool redone)
        {
            if (redone && !ReferenceEquals(checkedUnit, _unit))
            {
                _unit?.Dispose();
                _unit = checkedUnit;
                _raw = true;
            }
            return _raw;
        }

        public void Dispose()
        {
            _unit?.Dispose();
            if (bytes is not null)
            {
                bytes.Dispose();
                slot!.Reader.Input.Release(index);
                slot.Release();
            }
        }
    }

    // ---- the window -----------------------------------------------------------------------------------------------

    /// <summary>
    /// A file the window reads: its source and reader (the header read), and what was decided for it. Units known up front
    /// (records cut into blocks) or found as the file is read (a stream, a compressed file), their bytes kept until decoded.
    /// </summary>
    private sealed class FileSlot(CsvDatasetReader owner, int index, IRandomAccess source, CsvReader reader, FileState state, RequestGate? gate) : UnitGroup(index)
    {
        private readonly object _lock = new();
        private readonly Dictionary<int, TextUnitBytes> _found = [];
        private int _foundCount;
        private bool _over;
        private Exception? _failure;

        public CsvReader Reader => reader;

        public FileState State => state;

        private TextUnits Input => reader.Input;

        public override (int Found, bool Over) Progress
        {
            get
            {
                if (Input.Count >= 0) return (Input.Count, true);
                lock (_lock) return (_foundCount, _over);
            }
        }

        public override Exception? Failure
        {
            get
            {
                lock (_lock) return _failure;
            }
        }

        public override long UnitBytes(int unit)
        {
            lock (_lock) return _found.TryGetValue(unit, out TextUnitBytes? bytes) ? Math.Max(1, bytes.Memory.Length) : owner._options.UnitBytes;
        }

        /// <summary>
        /// Finds units up to <paramref name="units"/>: their bytes read (in order, as a stream gives them), kept for
        /// decoding; the end of the file ends the group.
        /// </summary>
        public override Task FindAsync(int units, CancellationToken cancellationToken)
        {
            int from;
            lock (_lock) from = _foundCount;
            ValueTask<RequestGate.Lease> entering = gate is null ? default : gate.EnterAsync(Index, cancellationToken);
            return Task.Run(async () =>
            {
                try
                {
                    using RequestGate.Lease lease = await entering.ConfigureAwait(false);
                    for (int k = from; k < Math.Max(from + 1, units); k++)
                    {
                        TextUnitBytes? bytes = await Input.GetAsync(k, useAsync: true, cancellationToken).ConfigureAwait(false);
                        lock (_lock)
                        {
                            if (bytes is null || _over)
                            {
                                bytes?.Dispose();
                                _over = true;
                                break;
                            }
                            _found[k] = bytes;
                            _foundCount = k + 1;
                        }
                    }
                }
                catch (Exception e)
                {
                    lock (_lock)
                    {
                        _failure = owner.InFile(Index, e);
                        _over = true;
                    }
                }
            }, CancellationToken.None);
        }

        /// <summary>The bytes of a unit found (handed over: taken once).</summary>
        public TextUnitBytes? Take(int unit)
        {
            lock (_lock) return _found.Remove(unit, out TextUnitBytes? bytes) ? bytes : null;
        }

        protected override void Free()
        {
            lock (_lock)
            {
                foreach (TextUnitBytes bytes in _found.Values) bytes.Dispose();
                _found.Clear();
            }
            reader.Dispose();
            owner._dataset.Versions.Note(Index, source); // the version read (a store learns it reading)
            CsvDataset.Close(source).AsTask().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// The dataset's files for the window: opened (their headers read, their columns decided) on the thread pool, their
    /// units read and decoded there; requests (opens, reads) go through one gate, the lower files first.
    /// </summary>
    private sealed class Units(CsvDatasetReader owner, RequestGate? gate) : IUnitSource<FileSlot, Output>
    {
        private int _nextFile;

        public bool GroupsLeft
        {
            get
            {
                while (_nextFile < owner._dataset.Files.Count && !owner.Needed(_nextFile)) _nextFile++;
                return _nextFile < owner._dataset.Files.Count;
            }
        }

        /// <summary>The next file to read, past those a filter over partition columns leaves out.</summary>
        public int? NextGroup()
        {
            for (; _nextFile < owner._dataset.Files.Count; _nextFile++)
            {
                if (owner.Needed(_nextFile)) return _nextFile++;
            }
            return null;
        }

        public Task<FileSlot> Plan(int index, CancellationToken token)
        {
            // The open's place is asked for here, in file order (the gate lets waiting requests in by order).
            ValueTask<RequestGate.Lease> entering = gate is null ? default : gate.EnterAsync(index, token);
            return Task.Run(async () =>
            {
                IRandomAccess? source = null;
                CsvReader? reader = null;
                try
                {
                    CsvDatasetFile file = owner._dataset.Files[index];
                    var state = new FileState { CountOnly = !owner._read[index] };
                    bool mappable = owner._options.MemoryMap == CsvMemoryMap.Auto;
                    using (RequestGate.Lease lease = await entering.ConfigureAwait(false))
                    {
                        source = await CsvDataset.OpenSourceAsync(owner._dataset.Store, file, owner._dataset.Versions, index, token).ConfigureAwait(false);
                        reader = await CsvReader.OpenFileAsync(source, owner.FileOptions(index, state, forceAfter: false), mappable, useAsync: true, token).ConfigureAwait(false);
                        if (!state.CountOnly && owner.FiltersExactly && owner._shape.FiltersAfter && !state.After && !reader.FiltersExactly)
                        {
                            // The file's filtering is not exact: decode what the dataset's filter needs, and filter after.
                            reader.Dispose();
                            reader = null;
                            reader = await CsvReader.OpenFileAsync(source, owner.FileOptions(index, state, forceAfter: true), mappable, useAsync: true, token).ConfigureAwait(false);
                        }
                    }
                    if (!state.CountOnly)
                    {
                        if (state.After) Interlocked.Increment(ref owner._filteredAfter);
                        state.Converted = owner._shape.Conversions(reader.Schema, state.After, file.Path);
                    }
                    return new FileSlot(owner, index, source, reader, state, gate);
                }
                catch (Exception e)
                {
                    reader?.Dispose();
                    if (source is not null) await CsvDataset.Close(source).ConfigureAwait(false);
                    if (owner.InFile(index, e) is var named && !ReferenceEquals(named, e)) throw named;
                    throw;
                }
            }, CancellationToken.None);
        }

        public Task<Output> Start(FileSlot slot, int unit, bool last, CancellationToken token) => Task.Run(() => ReadAsync(slot, unit, token), token);

        public Output Empty(FileSlot slot) => new(slot, slot.Index, null, null, -1);

        /// <summary>A unit keeps its file until taken: the consumer may decode it again (the file's formats).</summary>
        public bool Keeps(Output output) => output.Bytes is not null;

        /// <summary>Reads (unless found already) and decodes a unit, its batches made the dataset's when it decoded without error.</summary>
        private async Task<Output> ReadAsync(FileSlot slot, int unit, CancellationToken token)
        {
            TextUnitBytes? bytes = slot.Take(unit);
            if (bytes is null)
            {
                using RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(slot.Index, token).ConfigureAwait(false);
                try
                {
                    bytes = await slot.Reader.Input.GetAsync(unit, useAsync: true, token).ConfigureAwait(false);
                }
                catch (Exception e) when (owner.InFile(slot.Index, e) is var named && !ReferenceEquals(named, e))
                {
                    throw named;
                }
            }
            if (bytes is null) return new Output(slot, slot.Index, CsvUnit.EndOfInput, null, unit);
            CsvUnit decoded;
            try
            {
                decoded = slot.Reader.Decode(bytes, unit, long.MaxValue);
                FileState state = slot.State;
                if (decoded.Error is null && !state.CountOnly)
                {
                    for (int k = 0; k < decoded.Batches.Count; k++)
                    {
                        RecordBatch batch = decoded.Batches[k]!;
                        decoded.Batches[k] = null;
                        decoded.Batches[k] = owner._shape.Convert(batch, state.Converted!, state.After, slot.Index);
                    }
                    decoded.Batches.RemoveAll(static b => b is null); // batches the dataset's filter emptied
                }
            }
            catch
            {
                bytes.Dispose();
                slot.Reader.Input.Release(unit);
                throw;
            }
            return new Output(slot, slot.Index, decoded, bytes, unit);
        }
    }
}
