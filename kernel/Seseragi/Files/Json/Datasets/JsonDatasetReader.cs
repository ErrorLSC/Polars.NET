using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json.Reading;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// Reads a dataset's files in order, each as <see cref="JsonReader"/> reads a file with the options given, into batches
/// of one schema (batches never span files): the files' chunks (a chunk of lines or array elements, a small file whole)
/// decoded side by side ahead of the consumer, files opened ahead (<see cref="DatasetOptions.MaxFilesInFlight"/>),
/// requests to a remote store bounded apart (<see cref="DatasetOptions.MaxRequestsInFlight"/>, those of the files read
/// first let in first). A compressed file (or a stream-like source) is read in order, its chunks found as it is read.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>The dataset's options decide the layout and the columns: the read options' <see cref="JsonReadOptions.Schema"/>
/// and <see cref="JsonReadOptions.SchemaOverrides"/> must be null; their <see cref="JsonReadOptions.Format"/>,
/// <see cref="JsonReadOptions.InferSchemaLength"/> and <see cref="JsonReadOptions.ViewTypes"/> are not used.</item>
/// <item><see cref="JsonReadOptions.Columns"/> names the dataset's columns (all of them when null): columns from files
/// and partition columns. Batches hold the columns from files in the dataset's order (a partition column of the same
/// name in its place, holding the path's value), then the other partition columns, then the file path.</item>
/// <item><see cref="JsonReadOptions.Filter"/>: its conjuncts over partition columns only decide which files are read
/// (exactly, on the files' partition values: the others are not opened); with <see cref="JsonReadOptions.FilterRows"/> the
/// others filter the rows of every file read as they are decoded, partition columns replaced by the file's values.</item>
/// <item><see cref="JsonReadOptions.MaxRows"/> counts the rows delivered across the files.</item>
/// </list>
/// Errors in a file name it, and the line (and array index) in it.
/// </remarks>
public sealed class JsonDatasetReader : IDisposable, IAsyncDisposable
{
    private readonly JsonDataset _dataset;
    private readonly JsonReadOptions _options;
    private readonly DatasetShape _shape; // the columns, the filter's split, partition values and the file path added
    private readonly bool[] _read;        // the files a filter over partition columns leaves
    private readonly bool _document;
    private int _filteredAfter;

    internal JsonDatasetReader(JsonDataset dataset, JsonReadOptions options)
    {
        if (options.Schema is not null || options.SchemaOverrides is not null)
            throw new ArgumentException("The dataset's options decide the columns: give the read options no schema (nor schema overrides).", nameof(options));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BatchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxDegreeOfParallelism);
        if (options.MaxRows is < 0) throw new ArgumentOutOfRangeException(nameof(options), "MaxRows cannot be negative.");
        if (options.Filter is { } given) options = options with { Filter = FilterShapes.normalize(given) };
        _dataset = dataset;
        _options = options;
        _document = dataset.Options.Format == JsonFormat.Document;
        _shape = new DatasetShape(dataset.Options, dataset.Partitions, dataset.PartitionField, dataset.FileSchema, dataset.Files.Count,
            options.Columns, null, options.Filter, options.FilterRows, null, views: true,
            PathOf, static (message, inner) => new JsonFormatException(message, inner));
        _read = _shape.Read;
        Schema = _shape.Output;
        FiltersExactly = _shape.FiltersExactly;
    }

    /// <summary>The schema of the batches: the columns read (nullable), the file path if asked.</summary>
    public Schema Schema { get; }

    /// <summary>
    /// Whether, with <see cref="JsonReadOptions.FilterRows"/>, the batches hold exactly the rows the filter keeps: when the
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
    /// <exception cref="JsonFormatException">A file is not JSON of the layout read (the message names it and the line).</exception>
    /// <exception cref="JsonValueException">A value its column's type cannot hold (the message names the file and the line).</exception>
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
            long lines = 0, records = 0; // in the file read, before the unit taken
            while (left != 0 && await window.NextAsync().ConfigureAwait(false) is { } taken)
            {
                try
                {
                    if (taken.File != file) (file, lines, records) = (taken.File, 0, 0);
                    if (taken.Unit is not { } unit) continue;
                    if (unit.Error is { } error)
                    {
                        // Past the rows still wanted the error is never met (as one file reads it): the unit read again up to them.
                        if (left is long wanted && taken.Plan!.Filter is null && taken.Bytes is { } again)
                        {
                            unit = taken.Redo(Decode(taken.Plan, again, wanted));
                            if (unit.Error is { } still) throw InFile(file, still.At(lines, records));
                        }
                        else throw InFile(file, error.At(lines, records));
                    }
                    for (int k = 0; k < unit.Batches.Count && left != 0; k++)
                    {
                        RecordBatch? batch = unit.Batches[k];
                        unit.Batches[k] = null;
                        if (batch is null) continue;
                        if (taken.Raw) batch = _shape.Convert(batch, taken.Plan!.Converted, taken.Plan.FilterAfter, file);
                        if (batch is null) continue;
                        yield return _shape.Cut(batch, ref left, null);
                    }
                    lines += unit.Lines;
                    records += unit.Rows;
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
    /// How a file is decoded: the columns (and those the dataset's filter reads after decoding), its row filter (its
    /// partition values in place), and how its batches become the dataset's.
    /// </summary>
    private sealed record FilePlan(JsonDecodeSettings Settings, JsonRowFilter? Filter, DatasetShape.Converted Converted, bool FilterAfter);

    /// <summary>The plan of file <paramref name="index"/>: every file holds the dataset's columns (a field a record lacks is null).</summary>
    private FilePlan Plan(int index)
    {
        string path = PathOf(index);
        Dictionary<string, Field> has = new(StringComparer.Ordinal);
        foreach (Field field in _dataset.FileSchema.FieldsList) has.TryAdd(field.Name, field);
        DatasetShape.FileColumns columns = _shape.ColumnsOf(has, path);
        var (fileFilter, complete) = _shape.FileFilter(index, columns.Missing, columns.Dropped, has);
        bool after = FiltersExactly && _shape.FiltersAfter && !complete;
        var plan = Prepare(columns, fileFilter, after, path);
        if (FiltersExactly && _shape.FiltersAfter && !after && !plan.Exact)
        {
            // The file's filtering is not exact: decode what the dataset's filter needs, and filter after.
            after = true;
            plan = Prepare(columns, fileFilter, after, path);
        }
        if (after) Interlocked.Increment(ref _filteredAfter);
        Schema decoded = plan.Filter?.Selected ?? plan.Settings.Schema;
        return new FilePlan(plan.Settings, plan.Filter, _shape.Conversions(decoded, after, path), after);
    }

    private (JsonDecodeSettings Settings, JsonRowFilter? Filter, bool Exact) Prepare(DatasetShape.FileColumns columns, ScalarExpr? filter, bool after, string path)
    {
        // A document's fields the inference did not see are errors, as in one file.
        bool strict = _document && _dataset.Inferred && !_options.IgnoreErrors;
        try
        {
            return JsonReader.Prepare(_dataset.FileSchema, columns.Lists[after ? 1 : 0].Columns, _options.FilterRows ? filter : null, _options, strict);
        }
        catch (Exception e) when (JsonDataset.Named(path, e) is { } named)
        {
            throw named;
        }
    }

    /// <summary>A unit's records decoded (up to <paramref name="rows"/>) and filtered by the file's filter.</summary>
    private JsonUnit Decode(FilePlan plan, JsonUnitBytes bytes, long rows) => JsonReader.Decode(bytes, plan.Settings, plan.Filter, _document, rows);

    /// <summary>A unit's error placed in its file.</summary>
    private Exception InFile(int file, Exception e) => JsonDataset.Named(PathOf(file), e) ?? e;

    /// <summary>
    /// What a unit gives the consumer: its decoded records (<see cref="Unit"/>; the batches already the dataset's unless
    /// <see cref="Raw"/>), or nothing for a file with no unit. A unit that failed keeps its bytes and its file for the
    /// consumer to decode it again up to the rows still wanted.
    /// </summary>
    private sealed class Output(FileSlot? slot, int file, FilePlan? plan, JsonUnit? unit, JsonUnitBytes? bytes, int index, bool raw) : IDisposable
    {
        private JsonUnit? _unit = unit;

        public int File => file;

        public FilePlan? Plan => plan;

        public JsonUnit? Unit => _unit;

        /// <summary>The unit's bytes, kept when it failed (null otherwise).</summary>
        public JsonUnitBytes? Bytes => bytes;

        /// <summary>Whether the batches are still the file's (a unit decoded again by the consumer).</summary>
        public bool Raw { get; private set; } = raw;

        /// <summary>The unit decoded again (by the consumer): its batches converted as they are delivered.</summary>
        public JsonUnit Redo(JsonUnit again)
        {
            _unit?.Dispose();
            _unit = again;
            Raw = true;
            return again;
        }

        public void Dispose()
        {
            _unit?.Dispose();
            if (bytes is not null)
            {
                bytes.Dispose();
                slot!.Input.Release(index);
                slot.Release();
            }
        }
    }

    // ---- the window -----------------------------------------------------------------------------------------------

    /// <summary>
    /// A file the window reads: its source and input, and its plan. Units known up front (lines cut into blocks) or found
    /// as the file is read (a stream, a compressed file, a document cut as it is scanned), their bytes kept until decoded.
    /// </summary>
    private sealed class FileSlot(JsonDatasetReader owner, int index, IRandomAccess source, JsonInput input, FilePlan plan, RequestGate? gate) : UnitGroup(index)
    {
        private readonly object _lock = new();
        private readonly Dictionary<int, JsonUnitBytes> _found = [];
        private int _foundCount;
        private bool _over;
        private Exception? _failure;

        public JsonInput Input => input;

        public FilePlan Plan => plan;

        public override (int Found, bool Over) Progress
        {
            get
            {
                if (input.Count >= 0) return (input.Count, true);
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
            lock (_lock) return _found.TryGetValue(unit, out JsonUnitBytes? bytes) ? Math.Max(1, bytes.Memory.Length) : owner._options.UnitBytes;
        }

        /// <summary>
        /// Finds units up to <paramref name="units"/>: their bytes read (several asked at once, so that a document is cut
        /// in parallel), kept for decoding; the end of the file ends the group.
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
                    Task<JsonUnitBytes?>[] reads = [.. Enumerable.Range(from, Math.Max(1, units - from))
                        .Select(k => Task.Run(async () => await input.GetAsync(k, useAsync: true, cancellationToken).ConfigureAwait(false), CancellationToken.None))];
                    for (int r = 0; r < reads.Length; r++)
                    {
                        JsonUnitBytes? bytes;
                        try
                        {
                            bytes = await reads[r].ConfigureAwait(false);
                        }
                        catch
                        {
                            for (int rest = r + 1; rest < reads.Length; rest++) Drop(reads[rest]);
                            throw;
                        }
                        lock (_lock)
                        {
                            if (bytes is null || _over)
                            {
                                bytes?.Dispose();
                                _over = true;
                                continue;
                            }
                            _found[from + r] = bytes;
                            _foundCount = from + r + 1;
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

            static void Drop(Task<JsonUnitBytes?> read) =>
                read.ContinueWith(static t => { if (t.IsCompletedSuccessfully) t.Result?.Dispose(); }, TaskScheduler.Default);
        }

        /// <summary>The bytes of a unit found (handed over: taken once).</summary>
        public JsonUnitBytes? Take(int unit)
        {
            lock (_lock) return _found.Remove(unit, out JsonUnitBytes? bytes) ? bytes : null;
        }

        protected override void Free()
        {
            lock (_lock)
            {
                foreach (JsonUnitBytes bytes in _found.Values) bytes.Dispose();
                _found.Clear();
            }
            input.Dispose();
            JsonDataset.Close(source).AsTask().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// The dataset's files for the window: opened and planned on the thread pool, their units read and decoded there;
    /// requests (opens, reads) go through one gate, the lower files first.
    /// </summary>
    private sealed class Units(JsonDatasetReader owner, RequestGate? gate) : IUnitSource<FileSlot, Output>
    {
        private int _nextFile;

        public bool GroupsLeft
        {
            get
            {
                while (_nextFile < owner._dataset.Files.Count && !owner._read[_nextFile]) _nextFile++;
                return _nextFile < owner._dataset.Files.Count;
            }
        }

        /// <summary>The next file to read, past those a filter over partition columns leaves out.</summary>
        public int? NextGroup()
        {
            for (; _nextFile < owner._dataset.Files.Count; _nextFile++)
            {
                if (owner._read[_nextFile]) return _nextFile++;
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
                JsonInput? input = null;
                try
                {
                    JsonDatasetFile file = owner._dataset.Files[index];
                    FilePlan plan = owner.Plan(index);
                    using (RequestGate.Lease lease = await entering.ConfigureAwait(false))
                    {
                        source = await JsonDataset.OpenSourceAsync(owner._dataset.Store, file, token).ConfigureAwait(false);
                        input = await JsonReader.InputAsync(source, owner._dataset.Options.Format, owner._options.MemoryMap == JsonMemoryMap.Auto,
                            owner._options.UnitBytes, owner._options.MaxDegreeOfParallelism, useAsync: true, token).ConfigureAwait(false);
                    }
                    return new FileSlot(owner, index, source, input, plan, gate);
                }
                catch (Exception e)
                {
                    input?.Dispose();
                    if (source is not null) await JsonDataset.Close(source).ConfigureAwait(false);
                    if (owner.InFile(index, e) is var named && !ReferenceEquals(named, e)) throw named;
                    throw;
                }
            }, CancellationToken.None);
        }

        public Task<Output> Start(FileSlot slot, int unit, bool last, CancellationToken token) => Task.Run(() => ReadAsync(slot, unit, token), token);

        public Output Empty(FileSlot slot) => new(null, slot.Index, slot.Plan, null, null, -1, raw: false);

        /// <summary>A unit that failed keeps its file: the consumer may decode it again (see <see cref="Output.Redo"/>).</summary>
        public bool Keeps(Output output) => output.Bytes is not null;

        /// <summary>Reads (unless found already) and decodes a unit, its batches made the dataset's; errors name the file.</summary>
        private async Task<Output> ReadAsync(FileSlot slot, int unit, CancellationToken token)
        {
            JsonUnitBytes? bytes = slot.Take(unit);
            if (bytes is null)
            {
                using RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(slot.Index, token).ConfigureAwait(false);
                try
                {
                    bytes = await slot.Input.GetAsync(unit, useAsync: true, token).ConfigureAwait(false);
                }
                catch (Exception e) when (owner.InFile(slot.Index, e) is var named && !ReferenceEquals(named, e))
                {
                    throw named;
                }
            }
            if (bytes is null) return new Output(null, slot.Index, slot.Plan, null, null, -1, raw: false);
            FilePlan plan = slot.Plan;
            JsonUnit decoded = owner.Decode(plan, bytes, long.MaxValue);
            if (decoded.Error is not null) return new Output(slot, slot.Index, plan, decoded, bytes, unit, raw: false); // kept: decoded again if wanted
            bytes.Dispose();
            slot.Input.Release(unit);
            try
            {
                for (int k = 0; k < decoded.Batches.Count; k++)
                {
                    RecordBatch batch = decoded.Batches[k]!;
                    decoded.Batches[k] = null;
                    decoded.Batches[k] = owner._shape.Convert(batch, plan.Converted, plan.FilterAfter, slot.Index);
                }
            }
            catch
            {
                decoded.Dispose();
                throw;
            }
            return new Output(null, slot.Index, plan, decoded, null, unit, raw: false);
        }
    }
}
