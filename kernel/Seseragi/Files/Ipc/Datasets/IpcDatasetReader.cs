using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Network;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Reads a dataset's files in order, each as <see cref="IpcFile.OpenReader(IpcReadOptions?)"/> does with the options
/// given, into batches of one schema: batches never span files. With <see cref="IpcReadOptions.MaxDegreeOfParallelism"/>
/// above 1 the record batches of several files are read and decoded side by side ahead of the consumer, files opened
/// and planned ahead (<see cref="DatasetOptions.MaxFilesInFlight"/>), requests to a remote store bounded apart
/// (<see cref="DatasetOptions.MaxRequestsInFlight"/>, those of the files read first let in first); with 1, from local
/// files and memory file after file on the calling thread, from other stores (whose requests wait on the network) as
/// with several threads, on one.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="IpcReadOptions.Columns"/> names the dataset's columns (all of them when null): columns from files
/// and partition columns. Batches hold the columns from files in the dataset's order (a partition column of the same
/// name in its place, holding the path's value), then the other partition columns, then the file path.</item>
/// <item>A file's column converts to the dataset's type: as it is when the types agree (nullability aside); else by a
/// lossless widening (wider integers, floats holding the integers, decimals of no fewer digits, finer time units, dates
/// into timestamps, strings and binaries into the other layout, struct fields matched by name — those the file has
/// beyond the dataset's left out —, the elements of lists and maps, dictionaries into their values and back); by an
/// approximating conversion only with <see cref="DatasetOptions.AllowLossyCasts"/>; else the read fails naming the file
/// and the column. A column or struct field a file lacks is an error, or nulls with
/// <see cref="DatasetOptions.AllowMissingColumns"/> (always when united by name).</item>
/// <item><see cref="IpcReadOptions.Filter"/>: its conjuncts over partition columns only decide which files are read
/// (exactly, on the files' partition values); with <see cref="IpcReadOptions.FilterRows"/> the others filter the rows
/// of every file read, partition columns replaced by the file's values and columns the file lacks by nulls (Arrow IPC
/// has no statistics: without it they do nothing). Conjuncts over a column a file's values convert into lossily do not
/// filter that file; where the file's own filtering cannot be exact, the dataset filters its rows after the
/// conversion, so that <see cref="FiltersExactly"/> holds for every file.</item>
/// <item><see cref="IpcReadOptions.RowIndexColumn"/> numbers the rows across the files, in the order listed, as if they
/// were one (the numbers of each file follow the previous files' rows, rows of files left out included: their batches'
/// messages are read for their row counts): the first column.</item>
/// <item><see cref="IpcReadOptions.MaxRows"/> counts the rows delivered across the files.</item>
/// </list>
/// Errors in a file name it.
/// </remarks>
public sealed class IpcDatasetReader : IDisposable, IAsyncDisposable
{
    private readonly IpcDataset _dataset;
    private readonly IpcReadOptions _options;
    private readonly DatasetShape _shape; // the columns, the filter's split and the batches' conversion
    private readonly bool[] _read;        // the files a filter over partition columns leaves
    private readonly bool _indexed;       // rows numbered: batches converted by the consumer, after their numbers
    private int _filteredAfter;

    internal IpcDatasetReader(IpcDataset dataset, IpcReadOptions options)
    {
        options.Validate();
        if (options.Filter is { } given) options = options with { Filter = FilterShapes.normalize(given) };
        _dataset = dataset;
        _options = options;
        _shape = new DatasetShape(dataset.Options, dataset.Partitions, dataset.PartitionField, dataset.FileSchema, dataset.Files.Count,
            options.Columns, null, options.Filter, options.FilterRows, options.RowIndexColumn, views: true,
            PathOf, static (message, inner) => new IpcFormatException(message, inner));
        _read = _shape.Read;
        _indexed = options.RowIndexColumn is not null;
        Schema = _shape.Output;
        FiltersExactly = _shape.FiltersExactly;
    }

    /// <summary>The schema of the batches: the row index if asked, the columns read (nullable), the file path if asked.</summary>
    public Schema Schema { get; }

    /// <summary>
    /// Whether, with <see cref="IpcReadOptions.FilterRows"/>, the batches hold exactly the rows the filter keeps: when the
    /// filter lowers exactly over the dataset's types and its conjuncts over partition columns are decided exactly
    /// (files whose own filtering is not exact are filtered again after their columns convert).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The files a filter over partition columns leaves (for tests).</summary>
    internal IReadOnlyList<bool> FilesRead => _read;

    /// <summary>The files whose rows the dataset filtered after converting them (for tests).</summary>
    internal int FilesFilteredAfter => Volatile.Read(ref _filteredAfter);

    /// <summary>The most requests to the store at once in the last windowed read (0 when not bounded; for tests).</summary>
    internal int PeakRequests { get; private set; }

    /// <summary>The most body bytes and units read ahead at once, and files open at once, in the last windowed read (for tests).</summary>
    internal (long Bytes, int Units, int Files) PeakInFlight { get; private set; }

    /// <summary>Reads the batches, in file order.</summary>
    /// <exception cref="IpcFormatException">
    /// A file is damaged, is not an Arrow IPC file, lacks a column read, holds a column of a type that does not convert
    /// to the dataset's, or a value beyond the range of the dataset's type (the message names the file). It surfaces
    /// after the rows of the files before it.
    /// </exception>
    /// <exception cref="FileNotFoundException">A file named does not exist.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        IAsyncEnumerator<RecordBatch> batches = ReadCoreAsync(useAsync: false, default).GetAsyncEnumerator();
        try
        {
            while (IpcIO.Sync(batches.MoveNextAsync())) yield return batches.Current;
        }
        finally
        {
            IpcIO.Sync(batches.DisposeAsync());
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public IAsyncEnumerable<RecordBatch> ReadBatchesAsync(CancellationToken cancellationToken = default) => ReadCoreAsync(useAsync: true, cancellationToken);

    /// <summary>Reads the batches as an Arrow stream (each batch disposed when the next is read).</summary>
    public Apache.Arrow.Ipc.IArrowArrayStream ReadArrowStream() => new IpcReader.OwningBatchStream(Schema, ReadBatches());

    /// <summary>Nothing is held between reads.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private string PathOf(int file) => _dataset.Files[file].Path;

    private bool Remote => DatasetLimits.Remote(_dataset.Store);

    private int UnitsAhead => DatasetLimits.UnitsAhead(_options.MaxDegreeOfParallelism, Remote);

    private RequestGate? NewRequestGate() => DatasetLimits.RequestsInFlight(_dataset.Options, Remote) is int requests ? new RequestGate(requests) : null;

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(bool useAsync, [EnumeratorCancellation] CancellationToken ct)
    {
        if (_options.MaxDegreeOfParallelism > 1 || Remote)
        {
            await using var window = new Window(this, ct);
            long? left = _options.MaxRows;
            using DictionaryUnifier? windowed = _shape.NewUnifier(); // per reading: values numbered as they come
            long next = 0; // the rows before the next unit, across the files (the row index)
            while (left != 0 && await window.NextAsync().ConfigureAwait(false) is { } taken)
            {
                try
                {
                    Output output = taken.Failure is null ? taken : await RedoAsync(taken, left, useAsync, ct).ConfigureAwait(false);
                    if (Deliver(output, ref next, left) is { } batch) yield return _shape.Cut(batch, ref left, windowed);
                }
                finally
                {
                    if (taken.Failure is not null) taken.Dispose(); // its file's hold
                }
            }
            yield break;
        }

        // One thread, local files or memory: file after file on the calling thread.
        long? remaining = _options.MaxRows;
        using DictionaryUnifier? unifier = _shape.NewUnifier(); // per reading: values numbered as they come
        long row = 0;
        for (int i = 0; i < _dataset.Files.Count && remaining != 0; i++)
        {
            if (!_read[i] && !_indexed) continue;
            IpcFile file = await IpcDataset.OpenFileAsync(_dataset.Store, _dataset.Files[i], _dataset.Footers, i, useAsync, ct).ConfigureAwait(false);
            try
            {
                if (!_read[i])
                {
                    row += await Named(i, file.RowCountAsync(useAsync, ct)).ConfigureAwait(false);
                    continue;
                }
                using FilePlan plan = Plan(file, i);
                await Named(i, plan.Reader.PrepareAsync(useAsync, ct)).ConfigureAwait(false);
                for (int b = 0; b < file.BatchCount && remaining != 0; b++)
                {
                    var (message, body) = await Named(i, plan.Reader.FetchAsync(b, useAsync, ct)).ConfigureAwait(false);
                    IpcDecoded decoded = Decode(plan, i, message, body, plan.FilterAfter ? long.MaxValue : remaining ?? long.MaxValue);
                    if (Deliver(new Output(plan, i, decoded, null, 0), ref row, remaining) is { } batch) yield return _shape.Cut(batch, ref remaining, unifier);
                }
            }
            finally
            {
                await file.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    // ---- one file -------------------------------------------------------------------------------------------------

    /// <summary>
    /// How a file is read: its reader (the row index, when asked, numbered from 0: the consumer numbers rows across
    /// files), and how its columns (and those the dataset's filter reads after converting) convert to the dataset's.
    /// </summary>
    private sealed class FilePlan(IpcReader reader, DatasetShape.Converted converted, bool filterAfter) : IDisposable
    {
        public IpcReader Reader { get; } = reader;

        public DatasetShape.Converted Converted { get; } = converted;

        /// <summary>Whether the dataset filters the file's rows after converting them.</summary>
        public bool FilterAfter { get; } = filterAfter;

        public void Dispose() => Reader.Dispose();
    }

    /// <summary>The plan of file <paramref name="index"/>.</summary>
    private FilePlan Plan(IpcFile file, int index)
    {
        string path = PathOf(index);
        Dictionary<string, Field> has = new(StringComparer.Ordinal);
        foreach (Field field in file.Schema.FieldsList) has.TryAdd(field.Name, field);
        DatasetShape.FileColumns columns = _shape.ColumnsOf(has, path);
        var (fileFilter, complete) = _shape.FileFilter(index, columns.Missing, columns.Dropped, has);

        bool after = FiltersExactly && _shape.FiltersAfter && !complete;
        IpcReader reader = Open(file, index, columns, fileFilter, after);
        if (FiltersExactly && _shape.FiltersAfter && !after && !reader.FiltersExactly)
        {
            // The file's filtering is not exact: read again with what the dataset's filter needs.
            reader.Dispose();
            after = true;
            reader = Open(file, index, columns, fileFilter, after);
        }
        try
        {
            if (after) Interlocked.Increment(ref _filteredAfter);
            return new FilePlan(reader, _shape.Conversions(reader.Schema, after, path), after);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    /// <summary>The file's reader: the columns read, and with <paramref name="after"/> the columns the dataset's filter reads, whole.</summary>
    private IpcReader Open(IpcFile file, int index, DatasetShape.FileColumns columns, ScalarExpr? filter, bool after)
    {
        try
        {
            return file.OpenReader(_options with
            {
                Columns = columns.Lists[after ? 1 : 0].Columns,
                Filter = filter,
                MaxRows = null,
                RowIndexOffset = 0,
            });
        }
        catch (IpcFormatException e)
        {
            throw IpcDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>Decodes a batch fetched (up to <paramref name="rows"/> rows); errors name the file.</summary>
    private IpcDecoded Decode(FilePlan plan, int index, IpcMessage message, IpcBody body, long rows)
    {
        try
        {
            return plan.Reader.Decode(message, body, rows);
        }
        catch (IpcFormatException e)
        {
            throw IpcDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>
    /// What a unit gives the consumer: a file's batch decoded (<see cref="Raw"/>: rows numbered, then converted, by the
    /// consumer) or converted already on a thread of the window (<see cref="Batch"/>; null when no row is left), or
    /// the rows of a file not read (<see cref="Rows"/>), counted for the row index.
    /// </summary>
    /// <remarks>
    /// A unit whose conversion failed on the window's thread carries the <see cref="Failure"/> instead (and its batch
    /// number, <see cref="Unit"/>): the consumer then knows how many rows are still wanted (see <see cref="RedoAsync"/>).
    /// </remarks>
    private sealed record Output(FilePlan? Plan, int File, IpcDecoded? Raw, RecordBatch? Batch, long Rows, int Unit = -1, Exception? Failure = null) : IDisposable
    {
        /// <summary>The file a failed unit keeps open for the consumer (released when the output is disposed).</summary>
        public FileSlot? Held { get; init; }

        public void Dispose()
        {
            Raw?.Dispose();
            Batch?.Dispose();
            Held?.Release();
        }
    }

    /// <summary>
    /// A unit's batch in the dataset's schema, numbered from <paramref name="next"/> (moved past the unit's rows); null
    /// when it has none (a filter kept none, or the unit is a file not read). Called in order, on the consumer.
    /// </summary>
    /// <remarks>
    /// Rows past <paramref name="left"/> (those still wanted) are not converted unless the dataset filters after
    /// converting: a value that cannot be converted beyond a row limit is never met, as on the one-thread path.
    /// </remarks>
    private RecordBatch? Deliver(Output output, ref long next, long? left)
    {
        if (output.Raw is not { } raw)
        {
            next += output.Rows;
            return output.Batch;
        }
        FilePlan plan = output.Plan!;
        RecordBatch batch = plan.Reader.Deliver(raw, ref next);
        if (batch.Length == 0 && plan.Reader.Filters)
        {
            batch.Dispose();
            return null;
        }
        if (left is long wanted && wanted < batch.Length && !plan.FilterAfter)
        {
            // Cut over the same memory (the slices own it from now on, as DatasetShape.Cut's do).
            int rows = (int)wanted;
            batch = new RecordBatch(batch.Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, rows))], rows);
        }
        return _shape.Convert(batch, plan.Converted, plan.FilterAfter, output.File);
    }

    /// <summary>
    /// A unit whose conversion failed on a window's thread (the whole batch converted): its error, unless rows are
    /// limited and the dataset does not filter after converting; then the unit read again for the rows still wanted
    /// (<paramref name="left"/>) and handed over unconverted, as the one-thread path reads it (the error surfaces only
    /// if a wanted row holds the value).
    /// </summary>
    private async ValueTask<Output> RedoAsync(Output failed, long? left, bool useAsync, CancellationToken ct)
    {
        FilePlan plan = failed.Plan!;
        if (left is long rows && !plan.FilterAfter)
        {
            var (message, body) = await Named(failed.File, plan.Reader.FetchAsync(failed.Unit, useAsync, ct)).ConfigureAwait(false);
            return new Output(plan, failed.File, Decode(plan, failed.File, message, body, rows), null, 0);
        }
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(failed.Failure!);
        return failed; // not reached
    }

    /// <summary>
    /// A unit (record batch <paramref name="unit"/>) decoded on a thread of the window: converted there unless rows are
    /// numbered (the consumer then does it); a conversion that fails is handed over as the unit's failure (see
    /// <see cref="RedoAsync"/>).
    /// </summary>
    private Output Finish(FilePlan plan, int file, int unit, IpcDecoded decoded)
    {
        if (_indexed) return new Output(plan, file, decoded, null, 0);
        long unused = 0;
        RecordBatch batch = plan.Reader.Deliver(decoded, ref unused);
        if (batch.Length == 0 && plan.Reader.Filters)
        {
            batch.Dispose();
            return new Output(plan, file, null, null, 0);
        }
        try
        {
            return new Output(plan, file, null, _shape.Convert(batch, plan.Converted, plan.FilterAfter, file), 0);
        }
        catch (IpcFormatException e)
        {
            return new Output(plan, file, null, null, 0, unit, e); // the batch is consumed: read again if wanted
        }
    }

    /// <summary>Awaits a file's operation; its errors name the file.</summary>
    private async ValueTask<T> Named<T>(int index, ValueTask<T> operation)
    {
        try
        {
            return await operation.ConfigureAwait(false);
        }
        catch (IpcFormatException e)
        {
            throw IpcDataset.InFile(PathOf(index), e);
        }
        catch (IOException e) when (DatasetFiles.Unnamed(e, PathOf(index)))
        {
            throw DatasetFiles.InFile(PathOf(index), e);
        }
    }

    private async ValueTask Named(int index, ValueTask operation) => await Named(index, Done(operation)).ConfigureAwait(false);

    private static async ValueTask<bool> Done(ValueTask operation)
    {
        await operation.ConfigureAwait(false);
        return true;
    }

    // ---- the window -----------------------------------------------------------------------------------------------

    /// <summary>A file the window reads: its file and plan (null for a file only counted), released once its units are taken.</summary>
    private sealed class FileSlot(int index, IpcFile? file, FilePlan? plan, int units, long rows) : IDisposable
    {
        private int _taken, _holds;
        private bool _disposed;

        public int Index => index;

        public FilePlan? Plan => plan;

        /// <summary>The record batches to read (a file only counted: none).</summary>
        public int Units => units;

        /// <summary>The rows of a file only counted (for the row index).</summary>
        public long Rows => rows;

        /// <summary>One more unit taken; true when it was the last (the slot is then disposed, once no output holds it).</summary>
        public bool Taken()
        {
            if (++_taken < Math.Max(1, units)) return false;
            if (_holds == 0) Dispose();
            return true;
        }

        /// <summary>Keeps the file open for an output beyond its last unit taken (on the consumer, as <see cref="Taken"/>).</summary>
        public void Hold() => _holds++;

        /// <summary>An output's hold let go: the slot disposed if every unit was taken and nothing else holds it.</summary>
        public void Release()
        {
            if (--_holds == 0 && _taken >= Math.Max(1, units)) Dispose();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            plan?.Dispose();
            file?.Dispose();
        }
    }

    /// <summary>A unit of the window: a file's record batch (or a file only counted), read by its task once started.</summary>
    private sealed class Unit(FileSlot file, int batch, long bytes)
    {
        public FileSlot File => file;

        public int Batch => batch;

        public long Bytes => bytes;

        public Task<Output>? Task { get; set; }
    }

    /// <summary>
    /// The units of the dataset's files (record batches) in output order, read and decoded ahead of the consumer: the
    /// unit the consumer waits for always starts; the others start up to the units ahead beyond it while their body
    /// bytes stay within <see cref="IpcReadOptions.InFlightBytes"/>. Files are opened and planned ahead (their
    /// dictionaries read), up to the files in flight; their units join the window in file order as their plans
    /// complete. Requests (opens, dictionaries, batches) go through one gate, the lower files first. A failure surfaces
    /// when the consumer reaches it. Disposing the window stops the work, waits for what runs, frees what was decoded
    /// and not taken, then the files.
    /// </summary>
    private sealed class Window : IAsyncDisposable
    {
        private readonly IpcDatasetReader _owner;
        private readonly CancellationTokenSource _cancel;
        private readonly RequestGate? _gate;
        private readonly int _ahead, _filesInFlight;
        private readonly long _budget;
        private readonly object _lock = new();
        private readonly List<Unit> _units = [];
        private readonly List<Task<FileSlot>?> _plans = [];  // the files planning or planned, in order (null once joined)
        private readonly HashSet<FileSlot> _live = [];       // the files joined whose units are not all taken
        private int _nextFile, _integrated, _head, _started;
        private long _inFlight, _peakBytes;
        private int _peakUnits, _peakFiles;
        private bool _disposed;

        public Window(IpcDatasetReader owner, CancellationToken cancellationToken)
        {
            _owner = owner;
            _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _gate = owner.NewRequestGate();
            _ahead = owner.UnitsAhead;
            _filesInFlight = DatasetLimits.FilesInFlight(owner._dataset.Options, owner.Remote, _ahead);
            _budget = owner._options.InFlightBytes;
        }

        /// <summary>The next unit's output, in order; null when every unit was taken.</summary>
        public async Task<Output?> NextAsync()
        {
            while (true)
            {
                Task<FileSlot> waiting;
                lock (_lock)
                {
                    Fill();
                    if (_head < _units.Count) break;
                    if (_integrated == _plans.Count) return null; // every file planned and taken
                    waiting = _plans[_integrated]!;
                }
                await waiting.ConfigureAwait(false); // a failure to open or plan surfaces here, in order
            }
            Unit unit;
            lock (_lock) unit = _units[_head];
            Output output = await unit.Task!.ConfigureAwait(false);
            lock (_lock)
            {
                unit.Task = null;
                _inFlight -= unit.Bytes;
                _units[_head++] = null!;
                if (output.Failure is not null)
                {
                    // The consumer may read the unit again: its file stays open until the output is disposed.
                    unit.File.Hold();
                    output = output with { Held = unit.File };
                }
                if (unit.File.Taken()) _live.Remove(unit.File);
            }
            return output;
        }

        /// <summary>Under the lock: joins the plans completed in order, starts the units the window allows, plans more files.</summary>
        private void Fill()
        {
            if (_disposed) return;
            while (_integrated < _plans.Count && _plans[_integrated]!.IsCompletedSuccessfully)
            {
                FileSlot slot = _plans[_integrated]!.Result;
                _plans[_integrated++] = null;
                _live.Add(slot);
                if (slot.Plan is null) _units.Add(new Unit(slot, -1, 0) { Task = Task.FromResult(new Output(null, slot.Index, null, null, slot.Rows)) });
                else for (int b = 0; b < slot.Units; b++) _units.Add(new Unit(slot, b, slot.Plan.Reader.BodyBytes(b)));
                if (slot.Units == 0 && slot.Plan is not null) _units.Add(new Unit(slot, -1, 0) { Task = Task.FromResult(new Output(null, slot.Index, null, null, 0)) });
            }
            while (_started < _units.Count && (_started == _head || _started - _head < _ahead && _inFlight + _units[_started].Bytes <= _budget))
            {
                Unit unit = _units[_started++];
                if (unit.Task is not null) continue; // a file only counted, or without batches
                _inFlight += unit.Bytes;
                _peakBytes = Math.Max(_peakBytes, _inFlight);
                _peakUnits = Math.Max(_peakUnits, _started - _head);
                CancellationToken token = _cancel.Token;
                unit.Task = Task.Run(() => ReadAsync(unit, token), token);
            }
            // Files open: those planning or planned and not joined, and those joined with units not taken.
            while (_plans.Count - _integrated + _live.Count < _filesInFlight && _units.Count - _head < _ahead && Next() is int file)
            {
                _plans.Add(Plan(file));
                _peakFiles = Math.Max(_peakFiles, _plans.Count - _integrated + _live.Count);
            }
        }

        /// <summary>The next file to plan (read, or counted for row numbers), past those left out; null when none is left.</summary>
        private int? Next()
        {
            for (; _nextFile < _owner._dataset.Files.Count; _nextFile++)
            {
                if (_owner._read[_nextFile] || _owner._indexed) return _nextFile++;
            }
            return null;
        }

        /// <summary>Reads and decodes a unit (its requests through the gate, by its file's place); errors name the file.</summary>
        private async Task<Output> ReadAsync(Unit unit, CancellationToken token)
        {
            FileSlot slot = unit.File;
            FilePlan plan = slot.Plan!;
            (IpcMessage Message, IpcBody Body) fetched;
            using (RequestGate.Lease lease = _gate is null ? default : await _gate.EnterAsync(slot.Index, token).ConfigureAwait(false))
                fetched = await _owner.Named(slot.Index, plan.Reader.FetchAsync(unit.Batch, useAsync: true, token)).ConfigureAwait(false);
            IpcDecoded decoded = _owner.Decode(plan, slot.Index, fetched.Message, fetched.Body, long.MaxValue);
            return _owner.Finish(plan, slot.Index, unit.Batch, decoded);
        }

        /// <summary>Opens and plans a file on the thread pool (its dictionaries read), or counts its rows; once done, the window fills on.</summary>
        private Task<FileSlot> Plan(int index)
        {
            CancellationToken token = _cancel.Token;
            IpcDatasetReader owner = _owner;
            RequestGate? gate = _gate;
            // The open's place is asked for here, in file order (Fill plans files in order), not on the thread pool:
            // the gate lets waiting requests in by order, so the file the consumer waits for opens first.
            ValueTask<RequestGate.Lease> entering = gate is null ? default : gate.EnterAsync(index, token);
            // Started without the token: a planning cancelled before it ran would leave nothing to await.
            Task<FileSlot> planning = Task.Run(async () =>
            {
                IpcFile? file = null;
                FilePlan? plan = null;
                try
                {
                    using (RequestGate.Lease lease = await entering.ConfigureAwait(false))
                        file = await IpcDataset.OpenFileAsync(owner._dataset.Store, owner._dataset.Files[index], owner._dataset.Footers, index, useAsync: true, token)
                            .ConfigureAwait(false);
                    if (!owner._read[index])
                    {
                        long rows;
                        using (RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(index, token).ConfigureAwait(false))
                            rows = await owner.Named(index, file.RowCountAsync(useAsync: true, token)).ConfigureAwait(false);
                        file.Dispose();
                        return new FileSlot(index, null, null, 0, rows);
                    }
                    plan = owner.Plan(file, index);
                    using (RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(index, token).ConfigureAwait(false))
                        await owner.Named(index, plan.Reader.PrepareAsync(useAsync: true, token)).ConfigureAwait(false);
                    return new FileSlot(index, file, plan, file.BatchCount, 0);
                }
                catch
                {
                    plan?.Dispose();
                    file?.Dispose();
                    throw;
                }
            }, CancellationToken.None);
            planning.ContinueWith(_ =>
            {
                lock (_lock) Fill();
            }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
            return planning;
        }

        public async ValueTask DisposeAsync()
        {
            Task<FileSlot>[] plans;
            Unit[] running;
            FileSlot[] live;
            lock (_lock)
            {
                _disposed = true;
                _cancel.Cancel();
                plans = [.. _plans.Skip(_integrated).OfType<Task<FileSlot>>()];
                running = [.. _units.Skip(_head).Where(u => u?.Task is not null)];
                live = [.. _live];
            }
            // What runs finishes first (a unit decodes against its file's dictionaries), then everything is freed.
            foreach (Unit unit in running)
            {
                try
                {
                    (await unit.Task!.ConfigureAwait(false)).Dispose();
                }
                catch
                {
                    // abandoned: its error no longer matters
                }
            }
            foreach (Task<FileSlot> plan in plans)
            {
                try
                {
                    (await plan.ConfigureAwait(false)).Dispose();
                }
                catch
                {
                    // abandoned
                }
            }
            foreach (FileSlot slot in live) slot.Dispose();
            _cancel.Dispose();
            _owner.PeakInFlight = (_peakBytes, _peakUnits, _peakFiles);
            _owner.PeakRequests = _gate?.PeakInside ?? 0;
        }
    }
}
