using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Concurrency;
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

    /// <summary>Whether local stream files are scanned through their mapping (not when the options turn mapping off).</summary>
    private bool Mapped => _options.MemoryMap != IpcMemoryMap.Off;

    private RequestGate? NewRequestGate() => DatasetLimits.RequestsInFlight(_dataset.Options, Remote) is int requests ? new RequestGate(requests) : null;

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(bool useAsync, [EnumeratorCancellation] CancellationToken ct)
    {
        if (_options.MaxDegreeOfParallelism > 1 || Remote)
        {
            RequestGate? gate = NewRequestGate();
            int ahead = UnitsAhead;
            var window = new OrderedUnits<FileSlot, Output>(new Units(this, gate),
                new OrderedUnitsLimits(ahead, _options.InFlightBytes, DatasetLimits.FilesInFlight(_dataset.Options, Remote, ahead)), ct);
            try
            {
                long? left = _options.MaxRows;
                using DictionaryUnifier? windowed = _shape.NewUnifier(); // per reading: values numbered as they come
                long next = 0; // the rows before the next unit, across the files (the row index)
                while (left != 0 && await window.NextAsync().ConfigureAwait(false) is { } taken)
                {
                    try
                    {
                        Output output = taken.Failure is null ? taken : await RedoAsync(taken, left, useAsync, ct).ConfigureAwait(false);
                        RecordBatch? delivered = Deliver(output, ref next, left);
                        // A stream's dictionaries no later batch of the file needs are let go (units come in order).
                        if (taken.Unit >= 0) taken.Plan!.Reader.TrimDictionaries(taken.Unit);
                        if (delivered is { } batch) yield return _shape.Cut(batch, ref left, windowed);
                    }
                    finally
                    {
                        if (taken.Failure is not null) taken.Dispose(); // its file's hold
                    }
                }
            }
            finally
            {
                await window.DisposeAsync().ConfigureAwait(false);
                (PeakInFlight, PeakRequests) = (window.Peak, gate?.PeakInside ?? 0);
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
            IpcFile file = await IpcDataset.OpenFileAsync(_dataset.Store, _dataset.Files[i], _dataset.Footers, i, Mapped, useAsync, ct).ConfigureAwait(false);
            try
            {
                if (!_read[i])
                {
                    row += await Named(i, file.RowCountAsync(useAsync, ct)).ConfigureAwait(false);
                    continue;
                }
                using FilePlan plan = Plan(file, i);
                await Named(i, plan.Reader.PrepareAsync(useAsync, ct)).ConfigureAwait(false);
                for (int b = 0; remaining != 0 && await Named(i, file.HasBatchAsync(b, useAsync, ct)).ConfigureAwait(false); b++)
                {
                    var (message, body) = await Named(i, plan.Reader.FetchAsync(b, useAsync, ct)).ConfigureAwait(false);
                    IpcDecoded decoded = Decode(plan, i, b, message, body, plan.FilterAfter ? long.MaxValue : remaining ?? long.MaxValue);
                    RecordBatch? delivered = Deliver(new Output(plan, i, decoded, null, 0), ref row, remaining);
                    plan.Reader.TrimDictionaries(b);
                    if (delivered is { } batch) yield return _shape.Cut(batch, ref remaining, unifier);
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
    private IpcDecoded Decode(FilePlan plan, int index, int batch, IpcMessage message, IpcBody body, long rows)
    {
        try
        {
            return plan.Reader.Decode(batch, message, body, rows);
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
            return new Output(plan, failed.File, Decode(plan, failed.File, failed.Unit, message, body, rows), null, 0);
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
        if (_indexed) return new Output(plan, file, decoded, null, 0, unit);
        long unused = 0;
        RecordBatch batch = plan.Reader.Deliver(decoded, ref unused);
        if (batch.Length == 0 && plan.Reader.Filters)
        {
            batch.Dispose();
            return new Output(plan, file, null, null, 0, unit);
        }
        try
        {
            return new Output(plan, file, null, _shape.Convert(batch, plan.Converted, plan.FilterAfter, file), 0, unit);
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

    /// <summary>
    /// A file the window reads (its units: its record batches, a stream's found as its scan goes on): its file and plan,
    /// null for a file only counted (for the row index), freed once its units are taken.
    /// </summary>
    private sealed class FileSlot(IpcDatasetReader owner, int index, IpcFile? file, FilePlan? plan, long rows, RequestGate? gate) : UnitGroup(index)
    {
        public FilePlan? Plan => plan;

        /// <summary>The rows of a file only counted (for the row index).</summary>
        public long Rows => rows;

        public override (int Found, bool Over) Progress => file is null ? (0, true) : file.Stream?.Progress ?? (file.BatchCount, true);

        public override Exception? Failure => file?.Stream?.Failure is { } failure ? IpcDataset.InFile(owner.PathOf(Index), failure) : null;

        public override long UnitBytes(int unit) => plan!.Reader.BodyBytes(unit);

        /// <summary>The stream scanned on (through the gate, by its file's place).</summary>
        public override Task FindAsync(int units, CancellationToken cancellationToken)
        {
            IpcStreamScan stream = file!.Stream!;
            ValueTask<RequestGate.Lease> entering = gate is null ? default : gate.EnterAsync(Index, cancellationToken);
            return Task.Run(async () =>
            {
                using (RequestGate.Lease lease = await entering.ConfigureAwait(false))
                    await owner.Named(Index, stream.ScanAheadAsync(units, useAsync: true, cancellationToken)).ConfigureAwait(false);
            }, CancellationToken.None);
        }

        protected override void Free()
        {
            plan?.Dispose();
            file?.Dispose();
        }
    }

    /// <summary>
    /// The dataset's files for the window: planned on the thread pool (opened, their dictionaries read, or their rows
    /// counted), their record batches read and decoded there; requests (opens, dictionaries, scans, batches) go through
    /// one gate, the lower files first.
    /// </summary>
    private sealed class Units(IpcDatasetReader owner, RequestGate? gate) : IUnitSource<FileSlot, Output>
    {
        private int _nextFile;

        public bool GroupsLeft => _nextFile < owner._dataset.Files.Count;

        /// <summary>The next file to plan (read, or counted for row numbers), past those left out.</summary>
        public int? NextGroup()
        {
            for (; _nextFile < owner._dataset.Files.Count; _nextFile++)
            {
                if (owner._read[_nextFile] || owner._indexed) return _nextFile++;
            }
            return null;
        }

        public Task<FileSlot> Plan(int index, CancellationToken token)
        {
            // The open's place is asked for here, in file order (the window plans files in order), not on the thread
            // pool: the gate lets waiting requests in by order, so the file the consumer waits for opens first.
            ValueTask<RequestGate.Lease> entering = gate is null ? default : gate.EnterAsync(index, token);
            // Started without the token: a planning cancelled before it ran would leave nothing to await.
            return Task.Run(async () =>
            {
                IpcFile? file = null;
                FilePlan? plan = null;
                try
                {
                    using (RequestGate.Lease lease = await entering.ConfigureAwait(false))
                        file = await IpcDataset.OpenFileAsync(owner._dataset.Store, owner._dataset.Files[index], owner._dataset.Footers, index, owner.Mapped, useAsync: true, token)
                            .ConfigureAwait(false);
                    if (!owner._read[index])
                    {
                        long rows;
                        using (RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(index, token).ConfigureAwait(false))
                            rows = await owner.Named(index, file.RowCountAsync(useAsync: true, token)).ConfigureAwait(false);
                        file.Dispose();
                        return new FileSlot(owner, index, null, null, rows, gate);
                    }
                    plan = owner.Plan(file, index);
                    using (RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(index, token).ConfigureAwait(false))
                        await owner.Named(index, plan.Reader.PrepareAsync(useAsync: true, token)).ConfigureAwait(false);
                    return new FileSlot(owner, index, file, plan, 0, gate);
                }
                catch
                {
                    plan?.Dispose();
                    file?.Dispose();
                    throw;
                }
            }, CancellationToken.None);
        }

        public Task<Output> Start(FileSlot slot, int unit, bool last, CancellationToken token) => Task.Run(() => ReadAsync(slot, unit, token), token);

        public Output Empty(FileSlot slot) => new(null, slot.Index, null, null, slot.Rows);

        /// <summary>A unit whose conversion failed keeps its file: the consumer may read it again (see <see cref="RedoAsync"/>).</summary>
        public bool Keeps(Output output) => output.Failure is not null;

        /// <summary>Reads and decodes a unit (its requests through the gate, by its file's place); errors name the file.</summary>
        private async Task<Output> ReadAsync(FileSlot slot, int unit, CancellationToken token)
        {
            FilePlan plan = slot.Plan!;
            (IpcMessage Message, IpcBody Body) fetched;
            using (RequestGate.Lease lease = gate is null ? default : await gate.EnterAsync(slot.Index, token).ConfigureAwait(false))
                fetched = await owner.Named(slot.Index, plan.Reader.FetchAsync(unit, useAsync: true, token)).ConfigureAwait(false);
            IpcDecoded decoded = owner.Decode(plan, slot.Index, unit, fetched.Message, fetched.Body, long.MaxValue);
            Output output = owner.Finish(plan, slot.Index, unit, decoded);
            return output.Failure is null ? output : output with { Held = slot };
        }
    }
}
