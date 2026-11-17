using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;
using Kaminari.Seseragi.Files.Parquet.Writing;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Writes Arrow record batches as a Parquet file: flat and nested columns (structs, lists of every layout, maps,
/// run-end encoded and dictionary-encoded nested values; not unions), each row group
/// buffered in memory until it is full (<see cref="ParquetWriteOptions.RowGroupRows"/>, <see cref="ParquetWriteOptions.RowGroupBytes"/>)
/// and then written column chunk by column chunk; the footer on <see cref="Complete"/>. Values Parquet cannot hold
/// exactly are refused with <see cref="NotSupportedException"/>, never changed; types it cannot say (time zone
/// names, durations, seconds, dictionaries, large and view types) come back through the stored Arrow schema.
/// </summary>
public sealed class ParquetWriter : IAsyncDisposable, IDisposable
{
    private static ReadOnlySpan<byte> Magic => "PAR1"u8;
    private readonly Stream _stream;
    private readonly ParquetWriteOptions _options;
    private readonly SchemaElement[] _elements;
    private readonly ColumnPlan[] _plans;
    private ColumnWriter[] _columns;   // the open row group's column writers (with the caller owning the batches)
    private Shredder?[] _shredders;
    private readonly List<RowGroup> _rowGroups = [];
    private readonly List<(ColumnChunk Chunk, ColumnIndex? Columns, OffsetIndex? Offsets)> _pageIndexes = [];
    private readonly List<(ColumnMetaData Chunk, NativeBufferWriter Bitset)> _bloomFilters = []; // native, freed once written
    private readonly KeyValue[]? _keyValues;
    private readonly WorkBudget _budget;
    private readonly PageSettings[] _columnSettings;
    private Task _writing = Task.CompletedTask; // the last row group's bytes, written while the next one is encoded

    // With the writer owning the batches and more than one thread, row groups are encoded side by side, each by a set
    // of column writers of its own, from the batches themselves; laid out and written in order. With the caller owning
    // them and more than one thread, a full row group's set is sealed instead: finished (its last pages compressed, its
    // dictionary page, indexes) by a task while the next row group fills another set; laid out in order the same way.
    private readonly ConcurrentExclusiveSchedulerPair? _columnWork; // the column work of every row group, MaxDegreeOfParallelism at once
    private readonly ConcurrentStack<(ColumnWriter[] Columns, Shredder?[] Shredders)> _spares = new();
    private readonly Queue<(Task<FinishedChunk[]> Encoding, long Rows, long Bytes)> _encoding = new();
    private long _encodingBytes; // the logical Arrow bytes of the row groups being encoded
    private long _laidOutLogicalBytes, _laidOutChunkBytes; // of the row groups encoded from batches and laid out: their compression ratio
    private readonly List<Segment> _open = []; // the open row group's rows
    private long _openBytes;                   // their batches' logical Arrow bytes
    private long _rowGroupRows, _totalRows, _position;
    private bool _started, _completed, _faulted, _aborted, _disposed;
    private ParquetMetadata? _metadata;

    private ParquetWriter(Stream stream, Apache.Arrow.Schema schema, ParquetWriteOptions options)
    {
        options.Validate();
        if (options.TimestampUnit == TimeUnit.Second)
            throw new ArgumentException("Parquet stores no timestamps in seconds: TimestampUnit must be milliseconds, microseconds or nanoseconds.", nameof(options));
        if (schema.FieldsList.Count == 0) throw new ArgumentException("A Parquet file needs at least one column.", nameof(schema));
        _stream = stream;
        _options = options;
        Schema = schema;
        var (elements, plans) = WriteSchema.Plan(schema, options);
        var settings = new PageSettings(options.Compression.ParquetCodec, options.Compression.ParquetLevel, options.DataPageVersion,
            options.DataPageBytes, options.DataPageRows, options.Dictionary, options.DictionaryPageBytes, options.Statistics,
            options.StatisticsTruncateLength, options.WriteChecksums, options.PageIndex, options.SizeStatistics,
            Budget: _budget = options.SharedBudget ?? new WorkBudget(options.MaxDegreeOfParallelism));
        _elements = [new SchemaElement { Name = "schema", NumChildren = schema.FieldsList.Count }, .. elements];
        _plans = plans;
        ParquetColumnOptions?[] columns = ColumnSettings.Resolve(plans, options.Columns);
        _columnSettings = [.. columns.Select(column => column is not null
            ? settings with { Dictionary = column.Dictionary ?? options.Dictionary, Encoding = column.Encoding, Bloom = column.BloomFilter }
            : settings)];
        (_columns, _shredders) = NewColumns();
        if (options.BatchOwnership == ParquetBatchOwnership.Writer && options.MaxDegreeOfParallelism > 1)
        {
            _columnWork = new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, options.MaxDegreeOfParallelism);
            _spares.Push((_columns, _shredders));
        }
        List<KeyValue> keyValues = [];
        if (options.StoreArrowSchema) keyValues.Add(new KeyValue(Parquet.Schema.ArrowMapping.ArrowSchemaKey, Convert.ToBase64String(ArrowSchemaMessage(schema))));
        if (options.Metadata is { } metadata) keyValues.AddRange(metadata.Select(kv => new KeyValue(kv.Key, kv.Value)));
        _keyValues = keyValues.Count > 0 ? [.. keyValues] : null;
    }

    /// <summary>A set of column writers (and the shredders of nested columns): one row group being encoded.</summary>
    private (ColumnWriter[] Columns, Shredder?[] Shredders) NewColumns()
    {
        ColumnWriter[] columns = [.. _plans.Select((p, c) => p.Leaf.CreateWriter(_columnSettings[c]))];
        Shredder?[] shredders = [.. _plans.Select(p => p.Steps is null ? null : new Shredder(p.Steps))];
        lock (_allColumns)
        {
            _allColumns.AddRange(columns);
            foreach (Shredder? shredder in shredders)
            {
                if (shredder is not null) _allColumns.Add(shredder);
            }
        }
        return (columns, shredders);
    }

    /// <summary>Every column writer and shredder made (their native buffers are freed when the writer is disposed).</summary>
    private readonly List<IDisposable> _allColumns = [];

    /// <summary>Frees the column writers' and shredders' native buffers, once nothing encodes or writes any more.</summary>
    private void FreeColumns()
    {
        lock (_allColumns)
        {
            foreach (IDisposable column in _allColumns) column.Dispose();
            _allColumns.Clear();
        }
        FreeBloomFilters();
    }

    /// <summary>Frees the bitsets of the Bloom filters taken from the chunks laid out (written, or the file given up).</summary>
    private void FreeBloomFilters()
    {
        foreach (var (_, bitset) in _bloomFilters) bitset.Dispose();
        _bloomFilters.Clear();
    }

    /// <summary>The Arrow schema the batches must have.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// The bytes of the file so far (the row groups closed, and the footer once completed; with more than one thread,
    /// the last few row groups closed by <see cref="Write(RecordBatch)"/> may still be on their way to the stream until
    /// later ones close, <see cref="EndRowGroup"/> is called or the file completes).
    /// </summary>
    public long BytesWritten => _position;

    /// <summary>
    /// About how many bytes the row groups not in <see cref="BytesWritten"/> yet will take: the open one (its pages so
    /// far, the open ones as plainly encoded) and, with more than one thread, those closed and still being finished
    /// (exactly: waits for them). With <see cref="ParquetBatchOwnership.Writer"/> and more than one thread,
    /// also those still being encoded: exactly once encoded, else (and the open one) their batches' logical Arrow bytes scaled by
    /// the ratio the row groups laid out so far compressed to (unscaled before the first). So BytesWritten +
    /// PendingBytes is what the file would take now, which a caller splitting files by size can go by.
    /// </summary>
    public long PendingBytes
    {
        get
        {
            if (_columnWork is null)
            {
                long sealedBytes = 0;
                foreach (var (sealing, _, _) in _encoding) sealedBytes += sealing.GetAwaiter().GetResult().Sum(c => c.CompressedSize);
                return sealedBytes + _columns.Sum(c => c.BufferedBytes);
            }
            double ratio = _laidOutLogicalBytes > 0 ? (double)_laidOutChunkBytes / _laidOutLogicalBytes : 1;
            double pending = _openBytes * ratio;
            foreach (var (encoding, _, bytes) in _encoding)
            {
                pending += encoding.IsCompletedSuccessfully ? encoding.Result.Sum(c => c.CompressedSize) : bytes * ratio;
            }
            return (long)pending;
        }
    }

    /// <summary>
    /// At least what the row groups not in <see cref="BytesWritten"/> yet take, with the caller owning the batches,
    /// known without waiting for pages being compressed (those count at their largest compressed size): for holding
    /// many writers' buffered bytes under a budget.
    /// </summary>
    internal long PendingBytesBound
    {
        get
        {
            long bytes = 0;
            foreach (ColumnWriter column in _columns) bytes += column.BufferedBytesBound;
            foreach (var (sealing, _, bound) in _encoding)
            {
                if (!sealing.IsCompletedSuccessfully)
                {
                    bytes += bound;
                    continue;
                }
                foreach (FinishedChunk chunk in sealing.Result) bytes += chunk.CompressedSize;
            }
            return bytes;
        }
    }

    /// <summary>The native bytes the open row group's column writers keep as scratch (see <see cref="TrimScratch"/>).</summary>
    internal long ScratchBytes
    {
        get
        {
            long bytes = 0;
            foreach (ColumnWriter column in _columns) bytes += column.ScratchBytes;
            return bytes;
        }
    }

    /// <summary>
    /// Returns the column writers' scratch memory to the pool, between row groups (after <see cref="EndRowGroup"/>):
    /// for a writer that stays open long with little to write (one per partition), at the cost of growing it again.
    /// </summary>
    /// <exception cref="InvalidOperationException">A row group is open.</exception>
    internal void TrimScratch()
    {
        if (_rowGroupRows != 0) throw new InvalidOperationException("A row group is open.");
        foreach (ColumnWriter column in _columns) column.Trim();
    }

    /// <summary>
    /// The rows of a batch the columns take at once, in parallel; the row group's size is checked after each segment
    /// (segments depend on the rows alone, never on the threads, so neither do the file's bytes).
    /// </summary>
    private const int SegmentRows = 16_384;

    /// <summary>Whether the open row group reached RowGroupBytes: decided on the pages' exact sizes, waited for only near the limit.</summary>
    private bool RowGroupFull() =>
        _columns.Sum(c => c.BufferedBytesBound) >= _options.RowGroupBytes && _columns.Sum(c => c.BufferedBytes) >= _options.RowGroupBytes;

    /// <summary>
    /// Runs <paramref name="work"/> for every column, in parallel when the budget allows and the work is worth it; the
    /// first column's exception (in column order, as one thread would meet it) is rethrown.
    /// </summary>
    private void ForEachColumn(Action<int> work, bool parallel)
    {
        if (!parallel || _budget.Max == 1 || _columns.Length == 1)
        {
            _budget.Enter();
            try
            {
                for (int c = 0; c < _columns.Length; c++) work(c);
            }
            finally
            {
                _budget.Exit();
            }
            return;
        }
        var errors = new Exception?[_columns.Length];
        Parallel.For(0, _columns.Length, new ParallelOptions { MaxDegreeOfParallelism = _budget.Max }, c =>
        {
            _budget.Enter();
            try
            {
                work(c);
            }
            catch (Exception e)
            {
                errors[c] = e;
            }
            finally
            {
                _budget.Exit();
            }
        });
        if (errors.FirstOrDefault(e => e is not null) is { } first) ExceptionDispatchInfo.Throw(first);
    }

    /// <summary>The footer written, once the file is complete.</summary>
    /// <exception cref="InvalidOperationException">The file is not complete yet.</exception>
    public ParquetMetadata Metadata => _metadata ?? throw new InvalidOperationException("The Parquet file is not complete yet: its footer is written by Complete.");

    /// <summary>Creates a writer over a stream. Nothing is written before the first row group closes (or the file completes).</summary>
    public static ParquetWriter Create(Stream stream, Apache.Arrow.Schema schema, ParquetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        if (!stream.CanWrite) throw new ArgumentException("The stream is not writable.", nameof(stream));
        return new(stream, schema, options ?? new());
    }

    /// <summary>Creates (or overwrites) a file.</summary>
    public static ParquetWriter Create(string path, Apache.Arrow.Schema schema, ParquetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1, FileOptions.Asynchronous);
        try
        {
            return new(stream, schema, (options ?? new()) with { LeaveOpen = false });
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>Writes all batches to a stream and completes the file. If writing fails, the stream holds a file without its footer.</summary>
    public static void Write(Stream stream, Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches, ParquetWriteOptions? options = null)
    {
        using var writer = Create(stream, schema, options);
        writer.WriteAll(batches);
    }

    /// <summary>Writes all batches to a file and completes it. If writing fails, the partial file is deleted.</summary>
    public static void Write(string path, Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches, ParquetWriteOptions? options = null)
    {
        var writer = Create(path, schema, options);
        try
        {
            writer.WriteAll(batches);
        }
        catch
        {
            writer.Dispose();
            File.Delete(path);
            throw;
        }
        writer.Dispose();
    }

    /// <inheritdoc cref="Write(Stream, Apache.Arrow.Schema, IEnumerable{RecordBatch}, ParquetWriteOptions?)"/>
    public static async Task WriteAsync(Stream stream, Apache.Arrow.Schema schema, IAsyncEnumerable<RecordBatch> batches,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        await using var writer = Create(stream, schema, options);
        await writer.WriteAllAsync(batches, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="Write(string, Apache.Arrow.Schema, IEnumerable{RecordBatch}, ParquetWriteOptions?)"/>
    public static async Task WriteAsync(string path, Apache.Arrow.Schema schema, IAsyncEnumerable<RecordBatch> batches,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        var writer = Create(path, schema, options);
        try
        {
            await writer.WriteAllAsync(batches, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await writer.DisposeAsync().ConfigureAwait(false);
            File.Delete(path);
            throw;
        }
        await writer.DisposeAsync().ConfigureAwait(false);
    }

    private void WriteAll(IEnumerable<RecordBatch> batches)
    {
        try
        {
            foreach (RecordBatch batch in batches) Write(batch);
        }
        catch
        {
            Abort();
            throw;
        }
        Complete();
    }

    private async Task WriteAllAsync(IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        try
        {
            await foreach (RecordBatch batch in batches.WithCancellation(ct).ConfigureAwait(false))
                await WriteAsync(batch, ct).ConfigureAwait(false);
        }
        catch
        {
            Abort();
            throw;
        }
        await CompleteAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Adds a batch's rows. By default the batch is not kept: it may be disposed once this returns. With
    /// <see cref="ParquetBatchOwnership.Writer"/> the writer takes it (in every case, also when this fails) and disposes
    /// it once its rows are encoded, perhaps after this returns.
    /// </summary>
    public void Write(RecordBatch batch) => WriteCoreAsync(batch, false, default).AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// Writes rows <paramref name="start"/> .. +<paramref name="count"/> of a batch, as <see cref="Write(RecordBatch)"/>
    /// writes a slice of it, without slicing (a slice is an array object per column: a sink cutting batches into runs
    /// of many partitions made them by the hundred thousand). The caller owns the batch.
    /// </summary>
    internal void Write(RecordBatch batch, int start, int count)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (_options.BatchOwnership != ParquetBatchOwnership.Caller)
            throw new InvalidOperationException("Rows of a batch are written only when the caller owns the batches.");
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, batch.Length - start);
        WriteNowAsync(batch, start, count, false, default).AsTask().GetAwaiter().GetResult();
    }

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => WriteCoreAsync(batch, true, cancellationToken);

    /// <summary>
    /// Closes the open row group now (nothing happens when it holds no rows); its bytes, and those of the row groups
    /// before it, are written to the stream before this returns (a failure to write them surfaces here).
    /// </summary>
    public void EndRowGroup() => EndRowGroupCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="EndRowGroup"/>
    public ValueTask EndRowGroupAsync(CancellationToken cancellationToken = default) => EndRowGroupCoreAsync(true, cancellationToken);

    /// <summary>Writes the last row group and the footer, and flushes the stream.</summary>
    public void Complete() => CompleteCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => CompleteCoreAsync(true, cancellationToken);

    /// <summary>
    /// Gives the file up: <see cref="Dispose"/> then frees the writer without completing it (no last row group, no
    /// footer: the stream keeps what was written so far, and is closed unless <see cref="ParquetWriteOptions.LeaveOpen"/>).
    /// For a failure outside the writer, which it cannot see:
    /// <c>try { …; writer.Complete(); } catch { writer.Abort(); throw; }</c>. Writing or completing afterwards throws.
    /// Does no I/O and waits for nothing; repeating it, or calling it on a completed or disposed writer, does nothing.
    /// </summary>
    public void Abort()
    {
        if (!_completed && !_disposed) _aborted = true;
    }

    private void CheckWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("The Parquet writer has been completed.");
        if (_aborted) throw new InvalidOperationException("The Parquet writer was aborted; the file cannot be completed.");
        if (_faulted) throw new InvalidOperationException("The Parquet writer failed earlier; the file cannot be completed.");
    }

    private async ValueTask WriteCoreAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (_options.BatchOwnership == ParquetBatchOwnership.Caller)
        {
            await WriteNowAsync(batch, 0, batch.Length, useAsync, ct).ConfigureAwait(false);
            return;
        }
        var taken = new Taken(batch);
        try
        {
            if (_columnWork is null) await WriteNowAsync(batch, 0, batch.Length, useAsync, ct).ConfigureAwait(false);
            else await WriteTakenAsync(taken, useAsync, ct).ConfigureAwait(false);
        }
        finally
        {
            taken.Done(); // this call's hold on it
        }
    }

    /// <summary>
    /// Encodes rows <paramref name="start"/> .. +<paramref name="count"/> of a batch before returning, segment by
    /// segment, each segment's columns side by side.
    /// </summary>
    private async ValueTask WriteNowAsync(RecordBatch batch, int start, int count, bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        CheckBatch(batch, start, count);
        try
        {
            int offset = start, end = start + count;
            while (offset < end)
            {
                int take = (int)Math.Min(end - offset, Math.Min(_options.RowGroupRows - _rowGroupRows, SegmentRows));
                int at = offset;
                ForEachColumn(c => WriteColumn(_columns[c], _shredders[c], batch.Column(_plans[c].Field), at, take),
                    parallel: (long)take * _columns.Length >= 4096);
                _rowGroupRows += take;
                offset += take;
                if (_rowGroupRows >= _options.RowGroupRows || RowGroupFull())
                    await FlushRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>Adds rows of a column to its writer; nested columns are shredded in steps, keeping the level buffers small.</summary>
    private static void WriteColumn(ColumnWriter column, Shredder? shredder, IArrowArray array, int at, int take)
    {
        if (shredder is null)
        {
            column.Write(array, at, take);
            return;
        }
        for (int step = 0; step < take; step += 1024)
        {
            int n = Math.Min(1024, take - step);
            shredder.Shred(array, at + step, n);
            column.Write(shredder.Def, shredder.Rep, shredder.Leaf, shredder.Indices);
        }
    }

    /// <summary>A batch the writer owns: disposed once nothing reads it (the Write call, and each row group encoding some of its rows).</summary>
    private sealed class Taken(RecordBatch batch)
    {
        private int _readers = 1;

        public RecordBatch Batch => batch;

        public void Read() => Interlocked.Increment(ref _readers);

        public void Done()
        {
            if (Interlocked.Decrement(ref _readers) == 0) batch.Dispose();
        }
    }

    /// <summary>Rows of a batch in a row group (cut as <see cref="WriteNowAsync"/> cuts them: the file's bytes are the same).</summary>
    private readonly record struct Segment(Taken Batch, int Offset, int Rows);

    /// <summary>The row groups encoded at once at most (each holding its batches and its column writers).</summary>
    internal int MaxRowGroupsInFlight => Math.Max(2, _options.MaxDegreeOfParallelism / 2);

    /// <summary>
    /// The row groups sealed and not laid out yet, at most, with the caller owning the batches (each holding its
    /// compressed pages and a set of column writers; finishing one takes far less than filling the next).
    /// </summary>
    internal const int MaxSealedRowGroups = 1;

    /// <summary>The most row groups that were being encoded at once (for tests).</summary>
    internal int PeakRowGroupsInFlight { get; private set; }

    /// <summary>
    /// Adds a batch the writer owns to the open row group, as segments; a row group full (by rows, or by its batches'
    /// logical Arrow bytes: known before encoding) goes to be encoded while the next one fills.
    /// </summary>
    private async ValueTask WriteTakenAsync(Taken taken, bool useAsync, CancellationToken ct)
    {
        RecordBatch batch = taken.Batch;
        CheckWritable();
        CheckBatch(batch, 0, batch.Length);
        try
        {
            long bytes = ArrowSizes.LogicalBytes(batch);
            int offset = 0;
            while (offset < batch.Length)
            {
                int take = (int)Math.Min(batch.Length - offset, Math.Min(_options.RowGroupRows - _rowGroupRows, SegmentRows));
                taken.Read();
                _open.Add(new Segment(taken, offset, take));
                _rowGroupRows += take;
                _openBytes += bytes * take / batch.Length;
                offset += take;
                if (_rowGroupRows >= _options.RowGroupRows || _openBytes >= _options.RowGroupBytes)
                    await CloseRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>
    /// Sends the open row group to be encoded by a set of column writers of its own, once fewer than
    /// <see cref="MaxRowGroupsInFlight"/> are, holding less than <see cref="ParquetWriteOptions.InFlightBytes"/> with it (the oldest are laid
    /// out and written meanwhile).
    /// </summary>
    private async ValueTask CloseRowGroupAsync(bool useAsync, CancellationToken ct)
    {
        if (_rowGroupRows == 0) return;
        while (_encoding.Count >= MaxRowGroupsInFlight || _encoding.Count > 0 && _encodingBytes + _openBytes > _options.InFlightBytes)
            await WriteEncodedAsync(useAsync, ct).ConfigureAwait(false);
        Segment[] segments = [.. _open];
        _open.Clear();
        long rows = _rowGroupRows, bytes = _openBytes;
        _rowGroupRows = 0;
        _openBytes = 0;
        var set = _spares.TryPop(out var spare) ? spare : NewColumns();
        _encoding.Enqueue((Task.Run(() => EncodeAsync(set, segments)), rows, bytes));
        _encodingBytes += bytes;
        PeakRowGroupsInFlight = Math.Max(PeakRowGroupsInFlight, _encoding.Count);
    }

    /// <summary>Encodes a row group's segments and finishes its chunks; its batches are let go, its column writers kept for another.</summary>
    private async Task<FinishedChunk[]> EncodeAsync((ColumnWriter[] Columns, Shredder?[] Shredders) set, Segment[] segments)
    {
        var finished = new FinishedChunk[set.Columns.Length];
        bool reusable = false;
        try
        {
            foreach (Segment segment in segments)
            {
                RecordBatch batch = segment.Batch.Batch;
                await ForEachColumnAsync(c => WriteColumn(set.Columns[c], set.Shredders[c], batch.Column(_plans[c].Field), segment.Offset, segment.Rows),
                    parallel: (long)segment.Rows * set.Columns.Length >= 4096).ConfigureAwait(false);
            }
            await ForEachColumnAsync(c => finished[c] = set.Columns[c].Finish(), parallel: true).ConfigureAwait(false);
            foreach (ColumnWriter column in set.Columns) column.Reset();
            reusable = true;
            return finished;
        }
        catch
        {
            foreach (FinishedChunk chunk in finished)
            {
                if (chunk.Pages is not null) chunk.Release(); // chunks finished before the failure
            }
            throw;
        }
        finally
        {
            foreach (Segment segment in segments) segment.Batch.Done();
            if (reusable) _spares.Push(set);
        }
    }

    /// <summary>
    /// Runs <paramref name="work"/> for every column on the writer's column scheduler (at most MaxDegreeOfParallelism
    /// at once, whatever the row groups); the first column's exception, in column order, is rethrown.
    /// </summary>
    private async Task ForEachColumnAsync(Action<int> work, bool parallel)
    {
        TaskScheduler scheduler = _columnWork!.ConcurrentScheduler;
        int count = _plans.Length;
        if (!parallel || count == 1)
        {
            await Task.Factory.StartNew(() => Counted(0, count), CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler).ConfigureAwait(false);
            return;
        }
        var tasks = new Task[count];
        for (int c = 0; c < count; c++)
        {
            int column = c;
            tasks[c] = Task.Factory.StartNew(() => Counted(column, column + 1), CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler);
        }
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch
        {
            ExceptionDispatchInfo.Throw(System.Array.Find(tasks, t => t.IsFaulted)!.Exception!.InnerException!);
        }

        void Counted(int first, int end)
        {
            _budget.Enter();
            try
            {
                for (int c = first; c < end; c++) work(c);
            }
            finally
            {
                _budget.Exit();
            }
        }
    }

    /// <summary>Lays out and writes the oldest row group being encoded, once it is (its failure surfaces here).</summary>
    private async ValueTask WriteEncodedAsync(bool useAsync, CancellationToken ct)
    {
        var (encoding, rows, bytes) = _encoding.Dequeue();
        FinishedChunk[] finished;
        if (_columnWork is null) finished = await encoding.ConfigureAwait(false); // a sealed row group (its bytes a bound)
        else
        {
            _encodingBytes -= bytes;
            finished = await encoding.ConfigureAwait(false);
            _laidOutLogicalBytes += bytes;
            _laidOutChunkBytes += finished.Sum(c => c.CompressedSize);
        }
        await LayOutAsync(finished, rows, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>Closes the open row group and lays out every row group encoded or being encoded.</summary>
    private async ValueTask EndOpenRowGroupAsync(bool useAsync, CancellationToken ct)
    {
        if (_columnWork is null) await FlushRowGroupAsync(useAsync, ct).ConfigureAwait(false);
        else await CloseRowGroupAsync(useAsync, ct).ConfigureAwait(false);
        while (_encoding.Count > 0) await WriteEncodedAsync(useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>Lets go of the rows not written (a writer given up): the open row group's batches, the row groups being encoded.</summary>
    private void Abandon()
    {
        foreach (Segment segment in _open) segment.Batch.Done();
        _open.Clear();
        while (_encoding.TryDequeue(out var pending))
        {
            try
            {
                foreach (FinishedChunk chunk in pending.Encoding.GetAwaiter().GetResult()) chunk.Release();
            }
            catch (Exception)
            {
                // Its failure no longer matters: the file is given up.
            }
        }
    }

    /// <summary>
    /// The batch's columns have the writer's types and lengths, and no null where the schema says non-nullable, at
    /// any depth (checked before anything is written).
    /// </summary>
    /// <summary>The batch fits the writer schema, and rows <paramref name="start"/> .. +<paramref name="count"/> its nullability.</summary>
    private void CheckBatch(RecordBatch batch, int start, int count)
    {
        int fields = Schema.FieldsList.Count;
        if (batch.ColumnCount != fields)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns; the writer schema has {fields}.", nameof(batch));
        for (int f = 0; f < fields; f++)
        {
            Field field = Schema.FieldsList[f];
            IArrowArray column = batch.Column(f);
            if (column.Length != batch.Length)
                throw new ArgumentException($"Column {f} ('{field.Name}') has {column.Length} rows; the batch has {batch.Length}.", nameof(batch));
            if (!ReferenceEquals(batch.Schema, Schema) && !ArrowTypes.SameType(field.DataType, column.Data.DataType))
                throw new ArgumentException($"Column {f} ('{field.Name}') is {column.Data.DataType.Name} in the batch but {field.DataType.Name} in the writer schema.", nameof(batch));
            if (Mismatch(field.DataType, column.Data, field.Name) is { } where)
                throw new ArgumentException($"The arrays of column {f} ('{field.Name}') do not have the types its type declares: {where}.", nameof(batch));
        }
        for (int c = 0; c < _columns.Length; c++)
        {
            IArrowArray column = batch.Column(_plans[c].Field);
            if (_shredders[c] is { } shredder)
            {
                try
                {
                    shredder.Validate(column);
                }
                catch (ArgumentException e)
                {
                    throw new ArgumentException(e.Message, nameof(batch));
                }
            }
            else if (_plans[c].Leaf.Element.Repetition == FieldRepetition.Required && ArrowValues.NullCount(column) > 0)
            {
                for (int i = start; i < start + count; i++)
                {
                    if (ArrowValues.IsNull(column, i))
                        throw new ArgumentException($"Column '{_plans[c].Path[0]}' is not nullable but row {i} of the batch is null.", nameof(batch));
                }
            }
        }
    }

    /// <summary>
    /// Where the arrays under a nested array do not have the types its type declares (Apache.Arrow builds such arrays
    /// without a check), or null.
    /// </summary>
    private static string? Mismatch(IArrowType declared, ArrayData data, string where)
    {
        if (!ArrowTypes.SameType(declared, data.DataType)) return $"'{where}' is {data.DataType.Name}, not {declared.Name}";
        return declared switch
        {
            StructType st when data.Children.Length != st.Fields.Count => $"'{where}' has {data.Children.Length} child arrays for {st.Fields.Count} fields",
            StructType st => FieldsMismatch(st, data, where),
            ListType or LargeListType or FixedSizeListType or ListViewType or LargeListViewType when data.Children.Length != 1 => $"'{where}' has no item array",
            ListType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            LargeListType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            FixedSizeListType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            ListViewType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            LargeListViewType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            MapType m when data.Children.Length != 1 => $"'{where}' has no entries array",
            MapType m => Mismatch(m.KeyValueType, data.Children[0], where),
            RunEndEncodedType r when data.Children.Length != 2 => $"'{where}' has no run ends and values arrays",
            RunEndEncodedType r => Mismatch(r.RunEndsDataType, data.Children[0], where + ".run_ends") ?? Mismatch(r.ValuesDataType, data.Children[1], where + ".values"),
            DictionaryType d when data.Dictionary is null => $"'{where}' has no dictionary",
            DictionaryType d => Mismatch(d.ValueType, data.Dictionary, where + ".dictionary"),
            _ => null,
        };
    }

    /// <summary>
    /// The first mismatch among a struct's fields (a loop of its own: a lambda in <see cref="Mismatch"/> would make it
    /// allocate a closure on every call, every type).
    /// </summary>
    private static string? FieldsMismatch(StructType type, ArrayData data, string where)
    {
        for (int k = 0; k < type.Fields.Count; k++)
        {
            Field field = type.Fields[k];
            if (Mismatch(field.DataType, data.Children[k], $"{where}.{field.Name}") is { } mismatch) return mismatch;
        }
        return null;
    }

    private async ValueTask EndRowGroupCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        try
        {
            await EndOpenRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            await _writing.ConfigureAwait(false); // written behind only when Write closes the group
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private async ValueTask FlushRowGroupAsync(bool useAsync, CancellationToken ct)
    {
        if (_rowGroupRows == 0) return;
        long rows = _rowGroupRows;
        // Writers sharing a budget (one per partition, many open) do not seal: a second set of column writers each would
        // double what they hold.
        if (_budget.Max > 1 && _options.SharedBudget is null)
        {
            // Sealed: its column writers finish it on a task (the oldest sealed laid out first when too many are),
            // the next row group fills a spare set.
            while (_encoding.Count >= MaxSealedRowGroups) await WriteEncodedAsync(useAsync, ct).ConfigureAwait(false);
            var set = (_columns, _shredders);
            (_columns, _shredders) = _spares.TryPop(out var spare) ? spare : NewColumns();
            _rowGroupRows = 0;
            // Its bytes as far as known without waiting (the pages in flight at their largest), for PendingBytesBound.
            _encoding.Enqueue((Task.Run(() => Finish(set, spare: true)), rows, set.Item1.Sum(c => c.BufferedBytesBound)));
            PeakRowGroupsInFlight = Math.Max(PeakRowGroupsInFlight, _encoding.Count);
            return;
        }
        FinishedChunk[] finished = Finish((_columns, _shredders), spare: false);
        _rowGroupRows = 0;
        await LayOutAsync(finished, rows, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Finishes every chunk of a set of column writers (last pages, dictionary pages, statistics, indexes) in parallel
    /// and readies the set for another row group (a sealed set goes back to the spares); on a failure the chunks
    /// finished are let go and the set is not used again.
    /// </summary>
    private FinishedChunk[] Finish((ColumnWriter[] Columns, Shredder?[] Shredders) set, bool spare)
    {
        var finished = new FinishedChunk[set.Columns.Length];
        try
        {
            ForEachColumn(c => finished[c] = set.Columns[c].Finish(), parallel: true);
            foreach (ColumnWriter column in set.Columns) column.Reset();
        }
        catch
        {
            foreach (FinishedChunk chunk in finished)
            {
                if (chunk.Pages is not null) chunk.Release();
            }
            throw;
        }
        if (spare) _spares.Push(set);
        return finished;
    }

    /// <summary>
    /// Lays a row group's finished chunks out after the row groups before it (in column order) and writes them: after
    /// the previous row group's bytes, by a task when threads are allowed. The chunks are released once written.
    /// </summary>
    private async ValueTask LayOutAsync(FinishedChunk[] finished, long rows, bool useAsync, CancellationToken ct)
    {
        await StartAsync(useAsync, ct).ConfigureAwait(false);
        long groupStart = _position, uncompressed = 0, compressed = 0;
        var chunks = new ColumnChunk[finished.Length];
        List<ReadOnlyMemory<byte>> pieces = [];
        for (int c = 0; c < finished.Length; c++)
        {
            FinishedChunk chunk = finished[c];
            long start = _position;
            pieces.Add(chunk.DictionaryPage);
            pieces.AddRange(chunk.DataPages);
            _position += chunk.CompressedSize;
            SchemaElement element = _plans[c].Leaf.Element;
            var meta = new ColumnMetaData
            {
                Type = element.Type!.Value,
                Encodings = chunk.Encodings,
                PathInSchema = _plans[c].Path,
                Codec = _options.Compression.ParquetCodec,
                NumValues = chunk.Values,
                TotalUncompressedSize = chunk.UncompressedSize,
                TotalCompressedSize = chunk.CompressedSize,
                DataPageOffset = start + chunk.DictionaryPage.Length,
                DictionaryPageOffset = chunk.DictionaryPage.IsEmpty ? null : start,
                Statistics = chunk.Statistics,
                EncodingStats = chunk.EncodingStats,
                SizeStatistics = chunk.SizeStatistics,
            };
            chunks[c] = new ColumnChunk { FileOffset = start, MetaData = meta };
            if (chunk.BloomFilter is { } bitset) _bloomFilters.Add((meta, bitset));
            if (chunk.ColumnIndex is not null || chunk.OffsetIndex is not null)
                _pageIndexes.Add((chunks[c], chunk.ColumnIndex, Rebase(chunk.OffsetIndex, meta.DataPageOffset)));
            uncompressed += chunk.UncompressedSize;
            compressed += chunk.CompressedSize;
        }
        await _writing.ConfigureAwait(false); // one row group on its way at most
        if (_budget.Max == 1)
        {
            try
            {
                await WritePiecesAsync(pieces, useAsync, ct).ConfigureAwait(false);
            }
            finally
            {
                foreach (FinishedChunk chunk in finished) chunk.ReleasePages(); // the Bloom filters are the writer's now
            }
        }
        else
        {
            _writing = Task.Run(async () =>
            {
                try
                {
                    await WritePiecesAsync(pieces, useAsync, ct).ConfigureAwait(false);
                }
                finally
                {
                    foreach (FinishedChunk chunk in finished) chunk.ReleasePages();
                }
            }, CancellationToken.None);
        }
        _rowGroups.Add(new RowGroup
        {
            Columns = chunks,
            TotalByteSize = uncompressed,
            NumRows = rows,
            FileOffset = groupStart,
            TotalCompressedSize = compressed,
            Ordinal = (short)Math.Min(_rowGroups.Count, short.MaxValue),
        });
        _totalRows += rows;
    }

    /// <summary>An offset index whose page offsets count from the chunk's data pages, with file offsets.</summary>
    private static OffsetIndex? Rebase(OffsetIndex? index, long dataPages) => index is null ? null : new OffsetIndex
    {
        PageLocations = [.. index.PageLocations.Select(p => p with { Offset = p.Offset + dataPages })],
        UnencodedByteArrayDataBytes = index.UnencodedByteArrayDataBytes,
    };

    /// <summary>
    /// Writes the Bloom filters after the row groups (each a header and its bitset, as pyarrow places them: together,
    /// so that a reader fetches them at once) and points the column chunks at them.
    /// </summary>
    private async ValueTask WriteBloomFiltersAsync(bool useAsync, CancellationToken ct)
    {
        var thrift = new ThriftCompactWriter();
        foreach (var (chunk, bitset) in _bloomFilters)
        {
            thrift.Clear();
            // Members by field id: algorithm BLOCK = 1, hash XXHASH = 1, compression UNCOMPRESSED = 1.
            new BloomFilterHeader(bitset.WrittenCount, 1, 1, 1).Write(thrift);
            chunk.BloomFilterOffset = _position;
            chunk.BloomFilterLength = thrift.WrittenSpan.Length + bitset.WrittenCount;
            await WriteBytesAsync(thrift.WrittenSpan.ToArray(), useAsync, ct).ConfigureAwait(false);
            await WriteBytesAsync(bitset.WrittenMemory, useAsync, ct).ConfigureAwait(false);
        }
        FreeBloomFilters();
    }

    /// <summary>
    /// Writes the page indexes after the row groups (every column index, then every offset index, as parquet-mr and
    /// Arrow place them) and points the column chunks at them.
    /// </summary>
    private async ValueTask WritePageIndexesAsync(bool useAsync, CancellationToken ct)
    {
        var thrift = new ThriftCompactWriter();
        foreach (var (chunk, columns, _) in _pageIndexes)
        {
            if (columns is null) continue;
            thrift.Clear();
            columns.Write(thrift);
            chunk.ColumnIndexOffset = _position;
            chunk.ColumnIndexLength = thrift.WrittenSpan.Length;
            await WriteBytesAsync(thrift.WrittenSpan.ToArray(), useAsync, ct).ConfigureAwait(false);
        }
        foreach (var (chunk, _, offsets) in _pageIndexes)
        {
            if (offsets is null) continue;
            thrift.Clear();
            offsets.Write(thrift);
            chunk.OffsetIndexOffset = _position;
            chunk.OffsetIndexLength = thrift.WrittenSpan.Length;
            await WriteBytesAsync(thrift.WrittenSpan.ToArray(), useAsync, ct).ConfigureAwait(false);
        }
        _pageIndexes.Clear();
    }

    private async ValueTask StartAsync(bool useAsync, CancellationToken ct)
    {
        if (_started) return;
        _started = true;
        await WriteBytesAsync(Magic.ToArray(), useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes page headers and bodies (laid out beforehand: <see cref="_position"/> already counts them), small pieces
    /// gathered into one write, large ones as they are.
    /// </summary>
    private async Task WritePiecesAsync(IReadOnlyList<ReadOnlyMemory<byte>> pieces, bool useAsync, CancellationToken ct)
    {
        const int Gather = 1 << 16;
        var staging = new NativeByteBuffer(Gather, pooled: true);
        try
        {
            int staged = 0;
            foreach (ReadOnlyMemory<byte> piece in pieces)
            {
                if (staged + piece.Length > Gather && staged > 0)
                {
                    await RawWriteAsync(staging.Memory[..staged], useAsync, ct).ConfigureAwait(false);
                    staged = 0;
                }
                if (piece.Length >= Gather) await RawWriteAsync(piece, useAsync, ct).ConfigureAwait(false);
                else
                {
                    piece.Span.CopyTo(staging.Span[staged..]);
                    staged += piece.Length;
                }
            }
            if (staged > 0) await RawWriteAsync(staging.Memory[..staged], useAsync, ct).ConfigureAwait(false);
        }
        finally
        {
            staging.Release();
        }
    }

    private async ValueTask WriteBytesAsync(ReadOnlyMemory<byte> bytes, bool useAsync, CancellationToken ct)
    {
        await RawWriteAsync(bytes, useAsync, ct).ConfigureAwait(false);
        _position += bytes.Length;
    }

    private async ValueTask RawWriteAsync(ReadOnlyMemory<byte> bytes, bool useAsync, CancellationToken ct)
    {
        if (bytes.IsEmpty) return;
        if (useAsync) await _stream.WriteAsync(bytes, ct).ConfigureAwait(false);
        else _stream.Write(bytes.Span);
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) return;
        if (_aborted) throw new InvalidOperationException("The Parquet writer was aborted; the file cannot be completed.");
        if (_faulted) throw new InvalidOperationException("The Parquet writer failed earlier; the file cannot be completed.");
        try
        {
            await EndOpenRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            await StartAsync(useAsync, ct).ConfigureAwait(false);
            await _writing.ConfigureAwait(false);
            await WriteBloomFiltersAsync(useAsync, ct).ConfigureAwait(false);
            await WritePageIndexesAsync(useAsync, ct).ConfigureAwait(false);
            var footer = new FileMetaData
            {
                Version = 2,
                Schema = _elements,
                NumRows = _totalRows,
                RowGroups = [.. _rowGroups],
                KeyValueMetadata = _keyValues,
                CreatedBy = CreatedBy,
                ColumnOrders = [.. _columns.Select(_ => ColumnOrder.TypeDefinedOrder)],
            };
            var thrift = new ThriftCompactWriter();
            footer.Write(thrift);
            byte[] tail = new byte[thrift.WrittenSpan.Length + 8];
            thrift.WrittenSpan.CopyTo(tail);
            BinaryPrimitives.WriteInt32LittleEndian(tail.AsSpan(thrift.WrittenSpan.Length), thrift.WrittenSpan.Length);
            Magic.CopyTo(tail.AsSpan(tail.Length - 4));
            await WriteBytesAsync(tail, useAsync, ct).ConfigureAwait(false);
            if (useAsync) await _stream.FlushAsync(ct).ConfigureAwait(false);
            else _stream.Flush();
            _completed = true;
            _metadata = ParquetMetadata.FromThrift(footer);
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>The writer's name and version, as readers parse it (<c>app version x.y.z</c>).</summary>
    private static readonly string CreatedBy = $"seseragi version {typeof(ParquetWriter).Assembly.GetName().Version?.ToString(3) ?? "0.0.0"}";

    /// <summary>The Arrow IPC schema message, as Arrow writers store it in <c>ARROW:schema</c>.</summary>
    private static byte[] ArrowSchemaMessage(Apache.Arrow.Schema schema)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ArrowStreamWriter(buffer, schema, leaveOpen: true)) writer.WriteStart();
        return buffer.ToArray();
    }

    /// <summary>Lets a row group still on its way to the stream finish (its failure was or will be reported elsewhere).</summary>
    private void WaitForWrites()
    {
        try
        {
            _writing.Wait();
        }
        catch (AggregateException)
        {
            // A failed write faulted the writer where it was awaited (or the file is being given up anyway).
        }
    }

    /// <summary>
    /// Completes the file unless writing failed or the writer was aborted (<see cref="Abort"/>; then the stream keeps
    /// what was written, without a footer), and closes the stream unless asked not to.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted && !_aborted) Complete();
        }
        finally
        {
            _disposed = true;
            Abandon();
            WaitForWrites();
            FreeColumns();
            if (!_options.LeaveOpen) _stream.Dispose();
        }
    }

    /// <inheritdoc cref="Dispose"/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted && !_aborted) await CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
            Abandon();
            WaitForWrites();
            FreeColumns();
            if (!_options.LeaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
