using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.IO.Writing;

/// <summary>What a text format writes: its rows, and what follows the last.</summary>
internal interface IRowEncoder<TCell>
{
    /// <summary>
    /// Rows [<paramref name="start"/>, <paramref name="end"/>) of a batch's cells appended to <paramref name="buffer"/>,
    /// the first of them row <paramref name="first"/> of all written. A value the format cannot hold fails naming its row.
    /// </summary>
    void EncodeRows(TCell[] cells, NativeBufferWriter buffer, int start, int end, long first);

    /// <summary>The bytes after the last row (none: empty), given the rows written.</summary>
    ReadOnlySpan<byte> Tail(long rows);
}

/// <summary>How a <see cref="RowWriter{TCell}"/> works (its format's options, checked).</summary>
/// <param name="Format">The format's name, for messages ("JSON", "CSV").</param>
/// <param name="MaxDegreeOfParallelism">How many pieces are encoded at once.</param>
/// <param name="Ownership">Who owns the batches written.</param>
/// <param name="Compression">The pieces' compression.</param>
/// <param name="CompressionLevel">The compression level (the codec's default when null).</param>
/// <param name="PieceBytes">The encoded bytes a piece aims at.</param>
/// <param name="InFlightBytes">The bytes of encoded pieces on their way at most (one batch goes beyond alone).</param>
/// <param name="RowBytes">The first guess of a row's encoded bytes (pieces are sized by it until rows are measured).</param>
/// <param name="Window">The pieces encoded (or being encoded) and not yet written, at most (the first not written always starts).</param>
internal readonly record struct RowWriterSettings(
    string Format, int MaxDegreeOfParallelism, BatchOwnership Ownership, PieceCompression Compression, int? CompressionLevel,
    int PieceBytes, long InFlightBytes, double RowBytes, int Window);

/// <summary>
/// The engine of the text writers (JSON, CSV): batches cut into pieces of rows encoded on several threads into native
/// memory and written in order as they come (the bytes do not depend on the threads); small batches and rows appended a
/// few at a time encoded inline into the piece being filled; failures reported in order; the output completed, aborted
/// or closed. Memory stays bounded: every piece has a number (in the order written), and piece n starts only once fewer
/// than the window's pieces before it are still unwritten. The format gives the cells of a batch's columns and encodes
/// rows of them (<see cref="IRowEncoder{TCell}"/>).
/// </summary>
internal sealed class RowWriter<TCell>
{
    private readonly ByteSink _sink;
    private readonly IRowEncoder<TCell> _encoder;
    private readonly Func<ArrayData, Field, Pins, TCell> _cell;
    private readonly RowWriterSettings _settings;
    private readonly NativeBufferWriter?[] _spare;           // buffers of pieces written, used again (no lock: slots taken by exchange)
    private readonly Action<NativeBufferWriter> _giveBack;
    // A batch's pieces are encoded side by side by workers taking them in turn, within the window, and written in order
    // by a chain of tasks (each batch's write waits for the one before) as they are encoded: the queue holds each
    // batch's write, for the budget of batches on their way.
    private readonly Queue<(Task Written, long Bytes)> _ahead = [];
    private readonly PieceWindow _window;
    private NativeBufferWriter? _open; // rows encoded by AppendRows, not handed to the writes yet
    private long _aheadBytes, _rows, _written, _numbered;
    private Task _chain = Task.CompletedTask;
    private Exception? _failure;             // what failed a piece or a write (the chain drops the pieces after it)
    private volatile bool _stopping;         // aborted or disposed: pieces still on their way are dropped
    private double _rowBytes;
    private bool _completed, _aborted, _faulted, _disposed;

    public RowWriter(ByteSink sink, Schema schema, IRowEncoder<TCell> encoder, Func<ArrayData, Field, Pins, TCell> cell, RowWriterSettings settings)
    {
        _sink = sink;
        _encoder = encoder;
        _cell = cell;
        _settings = settings;
        Schema = schema;
        _rowBytes = settings.RowBytes;
        _giveBack = Spare;
        _window = new PieceWindow(Math.Max(1, settings.Window));
        _spare = new NativeBufferWriter?[Math.Min(4 * settings.MaxDegreeOfParallelism, Math.Max(1, settings.Window) + 1)];
    }

    /// <summary>The schema the batches are written with.</summary>
    public Schema Schema { get; }

    /// <summary>Whether batches can still be written.</summary>
    public bool IsWritable => !_completed && !_aborted && !_faulted && !_disposed;

    /// <summary>The bytes handed to the output so far.</summary>
    public long BytesWritten => Interlocked.Read(ref _written);

    /// <summary>The cells of a batch (its buffers pinned until released).</summary>
    public BatchCells<TCell> Cells(RecordBatch batch, bool owned) => new(batch, Schema, owned, _cell);

    public void Write(RecordBatch batch) => Sync(WriteCoreAsync(batch, false, default));

    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken) => WriteCoreAsync(batch, true, cancellationToken);

    public void Flush() => Sync(FlushCoreAsync(false, default));

    public ValueTask FlushAsync(CancellationToken cancellationToken) => FlushCoreAsync(true, cancellationToken);

    private async ValueTask FlushCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        await HandOffAsync(useAsync).ConfigureAwait(false);
        await DrainAsync(useAsync).ConfigureAwait(false);
        ThrowIfFailed();
        await _sink.CompleteAsync(useAsync, ct).ConfigureAwait(false);
    }

    public void Complete() => Sync(CompleteCoreAsync(false, default));

    public ValueTask CompleteAsync(CancellationToken cancellationToken) => CompleteCoreAsync(true, cancellationToken);

    public void Abort()
    {
        if (_completed || _disposed) return;
        _aborted = true;
        _stopping = true;
    }

    private static void Sync(ValueTask task)
    {
        if (task.IsCompletedSuccessfully) return;
        task.AsTask().GetAwaiter().GetResult();
    }

    private void CheckWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException($"The {_settings.Format} writer has been completed.");
        if (_aborted) throw new InvalidOperationException($"The {_settings.Format} writer was aborted.");
        if (_faulted) throw new InvalidOperationException($"The {_settings.Format} writer failed earlier; its output cannot be completed.");
    }

    private void CheckBatch(RecordBatch batch)
    {
        IReadOnlyList<Field> fields = Schema.FieldsList;
        if (batch.ColumnCount != fields.Count)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns, the schema {fields.Count}.", nameof(batch));
        for (int i = 0; i < fields.Count; i++)
            if (!ArrowTypes.SameType(batch.Column(i).Data.DataType, fields[i].DataType))
                throw new ArgumentException($"Column '{fields[i].Name}' is {batch.Column(i).Data.DataType.Name} in the batch, {fields[i].DataType.Name} in the schema.", nameof(batch));
    }

    private async ValueTask WriteCoreAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        bool owned = _settings.Ownership == BatchOwnership.Writer;
        BatchCells<TCell> cells;
        try
        {
            CheckWritable();
            CheckBatch(batch);
            if (batch.Length == 0)
            {
                if (owned) batch.Dispose();
                return;
            }
            cells = Cells(batch, owned);
        }
        catch
        {
            if (owned) batch.Dispose();
            throw;
        }
        // Pieces of about a piece's bytes, smaller when that leaves threads idle on this batch (four per thread at least).
        int threads = _settings.MaxDegreeOfParallelism, length = batch.Length;
        double rowBytes = Math.Max(1, Volatile.Read(ref _rowBytes));
        long target = Math.Min(_settings.PieceBytes, Math.Max((long)(rowBytes * length) / (4 * threads), Math.Min(_settings.PieceBytes, 16L << 10)));
        int pieceRows = (int)Math.Clamp(target / rowBytes, 1, length);
        int pieces = (length + pieceRows - 1) / pieceRows;
        long bytes = (long)(rowBytes * length);
        if (pieces == 1)
        {
            // A batch of one piece: encoded here, after the rows appended before, into the piece being filled (one
            // worker would encode it alone, the caller waiting): small batches cost no task and no write each.
            bool full;
            try
            {
                full = Append(cells, 0, length);
            }
            finally
            {
                cells.Expect(1);
                cells.Release();
            }
            if (full) await HandOffAsync(useAsync).ConfigureAwait(false);
            return;
        }
        try
        {
            await HandOffAsync(useAsync).ConfigureAwait(false); // rows appended before go first
        }
        catch
        {
            cells.Expect(1);
            cells.Release();
            throw;
        }
        cells.Expect(pieces);
        try
        {
            // Room for the batch: the writes ahead within the budget (one batch goes beyond alone).
            while (_ahead.Count > 0 && (_ahead.Count >= 4 || _aheadBytes + bytes > _settings.InFlightBytes))
            {
                var (written, done) = _ahead.Dequeue();
                _aheadBytes -= done;
                if (useAsync) await written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                else written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
                ThrowIfFailed();
            }
        }
        catch
        {
            for (int k = 0; k < pieces; k++) cells.Release();
            throw;
        }
        long first = _rows, number = _numbered;
        _rows += length;
        _numbered += pieces;
        var slots = new TaskCompletionSource<EncodedPiece?>[pieces];
        for (int k = 0; k < pieces; k++) slots[k] = new TaskCompletionSource<EncodedPiece?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _chain = WriteInOrderAsync(_chain, slots, useAsync);
        _ahead.Enqueue((_chain, bytes));
        _aheadBytes += bytes;
        int next = 0, workers = Math.Min(threads, pieces);
        // The caller's batch: the caller, waiting for its pieces anyway, encodes them with the workers.
        var tasks = new Task[owned ? workers : workers - 1];
        for (int w = 0; w < tasks.Length; w++) tasks[w] = Task.Run(() => EncodePiecesAsync(true));
        if (!owned)
        {
            if (useAsync) await EncodePiecesAsync(true).ConfigureAwait(false);
            else EncodePiecesAsync(false).GetAwaiter().GetResult();
        }
        Task encoded = Task.WhenAll(tasks);
        // The caller's batch is free once its pieces are encoded (they are written behind); a piece's failure fails this
        // call (the first in order).
        if (!owned)
        {
            if (useAsync) await encoded.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            else encoded.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
            foreach (TaskCompletionSource<EncodedPiece?> slot in slots)
                if (slot.Task.Result?.Error is { } error)
                {
                    Interlocked.CompareExchange(ref _failure, error, null);
                    break;
                }
        }
        ThrowIfFailed();

        // Pieces taken in turn, each once the window lets it start; after a failure in order (or once the writer stops)
        // the rest are dropped, not encoded. Every piece taken is settled (its slot set, the batch released once).
        async Task EncodePiecesAsync(bool asynchronous)
        {
            for (int k; (k = Interlocked.Increment(ref next) - 1) < pieces;)
            {
                Task admitted = _window.WaitAsync(number + k);
                if (!admitted.IsCompleted)
                {
                    if (asynchronous) await admitted.ConfigureAwait(false);
                    else admitted.GetAwaiter().GetResult();
                }
                if (_stopping || Volatile.Read(ref _failure) is not null)
                {
                    cells.Release();
                    slots[k].SetResult(null);
                    continue;
                }
                int from = k * pieceRows;
                slots[k].SetResult(Encode(cells, from, Math.Min(length, from + pieceRows), first + from));
            }
        }
    }

    // A failure met behind (a piece's, a write's) fails the writer at the next call.
    private void ThrowIfFailed()
    {
        if (Volatile.Read(ref _failure) is not { } failure) return;
        _faulted = true;
        ExceptionDispatchInfo.Throw(failure);
    }

    // Writes a batch's pieces in order once the batches before are written, each run of pieces encoded by then in one
    // write as soon as the first of it is (dropped after a failure, or once the writer stops). Every piece is settled:
    // freed and counted out of the window.
    private async Task WriteInOrderAsync(Task previous, TaskCompletionSource<EncodedPiece?>[] slots, bool useAsync)
    {
        await previous.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        int k = 0, n = slots.Length;
        try
        {
            // The pieces up to a failure, written as the caller writes (an output may take asynchronous writes only).
            List<ReadOnlyMemory<byte>> ready = new(Math.Min(n, 64));
            while (k < n)
            {
                await slots[k].Task.ConfigureAwait(false);
                ready.Clear();
                long bytes = 0;
                int j = k;
                bool stop = false;
                for (; j < n && slots[j].Task.IsCompleted; j++)
                {
                    EncodedPiece? piece = slots[j].Task.Result;
                    if (piece is null || _stopping || Volatile.Read(ref _failure) is not null)
                    {
                        stop = true;
                        break;
                    }
                    if (piece.Error is { } error)
                    {
                        Volatile.Write(ref _failure, error);
                        stop = true;
                        break;
                    }
                    ready.Add(piece.Bytes);
                    bytes += piece.Bytes.Length;
                }
                if (ready.Count > 0)
                {
                    if (_sink is FileSink file) file.WriteAll(ready, bytes);
                    else
                        foreach (ReadOnlyMemory<byte> piece in ready) await _sink.WriteAsync(piece, useAsync, CancellationToken.None).ConfigureAwait(false);
                    Interlocked.Add(ref _written, bytes);
                }
                for (; k < j; k++) Settle(slots[k].Task.Result);
                if (stop) break;
            }
        }
        catch (Exception e)
        {
            Volatile.Write(ref _failure, e);
        }
        finally
        {
            // The pieces not written: freed once encoded (or dropped).
            for (; k < n; k++) Settle(await slots[k].Task.ConfigureAwait(false));
        }
    }

    // A piece written or dropped: its buffer freed (or kept), the window moved on.
    private void Settle(EncodedPiece? piece)
    {
        piece?.Dispose();
        _window.Settle();
    }

    // Rows [start, end) of a batch, compressed as asked. A failure is the piece's, reported in order.
    private EncodedPiece Encode(BatchCells<TCell> cells, int start, int end, long first)
    {
        NativeBufferWriter buffer = Rent(end - start);
        try
        {
            EncodeRows(cells.Cells, buffer, start, end, first);
            return Piece(buffer);
        }
        catch (Exception e)
        {
            Spare(buffer);
            return new EncodedPiece(null, e);
        }
        finally { cells.Release(); }
    }

    // The format's rows, their bytes measured (pieces are sized by them).
    private void EncodeRows(TCell[] cells, NativeBufferWriter buffer, int start, int end, long first)
    {
        int before = buffer.WrittenCount;
        _encoder.EncodeRows(cells, buffer, start, end, first);
        Volatile.Write(ref _rowBytes, (double)(buffer.WrittenCount - before) / (end - start));
    }

    // A buffer for about `rows` rows: a spare one, else a new one.
    private NativeBufferWriter Rent(int rows)
    {
        NativeBufferWriter? buffer = null;
        for (int k = 0; k < _spare.Length && buffer is null; k++) buffer = Interlocked.Exchange(ref _spare[k], null);
        return buffer ?? new NativeBufferWriter((int)Math.Min(1 << 24, (long)(Volatile.Read(ref _rowBytes) * rows * 1.25) + 64));
    }

    // Encoded rows as a piece to write: compressed as asked (one gzip member or Zstandard frame of its own).
    private EncodedPiece Piece(NativeBufferWriter buffer) => _settings.Compression == PieceCompression.None
        ? new EncodedPiece(buffer, null, _giveBack)
        : new EncodedPiece(EncodedPiece.Compress(buffer, _settings.Compression, _settings.CompressionLevel), null);

    /// <summary>
    /// Rows [<paramref name="start"/>, +<paramref name="count"/>) of the batch <paramref name="cells"/> read, encoded on
    /// this thread into the piece being filled (the caller's batch is free once this returns): the piece is written once
    /// it holds a piece's bytes, or before anything written after it (Write, Flush, Complete). Rows a few at a time (a
    /// partition's rows of each batch of a dataset written side by side) cost no write each, nor a copy before encoding.
    /// A value the format cannot hold fails the writer.
    /// </summary>
    public void AppendRows(BatchCells<TCell> cells, int start, int count)
    {
        CheckWritable();
        if (count == 0)
        {
            ThrowIfFailed();
            return;
        }
        if (Append(cells, start, count)) Sync(HandOffAsync(false));
    }

    // Rows encoded on this thread into the piece being filled; true once it holds a piece's bytes (to hand off).
    private bool Append(BatchCells<TCell> cells, int start, int count)
    {
        ThrowIfFailed();
        NativeBufferWriter open = _open ??= Rent(count);
        try
        {
            EncodeRows(cells.Cells, open, start, start + count, _rows);
        }
        catch
        {
            _faulted = true; // the piece holds part of a row
            throw;
        }
        _rows += count;
        return open.WrittenCount >= _settings.PieceBytes;
    }

    /// <summary>
    /// Bytes before every row (a byte order mark, a header): put in the piece being filled, so the first written (with
    /// the first rows, or by <see cref="Complete"/> when there are none). Called before anything is written.
    /// </summary>
    public void Prepend(ReadOnlySpan<byte> bytes)
    {
        NativeBufferWriter open = _open ??= Rent(1);
        bytes.CopyTo(open.GetSpan(bytes.Length));
        open.Advance(bytes.Length);
    }

    /// <summary>The bytes encoded by <see cref="AppendRows"/>, not handed to the writes yet.</summary>
    public long OpenBytes => _open?.WrittenCount ?? 0;

    /// <summary>Hands the rows encoded by <see cref="AppendRows"/> to the writes (to hold less memory), without waiting for them.</summary>
    public void HandOff()
    {
        CheckWritable();
        Sync(HandOffAsync(false));
    }

    // The piece being filled, written after those before (the writes ahead within the budget first).
    private async ValueTask HandOffAsync(bool useAsync)
    {
        if (_open is not { WrittenCount: > 0 } open) return;
        _open = null;
        EncodedPiece piece = Piece(open);
        long bytes = piece.Bytes.Length;
        try
        {
            while (_ahead.Count > 0 && (_ahead.Count >= 4 || _aheadBytes + bytes > _settings.InFlightBytes))
            {
                var (written, done) = _ahead.Dequeue();
                _aheadBytes -= done;
                if (useAsync) await written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                else written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
                ThrowIfFailed();
            }
        }
        catch
        {
            piece.Dispose();
            throw;
        }
        var slot = new TaskCompletionSource<EncodedPiece?>(TaskCreationOptions.RunContinuationsAsynchronously);
        slot.SetResult(piece);
        _chain = WriteInOrderAsync(_chain, [slot], useAsync);
        _ahead.Enqueue((_chain, bytes));
        _aheadBytes += bytes;
        // The rows appended next wait while the window is full (the piece's memory is held already).
        Task admitted = _window.WaitAsync(_numbered++);
        if (!admitted.IsCompleted)
        {
            if (useAsync) await admitted.ConfigureAwait(false);
            else admitted.GetAwaiter().GetResult();
        }
    }

    // A buffer written, kept for the next pieces (as many as may be on their way at once; the others go).
    private void Spare(NativeBufferWriter buffer)
    {
        buffer.ResetWrittenCount();
        if (!_disposed)
            for (int k = 0; k < _spare.Length; k++)
                if (Interlocked.CompareExchange(ref _spare[k], buffer, null) is null) return;
        buffer.Dispose();
    }

    // Every piece's write, waited for.
    private async ValueTask DrainAsync(bool useAsync)
    {
        _ahead.Clear();
        _aheadBytes = 0;
        if (useAsync) await _chain.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        else _chain.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        await HandOffAsync(useAsync).ConfigureAwait(false);
        await DrainAsync(useAsync).ConfigureAwait(false);
        ThrowIfFailed();
        ReadOnlySpan<byte> tail = _encoder.Tail(_rows);
        if (!tail.IsEmpty)
        {
            var end = new NativeBufferWriter(Math.Max(8, tail.Length));
            tail.CopyTo(end.GetSpan(tail.Length));
            end.Advance(tail.Length);
            await WriteLastAsync(end, useAsync, ct).ConfigureAwait(false);
        }
        else if (_settings.Compression != PieceCompression.None && Interlocked.Read(ref _written) == 0)
        {
            // No rows: one empty member (or frame), for the output to be a compressed file still.
            await WriteLastAsync(new NativeBufferWriter(1), useAsync, ct).ConfigureAwait(false);
        }
        _completed = true;
        await _sink.CompleteAsync(useAsync, ct).ConfigureAwait(false);
    }

    // Bytes after every piece (compressed as asked), written at once.
    private async ValueTask WriteLastAsync(NativeBufferWriter raw, bool useAsync, CancellationToken ct)
    {
        using var last = new EncodedPiece(EncodedPiece.Compress(raw, _settings.Compression, _settings.CompressionLevel), null);
        try { await _sink.WriteAsync(last.Bytes, useAsync, ct).ConfigureAwait(false); }
        catch
        {
            _faulted = true;
            throw;
        }
        _written += last.Bytes.Length;
    }

    private void FreeSpare()
    {
        for (int k = 0; k < _spare.Length; k++) Interlocked.Exchange(ref _spare[k], null)?.Dispose();
        _open?.Dispose(); // rows appended, not written (an aborted or failed writer)
        _open = null;
    }

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
            _stopping = true;
            Sync(DrainAsync(false));
            FreeSpare();
            Sync(_sink.CloseAsync(false));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted && !_aborted) await CompleteAsync(default).ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
            _stopping = true;
            await DrainAsync(true).ConfigureAwait(false);
            FreeSpare();
            await _sink.CloseAsync(true).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// The window of a writer's pieces: piece n (numbered in the order written) may start once fewer than the window's
/// pieces before it are still unsettled (written or dropped). The first unsettled piece always may.
/// </summary>
internal sealed class PieceWindow(int size)
{
    private readonly object _lock = new();
    private readonly List<(long Number, TaskCompletionSource Admitted)> _waiting = [];
    private long _settled;

    /// <summary>Completes once piece <paramref name="number"/> may start.</summary>
    public Task WaitAsync(long number)
    {
        lock (_lock)
        {
            if (number < _settled + size) return Task.CompletedTask;
            var admitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiting.Add((number, admitted));
            return admitted.Task;
        }
    }

    /// <summary>The next piece in order is written or dropped.</summary>
    public void Settle()
    {
        List<TaskCompletionSource>? admitted = null;
        lock (_lock)
        {
            _settled++;
            for (int k = _waiting.Count - 1; k >= 0; k--)
                if (_waiting[k].Number < _settled + size)
                {
                    (admitted ??= []).Add(_waiting[k].Admitted);
                    _waiting.RemoveAt(k);
                }
        }
        if (admitted is not null)
            foreach (TaskCompletionSource waiter in admitted) waiter.SetResult();
    }
}
