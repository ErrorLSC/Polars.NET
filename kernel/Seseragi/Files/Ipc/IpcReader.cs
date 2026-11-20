using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Reads the record batches of an <see cref="IpcFile"/>: the selected columns, every array checked before it is
/// returned. Dictionaries are read once, on the first batch (the file format applies every dictionary batch, deltas
/// in footer order, to all record batches); of a stream, in order as far as each batch needs (a batch sees the
/// dictionary batches sent before it: replaced or extended later for the batches after). Batches own their memory (native, or the file mapping): dispose them.
/// With <see cref="IpcReadOptions.MaxDegreeOfParallelism"/> above 1, <see cref="ReadBatches"/> reads and decodes
/// batches side by side ahead of the consumer, delivering them in order. Not thread-safe; open one reader per thread.
/// </summary>
public sealed class IpcReader : IDisposable
{
    private readonly IpcFile _file;
    private readonly IpcReadOptions _options;
    private readonly IpcDecoding _decoding;
    private IpcDictionaries _dictionaries = new();
    private readonly MappedFile? _map;
    private readonly long _mapStart;
    private readonly object _lock = new();
    private readonly List<IDisposable> _windows = []; // parallel reads in progress, drained before the dictionaries go
    private bool _dictionariesRead;
    private bool _disposed;

    // A stream's dictionaries: applied in order to _dictionaries as far as the batches read need, a snapshot of the
    // dictionaries used kept after each change (a batch decodes with the latest one at or before its place).
    private readonly SemaphoreSlim? _applying;
    private readonly List<(int Applied, IpcDictionaries Dictionaries)> _states = [];
    private readonly IpcDictionaries _none = new();
    private int _applied;
    private bool _trimmed; // snapshots no batch after the one delivered needs were let go

    internal IpcReader(IpcFile file, IpcReadOptions options)
    {
        options.Validate();
        _file = file;
        _options = options;
        _decoding = new IpcDecoding(file.Footer.Schema, options);
        if (file.Stream is not null) _applying = new SemaphoreSlim(1, 1);
        // Every body is read from the mapping (On and Auto alike today): compressed ones are decompressed out of it.
        if (options.MemoryMap != IpcMemoryMap.Off && file.Source is ILocalSource local) _map = local.Map(out _mapStart);
    }

    /// <summary>The schema of the batches: the row index when asked, then the selected columns.</summary>
    public Schema Schema => _decoding.OutputSchema;

    /// <summary>
    /// Whether the batches hold exactly the rows <see cref="IpcReadOptions.Filter"/> keeps: with
    /// <see cref="IpcReadOptions.FilterRows"/>, when there is no filter or every conjunct of it is evaluated here (the
    /// caller may then drop its own filter). False without <see cref="IpcReadOptions.FilterRows"/> (rows are not
    /// filtered) and when some conjunct is not evaluated (the rows delivered are then a superset).
    /// </summary>
    public bool FiltersExactly => _decoding.FiltersExactly;

    /// <summary>
    /// Reads batch <paramref name="index"/> (with <see cref="IpcReadOptions.FilterRows"/>, the rows it keeps: maybe
    /// none). With <see cref="IpcReadOptions.RowIndexColumn"/>, the messages of the batches before it are read for their
    /// row counts (once per reader).
    /// </summary>
    /// <exception cref="IpcFormatException">The batch or a dictionary is damaged or invalid.</exception>
    public RecordBatch ReadBatch(int index) => IpcIO.Sync(ReadOneAsync(index, false, default));

    /// <inheritdoc cref="ReadBatch"/>
    public ValueTask<RecordBatch> ReadBatchAsync(int index, CancellationToken cancellationToken = default) => ReadOneAsync(index, true, cancellationToken);

    private async ValueTask<RecordBatch> ReadOneAsync(int index, bool useAsync, CancellationToken ct)
    {
        IpcDecoded decoded = await ReadBatchCoreAsync(index, long.MaxValue, useAsync, ct).ConfigureAwait(false);
        long next = 0;
        try
        {
            if (Schema != _decoding.Schema) next = await RowsBeforeAsync(index, useAsync, ct).ConfigureAwait(false);
        }
        catch
        {
            decoded.Dispose();
            throw;
        }
        return _decoding.Deliver(decoded, ref next);
    }

    private long[]? _rowsBefore; // per batch, the rows of the batches before it (read for the row index of one batch)

    /// <summary>The rows of the batches before batch <paramref name="index"/> (from their messages, read once).</summary>
    private async ValueTask<long> RowsBeforeAsync(int index, bool useAsync, CancellationToken ct)
    {
        if (_file.Stream is { } stream) return stream.BatchAt(index).RowsBefore; // found by the scan (as far as the batch read)
        if (_rowsBefore is null)
        {
            IpcBlock[] blocks = _file.Footer.RecordBatches;
            var before = new long[blocks.Length];
            long rows = 0;
            for (int i = 0; i < blocks.Length; i++)
            {
                before[i] = rows;
                if (i == blocks.Length - 1) break;
                IpcMessage message = await IpcIO.MessageAsync(_file.Source, _map, _mapStart, blocks[i], useAsync, ct).ConfigureAwait(false);
                rows += message.RecordBatch?.Length
                    ?? throw new IpcFormatException($"Arrow IPC footer block {i} of the record batches holds a {message.Kind} message.");
            }
            _rowsBefore = before;
        }
        return _rowsBefore[index];
    }

    /// <summary>
    /// Reads the batches in order, up to <see cref="IpcReadOptions.MaxRows"/> rows (with
    /// <see cref="IpcReadOptions.FilterRows"/>, batches where the filter keeps no row are passed over).
    /// </summary>
    /// <exception cref="IpcFormatException">A batch or a dictionary is damaged or invalid.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Parallel ? ReadParallel() : ReadSequential();
    }

    /// <inheritdoc cref="ReadBatches"/>
    public IAsyncEnumerable<RecordBatch> ReadBatchesAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Parallel ? ReadParallelAsync(cancellationToken) : ReadSequentialAsync(cancellationToken);
    }

    private bool Parallel => _options.MaxDegreeOfParallelism > 1 && (_file.Stream is not null || _file.BatchCount > 1);

    private IEnumerable<RecordBatch> ReadSequential()
    {
        long left = _options.MaxRows ?? long.MaxValue, next = 0;
        for (int i = 0; left > 0 && IpcIO.Sync(_file.HasBatchAsync(i, false, default)); i++)
        {
            RecordBatch batch = _decoding.Deliver(IpcIO.Sync(ReadBatchCoreAsync(i, left, false, default)), ref next);
            TrimDictionaries(i);
            if (Skip(batch)) continue;
            left -= batch.Length;
            yield return batch;
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadSequentialAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long left = _options.MaxRows ?? long.MaxValue, next = 0;
        for (int i = 0; left > 0 && await _file.HasBatchAsync(i, true, cancellationToken).ConfigureAwait(false); i++)
        {
            RecordBatch batch = _decoding.Deliver(await ReadBatchCoreAsync(i, left, true, cancellationToken).ConfigureAwait(false), ref next);
            TrimDictionaries(i);
            if (Skip(batch)) continue;
            left -= batch.Length;
            yield return batch;
        }
    }

    /// <summary>
    /// Batches read and decoded on the thread pool, up to <see cref="IpcReadOptions.MaxDegreeOfParallelism"/> of them
    /// ahead of the consumer (fewer while their bodies pass <see cref="IpcReadOptions.InFlightBytes"/>), delivered in
    /// order; each decoded whole, the last cut to <see cref="IpcReadOptions.MaxRows"/>. A consumer that stops early
    /// cancels the rest and frees what they decoded.
    /// </summary>
    private IEnumerable<RecordBatch> ReadParallel()
    {
        long left = _options.MaxRows ?? long.MaxValue, next = 0;
        if (left == 0) yield break;
        IpcIO.Sync(ReadDictionariesAsync(false, default));
        using OrderedWindow<IpcDecoded> window = Window(false, default);
        try
        {
            for (int i = 0; left > 0 && IpcIO.Sync(Ahead(window, i, false, default)); i++)
            {
                RecordBatch batch = _decoding.Deliver(window.Take(i).GetAwaiter().GetResult(), ref next);
                TrimDictionaries(i);
                if (Skip(batch)) continue;
                yield return Limit(batch, ref left);
            }
        }
        finally
        {
            Leave(window);
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadParallelAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long left = _options.MaxRows ?? long.MaxValue, next = 0;
        if (left == 0) yield break;
        await ReadDictionariesAsync(true, cancellationToken).ConfigureAwait(false);
        using OrderedWindow<IpcDecoded> window = Window(true, cancellationToken);
        try
        {
            for (int i = 0; left > 0 && await Ahead(window, i, true, cancellationToken).ConfigureAwait(false); i++)
            {
                RecordBatch batch = _decoding.Deliver(await window.Take(i).ConfigureAwait(false), ref next);
                TrimDictionaries(i);
                if (Skip(batch)) continue;
                yield return Limit(batch, ref left);
            }
        }
        finally
        {
            Leave(window);
        }
    }

    private OrderedWindow<IpcDecoded> Window(bool useAsync, CancellationToken ct)
    {
        int count = _file.Stream is { } stream ? stream.Known : _file.BatchCount;
        var window = new OrderedWindow<IpcDecoded>(count, (i, token) => ReadBatchCoreAsync(i, long.MaxValue, useAsync, token).AsTask(),
            _options.MaxDegreeOfParallelism, BodyBytes, _options.InFlightBytes, ct);
        lock (_lock)
        {
            if (_disposed)
            {
                window.Dispose();
                throw new ObjectDisposedException(nameof(IpcReader));
            }
            _windows.Add(window);
        }
        return window;
    }

    /// <summary>
    /// Whether batch <paramref name="index"/> exists; of a stream, scanned first as far as the window may read ahead of
    /// it (the batches found join the window).
    /// </summary>
    private async ValueTask<bool> Ahead(OrderedWindow<IpcDecoded> window, int index, bool useAsync, CancellationToken ct)
    {
        if (_file.Stream is not { } stream) return index < _file.BatchCount;
        try
        {
            await stream.ScanAheadAsync(index + _options.MaxDegreeOfParallelism, useAsync, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (index < stream.Known && e is not OperationCanceledException)
        {
            // A read failing beyond the batch asked for: delivered first (the read is tried again when it is needed).
        }
        window.Grow(stream.Known);
        return await stream.HasBatchAsync(index, useAsync, ct).ConfigureAwait(false);
    }

    private void Leave(IDisposable window)
    {
        lock (_lock) _windows.Remove(window);
    }

    /// <summary>Whether a batch is passed over: the filter kept none of its rows.</summary>
    private bool Skip(RecordBatch batch)
    {
        if (batch.Length > 0 || !_decoding.Filters) return false;
        batch.Dispose();
        return true;
    }

    /// <summary>A batch cut to the rows left to deliver (counted down).</summary>
    private static RecordBatch Limit(RecordBatch batch, ref long left)
    {
        if (batch.Length <= left)
        {
            left -= batch.Length;
            return batch;
        }
        int rows = (int)left;
        left = 0;
        return new RecordBatch(batch.Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, rows))], rows);
    }

    /// <summary>
    /// The batches as an Arrow stream that owns them, for handing to another library: each batch is disposed when the
    /// next one is read or the stream is disposed. A consumer that keeps a batch beyond the next read must use
    /// <see cref="ReadBatches"/> instead.
    /// </summary>
    public Apache.Arrow.Ipc.IArrowArrayStream ReadArrowStream() => new OwningBatchStream(Schema, ReadBatches());

    private async ValueTask<IpcDecoded> ReadBatchCoreAsync(int index, long rows, bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        if (!await _file.HasBatchAsync(index, useAsync, ct).ConfigureAwait(false))
            throw new ArgumentOutOfRangeException(nameof(index), index, "There is no such batch.");
        if (!_dictionariesRead) await ReadDictionariesAsync(useAsync, ct).ConfigureAwait(false);
        var (message, body) = await FetchAsync(index, useAsync, ct).ConfigureAwait(false);
        return Decode(index, message, body, rows);
    }

    // ---- for datasets: a batch's reading split into its requests and its decoding ------------------------------------

    /// <summary>The schema of the batches decoded, before the row index is put first (<see cref="Deliver"/>).</summary>
    internal Schema DecodedSchema => _decoding.Schema;

    /// <summary>Whether rows are filtered (batches may then come empty).</summary>
    internal bool Filters => _decoding.Filters;

    /// <summary>The body bytes of batch <paramref name="index"/> (what reading it brings into memory, at most).</summary>
    internal long BodyBytes(int index) => _file.BatchBlock(index).BodyLength;

    /// <summary>Reads the dictionaries the columns read use: before batches are decoded side by side (<see cref="FetchAsync"/>, <see cref="Decode"/>).</summary>
    internal ValueTask PrepareAsync(bool useAsync, CancellationToken ct) => ReadDictionariesAsync(useAsync, ct);

    /// <summary>
    /// Reads batch <paramref name="index"/>'s message and the body buffers the columns read need (the requests); of a
    /// stream, the dictionary batches before it are applied first.
    /// </summary>
    internal async ValueTask<(IpcMessage Message, IpcBody Body)> FetchAsync(int index, bool useAsync, CancellationToken ct)
    {
        if (_file.Stream is { } stream) await ApplyDictionariesAsync(stream.BatchAt(index).Epoch, useAsync, ct).ConfigureAwait(false);
        IpcBlock block = _file.BatchBlock(index);
        IpcMessage message = await IpcIO.MessageAsync(_file.Source, _map, _mapStart, block, useAsync, ct).ConfigureAwait(false);
        if (message.Kind != IpcMessageKind.RecordBatch)
            throw new IpcFormatException($"Arrow IPC footer block {index} of the record batches holds a {message.Kind} message.");
        bool[] needed = _decoding.Plan(message);
        IpcBody body = await IpcIO.BodyAsync(_file.Source, _map, _mapStart, block.Offset + block.MetadataLength, message, needed, useAsync, ct)
            .ConfigureAwait(false);
        return (message, body);
    }

    /// <summary>
    /// Decodes batch <paramref name="index"/> fetched (its first <paramref name="rows"/> rows, or those the filter keeps);
    /// the dictionaries are read (<see cref="FetchAsync"/>).
    /// </summary>
    internal IpcDecoded Decode(int index, IpcMessage message, IpcBody body, long rows) => _decoding.Decode(message, body, DictionariesOf(index), rows);

    /// <summary>Whether the snapshot a batch of <paramref name="epoch"/> needs was let go (<see cref="TrimDictionaries"/>).</summary>
    private bool Rewound(int epoch)
    {
        lock (_lock) return _trimmed && (_states.Count == 0 || _states[0].Applied > epoch);
    }

    /// <summary>The dictionaries batch <paramref name="index"/> decodes with: the file's, or the stream's as sent before it.</summary>
    private IpcDictionaries DictionariesOf(int index)
    {
        if (_file.Stream is not { } stream) return _dictionaries;
        int epoch = stream.BatchAt(index).Epoch;
        lock (_lock)
        {
            for (int i = _states.Count - 1; i >= 0; i--)
            {
                if (_states[i].Applied <= epoch) return _states[i].Dictionaries;
            }
        }
        return _none;
    }

    /// <summary>
    /// A stream's batches delivered in order up to <paramref name="index"/>: the snapshots no later batch needs are let
    /// go (a stream may send a new dictionary before every batch). Batches decoding ahead come after it.
    /// </summary>
    internal void TrimDictionaries(int index)
    {
        if (_file.Stream is not { } stream) return;
        int epoch = stream.BatchAt(index).Epoch;
        lock (_lock)
        {
            int keep = _states.FindLastIndex(s => s.Applied <= epoch); // the one batch index uses: kept, and those after
            for (int i = 0; i < keep; i++) _states[i].Dictionaries.Dispose();
            if (keep > 0)
            {
                _states.RemoveRange(0, keep);
                _trimmed = true;
            }
        }
    }

    /// <summary>
    /// Applies a stream's dictionary batches, in order, up to the first <paramref name="epoch"/> (those no column read
    /// uses passed over), keeping a snapshot after each that changed one used. A batch before those whose snapshots
    /// were let go (read at random after the batches were read in order) starts the stream's dictionaries again.
    /// </summary>
    private async ValueTask ApplyDictionariesAsync(int epoch, bool useAsync, CancellationToken ct)
    {
        if (Volatile.Read(ref _applied) >= epoch && !Rewound(epoch)) return;
        if (useAsync) await _applying!.WaitAsync(ct).ConfigureAwait(false);
        else _applying!.Wait(ct);
        try
        {
            if (Rewound(epoch))
            {
                lock (_lock)
                {
                    foreach (var (_, state) in _states) state.Dispose();
                    _states.Clear();
                    _trimmed = false;
                }
                _dictionaries.Dispose();
                _dictionaries = new IpcDictionaries();
                Volatile.Write(ref _applied, 0);
            }
            IpcFooter footer = _file.Footer;
            HashSet<long> wanted = _decoding.DictionaryIds;
            while (_applied < epoch)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                IpcBlock block = _file.DictionaryBlock(_applied);
                IpcMessage message = await IpcIO.MessageAsync(_file.Source, _map, _mapStart, block, useAsync, ct).ConfigureAwait(false);
                if (wanted.Contains(message.DictionaryBatch!.Id))
                {
                    IpcField field = footer.Schema.Dictionaries.TryGetValue(message.DictionaryBatch.Id, out IpcField? f) ? f
                        : throw new IpcFormatException($"Arrow IPC dictionary {message.DictionaryBatch.Id} belongs to no field.");
                    bool[] needed = IpcIO.Plan([field.ValueField()], null, message, message.Version);
                    IpcBody body = await IpcIO.BodyAsync(_file.Source, _map, _mapStart, block.Offset + block.MetadataLength, message, needed, useAsync, ct)
                        .ConfigureAwait(false);
                    IpcIO.Dictionary(footer.Schema, message, body, _dictionaries, message.Version, _options.ValidateUtf8, replacements: true);
                    IpcDictionaries snapshot = _dictionaries.Snapshot();
                    lock (_lock) _states.Add((_applied + 1, snapshot));
                }
                Volatile.Write(ref _applied, _applied + 1);
            }
        }
        finally
        {
            _applying!.Release();
        }
    }

    /// <summary>A decoded batch as delivered: the row index put first when asked (see <see cref="IpcDecoding.Deliver"/>).</summary>
    internal RecordBatch Deliver(IpcDecoded decoded, ref long next) => _decoding.Deliver(decoded, ref next);


    private async ValueTask ReadDictionariesAsync(bool useAsync, CancellationToken ct)
    {
        if (_dictionariesRead || _file.Stream is not null) return; // a stream's are applied batch by batch (FetchAsync)
        IpcFooter footer = _file.Footer;
        HashSet<long> wanted = _decoding.DictionaryIds;
        foreach (IpcBlock block in footer.Dictionaries)
        {
            IpcMessage message = await IpcIO.MessageAsync(_file.Source, _map, _mapStart, block, useAsync, ct).ConfigureAwait(false);
            if (message.Kind != IpcMessageKind.DictionaryBatch)
                throw new IpcFormatException($"An Arrow IPC footer block of the dictionaries holds a {message.Kind} message.");
            if (!wanted.Contains(message.DictionaryBatch!.Id)) continue;
            IpcField field = footer.Schema.Dictionaries.TryGetValue(message.DictionaryBatch.Id, out IpcField? f) ? f
                : throw new IpcFormatException($"Arrow IPC dictionary {message.DictionaryBatch.Id} belongs to no field.");
            bool[] needed = IpcIO.Plan([field.ValueField()], null, message, message.Version);
            IpcBody body = await IpcIO.BodyAsync(_file.Source, _map, _mapStart, block.Offset + block.MetadataLength, message, needed, useAsync, ct)
                .ConfigureAwait(false);
            IpcIO.Dictionary(footer.Schema, message, body, _dictionaries, message.Version, _options.ValidateUtf8, replacements: false);
        }
        _dictionariesRead = true;
    }

    /// <summary>Releases the dictionaries, once the batches being decoded ahead are done (batches already read keep what they use).</summary>
    public void Dispose()
    {
        IDisposable[] windows;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            windows = [.. _windows];
        }
        foreach (IDisposable window in windows) window.Dispose();
        _dictionaries.Dispose();
        lock (_lock)
        {
            foreach (var (_, state) in _states) state.Dispose();
            _states.Clear();
        }
        _none.Dispose();
    }

    internal sealed class OwningBatchStream(Schema schema, IEnumerable<RecordBatch> batches) : Apache.Arrow.Ipc.IArrowArrayStream
    {
        private readonly IEnumerator<RecordBatch> _batches = batches.GetEnumerator();
        private RecordBatch? _current;

        public Schema Schema => schema;

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            _current?.Dispose();
            _current = _batches.MoveNext() ? _batches.Current : null;
            return ValueTask.FromResult(_current);
        }

        public void Dispose()
        {
            _current?.Dispose();
            _current = null;
            _batches.Dispose();
        }
    }
}
