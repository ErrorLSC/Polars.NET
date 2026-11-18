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
/// in footer order, to all record batches). Batches own their memory (native, or the file mapping): dispose them.
/// With <see cref="IpcReadOptions.MaxDegreeOfParallelism"/> above 1, <see cref="ReadBatches"/> reads and decodes
/// batches side by side ahead of the consumer, delivering them in order. Not thread-safe; open one reader per thread.
/// </summary>
public sealed class IpcReader : IDisposable
{
    private readonly IpcFile _file;
    private readonly IpcReadOptions _options;
    private readonly IpcDecoding _decoding;
    private readonly IpcDictionaries _dictionaries = new();
    private readonly MappedFile? _map;
    private readonly long _mapStart;
    private readonly object _lock = new();
    private readonly List<IDisposable> _windows = []; // parallel reads in progress, drained before the dictionaries go
    private bool _dictionariesRead;
    private bool _disposed;

    internal IpcReader(IpcFile file, IpcReadOptions options)
    {
        options.Validate();
        _file = file;
        _options = options;
        _decoding = new IpcDecoding(file.Footer.Schema, options);
        if (options.MemoryMap != IpcMemoryMap.Off && file.Source is ILocalSource local) _map = local.Map(out _mapStart);
    }

    /// <summary>The schema of the batches (the selected columns).</summary>
    public Schema Schema => _decoding.Schema;

    /// <summary>
    /// Whether the batches hold exactly the rows <see cref="IpcReadOptions.Filter"/> keeps: with
    /// <see cref="IpcReadOptions.FilterRows"/>, when there is no filter or every conjunct of it is evaluated here (the
    /// caller may then drop its own filter). False without <see cref="IpcReadOptions.FilterRows"/> (rows are not
    /// filtered) and when some conjunct is not evaluated (the rows delivered are then a superset).
    /// </summary>
    public bool FiltersExactly => _decoding.FiltersExactly;

    /// <summary>Reads batch <paramref name="index"/> (with <see cref="IpcReadOptions.FilterRows"/>, the rows it keeps: maybe none).</summary>
    /// <exception cref="IpcFormatException">The batch or a dictionary is damaged or invalid.</exception>
    public RecordBatch ReadBatch(int index) => IpcIO.Sync(ReadBatchCoreAsync(index, long.MaxValue, false, default));

    /// <inheritdoc cref="ReadBatch"/>
    public ValueTask<RecordBatch> ReadBatchAsync(int index, CancellationToken cancellationToken = default) =>
        ReadBatchCoreAsync(index, long.MaxValue, true, cancellationToken);

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

    private bool Parallel => _options.MaxDegreeOfParallelism > 1 && _file.BatchCount > 1;

    private IEnumerable<RecordBatch> ReadSequential()
    {
        long left = _options.MaxRows ?? long.MaxValue;
        for (int i = 0; i < _file.BatchCount && left > 0; i++)
        {
            RecordBatch batch = IpcIO.Sync(ReadBatchCoreAsync(i, left, false, default));
            if (Skip(batch)) continue;
            left -= batch.Length;
            yield return batch;
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadSequentialAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long left = _options.MaxRows ?? long.MaxValue;
        for (int i = 0; i < _file.BatchCount && left > 0; i++)
        {
            RecordBatch batch = await ReadBatchCoreAsync(i, left, true, cancellationToken).ConfigureAwait(false);
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
        long left = _options.MaxRows ?? long.MaxValue;
        if (left == 0) yield break;
        IpcIO.Sync(ReadDictionariesAsync(false, default));
        using OrderedWindow<RecordBatch> window = Window(false, default);
        try
        {
            for (int i = 0; i < _file.BatchCount && left > 0; i++)
            {
                RecordBatch batch = window.Take(i).GetAwaiter().GetResult();
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
        long left = _options.MaxRows ?? long.MaxValue;
        if (left == 0) yield break;
        await ReadDictionariesAsync(true, cancellationToken).ConfigureAwait(false);
        using OrderedWindow<RecordBatch> window = Window(true, cancellationToken);
        try
        {
            for (int i = 0; i < _file.BatchCount && left > 0; i++)
            {
                RecordBatch batch = await window.Take(i).ConfigureAwait(false);
                if (Skip(batch)) continue;
                yield return Limit(batch, ref left);
            }
        }
        finally
        {
            Leave(window);
        }
    }

    private OrderedWindow<RecordBatch> Window(bool useAsync, CancellationToken ct)
    {
        IpcBlock[] blocks = _file.Footer.RecordBatches;
        var window = new OrderedWindow<RecordBatch>(blocks.Length, (i, token) => ReadBatchCoreAsync(i, long.MaxValue, useAsync, token).AsTask(),
            _options.MaxDegreeOfParallelism, i => blocks[i].BodyLength, _options.InFlightBytes, ct);
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

    private async ValueTask<RecordBatch> ReadBatchCoreAsync(int index, long rows, bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _file.BatchCount);
        if (!_dictionariesRead) await ReadDictionariesAsync(useAsync, ct).ConfigureAwait(false);
        IpcBlock block = _file.Footer.RecordBatches[index];
        IpcMessage message = await IpcIO.MessageAsync(_file.Source, block, useAsync, ct).ConfigureAwait(false);
        if (message.Kind != IpcMessageKind.RecordBatch)
            throw new IpcFormatException($"Arrow IPC footer block {index} of the record batches holds a {message.Kind} message.");
        bool[] needed = _decoding.Plan(message);
        IpcBody body = await IpcIO.BodyAsync(_file.Source, MapFor(message), _mapStart, block.Offset + block.MetadataLength, message, needed, useAsync, ct)
            .ConfigureAwait(false);
        return _decoding.Decode(message, body, _dictionaries, rows);
    }

    /// <summary>The mapping to read a message's body from: always with <see cref="IpcMemoryMap.On"/>; for uncompressed bodies with Auto.</summary>
    private MappedFile? MapFor(IpcMessage message) => _map is not null
        && (_options.MemoryMap == IpcMemoryMap.On || (message.RecordBatch ?? message.DictionaryBatch!.Data).Compression == IpcCompression.None)
        ? _map
        : null;

    private async ValueTask ReadDictionariesAsync(bool useAsync, CancellationToken ct)
    {
        if (_dictionariesRead) return;
        IpcFooter footer = _file.Footer;
        HashSet<long> wanted = _decoding.DictionaryIds;
        foreach (IpcBlock block in footer.Dictionaries)
        {
            IpcMessage message = await IpcIO.MessageAsync(_file.Source, block, useAsync, ct).ConfigureAwait(false);
            if (message.Kind != IpcMessageKind.DictionaryBatch)
                throw new IpcFormatException($"An Arrow IPC footer block of the dictionaries holds a {message.Kind} message.");
            if (!wanted.Contains(message.DictionaryBatch!.Id)) continue;
            IpcField field = footer.Schema.Dictionaries.TryGetValue(message.DictionaryBatch.Id, out IpcField? f) ? f
                : throw new IpcFormatException($"Arrow IPC dictionary {message.DictionaryBatch.Id} belongs to no field.");
            bool[] needed = IpcIO.Plan([field.ValueField()], null, message, message.Version);
            IpcBody body = await IpcIO.BodyAsync(_file.Source, MapFor(message), _mapStart, block.Offset + block.MetadataLength, message, needed, useAsync, ct)
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
