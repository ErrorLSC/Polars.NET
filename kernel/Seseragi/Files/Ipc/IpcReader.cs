using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Reads the record batches of an <see cref="IpcFile"/>: the selected columns, every array checked before it is
/// returned. Dictionaries are read once, on the first batch (the file format applies every dictionary batch, deltas
/// in footer order, to all record batches). Batches own their memory (native, or the file mapping): dispose them.
/// Not thread-safe; open one reader per thread.
/// </summary>
public sealed class IpcReader : IDisposable
{
    private readonly IpcFile _file;
    private readonly IpcReadOptions _options;
    private readonly bool[]? _selected;
    private readonly IpcDictionaries _dictionaries = new();
    private readonly MappedFile? _map;
    private readonly long _mapStart;
    private bool _dictionariesRead;
    private bool _disposed;

    internal IpcReader(IpcFile file, IpcReadOptions options)
    {
        options.Validate();
        _file = file;
        _options = options;
        _selected = IpcFraming.Select(file.Footer.Schema, options.Columns);
        Schema = IpcFraming.Project(file.Footer.Schema, _selected);
        if (options.MemoryMap != IpcMemoryMap.Off && file.Source is ILocalSource local) _map = local.Map(out _mapStart);
    }

    /// <summary>The schema of the batches (the selected columns).</summary>
    public Schema Schema { get; }

    /// <summary>Reads batch <paramref name="index"/>.</summary>
    /// <exception cref="IpcFormatException">The batch or a dictionary is damaged or invalid.</exception>
    public RecordBatch ReadBatch(int index) => IpcIO.Sync(ReadBatchCoreAsync(index, int.MaxValue, false, default));

    /// <inheritdoc cref="ReadBatch"/>
    public ValueTask<RecordBatch> ReadBatchAsync(int index, CancellationToken cancellationToken = default) =>
        ReadBatchCoreAsync(index, int.MaxValue, true, cancellationToken);

    /// <summary>Reads the batches in order, up to <see cref="IpcReadOptions.MaxRows"/> rows.</summary>
    /// <exception cref="IpcFormatException">A batch or a dictionary is damaged or invalid.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        long left = _options.MaxRows ?? long.MaxValue;
        for (int i = 0; i < _file.BatchCount && left > 0; i++)
        {
            RecordBatch batch = IpcIO.Sync(ReadBatchCoreAsync(i, left, false, default));
            left -= batch.Length;
            yield return batch;
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long left = _options.MaxRows ?? long.MaxValue;
        for (int i = 0; i < _file.BatchCount && left > 0; i++)
        {
            RecordBatch batch = await ReadBatchCoreAsync(i, left, true, cancellationToken).ConfigureAwait(false);
            left -= batch.Length;
            yield return batch;
        }
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
        IpcFooter footer = _file.Footer;
        IpcBlock block = footer.RecordBatches[index];
        IpcMessage message = await IpcIO.MessageAsync(_file.Source, block, useAsync, ct).ConfigureAwait(false);
        if (message.Kind != IpcMessageKind.RecordBatch)
            throw new IpcFormatException($"Arrow IPC footer block {index} of the record batches holds a {message.Kind} message.");
        IpcMetadataVersion version = message.Version;
        bool[] needed = IpcIO.Plan(footer.Schema.Fields, _selected, message, version);
        IpcBody body = await IpcIO.BodyAsync(_file.Source, MapFor(message), _mapStart, block.Offset + block.MetadataLength, message, needed, useAsync, ct)
            .ConfigureAwait(false);
        return IpcIO.Batch(footer.Schema, Schema, _selected, message, body, _dictionaries, version, rows, _options.ValidateUtf8);
    }

    /// <summary>The mapping to read a message's body from: always with <see cref="IpcMemoryMap.On"/>; for uncompressed bodies with Auto.</summary>
    private MappedFile? MapFor(IpcMessage message) => _map is not null
        && (_options.MemoryMap == IpcMemoryMap.On || (message.RecordBatch ?? message.DictionaryBatch!.Data).Compression == IpcCompression.None)
        ? _map
        : null;

    private async ValueTask ReadDictionariesAsync(bool useAsync, CancellationToken ct)
    {
        IpcFooter footer = _file.Footer;
        HashSet<long> wanted = IpcFraming.DictionaryIds(footer.Schema.Fields, _selected);
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

    /// <summary>Releases the dictionaries (batches already read keep what they use).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
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
