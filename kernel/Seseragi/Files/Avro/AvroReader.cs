using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Array = System.Array;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>Options for <see cref="AvroReader"/>.</summary>
public sealed record AvroReadOptions
{
    /// <summary>Default number of rows per record batch.</summary>
    public const int DefaultBatchSize = 65_536;

    /// <summary>Rows per record batch (the last batch may be shorter). Batches do not follow Avro block bounds.</summary>
    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>Top-level columns to read, by name. Other columns are skipped without being decoded.
    /// Columns come back in file order. Cannot be combined with <see cref="ColumnIndices"/>.</summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>Top-level columns to read, by zero-based position. Columns come back in file order.</summary>
    public IReadOnlyList<int>? ColumnIndices { get; init; }

    /// <summary>Stop after this many rows.</summary>
    public long? RowLimit { get; init; }

    /// <summary>Leave the stream open when the reader is disposed.</summary>
    public bool LeaveOpen { get; init; }

    /// <summary>
    /// How many blocks may be decompressed and decoded at once. 1 (the default) decodes on the reading thread and
    /// every batch but the last has exactly <see cref="BatchSize"/> rows. Above 1 (or -1 for one per processor),
    /// runs of whole blocks are decoded on the thread pool and returned in file order; a batch then holds at most
    /// <see cref="BatchSize"/> rows, since batches end where such a run of blocks ends. A file with a single block
    /// gains nothing.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = 1;
}

/// <summary>
/// Streams an Avro object container file as Arrow record batches. Blocks are read one at a time into native
/// buffers, decompressed, and decoded straight into native-memory Arrow buffers that the returned batches own
/// (dispose each batch to free its memory promptly). Projected-away columns are skipped without decoding.
/// </summary>
public sealed class AvroReader : IAsyncDisposable, IDisposable
{
    private static ReadOnlySpan<byte> Magic => "Obj\u0001"u8;
    private const int SyncSize = 16;

    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private readonly AvroReadOptions _options;
    private readonly byte[] _sync = new byte[SyncSize];

    // Buffered input in native memory. A block (raw) always sits contiguously in _io while it is being decoded.
    private readonly NativeByteBuffer _io = new(64 * 1024);
    private int _ioStart;
    private int _ioEnd;
    private bool _eof;

    private AvroCodec _codec = NullCodec.Instance;
    private readonly NativeByteBuffer _decompressed = new();

    // Current block: data in _blockData[_blockPos.._blockEnd], decode position, records left.
    private NativeByteBuffer? _blockData;
    private int _blockPos;
    private int _blockEnd;
    private long _blockRecords;
    private bool _finished;
    private long _rowsEmitted;

    private NativeStructBuilder? _root;
    private RowDecoder? _rowDecoder;
    private bool _started;

    // Projection, kept so that parallel workers can build their own decoders.
    private IReadOnlyList<(string Name, AvroSchema Schema)> _fields = [];
    private bool[] _selected = [];

    // Parallel decoding (MaxDegreeOfParallelism > 1).
    private readonly int _parallelism;
    private readonly ConcurrentBag<Worker> _workers = [];
    private readonly Queue<Task<List<RecordBatch>>> _inflight = new();
    private readonly Queue<RecordBatch> _ready = new();
    private RawBlock? _pendingBlock;
    private long _rowsScheduled;

    private AvroReader(Stream stream, AvroReadOptions options)
    {
        _stream = stream;
        _options = options;
        _leaveOpen = options.LeaveOpen;
        if (options.BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(options), "BatchSize must be positive.");
        if (options.RowLimit < 0) throw new ArgumentOutOfRangeException(nameof(options), "RowLimit must not be negative.");
        if (options.Columns is not null && options.ColumnIndices is not null)
            throw new ArgumentException("Specify Columns or ColumnIndices, not both.", nameof(options));
        _parallelism = options.MaxDegreeOfParallelism switch
        {
            -1 => Environment.ProcessorCount,
            >= 1 and var n => n,
            _ => throw new ArgumentOutOfRangeException(nameof(options), "MaxDegreeOfParallelism must be positive or -1."),
        };
    }

    /// <summary>The Arrow schema of the batches (after projection).</summary>
    public Schema Schema { get; private set; } = null!;

    /// <summary>The writer schema stored in the file, as JSON.</summary>
    public string WriterSchemaJson { get; private set; } = "";

    /// <summary>The block codec name (<c>null</c>, <c>deflate</c>, <c>snappy</c> or <c>zstandard</c>).</summary>
    public string Codec => _codec.Name;

    /// <summary>The file metadata, including <c>avro.schema</c> and <c>avro.codec</c>.</summary>
    public IReadOnlyDictionary<string, byte[]> Metadata { get; private set; } = new Dictionary<string, byte[]>();

    internal AvroSchema WriterSchema { get; private set; } = null!;

    /// <summary>Opens an Avro file and reads its header.</summary>
    public static Task<AvroReader> OpenAsync(string path, AvroReadOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenAsync(OpenFile(path), (options ?? new()) with { LeaveOpen = false }, cancellationToken);

    /// <summary>Opens an Avro stream and reads its header.</summary>
    public static async Task<AvroReader> OpenAsync(Stream stream, AvroReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var reader = new AvroReader(stream, options ?? new());
        try
        {
            await reader.ReadHeaderAsync(true, cancellationToken).ConfigureAwait(false);
            return reader;
        }
        catch
        {
            await reader.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Opens an Avro file and reads its header synchronously.</summary>
    public static AvroReader Open(string path, AvroReadOptions? options = null) =>
        Open(OpenFile(path), (options ?? new()) with { LeaveOpen = false });

    /// <summary>Opens an Avro stream and reads its header synchronously.</summary>
    public static AvroReader Open(Stream stream, AvroReadOptions? options = null)
    {
        var reader = new AvroReader(stream, options ?? new());
        try
        {
            reader.ReadHeaderAsync(false, default).AsTask().GetAwaiter().GetResult();
            return reader;
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    private static FileStream OpenFile(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1, FileOptions.Asynchronous | FileOptions.SequentialScan);

    /// <summary>Reads the remaining rows as record batches. The reader can be enumerated once.</summary>
    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        BeginRead();
        while (true)
        {
            var batch = await NextBatchAsync(true, cancellationToken).ConfigureAwait(false);
            if (batch is null) yield break;
            yield return batch;
        }
    }

    /// <summary>Reads the remaining rows as record batches synchronously. The reader can be enumerated once.</summary>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        BeginRead();
        return Iterate();

        IEnumerable<RecordBatch> Iterate()
        {
            while (NextBatchAsync(false, default).AsTask().GetAwaiter().GetResult() is { } batch)
                yield return batch;
        }
    }

    private void BeginRead()
    {
        if (_started) throw new InvalidOperationException("An AvroReader can be enumerated only once.");
        _started = true;
    }

    // ---------------------------------------------------------------- header

    private async ValueTask ReadHeaderAsync(bool useAsync, CancellationToken ct)
    {
        if (!await EnsureAsync(Magic.Length, useAsync, ct).ConfigureAwait(false) || !_io.Span.Slice(_ioStart, Magic.Length).SequenceEqual(Magic))
            throw new AvroFormatException("Not an Avro object container file (bad magic bytes).");
        _ioStart += Magic.Length;

        var metadata = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        while (true)
        {
            long count = await ReadLongAsync(useAsync, ct).ConfigureAwait(false);
            if (count == 0) break;
            if (count < 0)
            {
                count = -count;
                await ReadLongAsync(useAsync, ct).ConfigureAwait(false); // block byte size
            }
            for (long i = 0; i < count; i++)
            {
                string key = Encoding.UTF8.GetString(await ReadBytesAsync(useAsync, ct).ConfigureAwait(false));
                metadata[key] = await ReadBytesAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        if (!await EnsureAsync(SyncSize, useAsync, ct).ConfigureAwait(false)) throw AvroSpanReader.Truncated();
        _io.Span.Slice(_ioStart, SyncSize).CopyTo(_sync);
        _ioStart += SyncSize;

        Metadata = metadata;
        if (!metadata.TryGetValue("avro.schema", out var schemaBytes))
            throw new AvroFormatException("The Avro file header has no avro.schema.");
        WriterSchemaJson = Encoding.UTF8.GetString(schemaBytes);
        try
        {
            WriterSchema = AvroSchema.Parse(schemaBytes);
        }
        catch (System.Text.Json.JsonException e)
        {
            throw new AvroFormatException("The avro.schema in the file header is not valid JSON.", e);
        }
        _codec = AvroCodec.ForName(metadata.TryGetValue("avro.codec", out var codec) ? Encoding.UTF8.GetString(codec) : null);
        BuildProjection();
    }

    private void BuildProjection()
    {
        var fields = AvroReadSchema.TopLevelFields(WriterSchema);
        var selected = new bool[fields.Count];
        if (_options.Columns is { } names)
        {
            foreach (var name in names)
            {
                int index = -1;
                for (int i = 0; i < fields.Count; i++)
                    if (fields[i].Name == name) { index = i; break; }
                if (index < 0) throw new ArgumentException($"The Avro file has no column named '{name}'.");
                selected[index] = true;
            }
        }
        else if (_options.ColumnIndices is { } indices)
        {
            foreach (int index in indices)
            {
                if ((uint)index >= (uint)fields.Count)
                    throw new ArgumentOutOfRangeException(nameof(AvroReadOptions.ColumnIndices), $"Column index {index} is out of range ({fields.Count} columns).");
                selected[index] = true;
            }
        }
        else
        {
            Array.Fill(selected, true);
        }

        List<Field> arrowFields = [];
        for (int i = 0; i < fields.Count; i++)
            if (selected[i]) arrowFields.Add(AvroReadSchema.ToField(fields[i].Name, fields[i].Schema));
        Schema = new Schema(arrowFields, null);
        _fields = fields;
        _selected = selected;
        (_root, _rowDecoder) = CreateDecoders();
    }

    /// <summary>A fresh builder tree for the projected schema and the row decoder that fills it.</summary>
    private (NativeStructBuilder Root, RowDecoder Decoder) CreateDecoders()
    {
        var root = (NativeStructBuilder)NativeColumnBuilder.Create(new StructType(Schema.FieldsList));
        var decoders = new AvroDecoder?[_fields.Count];
        var skippers = new AvroSkipper?[_fields.Count];
        int child = 0;
        for (int i = 0; i < _fields.Count; i++)
        {
            if (_selected[i]) decoders[i] = AvroDecoder.Compile(_fields[i].Schema, root.Children[child++]);
            else skippers[i] = AvroSkipper.Compile(_fields[i].Schema);
        }
        return (root, new RowDecoder(root, decoders, skippers));
    }

    // ---------------------------------------------------------------- batches

    private ValueTask<RecordBatch?> NextBatchAsync(bool useAsync, CancellationToken ct) =>
        _parallelism > 1 ? NextBatchParallelAsync(useAsync, ct) : NextBatchSequentialAsync(useAsync, ct);

    private async ValueTask<RecordBatch?> NextBatchSequentialAsync(bool useAsync, CancellationToken ct)
    {
        var root = _root!;
        long limit = _options.RowLimit ?? long.MaxValue;
        long target = Math.Min(_options.BatchSize, limit - _rowsEmitted);
        if (_finished || target <= 0) return null;

        long rows = 0;
        while (rows < target)
        {
            if (_blockRecords == 0)
            {
                if (!await LoadBlockAsync(useAsync, ct).ConfigureAwait(false))
                {
                    _finished = true;
                    break;
                }
                continue;
            }
            long n = Math.Min(_blockRecords, target - rows);
            DecodeRows(n);
            rows += n;
        }

        if (rows == 0) return null;
        _rowsEmitted += rows;
        return root.ExportBatch(Schema);
    }

    private void DecodeRows(long n)
    {
        var r = new AvroSpanReader(_blockData!.Span[_blockPos.._blockEnd]);
        var decoder = _rowDecoder!;
        for (long i = 0; i < n; i++) decoder.Decode(ref r);
        _blockPos += r.Position;
        _blockRecords -= n;
        if (_blockRecords == 0 && _blockPos != _blockEnd)
            throw new AvroFormatException($"An Avro block has {_blockEnd - _blockPos} bytes left after its last record.");
    }

    /// <summary>Reads the next block into memory (decompressed). False at the end of the file.</summary>
    private async ValueTask<bool> LoadBlockAsync(bool useAsync, CancellationToken ct)
    {
        while (true)
        {
            if (!await EnsureAsync(1, useAsync, ct).ConfigureAwait(false)) return false;
            long count = await ReadLongAsync(useAsync, ct).ConfigureAwait(false);
            long size = await ReadLongAsync(useAsync, ct).ConfigureAwait(false);
            if (count < 0) throw new AvroFormatException($"Negative Avro block record count {count}.");
            if (size < 0 || size > Array.MaxLength - SyncSize) throw new AvroFormatException($"Invalid Avro block size {size}.");
            if (!await EnsureAsync((int)size + SyncSize, useAsync, ct).ConfigureAwait(false)) throw AvroSpanReader.Truncated();

            var raw = _io.Span.Slice(_ioStart, (int)size);
            if (!_io.Span.Slice(_ioStart + (int)size, SyncSize).SequenceEqual(_sync))
                throw new AvroFormatException("Avro sync marker mismatch: the file is corrupt.");

            if (_codec is NullCodec)
            {
                _blockData = _io;
                _blockPos = _ioStart;
                _blockEnd = _ioStart + (int)size;
            }
            else
            {
                int length = _codec.Decompress(raw, _decompressed);
                _blockData = _decompressed;
                _blockPos = 0;
                _blockEnd = length;
            }
            _ioStart += (int)size + SyncSize;
            _blockRecords = count;
            if (count > 0) return true;
        }
    }

    // ---------------------------------------------------------------- parallel decoding

    /// <summary>A block's raw (still compressed) bytes, copied out of the input buffer.</summary>
    private sealed record RawBlock(long Count, NativeByteBuffer Data, int Length);

    /// <summary>Consecutive blocks decoded together; at most <see cref="RowCap"/> rows are kept (row limit).</summary>
    private sealed record Chunk(List<RawBlock> Blocks, long RowCap);

    /// <summary>One decoding context per concurrently running chunk: builders, decoders and codec state.</summary>
    private sealed class Worker(NativeStructBuilder root, RowDecoder decoder, AvroCodec codec) : IDisposable
    {
        public NativeStructBuilder Root { get; } = root;
        public RowDecoder Decoder { get; } = decoder;
        public AvroCodec Codec { get; } = codec;
        public NativeByteBuffer Decompressed { get; } = new();

        public void Dispose()
        {
            Root.Dispose();
            Decompressed.Release();
        }
    }

    private async ValueTask<RecordBatch?> NextBatchParallelAsync(bool useAsync, CancellationToken ct)
    {
        long limit = _options.RowLimit ?? long.MaxValue;
        while (true)
        {
            if (_ready.TryDequeue(out var ready))
            {
                _rowsEmitted += ready.Length;
                return ready;
            }
            while (_inflight.Count < _parallelism && !_finished && _rowsScheduled < limit)
            {
                var chunk = await ReadChunkAsync(_options.BatchSize, limit - _rowsScheduled, useAsync, ct).ConfigureAwait(false);
                if (chunk is null)
                {
                    _finished = true;
                    break;
                }
                _rowsScheduled += chunk.RowCap;
                _inflight.Enqueue(Task.Run(() => DecodeChunk(chunk)));
            }
            if (!_inflight.TryDequeue(out var next)) return null;
            var batches = useAsync ? await next.ConfigureAwait(false) : next.GetAwaiter().GetResult();
            foreach (var b in batches) _ready.Enqueue(b);
        }
    }

    /// <summary>Gathers whole blocks until the next one would push the run past <paramref name="batchRows"/>
    /// (a single larger block forms a run of its own and is split into several batches when decoded).</summary>
    private async ValueTask<Chunk?> ReadChunkAsync(long batchRows, long rowsLeft, bool useAsync, CancellationToken ct)
    {
        List<RawBlock> blocks = [];
        long rows = 0;
        while (rows < Math.Min(batchRows, rowsLeft))
        {
            var block = _pendingBlock ?? await ReadRawBlockAsync(useAsync, ct).ConfigureAwait(false);
            _pendingBlock = null;
            if (block is null) break;
            if (blocks.Count > 0 && rows + block.Count > batchRows)
            {
                _pendingBlock = block;
                break;
            }
            blocks.Add(block);
            rows += block.Count;
        }
        return blocks.Count == 0 ? null : new Chunk(blocks, Math.Min(rows, rowsLeft));
    }

    /// <summary>Reads the next non-empty block's raw bytes. Null at the end of the file.</summary>
    private async ValueTask<RawBlock?> ReadRawBlockAsync(bool useAsync, CancellationToken ct)
    {
        while (true)
        {
            if (!await EnsureAsync(1, useAsync, ct).ConfigureAwait(false)) return null;
            long count = await ReadLongAsync(useAsync, ct).ConfigureAwait(false);
            long size = await ReadLongAsync(useAsync, ct).ConfigureAwait(false);
            if (count < 0) throw new AvroFormatException($"Negative Avro block record count {count}.");
            if (size < 0 || size > Array.MaxLength - SyncSize) throw new AvroFormatException($"Invalid Avro block size {size}.");
            if (!await EnsureAsync((int)size + SyncSize, useAsync, ct).ConfigureAwait(false)) throw AvroSpanReader.Truncated();
            if (!_io.Span.Slice(_ioStart + (int)size, SyncSize).SequenceEqual(_sync))
                throw new AvroFormatException("Avro sync marker mismatch: the file is corrupt.");
            RawBlock? block = null;
            if (count > 0)
            {
                var data = new NativeByteBuffer((int)Math.Max(size, 1));
                _io.Span.Slice(_ioStart, (int)size).CopyTo(data.Span);
                block = new RawBlock(count, data, (int)size);
            }
            _ioStart += (int)size + SyncSize;
            if (block is not null) return block;
        }
    }

    /// <summary>Decodes a chunk on a pool thread into batches of at most BatchSize rows.</summary>
    private List<RecordBatch> DecodeChunk(Chunk chunk)
    {
        if (!_workers.TryTake(out var worker))
        {
            var (root, decoder) = CreateDecoders();
            worker = new Worker(root, decoder, AvroCodec.ForName(_codec.Name));
        }
        List<RecordBatch> output = [];
        try
        {
            long remaining = chunk.RowCap;
            foreach (var block in chunk.Blocks)
            {
                if (remaining == 0) break;
                var raw = block.Data.Span[..block.Length];
                ReadOnlySpan<byte> data = raw;
                if (worker.Codec is not NullCodec)
                {
                    int length = worker.Codec.Decompress(raw, worker.Decompressed); // may grow the buffer: take the span after
                    data = worker.Decompressed.Span[..length];
                }
                long take = Math.Min(block.Count, remaining);
                DecodeBlock(worker, data, take, take == block.Count, output);
                remaining -= take;
            }
            if (worker.Root.Length > 0) output.Add(worker.Root.ExportBatch(Schema));
            _workers.Add(worker);
            return output;
        }
        catch
        {
            foreach (var b in output) b.Dispose();
            worker.Dispose(); // its builders may hold a partial row
            throw;
        }
        finally
        {
            foreach (var block in chunk.Blocks) block.Data.Release();
        }
    }

    private void DecodeBlock(Worker worker, ReadOnlySpan<byte> data, long rows, bool wholeBlock, List<RecordBatch> output)
    {
        var r = new AvroSpanReader(data);
        for (long i = 0; i < rows; i++)
        {
            worker.Decoder.Decode(ref r);
            if (worker.Root.Length == _options.BatchSize) output.Add(worker.Root.ExportBatch(Schema));
        }
        if (wholeBlock && !r.IsAtEnd)
            throw new AvroFormatException($"An Avro block has {r.Remaining} bytes left after its last record.");
    }

    /// <summary>Waits for chunks still being decoded and frees everything they and the queues hold.</summary>
    private async ValueTask DrainParallelAsync(bool useAsync)
    {
        while (_inflight.TryDequeue(out var task))
        {
            try
            {
                var batches = useAsync ? await task.ConfigureAwait(false) : task.GetAwaiter().GetResult();
                foreach (var b in batches) b.Dispose();
            }
            catch
            {
                // The error was either already reported to the reader's consumer or is moot now that it is disposed.
            }
        }
        while (_ready.TryDequeue(out var b)) b.Dispose();
        _pendingBlock?.Data.Release();
        _pendingBlock = null;
        while (_workers.TryTake(out var w)) w.Dispose();
    }

    // ---------------------------------------------------------------- buffered input

    /// <summary>Makes at least <paramref name="n"/> bytes available at <c>_io[_ioStart..]</c>.
    /// False when the stream ends first with no bytes buffered (for n == 1 that is a clean end of file).</summary>
    private async ValueTask<bool> EnsureAsync(int n, bool useAsync, CancellationToken ct)
    {
        while (_ioEnd - _ioStart < n)
        {
            if (_eof) return false;
            if (_io.Capacity - _ioStart < n)
            {
                // Compact (and grow) so that n bytes fit after _ioStart. Blocks only move between decodes.
                int buffered = _ioEnd - _ioStart;
                _io.Span.Slice(_ioStart, buffered).CopyTo(_io.Span);
                _ioStart = 0;
                _ioEnd = buffered;
                _io.EnsureCapacity(n);
            }
            // Read in chunks of at most 1 MiB beyond what is needed, so a small request does not fill a huge buffer.
            int room = (int)Math.Min(_io.Capacity - _ioEnd, Math.Max(n - (_ioEnd - _ioStart), 1 << 20));
            int read = useAsync
                ? await _stream.ReadAsync(_io.Memory.Slice(_ioEnd, room), ct).ConfigureAwait(false)
                : _stream.Read(_io.Span.Slice(_ioEnd, room));
            if (read == 0)
            {
                _eof = true;
                return _ioEnd - _ioStart >= n;
            }
            _ioEnd += read;
        }
        return true;
    }

    private async ValueTask<long> ReadLongAsync(bool useAsync, CancellationToken ct)
    {
        // A varint is at most 10 bytes; near the end of the file fewer may be available.
        if (!await EnsureAsync(10, useAsync, ct).ConfigureAwait(false) && _ioEnd == _ioStart) throw AvroSpanReader.Truncated();
        var r = new AvroSpanReader(_io.Span[_ioStart.._ioEnd]);
        long value = r.ReadLong();
        _ioStart += r.Position;
        return value;
    }

    private async ValueTask<byte[]> ReadBytesAsync(bool useAsync, CancellationToken ct)
    {
        long length = await ReadLongAsync(useAsync, ct).ConfigureAwait(false);
        if (length < 0 || length > Array.MaxLength) throw new AvroFormatException($"Invalid Avro bytes length {length}.");
        if (!await EnsureAsync((int)length, useAsync, ct).ConfigureAwait(false)) throw AvroSpanReader.Truncated();
        var bytes = _io.Span.Slice(_ioStart, (int)length).ToArray();
        _ioStart += (int)length;
        return bytes;
    }

    // ---------------------------------------------------------------- disposal

    private void ReleaseBuffers()
    {
        _root?.Dispose();
        _root = null;
        _io.Release();
        _decompressed.Release();
        _blockData = null;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        DrainParallelAsync(false).AsTask().GetAwaiter().GetResult();
        ReleaseBuffers();
        if (!_leaveOpen) _stream.Dispose();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await DrainParallelAsync(true).ConfigureAwait(false); // in-flight chunks finish on the pool; do not block a thread on them
        ReleaseBuffers();
        if (!_leaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
    }
}
