using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>Options for <see cref="AvroWriter"/>.</summary>
public sealed record AvroWriteOptions
{
    /// <summary>Default target size of a block before compression (1 MiB).</summary>
    public const int DefaultBlockSize = 1 << 20;

    /// <summary>Block compression: <see cref="NoCompression"/> (default), <see cref="DeflateCompression"/>,
    /// <see cref="SnappyCompression"/> or <see cref="ZstandardCompression"/>, each carrying its own level.</summary>
    public IAvroCompression Compression { get; init; } = NoCompression.Instance;

    /// <summary>Name of the top-level record (an Avro name). Nested records are named after their fields.</summary>
    public string RecordName { get; init; } = "topLevelRecord";

    /// <summary>Namespace of the top-level record, or null for none.</summary>
    public string? RecordNamespace { get; init; }

    /// <summary>A block is closed once its encoded rows reach this many bytes (before compression).</summary>
    public int BlockSize { get; init; } = DefaultBlockSize;

    /// <summary>Extra file metadata. Keys starting with <c>avro.</c> are reserved.</summary>
    public IReadOnlyDictionary<string, byte[]>? Metadata { get; init; }

    /// <summary>Leave the stream open when the writer is disposed.</summary>
    public bool LeaveOpen { get; init; }
}

/// <summary>
/// Writes Arrow record batches as an Avro object container file. Nullable fields become <c>["null", T]</c>
/// unions; other types map as spark-avro does (see the package docs). Values that Avro
/// cannot hold exactly are refused with <see cref="NotSupportedException"/>, never silently changed.
/// </summary>
public sealed class AvroWriter : IAsyncDisposable, IDisposable
{
    private static ReadOnlySpan<byte> Magic => "Obj\u0001"u8;

    private readonly Stream _stream;
    private readonly AvroWriteOptions _options;
    private readonly AvroCodec _codec;
    private readonly RowEncoder _encoder;
    private readonly int _columnCount;
    private readonly byte[] _sync = new byte[16];
    private readonly byte[] _schemaJson;
    private readonly AvroBufferWriter _block = new();
    private readonly NativeByteBuffer _compressed = new();
    private long _blockRows;
    private bool _headerWritten;
    private bool _completed;
    private bool _faulted;
    private bool _disposed;

    private AvroWriter(Stream stream, Schema schema, AvroWriteOptions options)
    {
        if (options.BlockSize <= 0) throw new ArgumentOutOfRangeException(nameof(options), "BlockSize must be positive.");
        if (options.Metadata?.Keys.FirstOrDefault(k => k.StartsWith("avro.", StringComparison.Ordinal)) is { } reserved)
            throw new ArgumentException($"The metadata key '{reserved}' is reserved by Avro.", nameof(options));
        _stream = stream;
        _options = options;
        _codec = (options.Compression ?? throw new ArgumentNullException(nameof(options), "Compression must not be null.")).CreateAvroCodec();
        var (avroSchema, encoder) = AvroWriteSchema.Build(schema, options.RecordName, options.RecordNamespace);
        _encoder = encoder;
        _columnCount = schema.FieldsList.Count;
        Schema = schema;
        _schemaJson = AvroSchemaJsonWriter.WriteUtf8(avroSchema);
        RandomNumberGenerator.Fill(_sync);
    }

    /// <summary>The Arrow schema the batches must have.</summary>
    public Schema Schema { get; }

    /// <summary>The Avro writer schema written to the file header.</summary>
    public string WriterSchemaJson => Encoding.UTF8.GetString(_schemaJson);

    /// <summary>Creates a writer over a stream. The header is written with the first block (or on completion).</summary>
    public static AvroWriter Create(Stream stream, Schema schema, AvroWriteOptions? options = null) =>
        new(stream, schema, options ?? new());

    /// <summary>Creates (or overwrites) a file.</summary>
    public static AvroWriter Create(string path, Schema schema, AvroWriteOptions? options = null)
    {
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

    /// <summary>Writes all batches to a stream and completes the file. If the source fails or is cancelled, the
    /// final block is not written and the error propagates; what reached the stream is the caller's to discard.</summary>
    public static async Task WriteAsync(Stream stream, Schema schema, IAsyncEnumerable<RecordBatch> batches,
        AvroWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        await using var writer = Create(stream, schema, options);
        await writer.WriteAllAsync(batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes all batches to a file and completes it. If writing fails (including a failing or cancelled
    /// source), the partial file is deleted: Avro has no footer, so a file cut at a block boundary would read as
    /// complete.</summary>
    public static async Task WriteAsync(string path, Schema schema, IAsyncEnumerable<RecordBatch> batches,
        AvroWriteOptions? options = null, CancellationToken cancellationToken = default)
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

    /// <summary>Writes a stream of batches and completes the file. If the source fails or is cancelled, the
    /// file is left without its final block (incomplete, not silently short).</summary>
    private async Task WriteAllAsync(IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        try
        {
            await foreach (var batch in batches.WithCancellation(ct).ConfigureAwait(false))
                await WriteAsync(batch, ct).ConfigureAwait(false);
        }
        catch
        {
            _faulted = true;
            throw;
        }
        await CompleteAsync(ct).ConfigureAwait(false);
    }

    private void WriteAll(IEnumerable<RecordBatch> batches)
    {
        try
        {
            foreach (var batch in batches) Write(batch);
        }
        catch
        {
            _faulted = true;
            throw;
        }
        Complete();
    }

    /// <summary>Writes all batches and completes the file synchronously.</summary>
    public static void Write(Stream stream, Schema schema, IEnumerable<RecordBatch> batches, AvroWriteOptions? options = null)
    {
        using var writer = Create(stream, schema, options);
        writer.WriteAll(batches);
    }

    /// <summary>Writes all batches to a file and completes it synchronously; a failure deletes the partial file.</summary>
    public static void Write(string path, Schema schema, IEnumerable<RecordBatch> batches, AvroWriteOptions? options = null)
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

    /// <summary>Appends a batch. Full blocks are written to the stream as they fill.</summary>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) =>
        WriteCoreAsync(batch, true, cancellationToken);

    /// <summary>Appends a batch synchronously.</summary>
    public void Write(RecordBatch batch) => WriteCoreAsync(batch, false, default).AsTask().GetAwaiter().GetResult();

    /// <summary>Writes the last block (and the header of an empty file) and flushes the stream.</summary>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => CompleteCoreAsync(true, cancellationToken);

    /// <summary>Writes the last block and flushes the stream synchronously.</summary>
    public void Complete() => CompleteCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    private async ValueTask WriteCoreAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("The Avro writer has been completed.");
        CheckSchema(batch);
        try
        {
            _encoder.Bind(batch);
            for (int row = 0; row < batch.Length; row++)
            {
                _encoder.Encode(row, _block);
                _blockRows++;
                if (_block.Length >= _options.BlockSize) await FlushBlockAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
        finally
        {
            _encoder.Release();
        }
    }

    private void CheckSchema(RecordBatch batch)
    {
        if (batch.ColumnCount != _columnCount)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns; the writer schema has {_columnCount}.", nameof(batch));
        if (ReferenceEquals(batch.Schema, Schema)) return;
        for (int i = 0; i < _columnCount; i++)
        {
            var expected = Schema.FieldsList[i].DataType;
            var actual = batch.Column(i).Data.DataType;
            if (!AvroWriteSchema.SameEncoding(expected, actual))
                throw new ArgumentException($"Column {i} ('{Schema.FieldsList[i].Name}') is {actual.Name} in the batch but {expected.Name} in the writer schema.", nameof(batch));
        }
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) return;
        try
        {
            await FlushBlockAsync(useAsync, ct).ConfigureAwait(false);
            await WriteHeaderAsync(useAsync, ct).ConfigureAwait(false);
            if (useAsync) await _stream.FlushAsync(ct).ConfigureAwait(false);
            else _stream.Flush();
            _completed = true;
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private async ValueTask WriteHeaderAsync(bool useAsync, CancellationToken ct)
    {
        if (_headerWritten) return;
        using var header = new AvroBufferWriter(4096);
        header.WriteFixed(Magic);
        List<(string Key, byte[] Value)> metadata =
        [
            ("avro.schema", _schemaJson),
            ("avro.codec", Encoding.UTF8.GetBytes(_codec.Name)),
            .. (_options.Metadata ?? new Dictionary<string, byte[]>()).Select(kv => (kv.Key, kv.Value)),
        ];
        header.WriteLong(metadata.Count);
        foreach (var (key, value) in metadata)
        {
            header.WriteBytes(Encoding.UTF8.GetBytes(key));
            header.WriteBytes(value);
        }
        header.WriteLong(0);
        header.WriteFixed(_sync);
        await WriteToStreamAsync(header.WrittenMemory, useAsync, ct).ConfigureAwait(false);
        _headerWritten = true;
    }

    private async ValueTask FlushBlockAsync(bool useAsync, CancellationToken ct)
    {
        if (_blockRows == 0) return;
        await WriteHeaderAsync(useAsync, ct).ConfigureAwait(false);
        ReadOnlyMemory<byte> data;
        if (_codec is NullCodec)
        {
            data = _block.WrittenMemory;
        }
        else
        {
            int n = _codec.Compress(_block.WrittenSpan, _compressed);
            data = _compressed.Memory[..n];
        }
        using (var prefix = new AvroBufferWriter(32))
        {
            prefix.WriteLong(_blockRows);
            prefix.WriteLong(data.Length);
            await WriteToStreamAsync(prefix.WrittenMemory, useAsync, ct).ConfigureAwait(false);
        }
        await WriteToStreamAsync(data, useAsync, ct).ConfigureAwait(false);
        await WriteToStreamAsync(_sync, useAsync, ct).ConfigureAwait(false);
        _block.Clear();
        _blockRows = 0;
    }

    private ValueTask WriteToStreamAsync(ReadOnlyMemory<byte> data, bool useAsync, CancellationToken ct)
    {
        if (useAsync) return _stream.WriteAsync(data, ct);
        _stream.Write(data.Span);
        return ValueTask.CompletedTask;
    }

    private void Release()
    {
        _disposed = true;
        _block.Dispose();
        _compressed.Release();
    }

    /// <summary>Completes the file (unless a write failed) and releases the stream.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted) Complete();
        }
        finally
        {
            Release();
            if (!_options.LeaveOpen) _stream.Dispose();
        }
    }

    /// <inheritdoc cref="Dispose"/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted) await CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            Release();
            if (!_options.LeaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
