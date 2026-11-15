using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;
using Kaminari.Seseragi.Files.Parquet.Writing;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Writes Arrow record batches as a Parquet file: flat columns (nested ones are not written yet), each row group
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
    private readonly ColumnWriter[] _columns;
    private readonly bool[] _required;
    private readonly List<RowGroup> _rowGroups = [];
    private readonly KeyValue[]? _keyValues;
    private long _rowGroupRows, _totalRows, _position;
    private bool _started, _completed, _faulted, _disposed;
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
        LeafPlan[] plans = [.. schema.FieldsList.Select(f => WriteSchema.Plan(f, options))];
        var settings = new PageSettings(options.Compression.ParquetCodec, options.Compression.ParquetLevel, options.DataPageVersion,
            options.DataPageBytes, options.DataPageRows, options.Dictionary, options.DictionaryPageBytes, options.Statistics,
            options.StatisticsTruncateLength, options.WriteChecksums);
        _elements = [new SchemaElement { Name = "schema", NumChildren = plans.Length }, .. plans.Select(p => p.Element)];
        _columns = [.. plans.Select(p => p.CreateWriter(settings))];
        _required = [.. plans.Select(p => p.Element.Repetition == FieldRepetition.Required)];
        List<KeyValue> keyValues = [];
        if (options.StoreArrowSchema) keyValues.Add(new KeyValue(Parquet.Schema.ArrowMapping.ArrowSchemaKey, Convert.ToBase64String(ArrowSchemaMessage(schema))));
        if (options.Metadata is { } metadata) keyValues.AddRange(metadata.Select(kv => new KeyValue(kv.Key, kv.Value)));
        _keyValues = keyValues.Count > 0 ? [.. keyValues] : null;
    }

    /// <summary>The Arrow schema the batches must have.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>The bytes written to the stream so far (the row groups closed, and the footer once completed).</summary>
    public long BytesWritten => _position;

    /// <summary>About how many bytes the open row group will take (its pages so far, the open ones as plainly encoded).</summary>
    public long PendingBytes => _columns.Sum(c => c.BufferedBytes);

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
            _faulted = true;
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
            _faulted = true;
            throw;
        }
        await CompleteAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Adds a batch's rows. The batch is not kept: it may be disposed once this returns.</summary>
    public void Write(RecordBatch batch) => WriteCoreAsync(batch, false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => WriteCoreAsync(batch, true, cancellationToken);

    /// <summary>Closes the open row group now (nothing happens when it holds no rows).</summary>
    public void EndRowGroup() => EndRowGroupCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="EndRowGroup"/>
    public ValueTask EndRowGroupAsync(CancellationToken cancellationToken = default) => EndRowGroupCoreAsync(true, cancellationToken);

    /// <summary>Writes the last row group and the footer, and flushes the stream.</summary>
    public void Complete() => CompleteCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => CompleteCoreAsync(true, cancellationToken);

    private void CheckWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("The Parquet writer has been completed.");
        if (_faulted) throw new InvalidOperationException("The Parquet writer failed earlier; the file cannot be completed.");
    }

    private async ValueTask WriteCoreAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        CheckWritable();
        CheckBatch(batch);
        try
        {
            int offset = 0;
            while (offset < batch.Length)
            {
                int take = (int)Math.Min(batch.Length - offset, Math.Min(_options.RowGroupRows - _rowGroupRows, _options.DataPageRows));
                for (int c = 0; c < _columns.Length; c++) _columns[c].Write(batch.Column(c), offset, take);
                _rowGroupRows += take;
                offset += take;
                if (_rowGroupRows >= _options.RowGroupRows || PendingBytes >= _options.RowGroupBytes)
                    await FlushRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>The batch's columns have the writer's types, and required columns no nulls (checked before anything is written).</summary>
    private void CheckBatch(RecordBatch batch)
    {
        if (batch.ColumnCount != _columns.Length)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns; the writer schema has {_columns.Length}.", nameof(batch));
        for (int c = 0; c < _columns.Length; c++)
        {
            Field field = Schema.FieldsList[c];
            IArrowArray column = batch.Column(c);
            if (column.Length != batch.Length)
                throw new ArgumentException($"Column {c} ('{field.Name}') has {column.Length} rows; the batch has {batch.Length}.", nameof(batch));
            if (!ReferenceEquals(batch.Schema, Schema) && !SameType(field.DataType, column.Data.DataType))
                throw new ArgumentException($"Column {c} ('{field.Name}') is {column.Data.DataType.Name} in the batch but {field.DataType.Name} in the writer schema.", nameof(batch));
            if (_required[c] && ArrowValues.NullCount(column) > 0)
            {
                for (int i = 0; i < column.Length; i++)
                {
                    if (ArrowValues.IsNull(column, i))
                        throw new ArgumentException($"Column '{field.Name}' is not nullable but row {i} of the batch is null.", nameof(batch));
                }
            }
        }
    }

    /// <summary>Whether two Arrow types hold their values the same way (the type and its parameters).</summary>
    private static bool SameType(IArrowType a, IArrowType b) => (a, b) switch
    {
        (DictionaryType x, DictionaryType y) => SameType(x.IndexType, y.IndexType) && SameType(x.ValueType, y.ValueType),
        (TimestampType x, TimestampType y) => x.Unit == y.Unit && (x.Timezone ?? "") == (y.Timezone ?? ""),
        (Time32Type x, Time32Type y) => x.Unit == y.Unit,
        (Time64Type x, Time64Type y) => x.Unit == y.Unit,
        (DurationType x, DurationType y) => x.Unit == y.Unit,
        (IntervalType x, IntervalType y) => x.Unit == y.Unit,
        (Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type, _) =>
            a.TypeId == b.TypeId && WriteSchema.DecimalParameters(a) == WriteSchema.DecimalParameters(b),
        (FixedSizeBinaryType x, FixedSizeBinaryType y) => a.TypeId == b.TypeId && x.ByteWidth == y.ByteWidth,
        _ => a.TypeId == b.TypeId,
    };

    private async ValueTask EndRowGroupCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        try
        {
            await FlushRowGroupAsync(useAsync, ct).ConfigureAwait(false);
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
        await StartAsync(useAsync, ct).ConfigureAwait(false);
        long groupStart = _position, uncompressed = 0, compressed = 0;
        var chunks = new ColumnChunk[_columns.Length];
        for (int c = 0; c < _columns.Length; c++)
        {
            FinishedChunk chunk = _columns[c].Finish();
            long start = _position;
            await WriteBytesAsync(chunk.DictionaryPage, useAsync, ct).ConfigureAwait(false);
            await WriteBytesAsync(chunk.DataPages, useAsync, ct).ConfigureAwait(false);
            SchemaElement element = _elements[c + 1];
            var meta = new ColumnMetaData
            {
                Type = element.Type!.Value,
                Encodings = chunk.Encodings,
                PathInSchema = [element.Name],
                Codec = _options.Compression.ParquetCodec,
                NumValues = chunk.Values,
                TotalUncompressedSize = chunk.UncompressedSize,
                TotalCompressedSize = chunk.CompressedSize,
                DataPageOffset = start + chunk.DictionaryPage.Length,
                DictionaryPageOffset = chunk.DictionaryPage.IsEmpty ? null : start,
                Statistics = chunk.Statistics,
                EncodingStats = chunk.EncodingStats,
            };
            chunks[c] = new ColumnChunk { FileOffset = start, MetaData = meta };
            uncompressed += chunk.UncompressedSize;
            compressed += chunk.CompressedSize;
            _columns[c].Reset();
        }
        _rowGroups.Add(new RowGroup
        {
            Columns = chunks,
            TotalByteSize = uncompressed,
            NumRows = _rowGroupRows,
            FileOffset = groupStart,
            TotalCompressedSize = compressed,
            Ordinal = (short)Math.Min(_rowGroups.Count, short.MaxValue),
        });
        _totalRows += _rowGroupRows;
        _rowGroupRows = 0;
    }

    private async ValueTask StartAsync(bool useAsync, CancellationToken ct)
    {
        if (_started) return;
        _started = true;
        await WriteBytesAsync(Magic.ToArray(), useAsync, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteBytesAsync(ReadOnlyMemory<byte> bytes, bool useAsync, CancellationToken ct)
    {
        if (bytes.IsEmpty) return;
        if (useAsync) await _stream.WriteAsync(bytes, ct).ConfigureAwait(false);
        else _stream.Write(bytes.Span);
        _position += bytes.Length;
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) return;
        if (_faulted) throw new InvalidOperationException("The Parquet writer failed earlier; the file cannot be completed.");
        try
        {
            await FlushRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            await StartAsync(useAsync, ct).ConfigureAwait(false);
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

    /// <summary>Completes the file unless writing failed (then the stream keeps what was written, without a footer), and closes the stream unless asked not to.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted) Complete();
        }
        finally
        {
            _disposed = true;
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
            _disposed = true;
            if (!_options.LeaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
