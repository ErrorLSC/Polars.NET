using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// Writes rows of <typeparamref name="T"/> (records, classes, structs, F# records) as CSV: one column per public
/// readable property, named by it in the header, laid out as <c>DataFrame.From&lt;T&gt;</c> lays them out
/// (<see cref="ArrowRowBuilder{T}"/>), with GUIDs as their text, enums as their member names, decimals with the fewest
/// digits that hold them exactly and time spans as ISO 8601 (<see cref="CsvReader{T}"/> reads it back); any other
/// <typeparamref name="T"/> as one column named <c>value</c>. A member that is a record, a collection, a dictionary or
/// bytes is refused when the writer is created (a CSV field holds one value). The rows are gathered into record batches
/// of <see cref="BatchRows"/> in native memory, each handed to a <see cref="CsvWriter"/> (which owns and frees it,
/// encoding several side by side); the output depends on the rows alone. The rows of an array or a
/// <see cref="List{T}"/> given to <see cref="WriteRows"/> are turned into batches on several threads; rows given one by
/// one or by a lazy sequence are read as they come, on the caller's thread (a producer may reuse one object for every
/// row).
/// </summary>
/// <remarks>
/// A row the layout refuses (a decimal that does not fit decimal(38, 18) exactly, a value outside its enum) aborts the
/// output (<see cref="Abort"/>): the rows before it in the open batch are lost, and later calls throw.
/// </remarks>
public sealed class CsvWriter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    /// <summary>The most rows of a record batch.</summary>
    public const int BatchRows = RowBatcher<T>.BatchRows;

    private readonly CsvWriter _writer;
    private readonly RowBatcher<T> _rows;
    private bool _disposed;

    private CsvWriter(CsvWriter writer, CsvWriteOptions? options)
    {
        _writer = writer;
        _rows = new RowBatcher<T>(new Sink(writer), (options ?? new()).MaxDegreeOfParallelism, "CSV");
    }

    /// <summary>The CSV writer as the batches' sink.</summary>
    private sealed class Sink(CsvWriter writer) : IBatchSink
    {
        public bool IsWritable => writer.IsWritable;

        public bool TakesBatches => true; // the writer owns the batches: several are encoded side by side

        public void Abort() => writer.Abort();

        public async ValueTask WriteAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
        {
            // Not cancelled once handed over (the rows are counted already); cancellation stops the rows at their source.
            if (useAsync) await writer.WriteAsync(batch, CancellationToken.None).ConfigureAwait(false);
            else writer.Write(batch);
        }
    }

    /// <summary>Creates a writer over a stream.</summary>
    /// <exception cref="NotSupportedException">A member CSV cannot hold.</exception>
    public static CsvWriter<T> Create(Stream stream, CsvWriteOptions? options = null) => new(CsvWriter.Create(stream, CsvRowSchema<T>.Write, Owned(options)), options);

    /// <summary>Creates (or overwrites) a file.</summary>
    /// <inheritdoc cref="Create(Stream, CsvWriteOptions?)" path="/exception"/>
    public static CsvWriter<T> Create(string path, CsvWriteOptions? options = null) => new(CsvWriter.Create(path, CsvRowSchema<T>.Write, Owned(options)), options);

    /// <summary>Creates a writer into a buffer writer (in the options' encoding).</summary>
    /// <inheritdoc cref="Create(Stream, CsvWriteOptions?)" path="/exception"/>
    public static CsvWriter<T> Create(IBufferWriter<byte> output, CsvWriteOptions? options = null) =>
        new(CsvWriter.Create(output, CsvRowSchema<T>.Write, Owned(options)), options);

    /// <summary>Creates a writer of text (UTF-16, uncompressed).</summary>
    /// <inheritdoc cref="Create(Stream, CsvWriteOptions?)" path="/exception"/>
    public static CsvWriter<T> Create(TextWriter output, CsvWriteOptions? options = null) =>
        new(CsvWriter.Create(output, CsvRowSchema<T>.Write, Owned(options)), options);

    /// <summary>The batches are made here and handed over: the writer owns them (whatever the options say).</summary>
    internal static CsvWriteOptions Owned(CsvWriteOptions? options) => (options ?? new()) with { BatchOwnership = BatchOwnership.Writer };

    /// <summary>The Arrow schema the rows are written with (<see cref="ArrowRowBuilder{T}.Schema"/>, GUIDs and decimals marked).</summary>
    public Schema Schema => _writer.Schema;

    /// <summary>The rows written so far (those still gathered into the open batch included).</summary>
    public long RowCount => _rows.RowCount;

    /// <summary>The bytes handed to the output so far (the rows of the open batch not included).</summary>
    public long BytesWritten => _writer.BytesWritten;

    /// <summary>Adds a row.</summary>
    /// <exception cref="ArgumentException"><paramref name="row"/> is null (rows of records cannot be null).</exception>
    public void WriteRow(T row) => _rows.WriteRow(row);

    /// <summary>Adds rows (an array or a <see cref="List{T}"/> is read as a span).</summary>
    /// <inheritdoc cref="WriteRow"/>
    public void WriteRows(IEnumerable<T> rows) => _rows.WriteRowsAsync(rows, false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="WriteRows"/>
    public ValueTask WriteRowsAsync(IEnumerable<T> rows, CancellationToken cancellationToken = default) => _rows.WriteRowsAsync(rows, true, cancellationToken);

    /// <inheritdoc cref="WriteRows"/>
    public ValueTask WriteRowsAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken = default) => _rows.WriteRowsAsync(rows, cancellationToken);

    /// <summary>Writes the rows so far (the open batch included) to the output (<see cref="CsvWriter.Flush"/>).</summary>
    public void Flush()
    {
        _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        _writer.Flush();
    }

    /// <inheritdoc cref="Flush"/>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _rows.FlushAsync(true, cancellationToken).ConfigureAwait(false);
        await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes the rows not written yet and ends the output (the header alone when there were none).</summary>
    public void Complete()
    {
        _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        _writer.Complete();
    }

    /// <inheritdoc cref="Complete"/>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        await _rows.FlushAsync(true, cancellationToken).ConfigureAwait(false);
        await _writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="CsvWriter.Abort"/>
    public void Abort() => _writer.Abort();

    /// <summary>Completes the output (the rows not written yet included) unless writing failed or the writer was aborted, and closes it.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            if (_writer.IsWritable) _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            _writer.Dispose();
            _rows.Dispose();
        }
    }

    /// <inheritdoc cref="Dispose"/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            if (_writer.IsWritable) await _rows.FlushAsync(true, default).ConfigureAwait(false);
        }
        finally
        {
            await _writer.DisposeAsync().ConfigureAwait(false);
            _rows.Dispose();
        }
    }
}
