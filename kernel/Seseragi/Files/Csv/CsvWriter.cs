using System.Buffers;
using Apache.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Csv.Writing;
using Kaminari.Seseragi.Files.IO.Writing;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// Writes Apache Arrow record batches as delimited text (UTF-8), the text the reference dataframe engine writes: a header
/// naming the columns, then one line per row, fields quoted where needed (<see cref="CsvWriteOptions.QuoteStyle"/>).
/// Rows are cut into pieces encoded on several threads into native memory and written in order as they come; the bytes
/// do not depend on the threads, and the memory a write holds is a few pieces. Values: integers of every width, floats as
/// their shortest digits (<c>NaN</c>, <c>inf</c>, <c>-inf</c>), decimals with their scale, booleans <c>true</c> /
/// <c>false</c>, dates <c>yyyy-MM-dd</c>, times <c>HH:mm:ss.fffffffff</c>, timestamps <c>yyyy-MM-ddTHH:mm:ss.fff</c>
/// (the digits of their unit; with a zone, its local time and offset <c>+0900</c>), durations in ISO 8601 (<c>PT1.5S</c>),
/// categoricals as their values, nulls as <see cref="CsvWriteOptions.NullValue"/>. Binary and nested columns are refused.
/// </summary>
public sealed class CsvWriter : IDisposable, IAsyncDisposable, IRowEncoder<CsvCell>
{
    private readonly CsvSyntax _syntax;
    private readonly RowWriter<CsvCell> _rows;

    private CsvWriter(ByteSink sink, Schema schema, CsvWriteOptions options)
    {
        options.Validate();
        if (sink.IsText && options.Compression != CsvCompression.None)
            throw new ArgumentException("Text is written uncompressed (write bytes to compress them).", nameof(options));
        Check(schema);
        var syntax = new CsvSyntax(options);
        _syntax = syntax;
        _rows = new RowWriter<CsvCell>(sink, schema, this, (data, field, pins) => CsvCell.For(data, field, pins, syntax), new RowWriterSettings(
            "CSV", options.MaxDegreeOfParallelism, options.BatchOwnership, Compression(options.Compression), options.CompressionLevel,
            options.PieceBytes, options.InFlightBytes, 8 + 12 * schema.FieldsList.Count,
            options.WindowPieces > 0 ? options.WindowPieces : 2 * options.MaxDegreeOfParallelism));
        _rows.Prepend(Head(schema, options, syntax));
    }

    // A column CSV cannot hold is refused before anything is written.
    private static void Check(Schema schema)
    {
        foreach (Field field in schema.FieldsList)
            if (CsvCell.Refusal(field) is { } refusal) throw new NotSupportedException(refusal);
    }

    // The byte order mark and the header, as asked.
    private static byte[] Head(Schema schema, CsvWriteOptions options, CsvSyntax syntax)
    {
        using var buffer = new NativeBufferWriter(64 + 16 * schema.FieldsList.Count);
        var o = new TextOut(buffer);
        if (options.IncludeBom) o.Bytes([0xEF, 0xBB, 0xBF]);
        if (options.IncludeHeader)
        {
            for (int k = 0; k < schema.FieldsList.Count; k++)
            {
                if (k > 0) o.Byte(syntax.Separator);
                syntax.WriteName(ref o, schema.FieldsList[k].Name);
            }
            o.Bytes(syntax.LineTerminator);
        }
        o.Commit();
        return buffer.WrittenSpan.ToArray();
    }

    private static PieceCompression Compression(CsvCompression compression) => compression switch
    {
        CsvCompression.Gzip => PieceCompression.Gzip,
        CsvCompression.Zstd => PieceCompression.Zstd,
        _ => PieceCompression.None,
    };

    /// <summary>The schema the batches are written with.</summary>
    public Schema Schema => _rows.Schema;

    /// <summary>Whether batches can still be written.</summary>
    internal bool IsWritable => _rows.IsWritable;

    /// <summary>The bytes handed to the output so far.</summary>
    public long BytesWritten => _rows.BytesWritten;

    /// <summary>Creates a writer over a stream (nothing is written before the first rows, or <see cref="Complete"/>).</summary>
    /// <exception cref="NotSupportedException">The schema has a column CSV cannot hold (binary, nested, a time zone not known).</exception>
    public static CsvWriter Create(Stream stream, Schema schema, CsvWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        if (!stream.CanWrite) throw new ArgumentException("The stream is not writable.", nameof(stream));
        options ??= new();
        return new(new StreamSink(stream, options.LeaveOpen), schema, options);
    }

    /// <summary>Creates (or overwrites) a file.</summary>
    /// <exception cref="NotSupportedException">The schema has a column CSV cannot hold (binary, nested, a time zone not known).</exception>
    public static CsvWriter Create(string path, Schema schema, CsvWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(schema);
        options ??= new();
        options.Validate();
        Check(schema);
        var sink = new FileSink(path);
        try
        {
            return new(sink, schema, options);
        }
        catch
        {
            sink.CloseAsync(false).GetAwaiter().GetResult();
            throw;
        }
    }

    /// <summary>
    /// Creates a writer into a buffer writer: an array or native buffer writer, a pipe. The rows are written behind the
    /// calls: the output is the writer's until <see cref="Flush"/>, <see cref="Complete"/> or <see cref="Dispose"/>
    /// returns. A <see cref="System.IO.Pipelines.PipeWriter"/> is flushed after every piece of rows (writing waits while
    /// its reader is behind: write asynchronously then) and left uncompleted; a reader that stops fails the writer with
    /// an <see cref="IOException"/>.
    /// </summary>
    /// <exception cref="NotSupportedException">The schema has a column CSV cannot hold (binary, nested, a time zone not known).</exception>
    public static CsvWriter Create(IBufferWriter<byte> output, Schema schema, CsvWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(schema);
        return new(output is System.IO.Pipelines.PipeWriter pipe ? new PipeSink(pipe) : new BufferSink(output), schema, options ?? new());
    }

    /// <summary>Creates a writer of text (UTF-16, uncompressed).</summary>
    /// <exception cref="NotSupportedException">The schema has a column CSV cannot hold (binary, nested, a time zone not known).</exception>
    public static CsvWriter Create(TextWriter output, Schema schema, CsvWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(schema);
        options ??= new();
        return new(new TextSink(output, options.LeaveOpen), schema, options);
    }

    /// <summary>Writes all batches to a stream and completes it.</summary>
    public static void Write(Stream stream, Schema schema, IEnumerable<RecordBatch> batches, CsvWriteOptions? options = null)
    {
        using CsvWriter writer = Create(stream, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>Writes all batches to a file.</summary>
    public static void Write(string path, Schema schema, IEnumerable<RecordBatch> batches, CsvWriteOptions? options = null)
    {
        using CsvWriter writer = Create(path, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>Writes all batches to a buffer writer.</summary>
    public static void Write(IBufferWriter<byte> output, Schema schema, IEnumerable<RecordBatch> batches, CsvWriteOptions? options = null)
    {
        using CsvWriter writer = Create(output, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>Writes all batches to a text writer.</summary>
    public static void Write(TextWriter output, Schema schema, IEnumerable<RecordBatch> batches, CsvWriteOptions? options = null)
    {
        using CsvWriter writer = Create(output, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>The batches as a string of CSV.</summary>
    public static string WriteString(Schema schema, IEnumerable<RecordBatch> batches, CsvWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var sink = new StringSink();
        try
        {
            using (var writer = new CsvWriter(sink, schema, options ?? new())) WriteAll(writer, batches);
            return sink.TakeText();
        }
        catch
        {
            sink.TakeText();
            throw;
        }
    }

    /// <summary>Writes all batches to a stream and completes it.</summary>
    public static async Task WriteAsync(Stream stream, Schema schema, IAsyncEnumerable<RecordBatch> batches, CsvWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using CsvWriter writer = Create(stream, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes all batches to a file.</summary>
    public static async Task WriteAsync(string path, Schema schema, IAsyncEnumerable<RecordBatch> batches, CsvWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using CsvWriter writer = Create(path, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes all batches to a buffer writer.</summary>
    public static async Task WriteAsync(IBufferWriter<byte> output, Schema schema, IAsyncEnumerable<RecordBatch> batches, CsvWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using CsvWriter writer = Create(output, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes all batches to a text writer.</summary>
    public static async Task WriteAsync(TextWriter output, Schema schema, IAsyncEnumerable<RecordBatch> batches, CsvWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using CsvWriter writer = Create(output, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    private static void WriteAll(CsvWriter writer, IEnumerable<RecordBatch> batches)
    {
        ArgumentNullException.ThrowIfNull(batches);
        try
        {
            foreach (RecordBatch batch in batches) writer.Write(batch);
            writer.Complete();
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    private static async Task WriteAllAsync(CsvWriter writer, IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batches);
        try
        {
            await foreach (RecordBatch batch in batches.WithCancellation(ct).ConfigureAwait(false)) await writer.WriteAsync(batch, ct).ConfigureAwait(false);
            await writer.CompleteAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    /// <summary>Writes a batch's rows (taken over with <see cref="BatchOwnership.Writer"/>).</summary>
    /// <exception cref="ArgumentException">The batch's columns are not the schema's.</exception>
    /// <exception cref="CsvWriteException">A value the writer cannot write (a string that is not UTF-8); the writer cannot be completed then.</exception>
    public void Write(RecordBatch batch) => _rows.Write(batch);

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => _rows.WriteAsync(batch, cancellationToken);

    /// <summary>
    /// Writes the rows still on their way (rows are encoded and written behind <see cref="Write(RecordBatch)"/>): the
    /// output holds every row written so far, and is the caller's until the next write.
    /// </summary>
    public void Flush() => _rows.Flush();

    /// <inheritdoc cref="Flush"/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => _rows.FlushAsync(cancellationToken);

    /// <summary>Writes the rows still on their way (the header when there were none); nothing can be written afterwards.</summary>
    public void Complete() => _rows.Complete();

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => _rows.CompleteAsync(cancellationToken);

    /// <summary>
    /// Gives the output up: rows still on their way are not written and the output is not completed; <see cref="Dispose"/>
    /// then closes it. Writing or completing afterwards throws.
    /// </summary>
    public void Abort() => _rows.Abort();

    // Rows [start, end) of a batch's cells appended to `buffer`: fields separated, each line ended; a value that cannot be
    // written fails naming its row among all.
    void IRowEncoder<CsvCell>.EncodeRows(CsvCell[] columns, NativeBufferWriter buffer, int start, int end, long first)
    {
        int row = start;
        byte separator = _syntax.Separator;
        ReadOnlySpan<byte> nullText = _syntax.Null, lineEnd = _syntax.LineTerminator;
        try
        {
            var o = new TextOut(buffer);
            for (; row < end; row++)
            {
                for (int k = 0; k < columns.Length; k++)
                {
                    if (k > 0) o.Byte(separator);
                    CsvCell cell = columns[k];
                    if (cell.IsNull(row)) o.Bytes(nullText);
                    else cell.Write(ref o, row);
                }
                o.Bytes(lineEnd);
            }
            o.Commit();
        }
        catch (CsvWriteException e) when (e.Row < 0)
        {
            long at = first + row - start;
            throw new CsvWriteException($"Row {at}: {e.Message}", e.Column, at);
        }
    }

    ReadOnlySpan<byte> IRowEncoder<CsvCell>.Tail(long rows) => default;

    /// <summary>Completes the output unless writing failed or the writer was aborted, and closes it (unless asked not to).</summary>
    public void Dispose() => _rows.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _rows.DisposeAsync();
}
