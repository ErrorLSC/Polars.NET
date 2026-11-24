using System.Buffers;
using Apache.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Text;
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
    private readonly TextEncoder? _encoder;     // null: UTF-8, written as it is
    private readonly string _encodingName = "";
    private double _utf8RowBytes = 64;          // the UTF-8 bytes of a row, measured (scratch sizes when encoding)

    private CsvWriter(ByteSink sink, Schema schema, CsvWriteOptions options)
    {
        options.Validate();
        if (sink.IsText && options.Compression != CsvCompression.None)
            throw new ArgumentException("Text is written uncompressed (write bytes to compress them).", nameof(options));
        Check(schema);
        var syntax = new CsvSyntax(options);
        CheckFormats(schema, syntax);
        _syntax = syntax;
        if (options.Encoding is { } encoding)
        {
            if (sink.IsText && encoding.CodePage != 65001)
                throw new ArgumentException("Text is written as characters, in no encoding (write bytes to encode them).", nameof(options));
            _encoder = TextEncoder.For(encoding, options.InvalidText == CsvInvalidText.Replace);
            _encodingName = $"{encoding.WebName} ({encoding.CodePage})";
            CheckEncoding(options, encoding);
        }
        _rows = new RowWriter<CsvCell>(sink, schema, this, (data, field, pins) => CsvCell.For(data, field, pins, syntax), new RowWriterSettings(
            "CSV", options.MaxDegreeOfParallelism, options.BatchOwnership, Compression(options.Compression), options.CompressionLevel,
            options.PieceBytes, options.InFlightBytes, 8 + 12 * schema.FieldsList.Count,
            options.WindowPieces > 0 ? options.WindowPieces : 8 * options.MaxDegreeOfParallelism,
            options.WindowPieces > 0 ? 0 : options.WindowBytes, options.Budget));
        PrependHead(schema, options, syntax);
    }

    // The byte order mark (the encoding's: raw), then the header (encoded; a name the encoding cannot write is refused),
    // put before the first rows: built in native memory, handed over as it is.
    private void PrependHead(Schema schema, CsvWriteOptions options, CsvSyntax syntax)
    {
        using var utf8 = new NativeBufferWriter(64 + 16 * schema.FieldsList.Count);
        if (_encoder is null)
        {
            if (options.IncludeBom) utf8.Write("\uFEFF"u8); // EF BB BF
            WriteUtf8Head(utf8, schema, options, syntax);
            _rows.Prepend(utf8.WrittenSpan);
            return;
        }
        WriteUtf8Head(utf8, schema, options, syntax);
        using var output = new NativeBufferWriter(utf8.WrittenCount * 2 + 8);
        output.Write(options.IncludeBom ? TextEncoder.ByteOrderMark(options.Encoding!.CodePage) : []);
        if (!_encoder.TryEncode(utf8.WrittenSpan, output, out _)) throw UnwritableName(schema);
        _rows.Prepend(output.WrittenSpan);
    }

    // The first column name the encoding cannot write, as the header's error.
    private ArgumentException UnwritableName(Schema schema)
    {
        using var name = new NativeBufferWriter(256);
        using var scratch = new NativeBufferWriter(256);
        foreach (Field field in schema.FieldsList)
        {
            name.ResetWrittenCount();
            scratch.ResetWrittenCount();
            System.Text.EncodingExtensions.GetBytes(System.Text.Encoding.UTF8, field.Name, name);
            if (!_encoder!.TryEncode(name.WrittenSpan, scratch, out int at))
                return new ArgumentException($"The header: column '{field.Name}' has {Character(name.WrittenSpan, at)}, which {_encodingName} cannot write: "
                    + "rename the column, set InvalidText to Replace (it becomes '?'), or write UTF-8.", nameof(schema));
        }
        return new ArgumentException($"The header cannot be written in {_encodingName}.", nameof(schema));
    }

    // A column CSV cannot hold is refused before anything is written.
    private static void Check(Schema schema)
    {
        foreach (Field field in schema.FieldsList)
            if (CsvCell.Refusal(field) is { } refusal) throw new NotSupportedException(refusal);
    }

    // What the encoding needs of the options: a byte order mark only for Unicode, no '?' as a separator or quote when
    // '?' replaces, and a null value and line terminator it can write.
    private void CheckEncoding(CsvWriteOptions options, System.Text.Encoding encoding)
    {
        if (options.IncludeBom && TextEncoder.ByteOrderMark(encoding.CodePage).IsEmpty)
            throw new ArgumentException($"{_encodingName} has no byte order mark (UTF-8, UTF-16 and UTF-32 have one): leave IncludeBom off.", nameof(options));
        if (_encoder is { Replace: true } && (options.Separator == '?' || options.Quote == '?'))
            throw new ArgumentException("'?' replaces the characters the encoding cannot write: it cannot be the separator or the quote too.", nameof(options));
        if (_encoder is null) return;
        using var utf8 = new NativeBufferWriter(64);
        using var output = new NativeBufferWriter(64);
        foreach ((string text, string what) in ((string, string)[])[(options.NullValue, "null value"), (options.LineTerminator, "line terminator")])
        {
            utf8.ResetWrittenCount();
            output.ResetWrittenCount();
            System.Text.EncodingExtensions.GetBytes(System.Text.Encoding.UTF8, text, utf8);
            if (!_encoder.TryEncode(utf8.WrittenSpan, output, out int at))
                throw new ArgumentException($"The {what} has {Character(utf8.WrittenSpan, at)}, which {_encodingName} cannot write.", nameof(options));
        }
    }

    // The character at a UTF-8 offset, as messages show it: 'é' (U+00E9).
    private static string Character(ReadOnlySpan<byte> utf8, int at)
    {
        System.Text.Rune.DecodeFromUtf8(utf8[at..], out System.Text.Rune rune, out _);
        return $"'{rune}' (U+{rune.Value:X4})";
    }

    // A date, time or timestamp format that does not fit a column is refused before anything is written.
    private static void CheckFormats(Schema schema, CsvSyntax syntax)
    {
        foreach (Field field in schema.FieldsList)
            if (CsvCell.FormatRefusal(field, syntax) is { } refusal) throw new ArgumentException(refusal, nameof(schema));
    }

    // The header, as asked, in UTF-8, appended to `buffer`.
    private static void WriteUtf8Head(NativeBufferWriter buffer, Schema schema, CsvWriteOptions options, CsvSyntax syntax)
    {
        var o = new TextOut(buffer);
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

    /// <summary>The most pieces encoded at once so far (tests: never more than MaxDegreeOfParallelism).</summary>
    internal int PeakEncoding => _rows.PeakEncoding;

    /// <summary>The pieces encoded (or being encoded) so far (tests: the window's reach).</summary>
    internal long PiecesEncoded => _rows.PiecesEncoded;

    /// <summary>The bytes handed to the output so far.</summary>
    public long BytesWritten => _rows.BytesWritten;

    /// <summary>
    /// The cells of a batch's columns as this writer writes them (its buffers pinned until disposed): writers of the same
    /// schema and options read them alike (a dataset's files share them).
    /// </summary>
    internal BatchCells<CsvCell> CellsOf(RecordBatch batch)
    {
        CsvSyntax syntax = _syntax;
        return new(batch, _rows.Schema, false, (data, field, pins) => CsvCell.For(data, field, pins, syntax));
    }

    /// <summary>
    /// Rows [<paramref name="start"/>, +<paramref name="count"/>) of the batch <paramref name="cells"/> read, encoded on
    /// this thread into the piece being filled (the caller's batch is free once this returns): the piece is written once
    /// it holds a piece's bytes, or before anything written after it. Rows a few at a time (a partition's rows of each
    /// batch of a dataset written side by side) cost no write each, nor a copy before encoding. A value the writer
    /// refuses fails it.
    /// </summary>
    internal void AppendRows(BatchCells<CsvCell> cells, int start, int count) => _rows.AppendRows(cells, start, count);

    /// <summary>The bytes encoded by <see cref="AppendRows"/>, not handed to the writes yet.</summary>
    internal long OpenBytes => _rows.OpenBytes;

    /// <summary>Whether so many rows written as a batch would be encoded on several threads (else better appended).</summary>
    internal bool EncodesApart(int rows) => _rows.EncodesApart(rows);

    /// <summary>Hands the rows encoded by <see cref="AppendRows"/> to the writes (to hold less memory), without waiting for them.</summary>
    internal void HandOff() => _rows.HandOff();

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
    /// <exception cref="CsvWriteException">A value the writer cannot write (a character the encoding cannot write, a string that is not UTF-8 when encoding); the writer cannot be completed then.</exception>
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
        if (_encoder is null)
        {
            EncodeUtf8(columns, buffer, start, end, first);
            return;
        }
        // The rows in UTF-8 (in scratch sized by the rows before: no growing), then encoded; a character the encoding
        // cannot write (or bytes that are not UTF-8) is found again row by row, field by field.
        using var utf8 = new NativeBufferWriter((int)Math.Min(1 << 26, (long)(Volatile.Read(ref _utf8RowBytes) * (end - start) * 1.25) + 256));
        try
        {
            EncodeUtf8(columns, utf8, start, end, first);
        }
        catch (CsvWriteException e) when (e.Row > first)
        {
            // A value refused at a row: a row before it may hold what the encoding cannot write (found when encoding
            // the whole piece, which this row stopped) — the first failure in order is the one reported.
            throw Unwritable(columns, start, start + (int)(e.Row - first), first) ?? e;
        }
        Volatile.Write(ref _utf8RowBytes, (double)utf8.WrittenCount / (end - start));
        if (!_encoder.TryEncode(utf8.WrittenSpan, buffer, out _))
            throw Unwritable(columns, start, end, first)
                ?? new CsvWriteException($"Rows {first}–{first + end - start - 1}: a character cannot be written in {_encodingName}.", "", first);
    }

    // The first field (in order) of rows [start, end) holding a character the encoding cannot write or bytes that are not
    // UTF-8, as an error naming its row and column; null when there is none.
    private CsvWriteException? Unwritable(CsvCell[] columns, int start, int end, long first)
    {
        using var field = new NativeBufferWriter(256);
        using var discard = new NativeBufferWriter(256);
        for (int row = start; row < end; row++)
            for (int k = 0; k < columns.Length; k++)
            {
                field.ResetWrittenCount();
                discard.ResetWrittenCount();
                var o = new TextOut(field);
                if (columns[k].IsNull(row)) o.Bytes(_syntax.Null);
                else columns[k].Write(ref o, row);
                o.Commit();
                if (_encoder!.TryEncode(field.WrittenSpan, discard, out int at)) continue;
                long r = first + row - start;
                string name = columns[k].Name;
                if (!System.Text.Unicode.Utf8.IsValid(field.WrittenSpan))
                    return new CsvWriteException($"Row {r}: Column '{name}': a string that is not UTF-8 (Arrow strings must be).", name, r);
                return new CsvWriteException($"Row {r}: Column '{name}': {Character(field.WrittenSpan, at)} cannot be written in {_encodingName}: "
                    + "set InvalidText to Replace (it becomes '?'), or write UTF-8.", name, r);
            }
        return null;
    }

    private void EncodeUtf8(CsvCell[] columns, NativeBufferWriter buffer, int start, int end, long first)
    {
        int row = start;
        byte separator = _syntax.Separator;
        ReadOnlySpan<byte> nullText = _syntax.Null, lineEnd = _syntax.LineTerminator;
        // A one-byte line end (the default "\n") written as a byte, not a copy per row.
        int lineByte = lineEnd.Length == 1 ? lineEnd[0] : -1;
        try
        {
            var o = new TextOut(buffer);
            for (; row < end; row++)
            {
                for (int k = 0; k < columns.Length; k++)
                {
                    if (k > 0) o.Byte(separator);
                    CsvCell cell = columns[k];
                    if (!cell.IsNull(row)) cell.Write(ref o, row);
                    else if (!nullText.IsEmpty) o.Bytes(nullText);
                }
                if (lineByte >= 0) o.Byte((byte)lineByte);
                else o.Bytes(lineEnd);
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
