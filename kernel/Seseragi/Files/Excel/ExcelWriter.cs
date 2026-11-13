using System.Buffers;
using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Kaminari.Seseragi.Files.Excel.Writing;

namespace Kaminari.Seseragi.Files.Excel;

/// <summary>Options for <see cref="ExcelWriter"/>.</summary>
public sealed record ExcelWriteOptions
{
    /// <summary>The sheet name: 1 to 31 characters, none of <c>[ ] : * ? / \</c>, not starting or ending with an
    /// apostrophe, not <c>History</c>.</summary>
    public string SheetName { get; init; } = "Sheet1";

    /// <summary>Write the column names as the first row (default).</summary>
    public bool IncludeHeader { get; init; } = true;

    /// <summary>Number format of date columns.</summary>
    public string DateFormat { get; init; } = "yyyy-mm-dd";

    /// <summary>Number format of timestamp columns.</summary>
    public string DatetimeFormat { get; init; } = "yyyy-mm-dd hh:mm:ss";

    /// <summary>Number format of time-of-day columns.</summary>
    public string TimeFormat { get; init; } = "hh:mm:ss";

    /// <summary>Number format of duration columns (elapsed hours).</summary>
    public string DurationFormat { get; init; } = "[h]:mm:ss";

    /// <summary>
    /// Store strings once in the shared string table (default; what Excel itself writes, and smaller files with
    /// repeated values). The table is kept in native memory while the sheet streams and written last. Off, strings
    /// are written inline in their cells and nothing is kept.
    /// </summary>
    public bool SharedStrings { get; init; } = true;

    /// <summary>Set column widths from the header and the first <see cref="AutofitRows"/> rows (held back until then,
    /// since column widths precede the cells in the sheet).</summary>
    public bool Autofit { get; init; }

    /// <summary>Rows <see cref="Autofit"/> measures.</summary>
    public int AutofitRows { get; init; } = 100;

    /// <summary>Keep the header row in view when scrolling.</summary>
    public bool FreezeHeader { get; init; }

    /// <summary>Compression of the zip entries.</summary>
    public CompressionLevel CompressionLevel { get; init; } = CompressionLevel.Optimal;

    /// <summary>Leave the stream open when the writer is disposed.</summary>
    public bool LeaveOpen { get; init; }
}

/// <summary>
/// Writes Arrow record batches as one sheet of an xlsx workbook, streaming: rows are encoded as they arrive and
/// compressed into the zip entry of the sheet; the shared strings, styles and workbook parts follow on
/// <see cref="Complete"/>. Values Excel cannot hold are refused with <see cref="NotSupportedException"/>, never
/// changed silently: types without a cell form (binary, nested), more rows than a sheet holds, longer strings than a
/// cell holds, sub-microsecond times, dates before 1900. Nulls are empty cells; NaN and infinities are <c>#NUM!</c>
/// errors; Int64, UInt64 and decimals are numbers when a double holds them exactly, text otherwise.
/// </summary>
public sealed class ExcelWriter : IDisposable, IAsyncDisposable
{
    private const int FlushSize = 1 << 20;
    private static readonly SearchValues<char> SheetNameForbidden = SearchValues.Create("[]:*?/\\");

    private readonly Stream _stream;
    private readonly ExcelWriteOptions _options;
    private readonly ZipArchive _zip;
    private readonly ExcelStyles _styles;
    private readonly CellEncoder[] _encoders;
    private readonly SharedStringTable? _sharedStrings;
    private readonly XmlOut _xml = new();
    private readonly CellSink _sink;
    private readonly double[] _widths;
    private Stream? _sheetStream;
    private int _rowsWritten;     // sheet rows, header included
    private int _dataRows;
    private bool _headStarted;    // the worksheet start (and widths) went out
    private bool _completed;
    private bool _faulted;
    private bool _disposed;

    private ExcelWriter(Stream stream, Schema schema, ExcelWriteOptions options)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        if (!stream.CanWrite) throw new ArgumentException("The stream is not writable.", nameof(stream));
        ValidateSheetName(options.SheetName);
        if (options.AutofitRows < 0) throw new ArgumentOutOfRangeException(nameof(options), "AutofitRows must not be negative.");
        if (schema.FieldsList.Count > ExcelCellReference.MaxColumns)
            throw new NotSupportedException($"A sheet holds at most {ExcelCellReference.MaxColumns} columns; the schema has {schema.FieldsList.Count}.");
        _stream = stream;
        _options = options;
        Schema = schema;
        _styles = new ExcelStyles(options);
        _encoders = [.. schema.FieldsList.Select(f => CellEncoder.Create(f.DataType, _styles)
            ?? throw new NotSupportedException($"Column '{f.Name}' of type {f.DataType.Name} has no Excel cell form."))];
        _sharedStrings = options.SharedStrings ? new SharedStringTable() : null;
        _sink = new XlsxCellSink(_xml, _sharedStrings, _encoders.Length);
        _widths = new double[_encoders.Length];
        _zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: true);
        if (options.IncludeHeader) WriteHeader();

        static void ValidateSheetName(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length > 31)
                throw new ArgumentException($"Sheet name '{name}' must have 1 to 31 characters.", nameof(name));
            if (name.AsSpan().IndexOfAny(SheetNameForbidden) >= 0)
                throw new ArgumentException($"Sheet name '{name}' must not contain any of [ ] : * ? / \\.", nameof(name));
            if (name[0] == '\'' || name[^1] == '\'')
                throw new ArgumentException($"Sheet name '{name}' must not start or end with an apostrophe.", nameof(name));
            if (string.Equals(name, "History", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("'History' is reserved by Excel as a sheet name.", nameof(name));
        }

        void WriteHeader()
        {
            BeginRow();
            for (int c = 0; c < _encoders.Length; c++)
            {
                SetColumn(c);
                var name = Encoding.UTF8.GetBytes(Schema.FieldsList[c].Name);
                _sink.String(name);
                Measure(c, name.Length);
            }
            _sink.EndRow();
        }
    }

    /// <summary>The Arrow schema the batches must have.</summary>
    public Schema Schema { get; }

    /// <summary>Creates a writer over a stream.</summary>
    public static ExcelWriter Create(Stream stream, Schema schema, ExcelWriteOptions? options = null) =>
        new(stream, schema, options ?? new());

    /// <summary>Creates (or overwrites) a file.</summary>
    public static ExcelWriter Create(string path, Schema schema, ExcelWriteOptions? options = null)
    {
        var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16, FileOptions.Asynchronous);
        try
        {
            return new(stream, schema, (options ?? new()) with { LeaveOpen = false });
        }
        catch
        {
            stream.Dispose();
            File.Delete(path);
            throw;
        }
    }

    /// <summary>Writes all batches to a stream and completes the workbook.</summary>
    public static void Write(Stream stream, Schema schema, IEnumerable<RecordBatch> batches, ExcelWriteOptions? options = null)
    {
        using var writer = Create(stream, schema, options);
        foreach (var b in batches) writer.Write(b);
        writer.Complete();
    }

    /// <summary>Writes all batches to a file and completes the workbook; a failure deletes the partial file.</summary>
    public static void Write(string path, Schema schema, IEnumerable<RecordBatch> batches, ExcelWriteOptions? options = null)
    {
        try
        {
            using var writer = Create(path, schema, options);
            foreach (var b in batches) writer.Write(b);
            writer.Complete();
        }
        catch
        {
            File.Delete(path);
            throw;
        }
    }

    /// <inheritdoc cref="Write(Stream, Schema, IEnumerable{RecordBatch}, ExcelWriteOptions?)"/>
    public static async Task WriteAsync(Stream stream, Schema schema, IAsyncEnumerable<RecordBatch> batches,
        ExcelWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        await using var writer = Create(stream, schema, options);
        await foreach (var b in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
            await writer.WriteAsync(b, cancellationToken).ConfigureAwait(false);
        await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="Write(string, Schema, IEnumerable{RecordBatch}, ExcelWriteOptions?)"/>
    public static async Task WriteAsync(string path, Schema schema, IAsyncEnumerable<RecordBatch> batches,
        ExcelWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var writer = Create(path, schema, options);
            await foreach (var b in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
                await writer.WriteAsync(b, cancellationToken).ConfigureAwait(false);
            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            File.Delete(path);
            throw;
        }
    }

    /// <summary>Encodes a batch as rows (compressed into the sheet as the buffer fills).</summary>
    public void Write(RecordBatch batch) => WriteCoreAsync(batch, false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => WriteCoreAsync(batch, true, cancellationToken);

    /// <summary>Finishes the sheet and writes the remaining parts and the zip directory.</summary>
    public void Complete() => CompleteCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => CompleteCoreAsync(true, cancellationToken);

    private async ValueTask WriteCoreAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("The writer is completed.");
        if (_faulted) throw new InvalidOperationException("An earlier write failed; the workbook is incomplete.");
        CheckSchema(batch);
        try
        {
            if (_rowsWritten + (long)batch.Length > ExcelCellReference.MaxRows)
                throw new NotSupportedException($"A sheet holds at most {ExcelCellReference.MaxRows:N0} rows; this batch would make {_rowsWritten + (long)batch.Length:N0}.");
            for (int c = 0; c < _encoders.Length; c++) _encoders[c].Bind(batch.Column(c));
            for (int r = 0; r < batch.Length; r++)
            {
                WriteRow(r);
                if (_xml.Length >= FlushSize && (_headStarted || !Measuring)) await FlushAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }

        void CheckSchema(RecordBatch batch)
        {
            var fields = batch.Schema.FieldsList;
            if (fields.Count != _encoders.Length)
                throw new ArgumentException($"The batch has {fields.Count} columns; the writer's schema has {_encoders.Length}.", nameof(batch));
            for (int i = 0; i < fields.Count; i++)
            {
                if (!SameType(fields[i].DataType, Schema.FieldsList[i].DataType))
                    throw new ArgumentException($"Column '{fields[i].Name}' is {fields[i].DataType.Name}; the writer's schema says {Schema.FieldsList[i].DataType.Name}.", nameof(batch));
            }

            static bool SameType(Apache.Arrow.Types.IArrowType a, Apache.Arrow.Types.IArrowType b) => (a, b) switch
            {
                (Apache.Arrow.Types.TimestampType x, Apache.Arrow.Types.TimestampType y) => x.Unit == y.Unit && x.Timezone == y.Timezone,
                (Apache.Arrow.Types.DictionaryType x, Apache.Arrow.Types.DictionaryType y) => x.IndexType.TypeId == y.IndexType.TypeId && x.ValueType.TypeId == y.ValueType.TypeId,
                _ => a.TypeId == b.TypeId,
            };
        }
    }

    private bool Measuring => _options.Autofit && _dataRows < _options.AutofitRows;

    private void WriteRow(int index)
    {
        BeginRow();
        bool measure = Measuring;
        for (int c = 0; c < _encoders.Length; c++)
        {
            SetColumn(c);
            int width = _encoders[c].Encode(index, _sink);
            if (measure) Measure(c, width);
        }
        _sink.EndRow();
        _dataRows++;
    }

    private void BeginRow() => _sink.BeginRow(++_rowsWritten);

    private void SetColumn(int c) => _sink.SetColumn(c, Schema.FieldsList[c].Name);

    private void Measure(int column, int chars)
    {
        if (chars > _widths[column]) _widths[column] = chars;
    }

    // The worksheet start: views and column widths (known once the measured rows are in), then <sheetData>, then
    // the rows buffered so far.
    private async ValueTask FlushAsync(bool useAsync, CancellationToken ct)
    {
        if (!_headStarted)
        {
            _headStarted = true;
            _sheetStream = _zip.CreateEntry(XlsxParts.SheetPart, _options.CompressionLevel).Open();
            using var head = new XmlOut();
            head.Raw(XlsxParts.WorksheetStart);
            if (_options.FreezeHeader && _options.IncludeHeader) head.Raw(XlsxParts.FrozenHeaderView);
            if (_options.Autofit && _encoders.Length > 0)
            {
                head.Raw("<cols>"u8);
                for (int c = 0; c < _encoders.Length; c++)
                {
                    head.Raw("<col min=\""u8);
                    head.Int(c + 1);
                    head.Raw("\" max=\""u8);
                    head.Int(c + 1);
                    head.Raw("\" width=\""u8);
                    head.Double(Math.Round(Math.Min(255, Math.Max(_widths[c], 4) * 1.1 + 2), 2));
                    head.Raw("\" customWidth=\"1\"/>"u8);
                }
                head.Raw("</cols>"u8);
            }
            head.Raw("<sheetData>"u8);
            await WriteToAsync(_sheetStream, head, useAsync, ct).ConfigureAwait(false);
        }
        if (_xml.Length > 0)
        {
            await WriteToAsync(_sheetStream!, _xml, useAsync, ct).ConfigureAwait(false);
            _xml.Clear();
        }
    }

    private static async ValueTask WriteToAsync(Stream stream, XmlOut xml, bool useAsync, CancellationToken ct)
    {
        if (useAsync) await stream.WriteAsync(xml.WrittenMemory, ct).ConfigureAwait(false);
        else stream.Write(xml.Written);
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) return;
        if (_faulted) throw new InvalidOperationException("An earlier write failed; the workbook is incomplete.");
        try
        {
            await FlushAsync(useAsync, ct).ConfigureAwait(false);
            _xml.Raw("</sheetData></worksheet>"u8);
            await WriteToAsync(_sheetStream!, _xml, useAsync, ct).ConfigureAwait(false);
            _xml.Clear();
            if (useAsync) await _sheetStream!.DisposeAsync().ConfigureAwait(false);
            else _sheetStream!.Dispose();

            bool shared = _sharedStrings is { Count: > 0 };
            if (shared) await WritePartAsync(XlsxParts.SharedStringsPart, WriteSharedStrings, useAsync, ct).ConfigureAwait(false);
            await WritePartAsync(XlsxParts.StylesPart, _styles.Write, useAsync, ct).ConfigureAwait(false);
            await WritePartAsync(XlsxParts.WorkbookPart, x => XlsxParts.Workbook(x, _options.SheetName), useAsync, ct).ConfigureAwait(false);
            await WritePartAsync(XlsxParts.WorkbookRelsPart, x => XlsxParts.WorkbookRels(x, shared), useAsync, ct).ConfigureAwait(false);
            await WritePartAsync(XlsxParts.RootRelsPart, static x => x.Raw(XlsxParts.RootRels), useAsync, ct).ConfigureAwait(false);
            await WritePartAsync(XlsxParts.ContentTypesPart, x => XlsxParts.ContentTypes(x, shared), useAsync, ct).ConfigureAwait(false);
            _zip.Dispose(); // the central directory (synchronous: ZipArchive has no asynchronous disposal on net8.0)
            if (useAsync) await _stream.FlushAsync(ct).ConfigureAwait(false);
            else _stream.Flush();
            _completed = true;
        }
        catch
        {
            _faulted = true;
            throw;
        }

        void WriteSharedStrings(XmlOut x)
        {
            var table = _sharedStrings!;
            x.Raw(XlsxParts.XmlDeclaration);
            x.Raw("\n<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" count=\""u8);
            x.Int(table.References);
            x.Raw("\" uniqueCount=\""u8);
            x.Int(table.Count);
            x.Raw("\">"u8);
            for (int i = 0; i < table.Count; i++)
            {
                var s = table.Get(i);
                x.Raw(XmlOut.NeedsPreserve(s) ? "<si><t xml:space=\"preserve\">"u8 : "<si><t>"u8);
                x.Text(s);
                x.Raw("</t></si>"u8);
            }
            x.Raw("</sst>"u8);
        }
    }

    private async ValueTask WritePartAsync(string name, Action<XmlOut> write, bool useAsync, CancellationToken ct)
    {
        using var x = new XmlOut();
        write(x);
        var entry = _zip.CreateEntry(name, _options.CompressionLevel).Open();
        await using (entry.ConfigureAwait(false))
        {
            await WriteToAsync(entry, x, useAsync, ct).ConfigureAwait(false);
        }
    }

    /// <summary>Disposes the writer; an uncompleted workbook is left incomplete (not a valid file).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // An uncompleted workbook gets no zip directory (the path overloads delete the partial file).
        _xml.Dispose();
        _sharedStrings?.Dispose();
        if (!_options.LeaveOpen) _stream.Dispose();
    }

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
