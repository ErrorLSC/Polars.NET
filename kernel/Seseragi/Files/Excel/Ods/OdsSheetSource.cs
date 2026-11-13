using System.Text;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Ods;

/// <summary>
/// Streams the rows of one <c>table:table</c> of an OpenDocument spreadsheet, from its start tag to its end tag.
/// Markup is parsed as it is buffered (up to the last <c>&lt;</c>), element by element, so rows of any size stream.
/// <list type="bullet">
/// <item>Rows (<c>table:table-row</c>, also inside row groups and header rows) and cells (<c>table:table-cell</c>,
/// <c>table:covered-table-cell</c>) repeat by <c>table:number-rows-repeated</c> and
/// <c>table:number-columns-repeated</c>: empty ones only move the position (LibreOffice pads sheets with them),
/// others are emitted that many times; past Excel's 1,048,576 rows or 16,384 columns they are refused.</item>
/// <item>Values come from the value attributes by <c>office:value-type</c>: float, percentage and currency numbers,
/// dates and date-times, times (a time of day, or elapsed time when the cell's data style does not wrap at 24
/// hours or the value lies outside a day), booleans, and strings (<c>office:string-value</c>, else the cell's
/// paragraphs). A LibreOffice error result (<c>calcext:value-type="error"</c>) is an error cell; a cell without a
/// value type is empty. Dates with a time of day other than midnight make a column of date-times.</item>
/// <item>A string's paragraphs (<c>text:p</c>, <c>text:h</c>) are joined by line feeds. Their text is kept as
/// written, as LibreOffice reads it (not collapsed as [ODF 1.3] part 3, 6.1.2 asks: LibreOffice writes a single
/// trailing space as a plain one and keeps it on reading), with <c>text:s</c>, <c>text:tab</c> and
/// <c>text:line-break</c> as spaces, tabs and line feeds. Spans, links, fields and the like give their text;
/// annotations, notes, ruby annotations and drawing objects in a paragraph are left out with their text, as are
/// annotations, lists, drawings and nested tables in a cell.</item>
/// </list>
/// </summary>
internal sealed class OdsSheetSource : SheetSource
{
    private readonly Stream _stream;
    private readonly ChunkReader _reader;
    private readonly string _sheet;
    private readonly OdsStyles _styles;
    private readonly List<(long First, long Last, string Style)> _columnStyles = [];
    private long _nextColumnDefinition;

    private int _depth;        // open elements within the table (the table itself is 1)
    private bool _done;
    private long _nextRow;     // the row the next row element starts at
    private int _rowDepth = -1; // depth inside the current row element (-1: no row open)
    private long _rowRepeat;
    private string? _rowStyle;
    private long _column;

    // The current cell.
    private int _cellDepth = -1;
    private long _cellRepeat;
    private CellType _cellType;
    private ExcelValueKind _cellKind;
    private double _cellNumber;
    private long _cellMicros;
    private bool _cellText;     // the value is the cell's paragraph text
    private int _skipDepth = -1; // an element being left out with its content
    private int _paragraphDepth = -1;
    private int _paragraphs;
    private byte[] _text = new byte[256];
    private int _textLength;

    // A repeated row still to hand out.
    private RowBuffer? _repeat;
    private long _repeatLeft;

    private OdsSheetSource(Stream stream, ChunkReader reader, string sheet, OdsStyles styles)
    {
        _stream = stream;
        _reader = reader;
        _sheet = sheet;
        _styles = styles;
    }

    /// <summary>Takes over a stream and its buffer, which starts at the sheet's start tag.</summary>
    public static OdsSheetSource Continue(Stream stream, ChunkReader reader, string sheet, OdsStyles styles) => new(stream, reader, sheet, styles);

    /// <summary>Opens the sheet whose start tag the stream is positioned at.</summary>
    public static async ValueTask<OdsSheetSource> OpenAsync(Stream stream, string sheet, OdsStyles styles, bool useAsync, CancellationToken ct)
    {
        var source = new OdsSheetSource(stream, new ChunkReader(stream, xml: false), sheet, styles);
        try
        {
            bool more = useAsync ? await source._reader.FillAsync(ct).ConfigureAwait(false) : source._reader.Fill();
            var b = source._reader.Buffered;
            if (!more || b.Length < 2 || b[0] != (byte)'<') throw new ExcelFormatException($"Sheet '{sheet}' is not where the document's index places it.");
            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    private enum Step : byte { Row, NeedMore, End }

    public override bool ReadRow(RowBuffer row)
    {
        while (true)
        {
            var step = TryReadRow(row);
            if (step != Step.NeedMore) return step == Step.Row;
            if (!_reader.Fill() && TryReadRow(row) is var last)
            {
                if (last == Step.NeedMore) throw Truncated();
                return last == Step.Row;
            }
        }
    }

    public override async ValueTask<bool> ReadRowAsync(RowBuffer row, CancellationToken cancellationToken)
    {
        while (true)
        {
            var step = TryReadRow(row);
            if (step != Step.NeedMore) return step == Step.Row;
            if (!await _reader.FillAsync(cancellationToken).ConfigureAwait(false) && TryReadRow(row) is var last)
            {
                if (last == Step.NeedMore) throw Truncated();
                return last == Step.Row;
            }
        }
    }

    private ExcelFormatException Truncated() => new($"Sheet '{_sheet}' is truncated (the document ends inside it).");

    private Step TryReadRow(RowBuffer row)
    {
        if (_repeatLeft > 0)
        {
            row.CopyFrom(_repeat!);
            row.Row = (int)(_nextRow - _repeatLeft);
            _repeatLeft--;
            return Step.Row;
        }
        if (_done) return Step.End;
        var buf = _reader.Buffered;
        // Whole markup only, also at the end: the table ends before the document's last tag.
        int end = buf.LastIndexOf((byte)'<');
        if (end <= 0) return Step.NeedMore;
        var x = new XmlSpanScanner(buf[..end]);
        try
        {
            while (x.Read())
            {
                switch (x.Token)
                {
                    case XmlToken.StartElement:
                        var started = Start(ref x, row);
                        if (started != Step.NeedMore) return started;
                        break;
                    case XmlToken.EndElement:
                        var ended = EndElement(ref x, row);
                        if (ended != Step.NeedMore) return ended;
                        break;
                    case XmlToken.Text when _paragraphDepth >= 0 && _skipDepth < 0:
                        AppendText(x.Text, x.IsCData);
                        break;
                }
            }
            return Step.NeedMore;
        }
        finally
        {
            _reader.Consume(x.Token == XmlToken.EndOfInput ? end : x.Position);
        }
    }

    // Each branch moves the depth itself: an element that is not empty opens one level.
    private Step Start(ref XmlSpanScanner x, RowBuffer row)
    {
        bool empty = x.IsEmptyElement;
        if (_depth == 0)
        {
            if (!x.Is("table"u8)) throw new ExcelFormatException($"Sheet '{_sheet}' is not where the document's index places it.");
            if (empty)
            {
                _done = true;
                return Step.End;
            }
            _depth = 1;
            return Step.NeedMore;
        }
        if (_skipDepth >= 0)
        {
            if (!empty) _depth++;
        }
        else if (_cellDepth >= 0)
        {
            CellContent(ref x);
        }
        else if (_rowDepth >= 0)
        {
            if (x.Is("table-cell"u8) || x.Is("covered-table-cell"u8)) StartCell(ref x, row);
            else Skip(empty);
        }
        else if (x.Is("table-row"u8))
        {
            _rowRepeat = Repeat(ref x, "number-rows-repeated"u8);
            _rowStyle = x.TryGetAttribute("default-cell-style-name"u8, out var s) ? XmlText.DecodeString(s, false) : null;
            row.Clear();
            _column = 0;
            if (empty)
            {
                _nextRow += _rowRepeat;
            }
            else
            {
                _rowDepth = _depth;
                _depth++;
            }
        }
        else if (x.Is("table-column"u8))
        {
            long repeat = Repeat(ref x, "number-columns-repeated"u8);
            if (x.TryGetAttribute("default-cell-style-name"u8, out var s))
                _columnStyles.Add((_nextColumnDefinition, _nextColumnDefinition + repeat - 1, XmlText.DecodeString(s, false)));
            _nextColumnDefinition += repeat;
            Skip(empty);
        }
        else if (x.Is("table-row-group"u8) || x.Is("table-header-rows"u8) || x.Is("table-rows"u8)
            || x.Is("table-column-group"u8) || x.Is("table-header-columns"u8) || x.Is("table-columns"u8))
        {
            if (!empty) _depth++; // groups of rows or columns: their rows and columns count
        }
        else
        {
            Skip(empty); // shapes, forms, named expressions, …
        }
        return Step.NeedMore;
    }

    // Leaves the current element out with its content.
    private void Skip(bool empty)
    {
        if (empty) return;
        _skipDepth = _depth;
        _depth++;
    }

    private Step EndElement(ref XmlSpanScanner x, RowBuffer row)
    {
        _depth--;
        if (_skipDepth >= 0)
        {
            if (_depth == _skipDepth) _skipDepth = -1;
            return Step.NeedMore;
        }
        if (_depth == 0)
        {
            _done = true;
            return Step.End;
        }
        if (_paragraphDepth >= 0 && _depth == _paragraphDepth)
        {
            _paragraphDepth = -1;
            return Step.NeedMore;
        }
        if (_cellDepth >= 0 && _depth == _cellDepth)
        {
            EndCell(row);
            return Step.NeedMore;
        }
        if (_rowDepth >= 0 && _depth == _rowDepth)
        {
            _rowDepth = -1;
            return EndRow(row);
        }
        return Step.NeedMore;
    }

    private Step EndRow(RowBuffer row)
    {
        long first = _nextRow, repeat = _rowRepeat;
        _nextRow += repeat;
        if (row.Count == 0) return Step.NeedMore;
        if (first + repeat > ExcelCellReference.MaxRows)
            throw new ExcelFormatException($"Sheet '{_sheet}' has values past row {ExcelCellReference.MaxRows}, the most a sheet holds.");
        row.Row = (int)first;
        row.SharedStrings = null;
        if (repeat > 1)
        {
            (_repeat ??= new RowBuffer()).CopyFrom(row);
            _repeatLeft = repeat - 1;
        }
        return Step.Row;
    }

    private long Repeat(ref XmlSpanScanner x, ReadOnlySpan<byte> attribute)
    {
        if (!x.TryGetAttribute(attribute, out var raw)) return 1;
        if (!long.TryParse(XmlText.Trim(raw), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out long n) || n < 1)
            throw new ExcelFormatException($"Sheet '{_sheet}' has an invalid {Encoding.ASCII.GetString(attribute)} value '{Encoding.UTF8.GetString(raw)}'.");
        return n;
    }

    private string Address(long column) =>
        ExcelCellReference.Format((int)Math.Min(_nextRow, ExcelCellReference.MaxRows - 1), (int)Math.Min(column, ExcelCellReference.MaxColumns - 1));

    private void StartCell(ref XmlSpanScanner x, RowBuffer row)
    {
        _cellRepeat = Repeat(ref x, "number-columns-repeated"u8);
        _cellType = CellType.Empty;
        _cellKind = ExcelValueKind.Number;
        _cellText = false;
        _textLength = 0;
        _paragraphs = 0;
        ReadOnlySpan<byte> type = default, value = default, date = default, time = default, boolean = default, text = default, style = default;
        bool error = false, hasText = false;
        foreach (var a in x.EnumerateAttributes())
        {
            var name = a.LocalName;
            if (name.SequenceEqual("value-type"u8))
            {
                // office:value-type, and LibreOffice's calcext:value-type, which says "error" for an error result.
                if (a.Value.SequenceEqual("error"u8)) error = true;
                else if (type.IsEmpty) type = a.Value;
            }
            else if (name.SequenceEqual("value"u8)) value = a.Value;
            else if (name.SequenceEqual("date-value"u8)) date = a.Value;
            else if (name.SequenceEqual("time-value"u8)) time = a.Value;
            else if (name.SequenceEqual("boolean-value"u8)) boolean = a.Value;
            else if (name.SequenceEqual("string-value"u8))
            {
                text = a.Value;
                hasText = true;
            }
            else if (name.SequenceEqual("style-name"u8)) style = a.Value;
        }
        if (error)
        {
            _cellType = CellType.Error;
        }
        else if (type.SequenceEqual("float"u8) || type.SequenceEqual("percentage"u8) || type.SequenceEqual("currency"u8))
        {
            if (!XmlText.TryParseDouble(value, out _cellNumber)) throw Bad("number", value);
            _cellType = CellType.Number;
        }
        else if (type.SequenceEqual("date"u8))
        {
            // A date, with a time of day or not (LibreOffice writes midnight as T00:00:00): as for a date format in
            // xlsx, a column is a date column when no value has a time.
            if (!OdsValues.TryParseDate(date, out _cellMicros, out _)) throw Bad("date", date);
            _cellType = CellType.Temporal;
            _cellKind = ExcelValueKind.Date;
        }
        else if (type.SequenceEqual("time"u8))
        {
            if (!OdsValues.TryParseDuration(time, out _cellMicros)) throw Bad("time", time);
            _cellType = CellType.Temporal;
            string? styleName = !style.IsEmpty ? XmlText.DecodeString(style, false) : _rowStyle ?? ColumnStyle(_column);
            _cellKind = _styles.IsElapsed(styleName) || _cellMicros is < 0 or >= 86_400_000_000L ? ExcelValueKind.Duration : ExcelValueKind.Time;
        }
        else if (type.SequenceEqual("boolean"u8))
        {
            _cellType = CellType.Boolean;
            _cellNumber = XmlText.ParseBoolean(boolean) ? 1 : 0;
        }
        else if (type.SequenceEqual("string"u8))
        {
            if (hasText)
            {
                EnsureText(text.Length);
                _textLength = XmlText.DecodeEntities(text, _text);
                _cellType = CellType.String;
            }
            else
            {
                _cellText = true;
            }
        }
        if (x.IsEmptyElement) EndCell(row);
        else
        {
            _cellDepth = _depth;
            _depth++;
        }

        ExcelFormatException Bad(string what, ReadOnlySpan<byte> raw) =>
            new($"Sheet '{_sheet}' has an invalid {what} value '{Encoding.UTF8.GetString(raw)}' in cell {Address(_column)}.");
    }

    private string? ColumnStyle(long column)
    {
        foreach (var (first, last, style) in _columnStyles)
            if (column >= first && column <= last) return style;
        return null;
    }

    // An element inside a cell: paragraphs (when the cell's text is its value) and what they hold.
    private void CellContent(ref XmlSpanScanner x)
    {
        bool empty = x.IsEmptyElement;
        if (_paragraphDepth < 0)
        {
            if (_cellText && (x.Is("p"u8) || x.Is("h"u8)))
            {
                if (_paragraphs++ > 0) AppendByte((byte)'\n');
                if (!empty)
                {
                    _paragraphDepth = _depth;
                    _depth++;
                }
                return;
            }
            Skip(empty); // annotations, lists, frames, nested tables, …
            return;
        }
        if (x.Is("s"u8))
        {
            int count = x.TryGetAttribute("c"u8, out var c) && XmlText.TryParseInt(c, out int n) && n > 0 ? n : 1;
            Append((byte)' ', count);
            Skip(empty);
        }
        else if (x.Is("tab"u8))
        {
            Append((byte)'\t', 1);
            Skip(empty);
        }
        else if (x.Is("line-break"u8))
        {
            Append((byte)'\n', 1);
            Skip(empty);
        }
        else if (LeftOut(x.LocalName))
        {
            Skip(empty);
        }
        else if (!empty)
        {
            _depth++; // spans, links, fields, …: their text counts
        }
    }

    // Elements of a paragraph whose text is not the cell's: annotations, notes, ruby annotations, drawing objects.
    private static bool LeftOut(ReadOnlySpan<byte> name) =>
        name.SequenceEqual("annotation"u8) || name.SequenceEqual("note"u8) || name.SequenceEqual("ruby-text"u8)
        || name.SequenceEqual("frame"u8) || name.SequenceEqual("g"u8) || name.SequenceEqual("custom-shape"u8)
        || name.SequenceEqual("rect"u8) || name.SequenceEqual("line"u8) || name.SequenceEqual("polyline"u8)
        || name.SequenceEqual("polygon"u8) || name.SequenceEqual("regular-polygon"u8) || name.SequenceEqual("path"u8)
        || name.SequenceEqual("circle"u8) || name.SequenceEqual("ellipse"u8) || name.SequenceEqual("connector"u8)
        || name.SequenceEqual("control"u8) || name.SequenceEqual("caption"u8) || name.SequenceEqual("measure"u8)
        || name.SequenceEqual("page-thumbnail"u8) || name.SequenceEqual("scene"u8);

    private void Append(byte b, int count)
    {
        EnsureText(count);
        _text.AsSpan(_textLength, count).Fill(b);
        _textLength += count;
    }

    // Character data of a paragraph, entities decoded.
    private void AppendText(ReadOnlySpan<byte> raw, bool cdata)
    {
        if (raw.IsEmpty) return;
        EnsureText(raw.Length);
        if (cdata)
        {
            raw.CopyTo(_text.AsSpan(_textLength));
            _textLength += raw.Length;
        }
        else
        {
            _textLength += XmlText.DecodeEntities(raw, _text.AsSpan(_textLength));
        }
    }

    private void AppendByte(byte b)
    {
        EnsureText(1);
        _text[_textLength++] = b;
    }

    private void EnsureText(int more)
    {
        if (_textLength + more > _text.Length) Array.Resize(ref _text, Math.Max(_text.Length * 2, _textLength + more));
    }

    private void EndCell(RowBuffer row)
    {
        _cellDepth = -1;
        _paragraphDepth = -1;
        if (_cellText) _cellType = CellType.String;
        bool hasValue = _cellType != CellType.Empty && !(_cellType == CellType.String && _textLength == 0); // empty text: an empty cell
        long first = _column;
        _column += _cellRepeat;
        if (!hasValue) return;
        if (_column > ExcelCellReference.MaxColumns)
            throw new ExcelFormatException($"Sheet '{_sheet}' has values past column {ExcelCellReference.MaxColumns} in row {_nextRow + 1}, the most a sheet holds.");
        if (_nextRow >= ExcelCellReference.MaxRows)
            throw new ExcelFormatException($"Sheet '{_sheet}' has values past row {ExcelCellReference.MaxRows}, the most a sheet holds.");
        int offset = 0, length = 0;
        if (_cellType == CellType.String)
        {
            var into = row.ReserveText(_textLength);
            _text.AsSpan(0, _textLength).CopyTo(into);
            offset = row.CommitText(_textLength);
            length = _textLength;
        }
        for (long c = first; c < _column; c++)
        {
            ref Cell cell = ref row.Add();
            cell.Column = (int)c;
            cell.Type = _cellType;
            cell.Kind = _cellKind;
            cell.Number = _cellNumber;
            cell.Micros = _cellMicros;
            cell.Offset = offset;
            cell.Length = length;
        }
    }

    public override void Dispose()
    {
        _reader.Dispose();
        _stream.Dispose();
    }
}
