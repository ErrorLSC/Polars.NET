using System.Buffers;
using System.Globalization;
using System.Text;
using Kaminari.Seseragi.Files.Excel.Packaging;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Xlsx;

/// <summary>
/// Streams the rows of a worksheet part. The part is decompressed into a native buffer; each <c>&lt;row&gt;</c>
/// element is parsed once it is buffered whole, so a refill (synchronous or asynchronous) only happens between rows.
/// Rows and cells without an <c>r</c> attribute take the position after the previous one.
/// </summary>
internal sealed class XlsxSheetSource : SheetSource
{
    // Bytes that cannot sit between '<' and "sheetData" in a sheetData tag (only a namespace prefix may).
    private static readonly SearchValues<byte> NotInPrefix = SearchValues.Create(" \t\r\n/>\""u8);

    private readonly Stream _stream;
    private readonly ChunkReader _reader;
    private readonly ExcelValueKind[] _styles;
    private readonly SharedStringStore? _sharedStrings;
    private byte[] _rowEnd = "</row>"u8.ToArray();
    private int _nextRow;
    private bool _done;

    private XlsxSheetSource(Stream stream, ExcelValueKind[] styles, SharedStringStore? sharedStrings)
    {
        _stream = stream;
        _reader = new ChunkReader(stream);
        _styles = styles;
        _sharedStrings = sharedStrings;
    }

    public static async ValueTask<XlsxSheetSource> OpenAsync(Stream stream, ExcelValueKind[] styles,
        SharedStringStore? sharedStrings, bool useAsync, CancellationToken cancellationToken)
    {
        var source = new XlsxSheetSource(stream, styles, sharedStrings);
        try
        {
            while (!source.TryReadHead())
            {
                bool more = useAsync ? await source._reader.FillAsync(cancellationToken).ConfigureAwait(false) : source._reader.Fill();
                if (!more && !source.TryReadHead()) throw new ExcelFormatException("The worksheet has no sheetData element.");
                if (!more) break;
            }
            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    // Finds <sheetData> in the buffer; reads <dimension> before it. False when more input is needed.
    private bool TryReadHead()
    {
        var buf = _reader.Buffered;
        int at = 0;
        while (true)
        {
            int i = buf[at..].IndexOf("sheetData"u8);
            if (i < 0) return false;
            i += at;
            // Preceded by '<' or "<prefix:".
            int lt = buf[..i].LastIndexOf((byte)'<');
            if (lt < 0 || buf[(lt + 1)..i].IndexOfAny(NotInPrefix) >= 0)
            {
                at = i + 9;
                continue;
            }
            int gt = buf[i..].IndexOf((byte)'>');
            if (gt < 0) return false;
            gt += i;
            ReadDimension(buf[..lt]);
            var prefix = buf[(lt + 1)..i];
            _rowEnd = [.. "</"u8, .. prefix, .. "row>"u8];
            _done = buf[gt - 1] == (byte)'/';
            _reader.Consume(gt + 1);
            return true;
        }

        void ReadDimension(ReadOnlySpan<byte> head)
        {
            var x = new XmlSpanScanner(head);
            while (x.Read())
            {
                if (x.Token != XmlToken.StartElement || !x.Is("dimension"u8)) continue;
                if (x.TryGetAttribute("ref"u8, out var r) && ExcelCellReference.TryParseRange(Encoding.ASCII.GetString(r), out var first, out var last))
                    Dimension = (first.Row, first.Column, last.Row, last.Column);
                return;
            }
        }
    }


    private enum Step : byte { Row, NeedMore, End }

    public override bool ReadRow(RowBuffer row)
    {
        while (true)
        {
            switch (TryReadRow(row))
            {
                case Step.Row: return true;
                case Step.End: return false;
            }
            if (!_reader.Fill() && TryReadRow(row) is var last)
                return last == Step.Row || (last == Step.End ? false : throw Truncated());
        }
    }

    public override async ValueTask<bool> ReadRowAsync(RowBuffer row, CancellationToken cancellationToken)
    {
        while (true)
        {
            switch (TryReadRow(row))
            {
                case Step.Row: return true;
                case Step.End: return false;
            }
            if (!await _reader.FillAsync(cancellationToken).ConfigureAwait(false) && TryReadRow(row) is var last)
                return last == Step.Row || (last == Step.End ? false : throw Truncated());
        }
    }

    private static ExcelFormatException Truncated() => new("The worksheet is truncated.");

    // Parses the next buffered row that has cells.
    private Step TryReadRow(RowBuffer row)
    {
        if (_done) return Step.End;
        while (true)
        {
            var buf = _reader.Buffered;
            int lt = buf.IndexOf((byte)'<');
            if (lt < 0)
            {
                _reader.Consume(buf.Length);
                return _reader.EndOfStream ? End() : Step.NeedMore;
            }
            var rest = buf[lt..];
            if (rest.Length < 2) return _reader.EndOfStream ? End() : Step.NeedMore;
            if (rest[1] == (byte)'/') return End(); // </sheetData>
            int gt = rest.IndexOf((byte)'>');
            if (gt < 0) return _reader.EndOfStream ? throw Truncated() : Step.NeedMore;
            if (!XlsxSharedStrings.IsTag(rest, "row"u8, out _))
            {
                _reader.Consume(lt + gt + 1); // a comment or an unexpected element start: skip the tag
                continue;
            }
            if (rest[gt - 1] == (byte)'/')
            {
                // <row r="…"/>: no cells.
                var x = new XmlSpanScanner(rest[..(gt + 1)]);
                x.Read();
                RowNumber(ref x);
                _reader.Consume(lt + gt + 1);
                continue;
            }
            int end = rest.IndexOf(_rowEnd);
            if (end < 0) return _reader.EndOfStream ? throw Truncated() : Step.NeedMore;
            end += _rowEnd.Length;
            ParseRow(rest[..end], row);
            _reader.Consume(lt + end);
            if (row.Count > 0) return Step.Row;
        }

        Step End()
        {
            _done = true;
            return Step.End;
        }
    }

    private int RowNumber(ref XmlSpanScanner x)
    {
        int row = _nextRow;
        if (x.TryGetAttribute("r"u8, out var r))
        {
            if (!XmlText.TryParseInt(r, out int n) || n < 1 || n > ExcelCellReference.MaxRows)
                throw new ExcelFormatException($"Invalid row number '{Encoding.UTF8.GetString(r)}'.");
            row = n - 1;
        }
        _nextRow = row + 1;
        return row;
    }

    private void ParseRow(ReadOnlySpan<byte> xml, RowBuffer row)
    {
        row.Clear();
        row.SharedStrings = _sharedStrings;
        var x = new XmlSpanScanner(xml);
        x.Read(); // <row>
        row.Row = RowNumber(ref x);
        int nextColumn = 0;
        while (x.Read())
        {
            if (x.Token != XmlToken.StartElement) continue;
            if (!x.Is("c"u8))
            {
                x.SkipElement();
                continue;
            }
            // r, s and t in one walk over the attributes.
            int column = nextColumn;
            var kind = ExcelValueKind.Number;
            ReadOnlySpan<byte> type = "n"u8;
            foreach (var a in x.EnumerateAttributes())
            {
                if (a.LocalName.Length != 1) continue;
                switch (a.LocalName[0])
                {
                    case (byte)'r':
                        if (!ExcelCellReference.TryParse(a.Value, out _, out column) || column >= ExcelCellReference.MaxColumns)
                            throw new ExcelFormatException($"Invalid cell reference '{Encoding.UTF8.GetString(a.Value)}'.");
                        break;
                    case (byte)'s':
                        if (XmlText.TryParseInt(a.Value, out int style) && (uint)style < (uint)_styles.Length) kind = _styles[style];
                        break;
                    case (byte)'t':
                        type = a.Value;
                        break;
                }
            }
            nextColumn = column + 1;
            if (x.IsEmptyElement) continue;
            ParseCell(ref x, row, column, kind, type, _sharedStrings);
        }
    }

    // Text without characters (a formula's "", an empty shared string) is an empty cell, as in Python Polars.
    private static void ParseCell(ref XmlSpanScanner x, RowBuffer row, int column, ExcelValueKind kind, ReadOnlySpan<byte> type,
        SharedStringStore? sharedStrings)
    {
        ReadOnlySpan<byte> value = default;
        bool hasValue = false, valueCData = false;
        int textStart = row.TextLength;
        int inlineLength = -1;
        while (x.Read())
        {
            if (x.Token == XmlToken.EndElement)
            {
                if (x.Is("c"u8)) break;
                continue;
            }
            if (x.Token != XmlToken.StartElement) continue;
            if (x.Is("v"u8))
            {
                hasValue = true;
                if (x.IsEmptyElement) continue;
                // The value: one text token (or none for <v></v>), then </v>.
                if (x.Read() && x.Token == XmlToken.Text)
                {
                    value = x.Text;
                    valueCData = x.IsCData;
                    x.Read();
                }
            }
            else if (x.Is("is"u8))
            {
                inlineLength = ReadInline(ref x, row);
            }
            else
            {
                x.SkipElement(); // <f>, <extLst>
            }
        }

        ref Cell cell = ref row.Add();
        cell.Column = column;
        switch (type.Length == 1 ? type[0] : type.SequenceEqual("str"u8) ? (byte)'f' : type.SequenceEqual("inlineStr"u8) ? (byte)'i' : (byte)'?')
        {
            case (byte)'n':
                if (!hasValue || value.IsEmpty || !XmlText.TryParseNumber(value, out double number))
                {
                    if (hasValue && !value.IsEmpty) throw new ExcelFormatException($"Invalid number '{Encoding.UTF8.GetString(value)}' in a cell.");
                    row.RemoveLast();
                    return;
                }
                cell.Type = CellType.Number;
                cell.Number = number;
                cell.Kind = kind;
                return;
            case (byte)'s':
                if (!hasValue || !XmlText.TryParseInt(value, out int index)
                    || (sharedStrings is not null && (uint)index < (uint)sharedStrings.Count && sharedStrings.Get(index).IsEmpty))
                {
                    row.RemoveLast();
                    return;
                }
                cell.Type = CellType.SharedString;
                cell.Offset = index;
                return;
            case (byte)'f': // formula string: the cached result in <v>
            {
                if (!hasValue)
                {
                    row.RemoveLast();
                    return;
                }
                int n = row.AppendXmlText(value, valueCData);
                n = XmlText.DecodeEscapes(row.TextRange(textStart, n));
                row.TruncateText(textStart + n);
                if (n == 0)
                {
                    row.RemoveLast();
                    return;
                }
                cell.Type = CellType.String;
                cell.Offset = textStart;
                cell.Length = n;
                return;
            }
            case (byte)'i':
                if (inlineLength <= 0)
                {
                    row.RemoveLast();
                    return;
                }
                cell.Type = CellType.String;
                cell.Offset = textStart;
                cell.Length = inlineLength;
                return;
            case (byte)'b':
                if (!hasValue)
                {
                    row.RemoveLast();
                    return;
                }
                cell.Type = CellType.Boolean;
                cell.Number = XmlText.ParseBoolean(value) ? 1 : 0;
                return;
            case (byte)'e':
                cell.Type = CellType.Error;
                return;
            case (byte)'d':
                if (!hasValue || !TryParseIso(XmlText.DecodeString(value, false), out cell.Micros, out cell.Kind))
                {
                    if (hasValue && !value.IsEmpty) throw new ExcelFormatException($"Invalid ISO 8601 date '{Encoding.UTF8.GetString(value)}' in a cell.");
                    row.RemoveLast();
                    return;
                }
                cell.Type = CellType.Temporal;
                return;
            default:
                throw new ExcelFormatException($"Unknown cell type '{Encoding.UTF8.GetString(type)}'.");
        }
    }

    // <is>: text of its <t> elements outside <rPh>, appended to the row text; returns the decoded length.
    private static int ReadInline(ref XmlSpanScanner x, RowBuffer row)
    {
        int start = row.TextLength;
        if (x.IsEmptyElement) return 0;
        int phonetic = 0;
        bool inText = false;
        while (x.Read())
        {
            switch (x.Token)
            {
                case XmlToken.EndElement when x.Is("is"u8):
                    int n = XmlText.DecodeEscapes(row.TextRange(start, row.TextLength - start));
                    row.TruncateText(start + n);
                    return n;
                case XmlToken.StartElement when x.Is("rPh"u8):
                    if (!x.IsEmptyElement) phonetic++;
                    break;
                case XmlToken.EndElement when x.Is("rPh"u8):
                    phonetic--;
                    break;
                case XmlToken.StartElement when x.Is("t"u8):
                    inText = !x.IsEmptyElement && phonetic == 0;
                    break;
                case XmlToken.EndElement when x.Is("t"u8):
                    inText = false;
                    break;
                case XmlToken.Text when inText:
                    row.AppendXmlText(x.Text, x.IsCData);
                    break;
            }
        }
        throw new ExcelFormatException("An inline string is not closed.");
    }

    // t="d" cells: ISO 8601 date, date-time or time (the wall-clock value; an offset is ignored).
    private static bool TryParseIso(string text, out long micros, out ExcelValueKind kind)
    {
        text = text.Trim();
        micros = 0;
        kind = ExcelValueKind.DateTime;
        if (text.Length == 0) return false;
        bool timeOnly = text[0] == 'T' || (text.Contains(':') && !text.Contains('-'));
        if (timeOnly)
        {
            if (!TimeSpan.TryParse(text.TrimStart('T'), CultureInfo.InvariantCulture, out var ts) || ts < TimeSpan.Zero || ts.Days > 0) return false;
            micros = ts.Ticks / 10;
            kind = ExcelValueKind.Time;
            return true;
        }
        if (!DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto)) return false;
        var local = dto.DateTime; // the clock time as written
        micros = (local.Ticks - DateTime.UnixEpoch.Ticks) / 10;
        kind = text.Contains('T') || text.Contains(' ') ? ExcelValueKind.DateTime : ExcelValueKind.Date;
        return true;
    }

    public override void Dispose()
    {
        _reader.Dispose();
        _stream.Dispose();
    }
}
