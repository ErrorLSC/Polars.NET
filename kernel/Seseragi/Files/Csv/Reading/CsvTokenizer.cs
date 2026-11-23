using System.Buffers;
using System.Runtime.CompilerServices;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>
/// A field of a record in a unit's bytes: its text (inside the quotes when quoted) and flags in the length's high bits,
/// or a marker length: <see cref="Missing"/> (the record has fewer fields) or <see cref="Blank"/> (a blank line).
/// </summary>
internal struct CsvField
{
    public const int Quoted = 1 << 30, Escaped = 1 << 29, LengthMask = Escaped - 1;

    /// <summary>A field the record lacks (it has fewer fields than columns).</summary>
    public const int Missing = -1;

    /// <summary>A field of a blank line (a null row).</summary>
    public const int Blank = -2;

    /// <summary>The longest field (512 MiB).</summary>
    public const int MaxLength = LengthMask;

    public int Start;
    public int Bits;

    public readonly int Length => Bits & LengthMask;

    public readonly bool IsQuoted => Bits >= 0 && (Bits & Quoted) != 0;

    public readonly bool IsEscaped => Bits >= 0 && (Bits & Escaped) != 0;

    /// <summary>Whether the field is no text at all: missing, of a blank line, or empty and unquoted.</summary>
    public readonly bool IsAbsent => Bits <= 0;
}

/// <summary>What ended a field.</summary>
internal enum FieldEnd : sbyte
{
    /// <summary>A separator: another field follows.</summary>
    Separator,
    /// <summary>A line end (consumed): the record ends.</summary>
    Record,
    /// <summary>The end of the bytes: the record ends.</summary>
    Input,
    /// <summary>A quote inside an unquoted field.</summary>
    QuoteInside,
    /// <summary>Text after a field's closing quote.</summary>
    AfterQuote,
    /// <summary>A quote opened a field and never closed.</summary>
    Unterminated,
    /// <summary>A field longer than <see cref="CsvField.MaxLength"/>.</summary>
    TooLong,
    /// <summary>A record with more fields than columns.</summary>
    TooManyFields,
}

/// <summary>
/// The grammar of delimited records (the decided rules of the managed CSV reader): fields split by the separator; a
/// field starting with the quote runs to the next quote not doubled and is followed by a separator, a line end or the
/// end; a quote inside an unquoted field is an error; a record ends at a line end — LF, CR or CRLF, or the chosen byte —
/// outside quotes; an empty line is a blank record (a null row); a record start beginning with the comment prefix starts a
/// comment line (to its line end, quotes and all), no record. Tokenizes one record at a time (header, inference) or
/// many into a column-major table (<see cref="Records"/>), records with more fields than columns dropping the extra ones
/// when truncating (still tokenized: their errors count).
/// </summary>
internal sealed class CsvTokenizer
{
    private static readonly SearchValues<byte> CommaQuoteStops = SearchValues.Create(",\"\n\r"u8);
    private static readonly SearchValues<byte> CommaStops = SearchValues.Create(",\n\r"u8);

    private readonly SearchValues<byte> _stops, _recordStops;
    private readonly byte _separator, _quote;
    private readonly bool _quoting, _truncate;
    private readonly int _lineEnd; // -1: LF, CR or CRLF
    private readonly byte[]? _comment;

    public CsvTokenizer(byte separator, byte? quote, byte? lineEnd, byte[]? comment = null, bool truncate = false)
    {
        _separator = separator;
        _quoting = quote is not null;
        _quote = quote ?? 0;
        _lineEnd = lineEnd ?? -1;
        _comment = comment is { Length: > 0 } ? comment : null;
        _truncate = truncate;
        _recordStops = (quote, lineEnd) switch
        {
            ({ } q, null) => SearchValues.Create([q, (byte)'\n', (byte)'\r']),
            (null, null) => SearchValues.Create("\n\r"u8),
            ({ } q, { } e) => SearchValues.Create([q, e]),
            (null, { } e) => SearchValues.Create([e]),
        };
        _stops = (separator, quote, lineEnd) switch
        {
            ((byte)',', (byte)'"', null) => CommaQuoteStops,
            ((byte)',', null, null) => CommaStops,
            (_, { } q, null) => SearchValues.Create([separator, q, (byte)'\n', (byte)'\r']),
            (_, null, null) => SearchValues.Create([separator, (byte)'\n', (byte)'\r']),
            (_, { } q, { } e) => SearchValues.Create([separator, q, e]),
            (_, null, { } e) => SearchValues.Create([separator, e]),
        };
    }

    /// <summary>Whether a line end starts at <paramref name="pos"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool AtLineEnd(ReadOnlySpan<byte> b, int pos) => _lineEnd < 0 ? b[pos] is (byte)'\n' or (byte)'\r' : b[pos] == _lineEnd;

    /// <summary>Whether comment lines are passed over.</summary>
    public bool HasComments => _comment is not null;

    /// <summary>Whether a comment line starts at <paramref name="pos"/> (a record start before the end).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool AtComment(ReadOnlySpan<byte> b, int pos) => _comment is not null && b[pos..].StartsWith(_comment);

    /// <summary>Passes over the comment lines at <paramref name="pos"/> (a record start): to the next record start, or the end.</summary>
    public int SkipComments(ReadOnlySpan<byte> b, int pos)
    {
        while (pos < b.Length && AtComment(b, pos)) pos = LineAfter(b, pos);
        return pos;
    }

    /// <summary>The position after the line end of the line at <paramref name="pos"/>, quotes ignored, or the end.</summary>
    public int LineAfter(ReadOnlySpan<byte> b, int pos)
    {
        int k = _lineEnd >= 0 ? b[pos..].IndexOf((byte)_lineEnd) : b[pos..].IndexOfAny((byte)'\n', (byte)'\r');
        return k < 0 ? b.Length : SkipLineEnd(b, pos + k);
    }

    /// <summary>
    /// Passes over the record at <paramref name="pos"/> as the cut finds records (every quote toggles; no field grammar,
    /// so nothing in it fails): to the next record start, or the end (a quote left open runs to it).
    /// </summary>
    public int SkipRecord(ReadOnlySpan<byte> b, int pos)
    {
        bool inside = false;
        while (true)
        {
            int k = inside ? b[pos..].IndexOf(_quote) : b[pos..].IndexOfAny(_recordStops);
            if (k < 0) return b.Length;
            pos += k;
            if (_quoting && b[pos] == _quote)
            {
                inside = !inside;
                pos++;
                continue;
            }
            return SkipLineEnd(b, pos);
        }
    }

    /// <summary>Passes over the line end at <paramref name="pos"/> (CRLF as one).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int SkipLineEnd(ReadOnlySpan<byte> b, int pos) =>
        _lineEnd < 0 && b[pos] == '\r' && pos + 1 < b.Length && b[pos + 1] == '\n' ? pos + 2 : pos + 1;

    /// <summary>
    /// Reads the field at <paramref name="pos"/> (a field start) into <paramref name="field"/> and moves past its
    /// delimiter (a separator or line end); errors leave <paramref name="pos"/> at the field's start.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public FieldEnd Field(ReadOnlySpan<byte> b, ref int pos, out CsvField field)
    {
        int start = pos;
        field.Start = start;
        if (_quoting && start < b.Length && b[start] == _quote) return Quoted(b, ref pos, out field);
        int k = b[start..].IndexOfAny(_stops);
        int end = k < 0 ? b.Length : start + k;
        if (end - start > CsvField.MaxLength)
        {
            field.Bits = 0;
            return FieldEnd.TooLong;
        }
        field.Bits = end - start;
        if (k < 0)
        {
            pos = b.Length;
            return FieldEnd.Input;
        }
        byte d = b[end];
        if (d == _separator)
        {
            pos = end + 1;
            return FieldEnd.Separator;
        }
        if (_quoting && d == _quote) return FieldEnd.QuoteInside;
        pos = SkipLineEnd(b, end);
        return FieldEnd.Record;
    }

    private FieldEnd Quoted(ReadOnlySpan<byte> b, ref int pos, out CsvField field)
    {
        int start = pos, p = start + 1;
        bool escaped = false;
        field.Start = start + 1;
        field.Bits = 0;
        while (true)
        {
            int k = b[p..].IndexOf(_quote);
            if (k < 0) return FieldEnd.Unterminated;
            p += k;
            if (p + 1 < b.Length && b[p + 1] == _quote)
            {
                escaped = true;
                p += 2;
                continue;
            }
            break;
        }
        int length = p - (start + 1);
        if (length > CsvField.MaxLength) return FieldEnd.TooLong;
        field.Bits = length | CsvField.Quoted | (escaped ? CsvField.Escaped : 0);
        int after = p + 1;
        if (after == b.Length)
        {
            pos = after;
            return FieldEnd.Input;
        }
        byte d = b[after];
        if (d == _separator)
        {
            pos = after + 1;
            return FieldEnd.Separator;
        }
        if (!AtLineEnd(b, after)) return FieldEnd.AfterQuote;
        pos = SkipLineEnd(b, after);
        return FieldEnd.Record;
    }

    /// <summary>
    /// Reads one record at <paramref name="pos"/> (a record start before the end) into <paramref name="fields"/>,
    /// every field (any count); a blank line gives no field and <paramref name="blank"/>. Returns
    /// <see cref="FieldEnd.Record"/> (or <see cref="FieldEnd.Input"/>) or the error, <paramref name="pos"/> then at the
    /// failing field's start and <paramref name="failed"/> its index.
    /// </summary>
    public FieldEnd Record(ReadOnlySpan<byte> b, ref int pos, List<CsvField> fields, out bool blank, out int failed)
    {
        fields.Clear();
        failed = -1;
        blank = AtLineEnd(b, pos);
        if (blank)
        {
            pos = SkipLineEnd(b, pos);
            return FieldEnd.Record;
        }
        while (true)
        {
            FieldEnd end = Field(b, ref pos, out CsvField field);
            if (end > FieldEnd.Input)
            {
                failed = fields.Count;
                return end;
            }
            fields.Add(field);
            if (end == FieldEnd.Separator && pos == b.Length)
            {
                // A separator last: an empty field ends the record.
                fields.Add(new CsvField { Start = pos, Bits = 0 });
                return FieldEnd.Input;
            }
            if (end != FieldEnd.Separator) return end;
        }
    }

    /// <summary>
    /// Tokenizes up to <paramref name="capacity"/> records from <paramref name="pos"/> into <paramref name="table"/>:
    /// the field of file column c of record r at <c>table[slots[c] * capacity + r]</c> for the columns kept
    /// (<paramref name="slots"/>[c] ≥ 0); fewer fields are <see cref="CsvField.Missing"/>, a blank line's are
    /// <see cref="CsvField.Blank"/>; <paramref name="starts"/>[r] is record r's start. Returns the records read; on an
    /// error, the records before the failing one, with <paramref name="error"/> its kind, <paramref name="failedColumn"/>
    /// its field's column (or the first extra one) and <paramref name="pos"/> its record's start.
    /// </summary>
    public int Records(ReadOnlySpan<byte> b, ref int pos, int columns, ReadOnlySpan<int> slots, Span<CsvField> table, Span<int> starts, int capacity,
        out FieldEnd error, out int failedColumn)
    {
        error = FieldEnd.Record;
        failedColumn = -1;
        int r = 0;
        for (; r < capacity && pos < b.Length; r++)
        {
            if (_comment is not null)
            {
                pos = SkipComments(b, pos);
                if (pos == b.Length) break;
            }
            int recordStart = pos;
            starts[r] = recordStart;
            if (AtLineEnd(b, pos))
            {
                for (int c = 0; c < columns; c++)
                    if (slots[c] >= 0) table[slots[c] * capacity + r] = new CsvField { Start = pos, Bits = CsvField.Blank };
                pos = SkipLineEnd(b, pos);
                continue;
            }
            int column = 0;
            while (true)
            {
                FieldEnd end = Field(b, ref pos, out CsvField field);
                if (end > FieldEnd.Input)
                {
                    error = end;
                    failedColumn = column;
                    pos = recordStart;
                    return r;
                }
                if (column >= columns)
                {
                    if (_truncate)
                    {
                        // The extra fields are tokenized (a malformed one fails the record) and dropped.
                        while (end == FieldEnd.Separator && pos < b.Length)
                        {
                            column++;
                            end = Field(b, ref pos, out _);
                            if (end > FieldEnd.Input)
                            {
                                error = end;
                                failedColumn = column;
                                pos = recordStart;
                                return r;
                            }
                        }
                        break;
                    }
                    error = FieldEnd.TooManyFields;
                    failedColumn = column;
                    pos = recordStart;
                    return r;
                }
                int slot = slots[column];
                if (slot >= 0) table[slot * capacity + r] = field;
                column++;
                if (end == FieldEnd.Separator)
                {
                    if (pos < b.Length) continue;
                    // A separator last: an empty field ends the record.
                    if (column >= columns)
                    {
                        if (_truncate) break;
                        error = FieldEnd.TooManyFields;
                        failedColumn = column;
                        pos = recordStart;
                        return r;
                    }
                    slot = slots[column];
                    if (slot >= 0) table[slot * capacity + r] = new CsvField { Start = pos, Bits = 0 };
                    column++;
                }
                break;
            }
            for (; column < columns; column++)
                if (slots[column] >= 0) table[slots[column] * capacity + r] = new CsvField { Start = pos, Bits = CsvField.Missing };
        }
        return r;
    }

    /// <summary>The unescaped text of a quoted field with doubled quotes, written to <paramref name="destination"/> (as long as the field at least).</summary>
    public int Unescape(ReadOnlySpan<byte> text, Span<byte> destination)
    {
        int written = 0;
        while (true)
        {
            int k = text.IndexOf(_quote);
            if (k < 0)
            {
                text.CopyTo(destination[written..]);
                return written + text.Length;
            }
            text[..(k + 1)].CopyTo(destination[written..]);
            written += k + 1;
            text = text[(k + 2)..]; // the doubled quote's second half
        }
    }

    /// <summary>Whether records with more fields than columns drop the extra ones.</summary>
    public bool Truncates => _truncate;

    /// <summary>The line ends in <paramref name="b"/> (inside quotes too): LF, CR not before LF, or the chosen byte.</summary>
    public long CountLines(ReadOnlySpan<byte> b)
    {
        if (_lineEnd >= 0) return b.Count((byte)_lineEnd);
        long lf = b.Count((byte)'\n');
        if (b.IndexOf((byte)'\r') < 0) return lf;
        long crlf = 0, cr = 0;
        for (int at = 0; ;)
        {
            int k = b[at..].IndexOf((byte)'\r');
            if (k < 0) break;
            at += k + 1;
            cr++;
            if (at < b.Length && b[at] == '\n') crlf++;
        }
        return lf + cr - crlf;
    }
}
