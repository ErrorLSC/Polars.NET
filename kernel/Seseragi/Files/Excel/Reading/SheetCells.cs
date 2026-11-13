namespace Kaminari.Seseragi.Files.Excel.Reading;

/// <summary>What a cell holds, as stored in the file.</summary>
internal enum CellType : byte
{
    /// <summary>No value (a formatted but empty cell).</summary>
    Empty,

    /// <summary>A number; <see cref="Cell.Kind"/> says whether its format makes it a date, time or duration.</summary>
    Number,

    /// <summary>Text in the row's text buffer.</summary>
    String,

    /// <summary>Text in the workbook's shared string table, by index.</summary>
    SharedString,

    /// <summary>A boolean (<see cref="Cell.Number"/> is 0 or 1).</summary>
    Boolean,

    /// <summary>An error value (#N/A, #DIV/0! …): read as null.</summary>
    Error,

    /// <summary>A date, date-time, time or duration stored as such (<see cref="Cell.Kind"/>; <see cref="Cell.Micros"/>:
    /// Unix microseconds, microseconds of the day for times, microseconds for durations).</summary>
    Temporal,
}

/// <summary>One non-empty cell of a row (zero-based column).</summary>
internal struct Cell
{
    public int Column;
    public CellType Type;
    public ExcelValueKind Kind;
    public double Number;
    public long Micros;
    public int Offset; // String: start in the row text; SharedString: the index
    public int Length; // String: byte length
}

/// <summary>The shared string table of a workbook: UTF-8 strings by index.</summary>
internal interface ISharedStrings
{
    int Count { get; }

    ReadOnlySpan<byte> Get(int index);
}

/// <summary>
/// A row of cells as a sheet source produces it: the cells in column order (empty ones left out), and the bytes
/// of the strings stored inline. Reused from row to row.
/// </summary>
internal sealed class RowBuffer
{
    private Cell[] _cells = new Cell[16];
    private byte[] _text = new byte[256];

    /// <summary>Zero-based row number.</summary>
    public int Row { get; set; }

    public int Count { get; private set; }

    public int TextLength { get; private set; }

    public ISharedStrings? SharedStrings { get; set; }

    public ReadOnlySpan<Cell> Cells => _cells.AsSpan(0, Count);

    public ref Cell this[int i] => ref _cells[i];

    public void Clear()
    {
        Count = 0;
        TextLength = 0;
    }

    public ref Cell Add()
    {
        if (Count == _cells.Length) Array.Resize(ref _cells, _cells.Length * 2);
        ref Cell c = ref _cells[Count++];
        c = default;
        return ref c;
    }

    /// <summary>Removes the last added cell (it turned out empty).</summary>
    public void RemoveLast() => Count--;

    /// <summary>Space for <paramref name="bytes"/> more text bytes at <see cref="TextLength"/>.</summary>
    public Span<byte> ReserveText(int bytes)
    {
        if (TextLength + bytes > _text.Length) Array.Resize(ref _text, Math.Max(_text.Length * 2, TextLength + bytes));
        return _text.AsSpan(TextLength, bytes);
    }

    /// <summary>Commits <paramref name="bytes"/> text bytes written into the reserved span; returns their offset.</summary>
    public int CommitText(int bytes)
    {
        int at = TextLength;
        TextLength += bytes;
        return at;
    }

    /// <summary>Committed text bytes, for decoding in place.</summary>
    public Span<byte> TextRange(int offset, int length) => _text.AsSpan(offset, length);

    /// <summary>Drops committed text from <paramref name="length"/> on.</summary>
    public void TruncateText(int length) => TextLength = length;

    /// <summary>Appends raw XML text (entities decoded unless CDATA); returns the bytes added.</summary>
    public int AppendXmlText(ReadOnlySpan<byte> raw, bool cdata)
    {
        if (raw.IsEmpty) return 0;
        var into = ReserveText(raw.Length);
        int n;
        if (cdata)
        {
            raw.CopyTo(into);
            n = raw.Length;
        }
        else
        {
            n = Xml.XmlText.DecodeEntities(raw, into);
        }
        CommitText(n);
        return n;
    }

    /// <summary>The text of a string cell.</summary>
    public ReadOnlySpan<byte> GetText(scoped in Cell cell) => cell.Type switch
    {
        CellType.String => _text.AsSpan(cell.Offset, cell.Length),
        CellType.SharedString => (SharedStrings ?? throw new ExcelFormatException("A cell refers to the shared string table, which the workbook lacks.")).Get(cell.Offset),
        _ => default,
    };

    /// <summary>Makes this row a copy of <paramref name="other"/> (for rows repeated in the file).</summary>
    public void CopyFrom(RowBuffer other)
    {
        Row = other.Row;
        SharedStrings = other.SharedStrings;
        Clear();
        foreach (ref readonly var c in other.Cells) Add() = c;
        other._text.AsSpan(0, other.TextLength).CopyTo(ReserveText(other.TextLength));
        CommitText(other.TextLength);
    }

    /// <summary>A copy of this row (for the rows held back while the schema is inferred).</summary>
    public RowBuffer Clone()
    {
        var copy = new RowBuffer
        {
            Row = Row,
            SharedStrings = SharedStrings,
            _cells = _cells.AsSpan(0, Count).ToArray(),
            _text = _text.AsSpan(0, TextLength).ToArray(),
        };
        copy.Count = Count;
        copy.TextLength = TextLength;
        return copy;
    }
}

/// <summary>The rows of one sheet, in order, from a format-specific parser.</summary>
internal abstract class SheetSource : IDisposable
{
    /// <summary>The sheet's used range as the file declares it (<c>&lt;dimension&gt;</c>), when it does.</summary>
    public (int FirstRow, int FirstColumn, int LastRow, int LastColumn)? Dimension { get; protected set; }

    /// <summary>Fills <paramref name="row"/> with the next row that has cells (rows come in increasing order);
    /// false at the end of the sheet.</summary>
    public abstract bool ReadRow(RowBuffer row);

    /// <inheritdoc cref="ReadRow"/>
    public abstract ValueTask<bool> ReadRowAsync(RowBuffer row, CancellationToken cancellationToken);

    public abstract void Dispose();
}
