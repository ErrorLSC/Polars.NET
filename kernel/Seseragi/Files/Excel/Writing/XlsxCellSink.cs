using System.Buffers.Text;

namespace Kaminari.Seseragi.Files.Excel.Writing;

/// <summary>Cells as SpreadsheetML: <c>&lt;c r="B5" s="1" t="…"&gt;&lt;v&gt;…&lt;/v&gt;&lt;/c&gt;</c> inside <c>&lt;row&gt;</c>.</summary>
internal sealed class XlsxCellSink(XmlOut xml, SharedStringTable? sharedStrings, int columns) : CellSink
{
    private readonly byte[][] _letters = [.. Enumerable.Range(0, columns).Select(Letters)];
    private readonly byte[] _row = new byte[8];
    private int _rowLength;

    private static byte[] Letters(int column)
    {
        Span<byte> b = stackalloc byte[3];
        return b[..ExcelCellReference.WriteColumn(column, b)].ToArray();
    }

    private ReadOnlySpan<byte> Row => _row.AsSpan(0, _rowLength);

    protected override void StartRow()
    {
        Utf8Formatter.TryFormat(RowNumber, _row, out _rowLength);
        xml.Raw("<row r=\""u8);
        xml.Raw(Row);
        xml.Raw("\">"u8);
    }

    public override void EndRow() => xml.Raw("</row>"u8);

    private void Open(byte style, ReadOnlySpan<byte> type)
    {
        xml.Raw("<c r=\""u8);
        xml.CellRef(_letters[ColumnIndex], Row);
        xml.Byte((byte)'"');
        if (style != 0)
        {
            xml.Raw(" s=\""u8);
            xml.Int(style);
            xml.Byte((byte)'"');
        }
        if (!type.IsEmpty)
        {
            xml.Raw(" t=\""u8);
            xml.Raw(type);
            xml.Byte((byte)'"');
        }
        xml.Raw("><v>"u8);
    }

    private void Close() => xml.Raw("</v></c>"u8);

    public override void Number(double value, byte style = 0)
    {
        Open(style, default);
        xml.Double(value);
        Close();
    }

    public override void Integer(long value, byte style = 0)
    {
        Open(style, default);
        xml.Int(value);
        Close();
    }

    public override void UnsignedInteger(ulong value)
    {
        Open(0, default);
        xml.UInt(value);
        Close();
    }

    public override void Single(float value)
    {
        Open(0, default);
        xml.Single(value);
        Close();
    }

    public override void Boolean(bool value)
    {
        Open(0, "b"u8);
        xml.Byte(value ? (byte)'1' : (byte)'0');
        Close();
    }

    public override void NumError()
    {
        Open(0, "e"u8);
        xml.Raw("#NUM!"u8);
        Close();
    }

    /// <summary>A shared string (default) or an inline one.</summary>
    public override void String(ReadOnlySpan<byte> utf8)
    {
        CheckLength(utf8);
        if (sharedStrings is not null)
        {
            Open(0, "s"u8);
            xml.Int(sharedStrings.Add(utf8));
            Close();
            return;
        }
        xml.Raw("<c r=\""u8);
        xml.CellRef(_letters[ColumnIndex], Row);
        xml.Raw("\" t=\"inlineStr\"><is>"u8);
        xml.Raw(XmlOut.NeedsPreserve(utf8) ? "<t xml:space=\"preserve\">"u8 : "<t>"u8);
        xml.Text(utf8);
        xml.Raw("</t></is></c>"u8);
    }
}
