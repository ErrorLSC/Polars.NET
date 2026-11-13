using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Unicode;

namespace Kaminari.Seseragi.Files.Excel.Xlsb;

/// <summary>Record types of [MS-XLSB] section 2.3 that the reader uses.</summary>
internal static class Brt
{
    // Worksheet cells (2.4.770, 2.4.319–2.4.330, 2.4.684–2.4.687).
    public const int RowHdr = 0;
    public const int CellBlank = 1;
    public const int CellRk = 2;
    public const int CellError = 3;
    public const int CellBool = 4;
    public const int CellReal = 5;
    public const int CellSt = 6;
    public const int CellIsst = 7;
    public const int FmlaString = 8;
    public const int FmlaNum = 9;
    public const int FmlaBool = 10;
    public const int FmlaError = 11;
    public const int CellRString = 62;
    public const int BeginSheetData = 145;
    public const int EndSheetData = 146;
    public const int WsDim = 148;

    // Shared strings, styles, workbook, tables.
    public const int SstItem = 19;
    public const int Fmt = 44;
    public const int Xf = 47;
    public const int EndBundleShs = 144;
    public const int WbProp = 153;
    public const int BundleSh = 156;
    public const int BeginSst = 159;
    public const int EndSst = 160;
    public const int BeginList = 343;
    public const int BeginFmts = 615;
    public const int EndFmts = 616;
    public const int BeginCellXfs = 617;
    public const int EndCellXfs = 618;
}

/// <summary>
/// BIFF12 records ([MS-XLSB] 2.1.4): a type of one or two bytes (7 bits each), a size of one to four bytes (7 bits
/// each, the fourth byte's high bit ignored), then the data.
/// </summary>
internal static class Biff12
{
    /// <summary>
    /// Reads the record header at the start of <paramref name="buffer"/>. False when the header or the data is not
    /// all there yet (<paramref name="headerLength"/> is 0 when even the header is incomplete).
    /// </summary>
    public static bool TryReadHeader(ReadOnlySpan<byte> buffer, out int type, out int size, out int headerLength)
    {
        type = size = headerLength = 0;
        if (buffer.Length < 2) return false;
        int i = 0;
        int b = buffer[i++];
        type = b & 0x7F;
        if ((b & 0x80) != 0)
        {
            b = buffer[i++];
            type |= (b & 0x7F) << 7;
        }
        for (int k = 0; k < 4; k++)
        {
            if (i >= buffer.Length) return false;
            b = buffer[i++];
            size |= (b & 0x7F) << (7 * k); // the fourth byte's high bit is ignored
            if (k == 3 || (b & 0x80) == 0) break;
        }
        headerLength = i;
        return buffer.Length - i >= size;
    }

    /// <summary>Enumerates the records of a part held whole in memory.</summary>
    public static RecordEnumerator Records(ReadOnlySpan<byte> part) => new(part);

    internal static ExcelFormatException Truncated(int type) => new($"A binary workbook record (type {type}) is shorter than its contents.");
}

/// <summary>The records of a part in memory, in order; a truncated last record is an error.</summary>
internal ref struct RecordEnumerator(ReadOnlySpan<byte> part)
{
    private ReadOnlySpan<byte> _rest = part;

    public int Type { get; private set; }

    public ReadOnlySpan<byte> Data { get; private set; }

    public readonly RecordEnumerator GetEnumerator() => this;

    public readonly RecordEnumerator Current => this;

    public bool MoveNext()
    {
        if (_rest.IsEmpty) return false;
        if (!Biff12.TryReadHeader(_rest, out int type, out int size, out int header))
            throw new ExcelFormatException("A binary workbook part ends inside a record.");
        Type = type;
        Data = _rest.Slice(header, size);
        _rest = _rest[(header + size)..];
        return true;
    }
}

/// <summary>Bounds-checked reads of record fields (little endian); a short record raises <see cref="ExcelFormatException"/>.</summary>
internal ref struct RecordReader(ReadOnlySpan<byte> data, int type)
{
    private readonly ReadOnlySpan<byte> _data = data;
    private readonly int _type = type;

    public int Position { get; set; }

    public readonly int Remaining => _data.Length - Position;

    private ReadOnlySpan<byte> Take(int bytes)
    {
        if (bytes < 0 || Position + bytes > _data.Length) throw Biff12.Truncated(_type);
        var s = _data.Slice(Position, bytes);
        Position += bytes;
        return s;
    }

    public byte U8() => Take(1)[0];

    public ushort U16() => BinaryPrimitives.ReadUInt16LittleEndian(Take(2));

    public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));

    public int I32() => BinaryPrimitives.ReadInt32LittleEndian(Take(4));

    public double F64() => BinaryPrimitives.ReadDoubleLittleEndian(Take(8));

    public void Skip(int bytes) => Take(bytes);

    public ReadOnlySpan<byte> Bytes(int count) => Take(count);

    /// <summary>An XLWideString (2.5.169): a 32-bit character count, then UTF-16LE characters.</summary>
    public ReadOnlySpan<byte> WideStringBytes()
    {
        uint chars = U32();
        if (chars > int.MaxValue / 2) throw Biff12.Truncated(_type);
        return Take((int)chars * 2);
    }

    /// <summary>An XLNullableWideString (2.5.167): like <see cref="WideStringBytes"/>, a count of 0xFFFFFFFF meaning NULL.</summary>
    public bool TryNullableWideStringBytes(out ReadOnlySpan<byte> utf16)
    {
        uint chars = U32();
        if (chars == uint.MaxValue)
        {
            utf16 = default;
            return false;
        }
        if (chars > int.MaxValue / 2) throw Biff12.Truncated(_type);
        utf16 = Take((int)chars * 2);
        return true;
    }

    /// <summary>A wide string as a .NET string (for names; cell text goes through <see cref="WideText.ToUtf8"/>).</summary>
    public string WideString() => WideText.ToString(WideStringBytes());
}

/// <summary>UTF-16LE text of records to UTF-8 (unpaired surrogates become U+FFFD).</summary>
internal static class WideText
{
    /// <summary>The most UTF-8 bytes <paramref name="utf16Bytes"/> can need.</summary>
    public static int MaxUtf8(int utf16Bytes) => utf16Bytes / 2 * 3;

    public static int ToUtf8(ReadOnlySpan<byte> utf16, Span<byte> destination)
    {
        Utf8.FromUtf16(MemoryMarshal.Cast<byte, char>(utf16), destination, out _, out int written);
        return written;
    }

    public static string ToString(ReadOnlySpan<byte> utf16) => Encoding.Unicode.GetString(utf16);
}
