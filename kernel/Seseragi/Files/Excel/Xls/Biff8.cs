using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text.Unicode;
using Kaminari.Seseragi.Files.Excel.Reading;

namespace Kaminari.Seseragi.Files.Excel.Xls;

/// <summary>Record types of [MS-XLS] section 2.3 that the reader uses.</summary>
internal static class Rt
{
    public const int Formula = 0x0006;     // 2.4.127
    public const int Eof = 0x000A;         // 2.4.103
    public const int Date1904 = 0x0022;    // 2.4.77
    public const int FilePass = 0x002F;    // 2.4.117
    public const int Continue = 0x003C;    // 2.4.58
    public const int WsBool = 0x0081;      // 2.4.351
    public const int BoundSheet8 = 0x0085; // 2.4.28
    public const int MulRk = 0x00BD;       // 2.4.175
    public const int MulBlank = 0x00BE;    // 2.4.174
    public const int RString = 0x00D6;     // not in [MS-XLS]: a BIFF5/7 rich-text cell
    public const int Xf = 0x00E0;          // 2.4.353
    public const int Sst = 0x00FC;         // 2.4.265
    public const int LabelSst = 0x00FD;    // 2.4.149
    public const int Dimensions = 0x0200;  // 2.4.90
    public const int Blank = 0x0201;       // 2.4.20
    public const int Number = 0x0203;      // 2.4.180
    public const int Label = 0x0204;       // 2.4.148
    public const int BoolErr = 0x0205;     // 2.4.24
    public const int String = 0x0207;      // 2.4.268
    public const int Rk = 0x027E;          // 2.4.220
    public const int Format = 0x041E;      // 2.4.126
    public const int Bof = 0x0809;         // 2.4.21

    // BOF records of BIFF2, BIFF3 and BIFF4 (before [MS-XLS]): recognised only to name them in the error.
    public const int Bof2 = 0x0009;
    public const int Bof3 = 0x0209;
    public const int Bof4 = 0x0409;
}

/// <summary>
/// The records of a BIFF8 stream ([MS-XLS] 2.1.4: a 16-bit type, a 16-bit size, the data), pulled one at a time from
/// a buffer over the stream. The current record's <see cref="Data"/> stays valid until the next pull. A record can
/// be kept for the next pull (<see cref="Keep"/>), when the reader learns only from it that something else ended.
/// The buffer starts at <paramref name="bufferSize"/> bytes (one read) and grows for a larger record.
/// </summary>
internal sealed class BiffRecords(Stream stream, int bufferSize = 64 * 1024) : IDisposable
{
    private readonly ChunkReader _reader = new(stream, xml: false, bufferSize);
    private readonly long _start = stream.Position;
    private long _consumed;   // bytes consumed before the current record
    private int _current;     // length of the current record, header included (0 before the first pull)
    private bool _keep;

    public int Type { get; private set; }

    public int Size { get; private set; }

    /// <summary>The stream offset of the current record's header.</summary>
    public long Position => _start + _consumed;

    public ReadOnlySpan<byte> Data => _reader.Buffered.Slice(4, Size);

    /// <summary>The next pull returns the current record again.</summary>
    public void Keep() => _keep = true;

    public bool Next()
    {
        if (TryAdvance()) return true;
        while (_reader.Fill())
            if (TryHeader()) return true;
        return AtEnd();
    }

    public ValueTask<bool> NextAsync(CancellationToken cancellationToken) =>
        TryAdvance() ? new(true) : SlowAsync(cancellationToken);

    private async ValueTask<bool> SlowAsync(CancellationToken cancellationToken)
    {
        while (await _reader.FillAsync(cancellationToken).ConfigureAwait(false))
            if (TryHeader()) return true;
        return AtEnd();
    }

    // Drops the current record (unless kept) and parses the next one if it is all buffered.
    private bool TryAdvance()
    {
        if (_keep)
        {
            _keep = false;
            return true;
        }
        _reader.Consume(_current);
        _consumed += _current;
        _current = 0;
        return TryHeader();
    }

    private bool TryHeader()
    {
        var b = _reader.Buffered;
        if (b.Length < 4) return false;
        int size = BinaryPrimitives.ReadUInt16LittleEndian(b[2..]);
        if (b.Length < 4 + size) return false;
        Type = BinaryPrimitives.ReadUInt16LittleEndian(b);
        Size = size;
        _current = 4 + size;
        return true;
    }

    private bool AtEnd()
    {
        if (!_reader.Buffered.IsEmpty) throw new ExcelFormatException("The workbook stream ends inside a record.");
        return false;
    }

    public void Dispose() => _reader.Dispose();
}

/// <summary>
/// Decodes the characters of an XLUnicodeString or XLUnicodeRichExtendedString ([MS-XLS] 2.5.294, 2.5.293), which
/// may run on into Continue records: each continuation that resumes the characters starts with a byte whose low bit
/// says whether they are now one byte (Latin-1, the low bytes of UTF-16) or two (UTF-16LE) each. The characters are
/// collected first, so that a surrogate pair split across records still converts.
/// </summary>
internal sealed class BiffText
{
    private char[] _chars = new char[256];
    private int _count;
    private int _remaining;
    private bool _wide;

    /// <summary>All characters read.</summary>
    public bool Done => _remaining == 0;

    public ReadOnlySpan<char> Chars => _chars.AsSpan(0, _count);

    public void Begin(int chars, bool wide)
    {
        if (_chars.Length < chars) _chars = new char[Math.Max(chars, _chars.Length * 2)];
        _count = 0;
        _remaining = chars;
        _wide = wide;
    }

    /// <summary>A continuation resumes the characters: its first byte gives their width.</summary>
    public void Resume(byte flags) => _wide = (flags & 1) != 0;

    /// <summary>Takes as many characters as <paramref name="data"/> holds; returns the bytes used.</summary>
    public int Feed(ReadOnlySpan<byte> data)
    {
        int n;
        if (_wide)
        {
            n = Math.Min(_remaining, data.Length / 2);
            MemoryMarshal.Cast<byte, char>(data[..(2 * n)]).CopyTo(_chars.AsSpan(_count));
        }
        else
        {
            n = Math.Min(_remaining, data.Length);
            var into = _chars.AsSpan(_count, n);
            for (int i = 0; i < n; i++) into[i] = (char)data[i];
        }
        _count += n;
        _remaining -= n;
        return _wide ? 2 * n : n;
    }

    /// <summary>The most UTF-8 bytes the characters can need.</summary>
    public int MaxUtf8 => _count * 3;

    public int ToUtf8(Span<byte> destination)
    {
        Utf8.FromUtf16(Chars, destination, out _, out int written);
        return written;
    }
}
