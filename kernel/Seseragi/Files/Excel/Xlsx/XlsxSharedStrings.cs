using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Xlsx;

/// <summary>
/// The shared string table, decoded once into one native UTF-8 buffer with an offset per string (no managed string
/// per entry). Rich-text runs are concatenated; phonetic runs (<c>rPh</c>) are left out.
/// </summary>
internal sealed class XlsxSharedStrings : ISharedStrings, IDisposable
{
    private NativeGrowBuffer _bytes;
    private NativeGrowBuffer _offsets; // long per string boundary: string i spans [i] .. [i + 1]
    private int _count;

    private XlsxSharedStrings() { }

    public int Count => _count;

    public unsafe ReadOnlySpan<byte> Get(int index)
    {
        if ((uint)index >= (uint)_count)
            throw new ExcelFormatException($"A cell refers to shared string {index}, but the table holds {_count}.");
        long* offsets = (long*)_offsets.Pointer;
        long start = offsets[index];
        return new(_bytes.Pointer + start, (int)(offsets[index + 1] - start));
    }

    public static async ValueTask<XlsxSharedStrings> LoadAsync(Stream part, bool useAsync, CancellationToken cancellationToken)
    {
        var table = new XlsxSharedStrings();
        try
        {
            using var reader = new XmlChunkReader(part);
            await table.LoadCoreAsync(reader, useAsync, cancellationToken).ConfigureAwait(false);
            return table;
        }
        catch
        {
            table.Dispose();
            throw;
        }
    }

    private async ValueTask LoadCoreAsync(XmlChunkReader reader, bool useAsync, CancellationToken ct)
    {
        _offsets.Append(0L);
        while (true)
        {
            int consumed = ParseAvailable(reader.Buffered, reader.EndOfStream);
            reader.Consume(consumed);
            if (reader.EndOfStream) break;
            if (useAsync) await reader.FillAsync(ct).ConfigureAwait(false);
            else reader.Fill();
        }
    }

    // Parses every complete <si> element in the buffer (and the start of the part); returns the bytes consumed.
    private int ParseAvailable(ReadOnlySpan<byte> buffer, bool final)
    {
        int pos = 0;
        while (true)
        {
            int lt = buffer[pos..].IndexOf((byte)'<');
            if (lt < 0) return buffer.Length;
            int start = pos + lt;
            var rest = buffer[start..];
            if (!IsTag(rest, "si"u8, out int nameEnd))
            {
                // Some other tag (<?xml?>, <sst>, </sst>, …): skip past its '>'.
                int gt = rest.IndexOf((byte)'>');
                if (gt < 0) return final ? buffer.Length : start;
                pos = start + gt + 1;
                continue;
            }
            int tagEnd = rest.IndexOf((byte)'>');
            if (tagEnd < 0) return final ? throw Truncated() : start;
            if (rest[tagEnd - 1] == (byte)'/')
            {
                AddString(default);
                pos = start + tagEnd + 1;
                continue;
            }
            int close = FindEnd(rest, rest[1..nameEnd]);
            if (close < 0) return final ? throw Truncated() : start;
            AddString(rest[..close]);
            pos = start + close;
        }
    }

    // Appends the text of one <si> element (its <t> elements outside <rPh>).
    private unsafe void AddString(ReadOnlySpan<byte> si)
    {
        long start = _bytes.Length;
        var x = new XmlSpanScanner(si);
        int phonetic = 0;
        bool inText = false;
        while (x.Read())
        {
            switch (x.Token)
            {
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
                    AppendDecoded(x.Text, x.IsCData);
                    break;
            }
        }
        // Escapes may span text tokens split by CDATA; decode over the whole string.
        int length = (int)(_bytes.Length - start);
        if (length > 0)
        {
            int decoded = XmlText.DecodeEscapes(new Span<byte>(_bytes.Pointer + start, length));
            _bytes.SetLength(start + decoded);
        }
        _offsets.Append(_bytes.Length);
        _count++;
    }

    private unsafe void AppendDecoded(ReadOnlySpan<byte> raw, bool cdata)
    {
        if (raw.IsEmpty) return;
        long at = _bytes.Length;
        byte* p = _bytes.Reserve(raw.Length);
        int n;
        if (cdata)
        {
            raw.CopyTo(new Span<byte>(p, raw.Length));
            n = raw.Length;
        }
        else
        {
            n = XmlText.DecodeEntities(raw, new Span<byte>(p, raw.Length));
        }
        _bytes.SetLength(at + n);
    }

    // "<si" or "<x:si" followed by whitespace, '>' or '/'. nameEnd = index after the (prefixed) name.
    internal static bool IsTag(ReadOnlySpan<byte> s, ReadOnlySpan<byte> localName, out int nameEnd)
    {
        nameEnd = 0;
        if (s.Length < 2 || s[0] != (byte)'<') return false;
        int i = 1;
        int end = i;
        while (end < s.Length && s[end] is not ((byte)' ' or (byte)'>' or (byte)'/' or (byte)'\t' or (byte)'\r' or (byte)'\n')) end++;
        if (end >= s.Length) return false;
        var name = s[i..end];
        int colon = name.IndexOf((byte)':');
        if (!name[(colon + 1)..].SequenceEqual(localName)) return false;
        nameEnd = end;
        return true;
    }

    // The index just past "</name>" (name with its prefix) in s, or -1.
    internal static int FindEnd(ReadOnlySpan<byte> s, ReadOnlySpan<byte> qualifiedName)
    {
        Span<byte> tag = stackalloc byte[qualifiedName.Length + 3];
        tag[0] = (byte)'<';
        tag[1] = (byte)'/';
        qualifiedName.CopyTo(tag[2..]);
        tag[^1] = (byte)'>';
        int i = s.IndexOf(tag);
        return i < 0 ? -1 : i + tag.Length;
    }

    private static ExcelFormatException Truncated() => new("The shared string table is truncated.");

    public void Dispose()
    {
        _bytes.Free();
        _offsets.Free();
    }
}
