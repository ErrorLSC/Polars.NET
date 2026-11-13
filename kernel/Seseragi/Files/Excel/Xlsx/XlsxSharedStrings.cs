using Kaminari.Seseragi.Files.Excel.Packaging;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Xlsx;

/// <summary>
/// Loads <c>sharedStrings.xml</c> into a <see cref="SharedStringStore"/>, one <c>&lt;si&gt;</c> at a time as the
/// part decompresses. Rich-text runs are concatenated; phonetic runs (<c>rPh</c>) are left out.
/// </summary>
internal sealed class XlsxSharedStrings
{
    private readonly SharedStringStore _store = new();

    private XlsxSharedStrings() { }

    public static async ValueTask<SharedStringStore> LoadAsync(Stream part, bool useAsync, CancellationToken cancellationToken)
    {
        var loader = new XlsxSharedStrings();
        try
        {
            using var reader = new ChunkReader(part);
            await loader.LoadCoreAsync(reader, useAsync, cancellationToken).ConfigureAwait(false);
            return loader._store;
        }
        catch
        {
            loader._store.Dispose();
            throw;
        }
    }

    private async ValueTask LoadCoreAsync(ChunkReader reader, bool useAsync, CancellationToken ct)
    {
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
    private void AddString(ReadOnlySpan<byte> si)
    {
        long start = _store.Length;
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
        if (_store.Length > start) _store.Truncate(start + XmlText.DecodeEscapes(_store.Since(start)));
        _store.EndString();
    }

    private void AppendDecoded(ReadOnlySpan<byte> raw, bool cdata)
    {
        if (raw.IsEmpty) return;
        long at = _store.Length;
        var into = _store.Reserve(raw.Length);
        int n;
        if (cdata)
        {
            raw.CopyTo(into);
            n = raw.Length;
        }
        else
        {
            n = XmlText.DecodeEntities(raw, into);
        }
        _store.Truncate(at + n);
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
}
