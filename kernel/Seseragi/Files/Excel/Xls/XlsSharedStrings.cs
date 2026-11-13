using System.Buffers.Binary;
using Kaminari.Seseragi.Files.Excel.Reading;

namespace Kaminari.Seseragi.Files.Excel.Xls;

/// <summary>
/// Loads the SST record ([MS-XLS] 2.4.265) and its Continue records into a <see cref="SharedStringStore"/>. Each
/// string is an XLUnicodeRichExtendedString (2.5.293) of which only the characters are kept; its header is never
/// split, but its characters, formatting runs and phonetic data may run on into the next Continue record. A
/// continuation starts with a width byte only when it resumes the characters. The declared string counts are not
/// trusted: the strings are read until the records end.
/// </summary>
internal static class XlsSharedStrings
{
    public static async ValueTask<SharedStringStore> LoadAsync(Stream workbook, long position, bool useAsync, CancellationToken ct)
    {
        workbook.Position = position;
        using var records = new BiffRecords(workbook);
        var store = new SharedStringStore();
        try
        {
            if (!await NextAsync().ConfigureAwait(false) || records.Type != Rt.Sst || records.Size < 8)
                throw new ExcelFormatException("The shared string table is not where the workbook stream places it.");
            var parser = new Parser(store);
            parser.Feed(records.Data[8..], continuation: false); // cstTotal, cstUnique
            while (await NextAsync().ConfigureAwait(false) && records.Type == Rt.Continue) parser.Feed(records.Data, continuation: true);
            parser.End();
            return store;
        }
        catch
        {
            store.Dispose();
            throw;
        }

        async ValueTask<bool> NextAsync() => useAsync ? await records.NextAsync(ct).ConfigureAwait(false) : records.Next();
    }

    private sealed class Parser(SharedStringStore store)
    {
        private readonly BiffText _text = new();
        private bool _inChars;
        private long _skip; // formatting runs and phonetic data still to pass

        public void Feed(ReadOnlySpan<byte> data, bool continuation)
        {
            int pos = 0;
            if (continuation && _inChars && !data.IsEmpty)
            {
                _text.Resume(data[0]);
                pos = 1;
            }
            while (pos < data.Length)
            {
                if (_inChars)
                {
                    pos += _text.Feed(data[pos..]);
                    if (!_text.Done)
                    {
                        if (pos < data.Length) throw new ExcelFormatException("A shared string's two-byte characters are split across records.");
                        break;
                    }
                    EndString();
                    continue;
                }
                if (_skip > 0)
                {
                    int n = (int)Math.Min(_skip, data.Length - pos);
                    pos += n;
                    _skip -= n;
                    continue;
                }
                // cch, then flags: fHighByte (bit 0), fExtSt (bit 2), fRichSt (bit 3); then cRun and cbExtRst if flagged.
                var rest = data[pos..];
                if (rest.Length < 3) throw SplitHeader();
                int chars = BinaryPrimitives.ReadUInt16LittleEndian(rest);
                byte flags = rest[2];
                bool rich = (flags & 8) != 0, ext = (flags & 4) != 0;
                int header = 3 + (rich ? 2 : 0) + (ext ? 4 : 0);
                if (rest.Length < header) throw SplitHeader();
                int runs = rich ? BinaryPrimitives.ReadUInt16LittleEndian(rest[3..]) : 0;
                int extLength = ext ? BinaryPrimitives.ReadInt32LittleEndian(rest[(rich ? 5 : 3)..]) : 0;
                if (extLength < 0) throw new ExcelFormatException("A shared string has a negative phonetic data size.");
                _skip = 4L * runs + extLength;
                _text.Begin(chars, (flags & 1) != 0);
                _inChars = true;
                pos += header;
            }
            if (_inChars && _text.Done) EndString(); // an empty string at the end of the record
        }

        private void EndString()
        {
            _inChars = false;
            long start = store.Length;
            int n = _text.ToUtf8(store.Reserve(_text.MaxUtf8));
            store.Truncate(start + n);
            store.EndString();
        }

        public void End()
        {
            if (_inChars || _skip > 0) throw new ExcelFormatException("The shared string table ends inside a string.");
        }

        private static ExcelFormatException SplitHeader() => new("A shared string's header is split across records.");
    }
}
