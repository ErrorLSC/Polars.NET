using Kaminari.Seseragi.Files.Excel.Reading;

namespace Kaminari.Seseragi.Files.Excel.Xlsb;

/// <summary>
/// Loads <c>sharedStrings.bin</c> into a <see cref="SharedStringStore"/>, record by record as the part decompresses:
/// each BrtSSTItem (2.4.807) holds a RichStr (2.5.122), of which only the text is kept.
/// </summary>
internal static class XlsbSharedStrings
{
    public static async ValueTask<SharedStringStore> LoadAsync(Stream part, bool useAsync, CancellationToken cancellationToken)
    {
        var store = new SharedStringStore();
        try
        {
            using var reader = new ChunkReader(part, xml: false);
            while (true)
            {
                reader.Consume(ParseAvailable(reader.Buffered, store));
                if (reader.EndOfStream) break;
                bool more = useAsync ? await reader.FillAsync(cancellationToken).ConfigureAwait(false) : reader.Fill();
                if (!more)
                {
                    reader.Consume(ParseAvailable(reader.Buffered, store));
                    if (!reader.Buffered.IsEmpty) throw new ExcelFormatException("The shared string table ends inside a record.");
                    break;
                }
            }
            return store;
        }
        catch
        {
            store.Dispose();
            throw;
        }

        // Every complete record in the buffer; returns the bytes consumed.
        static int ParseAvailable(ReadOnlySpan<byte> buffer, SharedStringStore store)
        {
            int pos = 0;
            while (Biff12.TryReadHeader(buffer[pos..], out int type, out int size, out int header))
            {
                if (type == Brt.SstItem) Add(buffer.Slice(pos + header, size), store);
                pos += header + size;
            }
            return pos;

            static void Add(ReadOnlySpan<byte> data, SharedStringStore store)
            {
                var x = new RecordReader(data, Brt.SstItem);
                x.Skip(1); // fRichStr, fExtStr
                var utf16 = x.WideStringBytes();
                long start = store.Length;
                int n = WideText.ToUtf8(utf16, store.Reserve(WideText.MaxUtf8(utf16.Length)));
                store.Truncate(start + n);
                store.EndString();
            }
        }
    }
}
