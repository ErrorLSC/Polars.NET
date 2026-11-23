using System.Text;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>A CSV input opened: its units, how their bytes become text, and the byte order mark the first unit starts with.</summary>
internal sealed record CsvOpened(TextUnits Units, CsvCharset Charset, int Mark, IDisposable? Owned = null)
{
    /// <summary>The lines passed over before the units (<see cref="CsvReadOptions.SkipLines"/>): line numbers start after them.</summary>
    public long SkippedLines { get; init; }
}

/// <summary>
/// Opens a CSV input in its encoding. The encoding is chosen from the first bytes (after decompression): a byte order mark
/// wins over the UTF-8 default, a given encoding other than UTF-8 meeting another encoding's mark is an error. Encodings
/// whose bytes below 0x80 are ASCII wherever they appear (UTF-8, the table code pages) are cut and tokenized on their
/// own bytes when the separator, quote and line end cannot be part of a character (a double-byte page's trail bytes go
/// down to 0x40), fields with other bytes decoded as they are read; every other input is decoded into UTF-8 first, in
/// order (UTF-16, stateful or EBCDIC pages, a double-byte page with a structural byte from 0x40 up).
/// </summary>
internal static class CsvInput
{
    /// <summary>A source read at offsets (the caller keeps it open while it is read).</summary>
    public static async ValueTask<CsvOpened> OpenAsync(IRandomAccess source, CsvReadOptions options, RecordCut cut, bool map, bool useAsync,
        CancellationToken ct)
    {
        byte[] head = await HeadAsync(new RandomAccessStream(source, 0), useAsync, ct).ConfigureAwait(false);
        Stream decompressed = StreamInput.Decompressed(new RandomAccessStream(source, 0), head, out bool compressed);
        byte[] text = head;
        if (compressed)
        {
            // The mark is inside the compressed text: its first bytes are read, then come again.
            text = await HeadAsync(decompressed, useAsync, ct).ConfigureAwait(false);
            decompressed = new PrefixedStream(text, decompressed, leaveOpen: false);
        }
        var (charset, decoder, mark) = Choose(options, text);
        if (decoder is null && compressed && options.SkipLines > 0)
        {
            // The lines are read off the decompressed text, then the rest is cut in order.
            Stream rest = await SkipLinesAsync(decompressed, mark, options, useAsync, ct).ConfigureAwait(false);
            return new CsvOpened(new StreamInput(rest, leaveOpen: false, options.UnitBytes, cut), charset, 0) { SkippedLines = options.SkipLines };
        }
        if (decoder is null)
        {
            await decompressed.DisposeAsync().ConfigureAwait(false);
            if (options.SkipLines > 0)
            {
                // The units start after the lines: the cut's quote parity starts clean there (a skipped line may end inside quotes).
                var skip = new LineSkip(options.SkipLines, LineEnd(options));
                long start = await skip.SkipAsync(source, Math.Min(mark, source.Length), useAsync, ct).ConfigureAwait(false);
                IRandomAccess after = start < 0 ? PastEnd(options, skip) : new RandomAccessSlice(source, start);
                TextUnits sliced = await TextUnits.RecordsAsync(after, map, options.UnitBytes, cut, options.MaxDegreeOfParallelism, useAsync, ct, plain: true).ConfigureAwait(false);
                return new CsvOpened(sliced, charset, 0) { SkippedLines = options.SkipLines };
            }
            TextUnits units = await TextUnits.RecordsAsync(source, map, options.UnitBytes, cut, options.MaxDegreeOfParallelism, useAsync, ct).ConfigureAwait(false);
            return new CsvOpened(units, charset, mark);
        }
        if (!compressed && options.MaxDegreeOfParallelism > 1 && InMemory(source, map, decoder, mark, options) is { } decoded)
        {
            // Local text decoded whole, side by side, then cut and read as UTF-8 in memory is.
            await decompressed.DisposeAsync().ConfigureAwait(false);
            try
            {
                IRandomAccess memory = new MemoryRandomAccess(decoded.Buffer.Memory[..decoded.Length]);
                if (options.SkipLines > 0)
                {
                    var skip = new LineSkip(options.SkipLines, LineEnd(options));
                    long start = await skip.SkipAsync(memory, 0, useAsync, ct).ConfigureAwait(false);
                    memory = start < 0 ? PastEnd(options, skip) : new RandomAccessSlice(memory, start);
                }
                TextUnits units = await TextUnits.RecordsAsync(memory, false, options.UnitBytes, cut, options.MaxDegreeOfParallelism, useAsync, ct, plain: true).ConfigureAwait(false);
                return new CsvOpened(units, charset, 0, new Releaser(decoded.Buffer)) { SkippedLines = options.SkipLines };
            }
            catch
            {
                decoded.Buffer.Release();
                throw;
            }
        }
        Stream transcoded = await SkipLinesAsync(Transcoded(decompressed, decoder, mark, options), 0, options, useAsync, ct).ConfigureAwait(false);
        return new CsvOpened(new StreamInput(transcoded, leaveOpen: false, options.UnitBytes, cut), charset, 0) { SkippedLines = options.SkipLines };
    }

    // The line end SkipLines counts (null: LF, CR or CRLF).
    private static byte? LineEnd(CsvReadOptions options) => options.LineEnd is { } e ? (byte)e : null;

    // The text after the lines SkipLines passes over (and a byte order mark before them); the whole stream when none.
    private static async ValueTask<Stream> SkipLinesAsync(Stream text, int mark, CsvReadOptions options, bool useAsync, CancellationToken ct)
    {
        if (options.SkipLines == 0 && mark == 0) return text;
        if (mark > 0)
        {
            byte[] bom = new byte[mark];
            int got = 0;
            while (got < mark)
            {
                int n = useAsync ? await text.ReadAsync(bom.AsMemory(got), ct).ConfigureAwait(false) : text.Read(bom.AsSpan(got));
                if (n <= 0) break;
                got += n;
            }
        }
        var skip = new LineSkip(options.SkipLines, LineEnd(options));
        Stream? rest = await skip.SkipAsync(text, useAsync, ct).ConfigureAwait(false);
        if (rest is not null) return rest;
        await text.DisposeAsync().ConfigureAwait(false);
        PastEnd(options, skip);
        return Stream.Null;
    }

    // SkipLines passing over the whole input: an error (RaiseIfEmpty), else no text at all.
    private static IRandomAccess PastEnd(CsvReadOptions options, LineSkip skip)
    {
        if (options.RaiseIfEmpty)
            throw new CsvFormatException($"SkipLines ({options.SkipLines}) passes over the whole input, which holds {skip.Found} line{(skip.Found == 1 ? "" : "s")}: " +
                "skip fewer lines, or give RaiseIfEmpty = false to read it as an empty frame.");
        return new MemoryRandomAccess(ReadOnlyMemory<byte>.Empty);
    }

    // A local source (memory, a mapped file) of text whose characters can be found from any boundary, decoded whole into
    // UTF-8 on every core; null otherwise (or when the text could exceed 2 GB decoded).
    private static unsafe (Sazanami.Buffers.NativeByteBuffer Buffer, int Length)? InMemory(IRandomAccess source, bool map, TextDecoder decoder, int mark,
        CsvReadOptions options)
    {
        if (source is not ILocalSource local || !ParallelTextDecodingStream.Splits(decoder)) return null;
        Func<TextDecoder> factory = Factory(decoder);
        int block = Math.Min(options.UnitBytes, 1 << 20), threads = options.MaxDegreeOfParallelism;
        if (local.TryGetMemory(out ReadOnlyMemory<byte> memory))
        {
            using System.Buffers.MemoryHandle pin = memory.Pin();
            byte* at = (byte*)pin.Pointer + Math.Min(mark, memory.Length);
            var all = ParallelTextDecodingStream.DecodeAll(at, memory.Length - Math.Min(mark, memory.Length), factory, threads, out int written, block);
            return all is null ? null : (all, written);
        }
        if (map && local.Map(out long start) is { } mapped && mapped.TryAcquire())
        {
            try
            {
                long length = source.Length;
                byte* at = mapped.At(start, length) + Math.Min(mark, length);
                var all = ParallelTextDecodingStream.DecodeAll(at, length - Math.Min(mark, length), factory, threads, out int written, block);
                return all is null ? null : (all, written);
            }
            finally
            {
                mapped.Release();
            }
        }
        return null;
    }

    // New decoders of a decoder's encoding (one per block).
    private static Func<TextDecoder> Factory(TextDecoder decoder)
    {
        Encoding encoding = decoder switch
        {
            Utf16TextDecoder u => u.BigEndian ? Encoding.BigEndianUnicode : Encoding.Unicode,
            SequentialTextDecoder s => s.Encoding,
            LegacyText l => TextEncodings.Resolve(l.CodePage),
            _ => throw new InvalidOperationException($"{decoder.GetType().Name} has no blocks."),
        };
        bool replace = decoder.Replace;
        return () => TextEncodings.Decoder(encoding, replace);
    }

    /// <summary>A stream read in order (owned from here: disposed with the input, unless options say to leave it open).</summary>
    public static async ValueTask<CsvOpened> OpenAsync(Stream stream, CsvReadOptions options, RecordCut cut, bool useAsync, CancellationToken ct)
    {
        byte[] head = await HeadAsync(stream, useAsync, ct).ConfigureAwait(false);
        Stream data = StreamInput.Decompressed(new PrefixedStream(head, stream, options.LeaveOpen), head, out bool compressed);
        byte[] text = head;
        if (compressed)
        {
            text = await HeadAsync(data, useAsync, ct).ConfigureAwait(false);
            data = new PrefixedStream(text, data, leaveOpen: false);
        }
        var (charset, decoder, mark) = Choose(options, text);
        if (decoder is null && options.SkipLines == 0) return new CsvOpened(new StreamInput(data, leaveOpen: false, options.UnitBytes, cut), charset, mark);
        Stream rest = decoder is null
            ? await SkipLinesAsync(data, mark, options, useAsync, ct).ConfigureAwait(false)
            : await SkipLinesAsync(Transcoded(data, decoder, mark, options), 0, options, useAsync, ct).ConfigureAwait(false);
        return new CsvOpened(new StreamInput(rest, leaveOpen: false, options.UnitBytes, cut), charset, 0) { SkippedLines = options.SkipLines };
    }

    // The text decoded into UTF-8: blocks side by side when its characters can be found from any boundary, else in order.
    private static Stream Transcoded(Stream data, TextDecoder decoder, int mark, CsvReadOptions options)
    {
        if (options.MaxDegreeOfParallelism > 1 && ParallelTextDecodingStream.Splits(decoder))
            return new ParallelTextDecodingStream(data, Factory(decoder), mark, options.MaxDegreeOfParallelism, leaveOpen: false,
                Math.Min(options.UnitBytes, 1 << 20));
        return new TextDecodingStream(data, decoder, mark, leaveOpen: false);
    }

    // Up to four first bytes.
    private static async ValueTask<byte[]> HeadAsync(Stream stream, bool useAsync, CancellationToken ct)
    {
        byte[] head = new byte[4];
        int got = 0;
        while (got < head.Length)
        {
            int n = useAsync ? await stream.ReadAsync(head.AsMemory(got), ct).ConfigureAwait(false) : stream.Read(head.AsSpan(got));
            if (n == 0) break;
            got += n;
        }
        return head[..got];
    }

    /// <summary>
    /// The text's charset, the decoder transcoding the whole input (null: its bytes are cut and tokenized as they are)
    /// and the byte order mark's length.
    /// </summary>
    private static (CsvCharset Charset, TextDecoder? Transcoder, int Mark) Choose(CsvReadOptions options, ReadOnlySpan<byte> start)
    {
        if (!TextEncodings.TryChoose(options.Encoding, start, out Encoding encoding, out int mark))
        {
            Encoding marked = TextEncodings.DetectBom(start, out _)!;
            throw new CsvFormatException($"The input starts with a {marked.WebName} byte order mark, but the encoding given is {options.Encoding!.WebName} " +
                "(give the encoding the file is in, or none to follow the mark).");
        }
        bool replace = options.InvalidText == CsvInvalidText.Replace;
        TextDecoder decoder = TextEncodings.Decoder(encoding, replace);
        bool utf8 = encoding.CodePage == 65001;
        if (!options.ForceTranscode && OnItsBytes(decoder, options))
            return (utf8 && !replace ? CsvCharset.Utf8Text(null) : CsvCharset.Fields(encoding, decoder), null, mark);
        return (CsvCharset.Utf8Text(encoding), decoder, mark);
    }

    // Whether the input can be cut and tokenized on its own bytes: ASCII bytes are ASCII characters wherever they are,
    // and no structural byte can be a double-byte character's trail.
    private static bool OnItsBytes(TextDecoder decoder, CsvReadOptions options)
    {
        if (decoder.Layout != TextLayout.AsciiCompatible) return false;
        // A comment prefix is compared on the bytes as UTF-8: in another encoding only an ASCII one (else on the text decoded).
        if (options.CommentPrefix is { } prefix && decoder is not Utf8TextDecoder && !System.Text.Ascii.IsValid(prefix)) return false;
        if (decoder is not LegacyText { DoubleByte: true }) return true;
        return options.Separator < 0x40 && (options.Quote ?? '"') < 0x40 && (options.LineEnd ?? '\n') < 0x40
            && (options.CommentPrefix ?? "").All(c => c < 0x40);
    }
}
