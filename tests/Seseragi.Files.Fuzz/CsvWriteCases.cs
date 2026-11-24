using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests;
using Seseragi.Files.Tests.Csv;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// The CSV writer cases (corpus directory <c>csv-write</c>: its Arrow files are seed frames): batches from the seeds,
/// from generated files read by <see cref="CsvReader"/>, or random columns of every type the writer takes (and, a sixth,
/// one it refuses; a sixth with a string that is not UTF-8), in managed or pooled native buffers, sliced; random options
/// over every <see cref="CsvWriteOptions"/> field. Written twice: the reference on one thread, the caller's batches,
/// one piece a batch, to a stream; the candidate with random threads, pieces, windows, ownership, re-split batches, sync
/// or async, flushes, to a stream, a file, a buffer writer, a text writer or a string. Contract: both end, or are
/// refused alike (CsvWriteException, ArgumentException, NotSupportedException: type and message); the same bytes
/// (decompressed); generated files written with options that keep their values read back to the same rows.
/// </summary>
internal sealed class CsvWriteCases(IReadOnlyList<string> seeds) : IFuzzCases
{
    public string Extension => ".csv";

    public bool HasFile(long n) => false;

    public byte[] FileOf(long seed, long n, out string source) => throw new NotSupportedException("Writer cases have no input file.");

    // With CSV_FUZZ_TALLY set to a directory, each worker writes one line per case there: what it was made of, how it ended.
    private static readonly StreamWriter? Tally = Environment.GetEnvironmentVariable("CSV_FUZZ_TALLY") is { Length: > 0 } dir
        ? new StreamWriter(Path.Combine(dir, $"tally-{Environment.ProcessId}.txt"), append: true) { AutoFlush = true }
        : null;

    public Outcome Run(long seed, long n)
    {
        List<string> traits = [];
        Outcome outcome = Compare(Cases.RandomFor(seed, n), traits);
        Tally?.WriteLine($"{string.Join(' ', traits)} {(outcome.Finding ? "finding" : outcome.Signature)}"
            + (outcome.Signature == "refused" ? $" | {Shape(outcome.Detail)}" : ""));
        return outcome;
    }

    // A refusal's message with its rows, names and numbers blanked (the kinds of refusal, counted).
    private static string Shape(string message)
    {
        string shape = System.Text.RegularExpressions.Regex.Replace(message, @"Row \d+: |'[^']*'|\d+", "_");
        return shape[..Math.Min(70, shape.Length)];
    }

    private enum Output { Stream, File, BufferWriter, Text, String }

    private Outcome Compare(Random random, List<string> traits)
    {
        List<RecordBatch> batches = [];
        try
        {
            (Schema schema, bool generated) = Input(random, batches, traits);
            bool roundTrip = generated && random.Next(2) == 0;
            CsvWriteOptions options = roundTrip ? RoundTripOptions(random) : Options(random, traits);
            traits.Add(roundTrip ? "roundtrip" : "options:any");
            var reference = Write(schema, batches, options with
            {
                MaxDegreeOfParallelism = 1, BatchOwnership = BatchOwnership.Caller, PieceBytes = 1 << 26, WindowPieces = 1,
            }, Output.Stream, async: false, null, null);

            bool text = (options.Encoding is null || options.Encoding.CodePage == 65001) && options.Compression == CsvCompression.None;
            var output = (Output)random.Next(text ? 5 : 3);
            bool useAsync = random.Next(3) == 0;
            int threads = random.Next(1, 9);
            CsvWriteOptions candidateOptions = options with
            {
                MaxDegreeOfParallelism = threads,
                BatchOwnership = random.Next(2) == 0 ? BatchOwnership.Writer : BatchOwnership.Caller,
                PieceBytes = random.Next(3) switch { 0 => random.Next(1, 64), 1 => random.Next(64, 1 << 14), _ => random.Next(1 << 14, 1 << 20) },
                WindowPieces = random.Next(3) == 0 ? random.Next(1, 8) : 0,
                WindowBytes = random.Next(2) == 0 ? random.Next(1, 1 << 16) : 8L << 20,
                InFlightBytes = random.Next(3) == 0 ? random.Next(1, 1 << 16) : 256L << 20,
            };
            List<RecordBatch> split = Resplit(random, batches);
            traits.AddRange([$"output:{output}", $"threads:{threads}", $"own:{candidateOptions.BatchOwnership}", useAsync ? "async" : "sync",
                $"enc:{options.Encoding?.CodePage.ToString() ?? "utf8"}", $"comp:{options.Compression}", $"style:{options.QuoteStyle}"]);
            var candidate = Write(schema, split, candidateOptions, output, useAsync, random.Next(4) == 0 ? new Random(random.Next()) : null, random);
            string described = $"output {output}, threads {threads}, ownership {candidateOptions.BatchOwnership}, pieces {candidateOptions.PieceBytes}, "
                + $"window {candidateOptions.WindowPieces}/{candidateOptions.WindowBytes}, in flight {candidateOptions.InFlightBytes}, async {useAsync}, "
                + $"batches {batches.Count} → {split.Count}";

            if (reference.Error is { } e1)
            {
                if (candidate.Error is not { } e2) return new Outcome(true, "refused by the reference only", $"{e1.GetType().Name}: {e1.Message}\n{described}");
                if (e1.GetType() != e2.GetType() || e1.Message != e2.Message)
                    return new Outcome(true, "different refusals", $"{e1.GetType().Name}: {e1.Message}\n{e2.GetType().Name}: {e2.Message}\n{described}");
                return Outcome.Refused(e1);
            }
            if (candidate.Error is { } e) return new Outcome(true, "refused by the candidate only", $"{e}\n{described}");
            byte[] expected = Decompressed(reference.Bytes!, options.Compression), actual;
            if (output is Output.Text or Output.String)
            {
                string expectedText = Encoding.UTF8.GetString(expected), actualText = Encoding.UTF8.GetString(candidate.Bytes!);
                if (expectedText != actualText) return Different(expectedText, actualText, described);
            }
            else
            {
                actual = Decompressed(candidate.Bytes!, options.Compression);
                if (!expected.AsSpan().SequenceEqual(actual)) return Different(Encoding.Latin1.GetString(expected), Encoding.Latin1.GetString(actual), described);
            }
            if (roundTrip && ReadBack(schema, batches, options, expected) is { } failure) return new Outcome(true, "does not read back", $"{failure}\n{described}");
            return Outcome.Read;
        }
        catch (Exception e)
        {
            return Outcome.From(e);
        }
        finally
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }

    private static Outcome Different(string expected, string actual, string described)
    {
        int at = expected.AsSpan().CommonPrefixLength(actual);
        int from = Math.Max(0, at - 60);
        return new Outcome(true, "different bytes", $"{expected.Length} vs {actual.Length} chars, first difference at {at} ({described}):\n"
            + $"reference …{Escape(expected[from..Math.Min(expected.Length, at + 60)])}\ncandidate …{Escape(actual[from..Math.Min(actual.Length, at + 60)])}");

        static string Escape(string s) => s.Replace("\r", "\\r").Replace("\n", "\\n");
    }

    // ---------------------------------------------------------------- writing

    // The output written (UTF-8 for text outputs), or the first refusal (anything else escapes as a finding).
    private static (byte[]? Bytes, Exception? Error) Write(Schema schema, List<RecordBatch> batches, CsvWriteOptions options, Output output, bool async,
        Random? flushes, Random? copies)
    {
        bool owned = options.BatchOwnership == BatchOwnership.Writer;
        string? path = output == Output.File ? Path.Combine(Path.GetTempPath(), $"csv-write-fuzz-{Environment.ProcessId}-{Guid.NewGuid():N}.csv") : null;
        int given = 0;
        try
        {
            switch (output)
            {
                case Output.String:
                    try { return (Encoding.UTF8.GetBytes(CsvWriter.WriteString(schema, Feed(), options)), null); }
                    catch (Exception e) when (Expected(e)) { return (null, e); }
                case Output.Text:
                {
                    using var text = new System.IO.StringWriter();
                    Exception? error = Run(() => CsvWriter.Create(text, schema, options));
                    return error is null ? (Encoding.UTF8.GetBytes(text.ToString()), null) : (null, error);
                }
                case Output.BufferWriter:
                {
                    var buffer = new ArrayBufferWriter<byte>();
                    Exception? error = Run(() => CsvWriter.Create(buffer, schema, options));
                    return error is null ? (buffer.WrittenSpan.ToArray(), null) : (null, error);
                }
                case Output.File:
                {
                    Exception? error = Run(() => CsvWriter.Create(path!, schema, options));
                    return error is null ? (File.ReadAllBytes(path!), null) : (null, error);
                }
                default:
                {
                    using var stream = new MemoryStream();
                    Exception? error = Run(() => CsvWriter.Create(stream, schema, options));
                    return error is null ? (stream.ToArray(), null) : (null, error);
                }
            }
        }
        finally
        {
            if (path is not null) File.Delete(path);
        }

        // The batches as the writer takes them: owned ones copied (it disposes them), the caller's as they are.
        IEnumerable<RecordBatch> Feed()
        {
            foreach (RecordBatch batch in batches)
            {
                given++;
                yield return owned ? Copy(batch, copies) : batch;
            }
        }

        Exception? Run(Func<CsvWriter> create)
        {
            CsvWriter writer;
            try { writer = create(); }
            catch (Exception e) when (Expected(e)) { return e; }
            try
            {
                foreach (RecordBatch batch in batches)
                {
                    RecordBatch handed = owned ? Copy(batch, copies) : batch;
                    if (async) writer.WriteAsync(handed).AsTask().GetAwaiter().GetResult();
                    else writer.Write(handed);
                    if (flushes?.Next(3) == 0)
                    {
                        if (async) writer.FlushAsync().AsTask().GetAwaiter().GetResult();
                        else writer.Flush();
                    }
                }
                if (async) writer.CompleteAsync().AsTask().GetAwaiter().GetResult();
                else writer.Complete();
                return null;
            }
            catch (Exception e) when (Expected(e))
            {
                writer.Abort();
                return e;
            }
            finally
            {
                if (async) writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
                else writer.Dispose();
            }
        }
    }

    // The refusals of the contract.
    private static bool Expected(Exception e) => e is CsvWriteException or ArgumentException or NotSupportedException;

    private static byte[] Decompressed(byte[] bytes, CsvCompression compression)
    {
        if (compression == CsvCompression.None) return bytes;
        using Stream unpacked = compression == CsvCompression.Gzip
            ? new GZipStream(new MemoryStream(bytes), CompressionMode.Decompress)
            : new ZstdSharp.DecompressionStream(new MemoryStream(bytes));
        using var plain = new MemoryStream();
        unpacked.CopyTo(plain);
        return plain.ToArray();
    }

    // The batches cut again at random rows (slices of the same buffers), empty slices among them.
    private static List<RecordBatch> Resplit(Random random, List<RecordBatch> batches)
    {
        if (random.Next(3) == 0) return batches;
        List<RecordBatch> split = [];
        foreach (RecordBatch batch in batches)
            for (int at = 0; at < batch.Length;)
            {
                int length = Math.Min(batch.Length - at, random.Next(4) == 0 ? random.Next(0, 3) : random.Next(1, Math.Max(2, batch.Length)));
                split.Add(batch.Slice(at, length));
                at += length;
            }
        return split;
    }

    // A deep copy (an owned batch the writer disposes): buffers in managed or pooled native memory.
    private static RecordBatch Copy(RecordBatch batch, Random? random)
    {
        bool native = random?.Next(2) == 0;
        return new RecordBatch(batch.Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.BuildArray(CopyData(a.Data, native)))], batch.Length);
    }

    private static ArrayData CopyData(ArrayData data, bool native) => new(data.DataType, data.Length, data.NullCount, data.Offset,
        [.. data.Buffers.Select(b => Buffer(b.Span, native))], [.. (data.Children ?? []).Select(c => CopyData(c, native))],
        data.Dictionary is { } d ? CopyData(d, native) : null);

    private static ArrowBuffer Buffer(ReadOnlySpan<byte> bytes, bool native)
    {
        if (bytes.IsEmpty) return ArrowBuffer.Empty;
        if (!native) return new ArrowBuffer(bytes.ToArray());
        return new ArrowBuffer.Builder<byte>(bytes.Length).Append(bytes).Build(PooledArrowAllocator.Instance);
    }

    // ---------------------------------------------------------------- reading back

    // Generated rows written with options that keep them, read back with the schema: the first difference, or null.
    private static string? ReadBack(Schema schema, List<RecordBatch> batches, CsvWriteOptions options, byte[] written)
    {
        var back = new CsvReadOptions
        {
            Schema = schema,
            HasHeader = options.IncludeHeader,
            Separator = options.Separator,
            Quote = options.Quote,
            NullValues = options.NullValue.Length > 0 ? [options.NullValue] : null,
            DecimalComma = options.DecimalComma,
            ThousandsSeparator = options.ThousandsSeparator,
        };
        using CsvReader reader = CsvReader.Open(new MemoryStream(written), back);
        List<RecordBatch> again = [.. reader.ReadBatches()];
        try
        {
            List<string> expected = [.. batches.SelectMany(ArrowRender.Rows)], actual = [.. again.SelectMany(ArrowRender.Rows)];
            if (expected.SequenceEqual(actual)) return null;
            int at = expected.Zip(actual).TakeWhile(p => p.First == p.Second).Count();
            return $"{expected.Count} vs {actual.Count} rows; row {at}:\n{expected.ElementAtOrDefault(at)}\n{actual.ElementAtOrDefault(at)}";
        }
        finally { again.ForEach(b => b.Dispose()); }
    }

    // ---------------------------------------------------------------- options

    // Options a CSV reader reads back to the same values (the writer tests' round trip).
    private static CsvWriteOptions RoundTripOptions(Random random)
    {
        CsvQuoteStyle style = random.Next(3) switch { 0 => CsvQuoteStyle.Always, 1 => CsvQuoteStyle.NonNumeric, _ => CsvQuoteStyle.Necessary };
        bool comma = random.Next(3) == 0;
        string? group = random.Next(4) != 0 ? null : comma ? ((string[])[".", " ", "'"])[random.Next(3)] : ((string[])[" ", "'", ","])[random.Next(3)];
        return new CsvWriteOptions
        {
            IncludeBom = random.Next(4) == 0,
            FloatScientific = random.Next(3) switch { 0 => true, 1 => false, _ => null },
            DecimalComma = comma,
            ThousandsSeparator = group,
            Separator = random.Next(4) switch { 0 => ';', 1 => '\t', 2 => '|', _ => ',' },
            Quote = random.Next(4) == 0 && group != "'" ? '\'' : '"',
            QuoteStyle = style,
            NullValue = style == CsvQuoteStyle.Always || random.Next(3) == 0 ? "<null>" : "",
            LineTerminator = random.Next(2) == 0 ? "\n" : "\r\n",
        };
    }

    private static readonly int[] CodePages = [932, 936, 949, 950, 1252, 28591, 1200, 1201, 54936, 12000, 65001];

    private static readonly string[] DotNetDates = ["yyyy-MM-dd", "dd/MM/yyyy", "d", "D", "MMM d, yyyy", "yyyyMMdd", "'Q'yyyy", "dddd", "M", "y"];
    private static readonly string[] DotNetTimes = ["HH:mm", "HH:mm:ss.fffffff", "t", "T", "h:mm tt", "HHmmss", "FFFFFFF", "HH:mm:ss.ffff"];
    private static readonly string[] DotNetStamps = ["o", "s", "u", "r", "yyyy-MM-ddTHH:mm:ss.fffK", "yyyy-MM-dd HH:mm zzz", "g", "G", "f", "F", "dd MMM yyyy HH:mm:ss.FFFFFFF"];
    private static readonly string[] ChronoDates = ["%Y-%m-%d", "%d/%m/%Y", "%F", "%j", "%C%y", "%e %b %Y", "%A %B", "%Y%m%d", "%Q"];
    private static readonly string[] ChronoTimes = ["%H:%M", "%T", "%H:%M:%S%.f", "%I:%M %p", "%k", "%.3f", "%R"];
    private static readonly string[] ChronoStamps = ["%Y-%m-%dT%H:%M:%S%.f", "%F %T%:z", "%s", "%c", "%Y-%m-%d %H:%M:%S %Z", "%FT%T%.9f%z", "%D %r"];

    private static T Pick<T>(Random random, T[] items) => items[random.Next(items.Length)];

    // Options over every field, a few of them ones the writer refuses.
    private static CsvWriteOptions Options(Random random, List<string> traits)
    {
        string? Format(string[] net, string[] chrono) => random.Next(4) switch { 0 => Pick(random, net), 1 => Pick(random, chrono), _ => null };
        var options = new CsvWriteOptions
        {
            IncludeHeader = random.Next(5) != 0,
            IncludeBom = random.Next(6) == 0,
            // Now and then a separator or quote the writer refuses (a line end, the other's character).
            Separator = random.Next(30) == 0 ? Pick(random, (char[])['"', '\n', '\r']) : Pick(random, (char[])[',', ',', ';', '\t', '|', ' ', 'x']),
            Quote = random.Next(30) == 0 ? ',' : Pick(random, (char[])['"', '"', '\'', '`']),
            QuoteStyle = (CsvQuoteStyle)random.Next(4),
            NullValue = Pick(random, (string[])["", "", "NA", "\\N", "null", "日本", ",", "\"q\"", "a\nb"]),
            LineTerminator = Pick(random, (string[])["\n", "\n", "\r\n", "\r", ";", "", "<eol>"]),
            FloatScientific = random.Next(3) switch { 0 => true, 1 => false, _ => null },
            FloatPrecision = random.Next(4) == 0 ? random.Next(0, random.Next(4) == 0 ? 400 : 12) : null,
            DecimalComma = random.Next(4) == 0,
            ThousandsSeparator = random.Next(5) == 0 ? Pick(random, (string[])[" ", ".", "'", ",", "_", "\u00A0", "\""]) : null,
            DateFormat = Format(DotNetDates, ChronoDates),
            TimeFormat = Format(DotNetTimes, ChronoTimes),
            DatetimeFormat = Format(DotNetStamps, ChronoStamps),
            Compression = random.Next(5) switch { 0 => CsvCompression.Gzip, 1 => CsvCompression.Zstd, _ => CsvCompression.None },
        };
        if (options.Compression != CsvCompression.None && random.Next(3) == 0)
            options = options with { CompressionLevel = random.Next(10) == 0 ? random.Next(-2, 30) : options.Compression == CsvCompression.Gzip ? random.Next(0, 10) : random.Next(1, 20) };
        if (random.Next(4) == 0)
        {
            int page = Pick(random, CodePages);
            options = options with { Encoding = TextEncodings.Resolve(page), InvalidText = random.Next(2) == 0 ? CsvInvalidText.Replace : CsvInvalidText.Error };
        }
        if (options.DateFormat is not null || options.TimeFormat is not null || options.DatetimeFormat is not null) traits.Add("formats");
        if (options.FloatPrecision is not null || options.FloatScientific is not null) traits.Add("floats");
        if (options.DecimalComma || options.ThousandsSeparator is not null) traits.Add("numbers");
        return options;
    }

    // ---------------------------------------------------------------- batches

    private static readonly string[] Pieces = ["a", "b", "é", "日本", ",", ";", "\t", "|", "\"", "'", "\r", "\n", " ", "x1", "0", "😀", "`", "<null>", "NA"];

    // The batches of a case (added to `batches`, disposed by the caller); true when they come from a generated file.
    private (Schema Schema, bool Generated) Input(Random random, List<RecordBatch> batches, List<string> traits)
    {
        int kind = random.Next(seeds.Count > 0 ? 6 : 4);
        if (kind < 2)
        {
            traits.Add("input:generated");
            for (int attempt = 0; ; attempt++)
            {
                var (bytes, readOptions) = CsvGenerator.Generate(random);
                try
                {
                    using CsvReader reader = CsvReader.Open(new MemoryStream(bytes), readOptions with { RowIndexColumn = null });
                    batches.AddRange(reader.ReadBatches());
                    return (reader.Schema, true);
                }
                catch (Exception e) when (e is CsvFormatException or CsvValueException && attempt < 20)
                {
                    batches.ForEach(b => b.Dispose());
                    batches.Clear();
                }
            }
        }
        if (kind >= 4)
        {
            traits.Add("input:seed");
            using IpcFile file = IpcFile.Open(seeds[random.Next(seeds.Count)]);
            using IpcReader reader = file.OpenReader();
            batches.AddRange(reader.ReadBatches());
            return (reader.Schema, false);
        }
        traits.Add("input:random");
        int rows = random.Next(4) == 0 ? random.Next(0, 4) : random.Next(1, random.Next(2) == 0 ? 64 : 2000);
        int columns = random.Next(1, 9);
        bool native = random.Next(2) == 0, refused = random.Next(6) == 0, notUtf8 = random.Next(6) == 0;
        List<Field> fields = [];
        List<IArrowArray> arrays = [];
        for (int c = 0; c < columns; c++)
        {
            string name = Pick(random, (string[])["a", "b", "c d", "q\"x", "", "名", "x,y", "n\nl", "a"]) + (random.Next(2) == 0 ? c.ToString() : "");
            var (field, array) = Column(random, name, rows, native, refused && c == 0, notUtf8 && c == columns - 1, traits);
            fields.Add(field);
            arrays.Add(array);
        }
        if (refused) traits.Add("refused-type");
        if (notUtf8) traits.Add("not-utf8");
        var schema = new Schema(fields, null);
        var whole = new RecordBatch(schema, arrays, rows);
        if (random.Next(3) == 0 && rows > 2)
        {
            // Slices with offsets (the writer reads rows past their arrays' offsets).
            int from = random.Next(1, rows);
            batches.Add(whole.Slice(0, from));
            batches.Add(whole.Slice(from, rows - from));
            batches.Add(whole);
            traits.Add("sliced");
        }
        else batches.Add(whole);
        return (schema, false);
    }

    // A random column of `rows` rows: its field and array (buffers in managed or pooled native memory).
    private static (Field Field, IArrowArray Array) Column(Random random, string name, int rows, bool native, bool refused, bool notUtf8, List<string> traits)
    {
        bool[] nulls = [.. Enumerable.Range(0, rows).Select(_ => random.Next(6) == 0)];
        if (refused)
        {
            traits.Add("type:binary");
            var type = random.Next(2) == 0 ? (IArrowType)BinaryType.Default : new ListType(Int32Type.Default);
            ArrayData data = type is BinaryType
                ? Variable(BinaryType.Default, rows, nulls, i => [(byte)i, 0xFF], native)
                : new ArrayData(type, rows, 0, 0, [ArrowBuffer.Empty, Buffer(Ints(rows + 1, i => 0), native)], [Fixed(Int32Type.Default, 0, [], 4, (_, _) => { }, native)]);
            return (new Field(name, type, true), ArrowArrayFactory.BuildArray(data));
        }
        if (notUtf8)
        {
            traits.Add("type:string-not-utf8");
            return (new Field(name, StringType.Default, true), ArrowArrayFactory.BuildArray(Variable(StringType.Default, rows, nulls,
                i => i % 3 == 1 ? [(byte)'o', 0xC3, 0x28] : Encoding.UTF8.GetBytes($"s{i}"), native)));
        }
        int kind = random.Next(22);
        traits.Add($"type:{kind}");
        long Long() => random.Next(6) switch { 0 => long.MinValue, 1 => long.MaxValue, 2 => 0, _ => random.NextInt64(-1_000_000_000_000, 1_000_000_000_000) };
        switch (kind)
        {
            case 0: return (new Field(name, Int8Type.Default, true), Build(Int8Type.Default, 1, (s, i) => s[0] = (byte)random.Next(256)));
            case 1: return (new Field(name, Int16Type.Default, true), Build(Int16Type.Default, 2, (s, i) => BinaryPrimitives.WriteInt16LittleEndian(s, (short)random.Next(short.MinValue, short.MaxValue + 1))));
            case 2: return (new Field(name, Int32Type.Default, true), Build(Int32Type.Default, 4, (s, i) => BinaryPrimitives.WriteInt32LittleEndian(s, random.Next(int.MinValue, int.MaxValue))));
            case 3: return (new Field(name, Int64Type.Default, true), Build(Int64Type.Default, 8, (s, i) => BinaryPrimitives.WriteInt64LittleEndian(s, Long())));
            case 4: return (new Field(name, UInt64Type.Default, true), Build(UInt64Type.Default, 8, (s, i) => BinaryPrimitives.WriteUInt64LittleEndian(s, (ulong)Long())));
            case 5: return (new Field(name, UInt8Type.Default, true), Build(UInt8Type.Default, 1, (s, i) => s[0] = (byte)random.Next(256)));
            case 6: return (new Field(name, HalfFloatType.Default, true), Build(HalfFloatType.Default, 2, (s, i) => BinaryPrimitives.WriteHalfLittleEndian(s, (Half)Double())));
            case 7: return (new Field(name, FloatType.Default, true), Build(FloatType.Default, 4, (s, i) => BinaryPrimitives.WriteSingleLittleEndian(s, (float)Double())));
            case 8: return (new Field(name, DoubleType.Default, true), Build(DoubleType.Default, 8, (s, i) => BinaryPrimitives.WriteDoubleLittleEndian(s, Double())));
            case 9:
            {
                var type = new Decimal128Type(38, random.Next(-3, 21));
                return (new Field(name, type, true), Build(type, 16, (s, i) => BinaryPrimitives.WriteInt128LittleEndian(s, random.Next(4) == 0 ? Int128.MaxValue / random.Next(1, 1000) : (Int128)Long() * random.Next(-1000, 1000))));
            }
            case 10:
            {
                var type = new Decimal64Type(18, random.Next(0, 10));
                return (new Field(name, type, true), Build(type, 8, (s, i) => BinaryPrimitives.WriteInt64LittleEndian(s, Long() % 1_000_000_000_000_000_000L)));
            }
            case 11:
            {
                bool unsigned = random.Next(2) == 0;
                var field = new Field(name, WideIntegers.Type, true, [new(WideIntegers.MetadataKey, unsigned ? WideIntegers.Unsigned : WideIntegers.Signed)]);
                return (field, Build(WideIntegers.Type, 16, (s, i) => BinaryPrimitives.WriteInt128LittleEndian(s, random.Next(3) switch
                {
                    0 => unsigned ? Int128.MaxValue : Int128.MinValue,
                    1 => (Int128)Long() * Long(),
                    _ => unsigned ? Long() & long.MaxValue : Long(),
                })));
            }
            case 12: return (new Field(name, BooleanType.Default, true), ArrowArrayFactory.BuildArray(Bits(rows, nulls, native, random)));
            case 13:
            case 14:
            {
                IArrowType type = kind == 13 ? StringType.Default : LargeStringType.Default;
                return (new Field(name, type, true), ArrowArrayFactory.BuildArray(Variable(type, rows, nulls, _ => Encoding.UTF8.GetBytes(Text()), native)));
            }
            case 15:
            {
                var builder = new StringViewArray.Builder();
                for (int i = 0; i < rows; i++) _ = nulls[i] ? builder.AppendNull() : builder.Append(Text());
                IArrowArray built = builder.Build(PooledArrowAllocator.Instance);
                return (new Field(name, StringViewType.Default, true), native ? built : ArrowArrayFactory.BuildArray(CopyData(built.Data, false)));
            }
            case 16:
            {
                // A categorical: int indices into a few strings.
                string[] words = [.. Enumerable.Range(0, random.Next(1, 6)).Select(_ => Text())];
                ArrayData dictionary = Variable(StringType.Default, words.Length, new bool[words.Length], i => Encoding.UTF8.GetBytes(words[i]), native);
                var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
                ArrayData indices = Fixed(Int32Type.Default, rows, nulls, 4, (s, i) => BinaryPrimitives.WriteInt32LittleEndian(s, random.Next(words.Length)), native);
                return (new Field(name, type, true), new DictionaryArray(type, ArrowArrayFactory.BuildArray(indices), ArrowArrayFactory.BuildArray(dictionary)));
            }
            case 17:
            {
                IArrowType type = random.Next(2) == 0 ? Date32Type.Default : Date64Type.Default;
                return (new Field(name, type, true), type is Date32Type
                    ? Build(type, 4, (s, i) => BinaryPrimitives.WriteInt32LittleEndian(s, random.Next(-800_000, 3_000_000)))
                    : Build(type, 8, (s, i) => BinaryPrimitives.WriteInt64LittleEndian(s, random.NextInt64(-800_000, 3_000_000) * 86_400_000 + (random.Next(5) == 0 ? random.Next(-5, 5) : 0))));
            }
            case 18:
            {
                // Times of day (a few not: refused as the reference refuses them).
                TimeUnit unit = (TimeUnit)random.Next(4);
                long perDay = unit switch { TimeUnit.Second => 86_400, TimeUnit.Millisecond => 86_400_000, TimeUnit.Microsecond => 86_400_000_000, _ => 86_400_000_000_000 };
                bool bad = random.Next(20) == 0;
                long Time() => bad && random.Next(50) == 0 ? perDay : random.NextInt64(0, perDay);
                IArrowType type = unit <= TimeUnit.Millisecond ? new Time32Type(unit) : new Time64Type(unit);
                return (new Field(name, type, true), unit <= TimeUnit.Millisecond
                    ? Build(type, 4, (s, i) => BinaryPrimitives.WriteInt32LittleEndian(s, (int)Time()))
                    : Build(type, 8, (s, i) => BinaryPrimitives.WriteInt64LittleEndian(s, Time())));
            }
            case 19:
            case 20:
            {
                TimeUnit unit = (TimeUnit)random.Next(4);
                string? zone = kind == 19 ? null : Pick(random, (string[])["UTC", "+05:45", "-03:30", "America/New_York", "Asia/Tokyo", "Europe/Amsterdam", "Australia/Lord_Howe"]);
                long scale = unit switch { TimeUnit.Second => 1, TimeUnit.Millisecond => 1_000, TimeUnit.Microsecond => 1_000_000, _ => 1_000_000_000 };
                var type = new TimestampType(unit, zone);
                return (new Field(name, type, true), Build(type, 8, (s, i) => BinaryPrimitives.WriteInt64LittleEndian(s,
                    random.Next(4) == 0 ? random.NextInt64(long.MinValue / scale / 2, long.MaxValue / scale / 2) * (scale == 1_000_000_000 ? 1 : 1) : random.NextInt64(-6_000_000_000, 8_000_000_000) * scale + random.NextInt64(0, scale))));
            }
            default:
            {
                if (random.Next(2) == 0) return (new Field(name, NullType.Default, true), new NullArray(rows));
                DurationType type = random.Next(4) switch { 0 => DurationType.Second, 1 => DurationType.Millisecond, 2 => DurationType.Microsecond, _ => DurationType.Nanosecond };
                return (new Field(name, type, true), Build(type, 8, (s, i) => BinaryPrimitives.WriteInt64LittleEndian(s, Long())));
            }
        }

        IArrowArray Build(IArrowType type, int width, Action<Span<byte>, int> value) => ArrowArrayFactory.BuildArray(Fixed(type, rows, nulls, width, value, native));

        double Double() => random.Next(12) switch
        {
            0 => double.NaN,
            1 => random.Next(2) == 0 ? double.PositiveInfinity : double.NegativeInfinity,
            2 => random.Next(2) == 0 ? 0.0 : -0.0,
            3 => BitConverter.Int64BitsToDouble(random.NextInt64(1, 1L << 52)), // subnormal
            4 => Math.Round(random.NextDouble() * 1e6, random.Next(0, 4)),       // short decimals
            5 => random.NextInt64(-1L << 53, 1L << 53),                           // whole numbers
            6 => (random.NextDouble() - 0.5) * Math.Pow(10, random.Next(-30, 30)),
            7 => BitConverter.Int64BitsToDouble(random.NextInt64()) is var d && double.IsFinite(d) ? d : 1.5,
            _ => (random.NextDouble() - 0.5) * 200,
        };

        string Text()
        {
            int length = random.Next(4) == 0 ? random.Next(10, 40) : random.Next(0, 14);
            var text = new StringBuilder();
            while (Encoding.UTF8.GetByteCount(text.ToString()) < length) text.Append(Pick(random, random.Next(3) == 0 ? Pieces : ["a", "b"]));
            return text.ToString();
        }
    }

    private static byte[] Ints(int count, Func<int, int> value)
    {
        var bytes = new byte[4 * count];
        for (int i = 0; i < count; i++) BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4 * i), value(i));
        return bytes;
    }

    // A fixed-width column: values (null rows hold garbage, as producers leave them) and validity when there are nulls.
    private static ArrayData Fixed(IArrowType type, int rows, bool[] nulls, int width, Action<Span<byte>, int> value, bool native)
    {
        var values = new byte[Math.Max(1, rows * width)];
        for (int i = 0; i < rows; i++) value(values.AsSpan(i * width, width), i);
        (byte[]? validity, int nullCount) = Validity(nulls);
        return new ArrayData(type, rows, nullCount, 0, [validity is null ? ArrowBuffer.Empty : Buffer(validity, native), Buffer(values.AsSpan(0, rows * width), native)]);
    }

    private static ArrayData Bits(int rows, bool[] nulls, bool native, Random random)
    {
        var values = new byte[(rows + 7) / 8];
        for (int i = 0; i < rows; i++)
            if (random.Next(2) == 0) values[i >> 3] |= (byte)(1 << (i & 7));
        (byte[]? validity, int nullCount) = Validity(nulls);
        return new ArrayData(BooleanType.Default, rows, nullCount, 0, [validity is null ? ArrowBuffer.Empty : Buffer(validity, native), Buffer(values, native)]);
    }

    // Variable-width values (strings, binary; 64-bit offsets for large strings).
    private static ArrayData Variable(IArrowType type, int rows, bool[] nulls, Func<int, byte[]> value, bool native)
    {
        bool large = type is LargeStringType;
        List<byte> data = [];
        var offsets = new byte[(rows + 1) * (large ? 8 : 4)];
        for (int i = 0; i < rows; i++)
        {
            if (!nulls[i]) data.AddRange(value(i));
            if (large) BinaryPrimitives.WriteInt64LittleEndian(offsets.AsSpan(8 * (i + 1)), data.Count);
            else BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(4 * (i + 1)), data.Count);
        }
        (byte[]? validity, int nullCount) = Validity(nulls);
        return new ArrayData(type, rows, nullCount, 0, [validity is null ? ArrowBuffer.Empty : Buffer(validity, native), Buffer(offsets, native), Buffer([.. data], native)]);
    }

    private static (byte[]? Bits, int Nulls) Validity(bool[] nulls)
    {
        int count = nulls.Count(n => n);
        if (count == 0) return (null, 0);
        var bits = new byte[(nulls.Length + 7) / 8];
        for (int i = 0; i < nulls.Length; i++)
            if (!nulls[i]) bits[i >> 3] |= (byte)(1 << (i & 7));
        return (bits, count);
    }
}
