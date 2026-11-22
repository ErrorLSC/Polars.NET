using System.IO.Compression;
using Apache.Arrow;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json;
using Seseragi.Files.Tests;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// The JSON cases: a corpus file (TestData/json: newline-delimited .ndjson, documents .json) with a few mutations aimed
/// in part at line and element starts, sometimes gzipped, read in its layout twice: once as the reference (one thread,
/// defaults) and once with random options (unit size, threads, source kind, row limit, a given schema, ignoring errors).
/// Contract: a read ends or is refused with JsonFormatException / JsonValueException, and the two reads agree (the same
/// rows, the row limit's first ones, or the same refusal). Half the inputs read are written back with random options
/// (layout, threads, pieces, budget, compression): the bytes, decompressed, are those of a one-thread write, and they
/// read back (with the schema) as the rows read.
/// </summary>
internal sealed class JsonCases(IReadOnlyList<string> corpus) : IFuzzCases
{
    public string Extension => ".ndjson";

    public bool HasFile(long n) => true;

    public Outcome Run(long seed, long n)
    {
        Random random = Cases.RandomFor(seed, n);
        try
        {
            byte[] bytes = Mutate(random, out string source);
            if (random.Next(8) == 0) bytes = Gzip(bytes);
            JsonFormat format = FormatOf(source);
            Schema? given = random.Next(3) == 0 ? SchemaOf(File.ReadAllBytes(source), format) : null;
            bool ignore = random.Next(4) == 0;
            var reference = Read(bytes, new JsonReadOptions { Format = format, Schema = given, IgnoreErrors = ignore, MaxDegreeOfParallelism = 1 }, 0);
            long? max = random.Next(4) == 0 ? random.Next(0, 300) : null;
            bool indexed = random.Next(4) == 0;
            var options = new JsonReadOptions
            {
                Format = format,
                Schema = given,
                IgnoreErrors = ignore,
                MaxDegreeOfParallelism = random.Next(1, 5),
                UnitBytes = random.Next(3) switch { 0 => random.Next(1, 64), 1 => random.Next(64, 8192), _ => 1 << 20 },
                MaxRows = max,
                BatchSize = random.Next(2) == 0 ? random.Next(1, 100) : JsonReadOptions.DefaultBatchSize,
                RowIndexColumn = indexed ? "__fuzz_row_index__" : null,
            };
            int kind = random.Next(4);
            var candidate = Read(bytes, options, kind);
            string described = $"{format}, source {kind}, threads {options.MaxDegreeOfParallelism}, unit {options.UnitBytes}, batch {options.BatchSize}, max {max}, schema given {given is not null}, ignore {ignore}, row index {indexed}, gzip {bytes.Length > 1 && bytes[0] == 0x1F}";
            if (reference.Error is { } e1)
            {
                // A limited read may stop before the refused line.
                if (candidate.Error is { } e2 && (e1.GetType() != e2.GetType() || e1.Message != e2.Message))
                    return new Outcome(true, "different refusals", $"{e1.Message}\n{e2.Message}\n{described}");
                if (candidate.Error is null && max is null) return new Outcome(true, "refused once only", e1.Message);
                return Outcome.Refused(e1);
            }
            if (candidate.Error is { } e) return new Outcome(true, "refused the second time only", e.ToString());
            var expected = max is long m ? reference.Rows!.Take((int)m).ToList() : reference.Rows!;
            // The row index numbers the records read first.
            if (indexed) expected = [.. expected.Select((row, i) => row.Length == 0 ? $"{i}" : $"{i} | {row}")];
            if (!expected.SequenceEqual(candidate.Rows!))
            {
                int at = expected.Zip(candidate.Rows!).TakeWhile(p => p.First == p.Second).Count();
                return new Outcome(true, "different rows", $"{expected.Count} vs {candidate.Rows!.Count} rows ({described}); row {at}:\n{expected.ElementAtOrDefault(at)}\n{candidate.Rows!.ElementAtOrDefault(at)}");
            }
            if (random.Next(2) == 0 && WriteBack(random, bytes, format, given, ignore, reference.Rows!) is { } written) return written;
            return Outcome.Read;
        }
        catch (Exception e)
        {
            return Outcome.From(e);
        }
    }

    // The rows read, written back: the same bytes whatever the options, read back as the same rows.
    private static Outcome? WriteBack(Random random, byte[] bytes, JsonFormat format, Schema? given, bool ignore, List<string> rows)
    {
        using JsonReader reader = JsonReader.Open(new MemoryStream(bytes), new JsonReadOptions { Format = format, Schema = given, IgnoreErrors = ignore, MaxDegreeOfParallelism = 1 });
        List<RecordBatch> batches = [.. reader.ReadBatches()];
        try
        {
            JsonFormat layout = random.Next(2) == 0 ? JsonFormat.Lines : JsonFormat.Document;
            string text = JsonWriter.WriteString(reader.Schema, batches, new JsonWriteOptions { Format = layout, MaxDegreeOfParallelism = 1 });
            var compression = (JsonCompression)random.Next(3);
            var options = new JsonWriteOptions
            {
                Format = layout,
                Compression = compression,
                MaxDegreeOfParallelism = random.Next(1, 5),
                PieceBytes = random.Next(2) == 0 ? random.Next(1, 4096) : 1 << 20,
                InFlightBytes = random.Next(2) == 0 ? random.Next(1, 100_000) : 256L << 20,
            };
            using var output = new MemoryStream();
            JsonWriter.Write(output, reader.Schema, batches, options with { LeaveOpen = true });
            byte[] written = output.ToArray();
            if (compression != JsonCompression.None)
            {
                using Stream unpacked = compression == JsonCompression.Gzip
                    ? new GZipStream(new MemoryStream(written), CompressionMode.Decompress)
                    : new ZstdSharp.DecompressionStream(new MemoryStream(written));
                using var plain = new MemoryStream();
                unpacked.CopyTo(plain);
                written = plain.ToArray();
            }
            string described = $"{layout}, {compression}, threads {options.MaxDegreeOfParallelism}, piece {options.PieceBytes}, in flight {options.InFlightBytes}";
            if (!System.Text.Encoding.UTF8.GetBytes(text).AsSpan().SequenceEqual(written))
                return new Outcome(true, "written differently", described);
            using JsonReader back = JsonReader.Parse(text, new JsonReadOptions { Format = layout, Schema = reader.Schema });
            List<string> read = [];
            foreach (RecordBatch batch in back.ReadBatches())
                using (batch) read.AddRange(ArrowRender.Rows(batch));
            if (!read.SequenceEqual(rows))
            {
                int at = read.Zip(rows).TakeWhile(p => p.First == p.Second).Count();
                return new Outcome(true, "written rows read back differently", $"{described}; row {at}:\n{rows.ElementAtOrDefault(at)}\n{read.ElementAtOrDefault(at)}");
            }
            return null;
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    public byte[] FileOf(long seed, long n, out string source) => Mutate(Cases.RandomFor(seed, n), out source);

    private byte[] Mutate(Random random, out string source)
    {
        source = corpus[random.Next(corpus.Count)];
        byte[] original = File.ReadAllBytes(source);
        // Line starts, and element starts in a compact document.
        long[] hot = [.. Enumerable.Range(0, original.Length).Where(i => i == 0 || original[i - 1] == '\n' || original[i - 1] == ',' && original[i] == '{')
            .Select(i => (long)i).Where((_, k) => k % 7 == 0)];
        return Mutations.Apply(random, original, hot, 0, r => File.ReadAllBytes(corpus[r.Next(corpus.Count)]));
    }

    private static JsonFormat FormatOf(string path) => path.EndsWith(".ndjson", StringComparison.Ordinal) ? JsonFormat.Lines : JsonFormat.Document;

    private static Schema? SchemaOf(byte[] original, JsonFormat format)
    {
        try
        {
            using var reader = JsonReader.Open(new MemoryStream(original), new JsonReadOptions { Format = format, InferSchemaLength = null });
            return reader.Schema;
        }
        catch (Exception e) when (e is JsonFormatException or JsonValueException) { return null; }
    }

    // The rows read, or the expected refusal (anything else escapes as a finding).
    private static (List<string>? Rows, Exception? Error) Read(byte[] bytes, JsonReadOptions options, int source)
    {
        try
        {
            using JsonReader reader = source switch
            {
                1 => JsonReader.Open(new NonSeekable(new MemoryStream(bytes)), options),
                2 => JsonReader.Open(new BytesAccess(bytes), options),
                _ => JsonReader.Open(new MemoryStream(bytes), options),
            };
            List<string> rows = [];
            foreach (RecordBatch batch in reader.ReadBatches())
                using (batch) rows.AddRange(ArrowRender.Rows(batch));
            return (rows, null);
        }
        catch (Exception e) when (e is JsonFormatException or JsonValueException)
        {
            return (null, e);
        }
    }

    private static byte[] Gzip(byte[] data)
    {
        using var output = new MemoryStream();
        using (var gz = new GZipStream(output, CompressionLevel.Fastest)) gz.Write(data);
        return output.ToArray();
    }

    private sealed class NonSeekable(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, Math.Min(count, 997));
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class BytesAccess(byte[] bytes) : IRandomAccess
    {
        public long Length => bytes.Length;

        public int ReadAt(long offset, Span<byte> destination)
        {
            int n = (int)Math.Max(0, Math.Min(destination.Length, bytes.Length - offset));
            bytes.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ReadAt(offset, destination.Span));
    }
}
