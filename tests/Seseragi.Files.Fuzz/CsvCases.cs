using System.Text;
using Apache.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Csv;
using Seseragi.Files.Tests;
using Seseragi.Files.Tests.Csv;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// The CSV cases: half generated files (<see cref="CsvGenerator.Generate"/>, with the options they are read
/// with), half corpus seeds (excerpts of real files; options from the name: <c>semicolon</c>, <c>tab</c>,
/// <c>cp932</c>, <c>cp1252</c>, <c>skipN</c>); a few mutations aimed in part at record starts, quotes and separators; a
/// third re-encoded (<see cref="CsvGenerator.Encode"/>); a sixth with other options (comments, skips,
/// truncation, nulls, ignored errors, decimal comma, inference lengths). Read twice: the reference in one unit on one
/// thread from memory, the candidate with random units (down to one byte), threads, batches, a row limit, a row index,
/// a column subset, from memory, guarded memory, streams or a temporary file. Contract: both end or are refused alike
/// (CsvFormatException, CsvValueException, ArgumentException), with the same schema and rows (the candidate's limit
/// and projection applied to the reference's).
/// </summary>
internal sealed class CsvCases(IReadOnlyList<string> corpus) : IFuzzCases
{
    public string Extension => ".csv";

    public bool HasFile(long n) => true;

    // With CSV_FUZZ_TALLY set to a directory, each worker writes one line per case there: what the case was made of
    // and how it ended (the coverage facts).
    private static readonly StreamWriter? Tally = Environment.GetEnvironmentVariable("CSV_FUZZ_TALLY") is { Length: > 0 } dir
        ? new StreamWriter(Path.Combine(dir, $"tally-{Environment.ProcessId}.txt"), append: true) { AutoFlush = true }
        : null;

    public byte[] FileOf(long seed, long n, out string source) => Input(Cases.RandomFor(seed, n), out source, out _, null);

    public Outcome Run(long seed, long n)
    {
        List<string> traits = [];
        Outcome outcome = Compare(seed, n, traits);
        Tally?.WriteLine($"{string.Join(' ', traits)} {(outcome.Finding ? "finding" : outcome.Signature)}");
        return outcome;
    }

    private Outcome Compare(long seed, long n, List<string> traits)
    {
        Random random = Cases.RandomFor(seed, n);
        try
        {
            byte[] bytes = Input(random, out _, out CsvReadOptions options, traits);
            var reference = Read(bytes, options with { MaxDegreeOfParallelism = 1, UnitBytes = Math.Max(1, bytes.Length + 16) }, Source.Memory);
            CsvReadOptions candidateOptions = options with
            {
                MaxDegreeOfParallelism = random.Next(1, 5),
                UnitBytes = random.Next(3) switch { 0 => random.Next(1, 64), 1 => random.Next(64, 8192), _ => options.UnitBytes },
                BatchSize = random.Next(3) == 0 ? random.Next(1, 50) : options.BatchSize,
                // A limit within the reference's own (a generated file may come with one).
                MaxRows = random.Next(5) == 0 ? Math.Min(random.Next(0, 300), options.MaxRows ?? long.MaxValue) : options.MaxRows,
                RowIndexColumn = options.RowIndexColumn ?? (random.Next(6) == 0 ? "__fuzz_row__" : null),
            };
            bool indexAdded = options.RowIndexColumn is null && candidateOptions.RowIndexColumn is not null;
            string[]? columns = null;
            if (reference.Schema is { } full && options.Columns is null && options.HasHeader && random.Next(5) == 0 && full.Length > 0)
            {
                columns = [.. full.Where(c => c != options.RowIndexColumn && random.Next(2) == 0).Distinct()];
                if (columns.Length == 0 || columns.Length != full.Count(c => columns.Contains(c))) columns = null; // duplicate names: no subset
                else candidateOptions = candidateOptions with { Columns = columns };
            }
            var source = (Source)random.Next(6);
            traits.AddRange([
                $"source:{source}",
                candidateOptions.UnitBytes switch { < 64 => "unit:tiny", < 8192 => "unit:small", _ => "unit:default" },
                $"threads:{candidateOptions.MaxDegreeOfParallelism}"]);
            if (candidateOptions.MaxRows != options.MaxRows) traits.Add("limit");
            if (indexAdded) traits.Add("index");
            if (columns is not null) traits.Add("subset");
            var candidate = Read(bytes, candidateOptions, source);
            string described = $"source {source}, threads {candidateOptions.MaxDegreeOfParallelism}, unit {candidateOptions.UnitBytes}, batch {candidateOptions.BatchSize}, max {candidateOptions.MaxRows}, row index {candidateOptions.RowIndexColumn is not null}, columns {(columns is null ? "all" : string.Join(";", columns))}";
            if (reference.Error is { } e1)
            {
                // A limited read may stop before the refused record; a subset may not decode a refused value's column.
                if (candidate.Error is { } e2)
                {
                    if (e1.GetType() != e2.GetType() || e1.Message != e2.Message)
                    {
                        if (columns is not null && e1 is CsvValueException) return Outcome.Refused(e1);
                        return new Outcome(true, "different refusals", $"{e1.GetType().Name}: {e1.Message}\n{e2.GetType().Name}: {e2.Message}\n{described}");
                    }
                    return Outcome.Refused(e1);
                }
                if (candidateOptions.MaxRows is not null || (columns is not null && e1 is CsvValueException)) return Outcome.Refused(e1);
                return new Outcome(true, "refused by the reference only", $"{e1.GetType().Name}: {e1.Message}\n{described}");
            }
            if (candidate.Error is { } e) return new Outcome(true, "refused by the candidate only", $"{e}\n{described}");
            // A row index the reference has too stays in front of a column subset.
            string[]? kept = columns is not null && options.RowIndexColumn is { } index ? [index, .. columns] : columns;
            List<string> expected = Expected(reference, candidateOptions, kept, indexAdded);
            List<string> actual = [.. candidate.Rows!.Select(r => string.Join(" | ", r))];
            string expectedSchema = string.Join(",", indexAdded ? (kept ?? reference.Schema!).Prepend(candidateOptions.RowIndexColumn!) : kept ?? reference.Schema!);
            string actualSchema = string.Join(",", candidate.Schema!);
            if (expectedSchema != actualSchema) return new Outcome(true, "different schemas", $"{expectedSchema}\n{actualSchema}\n{described}");
            if (!expected.SequenceEqual(actual))
            {
                int at = expected.Zip(actual).TakeWhile(p => p.First == p.Second).Count();
                return new Outcome(true, "different rows", $"{expected.Count} vs {actual.Count} rows ({described}); row {at}:\n{expected.ElementAtOrDefault(at)}\n{actual.ElementAtOrDefault(at)}");
            }
            return Outcome.Read;
        }
        catch (Exception e)
        {
            return Outcome.From(e);
        }
    }

    // The reference's rows as the candidate reads them: its columns, its row limit, its row index.
    private static List<string> Expected((string[]? Schema, List<string[]>? Rows, Exception? Error) reference, CsvReadOptions options, string[]? columns, bool indexAdded)
    {
        IEnumerable<string[]> rows = reference.Rows!;
        if (columns is not null)
        {
            int[] at = [.. columns.Select(c => System.Array.IndexOf(reference.Schema!, c))];
            rows = rows.Select(r => at.Select(i => r[i]).ToArray());
        }
        if (options.MaxRows is long max) rows = rows.Take((int)Math.Min(max, int.MaxValue));
        if (indexAdded) rows = rows.Select((r, i) => (string[])[(options.RowIndexOffset + i).ToString(System.Globalization.CultureInfo.InvariantCulture), .. r]);
        return [.. rows.Select(r => string.Join(" | ", r))];
    }

    private enum Source { Memory, Guarded, Seekable, NonSeekable, Mapped, Unmapped }

    // The schema's names and the rows' values, or the refusal (anything else escapes as a finding).
    private static (string[]? Schema, List<string[]>? Rows, Exception? Error) Read(byte[] bytes, CsvReadOptions options, Source source)
    {
        GuardedMemory.Block? guarded = null;
        string? path = null;
        try
        {
            CsvReader reader;
            switch (source)
            {
                case Source.Guarded:
                    guarded = GuardedMemory.Block.Copy(bytes);
                    reader = CsvReader.Open((ReadOnlyMemory<byte>)guarded.Memory, options);
                    break;
                case Source.Seekable:
                    reader = CsvReader.Open(new MemoryStream(bytes), options);
                    break;
                case Source.NonSeekable:
                    reader = CsvReader.Open(new NonSeekable(bytes), options);
                    break;
                case Source.Mapped or Source.Unmapped:
                    path = Path.GetTempFileName();
                    File.WriteAllBytes(path, bytes);
                    reader = CsvReader.Open(path, options with { MemoryMap = source == Source.Mapped ? CsvMemoryMap.Auto : CsvMemoryMap.Off });
                    break;
                default:
                    reader = CsvReader.Open(bytes.AsMemory(), options);
                    break;
            }
            using (reader)
            {
                List<string[]> rows = [];
                foreach (RecordBatch batch in reader.ReadBatches())
                    using (batch)
                        for (int r = 0; r < batch.Length; r++)
                            rows.Add([.. Enumerable.Range(0, batch.ColumnCount).Select(c => ArrowRender.Value(batch.Column(c), r))]);
                return ([.. reader.Schema.FieldsList.Select(f => f.Name)], rows, null);
            }
        }
        catch (Exception e) when (e is CsvFormatException or CsvValueException or ArgumentException)
        {
            return (null, null, e);
        }
        finally
        {
            ((IDisposable?)guarded)?.Dispose();
            if (path is not null) File.Delete(path);
        }
    }

    // A case's input and the options it is read with.
    private byte[] Input(Random random, out string source, out CsvReadOptions options, List<string>? traits)
    {
        byte[] bytes;
        if (corpus.Count == 0 || random.Next(2) == 0)
        {
            (bytes, options) = CsvGenerator.Generate(random);
            source = "generated";
        }
        else
        {
            source = corpus[random.Next(corpus.Count)];
            bytes = File.ReadAllBytes(source);
            options = OptionsOf(Path.GetFileName(source));
        }
        traits?.Add(source == "generated" ? "generated" : options.Encoding is null ? "corpus" : "corpus-legacy");
        if (random.Next(4) != 0)
        {
            bytes = Mutate(random, bytes);
            traits?.Add("mutated");
        }
        if (random.Next(3) == 0 && options.Encoding is null && CsvGenerator.Encode(random, bytes, options) is { } encoded)
        {
            (bytes, options) = (encoded.Bytes, encoded.Options);
            traits?.Add("encoded");
        }
        if (random.Next(6) == 0)
        {
            int which = random.Next(10);
            options = Other(random, options, which);
            traits?.Add($"other:{which}");
        }
        return bytes;
    }

    private static CsvReadOptions OptionsOf(string name)
    {
        var options = new CsvReadOptions();
        if (name.Contains(".semicolon.", StringComparison.Ordinal)) options = options with { Separator = ';' };
        if (name.Contains(".tab.", StringComparison.Ordinal)) options = options with { Separator = '\t' };
        if (name.Contains(".cp932.", StringComparison.Ordinal)) options = options with { Encoding = Kaminari.Sazanami.Text.TextEncodings.Resolve(932) };
        if (name.Contains(".cp1252.", StringComparison.Ordinal)) options = options with { Encoding = Kaminari.Sazanami.Text.TextEncodings.Resolve(1252) };
        if (name.Contains(".skip2.", StringComparison.Ordinal)) options = options with { SkipRows = 2 };
        return options;
    }

    // Mutations aimed in part at the bytes that shape records.
    private byte[] Mutate(Random random, byte[] original)
    {
        long[] hot = [.. Enumerable.Range(0, original.Length)
            .Where(i => i == 0 || original[i - 1] is (byte)'\n' or (byte)'\r' || original[i] is (byte)'"' or (byte)',' or (byte)';' or (byte)'\t')
            .Select(i => (long)i).Where((_, k) => k % 5 == 0)];
        return Mutations.Apply(random, original, hot, 0, r => corpus.Count > 0 ? File.ReadAllBytes(corpus[r.Next(corpus.Count)]) : Encoding.ASCII.GetBytes("a,b\n1,2\n"));
    }

    // Options the generator and the seeds do not vary.
    private static CsvReadOptions Other(Random random, CsvReadOptions options, int which) => which switch
    {
        0 => options with { CommentPrefix = random.Next(2) == 0 ? "#" : "//" },
        1 => options with { SkipRows = random.Next(0, 5) },
        2 => options with { SkipLines = random.Next(0, 5), SkipRows = 0 },
        3 => options with { SkipRowsAfterHeader = random.Next(0, 5) },
        4 => options with { TruncateRaggedLines = true },
        5 => options with { MissingIsNull = false, NullValues = ["NA", ""] },
        6 => options with { IgnoreErrors = true },
        7 => options with { DecimalComma = true, Separator = ';', ThousandsSeparator = random.Next(2) == 0 ? "." : null },
        8 => options with { InferSchemaLength = random.Next(3) switch { 0 => 0, 1 => 1, _ => null } },
        _ => options with { TryParseDates = false },
    };

    /// <summary>A stream that cannot seek, read a few hundred bytes at a time.</summary>
    private sealed class NonSeekable(byte[] bytes) : Stream
    {
        private int _at;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
        public override int Read(Span<byte> buffer)
        {
            int n = Math.Min(Math.Min(buffer.Length, 997), bytes.Length - _at);
            bytes.AsSpan(_at, n).CopyTo(buffer);
            _at += n;
            return n;
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
