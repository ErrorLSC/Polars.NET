using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.IO;
using Seseragi.Files.Tests;
using Seseragi.Files.Tests.Csv;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// The CSV dataset cases (corpus directory <c>csv-dataset</c>, which may be empty): a generated file
/// (<see cref="CsvGenerator.Generate"/>, a sixth with other options) cut into one to six files by lines (each with the
/// lines the options pass over and the header), some under <c>k=…</c> directories, held in a memory store; options of
/// the dataset (given schema, missing and extra columns, files inferred, a file path column) and of the read (rows,
/// a row index, a column subset, a filter over the partition column) at random. Read twice: the reference on one
/// thread in chunks of 1 MiB, the files as they are; the candidate on random threads, chunks and batches, some files
/// gzip or zstd compressed (read as they are found). Contract: both open and read alike — the same schema and rows
/// (paths compared without the compression's suffix), or the same refusal (type and message).
/// </summary>
internal sealed class CsvDatasetCases : IFuzzCases
{
    public string Extension => ".csv";

    public bool HasFile(long n) => false;

    public byte[] FileOf(long seed, long n, out string source) => throw new NotSupportedException();

    // With CSV_FUZZ_TALLY set to a directory, each worker writes one line per case there (the coverage facts).
    private static readonly StreamWriter? Tally = Environment.GetEnvironmentVariable("CSV_FUZZ_TALLY") is { Length: > 0 } dir
        ? new StreamWriter(Path.Combine(dir, $"tally-{Environment.ProcessId}.txt"), append: true) { AutoFlush = true }
        : null;

    public Outcome Run(long seed, long n)
    {
        List<string> traits = [];
        Outcome outcome = Compare(seed, n, traits);
        Tally?.WriteLine($"{string.Join(' ', traits)} {(outcome.Finding ? "finding" : outcome.Signature)}");
        return outcome;
    }

    private static Outcome Compare(long seed, long n, List<string> traits)
    {
        Random random = Cases.RandomFor(seed, n);
        try
        {
            (byte[] bytes, CsvReadOptions options) = CsvGenerator.Generate(random);
            if (random.Next(6) == 0)
            {
                int which = random.Next(10);
                options = CsvCases.Other(random, options, which);
                traits.Add($"other:{which}");
            }
            // The files: the lines the options pass over and the header, then a run of the data lines each.
            List<byte[]> lines = Lines(bytes);
            int prefix = (int)Math.Min(lines.Count, options.SkipLines + options.SkipRows + (options.HasHeader ? 1 : 0) + options.SkipRowsAfterHeader);
            byte[] head = [.. lines.Take(prefix).SelectMany(l => l)];
            List<byte[]> data = lines.Skip(prefix).ToList();
            int files = 1 + random.Next(6);
            bool partitioned = random.Next(3) == 0;
            var reference = new MemoryFileStore();
            var candidate = new MemoryFileStore();
            for (int f = 0; f < files; f++)
            {
                int start = data.Count == 0 ? 0 : random.Next(data.Count), count = data.Count == 0 ? 0 : random.Next(data.Count - start + 1);
                byte[] file = [.. head, .. data.Skip(start).Take(count).SelectMany(l => l)];
                string name = $"d/{(partitioned ? $"k={f % 3}/" : "")}f{f:D2}.csv";
                reference.Add(name, file);
                if (Environment.GetEnvironmentVariable("CSV_DATASET_DUMP") is { Length: > 0 } dump)
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(Path.Combine(dump, name))!);
                    File.WriteAllBytes(Path.Combine(dump, name), file);
                }
                // An empty file stays empty (listings leave it out); a compressed one would not be.
                switch (file.Length == 0 ? 3 : random.Next(4))
                {
                    case 0:
                        candidate.Add(name + ".gz", Gzip(file));
                        traits.Add("gzip");
                        break;
                    case 1:
                        candidate.Add(name + ".zst", Zstd(file));
                        traits.Add("zstd");
                        break;
                    default:
                        candidate.Add(name, file);
                        break;
                }
            }
            traits.Add($"files:{files}");
            if (partitioned) traits.Add("partitioned");

            // The dataset's options.
            var datasetOptions = new CsvDatasetOptions
            {
                Format = options with { Schema = null, Columns = null, Filter = null, FilterRows = false, MaxRows = null, RowIndexColumn = null, RowIndexOffset = 0 },
                Schema = options.Schema,
                AllowMissingColumns = random.Next(3) == 0,
                IgnoreExtraColumns = random.Next(3) == 0,
                InferSchemaFiles = 1 + random.Next(files),
                FilePathColumn = random.Next(4) == 0 ? "__path__" : null,
            };
            if (options.Schema is not null) traits.Add("schema");
            if (datasetOptions.AllowMissingColumns) traits.Add("missing");
            if (datasetOptions.IgnoreExtraColumns) traits.Add("extra");
            if (datasetOptions.FilePathColumn is not null) traits.Add("path");

            // The read's options (both reads), and the candidate's threads and chunks.
            var read = new CsvReadOptions
            {
                MaxRows = random.Next(5) == 0 ? random.Next(0, 300) : null,
                RowIndexColumn = random.Next(5) == 0 ? "__row__" : null,
                RowIndexOffset = random.Next(3) == 0 ? random.Next(100) : 0,
                IgnoreErrors = options.IgnoreErrors,
            };
            if (partitioned && random.Next(2) == 0)
            {
                read = read with
                {
                    Filter = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("k"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(random.Next(3)))),
                    FilterRows = true,
                };
                traits.Add("filter");
            }
            if (read.MaxRows is not null) traits.Add("limit");
            if (read.RowIndexColumn is not null) traits.Add("index");
            var candidateRead = read with
            {
                MaxDegreeOfParallelism = random.Next(1, 9),
                UnitBytes = random.Next(3) switch { 0 => random.Next(1, 64), 1 => random.Next(64, 8192), _ => 1 << 20 },
                BatchSize = random.Next(3) == 0 ? random.Next(1, 50) : read.BatchSize,
            };
            traits.Add(candidateRead.UnitBytes switch { < 64 => "unit:tiny", < 8192 => "unit:small", _ => "unit:default" });
            traits.Add($"threads:{candidateRead.MaxDegreeOfParallelism}");

            var expected = Read(reference, datasetOptions, read with { MaxDegreeOfParallelism = 1, UnitBytes = 1 << 20 }, random.Next(4) == 0, out string[]? subset);
            var actual = Read(candidate, datasetOptions, candidateRead, subset);
            if (subset is not null) traits.Add("subset");
            if (Environment.GetEnvironmentVariable("CSV_DATASET_DUMP") is { Length: > 0 } dumped)
                File.WriteAllText(Path.Combine(dumped, "options.txt"), $"{datasetOptions}\n{datasetOptions.Format}\n{candidateRead}\n{string.Join(",", candidate.ListAsync("d", true).ToBlockingEnumerable().Select(f => f.Path))}");
            string described = $"{files} files{(partitioned ? " partitioned" : "")}, threads {candidateRead.MaxDegreeOfParallelism}, unit {candidateRead.UnitBytes}, " +
                $"batch {candidateRead.BatchSize}, max {read.MaxRows}, index {read.RowIndexColumn is not null}, filter {read.Filter is not null}, " +
                $"columns {(subset is null ? "all" : string.Join(";", subset))}, missing {datasetOptions.AllowMissingColumns}, extra {datasetOptions.IgnoreExtraColumns}, inferred {datasetOptions.InferSchemaFiles}";
            if (expected.Error is not null || actual.Error is not null)
            {
                if (expected.Error is not { } e1) return new Outcome(true, "refused by the candidate only", $"{actual.Error}\n{described}");
                if (actual.Error is not { } e2) return new Outcome(true, "refused by the reference only", $"{e1.GetType().Name}: {e1.Message}\n{described}");
                if (e1.GetType() != e2.GetType() || e1.Message != Plain(e2.Message))
                    return new Outcome(true, "different refusals", $"{e1.GetType().Name}: {e1.Message}\n{e2.GetType().Name}: {e2.Message}\n{described}");
                return Outcome.Refused(e1);
            }
            if (!expected.Schema!.SequenceEqual(actual.Schema!))
                return new Outcome(true, "different schemas", $"{string.Join(",", expected.Schema!)}\n{string.Join(",", actual.Schema!)}\n{described}");
            List<string> want = expected.Rows!, got = [.. actual.Rows!.Select(Plain)];
            if (!want.SequenceEqual(got))
            {
                int at = want.Zip(got).TakeWhile(p => p.First == p.Second).Count();
                return new Outcome(true, "different rows", $"{want.Count} vs {got.Count} rows ({described}); row {at}:\n{want.ElementAtOrDefault(at)}\n{got.ElementAtOrDefault(at)}");
            }
            return Outcome.Read;
        }
        catch (Exception e)
        {
            return Outcome.From(e);
        }
    }

    // Paths and messages without the compression's suffix (the candidate's compressed files).
    private static string Plain(string text) => text.Replace(".csv.gz", ".csv", StringComparison.Ordinal).Replace(".csv.zst", ".csv", StringComparison.Ordinal);

    // The bytes cut into lines (each with its LF).
    private static List<byte[]> Lines(byte[] bytes)
    {
        List<byte[]> lines = [];
        int start = 0;
        for (int i = 0; i < bytes.Length; i++)
        {
            if (bytes[i] != (byte)'\n') continue;
            lines.Add(bytes[start..(i + 1)]);
            start = i + 1;
        }
        if (start < bytes.Length) lines.Add(bytes[start..]);
        return lines;
    }

    private static byte[] Gzip(byte[] bytes)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest)) gzip.Write(bytes);
        return output.ToArray();
    }

    private static byte[] Zstd(byte[] bytes)
    {
        using var output = new MemoryStream();
        using (var zstd = new ZstdSharp.CompressionStream(output)) zstd.Write(bytes);
        return output.ToArray();
    }

    // The dataset read: the schema's names and the rows, or the refusal (anything else escapes as a finding). With
    // `pick`, a random subset of the columns is read (given back, for the other read).
    private static (string[]? Schema, List<string>? Rows, Exception? Error) Read(IFileStore store, CsvDatasetOptions options, CsvReadOptions read, bool pick, out string[]? subset)
    {
        subset = null;
        try
        {
            using CsvDataset dataset = CsvDataset.Open("d", options with { Store = store });
            if (pick)
            {
                string[] all = [.. dataset.Schema.FieldsList.Select(f => f.Name)];
                var random = new Random(all.Length * 7919 + dataset.Files.Count);
                subset = [.. all.Where(_ => random.Next(2) == 0)];
                if (subset.Length == 0) subset = null;
                else read = read with { Columns = subset };
            }
            return ReadAll(dataset, read);
        }
        catch (Exception e) when (e is CsvFormatException or CsvValueException or ArgumentException or FileNotFoundException)
        {
            return (null, null, e);
        }
    }

    private static (string[]? Schema, List<string>? Rows, Exception? Error) Read(IFileStore store, CsvDatasetOptions options, CsvReadOptions read, string[]? subset)
    {
        try
        {
            using CsvDataset dataset = CsvDataset.Open("d", options with { Store = store });
            return ReadAll(dataset, subset is null ? read : read with { Columns = subset });
        }
        catch (Exception e) when (e is CsvFormatException or CsvValueException or ArgumentException or FileNotFoundException)
        {
            return (null, null, e);
        }
    }

    private static (string[]? Schema, List<string>? Rows, Exception? Error) ReadAll(CsvDataset dataset, CsvReadOptions read)
    {
        using CsvDatasetReader reader = dataset.OpenReader(read);
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
            using (batch)
                for (int r = 0; r < batch.Length; r++)
                    rows.Add(string.Join(" | ", Enumerable.Range(0, batch.ColumnCount).Select(c => ArrowRender.Value(batch.Column(c), r))));
        return ([.. reader.Schema.FieldsList.Select(f => $"{f.Name}:{f.DataType.Name}")], rows, null);
    }
}
