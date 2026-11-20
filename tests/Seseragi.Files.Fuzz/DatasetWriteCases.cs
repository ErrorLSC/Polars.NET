using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Parquet;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// Dataset writes read back: random columns of every type (<see cref="ParquetWriteData.Wide"/>) and 0 to 2 partition
/// keys of few values (integers, strings with characters paths escape, empty and null, dates, booleans, floats with NaN
/// and infinities, decimals, zoned timestamps), in batches of random sizes, written as Parquet, IPC files or IPC
/// streams with random options (keys in the files or not, files cut by rows and by size, threads, a memory budget that
/// holds next to nothing, codecs), then read back as a dataset with the keys' types given: every row must come back
/// (in any order), every file hold its rows' limit, every path be apart. Any failure is a finding.
/// </summary>
internal static class DatasetWriteCases
{
    private static readonly string?[] Strings = ["a", "b c", "x/y", "p=q", "50%", "é", null, "", "C:\\x", "q?#", "[x]", "{y}", "it's"];
    private static readonly double[] Doubles = [1.5, double.NaN, double.PositiveInfinity, 0.1, 1e20, -3.25, 1e-7];

    /// <summary>A key column of <paramref name="rows"/> rows of about <paramref name="distinct"/> values.</summary>
    private static (Field Field, IArrowArray Array) Key(Random random, string name, int rows, int distinct)
    {
        int[] picks = [.. Enumerable.Range(0, rows).Select(_ => random.Next(distinct))];
        bool[] nulls = [.. Enumerable.Range(0, rows).Select(_ => random.Next(10) == 0)];
        long instant = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000;
        IArrowArray array;
        IArrowType type;
        switch (random.Next(8))
        {
            case 0:
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < rows; i++) _ = nulls[i] ? b.AppendNull() : b.Append(picks[i] - 2);
                (type, array) = (Int32Type.Default, b.Build());
                break;
            }
            case 1:
            {
                var b = new Int64Array.Builder();
                for (int i = 0; i < rows; i++) _ = nulls[i] ? b.AppendNull() : b.Append(picks[i] * 123456789012L);
                (type, array) = (Int64Type.Default, b.Build());
                break;
            }
            case 2:
            {
                var b = new StringArray.Builder();
                for (int i = 0; i < rows; i++) b.Append(Strings[picks[i] % Strings.Length]);
                (type, array) = (StringType.Default, b.Build());
                break;
            }
            case 3:
            {
                var b = new Date32Array.Builder();
                for (int i = 0; i < rows; i++) _ = nulls[i] ? b.AppendNull() : b.Append(new DateTime(2024, 1, 1).AddDays(picks[i] * 97));
                (type, array) = (Date32Type.Default, b.Build());
                break;
            }
            case 4:
            {
                var b = new BooleanArray.Builder();
                for (int i = 0; i < rows; i++) _ = nulls[i] ? b.AppendNull() : b.Append(picks[i] % 2 == 0);
                (type, array) = (BooleanType.Default, b.Build());
                break;
            }
            case 5:
            {
                var b = new DoubleArray.Builder();
                for (int i = 0; i < rows; i++) _ = nulls[i] ? b.AppendNull() : b.Append(Doubles[picks[i] % Doubles.Length]);
                (type, array) = (DoubleType.Default, b.Build());
                break;
            }
            case 6:
            {
                var b = new Decimal128Array.Builder(new Decimal128Type(10, 2));
                for (int i = 0; i < rows; i++) _ = nulls[i] ? b.AppendNull() : b.Append((picks[i] - 3) * 1.25m);
                (type, array) = (new Decimal128Type(10, 2), b.Build());
                break;
            }
            default:
            {
                type = new TimestampType(TimeUnit.Microsecond, random.Next(2) == 0 ? "UTC" : "Asia/Tokyo");
                var b = new TimestampArray.Builder((TimestampType)type);
                for (int i = 0; i < rows; i++)
                    _ = nulls[i] ? b.AppendNull() : b.Append(DateTimeOffset.FromUnixTimeMilliseconds((instant + picks[i] * 3_601_000_007L) / 1000));
                array = b.Build();
                break;
            }
        }
        return (new Field(name, type, true), array);
    }

    public static Outcome Run(Random random)
    {
        int rows = random.Next(random.Next(3) == 0 ? 40 : 4000);
        RecordBatch wide = ParquetWriteData.Wide(random.Next(), rows);
        int[] columns = [.. Enumerable.Range(0, wide.ColumnCount).Where(_ => random.Next(4) == 0).DefaultIfEmpty(random.Next(wide.ColumnCount))];
        List<Field> fields = [.. columns.Select(c => wide.Schema.FieldsList[c])];
        List<IArrowArray> arrays = [.. columns.Select(wide.Column)];
        int keyCount = random.Next(3);
        for (int k = 0; k < keyCount; k++)
        {
            var (field, array) = Key(random, $"pk{k}", rows, 1 + random.Next(random.Next(2) == 0 ? 4 : 40));
            int at = random.Next(fields.Count + 1);
            fields.Insert(at, field);
            arrays.Insert(at, array);
        }
        var schema = new Schema(fields, null);
        var batch = new RecordBatch(schema, arrays, rows);
        string[] keys = [.. Enumerable.Range(0, keyCount).Select(k => $"pk{k}")];

        string format = random.Next(3) switch { 0 => "parquet", 1 => "ipc", _ => "ipcstream" };
        var store = new MemoryFileStore();
        bool include = random.Next(2) == 0;
        DatasetWriteOptions options = format == "parquet"
            ? new ParquetDatasetWriteOptions { Writer = new ParquetWriteOptions { RowGroupRows = 1 + random.Next(random.Next(2) == 0 ? 50 : 5000) } }
            : new IpcDatasetWriteOptions
            {
                Writer = new IpcWriteOptions { Format = format == "ipc" ? IpcFormat.File : IpcFormat.Stream, Compression = random.Next(2) == 0 ? NoCompression.Instance : new Lz4FrameCompression() },
            };
        long? maxRows = random.Next(3) == 0 ? 1 + random.Next(300) : null;
        options = options with
        {
            Store = store,
            PartitionBy = keys,
            IncludePartitionColumns = include || keys.Length == schema.FieldsList.Count,
            MaxRowsPerFile = maxRows,
            ApproxBytesPerFile = random.Next(4) == 0 ? 1024 + random.Next(1 << 18) : null,
            MaxDegreeOfParallelism = 1 + random.Next(8),
            MemoryBudget = random.Next(3) == 0 ? 1 << 16 : null,
        };

        // The rows in batches of random sizes (slices of the batch: never disposed, they share its buffers).
        List<RecordBatch> parts = [];
        for (int at = 0; at < rows;)
        {
            int take = Math.Min(rows - at, random.Next(2) == 0 ? rows : 1 + random.Next(500));
            parts.Add(batch.Slice(at, take));
            at += take;
        }
        IReadOnlyList<DatasetWrittenFile> files = options is ParquetDatasetWriteOptions parquet
            ? ParquetDatasetWriter.Write("out", schema, parts, parquet).Files
            : IpcDatasetWriter.Write("out", schema, parts, (IpcDatasetWriteOptions)options).Files;

        string where = $"{format}, keys {string.Join(",", keys.Select(k => schema.GetFieldByName(k).DataType.Name))}, include {options.IncludePartitionColumns}, max rows {maxRows}, {files.Count} files";
        if (files.Sum(f => f.Rows) != rows) return new Outcome(true, "A dataset write lost or added rows", $"{files.Sum(f => f.Rows)} of {rows}; {where}");
        if (files.Select(f => f.Path).Distinct().Count() != files.Count) return new Outcome(true, "A dataset write named two files alike", where);
        if (maxRows is long limit && files.Any(f => f.Rows > limit)) return new Outcome(true, "A dataset file holds more rows than its limit", where);
        if (rows == 0) return files.Count == 0 ? Outcome.Read : new Outcome(true, "No rows wrote a file", where);

        List<string> expected = Rows(batch, [.. schema.FieldsList.Select(f => f.Name)]);
        List<string> got;
        Schema? partitions = keys.Length == 0 ? null : new Schema([.. keys.Select(schema.GetFieldByName)], null);
        if (format == "parquet")
        {
            using var dataset = ParquetDataset.Open("out/", new ParquetDatasetOptions { Store = store, PartitionSchema = partitions });
            using ParquetDatasetReader reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 1 + random.Next(4) });
            got = Read(reader.ReadBatches(), schema);
        }
        else
        {
            using var dataset = IpcDataset.Open("out/", new IpcDatasetOptions { Store = store, PartitionSchema = partitions });
            using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = 1 + random.Next(4) });
            got = Read(reader.ReadBatches(), schema);
        }
        if (!expected.SequenceEqual(got))
        {
            int at = Enumerable.Range(0, Math.Min(expected.Count, got.Count)).FirstOrDefault(i => expected[i] != got[i], Math.Min(expected.Count, got.Count));
            return new Outcome(true, "Rows written to a dataset do not come back",
                $"{where}; {expected.Count} vs {got.Count} rows; first difference at {at}: {(at < expected.Count ? expected[at] : "-")} vs {(at < got.Count ? got[at] : "-")}");
        }
        return Outcome.Read;
    }

    /// <summary>Every row (the columns of <paramref name="schema"/>, by name), canonical, sorted.</summary>
    private static List<string> Read(IEnumerable<RecordBatch> batches, Schema schema)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch) rows.AddRange(Rows(batch, [.. schema.FieldsList.Select(f => f.Name)]));
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static List<string> Rows(RecordBatch batch, string[] names)
    {
        int[] at = [.. names.Select(n => batch.Schema.GetFieldIndex(n))];
        List<string> rows = [.. Enumerable.Range(0, batch.Length).Select(r => string.Join('|', at.Select(c => ParquetWriteData.Canonical(batch.Column(c), r))))];
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
