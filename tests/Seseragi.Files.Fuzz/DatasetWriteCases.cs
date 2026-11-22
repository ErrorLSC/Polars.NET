using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// Dataset writes read back: random columns of every type (<see cref="ParquetWriteData.Wide"/>) and 0 to 2 partition
/// keys of few values (integers, strings with characters paths escape, empty and null, dates, booleans, floats with NaN
/// and infinities, decimals, zoned timestamps), in batches of random sizes, written as Parquet, IPC files or IPC
/// streams with random options (keys in the files or not, files cut by rows and by size, threads, a memory budget that
/// holds next to nothing, codecs), then read back as a dataset with the keys' types given: every row must come back
/// (in any order), every file hold its rows' limit, every path be apart. As JSON (the columns JSON writes; lines or
/// documents, compressed or not): the files' lines, decompressed, must be the lines one JsonWriter writes for the rows
/// (in any order), a document file must be what JsonWriter writes for the rows it reads back as. Any failure is a finding.
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

        string format = random.Next(4) switch { 0 => "parquet", 1 => "ipc", 2 => "ipcstream", _ => "json" };
        // The columns of the files: the keys' left out unless included.
        bool include = random.Next(2) == 0 || keys.Length == schema.FieldsList.Count;
        var dataSchema = new Schema([.. schema.FieldsList.Where(f => include || !keys.Contains(f.Name))], null);
        if (format == "json" && !JsonWrites(dataSchema)) format = "ipc";
        var jsonWriter = new JsonWriteOptions
        {
            Format = random.Next(2) == 0 ? JsonFormat.Lines : JsonFormat.Document,
            Compression = random.Next(3) switch { 0 => JsonCompression.Gzip, 1 => JsonCompression.Zstd, _ => JsonCompression.None },
            PieceBytes = random.Next(2) == 0 ? 1 << 20 : 64 + random.Next(1 << 14),
        };
        var store = new MemoryFileStore();
        DatasetWriteOptions options = format switch
        {
            "parquet" => new ParquetDatasetWriteOptions { Writer = new ParquetWriteOptions { RowGroupRows = 1 + random.Next(random.Next(2) == 0 ? 50 : 5000) } },
            "json" => new JsonDatasetWriteOptions { Writer = jsonWriter },
            _ => new IpcDatasetWriteOptions
            {
                Writer = new IpcWriteOptions { Format = format == "ipc" ? IpcFormat.File : IpcFormat.Stream, Compression = random.Next(2) == 0 ? NoCompression.Instance : new Lz4FrameCompression() },
            },
        };
        long? maxRows = random.Next(3) == 0 ? 1 + random.Next(300) : null;
        options = options with
        {
            Store = store,
            PartitionBy = keys,
            IncludePartitionColumns = include,
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
        IReadOnlyList<DatasetWrittenFile> files = options switch
        {
            ParquetDatasetWriteOptions parquet => ParquetDatasetWriter.Write("out", schema, parts, parquet).Files,
            JsonDatasetWriteOptions json => JsonDatasetWriter.Write("out", schema, parts, json).Files,
            _ => IpcDatasetWriter.Write("out", schema, parts, (IpcDatasetWriteOptions)options).Files,
        };

        string where = $"{format}, keys {string.Join(",", keys.Select(k => schema.GetFieldByName(k).DataType.Name))}, include {options.IncludePartitionColumns}, max rows {maxRows}, {files.Count} files";
        if (files.Sum(f => f.Rows) != rows) return new Outcome(true, "A dataset write lost or added rows", $"{files.Sum(f => f.Rows)} of {rows}; {where}");
        if (files.Select(f => f.Path).Distinct().Count() != files.Count) return new Outcome(true, "A dataset write named two files alike", where);
        if (maxRows is long limit && files.Any(f => f.Rows > limit)) return new Outcome(true, "A dataset file holds more rows than its limit", where);
        if (rows == 0) return files.Count == 0 ? Outcome.Read : new Outcome(true, "No rows wrote a file", where);

        if (format == "json") return CheckJson(store, files, batch, dataSchema, jsonWriter, where);

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

    // Whether JSON writes every column of the schema (types it refuses leave the case to another format).
    private static bool JsonWrites(Schema schema)
    {
        try
        {
            using var writer = JsonWriter.Create(Stream.Null, schema);
            writer.Complete();
            return true;
        }
        catch (Exception e) when (e is NotSupportedException or ArgumentException)
        {
            return false;
        }
    }

    // A JSON dataset's files against one JsonWriter's text of the rows: its objects (a line's, a document's elements)
    // as a multiset.
    private static Outcome CheckJson(MemoryFileStore store, IReadOnlyList<DatasetWrittenFile> files, RecordBatch batch, Schema data, JsonWriteOptions options, string where)
    {
        var lines = options with { Format = JsonFormat.Lines, Compression = JsonCompression.None, PieceBytes = 1 << 20, MaxDegreeOfParallelism = 1 };
        var columns = new RecordBatch(data, [.. data.FieldsList.Select(f => batch.Column(batch.Schema.GetFieldIndex(f.Name)))], batch.Length);
        List<string> got = [];
        foreach (DatasetWrittenFile file in files)
        {
            IRandomAccess source = store.OpenAsync(file.Path).AsTask().GetAwaiter().GetResult()!;
            byte[] bytes = new byte[source.Length];
            source.ReadAt(0, bytes);
            if (bytes.Length != file.Bytes) return new Outcome(true, "A JSON dataset file's size is not the one reported", $"{where}; {bytes.Length} vs {file.Bytes}");
            string text = Decompressed(bytes, options.Compression);
            if (options.Format == JsonFormat.Lines)
            {
                if (!text.EndsWith('\n')) return new Outcome(true, "A JSON dataset file does not end its last line", where);
                got.AddRange(text.Split('\n')[..^1]);
            }
            else if (Elements(text) is { } elements) got.AddRange(elements);
            else return new Outcome(true, "A JSON dataset file is not one array of objects", $"{where}; {file.Path}");
        }
        List<string> expected = [.. JsonWriter.WriteString(data, [columns], lines).Split('\n')[..^1]];
        expected.Sort(StringComparer.Ordinal);
        got.Sort(StringComparer.Ordinal);
        if (expected.SequenceEqual(got)) return Outcome.Read;
        int at = Enumerable.Range(0, Math.Min(expected.Count, got.Count)).FirstOrDefault(i => expected[i] != got[i], Math.Min(expected.Count, got.Count));
        return new Outcome(true, "A JSON dataset's objects are not those JsonWriter writes for the rows",
            $"{where}; {expected.Count} vs {got.Count} objects; first difference at {at}: {(at < expected.Count ? expected[at] : "-")} vs {(at < got.Count ? got[at] : "-")}");

        // The texts of a document's elements (null when it is not one array, written without spaces).
        static List<string>? Elements(string text)
        {
            byte[] utf8 = System.Text.Encoding.UTF8.GetBytes(text);
            var reader = new System.Text.Json.Utf8JsonReader(utf8);
            if (!reader.Read() || reader.TokenType != System.Text.Json.JsonTokenType.StartArray || reader.TokenStartIndex != 0) return null;
            List<string> elements = [];
            long expectedStart = 1;
            while (reader.Read() && reader.TokenType != System.Text.Json.JsonTokenType.EndArray)
            {
                long start = reader.TokenStartIndex;
                if (start != expectedStart) return null;
                reader.Skip();
                elements.Add(System.Text.Encoding.UTF8.GetString(utf8, (int)start, (int)(reader.BytesConsumed - start)));
                expectedStart = reader.BytesConsumed + 1; // the comma after it
            }
            return reader.TokenType == System.Text.Json.JsonTokenType.EndArray && reader.BytesConsumed == utf8.Length && !reader.Read() ? elements : null;
        }

        static string Decompressed(byte[] bytes, JsonCompression compression)
        {
            using Stream input = new MemoryStream(bytes);
            using Stream text = compression switch
            {
                JsonCompression.Gzip => new System.IO.Compression.GZipStream(input, System.IO.Compression.CompressionMode.Decompress),
                JsonCompression.Zstd => new ZstdSharp.DecompressionStream(input),
                _ => input,
            };
            using var reader = new StreamReader(text, System.Text.Encoding.UTF8);
            return reader.ReadToEnd();
        }
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
