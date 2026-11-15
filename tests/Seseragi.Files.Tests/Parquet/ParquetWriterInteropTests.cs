using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.Parquet.ParquetWriteData;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Files written by <see cref="ParquetWriter"/> read by other readers (TestData/parquet/writer_interop.py,
/// writer_spark.py): pyarrow, DuckDB and Python Polars read every layout (their tables come back as Arrow IPC and are
/// compared here with the values written), Spark reads its subset with the vectorized reader and without (and writes
/// it back for our reader to compare); every reader filters at the bounds with statistics and without, with the same
/// rows. Opt-in: SESERAGI_PARQUET_PYTHON (pyarrow, duckdb, polars; e.g. ~/.cache/seseragi/avro-venv/bin/python) and
/// SESERAGI_PARQUET_SPARK_PYTHON (pyspark 4.2, with JAVA_HOME set to a JDK 21). Without them the files are only written.
/// </summary>
[Trait("Category", "Interop")]
public sealed class ParquetWriterInteropTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-interop").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    /// <summary>Columns a reader cannot read, or reads lossily, and why (what the reader does, not what we write).</summary>
    private static readonly Dictionary<(string Reader, string Column), string> Known = new()
    {
        [("duckdb", "ts_s")] = "the extreme values (±2^63 ms) exceed DuckDB's microsecond timestamps",
        [("duckdb", "ts_ms_utc")] = "the extreme values (±2^63 ms) exceed DuckDB's microsecond timestamps",
        [("duckdb", "dec60")] = "DuckDB holds at most 38 decimal digits: it reads wider decimals as doubles",
        [("polars", "dec60")] = "Polars holds at most 38 decimal digits",
        [("polars", "dur_s")] = "Polars has no durations in seconds: it multiplies them into milliseconds, wrapping the extreme values",
    };

    /// <summary>Whether a reader's failure is a known limit: Polars opens no file holding an INTERVAL column, whoever wrote it.</summary>
    private static bool KnownFailure(string reader, string column, string error) =>
        Known.ContainsKey((reader, column)) || (reader == "polars" && error.Contains("nterval", StringComparison.Ordinal));

    /// <summary>
    /// A read value in the form of the value written, where a reader returns the same value as another type: INTERVAL
    /// as its 12 bytes (pyarrow), durations as their integers (DuckDB ignores the stored Arrow schema), date64 as a
    /// timestamp (Polars has no date64), INT96 without a zone (it carries no UTC flag).
    /// </summary>
    private static string Adapt(string file, IArrowType written, IArrowType read, string value) => (written, read) switch
    {
        _ when value == "null" => value,
        (IntervalType, FixedSizeBinaryType { ByteWidth: 12 }) => Interval(Convert.FromHexString(value[2..])),
        (DurationType d, Int64Type) => "dur" + (System.Numerics.BigInteger.Parse(value) * Nanos(d.Unit)),
        (Date64Type, TimestampType) => "day" + (System.Numerics.BigInteger.Parse(value[2..].TrimEnd('Z')) / 86_400_000_000_000),
        (TimestampType { Timezone: { Length: > 0 } }, TimestampType { Timezone: null or "" }) when file.Contains("int96") => value + "Z",
        _ => value,
    };

    private static string NanAsNan(string value) => value switch
    {
        ['f', .. var bits] when bits.Length == 8 && IsHex(bits) && float.IsNaN(BitConverter.Int32BitsToSingle(Convert.ToInt32(bits, 16))) => "NaN",
        ['d', .. var bits] when bits.Length == 16 && IsHex(bits) && double.IsNaN(BitConverter.Int64BitsToDouble(Convert.ToInt64(bits, 16))) => "NaN",
        _ => value,
    };

    private static bool IsHex(string s) => s.All(char.IsAsciiHexDigitUpper);

    private static string Interval(byte[] b) =>
        $"iv{BitConverter.ToUInt32(b, 0)},{BitConverter.ToUInt32(b, 4)},{BitConverter.ToUInt32(b, 8) * 1_000_000L}";

    private static long Nanos(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000,
        TimeUnit.Millisecond => 1_000_000,
        TimeUnit.Microsecond => 1_000,
        _ => 1,
    };

    private static readonly (string Name, Func<RecordBatch> Batch, ParquetWriteOptions Options)[] Layouts =
    [
        ("v1_snappy", () => Wide(11, 3000), new() { RowGroupRows = 1000, DataPageRows = 300, DictionaryPageBytes = 2000 }),
        ("v2_zstd", () => Wide(12, 3000), new() { DataPageVersion = ParquetPageVersion.V2, Compression = new ZstandardCompression(), RowGroupRows = 1200 }),
        ("v1_gzip_plain", () => Wide(13, 2000), new() { Compression = new GzipCompression(), Dictionary = false }),
        ("v2_lz4", () => Wide(14, 2000), new() { DataPageVersion = ParquetPageVersion.V2, Compression = Lz4RawCompression.Instance, DataPageRows = 128 }),
        ("v1_brotli", () => Wide(15, 2000), new() { Compression = new BrotliCompression() }),
        ("v1_none", () => Wide(16, 2000), new() { Compression = NoCompression.Instance }),
        // Polars opens no file with an INTERVAL column: the other types once more without them.
        ("no_intervals", () => Without(Wide(17, 2000), "iv_ym", "iv_dt", "iv_mdn"), new() { DataPageRows = 400 }),
    ];

    private static RecordBatch Without(RecordBatch batch, params string[] names)
    {
        int[] keep = [.. Enumerable.Range(0, batch.ColumnCount).Where(c => !names.Contains(batch.Schema.FieldsList[c].Name))];
        return new RecordBatch(new Apache.Arrow.Schema([.. keep.Select(c => batch.Schema.FieldsList[c])], null), keep.Select(batch.Column), batch.Length);
    }

    /// <summary>The columns Spark reads (no half floats, times, nanoseconds, durations, decimals above 38 digits or intervals).</summary>
    private static readonly string[] SparkColumns =
    [
        "b", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "d32", "d64", "ts_s", "ts_ms_utc", "ts_us_tokyo",
        "dec32", "dec64", "dec5", "dec15", "dec30", "str", "lstr", "vstr", "bin", "lbin", "vbin", "fsb", "dict_str", "dict_i64", "nul", "req_i64", "req_str",
    ];

    /// <summary>The Spark columns of a wide batch, timestamps within years Spark's microseconds hold.</summary>
    private static RecordBatch SparkBatch(int seed, int rows)
    {
        RecordBatch wide = Wide(seed, rows);
        var random = new Random(seed);
        List<Field> fields = [];
        List<IArrowArray> arrays = [];
        foreach (string name in SparkColumns)
        {
            int index = wide.Schema.GetFieldIndex(name);
            Field field = wide.Schema.FieldsList[index];
            IArrowArray array = wide.Column(index);
            if (field.DataType is TimestampType t)
            {
                long per = t.Unit switch { TimeUnit.Second => 1, TimeUnit.Millisecond => 1000, _ => 1_000_000 };
                array = Fixed(t, [.. Enumerable.Range(0, rows).Select(i => array.IsNull(i) ? null : (long?)random.NextInt64(-2_000_000_000L, 4_000_000_000L) * per)]);
            }
            fields.Add(field);
            arrays.Add(array);
        }
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, rows);
    }

    private static RecordBatch Int96Batch()
    {
        var random = new Random(96);
        long?[] Values(long per) => [.. Enumerable.Range(0, 500).Select(i => i % 7 == 0 ? null : (long?)random.NextInt64(-5_000_000_000L, 5_000_000_000L) * per)];
        return Batch(("ts_utc", Fixed(new TimestampType(TimeUnit.Microsecond, "UTC"), Values(1_000_000)), true),
            ("ts_ms", Fixed(new TimestampType(TimeUnit.Millisecond, "UTC"), Values(1000)), true));
    }

    private Dictionary<string, RecordBatch> WriteFiles()
    {
        Dictionary<string, RecordBatch> written = [];
        void Write(string name, RecordBatch batch, ParquetWriteOptions options)
        {
            ParquetWriter.Write(System.IO.Path.Combine(_dir, name + ".parquet"), batch.Schema, [batch.Slice(0, batch.Length / 3), batch.Slice(batch.Length / 3, batch.Length - batch.Length / 3)], options);
            written[name] = batch;
        }
        foreach (var (name, batch, options) in Layouts) Write(name, batch(), options);
        Write("int96", Int96Batch(), new ParquetWriteOptions { Int96Timestamps = true });
        Write("sorted", Sorted(4000, new Random(4000)), new ParquetWriteOptions { RowGroupRows = 500, DataPageRows = 100 });
        // pyarrow keeps every row group holding a null, and float chunks with NaNs have no bounds: probed without both too.
        Write("sorted_nonnull", Sorted(4000, new Random(4001), nulls: false), new ParquetWriteOptions { RowGroupRows = 500, DataPageRows = 100 });
        Write("spark_v1_snappy", SparkBatch(21, 3000), new ParquetWriteOptions { RowGroupRows = 1000, DataPageRows = 300, DictionaryPageBytes = 2000 });
        Write("spark_v2_zstd", SparkBatch(22, 2000), new ParquetWriteOptions { DataPageVersion = ParquetPageVersion.V2, Compression = new ZstandardCompression() });
        Write("spark_v1_lz4_plain", SparkBatch(23, 2000), new ParquetWriteOptions { Compression = Lz4RawCompression.Instance, Dictionary = false });
        Write("spark_int96", Int96Batch(), new ParquetWriteOptions { Int96Timestamps = true });
        return written;
    }

    /// <summary>A canonical value where readers legitimately differ in form: an integer read as a decimal of scale 0.</summary>
    private static string Loose(string value) => value.EndsWith("e-0", StringComparison.Ordinal) ? value[..^3] : value;

    /// <summary>Compares a reader's table with the batch written, column by column; returns the differences.</summary>
    private static IEnumerable<string> Differences(string what, string reader, RecordBatch written, IReadOnlyList<RecordBatch> read)
    {
        Apache.Arrow.Schema schema = read.Count > 0 ? read[0].Schema : new Apache.Arrow.Schema([], null);
        for (int c = 0; c < written.ColumnCount; c++)
        {
            string name = written.Schema.FieldsList[c].Name;
            int index = schema.GetFieldIndex(name);
            if (index < 0 || Known.ContainsKey((reader, name)) && !what.StartsWith("spark_", StringComparison.Ordinal)) continue; // missing columns: errors.json
            // Spark writes NaNs back normalized (one bit pattern): any NaN is NaN there.
            Func<string, string> nan = reader.StartsWith("spark", StringComparison.Ordinal) ? NanAsNan : static v => v;
            string[] expected = [.. Canonical(written.Column(c)).Select(Loose).Select(nan)];
            IArrowType readType = schema.FieldsList[index].DataType, writtenType = written.Schema.FieldsList[c].DataType;
            string[] actual = [.. read.SelectMany(b => Canonical(b.Column(index))).Select(v => nan(Loose(Adapt(what, writtenType, readType, v))))];
            if (!expected.SequenceEqual(actual))
            {
                int row = expected.Zip(actual).TakeWhile(p => p.First == p.Second).Count();
                yield return $"{what} {reader} {name} ({schema.FieldsList[index].DataType.Name}): row {row} expected {expected.ElementAtOrDefault(row)}, got {actual.ElementAtOrDefault(row)} ({expected.Length} vs {actual.Length} rows)";
            }
        }
    }

    private static List<RecordBatch> ReadIpc(string path)
    {
        using var stream = File.OpenRead(path);
        using var reader = new ArrowFileReader(stream);
        List<RecordBatch> batches = [];
        while (reader.ReadNextRecordBatch() is { } batch) batches.Add(batch);
        return batches;
    }

    private static async Task<string> Run(string python, string script, string dir)
    {
        var psi = new ProcessStartInfo(python, ["-I", script, dir]) { RedirectStandardOutput = true, RedirectStandardError = true };
        using var process = Process.Start(psi)!;
        Task<string> stdout = process.StandardOutput.ReadToEndAsync(), stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        string output = await stdout + await stderr;
        Assert.True(process.ExitCode == 0, output);
        return output;
    }

    [Fact]
    public async Task PyArrow_DuckDb_AndPolars_ReadOurFiles()
    {
        Dictionary<string, RecordBatch> written = WriteFiles();
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        await Run(python, System.IO.Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "writer_interop.py"), _dir);

        List<string> differences = [];
        foreach (var (name, batch) in written)
        {
            foreach (string reader in (string[])["pyarrow", "duckdb", "polars"])
                differences.AddRange(Differences(name, reader, batch, ReadIpc(System.IO.Path.Combine(_dir, $"{name}.{reader}.arrow"))));
        }
        using JsonDocument errors = JsonDocument.Parse(File.ReadAllText(System.IO.Path.Combine(_dir, "errors.json")));
        foreach (JsonElement e in errors.RootElement.EnumerateArray())
        {
            string reader = e.GetProperty("reader").GetString()!, column = e.GetProperty("column").GetString()!;
            if (!KnownFailure(reader, column, e.GetProperty("error").GetString()!))
                differences.Add($"{e.GetProperty("file").GetString()} {reader} {column}: unreadable: {e.GetProperty("error").GetString()}");
        }
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }

    [Fact]
    public async Task Spark_ReadsOurFiles()
    {
        Dictionary<string, RecordBatch> written = WriteFiles();
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_SPARK_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        await Run(python, System.IO.Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "writer_spark.py"), _dir);

        List<string> differences = [];
        foreach (var (name, batch) in written.Where(w => w.Key.StartsWith("spark_", StringComparison.Ordinal)))
        {
            foreach (string mode in (string[])["vectorized", "rows"])
            {
                string part = Directory.GetFiles(System.IO.Path.Combine(_dir, $"{name}.spark-{mode}"), "*.parquet").Single();
                using var file = ParquetFile.Open(part);
                using var reader = file.OpenReader();
                List<RecordBatch> read = [.. reader.ReadBatches()];
                differences.AddRange(Differences(name, "spark-" + mode, batch, read));
                foreach (RecordBatch b in read) b.Dispose();
            }
        }
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }
}
