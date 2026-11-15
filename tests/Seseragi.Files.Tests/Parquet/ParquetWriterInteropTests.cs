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

    /// <summary>Whether a reader's failure is a known limit: Polars opens no file holding an INTERVAL column or a list view, whoever wrote it.</summary>
    private static bool KnownFailure(string reader, string column, string error) =>
        Known.ContainsKey((reader, column)) || (reader == "polars" && error.Contains("nterval", StringComparison.Ordinal))
        // Polars opens no file whose Arrow schema holds a list view (whoever wrote it; pyarrow's too).
        || (reader == "polars" && column is "v_list" or "v_large" or "ree_str" or "ree_struct" or "dict_list" && error.Contains("not yet implemented", StringComparison.Ordinal))
        // Encodings the specification allows on FIXED_LEN_BYTE_ARRAY that a reader does not decode there.
        || (reader == "polars" && error.Contains("Decoding FixedLenByteArray", StringComparison.Ordinal) && error.Contains("not yet supported", StringComparison.Ordinal)
            && (error.Contains("\"ByteStreamSplit\"", StringComparison.Ordinal) || error.Contains("\"DeltaByteArray\"", StringComparison.Ordinal)))
        || (reader == "duckdb" && error.Contains("BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32 or INT64", StringComparison.Ordinal));

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

    /// <summary>The columns of spark_bloom.parquet with Bloom filters (writer_spark.py filters on the same).</summary>
    private static readonly string[] SparkBloomColumns = ["i32", "i64", "str", "bin", "dec15", "dict_str"];

    /// <summary>Every encoding in both page versions.</summary>
    private static readonly (string File, ParquetValueEncoding Encoding, ParquetPageVersion Version)[] Encodings =
    [
        .. Enum.GetValues<ParquetValueEncoding>().Where(e => e != ParquetValueEncoding.Plain).SelectMany(e => ((ParquetPageVersion[])[ParquetPageVersion.V1, ParquetPageVersion.V2])
            .Select(v => ($"enc_{e}_{(v == ParquetPageVersion.V1 ? "v1" : "v2")}".ToLowerInvariant(), e, v))),
    ];

    /// <summary>
    /// The batch with three more columns whose first 700 rows are null (a string, a boolean, an integer): pages
    /// without values in every encoding.
    /// </summary>
    private static RecordBatch WithNullRuns(RecordBatch batch)
    {
        int n = batch.Length;
        var r = new Random(n);
        var booleans = new BooleanArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i < 700) booleans.AppendNull();
            else booleans.Append(r.Next(3) == 0);
        }
        (string Name, IArrowArray Array)[] runs =
        [
            ("run_str", Strings([.. Enumerable.Range(0, n).Select(i => i < 700 ? null : "r" + r.Next(500))])),
            ("run_b", booleans.Build()),
            ("run_i64", Fixed(Int64Type.Default, [.. Enumerable.Range(0, n).Select(i => i < 700 ? null : (long?)r.NextInt64())])),
        ];
        return new RecordBatch(new Apache.Arrow.Schema([.. batch.Schema.FieldsList, .. runs.Select(c => new Field(c.Name, c.Array.Data.DataType, true))], null),
            [.. Enumerable.Range(0, batch.ColumnCount).Select(batch.Column), .. runs.Select(c => c.Array)], n);
    }

    /// <summary>Settings writing every column of <paramref name="batch"/> whose physical type takes <paramref name="encoding"/> in it.</summary>
    private Dictionary<string, ParquetColumnOptions> EncodedColumns(RecordBatch batch, ParquetValueEncoding encoding)
    {
        string probe = System.IO.Path.Combine(_dir, $"probe-{Guid.NewGuid():N}.parquet.tmp");
        ParquetWriter.Write(probe, batch.Schema, [batch.Slice(0, 1)]);
        using var file = ParquetFile.Open(probe);
        return file.Metadata.Thrift!.RowGroups[0].Columns
            .Where(c => Kaminari.Seseragi.Files.Parquet.Writing.ColumnSettings.Takes(c.MetaData!.Type, encoding))
            .ToDictionary(c => string.Join('.', c.MetaData!.PathInSchema), _ => new ParquetColumnOptions { Encoding = encoding });
    }

    /// <summary>The flat columns pyarrow writes as we do.</summary>
    private static readonly string[] FlatCommon =
        ["b", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "d32", "t64us", "ts_us_tokyo", "str", "bin", "fsb", "req_i64", "req_str"];

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

    /// <summary>Nested columns every reader is expected to read: lists, large lists, fixed-size lists, structs and maps, nested in each other.</summary>
    internal static RecordBatch NestedCommon(int seed, int rows)
    {
        var r = new Random(seed);
        Field F(string name, IArrowType type, bool nullable = true) => new(name, type, nullable);
        (string Name, IArrowType Type, bool Nullable)[] columns =
        [
            ("c_list", new ListType(F("item", Int64Type.Default)), true),
            ("c_struct", new StructType([F("a", StringType.Default), F("b", new ListType(F("item", Int32Type.Default)))]), true),
            ("c_map", new MapType(F("key", StringType.Default, false), F("value", new StructType([F("x", Int32Type.Default)]))), true),
            ("c_fsl", new FixedSizeListType(F("item", Int32Type.Default), 3), true),
            ("c_large", new LargeListType(F("item", new ListType(F("item", StringType.Default)))), true),
            ("c_lmap", new ListType(F("item", new MapType(F("key", Int64Type.Default, false), F("value", StringType.Default)))), true),
            ("c_req", new StructType([F("id", Int64Type.Default, false), F("tags", new ListType(F("item", StringType.Default, false)), false)]), false),
        ];
        ParquetNestedData.Column[] built = [.. columns.Select(c => ParquetNestedData.Build(r, c.Name, c.Type, c.Nullable, rows))];
        return new RecordBatch(new Apache.Arrow.Schema([.. built.Select(b => b.Field)], null), built.Select(b => b.Array), rows);
    }

    /// <summary>Nested layouts some readers cannot restore (list views, run-end encoding, dictionaries of nested values).</summary>
    internal static RecordBatch NestedLayouts(int seed, int rows)
    {
        var r = new Random(seed);
        Field F(string name, IArrowType type, bool nullable = true) => new(name, type, nullable);
        (string Name, IArrowType Type)[] columns =
        [
            ("v_list", new ListViewType(F("item", Int64Type.Default))),
            ("v_large", new LargeListViewType(F("item", StringType.Default))),
            ("ree_str", new RunEndEncodedType(Int32Type.Default, StringType.Default)),
            ("ree_struct", new RunEndEncodedType(Int32Type.Default, new StructType([F("a", Int64Type.Default)]))),
            ("dict_list", new DictionaryType(Int32Type.Default, new ListType(F("item", Int64Type.Default)), false)),
        ];
        ParquetNestedData.Column[] built = [.. columns.Select(c => ParquetNestedData.Build(r, c.Name, c.Type, true, rows))];
        return new RecordBatch(new Apache.Arrow.Schema([.. built.Select(b => b.Field)], null), built.Select(b => b.Array), rows);
    }

    /// <summary>Field ids on every named field (writer_interop.py checks pyarrow reads them where they were put).</summary>
    private static RecordBatch FieldIdBatch()
    {
        static Field Id(Field f, int id) => Kaminari.Sazanami.Arrow.ArrowFieldIds.With(f, id);
        var element = Id(new Field("item", Int32Type.Default, true), 3);
        var listType = new ListType(element);
        var stType = new StructType([Id(new Field("a", Int64Type.Default, true), 5)]);
        var mapType = new MapType(Id(new Field("key", StringType.Default, false), 7), Id(new Field("value", Int32Type.Default, true), 8), false);
        var schema = new Apache.Arrow.Schema(
            [Id(new Field("li", listType, true), 2), Id(new Field("st", stType, true), 4), Id(new Field("mp", mapType, true), 6), Id(new Field("x", Int64Type.Default, true), 9)], null);
        var r = new Random(9);
        return new RecordBatch(schema, schema.FieldsList.Select(f => ParquetNestedData.Build(r, f.Name, f.DataType, true, 20).Array), 20);
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
        // One row group, whole bounds: footers and page indexes compared with what pyarrow writes for the same table
        // (<name>.source.arrow, pages of <name>.pages rows).
        // Bloom filters on <name>.bloom's columns, sized for the chunk's distinct values (as pyarrow sizes them, its ndv
        // an upper bound): pyarrow's must be the same bits.
        void Reference(string name, RecordBatch batch, int pageRows, string[]? bloom = null)
        {
            var filter = new ParquetColumnOptions { BloomFilter = new() };
            Write(name, batch, new ParquetWriteOptions
            {
                StatisticsTruncateLength = null, DataPageRows = pageRows, Columns = bloom?.ToDictionary(c => c, _ => filter),
            });
            File.WriteAllText(System.IO.Path.Combine(_dir, name + ".pages"), pageRows.ToString(System.Globalization.CultureInfo.InvariantCulture));
            if (bloom is not null) File.WriteAllText(System.IO.Path.Combine(_dir, name + ".bloom"), string.Join(',', bloom));
            using var stream = File.Create(System.IO.Path.Combine(_dir, name + ".source.arrow"));
            using var ipc = new ArrowFileWriter(stream, batch.Schema);
            ipc.WriteRecordBatch(batch);
            ipc.WriteEnd();
        }
        Reference("nested_common", NestedCommon(31, 1500), 200);
        // The flat types pyarrow maps as we do (no decimals, which it writes as FLBA; no floats, whose NaN chunks it indexes).
        RecordBatch wide = Wide(35, 2500);
        Reference("flat_common", Without(wide, [.. wide.Schema.FieldsList.Select(f => f.Name).Where(n => !FlatCommon.Contains(n))]), 300,
            ["i32", "i64", "u64", "d32", "ts_us_tokyo", "str", "bin", "fsb", "req_str"]);
        // Every encoding over every type that takes it, the dictionary first (giving up halfway); the readers' limits in Known.
        RecordBatch encoded = WithNullRuns(Without(Wide(36, 3000), "iv_ym", "iv_dt", "iv_mdn"));
        foreach (var (file, encoding, version) in Encodings)
        {
            Write(file, encoded, new ParquetWriteOptions
            {
                DataPageVersion = version, RowGroupRows = 1200, DataPageRows = 250, DictionaryPageBytes = 1000,
                Columns = EncodedColumns(encoded, encoding),
            });
        }
        RecordBatch sparkEncoded = WithNullRuns(SparkBatch(24, 2500));
        foreach (ParquetPageVersion version in (ParquetPageVersion[])[ParquetPageVersion.V1, ParquetPageVersion.V2])
        {
            string v = version == ParquetPageVersion.V1 ? "v1" : "v2";
            var options = new ParquetWriteOptions { DataPageVersion = version, RowGroupRows = 1000, DataPageRows = 250, DictionaryPageBytes = 1000 };
            Write($"spark_delta_{v}", sparkEncoded, options with
            {
                Columns = ((ParquetValueEncoding[])[ParquetValueEncoding.DeltaBinaryPacked, ParquetValueEncoding.DeltaLengthByteArray, ParquetValueEncoding.Rle])
                    .SelectMany(e => EncodedColumns(sparkEncoded, e)).ToDictionary(),
            });
            Write($"spark_dba_{v}", sparkEncoded, options with { Columns = EncodedColumns(sparkEncoded, ParquetValueEncoding.DeltaByteArray) });
            // Spark's vectorized reader cannot read BYTE_STREAM_SPLIT: its row reader only (spark_rows_*).
            Write($"spark_rows_bss_{v}", sparkEncoded, options with { Columns = EncodedColumns(sparkEncoded, ParquetValueEncoding.ByteStreamSplit) });
        }
        // Bloom filters over several row groups, which Spark uses to skip row groups (writer_spark.py filters at their values).
        Write("spark_bloom", SparkBatch(25, 4000), new ParquetWriteOptions
        {
            RowGroupRows = 500,
            Columns = SparkBloomColumns.ToDictionary(c => c, _ => new ParquetColumnOptions { BloomFilter = new() }),
        });
        Write("nested_v2", NestedCommon(32, 1500), new ParquetWriteOptions { DataPageVersion = ParquetPageVersion.V2, Compression = new ZstandardCompression(), RowGroupRows = 500 });
        Write("nested_layouts", NestedLayouts(33, 1500), new ParquetWriteOptions { RowGroupRows = 600 });
        Write("spark_nested", NestedCommon(34, 1500), new ParquetWriteOptions { RowGroupRows = 700 });
        Write("field_ids", FieldIdBatch(), new ParquetWriteOptions());
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
            IArrowType writtenType = written.Schema.FieldsList[c].DataType;
            string[] expected = [.. ParquetNestedData.Canonical(written.Column(c)).Select(Loose).Select(nan)];
            IArrowType readType = schema.FieldsList[index].DataType;
            string[] actual = [.. read.SelectMany(b => ParquetNestedData.Canonical(b.Column(index), writtenType)).Select(v => nan(Loose(Adapt(what, writtenType, readType, v))))];
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
            foreach (string mode in name.StartsWith("spark_rows_", StringComparison.Ordinal) ? (string[])["rows"] : ["vectorized", "rows"])
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
