using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Datasets read as Polars reads them (<c>scan_parquet</c>): the files a directory, a glob or several inputs give,
/// in order, with the row index, the file path column and a row limit; files of mixed types as Polars, DuckDB and
/// pyarrow read them where the rules agree. Opt-in: SESERAGI_PARQUET_PYTHON (Polars, DuckDB, pyarrow).
/// </summary>
[Trait("Category", "Interop")]
public sealed class ParquetDatasetInteropTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-dataset-interop-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    private void Write(string relative, long first, int rows, int rowGroupRows)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            ids.Append(first + i);
            if (i % 7 == 3) names.AppendNull();
            else names.Append($"n{first + i}");
        }
        using var writer = ParquetWriter.Create(path, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows });
        writer.Write(new RecordBatch(schema, [ids.Build(), names.Build()], rows));
        writer.Complete();
    }

    /// <summary>A dataset read: <paramref name="Hive"/> null is Auto (Polars' None); <paramref name="Filter"/> is a column and the value it equals.</summary>
    private sealed record Case(string[] Inputs, string? RowIndex, string? FileColumn, long? Rows, bool? Hive = false, bool ParseDates = true,
        (string Column, object Value)? Filter = null);

    [Fact]
    public async Task Datasets_ReadAsPolarsReadsThem()
    {
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        Write("t/b.parquet", 100, 30, 8);
        Write("t/a.parquet", 0, 5, 1000);
        Write("t/c.parquet", 200, 3, 1000);
        Write("t/sub/d.parquet", 300, 12, 5);
        Write("t/sub/deeper/e.parquet", 400, 4, 1000);
        Write("t/.hidden.parquet", 500, 2, 1000);
        Write("t/_meta.parquet", 600, 2, 1000);
        File.WriteAllBytes(At("t/sub/empty.parquet"), []);
        Dictionary<string, Case> cases = new()
        {
            ["dir"] = new([At("t")], null, null, null),
            ["dir_slash"] = new([At("t") + "/"], null, null, null),
            ["glob_all"] = new([At("t/**/*.parquet")], null, null, null),
            ["glob_top"] = new([At("t/*.parquet")], null, null, null),
            ["glob_one_char"] = new([At("t/?.parquet")], null, null, null),
            ["glob_class"] = new([At("t/[ab].parquet")], null, null, null),
            ["glob_mid"] = new([At("t/*/d.parquet")], null, null, null),
            ["list"] = new([At("t/sub"), At("t/a.parquet"), At("t/*.parquet")], null, null, null),
            ["index_paths"] = new([At("t")], "n", "file", null),
            ["index_paths_limit"] = new([At("t/**/*.parquet")], "n", "file", 41),
            ["limit_zero"] = new([At("t")], "n", null, 0),
        };
        await Compare(python, cases);
    }

    /// <summary>Partitioned datasets: keys, types and values from paths, collisions, filters over partitions, row numbers past files left out.</summary>
    [Fact]
    public async Task HiveDatasets_ReadAsPolarsReadsThem()
    {
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        int n = 0;
        foreach (string year in (string[])["2023", "2024", "2025"])
        {
            foreach (string month in (string[])["01", "12"]) Write($"h/year={year}/month={month}/p.parquet", n++ * 10, 3 + n % 4, 1000);
        }
        Write("s/s=x/p.parquet", 0, 2, 1000);
        Write("s/s=/p.parquet", 10, 2, 1000);
        Write("s/s=__HIVE_DEFAULT_PARTITION__/p.parquet", 20, 2, 1000);
        Write("s/s=a%2Fb%20c/p.parquet", 30, 2, 1000);
        Write("d/d=2024-01-15/p.parquet", 0, 2, 1000);
        Write("d/d=2024-02-29/p.parquet", 10, 2, 1000);
        Write("f/k=1/p.parquet", 0, 1, 1000);
        Write("f/k=1.5/p.parquet", 10, 1, 1000);
        Write("f/k=-2/p.parquet", 20, 1, 1000);
        Write("b/k=True/p.parquet", 0, 1, 1000);
        Write("b/k=false/p.parquet", 10, 1, 1000);
        Write("t/t=2024-01-15 10%3A00%3A00/p.parquet", 0, 1, 1000);
        Write("t/t=2024-01-15T11%3A30%3A00.5/p.parquet", 10, 1, 1000);
        Write("u/t=2024-01-15T10%3A00%3A00Z/p.parquet", 0, 1, 1000);
        Write("u/t=2024-01-15T10%3A00%3A00%2B02%3A00/p.parquet", 10, 1, 1000);
        Write("i/k=99999999999999999999/p.parquet", 0, 1, 1000);
        Write("c/id=7/p.parquet", 0, 3, 1000);
        Dictionary<string, Case> cases = new()
        {
            ["dir_auto"] = new([At("h")], null, null, null, Hive: null),
            ["dir_off"] = new([At("h")], null, null, null, Hive: false),
            ["glob_on"] = new([At("h/**/*.parquet")], "n", "file", null, Hive: true),
            ["glob_auto"] = new([At("h/**/*.parquet")], null, null, null, Hive: null),
            ["filter_partition_index"] = new([At("h")], "n", null, null, Hive: null, Filter: ("year", 2024L)),
            ["filter_partition_limit"] = new([At("h")], "n", "file", 4, Hive: null, Filter: ("month", 12L)),
            ["strings"] = new([At("s")], null, null, null, Hive: null),
            ["strings_filter"] = new([At("s")], null, null, null, Hive: null, Filter: ("s", "a/b c")),
            ["dates"] = new([At("d")], null, null, null, Hive: null),
            ["dates_off"] = new([At("d")], null, null, null, Hive: null, ParseDates: false),
            ["floats"] = new([At("f")], null, null, null, Hive: null),
            ["booleans"] = new([At("b")], null, null, null, Hive: null),
            ["datetimes"] = new([At("t")], null, null, null, Hive: null),
            ["datetimes_utc"] = new([At("u")], null, null, null, Hive: null),
            ["int128"] = new([At("i")], null, null, null, Hive: null),
            ["collision"] = new([At("c")], "n", null, null, Hive: null),
        };
        await Compare(python, cases);
    }

    /// <summary>A dataset of files typed differently: the options, and the engine reading it alike (null: we refuse it).</summary>
    private sealed record SchemaCase(string Directory, ParquetDatasetOptions Options, string? Oracle);

    /// <summary>
    /// Files whose column types differ (written by pyarrow): converted as Polars (lossless upcasts; lossy casts), DuckDB
    /// (first file's types, where lossless; united by name) or pyarrow read them where the rules agree; refused
    /// where ours refuse (narrowing, wall-clock times and instants, strings and numbers, lossy casts without the switch).
    /// Opt-in: SESERAGI_PARQUET_PYTHON (Polars, DuckDB, pyarrow).
    /// </summary>
    [Fact]
    public async Task DatasetsOfMixedTypes_ReadAsOtherEnginesReadThem()
    {
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        string script = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "dataset_schema_oracle.py"), root = At("mixed"), outPath = At("mixed.json");
        await Python(python, script, "make", root);
        await Python(python, script, "read", root, outPath);

        ParquetDatasetOptions first = new(), missing = new() { AllowMissingColumns = true }, lossy = new() { AllowLossyCasts = true };
        ParquetDatasetOptions union = new() { SchemaMode = DatasetSchemaMode.UnionByName }, unionLossy = union with { AllowLossyCasts = true };
        SchemaCase[] cases =
        [
            new("i64_then_i32", first, "polars_upcast"), new("i16_then_i8", first, "polars_upcast"), new("i64_then_u32", first, "polars_upcast"),
            new("u64_then_u8", first, "polars_upcast"), new("f64_then_i32", first, "duckdb_first"), new("f32_then_i16", first, "duckdb_first"),
            new("f64_then_f32", first, "polars_upcast"), new("f32_then_f16", first, "polars_upcast"), new("d10_2_then_d5_2", first, "polars_upcast"),
            new("d10_3_then_d9_2", first, "duckdb_first"), new("d12_2_then_i32", first, "duckdb_first"), new("d38_0_then_i64", first, "duckdb_first"),
            new("tsus_then_date", first, "duckdb_first"), new("tsus_then_tsms", first, "polars_upcast"), new("timeus_then_timems", first, "polars_upcast"),
            new("struct_extra_field", first, "polars_upcast"), new("struct_field_i64_then_i32", first, "polars_upcast"),
            new("struct_order_differs", first, "polars_upcast"), new("list_i64_then_i32", first, "polars_upcast"), new("req_then_opt", first, "polars_upcast"),
            new("i32_then_i64", first, null), new("i32_then_u32", first, null), new("f64_then_i64", first, null), new("f32_then_f64", first, null),
            new("d10_3_then_d10_2", first, null), new("tsms_then_tsus", first, null), new("tsus_then_tsns", first, null),
            new("tsusutc_then_tsus", first, null), new("tsus_then_tsusutc", first, null), new("date_then_tsus", first, null),
            new("str_then_i64", first, null), new("i64_then_str", first, null), new("str_then_bin", first, null), new("bin_then_str", first, null),
            new("struct_missing_field", first, null), new("extra_missing", first, null), new("union3", first, null),
            new("struct_missing_field", missing, "polars_upcast"), new("extra_missing", missing, "polars_upcast"),
            new("f64_then_i64", lossy, "polars_lossy"), new("f32_then_f64", lossy, "polars_lossy"), new("tsms_then_tsus", lossy, "polars_lossy"),
            new("tsus_then_tsns", lossy, "polars_lossy"), new("i32_then_i64", lossy, null), new("tsusutc_then_tsus", lossy, null), new("str_then_i64", lossy, null),
            new("i32_then_i64", union, "duckdb_union"), new("i32_then_u32", union, "duckdb_union"), new("f32_then_f64", union, "duckdb_union"),
            new("d10_3_then_d10_2", union, "duckdb_union"), new("date_then_tsus", union, "duckdb_union"), new("tsms_then_tsus", union, "duckdb_union"),
            new("tsus_then_tsns", union, "duckdb_union"), new("union3", union, "pyarrow_unify"), new("union_struct", union, "pyarrow_unify"),
            new("struct_extra_field", union, "pyarrow_unify"), new("struct_missing_field", union, "pyarrow_unify"), new("extra_missing", union, "pyarrow_unify"),
            new("union_conflict", union, null), new("str_then_i64", union, null), new("i64_then_str", union, null), new("str_then_bin", union, null),
            new("tsusutc_then_tsus", union, null), new("f64_then_i64", union, null), new("f64_then_i64", unionLossy, "duckdb_union"),
        ];

        using JsonDocument oracle = JsonDocument.Parse(File.ReadAllText(outPath));
        List<string> differences = [];
        foreach (SchemaCase c in cases)
        {
            string label = $"{c.Directory} ({(c.Options.SchemaMode == DatasetSchemaMode.UnionByName ? "union" : "first")}"
                + $"{(c.Options.AllowLossyCasts ? ", lossy" : "")}{(c.Options.AllowMissingColumns ? ", missing" : "")})";
            List<string>? rows = null;
            string? error = null;
            try
            {
                using var dataset = ParquetDataset.Open(Path.Combine(root, c.Directory) + "/", c.Options);
                using var reader = dataset.OpenReader();
                rows = [];
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    using (batch)
                    {
                        for (int i = 0; i < batch.Length; i++)
                            rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(k => Canonical(batch.Column(k), i))));
                    }
                }
            }
            catch (ParquetFormatException e)
            {
                error = e.Message;
            }
            if (c.Oracle is null)
            {
                if (error is null) differences.Add($"{label}: read ({string.Join(", ", rows!)}), expected a refusal");
                continue;
            }
            JsonElement expected = oracle.RootElement.GetProperty(c.Directory).GetProperty(c.Oracle);
            if (expected.TryGetProperty("error", out JsonElement theirs))
            {
                differences.Add($"{label}: {c.Oracle} failed: {theirs}");
                continue;
            }
            string[] wanted = [.. expected.GetProperty("rows").EnumerateArray().Select(e => e.GetString()!)];
            if (error is not null) differences.Add($"{label}: refused ({error}), {c.Oracle} reads {string.Join(", ", wanted)}");
            else if (!rows!.SequenceEqual(wanted)) differences.Add($"{label}: {string.Join(", ", rows!)}; {c.Oracle}: {string.Join(", ", wanted)}");
        }
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }

    private static async Task Python(string python, params string[] arguments)
    {
        var psi = new ProcessStartInfo(python, ["-I", .. arguments]) { RedirectStandardOutput = true, RedirectStandardError = true };
        using var process = Process.Start(psi)!;
        Task<string> stderr = process.StandardError.ReadToEndAsync();
        await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        Assert.True(process.ExitCode == 0, await stderr);
    }

    /// <summary>A value as dataset_schema_oracle.py renders Python values.</summary>
    private static string Canonical(IArrowArray array, int i) => array.IsNull(i) ? "null" : array switch
    {
        BooleanArray a => a.GetValue(i)!.Value ? "true" : "false",
        Int8Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Int16Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Int32Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Int64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        UInt8Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        UInt32Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        UInt64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        HalfFloatArray a => Float((double)a.GetValue(i)!.Value),
        FloatArray a => Float(a.GetValue(i)!.Value),
        DoubleArray a => Float(a.GetValue(i)!.Value),
        Decimal128Array a => a.GetSqlDecimal(i)!.Value.ToString(),
        Date32Array a => a.GetDateOnly(i)!.Value.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
        TimestampArray a => a.GetTimestamp(i)!.Value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture)
            + (((TimestampType)a.Data.DataType).Timezone is null ? "" : "Z"),
        Time32Array or Time64Array => Time(array, i),
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        BinaryViewArray b => "b:" + Convert.ToHexStringLower(b.GetBytes(i)),
        BinaryArray b => "b:" + Convert.ToHexStringLower(b.GetBytes(i)),
        StructArray s => "{" + string.Join(",", ((StructType)s.Data.DataType).Fields.Select((f, k) => $"{f.Name}:{Canonical(s.Fields[k], i)}")) + "}",
        ListArray l => "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[i], l.GetValueLength(i)).Select(k => Canonical(l.Values, k))) + "]",
        _ => throw new NotSupportedException(array.GetType().Name),
    };

    private static string Time(IArrowArray array, int i)
    {
        long ticks = array switch
        {
            Time32Array t => ((Time32Type)t.Data.DataType).Unit == TimeUnit.Second ? t.GetValue(i)!.Value * TimeSpan.TicksPerSecond : t.GetValue(i)!.Value * TimeSpan.TicksPerMillisecond,
            Time64Array t => ((Time64Type)t.Data.DataType).Unit == TimeUnit.Microsecond ? t.GetValue(i)!.Value * 10 : t.GetValue(i)!.Value / 100,
            _ => 0,
        };
        var time = new TimeOnly(ticks);
        return time.ToString(ticks % TimeSpan.TicksPerSecond == 0 ? "HH:mm:ss" : "HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task Compare(string python, Dictionary<string, Case> cases)
    {
        string casesPath = At("cases.json"), outPath = At("polars.json");
        using (var stream = File.Create(casesPath))
        using (var json = new Utf8JsonWriter(stream))
        {
            json.WriteStartObject();
            foreach (var (name, c) in cases)
            {
                json.WriteStartObject(name);
                json.WriteStartArray("inputs");
                foreach (string input in c.Inputs) json.WriteStringValue(input);
                json.WriteEndArray();
                if (c.RowIndex is null) json.WriteNull("row_index"); else json.WriteString("row_index", c.RowIndex);
                if (c.FileColumn is null) json.WriteNull("file_column"); else json.WriteString("file_column", c.FileColumn);
                if (c.Rows is null) json.WriteNull("n_rows"); else json.WriteNumber("n_rows", c.Rows.Value);
                if (c.Hive is null) json.WriteNull("hive"); else json.WriteBoolean("hive", c.Hive.Value);
                json.WriteBoolean("parse_dates", c.ParseDates);
                if (c.Filter is var (column, value))
                {
                    json.WriteStartArray("filter");
                    json.WriteStringValue(column);
                    json.WriteStringValue("==");
                    if (value is long number) json.WriteNumberValue(number); else json.WriteStringValue((string)value);
                    json.WriteEndArray();
                }
                json.WriteEndObject();
            }
            json.WriteEndObject();
        }
        var psi = new ProcessStartInfo(python, ["-I", Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "dataset_polars.py"), casesPath, outPath])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using (var process = Process.Start(psi)!)
        {
            Task<string> stderr = process.StandardError.ReadToEndAsync();
            await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            Assert.True(process.ExitCode == 0, await stderr);
        }

        using JsonDocument polars = JsonDocument.Parse(File.ReadAllText(outPath));
        List<string> differences = [];
        foreach (var (name, c) in cases)
        {
            JsonElement expected = polars.RootElement.GetProperty(name);
            Assert.False(expected.TryGetProperty("error", out JsonElement error), $"{name}: Polars failed: {error}");
            var options = new ParquetDatasetOptions
            {
                FilePathColumn = c.FileColumn,
                Partitioning = c.Hive switch { null => HivePartitioning.Auto, true => HivePartitioning.On, false => HivePartitioning.Off },
                ParseHiveDates = c.ParseDates,
            };
            using var dataset = ParquetDataset.Open(c.Inputs, options);
            Kaminari.Samidare.IR.ScalarExpr? filter = c.Filter is var (column, value)
                ? Kaminari.Samidare.IR.ScalarExpr.NewBinary(Kaminari.Samidare.IR.BinaryOp.Eq, Kaminari.Samidare.IR.ScalarExpr.NewColumn(column),
                    Kaminari.Samidare.IR.ScalarExpr.NewLiteral(value is long number ? Kaminari.Samidare.IR.LiteralValue.NewInt64(number)
                        : Kaminari.Samidare.IR.LiteralValue.NewString((string)value)))
                : null;
            using var reader = dataset.OpenReader(new ParquetReadOptions { RowIndexColumn = c.RowIndex, MaxRows = c.Rows, Filter = filter, FilterRows = filter is not null });
            string[] columns = [.. expected.GetProperty("columns").EnumerateArray().Select(e => e.GetString()!)];
            if (!columns.SequenceEqual(reader.Schema.FieldsList.Select(f => f.Name)))
                differences.Add($"{name}: columns {string.Join(",", reader.Schema.FieldsList.Select(f => f.Name))}, Polars {string.Join(",", columns)}");
            // Types as Polars names them (the row index aside: Polars numbers rows as UInt32, we as Int64).
            string[] types = [.. expected.GetProperty("types").EnumerateArray().Select(e => e.GetString()!)];
            string[] ours = [.. reader.Schema.FieldsList.Select(f => f.Name == c.RowIndex ? "UInt32" : PolarsType(f.DataType))];
            if (!types.SequenceEqual(ours)) differences.Add($"{name}: types {string.Join(",", ours)}, Polars {string.Join(",", types)}");
            List<string> rows = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    for (int i = 0; i < batch.Length; i++)
                        rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(k => Render(batch.Column(k), i))));
                }
            }
            string[] theirs = [.. expected.GetProperty("rows").EnumerateArray().Select(e => e.GetString()!)];
            if (!rows.SequenceEqual(theirs))
            {
                int at = Enumerable.Range(0, Math.Min(rows.Count, theirs.Length)).FirstOrDefault(i => rows[i] != theirs[i], Math.Min(rows.Count, theirs.Length));
                differences.Add($"{name}: {rows.Count} rows, Polars {theirs.Length}; first difference at {at}: "
                    + $"{(at < rows.Count ? rows[at] : "-")} vs {(at < theirs.Length ? theirs[at] : "-")}");
            }
        }
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }

    /// <summary>An Arrow type as Polars names the type it reads it as.</summary>
    private static string PolarsType(IArrowType type) => type switch
    {
        Int64Type => "Int64",
        Int32Type => "Int32",
        DoubleType => "Float64",
        BooleanType => "Boolean",
        StringType or StringViewType => "String",
        Date32Type => "Date",
        TimestampType t => $"Datetime(time_unit='{t.Unit switch { TimeUnit.Microsecond => "us", TimeUnit.Nanosecond => "ns", _ => "ms" }}', time_zone={(t.Timezone is null ? "None" : $"'{t.Timezone}'")})",
        Decimal128Type { Precision: 38, Scale: 0 } => "Int128",
        _ => type.Name,
    };

    /// <summary>A value as Python's <c>str</c> writes it.</summary>
    private static string Render(IArrowArray array, int i) => array.IsNull(i) ? "" : array switch
    {
        Int64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Int32Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        DoubleArray a => Float(a.GetValue(i)!.Value),
        BooleanArray a => a.GetValue(i)!.Value ? "True" : "False",
        Date32Array a => a.GetDateOnly(i)!.Value.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
        TimestampArray a => Timestamp(a, i),
        Decimal128Array a => a.GetSqlDecimal(i)!.Value.ToString(),
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        _ => throw new NotSupportedException(array.GetType().Name),
    };

    private static string Float(double value) => double.IsNaN(value) ? "nan" : double.IsInfinity(value) ? value > 0 ? "inf" : "-inf"
        : value == Math.Floor(value) && Math.Abs(value) < 1e16 ? value.ToString("R", System.Globalization.CultureInfo.InvariantCulture) + ".0"
        : value.ToString("R", System.Globalization.CultureInfo.InvariantCulture);

    private static string Timestamp(TimestampArray array, int i)
    {
        DateTime time = array.GetTimestamp(i)!.Value.UtcDateTime;
        string text = time.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        if (time.Ticks % TimeSpan.TicksPerSecond != 0) text += "." + (time.Ticks % TimeSpan.TicksPerSecond / 10).ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
        return ((TimestampType)array.Data.DataType).Timezone is null ? text : text + "+00:00";
    }
}
