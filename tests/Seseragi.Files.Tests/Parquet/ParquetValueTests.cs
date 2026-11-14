using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Every Parquet test file read here against pyarrow and DuckDB (TestData/parquet/values.json). Each file falls in
/// exactly one bucket — compared, refused for a named reason, nested only (read later), or special — and the
/// bucket sizes are asserted, so no file is skipped silently.
/// </summary>
public class ParquetValueTests
{
    private static readonly Lazy<JsonDocument> Oracle = new(() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(ParquetFixtures.Root, "values.json"))));

    /// <summary>Files refused, with a fragment of the error each must give.</summary>
    private static readonly Dictionary<string, string> Refused = new()
    {
        ["apache/bad_data/ARROW-GH-41317.parquet"] = "ends before its row group",           // columns of different lengths
        ["apache/bad_data/ARROW-GH-41321.parquet"] = "cut short",                           // levels shorter than num_values
        ["apache/bad_data/ARROW-GH-47662.parquet"] = "fewer values",                        // a required column with nulls
        ["apache/bad_data/ARROW-RS-GH-11261-FLBA-DICT.parquet"] = "DeltaLengthByteArray",   // P5 (then: wrong type)
        ["apache/bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet"] = "outside the file",
        ["apache/bad_data/PARQUET-1481.parquet"] = "unknown physical type",
        ["apache/data/alp_extended.zstd.parquet"] = "Alp are not supported",
        ["apache/data/byte_stream_split.zstd.parquet"] = "ByteStreamSplit are not supported yet",
        ["apache/data/byte_stream_split_extended.gzip.parquet"] = "ByteStreamSplit are not supported yet",
        ["apache/data/datapage_v2.snappy.parquet"] = "DeltaBinaryPacked are not supported yet",
        ["apache/data/delta_binary_packed.parquet"] = "DeltaBinaryPacked are not supported yet",
        ["apache/data/delta_byte_array.parquet"] = "DeltaByteArray are not supported yet",
        ["apache/data/delta_encoding_optional_column.parquet"] = "DeltaBinaryPacked are not supported yet",
        ["apache/data/delta_encoding_required_column.parquet"] = "DeltaBinaryPacked are not supported yet",
        ["apache/data/delta_length_byte_array.parquet"] = "DeltaLengthByteArray are not supported yet",
        ["apache/data/flba12_timestamp.parquet"] = "does not fit a 64-bit timestamp",   // pyarrow and DuckDB refuse it too
        ["apache/bad_data/ARROW-GH-45185.parquet"] = "starts with repetition level 1",   // a record starting mid-list
        ["apache/bad_data/ARROW-RS-GH-6229-LEVELS.parquet"] = "more than the",           // a page claims more values than its chunk
    };

    /// <summary>Files where pyarrow fails and DuckDB reads (compared with DuckDB).</summary>
    private static readonly string[] PyarrowFails = ["apache/data/incorrect_map_schema.parquet"]; // optional map keys

    /// <summary>Files checked by their own tests (the oracles are wrong there).</summary>
    private static readonly string[] Special = ["apache/data/int96_from_spark.parquet"];

    /// <summary>Files where DuckDB, not pyarrow, is right: pyarrow 26 reads 0 rows where empty data pages lead (the README says 3).</summary>
    private static readonly string[] DuckDbFiles = ["apache/data/empty_data_pages_v1.parquet", "apache/data/empty_data_pages_v2.parquet"];

    private static string[] Names => [.. Oracle.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name)];

    public static TheoryData<string> Files => [.. Names];

    [Fact]
    public void EveryFile_FallsInOneBucket()
    {
        string[] all = Names;
        string[] skipped = [.. all.Where(n => Oracle.Value.RootElement.GetProperty("files").GetProperty(n).TryGetProperty("skipped", out _))];
        string[] compared = [.. all.Except(Refused.Keys).Except(Special).Except(skipped)];
        Assert.Equal(all.Length, compared.Length + Refused.Count + Special.Length + skipped.Length);
        Assert.Equal((153, 133, 18, 1, 1), (all.Length, compared.Length, Refused.Count, Special.Length, skipped.Length));
        Assert.Equal(["apache/data/large_string_map.brotli.parquet"], skipped);
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void File_ReadsLikeTheOracles(string name)
    {
        JsonElement expected = Oracle.Value.RootElement.GetProperty("files").GetProperty(name);
        if (expected.TryGetProperty("skipped", out _) || Special.Contains(name)) return;
        if (Refused.TryGetValue(name, out string? reason))
        {
            var e = Assert.ThrowsAny<ParquetFormatException>(() => ReadAll(name, batchSize: 65_536));
            Assert.Contains(reason, e.Message);
            return;
        }
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        var (schema, columns) = ReadAll(name, batchSize: 65_536);
        List<string> diffs = [];
        for (int c = 0; c < schema.FieldsList.Count; c++)
        {
            Field field = schema.FieldsList[c];
            bool duckDb = DuckDbFiles.Contains(name) || PyarrowFails.Contains(name) || field.DataType is IntervalType;
            JsonElement oracle = Column(expected.GetProperty(duckDb ? "duckdb" : "pyarrow"), field.Name);
            List<string> values = columns[c];
            if (CanonicalRender.Hash(values) != oracle.GetProperty("hash").GetString())
                diffs.Add($"{field.Name}: ours {string.Join(" ", values.Take(3))} vs {string.Join(" ", oracle.GetProperty("sample").EnumerateArray().Select(s => s.GetString()))}");
            if (values.Count != oracle.GetProperty("rows").GetInt32()) diffs.Add($"{field.Name}: {values.Count} rows, oracle {oracle.GetProperty("rows").GetInt32()}");
            if (values.Count(v => v == "null") != oracle.GetProperty("nulls").GetInt32()) diffs.Add($"{field.Name}: {values.Count(v => v == "null")} nulls, oracle {oracle.GetProperty("nulls").GetInt32()}");
            var physical = file.Thrift.Schema.FirstOrDefault(e => e.Name == field.Name)?.Type;
            if (!duckDb && field.DataType is not (ListType or StructType or MapType or FixedSizeListType) && TypeName(field.DataType) != Normalize(oracle.GetProperty("type").GetString()!, field.DataType, physical))
                diffs.Add($"{field.Name}: type {TypeName(field.DataType)}, pyarrow {oracle.GetProperty("type").GetString()}");
        }
        Assert.True(diffs.Count == 0, $"{name}:\n{string.Join('\n', diffs)}");
    }

    /// <summary>One thread reads what many do: the same batches in the same order, and the same errors.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void Sequential_ReadsLikeParallel(string name)
    {
        JsonElement expected = Oracle.Value.RootElement.GetProperty("files").GetProperty(name);
        if (expected.TryGetProperty("skipped", out _)) return;
        if (Refused.ContainsKey(name))
        {
            string one = Assert.ThrowsAny<ParquetFormatException>(() => ReadAll(name, 1_000, parallelism: 1)).Message;
            string many = Assert.ThrowsAny<ParquetFormatException>(() => ReadAll(name, 1_000, parallelism: 8)).Message;
            Assert.Equal(one, many);
            return;
        }
        // Small batches: several per row group, so the order of batches is checked too.
        var (_, sequential) = ReadAll(name, 1_000, parallelism: 1);
        var (_, parallel) = ReadAll(name, 1_000, parallelism: 8);
        for (int c = 0; c < sequential.Length; c++) Assert.Equal(sequential[c], parallel[c]);
    }

    /// <summary>
    /// Column chunks read the same however their bytes are got: fetched before decoding, sliced from memory, or read
    /// page by page (forced here; most fixtures are small enough to be served from memory otherwise), on one thread
    /// and on many; refused files fail with the same message.
    /// </summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void ChunkReadings_ReadAlike(string name)
    {
        JsonElement expected = Oracle.Value.RootElement.GetProperty("files").GetProperty(name);
        if (expected.TryGetProperty("skipped", out _)) return;
        (ChunkReading Reading, bool FromMemory)[] ways = [(ChunkReading.Fetch, false), (ChunkReading.Stream, false), (ChunkReading.Slice, true), (ChunkReading.Stream, true)];
        if (Refused.ContainsKey(name))
        {
            string reference = Assert.ThrowsAny<ParquetFormatException>(() => ReadAll(name, 1_000, parallelism: 1)).Message;
            foreach (var (reading, fromMemory) in ways)
            {
                foreach (int parallelism in (int[])[1, 8])
                    Assert.Equal(reference, Assert.ThrowsAny<ParquetFormatException>(() => ReadAll(name, 1_000, parallelism: parallelism, reading: reading, fromMemory: fromMemory)).Message);
            }
            return;
        }
        var (_, columns) = ReadAll(name, 1_000, parallelism: 1);
        foreach (var (reading, fromMemory) in ways)
        {
            foreach (int parallelism in (int[])[1, 8])
            {
                var (_, other) = ReadAll(name, 1_000, parallelism: parallelism, reading: reading, fromMemory: fromMemory);
                for (int c = 0; c < columns.Length; c++) Assert.True(columns[c].SequenceEqual(other[c]), $"{reading}{(fromMemory ? " from memory" : "")}, {parallelism} threads, column {c}");
            }
        }
    }

    /// <summary>Strings and binaries read the same in the classic layout (Utf8, Binary) as in the view layout the oracles were compared with.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void ClassicLayout_ReadsLikeTheViewLayout(string name)
    {
        JsonElement expected = Oracle.Value.RootElement.GetProperty("files").GetProperty(name);
        if (expected.TryGetProperty("skipped", out _) || Refused.ContainsKey(name)) return;
        var (views, viewColumns) = ReadAll(name, batchSize: 65_536);
        CheckViews(name);
        var (classic, classicColumns) = ReadAll(name, batchSize: 65_536, viewTypes: false);
        Assert.Equal(views.FieldsList.Select(f => TypeName(f.DataType)), classic.FieldsList.Select(f => TypeName(f.DataType)));
        Assert.DoesNotContain(classic.FieldsList, f => f.DataType is StringViewType or BinaryViewType);
        for (int c = 0; c < viewColumns.Length; c++) Assert.Equal(viewColumns[c], classicColumns[c]);
    }

    /// <summary>
    /// The view layout's rules hold for every value (consumers compare by them): up to 12 bytes inline, zero-padded;
    /// longer ones with their first 4 bytes as the prefix and a buffer index and offset inside the buffers.
    /// </summary>
    private static void CheckViews(string name)
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        using var reader = file.OpenReader(new() { BatchSize = 1_000 });
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                foreach (IArrowArray column in batch.Arrays) Check(column);
            }
        }

        static void Check(IArrowArray array)
        {
            switch (array)
            {
                case BinaryViewArray views:
                {
                    ReadOnlySpan<byte> raw = views.Data.Buffers[1].Span;
                    for (int i = 0; i < views.Length; i++)
                    {
                        if (views.IsNull(i)) continue;
                        ReadOnlySpan<byte> view = raw.Slice((views.Offset + i) * 16, 16);
                        int length = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(view);
                        ReadOnlySpan<byte> value = views.GetBytes(i);
                        Assert.Equal(value.Length, length);
                        if (length <= 12)
                        {
                            Assert.True(view.Slice(4, length).SequenceEqual(value));
                            Assert.True(view[(4 + length)..].IndexOfAnyExcept((byte)0) < 0, "inline padding");
                        }
                        else
                        {
                            Assert.True(view.Slice(4, 4).SequenceEqual(value[..4]), "prefix");
                            int buffer = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(view[8..]);
                            int offset = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(view[12..]);
                            Assert.InRange(buffer, 0, views.Data.Buffers.Length - 3);
                            Assert.InRange(offset + length, length, views.Data.Buffers[2 + buffer].Length);
                        }
                    }
                    break;
                }
                case StructArray s:
                    foreach (IArrowArray child in s.Fields) Check(child);
                    break;
                case ListArray l:
                    Check(l.Values);
                    break;
                case FixedSizeListArray f:
                    Check(f.Values);
                    break;
            }
        }
    }

    /// <summary>Every column's Arrow type, nested ones included, is pyarrow's (by the normalization rules below).</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void Schema_MatchesPyarrow(string name)
    {
        JsonElement expected = Oracle.Value.RootElement.GetProperty("files").GetProperty(name);
        if (expected.TryGetProperty("skipped", out _) || !expected.GetProperty("pyarrow").TryGetProperty("columns", out JsonElement columns)) return;
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        List<string> diffs = [];
        foreach (JsonElement column in columns.EnumerateArray())
        {
            string columnName = column.GetProperty("name").GetString()!;
            Field field = file.Schema.FieldsList.Single(f => f.Name == columnName);
            if (field.DataType is not (ListType or StructType or MapType or FixedSizeListType)) continue; // flat ones: in File_ReadsLikeTheOracles
            string pyarrow = System.Text.RegularExpressions.Regex.Replace(column.GetProperty("type").GetString()!, @" \('[^']*'\)", "");
            pyarrow = pyarrow.Replace("large_list<", "list<", StringComparison.Ordinal).Replace("large_string", "string", StringComparison.Ordinal);
            if (TypeName(field.DataType) != pyarrow) diffs.Add($"{columnName}: ours {TypeName(field.DataType)}, pyarrow {pyarrow}");
        }
        Assert.True(diffs.Count == 0, $"{name}:\n{string.Join('\n', diffs)}");
    }

    /// <summary>
    /// Spark writes timestamps beyond the nanosecond range wrapped, and reads them back with the same wrapping
    /// arithmetic; read in microseconds, these are the values the file's README lists (both oracles differ).
    /// </summary>
    [Fact]
    public void Int96FromSpark_ReadsAsSparkWroteIt()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("apache/data/int96_from_spark.parquet"));
        Assert.Equal(new TimestampType(TimeUnit.Microsecond, (string?)null).Name, file.Schema.FieldsList[0].DataType.Name);
        using var reader = file.OpenReader();
        RecordBatch batch = Assert.Single(reader.ReadBatches());
        var values = (TimestampArray)batch.Column(0);
        long?[] micros = [.. Enumerable.Range(0, values.Length).Select(i => values.IsNull(i) ? null : (long?)values.GetValue(i))];
        Assert.Equal([1704141296123456, 1704070800000000, 253402225200000000, 1735599600000000, null, 9089380393200000000], micros);

        using var nanos = file.OpenReader(new() { Int96Unit = TimeUnit.Nanosecond });
        Assert.Contains("does not fit a 64-bit timestamp in nanoseconds", Assert.Throws<ParquetFormatException>(() => nanos.ReadBatches().ToList()).Message);
    }

    [Theory]
    [InlineData("apache/data/alltypes_tiny_pages.parquet")]
    [InlineData("apache/data/alltypes_tiny_pages_plain.parquet")]
    [InlineData("matrix/pa_pages_rowgroups.parquet")]
    [InlineData("matrix/polars_lz4.parquet")]
    [InlineData("apache/data/overflow_i16_page_cnt.parquet")]
    [InlineData("nested/pa_nested_small_pages.parquet")]
    [InlineData("nested/polars_nested_small.parquet")]
    [InlineData("nested/delta_checkpoint_classic.parquet")]
    [InlineData("apache/data/nested_lists.snappy.parquet")]
    public void BatchSize_DoesNotChangeTheValues(string name)
    {
        var (_, expected) = ReadAll(name, batchSize: 65_536);
        foreach (int size in (int[])[1, 7, 997, 10_000_000])
        {
            var (_, actual) = ReadAll(name, batchSize: size);
            Assert.Equal(expected, actual);
        }
    }

    // ------------------------------------------------------------------

    internal static string[] FlatColumns(Apache.Arrow.Schema schema) =>
        [.. schema.FieldsList.Where(f => f.DataType is not (ListType or StructType or MapType or FixedSizeListType)).Select(f => f.Name)];

    /// <summary>The columns of a file, each rendered row by row.</summary>
    internal static (Apache.Arrow.Schema Schema, List<string>[] Columns) ReadAll(string name, int batchSize, bool viewTypes = true, int? parallelism = null,
        ChunkReading reading = ChunkReading.Auto, bool fromMemory = false)
    {
        using var file = fromMemory ? ParquetFile.Open(new MemoryStream(File.ReadAllBytes(ParquetFixtures.FullPath(name)), 0, (int)new FileInfo(ParquetFixtures.FullPath(name)).Length, false, publiclyVisible: true))
            : ParquetFile.Open(ParquetFixtures.FullPath(name));
        using var reader = file.OpenReader(new()
        {
            BatchSize = batchSize, ViewTypes = viewTypes, MaxDegreeOfParallelism = parallelism ?? Environment.ProcessorCount, Reading = reading,
        });
        List<string>[] columns = [.. reader.Schema.FieldsList.Select(_ => (List<string>)[])];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            Assert.True(batch.Length <= batchSize);
            for (int c = 0; c < columns.Length; c++)
            {
                for (int i = 0; i < batch.Length; i++) columns[c].Add(CanonicalRender.Value(batch.Column(c), i));
            }
            batch.Dispose();
        }
        return (reader.Schema, columns);
    }

    private static JsonElement Column(JsonElement oracle, string name) =>
        oracle.GetProperty("columns").EnumerateArray().First(c => c.GetProperty("name").GetString() == name);

    /// <summary>An Arrow type as pyarrow prints it.</summary>
    internal static string TypeName(IArrowType type) => type switch
    {
        BooleanType => "bool",
        Int8Type => "int8",
        Int16Type => "int16",
        Int32Type => "int32",
        Int64Type => "int64",
        UInt8Type => "uint8",
        UInt16Type => "uint16",
        UInt32Type => "uint32",
        UInt64Type => "uint64",
        HalfFloatType => "halffloat",
        FloatType => "float",
        DoubleType => "double",
        Decimal128Type d => $"decimal128({d.Precision}, {d.Scale})",
        Decimal256Type d => $"decimal256({d.Precision}, {d.Scale})",
        Date32Type => "date32[day]",
        Time32Type t => $"time32[{Unit(t.Unit)}]",
        Time64Type t => $"time64[{Unit(t.Unit)}]",
        TimestampType t => t.Timezone is { } zone ? $"timestamp[{Unit(t.Unit)}, tz={zone}]" : $"timestamp[{Unit(t.Unit)}]",
        DurationType d => $"duration[{Unit(d.Unit)}]",
        // The view layouts hold the same logical types (pyarrow reads them as string and binary).
        StringType or StringViewType => "string",
        BinaryType or BinaryViewType => "binary",
        FixedSizeBinaryType f => $"fixed_size_binary[{f.ByteWidth}]",
        DictionaryType d => $"dictionary<values={TypeName(d.ValueType)}, indices={TypeName(d.IndexType)}, ordered=0>",
        NullType => "null",
        IntervalType { Unit: IntervalUnit.MonthDayNanosecond } => "month_day_nano_interval",
        ListType l => $"list<{Child(l.ValueField)}>",
        FixedSizeListType f => $"fixed_size_list<{Child(f.ValueField)}>[{f.ListSize}]",
        StructType s => $"struct<{string.Join(", ", s.Fields.Select(Child))}>",
        MapType m => $"map<{TypeName(m.KeyField.DataType)}, {TypeName(m.ValueField.DataType)}>",
        _ => type.Name,
    };

    private static string Child(Field f) => $"{f.Name}: {TypeName(f.DataType)}{(f.IsNullable ? "" : " not null")}";

    private static string Unit(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => "s",
        TimeUnit.Millisecond => "ms",
        TimeUnit.Microsecond => "us",
        _ => "ns",
    };

    /// <summary>
    /// pyarrow's type, as this library maps it by design: large types not restored, canonical extensions as their
    /// storage, INT96 in the stored Arrow unit and zone (pyarrow coerces INT96 to microseconds and drops the zone),
    /// integers and timestamp units the stored Arrow schema refines in INT64 (pyarrow keeps INT64, Polars narrows, as here), dictionary
    /// indices always Int32 (Polars writes UInt32 for Categorical, ordered UInt8 for Enum).
    /// </summary>
    private static string Normalize(string pyarrow, IArrowType ours, Kaminari.Seseragi.Files.Parquet.Metadata.ParquetType? physical) => pyarrow switch
    {
        _ when physical == Kaminari.Seseragi.Files.Parquet.Metadata.ParquetType.Int96 && ours is TimestampType && pyarrow.StartsWith("timestamp[", StringComparison.Ordinal) => TypeName(ours),
        "int64" when physical == Kaminari.Seseragi.Files.Parquet.Metadata.ParquetType.Int64 && ours is IntegerType => TypeName(ours),
        // INT64 timestamps in the stored Arrow unit (format 1.0 stores nanoseconds as microseconds; pyarrow keeps them so).
        _ when physical == Kaminari.Seseragi.Files.Parquet.Metadata.ParquetType.Int64 && ours is TimestampType t && pyarrow.StartsWith("timestamp[", StringComparison.Ordinal)
            => "timestamp[" + Unit(t.Unit) + pyarrow[pyarrow.IndexOfAny([',', ']'])..],
        _ when ours is DictionaryType && pyarrow.StartsWith("dictionary<values=", StringComparison.Ordinal) =>
            $"dictionary<values={pyarrow["dictionary<values=".Length..pyarrow.IndexOf(',', StringComparison.Ordinal)].Replace("large_string", "string", StringComparison.Ordinal)}, indices=int32, ordered=0>",
        "large_string" or "extension<arrow.json>" => "string",
        "large_binary" => "binary",
        "extension<arrow.uuid>" => "fixed_size_binary[16]",
        _ => pyarrow,
    };
}
