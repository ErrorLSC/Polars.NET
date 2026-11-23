using System.Globalization;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Marshalling;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// The behaviour table (gate G1 of the managed CSV reader's step f1): every read case of the probe
/// (TestData/csv/cases.json, made by csv-ref/probe/cases.py) read by <see cref="CsvReader"/> and compared with what
/// Polars' native reader gave (TestData/csv/native.txt), except where a rule decided otherwise (<see cref="Decided"/>),
/// where native's outcome is an error (<see cref="Errors"/>: our error's kind and facts), or where a later step reads
/// the case (<see cref="Later"/>: refused for now).
/// </summary>
public class CsvBehaviourTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "TestData", "csv");

    /// <summary>Cases a later step of the reader reads; refused with NotSupportedException until then.</summary>
    private static readonly Dictionary<string, string> Later = new()
    {
        ["truncate_ragged"] = "f4", ["comment_lines"] = "f4", ["comment_two_chars"] = "f4", ["comment_in_quoted"] = "f4",
        ["skip_rows"] = "f4", ["skip_rows_quoted_newline"] = "f4", ["skip_lines_quoted_newline"] = "f4", ["skip_rows_after_header"] = "f4",
        ["comment_odd_quote"] = "f4", ["comment_odd_quote_many_rows"] = "f4", ["eprx_cp932"] = "f4",
        ["eprx_cp932_lossy"] = "f4",
    };

    /// <summary>Cases where a decided rule differs from native (the outcome rendered as the probe renders it).</summary>
    private static readonly Dictionary<string, string> Decided = new()
    {
        // Rule 5: a CR alone ends a record (native reads LF only).
        ["cr_only"] = "a:i64,b:str | 1,\"x\" | 2,\"y\"",
        ["cr_only_no_quote"] = "a:i64,b:str | 1,\"x\" | 2,\"y\"",
        ["f1_lone_cr_in_field"] = "a:str,b:str | \"1\",\"x\" | \"y\",null",
        // Rule 7: RaiseIfEmpty = false reads empty input as an empty frame (native's wrapper refused empty bytes).
        ["empty_input_no_raise"] = "",
        // The row index column is Int64 at the reader (the frame layer casts it to the engine's index type).
        ["row_index"] = "idx:i64,a:i64 | 10,5 | 11,6",
        ["f1_row_index_n_rows"] = "idx:i64,a:i64 | 0,5 | 1,6",
        // D1 (C1b): a datetime needing nanoseconds is inferred datetime[ns] (native truncates to μs).
        ["datetime_fraction"] = "t:datetime[ns] | 2024-01-02T03:04:05.1234567",
        // A zoned column stores instants: naive text is read as UTC, as natively (the probe shows them in the zone,
        // 12:04:05+09:00; this renderer in UTC — the same instant).
        ["f1_schema_tz_tokyo_naive"] = "t:datetime[μs, Asia/Tokyo] | 2024-01-02T03:04:05+00:00",
        ["f1_schema_tz_tokyo_offset"] = "t:datetime[μs, Asia/Tokyo] | 2024-01-02T03:04:05+00:00",
        // A UTF-16 byte order mark is followed (native reads UTF-8 only).
        ["utf16le_bom"] = "a:i64,b:str | 1,\"表\"",
        // R4: a Time column reads each value as it is written.
        ["time_only_infer"] = "t:time | 12:34:56 | 12:00:00",
        // Rule 1: a schema with a header names the columns: output in the schema's order.
        ["schema_same_names_reordered"] = "b:str,a:i64 | \"x\",1",
        // Header names lose their quotes and doubled quotes (native keeps a""b).
        ["f1_header_quoted_doubled"] = "a\"b:i64,c:i64 | 1,2",
        ["f1_header_newline_quoted"] = "a\nb:i64,c:i64 | 1,2",
    };

    /// <summary>Errors: the exception type and facts its message holds.</summary>
    private static readonly Dictionary<string, (Type Type, string[] Facts)> Errors = new()
    {
        ["quote_inside_unquoted"] = (typeof(CsvFormatException), ["Row 1 (line 2)", "field 2 holds a quote"]),
        ["text_after_closing_quote"] = (typeof(CsvFormatException), ["Row 1 (line 2)", "field 2 has text after its closing quote"]),
        ["unterminated_quote"] = (typeof(CsvFormatException), ["Row 1 (line 2)", "field 2 opens a quote that never closes"]),
        ["more_fields_than_header"] = (typeof(CsvFormatException), ["Row 1 (line 2)", "more fields than the 2 columns"]),
        ["no_quote_char"] = (typeof(CsvFormatException), ["Row 1 (line 2)", "more fields than the 2 columns"]),
        ["f1_trailing_sep_extra"] = (typeof(CsvFormatException), ["Row 1 (line 2)", "more fields than the 2 columns"]),
        ["empty_input"] = (typeof(CsvFormatException), ["no records", "RaiseIfEmpty"]),
        ["only_newline"] = (typeof(CsvFormatException), ["no records"]),
        ["late_string_in_int"] = (typeof(CsvValueException), ["column 'a'", "'x' is not an Int64 integer"]),
        ["late_float_in_int"] = (typeof(CsvValueException), ["column 'a'", "'1.5' is not an Int64 integer"]),
        ["f1_n_rows_ragged_after"] = (typeof(CsvFormatException), ["Row 4 (line 5)", "more fields than the 1 columns"]),
        ["f1_inference_limit_then_float"] = (typeof(CsvValueException), ["Row 4 (line 5)", "column 'a'", "'1.5' is not an Int64 integer"]),
        // Rule 1: by name with a header.
        ["schema_rename"] = (typeof(CsvFormatException), ["file's column 'a' is not in the given schema"]),
        ["schema_partial_rename"] = (typeof(CsvFormatException), ["file's column 'b' is not in the given schema"]),
        ["schema_fewer"] = (typeof(CsvFormatException), ["file's column 'c' is not in the given schema"]),
        ["schema_more"] = (typeof(CsvFormatException), ["schema's column 'c' is not in the file"]),
        ["schema_types_follow_position"] = (typeof(CsvValueException), ["column 'b'", "'x' is not an Int64 integer"]),
        ["schema_no_header_fewer"] = (typeof(CsvFormatException), ["schema has 2 columns, the records 3"]),
        ["schema_int8_overflow"] = (typeof(CsvValueException), ["column 'x'", "'300' is not an Int8 integer (out of range"]),
        ["schema_bool_from_01"] = (typeof(CsvValueException), ["column 'x'", "'1' is not a boolean"]),
        ["schema_int_from_float_text"] = (typeof(CsvValueException), ["column 'x'", "'1.0' is not an Int64 integer"]),
        ["bad_int_value"] = (typeof(CsvValueException), ["Row 2 (line 3)", "column 'a'", "'x' is not an Int64 integer"]),
        ["invalid_utf8"] = (typeof(CsvFormatException), ["Row 1 (line 2)", "not UTF-8"]),
        ["invalid_utf8_in_int_col"] = (typeof(CsvFormatException), ["Row 2 (line 3)", "not UTF-8"]),
        ["cp1252_pt"] = (typeof(CsvFormatException), ["The header (line 1)", "not UTF-8"]),
        ["cp932_header"] = (typeof(CsvFormatException), ["The header (line 1)", "not UTF-8"]),
        ["float_hex"] = (typeof(CsvValueException), ["column 'a'", "'0x10' is not a floating-point number"]),
        ["f1_schema_u8_neg"] = (typeof(CsvValueException), ["'-1' is not a UInt8 integer"]),
        ["f1_schema_i64_trail_space"] = (typeof(CsvValueException), ["'5 ' is not an Int64 integer"]),
        ["f1_schema_date_from_datetime"] = (typeof(CsvValueException), ["'2024-01-02 03:04' is not a date"]),
        ["f1_schema_enum_unknown"] = (typeof(CsvValueException), ["Row 2 (line 3)", "'c' is not one of the Enum's categories ('a', 'b')", "add it to the Enum"]),
        // R3: finer than the given unit, a precision error with the fix.
        ["datetime_us_from_ns_text"] = (typeof(CsvValueException), ["column 't'", "more precise than the column's unit (μs)", "datetime[ns]"]),
        // R2: more fraction digits than the scale.
        ["schema_decimal"] = (typeof(CsvValueException), ["Row 3 (line 4)", "'1.234' has 3 decimal places, more than the column's scale of 2"]),
        ["f1_schema_decimal_rounding"] = (typeof(CsvValueException), ["'1.234' has 3 decimal places, more than the column's scale of 2", "decimal(11, 3)"]),
    };

    public static TheoryData<string> Cases()
    {
        var data = new TheoryData<string>();
        foreach (JsonElement c in Doc().RootElement.GetProperty("reads").EnumerateArray()) data.Add(c.GetProperty("name").GetString()!);
        return data;
    }

    private static JsonDocument Doc() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(Dir, "cases.json")));

    private static Dictionary<string, string> Native()
    {
        Dictionary<string, string> outcomes = [];
        foreach (string line in File.ReadAllLines(Path.Combine(Dir, "native.txt")))
        {
            if (!line.StartsWith("R ", StringComparison.Ordinal)) continue;
            int space = line.IndexOf(' ', 2);
            outcomes[line[2..space]] = line[(space + 1)..];
        }
        return outcomes;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Case(string name)
    {
        JsonElement c = Doc().RootElement.GetProperty("reads").EnumerateArray().First(e => e.GetProperty("name").GetString() == name);
        byte[] bytes = Convert.FromHexString(c.GetProperty("hex").GetString()!);
        CsvReadOptions options = Options(c.GetProperty("opts"));
        if (Later.ContainsKey(name))
        {
            Assert.Throws<NotSupportedException>(() => Read(bytes, options));
            return;
        }
        string native = Native()[name];
        if (Errors.TryGetValue(name, out var expected))
        {
            Exception e = Assert.ThrowsAny<Exception>(() => Read(bytes, options));
            Assert.True(e.GetType() == expected.Type, $"{name}: {e.GetType().Name}: {e.Message}");
            foreach (string fact in expected.Facts) Assert.True(e.Message.Contains(fact, StringComparison.Ordinal), $"{name}: '{e.Message}' lacks '{fact}'");
            return;
        }
        string want = Decided.TryGetValue(name, out string? decided) ? decided : native.StartsWith("OK ", StringComparison.Ordinal) ? native[3..] : native;
        Assert.False(want.StartsWith("ERR", StringComparison.Ordinal), $"{name}: native fails ({native}) and no decided outcome is written");
        Assert.Equal(want, Read(bytes, options));
    }

    /// <summary>Every case read in units of a few bytes, from every kind of source, gives what one unit gives (values or error text).</summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public void CaseInSmallUnits(string name)
    {
        JsonElement c = Doc().RootElement.GetProperty("reads").EnumerateArray().First(e => e.GetProperty("name").GetString() == name);
        byte[] bytes = Convert.FromHexString(c.GetProperty("hex").GetString()!);
        CsvReadOptions options = Options(c.GetProperty("opts"));
        string whole = Outcome(() => Read(bytes, options));
        foreach (int unit in (int[])[1, 2, 3, 7, 64])
            foreach (CsvSource source in Enum.GetValues<CsvSource>())
                Assert.Equal(whole, Outcome(() => Read(bytes, options with { UnitBytes = unit, MaxDegreeOfParallelism = 3 }, source)));

        static string Outcome(Func<string> read)
        {
            try { return read(); }
            catch (Exception e) { return $"{e.GetType().Name}: {e.Message}"; }
        }
    }

    internal enum CsvSource { Memory, Path, PathUnmapped, Seekable, NonSeekable }

    internal static string Read(byte[] bytes, CsvReadOptions options, CsvSource source)
    {
        string? path = null;
        try
        {
            CsvReader reader;
            switch (source)
            {
                case CsvSource.Memory:
                    reader = CsvReader.Open(bytes, options);
                    break;
                case CsvSource.Path or CsvSource.PathUnmapped:
                    path = Path.GetTempFileName();
                    File.WriteAllBytes(path, bytes);
                    reader = CsvReader.Open(path, options with { MemoryMap = source == CsvSource.Path ? CsvMemoryMap.Auto : CsvMemoryMap.Off });
                    break;
                case CsvSource.Seekable:
                    reader = CsvReader.Open(new MemoryStream(bytes), options);
                    break;
                default:
                    reader = CsvReader.Open(new NonSeekableStream(bytes), options);
                    break;
            }
            using (reader)
            {
                var batches = reader.ReadBatches().ToList();
                try { return Render(reader.Schema, batches); }
                finally { batches.ForEach(b => b.Dispose()); }
            }
        }
        finally
        {
            if (path is not null) File.Delete(path);
        }
    }

    /// <summary>A stream that cannot seek (read in order, a few bytes at a time).</summary>
    internal sealed class NonSeekableStream(byte[] bytes) : Stream
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
            int n = Math.Min(Math.Min(buffer.Length, 5), bytes.Length - _at);
            bytes.AsSpan(_at, n).CopyTo(buffer);
            _at += n;
            return n;
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>The options of a case (Polars' snake_case names; absent ones take our defaults).</summary>
    internal static CsvReadOptions Options(JsonElement o)
    {
        var options = new CsvReadOptions();
        foreach (JsonProperty p in o.EnumerateObject())
        {
            JsonElement v = p.Value;
            options = p.Name switch
            {
                "schema" => options with { Schema = new Schema([.. v.EnumerateObject().Select(f => FieldOf(f.Name, f.Value.GetString()!))], null) },
                "schema_overrides" => options with { SchemaOverrides = v.EnumerateObject().ToDictionary(f => f.Name, f => FieldOf(f.Name, f.Value.GetString()!).DataType) },
                "has_header" => options with { HasHeader = v.GetBoolean() },
                "separator" => options with { Separator = v.GetString()![0] },
                "quote_char" => options with { Quote = v.ValueKind == JsonValueKind.Null ? null : v.GetString()![0] },
                "eol_char" => options with { LineEnd = v.GetString()![0] },
                "ignore_errors" => options with { IgnoreErrors = v.GetBoolean() },
                "try_parse_dates" => options with { TryParseDates = v.GetBoolean() },
                "raise_if_empty" => options with { RaiseIfEmpty = v.GetBoolean() },
                "skip_rows" => options with { SkipRows = v.GetInt64() },
                "skip_rows_after_header" => options with { SkipRowsAfterHeader = v.GetInt64() },
                "skip_lines" => options with { SkipLines = v.GetInt64() },
                "n_rows" => options with { MaxRows = v.GetInt64() },
                "infer_schema_length" => options with { InferSchemaLength = v.ValueKind == JsonValueKind.Null ? null : v.GetInt32() },
                "row_index_name" => options with { RowIndexColumn = v.GetString() },
                "row_index_offset" => options with { RowIndexOffset = v.GetInt64() },
                "encoding" => options with { InvalidText = v.GetString() == "utf8-lossy" ? CsvInvalidText.Replace : CsvInvalidText.Error },
                "null_values" => options with { NullValues = [.. v.EnumerateArray().Select(x => x.GetString()!)] },
                "missing_is_null" => options with { MissingIsNull = v.GetBoolean() },
                "comment_prefix" => options with { CommentPrefix = v.GetString() },
                "decimal_comma" => options with { DecimalComma = v.GetBoolean() },
                "truncate_ragged_lines" => options with { TruncateRaggedLines = v.GetBoolean() },
                _ => throw new ArgumentException($"Unknown probe option {p.Name}."),
            };
        }
        return options;
    }

    private static Field FieldOf(string name, string type) => type switch
    {
        "i64" => new Field(name, Int64Type.Default, true),
        "i8" => new Field(name, Int8Type.Default, true),
        "u8" => new Field(name, UInt8Type.Default, true),
        "u64" => new Field(name, UInt64Type.Default, true),
        "i128" => new Field(name, WideIntegers.Type, true, [new(WideIntegers.MetadataKey, WideIntegers.Signed)]),
        "f64" => new Field(name, DoubleType.Default, true),
        "f32" => new Field(name, FloatType.Default, true),
        "str" => new Field(name, StringType.Default, true),
        "bool" => new Field(name, BooleanType.Default, true),
        "date" => new Field(name, Date32Type.Default, true),
        "time" => new Field(name, new Time64Type(TimeUnit.Nanosecond), true),
        "cat" => new Field(name, new DictionaryType(Int32Type.Default, StringType.Default, false), true),
        "enum_ab" => new Field(name, new DictionaryType(Int32Type.Default, StringType.Default, false), true,
            [new(EnumCategories.EnumMetadataKey, "1;a1;b")]),
        "datetime_us" => new Field(name, new TimestampType(TimeUnit.Microsecond, (string?)null), true),
        "datetime_us_utc" => new Field(name, new TimestampType(TimeUnit.Microsecond, "UTC"), true),
        "datetime_us_tokyo" => new Field(name, new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), true),
        "datetime_ns" => new Field(name, new TimestampType(TimeUnit.Nanosecond, (string?)null), true),
        "decimal_10_2" => new Field(name, new Decimal128Type(10, 2), true),
        _ => throw new ArgumentException(type),
    };

    /// <summary>The frame read, rendered as the probe renders Polars frames.</summary>
    internal static string Read(byte[] bytes, CsvReadOptions options)
    {
        using CsvReader reader = CsvReader.Open(bytes, options);
        var batches = reader.ReadBatches().ToList();
        try { return Render(reader.Schema, batches); }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    internal static string Render(Schema schema, List<RecordBatch> batches)
    {
        var sb = new StringBuilder(string.Join(",", schema.FieldsList.Select(f => $"{f.Name}:{TypeName(f)}")));
        foreach (RecordBatch batch in batches)
            for (int r = 0; r < batch.Length; r++)
            {
                sb.Append(" | ");
                sb.Append(string.Join(",", Enumerable.Range(0, batch.ColumnCount).Select(c => Value(schema.FieldsList[c], batch.Column(c), r))));
            }
        return sb.ToString();
    }

    private static string TypeName(Field f) => f.DataType switch
    {
        Int64Type => "i64",
        Int32Type => "i32",
        Int8Type => "i8",
        UInt8Type => "u8",
        UInt32Type => "u32",
        UInt64Type => "u64",
        DoubleType => "f64",
        FloatType => "f32",
        BooleanType => "bool",
        StringType or StringViewType or LargeStringType => "str",
        Date32Type => "date",
        Time64Type => "time",
        TimestampType t => $"datetime[{(t.Unit == TimeUnit.Nanosecond ? "ns" : t.Unit == TimeUnit.Microsecond ? "μs" : "ms")}{(t.Timezone is { } z ? $", {z}" : "")}]",
        Decimal128Type when WideIntegers.KindOf(f) is not null => "i128",
        Decimal128Type d => $"decimal[{d.Precision},{d.Scale}]",
        DictionaryType when f.Metadata?.ContainsKey(EnumCategories.EnumMetadataKey) == true => "enum",
        DictionaryType => "cat",
        _ => f.DataType.Name,
    };

    private static string Value(Field f, IArrowArray a, int i)
    {
        if (a.IsNull(i)) return "null";
        return a switch
        {
            BooleanArray b => b.GetValue(i)!.Value ? "true" : "false",
            Int8Array x => x.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int32Array x => x.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array x => x.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt8Array x => x.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt64Array x => x.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            DoubleArray x => x.GetValue(i)!.Value.ToString("R", CultureInfo.InvariantCulture),
            FloatArray x => x.GetValue(i)!.Value.ToString("R", CultureInfo.InvariantCulture),
            StringArray s => Escape(s.GetString(i)),
            StringViewArray s => Escape(s.GetString(i)),
            LargeStringArray s => Escape(s.GetString(i)),
            DictionaryArray d => Escape(((StringArray)d.Dictionary).GetString(Convert.ToInt32(((Int32Array)d.Indices).GetValue(i)!.Value))),
            Date32Array x => DateOnly.FromDayNumber(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + x.Values[i]).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            Time64Array x => new TimeOnly(x.Values[i] / (((Time64Type)x.Data.DataType).Unit == TimeUnit.Nanosecond ? 100 : 1) * (((Time64Type)x.Data.DataType).Unit == TimeUnit.Nanosecond ? 1 : 10))
                .ToString("HH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture),
            TimestampArray x => Timestamp((TimestampType)x.Data.DataType, x.Values[i]),
            Decimal128Array d => Decimal(f, d.ValueBuffer.Span.Slice(i * 16 + d.Offset * 16, 16)),
            _ => throw new NotSupportedException(a.GetType().Name),
        };
    }

    private static string Timestamp(TimestampType t, long value)
    {
        long ticks = t.Unit switch
        {
            TimeUnit.Nanosecond => value / 100,
            TimeUnit.Microsecond => value * 10,
            TimeUnit.Millisecond => value * 10_000,
            _ => value * 10_000_000,
        };
        DateTime at = DateTime.UnixEpoch.AddTicks(ticks);
        return t.Timezone is null
            ? at.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture)
            : new DateTimeOffset(at, TimeSpan.Zero).ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFzzz", CultureInfo.InvariantCulture);
    }

    private static string Decimal(Field f, ReadOnlySpan<byte> bytes)
    {
        Int128 unscaled = System.Buffers.Binary.BinaryPrimitives.ReadInt128LittleEndian(bytes);
        if (WideIntegers.KindOf(f) is not null) return unscaled.ToString(CultureInfo.InvariantCulture);
        int scale = ((Decimal128Type)f.DataType).Scale;
        string digits = Int128.Abs(unscaled).ToString(CultureInfo.InvariantCulture).PadLeft(scale + 1, '0');
        string text = scale == 0 ? digits : $"{digits[..^scale]}.{digits[^scale..]}";
        return unscaled < 0 ? "-" + text : text;
    }

    private static string Escape(string s)
    {
        var sb = new StringBuilder("\"");
        foreach (char ch in s)
            sb.Append(ch switch
            {
                '\n' => "\\n",
                '\r' => "\\r",
                '\t' => "\\t",
                '"' => "\\\"",
                '\\' => "\\\\",
                < ' ' => $"\\u{(int)ch:x4}",
                _ => ch.ToString(),
            });
        return sb.Append('"').ToString();
    }
}
