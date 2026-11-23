using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Json.Reading;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// Newline-delimited JSON read by <see cref="JsonReader"/>: the expectations are Python Polars 2.1's
/// (<c>read_ndjson</c>), except where Polars changes or drops values (see the tests naming the difference).
/// </summary>
public class JsonReadTests
{
    // ---------------------------------------------------------------- helpers

    internal enum Source { Memory, Path, PathUnmapped, NonSeekable, RandomAccess }

    internal static readonly Source[] Sources = [Source.Memory, Source.Path, Source.PathUnmapped, Source.NonSeekable, Source.RandomAccess];

    /// <summary>The rows of <paramref name="text"/> read from <paramref name="source"/>, and the schema.</summary>
    internal static (Schema Schema, List<string> Rows) Read(string text, JsonReadOptions? options = null, Source source = Source.Memory) =>
        Read(Encoding.UTF8.GetBytes(text), options, source);

    internal static (Schema Schema, List<string> Rows) Read(byte[] bytes, JsonReadOptions? options = null, Source source = Source.Memory)
    {
        options ??= new JsonReadOptions();
        string? path = null;
        try
        {
            using JsonReader reader = Open(bytes, options, source, ref path);
            var batches = reader.ReadBatches().ToList();
            try { return (reader.Schema, ArrowRender.Rows(batches)); }
            finally { batches.ForEach(b => b.Dispose()); }
        }
        finally
        {
            if (path is not null) File.Delete(path);
        }
    }

    internal static JsonReader Open(byte[] bytes, JsonReadOptions options, Source source, ref string? path)
    {
        switch (source)
        {
            case Source.Memory: return JsonReader.Open(new MemoryStream(bytes), options);
            case Source.NonSeekable: return JsonReader.Open(new NonSeekable(new MemoryStream(bytes)), options);
            case Source.RandomAccess: return JsonReader.Open(new BytesAccess(bytes), options);
            default:
                path = Path.Combine(Path.GetTempPath(), $"json_{Guid.NewGuid():N}.ndjson");
                File.WriteAllBytes(path, bytes);
                return JsonReader.Open(path, options with { MemoryMap = source == Source.Path ? JsonMemoryMap.Auto : JsonMemoryMap.Off });
        }
    }

    internal static Exception Fails(string text, JsonReadOptions? options = null) => Assert.ThrowsAny<Exception>(() => Read(text, options));

    internal static string Types(Schema schema) => string.Join(", ", schema.FieldsList.Select(f => $"{f.Name}: {Name(f.DataType)}"));

    internal static string Name(IArrowType t) => t switch
    {
        StructType s => "{" + string.Join(", ", s.Fields.Select(f => $"{f.Name}: {Name(f.DataType)}")) + "}",
        ListType l => $"list<{Name(l.ValueDataType)}>",
        Decimal128Type d => $"decimal({d.Precision},{d.Scale})",
        _ => t.Name,
    };

    internal static Schema Schema(params (string Name, IArrowType Type)[] fields) => new([.. fields.Select(f => new Field(f.Name, f.Type, true))], null);

    /// <summary>A stream that cannot seek (read in order).</summary>
    internal sealed class NonSeekable(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, Math.Min(count, 7_001));
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        protected override void Dispose(bool disposing) { if (disposing) inner.Dispose(); base.Dispose(disposing); }
    }

    /// <summary>Bytes read at offsets, as a store's object is (never in place).</summary>
    internal sealed class BytesAccess(byte[] bytes) : IRandomAccess
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

    // ---------------------------------------------------------------- lines

    /// <summary>
    /// A source read at offsets in many small units, its last line with or without a newline: every line is read once,
    /// the last one whole (a unit reads past its block to finish its last line, up to the end of the input).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Lines_ReadAtOffsets_LastLineWhole(bool newlineLast)
    {
        string text = string.Join("\n", Enumerable.Range(0, 1000).Select(static i => $"{{\"a\":{i * 1001}}}")) + (newlineLast ? "\n" : "");
        foreach (int unit in (int[])[1, 7, 37, 1000])
        {
            using JsonReader reader = JsonReader.Open(new BytesAccess(Encoding.UTF8.GetBytes(text)), new JsonReadOptions { UnitBytes = unit });
            long rows = 0, sum = 0;
            foreach (RecordBatch batch in reader.ReadBatches())
                using (batch)
                {
                    rows += batch.Length;
                    var a = (Int64Array)batch.Column(0);
                    for (int i = 0; i < a.Length; i++) sum += a.GetValue(i)!.Value;
                }
            Assert.Equal(1000, rows);
            Assert.Equal(Enumerable.Range(0, 1000).Sum(static i => i * 1001L), sum);
        }
    }

    [Fact]
    public void BlankLines_CrLf_AndNoFinalNewline_AreRead()
    {
        var (_, rows) = Read("{\"a\":1}\r\n\r\n   \n{\"a\":2}\n\t\n{\"a\":3}");
        Assert.Equal(["1", "2", "3"], rows);
    }

    [Fact]
    public void AByteOrderMark_IsSkipped_UnlikePolars()
    {
        var (_, rows) = Read([0xEF, 0xBB, 0xBF, .. "{\"a\":1}\n{\"a\":2}\n"u8]);
        Assert.Equal(["1", "2"], rows);
    }

    [Theory]
    [InlineData("{\"a\":1} {\"a\":2}\n")]          // two objects on a line
    [InlineData("{\"a\":\n1}\n")]                  // an object over two lines
    [InlineData("{\"a\":1}\n5\n")]                 // a scalar line
    [InlineData("{\"a\":1}\nnull\n")]              // a null line
    [InlineData("[1,2]\n")]                        // an array line
    [InlineData("{\"a\":1,}\n")]                   // a trailing comma
    [InlineData("{\"a\":1} // c\n")]               // a comment
    [InlineData("{\"a\":NaN}\n")]                  // NaN
    [InlineData("{\"a\":\"x\u0001y\"}\n")]         // a raw control character
    [InlineData("{\"a\":\"\\ud800\"}\n")]          // a lone surrogate (Polars reads "\0")
    public void WhatIsNotOneJsonObjectPerLine_IsRefused(string text) =>
        Assert.IsType<JsonFormatException>(Fails(text, new JsonReadOptions { Schema = Schema(("a", StringType.Default)) }));

    [Fact]
    public void InvalidUtf8_IsRefused_AtItsLine()
    {
        var e = Assert.Throws<JsonFormatException>(() => Read([.. "{\"a\":\"x\"}\n{\"a\":\"x\"}\n{\"a\":\""u8, 0xFF, 0xFE, .. "\"}\n"u8]));
        Assert.StartsWith("Line 3:", e.Message);
    }

    [Fact]
    public void AnEmptyInput_HasNoRows_WithASchema_AndNothingToInferWithout()
    {
        Assert.Empty(Read("", new JsonReadOptions { Schema = Schema(("a", Int64Type.Default)) }).Rows);
        Assert.Empty(Read("\n \n", new JsonReadOptions { Schema = Schema(("a", Int64Type.Default)) }).Rows);
        Assert.IsType<JsonFormatException>(Fails(""));
    }

    [Fact]
    public void Errors_NameTheLine_AcrossUnits()
    {
        var text = new StringBuilder();
        for (int i = 1; i < 57; i++) text.Append($"{{\"a\":{i}}}\n");
        text.Append("{\"a\":\"oops\"}\n{\"a\":58}\n");
        foreach (Source source in Sources)
        {
            var e = Assert.Throws<JsonValueException>(() => Read(text.ToString(), new JsonReadOptions { InferSchemaLength = 10, UnitBytes = 50 }, source));
            Assert.StartsWith("Line 57: Column 'a':", e.Message);
        }
    }

    // ---------------------------------------------------------------- inference (Polars' types)

    [Theory]
    [InlineData("{\"a\":1}\n{\"a\":2.5}", "a: double", "1 | 2.5")]
    [InlineData("{\"a\":2.5}\n{\"a\":1}", "a: double", "2.5 | 1")]
    [InlineData("{\"a\":1}\n{\"a\":\"x\"}", "a: utf8view", "\"1\" | \"x\"")]
    [InlineData("{\"a\":true}\n{\"a\":\"x\"}", "a: utf8view", "\"true\" | \"x\"")]
    [InlineData("{\"a\":\"x\"}\n{\"a\":{\"b\": 1}}", "a: utf8view", "\"x\" | \"{\"b\": 1}\"")]
    [InlineData("{\"a\":null}\n{\"a\":null}", "a: null", "null | null")]
    [InlineData("{\"a\":null}\n{\"a\":{\"b\":1}}", "a: {b: int64}", "null | {b: 1}")]
    [InlineData("{\"a\":1}\n{\"b\":2}", "a: int64, b: int64", "1 | null | null | 2")]
    [InlineData("{\"a\":1,\"b\":2}\n{\"b\":3,\"a\":4}", "a: int64, b: int64", "1 | 2 | 4 | 3")]
    [InlineData("{\"z\":1}\n{\"y\":1,\"z\":2}", "y: int64, z: int64", "null | 1 | 1 | 2")]
    [InlineData("{\"b\":1,\"a\":2}\n{\"a\":3,\"c\":4}", "b: int64, a: int64, c: int64", "1 | 2 | null | null | 3 | 4")]
    [InlineData("{\"c\":1,\"b\":1}\n{\"a\":1,\"b\":1}\n{\"a\":1,\"c\":1}", "c: int64, b: int64, a: int64", "1 | 1 | null | null | 1 | 1 | 1 | null | 1")]
    [InlineData("{\"a\":1}\n{\"b\":1,\"c\":1}\n{\"d\":1,\"e\":1,\"a\":1}", "b: int64, c: int64, a: int64, d: int64, e: int64", "null | null | 1 | null | null | 1 | 1 | null | null | null | null | null | 1 | 1 | 1")]
    [InlineData("{\"n\":1,\"i\":1,\"o\":1}\n{\"i\":2,\"s\":2,\"f\":2,\"n\":2}", "i: int64, s: int64, f: int64, n: int64, o: int64", "1 | null | null | 1 | 1 | 2 | 2 | 2 | 2 | null")]
    [InlineData("{\"s\":{\"y\":1,\"x\":2}}\n{\"s\":{\"x\":3,\"w\":1}}", "s: {y: int64, x: int64, w: int64}", "{y: 1, x: 2, w: null} | {y: null, x: 3, w: 1}")]
    [InlineData("{\"s\":{\"x\":1}}\n{\"s\":{\"y\":\"z\"}}", "s: {x: int64, y: utf8view}", "{x: 1, y: null} | {x: null, y: \"z\"}")]
    [InlineData("{\"s\":{\"x\":1}}\n{\"s\":{\"x\":2.5}}", "s: {x: double}", "{x: 1} | {x: 2.5}")]
    [InlineData("{\"l\":[1,2.5]}", "l: list<double>", "[1, 2.5]")]
    [InlineData("{\"l\":[1,\"x\"]}", "l: list<utf8view>", "[\"1\", \"x\"]")]
    [InlineData("{\"l\":[]}", "l: list<null>", "[]")]
    [InlineData("{\"l\":[]}\n{\"l\":[1]}", "l: list<int64>", "[] | [1]")]
    [InlineData("{\"l\":[null,1]}", "l: list<int64>", "[null, 1]")]
    [InlineData("{\"l\":[[1],[2.5]]}", "l: list<list<double>>", "[[1], [2.5]]")]
    [InlineData("{\"l\":[{\"x\":1},{\"y\":2}]}", "l: list<{x: int64, y: int64}>", "[{x: 1, y: null}, {x: null, y: 2}]")]
    [InlineData("{\"a\":-0.0}", "a: double", "-0")]
    [InlineData("{\"a\":1.0}\n{\"a\":1e3}", "a: double", "1 | 1000")]
    [InlineData("{\"a\":1,\"a\":2}\n{\"a\":3}", "a: int64", "1 | 3")]
    [InlineData("{\"\\u0061\":1}", "a: int64", "1")]
    [InlineData("{\"a\\\"b\":1,\"\":2}", "a\"b: int64, : int64", "1 | 2")]
    [InlineData("{\"a\":\"\\u00e9\\ud83d\\ude00\"}", "a: utf8view", "\"é😀\"")]
    [InlineData("{\"a\":\"2024-01-02\"}", "a: utf8view", "\"2024-01-02\"")]
    public void Types_AreInferredAsPolarsInfersThem(string text, string types, string values)
    {
        var (schema, rows) = Read(text);
        Assert.Equal(types, Types(schema));
        Assert.Equal(values, string.Join(" | ", rows));
    }

    [Fact]
    public void IntegersBeyondInt64_AreInt128()
    {
        var (schema, rows) = Read("{\"a\":18446744073709551615}\n{\"a\":-99999999999999999999}\n{\"a\":1}");
        Assert.Equal("a: decimal(38,0)", Types(schema));
        Assert.Equal("i128", schema.FieldsList[0].Metadata["kaminari:int128"]);
        Assert.Equal(["18446744073709551615", "-99999999999999999999", "1"], rows);
        Assert.Equal("a: double", Types(Read("{\"a\":99999999999999999999}\n{\"a\":1.5}").Schema));
    }

    [Theory]
    [InlineData("{\"a\":true}\n{\"a\":1}")]
    [InlineData("{\"a\":true}\n{\"a\":1.5}")]
    [InlineData("{\"a\":{\"b\":1}}\n{\"a\":[1]}")]
    [InlineData("{\"a\":1}\n{\"a\":[1]}")]
    [InlineData("{\"s\":{\"x\":1}}\n{\"s\":5}")]
    [InlineData("{\"a\":1234567890123456789012345678901234567890}")]
    [InlineData("{\"a\":1e400}")]
    [InlineData("{\"l\":[{\"b\":1},2]}")]       // Polars: List(String), though [true,1] and the same mix across rows fail there
    [InlineData("{\"l\":[1.5,true]}")]           // Polars: List(String)
    [InlineData("{\"a\":{}}")]                    // Polars: Struct({}), which Apache Arrow (.NET) cannot type
    public void ValuesNoTypeHolds_AreRefused(string text) => Assert.IsType<JsonValueException>(Fails(text));

    [Fact]
    public void DeepNesting_IsRead()
    {
        string text = "{\"a\":" + new string('[', 70) + "1" + new string(']', 70) + "}";
        var (schema, rows) = Read(text);
        Assert.StartsWith("a: list<list<", Types(schema));
        Assert.Equal(new string('[', 70) + "1" + new string(']', 70), rows[0]);
    }

    [Fact]
    public void BeyondTheInferenceWindow_NewFieldsArePassedOver_AndMisfitsAreErrors()
    {
        var one = new JsonReadOptions { InferSchemaLength = 1 };
        Assert.Equal(["1", "2"], Read("{\"a\":1}\n{\"a\":2,\"b\":3}", one).Rows);
        Assert.Equal("Line 2: Column 'a': the string \"x\" is not an Int64 integer.", Fails("{\"a\":1}\n{\"a\":\"x\"}", one).Message);
        Assert.IsType<JsonValueException>(Fails("{\"a\":1}\n{\"a\":2.5}", one));
        Assert.IsType<JsonValueException>(Fails("{\"a\":null}\n{\"a\":3}", one));
        Assert.Equal(["1", "null"], Read("{\"a\":1}\n{\"a\":\"x\"}", one with { IgnoreErrors = true }).Rows);
        Assert.Equal(["\"1\"", "\"x\""], Read("{\"a\":1}\n{\"a\":\"x\"}", new JsonReadOptions { InferSchemaLength = null }).Rows);
    }

    // ---------------------------------------------------------------- a given schema

    [Fact]
    public void AGivenSchema_PassesOverOtherFields_AndNullsMissingOnes()
    {
        var o = new JsonReadOptions { Schema = Schema(("a", Int64Type.Default), ("s", new StructType([new Field("x", Int64Type.Default, true), new Field("y", StringType.Default, true)]))) };
        var (schema, rows) = Read("{\"z\":[1,{\"q\":2}],\"a\":1,\"s\":{\"x\":1,\"w\":{}}}\n{\"s\":null}\n{\"a\":null}", o);
        Assert.Equal("a: int64, s: {x: int64, y: utf8}", Types(schema));
        Assert.Equal(["1 | {x: 1, y: null}", "null | null", "null | null"], rows);
    }

    [Theory]
    [InlineData("{\"a\":300}", "int8")]
    [InlineData("{\"a\":-1}", "uint8")]
    [InlineData("{\"a\":1.5}", "int64")]
    [InlineData("{\"a\":1.0}", "int64")]
    [InlineData("{\"a\":1.5e2}", "int64")]
    [InlineData("{\"a\":\"12\"}", "int64")]
    [InlineData("{\"a\":1}", "bool")]
    [InlineData("{\"a\":\"true\"}", "bool")]
    [InlineData("{\"a\":true}", "double")]
    [InlineData("{\"a\":\"1.5\"}", "double")]
    [InlineData("{\"a\":19000}", "date32")]
    [InlineData("{\"a\":\"2024-01-02T03:04:05\"}", "date32")]
    [InlineData("{\"a\":1700000000000000}", "timestamp")]
    [InlineData("{\"a\":\"2024-01-02T03:04:05.123456789\"}", "timestamp")]
    [InlineData("{\"a\":\"2024-01-02T03:04:05.1234\"}", "timestamp_ms")]
    [InlineData("{\"a\":1.255}", "decimal")]
    [InlineData("{\"a\":\"aGk\"}", "binary")]
    [InlineData("{\"a\":5}", "list")]
    [InlineData("{\"a\":[1]}", "fixed2")]
    public void ValuesTheGivenTypeCannotHold_AreRefused(string text, string type)
    {
        IArrowType t = type switch
        {
            "int8" => Int8Type.Default,
            "uint8" => UInt8Type.Default,
            "int64" => Int64Type.Default,
            "bool" => BooleanType.Default,
            "double" => DoubleType.Default,
            "date32" => Date32Type.Default,
            "timestamp" => new TimestampType(TimeUnit.Microsecond, (string?)null),
            "timestamp_ms" => new TimestampType(TimeUnit.Millisecond, (string?)null),
            "decimal" => new Decimal128Type(10, 2),
            "binary" => BinaryType.Default,
            "list" => new ListType(new Field("item", Int64Type.Default, true)),
            _ => new FixedSizeListType(new Field("item", Int64Type.Default, true), 2),
        };
        var o = new JsonReadOptions { Schema = Schema(("a", t)) };
        Assert.IsType<JsonValueException>(Fails(text, o));
        if (type != "fixed2") Assert.Equal(["null"], Read(text, o with { IgnoreErrors = true }).Rows);
    }

    [Fact]
    public void Values_ConvertToTheGivenTypes()
    {
        var o = new JsonReadOptions
        {
            Schema = Schema(
                ("i8", Int8Type.Default), ("u64", UInt64Type.Default), ("f32", FloatType.Default), ("h", HalfFloatType.Default),
                ("s", StringType.Default), ("d", Date32Type.Default), ("ts", new TimestampType(TimeUnit.Microsecond, (string?)null)),
                ("tsz", new TimestampType(TimeUnit.Microsecond, "UTC")), ("ns", new TimestampType(TimeUnit.Nanosecond, (string?)null)),
                ("t", new Time64Type(TimeUnit.Nanosecond)), ("t32", new Time32Type(TimeUnit.Millisecond)), ("dur", DurationType.Microsecond),
                ("dec", new Decimal128Type(10, 2)), ("bin", BinaryType.Default), ("lv", BinaryViewType.Default),
                ("cat", new DictionaryType(Int32Type.Default, StringType.Default, false)),
                ("m", new MapType(new Field("key", StringType.Default, false), new Field("value", Int64Type.Default, true))),
                ("fx", new FixedSizeListType(new Field("item", Int64Type.Default, true), 2)),
                ("big", new Field("big", new Decimal128Type(38, 0), true, [new("kaminari:int128", "u128")]).DataType)),
        };
        // The u128 metadata travels on the field: give it there.
        o = o with { Schema = new Schema([.. o.Schema!.FieldsList.Select(f => f.Name == "big" ? new Field("big", new Decimal128Type(38, 0), true, [new("kaminari:int128", "u128")]) : f)], null) };
        string line = """
            {"i8":-128,"u64":18446744073709551615,"f32":16777217,"h":0.5,"s":{"b": [1, true]},"d":"2024-02-29","ts":"2024-01-02 03:04:05.5",
             "tsz":"2024-01-02T03:04:05+09:00","ns":"2024-01-02T03:04:05.123456789Z","t":"03:04:05.000000001","t32":"23:59:59.999","dur":"-P1DT2H3M4.000005S",
             "dec":"-0.05","bin":"aGk=","lv":"","cat":"x","m":{"k":1,"j":null},"fx":[1,2],"big":340282366920938463463374607431768211455}
            """.ReplaceLineEndings("");
        var (_, rows) = Read(line + "\n" + line.Replace("\"cat\":\"x\"", "\"cat\":\"y\"").Replace("\"x\"", "\"x\"") + "\n" + "{\"cat\":\"x\"}", o);
        string[] first = rows[0].Split(" | ");
        Assert.Equal(["-128", "18446744073709551615", "16777216", "0.5", "\"{\"b\": [1, true]}\"", "19782", "1704164645500000",
            "1704132245000000", "1704164645123456789", "11045000000001", "86399999", "-93784000005",
            "-0.05", "0x6869", "0x", "\"x\"", "{\"k\": 1, \"j\": null}", "[1, 2]"], first[..^1]);
        Assert.Equal("\"y\"", rows[1].Split(" | ")[15]);
        Assert.Equal("\"x\"", rows[2].Split(" | ")[15]);
    }

    [Fact]
    public void TimeZones_ReadAsPolarsReadsThem()
    {
        var o = new JsonReadOptions { Schema = Schema(("naive", new TimestampType(TimeUnit.Microsecond, (string?)null)), ("tokyo", new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"))) };
        // An offset into a naive column: the UTC wall clock; a naive text into a zoned column: read as UTC.
        var (_, rows) = Read("{\"naive\":\"2024-01-02T03:04:05+09:00\",\"tokyo\":\"2024-01-02 03:04:05\"}", o);
        Assert.Equal(["1704132245000000 | 1704164645000000"], rows);
    }

    [Fact]
    public void UnsupportedTypes_FailWhenOpening() =>
        Assert.Throws<NotSupportedException>(() => Read("{\"a\":1}", new JsonReadOptions { Schema = Schema(("a", new Decimal256Type(50, 0))) }));

    // ---------------------------------------------------------------- options

    [Fact]
    public void Columns_AreReadInSchemaOrder_AndUnknownOnesFail()
    {
        var (schema, rows) = Read("{\"a\":1,\"b\":\"x\",\"c\":true}", new JsonReadOptions { Columns = ["c", "a"] });
        Assert.Equal("a: int64, c: bool", Types(schema));
        Assert.Equal(["1 | true"], rows);
        Assert.Throws<ArgumentException>(() => Read("{\"a\":1}", new JsonReadOptions { Columns = ["z"] }));
    }

    [Fact]
    public void SchemaOverrides_TypeSomeColumns()
    {
        var (schema, rows) = Read("{\"a\":1,\"b\":2}", new JsonReadOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["a"] = Int32Type.Default } });
        Assert.Equal("a: int32, b: int64", Types(schema));
        Assert.Equal(["1 | 2"], rows);
        Assert.Throws<ArgumentException>(() => Read("{\"a\":1}", new JsonReadOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["z"] = Int32Type.Default } }));
        Assert.Throws<ArgumentException>(() => Read("{\"a\":1}", new JsonReadOptions { Schema = Schema(("a", Int64Type.Default)), SchemaOverrides = new Dictionary<string, IArrowType>() }));
    }

    [Fact]
    public void ViewTypesOff_InfersUtf8() =>
        Assert.Equal("a: utf8", Types(Read("{\"a\":\"x\"}", new JsonReadOptions { ViewTypes = false }).Schema));

    [Fact]
    public void BatchSize_CutsBatches()
    {
        string text = string.Concat(Enumerable.Range(0, 10).Select(i => $"{{\"a\":{i}}}\n"));
        using var reader = JsonReader.Open(new MemoryStream(Encoding.UTF8.GetBytes(text)), new JsonReadOptions { BatchSize = 4 });
        var batches = reader.ReadBatches().ToList();
        Assert.Equal([4, 4, 2], batches.Select(b => b.Length));
        batches.ForEach(b => b.Dispose());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(25)]
    [InlineData(1000)]
    public void MaxRows_KeepsExactlyTheFirstRows(long max)
    {
        string text = string.Concat(Enumerable.Range(0, 100).Select(i => $"{{\"a\":{i}}}\n"));
        foreach (Source source in Sources)
        {
            var (_, rows) = Read(text, new JsonReadOptions { MaxRows = max, UnitBytes = 40, Schema = Schema(("a", Int64Type.Default)) }, source);
            Assert.Equal(Enumerable.Range(0, (int)Math.Min(max, 100)).Select(i => i.ToString()), rows);
        }
    }

    // ---------------------------------------------------------------- inputs

    /// <summary>A messy input: every kind of value, escapes, blank and CRLF lines, long lines, fields in varying order.</summary>
    internal static string Messy(int lines)
    {
        var random = new Random(17);
        var text = new StringBuilder();
        for (int i = 0; i < lines; i++)
        {
            if (random.Next(10) == 0) text.Append(random.Next(2) == 0 ? "\n" : "  \r\n");
            List<string> fields = [$"\"id\":{i}", $"\"x\":{(random.Next(5) == 0 ? "null" : (i * 0.25).ToString(System.Globalization.CultureInfo.InvariantCulture))}",
                $"\"s\":\"{(i % 7 == 0 ? "tab\\there \\u00e9 \\\"q\\\"" : new string('w', random.Next(i % 50 == 0 ? 3000 : 20)))}\"",
                $"\"l\":[{string.Join(",", Enumerable.Range(0, random.Next(4)).Select(k => k * i))}]",
                $"\"o\":{{\"k\":\"{i % 3}\",\"n\":{(i % 4 == 0 ? "null" : "true")}}}"];
            if (i % 11 == 0) fields.Add("\"extra\":[\"1\",{\"deep\":[2]}]");
            fields = [.. fields.OrderBy(_ => random.Next(i % 5 == 0 ? 3 : 1))];
            text.Append('{').Append(string.Join(",", fields)).Append('}').Append(random.Next(3) == 0 ? "\r\n" : "\n");
        }
        return text.ToString();
    }

    [Fact]
    public void EverySource_UnitSize_AndThreadCount_ReadsTheSameRows()
    {
        string text = Messy(700);
        var (schema, expected) = Read(text, new JsonReadOptions { MaxDegreeOfParallelism = 1 });
        Assert.Equal(["extra: list<utf8view>", "id: int64", "l: list<int64>", "o: {k: utf8view, n: bool}", "s: utf8view", "x: double"],
            schema.FieldsList.Select(f => $"{f.Name}: {Name(f.DataType)}").Order());
        foreach (Source source in Sources)
            foreach (int unit in (int[])[1, 7, 64, 1000, 1 << 20])
                foreach (int threads in (int[])[1, 4])
                {
                    var (s, rows) = Read(text, new JsonReadOptions { UnitBytes = unit, MaxDegreeOfParallelism = threads, InferSchemaLength = null }, source);
                    Assert.Equal(Types(schema), Types(s));
                    Assert.Equal(expected, rows);
                }
    }

    [Fact]
    public void ManyColumns_AreRead_WithEachUnitsLinesCounted()
    {
        // More leaf columns than JsonDecodeSettings.CountedFrom: a unit's lines are counted first and its rows reserved
        // exactly; records of every size, blank lines, any unit, batch size and row limit.
        int columns = JsonDecodeSettings.CountedFrom + 6;
        var text = new StringBuilder();
        List<string> expected = [];
        for (int r = 0; r < 150; r++)
        {
            if (r % 50 == 7) text.Append(" \r\n");
            text.Append('{').AppendJoin(',', Enumerable.Range(0, columns).Select(c => $"\"c{c}\":{r * c}"))
                .Append(",\"pad\":\"").Append('x', r % 13 * 97).Append("\"}\n");
            expected.Add(string.Join(" | ", [.. Enumerable.Range(0, columns).Select(c => (r * c).ToString()), $"\"{new string('x', r % 13 * 97)}\""]));
        }
        foreach (var (unit, batch) in ((int, int)[])[(500, JsonReadOptions.DefaultBatchSize), (3000, 7), (1 << 20, JsonReadOptions.DefaultBatchSize)])
            foreach (long? max in (long?[])[null, 123])
            {
                var (_, rows) = Read(text.ToString(), new JsonReadOptions { UnitBytes = unit, BatchSize = batch, MaxRows = max });
                Assert.Equal(max is long m ? expected.Take((int)m) : expected, rows);
            }
        // The rule's edge: rows counted past CountedFrom leaves (nested fields counted as their leaves), estimated up to it.
        Assert.False(Settings(("a", Wide(JsonDecodeSettings.CountedFrom))).CountsLines);
        Assert.True(Settings(("a", Wide(JsonDecodeSettings.CountedFrom)), ("b", Int64Type.Default)).CountsLines);

        static IArrowType Wide(int leaves) => new StructType([.. Enumerable.Range(0, leaves).Select(c => new Field($"c{c}", Int64Type.Default, true))]);

        static JsonDecodeSettings Settings(params (string, IArrowType)[] fields) =>
            new(Schema(fields), 10, ignoreErrors: false, strict: false, passedOver: []);
    }

    [Fact]
    public void AFailure_QuotesWhatItFound_WithinItsRecord_WhateverTheUnits()
    {
        // The text quoted for a failure stops at its line's end, whatever follows in the unit (fuzz seed 20261134,
        // case 7585: a name's colon missing at a line's end once quoted the next line's start too; escapes alike).
        (string Text, string Message)[] cases =
        [
            ("{\"a\":1}\n{\"a\":2,\"s\"\"}\n{\"a\":3}\n{\"a\":4}\n", "Line 2: Expected ':' after a property name, found '\"'."),
            ("{\"a\":1}\n{\"a\":\"x\\q\n{\"a\":3}\n", "Line 2: Invalid escape '\\q' in a string."),
            ("{\"a\":1}\n{\"a\":\"x\\\n{\"a\":3}\n", "Line 2: Invalid escape '\\' in a string."),
            ("{\"a\":1}\n{\"a\":\"x\\u12\n{\"a\":3}\n", "Line 2: Invalid escape '\\u12' in a string."),
            ("{\"a\":1}\n{\"a\":\"x\\u12\"}\n{\"a\":3}\n", "Line 2: Invalid escape '\\u12' in a string."),
        ];
        foreach (var (text, message) in cases)
            foreach (int unit in (int[])[1, 12, 17, 31, 1 << 20])
                foreach (Source source in Sources)
                {
                    var e = Assert.Throws<JsonFormatException>(() => Read(text, new JsonReadOptions { Schema = Schema(("a", Int64Type.Default)), UnitBytes = unit }, source));
                    Assert.Equal(message, e.Message);
                }
    }

    [Fact]
    public void TheFirstFailure_InInputOrder_IsReported_WhateverTheUnits()
    {
        // A syntax error on line 2, bytes that are not UTF-8 on line 4: line 2 fails, in one unit or many.
        byte[] bytes = [.. "{\"a\":1}\n{\"a\":nul}\n{\"a\":2}\n{\"a\":\""u8, 0xFF, .. "\"}\n"u8];
        foreach (int unit in (int[])[1, 9, 1 << 20])
            foreach (Source source in Sources)
            {
                var e = Assert.Throws<JsonFormatException>(() => Read(bytes, new JsonReadOptions { Schema = Schema(("a", Int64Type.Default)), UnitBytes = unit }, source));
                Assert.StartsWith("Line 2:", e.Message);
            }
        // Inference reads the first records only: bytes that are not UTF-8 beyond them are not its failure.
        Assert.IsType<JsonFormatException>(Fails("{\"a\":1}\n{\"a\":nul}\n"));
        var (_, rows) = Read([.. "{\"a\":1}\n"u8, .. "{\"a\":2}\n"u8], new JsonReadOptions { InferSchemaLength = 1 });
        Assert.Equal(["1", "2"], rows);
    }

    [Fact]
    public void FieldOrder_IsTheRecordsOrder_WhateverTheUnits()
    {
        // Polars' order (records folded one by one): b, c, a, d, e.
        const string text = "{\"a\":1}\n{\"b\":1,\"c\":1}\n{\"d\":1,\"e\":1,\"a\":1}\n";
        foreach (int unit in (int[])[1, 9, 20, 1 << 20])
            foreach (Source source in Sources)
                foreach (int? length in (int?[])[null, 100])
                    Assert.Equal("b: int64, c: int64, a: int64, d: int64, e: int64",
                        Types(Read(text, new JsonReadOptions { UnitBytes = unit, InferSchemaLength = length }, source).Schema));
    }

    [Fact]
    public void WholeInputInference_SeesEveryUnit()
    {
        string text = string.Concat(Enumerable.Range(0, 200).Select(i => i == 150 ? "{\"a\":1,\"late\":\"x\"}\n" : "{\"a\":1}\n"));
        foreach (Source source in Sources)
            Assert.Equal("a: int64, late: utf8view", Types(Read(text, new JsonReadOptions { InferSchemaLength = null, UnitBytes = 64 }, source).Schema));
    }

    [Fact]
    public void CompressedInput_IsDecompressed()
    {
        byte[] raw = Encoding.UTF8.GetBytes(Messy(300));
        var expected = Read(raw).Rows;
        foreach (byte[] packed in (byte[][])[Gzip(raw), [.. Gzip(raw[..1000]), .. Gzip(raw[1000..])], Zlib(raw), new ZstdSharp.Compressor().Wrap(raw).ToArray()])
            foreach (Source source in Sources)
                Assert.Equal(expected, Read(packed, new JsonReadOptions { UnitBytes = 500 }, source).Rows);

        static byte[] Gzip(byte[] data)
        {
            using var output = new MemoryStream();
            using (var gz = new GZipStream(output, CompressionLevel.Fastest)) gz.Write(data);
            return output.ToArray();
        }

        static byte[] Zlib(byte[] data)
        {
            using var output = new MemoryStream();
            using (var z = new ZLibStream(output, CompressionLevel.Fastest)) z.Write(data);
            return output.ToArray();
        }
    }

    [Fact]
    public void DamagedCompressedInput_IsAFormatError()
    {
        // 0x78 0x01 reads as a zlib header; what follows is not deflate (fuzz finding, seed 20261127).
        byte[] bytes = [0x78, 0x01, .. "[{\"a\":1}]"u8];
        foreach (JsonFormat format in (JsonFormat[])[JsonFormat.Lines, JsonFormat.Document])
            foreach (Source source in Sources)
                Assert.StartsWith("The compressed input is damaged", Assert.Throws<JsonFormatException>(() => Read(bytes, new JsonReadOptions { Format = format }, source)).Message);
    }

    [Fact]
    public async Task AsyncReading_ReadsTheSameRows()
    {
        string text = Messy(300);
        var expected = Read(text).Rows;
        foreach (Source source in (Source[])[Source.Memory, Source.NonSeekable, Source.RandomAccess])
        {
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            var options = new JsonReadOptions { UnitBytes = 333 };
            await using JsonReader reader = source switch
            {
                Source.Memory => await JsonReader.OpenAsync(new MemoryStream(bytes), options),
                Source.NonSeekable => await JsonReader.OpenAsync(new NonSeekable(new MemoryStream(bytes)), options),
                _ => await JsonReader.OpenAsync(new BytesAccess(bytes), options),
            };
            List<RecordBatch> batches = [];
            await foreach (RecordBatch b in reader.ReadBatchesAsync()) batches.Add(b);
            Assert.Equal(expected, ArrowRender.Rows(batches));
            batches.ForEach(b => b.Dispose());
        }
    }

    [Fact]
    public void AStream_IsReadOnce_ASeekableSourceAgain()
    {
        byte[] bytes = "{\"a\":1}\n"u8.ToArray();
        using (var stream = JsonReader.Open(new NonSeekable(new MemoryStream(bytes))))
        {
            foreach (var b in stream.ReadBatches()) b.Dispose();
            Assert.Throws<InvalidOperationException>(() => stream.ReadBatches());
        }
        using var memory = JsonReader.Open(new MemoryStream(bytes));
        Assert.Equal(2, Enumerable.Range(0, 2).Sum(_ => memory.ReadBatches().Sum(b => { using (b) return b.Length; })));
    }

    [Fact]
    public void LeaveOpen_KeepsTheStream()
    {
        var stream = new MemoryStream("{\"a\":1}\n"u8.ToArray());
        using (JsonReader.Open(stream, new JsonReadOptions { LeaveOpen = true })) { }
        Assert.True(stream.CanRead);
        using (JsonReader.Open(stream)) { }
        Assert.False(stream.CanRead);
    }

    // ---------------------------------------------------------------- row index

    /// <summary>
    /// The row index numbers records in input order from 0 (blank lines are not records), before the filter (a row keeps
    /// its number), across chunks decoded side by side; the row limit cuts after it.
    /// </summary>
    [Theory]
    [InlineData(JsonFormat.Lines)]
    [InlineData(JsonFormat.Document)]
    public void ARowIndex_NumbersRecordsBeforeTheFilter(JsonFormat format)
    {
        var text = new StringBuilder(format == JsonFormat.Document ? "[\n" : "");
        for (int i = 0; i < 300; i++)
        {
            text.Append($$"""{"a":{{2 * i}}}""").Append(format == JsonFormat.Document && i < 299 ? ",\n" : "\n");
            if (i % 7 == 0) text.Append('\n');
        }
        if (format == JsonFormat.Document) text.Append(']');
        // a < 100 or a >= 560: rows 0..49 and 280..299, holes inside batches and chunks.
        ScalarExpr filter = ScalarExpr.NewBinary(BinaryOp.Or,
            ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("a"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(100))),
            ScalarExpr.NewBinary(BinaryOp.GtEq, ScalarExpr.NewColumn("a"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(560))));
        List<string> all = [.. Enumerable.Range(0, 300).Select(i => $"{i} | {2 * i}")];
        List<string> kept = [.. Enumerable.Range(0, 50).Concat(Enumerable.Range(280, 20)).Select(i => $"{i} | {2 * i}")];
        foreach (Source source in Sources)
        {
            foreach (int unit in (int[])[40, 333, 1 << 20])
            {
                foreach (int threads in (int[])[1, 8])
                {
                    var options = new JsonReadOptions { Format = format, RowIndexColumn = "i", UnitBytes = unit, MaxDegreeOfParallelism = threads, BatchSize = 16 };
                    var (schema, rows) = Read(text.ToString(), options, source);
                    Assert.Equal("i: int64, a: int64", Types(schema));
                    Assert.False(schema.GetFieldByName("i").IsNullable);
                    Assert.Equal(all, rows);
                    Assert.Equal(all[..17], Read(text.ToString(), options with { MaxRows = 17 }, source).Rows);
                    var filtered = options with { Filter = filter, FilterRows = true };
                    Assert.Equal(kept, Read(text.ToString(), filtered, source).Rows);
                    Assert.Equal(kept[..55], Read(text.ToString(), filtered with { MaxRows = 55 }, source).Rows);
                }
            }
        }
    }

    [Fact]
    public void ARowIndex_NeedsANameOfItsOwn_AndTypedRowsTakeNone()
    {
        Assert.Throws<ArgumentException>(() => Read("{\"a\":1}\n", new JsonReadOptions { RowIndexColumn = "a" }));
        Assert.Throws<ArgumentException>(() => Read("{\"a\":1}\n", new JsonReadOptions { RowIndexColumn = "" }));
        // A column not read may share its name.
        Assert.Equal(["0 | 1"], Read("{\"a\":1,\"b\":2}\n", new JsonReadOptions { RowIndexColumn = "b", Columns = ["a"] }).Rows);
        Assert.Throws<ArgumentException>(() => JsonReader.ParseList<RowIndexed>("{\"A\":1}\n", options: new JsonReadOptions { RowIndexColumn = "i" }));
    }

    private sealed record RowIndexed(long A);
}
