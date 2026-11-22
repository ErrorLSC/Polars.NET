using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json;
using static Seseragi.Files.Tests.Json.JsonReadTests;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// JSON documents (<see cref="JsonFormat.Document"/>: an array of objects, or one object) read by
/// <see cref="JsonReader"/>: the expectations are Python Polars 2.1's (<c>read_json</c>), except where Polars changes
/// values (see the tests naming the difference).
/// </summary>
public class JsonDocumentTests
{
    private static JsonReadOptions Doc(JsonReadOptions? options = null) => (options ?? new JsonReadOptions()) with { Format = JsonFormat.Document };

    private static List<string> Rows(string text, JsonReadOptions? options = null, Source source = Source.Memory) => Read(text, Doc(options), source).Rows;

    private static Exception DocFails(string text, JsonReadOptions? options = null, Source source = Source.Memory) =>
        Assert.ThrowsAny<Exception>(() => Read(text, Doc(options), source));

    /// <summary>The records of newline-delimited JSON as one array (blank lines left out), each line an element.</summary>
    private static string AsDocument(string lines, string separator = ",\n") =>
        "[" + string.Join(separator, lines.Split('\n').Select(l => l.TrimEnd('\r')).Where(l => l.Trim().Length > 0)) + "]";

    // ---------------------------------------------------------------- shapes

    [Fact]
    public void AnArrayOfObjects_IsARowPerElement_PrettyOrCompact()
    {
        foreach (Source source in Sources)
        {
            Assert.Equal(["1 | \"x\"", "2 | null"], Rows("[{\"a\":1,\"b\":\"x\"},{\"a\":2}]", source: source));
            Assert.Equal(["1 | \"x\"", "2 | null"], Rows("[\r\n  {\r\n    \"a\": 1,\r\n    \"b\": \"x\"\r\n  },\r\n  {\r\n    \"a\": 2\r\n  }\r\n]\r\n", source: source));
        }
    }

    [Fact]
    public void ASingleObject_IsOneRow_AnObjectOfArrays_OneRowOfLists()
    {
        foreach (Source source in Sources)
        {
            Assert.Equal(["1 | \"x\""], Rows(" {\"a\":1,\"b\":\"x\"}\n", source: source));
            var (schema, rows) = Read("{\"a\":[1,2],\"b\":[\"x\",\"y\"]}", Doc(), source);
            Assert.Equal("a: list<int64>, b: list<utf8view>", Types(schema));
            Assert.Equal(["[1, 2] | [\"x\", \"y\"]"], rows);
        }
    }

    [Fact]
    public void AByteOrderMark_IsSkipped_AsPolarsSkipsIt()
    {
        var (_, rows) = Read([0xEF, 0xBB, 0xBF, .. "[{\"a\":1}]"u8], Doc());
        Assert.Equal(["1"], rows);
    }

    [Fact]
    public void AnEmptyArray_HasNoColumns_AndNoRows()
    {
        foreach (string text in (string[])["[]", " [ \n ] \n"])
        {
            var (schema, rows) = Read(text, Doc());
            Assert.Empty(schema.FieldsList);
            Assert.Empty(rows);
        }
    }

    [Fact]
    public void EmptyObjects_AreRowsWithoutColumns_WherePolarsKeepsOneOfThemNot()
    {
        // Polars 2.1: [{}] and {} read as no rows, [{},{}] as two: a row per element here, always.
        foreach (Source source in Sources)
        {
            foreach (var (text, count) in (ValueTuple<string, int>[])[("[{}]", 1), ("[{},{}]", 2), ("{}", 1), ("[{} , {}, {}]", 3)])
            {
                var (schema, rows) = Read(text, Doc(new JsonReadOptions { UnitBytes = 2 }), source);
                Assert.Empty(schema.FieldsList);
                Assert.Equal(count, rows.Count);
            }
        }
        Assert.Equal(["null", "1"], Rows("[{},{\"a\":1}]"));
    }

    [Fact]
    public void NoColumns_CountsTheRows_InEitherLayout()
    {
        var options = new JsonReadOptions { Columns = [], UnitBytes = 16 };
        Assert.Equal(3, Read("{\"a\":1}\n{\"a\":2,\"b\":3}\n\n{\"a\":5}\n", options).Rows.Count);
        Assert.Equal(3, Rows("[{\"a\":1},{\"a\":2,\"b\":3},{\"a\":5}]", options with { InferSchemaLength = null }).Count);
        // The elements are still checked.
        DocFails("[{\"a\":1},{\"a\":nul}]", options);
        DocFails("[{\"a\":1},5]", options);
    }

    // ---------------------------------------------------------------- failures

    [Theory]
    [InlineData("[{\"a\":1},null]", "Line 1 (array index 1): An array element is null, not an object.")]
    [InlineData("[{\"a\":1},5]", "Line 1 (array index 1): An array element is a number, not an object.")]
    [InlineData("[[{\"a\":1}]]", "Line 1 (array index 0): An array element is an array, not an object.")]
    [InlineData("[{\"a\":1},]", "Line 1: An array element is missing (a comma after '[', before ']' or after another comma).")]
    [InlineData("[,{\"a\":1}]", "Line 1: An array element is missing (a comma after '[', before ']' or after another comma).")]
    [InlineData("[{\"a\":1},,{\"a\":2}]", "Line 1: An array element is missing (a comma after '[', before ']' or after another comma).")]
    [InlineData("[{\"a\":1}] x", "Line 1: Data follows the end of the JSON document.")]
    [InlineData("[{\"a\":1}][{\"a\":2}]", "Line 1: Data follows the end of the JSON document.")]
    [InlineData("[{\"a\":1}] [{\"a\":2},{\"a\":3},{\"a\":4}]", "Line 1: Data follows the end of the JSON document.")]
    [InlineData("[{\"a\":1}]]], {\"a\":2}", "Line 1: Data follows the end of the JSON document.")]
    [InlineData("\n\n\n   [{\"a\":1},{\"b\":\"\\\\\\\"]\"},{\"a\":2}] x", "Line 4: Data follows the end of the JSON document.")]
    [InlineData("{\"a\":1}{\"a\":2}", "Line 1: Data follows the end of the JSON document.")]
    [InlineData("{\"a\":1}\n{\"a\":2}\n", "Line 2: Data follows the end of the JSON document.")]
    [InlineData("[{\"a\":1}", "Line 1: The document ends before its array closes.")]
    [InlineData("[{\"a\":1},\n", "Line 2: The document ends before its array closes.")]
    [InlineData("[{\"a\":1} {\"a\":2}]", "Line 1: An array element is followed by something other than ',' or ']'.")]
    [InlineData("null", "Line 1: A JSON document to read as rows is an array of objects or one object.")]
    [InlineData("5", "Line 1: A JSON document to read as rows is an array of objects or one object.")]
    [InlineData("\"s\"", "Line 1: A JSON document to read as rows is an array of objects or one object.")]
    [InlineData("  \n ", "Line 2: The input holds no JSON document.")]
    [InlineData("", "Line 1: The input holds no JSON document.")]
    public void BrokenDocuments_Fail_AtTheSamePlace_WhateverTheUnits(string text, string message)
    {
        var schema = Schema(("a", Int64Type.Default));
        foreach (Source source in Sources)
            foreach (int unit in (int[])[1, 3, 1 << 20])
                foreach (JsonReadOptions options in (JsonReadOptions[])[new() { UnitBytes = unit }, new() { UnitBytes = unit, Schema = schema }])
                {
                    var e = Assert.IsType<JsonFormatException>(DocFails(text, options, source));
                    Assert.Equal(message, e.Message);
                }
    }

    [Fact]
    public void SyntaxErrors_InAnElement_Fail_AtTheirLine()
    {
        const string text = "[\n  {\"a\": 1},\n  {\"a\": 1,\n   \"b\": nul},\n  {\"a\": 3}\n]";
        foreach (Source source in Sources)
            foreach (int unit in (int[])[1, 5, 1 << 20])
            {
                var e = Assert.IsType<JsonFormatException>(DocFails(text, new JsonReadOptions { UnitBytes = unit, Schema = Schema(("a", Int64Type.Default)) }, source));
                Assert.StartsWith("Line 4 (array index 1): ", e.Message);
                Assert.DoesNotContain("LineNumber", e.Message);
            }
    }

    [Fact]
    public void ABrokenLiteral_IsQuotedToItsEnd_WhateverTheUnits()
    {
        // A broken literal is quoted to its delimiter, not to the end of the reader's input (the rest of a unit, cut
        // anywhere: a fuzz finding when Utf8JsonReader read the elements).
        const string text = "[{\"a\":1},{\"a\":nul,\"b\":2},{\"a\":3},{\"a\":4}]";
        foreach (int unit in (int[])[1, 12, 20, 1 << 20])
            foreach (Source source in Sources)
                Assert.Equal("Line 1 (array index 1): 'nul' is not a JSON literal (true, false or null).",
                    DocFails(text, new JsonReadOptions { UnitBytes = unit, Schema = Schema(("a", Int64Type.Default)) }, source).Message);
    }

    [Fact]
    public void ValuesThatDoNotFit_Fail_AtTheirLine_AndElement()
    {
        string text = "[\n" + string.Join(",\n", Enumerable.Range(0, 50).Select(i => i == 37 ? "{\"a\": true}" : $"{{\"a\": {i}}}")) + "\n]";
        foreach (int unit in (int[])[1, 64, 1 << 20])
        {
            var e = Assert.IsType<JsonValueException>(DocFails(text, new JsonReadOptions { UnitBytes = unit, InferSchemaLength = 10 }));
            Assert.Equal("Line 39 (array index 37): Column 'a': the boolean true is not an Int64 integer.", e.Message);
        }
    }

    [Fact]
    public void BoolAndIntegers_CannotBeTyped_WherePolarsReadsTrueAsOne()
    {
        // Polars 2.1 read_json infers Int64 and reads true as 1 (read_ndjson refuses): refused here in both layouts.
        Assert.IsType<JsonValueException>(DocFails("[{\"a\":1},{\"a\":true}]"));
        // A fraction in an integer column fails (Polars truncates 2.5 to 2 after the inference).
        Assert.IsType<JsonValueException>(DocFails("[{\"a\":1},{\"a\":2.5}]", new JsonReadOptions { InferSchemaLength = 1 }));
    }

    [Fact]
    public void NotUtf8_FailsAtItsElement_UnlessAnEarlierElementFails()
    {
        byte[] late = [.. "[{\"a\":1},\n{\"a\":nul},\n{\"a\":\""u8, 0xFF, .. "\"}]"u8];
        byte[] early = [.. "[{\"a\":1},\n{\"a\":\""u8, 0xFF, .. "\"},\n{\"a\":nul}]"u8];
        var schema = Schema(("a", StringType.Default));
        foreach (int unit in (int[])[1, 9, 1 << 20])
            foreach (Source source in Sources)
            {
                var options = Doc(new JsonReadOptions { Schema = schema, UnitBytes = unit });
                Assert.StartsWith("Line 2 (array index 1): ", Assert.Throws<JsonFormatException>(() => Read(late, options, source)).Message);
                var e = Assert.Throws<JsonFormatException>(() => Read(early, options, source));
                Assert.Equal("Line 2 (array index 1): The input is not UTF-8.", e.Message);
            }
    }

    // ---------------------------------------------------------------- fields the inference did not see

    [Fact]
    public void AFieldAfterTheInference_IsAnError_AtAnyDepth_AsPolarsReadJson()
    {
        var once = new JsonReadOptions { InferSchemaLength = 1 };
        Assert.Equal("Line 1 (array index 1): The field 'b' is not in the records the columns were inferred from (infer from more records, or give the schema to pass such fields over).",
            DocFails("[{\"a\":1},{\"a\":2,\"b\":3}]", once).Message);
        Assert.StartsWith("Line 1 (array index 1): Column 'a': the field 'y' is not", DocFails("[{\"a\":{\"x\":1}},{\"a\":{\"x\":2,\"y\":3}}]", once).Message);
        Assert.StartsWith("Line 1 (array index 1): Column 'a[]': the field 'y' is not", DocFails("[{\"a\":[{\"x\":1}]},{\"a\":[{\"x\":2,\"y\":3}]}]", once).Message);
        // Overridden types are still inferred columns.
        DocFails("[{\"a\":1},{\"a\":2,\"b\":3}]", once with { SchemaOverrides = new Dictionary<string, IArrowType> { ["a"] = Int32Type.Default } });
        // Lines pass them over (as read_ndjson does).
        Assert.Equal(["1", "2"], Read("{\"a\":1}\n{\"a\":2,\"b\":3}\n", once).Rows);
    }

    [Fact]
    public void AFieldAfterTheInference_IsPassedOver_WithASchema_IgnoreErrors_OrLeftOutColumns()
    {
        const string text = "[{\"a\":1,\"b\":{\"x\":1}},{\"a\":2,\"b\":{\"x\":2,\"y\":3},\"c\":4}]";
        Assert.Equal(["1", "2"], Rows(text, new JsonReadOptions { Schema = Schema(("a", Int64Type.Default)) }));
        Assert.Equal(["1 | {x: 1}", "2 | {x: 2}"], Rows(text, new JsonReadOptions { InferSchemaLength = 1, IgnoreErrors = true }));
        // b is inferred but not read: passed over; c was never inferred: an error.
        Assert.StartsWith("Line 1 (array index 1): The field 'c'", DocFails(text, new JsonReadOptions { InferSchemaLength = 1, Columns = ["a"] }).Message);
        Assert.Equal(["1", "2"], Rows("[{\"a\":1,\"b\":{\"x\":1}},{\"a\":2,\"b\":{\"x\":2,\"y\":3}}]", new JsonReadOptions { InferSchemaLength = 1, Columns = ["a"] }));
    }

    // ---------------------------------------------------------------- units, sources, threads

    /// <summary>Strings full of what the scan must pass over: brackets, braces, commas, quotes and runs of backslashes.</summary>
    private static string Tricky(int count)
    {
        var random = new Random(23);
        string[] pieces = ["]", "[", "{", "}", ",", "\\\"", "\\\\", "\\\\\\\"", "x", " ", "\\n", "é", "\\u005d"];
        var text = new StringBuilder();
        for (int i = 0; i < count; i++)
        {
            string s = string.Concat(Enumerable.Range(0, random.Next(12)).Select(_ => pieces[random.Next(pieces.Length)]));
            text.Append($"{{\"id\":{i},\"s\":\"{s}\",\"n\":{{\"l\":[[{i}],[]],\"t\":\"{s}\\\\\"}}}}\n");
        }
        return text.ToString();
    }

    [Fact]
    public void EverySource_UnitSize_AndThreadCount_ReadTheRowsTheLinesHold()
    {
        foreach (string lines in (string[])[Messy(500), Tricky(400)])
        {
            var (schema, expected) = Read(lines, new JsonReadOptions { InferSchemaLength = null });
            foreach (string document in (string[])[AsDocument(lines), AsDocument(lines, " ,\r\n\t "), "\n " + AsDocument(lines, ",") + " \n"])
                foreach (Source source in Sources)
                    foreach (int unit in (int[])[1, 7, 64, 1000, 1 << 20])
                        foreach (int threads in (int[])[1, 4])
                        {
                            var (s, rows) = Read(document, Doc(new JsonReadOptions { UnitBytes = unit, MaxDegreeOfParallelism = threads, InferSchemaLength = null }), source);
                            Assert.Equal(Types(schema), Types(s));
                            Assert.Equal(expected, rows);
                        }
        }
    }

    [Fact]
    public void LeadingWhitespace_AByteOrderMark_AndBackslashRuns_CutAnywhere()
    {
        // Segments of a few bytes: before '[' (not a closed array), and starting inside strings, after backslashes.
        string elements = string.Join(",", Enumerable.Range(0, 40).Select(i => $"{{\"a\":{i},\"s\":\"{new string('\\', 2 * (i % 5))}\\\"],{{\"}}"));
        byte[] document = [0xEF, 0xBB, 0xBF, .. Encoding.UTF8.GetBytes(new string(' ', 25) + "\n[" + elements + "]")];
        var expected = Read(document, Doc(new JsonReadOptions { MaxDegreeOfParallelism = 1 })).Rows;
        Assert.Equal(40, expected.Count);
        foreach (Source source in Sources)
            foreach (int unit in (int[])[1, 2, 3, 5, 8, 13])
                Assert.Equal(expected, Read(document, Doc(new JsonReadOptions { UnitBytes = unit, MaxDegreeOfParallelism = 3 }), source).Rows);
    }

    [Fact]
    public async Task MemoryIsCutInParallel_IntoAUnitPerSegment_EvenAfterLeadingWhitespace()
    {
        // 40 elements of about 10 bytes after 30 bytes of whitespace, in segments of 16: a cut in nearly every segment
        // past the '[' (whitespace before it must not read as a closed array: the document would be one unit).
        string document = new string(' ', 30) + "[" + string.Join(",", Enumerable.Range(0, 40).Select(i => $"{{\"a\":{i,3}}}")) + "]";
        using var input = Kaminari.Seseragi.Files.Json.Reading.DocumentInput.Of(new MemoryRandomAccess(Encoding.UTF8.GetBytes(document)), null, map: false, 16, threads: 4);
        Assert.True(input.InPlace);
        int units = 0;
        while (await input.GetAsync(units, useAsync: true, default) is { } bytes)
        {
            bytes.Dispose();
            units++;
        }
        Assert.InRange(units, 20, 30);
    }

    /// <summary>Bytes read at offsets slowly, as a store's object is: the reads at once and the bytes read are counted.</summary>
    private sealed class SlowAccess(byte[] bytes) : Kaminari.Seseragi.Files.IO.IRandomAccess
    {
        private int _now;

        public int MostAtOnce;
        public long BytesRead;

        public long Length => bytes.Length;

        public int ReadAt(long offset, Span<byte> destination)
        {
            int now = Interlocked.Increment(ref _now);
            InterlockedMax(ref MostAtOnce, now);
            Thread.Sleep(2);
            int n = (int)Math.Max(0, Math.Min(destination.Length, bytes.Length - offset));
            bytes.AsSpan((int)offset, n).CopyTo(destination);
            Interlocked.Add(ref BytesRead, n);
            Interlocked.Decrement(ref _now);
            return n;
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ReadAt(offset, destination.Span));

        private static void InterlockedMax(ref int target, int value)
        {
            for (int seen = target; value > seen; seen = target)
                if (Interlocked.CompareExchange(ref target, value, seen) == seen) return;
        }
    }

    [Fact]
    public void ASourceReadAtOffsets_IsReadInParallel_EachByteOnce()
    {
        byte[] bytes = Encoding.UTF8.GetBytes(AsDocument(Messy(2000)));
        var expected = Read(bytes, Doc()).Rows;
        var schema = Read(bytes, Doc(new JsonReadOptions { InferSchemaLength = null })).Schema;
        var source = new SlowAccess(bytes);
        using (var reader = JsonReader.Open(source, Doc(new JsonReadOptions { Schema = schema, UnitBytes = 4096, MaxDegreeOfParallelism = 4 })))
        {
            var batches = reader.ReadBatches().ToList();
            Assert.Equal(expected, ArrowRender.Rows(batches));
            batches.ForEach(b => b.Dispose());
        }
        Assert.True(source.MostAtOnce > 1, $"at most {source.MostAtOnce} read at once");
        // Every byte once, plus the first 4 (whether it is compressed) and the start (where the array opens).
        Assert.Equal(bytes.Length + 4 + Math.Min(bytes.Length, 64 << 10), source.BytesRead);
    }

    [Fact]
    public async Task ASourceReadAtOffsets_IsCutPerSegment_AndItsSegmentsAreFreed()
    {
        string document = new string(' ', 30) + "[" + string.Join(",", Enumerable.Range(0, 40).Select(i => $"{{\"a\":{i,3}}}")) + "]";
        byte[] bytes = Encoding.UTF8.GetBytes(document);
        using (var input = Kaminari.Seseragi.Files.Json.Reading.DocumentInput.Of(new BytesAccess(bytes), null, map: false, 16, threads: 2))
        {
            Assert.False(input.InPlace);
            int units = 0;
            StringBuilder joined = new();
            while (await input.GetAsync(units, useAsync: true, default) is { } unit)
            {
                using (unit) joined.Append(Encoding.UTF8.GetString(unit.Memory.Span)).Append(unit.FileEnd ? "" : ",");
                units++;
            }
            Assert.InRange(units, 20, 30);
            Assert.Equal(document, joined.ToString());
            Assert.Equal(0, input.HeldSegments);
            // Read again: by range.
            using var again = await input.GetAsync(3, useAsync: true, default);
            Assert.NotNull(again);
        }
        // Disposed half way: nothing kept.
        var early = Kaminari.Seseragi.Files.Json.Reading.DocumentInput.Of(new BytesAccess(bytes), null, map: false, 16, threads: 2);
        using (await early.GetAsync(5, useAsync: true, default)) { }
        early.Dispose();
        Assert.Equal(0, early.HeldSegments);
    }

    [Fact]
    public void FieldOrder_IsTheRecordsOrder_WhateverTheUnits()
    {
        const string text = "[{\"a\":1},{\"b\":1,\"c\":1},{\"d\":1,\"e\":1,\"a\":1}]";
        foreach (int unit in (int[])[1, 9, 20, 1 << 20])
            foreach (Source source in Sources)
                Assert.Equal("b: int64, c: int64, a: int64, d: int64, e: int64", Types(Read(text, Doc(new JsonReadOptions { UnitBytes = unit }), source).Schema));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(25)]
    [InlineData(1000)]
    public void MaxRows_KeepsExactlyTheFirstRows(long max)
    {
        string text = "[" + string.Join(",", Enumerable.Range(0, 100).Select(i => $"{{\"a\":{i}}}")) + "]";
        foreach (Source source in Sources)
            foreach (int unit in (int[])[1, 40, 1 << 20])
            {
                var (_, rows) = Read(text, Doc(new JsonReadOptions { MaxRows = max, UnitBytes = unit, BatchSize = 3 }), source);
                Assert.Equal(Enumerable.Range(0, (int)Math.Min(max, 100)).Select(i => i.ToString()), rows);
            }
    }

    [Fact]
    public void BatchSize_SplitsUnits_AndColumnsAreProjected()
    {
        string text = "[" + string.Join(",", Enumerable.Range(0, 100).Select(i => $"{{\"a\":{i},\"b\":\"{i}\"}}")) + "]";
        using var reader = JsonReader.Open(new MemoryStream(Encoding.UTF8.GetBytes(text)), Doc(new JsonReadOptions { BatchSize = 7, Columns = ["b"] }));
        var batches = reader.ReadBatches().ToList();
        Assert.All(batches, b => Assert.True(b.Length <= 7));
        Assert.Equal(Enumerable.Range(0, 100).Select(i => $"\"{i}\""), ArrowRender.Rows(batches));
        batches.ForEach(b => b.Dispose());
    }

    [Fact]
    public void CompressedDocuments_AreDecompressed()
    {
        byte[] raw = Encoding.UTF8.GetBytes(AsDocument(Messy(300)));
        var expected = Read(raw, Doc()).Rows;
        using var output = new MemoryStream();
        using (var gz = new GZipStream(output, CompressionLevel.Fastest)) gz.Write(raw);
        foreach (Source source in Sources)
            Assert.Equal(expected, Read(output.ToArray(), Doc(new JsonReadOptions { UnitBytes = 500 }), source).Rows);
    }

    [Fact]
    public async Task AsyncReading_ReadsTheSameRows()
    {
        byte[] bytes = Encoding.UTF8.GetBytes(AsDocument(Tricky(300)));
        var expected = Read(bytes, Doc()).Rows;
        foreach (Source source in (Source[])[Source.Memory, Source.NonSeekable, Source.RandomAccess])
        {
            var options = Doc(new JsonReadOptions { UnitBytes = 333 });
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
    public void ASourceRead_AtOffsets_IsReadAgain_AStreamOnce()
    {
        byte[] bytes = Encoding.UTF8.GetBytes(AsDocument(Messy(200)));
        var options = Doc(new JsonReadOptions { UnitBytes = 512 });
        foreach (JsonReader reader in (JsonReader[])[JsonReader.Open(new BytesAccess(bytes), options), JsonReader.Open(new MemoryStream(bytes), options)])
            using (reader)
                Assert.Equal([200, 200], Enumerable.Range(0, 2).Select(_ => reader.ReadBatches().Sum(b => { using (b) return b.Length; })));
        using var stream = JsonReader.Open(new NonSeekable(new MemoryStream(bytes)), options);
        foreach (var b in stream.ReadBatches()) b.Dispose();
        Assert.Throws<InvalidOperationException>(() => stream.ReadBatches());
    }
}
