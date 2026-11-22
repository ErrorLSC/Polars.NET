using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Json;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// <see cref="JsonReader{T}"/>: rows written by <see cref="JsonWriter{T}"/> read back, JSON from System.Text.Json read
/// (names in another case, GUIDs, enums as names or numbers, dictionaries as objects, .NET time spans), members that take
/// no nulls, predicates evaluated by the reader; and the batch reader's row filter and key matching.
/// </summary>
public sealed class JsonTypedReaderTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-json-typed").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    // A trade as text, its lists included (records compare lists by reference), its price without trailing zeros (rows
    // read decimals of scale 18).
    private static string Show(Trade t) =>
        $"{t with { Price = Plain(t.Price), Tags = null, Legs = null }}|{string.Join(",", t.Tags ?? ["<null>"])}|{string.Join(",", (t.Legs ?? []).Select(l => l?.ToString() ?? "<null>"))}";

    private static decimal Plain(decimal value) => value / 1.000000000000000000000000000000m;

    // ---------------------------------------------------------------- round trips

    [Theory]
    [InlineData(JsonFormat.Lines)]
    [InlineData(JsonFormat.Document)]
    public async Task RowsWritten_ReadBack_FromEveryInput(JsonFormat format)
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(5000);
        string path = Path($"trades.{format}");
        JsonWriter.WriteRows(path, rows, new JsonWriteOptions { Format = format });
        List<string> expected = [.. rows.Select(Show)];
        foreach (int threads in (int[])[1, 8])
        {
            var options = new JsonReadOptions { Format = format, MaxDegreeOfParallelism = threads, UnitBytes = 4096, BatchSize = 100 };
            Assert.Equal(expected, JsonReader.ReadList<Trade>(path, options: options).Select(Show));
            Assert.Equal(expected, (await JsonReader.ReadListAsync<Trade>(path, options: options)).Select(Show));
            byte[] bytes = File.ReadAllBytes(path);
            using (JsonReader<Trade> reader = JsonReader<Trade>.Open(bytes, options: options)) Assert.Equal(expected, reader.ReadRows().Select(Show));
            using (JsonReader<Trade> reader = JsonReader<Trade>.Open(new MemoryStream(bytes), options: options))
                Assert.Equal(expected, await reader.ReadRowsAsync().Select(Show).ToListAsync());
            Assert.Equal(expected, JsonReader.ParseList<Trade>(Encoding.UTF8.GetString(bytes), options: options).Select(Show));
        }
    }

    [Fact]
    public void EveryMemberType_RoundTrips_AndGuidsAndStringKeyedDictionariesAreWrittenAsSerializersWriteThem()
    {
        Everything[] rows = [.. Enumerable.Range(0, 40).Select(Everything.Make)];
        string text = JsonWriter.WriteString(rows);
        Assert.Contains($"\"Id\":\"{rows[3].Id}\"", text);
        Assert.Contains("\"Counts\":{\"a\":3,\"b\":4}", text);
        Assert.Contains("\"ByNumber\":[{\"Key\":3,\"Value\":\"x\"}]", text);
        List<Everything> read = JsonReader.ParseList<Everything>(text);
        // What is read is written as it was.
        Assert.Equal(text, JsonWriter.WriteString(read));
        Assert.Equal(rows.Select(r => r.Id), read.Select(r => r.Id));
        Assert.Equal(rows.Select(r => r.MaybeId), read.Select(r => r.MaybeId));
        Assert.Equal(rows.Select(r => r.Counts?["b"]), read.Select(r => r.Counts?["b"]));
        Assert.Equal(rows.Select(r => r.Shift), read.Select(r => r.Shift));
        Assert.Equal(rows.Select(r => (r.Color, r.Perm, r.MaybeColor)), read.Select(r => (r.Color, r.Perm, r.MaybeColor)));
    }

    // ---------------------------------------------------------------- System.Text.Json

    [Fact]
    public void SystemTextJsonOutput_IsRead_InEveryLayout()
    {
        Person[] people = [.. Enumerable.Range(0, 300).Select(Person.Make)];
        string lines = string.Join("\n", people.Select(p => JsonSerializer.Serialize(p, PeopleContext.Default.Person)));
        string document = JsonSerializer.Serialize(people, PeopleContext.Default.PersonArray);
        Assert.Contains("\"favorite\":4", lines); // an enum as its number
        Assert.Contains("\"mood\":\"Sell\"", lines); // an enum as its name
        Assert.Contains("\"shift\":\"1.02:03:04.5000000\"", lines); // a time span as .NET writes it
        foreach (var (text, format) in (ValueTuple<string, JsonFormat>[])[(lines, JsonFormat.Lines), (document, JsonFormat.Document)])
        {
            List<Person> read = JsonReader.ParseList<Person>(text, options: new JsonReadOptions { Format = format, UnitBytes = 2048 });
            Assert.Equal(people.Select(p => p.Show()), read.Select(p => p.Show()));
        }
    }

    // ---------------------------------------------------------------- names, nulls, values

    [Fact]
    public void Names_MatchIgnoringCase_TheFirstMatchingFieldWins_TheBatchReaderMatchesExactly()
    {
        Assert.Equal([1L, 3], JsonReader.ParseList<Narrow>("""
            {"id":1,"ID":2,"Id":3,"SYMBOL":"a"}
            {"Id":3,"id":1,"symbol":"b"}
            """).Select(n => n.Id));
        var schema = new Schema([new Field("Id", Int64Type.Default, true)], null);
        using (JsonReader reader = JsonReader.Parse("""{"id":1}""", new JsonReadOptions { Schema = schema }))
        using (RecordBatch batch = reader.ReadBatches().Single())
            Assert.True(batch.Column(0).IsNull(0));
        using (JsonReader reader = JsonReader.Parse("""{"iD":1}""", new JsonReadOptions { Schema = schema, KeysIgnoreCase = true }))
        using (RecordBatch batch = reader.ReadBatches().Single())
            Assert.Equal(1L, ((Int64Array)batch.Column(0)).GetValue(0));
    }

    [Fact]
    public void AValueMember_TakesNoNull_UnlessErrorsAreIgnored()
    {
        var e = Assert.Throws<JsonValueException>(() => JsonReader.ParseList<Narrow>("""
            {"Id":1,"Symbol":"a"}
            {"Symbol":"b"}
            """));
        Assert.Equal("Line 2: Column 'Id': the field is missing, but the column takes no nulls.", e.Message);
        e = Assert.Throws<JsonValueException>(() => JsonReader.ParseList<Narrow>("""[{"Id":1},{"Id":null}]""", options: new JsonReadOptions { Format = JsonFormat.Document }));
        Assert.Equal("Line 1 (array index 1): Column 'Id': null, but the column takes no nulls.", e.Message);
        // Ignoring errors, the null is decoded: the rows refuse it (as rows of a frame do), naming the member.
        var refused = Assert.Throws<InvalidOperationException>(() => JsonReader.ParseList<Narrow>("""
            {"Id":1,"Symbol":"a"}
            {"Id":null,"Symbol":"b"}
            """, options: new JsonReadOptions { IgnoreErrors = true }));
        Assert.Contains("Member 'Id' of non-nullable type Int64 cannot hold the null", refused.Message);
        // A schema's column that is not nullable takes no null in batches either.
        var schema = new Schema([new Field("a", Int64Type.Default, false)], null);
        Assert.Throws<JsonValueException>(() => JsonReader.Parse("""{"b":1}""", new JsonReadOptions { Schema = schema }).ReadBatches().ToList());
    }

    [Fact]
    public void AnObjectOfNulls_IsAnObject_FromJsonParquetAndIpc()
    {
        Sparse[] rows = [new(1, "a", new(2, "b")), new(null, null, null), new(null, null, new(null, null))];
        List<Sparse> json = JsonReader.ParseList<Sparse>("""
            {"Id":1,"Name":"a","Nested":{"A":2,"B":"b"}}
            {}
            {"Nested":{}}
            """);
        Assert.Equal(rows, json);
        string parquet = Path("sparse.parquet"), ipc = Path("sparse.arrow");
        Kaminari.Seseragi.Files.Parquet.ParquetWriter.WriteRows(parquet, rows);
        Kaminari.Seseragi.Files.Ipc.IpcWriter.WriteRows(ipc, rows);
        Assert.Equal(rows, Kaminari.Seseragi.Files.Parquet.ParquetFile.ReadList<Sparse>(parquet));
        Assert.Equal(rows, Kaminari.Seseragi.Files.Ipc.IpcFile.ReadList<Sparse>(ipc));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void ANullAMemberCannotHold_IsPlacedAmongAllTheRowsRead(int threads)
    {
        string lines = string.Join("\n", Enumerable.Range(0, 1000).Select(i => i == 777 ? """{"Id":null,"Symbol":"x"}""" : $$"""{"Id":{{i}},"Symbol":"x"}"""));
        var options = new JsonReadOptions { IgnoreErrors = true, BatchSize = 100, UnitBytes = 2048, MaxDegreeOfParallelism = threads };
        var e = Assert.Throws<InvalidOperationException>(() => JsonReader.ParseList<Narrow>(lines, options: options));
        Assert.Contains("cannot hold the null in column 'Id' at row 777.", e.Message);
        using JsonReader<Narrow> reader = JsonReader<Narrow>.Parse(lines, options: options);
        e = Assert.Throws<InvalidOperationException>(() => reader.ReadRows().ToList());
        Assert.Contains("at row 777.", e.Message);
    }

    [Fact]
    public void GuidsEnumsAndTimeSpans_ReadFromTheirTexts()
    {
        Guid id = Guid.NewGuid();
        Small row = JsonReader.ParseList<Small>($$"""{"Id":"{{id}}","Color":"blue","Span":"PT1.5S"}""").Single();
        Assert.Equal((id, Color.Blue, TimeSpan.FromSeconds(1.5)), (row.Id, row.Color, row.Span));
        Assert.Equal([Color.Blue, Color.Green, Color.Blue], JsonReader.ParseList<Small>($$"""
            {"Id":"{{id}}","Color":4,"Span":"-00:00:01"}
            {"Id":"{{id}}","Color":"Green","Span":"2.00:00:00.25"}
            {"Id":"{{id}}","Color":"4","Span":"00:01:00"}
            """).Select(r => r.Color));
        Assert.Equal([TimeSpan.FromSeconds(-1), new TimeSpan(2, 0, 0, 0, 250)], JsonReader.ParseList<Small>($$"""
            {"Id":"{{id}}","Color":4,"Span":"-00:00:01"}
            {"Id":"{{id}}","Color":4,"Span":"2.00:00:00.25"}
            """).Select(r => r.Span));
        Assert.Contains("GUIDs are strings", Assert.Throws<JsonValueException>(() => JsonReader.ParseList<Small>("""{"Id":"nope","Color":4,"Span":"PT1S"}""")).Message);
        Assert.Contains("GUIDs are strings", Assert.Throws<JsonValueException>(() => JsonReader.ParseList<Small>("""{"Id":12,"Color":4,"Span":"PT1S"}""")).Message);
        // A bare number is no duration (hours and minutes are there in .NET's text).
        Assert.Contains("a duration", Assert.Throws<JsonValueException>(() => JsonReader.ParseList<Small>($$"""{"Id":"{{id}}","Color":4,"Span":"12"}""")).Message);
    }

    [Fact]
    public void AGivenSchema_IsRefused()
    {
        var options = new JsonReadOptions { Schema = new Schema([new Field("Id", Int64Type.Default, true)], null) };
        Assert.Throws<ArgumentException>(() => JsonReader.ParseList<Narrow>("{}", options: options));
        Assert.Throws<ArgumentException>(() => JsonReader.ParseList<Narrow>("{}", options: new JsonReadOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["Id"] = Int64Type.Default } }));
    }

    [Fact]
    public void AStream_IsReadOnce()
    {
        using JsonReader<Narrow> reader = JsonReader<Narrow>.Open(new OneWayStream("""{"Id":1,"Symbol":"a"}"""u8.ToArray()));
        Assert.Single(reader.ReadRows());
        Assert.Throws<InvalidOperationException>(() => reader.ReadRows().ToList());
    }

    // ---------------------------------------------------------------- filters

    [Fact]
    public void APredicate_IsEvaluatedByTheReader()
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(30_000);
        string path = Path("trades.ndjson");
        JsonWriter.WriteRows(path, rows);
        foreach (int parallelism in (int[])[1, 8])
        {
            var options = new JsonReadOptions { MaxDegreeOfParallelism = parallelism, UnitBytes = 64 << 10 };
            using (JsonReader<Trade> reader = JsonReader<Trade>.Open(path, t => (t.Id < 10 || t.Id >= 29_000) && t.Note != null, options))
                Assert.Equal(rows.Where(t => (t.Id < 10 || t.Id >= 29_000) && t.Note != null).Select(t => t.Id), reader.ReadRows().Select(t => t.Id));
            using (JsonReader<Trade> reader = JsonReader<Trade>.Open(path, t => t.Symbol == "SYM5" && t.Fee > 100, options))
            {
                Assert.True(reader.FiltersExactly);
                Assert.True(reader.Inner.FiltersExactly);
                Assert.Equal(rows.Where(t => t.Symbol == "SYM5" && t.Fee > 100).Select(t => t.Id), reader.ReadRows().Select(t => t.Id));
            }
            // A row limit counts the rows delivered.
            using (JsonReader<Trade> reader = JsonReader<Trade>.Open(path, t => t.Flag, options with { MaxRows = 9_999 }))
                Assert.Equal(rows.Where(t => t.Flag).Take(9_999).Select(t => t.Id), reader.ReadRows().Select(t => t.Id));
            // Conjuncts the reader cannot evaluate are evaluated on the rows.
            using (JsonReader<Trade> reader = JsonReader<Trade>.Open(path, t => t.Symbol.EndsWith("7") && t.Id % 3 == 0, options))
                Assert.Equal(rows.Where(t => t.Symbol.EndsWith('7') && t.Id % 3 == 0).Select(t => t.Id), reader.ReadRows().Select(t => t.Id));
        }
        // Only the members' columns are read.
        using JsonReader<Narrow> narrow = JsonReader<Narrow>.Open(path);
        Assert.Equal(["Id", "Symbol"], narrow.Schema.FieldsList.Select(f => f.Name));
    }

    [Theory]
    [InlineData(JsonFormat.Lines)]
    [InlineData(JsonFormat.Document)]
    public void TheBatchReader_FiltersRows_ByColumnsItDoesNotDeliver(JsonFormat format)
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(20_000);
        string path = Path($"filter.{format}");
        JsonWriter.WriteRows(path, rows, new JsonWriteOptions { Format = format });
        ScalarExpr evaluated = ScalarExpr.NewBinary(BinaryOp.GtEq, ScalarExpr.NewColumn("Id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(15_000)));
        ScalarExpr betweenColumns = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("Id"), ScalarExpr.NewColumn("Fee"));
        foreach (int threads in (int[])[1, 8])
        {
            var options = new JsonReadOptions { Format = format, Columns = ["Symbol"], Filter = evaluated, FilterRows = true, MaxDegreeOfParallelism = threads, UnitBytes = 32 << 10 };
            using (JsonReader reader = JsonReader.Open(path, options))
            {
                Assert.True(reader.FiltersExactly);
                Assert.Equal(["Symbol"], reader.Schema.FieldsList.Select(f => f.Name));
                List<string> symbols = [.. reader.ReadBatches().SelectMany(Render)];
                Assert.Equal(rows.Where(t => t.Id >= 15_000).Select(t => $"\"{t.Symbol}\""), symbols);
            }
            using (JsonReader reader = JsonReader.Open(path, options with { MaxRows = 1234 }))
                Assert.Equal(rows.Where(t => t.Id >= 15_000).Take(1234).Select(t => $"\"{t.Symbol}\""), reader.ReadBatches().SelectMany(Render));
            // No rows kept, no batches; a filter without FilterRows does nothing.
            using (JsonReader reader = JsonReader.Open(path, options with { Filter = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("Id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(0))) }))
                Assert.Empty(reader.ReadBatches().SelectMany(Render));
            using (JsonReader reader = JsonReader.Open(path, options with { FilterRows = false }))
                Assert.Equal(rows.Length, reader.ReadBatches().SelectMany(Render).Count());
            using (JsonReader reader = JsonReader.Open(path, options with { Filter = betweenColumns }))
                Assert.False(reader.FiltersExactly);
        }

        static IEnumerable<string> Render(RecordBatch batch)
        {
            using (batch) return [.. ArrowRender.Rows(batch)];
        }
    }

    // A stream read forward only.
    private sealed class OneWayStream(byte[] bytes) : MemoryStream(bytes)
    {
        public override bool CanSeek => false;

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    }

    [Fact]
    public void AnErrorAfterFilteredRows_IsPlacedByTheRecordsBeforeIt()
    {
        string document = "[" + string.Join(",", Enumerable.Range(0, 2000).Select(i => i == 1500 ? "{\"Id\":\"x\"}" : $"{{\"Id\":{i}}}")) + "]";
        var options = new JsonReadOptions
        {
            Format = JsonFormat.Document,
            Schema = new Schema([new Field("Id", Int64Type.Default, true)], null),
            Filter = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("Id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(0))),
            FilterRows = true,
            UnitBytes = 512,
        };
        using JsonReader reader = JsonReader.Parse(document, options);
        var e = Assert.Throws<JsonValueException>(() => reader.ReadBatches().ToList());
        Assert.StartsWith("Line 1 (array index 1500): ", e.Message);
    }

    public sealed record Narrow(long Id, string Symbol);

    public sealed record Part(int? A, string? B);

    public sealed record Sparse(int? Id, string? Name, Part? Nested);

    public sealed record Small(Guid Id, Color Color, TimeSpan Span);

    public enum Color { Red = 1, Green = 2, Blue = 4 }

    [Flags]
    public enum Perm : byte { None = 0, Read = 1, Write = 2 }

    public sealed record Inner(int A, string? B);

    public sealed record Everything(
        sbyte I8, short I16, int I32, long I64, byte U8, ushort U16, uint U32, ulong U64, Half F16, float F32, double F64, decimal Money,
        bool Flag, string? Text, byte[]? Bytes, char Letter, Int128 Big, UInt128 Huge, Guid Id, Guid? MaybeId,
        DateTime At, DateTimeOffset AtOffset, TimeSpan Shift, TimeOnly Clock, DateOnly Day,
        Color Color, Perm Perm, Color? MaybeColor, int? MaybeInt,
        List<int>? Ints, string?[]? Texts, Inner? Nested, List<Inner?>? Nesteds, Dictionary<string, int>? Counts, Dictionary<int, string>? ByNumber,
        Dictionary<string, Guid>? GuidsByName)
    {
        public static Everything Make(int i)
        {
            bool odd = i % 2 == 1;
            return new Everything(
                (sbyte)(i - 3), (short)(i * 300 - 7), i * 100_000 - 5, (long)i << 40, (byte)i, (ushort)(i * 1000), (uint)i * 3_000_000u, ulong.MaxValue - (ulong)i,
                (Half)(i + 0.5), i + 0.25f, i * 1.5 - 0.125, 1234.5678m * i - 0.000000000000000001m,
                odd, odd ? null : $"text \"{i}\"\n", odd ? null : [.. Enumerable.Range(0, i).Select(b => (byte)b)], (char)('a' + i % 26),
                (Int128)i << 70, UInt128.MaxValue - (UInt128)i, new Guid(i * 7919, (short)i, (short)(i + 1), [1, 2, 3, 4, 5, 6, 7, (byte)i]), odd ? null : Guid.Empty,
                new DateTime(1960 + i, 1 + i % 12, 2, 3, 4, 5).AddTicks(1230 + i * 10), new DateTimeOffset(2020, 1, 1 + i % 28, 12, 0, 0, TimeSpan.Zero),
                TimeSpan.FromMilliseconds(i * 1234.5 - 2000), new TimeOnly(i % 24, i % 60, 3, 4), new DateOnly(1950 + i * 2, 2, 28),
                (Color)(1 << (i % 3)), (Perm)(i % 4), odd ? null : Color.Blue, odd ? i : null,
                odd ? null : [.. Enumerable.Range(0, i % 5)], odd ? [null, "b", ""] : null, odd ? null : new Inner(i, i % 4 == 0 ? null : $"in{i}"),
                [new Inner(i, "x"), null], i == 3 ? new() { ["a"] = 3, ["b"] = 4 } : odd ? null : new() { ["b"] = i }, i == 3 ? new() { [3] = "x" } : null,
                odd ? new() { ["g"] = Guid.Empty } : null);
        }
    }

    public sealed record Person(string Name, int Age, Guid Id, DateTime Born, TimeSpan Shift, Color Favorite,
        [property: JsonConverter(typeof(JsonStringEnumConverter<Side>))] Side Mood,
        Dictionary<string, int> Scores, List<string> Tags, decimal Balance, DateOnly Day, TimeOnly At, double? Score)
    {
        public static Person Make(int i) => new($"p{i} \"q\"", 20 + i % 50, new Guid(i, 2, 3, [4, 5, 6, 7, 8, 9, 10, (byte)i]),
            new DateTime(1980, 1, 1).AddDays(i).AddMilliseconds(i * 7), new TimeSpan(1, 2, 3, 4, 500).Add(TimeSpan.FromSeconds(i)), (Color)(1 << (i % 3)),
            (Side)(i % 3), new() { ["x"] = i, ["y"] = -i }, [.. Enumerable.Range(0, i % 4).Select(t => $"t{t}")], i * 1.25m, new DateOnly(2000, 1, 1).AddDays(i),
            new TimeOnly(i % 24, 5, 6, 7), i % 3 == 0 ? null : i * 0.5);

        public string Show() =>
            $"{Name}|{Age}|{Id}|{Born.Ticks}|{Shift}|{Favorite}|{Mood}|{string.Join(",", Scores.OrderBy(p => p.Key))}|{string.Join(",", Tags)}|{Plain(Balance)}|{Day}|{At}|{Score}";
    }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(JsonTypedReaderTests.Person))]
[JsonSerializable(typeof(JsonTypedReaderTests.Person[]))]
internal sealed partial class PeopleContext : JsonSerializerContext;
