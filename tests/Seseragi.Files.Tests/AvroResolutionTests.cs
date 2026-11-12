using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Avro;
using static Kaminari.Seseragi.Files.Avro.AvroField;
using static Seseragi.Files.Tests.AvroEdgeCaseTests;
using S = Kaminari.Seseragi.Files.Avro.AvroSchema;

namespace Seseragi.Files.Tests;

/// <summary>Reading with a reader schema (Avro schema resolution with the package's rules).</summary>
public class AvroResolutionTests
{
    private static byte[] File(string writerSchema, params Bytes[] rows) =>
        Container(writerSchema, [(rows.Length, [.. rows.SelectMany(r => r.ToArray())])]);

    private static Task<List<string>> ReadAs(byte[] file, AvroSchema reader, AvroFieldMatching matching = AvroFieldMatching.ByName,
        IReadOnlyList<string>? columns = null) =>
        Read(file, new() { ReaderSchema = reader, FieldMatching = matching, Columns = columns });

    private static async Task<Schema> SchemaOf(byte[] file, AvroSchema reader, AvroFieldMatching matching = AvroFieldMatching.ByName)
    {
        await using var r = await AvroReader.OpenAsync(new MemoryStream(file), new() { ReaderSchema = reader, FieldMatching = matching });
        return r.Schema;
    }

    private static string Record(params string[] fields) =>
        $$$"""{"type": "record", "name": "w", "fields": [{{{string.Join(", ", fields)}}}]}""";

    private static string F(string name, string type, int? id = null) =>
        id is int i ? $$"""{"name": "{{name}}", "type": {{type}}, "field-id": {{i}}}""" : $$"""{"name": "{{name}}", "type": {{type}}}""";

    // ---- records ------------------------------------------------------------------------------------------------

    private static readonly string Wide = Record(F("a", "\"int\""), F("b", "\"string\""),
        F("n", """{"type": "record", "name": "nw", "fields": [{"name": "x", "type": "int"}, {"name": "gone", "type": "string"}]}"""));

    private static Bytes WideRow(int a, string b, int x) => new Bytes().Long(a).Str(b).Long(x).Str("skip me");

    [Fact]
    public async Task Fields_match_by_name_in_any_order_writer_only_ones_are_skipped_and_defaults_fill_the_rest()
    {
        byte[] file = File(Wide, WideRow(1, "p", 10), WideRow(2, "q", 20));
        var reader = S.Record("anything_else", // record names are never compared
        [
            Required("b", S.String),
            Required("n", S.Record("nr", [Required("x", S.Long), Required("added", S.String, defaultJson: "\"dflt\"")])),
            Required("extra", S.Int, defaultJson: "7"),
            Optional("maybe", S.Double),
            Required("rec", S.Record("d", [Required("y", S.Int), Optional("z", S.String)]), defaultJson: """{"y": 5}"""),
        ]);
        Assert.Equal(
        [
            "\"p\" | {x: 10, added: \"dflt\"} | 7 | null | {y: 5, z: null}",
            "\"q\" | {x: 20, added: \"dflt\"} | 7 | null | {y: 5, z: null}",
        ], await ReadAs(file, reader));
        Assert.IsType<Int64Type>(((StructType)(await SchemaOf(file, reader)).GetFieldByName("n").DataType).Fields[0].DataType);
    }

    [Fact]
    public async Task A_missing_field_without_default_is_an_error_unless_it_is_not_read()
    {
        byte[] file = File(Wide, WideRow(1, "p", 10));
        var reader = S.Record("r", [Required("a", S.Int), Required("missing", S.Int), Optional("also_missing", S.Int, properties: null)]);
        var e = await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, reader));
        Assert.Contains("'missing'", e.Message);
        // A nullable field without a default is an error too (the specification's rule).
        var nullable = AvroSchema.Parse("""{"type": "record", "name": "r", "fields": [{"name": "m", "type": ["null", "int"]}]}""");
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, nullable));
        // Projected away, it is never needed.
        Assert.Equal(["1"], await ReadAs(file, reader, columns: ["a"]));
    }

    [Fact]
    public async Task A_bad_default_in_a_parsed_reader_schema_fails_at_open_only_when_used()
    {
        byte[] file = File(Wide, WideRow(1, "p", 10));
        var reader = AvroSchema.Parse("""{"type": "record", "name": "r", "fields": [{"name": "a", "type": "int", "default": "x"}, {"name": "z", "type": "int", "default": "nope"}]}""");
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, reader)); // z is missing: its default is used
        Assert.Equal(["1"], await ReadAs(file, reader, columns: ["a"]));               // a is present: its default is never used
    }

    [Fact]
    public async Task Fields_match_by_id_across_renames()
    {
        string writer = Record(F("old_name", "\"int\"", 1), F("b", "\"string\"", 2), F("dropped", "\"long\"", 3));
        byte[] file = File(writer, new Bytes().Long(5).Str("s").Long(9));
        var reader = S.Record("r", [Required("b_renamed", S.String, 2), Required("new_name", S.Long, 1), Required("added", S.Int, 4, defaultJson: "0")]);
        Assert.Equal(["\"s\" | 5 | 0"], await ReadAs(file, reader, AvroFieldMatching.ByFieldId));
        // By name the renamed fields are missing (and have no default).
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, reader));
    }

    [Fact]
    public async Task Id_matching_needs_ids()
    {
        byte[] withIds = File(Record(F("a", "\"int\"", 1)), new Bytes().Long(1));
        byte[] withoutIds = File(Record(F("a", "\"int\"")), new Bytes().Long(1));
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(withIds, S.Record("r", [Required("a", S.Int)]), AvroFieldMatching.ByFieldId));
        var e = await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(withoutIds, S.Record("r", [Required("a", S.Int, 1)]), AvroFieldMatching.ByFieldId));
        Assert.Contains("no field ids", e.Message);
        await Assert.ThrowsAsync<AvroResolutionException>(() =>
            ReadAs(withIds, S.Record("r", [Required("a", S.Int, 1), Required("b", S.Int, 1, defaultJson: "0")]), AvroFieldMatching.ByFieldId));
    }

    // ---- promotions and logical types ---------------------------------------------------------------------------

    [Fact]
    public async Task Lossless_promotions_read_and_lossy_ones_are_checked_per_value()
    {
        string writer = Record(F("i", "\"int\""), F("l", "\"long\""), F("f", "\"float\""), F("s", "\"string\""), F("b", "\"bytes\""));
        Bytes Row(int i, long l) => new Bytes().Long(i).Long(l).Raw(BitConverter.GetBytes(1.5f)).Str("ab").Long(1).Raw([0x63]);
        var reader = S.Record("r", [Required("i", S.Double), Required("l", S.Double), Required("f", S.Double), Required("s", S.Bytes), Required("b", S.String)]);
        Assert.Equal(["2147483647 | 9007199254740992 | 1.5 | 0x6162 | \"c\""], await ReadAs(File(writer, Row(int.MaxValue, 1L << 53)), reader));
        Assert.Equal(["-2147483648 | -9.223372036854776E+18 | 1.5 | 0x6162 | \"c\""], await ReadAs(File(writer, Row(int.MinValue, long.MinValue)), reader));

        var e = await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, Row(0, (1L << 53) + 1)), reader));
        Assert.Contains("'l'", e.Message);
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, Row(0, long.MaxValue)), reader)); // 2^63 is not a long

        var asFloat = S.Record("r", [Required("i", S.Float), Required("l", S.Float)]);
        Assert.Equal(["16777216 | -9.223372E+18"], await ReadAs(File(writer, Row(1 << 24, long.MinValue)), asFloat));
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, Row((1 << 24) + 1, 0)), asFloat));
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, Row(int.MaxValue, 0)), asFloat));
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, Row(0, long.MaxValue)), asFloat));

        // Narrowing is no promotion.
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, Row(0, 0)), S.Record("r", [Required("l", S.Int)])));
    }

    [Theory]
    // writer type, reader type, readable
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros"}""", """{"type": "long", "logicalType": "timestamp-micros", "kaminari.timezone": "Asia/Tokyo"}""", true)]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false}""", """{"type": "long", "logicalType": "local-timestamp-micros"}""", true)]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros"}""", """{"type": "long", "logicalType": "local-timestamp-micros"}""", false)]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true}""", """{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false}""", false)]
    [InlineData("""{"type": "long", "logicalType": "timestamp-millis"}""", """{"type": "long", "logicalType": "timestamp-micros"}""", false)]
    [InlineData("\"long\"", """{"type": "long", "logicalType": "timestamp-micros"}""", true)]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros"}""", "\"long\"", true)]
    [InlineData("""{"type": "int", "logicalType": "date"}""", """{"type": "long", "logicalType": "timestamp-micros"}""", false)]
    [InlineData("""{"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}""", """{"type": "fixed", "name": "d", "size": 6, "logicalType": "decimal", "precision": 12, "scale": 2}""", true)]
    [InlineData("""{"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}""", """{"type": "bytes", "logicalType": "decimal", "precision": 40, "scale": 2}""", true)]
    [InlineData("""{"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}""", """{"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 3}""", false)]
    [InlineData("""{"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}""", """{"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2}""", false)]
    [InlineData("""{"type": "fixed", "name": "f", "size": 2}""", """{"type": "fixed", "name": "other", "size": 2}""", true)]
    [InlineData("""{"type": "fixed", "name": "f", "size": 2}""", """{"type": "fixed", "name": "f", "size": 3}""", false)]
    [InlineData("\"int\"", "\"string\"", false)]
    public async Task Logical_types_and_decimals_resolve_by_meaning(string writerType, string readerType, bool readable)
    {
        string writer = Record(F("v", writerType));
        string reader = $$$"""{"type": "record", "name": "r", "fields": [{"name": "v", "type": {{{readerType}}}}]}""";
        bool isBytesDecimal = writerType.Contains("\"bytes\"");
        Bytes row = writerType.Contains("fixed") ? new Bytes().Raw([1, 2]) : isBytesDecimal ? new Bytes().Long(2).Raw([0x04, 0xD2]) : new Bytes().Long(1234);
        byte[] file = File(writer, row);
        if (!readable)
        {
            await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, AvroSchema.Parse(reader)));
            return;
        }
        string value = Assert.Single(await ReadAs(file, AvroSchema.Parse(reader)));
        if (isBytesDecimal) Assert.Equal("12.34", value); // the unscaled 1234 at scale 2, whatever the precision
    }

    // ---- unions -------------------------------------------------------------------------------------------------

    [Fact]
    public async Task Nullable_and_required_resolve_both_ways()
    {
        string nullableWriter = Record(F("c", """["null", "int"]"""));
        byte[] values = File(nullableWriter, new Bytes().Long(1).Long(3), new Bytes().Long(1).Long(4));
        byte[] withNull = File(nullableWriter, new Bytes().Long(1).Long(3), new Bytes().Long(0));
        var required = S.Record("r", [Required("c", S.Long)]);
        Assert.Equal(["3", "4"], await ReadAs(values, required));                         // v1 optional counts read as v2 required
        var e = await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(withNull, required)); // a null cannot be read as long
        Assert.Contains("'c'", e.Message);

        byte[] requiredFile = File(Record(F("c", "\"int\"")), new Bytes().Long(3));
        Assert.Equal(["3"], await ReadAs(requiredFile, S.Record("r", [Optional("c", S.Long)])));
        Assert.Equal(["3", "null"], await ReadAs(withNull, S.Record("r", [Optional("c", S.Long)])));
    }

    [Fact]
    public async Task General_unions_map_branches_by_type()
    {
        string writer = Record(F("u", """["int", "string"]"""));
        byte[] file = File(writer, new Bytes().Long(0).Long(7), new Bytes().Long(1).Str("x"));
        var reader = AvroSchema.Parse("""{"type": "record", "name": "r", "fields": [{"name": "u", "type": ["null", "string", "long"]}]}""");
        Assert.Equal(["{member0: null, member1: 7}", "{member0: \"x\", member1: null}"], await ReadAs(file, reader));

        // A non-union writer goes into the matching member.
        byte[] ints = File(Record(F("u", "\"int\"")), new Bytes().Long(5));
        Assert.Equal(["{member0: null, member1: 5}"], await ReadAs(ints, reader));

        // A writer branch the reader lacks is an error when it is read, not before.
        var onlyInts = AvroSchema.Parse("""{"type": "record", "name": "r", "fields": [{"name": "u", "type": ["null", "long"]}]}""");
        Assert.Equal(["7"], await ReadAs(File(writer, new Bytes().Long(0).Long(7)), onlyInts));
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, onlyInts));
    }

    [Fact]
    public async Task Record_branches_are_told_apart_by_name()
    {
        string writer = Record(F("u", """[{"type": "record", "name": "B", "fields": [{"name": "y", "type": "int"}]}]"""));
        byte[] file = File(writer, new Bytes().Long(0).Long(9));
        var reader = AvroSchema.Parse("""
            {"type": "record", "name": "r", "fields": [{"name": "u", "type": [
              {"type": "record", "name": "A", "fields": [{"name": "y", "type": "int"}]},
              {"type": "record", "name": "B", "fields": [{"name": "y", "type": "int"}]}]}]}
            """);
        Assert.Equal(["{member0: null, member1: {y: 9}}"], await ReadAs(file, reader));
    }

    // ---- enums, collections -------------------------------------------------------------------------------------

    [Fact]
    public async Task Enum_symbols_map_by_name_with_the_reader_default()
    {
        string writer = Record(F("e", """{"type": "enum", "name": "E", "symbols": ["A", "B", "C"]}"""));
        byte[] file = File(writer, new Bytes().Long(0), new Bytes().Long(2), new Bytes().Long(1));
        Assert.Equal(["\"A\"", "\"C\"", "\"Z\""], await ReadAs(file, S.Record("r", [Required("e", S.Enum("Other", ["C", "A", "Z"], @default: "Z"))])));
        var e = await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, S.Record("r", [Required("e", S.Enum("E", ["C", "A"]))])));
        Assert.Contains("'B'", e.Message);
    }

    [Fact]
    public async Task Items_values_and_map_entries_resolve()
    {
        string writer = Record(F("a", """{"type": "array", "items": "int"}"""), F("m", """{"type": "map", "values": "int"}"""),
            F("am", """{"type": "array", "logicalType": "map", "items": {"type": "record", "name": "kv", "fields": [{"name": "key", "type": ["null", "int"]}, {"name": "value", "type": "int"}]}}"""));
        Bytes Row(int keyBranch) => new Bytes().Long(2).Long(1).Long(2).Long(0).Long(1).Str("k").Long(3).Long(0).Long(1).Long(keyBranch).Long(5).Long(6).Long(0);
        var reader = S.Record("r", [Required("a", S.Array(S.Long)), Required("m", S.Map(S.Long)), Required("am", S.ArrayMap("e", S.Long, S.Long))]);
        Assert.Equal(["[1, 2] | {\"k\": 3} | {5: 6}"], await ReadAs(File(writer, Row(1)), reader));
        var schema = await SchemaOf(File(writer, Row(1)), reader);
        Assert.IsType<Int64Type>(((MapType)schema.GetFieldByName("am").DataType).KeyField.DataType);
        // The writer's nullable key may not hold a null for the reader's map.
        var nullKey = new Bytes().Long(0).Long(0).Long(1).Long(0).Long(6).Long(0); // a = [], m = {}, am = [{null: 6}]
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, nullKey), reader));
        // Also when the reader's own key is nullable (as DuckDB writes it): an Arrow map key is never null.
        var nullableKeyReader = AvroSchema.Parse("""
            {"type": "record", "name": "r", "fields": [{"name": "am", "type": {"type": "array", "logicalType": "map", "items":
              {"type": "record", "name": "e", "fields": [{"name": "key", "type": ["null", "long"]}, {"name": "value", "type": "long"}]}}}]}
            """);
        Assert.Equal(["{5: 6}"], await ReadAs(File(writer, Row(1)), nullableKeyReader));
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(File(writer, nullKey), nullableKeyReader));
    }

    [Fact]
    public async Task Mismatched_types_fail_at_open_with_the_field_path()
    {
        byte[] file = File(Wide, WideRow(1, "p", 10));
        var e = await Assert.ThrowsAsync<AvroResolutionException>(() =>
            ReadAs(file, S.Record("r", [Required("n", S.Record("nr", [Required("x", S.String)]))])));
        Assert.Contains("'n.x'", e.Message);
        await Assert.ThrowsAsync<AvroResolutionException>(() => ReadAs(file, S.Record("r", [Required("a", S.Array(S.Int))])));
    }

    [Fact]
    public async Task Columns_select_reader_fields_and_parallel_reads_agree()
    {
        var rows = Enumerable.Range(0, 500).Select(i => WideRow(i, $"s{i}", i * 2)).ToArray();
        byte[] file = Container(Wide, [(250, [.. rows[..250].SelectMany(r => r.ToArray())]), (250, [.. rows[250..].SelectMany(r => r.ToArray())])]);
        var reader = S.Record("r", [Required("x_default", S.Int, defaultJson: "1"), Required("a", S.Long), Required("b", S.String)]);
        var sequential = await Read(file, new() { ReaderSchema = reader, Columns = ["b", "a"], BatchSize = 64 });
        var parallel = await Read(file, new() { ReaderSchema = reader, Columns = ["b", "a"], BatchSize = 64, MaxDegreeOfParallelism = 4 });
        Assert.Equal(500, sequential.Count);
        Assert.Equal("0 | \"s0\"", sequential[0]); // reader order: a, b
        Assert.Equal(sequential, parallel);
    }
}
