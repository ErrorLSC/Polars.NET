using System.Text.Json;
using Kaminari.Seseragi.Files.Avro;
using static Kaminari.Seseragi.Files.Avro.AvroField;

namespace Seseragi.Files.Tests;

/// <summary>Schemas built with the factory methods: the same JSON and model the parser gives, and invalid
/// schemas refused as they are built.</summary>
public class AvroSchemaConstructionTests
{
    [Fact]
    public void The_iceberg_like_schema_builds_to_the_same_json()
    {
        AvroRecordSchema partition = AvroSchema.Record("r102",
        [
            Optional("tstz", AvroSchema.TimestampMicros(adjustToUtc: true), 1000),
            Optional("tsntz", AvroSchema.TimestampMicros(adjustToUtc: false), 1001),
            Optional("uid", AvroSchema.UuidFixed("uuid_fixed"), 1002),
            Optional("dec", AvroSchema.DecimalFixed("decimal_9_2", 9, 2), 1003),
        ]);
        AvroRecordSchema dataFile = AvroSchema.Record("r2",
        [
            Required("file_path", AvroSchema.String, 100, doc: "Location URI with FS scheme"),
            Required("partition", partition, 102),
            Optional("column_sizes", AvroSchema.ArrayMap("k117_v118", AvroSchema.Int, AvroSchema.Long, keyId: 117, valueId: 118), 108),
            Optional("split_offsets", AvroSchema.Array(AvroSchema.Long, elementId: 133), 132),
            Required("props", AvroSchema.Map(AvroSchema.String, keyId: 7, valueId: 8), 6, aliases: ["properties"],
                properties: [("order", "\"ignore\"")]),
            Required("odd", AvroSchema.Primitive(AvroType.Long, [("logicalType", "\"not-a-real-type\""), ("x-extra", """{"a": [1, 2]}""")]), 9),
            Required("baddec", AvroSchema.Primitive(AvroType.Bytes, [("logicalType", "\"decimal\""), ("precision", "0")]), 10),
            Required("kind", AvroSchema.Enum("Kind", ["A", "B"], doc: "kinds", aliases: ["K"], properties: [("x-enum", "1")]), 11),
        ]);
        AvroRecordSchema entry = AvroSchema.Record("manifest_entry", [Required("status", AvroSchema.Int, 0), Required("data_file", dataFile, 2)]);

        AssertSameJson(AvroSchemaApiTests.IcebergLike, entry.ToJson());
        // Constructed and parsed schemas are the same model.
        var back = (AvroRecordSchema)AvroSchema.Parse(entry.ToJson());
        var sizes = (AvroArraySchema)((AvroUnionSchema)((AvroRecordSchema)back.Fields[1].Schema).Fields[2].Schema).Branches[1];
        Assert.Equal(AvroLogicalKind.Map, sizes.Logical);
        Assert.Equal(JsonValueKind.Null, ((AvroRecordSchema)back.Fields[1].Schema).Fields[2].Default!.Value.ValueKind);
        Assert.Null(back.Fields[0].Default); // "no default" stays distinct from "default null"
    }

    [Fact]
    public void Defaults_are_validated_against_the_type()
    {
        Assert.Equal(0, Required("content", AvroSchema.Int, 134, defaultJson: "0").Default!.Value.GetInt32());
        Required("b", AvroSchema.Bytes, defaultJson: "\"\\u00ff\\u0000\"");
        Required("u", AvroSchema.Union(AvroSchema.Int, AvroSchema.Null), defaultJson: "null"); // any matching branch
        Required("r", AvroSchema.Record("p", [Required("x", AvroSchema.Int), Optional("y", AvroSchema.String)]), defaultJson: """{"x": 1}""");
        Required("e", AvroSchema.Enum("E", ["A", "B"]), defaultJson: "\"B\"");
        Required("a", AvroSchema.Array(AvroSchema.Long), defaultJson: "[1, 2]");

        Assert.Throws<ArgumentException>(() => Required("content", AvroSchema.Int, defaultJson: "\"0\""));
        Assert.Throws<ArgumentException>(() => Required("i", AvroSchema.Int, defaultJson: "3000000000"));
        Assert.Throws<ArgumentException>(() => Required("b", AvroSchema.Bytes, defaultJson: "\"\\u0100\""));
        Assert.Throws<ArgumentException>(() => Required("f", AvroSchema.Fixed("F", 2), defaultJson: "\"a\""));
        Assert.Throws<ArgumentException>(() => Required("r", AvroSchema.Record("p", [Required("x", AvroSchema.Int)]), defaultJson: "{}"));
        Assert.Throws<ArgumentException>(() => Required("e", AvroSchema.Enum("E", ["A"]), defaultJson: "\"Z\""));
        Assert.Throws<ArgumentException>(() => Required("j", AvroSchema.Int, defaultJson: "not json"));
    }

    [Fact]
    public void Invalid_schemas_are_refused_as_they_are_built()
    {
        Assert.Throws<ArgumentException>(() => AvroSchema.Record("1bad", []));
        Assert.Throws<ArgumentException>(() => AvroSchema.Record("r", [], ns: "a..b"));
        Assert.Throws<ArgumentException>(() => AvroSchema.Record("r", [Required("x", AvroSchema.Int), Required("x", AvroSchema.Long)]));
        Assert.Throws<ArgumentException>(() => AvroSchema.Enum("E", ["A", "A"]));
        Assert.Throws<ArgumentException>(() => AvroSchema.Enum("E", ["A"], @default: "B"));
        Assert.ThrowsAny<ArgumentException>(() => AvroSchema.DecimalFixed("d", 10, 2, size: 4)); // 4 bytes hold 9 digits
        Assert.ThrowsAny<ArgumentException>(() => AvroSchema.DecimalBytes(5, 6));
        Assert.Equal(16, AvroSchema.DecimalFixed("d", 38, 10).Size);
        Assert.Equal(4, AvroSchema.DecimalFixed("d", 9, 2).Size);

        Assert.Throws<ArgumentException>(() => AvroSchema.Union(AvroSchema.Long, AvroSchema.TimestampMicros())); // long twice
        Assert.Throws<ArgumentException>(() => AvroSchema.Union(AvroSchema.Null, AvroSchema.Union(AvroSchema.Int)));
        Assert.Throws<ArgumentException>(() => AvroSchema.Nullable(AvroSchema.Union(AvroSchema.Int, AvroSchema.String)));
        AvroSchema.Union(AvroSchema.Fixed("A", 1), AvroSchema.Fixed("B", 1)); // two named types of different names
        Assert.Throws<ArgumentException>(() => AvroSchema.ArrayMap("e", AvroSchema.Union(AvroSchema.Int, AvroSchema.String), AvroSchema.Long));

        // Attributes the node writes itself, or that would change how it reads, belong in the typed factories.
        Assert.Throws<ArgumentException>(() => AvroSchema.Array(AvroSchema.Int, properties: [("items", "\"long\"")]));
        Assert.Throws<ArgumentException>(() => AvroSchema.Primitive(AvroType.Long, [("logicalType", "\"timestamp-micros\"")]));
        Assert.Throws<ArgumentException>(() => AvroSchema.Primitive(AvroType.Long, [("spark.sql.catalyst.type", "\"interval day to second\"")]));
        Assert.Throws<ArgumentException>(() => Required("x", AvroSchema.Int, fieldId: 1, properties: [("field-id", "2")]));
        Assert.Throws<ArgumentException>(() => AvroSchema.Primitive(AvroType.Record));
    }

    [Fact]
    public void Two_different_types_of_one_name_cannot_be_written()
    {
        AvroRecordSchema r = AvroSchema.Record("r", [Required("a", AvroSchema.Fixed("F", 1)), Required("b", AvroSchema.Fixed("F", 2))]);
        Assert.Throws<InvalidOperationException>(() => r.ToJson());
        // The same instance twice is a definition and then a reference.
        AvroFixedSchema f = AvroSchema.Fixed("F", 1);
        string json = AvroSchema.Record("r", [Required("a", f), Required("b", f)]).ToJson();
        Assert.Equal("F", ((AvroRecordSchema)AvroSchema.Parse(json)).Fields[1].Schema switch { AvroFixedSchema x => x.Name, _ => "" });
    }

    [Fact]
    public void A_field_can_be_reused_and_gets_its_position_in_each_record()
    {
        AvroField id = Required("id", AvroSchema.Long, 1);
        AvroRecordSchema a = AvroSchema.Record("a", [id]);
        AvroRecordSchema b = AvroSchema.Record("b", [Required("x", AvroSchema.Int), id]);
        Assert.Equal((0, 1), (a.Fields[0].Position, b.Fields[1].Position));
        Assert.Equal(1, b.Fields[1].FieldId);
    }

    private static void AssertSameJson(string expected, string actual)
    {
        using JsonDocument e = JsonDocument.Parse(expected), a = JsonDocument.Parse(actual);
        Assert.True(JsonElement.DeepEquals(e.RootElement, a.RootElement), $"expected {expected}\nactual   {actual}");
    }
}
