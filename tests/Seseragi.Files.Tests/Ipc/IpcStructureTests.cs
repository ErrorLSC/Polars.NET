using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Ipc;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// The schema, metadata and batch structure of every IPC test file against an independent reading of its FlatBuffers
/// (TestData/ipc/messages.json, written by ipc_meta.py): field names, nullability, types and their parameters,
/// dictionary encodings, custom metadata at schema, field, message and footer level, batch counts.
/// </summary>
public class IpcStructureTests
{
    private static readonly Lazy<JsonDocument> Messages = new(() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(IpcValueTests.Root, "messages.json"))));

    public static TheoryData<string> Files => [.. Messages.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name)
        .Where(n => !n.StartsWith("apache/fuzz/") && !n.Contains("bigendian") && !n.EndsWith("feather_v1.feather"))];

    [Theory]
    [MemberData(nameof(Files))]
    public void Structure_MatchesTheFlatBuffers(string name)
    {
        JsonElement expected = Messages.Value.RootElement.GetProperty("files").GetProperty(name);
        string path = Path.Combine(IpcValueTests.Root, name);
        List<string> diffs = [];
        if (expected.GetProperty("format").GetString() == "file")
        {
            JsonElement footer = expected.GetProperty("footer");
            using IpcFile file = IpcFile.Open(path);
            CompareSchema(footer.GetProperty("schema"), file.Schema, diffs);
            Assert.Equal(Pairs(footer.GetProperty("metadata")), file.FileMetadata.OrderBy(p => p.Key));
            JsonElement[] messages = [.. expected.GetProperty("messages").EnumerateArray()];
            JsonElement[] batches = [.. messages.Where(m => m.GetProperty("header").GetString() == "RecordBatch")];
            Assert.Equal(footer.GetProperty("recordBatches").GetArrayLength(), file.BatchCount);
            for (int i = 0; i < batches.Length; i++)
                Assert.Equal(Pairs(batches[i].TryGetProperty("metadata", out JsonElement m) ? m : default), file.GetBatchMetadata(i).OrderBy(p => p.Key));
        }
        else
        {
            JsonElement[] messages = [.. expected.GetProperty("messages").EnumerateArray().Where(m => m.TryGetProperty("header", out _))];
            JsonElement schemaMessage = messages[0];
            using IpcStreamReader reader = IpcStreamReader.Open(File.OpenRead(path));
            CompareSchema(schemaMessage.GetProperty("schema"), reader.Schema, diffs);
            Assert.Equal(Pairs(schemaMessage.TryGetProperty("metadata", out JsonElement sm) ? sm : default), reader.SchemaMessageMetadata.OrderBy(p => p.Key));
            JsonElement[] batches = [.. messages.Where(m => m.GetProperty("header").GetString() == "RecordBatch")];
            int i = 0;
            while (reader.ReadNext() is { } batch)
            {
                using (batch)
                {
                    Assert.Equal(Pairs(batches[i].TryGetProperty("metadata", out JsonElement m) ? m : default), reader.BatchMetadata.OrderBy(p => p.Key));
                    Assert.Equal(batches[i].GetProperty("length").GetInt64(), batch.Length);
                }
                i++;
            }
            Assert.Equal(batches.Length, i);
        }
        Assert.True(diffs.Count == 0, $"{name}:\n{string.Join('\n', diffs)}");
    }

    private static KeyValuePair<string, string>[] Pairs(JsonElement pairs) => pairs.ValueKind != JsonValueKind.Array ? []
        : [.. pairs.EnumerateArray().Select(p => new KeyValuePair<string, string>(p[0].GetString()!, p[1].GetString()!)).GroupBy(p => p.Key).Select(g => g.Last()).OrderBy(p => p.Key)];

    private static void CompareSchema(JsonElement schema, Schema actual, List<string> diffs)
    {
        Assert.Equal(Pairs(schema.GetProperty("metadata")), (actual.Metadata ?? new Dictionary<string, string>()).OrderBy(p => p.Key));
        JsonElement[] fields = [.. schema.GetProperty("fields").EnumerateArray()];
        Assert.Equal(fields.Length, actual.FieldsList.Count);
        for (int i = 0; i < fields.Length; i++) CompareField(fields[i], actual.FieldsList[i], actual.FieldsList[i].Name, diffs);
    }

    private static void CompareField(JsonElement expected, Field field, string where, List<string> diffs)
    {
        if (expected.GetProperty("name").GetString() != field.Name) diffs.Add($"{where}: name {field.Name}, file {expected.GetProperty("name")}");
        if (expected.GetProperty("nullable").GetBoolean() != field.IsNullable) diffs.Add($"{where}: nullable {field.IsNullable}");
        KeyValuePair<string, string>[] metadata = Pairs(expected.TryGetProperty("metadata", out JsonElement m) ? m : default);
        if (!metadata.SequenceEqual((field.Metadata ?? new Dictionary<string, string>()).OrderBy(p => p.Key))) diffs.Add($"{where}: field metadata differs");

        IArrowType type = field.DataType;
        if (expected.TryGetProperty("dictionary", out JsonElement d))
        {
            if (type is not DictionaryType dictionary)
            {
                diffs.Add($"{where}: {type.Name}, not dictionary-encoded");
                return;
            }
            var index = (IntegerType)dictionary.IndexType;
            if (index.BitWidth != d.GetProperty("indexBitWidth").GetInt32() || index.IsSigned != d.GetProperty("indexSigned").GetBoolean() || dictionary.Ordered != d.GetProperty("ordered").GetBoolean())
                diffs.Add($"{where}: dictionary {index.Name} ordered {dictionary.Ordered}");
            type = dictionary.ValueType;
        }
        string tag = expected.GetProperty("type").GetString()!;
        string? problem = (tag, type) switch
        {
            ("Null", NullType) => null,
            ("Int", Decimal128Type { Precision: 38, Scale: 0 }) when expected.GetProperty("bitWidth").GetInt32() == 128 && expected.GetProperty("signed").GetBoolean() => null,
            ("Int", Decimal256Type { Precision: 39, Scale: 0 }) when expected.GetProperty("bitWidth").GetInt32() == 128 && !expected.GetProperty("signed").GetBoolean() => null,
            ("Int", IntegerType t) => t.BitWidth == expected.GetProperty("bitWidth").GetInt32() && t.IsSigned == expected.GetProperty("signed").GetBoolean() ? null : "width or sign",
            ("FloatingPoint", HalfFloatType) => Expect("precision", "HALF"),
            ("FloatingPoint", FloatType) => Expect("precision", "SINGLE"),
            ("FloatingPoint", DoubleType) => Expect("precision", "DOUBLE"),
            ("Decimal", Decimal32Type t) => Decimal(t.Precision, t.Scale, 32),
            ("Decimal", Decimal64Type t) => Decimal(t.Precision, t.Scale, 64),
            ("Decimal", Decimal128Type t) => Decimal(t.Precision, t.Scale, 128),
            ("Decimal", Decimal256Type t) => Decimal(t.Precision, t.Scale, 256),
            ("Date", Date32Type) => Expect("unit", "DAY"),
            ("Date", Date64Type) => Expect("unit", "MILLISECOND"),
            ("Time", TimeType t) => Expect("unit", Unit(t.Unit)) ?? (t.BitWidth == expected.GetProperty("bitWidth").GetInt32() ? null : "width"),
            ("Timestamp", TimestampType t) => Expect("unit", Unit(t.Unit)) ?? (t.Timezone == (expected.GetProperty("timezone").ValueKind == JsonValueKind.Null ? null : expected.GetProperty("timezone").GetString()) ? null : "timezone"),
            ("Duration", DurationType t) => Expect("unit", Unit(t.Unit)),
            ("Interval", IntervalType t) => Expect("unit", t.Unit switch { IntervalUnit.YearMonth => "YEAR_MONTH", IntervalUnit.DayTime => "DAY_TIME", _ => "MONTH_DAY_NANO" }),
            ("Utf8", StringType) or ("LargeUtf8", LargeStringType) or ("Utf8View", StringViewType) or ("Bool", BooleanType) => null,
            ("Binary", BinaryType) or ("LargeBinary", LargeBinaryType) or ("BinaryView", BinaryViewType) => null,
            ("FixedSizeBinary", FixedSizeBinaryType t) => t.ByteWidth == expected.GetProperty("byteWidth").GetInt32() ? null : "width",
            ("Map", MapType t) => t.KeySorted == expected.GetProperty("keysSorted").GetBoolean() ? null : "keysSorted",
            ("List", ListType) or ("LargeList", LargeListType) or ("ListView", ListViewType) or ("LargeListView", LargeListViewType) => null,
            ("FixedSizeList", FixedSizeListType t) => t.ListSize == expected.GetProperty("listSize").GetInt32() ? null : "size",
            ("Struct_", StructType) or ("RunEndEncoded", RunEndEncodedType) => null,
            ("Union", UnionType t) => Expect("mode", t.Mode == UnionMode.Dense ? "Dense" : "Sparse")
                ?? (expected.GetProperty("typeIds").ValueKind == JsonValueKind.Null || t.TypeIds.SequenceEqual(expected.GetProperty("typeIds").EnumerateArray().Select(e => e.GetInt32())) ? null : "type ids"),
            _ => "type",
        };
        if (problem is not null) diffs.Add($"{where}: {type.Name} vs {tag}: {problem}");

        IReadOnlyList<Field> children = type is NestedType n ? n.Fields : [];
        JsonElement[] expectedChildren = expected.TryGetProperty("children", out JsonElement c) ? [.. c.EnumerateArray()] : [];
        if (expectedChildren.Length != children.Count)
        {
            diffs.Add($"{where}: {children.Count} children, file {expectedChildren.Length}");
            return;
        }
        if (type is MapType)
        {
            // Apache.Arrow's MapType names its entries, keys and values canonically (entries, key, value), whatever the file says.
            expectedChildren = [.. expectedChildren.Select(Canonical("entries"))];
        }
        for (int i = 0; i < children.Count; i++) CompareField(expectedChildren[i], children[i], $"{where}.{children[i].Name}", diffs);

        string? Expect(string property, string value) => expected.GetProperty(property).GetString() == value ? null : property;
        string? Decimal(int precision, int scale, int width) => precision == expected.GetProperty("precision").GetInt32()
            && scale == expected.GetProperty("scale").GetInt32() && width == expected.GetProperty("bitWidth").GetInt32() ? null : "precision, scale or width";
    }

    /// <summary>A field renamed (and, for map entries, its key and value renamed too), as a JSON element.</summary>
    private static Func<JsonElement, JsonElement> Canonical(string name) => element =>
    {
        var node = System.Text.Json.Nodes.JsonNode.Parse(element.GetRawText())!.AsObject();
        node["name"] = name;
        if (name == "entries" && node["children"] is System.Text.Json.Nodes.JsonArray kids && kids.Count == 2)
        {
            kids[0]!["name"] = "key";
            kids[1]!["name"] = "value";
        }
        return JsonDocument.Parse(node.ToJsonString()).RootElement;
    };

    private static string Unit(Apache.Arrow.Types.TimeUnit unit) => unit switch
    {
        Apache.Arrow.Types.TimeUnit.Second => "SECOND",
        Apache.Arrow.Types.TimeUnit.Millisecond => "MILLISECOND",
        Apache.Arrow.Types.TimeUnit.Microsecond => "MICROSECOND",
        _ => "NANOSECOND",
    };
}
