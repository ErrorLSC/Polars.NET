using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// Which data files a type widening left in an older type, and which still hold dropped columns, from their footers.
/// </summary>
public class FileTypesTests
{
    private const int Int32 = 1, Int64 = 2, Float = 4, Double = 5, ByteArray = 6;

    private static ParquetSchemaElement Leaf(string name, int physical, int? bits = null, int? precision = null, int? scale = null, bool date = false) =>
        new(name, physical, 1, 0, null, scale, null, IntBitWidth: bits, DecimalPrecision: precision, IsDate: date);

    private static ParquetSchemaElement Group(string name, int children, int repetition = 1) => new(name, null, repetition, children, null, null, null);

    /// <summary>A footer of the nodes (flattened, depth first) under a root.</summary>
    private static ParquetMetadata Footer(params ParquetSchemaElement[] nodes)
    {
        int columns = 0;
        for (int at = 0; at < nodes.Length; columns++) at = Skip(at);
        return new() { Schema = [new("schema", null, null, columns, null, null, null), .. nodes], RowCount = 1, RowGroups = [] };

        int Skip(int at)
        {
            int children = nodes[at++].ChildCount;
            for (int c = 0; c < children; c++) at = Skip(at);
            return at;
        }
    }

    private static StructType Table(string fields) => StructType.Parse($$$"""{"type":"struct","fields":[{{{fields}}}]}""");

    private static string Field(string name, string type) => $$$"""{"name":"{{{name}}}","type":{{{type}}},"nullable":true,"metadata":{}}""";

    [Theory]
    [InlineData("\"short\"", Int32, 8, null, null, false, true)]       // byte → short
    [InlineData("\"integer\"", Int32, 16, null, null, false, true)]    // short → integer
    [InlineData("\"integer\"", Int32, 32, null, null, false, false)]
    [InlineData("\"integer\"", Int32, null, null, null, false, false)] // no annotation: a plain INT32 is an integer
    [InlineData("\"long\"", Int32, null, null, null, false, true)]     // integer → long
    [InlineData("\"long\"", Int64, null, null, null, false, false)]
    [InlineData("\"double\"", Float, null, null, null, false, true)]   // float → double
    [InlineData("\"double\"", Int32, null, null, null, false, true)]   // integer → double
    [InlineData("\"double\"", Double, null, null, null, false, false)]
    [InlineData("\"timestamp_ntz\"", Int32, null, null, null, true, true)] // date → timestamp_ntz
    [InlineData("\"date\"", Int32, null, null, null, true, false)]
    [InlineData("\"decimal(7,2)\"", Int32, null, 5, 2, false, true)]   // decimal(5,2) → decimal(7,2)
    [InlineData("\"decimal(7,2)\"", Int32, null, 7, 2, false, false)]
    [InlineData("\"decimal(10,0)\"", Int32, null, null, null, false, true)] // integer → decimal
    [InlineData("\"string\"", ByteArray, null, null, null, false, false)]
    public void ALeafInAnOlderType_IsFound(string tableType, int physical, int? bits, int? precision, int? scale, bool date, bool older)
    {
        StructType schema = Table(Field("c", tableType));
        Assert.Equal(older, FileTypes.HasOlderTypes(schema, [], Footer(Leaf("c", physical, bits, precision, scale, date))));
    }

    [Fact]
    public void NestedLeaves_StructsArraysAndMaps_AreCompared()
    {
        StructType schema = Table(string.Join(",",
            Field("s", """{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}"""),
            Field("a", """{"type":"array","elementType":"integer","containsNull":true}"""),
            Field("m", """{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true}""")));
        ParquetMetadata Of(int structBits, int arrayBits, int mapValue) => Footer(
            Group("s", 1), Leaf("x", Int32, structBits),
            Group("a", 1), Group("list", 1, repetition: 2), Leaf("element", Int32, arrayBits),
            Group("m", 1), Group("key_value", 2, repetition: 2), Leaf("key", ByteArray), Leaf("value", mapValue));

        Assert.False(FileTypes.HasOlderTypes(schema, [], Of(32, 32, Int64)));
        Assert.True(FileTypes.HasOlderTypes(schema, [], Of(16, 32, Int64)));
        Assert.True(FileTypes.HasOlderTypes(schema, [], Of(32, 8, Int64)));
        Assert.True(FileTypes.HasOlderTypes(schema, [], Of(32, 32, Int32)));
    }

    [Fact]
    public void ColumnsAreFoundByPhysicalName_AndPartitionOrMissingColumnsAreSkipped()
    {
        StructType schema = StructType.Parse("""
            {"type":"struct","fields":[
              {"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-1"}},
              {"name":"p","type":"long","nullable":true,"metadata":{}},
              {"name":"later","type":"long","nullable":true,"metadata":{}}]}
            """);
        Assert.True(FileTypes.HasOlderTypes(schema, [], Footer(Leaf("col-1", Int32))));
        Assert.False(FileTypes.HasOlderTypes(schema, [], Footer(Leaf("id", Int32))));      // the logical name is not the file's column
        Assert.False(FileTypes.HasOlderTypes(schema, ["p"], Footer(Leaf("col-1", Int64), Leaf("p", Int32))));
        Assert.False(FileTypes.HasOlderTypes(schema, [], Footer(Leaf("col-1", Int64))));   // "later" is not in the file
    }

    private static string Mapped(string name, string physical, string type) =>
        $$$"""{"name":"{{{name}}}","type":{{{type}}},"nullable":true,"metadata":{"delta.columnMapping.physicalName":"{{{physical}}}"}}""";

    [Fact]
    public void ADroppedTopLevelColumn_IsExtra_FoundByPhysicalNameInAnyCase()
    {
        StructType schema = Table(string.Join(",", Mapped("id", "col-1", "\"integer\""), Field("later", "\"long\"")));

        Assert.False(FileTypes.HasExtraColumns(schema, [], Footer(Leaf("col-1", Int32))));
        Assert.False(FileTypes.HasExtraColumns(schema, [], Footer(Leaf("COL-1", Int32))));          // any case, as Spark
        Assert.True(FileTypes.HasExtraColumns(schema, [], Footer(Leaf("col-1", Int32), Leaf("col-2", ByteArray))));
        Assert.True(FileTypes.HasExtraColumns(schema, [], Footer(Leaf("id", Int32))));              // the logical name is not the column
        Assert.False(FileTypes.HasExtraColumns(schema, [], Footer()));
    }

    [Fact]
    public void TheMaterializedRowTrackingColumns_AreNotExtra()
    {
        StructType schema = Table(Field("id", "\"integer\""));
        ParquetMetadata footer = Footer(Leaf("id", Int32), Leaf("_row-id-col-1", Int64), Leaf("_row-commit-version-col-1", Int64));

        Assert.False(FileTypes.HasExtraColumns(schema, ["_row-id-col-1", "_row-commit-version-col-1"], footer));
        Assert.True(FileTypes.HasExtraColumns(schema, ["_row-id-col-1"], footer));
    }

    [Fact]
    public void DroppedNestedFields_InStructsArrayElementsAndMapValues_AreExtra()
    {
        const string Xy = """{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}""";
        StructType schema = Table(string.Join(",",
            Field("s", Xy),
            Field("a", $$"""{"type":"array","elementType":{{Xy}},"containsNull":true}"""),
            Field("m", $$"""{"type":"map","keyType":"string","valueType":{{Xy}},"valueContainsNull":true}""")));
        static ParquetMetadata Of(bool structY, bool arrayY, bool mapY) => Footer([
            Group("s", structY ? 2 : 1), Leaf("x", Int32), .. Y(structY),
            Group("a", 1), Group("list", 1, repetition: 2), Group("element", arrayY ? 2 : 1), Leaf("x", Int32), .. Y(arrayY),
            Group("m", 1), Group("key_value", 2, repetition: 2), Leaf("key", ByteArray), Group("value", mapY ? 2 : 1), Leaf("x", Int32), .. Y(mapY)]);

        // The dropped field y, when the file still holds it.
        static ParquetSchemaElement[] Y(bool held) => held ? [Leaf("y", Int32)] : [];

        Assert.False(FileTypes.HasExtraColumns(schema, [], Of(false, false, false)));
        Assert.True(FileTypes.HasExtraColumns(schema, [], Of(true, false, false)));
        Assert.True(FileTypes.HasExtraColumns(schema, [], Of(false, true, false)));
        Assert.True(FileTypes.HasExtraColumns(schema, [], Of(false, false, true)));
    }

    [Fact]
    public void AStructWhereTheTableHoldsAPlainValue_IsExtra()
    {
        StructType schema = Table(Field("c", "\"integer\""));
        Assert.True(FileTypes.HasExtraColumns(schema, [], Footer(Group("c", 1), Leaf("x", Int32))));
    }
}
