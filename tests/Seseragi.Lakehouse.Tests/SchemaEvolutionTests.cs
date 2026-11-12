using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Seseragi.Lakehouse.Tests;

public class SchemaEvolutionTests
{
    private static StructField F(string name, DeltaType type, bool nullable = true) => new() { Name = name, Type = type, Nullable = nullable };
    private static readonly PrimitiveType Int = new(PrimitiveType.Integer), Long = new(PrimitiveType.Long), Str = new(PrimitiveType.String);

    private static MetadataAction Meta(StructType schema, params (string, string)[] config) => new()
    {
        Id = "t", SchemaString = schema.ToJson(), Configuration = config.ToDictionary(c => c.Item1, c => c.Item2),
    };

    private static readonly ProtocolAction Basic = new() { MinReaderVersion = 1, MinWriterVersion = 2 };
    private static readonly ProtocolAction Widening = new()
    {
        MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = [TableFeatures.TypeWidening], WriterFeatures = [TableFeatures.TypeWidening],
    };

    [Fact]
    public void Writes_CastUp_AndFillMissingNullableColumns_WithoutChangingTheSchema()
    {
        var table = new StructType([F("id", Long), F("name", Str), F("amount", new DecimalType(10, 2))]);
        var data = new StructType([F("id", Int), F("amount", new DecimalType(5, 1))]);

        StructType merged = SchemaEvolution.Merge(Meta(table), Basic, data, evolve: false, out MetadataAction? changed);

        Assert.Null(changed);
        Assert.Equal(table.ToJson(), merged.ToJson());
        // A wider type, a missing NOT NULL column, a new column: refused without evolution.
        Assert.Contains("allow schema evolution to widen", Assert.Throws<DeltaException>(() =>
            SchemaEvolution.Merge(Meta(new StructType([F("id", Int)])), Basic, new StructType([F("id", Long)]), false, out _)).Message);
        Assert.Contains("NOT NULL", Assert.Throws<DeltaException>(() =>
            SchemaEvolution.Merge(Meta(new StructType([F("id", Int, nullable: false), F("x", Str)])), Basic, new StructType([F("x", Str)]), false, out _)).Message);
        Assert.Contains("Allow schema evolution to add", Assert.Throws<DeltaException>(() =>
            SchemaEvolution.Merge(Meta(table), Basic, new StructType([F("id", Long), F("extra", Str)]), false, out _)).Message);
    }

    [Fact]
    public void Evolution_AddsNullableColumns_AndWidensOnlyWithTypeWidening()
    {
        var table = new StructType([F("id", Int, nullable: false)]);
        var data = new StructType([F("id", Long), F("extra", Str, nullable: false)]);

        Assert.Contains("needs type widening", Assert.Throws<DeltaException>(() => SchemaEvolution.Merge(Meta(table), Basic, data, true, out _)).Message);

        StructType merged = SchemaEvolution.Merge(Meta(table, (SchemaEvolution.EnableTypeWideningKey, "true")), Widening, data, true, out MetadataAction? changed);

        Assert.NotNull(changed);
        Assert.Equal("long", merged.Field("id")!.Type.ToString());
        Assert.False(merged.Field("id")!.Nullable);
        Assert.True(merged.Field("extra")!.Nullable);
        Assert.Equal("""[{"toType":"long","fromType":"integer"}]""", merged.Field("id")!.Metadata[SchemaEvolution.TypeChangesKey]);
        // The history survives the log's round trip as JSON, not as a string.
        StructType parsed = StructType.Parse(changed!.SchemaString);
        Assert.Contains("\"delta.typeChanges\":[{", changed.SchemaString);
        Assert.Equal(merged.Field("id")!.Metadata[SchemaEvolution.TypeChangesKey], parsed.Field("id")!.Metadata[SchemaEvolution.TypeChangesKey]);
    }

    [Fact]
    public void Widening_AppendsToAnEarlierHistory_KeepingItsFields()
    {
        // A history from the preview feature, with a numeric tableVersion.
        string schema = """{"type":"struct","fields":[{"name":"x","type":"short","nullable":true,"metadata":{"delta.typeChanges":[{"toType":"short","fromType":"byte","tableVersion":3}]}}]}""";
        var metadata = new MetadataAction { Id = "t", SchemaString = schema, Configuration = new Dictionary<string, string> { [SchemaEvolution.EnableTypeWideningKey] = "true" } };

        StructType merged = SchemaEvolution.Merge(metadata, Widening, new StructType([F("x", Long)]), true, out _);

        Assert.Equal(
            """[{"toType":"short","fromType":"byte","tableVersion":3},{"toType":"long","fromType":"short"}]""",
            merged.Field("x")!.Metadata[SchemaEvolution.TypeChangesKey]);
    }

    [Fact]
    public void ColumnMapping_GivesNewColumnsIdsAndPhysicalNames()
    {
        string schema = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"}}]}""";
        var metadata = new MetadataAction
        {
            Id = "t", SchemaString = schema,
            Configuration = new Dictionary<string, string> { [TableFeatures.ColumnMappingModeKey] = "name", [SchemaEvolution.MaxColumnIdKey] = "1" },
        };

        StructType merged = SchemaEvolution.Merge(metadata, Basic, new StructType([F("id", Int), F("tag", Str)]), true, out MetadataAction? changed);

        StructField tag = merged.Field("tag")!;
        Assert.Equal(2, tag.FieldId);
        Assert.StartsWith("col-", tag.PhysicalName);
        Assert.Equal("2", changed!.Configuration[SchemaEvolution.MaxColumnIdKey]);
        Assert.Equal("col-a", merged.Field("id")!.PhysicalName);

        // Overwriting the schema assigns everything afresh.
        MetadataAction replaced = SchemaEvolution.Replace(changed, new StructType([F("a", Str), F("b", Long)]), ["a"]);
        StructType fresh = StructType.Parse(replaced.SchemaString);
        Assert.Equal([3, 4], fresh.Fields.Select(f => f.FieldId!.Value));
        Assert.Equal(["a"], replaced.PartitionColumns);
        Assert.Equal("4", replaced.Configuration[SchemaEvolution.MaxColumnIdKey]);
    }

    [Theory]
    [InlineData("integer", "long", true, true)]
    [InlineData("long", "integer", false, false)]
    [InlineData("float", "double", true, true)]
    [InlineData("integer", "double", false, true)]
    [InlineData("long", "double", false, false)]
    [InlineData("date", "timestamp_ntz", false, true)]
    [InlineData("integer", "decimal(10,0)", true, true)]
    [InlineData("integer", "decimal(9,0)", false, false)]
    [InlineData("decimal(5,2)", "decimal(10,4)", true, true)]
    [InlineData("decimal(5,2)", "decimal(5,3)", false, false)]
    public void CastsAndWidenings(string from, string to, bool implicitly, bool widens)
    {
        static DeltaType Parse(string t) =>
            StructType.Parse("{\"type\":\"struct\",\"fields\":[{\"name\":\"c\",\"type\":\"" + t + "\",\"nullable\":true,\"metadata\":{}}]}").Fields[0].Type;
        Assert.Equal(implicitly, SchemaEvolution.CastsImplicitly(Parse(from), Parse(to)));
        Assert.Equal(widens, SchemaEvolution.CanWiden(Parse(from), Parse(to)));
    }

    [Fact]
    public void TimestampsWithoutTimeZone_NeedTheirFeature()
    {
        var table = new StructType([F("d", new PrimitiveType(PrimitiveType.Date))]);
        var data = new StructType([F("d", new PrimitiveType(PrimitiveType.TimestampNtz))]);
        Assert.Contains("timestampNtz", Assert.Throws<DeltaException>(() =>
            SchemaEvolution.Merge(Meta(table, (SchemaEvolution.EnableTypeWideningKey, "true")), Widening, data, true, out _)).Message);
    }

    [Fact]
    public void NestedFields_AreMerged_InStructsArraysAndMaps()
    {
        static StructType S(params StructField[] f) => new(f);
        var table = S(F("s", S(F("a", Int))), F("arr", new ArrayType(S(F("x", Int)), true)), F("m", new MapType(Str, S(F("v", Int)), true)));
        var data = S(F("s", S(F("a", Int), F("b", Str))), F("arr", new ArrayType(S(F("x", Int), F("y", Str)), true)),
            F("m", new MapType(Str, S(F("v", Int), F("w", Str)), true)));

        Assert.Contains("s.b", Assert.Throws<DeltaException>(() => SchemaEvolution.Merge(Meta(table), Basic, data, false, out _)).Message);
        StructType merged = SchemaEvolution.Merge(Meta(table), Basic, data, true, out MetadataAction? changed);

        Assert.NotNull(changed);
        Assert.Equal("struct<a:integer,b:string>", merged.Field("s")!.Type.ToString().Replace(" ", ""));
        Assert.Equal("array<struct<x:integer,y:string>>", merged.Field("arr")!.Type.ToString().Replace(" ", ""));
        Assert.Equal("map<string,struct<v:integer,w:string>>", merged.Field("m")!.Type.ToString().Replace(" ", ""));
        // Data without the new fields fits the merged table: nothing changes.
        SchemaEvolution.Merge(Meta(merged), Basic, table, false, out MetadataAction? none);
        Assert.Null(none);
    }

    [Fact]
    public void NestedFields_AreWidened_AndRecordTheirOwnChanges()
    {
        var table = new StructType([F("s", new StructType([F("n", Int), F("d", new PrimitiveType(PrimitiveType.Date))]))]);
        var meta = Meta(table, (SchemaEvolution.EnableTypeWideningKey, "true"));

        StructType widened = SchemaEvolution.Merge(meta, Widening,
            new StructType([F("s", new StructType([F("n", Long), F("d", new PrimitiveType(PrimitiveType.Date))]))]), true, out _);
        var n = ((StructType)widened.Field("s")!.Type).Field("n")!;
        Assert.Equal("long", n.Type.ToString());
        Assert.Contains("\"fromType\":\"integer\"", n.Metadata[SchemaEvolution.TypeChangesKey]);

        // Date to timestamp_ntz widens at any depth too, but needs the timestampNtz feature (as Spark).
        Assert.Contains("timestampNtz", Assert.Throws<DeltaException>(() => SchemaEvolution.Merge(meta, Widening,
            new StructType([F("s", new StructType([F("n", Int), F("d", new PrimitiveType(PrimitiveType.TimestampNtz))]))]), true, out _)).Message);
    }

    [Fact]
    public void ArrayElementsAndMapKeysAndValues_AreWidened_WithTheirPathOnTheField_AsSpark()
    {
        // delta-spark 4.4: ALTER COLUMN arr.element / mm.key / mm.value.element record on the column with fieldPath.
        var table = new StructType([
            F("arr", new ArrayType(Int, true)),
            F("mm", new MapType(Int, new ArrayType(Int, true), true)),
            F("arrs", new ArrayType(new StructType([F("x", new PrimitiveType(PrimitiveType.Short))]), true)),
        ]);
        var meta = Meta(table, (SchemaEvolution.EnableTypeWideningKey, "true"));
        StructType widened = SchemaEvolution.Merge(meta, Widening, new StructType([
            F("arr", new ArrayType(Long, true)),
            F("mm", new MapType(Long, new ArrayType(Long, true), true)),
            F("arrs", new ArrayType(new StructType([F("x", Int)]), true)),
        ]), true, out MetadataAction? changed);

        Assert.NotNull(changed);
        Assert.Equal("array<long>", widened.Field("arr")!.Type.ToString());
        Assert.Equal("""[{"toType":"long","fromType":"integer","fieldPath":"element"}]""", widened.Field("arr")!.Metadata[SchemaEvolution.TypeChangesKey]);
        Assert.Equal(
            """[{"toType":"long","fromType":"integer","fieldPath":"key"},{"toType":"long","fromType":"integer","fieldPath":"value.element"}]""",
            widened.Field("mm")!.Metadata[SchemaEvolution.TypeChangesKey]);
        // A struct inside an array: its field records its own change, without a path.
        StructField x = ((StructType)((ArrayType)widened.Field("arrs")!.Type).ElementType).Field("x")!;
        Assert.Equal("""[{"toType":"integer","fromType":"short"}]""", x.Metadata[SchemaEvolution.TypeChangesKey]);
        Assert.False(widened.Field("arrs")!.Metadata.ContainsKey(SchemaEvolution.TypeChangesKey));

        // Without type widening: refused, saying what would allow it.
        var plain = Meta(table);
        Assert.Contains("delta.enableTypeWidening", Assert.Throws<DeltaException>(() => SchemaEvolution.Merge(plain, Widening,
            new StructType([F("arr", new ArrayType(Long, true)), F("mm", table.Field("mm")!.Type), F("arrs", table.Field("arrs")!.Type)]), true, out _)).Message);
    }

    [Fact]
    public void NestedFields_AddedUnderColumnMapping_GetTheNextIdsAndPhysicalNames()
    {
        string mapped = """{"type":"struct","fields":[{"name":"s","type":{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b"}}]},"nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"}}]}""";
        var mappedMeta = new MetadataAction
        {
            Id = "t", SchemaString = mapped,
            Configuration = new Dictionary<string, string> { [TableFeatures.ColumnMappingModeKey] = "name", [SchemaEvolution.MaxColumnIdKey] = "2" },
        };
        // A new field b holding a list of structs: b, then the struct field inside the list, in Spark's order.
        StructType merged = SchemaEvolution.Merge(mappedMeta, Basic,
            new StructType([F("s", new StructType([F("a", Int), F("b", new ArrayType(new StructType([F("x", Int)]), true))]))]), true, out MetadataAction? changed);
        var s = (StructType)merged.Field("s")!.Type;
        Assert.Equal(("col-b", 2), (s.Field("a")!.PhysicalName, s.Field("a")!.FieldId!.Value));
        StructField b = s.Field("b")!;
        StructField x = ((StructType)((ArrayType)b.Type).ElementType).Field("x")!;
        Assert.Equal((3, 4), (b.FieldId!.Value, x.FieldId!.Value));
        Assert.StartsWith("col-", b.PhysicalName);
        Assert.StartsWith("col-", x.PhysicalName);
        Assert.Equal("4", changed!.Configuration[SchemaEvolution.MaxColumnIdKey]);
    }

    [Fact]
    public void ANewMappedSchema_NumbersStructFieldsInsideArraysAndMaps_DepthFirst_AsSpark()
    {
        var meta = new MetadataAction
        {
            Id = "t", SchemaString = """{"type":"struct","fields":[]}""",
            Configuration = new Dictionary<string, string> { [TableFeatures.ColumnMappingModeKey] = "id" },
        };
        // Spark 4.2: id INT, s STRUCT<a, b STRUCT<c>>, arr ARRAY<STRUCT<x>>, m MAP<STRING, STRUCT<y>> numbers 1..9.
        var schema = new StructType([
            F("id", Int),
            F("s", new StructType([F("a", Int), F("b", new StructType([F("c", Str)]))])),
            F("arr", new ArrayType(new StructType([F("x", Int)]), true)),
            F("m", new MapType(Str, new StructType([F("y", Int)]), true)),
        ]);
        MetadataAction replaced = SchemaEvolution.Replace(meta, schema, []);
        var mapped = StructType.Parse(replaced.SchemaString);
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], Ids(mapped));
        Assert.Equal("9", replaced.Configuration[SchemaEvolution.MaxColumnIdKey]);

        static List<int> Ids(DeltaType type) => type switch
        {
            StructType st => [.. st.Fields.SelectMany(f => (IEnumerable<int>)[f.FieldId!.Value, .. Ids(f.Type)])],
            ArrayType a => Ids(a.ElementType),
            MapType m => [.. Ids(m.KeyType), .. Ids(m.ValueType)],
            _ => [],
        };
    }
}
