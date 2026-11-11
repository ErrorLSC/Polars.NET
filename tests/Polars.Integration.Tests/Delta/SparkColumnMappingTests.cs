using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Parquet;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Wrappers;

namespace Polars.Integration.Tests;

/// <summary>
/// Column mapping (name and id mode, nested columns included) between the kernel and Spark: the kernel's files
/// hold every struct field, at any depth, under its physical name with its column mapping id as the Parquet field
/// id, as Spark's do.
/// </summary>
public class SparkColumnMappingTests
{
    private const string Nested = "id INT, s STRUCT<a: INT, b: STRUCT<c: STRING>>, arr ARRAY<STRUCT<x: INT, y: STRING>>";

    private const string NestedValues =
        "(1, named_struct('a', 10, 'b', named_struct('c', 'one')), array(named_struct('x', 1, 'y', 'p'), named_struct('x', 2, 'y', null)))," +
        "(2, null, array())," +
        "(3, named_struct('a', null, 'b', null), null)";

    private static string NewRoot() => Path.Combine(Path.GetTempPath(), $"seseragi_cm_{Guid.NewGuid():N}");

    private static void InRoot(Action<string> body)
    {
        string root = NewRoot();
        try
        {
            body(root);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>The rows Spark reads, as canonical JSON, with the schema.</summary>
    private static (string Schema, string Rows) Spark(string table)
    {
        JsonElement read = LocalSpark.Run("read", table)[0].RootElement;
        return (read.GetProperty("schema").GetString()!,
            string.Join(" ", read.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText())));
    }

    /// <summary>
    /// Every data file holds each field of the schema, at any depth, under its physical name with its column
    /// mapping id as field id, and no other field ids.
    /// </summary>
    private static void AssertFieldIds(string table)
    {
        var fs = new LocalFileSystem(table);
        Snapshot snapshot = Snapshot.Load(fs);
        var expected = new Dictionary<string, int>(StringComparer.Ordinal);
        void Collect(DeltaType type)
        {
            switch (type)
            {
                case StructType s:
                    foreach (StructField f in s.Fields)
                    {
                        if (!snapshot.PartitionColumns.Contains(f.Name)) expected[f.PhysicalName] = f.FieldId ?? throw new InvalidOperationException($"{f.Name} has no id");
                        Collect(f.Type);
                    }
                    break;
                case ArrayType a: Collect(a.ElementType); break;
                case MapType m: Collect(m.KeyType); Collect(m.ValueType); break;
            }
        }
        Collect(snapshot.Schema);

        Assert.NotEmpty(snapshot.Files);
        foreach (AddAction file in snapshot.Files)
        {
            ParquetMetadata footer = ParquetMetadata.Read(fs, file.DecodedPath, file.Size);
            var actual = footer.Schema.Skip(1).Where(e => e.FieldId is not null).ToDictionary(e => e.Name, e => e.FieldId!.Value);
            // Files written before a column was added lack it; every field a file has carries its id.
            foreach (var (name, id) in actual) Assert.True(expected.TryGetValue(name, out int want) && want == id, $"{file.Path}: {name} = {id}");
            foreach (ParquetSchemaElement element in footer.Schema.Skip(1).Where(e => expected.ContainsKey(e.Name)))
                Assert.True(element.FieldId is not null, $"{file.Path}: {element.Name} has no field id");
        }
    }

    [SparkTheory]
    [InlineData("name")]
    [InlineData("id")]
    [Trait("DeltaKernel", "Spark")]
    public void KernelWritesNestedColumns_UnderMapping_SparkReadsThem(string mode)
    {
        InRoot(root =>
        {
            string source = Path.Combine(root, "source"), table = Path.Combine(root, "mapped");
            LocalSpark.Run("sql", source, $"CREATE TABLE {{table}} ({Nested}) USING delta");
            LocalSpark.Run("sql", source, $"INSERT INTO {{table}} VALUES {NestedValues}");

            DeltaKernelWrite.Write(table, DeltaKernelScan.Scan(source), KernelWriteMode.ErrorIfExists,
                configuration: new Dictionary<string, string> { ["delta.columnMapping.mode"] = mode });
            Snapshot created = Snapshot.Load(new LocalFileSystem(table));
            // Ids in Spark's order (depth first) and the highest one recorded.
            Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], Ids(created.Schema));
            Assert.Equal("8", created.Metadata.Configuration[SchemaEvolution.MaxColumnIdKey]);
            AssertFieldIds(table);
            Assert.Equal(Spark(source), Spark(table));
        });
    }

    private static List<int> Ids(DeltaType type) => type switch
    {
        StructType s => [.. s.Fields.SelectMany(f => (IEnumerable<int>)[f.FieldId!.Value, .. Ids(f.Type)])],
        ArrayType a => Ids(a.ElementType),
        MapType m => [.. Ids(m.KeyType), .. Ids(m.ValueType)],
        _ => [],
    };

    [SparkTheory]
    [InlineData("name")]
    [InlineData("id")]
    [Trait("DeltaKernel", "Spark")]
    public void OnSparksTable_WithRenamedDroppedAndReAddedNestedFields_KernelOperationsMatchSparks(string mode)
    {
        InRoot(root =>
        {
            // The same table twice; Spark operates on one, the kernel on the other, then both are compared as Spark reads them.
            string byKernel = Path.Combine(root, "kernel"), bySpark = Path.Combine(root, "spark"), extra = Path.Combine(root, "extra");
            string[] tables = [byKernel, bySpark];
            foreach (string table in tables)
            {
                LocalSpark.Run("sql", table,
                    $"CREATE TABLE {{table}} ({Nested}) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='{mode}', 'delta.enableChangeDataFeed'='true')");
                LocalSpark.Run("sql", table, $"INSERT INTO {{table}} VALUES {NestedValues}");
                // A renamed nested field, and a nested field dropped and added again under the same name (new id, new physical name).
                LocalSpark.Run("sql", table, "ALTER TABLE {table} RENAME COLUMN s.a TO a2");
                LocalSpark.Run("sql", table, "ALTER TABLE {table} DROP COLUMN s.b");
                LocalSpark.Run("sql", table, "ALTER TABLE {table} ADD COLUMNS (s.b STRUCT<c: STRING>)");
                LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (4, named_struct('a2', 40, 'b', named_struct('c', 'four')), array(named_struct('x', 4, 'y', 'q')))");
            }
            LocalSpark.Run("sql", extra, "CREATE TABLE {table} (id INT, s STRUCT<a2: INT, b: STRUCT<c: STRING>>, arr ARRAY<STRUCT<x: INT, y: STRING>>) USING delta");
            LocalSpark.Run("sql", extra, "INSERT INTO {table} VALUES (5, named_struct('a2', 50, 'b', named_struct('c', 'five')), array(named_struct('x', 5, 'y', null)))");

            // Append, update (rewrites files), merge (updates every row, inserts none), optimize.
            DeltaKernelWrite.Write(byKernel, DeltaKernelScan.Scan(extra), KernelWriteMode.Append);
            LocalSpark.Run("sql", bySpark, $"INSERT INTO {{table}} SELECT * FROM delta.`{extra}`");
            DeltaKernelUpdate.Update(byKernel, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(1)), [("id", PolarsWrapper.Lit(100))]);
            LocalSpark.Run("sql", bySpark, "UPDATE {table} SET id = 100 WHERE id = 1");
            DeltaKernelMerge.Merge(byKernel, DeltaKernelScan.Scan(byKernel), ["id"],
                [new(MergeClauseKind.MatchedUpdate, PolarsWrapper.Lit(true)), new(MergeClauseKind.NotMatchedInsert, PolarsWrapper.Lit(true))]);
            LocalSpark.Run("sql", bySpark,
                "MERGE INTO {table} t USING (SELECT * FROM {table}) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *");
            DeltaKernelOptimize.Optimize(byKernel);
            LocalSpark.Run("sql", bySpark, "OPTIMIZE {table}");

            AssertFieldIds(byKernel);
            Assert.Equal(Spark(bySpark), Spark(byKernel));
            Assert.Equal(Changes(bySpark), Changes(byKernel));
        });
    }

    /// <summary>
    /// The change feed as Spark reads it (without commit times), from version 6 on: the first operation after the
    /// schema changes, which Spark does not read the feed across.
    /// </summary>
    private static string Changes(string table) =>
        string.Join(" ", LocalSpark.Run("read_changes_from", table, "6")[1].RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText()));

    [SparkTheory]
    [InlineData("name")]
    [InlineData("id")]
    [Trait("DeltaKernel", "Spark")]
    public void SchemaEvolution_AddsNestedFields_WithFreshIdsAndPhysicalNames(string mode)
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "mapped"), wider = Path.Combine(root, "wider");
            LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} ({Nested}) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='{mode}')");
            LocalSpark.Run("sql", table, $"INSERT INTO {{table}} VALUES {NestedValues}");
            LocalSpark.Run("sql", wider,
                "CREATE TABLE {table} (id INT, s STRUCT<a: INT, b: STRUCT<c: STRING, d: INT>, e: STRING>, arr ARRAY<STRUCT<x: INT, y: STRING, z: DOUBLE>>) USING delta");
            LocalSpark.Run("sql", wider,
                "INSERT INTO {table} VALUES (9, named_struct('a', 90, 'b', named_struct('c', 'nine', 'd', 99), 'e', 'new'), array(named_struct('x', 9, 'y', 'r', 'z', 0.5)))");

            DeltaKernelWrite.Write(table, DeltaKernelScan.Scan(wider), KernelWriteMode.Append, canEvolve: true);

            Snapshot evolved = Snapshot.Load(new LocalFileSystem(table));
            // The new fields take the next ids, in Spark's order, and fresh physical names.
            Assert.Equal([1, 2, 3, 4, 5, 9, 10, 6, 7, 8, 11], Ids(evolved.Schema));
            Assert.Equal("11", evolved.Metadata.Configuration[SchemaEvolution.MaxColumnIdKey]);
            var s = (StructType)evolved.Schema.Field("s")!.Type;
            Assert.StartsWith("col-", s.Field("e")!.PhysicalName);
            AssertFieldIds(table);
            var (schema, rows) = Spark(table);
            Assert.Equal("struct<id:int,s:struct<a:int,b:struct<c:string,d:int>,e:string>,arr:array<struct<x:int,y:string,z:double>>>", schema);
            Assert.Contains("\"d\": 99", rows);
            Assert.Contains("\"e\": \"new\"", rows);
            Assert.Contains("\"z\": 0.5", rows);
        });
    }
}
