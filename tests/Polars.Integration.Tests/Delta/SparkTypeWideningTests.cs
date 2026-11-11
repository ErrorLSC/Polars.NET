using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Wrappers;

namespace Polars.Integration.Tests;

/// <summary>Type widening inside nested columns (struct fields, array elements, map keys and values) between the kernel and Spark.</summary>
public class SparkTypeWideningTests
{
    private const string Columns =
        "id INT, s STRUCT<a: INT, d: DECIMAL(5,2), dt: DATE, f: FLOAT>, arr ARRAY<INT>, arrs ARRAY<STRUCT<x: SMALLINT>>, " +
        "m MAP<STRING, DECIMAL(5,2)>, mm MAP<INT, ARRAY<INT>>";

    private const string FirstRow =
        "(1, named_struct('a', 1, 'd', 1.25, 'dt', DATE'2024-01-02', 'f', 1.5), array(1, 2), array(named_struct('x', 3)), map('k', 2.5), map(1, array(5)))";

    private static readonly string[] Widenings =
    [
        "s.a TYPE BIGINT", "s.d TYPE DECIMAL(10,4)", "s.dt TYPE TIMESTAMP_NTZ", "s.f TYPE DOUBLE", "arr.element TYPE BIGINT",
        "arrs.element.x TYPE INT", "m.value TYPE DECIMAL(12,2)", "mm.key TYPE BIGINT", "mm.value.element TYPE BIGINT",
    ];

    private const string SecondRow =
        "(2, named_struct('a', 10000000000, 'd', 3.1234, 'dt', TIMESTAMP_NTZ'2024-01-03 04:05:06', 'f', 2.5D), array(10000000000), " +
        "array(named_struct('x', 70000)), map('k', 1234567890.12), map(10000000000, array(10000000000)))";

    private static void InRoot(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_tw_{Guid.NewGuid():N}");
        try
        {
            body(root);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>The schema and rows Spark reads, as canonical JSON.</summary>
    private static (string Schema, string Rows) Spark(string table)
    {
        JsonElement read = LocalSpark.Run("read", table)[0].RootElement;
        return (read.GetProperty("schema").GetString()!,
            string.Join(" ", read.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText())));
    }

    // The same without the map columns: the kernel cannot write map columns Spark reads yet (the engine has no
    // map type), so tables whose files the kernel rewrites leave them out.
    private const string ColumnsWithoutMaps =
        "id INT, s STRUCT<a: INT, d: DECIMAL(5,2), dt: DATE, f: FLOAT>, arr ARRAY<INT>, arrs ARRAY<STRUCT<x: SMALLINT>>";

    private const string FirstRowWithoutMaps =
        "(1, named_struct('a', 1, 'd', 1.25, 'dt', DATE'2024-01-02', 'f', 1.5), array(1, 2), array(named_struct('x', 3)))";

    private const string SecondRowWithoutMaps =
        "(2, named_struct('a', 10000000000, 'd', 3.1234, 'dt', TIMESTAMP_NTZ'2024-01-03 04:05:06', 'f', 2.5D), array(10000000000), " +
        "array(named_struct('x', 70000)))";

    /// <summary>A table whose first file holds the older types of every nested column Spark widened afterwards.</summary>
    private static void CreateWidened(string table, string mapping, bool maps = true)
    {
        LocalSpark.Run("sql", table,
            $"CREATE TABLE {{table}} ({(maps ? Columns : ColumnsWithoutMaps)}) USING delta TBLPROPERTIES ('delta.enableTypeWidening'='true', " +
            $"'delta.feature.timestampNtz'='supported', 'delta.columnMapping.mode'='{mapping}')");
        LocalSpark.Run("sql", table, $"INSERT INTO {{table}} VALUES {(maps ? FirstRow : FirstRowWithoutMaps)}");
        foreach (string widening in Widenings.Where(w => maps || !w.StartsWith('m')))
            LocalSpark.Run("sql", table, $"ALTER TABLE {{table}} ALTER COLUMN {widening}");
        LocalSpark.Run("sql", table, $"INSERT INTO {{table}} VALUES {(maps ? SecondRow : SecondRowWithoutMaps)}");
    }

    [SparkTheory]
    [InlineData("none")]
    [InlineData("name")]
    [Trait("DeltaKernel", "Spark")]
    public void Kernel_ReadsNestedWidenedColumns_AndRewritesOldFilesInTheNewTypes(string mapping)
    {
        InRoot(root =>
        {
            string withMaps = Path.Combine(root, "maps"), copy = Path.Combine(root, "copy");
            CreateWidened(withMaps, mapping);

            // What the kernel reads, written to a new table: Spark reads the same values from it. Rows only: a table
            // the kernel creates holds maps as lists of key/value structs (no map type in the engine).
            DeltaKernelWrite.Write(copy, DeltaKernelScan.Scan(withMaps), KernelWriteMode.ErrorIfExists);
            Assert.Equal(Spark(withMaps).Rows, Spark(copy).Rows);

            // An update rewrites the first file (older types) in the table's types.
            string byKernel = Path.Combine(root, "kernel"), bySpark = Path.Combine(root, "spark");
            CreateWidened(byKernel, mapping, maps: false);
            CreateWidened(bySpark, mapping, maps: false);
            DeltaKernelUpdate.Update(byKernel, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(1)), [("id", PolarsWrapper.Lit(100))]);
            LocalSpark.Run("sql", bySpark, "UPDATE {table} SET id = 100 WHERE id = 1");
            Assert.Equal(Spark(bySpark), Spark(byKernel));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Kernel_WidensNestedColumnsOnWrite_RecordingTheChangesAsSpark()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table"), wider = Path.Combine(root, "wider");
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, s STRUCT<a: INT>, arr ARRAY<INT>, arrs ARRAY<STRUCT<x: SMALLINT>>) USING delta " +
                "TBLPROPERTIES ('delta.enableTypeWidening'='true')");
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (1, named_struct('a', 1), array(1), array(named_struct('x', 2)))");
            LocalSpark.Run("sql", wider, "CREATE TABLE {table} (id INT, s STRUCT<a: BIGINT>, arr ARRAY<BIGINT>, arrs ARRAY<STRUCT<x: INT>>) USING delta");
            LocalSpark.Run("sql", wider,
                "INSERT INTO {table} VALUES (2, named_struct('a', 10000000000), array(10000000000), array(named_struct('x', 70000)))");

            DeltaKernelWrite.Write(table, DeltaKernelScan.Scan(wider), KernelWriteMode.Append, canEvolve: true);

            // As Spark's mergeSchema records them (delta-spark 4.1): on the struct field itself, or on the column
            // with the path to the array element.
            StructType schema = Snapshot.Load(new LocalFileSystem(table)).Schema;
            StructField a = ((StructType)schema.Field("s")!.Type).Field("a")!;
            StructField x = ((StructType)((ArrayType)schema.Field("arrs")!.Type).ElementType).Field("x")!;
            Assert.Equal("""[{"toType":"long","fromType":"integer"}]""", a.Metadata[SchemaEvolution.TypeChangesKey]);
            Assert.Equal("""[{"toType":"long","fromType":"integer","fieldPath":"element"}]""", schema.Field("arr")!.Metadata[SchemaEvolution.TypeChangesKey]);
            Assert.Equal("""[{"toType":"integer","fromType":"short"}]""", x.Metadata[SchemaEvolution.TypeChangesKey]);

            var (sparkSchema, rows) = Spark(table);
            Assert.Equal("struct<id:int,s:struct<a:bigint>,arr:array<bigint>,arrs:array<struct<x:int>>>", sparkSchema);
            Assert.Contains("\"a\": 10000000000", rows);
            Assert.Contains("\"x\": 70000", rows);
            Assert.Contains("\"a\": 1}", rows);
        });
    }
}
