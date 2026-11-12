using Polars.CSharp;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>Tables with variant columns (the variantType feature), written by Spark and read by the kernel.</summary>
public class SparkVariantTests
{
    private static void InTable(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_variant_{Guid.NewGuid():N}");
        try
        {
            body(Path.Combine(root, "table"));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>A Spark table with variant columns at the top level, in a struct and in an array; row 2 all null.</summary>
    private static void CreateVariantTable(string table)
    {
        LocalSpark.Run("sql", table, "CREATE TABLE {table} (id INT, v VARIANT, s STRUCT<x: VARIANT>, a ARRAY<VARIANT>) USING delta");
        LocalSpark.Run("sql", table, """
            INSERT INTO {table} SELECT 1, parse_json('{"a": 1, "b": [true, null, 2.5], "c": "hi"}'),
                named_struct('x', parse_json('42')), array(parse_json('"s"'), parse_json('null'))
            """);
        LocalSpark.Run("sql", table, "INSERT INTO {table} SELECT 2, NULL, NULL, NULL");
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantColumns_AreReadAsTheirValueAndMetadataBinaries()
    {
        InTable(table =>
        {
            CreateVariantTable(table);

            using DataFrame read = DataFrame.ReadDelta(table).Sort("id");
            Assert.Equal(["id", "v", "s", "a"], read.Columns);
            string type = read.Schema["v"].ToString();
            Assert.True(type.Contains("value", StringComparison.Ordinal) && type.Contains("metadata", StringComparison.Ordinal), type);
            using DataFrame v = read.Select(Polars.CSharp.Polars.Col("v").Struct.Field("value"), Polars.CSharp.Polars.Col("v").Struct.Field("metadata"));
            byte[]?[] values = v["value"].ToArray<byte[]?>();
            byte[]?[] metadata = v["metadata"].ToArray<byte[]?>();
            // Row 1: an object (basic type 2 in the low bits of the header), metadata version 1; row 2: null.
            Assert.Equal(2, values[0]![0] & 0x3);
            Assert.Equal(1, metadata[0]![0] & 0xF);
            using DataFrame nulls = read.Select(Polars.CSharp.Polars.Col("v").IsNull().Alias("v"), Polars.CSharp.Polars.Col("s").Struct.Field("x").IsNull().Alias("x"));
            Assert.Equal([false, true], nulls["v"].ToArray<bool>());
            Assert.Equal([false, true], nulls["x"].ToArray<bool>());
        });
    }

    /// <summary>Every file of the table, data and log, with its size.</summary>
    private static string[] Files(string table) =>
        [.. Directory.EnumerateFiles(table, "*", SearchOption.AllDirectories)
            .Select(f => $"{Path.GetRelativePath(table, f)}:{new FileInfo(f).Length}").Order(StringComparer.Ordinal)];

    [SparkTheory]
    [InlineData("delete")]
    [InlineData("update")]
    [InlineData("append")]
    [InlineData("optimize")]
    [InlineData("vacuum")]
    [InlineData("restore")]
    [InlineData("properties")]
    [InlineData("dropFeature")]
    [Trait("DeltaKernel", "Spark")]
    public void VariantTables_AreReadOnly(string operation)
    {
        InTable(table =>
        {
            CreateVariantTable(table);
            // A removed file for vacuum to find.
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id = 2");
            string[] before = Files(table);

            Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(() =>
            {
                switch (operation)
                {
                    case "delete":
                        Delta.Delete(table, Polars.CSharp.Polars.Col("id") == 1);
                        break;
                    case "update":
                        Delta.Update(table, Polars.CSharp.Polars.Col("id") == 1, new Dictionary<string, Expr> { ["id"] = Polars.CSharp.Polars.Lit(5) });
                        break;
                    case "append":
                        using (DataFrame rows = DataFrame.ReadDelta(table))
                            rows.WriteDelta(table, mode: DeltaSaveMode.Append);
                        break;
                    case "optimize":
                        Delta.Optimize(table);
                        break;
                    case "vacuum":
                        Delta.Vacuum(table, retentionHours: 0, enforceRetention: false);
                        break;
                    case "restore":
                        Delta.Restore(table, version: 1);
                        break;
                    case "properties":
                        Delta.SetTableProperties(table, new() { ["delta.appendOnly"] = "true" });
                        break;
                    case "dropFeature":
                        Delta.DropFeature(table, "variantType");
                        break;
                }
            });
            Assert.Equal(before, Files(table));
        });
    }

    /// <summary>Values of every kind a variant holds, one per row, as Spark casts or parses them.</summary>
    private static readonly string[] Values =
    [
        "parse_json('{\"b\": [true, null, 2.5], \"a\": 1, \"c\": \"hi\"}')",
        "parse_json('\"esc: \\\\\" \\\\\\\\ \\\\n \\\\t \\\\u0001 \\\\u001f / ü 😀\"')",
        "parse_json('12345678901234567890123')",
        "parse_json('1e400')",
        "parse_json('[]')",
        "parse_json('{}')",
        "cast(cast(-5 as tinyint) as variant)",
        "cast(cast(300 as smallint) as variant)",
        "cast(-70000 as variant)",
        "cast(5000000000 as variant)",
        "cast(cast(0.1 as float) as variant)",
        "cast(cast(1e10 as float) as variant)",
        "cast(0.1 + 0.2 as variant)",
        "cast(1e-5 as variant)",
        "cast(12345678.9 as variant)",
        "cast(1234567.0 as variant)",
        "cast(cast('NaN' as double) as variant)",
        "cast(cast('-Infinity' as double) as variant)",
        "cast(cast(-0.0 as double) as variant)",
        "cast(4.9e-324 as variant)",
        "cast(1234567.0D as variant)",
        "cast(1.0E7D as variant)",
        "cast(12345678.9D as variant)",
        "cast(0.001D as variant)",
        "cast(1.0E-4D as variant)",
        "cast(1e23D as variant)",
        "cast(100.0D as variant)",
        "cast(0.1D + 0.2D as variant)",
        "cast(cast(3.25 as float) as variant)",
        "cast(cast(1.4E-45 as float) as variant)",
        "cast(cast(1.50 as decimal(5, 2)) as variant)",
        "cast(cast(-0.005 as decimal(10, 3)) as variant)",
        "cast(cast('-12345678901234567890.1' as decimal(38, 1)) as variant)",
        "cast(date'2024-02-29' as variant)",
        "cast(date'1969-12-31' as variant)",
        "cast(timestamp'2024-01-02 03:04:05.5' as variant)",
        "cast(timestamp_ntz'1969-12-31 23:59:59.999999' as variant)",
        "cast(timestamp_ntz'2024-01-02 00:00:00' as variant)",
        "cast(X'010203' as variant)",
        "cast(true as variant)",
        "parse_json('null')",
        "cast(sequence(1, 300) as variant)",
        "parse_json(concat('{', concat_ws(',', transform(sequence(1, 300), i -> concat('\"k', i, '\":', i))), '}'))",
        "to_variant_object(named_struct('z', 1, 'a', array(1, 2), 'm', named_struct('q', 'x')))",
    ];

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantAsJson_RendersEveryValueAsSparksToJson()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, v VARIANT, s STRUCT<x: VARIANT>, a ARRAY<VARIANT>, m MAP<STRING, VARIANT>) USING delta");
            // A SELECT per value (inline VALUES cannot hold lambdas), then a row of nulls.
            string values = string.Join(" UNION ALL ", Values.Select((value, i) => $"SELECT {i} AS id, {value} AS v"));
            LocalSpark.Run("sql", table, $$"""
                INSERT INTO {table} SELECT id, v, named_struct('x', v), array(v, parse_json('7')), map('k', v) FROM ({{values}})
                UNION ALL SELECT {{Values.Length}}, NULL, NULL, NULL, NULL
                """);

            // Spark's rendering of each value, in each place.
            var spark = LocalSpark.Run("query", table, """
                SELECT id, to_json(v) AS v, to_json(s.x) AS sx, to_json(a[0]) AS a0, to_json(m['k']) AS mk
                FROM {table} ORDER BY id
                """)[0].RootElement.GetProperty("rows").EnumerateArray()
                .Select(r => (Id: r.GetProperty("id").GetInt32(), V: r.GetProperty("v").GetString(), Sx: r.GetProperty("sx").GetString(),
                    A0: r.GetProperty("a0").GetString(), Mk: r.GetProperty("mk").GetString()))
                .ToArray();

            using DataFrame read = DataFrame.ReadDelta(table, variantAsJson: true).Sort("id");
            using DataFrame flat = read.Select(
                Polars.CSharp.Polars.Col("v"),
                Polars.CSharp.Polars.Col("s").Struct.Field("x").Alias("sx"),
                Polars.CSharp.Polars.Col("a").List.Get(0).Alias("a0"),
                Polars.CSharp.Polars.Col("a").List.Get(1).Alias("a1"));
            string?[] v = flat["v"].ToArray<string?>(), sx = flat["sx"].ToArray<string?>(), a0 = flat["a0"].ToArray<string?>();
            Assert.Equal(spark.Length, v.Length);
            for (int i = 0; i < spark.Length; i++)
            {
                Assert.True(spark[i].V == v[i], $"row {i}: Spark {spark[i].V} / kernel {v[i]}");
                Assert.Equal(spark[i].Sx, sx[i]);
                Assert.Equal(spark[i].A0, a0[i]);
            }
            Assert.Equal("7", flat["a1"].ToArray<string?>()[0]);
            // The map's values too: a list of key/value structs.
            using DataFrame map = read.Select(Polars.CSharp.Polars.Col("m").List.Get(0).Struct.Field("value").Alias("mk"));
            Assert.Equal(spark.Select(r => r.Mk), map["mk"].ToArray<string?>());
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantColumns_OfAMappedPartitionedTableWithDeletionVectors_ReadAsSparkReadsThem()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table, """
                CREATE TABLE {table} (id INT, p STRING, v VARIANT) USING delta PARTITIONED BY (p)
                TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'='true')
                """);
            LocalSpark.Run("sql", table, """
                INSERT INTO {table} SELECT /*+ COALESCE(1) */ id, p, parse_json(j) FROM VALUES
                    (1, 'x', '{"k": [1, 2]}'), (2, 'x', '"two"'), (3, 'x', '3.5'), (4, 'y', 'null'), (5, 'y', '{"a": {"b": true}}')
                    AS t(id, p, j)
                """);
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id = 2");
            LocalSpark.Run("sql", table, "ALTER TABLE {table} RENAME COLUMN v TO w");
            Assert.Contains(Kaminari.Seseragi.Lakehouse.Delta.Snapshot.Load(new Kaminari.Seseragi.Lakehouse.Engine.LocalFileSystem(table)).Files,
                f => f.DeletionVector is not null);

            string spark = string.Join(" ", LocalSpark.Run("query", table, "SELECT id, p, to_json(w) AS w FROM {table} ORDER BY id")[0]
                .RootElement.GetProperty("rows").EnumerateArray().Select(r => $"{r.GetProperty("id")}|{r.GetProperty("p").GetString()}|{r.GetProperty("w").GetString()}"));
            using DataFrame json = DataFrame.ReadDelta(table, variantAsJson: true).Sort("id");
            int[] ids = json["id"].ToArray<int>();
            string[] parts = json["p"].ToArray<string>()!;
            string?[] values = json["w"].ToArray<string?>();
            Assert.Equal(spark, string.Join(" ", ids.Select((id, i) => $"{id}|{parts[i]}|{values[i]}")));

            // As stored too, with a row index (the deletion vector's rows dropped by position).
            using DataFrame raw = DataFrame.ReadDelta(table, rowIndexName: "row").Sort("id");
            Assert.Equal([1, 3, 4, 5], raw["id"].ToArray<int>());
            Assert.Contains("row", raw.Columns);
            Assert.Contains("w", raw.Columns);
        });
    }
}
