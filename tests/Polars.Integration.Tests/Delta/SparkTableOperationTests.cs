using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.CSharp;
using static Polars.CSharp.Polars;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// ADD COLUMNS, UNSET TBLPROPERTIES, DESCRIBE DETAIL, REPLACE TABLE, SHALLOW CLONE and CONVERT TO DELTA through the
/// public API, against Spark: what we commit Spark reads and writes on, what Spark commits we read, and the same
/// directory converted by both gives the same table.
/// </summary>
public class SparkTableOperationTests
{
    private sealed record Row(int id, string? s);
    private sealed record Wide(int id, string? s, long? n);

    private static void InDirectory(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_ops_{Guid.NewGuid():N}");
        try
        {
            body(root);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    private static void Append<T>(string table, params T[] rows)
    {
        using DataFrame df = DataFrame.From(rows);
        df.WriteDelta(table, mode: DeltaSaveMode.Append, mkdir: true);
    }

    private static List<JsonElement> Query(string table, string sql) =>
        [.. LocalSpark.Run("query", table, sql)[0].RootElement.GetProperty("rows").EnumerateArray().Select(r => r.Clone())];

    private static string Rows(string table, string columns = "id, s") =>
        string.Join(" ", Query(table, $"SELECT {columns} FROM {{table}} ORDER BY id").Select(r => r.GetRawText()));

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void What_we_alter_replace_and_clone_Spark_reads_and_writes_on()
    {
        InDirectory(root =>
        {
            string table = Path.Combine(root, "t");
            Delta.CreateTable(table, PolarsSchema.From<Row>(), properties: new()
            {
                ["delta.columnMapping.mode"] = "name", ["delta.enableDeletionVectors"] = "true", ["foo"] = "1",
            });
            Append(table, new Row(1, "a"), new Row(2, "b"));
            Delta.AddColumns(table, [new DeltaNewColumn("n", DataType.Int64, Comment: "count", After: "id")]);
            Delta.UnsetTableProperties(table, ["foo"]);
            LocalSpark.Run("sql", table, "INSERT INTO {table} (id, n, s) VALUES (3, 30, 'c')");
            Append(table, new Wide(4, "d", 40));
            Assert.Equal("""{"id": 1, "n": null, "s": "a"} {"id": 2, "n": null, "s": "b"} {"id": 3, "n": 30, "s": "c"} {"id": 4, "n": 40, "s": "d"}""",
                Rows(table, "id, n, s"));

            // DESCRIBE DETAIL agrees with ours.
            JsonElement spark = Query(table, "DESCRIBE DETAIL {table}").Single();
            DeltaTableDetail ours = Delta.Detail(table);
            Assert.Equal(spark.GetProperty("id").GetString(), ours.Id);
            Assert.Equal(spark.GetProperty("numFiles").GetInt64(), ours.NumFiles);
            Assert.Equal(spark.GetProperty("sizeInBytes").GetInt64(), ours.SizeInBytes);
            Assert.Equal(spark.GetProperty("tableFeatures").EnumerateArray().Select(f => f.GetString()!).Order(StringComparer.Ordinal), ours.TableFeatures);
            Assert.Equal(spark.GetProperty("properties").EnumerateArray().Select(p => p.GetProperty("key").GetString()!).Order(StringComparer.Ordinal),
                ours.Properties.Keys.Order(StringComparer.Ordinal));

            // A clone Spark reads and writes on; the source is left alone.
            string clone = Path.Combine(root, "clone");
            Delta.Delete(table, Col("id") == 2);
            Delta.Clone(table, clone);
            Assert.Equal(Rows(table), Rows(clone));
            LocalSpark.Run("sql", clone, "DELETE FROM {table} WHERE id = 1");
            LocalSpark.Run("sql", clone, "INSERT INTO {table} (id, n, s) VALUES (9, 90, 'z')");
            Assert.Equal("""{"id": 3, "s": "c"} {"id": 4, "s": "d"} {"id": 9, "s": "z"}""", Rows(clone));
            Assert.Equal("""{"id": 1, "s": "a"} {"id": 3, "s": "c"} {"id": 4, "s": "d"}""", Rows(table));

            // Replaced, the table reads the new definition in Spark, and Spark time-travels to the old one.
            Delta.ReplaceTable(table, PolarsSchema.From<Row>(), partitionBy: ["s"]);
            Assert.Equal("", Rows(table));
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (5, 'e')");
            Assert.Equal("""{"id": 5, "s": "e"}""", Rows(table));
            Assert.Equal(4, Query(table, "SELECT * FROM {table} VERSION AS OF 5").Count);
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void A_clone_Spark_made_we_read_and_write_on()
    {
        InDirectory(root =>
        {
            string source = Path.Combine(root, "src"), clone = Path.Combine(root, "clone");
            LocalSpark.Run("sql", source, "CREATE TABLE {table} (id INT, s STRING) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')");
            LocalSpark.Run("sql", source, "INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            LocalSpark.Run("sql", source, "DELETE FROM {table} WHERE id = 2");
            LocalSpark.Run("query", clone, $"CREATE TABLE {{table}} SHALLOW CLONE delta.`{source}`");

            using (DataFrame read = DataFrame.ReadDelta(clone).Sort("id"))
                Assert.Equal([1, 3], read["id"].ToArray<int>());
            Append(clone, new Row(4, "d"));
            Delta.Delete(clone, Col("id") == 1);
            Assert.Equal("""{"id": 3, "s": "c"} {"id": 4, "s": "d"}""", Rows(clone));
            Assert.Equal(2, Delta.Detail(clone).Version);
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void A_directory_converted_by_us_is_the_table_Spark_converts_it_into()
    {
        InDirectory(root =>
        {
            string written = Path.Combine(root, "written");
            string name = "parquet_" + Guid.NewGuid().ToString("N");
            LocalSpark.Run("query", written, $"""
                CREATE TABLE {name} USING parquet PARTITIONED BY (p) LOCATION '{written}' AS
                SELECT id, CAST(id AS STRING) AS s, map('k', id) AS m, array(id, NULL) AS a, named_struct('x', id) AS st,
                       TIMESTAMP '2021-03-05 04:00:00' + make_interval(0, 0, 0, 0, 0, 0, id) AS ts, CAST(id AS DECIMAL(5, 2)) AS d,
                       CASE WHEN id = 3 THEN NULL ELSE CONCAT('a b/', id % 2) END AS p
                FROM range(4)
                """);
            LocalSpark.Run("query", written, $"DROP TABLE {name}"); // the files stay: the table is external
            string bySpark = Path.Combine(root, "bySpark"), byUs = Path.Combine(root, "byUs");
            Copy(written, bySpark);
            Copy(written, byUs);
            LocalSpark.Run("query", bySpark, "CONVERT TO DELTA parquet.`" + bySpark + "` PARTITIONED BY (p STRING)");
            Delta.ConvertToDelta(byUs, partitionBy: PolarsSchema.From([("p", DataType.String)]));

            Snapshot spark = Snapshot.Load(new LocalFileSystem(bySpark)), ours = Snapshot.Load(new LocalFileSystem(byUs));
            Assert.Equal(spark.Metadata.SchemaString, ours.Metadata.SchemaString);
            Assert.Equal(spark.PartitionColumns, ours.PartitionColumns);
            Assert.Equal(
                spark.Files.Select(f => (f.Path, string.Join(",", f.PartitionValues.Select(kv => $"{kv.Key}={kv.Value}")))).Order(),
                ours.Files.Select(f => (f.Path, string.Join(",", f.PartitionValues.Select(kv => $"{kv.Key}={kv.Value}")))).Order());
            Assert.Equal(spark.Files.Sum(f => f.ParsedStats!.NumRecords), ours.Files.Sum(f => f.ParsedStats!.NumRecords));
            Assert.Equal(Rows(bySpark, "id, s, m, a, st, ts, d, p"), Rows(byUs, "id, s, m, a, st, ts, d, p"));
        });

        static void Copy(string from, string to)
        {
            foreach (string file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
            {
                string target = Path.Combine(to, Path.GetRelativePath(from, file));
                Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                File.Copy(file, target);
            }
        }
    }
}
