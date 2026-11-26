using System.Text.Json;
using Polars.CSharp;

namespace Polars.Integration.Tests;

/// <summary>
/// Column defaults Spark declared (DEFAULT, feature allowColumnDefaults): rows we append without those columns take
/// them as Spark gives them, and Spark writes after us.
/// </summary>
public class SparkColumnDefaultTests
{
    private sealed record Row(int a);

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Defaults_Spark_declared_fill_the_columns_our_rows_lack()
    {
        string table = Path.Combine(Path.GetTempPath(), $"seseragi_default_{Guid.NewGuid():N}");
        try
        {
            LocalSpark.Run("sql", table, """
                CREATE TABLE {table} (a INT, b BIGINT DEFAULT 42, s STRING DEFAULT 'x' || "y", d DATE DEFAULT DATE '2020-02-02',
                    e DECIMAL(5,2) DEFAULT 1.5, ts TIMESTAMP DEFAULT current_timestamp()) USING delta
                    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
                """);
            long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
            using (DataFrame rows = DataFrame.From([new Row(1), new Row(2)]))
                rows.WriteDelta(table, mode: DeltaSaveMode.Append);
            LocalSpark.Run("sql", table, "INSERT INTO {table} (a) VALUES (3)");

            // 2020-02-02 is day 18294.
            JsonElement[] read = [.. LocalSpark.Run("read", table)[0].RootElement.GetProperty("rows").EnumerateArray()];
            Assert.Equal(["1|42|xy|18294|1.50", "2|42|xy|18294|1.50", "3|42|xy|18294|1.50"],
                read.Select(r => $"{r.GetProperty("a")}|{r.GetProperty("b")}|{r.GetProperty("s")}|{r.GetProperty("d")}|{r.GetProperty("e")}"));
            long ours = read[0].GetProperty("ts").GetInt64();
            Assert.Equal(ours, read[1].GetProperty("ts").GetInt64());
            Assert.InRange(ours, before - 1000, read[2].GetProperty("ts").GetInt64());
        }
        finally
        {
            if (Directory.Exists(table)) Directory.Delete(table, true);
        }
    }
}
