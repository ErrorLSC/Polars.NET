using System.Linq.Expressions;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Core.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// LINQ queries over a Spark table leave out the files Spark's statistics rule out, and return what filtering every
/// row returns: column mapping (a renamed column), a partition column, a widened column (older files' statistics in
/// the older type), strings longer than Spark's statistics keep, timestamps finer than their statistics.
/// </summary>
public class SparkFileSkippingTests
{
    public record Row(long id, string p, string name, DateTime ts);

    /// <summary>Four files: batch k holds ids 100k..100k+9, partition even/odd, names of 40 times the letter a+k, times on day k+1 (123 µs apart).</summary>
    private static void CreateTable(string table)
    {
        LocalSpark.Run("sql", table,
            "CREATE TABLE {table} (id INT, p STRING, s STRING, ts TIMESTAMP_NTZ) USING delta PARTITIONED BY (p) " +
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableTypeWidening' = 'true')");
        for (int k = 0; k < 4; k++)
        {
            if (k == 3)
            {
                LocalSpark.Run("sql", table, "ALTER TABLE {table} ALTER COLUMN id TYPE BIGINT");
                LocalSpark.Run("sql", table, "ALTER TABLE {table} RENAME COLUMN s TO name");
            }
            char letter = (char)('a' + k);
            LocalSpark.Run("sql", table, $$"""
                INSERT INTO {table} SELECT /*+ REPARTITION(1) */ CAST({{100 * k}} + id AS INT), '{{(k % 2 == 0 ? "even" : "odd")}}',
                    concat(repeat('{{letter}}', 40), CAST(id AS STRING)),
                    TIMESTAMP_NTZ'2024-01-0{{k + 1}} 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, CAST(id * 0.000123 AS DECIMAL(18, 6)))
                FROM range(10)
                """);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Queries_SkipWhatSparksStatisticsRuleOut_AndKeepEveryMatchingRow()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_skipping_{Guid.NewGuid():N}");
        string table = Path.Combine(root, "table");
        try
        {
            CreateTable(table);
            using var lf = LazyFrame.ScanDelta(table);
            Row[] all = [.. lf.AsQueryable<Row>()];
            Assert.Equal(40, all.Length);

            var counts = new List<int>();
            DeltaKernelScan.ScannedFilesForTests.Value = counts.Add;
            try
            {
                void Check(Expression<Func<Row, bool>> where, int files)
                {
                    counts.Clear();
                    long[] got = [.. lf.AsQueryable<Row>().Where(where).Select(r => r.id).ToList().Order()];
                    long[] expected = [.. all.Where(where.Compile()).Select(r => r.id).Order()];
                    Assert.Equal(expected, got);
                    Assert.True(counts.Count > 0 && counts.All(c => c == files), $"{where}: scanned [{string.Join(", ", counts)}], expected {files}");
                }

                Check(r => r.id >= 300, 1);                              // the widened column: older files' statistics are ints
                Check(r => r.id * 2 > 600, 1);                           // arithmetic over it
                Check(r => r.p == "odd", 2);                             // the partition column
                Check(r => r.p == "odd" && r.id < 200, 1);
                Check(r => r.id < 5 || r.id > 305, 2);
                // Spark cuts both string bounds to 32 characters and ends the maximum with a tie-breaker (U+007F, or
                // U+10FFFF before a non-ASCII character), so it stays an upper bound in code point order.
                Check(r => r.name.StartsWith("ccc"), 1);
                // string.CompareTo compares by culture, not by code point: statistics cannot answer it.
                Check(r => r.name.CompareTo("d") < 0, 4);
                // The maximum time is cut to milliseconds: 2024-01-03 00:00:00.001107 lies above the stored maximum.
                Check(r => r.ts > new DateTime(2024, 1, 3, 0, 0, 0).AddTicks(10_000), 2);
                Check(r => r.ts < new DateTime(2024, 1, 2), 1);
            }
            finally
            {
                DeltaKernelScan.ScannedFilesForTests.Value = null;
            }
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }
}
