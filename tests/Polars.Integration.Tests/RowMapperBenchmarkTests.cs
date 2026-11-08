using System.Diagnostics;
using DuckDB.NET.Data;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Core.Data;
using Xunit.Abstractions;

namespace Polars.Integration.Tests;

public class RowMapperBenchmarkTests(ITestOutputHelper output)
{
    public record Customer(
        long c_customer_sk,
        string? c_customer_id,
        long? c_current_cdemo_sk,
        long? c_current_hdemo_sk,
        long? c_current_addr_sk,
        long? c_first_shipto_date_sk,
        long? c_first_sales_date_sk,
        string? c_salutation,
        string? c_first_name,
        string? c_last_name,
        string? c_preferred_cust_flag,
        long? c_birth_day,
        long? c_birth_month,
        long? c_birth_year,
        string? c_birth_country,
        string? c_login,
        string? c_email_address,
        int? c_last_review_date_sk
    );

    public record StoreSales(
        long? ss_sold_date_sk,
        long? ss_sold_time_sk,
        long ss_item_sk,
        long? ss_customer_sk,
        long? ss_cdemo_sk,
        long? ss_hdemo_sk,
        long? ss_addr_sk,
        long? ss_store_sk,
        long? ss_promo_sk,
        long ss_ticket_number,
        long? ss_quantity,
        decimal? ss_wholesale_cost,
        decimal? ss_list_price,
        decimal? ss_sales_price,
        decimal? ss_ext_discount_amt,
        decimal? ss_ext_sales_price,
        decimal? ss_ext_wholesale_cost,
        decimal? ss_ext_list_price,
        decimal? ss_ext_tax,
        decimal? ss_coupon_amt,
        decimal? ss_net_paid,
        decimal? ss_net_paid_inc_tax,
        decimal? ss_net_profit
    );

    private static string GetTpcdsPath(string fileName)
    {
        string path = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../Polars.Tpcds.Tests/Data", fileName));
        if (!File.Exists(path))
            throw new FileNotFoundException($"TPC-DS parquet file not found: {path}");
        return path;
    }

    [Fact]
    public async Task Benchmark_Tpcds_Parquet_Scan_And_ToList()
    {
        string customerPath = GetTpcdsPath("customer.parquet");
        string storeSalesPath = GetTpcdsPath("store_sales.parquet");

        output.WriteLine("=== Warmup ===");
        using (var warmupDf = LazyFrame.ScanParquet(customerPath).Head(100).Collect())
        {
            _ = warmupDf.AsEnumerable<Customer>().ToList();
        }
        using (var warmupSalesDf = LazyFrame.ScanParquet(storeSalesPath).Head(100).Collect())
        {
            _ = warmupSalesDf.AsEnumerable<StoreSales>().ToList();
        }

        // -------------------------------------------------------------------------
        // 1. TPC-DS customer.parquet (50,000 rows, 18 columns: strings + nullable ints)
        // -------------------------------------------------------------------------
        output.WriteLine("\n=== Dataset 1: TPC-DS customer.parquet (50,000 rows, 18 columns: strings + ints) ===");

        // 1A. Legacy per-row, per-column P/Invoke (simulated on customer.parquet)
        {
            using var df = LazyFrame.ScanParquet(customerPath).Collect();
            int height = (int)df.Height;
            var sw = Stopwatch.StartNew();
            var s0 = df[0]; var s1 = df[1]; var s2 = df[2]; var s3 = df[3];
            var s4 = df[4]; var s5 = df[5]; var s6 = df[6]; var s7 = df[7];
            var s8 = df[8]; var s9 = df[9]; var s10 = df[10]; var s11 = df[11];
            var s12 = df[12]; var s13 = df[13]; var s14 = df[14]; var s15 = df[15];
            var s16 = df[16]; var s17 = df[17];

            var list = new List<Customer>(height);
            for (int i = 0; i < height; i++)
            {
                list.Add(new Customer(
                    s0.GetValue<long>(i),
                    s1.GetValue<string?>(i),
                    s2.GetValue<long?>(i),
                    s3.GetValue<long?>(i),
                    s4.GetValue<long?>(i),
                    s5.GetValue<long?>(i),
                    s6.GetValue<long?>(i),
                    s7.GetValue<string?>(i),
                    s8.GetValue<string?>(i),
                    s9.GetValue<string?>(i),
                    s10.GetValue<string?>(i),
                    s11.GetValue<long?>(i),
                    s12.GetValue<long?>(i),
                    s13.GetValue<long?>(i),
                    s14.GetValue<string?>(i),
                    s15.GetValue<string?>(i),
                    s16.GetValue<string?>(i),
                    s17.GetValue<int?>(i)
                ));
            }
            sw.Stop();
            output.WriteLine($"[Old Polars RowMapper (Per-Row P/Invoke, materialize only)] {list.Count:N0} rows in {sw.Elapsed.TotalMilliseconds:F2} ms");
        }

        // 1B. New Polars ScanParquet + Collect + ArrowRowMapper (via ToArrow / rechunk_to_arrow)
        {
            var swTotal = Stopwatch.StartNew();
            using var df = LazyFrame.ScanParquet(customerPath).Collect();
            var swCollect = swTotal.Elapsed;

            var swExport = Stopwatch.StartNew();
            using var batch = df.ToArrow();
            swExport.Stop();

            var swMap = Stopwatch.StartNew();
            var list = new List<Customer>(batch.Length);
            using (var cursor = ArrowRowMapper<Customer>.CreateCursor(batch, ownsBatch: false))
            {
                for (int i = 0; i < cursor.Length; i++)
                    list.Add(cursor.ReadRow(i));
            }
            swMap.Stop();
            swTotal.Stop();

            output.WriteLine($"[New Polars ToArrow (with rechunk)] Total: {swTotal.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows) " +
                             $"(Scan+Collect: {swCollect.TotalMilliseconds:F2} ms, ToArrow/Rechunk: {swExport.Elapsed.TotalMilliseconds:F2} ms, Object Hydration: {swMap.Elapsed.TotalMilliseconds:F2} ms)");
        }

        // 1B-Stream. New Polars ScanParquet + Collect + ToArrowStream (chunk-by-chunk, NO rechunk!)
        {
            var swTotal = Stopwatch.StartNew();
            using var df = LazyFrame.ScanParquet(customerPath).Collect();
            var swCollect = swTotal.Elapsed;

            var swMap = Stopwatch.StartNew();
            var list = new List<Customer>((int)df.Height);
            int chunks = 0;
            using (var stream = df.ToArrowStream())
            {
                Apache.Arrow.RecordBatch? batch;
                while ((batch = await stream.ReadNextRecordBatchAsync()) != null)
                {
                    chunks++;
                    using var cursor = ArrowRowMapper<Customer>.CreateCursor(batch, ownsBatch: true);
                    for (int i = 0; i < cursor.Length; i++)
                        list.Add(cursor.ReadRow(i));
                }
            }
            swMap.Stop();
            swTotal.Stop();

            output.WriteLine($"[New Polars ToArrowStream ({chunks} chunks, no rechunk)] Total: {swTotal.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows) " +
                             $"(Scan+Collect: {swCollect.TotalMilliseconds:F2} ms, Stream+Hydration: {swMap.Elapsed.TotalMilliseconds:F2} ms)");
        }

        // 1C. End-to-End Polars LINQ: LazyFrame.ScanParquet(path).AsQueryable<Customer>().ToList()
        {
            var sw = Stopwatch.StartNew();
            var list = LazyFrame.ScanParquet(customerPath).AsQueryable<Customer>().ToList();
            sw.Stop();
            output.WriteLine($"[New Polars LINQ AsQueryable<Customer>().ToList()] Total: {sw.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows)");
        }

        // 1D. DuckDB.NET Scan Parquet + Hand-written DbDataReader Mapper
        {
            using var conn = new DuckDBConnection("Data Source=:memory:");
            conn.Open();
            // Warmup DuckDB
            using (var warmCmd = conn.CreateCommand())
            {
                warmCmd.CommandText = $"SELECT * FROM read_parquet('{customerPath}') LIMIT 10";
                using var r = warmCmd.ExecuteReader();
                while (r.Read()) { }
            }

            var sw = Stopwatch.StartNew();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM read_parquet('{customerPath}')";
            using var reader = cmd.ExecuteReader();
            var list = new List<Customer>(50000);
            while (reader.Read())
            {
                list.Add(new Customer(
                    reader.GetInt64(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetInt64(3),
                    reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    reader.IsDBNull(6) ? null : reader.GetInt64(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    reader.IsDBNull(9) ? null : reader.GetString(9),
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetInt64(11),
                    reader.IsDBNull(12) ? null : reader.GetInt64(12),
                    reader.IsDBNull(13) ? null : reader.GetInt64(13),
                    reader.IsDBNull(14) ? null : reader.GetString(14),
                    reader.IsDBNull(15) ? null : reader.GetString(15),
                    reader.IsDBNull(16) ? null : reader.GetString(16),
                    reader.IsDBNull(17) ? null : reader.GetInt32(17)
                ));
            }
            sw.Stop();
            output.WriteLine($"[DuckDB.NET read_parquet + DbDataReader] Total: {sw.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows)");

            // Pure in-memory table -> List<Customer> in DuckDB.NET
            using (var createCmd = conn.CreateCommand())
            {
                createCmd.CommandText = $"CREATE TABLE mem_customer AS SELECT * FROM read_parquet('{customerPath}')";
                createCmd.ExecuteNonQuery();
            }
            var swMem = Stopwatch.StartNew();
            using var memCmd = conn.CreateCommand();
            memCmd.CommandText = "SELECT * FROM mem_customer";
            using var memReader = memCmd.ExecuteReader();
            var memList = new List<Customer>(10000);
            while (memReader.Read())
            {
                memList.Add(new Customer(
                    memReader.GetInt64(0),
                    memReader.IsDBNull(1) ? null : memReader.GetString(1),
                    memReader.IsDBNull(2) ? null : memReader.GetInt64(2),
                    memReader.IsDBNull(3) ? null : memReader.GetInt64(3),
                    memReader.IsDBNull(4) ? null : memReader.GetInt64(4),
                    memReader.IsDBNull(5) ? null : memReader.GetInt64(5),
                    memReader.IsDBNull(6) ? null : memReader.GetInt64(6),
                    memReader.IsDBNull(7) ? null : memReader.GetString(7),
                    memReader.IsDBNull(8) ? null : memReader.GetString(8),
                    memReader.IsDBNull(9) ? null : memReader.GetString(9),
                    memReader.IsDBNull(10) ? null : memReader.GetString(10),
                    memReader.IsDBNull(11) ? null : memReader.GetInt64(11),
                    memReader.IsDBNull(12) ? null : memReader.GetInt64(12),
                    memReader.IsDBNull(13) ? null : memReader.GetInt64(13),
                    memReader.IsDBNull(14) ? null : memReader.GetString(14),
                    memReader.IsDBNull(15) ? null : memReader.GetString(15),
                    memReader.IsDBNull(16) ? null : memReader.GetString(16),
                    memReader.IsDBNull(17) ? null : memReader.GetInt32(17)
                ));
            }
            swMem.Stop();
            output.WriteLine($"[DuckDB.NET In-Memory Table -> List<Customer> (Hydration only)] {swMem.Elapsed.TotalMilliseconds:F2} ms ({memList.Count:N0} rows)");
        }

        // -------------------------------------------------------------------------
        // 2. TPC-DS store_sales.parquet (288,040 rows, 23 columns: long? + decimal?)
        // -------------------------------------------------------------------------
        output.WriteLine("\n=== Dataset 2: TPC-DS store_sales.parquet (288,040 rows, 23 columns: long? + decimal?) ===");

        // 2A. Legacy per-row, per-column P/Invoke on store_sales.parquet
        {
            using var df = LazyFrame.ScanParquet(storeSalesPath).Collect();
            int height = (int)df.Height;
            var sw = Stopwatch.StartNew();
            var s0 = df[0]; var s1 = df[1]; var s2 = df[2]; var s3 = df[3];
            var s4 = df[4]; var s5 = df[5]; var s6 = df[6]; var s7 = df[7];
            var s8 = df[8]; var s9 = df[9]; var s10 = df[10]; var s11 = df[11];
            var s12 = df[12]; var s13 = df[13]; var s14 = df[14]; var s15 = df[15];
            var s16 = df[16]; var s17 = df[17]; var s18 = df[18]; var s19 = df[19];
            var s20 = df[20]; var s21 = df[21]; var s22 = df[22];

            var list = new List<StoreSales>(height);
            for (int i = 0; i < height; i++)
            {
                list.Add(new StoreSales(
                    s0.GetValue<long?>(i), s1.GetValue<long?>(i), s2.GetValue<long>(i), s3.GetValue<long?>(i),
                    s4.GetValue<long?>(i), s5.GetValue<long?>(i), s6.GetValue<long?>(i), s7.GetValue<long?>(i),
                    s8.GetValue<long?>(i), s9.GetValue<long>(i), s10.GetValue<long?>(i), s11.GetValue<decimal?>(i),
                    s12.GetValue<decimal?>(i), s13.GetValue<decimal?>(i), s14.GetValue<decimal?>(i), s15.GetValue<decimal?>(i),
                    s16.GetValue<decimal?>(i), s17.GetValue<decimal?>(i), s18.GetValue<decimal?>(i), s19.GetValue<decimal?>(i),
                    s20.GetValue<decimal?>(i), s21.GetValue<decimal?>(i), s22.GetValue<decimal?>(i)
                ));
            }
            sw.Stop();
            output.WriteLine($"[Old Polars RowMapper (Per-Row P/Invoke, materialize only)] {list.Count:N0} rows in {sw.Elapsed.TotalMilliseconds:F2} ms");
        }

        // 2B. New Polars ScanParquet + Collect + ArrowRowMapper (via ToArrow / rechunk_to_arrow)
        {
            var swTotal = Stopwatch.StartNew();
            using var df = LazyFrame.ScanParquet(storeSalesPath).Collect();
            var swCollect = swTotal.Elapsed;

            var swExport = Stopwatch.StartNew();
            using var batch = df.ToArrow();
            swExport.Stop();

            var swMap = Stopwatch.StartNew();
            var list = new List<StoreSales>(batch.Length);
            using (var cursor = ArrowRowMapper<StoreSales>.CreateCursor(batch, ownsBatch: false))
            {
                for (int i = 0; i < cursor.Length; i++)
                    list.Add(cursor.ReadRow(i));
            }
            swMap.Stop();
            swTotal.Stop();

            output.WriteLine($"[New Polars ToArrow (with rechunk)] Total: {swTotal.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows) " +
                             $"(Scan+Collect: {swCollect.TotalMilliseconds:F2} ms, ToArrow/Rechunk: {swExport.Elapsed.TotalMilliseconds:F2} ms, Object Hydration: {swMap.Elapsed.TotalMilliseconds:F2} ms)");
        }

        // 2B-Stream. New Polars ScanParquet + Collect + ToArrowStream (chunk-by-chunk, NO rechunk!)
        {
            var swTotal = Stopwatch.StartNew();
            using var df = LazyFrame.ScanParquet(storeSalesPath).Collect();
            var swCollect = swTotal.Elapsed;

            var swMap = Stopwatch.StartNew();
            var list = new List<StoreSales>((int)df.Height);
            int chunks = 0;
            using (var stream = df.ToArrowStream())
            {
                Apache.Arrow.RecordBatch? batch;
                while ((batch = await stream.ReadNextRecordBatchAsync()) != null)
                {
                    chunks++;
                    using var cursor = ArrowRowMapper<StoreSales>.CreateCursor(batch, ownsBatch: true);
                    for (int i = 0; i < cursor.Length; i++)
                        list.Add(cursor.ReadRow(i));
                }
            }
            swMap.Stop();
            swTotal.Stop();

            output.WriteLine($"[New Polars ToArrowStream ({chunks} chunks, no rechunk)] Total: {swTotal.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows) " +
                             $"(Scan+Collect: {swCollect.TotalMilliseconds:F2} ms, Stream+Hydration: {swMap.Elapsed.TotalMilliseconds:F2} ms)");
        }

        // 2C. End-to-End Polars LINQ: LazyFrame.ScanParquet(path).AsQueryable<StoreSales>().ToList()
        {
            var sw = Stopwatch.StartNew();
            var list = LazyFrame.ScanParquet(storeSalesPath).AsQueryable<StoreSales>().ToList();
            sw.Stop();
            output.WriteLine($"[New Polars LINQ AsQueryable<StoreSales>().ToList()] Total: {sw.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows)");
        }

        // 2D. DuckDB.NET Scan Parquet + Hand-written DbDataReader Mapper
        {
            using var conn = new DuckDBConnection("Data Source=:memory:");
            conn.Open();
            using (var warmCmd = conn.CreateCommand())
            {
                warmCmd.CommandText = $"SELECT * FROM read_parquet('{storeSalesPath}') LIMIT 10";
                using var r = warmCmd.ExecuteReader();
                while (r.Read()) { }
            }

            var sw = Stopwatch.StartNew();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM read_parquet('{storeSalesPath}')";
            using var reader = cmd.ExecuteReader();
            var list = new List<StoreSales>(288040);
            while (reader.Read())
            {
                list.Add(new StoreSales(
                    reader.IsDBNull(0) ? null : reader.GetInt64(0),
                    reader.IsDBNull(1) ? null : reader.GetInt64(1),
                    reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetInt64(3),
                    reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    reader.IsDBNull(6) ? null : reader.GetInt64(6),
                    reader.IsDBNull(7) ? null : reader.GetInt64(7),
                    reader.IsDBNull(8) ? null : reader.GetInt64(8),
                    reader.GetInt64(9),
                    reader.IsDBNull(10) ? null : reader.GetInt64(10),
                    reader.IsDBNull(11) ? null : reader.GetDecimal(11),
                    reader.IsDBNull(12) ? null : reader.GetDecimal(12),
                    reader.IsDBNull(13) ? null : reader.GetDecimal(13),
                    reader.IsDBNull(14) ? null : reader.GetDecimal(14),
                    reader.IsDBNull(15) ? null : reader.GetDecimal(15),
                    reader.IsDBNull(16) ? null : reader.GetDecimal(16),
                    reader.IsDBNull(17) ? null : reader.GetDecimal(17),
                    reader.IsDBNull(18) ? null : reader.GetDecimal(18),
                    reader.IsDBNull(19) ? null : reader.GetDecimal(19),
                    reader.IsDBNull(20) ? null : reader.GetDecimal(20),
                    reader.IsDBNull(21) ? null : reader.GetDecimal(21),
                    reader.IsDBNull(22) ? null : reader.GetDecimal(22)
                ));
            }
            sw.Stop();
            output.WriteLine($"[DuckDB.NET read_parquet + DbDataReader (StoreSales)] Total: {sw.Elapsed.TotalMilliseconds:F2} ms ({list.Count:N0} rows)");

            // Pure in-memory table -> List<StoreSales> in DuckDB.NET
            using (var createCmd = conn.CreateCommand())
            {
                createCmd.CommandText = $"CREATE TABLE mem_store_sales AS SELECT * FROM read_parquet('{storeSalesPath}')";
                createCmd.ExecuteNonQuery();
            }
            var swMem = Stopwatch.StartNew();
            using var memCmd = conn.CreateCommand();
            memCmd.CommandText = "SELECT * FROM mem_store_sales";
            using var memReader = memCmd.ExecuteReader();
            var memList = new List<StoreSales>(288464);
            while (memReader.Read())
            {
                memList.Add(new StoreSales(
                    memReader.IsDBNull(0) ? null : memReader.GetInt64(0),
                    memReader.IsDBNull(1) ? null : memReader.GetInt64(1),
                    memReader.GetInt64(2),
                    memReader.IsDBNull(3) ? null : memReader.GetInt64(3),
                    memReader.IsDBNull(4) ? null : memReader.GetInt64(4),
                    memReader.IsDBNull(5) ? null : memReader.GetInt64(5),
                    memReader.IsDBNull(6) ? null : memReader.GetInt64(6),
                    memReader.IsDBNull(7) ? null : memReader.GetInt64(7),
                    memReader.IsDBNull(8) ? null : memReader.GetInt64(8),
                    memReader.GetInt64(9),
                    memReader.IsDBNull(10) ? null : memReader.GetInt64(10),
                    memReader.IsDBNull(11) ? null : memReader.GetDecimal(11),
                    memReader.IsDBNull(12) ? null : memReader.GetDecimal(12),
                    memReader.IsDBNull(13) ? null : memReader.GetDecimal(13),
                    memReader.IsDBNull(14) ? null : memReader.GetDecimal(14),
                    memReader.IsDBNull(15) ? null : memReader.GetDecimal(15),
                    memReader.IsDBNull(16) ? null : memReader.GetDecimal(16),
                    memReader.IsDBNull(17) ? null : memReader.GetDecimal(17),
                    memReader.IsDBNull(18) ? null : memReader.GetDecimal(18),
                    memReader.IsDBNull(19) ? null : memReader.GetDecimal(19),
                    memReader.IsDBNull(20) ? null : memReader.GetDecimal(20),
                    memReader.IsDBNull(21) ? null : memReader.GetDecimal(21),
                    memReader.IsDBNull(22) ? null : memReader.GetDecimal(22)
                ));
            }
            swMem.Stop();
            output.WriteLine($"[DuckDB.NET In-Memory Table -> List<StoreSales> (Hydration only)] {swMem.Elapsed.TotalMilliseconds:F2} ms ({memList.Count:N0} rows)");
        }
    }
}
