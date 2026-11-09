using System.Data;
using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;
using Kaminari.Sazanami.Data;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.Benchmarks.Tpcds;
using Polars.Tpcds;

namespace Polars.Benchmarks;

// A whole TPC-DS table materialized as a List<T>, through each path from Parquet to objects.
//
// Filter: --filter '*RowMapper*'
// Data: the TPC-DS benchmark data set (see TpcdsBenchEnvironment).

public record RowMapperCustomer(
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

public record RowMapperStoreSales(
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

/// <remarks>
/// Each invocation materializes a whole table, so the job runs one invocation per iteration. Per-row P/Invoke costs
/// one native call per cell: it reads only the first <see cref="PerRowSampleRows"/> rows, and is not comparable
/// with the other methods in absolute terms.
/// </remarks>
[MemoryDiagnoser]
[WarmupCount(1)]
[IterationCount(5)]
[InvocationCount(1)]
public abstract class RowMapperBenchmarks<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> where T : class
{
    private const int PerRowSampleRows = 100_000;

    private string _path = "";
    private DuckDBConnection _duckDb = null!;
    private DataFrame _perRowSample = null!;

    protected abstract string Table { get; }
    protected abstract T ReadReaderRow(IDataRecord reader);
    protected abstract T ReadSeriesRow(Series[] columns, int row);

    [GlobalSetup]
    public void Setup()
    {
        TpcdsBenchEnvironment.UseBenchmarkData();
        _path = Path.Combine(TpcdsData.DataDir, Table + ".parquet");

        _perRowSample = LazyFrame.ScanParquet(_path).Head(PerRowSampleRows).Collect();

        _duckDb = new DuckDBConnection("Data Source=:memory:");
        _duckDb.Open();
        using var create = _duckDb.CreateCommand();
        create.CommandText = $"CREATE TABLE mem AS SELECT * FROM read_parquet('{_path.Replace("'", "''")}')";
        create.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _perRowSample.Dispose();
        _duckDb.Dispose();
    }

    private int ReadDuckDb(string sql)
    {
        using var command = _duckDb.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var rows = new List<T>();
        while (reader.Read())
        {
            rows.Add(ReadReaderRow(reader));
        }
        return rows.Count;
    }

    [Benchmark(Description = "Per-row P/Invoke (sample)")]
    public int PerRow_PInvoke_Sample()
    {
        int height = (int)_perRowSample.Height;
        var columns = new Series[_perRowSample.Width];
        for (int c = 0; c < columns.Length; c++)
        {
            columns[c] = _perRowSample[c];
        }

        var rows = new List<T>(height);
        for (int i = 0; i < height; i++)
        {
            rows.Add(ReadSeriesRow(columns, i));
        }
        return rows.Count;
    }

    [Benchmark(Description = "ToArrow + row mapper")]
    public int ToArrow_RowMapper()
    {
        using var df = LazyFrame.ScanParquet(_path).Collect();
        using var batch = df.ToArrow();
        var rows = new List<T>(batch.Length);
        using var cursor = ArrowRowMapper<T>.CreateCursor(batch, ownsBatch: false);
        for (int i = 0; i < cursor.Length; i++)
        {
            rows.Add(cursor.ReadRow(i));
        }
        return rows.Count;
    }

    [Benchmark(Description = "ToArrowStream + row mapper")]
    public async Task<int> ToArrowStream_RowMapper()
    {
        using var df = LazyFrame.ScanParquet(_path).Collect();
        var rows = new List<T>((int)df.Height);
        using var stream = df.ToArrowStream();
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            using var cursor = ArrowRowMapper<T>.CreateCursor(batch, ownsBatch: true);
            for (int i = 0; i < cursor.Length; i++)
            {
                rows.Add(cursor.ReadRow(i));
            }
        }
        return rows.Count;
    }

    [Benchmark(Baseline = true, Description = "LINQ ToList")]
    public int Linq_ToList() => LazyFrame.ScanParquet(_path).AsQueryable<T>().ToList().Count;

    [Benchmark(Description = "DuckDB read_parquet + reader")]
    public int DuckDb_ReadParquet() => ReadDuckDb($"SELECT * FROM read_parquet('{_path.Replace("'", "''")}')");

    [Benchmark(Description = "DuckDB table + reader")]
    public int DuckDb_Table() => ReadDuckDb("SELECT * FROM mem");
}

public class RowMapperCustomerBenchmarks : RowMapperBenchmarks<RowMapperCustomer>
{
    protected override string Table => "customer";

    protected override RowMapperCustomer ReadSeriesRow(Series[] s, int i) => new(
        s[0].GetValue<long>(i),
        s[1].GetValue<string?>(i),
        s[2].GetValue<long?>(i),
        s[3].GetValue<long?>(i),
        s[4].GetValue<long?>(i),
        s[5].GetValue<long?>(i),
        s[6].GetValue<long?>(i),
        s[7].GetValue<string?>(i),
        s[8].GetValue<string?>(i),
        s[9].GetValue<string?>(i),
        s[10].GetValue<string?>(i),
        s[11].GetValue<long?>(i),
        s[12].GetValue<long?>(i),
        s[13].GetValue<long?>(i),
        s[14].GetValue<string?>(i),
        s[15].GetValue<string?>(i),
        s[16].GetValue<string?>(i),
        s[17].GetValue<int?>(i));

    protected override RowMapperCustomer ReadReaderRow(IDataRecord r) => new(
        r.GetInt64(0),
        r.IsDBNull(1) ? null : r.GetString(1),
        r.IsDBNull(2) ? null : r.GetInt64(2),
        r.IsDBNull(3) ? null : r.GetInt64(3),
        r.IsDBNull(4) ? null : r.GetInt64(4),
        r.IsDBNull(5) ? null : r.GetInt64(5),
        r.IsDBNull(6) ? null : r.GetInt64(6),
        r.IsDBNull(7) ? null : r.GetString(7),
        r.IsDBNull(8) ? null : r.GetString(8),
        r.IsDBNull(9) ? null : r.GetString(9),
        r.IsDBNull(10) ? null : r.GetString(10),
        r.IsDBNull(11) ? null : r.GetInt64(11),
        r.IsDBNull(12) ? null : r.GetInt64(12),
        r.IsDBNull(13) ? null : r.GetInt64(13),
        r.IsDBNull(14) ? null : r.GetString(14),
        r.IsDBNull(15) ? null : r.GetString(15),
        r.IsDBNull(16) ? null : r.GetString(16),
        r.IsDBNull(17) ? null : r.GetInt32(17));
}

public class RowMapperStoreSalesBenchmarks : RowMapperBenchmarks<RowMapperStoreSales>
{
    protected override string Table => "store_sales";

    protected override RowMapperStoreSales ReadSeriesRow(Series[] s, int i) => new(
        s[0].GetValue<long?>(i), s[1].GetValue<long?>(i), s[2].GetValue<long>(i), s[3].GetValue<long?>(i),
        s[4].GetValue<long?>(i), s[5].GetValue<long?>(i), s[6].GetValue<long?>(i), s[7].GetValue<long?>(i),
        s[8].GetValue<long?>(i), s[9].GetValue<long>(i), s[10].GetValue<long?>(i), s[11].GetValue<decimal?>(i),
        s[12].GetValue<decimal?>(i), s[13].GetValue<decimal?>(i), s[14].GetValue<decimal?>(i), s[15].GetValue<decimal?>(i),
        s[16].GetValue<decimal?>(i), s[17].GetValue<decimal?>(i), s[18].GetValue<decimal?>(i), s[19].GetValue<decimal?>(i),
        s[20].GetValue<decimal?>(i), s[21].GetValue<decimal?>(i), s[22].GetValue<decimal?>(i));

    protected override RowMapperStoreSales ReadReaderRow(IDataRecord r) => new(
        r.IsDBNull(0) ? null : r.GetInt64(0),
        r.IsDBNull(1) ? null : r.GetInt64(1),
        r.GetInt64(2),
        r.IsDBNull(3) ? null : r.GetInt64(3),
        r.IsDBNull(4) ? null : r.GetInt64(4),
        r.IsDBNull(5) ? null : r.GetInt64(5),
        r.IsDBNull(6) ? null : r.GetInt64(6),
        r.IsDBNull(7) ? null : r.GetInt64(7),
        r.IsDBNull(8) ? null : r.GetInt64(8),
        r.GetInt64(9),
        r.IsDBNull(10) ? null : r.GetInt64(10),
        r.IsDBNull(11) ? null : r.GetDecimal(11),
        r.IsDBNull(12) ? null : r.GetDecimal(12),
        r.IsDBNull(13) ? null : r.GetDecimal(13),
        r.IsDBNull(14) ? null : r.GetDecimal(14),
        r.IsDBNull(15) ? null : r.GetDecimal(15),
        r.IsDBNull(16) ? null : r.GetDecimal(16),
        r.IsDBNull(17) ? null : r.GetDecimal(17),
        r.IsDBNull(18) ? null : r.GetDecimal(18),
        r.IsDBNull(19) ? null : r.GetDecimal(19),
        r.IsDBNull(20) ? null : r.GetDecimal(20),
        r.IsDBNull(21) ? null : r.GetDecimal(21),
        r.IsDBNull(22) ? null : r.GetDecimal(22));
}
