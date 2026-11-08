using LinqToDB;
using LinqToDB.Data;
using Polars.Tpcds;

namespace Polars.Benchmarks.Tpcds;

/// <summary>
/// A large result for measuring materialization: store_sales rows with ss_quantity &lt;= 20 joined to their item
/// (about a fifth of store_sales, 1.15M rows at SF 2), ten columns of integers, decimals and strings, unordered.
/// TPC-DS queries mostly end in LIMIT 100, so they hardly exercise turning a result into objects.
/// </summary>
public sealed class WideRow
{
    public long ss_ticket_number { get; init; }
    public long ss_item_sk { get; init; }
    public long? ss_customer_sk { get; init; }
    public long? ss_sold_date_sk { get; init; }
    public long? ss_quantity { get; init; }
    public decimal? ss_sales_price { get; init; }
    public decimal? ss_ext_sales_price { get; init; }
    public decimal? ss_net_profit { get; init; }
    public string? i_item_id { get; init; }
    public string? i_category { get; init; }
}

internal static class WideResult
{
    public static IQueryable<WideRow> Polars() =>
        from ss in TpcdsData.StoreSalesQuery()
        join i in TpcdsData.ItemQuery() on ss.ss_item_sk equals i.i_item_sk
        where ss.ss_quantity <= 20
        select new WideRow
        {
            ss_ticket_number = ss.ss_ticket_number,
            ss_item_sk = ss.ss_item_sk,
            ss_customer_sk = ss.ss_customer_sk,
            ss_sold_date_sk = ss.ss_sold_date_sk,
            ss_quantity = ss.ss_quantity,
            ss_sales_price = ss.ss_sales_price,
            ss_ext_sales_price = ss.ss_ext_sales_price,
            ss_net_profit = ss.ss_net_profit,
            i_item_id = i.i_item_id,
            i_category = i.i_category,
        };

    public static string Sql() => $"""
        SELECT ss_ticket_number, ss_item_sk, ss_customer_sk, ss_sold_date_sk, ss_quantity,
               ss_sales_price, ss_ext_sales_price, ss_net_profit, i_item_id, i_category
        FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales
        JOIN read_parquet('{TpcdsData.ItemPath}') item ON ss_item_sk = i_item_sk
        WHERE ss_quantity <= 20
        """;

    public static IQueryable<WideRow> Linq2Db(DataConnection db) =>
        from ss in db.GetTable<StoreSales>()
        join i in db.GetTable<Item>() on ss.ss_item_sk equals i.i_item_sk
        where ss.ss_quantity <= 20
        select new WideRow
        {
            ss_ticket_number = ss.ss_ticket_number,
            ss_item_sk = ss.ss_item_sk,
            ss_customer_sk = ss.ss_customer_sk,
            ss_sold_date_sk = ss.ss_sold_date_sk,
            ss_quantity = ss.ss_quantity,
            ss_sales_price = ss.ss_sales_price,
            ss_ext_sales_price = ss.ss_ext_sales_price,
            ss_net_profit = ss.ss_net_profit,
            i_item_id = i.i_item_id,
            i_category = i.i_category,
        };
}
