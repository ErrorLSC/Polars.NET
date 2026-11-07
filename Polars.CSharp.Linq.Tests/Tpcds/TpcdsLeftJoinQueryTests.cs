#nullable enable
using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Tpcds.Tests;

public class TpcdsLeftJoinQueryTests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 40
     *
     * SELECT w_state,
     *        i_item_id,
     *        sum(CASE
     *                WHEN (cast(d_date AS date) < CAST ('2000-03-11' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash,0)
     *                ELSE 0
     *            END) AS sales_before,
     *        sum(CASE
     *                WHEN (cast(d_date AS date) >= CAST ('2000-03-11' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash,0)
     *                ELSE 0
     *            END) AS sales_after
     * FROM catalog_sales
     * LEFT OUTER JOIN catalog_returns ON (cs_order_number = cr_order_number
     *                                AND cs_item_sk = cr_item_sk),
     *      warehouse,
     *      item,
     *      date_dim
     * WHERE i_current_price BETWEEN 0.99 AND 1.49
     *   AND i_item_sk = cs_item_sk
     *   AND cs_warehouse_sk = w_warehouse_sk
     *   AND cs_sold_date_sk = d_date_sk
     *   AND d_date BETWEEN CAST ('2000-02-10' AS date) AND CAST ('2000-04-10' AS date)
     * GROUP BY w_state,
     *          i_item_id
     * ORDER BY w_state,
     *          i_item_id
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q40")]
    public void Test_Q40()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var pivotDate = new DateOnly(2000, 3, 11);
        var minDate = new DateOnly(2000, 2, 10);
        var maxDate = new DateOnly(2000, 4, 10);

        var filteredItems = itemQuery.Where(i => i.i_current_price >= 0.99m && i.i_current_price <= 1.49m);
        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);

        var q40 = (from cs in catalogSalesQuery
                   join cr in catalogReturnsQuery on new { order_number = cs.cs_order_number, item_sk = cs.cs_item_sk }
                                              equals new { order_number = cr.cr_order_number, item_sk = cr.cr_item_sk } into crGroup
                   from cr in crGroup.DefaultIfEmpty()
                   join w in warehouseQuery on cs.cs_warehouse_sk equals w.w_warehouse_sk
                   join i in filteredItems on cs.cs_item_sk equals i.i_item_sk
                   join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                   group new { cs, cr, d } by new { w.w_state, i.i_item_id } into g
                   orderby g.Key.w_state, g.Key.i_item_id
                   select new
                   {
                       g.Key.w_state,
                       g.Key.i_item_id,
                       sales_before = g.Sum(x => x.d.d_date < pivotDate ? x.cs.cs_sales_price - (x.cr.cr_refunded_cash ?? 0m) : 0m),
                       sales_after = g.Sum(x => x.d.d_date >= pivotDate ? x.cs.cs_sales_price - (x.cr.cr_refunded_cash ?? 0m) : 0m)
                   }).Take(100);

        AssertPlan(q40, NativePushdown);

        var polarsResult = q40.ToList();

        var duckDbResults = Q40Expected();

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.w_state, p.w_state);
            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.sales_before, p.sales_before);
            Assert.Equal(d.sales_after, p.sales_after);
        }
    }

    /// <summary>
    /// Q40 as written in SQL: catalog_sales LEFT OUTER JOIN catalog_returns on (order number, item) through the
    /// .NET 11 tuple LeftJoin. An unmatched sale has a null Inner, so its refund is treated as coalesce(refund, 0).
    /// </summary>
    [Fact]
    [Trait("TPCDS", "Q40")]
    [Trait("LINQ", "Net11_LeftJoin")]
    public void Test_Q40_LeftJoin()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var pivotDate = new DateOnly(2000, 3, 11);
        var minDate = new DateOnly(2000, 2, 10);
        var maxDate = new DateOnly(2000, 4, 10);

        var filteredItems = itemQuery.Where(i => i.i_current_price >= 0.99m && i.i_current_price <= 1.49m);
        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);

        var q40 = (from pair in catalogSalesQuery.LeftJoin(catalogReturnsQuery,
                        cs => new { order = cs.cs_order_number, item = cs.cs_item_sk },
                        cr => new { order = cr.cr_order_number, item = cr.cr_item_sk })
                   join w in warehouseQuery on pair.Outer.cs_warehouse_sk equals w.w_warehouse_sk
                   join i in filteredItems on pair.Outer.cs_item_sk equals i.i_item_sk
                   join d in filteredDates on pair.Outer.cs_sold_date_sk equals d.d_date_sk
                   let refunded = pair.Inner != null ? pair.Inner.cr_refunded_cash : null
                   group new { cs = pair.Outer, refunded, d } by new { w.w_state, i.i_item_id } into g
                   orderby g.Key.w_state, g.Key.i_item_id
                   select new
                   {
                       g.Key.w_state,
                       g.Key.i_item_id,
                       sales_before = g.Sum(x => x.d.d_date < pivotDate ? x.cs.cs_sales_price - (x.refunded ?? 0m) : 0m),
                       sales_after = g.Sum(x => x.d.d_date >= pivotDate ? x.cs.cs_sales_price - (x.refunded ?? 0m) : 0m)
                   }).Take(100);

        AssertPlan(q40, NativePushdown);

        var polarsResult = q40.ToList();

        var duckDbResults = Q40Expected();

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.w_state, p.w_state);
            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.sales_before, p.sales_before);
            Assert.Equal(d.sales_after, p.sales_after);
        }
    }

    [Fact]
    [Trait("TPCDS","Q93")]
    public void Test_Q93()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var reasonQuery = TpcdsData.ReasonQuery();

        var t = from ss in storeSalesQuery
                join sr in storeReturnsQuery
                  on new { item_sk = ss.ss_item_sk, ticket = ss.ss_ticket_number }
                  equals new { item_sk = sr.sr_item_sk, ticket = sr.sr_ticket_number }
                  into srGroup
                from sr in srGroup.DefaultIfEmpty()
                join r in reasonQuery on sr.sr_reason_sk equals r.r_reason_sk
                where r.r_reason_desc == "reason 28"
                select new
                {
                    ss.ss_customer_sk,
                    act_sales = sr.sr_return_quantity != null
                        ? (decimal?)(ss.ss_quantity - sr.sr_return_quantity) * ss.ss_sales_price
                        : (decimal?)ss.ss_quantity * ss.ss_sales_price
                };

        // SQL sum() is NULL for a group without non-null values, while LINQ Sum() is 0.
        var q93 = (from x in t
                   group x.act_sales by x.ss_customer_sk into g
                   let sumsales = g.Count(v => v != null) > 0 ? g.Sum() : null
                   orderby sumsales, g.Key
                   select new
                   {
                       ss_customer_sk = g.Key,
                       sumsales
                   })
                  .Take(100);

        AssertPlan(q93, NativePushdown);

        var polarsResult = q93.ToList();

        var duckDbResult = Q93Expected();

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].ss_customer_sk, polarsResult[i].ss_customer_sk);
            Assert.Equal(duckDbResult[i].sumsales, polarsResult[i].sumsales);
        }
    }

    /// <summary>
    /// Q93 as written in SQL: store_sales LEFT OUTER JOIN store_returns on (item, ticket), then the reason filter
    /// (which drops unmatched sales, since sr_reason_sk is NULL for them), through the .NET 11 tuple LeftJoin.
    /// </summary>
    [Fact]
    [Trait("TPCDS","Q93")]
    [Trait("LINQ","Net11_LeftJoin")]
    public void Test_Q93_LeftJoin()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var reasonQuery = TpcdsData.ReasonQuery();

        var t = from pair in storeSalesQuery.LeftJoin(storeReturnsQuery,
                        ss => new { item = ss.ss_item_sk, ticket = ss.ss_ticket_number },
                        sr => new { item = sr.sr_item_sk, ticket = sr.sr_ticket_number })
                join r in reasonQuery on pair.Inner.sr_reason_sk equals r.r_reason_sk
                where r.r_reason_desc == "reason 28"
                let returnQty = pair.Inner != null ? pair.Inner.sr_return_quantity : null
                select new
                {
                    pair.Outer.ss_customer_sk,
                    act_sales = returnQty != null
                        ? (decimal?)(pair.Outer.ss_quantity - returnQty) * pair.Outer.ss_sales_price
                        : (decimal?)pair.Outer.ss_quantity * pair.Outer.ss_sales_price
                };

        // SQL sum() is NULL for a group without non-null values, while LINQ Sum() is 0.
        var q93 = (from x in t
                   group x.act_sales by x.ss_customer_sk into g
                   let sumsales = g.Count(v => v != null) > 0 ? g.Sum() : null
                   orderby sumsales, g.Key
                   select new
                   {
                       ss_customer_sk = g.Key,
                       sumsales
                   })
                  .Take(100);

        AssertPlan(q93, NativePushdown);

        var polarsResult = q93.ToList();

        var duckDbResult = Q93Expected();

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].ss_customer_sk, polarsResult[i].ss_customer_sk);
            Assert.Equal(duckDbResult[i].sumsales, polarsResult[i].sumsales);
        }
    }

    private static List<Q40Row> Q40Expected()
    {
        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT w_state,
                   i_item_id,
                   sum(CASE
                           WHEN (cast(d_date AS date) < CAST ('2000-03-11' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash,0)
                           ELSE 0
                       END) AS sales_before,
                   sum(CASE
                           WHEN (cast(d_date AS date) >= CAST ('2000-03-11' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash,0)
                           ELSE 0
                       END) AS sales_after
            FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs
            LEFT OUTER JOIN read_parquet('{TpcdsData.CatalogReturnsPath}') cr
              ON (cs.cs_order_number = cr.cr_order_number AND cs.cs_item_sk = cr.cr_item_sk),
                 read_parquet('{TpcdsData.WarehousePath}') w,
                 read_parquet('{TpcdsData.ItemPath}') i,
                 read_parquet('{TpcdsData.DateDimPath}') d
            WHERE i.i_current_price BETWEEN 0.99 AND 1.49
              AND i.i_item_sk = cs.cs_item_sk
              AND cs.cs_warehouse_sk = w.w_warehouse_sk
              AND cs.cs_sold_date_sk = d.d_date_sk
              AND d.d_date BETWEEN CAST ('2000-02-10' AS date) AND CAST ('2000-04-10' AS date)
            GROUP BY w_state,
                     i_item_id
            ORDER BY w_state,
                     i_item_id
            LIMIT 100;
        ";

        return DuckDbFixture.Query(duckDbSql, r => new Q40Row(
            r.IsDBNull(0) ? null : r.GetString(0),
            r.GetString(1),
            r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2),
            r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3)));
    }

    private sealed record Q40Row(string? w_state, string i_item_id, decimal? sales_before, decimal? sales_after);

    private static List<Q93Row> Q93Expected()
    {
        // DuckDB Cross Validation
        var duckDbSql = $@"
SELECT ss_customer_sk
      ,sum(act_sales) sumsales
FROM (SELECT ss_item_sk
            ,ss_ticket_number
            ,ss_customer_sk
            ,CASE WHEN sr_return_quantity IS NOT NULL THEN (ss_quantity-sr_return_quantity)*ss_sales_price
                                                      ELSE (ss_quantity*ss_sales_price) END act_sales
      FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales
           LEFT OUTER JOIN read_parquet('{TpcdsData.StoreReturnsPath}') store_returns ON (sr_item_sk = ss_item_sk
                                                     AND sr_ticket_number = ss_ticket_number)
          ,read_parquet('{TpcdsData.ReasonPath}') reason
      WHERE sr_reason_sk = r_reason_sk
        AND r_reason_desc = 'reason 28') t
GROUP BY ss_customer_sk
ORDER BY sumsales NULLS FIRST, ss_customer_sk NULLS FIRST
LIMIT 100;
";

        return DuckDbFixture.Query(duckDbSql, r => new Q93Row(
            r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1)));
    }

    private sealed record Q93Row(long? ss_customer_sk, decimal? sumsales);
}
