#nullable enable
using System.Numerics;
using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Tpcds.Tests;

public class TpcdsFullJoinQueryTests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 51
     *
     * WITH web_v1 AS
     *   (SELECT ws_item_sk item_sk,
     *           d_date,
     *           sum(sum(ws_sales_price)) OVER (PARTITION BY ws_item_sk
     *                                          ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) cume_sales
     *    FROM web_sales,
     *         date_dim
     *    WHERE ws_sold_date_sk=d_date_sk
     *      AND d_month_seq BETWEEN 1200 AND 1200+11
     *      AND ws_item_sk IS NOT NULL
     *    GROUP BY ws_item_sk,
     *             d_date),
     *      store_v1 AS
     *   (SELECT ss_item_sk item_sk,
     *           d_date,
     *           sum(sum(ss_sales_price)) OVER (PARTITION BY ss_item_sk
     *                                          ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) cume_sales
     *    FROM store_sales,
     *         date_dim
     *    WHERE ss_sold_date_sk=d_date_sk
     *      AND d_month_seq BETWEEN 1200 AND 1200+11
     *      AND ss_item_sk IS NOT NULL
     *    GROUP BY ss_item_sk,
     *             d_date)
     * SELECT *
     * FROM
     *   (SELECT item_sk,
     *           d_date,
     *           web_sales,
     *           store_sales,
     *           max(web_sales) OVER (PARTITION BY item_sk
     *                                ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) web_cumulative,
     *           max(store_sales) OVER (PARTITION BY item_sk
     *                                  ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) store_cumulative
     *    FROM
     *      (SELECT CASE
     *                  WHEN web.item_sk IS NOT NULL THEN web.item_sk
     *                  ELSE store.item_sk
     *              END item_sk,
     *              CASE
     *                  WHEN web.d_date IS NOT NULL THEN web.d_date
     *                  ELSE store.d_date
     *              END d_date,
     *              web.cume_sales web_sales,
     *              store.cume_sales store_sales
     *       FROM web_v1 web
     *       FULL OUTER JOIN store_v1 store ON (web.item_sk = store.item_sk
     *                                          AND web.d_date = store.d_date))x)y
     * WHERE web_cumulative > store_cumulative
     * ORDER BY item_sk NULLS FIRST,
     *          d_date NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS","Q51")]
    public void Test_Q51()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var webDaily = from ws in webSalesQuery
                       join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                       where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                       group ws by new { item_sk = ws.ws_item_sk, d.d_date } into g
                       select new
                       {
                           g.Key.item_sk,
                           g.Key.d_date,
                           web_daily = g.Sum(x => x.ws_sales_price) ?? 0.00m,
                           store_daily = 0.00m,
                           has_web = 1L,
                           has_web_val = g.Max(x => x.ws_sales_price != null ? 1L : 0L),
                           has_store = 0L,
                           has_store_val = 0L
                       };

        var storeDaily = from ss in storeSalesQuery
                         join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                         where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                         group ss by new { item_sk = ss.ss_item_sk, d.d_date } into g
                         select new
                         {
                             g.Key.item_sk,
                             g.Key.d_date,
                             web_daily = 0.00m,
                             store_daily = g.Sum(x => x.ss_sales_price) ?? 0.00m,
                             has_web = 0L,
                             has_web_val = 0L,
                             has_store = 1L,
                             has_store_val = g.Max(x => x.ss_sales_price != null ? 1L : 0L)
                         };

        var mergedDaily = from x in webDaily.Concat(storeDaily)
                          group x by new { x.item_sk, x.d_date } into g
                          select new
                          {
                              g.Key.item_sk,
                              g.Key.d_date,
                              web_daily = g.Sum(x => x.web_daily),
                              store_daily = g.Sum(x => x.store_daily),
                              has_web = g.Max(x => x.has_web),
                              has_web_val = g.Max(x => x.has_web_val),
                              has_store = g.Max(x => x.has_store),
                              has_store_val = g.Max(x => x.has_store_val)
                          };

        var withCume = from m in mergedDaily
                       orderby m.item_sk, m.d_date
                       select new
                       {
                           m.item_sk,
                           m.d_date,
                           m.has_web,
                           m.has_store,
                           web_cume_raw = m.web_daily.CumSum().Over(m.item_sk),
                           store_cume_raw = m.store_daily.CumSum().Over(m.item_sk),
                           seen_web = m.has_web_val.CumSum().Over(m.item_sk),
                           seen_store = m.has_store_val.CumSum().Over(m.item_sk)
                       };

        var projected = from c in withCume
                        select new
                        {
                            c.item_sk,
                            c.d_date,
                            web_sales = c.has_web > 0 && c.seen_web > 0 ? (decimal?)c.web_cume_raw : null,
                            store_sales = c.has_store > 0 && c.seen_store > 0 ? (decimal?)c.store_cume_raw : null,
                            web_cumulative = c.seen_web > 0 ? (decimal?)c.web_cume_raw : null,
                            store_cumulative = c.seen_store > 0 ? (decimal?)c.store_cume_raw : null
                        };

        var q51 = (from y in projected
                   where y.web_cumulative > y.store_cumulative
                   orderby y.item_sk, y.d_date
                   select y)
                  .Take(100);

        AssertPlan(q51, NativePushdown);

        var polarsResult = q51.ToList();

        var duckDbResults = Q51Expected();

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.item_sk, p.item_sk);
            Assert.Equal(d.d_date, p.d_date);
            Assert.Equal(d.web_sales, p.web_sales);
            Assert.Equal(d.store_sales, p.store_sales);
            Assert.Equal(d.web_cumulative, p.web_cumulative);
            Assert.Equal(d.store_cumulative, p.store_cumulative);
        }
    }

    [Fact]
    [Trait("TPCDS","Q97")]
    public void Test_Q97()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var ssci = from ss in storeSalesQuery
                   join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                      && ss.ss_customer_sk != null
                   group ss by new
                   {
                       customer_sk = ss.ss_customer_sk,
                       item_sk = ss.ss_item_sk
                   } into g
                   select new
                   {
                       g.Key.customer_sk,
                       g.Key.item_sk,
                       in_store = 1L,
                       in_cat = 0L
                   };

        var csci = from cs in catalogSalesQuery
                   join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                      && cs.cs_bill_customer_sk != null
                   group cs by new
                   {
                       customer_sk = cs.cs_bill_customer_sk,
                       item_sk = cs.cs_item_sk
                   } into g
                   select new
                   {
                       g.Key.customer_sk,
                       g.Key.item_sk,
                       in_store = 0L,
                       in_cat = 1L
                   };

        var merged = from x in ssci.Concat(csci)
                     group x by new { x.customer_sk, x.item_sk } into g
                     select new
                     {
                         in_store = g.Max(x => x.in_store),
                         in_cat = g.Max(x => x.in_cat)
                     };

        var q97 = (from x in merged
                   group x by 1 into g
                   select new
                   {
                       store_only = g.Sum(x => x.in_store == 1L && x.in_cat == 0L ? 1L : 0L),
                       catalog_only = g.Sum(x => x.in_store == 0L && x.in_cat == 1L ? 1L : 0L),
                       store_and_catalog = g.Sum(x => x.in_store == 1L && x.in_cat == 1L ? 1L : 0L)
                   })
                  .Take(100);

        AssertPlan(q97, NativePushdown);

        var polarsResult = q97.ToList();

        var duckDbResult = Q97Expected();

        var expected = duckDbResult.FirstOrDefault();
        var actual = polarsResult.FirstOrDefault();
        Assert.Equal(expected?.store_only, actual?.store_only);
        Assert.Equal(expected?.catalog_only, actual?.catalog_only);
        Assert.Equal(expected?.store_and_catalog, actual?.store_and_catalog);
    }

    /// <summary>
    /// Q51 as written in SQL: each side's cumulative sales (web_v1 / store_v1), a real .NET 11 FullJoin on
    /// (item_sk, d_date), then the running maxima of both sides.
    /// </summary>
    [Fact]
    [Trait("TPCDS","Q51")]
    [Trait("LINQ","Net11_FullJoin")]
    public void Test_Q51_FullJoin()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();

        // sum(sum(price)) OVER (PARTITION BY item ORDER BY d_date): null until the item's first non-null price.
        var webV1 = from g in from ws in TpcdsData.WebSalesQuery()
                               join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                               where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                               group ws by new { item_sk = ws.ws_item_sk, d.d_date } into g
                               select new
                               {
                                   g.Key.item_sk,
                                   g.Key.d_date,
                                   daily = g.Sum(x => x.ws_sales_price) ?? 0.00m,
                                   has_val = g.Max(x => x.ws_sales_price != null ? 1L : 0L)
                               }
                    orderby g.item_sk, g.d_date
                    select new
                    {
                        g.item_sk,
                        g.d_date,
                        cume_raw = g.daily.CumSum().Over(g.item_sk),
                        seen = g.has_val.CumSum().Over(g.item_sk)
                    } into c
                    select new { c.item_sk, c.d_date, cume_sales = c.seen > 0 ? (decimal?)c.cume_raw : null };

        var storeV1 = from g in from ss in TpcdsData.StoreSalesQuery()
                                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                                 where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                                 group ss by new { item_sk = ss.ss_item_sk, d.d_date } into g
                                 select new
                                 {
                                     g.Key.item_sk,
                                     g.Key.d_date,
                                     daily = g.Sum(x => x.ss_sales_price) ?? 0.00m,
                                     has_val = g.Max(x => x.ss_sales_price != null ? 1L : 0L)
                                 }
                      orderby g.item_sk, g.d_date
                      select new
                      {
                          g.item_sk,
                          g.d_date,
                          cume_raw = g.daily.CumSum().Over(g.item_sk),
                          seen = g.has_val.CumSum().Over(g.item_sk)
                      } into c
                      select new { c.item_sk, c.d_date, cume_sales = c.seen > 0 ? (decimal?)c.cume_raw : null };

        var x = webV1
            .FullJoin(storeV1, w => new { w.item_sk, w.d_date }, s => new { s.item_sk, s.d_date })
            .Select(pair => new
            {
                item_sk = pair.Outer != null ? pair.Outer.item_sk : pair.Inner.item_sk,
                d_date = pair.Outer != null ? pair.Outer.d_date : pair.Inner.d_date,
                web_sales = pair.Outer != null ? pair.Outer.cume_sales : null,
                store_sales = pair.Inner != null ? pair.Inner.cume_sales : null
            });

        // SQL's max() OVER skips nulls and carries the running maximum forward; Polars' cum_max keeps a null
        // row null. So the maximum runs over the values with null as 0 (sales are non-negative), and a "seen"
        // count restores null before a side's first value.
        var y = from r in x
                orderby r.item_sk, r.d_date
                select new
                {
                    r.item_sk,
                    r.d_date,
                    r.web_sales,
                    r.store_sales,
                    web_max = (r.web_sales ?? 0m).CumMax().Over(r.item_sk),
                    store_max = (r.store_sales ?? 0m).CumMax().Over(r.item_sk),
                    web_seen = (r.web_sales != null ? 1L : 0L).CumSum().Over(r.item_sk),
                    store_seen = (r.store_sales != null ? 1L : 0L).CumSum().Over(r.item_sk)
                } into w
                select new
                {
                    w.item_sk,
                    w.d_date,
                    w.web_sales,
                    w.store_sales,
                    web_cumulative = w.web_seen > 0 ? (decimal?)w.web_max : null,
                    store_cumulative = w.store_seen > 0 ? (decimal?)w.store_max : null
                };

        var q51 = (from r in y
                   where r.web_cumulative > r.store_cumulative
                   orderby r.item_sk, r.d_date
                   select r)
                  .Take(100);

        AssertPlan(q51, NativePushdown);

        var polarsResult = q51.ToList();
        var duckDbResults = Q51Expected();

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.item_sk, p.item_sk);
            Assert.Equal(d.d_date, p.d_date);
            Assert.Equal(d.web_sales, p.web_sales);
            Assert.Equal(d.store_sales, p.store_sales);
            Assert.Equal(d.web_cumulative, p.web_cumulative);
            Assert.Equal(d.store_cumulative, p.store_cumulative);
        }
    }

    /// <summary>Q97 as written in SQL: a real .NET 11 FullJoin of the two (customer, item) sets.</summary>
    [Fact]
    [Trait("TPCDS","Q97")]
    [Trait("LINQ","Net11_FullJoin")]
    public void Test_Q97_FullJoin()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();

        var ssci = from ss in TpcdsData.StoreSalesQuery()
                   join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                   group ss by new { customer_sk = ss.ss_customer_sk, item_sk = ss.ss_item_sk } into g
                   select new { g.Key.customer_sk, g.Key.item_sk };

        var csci = from cs in TpcdsData.CatalogSalesQuery()
                   join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                   group cs by new { customer_sk = cs.cs_bill_customer_sk, item_sk = cs.cs_item_sk } into g
                   select new { g.Key.customer_sk, g.Key.item_sk };

        var q97 = (from pair in ssci.FullJoin(csci, s => new { s.customer_sk, s.item_sk }, c => new { c.customer_sk, c.item_sk })
                   let storeCustomer = pair.Outer != null ? pair.Outer.customer_sk : null
                   let catalogCustomer = pair.Inner != null ? pair.Inner.customer_sk : null
                   group new { storeCustomer, catalogCustomer } by 1 into g
                   select new
                   {
                       store_only = g.Sum(r => r.storeCustomer != null && r.catalogCustomer == null ? 1L : 0L),
                       catalog_only = g.Sum(r => r.storeCustomer == null && r.catalogCustomer != null ? 1L : 0L),
                       store_and_catalog = g.Sum(r => r.storeCustomer != null && r.catalogCustomer != null ? 1L : 0L)
                   })
                  .Take(100);

        AssertPlan(q97, NativePushdown);

        var actual = q97.ToList().FirstOrDefault();
        var expected = Q97Expected().FirstOrDefault();
        Assert.Equal(expected?.store_only, actual?.store_only);
        Assert.Equal(expected?.catalog_only, actual?.catalog_only);
        Assert.Equal(expected?.store_and_catalog, actual?.store_and_catalog);
    }

    private static List<Q51Row> Q51Expected()
    {
        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH web_v1 AS
              (SELECT ws_item_sk item_sk,
                      d_date,
                      sum(sum(ws_sales_price)) OVER (PARTITION BY ws_item_sk
                                                     ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) cume_sales
               FROM read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE ws_sold_date_sk=d_date_sk
                 AND d_month_seq BETWEEN 1200 AND 1200+11
                 AND ws_item_sk IS NOT NULL
               GROUP BY ws_item_sk,
                        d_date),
                 store_v1 AS
              (SELECT ss_item_sk item_sk,
                      d_date,
                      sum(sum(ss_sales_price)) OVER (PARTITION BY ss_item_sk
                                                     ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) cume_sales
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE ss_sold_date_sk=d_date_sk
                 AND d_month_seq BETWEEN 1200 AND 1200+11
                 AND ss_item_sk IS NOT NULL
               GROUP BY ss_item_sk,
                        d_date)
            SELECT *
            FROM
              (SELECT item_sk,
                      d_date,
                      web_sales,
                      store_sales,
                      max(web_sales) OVER (PARTITION BY item_sk
                                           ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) web_cumulative,
                      max(store_sales) OVER (PARTITION BY item_sk
                                             ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) store_cumulative
               FROM
                 (SELECT CASE
                             WHEN web.item_sk IS NOT NULL THEN web.item_sk
                             ELSE store.item_sk
                         END item_sk,
                         CASE
                             WHEN web.d_date IS NOT NULL THEN web.d_date
                             ELSE store.d_date
                         END d_date,
                         web.cume_sales web_sales,
                         store.cume_sales store_sales
                  FROM web_v1 web
                  FULL OUTER JOIN store_v1 store ON (web.item_sk = store.item_sk
                                                     AND web.d_date = store.d_date))x)y
            WHERE web_cumulative > store_cumulative
            ORDER BY item_sk NULLS FIRST,
                     d_date NULLS FIRST
            LIMIT 100;
        ";

        return DuckDbFixture.Query(duckDbSql, r => new Q51Row(
            r.GetInt64(0),
            r.IsDBNull(1) ? (DateOnly?)null : r.GetFieldValue<DateOnly>(1),
            r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2),
            r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3),
            r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4),
            r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5)));

    }

    private sealed record Q51Row(long item_sk, DateOnly? d_date, decimal? web_sales, decimal? store_sales, decimal? web_cumulative, decimal? store_cumulative);

    private static List<Q97Row> Q97Expected()
    {
        var duckDbSql = $@"
WITH ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from read_parquet('{TpcdsData.StoreSalesPath}') store_sales,read_parquet('{TpcdsData.DateDimPath}') date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1200 and 1200 + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales,read_parquet('{TpcdsData.DateDimPath}') date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1200 and 1200 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;
";

        return DuckDbFixture.Query(duckDbSql, r => new Q97Row(
            r.IsDBNull(0) ? (long?)null : (long)r.GetFieldValue<BigInteger>(0),
            r.IsDBNull(1) ? (long?)null : (long)r.GetFieldValue<BigInteger>(1),
            r.IsDBNull(2) ? (long?)null : (long)r.GetFieldValue<BigInteger>(2)));

    }

    private sealed record Q97Row(long? store_only, long? catalog_only, long? store_and_catalog);
}
