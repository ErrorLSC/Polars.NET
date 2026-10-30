namespace Polars.Tpcds.Tests;

public class TpcdsQuery51To60Tests
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

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            item_sk = r.GetInt64(0),
            d_date = r.IsDBNull(1) ? (DateOnly?)null : r.GetFieldValue<DateOnly>(1),
            web_sales = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2),
            store_sales = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3),
            web_cumulative = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4),
            store_cumulative = r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5)
        });

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

    /*
     * TPC-DS Query 52
     *
     * SELECT dt.d_year,
     *        item.i_brand_id brand_id,
     *        item.i_brand brand,
     *        sum(ss_ext_sales_price) ext_price
     * FROM date_dim dt,
     *      store_sales,
     *      item
     * WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
     *   AND store_sales.ss_item_sk = item.i_item_sk
     *   AND item.i_manager_id = 1
     *   AND dt.d_moy=11
     *   AND dt.d_year=2000
     * GROUP BY dt.d_year,
     *          item.i_brand,
     *          item.i_brand_id
     * ORDER BY dt.d_year,
     *          ext_price DESC,
     *          brand_id
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q52()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var q52 = (from dt in dateDimQuery
                   join ss in storeSalesQuery on dt.d_date_sk equals ss.ss_sold_date_sk
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   where i.i_manager_id == 1 && dt.d_moy == 11 && dt.d_year == 2000
                   group ss by new { dt.d_year, i.i_brand, i.i_brand_id } into g
                   let ext_price = g.Sum(x => x.ss_ext_sales_price)
                   orderby g.Key.d_year, ext_price descending, g.Key.i_brand_id
                   select new
                   {
                       g.Key.d_year,
                       brand_id = g.Key.i_brand_id,
                       brand = g.Key.i_brand,
                       ext_price = ext_price
                   })
                  .Take(100);

        AssertPlan(q52, NativePushdown);

        var polarsResult = q52.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT dt.d_year,
                   item.i_brand_id brand_id,
                   item.i_brand brand,
                   sum(ss_ext_sales_price) ext_price
            FROM read_parquet('{TpcdsData.DateDimPath}') dt,
                 read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                 read_parquet('{TpcdsData.ItemPath}') item
            WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
              AND store_sales.ss_item_sk = item.i_item_sk
              AND item.i_manager_id = 1
              AND dt.d_moy=11
              AND dt.d_year=2000
            GROUP BY dt.d_year,
                     item.i_brand,
                     item.i_brand_id
            ORDER BY dt.d_year,
                     ext_price DESC,
                     brand_id
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            d_year = r.GetInt64(0),
            brand_id = r.IsDBNull(1) ? (long?)null : r.GetInt64(1),
            brand = r.IsDBNull(2) ? null : r.GetString(2),
            ext_price = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.d_year, p.d_year);
            Assert.Equal(d.brand_id, p.brand_id);
            Assert.Equal(d.brand, p.brand);
            Assert.Equal(d.ext_price, p.ext_price);
        }
    }

    /*
     * TPC-DS Query 53
     *
     * SELECT *
     * FROM
     *   (SELECT i_manufact_id,
     *           sum(ss_sales_price) sum_sales,
     *           avg(sum(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
     *    FROM item,
     *         store_sales,
     *         date_dim,
     *         store
     *    WHERE ss_item_sk = i_item_sk
     *      AND ss_sold_date_sk = d_date_sk
     *      AND ss_store_sk = s_store_sk
     *      AND d_month_seq IN (1200, 1200+1, 1200+2, 1200+3, 1200+4, 1200+5, 1200+6, 1200+7, 1200+8, 1200+9, 1200+10, 1200+11)
     *      AND ((i_category IN ('Books', 'Children', 'Electronics')
     *            AND i_class IN ('personal', 'portable', 'reference', 'self-help')
     *            AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
     *           OR (i_category IN ('Women', 'Music', 'Men')
     *               AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
     *               AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))
     *    GROUP BY i_manufact_id,
     *             d_qoy) tmp1
     * WHERE CASE
     *           WHEN avg_quarterly_sales > 0 THEN ABS (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales
     *           ELSE NULL
     *       END > 0.1
     * ORDER BY avg_quarterly_sales,
     *          sum_sales,
     *          i_manufact_id
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q53()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var cat1 = new[] { "Books", "Children", "Electronics" };
        var class1 = new[] { "personal", "portable", "reference", "self-help" };
        var brand1 = new[] { "scholaramalgamalg #14", "scholaramalgamalg #7", "exportiunivamalg #9", "scholaramalgamalg #9" };

        var cat2 = new[] { "Women", "Music", "Men" };
        var class2 = new[] { "accessories", "classical", "fragrances", "pants" };
        var brand2 = new[] { "amalgimporto #1", "edu packscholar #1", "exportiimporto #1", "importoamalg #1" };

        var q53 = (from ss in storeSalesQuery
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                   join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                      && ((cat1.Contains(i.i_category) && class1.Contains(i.i_class) && brand1.Contains(i.i_brand))
                          || (cat2.Contains(i.i_category) && class2.Contains(i.i_class) && brand2.Contains(i.i_brand)))
                   group ss by new { i.i_manufact_id, d.d_qoy } into g
                   let sum_sales = g.Sum(x => x.ss_sales_price)
                   let avg_quarterly_sales = sum_sales.Average().Over(g.Key.i_manufact_id)
                   select new
                   {
                       g.Key.i_manufact_id,
                       sum_sales,
                       avg_quarterly_sales
                   } into t
                   where t.avg_quarterly_sales > 0.0
                      && (Math.Abs((double)t.sum_sales! - t.avg_quarterly_sales) / t.avg_quarterly_sales > 0.1)
                   orderby t.avg_quarterly_sales, t.sum_sales, t.i_manufact_id
                   select t)
                  .Take(100);

        AssertPlan(q53, NativePushdown);

        var polarsResult = q53.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT *
            FROM
              (SELECT i_manufact_id,
                      sum(ss_sales_price) sum_sales,
                      avg(sum(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
               FROM read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.StorePath}')
               WHERE ss_item_sk = i_item_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND ss_store_sk = s_store_sk
                 AND d_month_seq IN (1200,
                                     1200+1,
                                     1200+2,
                                     1200+3,
                                     1200+4,
                                     1200+5,
                                     1200+6,
                                     1200+7,
                                     1200+8,
                                     1200+9,
                                     1200+10,
                                     1200+11)
                 AND ((i_category IN ('Books',
                                      'Children',
                                      'Electronics')
                       AND i_class IN ('personal',
                                       'portable',
                                       'reference',
                                       'self-help')
                       AND i_brand IN ('scholaramalgamalg #14',
                                       'scholaramalgamalg #7',
                                       'exportiunivamalg #9',
                                       'scholaramalgamalg #9')) or(i_category IN ('Women','Music','Men')
                                                                   AND i_class IN ('accessories','classical','fragrances','pants')
                                                                   AND i_brand IN ('amalgimporto #1','edu packscholar #1','exportiimporto #1', 'importoamalg #1')))
               GROUP BY i_manufact_id,
                        d_qoy) tmp1
            WHERE CASE
                      WHEN avg_quarterly_sales > 0 THEN ABS (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales
                      ELSE NULL
                  END > 0.1
            ORDER BY avg_quarterly_sales,
                     sum_sales,
                     i_manufact_id
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_manufact_id = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            sum_sales = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1),
            avg_quarterly_sales = r.IsDBNull(2) ? (double?)null : (double)r.GetDecimal(2)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.i_manufact_id, p.i_manufact_id);
            Assert.Equal(d.sum_sales, p.sum_sales);
            Assert.Equal(d.avg_quarterly_sales ?? 0.0, p.avg_quarterly_sales, 4);
        }
    }

    /*
     * TPC-DS Query 54
     *
     * WITH my_customers AS
     *   (SELECT DISTINCT c_customer_sk,
     *                    c_current_addr_sk
     *    FROM
     *      (SELECT cs_sold_date_sk sold_date_sk,
     *              cs_bill_customer_sk customer_sk,
     *              cs_item_sk item_sk
     *       FROM catalog_sales
     *       UNION ALL SELECT ws_sold_date_sk sold_date_sk,
     *                        ws_bill_customer_sk customer_sk,
     *                        ws_item_sk item_sk
     *       FROM web_sales) cs_or_ws_sales,
     *         item,
     *         date_dim,
     *         customer
     *    WHERE sold_date_sk = d_date_sk
     *      AND item_sk = i_item_sk
     *      AND i_category = 'Women'
     *      AND i_class = 'maternity'
     *      AND c_customer_sk = cs_or_ws_sales.customer_sk
     *      AND d_moy = 12
     *      AND d_year = 1998 ),
     *      my_revenue AS
     *   (SELECT c_customer_sk,
     *           sum(ss_ext_sales_price) AS revenue
     *    FROM my_customers,
     *         store_sales,
     *         customer_address,
     *         store,
     *         date_dim
     *    WHERE c_current_addr_sk = ca_address_sk
     *      AND ca_county = s_county
     *      AND ca_state = s_state
     *      AND ss_sold_date_sk = d_date_sk
     *      AND c_customer_sk = ss_customer_sk
     *      AND d_month_seq BETWEEN
     *        (SELECT DISTINCT d_month_seq+1
     *         FROM date_dim
     *         WHERE d_year = 1998
     *           AND d_moy = 12) AND
     *        (SELECT DISTINCT d_month_seq+3
     *         FROM date_dim
     *         WHERE d_year = 1998
     *           AND d_moy = 12)
     *    GROUP BY c_customer_sk),
     *      segments AS
     *   (SELECT cast(round(revenue/50) AS int) AS SEGMENT
     *    FROM my_revenue)
     * SELECT SEGMENT,
     *        count(*) AS num_customers,
     *        SEGMENT*50 AS segment_base
     * FROM segments
     * GROUP BY SEGMENT
     * ORDER BY SEGMENT NULLS FIRST,
     *          num_customers NULLS FIRST,
     *          segment_base
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q54()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var baseMonthSeq = dateDimQuery
            .Where(d => d.d_year == 1998 && d.d_moy == 12)
            .Select(d => d.d_month_seq)
            .Distinct()
            .First()!.Value;

        var csSales = catalogSalesQuery.Select(cs => new
        {
            sold_date_sk = cs.cs_sold_date_sk,
            customer_sk = cs.cs_bill_customer_sk,
            item_sk = cs.cs_item_sk
        });

        var wsSales = webSalesQuery.Select(ws => new
        {
            sold_date_sk = ws.ws_sold_date_sk,
            customer_sk = ws.ws_bill_customer_sk,
            item_sk = ws.ws_item_sk
        });

        var myCustomers = (from s in csSales.Concat(wsSales)
                           join d in dateDimQuery on s.sold_date_sk equals d.d_date_sk
                           join i in itemQuery on s.item_sk equals i.i_item_sk
                           join c in customerQuery on s.customer_sk equals c.c_customer_sk
                           where i.i_category == "Women" && i.i_class == "maternity"
                              && d.d_moy == 12 && d.d_year == 1998
                           select new
                           {
                               c.c_customer_sk,
                               c.c_current_addr_sk
                           })
                          .Distinct();

        var myRevenue = from mc in myCustomers
                        join ca in customerAddressQuery on mc.c_current_addr_sk equals ca.ca_address_sk
                        join s in storeQuery on new { county = ca.ca_county, state = ca.ca_state }
                                         equals new { county = s.s_county, state = s.s_state }
                        join ss in storeSalesQuery on mc.c_customer_sk equals ss.ss_customer_sk
                        join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                        where d.d_month_seq >= baseMonthSeq + 1 && d.d_month_seq <= baseMonthSeq + 3
                        group ss by mc.c_customer_sk into g
                        select new
                        {
                            c_customer_sk = g.Key,
                            revenue = g.Sum(x => x.ss_ext_sales_price)
                        };

        var segments = from r in myRevenue
                       select new
                       {
                           segment = (long?)Math.Round((decimal)r.revenue! / 50m, MidpointRounding.AwayFromZero)
                       };

        var q54 = (from seg in segments
                   group seg by seg.segment into g
                   let num_customers = (long)g.Count()
                   let segment_base = g.Key * 50
                   orderby g.Key, num_customers, segment_base
                   select new
                   {
                       segment = g.Key,
                       num_customers,
                       segment_base
                   })
                  .Take(100);

        AssertPlan(q54, NativePushdown);

        var polarsResult = q54.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH my_customers AS
              (SELECT DISTINCT c_customer_sk,
                               c_current_addr_sk
               FROM
                 (SELECT cs_sold_date_sk sold_date_sk,
                         cs_bill_customer_sk customer_sk,
                         cs_item_sk item_sk
                  FROM read_parquet('{TpcdsData.CatalogSalesPath}')
                  UNION ALL SELECT ws_sold_date_sk sold_date_sk,
                                   ws_bill_customer_sk customer_sk,
                                   ws_item_sk item_sk
                  FROM read_parquet('{TpcdsData.WebSalesPath}')) cs_or_ws_sales,
                    read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.CustomerPath}') customer
               WHERE sold_date_sk = d_date_sk
                 AND item_sk = i_item_sk
                 AND i_category = 'Women'
                 AND i_class = 'maternity'
                 AND c_customer_sk = cs_or_ws_sales.customer_sk
                 AND d_moy = 12
                 AND d_year = 1998 ),
                 my_revenue AS
              (SELECT c_customer_sk,
                      sum(ss_ext_sales_price) AS revenue
               FROM my_customers,
                    read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.CustomerAddressPath}') customer_address,
                    read_parquet('{TpcdsData.StorePath}') store,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE c_current_addr_sk = ca_address_sk
                 AND ca_county = s_county
                 AND ca_state = s_state
                 AND ss_sold_date_sk = d_date_sk
                 AND c_customer_sk = ss_customer_sk
                 AND d_month_seq BETWEEN
                   (SELECT DISTINCT d_month_seq+1
                    FROM read_parquet('{TpcdsData.DateDimPath}')
                    WHERE d_year = 1998
                      AND d_moy = 12) AND
                   (SELECT DISTINCT d_month_seq+3
                    FROM read_parquet('{TpcdsData.DateDimPath}')
                    WHERE d_year = 1998
                      AND d_moy = 12)
               GROUP BY c_customer_sk),
                 segments AS
              (SELECT cast(round(revenue/50) AS int) AS SEGMENT
               FROM my_revenue)
            SELECT SEGMENT,
                   count(*) AS num_customers,
                   SEGMENT*50 AS segment_base
            FROM segments
            GROUP BY SEGMENT
            ORDER BY SEGMENT NULLS FIRST,
                     num_customers NULLS FIRST,
                     segment_base
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            segment = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            num_customers = r.GetInt64(1),
            segment_base = r.IsDBNull(2) ? (long?)null : r.GetInt64(2)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.segment, p.segment);
            Assert.Equal(d.num_customers, p.num_customers);
            Assert.Equal(d.segment_base, p.segment_base);
        }
    }

    /*
     * TPC-DS Query 55
     *
     * SELECT i_brand_id brand_id,
     *        i_brand brand,
     *        sum(ss_ext_sales_price) ext_price
     * FROM date_dim,
     *      store_sales,
     *      item
     * WHERE d_date_sk = ss_sold_date_sk
     *   AND ss_item_sk = i_item_sk
     *   AND i_manager_id=28
     *   AND d_moy=11
     *   AND d_year=1999
     * GROUP BY i_brand,
     *          i_brand_id
     * ORDER BY ext_price DESC,
     *          i_brand_id
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q55()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var q55 = (from d in dateDimQuery
                   join ss in storeSalesQuery on d.d_date_sk equals ss.ss_sold_date_sk
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   where i.i_manager_id == 28 && d.d_moy == 11 && d.d_year == 1999
                   group ss by new { i.i_brand, i.i_brand_id } into g
                   let ext_price = g.Sum(x => x.ss_ext_sales_price)
                   orderby ext_price descending, g.Key.i_brand_id
                   select new
                   {
                       brand_id = g.Key.i_brand_id,
                       brand = g.Key.i_brand,
                       ext_price = ext_price
                   })
                  .Take(100);

        AssertPlan(q55, NativePushdown);

        var polarsResult = q55.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT i_brand_id brand_id,
                   i_brand brand,
                   sum(ss_ext_sales_price) ext_price
            FROM read_parquet('{TpcdsData.DateDimPath}') date_dim,
                 read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                 read_parquet('{TpcdsData.ItemPath}') item
            WHERE d_date_sk = ss_sold_date_sk
              AND ss_item_sk = i_item_sk
              AND i_manager_id=28
              AND d_moy=11
              AND d_year=1999
            GROUP BY i_brand,
                     i_brand_id
            ORDER BY ext_price DESC,
                     i_brand_id
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            brand_id = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            brand = r.IsDBNull(1) ? null : r.GetString(1),
            ext_price = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.brand_id, p.brand_id);
            Assert.Equal(d.brand, p.brand);
            Assert.Equal(d.ext_price, p.ext_price);
        }
    }

    /*
     * TPC-DS Query 56
     *
     * WITH ss AS
     *   (SELECT i_item_id,
     *           sum(ss_ext_sales_price) total_sales
     *    FROM store_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_item_id IN
     *        (SELECT i_item_id
     *         FROM item
     *         WHERE i_color IN ('slate',
     *                           'blanched',
     *                           'burnished'))
     *      AND ss_item_sk = i_item_sk
     *      AND ss_sold_date_sk = d_date_sk
     *      AND d_year = 2001
     *      AND d_moy = 2
     *      AND ss_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_item_id),
     *      cs AS
     *   (SELECT i_item_id,
     *           sum(cs_ext_sales_price) total_sales
     *    FROM catalog_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_item_id IN
     *        (SELECT i_item_id
     *         FROM item
     *         WHERE i_color IN ('slate',
     *                           'blanched',
     *                           'burnished'))
     *      AND cs_item_sk = i_item_sk
     *      AND cs_sold_date_sk = d_date_sk
     *      AND d_year = 2001
     *      AND d_moy = 2
     *      AND cs_bill_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_item_id),
     *      ws AS
     *   (SELECT i_item_id,
     *           sum(ws_ext_sales_price) total_sales
     *    FROM web_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_item_id IN
     *        (SELECT i_item_id
     *         FROM item
     *         WHERE i_color IN ('slate',
     *                           'blanched',
     *                           'burnished'))
     *      AND ws_item_sk = i_item_sk
     *      AND ws_sold_date_sk = d_date_sk
     *      AND d_year = 2001
     *      AND d_moy = 2
     *      AND ws_bill_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_item_id)
     * SELECT i_item_id,
     *        sum(total_sales) total_sales
     * FROM
     *   (SELECT *
     *    FROM ss
     *    UNION ALL SELECT *
     *    FROM cs
     *    UNION ALL SELECT *
     *    FROM ws) tmp1
     * GROUP BY i_item_id
     * ORDER BY total_sales NULLS FIRST,
     *          i_item_id NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q56()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var colors = new[] { "slate", "blanched", "burnished" };
        var colorItemIds = itemQuery
            .Where(i => colors.Contains(i.i_color))
            .Select(i => new { i.i_item_id })
            .Distinct();

        var validItems = from i in itemQuery
                         join c in colorItemIds on i.i_item_id equals c.i_item_id
                         select i;

        var ss = from s in storeSalesQuery
                 join d in dateDimQuery on s.ss_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on s.ss_addr_sk equals ca.ca_address_sk
                 join i in validItems on s.ss_item_sk equals i.i_item_sk
                 where d.d_year == 2001 && d.d_moy == 2 && ca.ca_gmt_offset == -5m
                 group s by i.i_item_id into g
                 select new
                 {
                     i_item_id = g.Key,
                     total_sales = g.Sum(x => x.ss_ext_sales_price)
                 };

        var cs = from c in catalogSalesQuery
                 join d in dateDimQuery on c.cs_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on c.cs_bill_addr_sk equals ca.ca_address_sk
                 join i in validItems on c.cs_item_sk equals i.i_item_sk
                 where d.d_year == 2001 && d.d_moy == 2 && ca.ca_gmt_offset == -5m
                 group c by i.i_item_id into g
                 select new
                 {
                     i_item_id = g.Key,
                     total_sales = g.Sum(x => x.cs_ext_sales_price)
                 };

        var ws = from w in webSalesQuery
                 join d in dateDimQuery on w.ws_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on w.ws_bill_addr_sk equals ca.ca_address_sk
                 join i in validItems on w.ws_item_sk equals i.i_item_sk
                 where d.d_year == 2001 && d.d_moy == 2 && ca.ca_gmt_offset == -5m
                 group w by i.i_item_id into g
                 select new
                 {
                     i_item_id = g.Key,
                     total_sales = g.Sum(x => x.ws_ext_sales_price)
                 };

        var q56 = (from x in ss.Concat(cs).Concat(ws)
                   group x by x.i_item_id into g
                   let total_sales = g.Sum(x => x.total_sales)
                   orderby total_sales, g.Key
                   select new
                   {
                       i_item_id = g.Key,
                       total_sales = total_sales
                   })
                  .Take(100);

        AssertPlan(q56, NativePushdown);

        var polarsResult = q56.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH ss AS
              (SELECT i_item_id,
                      sum(ss_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_item_id IN
                   (SELECT i_item_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_color IN ('slate',
                                      'blanched',
                                      'burnished'))
                 AND ss_item_sk = i_item_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND d_year = 2001
                 AND d_moy = 2
                 AND ss_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_item_id),
                 cs AS
              (SELECT i_item_id,
                      sum(cs_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_item_id IN
                   (SELECT i_item_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_color IN ('slate',
                                      'blanched',
                                      'burnished'))
                 AND cs_item_sk = i_item_sk
                 AND cs_sold_date_sk = d_date_sk
                 AND d_year = 2001
                 AND d_moy = 2
                 AND cs_bill_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_item_id),
                 ws AS
              (SELECT i_item_id,
                      sum(ws_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_item_id IN
                   (SELECT i_item_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_color IN ('slate',
                                      'blanched',
                                      'burnished'))
                 AND ws_item_sk = i_item_sk
                 AND ws_sold_date_sk = d_date_sk
                 AND d_year = 2001
                 AND d_moy = 2
                 AND ws_bill_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_item_id)
            SELECT i_item_id,
                   sum(total_sales) total_sales
            FROM
              (SELECT *
               FROM ss
               UNION ALL SELECT *
               FROM cs
               UNION ALL SELECT *
               FROM ws) tmp1
            GROUP BY i_item_id
            ORDER BY total_sales NULLS FIRST,
                     i_item_id NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.IsDBNull(0) ? null : r.GetString(0),
            total_sales = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.total_sales, p.total_sales);
        }
    }

    /*
     * TPC-DS Query 57
     *
     * WITH v1 AS
     *   (SELECT i_category,
     *           i_brand,
     *           cc_name,
     *           d_year,
     *           d_moy,
     *           sum(cs_sales_price) sum_sales,
     *           avg(sum(cs_sales_price)) OVER (PARTITION BY i_category,
     *                                                       i_brand,
     *                                                       cc_name,
     *                                                       d_year) avg_monthly_sales,
     *           rank() OVER (PARTITION BY i_category,
     *                                     i_brand,
     *                                     cc_name
     *                        ORDER BY d_year,
     *                                 d_moy) rn
     *    FROM item,
     *         catalog_sales,
     *         date_dim,
     *         call_center
     *    WHERE cs_item_sk = i_item_sk
     *      AND cs_sold_date_sk = d_date_sk
     *      AND cc_call_center_sk= cs_call_center_sk
     *      AND (d_year = 1999
     *           OR (d_year = 1999-1
     *               AND d_moy =12)
     *           OR (d_year = 1999+1
     *               AND d_moy =1))
     *    GROUP BY i_category,
     *             i_brand,
     *             cc_name,
     *             d_year,
     *             d_moy),
     *      v2 AS
     *   (SELECT v1.i_category,
     *           v1.i_brand,
     *           v1.cc_name,
     *           v1.d_year,
     *           v1.d_moy,
     *           v1.avg_monthly_sales,
     *           v1.sum_sales,
     *           v1_lag.sum_sales psum,
     *           v1_lead.sum_sales nsum
     *    FROM v1,
     *         v1 v1_lag,
     *         v1 v1_lead
     *    WHERE v1.i_category = v1_lag.i_category
     *      AND v1.i_category = v1_lead.i_category
     *      AND v1.i_brand = v1_lag.i_brand
     *      AND v1.i_brand = v1_lead.i_brand
     *      AND v1.cc_name = v1_lag.cc_name
     *      AND v1.cc_name = v1_lead.cc_name
     *      AND v1.rn = v1_lag.rn + 1
     *      AND v1.rn = v1_lead.rn - 1)
     * SELECT *
     * FROM v2
     * WHERE d_year = 1999
     *   AND avg_monthly_sales > 0
     *   AND CASE
     *           WHEN avg_monthly_sales > 0 THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
     *           ELSE NULL
     *       END > 0.1
     * ORDER BY sum_sales - avg_monthly_sales NULLS FIRST, 1, 2, 3, 4, 5, 6, 7, 8, 9
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q57()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var callCenterQuery = TpcdsData.CallCenterQuery();

        var v1 = from cs in catalogSalesQuery
                 join i in itemQuery on cs.cs_item_sk equals i.i_item_sk
                 join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                 join cc in callCenterQuery on cs.cs_call_center_sk equals cc.cc_call_center_sk
                 where d.d_year == 1999 || (d.d_year == 1998 && d.d_moy == 12) || (d.d_year == 2000 && d.d_moy == 1)
                 group cs by new
                 {
                     i.i_category,
                     i.i_brand,
                     cc.cc_name,
                     d.d_year,
                     d.d_moy
                 } into g
                 orderby g.Key.d_year, g.Key.d_moy
                 let sum_sales = g.Sum(x => x.cs_sales_price)
                 select new
                 {
                     g.Key.i_category,
                     g.Key.i_brand,
                     g.Key.cc_name,
                     g.Key.d_year,
                     g.Key.d_moy,
                     avg_monthly_sales = sum_sales.Average().Over(new { g.Key.i_category, g.Key.i_brand, g.Key.cc_name, g.Key.d_year }),
                     sum_sales,
                     psum = sum_sales.Shift(1).Over(new { g.Key.i_category, g.Key.i_brand, g.Key.cc_name }),
                     nsum = sum_sales.Shift(-1).Over(new { g.Key.i_category, g.Key.i_brand, g.Key.cc_name })
                 };

        var q57 = (from x in v1
                   where x.d_year == 1999 && x.avg_monthly_sales > 0.0
                      && x.psum != null && x.nsum != null
                      && (Math.Abs((double)x.sum_sales! - x.avg_monthly_sales) / x.avg_monthly_sales > 0.1)
                   orderby (double?)x.sum_sales - x.avg_monthly_sales,
                           x.i_category,
                           x.i_brand,
                           x.cc_name,
                           x.d_year,
                           x.d_moy,
                           x.avg_monthly_sales,
                           x.sum_sales,
                           x.psum,
                           x.nsum
                   select x)
                  .Take(100);

        AssertPlan(q57, NativePushdown);

        var polarsResult = q57.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH v1 AS
              (SELECT i_category,
                      i_brand,
                      cc_name,
                      d_year,
                      d_moy,
                      sum(cs_sales_price) sum_sales,
                      avg(sum(cs_sales_price)) OVER (PARTITION BY i_category,
                                                                  i_brand,
                                                                  cc_name,
                                                                  d_year) avg_monthly_sales,
                      rank() OVER (PARTITION BY i_category,
                                                i_brand,
                                                cc_name
                                   ORDER BY d_year,
                                            d_moy) rn
               FROM read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CallCenterPath}')
               WHERE cs_item_sk = i_item_sk
                 AND cs_sold_date_sk = d_date_sk
                 AND cc_call_center_sk= cs_call_center_sk
                 AND (d_year = 1999
                      OR (d_year = 1999-1
                          AND d_moy =12)
                      OR (d_year = 1999+1
                          AND d_moy =1))
               GROUP BY i_category,
                        i_brand,
                        cc_name,
                        d_year,
                        d_moy),
                 v2 AS
              (SELECT v1.i_category,
                      v1.i_brand,
                      v1.cc_name,
                      v1.d_year,
                      v1.d_moy,
                      v1.avg_monthly_sales,
                      v1.sum_sales,
                      v1_lag.sum_sales psum,
                      v1_lead.sum_sales nsum
               FROM v1,
                    v1 v1_lag,
                    v1 v1_lead
               WHERE v1.i_category = v1_lag.i_category
                 AND v1.i_category = v1_lead.i_category
                 AND v1.i_brand = v1_lag.i_brand
                 AND v1.i_brand = v1_lead.i_brand
                 AND v1.cc_name = v1_lag.cc_name
                 AND v1.cc_name = v1_lead.cc_name
                 AND v1.rn = v1_lag.rn + 1
                 AND v1.rn = v1_lead.rn - 1)
            SELECT *
            FROM v2
            WHERE d_year = 1999
              AND avg_monthly_sales > 0
              AND CASE
                      WHEN avg_monthly_sales > 0 THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
                      ELSE NULL
                  END > 0.1
            ORDER BY sum_sales - avg_monthly_sales NULLS FIRST, 1, 2, 3, 4, 5, 6, 7, 8, 9
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_category = r.IsDBNull(0) ? null : r.GetString(0),
            i_brand = r.IsDBNull(1) ? null : r.GetString(1),
            cc_name = r.IsDBNull(2) ? null : r.GetString(2),
            d_year = r.GetInt64(3),
            d_moy = r.GetInt64(4),
            avg_monthly_sales = r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5),
            sum_sales = r.IsDBNull(6) ? (decimal?)null : r.GetDecimal(6),
            psum = r.IsDBNull(7) ? (decimal?)null : r.GetDecimal(7),
            nsum = r.IsDBNull(8) ? (decimal?)null : r.GetDecimal(8)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.i_category, p.i_category);
            Assert.Equal(d.i_brand, p.i_brand);
            Assert.Equal(d.cc_name, p.cc_name);
            Assert.Equal(d.d_year, p.d_year);
            Assert.Equal(d.d_moy, p.d_moy);
            Assert.Equal((double)(d.avg_monthly_sales ?? 0m), p.avg_monthly_sales, 4);
            Assert.Equal(d.sum_sales, p.sum_sales);
            Assert.Equal(d.psum, p.psum);
            Assert.Equal(d.nsum, p.nsum);
        }
    }

    /*
     * TPC-DS Query 58
     *
     * WITH ss_items AS
     *   (SELECT i_item_id item_id,
     *           sum(ss_ext_sales_price) ss_item_rev
     *    FROM store_sales,
     *         item,
     *         date_dim
     *    WHERE ss_item_sk = i_item_sk
     *      AND d_date IN
     *        (SELECT d_date
     *         FROM date_dim
     *         WHERE d_week_seq =
     *             (SELECT d_week_seq
     *              FROM date_dim
     *              WHERE d_date = '2000-01-03'))
     *      AND ss_sold_date_sk = d_date_sk
     *    GROUP BY i_item_id),
     *      cs_items AS
     *   (SELECT i_item_id item_id,
     *           sum(cs_ext_sales_price) cs_item_rev
     *    FROM catalog_sales,
     *         item,
     *         date_dim
     *    WHERE cs_item_sk = i_item_sk
     *      AND d_date IN
     *        (SELECT d_date
     *         FROM date_dim
     *         WHERE d_week_seq =
     *             (SELECT d_week_seq
     *              FROM date_dim
     *              WHERE d_date = '2000-01-03'))
     *      AND cs_sold_date_sk = d_date_sk
     *    GROUP BY i_item_id),
     *      ws_items AS
     *   (SELECT i_item_id item_id,
     *           sum(ws_ext_sales_price) ws_item_rev
     *    FROM web_sales,
     *         item,
     *         date_dim
     *    WHERE ws_item_sk = i_item_sk
     *      AND d_date IN
     *        (SELECT d_date
     *         FROM date_dim
     *         WHERE d_week_seq =
     *             (SELECT d_week_seq
     *              FROM date_dim
     *              WHERE d_date = '2000-01-03'))
     *      AND ws_sold_date_sk = d_date_sk
     *    GROUP BY i_item_id)
     * SELECT ss_items.item_id,
     *        ss_item_rev,
     *        ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev,
     *        cs_item_rev,
     *        cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev,
     *        ws_item_rev,
     *        ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev,
     *        (ss_item_rev+cs_item_rev+ws_item_rev)/3 average
     * FROM ss_items,
     *      cs_items,
     *      ws_items
     * WHERE ss_items.item_id=cs_items.item_id
     *   AND ss_items.item_id=ws_items.item_id
     *   AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
     *   AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
     *   AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
     *   AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
     *   AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
     *   AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
     * ORDER BY ss_items.item_id NULLS FIRST,
     *          ss_item_rev NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q58()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var targetDate = new DateOnly(2000, 1, 3);
        var targetWeekSeq = dateDimQuery
            .Where(d => d.d_date == targetDate)
            .Select(d => d.d_week_seq)
            .First();

        var targetDates = dateDimQuery
            .Where(d => d.d_week_seq == targetWeekSeq)
            .Select(d => new { d.d_date_sk })
            .Distinct();

        var ssItems = from ss in storeSalesQuery
                      join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                      join d in targetDates on ss.ss_sold_date_sk equals d.d_date_sk
                      group ss by i.i_item_id into g
                      select new
                      {
                          item_id = g.Key,
                          ss_item_rev = g.Sum(x => x.ss_ext_sales_price)
                      };

        var csItems = from cs in catalogSalesQuery
                      join i in itemQuery on cs.cs_item_sk equals i.i_item_sk
                      join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                      group cs by i.i_item_id into g
                      select new
                      {
                          item_id = g.Key,
                          cs_item_rev = g.Sum(x => x.cs_ext_sales_price)
                      };

        var wsItems = from ws in webSalesQuery
                      join i in itemQuery on ws.ws_item_sk equals i.i_item_sk
                      join d in targetDates on ws.ws_sold_date_sk equals d.d_date_sk
                      group ws by i.i_item_id into g
                      select new
                      {
                          item_id = g.Key,
                          ws_item_rev = g.Sum(x => x.ws_ext_sales_price)
                      };

        var q58 = (from ss in ssItems
                   join cs in csItems on ss.item_id equals cs.item_id
                   join ws in wsItems on ss.item_id equals ws.item_id
                   where ss.ss_item_rev >= 0.9m * cs.cs_item_rev && ss.ss_item_rev <= 1.1m * cs.cs_item_rev
                      && ss.ss_item_rev >= 0.9m * ws.ws_item_rev && ss.ss_item_rev <= 1.1m * ws.ws_item_rev
                      && cs.cs_item_rev >= 0.9m * ss.ss_item_rev && cs.cs_item_rev <= 1.1m * ss.ss_item_rev
                      && cs.cs_item_rev >= 0.9m * ws.ws_item_rev && cs.cs_item_rev <= 1.1m * ws.ws_item_rev
                      && ws.ws_item_rev >= 0.9m * ss.ss_item_rev && ws.ws_item_rev <= 1.1m * ss.ss_item_rev
                      && ws.ws_item_rev >= 0.9m * cs.cs_item_rev && ws.ws_item_rev <= 1.1m * cs.cs_item_rev
                   let avg_rev = ((double?)ss.ss_item_rev + (double?)cs.cs_item_rev + (double?)ws.ws_item_rev) / 3.0
                   orderby ss.item_id, ss.ss_item_rev
                   select new
                   {
                       ss.item_id,
                       ss.ss_item_rev,
                       ss_dev = (double?)ss.ss_item_rev / avg_rev * 100.0,
                       cs.cs_item_rev,
                       cs_dev = (double?)cs.cs_item_rev / avg_rev * 100.0,
                       ws.ws_item_rev,
                       ws_dev = (double?)ws.ws_item_rev / avg_rev * 100.0,
                       average = avg_rev
                   })
                  .Take(100);

        AssertPlan(q58, NativePushdown);

        var polarsResult = q58.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH ss_items AS
              (SELECT i_item_id item_id,
                      sum(ss_ext_sales_price) ss_item_rev
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE ss_item_sk = i_item_sk
                 AND d_date IN
                   (SELECT d_date
                    FROM read_parquet('{TpcdsData.DateDimPath}')
                    WHERE d_week_seq =
                        (SELECT d_week_seq
                         FROM read_parquet('{TpcdsData.DateDimPath}')
                         WHERE d_date = '2000-01-03'))
                 AND ss_sold_date_sk = d_date_sk
               GROUP BY i_item_id),
                 cs_items AS
              (SELECT i_item_id item_id,
                      sum(cs_ext_sales_price) cs_item_rev
               FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE cs_item_sk = i_item_sk
                 AND d_date IN
                   (SELECT d_date
                    FROM read_parquet('{TpcdsData.DateDimPath}')
                    WHERE d_week_seq =
                        (SELECT d_week_seq
                         FROM read_parquet('{TpcdsData.DateDimPath}')
                         WHERE d_date = '2000-01-03'))
                 AND cs_sold_date_sk = d_date_sk
               GROUP BY i_item_id),
                 ws_items AS
              (SELECT i_item_id item_id,
                      sum(ws_ext_sales_price) ws_item_rev
               FROM read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE ws_item_sk = i_item_sk
                 AND d_date IN
                   (SELECT d_date
                    FROM read_parquet('{TpcdsData.DateDimPath}')
                    WHERE d_week_seq =
                        (SELECT d_week_seq
                         FROM read_parquet('{TpcdsData.DateDimPath}')
                         WHERE d_date = '2000-01-03'))
                 AND ws_sold_date_sk = d_date_sk
               GROUP BY i_item_id)
            SELECT ss_items.item_id,
                   ss_item_rev,
                   ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev,
                   cs_item_rev,
                   cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev,
                   ws_item_rev,
                   ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev,
                   (ss_item_rev+cs_item_rev+ws_item_rev)/3 average
            FROM ss_items,
                 cs_items,
                 ws_items
            WHERE ss_items.item_id=cs_items.item_id
              AND ss_items.item_id=ws_items.item_id
              AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
              AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
              AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
              AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
              AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
              AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
            ORDER BY ss_items.item_id NULLS FIRST,
                     ss_item_rev NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            item_id = r.IsDBNull(0) ? null : r.GetString(0),
            ss_item_rev = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1),
            ss_dev = r.IsDBNull(2) ? (double?)null : (double)r.GetDecimal(2),
            cs_item_rev = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3),
            cs_dev = r.IsDBNull(4) ? (double?)null : (double)r.GetDecimal(4),
            ws_item_rev = r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5),
            ws_dev = r.IsDBNull(6) ? (double?)null : (double)r.GetDecimal(6),
            average = r.IsDBNull(7) ? (double?)null : (double)r.GetDecimal(7)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.item_id, p.item_id);
            Assert.Equal(d.ss_item_rev, p.ss_item_rev);
            Assert.Equal(d.ss_dev ?? 0.0, p.ss_dev ?? 0.0, 4);
            Assert.Equal(d.cs_item_rev, p.cs_item_rev);
            Assert.Equal(d.cs_dev ?? 0.0, p.cs_dev ?? 0.0, 4);
            Assert.Equal(d.ws_item_rev, p.ws_item_rev);
            Assert.Equal(d.ws_dev ?? 0.0, p.ws_dev ?? 0.0, 4);
            Assert.Equal(d.average ?? 0.0, p.average ?? 0.0, 4);
        }
    }

    /*
     * TPC-DS Query 59
     *
     * WITH wss AS
     *   (SELECT d_week_seq,
     *           ss_store_sk,
     *           sum(CASE WHEN (d_day_name='Sunday') THEN ss_sales_price ELSE NULL END) sun_sales,
     *           sum(CASE WHEN (d_day_name='Monday') THEN ss_sales_price ELSE NULL END) mon_sales,
     *           sum(CASE WHEN (d_day_name='Tuesday') THEN ss_sales_price ELSE NULL END) tue_sales,
     *           sum(CASE WHEN (d_day_name='Wednesday') THEN ss_sales_price ELSE NULL END) wed_sales,
     *           sum(CASE WHEN (d_day_name='Thursday') THEN ss_sales_price ELSE NULL END) thu_sales,
     *           sum(CASE WHEN (d_day_name='Friday') THEN ss_sales_price ELSE NULL END) fri_sales,
     *           sum(CASE WHEN (d_day_name='Saturday') THEN ss_sales_price ELSE NULL END) sat_sales
     *    FROM store_sales,
     *         date_dim
     *    WHERE d_date_sk = ss_sold_date_sk
     *    GROUP BY d_week_seq,
     *             ss_store_sk)
     * SELECT s_store_name1,
     *        s_store_id1,
     *        d_week_seq1,
     *        sun_sales1/sun_sales2 AS sun_sales_ratio,
     *        mon_sales1/mon_sales2 AS mon_sales_ratio,
     *        tue_sales1/tue_sales2 AS tue_sales_ratio,
     *        wed_sales1/wed_sales2 AS wed_sales_ratio,
     *        thu_sales1/thu_sales2 AS thu_sales_ratio,
     *        fri_sales1/fri_sales2 AS fri_sales_ratio,
     *        sat_sales1/sat_sales2 AS sat_sales_ratio
     * FROM
     *   (SELECT s_store_name s_store_name1,
     *           wss.d_week_seq d_week_seq1,
     *           s_store_id s_store_id1,
     *           sun_sales sun_sales1,
     *           mon_sales mon_sales1,
     *           tue_sales tue_sales1,
     *           wed_sales wed_sales1,
     *           thu_sales thu_sales1,
     *           fri_sales fri_sales1,
     *           sat_sales sat_sales1
     *    FROM wss,
     *         store,
     *         date_dim d
     *    WHERE d.d_week_seq = wss.d_week_seq
     *      AND ss_store_sk = s_store_sk
     *      AND d_month_seq BETWEEN 1212 AND 1212 + 11) y,
     *   (SELECT s_store_name s_store_name2,
     *           wss.d_week_seq d_week_seq2,
     *           s_store_id s_store_id2,
     *           sun_sales sun_sales2,
     *           mon_sales mon_sales2,
     *           tue_sales tue_sales2,
     *           wed_sales wed_sales2,
     *           thu_sales thu_sales2,
     *           fri_sales fri_sales2,
     *           sat_sales sat_sales2
     *    FROM wss,
     *         store,
     *         date_dim d
     *    WHERE d.d_week_seq = wss.d_week_seq
     *      AND ss_store_sk = s_store_sk
     *      AND d_month_seq BETWEEN 1212 + 12 AND 1212 + 23) x
     * WHERE s_store_id1=s_store_id2
     *   AND d_week_seq1=d_week_seq2-52
     * ORDER BY s_store_name1 NULLS FIRST,
     *          s_store_id1 NULLS FIRST,
     *          d_week_seq1 NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q59()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var wss = from ss in storeSalesQuery
                  join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                  group new { ss, d } by new { d.d_week_seq, ss.ss_store_sk } into g
                  select new
                  {
                      g.Key.d_week_seq,
                      g.Key.ss_store_sk,
                      sun_sales = g.Sum(x => x.d.d_day_name == "Sunday" ? x.ss.ss_sales_price : null),
                      mon_sales = g.Sum(x => x.d.d_day_name == "Monday" ? x.ss.ss_sales_price : null),
                      tue_sales = g.Sum(x => x.d.d_day_name == "Tuesday" ? x.ss.ss_sales_price : null),
                      wed_sales = g.Sum(x => x.d.d_day_name == "Wednesday" ? x.ss.ss_sales_price : null),
                      thu_sales = g.Sum(x => x.d.d_day_name == "Thursday" ? x.ss.ss_sales_price : null),
                      fri_sales = g.Sum(x => x.d.d_day_name == "Friday" ? x.ss.ss_sales_price : null),
                      sat_sales = g.Sum(x => x.d.d_day_name == "Saturday" ? x.ss.ss_sales_price : null)
                  };

        var y = from w in wss
                join s in storeQuery on w.ss_store_sk equals s.s_store_sk
                join d in dateDimQuery on w.d_week_seq equals d.d_week_seq
                where d.d_month_seq >= 1212 && d.d_month_seq <= 1223
                select new
                {
                    s_store_name1 = s.s_store_name,
                    d_week_seq1 = w.d_week_seq,
                    s_store_id1 = s.s_store_id,
                    sun_sales1 = w.sun_sales,
                    mon_sales1 = w.mon_sales,
                    tue_sales1 = w.tue_sales,
                    wed_sales1 = w.wed_sales,
                    thu_sales1 = w.thu_sales,
                    fri_sales1 = w.fri_sales,
                    sat_sales1 = w.sat_sales
                };

        var x = from w in wss
                join s in storeQuery on w.ss_store_sk equals s.s_store_sk
                join d in dateDimQuery on w.d_week_seq equals d.d_week_seq
                where d.d_month_seq >= 1224 && d.d_month_seq <= 1235
                select new
                {
                    s_store_name2 = s.s_store_name,
                    d_week_seq2 = w.d_week_seq,
                    s_store_id2 = s.s_store_id,
                    sun_sales2 = w.sun_sales,
                    mon_sales2 = w.mon_sales,
                    tue_sales2 = w.tue_sales,
                    wed_sales2 = w.wed_sales,
                    thu_sales2 = w.thu_sales,
                    fri_sales2 = w.fri_sales,
                    sat_sales2 = w.sat_sales
                };

        var q59 = (from yRow in y
                   join xRow in x on new { store_id = yRow.s_store_id1, week_seq = yRow.d_week_seq1 }
                              equals new { store_id = xRow.s_store_id2, week_seq = (long?)(xRow.d_week_seq2 - 52) }
                   orderby yRow.s_store_name1, yRow.s_store_id1, yRow.d_week_seq1
                   select new
                   {
                       yRow.s_store_name1,
                       yRow.s_store_id1,
                       yRow.d_week_seq1,
                       sun_sales_ratio = (double?)yRow.sun_sales1 / (double?)xRow.sun_sales2,
                       mon_sales_ratio = (double?)yRow.mon_sales1 / (double?)xRow.mon_sales2,
                       tue_sales_ratio = (double?)yRow.tue_sales1 / (double?)xRow.tue_sales2,
                       wed_sales_ratio = (double?)yRow.wed_sales1 / (double?)xRow.wed_sales2,
                       thu_sales_ratio = (double?)yRow.thu_sales1 / (double?)xRow.thu_sales2,
                       fri_sales_ratio = (double?)yRow.fri_sales1 / (double?)xRow.fri_sales2,
                       sat_sales_ratio = (double?)yRow.sat_sales1 / (double?)xRow.sat_sales2
                   })
                  .Take(100);

        AssertPlan(q59, NativePushdown);

        var polarsResult = q59.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH wss AS
              (SELECT d_week_seq,
                      ss_store_sk,
                      sum(CASE
                              WHEN (d_day_name='Sunday') THEN ss_sales_price
                              ELSE NULL
                          END) sun_sales,
                      sum(CASE
                              WHEN (d_day_name='Monday') THEN ss_sales_price
                              ELSE NULL
                          END) mon_sales,
                      sum(CASE
                              WHEN (d_day_name='Tuesday') THEN ss_sales_price
                              ELSE NULL
                          END) tue_sales,
                      sum(CASE
                              WHEN (d_day_name='Wednesday') THEN ss_sales_price
                              ELSE NULL
                          END) wed_sales,
                      sum(CASE
                              WHEN (d_day_name='Thursday') THEN ss_sales_price
                              ELSE NULL
                          END) thu_sales,
                      sum(CASE
                              WHEN (d_day_name='Friday') THEN ss_sales_price
                              ELSE NULL
                          END) fri_sales,
                      sum(CASE
                              WHEN (d_day_name='Saturday') THEN ss_sales_price
                              ELSE NULL
                          END) sat_sales
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE d_date_sk = ss_sold_date_sk
               GROUP BY d_week_seq,
                        ss_store_sk)
            SELECT s_store_name1,
                   s_store_id1,
                   d_week_seq1,
                   sun_sales1/sun_sales2 AS sun_sales_ratio,
                   mon_sales1/mon_sales2 AS mon_sales_ratio,
                   tue_sales1/tue_sales2 AS tue_sales_ratio,
                   wed_sales1/wed_sales2 AS wed_sales_ratio,
                   thu_sales1/thu_sales2 AS thu_sales_ratio,
                   fri_sales1/fri_sales2 AS fri_sales_ratio,
                   sat_sales1/sat_sales2 AS sat_sales_ratio
            FROM
              (SELECT s_store_name s_store_name1,
                      wss.d_week_seq d_week_seq1,
                      s_store_id s_store_id1,
                      sun_sales sun_sales1,
                      mon_sales mon_sales1,
                      tue_sales tue_sales1,
                      wed_sales wed_sales1,
                      thu_sales thu_sales1,
                      fri_sales fri_sales1,
                      sat_sales sat_sales1
               FROM wss,
                    read_parquet('{TpcdsData.StorePath}') store,
                    read_parquet('{TpcdsData.DateDimPath}') d
               WHERE d.d_week_seq = wss.d_week_seq
                 AND ss_store_sk = s_store_sk
                 AND d_month_seq BETWEEN 1212 AND 1212 + 11) y,
              (SELECT s_store_name s_store_name2,
                      wss.d_week_seq d_week_seq2,
                      s_store_id s_store_id2,
                      sun_sales sun_sales2,
                      mon_sales mon_sales2,
                      tue_sales tue_sales2,
                      wed_sales wed_sales2,
                      thu_sales thu_sales2,
                      fri_sales fri_sales2,
                      sat_sales sat_sales2
               FROM wss,
                    read_parquet('{TpcdsData.StorePath}') store,
                    read_parquet('{TpcdsData.DateDimPath}') d
               WHERE d.d_week_seq = wss.d_week_seq
                 AND ss_store_sk = s_store_sk
                 AND d_month_seq BETWEEN 1212 + 12 AND 1212 + 23) x
            WHERE s_store_id1=s_store_id2
              AND d_week_seq1=d_week_seq2-52
            ORDER BY s_store_name1 NULLS FIRST,
                     s_store_id1 NULLS FIRST,
                     d_week_seq1 NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            s_store_name1 = r.IsDBNull(0) ? null : r.GetString(0),
            s_store_id1 = r.IsDBNull(1) ? null : r.GetString(1),
            d_week_seq1 = r.IsDBNull(2) ? (long?)null : r.GetInt64(2),
            sun_sales_ratio = r.IsDBNull(3) ? (double?)null : (double)r.GetDecimal(3),
            mon_sales_ratio = r.IsDBNull(4) ? (double?)null : (double)r.GetDecimal(4),
            tue_sales_ratio = r.IsDBNull(5) ? (double?)null : (double)r.GetDecimal(5),
            wed_sales_ratio = r.IsDBNull(6) ? (double?)null : (double)r.GetDecimal(6),
            thu_sales_ratio = r.IsDBNull(7) ? (double?)null : (double)r.GetDecimal(7),
            fri_sales_ratio = r.IsDBNull(8) ? (double?)null : (double)r.GetDecimal(8),
            sat_sales_ratio = r.IsDBNull(9) ? (double?)null : (double)r.GetDecimal(9)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.s_store_name1, p.s_store_name1);
            Assert.Equal(d.s_store_id1, p.s_store_id1);
            Assert.Equal(d.d_week_seq1, p.d_week_seq1);
            Assert.Equal(d.sun_sales_ratio ?? 0.0, p.sun_sales_ratio is double sr && !double.IsNaN(sr) && !double.IsInfinity(sr) ? sr : 0.0, 4);
            Assert.Equal(d.mon_sales_ratio ?? 0.0, p.mon_sales_ratio is double mr && !double.IsNaN(mr) && !double.IsInfinity(mr) ? mr : 0.0, 4);
            Assert.Equal(d.tue_sales_ratio ?? 0.0, p.tue_sales_ratio is double tr && !double.IsNaN(tr) && !double.IsInfinity(tr) ? tr : 0.0, 4);
            Assert.Equal(d.wed_sales_ratio ?? 0.0, p.wed_sales_ratio is double wr && !double.IsNaN(wr) && !double.IsInfinity(wr) ? wr : 0.0, 4);
            Assert.Equal(d.thu_sales_ratio ?? 0.0, p.thu_sales_ratio is double thr && !double.IsNaN(thr) && !double.IsInfinity(thr) ? thr : 0.0, 4);
            Assert.Equal(d.fri_sales_ratio ?? 0.0, p.fri_sales_ratio is double fr && !double.IsNaN(fr) && !double.IsInfinity(fr) ? fr : 0.0, 4);
            Assert.Equal(d.sat_sales_ratio ?? 0.0, p.sat_sales_ratio is double sar && !double.IsNaN(sar) && !double.IsInfinity(sar) ? sar : 0.0, 4);
        }
    }

    /*
     * TPC-DS Query 60
     *
     * WITH ss AS
     *   (SELECT i_item_id,
     *           sum(ss_ext_sales_price) total_sales
     *    FROM store_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_item_id IN
     *        (SELECT i_item_id
     *         FROM item
     *         WHERE i_category ='Music')
     *      AND ss_item_sk = i_item_sk
     *      AND ss_sold_date_sk = d_date_sk
     *      AND d_year = 1998
     *      AND d_moy = 9
     *      AND ss_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_item_id),
     *      cs AS
     *   (SELECT i_item_id,
     *           sum(cs_ext_sales_price) total_sales
     *    FROM catalog_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_item_id IN
     *        (SELECT i_item_id
     *         FROM item
     *         WHERE i_category ='Music')
     *      AND cs_item_sk = i_item_sk
     *      AND cs_sold_date_sk = d_date_sk
     *      AND d_year = 1998
     *      AND d_moy = 9
     *      AND cs_bill_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_item_id),
     *      ws AS
     *   (SELECT i_item_id,
     *           sum(ws_ext_sales_price) total_sales
     *    FROM web_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_item_id IN
     *        (SELECT i_item_id
     *         FROM item
     *         WHERE i_category = 'Music')
     *      AND ws_item_sk = i_item_sk
     *      AND ws_sold_date_sk = d_date_sk
     *      AND d_year = 1998
     *      AND d_moy = 9
     *      AND ws_bill_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_item_id)
     * SELECT i_item_id,
     *        sum(total_sales) total_sales
     * FROM
     *   (SELECT *
     *    FROM ss
     *    UNION ALL SELECT *
     *    FROM cs
     *    UNION ALL SELECT *
     *    FROM ws) tmp1
     * GROUP BY i_item_id
     * ORDER BY i_item_id,
     *          total_sales
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q60()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var musicItemIds = itemQuery
            .Where(i => i.i_category == "Music")
            .Select(i => new { i.i_item_id })
            .Distinct();

        var validItems = from i in itemQuery
                         join m in musicItemIds on i.i_item_id equals m.i_item_id
                         select i;

        var ss = from s in storeSalesQuery
                 join d in dateDimQuery on s.ss_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on s.ss_addr_sk equals ca.ca_address_sk
                 join i in validItems on s.ss_item_sk equals i.i_item_sk
                 where d.d_year == 1998 && d.d_moy == 9 && ca.ca_gmt_offset == -5m
                 group s by i.i_item_id into g
                 select new
                 {
                     i_item_id = g.Key,
                     total_sales = g.Sum(x => x.ss_ext_sales_price)
                 };

        var cs = from c in catalogSalesQuery
                 join d in dateDimQuery on c.cs_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on c.cs_bill_addr_sk equals ca.ca_address_sk
                 join i in validItems on c.cs_item_sk equals i.i_item_sk
                 where d.d_year == 1998 && d.d_moy == 9 && ca.ca_gmt_offset == -5m
                 group c by i.i_item_id into g
                 select new
                 {
                     i_item_id = g.Key,
                     total_sales = g.Sum(x => x.cs_ext_sales_price)
                 };

        var ws = from w in webSalesQuery
                 join d in dateDimQuery on w.ws_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on w.ws_bill_addr_sk equals ca.ca_address_sk
                 join i in validItems on w.ws_item_sk equals i.i_item_sk
                 where d.d_year == 1998 && d.d_moy == 9 && ca.ca_gmt_offset == -5m
                 group w by i.i_item_id into g
                 select new
                 {
                     i_item_id = g.Key,
                     total_sales = g.Sum(x => x.ws_ext_sales_price)
                 };

        var q60 = (from x in ss.Concat(cs).Concat(ws)
                   group x by x.i_item_id into g
                   let total_sales = g.Sum(x => x.total_sales)
                   orderby g.Key, total_sales
                   select new
                   {
                       i_item_id = g.Key,
                       total_sales = total_sales
                   })
                  .Take(100);

        AssertPlan(q60, NativePushdown);

        var polarsResult = q60.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH ss AS
              (SELECT i_item_id,
                      sum(ss_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_item_id IN
                   (SELECT i_item_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_category ='Music')
                 AND ss_item_sk = i_item_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND d_year = 1998
                 AND d_moy = 9
                 AND ss_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_item_id),
                 cs AS
              (SELECT i_item_id,
                      sum(cs_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_item_id IN
                   (SELECT i_item_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_category ='Music')
                 AND cs_item_sk = i_item_sk
                 AND cs_sold_date_sk = d_date_sk
                 AND d_year = 1998
                 AND d_moy = 9
                 AND cs_bill_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_item_id),
                 ws AS
              (SELECT i_item_id,
                      sum(ws_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_item_id IN
                   (SELECT i_item_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_category = 'Music')
                 AND ws_item_sk = i_item_sk
                 AND ws_sold_date_sk = d_date_sk
                 AND d_year = 1998
                 AND d_moy = 9
                 AND ws_bill_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_item_id)
            SELECT i_item_id,
                   sum(total_sales) total_sales
            FROM
              (SELECT *
               FROM ss
               UNION ALL SELECT *
               FROM cs
               UNION ALL SELECT *
               FROM ws) tmp1
            GROUP BY i_item_id
            ORDER BY i_item_id NULLS FIRST,
                     total_sales NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.IsDBNull(0) ? null : r.GetString(0),
            total_sales = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.total_sales, p.total_sales);
        }
    }
}

