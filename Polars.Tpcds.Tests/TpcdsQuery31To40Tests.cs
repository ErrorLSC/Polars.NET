namespace Polars.Tpcds.Tests;

public class TpcdsQuery31To40Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 31
     *
     * WITH ss AS
     *   (SELECT ca_county,
     *           d_qoy,
     *           d_year,
     *           sum(ss_ext_sales_price) AS store_sales
     *    FROM store_sales,
     *         date_dim,
     *         customer_address
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND ss_addr_sk = ca_address_sk
     *    GROUP BY ca_county,
     *             d_qoy,
     *             d_year),
     *      ws AS
     *   (SELECT ca_county,
     *           d_qoy,
     *           d_year,
     *           sum(ws_ext_sales_price) AS web_sales
     *    FROM web_sales,
     *         date_dim,
     *         customer_address
     *    WHERE ws_sold_date_sk = d_date_sk
     *      AND ws_bill_addr_sk = ca_address_sk
     *    GROUP BY ca_county,
     *             d_qoy,
     *             d_year)
     * SELECT ss1.ca_county,
     *        ss1.d_year,
     *        (ws2.web_sales*1.0000)/ws1.web_sales web_q1_q2_increase,
     *        (ss2.store_sales*1.0000)/ss1.store_sales store_q1_q2_increase,
     *        (ws3.web_sales*1.0000)/ws2.web_sales web_q2_q3_increase,
     *        (ss3.store_sales*1.0000)/ss2.store_sales store_q2_q3_increase
     * FROM ss ss1,
     *      ss ss2,
     *      ss ss3,
     *      ws ws1,
     *      ws ws2,
     *      ws ws3
     * WHERE ss1.d_qoy = 1
     *   AND ss1.d_year = 2000
     *   AND ss1.ca_county = ss2.ca_county
     *   AND ss2.d_qoy = 2
     *   AND ss2.d_year = 2000
     *   AND ss2.ca_county = ss3.ca_county
     *   AND ss3.d_qoy = 3
     *   AND ss3.d_year = 2000
     *   AND ss1.ca_county = ws1.ca_county
     *   AND ws1.d_qoy = 1
     *   AND ws1.d_year = 2000
     *   AND ws1.ca_county = ws2.ca_county
     *   AND ws2.d_qoy = 2
     *   AND ws2.d_year = 2000
     *   AND ws1.ca_county = ws3.ca_county
     *   AND ws3.d_qoy = 3
     *   AND ws3.d_year = 2000
     *   AND CASE
     *           WHEN ws1.web_sales > 0 THEN (ws2.web_sales*1.0000)/ws1.web_sales
     *           ELSE NULL
     *       END > CASE
     *                 WHEN ss1.store_sales > 0 THEN (ss2.store_sales*1.0000)/ss1.store_sales
     *                 ELSE NULL
     *             END
     *   AND CASE
     *           WHEN ws2.web_sales > 0 THEN (ws3.web_sales*1.0000)/ws2.web_sales
     *           ELSE NULL
     *       END > CASE
     *                 WHEN ss2.store_sales > 0 THEN (ss3.store_sales*1.0000)/ss2.store_sales
     *                 ELSE NULL
     *             END
     * ORDER BY ss1.ca_county;
     */
    [Fact]
    [Trait("TPCDS", "Q31")]
    public void Test_Q31()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();

        var dates2000 = dateDimQuery.Where(d => d.d_year == 2000);

        var ss = from s_sales in storeSalesQuery
                 join d in dates2000 on s_sales.ss_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on s_sales.ss_addr_sk equals ca.ca_address_sk
                 group s_sales by new { ca.ca_county, d.d_qoy, d.d_year } into g
                 select new
                 {
                     g.Key.ca_county,
                     g.Key.d_qoy,
                     g.Key.d_year,
                     store_sales = g.Sum(x => x.ss_ext_sales_price)
                 };

        var ws = from w_sales in webSalesQuery
                 join d in dates2000 on w_sales.ws_sold_date_sk equals d.d_date_sk
                 join ca in customerAddressQuery on w_sales.ws_bill_addr_sk equals ca.ca_address_sk
                 group w_sales by new { ca.ca_county, d.d_qoy, d.d_year } into g
                 select new
                 {
                     g.Key.ca_county,
                     g.Key.d_qoy,
                     g.Key.d_year,
                     web_sales = g.Sum(x => x.ws_ext_sales_price)
                 };

        var ss1 = ss.Where(x => x.d_qoy == 1);
        var ss2 = ss.Where(x => x.d_qoy == 2);
        var ss3 = ss.Where(x => x.d_qoy == 3);

        var ws1 = ws.Where(x => x.d_qoy == 1);
        var ws2 = ws.Where(x => x.d_qoy == 2);
        var ws3 = ws.Where(x => x.d_qoy == 3);

        var q31 = from s1 in ss1
                  join s2 in ss2 on s1.ca_county equals s2.ca_county
                  join s3 in ss3 on s1.ca_county equals s3.ca_county
                  join w1 in ws1 on s1.ca_county equals w1.ca_county
                  join w2 in ws2 on s1.ca_county equals w2.ca_county
                  join w3 in ws3 on s1.ca_county equals w3.ca_county
                  where (w1.web_sales > 0m && s1.store_sales > 0m &&
                         (double)w2.web_sales! / (double)w1.web_sales! > (double)s2.store_sales! / (double)s1.store_sales!)
                     && (w2.web_sales > 0m && s2.store_sales > 0m &&
                         (double)w3.web_sales! / (double)w2.web_sales! > (double)s3.store_sales! / (double)s2.store_sales!)
                  orderby s1.ca_county
                  select new
                  {
                      s1.ca_county,
                      s1.d_year,
                      web_q1_q2_increase = (double)w2.web_sales! / (double)w1.web_sales!,
                      store_q1_q2_increase = (double)s2.store_sales! / (double)s1.store_sales!,
                      web_q2_q3_increase = (double)w3.web_sales! / (double)w2.web_sales!,
                      store_q2_q3_increase = (double)s3.store_sales! / (double)s2.store_sales!
                  };

        AssertPlan(q31, NativePushdown);

        var polarsResult = q31.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH ss AS
              (SELECT ca_county,
                      d_qoy,
                      d_year,
                      sum(ss_ext_sales_price) AS store_sales
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}')
               WHERE ss_sold_date_sk = d_date_sk
                 AND ss_addr_sk = ca_address_sk
               GROUP BY ca_county,
                        d_qoy,
                        d_year),
                 ws AS
              (SELECT ca_county,
                      d_qoy,
                      d_year,
                      sum(ws_ext_sales_price) AS web_sales
               FROM read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}')
               WHERE ws_sold_date_sk = d_date_sk
                 AND ws_bill_addr_sk = ca_address_sk
               GROUP BY ca_county,
                        d_qoy,
                        d_year)
            SELECT ss1.ca_county,
                   ss1.d_year,
                   cast((ws2.web_sales*1.0000)/ws1.web_sales as double) web_q1_q2_increase,
                   cast((ss2.store_sales*1.0000)/ss1.store_sales as double) store_q1_q2_increase,
                   cast((ws3.web_sales*1.0000)/ws2.web_sales as double) web_q2_q3_increase,
                   cast((ss3.store_sales*1.0000)/ss2.store_sales as double) store_q2_q3_increase
            FROM ss ss1,
                 ss ss2,
                 ss ss3,
                 ws ws1,
                 ws ws2,
                 ws ws3
            WHERE ss1.d_qoy = 1
              AND ss1.d_year = 2000
              AND ss1.ca_county = ss2.ca_county
              AND ss2.d_qoy = 2
              AND ss2.d_year = 2000
              AND ss2.ca_county = ss3.ca_county
              AND ss3.d_qoy = 3
              AND ss3.d_year = 2000
              AND ss1.ca_county = ws1.ca_county
              AND ws1.d_qoy = 1
              AND ws1.d_year = 2000
              AND ws1.ca_county = ws2.ca_county
              AND ws2.d_qoy = 2
              AND ws2.d_year = 2000
              AND ws1.ca_county = ws3.ca_county
              AND ws3.d_qoy = 3
              AND ws3.d_year = 2000
              AND CASE
                      WHEN ws1.web_sales > 0 THEN (ws2.web_sales*1.0000)/ws1.web_sales
                      ELSE NULL
                  END > CASE
                            WHEN ss1.store_sales > 0 THEN (ss2.store_sales*1.0000)/ss1.store_sales
                            ELSE NULL
                        END
              AND CASE
                      WHEN ws2.web_sales > 0 THEN (ws3.web_sales*1.0000)/ws2.web_sales
                      ELSE NULL
                  END > CASE
                            WHEN ss2.store_sales > 0 THEN (ss3.store_sales*1.0000)/ss2.store_sales
                            ELSE NULL
                        END
            ORDER BY ss1.ca_county;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            ca_county = r.IsDBNull(0) ? null : r.GetString(0),
            d_year = r.IsDBNull(1) ? null : (long?)r.GetInt64(1),
            web_q1_q2_increase = r.IsDBNull(2) ? (double?)null : r.GetDouble(2),
            store_q1_q2_increase = r.IsDBNull(3) ? (double?)null : r.GetDouble(3),
            web_q2_q3_increase = r.IsDBNull(4) ? (double?)null : r.GetDouble(4),
            store_q2_q3_increase = r.IsDBNull(5) ? (double?)null : r.GetDouble(5),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.ca_county, p.ca_county);
            Assert.Equal(d.d_year, p.d_year);
            Assert.NotNull(d.web_q1_q2_increase);
            Assert.Equal(d.web_q1_q2_increase!.Value, p.web_q1_q2_increase, 4);

            Assert.NotNull(d.store_q1_q2_increase);
            Assert.Equal(d.store_q1_q2_increase!.Value, p.store_q1_q2_increase, 4);

            Assert.NotNull(d.web_q2_q3_increase);
            Assert.Equal(d.web_q2_q3_increase!.Value, p.web_q2_q3_increase, 4);

            Assert.NotNull(d.store_q2_q3_increase);
            Assert.Equal(d.store_q2_q3_increase!.Value, p.store_q2_q3_increase, 4);
        }
    }

    /*
     * TPC-DS Query 32
     *
     * SELECT sum(cs_ext_discount_amt) AS "excess discount amount"
     * FROM catalog_sales,
     *      item,
     *      date_dim
     * WHERE i_manufact_id = 977
     *   AND i_item_sk = cs_item_sk
     *   AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
     *   AND d_date_sk = cs_sold_date_sk
     *   AND cs_ext_discount_amt >
     *     ( SELECT 1.3 * avg(cs_ext_discount_amt)
     *       FROM catalog_sales,
     *            date_dim
     *       WHERE cs_item_sk = i_item_sk
     *         AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
     *         AND d_date_sk = cs_sold_date_sk )
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q32")]
    public void Test_Q32()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var minDate = new DateOnly(2000, 1, 27);
        var maxDate = new DateOnly(2000, 4, 26);

        var targetDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var targetItems = itemQuery.Where(i => i.i_manufact_id == 977);

        var itemAvgDiscount = from cs in catalogSalesQuery
                              join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                              group cs by cs.cs_item_sk into g
                              select new
                              {
                                  item_sk = g.Key,
                                  threshold = (decimal?)(g.Average(x => (double?)x.cs_ext_discount_amt) * 1.3)
                              };

        var q32 = (from cs in catalogSalesQuery
                   join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                   join i in targetItems on cs.cs_item_sk equals i.i_item_sk
                   join avg in itemAvgDiscount on cs.cs_item_sk equals avg.item_sk
                   where cs.cs_ext_discount_amt > avg.threshold
                   group cs by 1 into g
                   select new
                   {
                       excess_discount_amount = g.Sum(x => x.cs_ext_discount_amt)
                   }).Take(100);

        AssertPlan(q32, NativePushdown);

        var polarsResult = q32.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT sum(cs_ext_discount_amt) AS ""excess discount amount""
            FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.DateDimPath}')
            WHERE i_manufact_id = 977
              AND i_item_sk = cs_item_sk
              AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
              AND d_date_sk = cs_sold_date_sk
              AND cs_ext_discount_amt >
                ( SELECT 1.3 * avg(cs_ext_discount_amt)
                  FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                       read_parquet('{TpcdsData.DateDimPath}')
                  WHERE cs_item_sk = i_item_sk
                    AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
                    AND d_date_sk = cs_sold_date_sk )
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            excess_discount_amount = r.IsDBNull(0) ? null : (decimal?)r.GetDecimal(0),
        });

        // When DuckDB produces 1 row of NULL for an empty aggregation and Polars produces 0 rows,
        // either both are 0 rows, or DuckDB has 1 NULL row and Polars has 0 (or 1 NULL) row.
        if (duckDbResults.Count == 1 && duckDbResults[0].excess_discount_amount == null)
        {
            if (polarsResult.Count == 1)
            {
                Assert.True(polarsResult[0].excess_discount_amount == null || polarsResult[0].excess_discount_amount == 0m);
            }
            else
            {
                Assert.Empty(polarsResult);
            }
        }
        else
        {
            Assert.Equal(duckDbResults.Count, polarsResult.Count);
            for (int i = 0; i < duckDbResults.Count; i++)
            {
                Assert.Equal(duckDbResults[i].excess_discount_amount, polarsResult[i].excess_discount_amount);
            }
        }
    }

    /*
     * TPC-DS Query 33
     *
     * WITH ss AS
     *   ( SELECT i_manufact_id,
     *            sum(ss_ext_sales_price) total_sales
     *    FROM store_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_manufact_id IN
     *        (SELECT i_manufact_id
     *         FROM item
     *         WHERE i_category IN ('Electronics'))
     *      AND ss_item_sk = i_item_sk
     *      AND ss_sold_date_sk = d_date_sk
     *      AND d_year = 1998
     *      AND d_moy = 5
     *      AND ss_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_manufact_id),
     *      cs AS
     *   ( SELECT i_manufact_id,
     *            sum(cs_ext_sales_price) total_sales
     *    FROM catalog_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_manufact_id IN
     *        (SELECT i_manufact_id
     *         FROM item
     *         WHERE i_category IN ('Electronics'))
     *      AND cs_item_sk = i_item_sk
     *      AND cs_sold_date_sk = d_date_sk
     *      AND d_year = 1998
     *      AND d_moy = 5
     *      AND cs_bill_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_manufact_id),
     *      ws AS
     *   ( SELECT i_manufact_id,
     *            sum(ws_ext_sales_price) total_sales
     *    FROM web_sales,
     *         date_dim,
     *         customer_address,
     *         item
     *    WHERE i_manufact_id IN
     *        (SELECT i_manufact_id
     *         FROM item
     *         WHERE i_category IN ('Electronics'))
     *      AND ws_item_sk = i_item_sk
     *      AND ws_sold_date_sk = d_date_sk
     *      AND d_year = 1998
     *      AND d_moy = 5
     *      AND ws_bill_addr_sk = ca_address_sk
     *      AND ca_gmt_offset = -5
     *    GROUP BY i_manufact_id)
     * SELECT i_manufact_id,
     *        sum(total_sales) total_sales
     * FROM
     *   (SELECT * FROM ss
     *    UNION ALL
     *    SELECT * FROM cs
     *    UNION ALL
     *    SELECT * FROM ws) tmp1
     * GROUP BY i_manufact_id
     * ORDER BY total_sales
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q33")]
    public void Test_Q33()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var targetManufactIds = itemQuery
            .Where(i => i.i_category == "Electronics")
            .Select(i => i.i_manufact_id)
            .Distinct();

        var targetItems = from i in itemQuery
                          join m in targetManufactIds on i.i_manufact_id equals m
                          select i;

        var targetDates = dateDimQuery.Where(d => d.d_year == 1998 && d.d_moy == 5);
        var targetAddresses = customerAddressQuery.Where(ca => ca.ca_gmt_offset == -5m);

        var ss = from s in storeSalesQuery
                 join d in targetDates on s.ss_sold_date_sk equals d.d_date_sk
                 join ca in targetAddresses on s.ss_addr_sk equals ca.ca_address_sk
                 join i in targetItems on s.ss_item_sk equals i.i_item_sk
                 group s by i.i_manufact_id into g
                 select new
                 {
                     i_manufact_id = g.Key,
                     total_sales = g.Sum(x => x.ss_ext_sales_price)
                 };

        var cs = from c in catalogSalesQuery
                 join d in targetDates on c.cs_sold_date_sk equals d.d_date_sk
                 join ca in targetAddresses on c.cs_bill_addr_sk equals ca.ca_address_sk
                 join i in targetItems on c.cs_item_sk equals i.i_item_sk
                 group c by i.i_manufact_id into g
                 select new
                 {
                     i_manufact_id = g.Key,
                     total_sales = g.Sum(x => x.cs_ext_sales_price)
                 };

        var ws = from w in webSalesQuery
                 join d in targetDates on w.ws_sold_date_sk equals d.d_date_sk
                 join ca in targetAddresses on w.ws_bill_addr_sk equals ca.ca_address_sk
                 join i in targetItems on w.ws_item_sk equals i.i_item_sk
                 group w by i.i_manufact_id into g
                 select new
                 {
                     i_manufact_id = g.Key,
                     total_sales = g.Sum(x => x.ws_ext_sales_price)
                 };

        var allSales = ss.Concat(cs).Concat(ws);

        var q33 = (from x in allSales
                   group x by x.i_manufact_id into g
                   select new
                   {
                       i_manufact_id = g.Key,
                       total_sales = g.Sum(x => x.total_sales)
                   } into res
                   orderby res.total_sales
                   select res).Take(100);

        AssertPlan(q33, NativePushdown);

        var polarsResult = q33.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH ss AS
              ( SELECT i_manufact_id,
                       sum(ss_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_manufact_id IN
                   (SELECT i_manufact_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_category IN ('Electronics'))
                 AND ss_item_sk = i_item_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND d_year = 1998
                 AND d_moy = 5
                 AND ss_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_manufact_id),
                 cs AS
              ( SELECT i_manufact_id,
                       sum(cs_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_manufact_id IN
                   (SELECT i_manufact_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_category IN ('Electronics'))
                 AND cs_item_sk = i_item_sk
                 AND cs_sold_date_sk = d_date_sk
                 AND d_year = 1998
                 AND d_moy = 5
                 AND cs_bill_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_manufact_id),
                 ws AS
              ( SELECT i_manufact_id,
                       sum(ws_ext_sales_price) total_sales
               FROM read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE i_manufact_id IN
                   (SELECT i_manufact_id
                    FROM read_parquet('{TpcdsData.ItemPath}')
                    WHERE i_category IN ('Electronics'))
                 AND ws_item_sk = i_item_sk
                 AND ws_sold_date_sk = d_date_sk
                 AND d_year = 1998
                 AND d_moy = 5
                 AND ws_bill_addr_sk = ca_address_sk
                 AND ca_gmt_offset = -5
               GROUP BY i_manufact_id)
            SELECT i_manufact_id,
                   sum(total_sales) total_sales
            FROM
              (SELECT * FROM ss
               UNION ALL
               SELECT * FROM cs
               UNION ALL
               SELECT * FROM ws) tmp1
            GROUP BY i_manufact_id
            ORDER BY total_sales
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_manufact_id = r.IsDBNull(0) ? null : (long?)r.GetInt64(0),
            total_sales = r.IsDBNull(1) ? null : (decimal?)r.GetDecimal(1),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_manufact_id, p.i_manufact_id);
            Assert.Equal(d.total_sales, p.total_sales);
        }
    }

    /*
     * TPC-DS Query 34
     *
     * SELECT c_last_name,
     *        c_first_name,
     *        c_salutation,
     *        c_preferred_cust_flag,
     *        ss_ticket_number,
     *        cnt
     * FROM
     *   (SELECT ss_ticket_number,
     *           ss_customer_sk,
     *           count(*) cnt
     *    FROM store_sales,
     *         date_dim,
     *         store,
     *         household_demographics
     *    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     *      AND store_sales.ss_store_sk = store.s_store_sk
     *      AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     *      AND (date_dim.d_dom BETWEEN 1 AND 3
     *           OR date_dim.d_dom BETWEEN 25 AND 28)
     *      AND (household_demographics.hd_buy_potential = '>10000'
     *           OR household_demographics.hd_buy_potential = 'Unknown')
     *      AND household_demographics.hd_vehicle_count > 0
     *      AND (CASE
     *               WHEN household_demographics.hd_vehicle_count > 0
     *               THEN (household_demographics.hd_dep_count*1.000)/ household_demographics.hd_vehicle_count
     *               ELSE NULL
     *           END) > 1.2
     *      AND date_dim.d_year IN (1999,
     *                              1999+1,
     *                              1999+2)
     *      AND store.s_county = 'Williamson County'
     *    GROUP BY ss_ticket_number,
     *             ss_customer_sk) dn,
     *        customer
     * WHERE ss_customer_sk = c_customer_sk
     *   AND cnt BETWEEN 15 AND 20
     * ORDER BY c_last_name NULLS FIRST,
     *          c_first_name NULLS FIRST,
     *          c_salutation NULLS FIRST,
     *          c_preferred_cust_flag DESC NULLS FIRST,
     *          ss_ticket_number NULLS FIRST;
     */
    [Fact]
    [Trait("TPCDS", "Q34")]
    public void Test_Q34()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var householdDemographicsQuery = TpcdsData.HouseholdDemographicsQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        var filteredDates = dateDimQuery.Where(d => ((d.d_dom >= 1 && d.d_dom <= 3) || (d.d_dom >= 25 && d.d_dom <= 28))
                                                 && (d.d_year >= 1999 && d.d_year <= 2001));

        var filteredStores = storeQuery.Where(s => s.s_county == "Williamson County");

        var filteredHd = householdDemographicsQuery.Where(hd => (hd.hd_buy_potential == ">10000" || hd.hd_buy_potential == "Unknown")
                                                             && hd.hd_vehicle_count > 0
                                                             && (double)hd.hd_dep_count! / (double)hd.hd_vehicle_count! > 1.2);

        var dn = from ss in storeSalesQuery
                 join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                 join s in filteredStores on ss.ss_store_sk equals s.s_store_sk
                 join hd in filteredHd on ss.ss_hdemo_sk equals hd.hd_demo_sk
                 group ss by new { ss.ss_ticket_number, ss.ss_customer_sk } into g
                 where g.LongCount() >= 15 && g.LongCount() <= 20
                 select new
                 {
                     g.Key.ss_ticket_number,
                     g.Key.ss_customer_sk,
                     cnt = g.LongCount()
                 };

        var q34 = from x in dn
                  join c in customerQuery on x.ss_customer_sk equals c.c_customer_sk
                  orderby c.c_last_name,
                          c.c_first_name,
                          c.c_salutation,
                          c.c_preferred_cust_flag descending,
                          x.ss_ticket_number
                  select new
                  {
                      c.c_last_name,
                      c.c_first_name,
                      c.c_salutation,
                      c.c_preferred_cust_flag,
                      x.ss_ticket_number,
                      x.cnt
                  };

        AssertPlan(q34, NativePushdown);

        var polarsResult = q34.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT c_last_name,
                   c_first_name,
                   c_salutation,
                   c_preferred_cust_flag,
                   ss_ticket_number,
                   cnt
            FROM
              (SELECT ss_ticket_number,
                      ss_customer_sk,
                      count(*) cnt
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics
               WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
                 AND store_sales.ss_store_sk = store.s_store_sk
                 AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND (date_dim.d_dom BETWEEN 1 AND 3
                      OR date_dim.d_dom BETWEEN 25 AND 28)
                 AND (household_demographics.hd_buy_potential = '>10000'
                      OR household_demographics.hd_buy_potential = 'Unknown')
                 AND household_demographics.hd_vehicle_count > 0
                 AND (CASE
                          WHEN household_demographics.hd_vehicle_count > 0
                          THEN (household_demographics.hd_dep_count*1.000)/ household_demographics.hd_vehicle_count
                          ELSE NULL
                      END) > 1.2
                 AND date_dim.d_year IN (1999,
                                         1999+1,
                                         1999+2)
                 AND store.s_county = 'Williamson County'
               GROUP BY ss_ticket_number,
                        ss_customer_sk) dn,
                  read_parquet('{TpcdsData.CustomerPath}') customer
            WHERE ss_customer_sk = c_customer_sk
              AND cnt BETWEEN 15 AND 20
            ORDER BY c_last_name NULLS FIRST,
                     c_first_name NULLS FIRST,
                     c_salutation NULLS FIRST,
                     c_preferred_cust_flag DESC NULLS FIRST,
                     ss_ticket_number NULLS FIRST;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            c_last_name = r.IsDBNull(0) ? null : r.GetString(0),
            c_first_name = r.IsDBNull(1) ? null : r.GetString(1),
            c_salutation = r.IsDBNull(2) ? null : r.GetString(2),
            c_preferred_cust_flag = r.IsDBNull(3) ? null : r.GetString(3),
            ss_ticket_number = r.GetInt64(4),
            cnt = r.GetInt64(5),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.c_last_name, p.c_last_name);
            Assert.Equal(d.c_first_name, p.c_first_name);
            Assert.Equal(d.c_salutation, p.c_salutation);
            Assert.Equal(d.c_preferred_cust_flag, p.c_preferred_cust_flag);
            Assert.Equal(d.ss_ticket_number, p.ss_ticket_number);
            Assert.Equal(d.cnt, p.cnt);
        }
    }

    /*
     * TPC-DS Query 35
     *
     * SELECT ca_state,
     *        cd_gender,
     *        cd_marital_status,
     *        cd_dep_count,
     *        count(*) cnt1,
     *        min(cd_dep_count) min1,
     *        max(cd_dep_count) max1,
     *        avg(cd_dep_count) avg1,
     *        cd_dep_employed_count,
     *        count(*) cnt2,
     *        min(cd_dep_employed_count) min2,
     *        max(cd_dep_employed_count) max2,
     *        avg(cd_dep_employed_count) avg2,
     *        cd_dep_college_count,
     *        count(*) cnt3,
     *        min(cd_dep_college_count) min3,
     *        max(cd_dep_college_count) max3,
     *        avg(cd_dep_college_count) avg3
     * FROM customer c,
     *      customer_address ca,
     *      customer_demographics
     * WHERE c.c_current_addr_sk = ca.ca_address_sk
     *   AND cd_demo_sk = c.c_current_cdemo_sk
     *   AND EXISTS
     *     (SELECT *
     *      FROM store_sales,
     *           date_dim
     *      WHERE c.c_customer_sk = ss_customer_sk
     *        AND ss_sold_date_sk = d_date_sk
     *        AND d_year = 2002
     *        AND d_qoy < 4)
     *   AND (EXISTS
     *          (SELECT *
     *           FROM web_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = ws_bill_customer_sk
     *             AND ws_sold_date_sk = d_date_sk
     *             AND d_year = 2002
     *             AND d_qoy < 4)
     *        OR EXISTS
     *          (SELECT *
     *           FROM catalog_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = cs_ship_customer_sk
     *             AND cs_sold_date_sk = d_date_sk
     *             AND d_year = 2002
     *             AND d_qoy < 4))
     * GROUP BY ca_state,
     *          cd_gender,
     *          cd_marital_status,
     *          cd_dep_count,
     *          cd_dep_employed_count,
     *          cd_dep_college_count
     * ORDER BY ca_state NULLS FIRST,
     *          cd_gender NULLS FIRST,
     *          cd_marital_status NULLS FIRST,
     *          cd_dep_count NULLS FIRST,
     *          cd_dep_employed_count NULLS FIRST,
     *          cd_dep_college_count NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q35")]
    public void Test_Q35()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var targetDates = dateDimQuery.Where(d => d.d_year == 2002 && d.d_qoy < 4);

        var storeCustomers = (from ss in storeSalesQuery
                              join d in targetDates on ss.ss_sold_date_sk equals d.d_date_sk
                              select ss.ss_customer_sk).Distinct();

        var webCustomers = (from ws in webSalesQuery
                            join d in targetDates on ws.ws_sold_date_sk equals d.d_date_sk
                            select ws.ws_bill_customer_sk).Distinct();

        var catalogCustomers = (from cs in catalogSalesQuery
                                join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                                select cs.cs_ship_customer_sk).Distinct();

        var nonStoreCustomers = (from c in webCustomers.Concat(catalogCustomers) select c).Distinct();

        var eligibleCustomers = (from sc in storeCustomers
                                 join nsc in nonStoreCustomers on sc equals nsc
                                 select sc).Distinct();

        var eligible = from c in customerQuery
                       join ec in eligibleCustomers on c.c_customer_sk equals ec
                       join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                       join cd in customerDemographicsQuery on c.c_current_cdemo_sk equals cd.cd_demo_sk
                       select new
                       {
                           ca.ca_state,
                           cd.cd_gender,
                           cd.cd_marital_status,
                           cd.cd_dep_count,
                           cd.cd_dep_employed_count,
                           cd.cd_dep_college_count
                       };

        var q35 = (from x in eligible
                   group x by new
                   {
                       x.ca_state,
                       x.cd_gender,
                       x.cd_marital_status,
                       x.cd_dep_count,
                       x.cd_dep_employed_count,
                       x.cd_dep_college_count
                   } into g
                   orderby g.Key.ca_state,
                           g.Key.cd_gender,
                           g.Key.cd_marital_status,
                           g.Key.cd_dep_count,
                           g.Key.cd_dep_employed_count,
                           g.Key.cd_dep_college_count
                   select new
                   {
                       g.Key.ca_state,
                       g.Key.cd_gender,
                       g.Key.cd_marital_status,
                       g.Key.cd_dep_count,
                       cnt1 = g.LongCount(),
                       min1 = g.Min(x => x.cd_dep_count),
                       max1 = g.Max(x => x.cd_dep_count),
                       avg1 = g.Average(x => (double?)x.cd_dep_count),
                       g.Key.cd_dep_employed_count,
                       cnt2 = g.LongCount(),
                       min2 = g.Min(x => x.cd_dep_employed_count),
                       max2 = g.Max(x => x.cd_dep_employed_count),
                       avg2 = g.Average(x => (double?)x.cd_dep_employed_count),
                       g.Key.cd_dep_college_count,
                       cnt3 = g.LongCount(),
                       min3 = g.Min(x => x.cd_dep_college_count),
                       max3 = g.Max(x => x.cd_dep_college_count),
                       avg3 = g.Average(x => (double?)x.cd_dep_college_count)
                   }).Take(100);

        AssertPlan(q35, NativePushdown);

        var polarsResult = q35.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT ca_state,
                   cd_gender,
                   cd_marital_status,
                   cd_dep_count,
                   count(*) cnt1,
                   min(cd_dep_count) min1,
                   max(cd_dep_count) max1,
                   avg(cd_dep_count) avg1,
                   cd_dep_employed_count,
                   count(*) cnt2,
                   min(cd_dep_employed_count) min2,
                   max(cd_dep_employed_count) max2,
                   avg(cd_dep_employed_count) avg2,
                   cd_dep_college_count,
                   count(*) cnt3,
                   min(cd_dep_college_count) min3,
                   max(cd_dep_college_count) max3,
                   avg(cd_dep_college_count) avg3
            FROM read_parquet('{TpcdsData.CustomerPath}') c,
                 read_parquet('{TpcdsData.CustomerAddressPath}') ca,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}') cd
            WHERE c.c_current_addr_sk = ca.ca_address_sk
              AND cd.cd_demo_sk = c.c_current_cdemo_sk
              AND EXISTS
                (SELECT *
                 FROM read_parquet('{TpcdsData.StoreSalesPath}') ss,
                      read_parquet('{TpcdsData.DateDimPath}') d
                 WHERE c.c_customer_sk = ss.ss_customer_sk
                   AND ss_sold_date_sk = d_date_sk
                   AND d.d_year = 2002
                   AND d.d_qoy < 4)
              AND (EXISTS
                     (SELECT *
                      FROM read_parquet('{TpcdsData.WebSalesPath}') ws,
                           read_parquet('{TpcdsData.DateDimPath}') d
                      WHERE c.c_customer_sk = ws.ws_bill_customer_sk
                        AND ws_sold_date_sk = d_date_sk
                        AND d.d_year = 2002
                        AND d.d_qoy < 4)
                   OR EXISTS
                     (SELECT *
                      FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs,
                           read_parquet('{TpcdsData.DateDimPath}') d
                      WHERE c.c_customer_sk = cs.cs_ship_customer_sk
                        AND cs_sold_date_sk = d_date_sk
                        AND d.d_year = 2002
                        AND d.d_qoy < 4))
            GROUP BY ca_state,
                     cd_gender,
                     cd_marital_status,
                     cd_dep_count,
                     cd_dep_employed_count,
                     cd_dep_college_count
            ORDER BY ca_state NULLS FIRST,
                     cd_gender NULLS FIRST,
                     cd_marital_status NULLS FIRST,
                     cd_dep_count NULLS FIRST,
                     cd_dep_employed_count NULLS FIRST,
                     cd_dep_college_count NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            ca_state = r.IsDBNull(0) ? null : r.GetString(0),
            cd_gender = r.IsDBNull(1) ? null : r.GetString(1),
            cd_marital_status = r.IsDBNull(2) ? null : r.GetString(2),
            cd_dep_count = r.IsDBNull(3) ? null : (long?)r.GetInt64(3),
            cnt1 = r.GetInt64(4),
            min1 = r.IsDBNull(5) ? null : (long?)r.GetInt64(5),
            max1 = r.IsDBNull(6) ? null : (long?)r.GetInt64(6),
            avg1 = r.IsDBNull(7) ? (double?)null : r.GetDouble(7),
            cd_dep_employed_count = r.IsDBNull(8) ? null : (long?)r.GetInt64(8),
            cnt2 = r.GetInt64(9),
            min2 = r.IsDBNull(10) ? null : (long?)r.GetInt64(10),
            max2 = r.IsDBNull(11) ? null : (long?)r.GetInt64(11),
            avg2 = r.IsDBNull(12) ? (double?)null : r.GetDouble(12),
            cd_dep_college_count = r.IsDBNull(13) ? null : (int?)r.GetInt32(13),
            cnt3 = r.GetInt64(14),
            min3 = r.IsDBNull(15) ? null : (int?)r.GetInt32(15),
            max3 = r.IsDBNull(16) ? null : (int?)r.GetInt32(16),
            avg3 = r.IsDBNull(17) ? (double?)null : r.GetDouble(17),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.ca_state, p.ca_state);
            Assert.Equal(d.cd_gender, p.cd_gender);
            Assert.Equal(d.cd_marital_status, p.cd_marital_status);
            Assert.Equal(d.cd_dep_count, p.cd_dep_count);
            Assert.Equal(d.cnt1, p.cnt1);
            Assert.Equal(d.min1, p.min1);
            Assert.Equal(d.max1, p.max1);
            if (d.avg1 == null) Assert.Null(p.avg1); else Assert.Equal(d.avg1.Value, p.avg1!.Value, 4);

            Assert.Equal(d.cd_dep_employed_count, p.cd_dep_employed_count);
            Assert.Equal(d.cnt2, p.cnt2);
            Assert.Equal(d.min2, p.min2);
            Assert.Equal(d.max2, p.max2);
            if (d.avg2 == null) Assert.Null(p.avg2); else Assert.Equal(d.avg2.Value, p.avg2!.Value, 4);

            Assert.Equal(d.cd_dep_college_count, p.cd_dep_college_count);
            Assert.Equal(d.cnt3, p.cnt3);
            Assert.Equal(d.min3, p.min3);
            Assert.Equal(d.max3, p.max3);
            if (d.avg3 == null) Assert.Null(p.avg3); else Assert.Equal(d.avg3.Value, p.avg3!.Value, 4);
        }
    }

    /*
     * TPC-DS Query 36
     *
     * WITH results AS
     *   (SELECT sum(ss_net_profit) AS ss_net_profit,
     *           sum(ss_ext_sales_price) AS ss_ext_sales_price,
     *           (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
     *           i_category,
     *           i_class,
     *           0 AS g_category,
     *           0 AS g_class
     *    FROM store_sales,
     *         date_dim d1,
     *         item,
     *         store
     *    WHERE d1.d_year = 2001
     *      AND d1.d_date_sk = ss_sold_date_sk
     *      AND i_item_sk = ss_item_sk
     *      AND s_store_sk = ss_store_sk
     *      AND s_state = 'TN'
     *    GROUP BY i_category,
     *             i_class),
     *      results_rollup AS
     *   (SELECT gross_margin,
     *           i_category,
     *           i_class,
     *           0 AS t_category,
     *           0 AS t_class,
     *           0 AS lochierarchy
     *    FROM results
     *    UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
     *                 i_category,
     *                 NULL AS i_class,
     *                 0 AS t_category,
     *                 1 AS t_class,
     *                 1 AS lochierarchy
     *    FROM results
     *    GROUP BY i_category
     *    UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
     *                 NULL AS i_category,
     *                 NULL AS i_class,
     *                 1 AS t_category,
     *                 1 AS t_class,
     *                 2 AS lochierarchy
     *    FROM results)
     * SELECT gross_margin,
     *        i_category,
     *        i_class,
     *        lochierarchy,
     *        rank() OVER (PARTITION BY lochierarchy,
     *                                  CASE
     *                                      WHEN t_class = 0 THEN i_category
     *                                  END
     *                     ORDER BY gross_margin ASC) AS rank_within_parent
     * FROM results_rollup
     * ORDER BY lochierarchy DESC NULLS FIRST,
     *          CASE
     *              WHEN lochierarchy = 0 THEN i_category
     *          END NULLS FIRST,
     *          rank_within_parent NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q36")]
    public void Test_Q36()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_year == 2001);
        var filteredStores = storeQuery.Where(s => s.s_state == "TN");

        var baseSales = from ss in storeSalesQuery
                        join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                        join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                        join s in filteredStores on ss.ss_store_sk equals s.s_store_sk
                        select new
                        {
                            i.i_category,
                            i.i_class,
                            ss.ss_net_profit,
                            ss.ss_ext_sales_price
                        };

        var results = from x in baseSales
                      group x by new { x.i_category, x.i_class } into g
                      select new
                      {
                          g.Key.i_category,
                          g.Key.i_class,
                          ss_net_profit = g.Sum(x => x.ss_net_profit),
                          ss_ext_sales_price = g.Sum(x => x.ss_ext_sales_price),
                          gross_margin = (double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!
                      };

        var level0 = from r in results
                     select new
                     {
                         gross_margin = r.gross_margin,
                         i_category = r.i_category,
                         i_class = r.i_class,
                         t_category = 0,
                         t_class = 0,
                         lochierarchy = 0
                     };

        var level1 = from r in results
                     group r by r.i_category into g
                     select new
                     {
                         gross_margin = (double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!,
                         i_category = g.Key,
                         i_class = (string?)null,
                         t_category = 0,
                         t_class = 1,
                         lochierarchy = 1
                     };

        var level2 = from r in results
                     group r by 1 into g
                     select new
                     {
                         gross_margin = (double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!,
                         i_category = (string?)null,
                         i_class = (string?)null,
                         t_category = 1,
                         t_class = 1,
                         lochierarchy = 2
                     };

        var resultsRollup = level0.Concat(level1).Concat(level2);

        var q36 = (from r in resultsRollup
                   select new
                   {
                       r.gross_margin,
                       r.i_category,
                       r.i_class,
                       r.lochierarchy,
                       parent = r.t_class == 0 ? r.i_category : null,
                       rank_within_parent = (long)r.gross_margin.Rank(RankMethod.Min).Over(new { r.lochierarchy, parent = r.t_class == 0 ? r.i_category : null })
                   } into res
                   orderby res.lochierarchy descending,
                           res.lochierarchy == 0 ? res.i_category : null,
                           res.rank_within_parent
                   select new
                   {
                       res.gross_margin,
                       res.i_category,
                       res.i_class,
                       res.lochierarchy,
                       res.rank_within_parent
                   }).Take(100);

        AssertPlan(q36, NativePushdown);

        var polarsResult = q36.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH results AS
              (SELECT sum(ss_net_profit) AS ss_net_profit,
                      sum(ss_ext_sales_price) AS ss_ext_sales_price,
                      (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
                      i_category,
                      i_class,
                      0 AS g_category,
                      0 AS g_class
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}') d1,
                    read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.StorePath}')
               WHERE d1.d_year = 2001
                 AND d1.d_date_sk = ss_sold_date_sk
                 AND i_item_sk = ss_item_sk
                 AND s_store_sk = ss_store_sk
                 AND s_state = 'TN'
               GROUP BY i_category,
                        i_class),
                 results_rollup AS
              (SELECT gross_margin,
                      i_category,
                      i_class,
                      0 AS t_category,
                      0 AS t_class,
                      0 AS lochierarchy
               FROM results
               UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
                            i_category,
                            NULL AS i_class,
                            0 AS t_category,
                            1 AS t_class,
                            1 AS lochierarchy
               FROM results
               GROUP BY i_category
               UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
                            NULL AS i_category,
                            NULL AS i_class,
                            1 AS t_category,
                            1 AS t_class,
                            2 AS lochierarchy
               FROM results)
            SELECT gross_margin,
                   i_category,
                   i_class,
                   lochierarchy,
                   rank() OVER (PARTITION BY lochierarchy,
                                             CASE
                                                 WHEN t_class = 0 THEN i_category
                                             END
                                ORDER BY gross_margin ASC) AS rank_within_parent
            FROM results_rollup
            ORDER BY lochierarchy DESC NULLS FIRST,
                     CASE
                         WHEN lochierarchy = 0 THEN i_category
                     END NULLS FIRST,
                     rank_within_parent NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            gross_margin = r.GetDouble(0),
            i_category = r.IsDBNull(1) ? null : r.GetString(1),
            i_class = r.IsDBNull(2) ? null : r.GetString(2),
            lochierarchy = r.GetInt32(3),
            rank_within_parent = r.GetInt64(4),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.gross_margin, p.gross_margin, 4);
            Assert.Equal(d.i_category, p.i_category);
            Assert.Equal(d.i_class, p.i_class);
            Assert.Equal(d.lochierarchy, p.lochierarchy);
            Assert.Equal(d.rank_within_parent, p.rank_within_parent);
        }
    }

    /*
     * TPC-DS Query 37
     *
     * SELECT i_item_id,
     *        i_item_desc,
     *        i_current_price
     * FROM item,
     *      inventory,
     *      date_dim,
     *      catalog_sales
     * WHERE i_current_price BETWEEN 68 AND 68 + 30
     *   AND inv_item_sk = i_item_sk
     *   AND d_date_sk=inv_date_sk
     *   AND d_date BETWEEN cast('2000-02-01' AS date) AND cast('2000-04-01' AS date)
     *   AND i_manufact_id IN (677, 940, 694, 808)
     *   AND inv_quantity_on_hand BETWEEN 100 AND 500
     *   AND cs_item_sk = i_item_sk
     * GROUP BY i_item_id,
     *          i_item_desc,
     *          i_current_price
     * ORDER BY i_item_id
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q37")]
    public void Test_Q37()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var inventoryQuery = TpcdsData.InventoryQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();

        long[] targetManufacts = [677, 940, 694, 808];
        var minDate = new DateOnly(2000, 2, 1);
        var maxDate = new DateOnly(2000, 4, 1);

        var filteredItems = itemQuery.Where(i => i.i_current_price >= 68m && i.i_current_price <= 98m
                                              && targetManufacts.Contains(i.i_manufact_id!.Value));

        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredInventory = inventoryQuery.Where(inv => inv.inv_quantity_on_hand >= 100 && inv.inv_quantity_on_hand <= 500);

        var q37 = (from i in filteredItems
                   join inv in filteredInventory on i.i_item_sk equals inv.inv_item_sk
                   join d in filteredDates on inv.inv_date_sk equals d.d_date_sk
                   join cs in catalogSalesQuery on i.i_item_sk equals cs.cs_item_sk
                   group i by new { i.i_item_id, i.i_item_desc, i.i_current_price } into g
                   orderby g.Key.i_item_id
                   select new
                   {
                       g.Key.i_item_id,
                       g.Key.i_item_desc,
                       g.Key.i_current_price
                   }).Take(100);

        AssertPlan(q37, NativePushdown);

        var polarsResult = q37.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT i_item_id,
                   i_item_desc,
                   i_current_price
            FROM read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.InventoryPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.CatalogSalesPath}')
            WHERE i_current_price BETWEEN 68 AND 68 + 30
              AND inv_item_sk = i_item_sk
              AND d_date_sk = inv_date_sk
              AND d_date BETWEEN cast('2000-02-01' AS date) AND cast('2000-04-01' AS date)
              AND i_manufact_id IN (677, 940, 694, 808)
              AND inv_quantity_on_hand BETWEEN 100 AND 500
              AND cs_item_sk = i_item_sk
            GROUP BY i_item_id,
                     i_item_desc,
                     i_current_price
            ORDER BY i_item_id
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            i_current_price = r.IsDBNull(2) ? null : (decimal?)r.GetDecimal(2),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.i_item_desc, p.i_item_desc);
            Assert.Equal(d.i_current_price, p.i_current_price);
        }
    }

    /*
     * TPC-DS Query 38
     *
     * SELECT count(*)
     * FROM
     *   (SELECT DISTINCT c_last_name,
     *                    c_first_name,
     *                    d_date
     *    FROM store_sales,
     *         date_dim,
     *         customer
     *    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     *      AND store_sales.ss_customer_sk = customer.c_customer_sk
     *      AND d_month_seq BETWEEN 1200 AND 1200 + 11
     *    INTERSECT
     *    SELECT DISTINCT c_last_name,
     *                    c_first_name,
     *                    d_date
     *    FROM catalog_sales,
     *         date_dim,
     *         customer
     *    WHERE catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
     *      AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     *      AND d_month_seq BETWEEN 1200 AND 1200 + 11
     *    INTERSECT
     *    SELECT DISTINCT c_last_name,
     *                    c_first_name,
     *                    d_date
     *    FROM web_sales,
     *         date_dim,
     *         customer
     *    WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk
     *      AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
     *      AND d_month_seq BETWEEN 1200 AND 1200 + 11 ) hot_cust
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q38")]
    public void Test_Q38()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        var dates = dateDimQuery.Where(d => d.d_month_seq >= 1200 && d.d_month_seq <= 1211);

        var storeStream = (from ss in storeSalesQuery
                           join d in dates on ss.ss_sold_date_sk equals d.d_date_sk
                           join c in customerQuery on ss.ss_customer_sk equals c.c_customer_sk
                           select new { c.c_last_name, c.c_first_name, d.d_date }).Distinct();

        var catalogStream = (from cs in catalogSalesQuery
                             join d in dates on cs.cs_sold_date_sk equals d.d_date_sk
                             join c in customerQuery on cs.cs_bill_customer_sk equals c.c_customer_sk
                             select new { c.c_last_name, c.c_first_name, d.d_date }).Distinct();

        var webStream = (from ws in webSalesQuery
                         join d in dates on ws.ws_sold_date_sk equals d.d_date_sk
                         join c in customerQuery on ws.ws_bill_customer_sk equals c.c_customer_sk
                         select new { c.c_last_name, c.c_first_name, d.d_date }).Distinct();

        var hotCust = from s in storeStream
                      join c in catalogStream on new { s.c_last_name, s.c_first_name, s.d_date }
                                          equals new { c.c_last_name, c.c_first_name, c.d_date }
                      join w in webStream on new { s.c_last_name, s.c_first_name, s.d_date }
                                      equals new { w.c_last_name, w.c_first_name, w.d_date }
                      select s;

        AssertPlan(hotCust, NativePushdown);

        var polarsCount = hotCust.LongCount();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT count(*)
            FROM
              (SELECT DISTINCT c_last_name,
                               c_first_name,
                               d_date
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.CustomerPath}') customer
               WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
                 AND store_sales.ss_customer_sk = customer.c_customer_sk
                 AND d_month_seq BETWEEN 1200 AND 1200 + 11
               INTERSECT
               SELECT DISTINCT c_last_name,
                               c_first_name,
                               d_date
               FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.CustomerPath}') customer
               WHERE catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
                 AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
                 AND d_month_seq BETWEEN 1200 AND 1200 + 11
               INTERSECT
               SELECT DISTINCT c_last_name,
                               c_first_name,
                               d_date
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.CustomerPath}') customer
               WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk
                 AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
                 AND d_month_seq BETWEEN 1200 AND 1200 + 11 ) hot_cust
            LIMIT 100;
        ";

        var duckDbCount = DuckDbFixture.Query(duckDbSql, r => r.GetInt64(0))[0];

        Assert.Equal(duckDbCount, polarsCount);
    }

    /*
     * TPC-DS Query 39
     *
     * WITH inv AS
     *   (SELECT w_warehouse_name,
     *           w_warehouse_sk,
     *           i_item_sk,
     *           d_moy,
     *           stdev,
     *           mean,
     *           CASE mean
     *               WHEN 0 THEN NULL
     *               ELSE stdev/mean
     *           END cov
     *    FROM
     *      (SELECT w_warehouse_name,
     *              w_warehouse_sk,
     *              i_item_sk,
     *              d_moy,
     *              stddev_samp(inv_quantity_on_hand)*1.000 stdev,
     *              avg(inv_quantity_on_hand) mean
     *       FROM inventory,
     *            item,
     *            warehouse,
     *            date_dim
     *       WHERE inv_item_sk = i_item_sk
     *         AND inv_warehouse_sk = w_warehouse_sk
     *         AND inv_date_sk = d_date_sk
     *         AND d_year = 2001
     *       GROUP BY w_warehouse_name,
     *                w_warehouse_sk,
     *                i_item_sk,
     *                d_moy) foo
     *    WHERE CASE mean
     *              WHEN 0 THEN 0
     *              ELSE stdev/mean
     *          END > 1)
     * SELECT inv1.w_warehouse_sk wsk1,
     *        inv1.i_item_sk isk1,
     *        inv1.d_moy dmoy1,
     *        inv1.mean mean1,
     *        inv1.cov cov1,
     *        inv2.w_warehouse_sk,
     *        inv2.i_item_sk,
     *        inv2.d_moy,
     *        inv2.mean,
     *        inv2.cov
     * FROM inv inv1,
     *      inv inv2
     * WHERE inv1.i_item_sk = inv2.i_item_sk
     *   AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
     *   AND inv1.d_moy = 1
     *   AND inv2.d_moy = 1 + 1
     * ORDER BY inv1.w_warehouse_sk NULLS FIRST,
     *          inv1.i_item_sk NULLS FIRST,
     *          inv1.d_moy NULLS FIRST,
     *          inv1.mean NULLS FIRST,
     *          inv1.cov NULLS FIRST,
     *          inv2.d_moy NULLS FIRST,
     *          inv2.mean NULLS FIRST,
     *          inv2.cov NULLS FIRST;
     */
    [Fact]
    [Trait("TPCDS", "Q39")]
    public void Test_Q39()
    {
        var inventoryQuery = TpcdsData.InventoryQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_year == 2001);

        var baseInv = from inv in inventoryQuery
                      join i in itemQuery on inv.inv_item_sk equals i.i_item_sk
                      join w in warehouseQuery on inv.inv_warehouse_sk equals w.w_warehouse_sk
                      join d in filteredDates on inv.inv_date_sk equals d.d_date_sk
                      select new
                      {
                          w.w_warehouse_name,
                          w.w_warehouse_sk,
                          i.i_item_sk,
                          d.d_moy,
                          inv.inv_quantity_on_hand
                      };

        var foo = from x in baseInv
                  group x by new
                  {
                      x.w_warehouse_name,
                      x.w_warehouse_sk,
                      x.i_item_sk,
                      x.d_moy
                  } into g
                  select new
                  {
                      g.Key.w_warehouse_name,
                      g.Key.w_warehouse_sk,
                      g.Key.i_item_sk,
                      g.Key.d_moy,
                      stdev = g.Std(x => (double?)x.inv_quantity_on_hand),
                      mean = g.Average(x => (double?)x.inv_quantity_on_hand)
                  };

        var invCte = from f in foo
                     where f.mean != 0.0 && f.stdev / f.mean > 1.0
                     select new
                     {
                         f.w_warehouse_name,
                         f.w_warehouse_sk,
                         f.i_item_sk,
                         f.d_moy,
                         f.stdev,
                         f.mean,
                         cov = f.stdev / f.mean
                     };

        var inv1 = invCte.Where(x => x.d_moy == 1);
        var inv2 = invCte.Where(x => x.d_moy == 2);

        var q39 = from i1 in inv1
                  join i2 in inv2 on new { i_item_sk = i1.i_item_sk, w_warehouse_sk = i1.w_warehouse_sk }
                              equals new { i_item_sk = i2.i_item_sk, w_warehouse_sk = i2.w_warehouse_sk }
                  orderby i1.w_warehouse_sk,
                          i1.i_item_sk,
                          i1.d_moy,
                          i1.mean,
                          i1.cov,
                          i2.d_moy,
                          i2.mean,
                          i2.cov
                  select new
                  {
                      wsk1 = i1.w_warehouse_sk,
                      isk1 = i1.i_item_sk,
                      dmoy1 = i1.d_moy,
                      mean1 = i1.mean,
                      cov1 = i1.cov,
                      w_warehouse_sk = i2.w_warehouse_sk,
                      i_item_sk = i2.i_item_sk,
                      d_moy = i2.d_moy,
                      mean = i2.mean,
                      cov = i2.cov
                  };

        AssertPlan(q39, NativePushdown);

        var polarsResult = q39.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH inv AS
              (SELECT w_warehouse_name,
                      w_warehouse_sk,
                      i_item_sk,
                      d_moy,
                      stdev,
                      mean,
                      CASE mean
                          WHEN 0 THEN NULL
                          ELSE stdev/mean
                      END cov
               FROM
                 (SELECT w_warehouse_name,
                         w_warehouse_sk,
                         i_item_sk,
                         d_moy,
                         stddev_samp(inv_quantity_on_hand)*1.000 stdev,
                         avg(inv_quantity_on_hand) mean
                  FROM read_parquet('{TpcdsData.InventoryPath}'),
                       read_parquet('{TpcdsData.ItemPath}'),
                       read_parquet('{TpcdsData.WarehousePath}'),
                       read_parquet('{TpcdsData.DateDimPath}')
                  WHERE inv_item_sk = i_item_sk
                    AND inv_warehouse_sk = w_warehouse_sk
                    AND inv_date_sk = d_date_sk
                    AND d_year = 2001
                  GROUP BY w_warehouse_name,
                           w_warehouse_sk,
                           i_item_sk,
                           d_moy) foo
               WHERE CASE mean
                         WHEN 0 THEN 0
                         ELSE stdev/mean
                     END > 1)
            SELECT inv1.w_warehouse_sk wsk1,
                   inv1.i_item_sk isk1,
                   inv1.d_moy dmoy1,
                   inv1.mean mean1,
                   inv1.cov cov1,
                   inv2.w_warehouse_sk,
                   inv2.i_item_sk,
                   inv2.d_moy,
                   inv2.mean,
                   inv2.cov
            FROM inv inv1,
                 inv inv2
            WHERE inv1.i_item_sk = inv2.i_item_sk
              AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
              AND inv1.d_moy = 1
              AND inv2.d_moy = 1 + 1
            ORDER BY inv1.w_warehouse_sk NULLS FIRST,
                     inv1.i_item_sk NULLS FIRST,
                     inv1.d_moy NULLS FIRST,
                     inv1.mean NULLS FIRST,
                     inv1.cov NULLS FIRST,
                     inv2.d_moy NULLS FIRST,
                     inv2.mean NULLS FIRST,
                     inv2.cov NULLS FIRST;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            wsk1 = r.GetInt64(0),
            isk1 = r.GetInt64(1),
            dmoy1 = r.GetInt64(2),
            mean1 = r.IsDBNull(3) ? (double?)null : r.GetDouble(3),
            cov1 = r.IsDBNull(4) ? (double?)null : r.GetDouble(4),
            w_warehouse_sk = r.GetInt64(5),
            i_item_sk = r.GetInt64(6),
            d_moy = r.GetInt64(7),
            mean = r.IsDBNull(8) ? (double?)null : r.GetDouble(8),
            cov = r.IsDBNull(9) ? (double?)null : r.GetDouble(9),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.wsk1, p.wsk1);
            Assert.Equal(d.isk1, p.isk1);
            Assert.Equal(d.dmoy1, p.dmoy1);
            if (d.mean1 == null) Assert.Null(p.mean1); else Assert.Equal(d.mean1.Value, p.mean1!.Value, 4);
            if (d.cov1 == null) Assert.Null(p.cov1); else Assert.Equal(d.cov1.Value, p.cov1!.Value, 4);

            Assert.Equal(d.w_warehouse_sk, p.w_warehouse_sk);
            Assert.Equal(d.i_item_sk, p.i_item_sk);
            Assert.Equal(d.d_moy, p.d_moy);
            if (d.mean == null) Assert.Null(p.mean); else Assert.Equal(d.mean.Value, p.mean!.Value, 4);
            if (d.cov == null) Assert.Null(p.cov); else Assert.Equal(d.cov.Value, p.cov!.Value, 4);
        }
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

        var joined = from cs in catalogSalesQuery
                     join cr in catalogReturnsQuery on new { order_number = cs.cs_order_number, item_sk = cs.cs_item_sk }
                                                equals new { order_number = cr.cr_order_number, item_sk = cr.cr_item_sk } into crGroup
                     from cr in crGroup.DefaultIfEmpty()
                     join w in warehouseQuery on cs.cs_warehouse_sk equals w.w_warehouse_sk
                     join i in filteredItems on cs.cs_item_sk equals i.i_item_sk
                     join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                     select new
                     {
                         w.w_state,
                         i.i_item_id,
                         cs.cs_sales_price,
                         cr.cr_refunded_cash,
                         d.d_date
                     };

        var q40 = (from x in joined
                   group x by new { x.w_state, x.i_item_id } into g
                   orderby g.Key.w_state, g.Key.i_item_id
                   select new
                   {
                       g.Key.w_state,
                       g.Key.i_item_id,
                       sales_before = g.Sum(x => x.d_date < pivotDate ? x.cs_sales_price - (x.cr_refunded_cash ?? 0m) : 0m),
                       sales_after = g.Sum(x => x.d_date >= pivotDate ? x.cs_sales_price - (x.cr_refunded_cash ?? 0m) : 0m)
                   }).Take(100);

        AssertPlan(q40, NativePushdown);

        var polarsResult = q40.ToList();

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

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            w_state = r.IsDBNull(0) ? null : r.GetString(0),
            i_item_id = r.GetString(1),
            sales_before = r.IsDBNull(2) ? null : (decimal?)r.GetDecimal(2),
            sales_after = r.IsDBNull(3) ? null : (decimal?)r.GetDecimal(3),
        });

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
}

