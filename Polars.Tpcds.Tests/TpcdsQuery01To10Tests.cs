namespace Polars.Tpcds.Tests;

public class TpcdsQuery01To10Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 1
     *
     * WITH customer_total_return AS
     *   (SELECT sr_customer_sk AS ctr_customer_sk,
     *           sr_store_sk AS ctr_store_sk,
     *           sum(sr_return_amt) AS ctr_total_return
     *    FROM store_returns,
     *         date_dim
     *    WHERE sr_returned_date_sk = d_date_sk
     *      AND d_year = 2000
     *    GROUP BY sr_customer_sk,
     *             sr_store_sk)
     * SELECT c_customer_id
     * FROM customer_total_return ctr1,
     *      store,
     *      customer
     * WHERE ctr1.ctr_total_return >
     *     (SELECT avg(ctr_total_return)*1.2
     *      FROM customer_total_return ctr2
     *      WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
     *   AND s_store_sk = ctr1.ctr_store_sk
     *   AND s_state = 'TN'
     *   AND ctr1.ctr_customer_sk = c_customer_sk
     * ORDER BY c_customer_id
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q1")]
    public void Test_Q1()
    {
        var returnsQuery = TpcdsData.StoreReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        // Step 1: customer_total_return CTE
        var ctr = from sr in returnsQuery
                  join d in dateDimQuery on sr.sr_returned_date_sk equals d.d_date_sk
                  where d.d_year == 2000
                  group sr by new { sr.sr_customer_sk, sr.sr_store_sk } into g
                  select new
                  {
                      ctr_customer_sk = g.Key.sr_customer_sk,
                      ctr_store_sk = g.Key.sr_store_sk,
                      ctr_total_return = g.Sum(x => x.sr_return_amt)
                  };

        // Step 2: Main query with subquery
        var q1 = from ctr1 in ctr
                 where ctr1.ctr_total_return > ctr.Where(ctr2 => ctr2.ctr_store_sk == ctr1.ctr_store_sk)
                                                  .Average(ctr2 => ctr2.ctr_total_return) * 1.2m
                 join s in storeQuery.Where(s => s.s_state == "TN") on ctr1.ctr_store_sk equals s.s_store_sk
                 join c in customerQuery on ctr1.ctr_customer_sk equals c.c_customer_sk
                 orderby c.c_customer_id
                 select c.c_customer_id;

        var limitedQ1 = q1.Take(100);

        AssertPlan(limitedQ1, NativePushdown);

        var polarsResult = limitedQ1.ToList();
        Assert.NotEmpty(polarsResult);

        // Ground-truth cross validation with DuckDB
        var duckDbSql = $@"
            WITH customer_total_return AS
              (SELECT sr_customer_sk AS ctr_customer_sk,
                      sr_store_sk AS ctr_store_sk,
                      sum(sr_return_amt) AS ctr_total_return
               FROM read_parquet('{TpcdsData.StoreReturnsPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE sr_returned_date_sk = d_date_sk
                 AND d_year = 2000
               GROUP BY sr_customer_sk,
                        sr_store_sk)
            SELECT c_customer_id
            FROM customer_total_return ctr1,
                 read_parquet('{TpcdsData.StorePath}'),
                 read_parquet('{TpcdsData.CustomerPath}')
            WHERE ctr1.ctr_total_return >
                (SELECT avg(ctr_total_return)*1.2
                 FROM customer_total_return ctr2
                 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
              AND s_store_sk = ctr1.ctr_store_sk
              AND s_state = 'TN'
              AND ctr1.ctr_customer_sk = c_customer_sk
            ORDER BY c_customer_id
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => r.GetString(0));

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        Assert.Equal(duckDbResult, polarsResult);
    }

    /*
     * TPC-DS Query 2
     *
     * WITH wscs AS
     *   (SELECT sold_date_sk,
     *           sales_price
     *    FROM
     *   (SELECT ws_sold_date_sk sold_date_sk,
     *                ws_ext_sales_price sales_price
     *         FROM web_sales
     *         UNION ALL SELECT cs_sold_date_sk sold_date_sk,
     *                          cs_ext_sales_price sales_price
     *         FROM catalog_sales) sq1),
     *        wswscs AS
     *     (SELECT d_week_seq,
     *             sum(CASE
     *                     WHEN (d_day_name='Sunday') THEN sales_price
     *                     ELSE NULL
     *                 END) sun_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Monday') THEN sales_price
     *                     ELSE NULL
     *                 END) mon_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Tuesday') THEN sales_price
     *                     ELSE NULL
     *                 END) tue_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Wednesday') THEN sales_price
     *                     ELSE NULL
     *                 END) wed_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Thursday') THEN sales_price
     *                     ELSE NULL
     *                 END) thu_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Friday') THEN sales_price
     *                     ELSE NULL
     *                 END) fri_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Saturday') THEN sales_price
     *                     ELSE NULL
     *                 END) sat_sales
     *      FROM wscs,
     *           date_dim
     *      WHERE d_date_sk = sold_date_sk
     *      GROUP BY d_week_seq)
     *   SELECT d_week_seq1,
     *          round(sun_sales1/sun_sales2, 2) r1,
     *          round(mon_sales1/mon_sales2, 2) r2,
     *          round(tue_sales1/tue_sales2, 2) r3,
     *          round(wed_sales1/wed_sales2, 2) r4,
     *          round(thu_sales1/thu_sales2, 2) r5,
     *          round(fri_sales1/fri_sales2, 2) r6,
     *          round(sat_sales1/sat_sales2, 2) r7
     *   FROM
     *     (SELECT wswscs.d_week_seq d_week_seq1,
     *             sun_sales sun_sales1,
     *             mon_sales mon_sales1,
     *             tue_sales tue_sales1,
     *             wed_sales wed_sales1,
     *             thu_sales thu_sales1,
     *             fri_sales fri_sales1,
     *             sat_sales sat_sales1
     *      FROM wswscs,
     *           date_dim
     *      WHERE date_dim.d_week_seq = wswscs.d_week_seq
     *        AND d_year = 2001) y,
     *     (SELECT wswscs.d_week_seq d_week_seq2,
     *             sun_sales sun_sales2,
     *             mon_sales mon_sales2,
     *             tue_sales tue_sales2,
     *             wed_sales wed_sales2,
     *             thu_sales thu_sales2,
     *             fri_sales fri_sales2,
     *             sat_sales sat_sales2
     *      FROM wswscs,
     *           date_dim
     *      WHERE date_dim.d_week_seq = wswscs.d_week_seq
     *        AND d_year = 2001+1) z
     *   WHERE d_week_seq1 = d_week_seq2-53
     *   ORDER BY d_week_seq1 NULLS FIRST;
     */
    [Fact]
    [Trait("TPCDS", "Q2")]
    public void Test_Q2()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        // 1. wscs: UNION ALL
        var ws = webSalesQuery.Select(w => new { sold_date_sk = w.ws_sold_date_sk, sales_price = w.ws_ext_sales_price });
        var cs = catalogSalesQuery.Select(c => new { sold_date_sk = c.cs_sold_date_sk, sales_price = c.cs_ext_sales_price });
        var wscs = ws.Concat(cs);

        // 2. wswscs: group by d_week_seq
        var wswscs = from s in wscs
                     join d in dateDimQuery on s.sold_date_sk equals d.d_date_sk
                     group new { s, d } by d.d_week_seq into g
                     select new
                     {
                         d_week_seq = g.Key,
                         sun_sales = g.Sum(x => x.d.d_day_name == "Sunday" ? x.s.sales_price : null),
                         mon_sales = g.Sum(x => x.d.d_day_name == "Monday" ? x.s.sales_price : null),
                         tue_sales = g.Sum(x => x.d.d_day_name == "Tuesday" ? x.s.sales_price : null),
                         wed_sales = g.Sum(x => x.d.d_day_name == "Wednesday" ? x.s.sales_price : null),
                         thu_sales = g.Sum(x => x.d.d_day_name == "Thursday" ? x.s.sales_price : null),
                         fri_sales = g.Sum(x => x.d.d_day_name == "Friday" ? x.s.sales_price : null),
                         sat_sales = g.Sum(x => x.d.d_day_name == "Saturday" ? x.s.sales_price : null),
                     };

        // 3. y (d_year = 2001) and z (d_year = 2002)
        var y = from w in wswscs
                join d in dateDimQuery.Where(d => d.d_year == 2001) on w.d_week_seq equals d.d_week_seq
                select new
                {
                    d_week_seq1 = w.d_week_seq,
                    sun_sales1 = w.sun_sales,
                    mon_sales1 = w.mon_sales,
                    tue_sales1 = w.tue_sales,
                    wed_sales1 = w.wed_sales,
                    thu_sales1 = w.thu_sales,
                    fri_sales1 = w.fri_sales,
                    sat_sales1 = w.sat_sales,
                };

        var z = from w in wswscs
                join d in dateDimQuery.Where(d => d.d_year == 2002) on w.d_week_seq equals d.d_week_seq
                select new
                {
                    d_week_seq2 = w.d_week_seq,
                    sun_sales2 = w.sun_sales,
                    mon_sales2 = w.mon_sales,
                    tue_sales2 = w.tue_sales,
                    wed_sales2 = w.wed_sales,
                    thu_sales2 = w.thu_sales,
                    fri_sales2 = w.fri_sales,
                    sat_sales2 = w.sat_sales,
                };

        // 4. Join y and z on d_week_seq1 = d_week_seq2 - 53
        var q2 = from yItem in y
                 join zItem in z on yItem.d_week_seq1 equals zItem.d_week_seq2 - 53
                 orderby yItem.d_week_seq1
                 select new
                 {
                     yItem.d_week_seq1,
                     r1 = (yItem.sun_sales1 == null || yItem.sun_sales1 == 0m || zItem.sun_sales2 == null || zItem.sun_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.sun_sales1 / (double)zItem.sun_sales2, 2),
                     r2 = (yItem.mon_sales1 == null || yItem.mon_sales1 == 0m || zItem.mon_sales2 == null || zItem.mon_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.mon_sales1 / (double)zItem.mon_sales2, 2),
                     r3 = (yItem.tue_sales1 == null || yItem.tue_sales1 == 0m || zItem.tue_sales2 == null || zItem.tue_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.tue_sales1 / (double)zItem.tue_sales2, 2),
                     r4 = (yItem.wed_sales1 == null || yItem.wed_sales1 == 0m || zItem.wed_sales2 == null || zItem.wed_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.wed_sales1 / (double)zItem.wed_sales2, 2),
                     r5 = (yItem.thu_sales1 == null || yItem.thu_sales1 == 0m || zItem.thu_sales2 == null || zItem.thu_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.thu_sales1 / (double)zItem.thu_sales2, 2),
                     r6 = (yItem.fri_sales1 == null || yItem.fri_sales1 == 0m || zItem.fri_sales2 == null || zItem.fri_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.fri_sales1 / (double)zItem.fri_sales2, 2),
                     r7 = (yItem.sat_sales1 == null || yItem.sat_sales1 == 0m || zItem.sat_sales2 == null || zItem.sat_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.sat_sales1 / (double)zItem.sat_sales2, 2),
                 };
        AssertPlan(q2, NativePushdown);

        var polarsResult = q2.ToList();
        Assert.NotEmpty(polarsResult);

        // Ground-truth cross validation with DuckDB
        var duckDbSql = $@"
            WITH wscs AS
              (SELECT sold_date_sk,
                      sales_price
               FROM
            (SELECT ws_sold_date_sk sold_date_sk,
                         ws_ext_sales_price sales_price
                  FROM read_parquet('{TpcdsData.WebSalesPath}')
                  UNION ALL SELECT cs_sold_date_sk sold_date_sk,
                                   cs_ext_sales_price sales_price
                  FROM read_parquet('{TpcdsData.CatalogSalesPath}')) sq1),
                 wswscs AS
              (SELECT d_week_seq,
                      sum(CASE
                              WHEN (d_day_name='Sunday') THEN sales_price
                              ELSE NULL
                          END) sun_sales,
                      sum(CASE
                              WHEN (d_day_name='Monday') THEN sales_price
                              ELSE NULL
                          END) mon_sales,
                      sum(CASE
                              WHEN (d_day_name='Tuesday') THEN sales_price
                              ELSE NULL
                          END) tue_sales,
                      sum(CASE
                              WHEN (d_day_name='Wednesday') THEN sales_price
                              ELSE NULL
                          END) wed_sales,
                      sum(CASE
                              WHEN (d_day_name='Thursday') THEN sales_price
                              ELSE NULL
                          END) thu_sales,
                      sum(CASE
                              WHEN (d_day_name='Friday') THEN sales_price
                              ELSE NULL
                          END) fri_sales,
                      sum(CASE
                              WHEN (d_day_name='Saturday') THEN sales_price
                              ELSE NULL
                          END) sat_sales
               FROM wscs,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE d_date_sk = sold_date_sk
               GROUP BY d_week_seq)
            SELECT d_week_seq1,
                   round(sun_sales1/sun_sales2, 2) r1,
                   round(mon_sales1/mon_sales2, 2) r2,
                   round(tue_sales1/tue_sales2, 2) r3,
                   round(wed_sales1/wed_sales2, 2) r4,
                   round(thu_sales1/thu_sales2, 2) r5,
                   round(fri_sales1/fri_sales2, 2) r6,
                   round(sat_sales1/sat_sales2, 2) r7
            FROM
              (SELECT wswscs.d_week_seq d_week_seq1,
                      sun_sales sun_sales1,
                      mon_sales mon_sales1,
                      tue_sales tue_sales1,
                      wed_sales wed_sales1,
                      thu_sales thu_sales1,
                      fri_sales fri_sales1,
                      sat_sales sat_sales1
               FROM wswscs,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE date_dim.d_week_seq = wswscs.d_week_seq
                 AND d_year = 2001) y,
              (SELECT wswscs.d_week_seq d_week_seq2,
                      sun_sales sun_sales2,
                      mon_sales mon_sales2,
                      tue_sales tue_sales2,
                      wed_sales wed_sales2,
                      thu_sales thu_sales2,
                      fri_sales fri_sales2,
                      sat_sales sat_sales2
               FROM wswscs,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE date_dim.d_week_seq = wswscs.d_week_seq
                 AND d_year = 2001+1) z
            WHERE d_week_seq1 = d_week_seq2-53
            ORDER BY d_week_seq1 NULLS FIRST;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            d_week_seq1 = r.GetInt64(0),
            r1 = r.IsDBNull(1) ? (double?)null : r.GetDouble(1),
            r2 = r.IsDBNull(2) ? (double?)null : r.GetDouble(2),
            r3 = r.IsDBNull(3) ? (double?)null : r.GetDouble(3),
            r4 = r.IsDBNull(4) ? (double?)null : r.GetDouble(4),
            r5 = r.IsDBNull(5) ? (double?)null : r.GetDouble(5),
            r6 = r.IsDBNull(6) ? (double?)null : r.GetDouble(6),
            r7 = r.IsDBNull(7) ? (double?)null : r.GetDouble(7),
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < Math.Min(duckDbResult.Count, polarsResult.Count); i++)
        {
            var d = duckDbResult[i];
            var p = polarsResult[i];
            Assert.Equal(d.d_week_seq1, p.d_week_seq1);
            Assert.Equal(d.r1, p.r1);
            Assert.Equal(d.r2, p.r2);
            Assert.Equal(d.r3, p.r3);
            Assert.Equal(d.r4, p.r4);
            Assert.Equal(d.r5, p.r5);
            Assert.Equal(d.r6, p.r6);
            Assert.Equal(d.r7, p.r7);
        }
    }

    /*
     * TPC-DS Query 3
     *
     * SELECT dt.d_year,
     *        item.i_brand_id brand_id,
     *        item.i_brand brand,
     *        sum(ss_ext_sales_price) sum_agg
     * FROM date_dim dt,
     *      store_sales,
     *      item
     * WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
     *   AND store_sales.ss_item_sk = item.i_item_sk
     *   AND item.i_manufact_id = 128
     *   AND dt.d_moy=11
     * GROUP BY dt.d_year,
     *          item.i_brand,
     *          item.i_brand_id
     * ORDER BY dt.d_year,
     *          sum_agg DESC,
     *          brand_id
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q3")]
    public void Test_Q3()
    {

        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();

        IQueryable<dynamic> BuildQuery(long manufactId)
        {
            var baseQuery = from ss in storeSalesQuery
                            join dt in dateDimQuery.Where(d => d.d_moy == 11) on ss.ss_sold_date_sk equals dt.d_date_sk
                            join i in itemQuery.Where(i => i.i_manufact_id == manufactId) on ss.ss_item_sk equals i.i_item_sk
                            group ss by new { dt.d_year, i.i_brand, i.i_brand_id } into g
                            select new
                            {
                                g.Key.d_year,
                                brand_id = g.Key.i_brand_id,
                                brand = g.Key.i_brand,
                                sum_agg = g.Sum(x => x.ss_ext_sales_price)
                            };

            return (from r in baseQuery
                    orderby r.d_year, r.sum_agg descending, r.brand_id
                    select r).Take(100);
        }

        // 1. Standard parameter (i_manufact_id = 128, yields 0 rows on SF=0.01)
        var standardQuery = BuildQuery(128);
        AssertPlan(standardQuery, NativePushdown);

        var standardResults = standardQuery.ToList();
        Assert.Empty(standardResults);

        // 2. Active parameter with data on SF=0.01 (i_manufact_id = 167, yields 10 rows)
        var activeQuery = BuildQuery(167);
        AssertPlan(activeQuery, NativePushdown);

        var activeResults = activeQuery.ToList();
        Assert.NotEmpty(activeResults);

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT dt.d_year,
                   item.i_brand_id brand_id,
                   item.i_brand brand,
                   sum(ss_ext_sales_price) sum_agg
            FROM read_parquet('{TpcdsData.DateDimPath}') dt,
                 read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                 read_parquet('{TpcdsData.ItemPath}') item
            WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
              AND store_sales.ss_item_sk = item.i_item_sk
              AND item.i_manufact_id = 167
              AND dt.d_moy = 11
            GROUP BY dt.d_year,
                     item.i_brand,
                     item.i_brand_id
            ORDER BY dt.d_year,
                     sum_agg DESC,
                     brand_id
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            d_year = r.GetInt64(0),
            brand_id = r.GetInt64(1),
            brand = r.GetString(2),
            sum_agg = r.GetDecimal(3)
        });

        Assert.Equal(duckDbResults.Count, activeResults.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = activeResults[i];
            Assert.Equal(d.d_year, (long)p.d_year);
            Assert.Equal(d.brand_id, (long)p.brand_id);
            Assert.Equal(d.brand, (string)p.brand);
            Assert.Equal(d.sum_agg, (decimal)p.sum_agg);
        }
    }

    /*
     * TPC-DS Query 4
     *
     * WITH year_total AS
     *   (SELECT c_customer_id customer_id,
     *           c_first_name customer_first_name,
     *           c_last_name customer_last_name,
     *           c_preferred_cust_flag customer_preferred_cust_flag,
     *           c_birth_country customer_birth_country,
     *           c_login customer_login,
     *           c_email_address customer_email_address,
     *           d_year dyear,
     *           sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
     *           's' sale_type
     *    FROM customer,
     *         store_sales,
     *         date_dim
     *    WHERE c_customer_sk = ss_customer_sk
     *      AND ss_sold_date_sk = d_date_sk
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             c_preferred_cust_flag,
     *             c_birth_country,
     *             c_login,
     *             c_email_address,
     *             d_year
     *    UNION ALL SELECT c_customer_id customer_id,
     *                     c_first_name customer_first_name,
     *                     c_last_name customer_last_name,
     *                     c_preferred_cust_flag customer_preferred_cust_flag,
     *                     c_birth_country customer_birth_country,
     *                     c_login customer_login,
     *                     c_email_address customer_email_address,
     *                     d_year dyear,
     *                     sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)) year_total,
     *                     'c' sale_type
     *    FROM customer,
     *         catalog_sales,
     *         date_dim
     *    WHERE c_customer_sk = cs_bill_customer_sk
     *      AND cs_sold_date_sk = d_date_sk
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             c_preferred_cust_flag,
     *             c_birth_country,
     *             c_login,
     *             c_email_address,
     *             d_year
     *    UNION ALL SELECT c_customer_id customer_id,
     *                     c_first_name customer_first_name,
     *                     c_last_name customer_last_name,
     *                     c_preferred_cust_flag customer_preferred_cust_flag,
     *                     c_birth_country customer_birth_country,
     *                     c_login customer_login,
     *                     c_email_address customer_email_address,
     *                     d_year dyear,
     *                     sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)) year_total,
     *                     'w' sale_type
     *    FROM customer,
     *         web_sales,
     *         date_dim
     *    WHERE c_customer_sk = ws_bill_customer_sk
     *      AND ws_sold_date_sk = d_date_sk
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             c_preferred_cust_flag,
     *             c_birth_country,
     *             c_login,
     *             c_email_address,
     *             d_year)
     * SELECT t_s_secyear.customer_id,
     *        t_s_secyear.customer_first_name,
     *        t_s_secyear.customer_last_name,
     *        t_s_secyear.customer_preferred_cust_flag
     * FROM year_total t_s_firstyear,
     *      year_total t_s_secyear,
     *      year_total t_c_firstyear,
     *      year_total t_c_secyear,
     *      year_total t_w_firstyear,
     *      year_total t_w_secyear
     * WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
     *   AND t_s_firstyear.customer_id = t_c_secyear.customer_id
     *   AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
     *   AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
     *   AND t_s_firstyear.customer_id = t_w_secyear.customer_id
     *   AND t_s_firstyear.sale_type = 's'
     *   AND t_c_firstyear.sale_type = 'c'
     *   AND t_w_firstyear.sale_type = 'w'
     *   AND t_s_secyear.sale_type = 's'
     *   AND t_c_secyear.sale_type = 'c'
     *   AND t_w_secyear.sale_type = 'w'
     *   AND t_s_firstyear.dyear = 2001
     *   AND t_s_secyear.dyear = 2001+1
     *   AND t_c_firstyear.dyear = 2001
     *   AND t_c_secyear.dyear = 2001+1
     *   AND t_w_firstyear.dyear = 2001
     *   AND t_w_secyear.dyear = 2001+1
     *   AND t_s_firstyear.year_total > 0
     *   AND t_c_firstyear.year_total > 0
     *   AND t_w_firstyear.year_total > 0
     *   AND CASE
     *           WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
     *           ELSE NULL
     *       END > CASE
     *                 WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
     *                 ELSE NULL
     *             END
     *   AND CASE
     *           WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
     *           ELSE NULL
     *       END > CASE
     *                 WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
     *                 ELSE NULL
     *             END
     * ORDER BY t_s_secyear.customer_id NULLS FIRST,
     *          t_s_secyear.customer_first_name NULLS FIRST,
     *          t_s_secyear.customer_last_name NULLS FIRST,
     *          t_s_secyear.customer_preferred_cust_flag NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q4")]
    public void Test_Q4()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        // 1. Channel streams grouped by customer & year
        var storeYear = from c in customerQuery
                        join ss in storeSalesQuery on c.c_customer_sk equals ss.ss_customer_sk
                        join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                        where d.d_year == 2001 || d.d_year == 2002
                        group new { c, ss, d } by new
                        {
                            c.c_customer_id,
                            c.c_first_name,
                            c.c_last_name,
                            c.c_preferred_cust_flag,
                            d.d_year
                        } into g
                        select new
                        {
                            customer_id = g.Key.c_customer_id,
                            customer_first_name = g.Key.c_first_name,
                            customer_last_name = g.Key.c_last_name,
                            customer_preferred_cust_flag = g.Key.c_preferred_cust_flag,
                            dyear = g.Key.d_year,
                            year_total = g.Sum(x => (x.ss.ss_ext_list_price - x.ss.ss_ext_wholesale_cost - x.ss.ss_ext_discount_amt + x.ss.ss_ext_sales_price) / 2m),
                        };

        var catalogYear = from c in customerQuery
                          join cs in catalogSalesQuery on c.c_customer_sk equals cs.cs_bill_customer_sk
                          join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                          where d.d_year == 2001 || d.d_year == 2002
                          group new { c, cs, d } by new { c.c_customer_id, d.d_year } into g
                          select new
                          {
                              customer_id = g.Key.c_customer_id,
                              dyear = g.Key.d_year,
                              year_total = g.Sum(x => (x.cs.cs_ext_list_price - x.cs.cs_ext_wholesale_cost - x.cs.cs_ext_discount_amt + x.cs.cs_ext_sales_price) / 2m),
                          };

        var webYear = from c in customerQuery
                      join ws in webSalesQuery on c.c_customer_sk equals ws.ws_bill_customer_sk
                      join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                      where d.d_year == 2001 || d.d_year == 2002
                      group new { c, ws, d } by new { c.c_customer_id, d.d_year } into g
                      select new
                      {
                          customer_id = g.Key.c_customer_id,
                          dyear = g.Key.d_year,
                          year_total = g.Sum(x => (x.ws.ws_ext_list_price - x.ws.ws_ext_wholesale_cost - x.ws.ws_ext_discount_amt + x.ws.ws_ext_sales_price) / 2m),
                      };

        // 2. Filter year and positive sales
        var ts1 = storeYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var ts2 = storeYear.Where(x => x.dyear == 2002);
        var tc1 = catalogYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var tc2 = catalogYear.Where(x => x.dyear == 2002);
        var tw1 = webYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var tw2 = webYear.Where(x => x.dyear == 2002);

        // 3. Multi-channel customer self-join and relative growth filter
        var q4 = from s1 in ts1
                 join s2 in ts2 on s1.customer_id equals s2.customer_id
                 join c1 in tc1 on s1.customer_id equals c1.customer_id
                 join c2 in tc2 on s1.customer_id equals c2.customer_id
                 join w1 in tw1 on s1.customer_id equals w1.customer_id
                 join w2 in tw2 on s1.customer_id equals w2.customer_id
                 where (double)c2.year_total! / (double)c1.year_total! > (double)s2.year_total! / (double)s1.year_total!
                    && (double)c2.year_total! / (double)c1.year_total! > (double)w2.year_total! / (double)w1.year_total!
                 orderby s2.customer_id, s2.customer_first_name, s2.customer_last_name, s2.customer_preferred_cust_flag
                 select new
                 {
                     s2.customer_id,
                     s2.customer_first_name,
                     s2.customer_last_name,
                     s2.customer_preferred_cust_flag
                 };

        var limitedQ4 = q4.Take(100);

        AssertPlan(limitedQ4, NativePushdown);

        var polarsResult = limitedQ4.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH year_total AS
              (SELECT c_customer_id customer_id,
                      c_first_name customer_first_name,
                      c_last_name customer_last_name,
                      c_preferred_cust_flag customer_preferred_cust_flag,
                      c_birth_country customer_birth_country,
                      c_login customer_login,
                      c_email_address customer_email_address,
                      d_year dyear,
                      sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
                      's' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE c_customer_sk = ss_customer_sk
                 AND ss_sold_date_sk = d_date_sk
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        c_preferred_cust_flag,
                        c_birth_country,
                        c_login,
                        c_email_address,
                        d_year
               UNION ALL SELECT c_customer_id customer_id,
                                c_first_name customer_first_name,
                                c_last_name customer_last_name,
                                c_preferred_cust_flag customer_preferred_cust_flag,
                                c_birth_country customer_birth_country,
                                c_login customer_login,
                                c_email_address customer_email_address,
                                d_year dyear,
                                sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)) year_total,
                                'c' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE c_customer_sk = cs_bill_customer_sk
                 AND cs_sold_date_sk = d_date_sk
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        c_preferred_cust_flag,
                        c_birth_country,
                        c_login,
                        c_email_address,
                        d_year
               UNION ALL SELECT c_customer_id customer_id,
                                c_first_name customer_first_name,
                                c_last_name customer_last_name,
                                c_preferred_cust_flag customer_preferred_cust_flag,
                                c_birth_country customer_birth_country,
                                c_login customer_login,
                                c_email_address customer_email_address,
                                d_year dyear,
                                sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)) year_total,
                                'w' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE c_customer_sk = ws_bill_customer_sk
                 AND ws_sold_date_sk = d_date_sk
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        c_preferred_cust_flag,
                        c_birth_country,
                        c_login,
                        c_email_address,
                        d_year)
            SELECT t_s_secyear.customer_id,
                   t_s_secyear.customer_first_name,
                   t_s_secyear.customer_last_name,
                   t_s_secyear.customer_preferred_cust_flag
            FROM year_total t_s_firstyear,
                 year_total t_s_secyear,
                 year_total t_c_firstyear,
                 year_total t_c_secyear,
                 year_total t_w_firstyear,
                 year_total t_w_secyear
            WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
              AND t_s_firstyear.customer_id = t_c_secyear.customer_id
              AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
              AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
              AND t_s_firstyear.customer_id = t_w_secyear.customer_id
              AND t_s_firstyear.sale_type = 's'
              AND t_c_firstyear.sale_type = 'c'
              AND t_w_firstyear.sale_type = 'w'
              AND t_s_secyear.sale_type = 's'
              AND t_c_secyear.sale_type = 'c'
              AND t_w_secyear.sale_type = 'w'
              AND t_s_firstyear.dyear = 2001
              AND t_s_secyear.dyear = 2001+1
              AND t_c_firstyear.dyear = 2001
              AND t_c_secyear.dyear = 2001+1
              AND t_w_firstyear.dyear = 2001
              AND t_w_secyear.dyear = 2001+1
              AND t_s_firstyear.year_total > 0
              AND t_c_firstyear.year_total > 0
              AND t_w_firstyear.year_total > 0
              AND CASE
                      WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
                      ELSE NULL
                  END > CASE
                            WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                            ELSE NULL
                        END
              AND CASE
                      WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
                      ELSE NULL
                  END > CASE
                            WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                            ELSE NULL
                        END
            ORDER BY t_s_secyear.customer_id NULLS FIRST,
                     t_s_secyear.customer_first_name NULLS FIRST,
                     t_s_secyear.customer_last_name NULLS FIRST,
                     t_s_secyear.customer_preferred_cust_flag NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            customer_id = r.GetString(0),
            customer_first_name = r.GetString(1),
            customer_last_name = r.GetString(2),
            customer_preferred_cust_flag = r.GetString(3),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
    }

    /*
     * TPC-DS Query 5
     *
     * WITH ssr AS
     *   (SELECT s_store_id,
     *           sum(sales_price) AS sales,
     *           sum(profit) AS profit,
     *           sum(return_amt) AS returns_,
     *           sum(net_loss) AS profit_loss
     *    FROM
     *      (SELECT ss_store_sk AS store_sk,
     *              ss_sold_date_sk AS date_sk,
     *              ss_ext_sales_price AS sales_price,
     *              ss_net_profit AS profit,
     *              cast(0 AS decimal(7,2)) AS return_amt,
     *              cast(0 AS decimal(7,2)) AS net_loss
     *       FROM store_sales
     *       UNION ALL SELECT sr_store_sk AS store_sk,
     *                        sr_returned_date_sk AS date_sk,
     *                        cast(0 AS decimal(7,2)) AS sales_price,
     *                        cast(0 AS decimal(7,2)) AS profit,
     *                        sr_return_amt AS return_amt,
     *                        sr_net_loss AS net_loss
     *       FROM store_returns ) salesreturns,
     *         date_dim,
     *         store
     *    WHERE date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
     *      AND store_sk = s_store_sk
     *    GROUP BY s_store_id) ,
     *      csr AS
     *   (SELECT cp_catalog_page_id,
     *           sum(sales_price) AS sales,
     *           sum(profit) AS profit,
     *           sum(return_amt) AS returns_,
     *           sum(net_loss) AS profit_loss
     *    FROM
     *      (SELECT cs_catalog_page_sk AS page_sk,
     *              cs_sold_date_sk AS date_sk,
     *              cs_ext_sales_price AS sales_price,
     *              cs_net_profit AS profit,
     *              cast(0 AS decimal(7,2)) AS return_amt,
     *              cast(0 AS decimal(7,2)) AS net_loss
     *       FROM catalog_sales
     *       UNION ALL SELECT cr_catalog_page_sk AS page_sk,
     *                        cr_returned_date_sk AS date_sk,
     *                        cast(0 AS decimal(7,2)) AS sales_price,
     *                        cast(0 AS decimal(7,2)) AS profit,
     *                        cr_return_amount AS return_amt,
     *                        cr_net_loss AS net_loss
     *       FROM catalog_returns ) salesreturns,
     *         date_dim,
     *         catalog_page
     *    WHERE date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
     *      AND page_sk = cp_catalog_page_sk
     *    GROUP BY cp_catalog_page_id) ,
     *      wsr AS
     *   (SELECT web_site_id,
     *           sum(sales_price) AS sales,
     *           sum(profit) AS profit,
     *           sum(return_amt) AS returns_,
     *           sum(net_loss) AS profit_loss
     *    FROM
     *      (SELECT ws_web_site_sk AS wsr_web_site_sk,
     *              ws_sold_date_sk AS date_sk,
     *              ws_ext_sales_price AS sales_price,
     *              ws_net_profit AS profit,
     *              cast(0 AS decimal(7,2)) AS return_amt,
     *              cast(0 AS decimal(7,2)) AS net_loss
     *       FROM web_sales
     *       UNION ALL SELECT ws_web_site_sk AS wsr_web_site_sk,
     *                        wr_returned_date_sk AS date_sk,
     *                        cast(0 AS decimal(7,2)) AS sales_price,
     *                        cast(0 AS decimal(7,2)) AS profit,
     *                        wr_return_amt AS return_amt,
     *                        wr_net_loss AS net_loss
     *       FROM web_returns
     *       LEFT OUTER JOIN web_sales ON (wr_item_sk = ws_item_sk
     *                                     AND wr_order_number = ws_order_number) ) salesreturns,
     *         date_dim,
     *         web_site
     *    WHERE date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
     *      AND wsr_web_site_sk = web_site_sk
     *    GROUP BY web_site_id)
     * SELECT channel ,
     *        id ,
     *        sum(sales) AS sales ,
     *        sum(returns_) AS returns_ ,
     *        sum(profit) AS profit
     * FROM
     *   (SELECT 'store channel' AS channel ,
     *           concat('store', s_store_id) AS id ,
     *           sales ,
     *           returns_ ,
     *           (profit - profit_loss) AS profit
     *    FROM ssr
     *    UNION ALL SELECT 'catalog channel' AS channel ,
     *                     concat('catalog_page', cp_catalog_page_id) AS id ,
     *                     sales ,
     *                     returns_ ,
     *                     (profit - profit_loss) AS profit
     *    FROM csr
     *    UNION ALL SELECT 'web channel' AS channel ,
     *                     concat('web_site', web_site_id) AS id ,
     *                     sales ,
     *                     returns_ ,
     *                     (profit - profit_loss) AS profit
     *    FROM wsr ) x
     * GROUP BY ROLLUP (channel,
     *                  id)
     * ORDER BY channel NULLS FIRST,
     *          id NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q5")]
    public void Test_Q5()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var catalogPageQuery = TpcdsData.CatalogPageQuery();
        var webSiteQuery = TpcdsData.WebSiteQuery();

        var minDate = new DateOnly(2000, 8, 23);
        var maxDate = new DateOnly(2000, 9, 6);
        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);

        // 1. Store Channel
        var ssUnion = storeSalesQuery.Select(ss => new
        {
            store_sk = ss.ss_store_sk,
            date_sk = ss.ss_sold_date_sk,
            sales_price = ss.ss_ext_sales_price,
            profit = ss.ss_net_profit,
            return_amt = (decimal?)0.00m,
            net_loss = (decimal?)0.00m
        }).Concat(storeReturnsQuery.Select(sr => new
        {
            store_sk = sr.sr_store_sk,
            date_sk = sr.sr_returned_date_sk,
            sales_price = (decimal?)0.00m,
            profit = (decimal?)0.00m,
            return_amt = sr.sr_return_amt,
            net_loss = sr.sr_net_loss
        }));

        var ssr = from sr in ssUnion
                  join d in filteredDates on sr.date_sk equals d.d_date_sk
                  join s in storeQuery on sr.store_sk equals s.s_store_sk
                  group sr by s.s_store_id into g
                  select new
                  {
                      channel = "store channel",
                      id = "store" + g.Key,
                      sales = g.Sum(x => x.sales_price),
                      returns_ = g.Sum(x => x.return_amt),
                      profit = g.Sum(x => x.profit) - g.Sum(x => x.net_loss)
                  };

        // 2. Catalog Channel
        var csUnion = catalogSalesQuery.Select(cs => new
        {
            page_sk = cs.cs_catalog_page_sk,
            date_sk = cs.cs_sold_date_sk,
            sales_price = cs.cs_ext_sales_price,
            profit = cs.cs_net_profit,
            return_amt = (decimal?)0.00m,
            net_loss = (decimal?)0.00m
        }).Concat(catalogReturnsQuery.Select(cr => new
        {
            page_sk = cr.cr_catalog_page_sk,
            date_sk = cr.cr_returned_date_sk,
            sales_price = (decimal?)0.00m,
            profit = (decimal?)0.00m,
            return_amt = cr.cr_return_amount,
            net_loss = cr.cr_net_loss
        }));

        var csr = from cr in csUnion
                  join d in filteredDates on cr.date_sk equals d.d_date_sk
                  join cp in catalogPageQuery on cr.page_sk equals cp.cp_catalog_page_sk
                  group cr by cp.cp_catalog_page_id into g
                  select new
                  {
                      channel = "catalog channel",
                      id = "catalog_page" + g.Key,
                      sales = g.Sum(x => x.sales_price),
                      returns_ = g.Sum(x => x.return_amt),
                      profit = g.Sum(x => x.profit) - g.Sum(x => x.net_loss)
                  };

        // 3. Web Channel
        var wsPart = webSalesQuery.Select(ws => new
        {
            wsr_web_site_sk = ws.ws_web_site_sk,
            date_sk = ws.ws_sold_date_sk,
            sales_price = ws.ws_ext_sales_price,
            profit = ws.ws_net_profit,
            return_amt = (decimal?)0.00m,
            net_loss = (decimal?)0.00m
        });

        var wrJoined = from wr in webReturnsQuery
                       join ws in webSalesQuery on new { item_sk = wr.wr_item_sk, order_num = wr.wr_order_number }
                                            equals new { item_sk = ws.ws_item_sk, order_num = ws.ws_order_number }
                       select new
                       {
                           wsr_web_site_sk = ws.ws_web_site_sk,
                           date_sk = wr.wr_returned_date_sk,
                           sales_price = (decimal?)0.00m,
                           profit = (decimal?)0.00m,
                           return_amt = wr.wr_return_amt,
                           net_loss = wr.wr_net_loss
                       };

        var wsUnion = wsPart.Concat(wrJoined);

        var wsr = from w in wsUnion
                  join d in filteredDates on w.date_sk equals d.d_date_sk
                  join ws in webSiteQuery on w.wsr_web_site_sk equals ws.web_site_sk
                  group w by ws.web_site_id into g
                  select new
                  {
                      channel = "web channel",
                      id = "web_site" + g.Key,
                      sales = g.Sum(x => x.sales_price),
                      returns_ = g.Sum(x => x.return_amt),
                      profit = g.Sum(x => x.profit) - g.Sum(x => x.net_loss)
                  };

        // Combined stream x
        var x = ssr.Concat(csr).Concat(wsr);

        // 3-Level ROLLUP via fluent GroupByRollup
        var q5 = x
            .GroupByRollup(
                r => new { r.channel, r.id },
                (key, g) => new
                {
                    channel = (string?)key.channel,
                    id = (string?)key.id,
                    sales = g.Sum(v => v.sales),
                    returns_ = g.Sum(v => v.returns_),
                    profit = g.Sum(v => v.profit)
                }
            )
            .OrderBy(r => r.channel)
            .ThenBy(r => r.id)
            .Take(100);

        AssertPlan(ssr, NativePushdown);
        AssertPlan(csr, NativePushdown);
        AssertPlan(wsr, NativePushdown);
        AssertPlan(x, NativePushdown);
        AssertPlan(q5, NativePushdown);

        var polarsResult = q5.ToList();
        Assert.NotEmpty(polarsResult);

        // DuckDB Ground Truth Cross-Validation
        var duckDbSql = $@"
            WITH ssr AS
              (SELECT s_store_id,
                      sum(sales_price) AS sales,
                      sum(profit) AS profit,
                      sum(return_amt) AS returns_,
                      sum(net_loss) AS profit_loss
               FROM
                 (SELECT ss_store_sk AS store_sk,
                         ss_sold_date_sk AS date_sk,
                         ss_ext_sales_price AS sales_price,
                         ss_net_profit AS profit,
                         cast(0 AS decimal(7,2)) AS return_amt,
                         cast(0 AS decimal(7,2)) AS net_loss
                  FROM read_parquet('{TpcdsData.StoreSalesPath}')
                  UNION ALL SELECT sr_store_sk AS store_sk,
                                   sr_returned_date_sk AS date_sk,
                                   cast(0 AS decimal(7,2)) AS sales_price,
                                   cast(0 AS decimal(7,2)) AS profit,
                                   sr_return_amt AS return_amt,
                                   sr_net_loss AS net_loss
                  FROM read_parquet('{TpcdsData.StoreReturnsPath}') ) salesreturns,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
                 AND store_sk = s_store_sk
               GROUP BY s_store_id) ,
                 csr AS
              (SELECT cp_catalog_page_id,
                      sum(sales_price) AS sales,
                      sum(profit) AS profit,
                      sum(return_amt) AS returns_,
                      sum(net_loss) AS profit_loss
               FROM
                 (SELECT cs_catalog_page_sk AS page_sk,
                         cs_sold_date_sk AS date_sk,
                         cs_ext_sales_price AS sales_price,
                         cs_net_profit AS profit,
                         cast(0 AS decimal(7,2)) AS return_amt,
                         cast(0 AS decimal(7,2)) AS net_loss
                  FROM read_parquet('{TpcdsData.CatalogSalesPath}')
                  UNION ALL SELECT cr_catalog_page_sk AS page_sk,
                                   cr_returned_date_sk AS date_sk,
                                   cast(0 AS decimal(7,2)) AS sales_price,
                                   cast(0 AS decimal(7,2)) AS profit,
                                   cr_return_amount AS return_amt,
                                   cr_net_loss AS net_loss
                  FROM read_parquet('{TpcdsData.CatalogReturnsPath}') ) salesreturns,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.CatalogPagePath}') catalog_page
               WHERE date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
                 AND page_sk = cp_catalog_page_sk
               GROUP BY cp_catalog_page_id) ,
                 wsr AS
              (SELECT web_site_id,
                      sum(sales_price) AS sales,
                      sum(profit) AS profit,
                      sum(return_amt) AS returns_,
                      sum(net_loss) AS profit_loss
               FROM
                 (SELECT ws_web_site_sk AS wsr_web_site_sk,
                         ws_sold_date_sk AS date_sk,
                         ws_ext_sales_price AS sales_price,
                         ws_net_profit AS profit,
                         cast(0 AS decimal(7,2)) AS return_amt,
                         cast(0 AS decimal(7,2)) AS net_loss
                  FROM read_parquet('{TpcdsData.WebSalesPath}')
                  UNION ALL SELECT ws_web_site_sk AS wsr_web_site_sk,
                                   wr_returned_date_sk AS date_sk,
                                   cast(0 AS decimal(7,2)) AS sales_price,
                                   cast(0 AS decimal(7,2)) AS profit,
                                   wr_return_amt AS return_amt,
                                   wr_net_loss AS net_loss
                  FROM read_parquet('{TpcdsData.WebReturnsPath}') web_returns
                  LEFT OUTER JOIN read_parquet('{TpcdsData.WebSalesPath}') web_sales ON (wr_item_sk = ws_item_sk
                                                AND wr_order_number = ws_order_number) ) salesreturns,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.WebSitePath}') web_site
               WHERE date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
                 AND wsr_web_site_sk = web_site_sk
               GROUP BY web_site_id)
            SELECT channel ,
                   id ,
                   sum(sales) AS sales ,
                   sum(returns_) AS returns_ ,
                   sum(profit) AS profit
            FROM
              (SELECT 'store channel' AS channel ,
                      concat('store', s_store_id) AS id ,
                      sales ,
                      returns_ ,
                      (profit - profit_loss) AS profit
               FROM ssr
               UNION ALL SELECT 'catalog channel' AS channel ,
                                concat('catalog_page', cp_catalog_page_id) AS id ,
                                sales ,
                                returns_ ,
                                (profit - profit_loss) AS profit
               FROM csr
               UNION ALL SELECT 'web channel' AS channel ,
                                concat('web_site', web_site_id) AS id ,
                                sales ,
                                returns_ ,
                                (profit - profit_loss) AS profit
               FROM wsr ) x
            GROUP BY ROLLUP (channel,
                             id)
            ORDER BY channel NULLS FIRST,
                     id NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            channel = r.IsDBNull(0) ? (string?)null : r.GetString(0),
            id = r.IsDBNull(1) ? (string?)null : r.GetString(1),
            sales = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2),
            returns_ = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3),
            profit = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.channel, (string?)p.channel);
            Assert.Equal(d.id, (string?)p.id);
            Assert.Equal(d.sales ?? 0m, (decimal?)p.sales ?? 0m);
            Assert.Equal(d.returns_ ?? 0m, (decimal?)p.returns_ ?? 0m);
            Assert.Equal(d.profit ?? 0m, (decimal?)p.profit ?? 0m);
        }
    }

    /*
     * TPC-DS Query 6
     *
     * SELECT a.ca_state state,
     *        count(*) cnt
     * FROM customer_address a ,
     *      customer c ,
     *      store_sales s ,
     *      date_dim d ,
     *      item i
     * WHERE a.ca_address_sk = c.c_current_addr_sk
     *   AND c.c_customer_sk = s.ss_customer_sk
     *   AND s.ss_sold_date_sk = d.d_date_sk
     *   AND s.ss_item_sk = i.i_item_sk
     *   AND d.d_month_seq =
     *     (SELECT DISTINCT (d_month_seq)
     *      FROM date_dim
     *      WHERE d_year = 2001
     *        AND d_moy = 1 )
     *   AND i.i_current_price > 1.2 *
     *     (SELECT avg(j.i_current_price)
     *      FROM item j
     *      WHERE j.i_category = i.i_category)
     * GROUP BY a.ca_state
     * HAVING count(*) >= 10
     * ORDER BY cnt NULLS FIRST,
     *          a.ca_state NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q6")]
    public void Test_Q6()
    {
        var addressQuery = TpcdsData.CustomerAddressQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var dateDimSubQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();

        // 1. Month sequence subquery
        var targetMonths = (from d in dateDimSubQuery
                            where d.d_year == 2001 && d.d_moy == 1
                            select d.d_month_seq).Distinct();

        var filteredDates = from d in dateDimQuery
                            join m in targetMonths on d.d_month_seq equals m
                            select d;

        // 2. Item partition window condition using Over()
        var filteredItems = from i in itemQuery
                            where (double)i.i_current_price! > 1.2 * i.i_current_price.Average().Over(i.i_category)
                            select i;

        // 3. Multi-table join + GroupBy state + Having count >= 10
        var q6 = (from a in addressQuery
                  join c in customerQuery on a.ca_address_sk equals c.c_current_addr_sk
                  join s in storeSalesQuery on c.c_customer_sk equals s.ss_customer_sk
                  join d in filteredDates on s.ss_sold_date_sk equals d.d_date_sk
                  join i in filteredItems on s.ss_item_sk equals i.i_item_sk
                  group a by a.ca_state into g
                  where g.Count() >= 10
                  select new
                  {
                      state = g.Key,
                      cnt = (long)g.Count()
                  } into r
                  orderby r.cnt, r.state
                  select r).Take(100);

        AssertPlan(q6, NativePushdown);

        var polarsResult = q6.ToList();
        Assert.NotEmpty(polarsResult);

        // 4. Correlated subquery (self-join) alternative for item price filter
        using var lfAddress2 = TpcdsData.CustomerAddress();
        using var lfCustomer2 = TpcdsData.Customer();
        using var lfStoreSales2 = TpcdsData.StoreSales();
        using var lfDateDim2 = TpcdsData.DateDim();
        using var lfDateDimSub2 = TpcdsData.DateDim();
        using var lfItem2 = TpcdsData.Item();
        using var lfItemSub2 = TpcdsData.Item();

        var addressQuery2 = lfAddress2.AsQueryable<CustomerAddress>();
        var customerQuery2 = lfCustomer2.AsQueryable<Customer>();
        var storeSalesQuery2 = lfStoreSales2.AsQueryable<StoreSales>();
        var dateDimQuery2 = lfDateDim2.AsQueryable<DateDim>();
        var dateDimSubQuery2 = lfDateDimSub2.AsQueryable<DateDim>();
        var itemQuery2 = lfItem2.AsQueryable<Item>();
        var itemSubQuery2 = lfItemSub2.AsQueryable<Item>();

        var targetMonths2 = (from d in dateDimSubQuery2
                             where d.d_year == 2001 && d.d_moy == 1
                             select d.d_month_seq).Distinct();

        var filteredDates2 = from d in dateDimQuery2
                             join m in targetMonths2 on d.d_month_seq equals m
                             select d;

        var filteredItems2 = from i in itemQuery2
                             where i.i_current_price > (from j in itemSubQuery2
                                                        where j.i_category == i.i_category
                                                        select j.i_current_price).Average() * 1.2m
                             select i;

        var q6SelfJoin = (from a in addressQuery2
                          join c in customerQuery2 on a.ca_address_sk equals c.c_current_addr_sk
                          join s in storeSalesQuery2 on c.c_customer_sk equals s.ss_customer_sk
                          join d in filteredDates2 on s.ss_sold_date_sk equals d.d_date_sk
                          join i in filteredItems2 on s.ss_item_sk equals i.i_item_sk
                          group a by a.ca_state into g
                          where g.Count() >= 10
                          select new
                          {
                              state = g.Key,
                              cnt = (long)g.Count()
                          } into r
                          orderby r.cnt, r.state
                          select r).Take(100);

        AssertPlan(q6SelfJoin, NativePushdown);

        using var dfOver = q6.ToDataFrame();
        using var dfSelfJoin = q6SelfJoin.ToDataFrame();
        Assert.True(dfOver.Equals(dfSelfJoin));

        // DuckDB Cross-Validation
        var duckDbSql = $@"
            SELECT a.ca_state state,
                   count(*) cnt
            FROM read_parquet('{TpcdsData.CustomerAddressPath}') a ,
                 read_parquet('{TpcdsData.CustomerPath}') c ,
                 read_parquet('{TpcdsData.StoreSalesPath}') s ,
                 read_parquet('{TpcdsData.DateDimPath}') d ,
                 read_parquet('{TpcdsData.ItemPath}') i
            WHERE a.ca_address_sk = c.c_current_addr_sk
              AND c.c_customer_sk = s.ss_customer_sk
              AND s.ss_sold_date_sk = d.d_date_sk
              AND s.ss_item_sk = i.i_item_sk
              AND d.d_month_seq =
                (SELECT DISTINCT (d_month_seq)
                 FROM read_parquet('{TpcdsData.DateDimPath}')
                 WHERE d_year = 2001
                   AND d_moy = 1 )
              AND i.i_current_price > 1.2 *
                (SELECT avg(j.i_current_price)
                 FROM read_parquet('{TpcdsData.ItemPath}') j
                 WHERE j.i_category = i.i_category)
            GROUP BY a.ca_state
            HAVING count(*) >= 10
            ORDER BY cnt NULLS FIRST,
                     a.ca_state NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            state = r.GetString(0),
            cnt = r.GetInt64(1)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int idx = 0; idx < duckDbResults.Count; idx++)
        {
            var d = duckDbResults[idx];
            var p = polarsResult[idx];
            Assert.Equal(d.state, (string)p.state);
            Assert.Equal(d.cnt, (long)p.cnt);
        }
    }

    /*
     * TPC-DS Query 7
     *
     * SELECT i_item_id,
     *        avg(ss_quantity) agg1,
     *        avg(ss_list_price) agg2,
     *        avg(ss_coupon_amt) agg3,
     *        avg(ss_sales_price) agg4
     * FROM store_sales,
     *      customer_demographics,
     *      date_dim,
     *      item,
     *      promotion
     * WHERE ss_sold_date_sk = d_date_sk
     *   AND ss_item_sk = i_item_sk
     *   AND ss_cdemo_sk = cd_demo_sk
     *   AND ss_promo_sk = p_promo_sk
     *   AND cd_gender = 'M'
     *   AND cd_marital_status = 'S'
     *   AND cd_education_status = 'College'
     *   AND (p_channel_email = 'N'
     *        OR p_channel_event = 'N')
     *   AND d_year = 2000
     * GROUP BY i_item_id
     * ORDER BY i_item_id
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q7")]
    public void Test_Q7()
    {

        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var promotionQuery = TpcdsData.PromotionQuery();

        var filteredCd = customerDemographicsQuery.Where(cd => cd.cd_gender == "M"
                                                            && cd.cd_marital_status == "S"
                                                            && cd.cd_education_status == "College");

        var filteredPromo = promotionQuery.Where(p => p.p_channel_email == "N" || p.p_channel_event == "N");

        var filteredDates = dateDimQuery.Where(d => d.d_year == 2000);

        var q7 = (from ss in storeSalesQuery
                  join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                  join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                  join cd in filteredCd on ss.ss_cdemo_sk equals cd.cd_demo_sk
                  join p in filteredPromo on ss.ss_promo_sk equals p.p_promo_sk
                  group ss by i.i_item_id into g
                  select new
                  {
                      i_item_id = g.Key,
                      agg1 = g.Average(x => (double?)x.ss_quantity),
                      agg2 = g.Average(x => (double?)x.ss_list_price),
                      agg3 = g.Average(x => (double?)x.ss_coupon_amt),
                      agg4 = g.Average(x => (double?)x.ss_sales_price),
                  } into r
                  orderby r.i_item_id
                  select r).Take(100);

        AssertPlan(q7, NativePushdown);

        var polarsResult = q7.ToList();
        Assert.NotEmpty(polarsResult);

        // DuckDB Cross-Validation
        var duckDbSql = $@"
            SELECT i_item_id,
                   avg(ss_quantity) agg1,
                   avg(ss_list_price) agg2,
                   avg(ss_coupon_amt) agg3,
                   avg(ss_sales_price) agg4
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.CustomerDemographicsPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.PromotionPath}')
            WHERE ss_sold_date_sk = d_date_sk
              AND ss_item_sk = i_item_sk
              AND ss_cdemo_sk = cd_demo_sk
              AND ss_promo_sk = p_promo_sk
              AND cd_gender = 'M'
              AND cd_marital_status = 'S'
              AND cd_education_status = 'College'
              AND (p_channel_email = 'N'
                   OR p_channel_event = 'N')
              AND d_year = 2000
            GROUP BY i_item_id
            ORDER BY i_item_id
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.GetString(0),
            agg1 = r.IsDBNull(1) ? (double?)null : r.GetDouble(1),
            agg2 = r.IsDBNull(2) ? (double?)null : r.GetDouble(2),
            agg3 = r.IsDBNull(3) ? (double?)null : r.GetDouble(3),
            agg4 = r.IsDBNull(4) ? (double?)null : r.GetDouble(4),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int idx = 0; idx < duckDbResults.Count; idx++)
        {
            var d = duckDbResults[idx];
            var p = polarsResult[idx];
            Assert.Equal(d.i_item_id, (string)p.i_item_id);
            if (d.agg1 == null) Assert.Null(p.agg1); else Assert.Equal(d.agg1.Value, (double)p.agg1!, 4);
            if (d.agg2 == null) Assert.Null(p.agg2); else Assert.Equal(d.agg2.Value, (double)p.agg2!, 4);
            if (d.agg3 == null) Assert.Null(p.agg3); else Assert.Equal(d.agg3.Value, (double)p.agg3!, 4);
            if (d.agg4 == null) Assert.Null(p.agg4); else Assert.Equal(d.agg4.Value, (double)p.agg4!, 4);
        }
    }

    private static readonly string[] Q8TargetZips = [
        "24128", "76232", "65084", "87816", "83926", "77556", "20548", "26231", "43848", "15126",
        "91137", "61265", "98294", "25782", "17920", "18426", "98235", "40081", "84093", "28577",
        "55565", "17183", "54601", "67897", "22752", "86284", "18376", "38607", "45200", "21756",
        "29741", "96765", "23932", "89360", "29839", "25989", "28898", "91068", "72550", "10390",
        "18845", "47770", "82636", "41367", "76638", "86198", "81312", "37126", "39192", "88424",
        "72175", "81426", "53672", "10445", "42666", "66864", "66708", "41248", "48583", "82276",
        "18842", "78890", "49448", "14089", "38122", "34425", "79077", "19849", "43285", "39861",
        "66162", "77610", "13695", "99543", "83444", "83041", "12305", "57665", "68341", "25003",
        "57834", "62878", "49130", "81096", "18840", "27700", "23470", "50412", "21195", "16021",
        "76107", "71954", "68309", "18119", "98359", "64544", "10336", "86379", "27068", "39736",
        "98569", "28915", "24208", "56529", "57647", "54940", "91631", "43844", "21270", "96494",
        "57195", "52775", "16994", "82626", "51235", "36918", "83698", "64501", "28294", "43254",
        "46875", "20188", "39939", "23102", "39473", "32426", "74005", "66123", "19869", "24284",
        "65520", "70370", "85643", "40214", "79699", "58051", "70281", "73315", "23105", "76110",
        "50508", "71866", "53971", "66081", "67308", "42359", "29465", "94576", "24107", "97389",
        "75005", "10740", "33725", "27549", "22367", "66682", "55845", "10363", "38324", "42962",
        "35442", "54246", "38125", "44810", "43535", "82762", "90772", "99849", "92183", "98895",
        "76586", "74586", "78340", "58749", "67980", "11388", "70946", "60453", "42085", "57768",
        "66108", "44641", "39626", "61348", "28394", "19551", "10980", "43560", "37435", "30282",
        "94042", "27760", "77915", "51200", "51222", "85781", "47747", "45519", "43962", "50192",
        "73428", "32126", "76140", "61310", "12066", "76635", "86376", "85683", "37869", "42397",
        "39061", "36395", "85913", "53365", "37876", "39805", "63708", "40984", "60063", "15672",
        "40858", "98982", "68349", "84360", "22432", "18483", "63772", "39809", "49439", "92789",
        "42581", "46926", "59033", "59416", "82003", "27660", "11624", "85625", "84961", "35961",
        "80593", "17258", "55131", "92058", "72183", "81111", "52692", "15339", "77638", "15366",
        "78589", "59275", "76903", "24718", "13025", "37933", "83462", "78404", "68323", "36915",
        "16773", "95662", "42689", "83782", "77905", "67224", "97984", "51148", "16029", "17954",
        "52925", "30141", "80140", "91243", "78272", "52057", "57877", "42838", "21114", "83962",
        "56651", "57126", "10397", "10476", "32270", "43633", "76000", "93173", "89848", "47256",
        "25377", "62373", "47583", "31500", "40277", "20257", "64582", "67768", "85034", "87140",
        "54648", "78138", "53708", "30934", "13193", "60218", "65673", "92081", "12065", "83891",
        "90457", "16450", "45539", "20601", "10093", "20008", "85397", "27363", "47305", "99307",
        "92987", "52972", "80469", "44052", "41747", "93707", "29380", "69124", "47669", "65344",
        "58120", "47541", "56101", "92636", "13377", "32779", "44061", "73570", "71231", "81335",
        "64872", "66872", "25368", "79805", "44524", "21671", "37736", "42233", "52232", "52719",
        "61626", "88805", "25876", "49601", "48763", "98683", "22478", "35769", "73905", "83765",
        "67865", "76359", "73019", "28455", "81215", "57833", "20738", "61804", "42940", "28724",
        "48089", "71863", "58735", "52631", "79358", "74170", "20654", "46643", "18734", "89915",
        "41317", "63954", "21753", "46587", "54207", "26524", "17764", "88255", "39968", "70320",
        "87534", "13054", "77274", "60666", "96554", "46049", "17776", "41247", "44851", "95877",
        "61166", "30605", "54608", "26780", "99009", "20372", "53268", "59858", "60913", "89533",
        "28616", "71101", "18984", "66160", "78052", "86976", "49269", "83886", "52437", "70578",
        "88321", "50061", "73145", "37691", "41991", "74773", "53426", "35680", "75466", "12246",
        "30066", "64720", "38867", "48481", "21193", "46228", "47244", "89508", "74494", "64818",
        "23075", "50416", "25733", "50863", "94324", "14458", "20100", "21900", "13417", "55791",
        "28655", "20106", "26536", "86199", "71618", "37511", "98730", "14871", "51961", "92186",
        "76676", "92557", "28603", "84352", "88050", "18139", "70288", "40921", "31858", "21177",
        "82425", "73140", "66218", "49939", "88168", "50163", "84098", "66649", "60125", "12461",
        "44977", "66301", "25584", "59673", "17761", "17373", "30050", "61387", "51607", "26508",
        "53131", "98083", "63322", "40996", "55777", "25913", "55409", "27859", "72555", "66512",
        "45451", "13494", "42405", "49673", "80841", "67398", "62745", "35701", "70187", "53910",
        "65881", "44022", "40164", "12970", "70920", "87784", "10811", "82640", "36368", "39723",
        "67009", "52263", "71948", "43656", "37877", "25538", "25276", "31272", "40857", "26545",
        "64324", "88758", "85382", "95050", "77105", "67871", "39920", "78465", "54602", "10653",
        "79580", "65813", "31071", "74161", "81708", "44550", "21790", "33534", "59845", "78933",
        "57419", "17510", "71668", "63792", "31589", "53524", "30802", "17406", "49101", "70272",
        "54409", "59897", "10008", "43149", "19239", "78270", "96445", "22469", "20539", "12692",
        "96840", "25097", "32982", "71504", "20589", "81269", "49221", "79703", "70837", "35160",
        "54589", "87097", "27197", "16715", "10643", "43805", "67020", "61394", "45701", "68725",
        "27422", "24626", "22608", "65659", "37400", "44529", "46081", "72228", "60769", "40520",
        "53341", "35265", "28698", "12044", "95543", "69770", "83286", "83849", "68908", "26689",
        "96451", "38193", "46820", "88885", "84935", "69035", "83144", "47537", "56616", "94983",
        "48033", "69952", "25486", "61547", "27385", "61860", "58048", "56910", "16807", "17871",
        "35258", "31387", "35458", "35576"
    ];

    /*
     * TPC-DS Query 8
     *
     * SELECT s_store_name,
     *        sum(ss_net_profit)
     * FROM store_sales,
     *      date_dim,
     *      store,
     *   (SELECT ca_zip
     *    FROM
     *      (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip
     *       FROM customer_address
     *       WHERE SUBSTRING(ca_zip, 1, 5) IN (<400 zip codes>)
     *       INTERSECT
     *       SELECT ca_zip
     *       FROM
     *         (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip,
     *                 count(*) cnt
     *          FROM customer_address,
     *               customer
     *          WHERE ca_address_sk = c_current_addr_sk
     *            AND c_preferred_cust_flag='Y'
     *          GROUP BY ca_zip
     *          HAVING count(*) > 10)A1)A2) V1
     * WHERE ss_store_sk = s_store_sk
     *   AND ss_sold_date_sk = d_date_sk
     *   AND d_qoy = 2
     *   AND d_year = 1998
     *   AND (SUBSTRING(s_zip, 1, 2) = SUBSTRING(V1.ca_zip, 1, 2))
     * GROUP BY s_store_name
     * ORDER BY s_store_name
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q8")]
    public void Test_Q8()
    {
        IQueryable<dynamic> BuildQuery(string[] targetZips, long minCount)
        {
            var storeSalesQuery = TpcdsData.StoreSalesQuery();
            var dateDimQuery = TpcdsData.DateDimQuery();
            var storeQuery = TpcdsData.StoreQuery();
            var addressQuery1 = TpcdsData.CustomerAddressQuery();
            var addressQuery2 = TpcdsData.CustomerAddressQuery();
            var customerQuery = TpcdsData.CustomerQuery();

            // V1: ca_zip set intersection
            var z1 = (from ca in addressQuery1
                      where ca.ca_zip != null && targetZips.Contains(ca.ca_zip.Substring(0, 5))
                      select new { ca_zip = ca.ca_zip!.Substring(0, 5) }).Distinct();

            var z2 = from ca in addressQuery2
                     join c in customerQuery on ca.ca_address_sk equals c.c_current_addr_sk
                     where c.c_preferred_cust_flag == "Y" && ca.ca_zip != null
                     group ca by ca.ca_zip!.Substring(0, 5) into g
                     where g.Count() > minCount
                     select new { ca_zip = g.Key };

            var v1 = z1.Intersect(z2);

            var filteredDates = dateDimQuery.Where(d => d.d_qoy == 2 && d.d_year == 1998);

            var query = (from ss in storeSalesQuery
                         join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                         join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                         join v in v1 on s.s_zip!.Substring(0, 2) equals v.ca_zip.Substring(0, 2)
                         group ss by s.s_store_name into g
                         select new
                         {
                             s_store_name = g.Key,
                             net_profit = g.Sum(x => x.ss_net_profit)
                         } into r
                         orderby r.s_store_name
                         select r).Take(100);

            return query;
        }

        // 1. Standard parameter (400 zip codes, minCount = 10, yields 0 rows on SF=0.01)
        var standardQuery = BuildQuery(Q8TargetZips, 10);
        AssertPlan(standardQuery, NativePushdown);

        var standardResult = standardQuery.ToList();
        Assert.Empty(standardResult);

        // 2. Active parameter with data on SF=0.01 (active zips starting with '31', minCount = 0)
        string[] activeZips = ["31289", "31749"];
        var activeQuery = BuildQuery(activeZips, 0);
        AssertPlan(activeQuery, NativePushdown);

        var activeResult = activeQuery.ToList();
        Assert.NotEmpty(activeResult);

        // DuckDB Cross-Validation for active parameter
        var duckDbSql = $@"
            WITH v1 AS (
              SELECT ca_zip
               FROM
                 (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip
                  FROM read_parquet('{TpcdsData.CustomerAddressPath}')
                  WHERE SUBSTRING(ca_zip, 1, 5) IN ('31289', '31749')
                  INTERSECT
                  SELECT ca_zip
                  FROM
                    (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip,
                            count(*) cnt
                     FROM read_parquet('{TpcdsData.CustomerAddressPath}'),
                          read_parquet('{TpcdsData.CustomerPath}')
                     WHERE ca_address_sk = c_current_addr_sk
                       AND c_preferred_cust_flag='Y'
                     GROUP BY ca_zip
                     HAVING count(*) > 0)A1)A2)
            SELECT s_store_name,
                   sum(ss_net_profit)
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.StorePath}'),
                 v1
            WHERE ss_store_sk = s_store_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_qoy = 2
              AND d_year = 1998
              AND (SUBSTRING(s_zip, 1, 2) = SUBSTRING(v1.ca_zip, 1, 2))
            GROUP BY s_store_name
            ORDER BY s_store_name
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            s_store_name = r.GetString(0),
            net_profit = r.GetDecimal(1)
        });

        Assert.Equal(duckDbResults.Count, activeResult.Count);
        for (int idx = 0; idx < duckDbResults.Count; idx++)
        {
            var d = duckDbResults[idx];
            var p = activeResult[idx];
            Assert.Equal(d.s_store_name, (string)p.s_store_name);
            Assert.Equal(d.net_profit, (decimal)p.net_profit);
        }
    }

    /*
     * TPC-DS Query 9
     *
     * SELECT CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 1 AND 20) > 74129 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 1 AND 20)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 1 AND 20)
     *        END bucket1,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 21 AND 40) > 122840 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 21 AND 40)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 21 AND 40)
     *        END bucket2,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 41 AND 60) > 56580 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 41 AND 60)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 41 AND 60)
     *        END bucket3,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 61 AND 80) > 10097 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 61 AND 80)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 61 AND 80)
     *        END bucket4,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 81 AND 100) > 165306 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 81 AND 100)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 81 AND 100)
     *        END bucket5
     * FROM reason
     * WHERE r_reason_sk = 1;
     */
    [Fact]
    [Trait("TPCDS", "Q9")]
    public void Test_Q9()
    {
        var reasonQuery = TpcdsData.ReasonQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();

        var q9 = from r in reasonQuery.Where(r => r.r_reason_sk == 1)
                 select new
                 {
                     bucket1 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 1 && ss.ss_quantity <= 20).LongCount() > 74129)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 1 && ss.ss_quantity <= 20).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 1 && ss.ss_quantity <= 20).Average(ss => (double?)ss.ss_net_paid),
                     bucket2 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 21 && ss.ss_quantity <= 40).LongCount() > 122840)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 21 && ss.ss_quantity <= 40).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 21 && ss.ss_quantity <= 40).Average(ss => (double?)ss.ss_net_paid),
                     bucket3 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 41 && ss.ss_quantity <= 60).LongCount() > 56580)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 41 && ss.ss_quantity <= 60).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 41 && ss.ss_quantity <= 60).Average(ss => (double?)ss.ss_net_paid),
                     bucket4 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 61 && ss.ss_quantity <= 80).LongCount() > 10097)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 61 && ss.ss_quantity <= 80).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 61 && ss.ss_quantity <= 80).Average(ss => (double?)ss.ss_net_paid),
                     bucket5 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 81 && ss.ss_quantity <= 100).LongCount() > 165306)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 81 && ss.ss_quantity <= 100).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 81 && ss.ss_quantity <= 100).Average(ss => (double?)ss.ss_net_paid),
                 };

        AssertPlan(q9, NativePushdown);

        var polarsResult = q9.ToList();
        Assert.Single(polarsResult);

        // DuckDB Cross-Validation
        var duckDbSql = $@"
            SELECT CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 1 AND 20) > 74129 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 1 AND 20)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 1 AND 20)
                   END bucket1,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 21 AND 40) > 122840 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 21 AND 40)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 21 AND 40)
                   END bucket2,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 41 AND 60) > 56580 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 41 AND 60)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 41 AND 60)
                   END bucket3,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 61 AND 80) > 10097 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 61 AND 80)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 61 AND 80)
                   END bucket4,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 81 AND 100) > 165306 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 81 AND 100)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 81 AND 100)
                   END bucket5
            FROM read_parquet('{TpcdsData.ReasonPath}')
            WHERE r_reason_sk = 1;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            bucket1 = r.GetDouble(0),
            bucket2 = r.GetDouble(1),
            bucket3 = r.GetDouble(2),
            bucket4 = r.GetDouble(3),
            bucket5 = r.GetDouble(4),
        });

        Assert.Single(duckDbResults);
        var d = duckDbResults[0];
        var p = polarsResult[0];

        Assert.Equal(d.bucket1, p.bucket1!.Value, 4);
        Assert.Equal(d.bucket2, p.bucket2!.Value, 4);
        Assert.Equal(d.bucket3, p.bucket3!.Value, 4);
        Assert.Equal(d.bucket4, p.bucket4!.Value, 4);
        Assert.Equal(d.bucket5, p.bucket5!.Value, 4);
    }

    private static readonly string[] Q10TargetCounties = [
        "Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"
    ];

    /*
     * TPC-DS Query 10
     *
     * SELECT cd_gender,
     *        cd_marital_status,
     *        cd_education_status,
     *        count(*) cnt1,
     *        cd_purchase_estimate,
     *        count(*) cnt2,
     *        cd_credit_rating,
     *        count(*) cnt3,
     *        cd_dep_count,
     *        count(*) cnt4,
     *        cd_dep_employed_count,
     *        count(*) cnt5,
     *        cd_dep_college_count,
     *        count(*) cnt6
     * FROM customer c,
     *      customer_address ca,
     *      customer_demographics
     * WHERE c.c_current_addr_sk = ca.ca_address_sk
     *   AND ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
     *   AND cd_demo_sk = c.c_current_cdemo_sk
     *   AND EXISTS
     *     (SELECT *
     *      FROM store_sales,
     *           date_dim
     *      WHERE c.c_customer_sk = ss_customer_sk
     *        AND ss_sold_date_sk = d_date_sk
     *        AND d_year = 2002
     *        AND d_moy BETWEEN 1 AND 1+3)
     *   AND (EXISTS
     *          (SELECT *
     *           FROM web_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = ws_bill_customer_sk
     *             AND ws_sold_date_sk = d_date_sk
     *             AND d_year = 2002
     *             AND d_moy BETWEEN 1 AND 1+3)
     *        OR EXISTS
     *          (SELECT *
     *           FROM catalog_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = cs_ship_customer_sk
     *             AND cs_sold_date_sk = d_date_sk
     *             AND d_year = 2002
     *             AND d_moy BETWEEN 1 AND 1+3))
     * GROUP BY cd_gender,
     *          cd_marital_status,
     *          cd_education_status,
     *          cd_purchase_estimate,
     *          cd_credit_rating,
     *          cd_dep_count,
     *          cd_dep_employed_count,
     *          cd_dep_college_count
     * ORDER BY cd_gender,
     *          cd_marital_status,
     *          cd_education_status,
     *          cd_purchase_estimate,
     *          cd_credit_rating,
     *          cd_dep_count,
     *          cd_dep_employed_count,
     *          cd_dep_college_count
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q10")]
    public void Test_Q10()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var storeSalesInPeriod = from ss in storeSalesQuery
                                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                                 where d.d_year == 2002 && d.d_moy >= 1 && d.d_moy <= 4
                                 select ss;

        var webSalesInPeriod = from ws in webSalesQuery
                               join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                               where d.d_year == 2002 && d.d_moy >= 1 && d.d_moy <= 4
                               select ws;

        var catSalesInPeriod = from cs in catalogSalesQuery
                               join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                               where d.d_year == 2002 && d.d_moy >= 1 && d.d_moy <= 4
                               select cs;

        var q10 = (from c in customerQuery
                   join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                   join cd in customerDemographicsQuery on c.c_current_cdemo_sk equals cd.cd_demo_sk
                   where Q10TargetCounties.Contains(ca.ca_county) &&
                         storeSalesInPeriod.Any(ss => ss.ss_customer_sk == c.c_customer_sk) &&
                         (webSalesInPeriod.Any(ws => ws.ws_bill_customer_sk == c.c_customer_sk) ||
                          catSalesInPeriod.Any(cs => cs.cs_ship_customer_sk == c.c_customer_sk))
                   group cd by new
                   {
                       cd.cd_gender,
                       cd.cd_marital_status,
                       cd.cd_education_status,
                       cd.cd_purchase_estimate,
                       cd.cd_credit_rating,
                       cd.cd_dep_count,
                       cd.cd_dep_employed_count,
                       cd.cd_dep_college_count,
                   } into g
                   orderby g.Key.cd_gender,
                           g.Key.cd_marital_status,
                           g.Key.cd_education_status,
                           g.Key.cd_purchase_estimate,
                           g.Key.cd_credit_rating,
                           g.Key.cd_dep_count,
                           g.Key.cd_dep_employed_count,
                           g.Key.cd_dep_college_count
                   select new
                   {
                       cd_gender = g.Key.cd_gender,
                       cd_marital_status = g.Key.cd_marital_status,
                       cd_education_status = g.Key.cd_education_status,
                       cnt1 = g.LongCount(),
                       cd_purchase_estimate = g.Key.cd_purchase_estimate,
                       cnt2 = g.LongCount(),
                       cd_credit_rating = g.Key.cd_credit_rating,
                       cnt3 = g.LongCount(),
                       cd_dep_count = g.Key.cd_dep_count,
                       cnt4 = g.LongCount(),
                       cd_dep_employed_count = g.Key.cd_dep_employed_count,
                       cnt5 = g.LongCount(),
                       cd_dep_college_count = g.Key.cd_dep_college_count,
                       cnt6 = g.LongCount(),
                   }).Take(100);

        AssertPlan(q10, NativePushdown);

        var polarsResult = q10.ToList();

        // DuckDB Cross-Validation
        var duckDbSql = $@"
            SELECT 
                cd_gender, 
                cd_marital_status, 
                cd_education_status, 
                COUNT(*) AS cnt1, 
                cd_purchase_estimate, 
                COUNT(*) AS cnt2, 
                cd_credit_rating, 
                COUNT(*) AS cnt3, 
                cd_dep_count, 
                COUNT(*) AS cnt4, 
                cd_dep_employed_count, 
                COUNT(*) AS cnt5, 
                cd_dep_college_count, 
                COUNT(*) AS cnt6 
            FROM 
                read_parquet('{TpcdsData.CustomerPath}') c, 
                read_parquet('{TpcdsData.CustomerAddressPath}') ca, 
                read_parquet('{TpcdsData.CustomerDemographicsPath}') cd
            WHERE 
                c.c_current_addr_sk = ca.ca_address_sk 
                AND ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County') 
                AND cd.cd_demo_sk = c.c_current_cdemo_sk 
                AND EXISTS (
                    SELECT * 
                    FROM read_parquet('{TpcdsData.StoreSalesPath}') ss, 
                         read_parquet('{TpcdsData.DateDimPath}') d 
                    WHERE c.c_customer_sk = ss.ss_customer_sk 
                      AND ss.ss_sold_date_sk = d.d_date_sk 
                      AND d.d_year = 2002 
                      AND d.d_moy BETWEEN 1 AND 1 + 3
                ) 
                AND (
                    EXISTS (
                        SELECT * 
                        FROM read_parquet('{TpcdsData.WebSalesPath}') ws, 
                             read_parquet('{TpcdsData.DateDimPath}') d 
                        WHERE c.c_customer_sk = ws.ws_bill_customer_sk 
                          AND ws.ws_sold_date_sk = d.d_date_sk 
                          AND d.d_year = 2002 
                          AND d.d_moy BETWEEN 1 AND 1 + 3
                    ) 
                    OR EXISTS (
                        SELECT * 
                        FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs, 
                             read_parquet('{TpcdsData.DateDimPath}') d 
                        WHERE c.c_customer_sk = cs.cs_ship_customer_sk 
                          AND cs_sold_date_sk = d.d_date_sk 
                          AND d.d_year = 2002 
                          AND d.d_moy BETWEEN 1 AND 1 + 3
                    )
                ) 
            GROUP BY 
                cd_gender, 
                cd_marital_status, 
                cd_education_status, 
                cd_purchase_estimate, 
                cd_credit_rating, 
                cd_dep_count, 
                cd_dep_employed_count, 
                cd_dep_college_count 
            ORDER BY 
                cd_gender, 
                cd_marital_status, 
                cd_education_status, 
                cd_purchase_estimate, 
                cd_credit_rating, 
                cd_dep_count, 
                cd_dep_employed_count, 
                cd_dep_college_count 
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            cd_gender = r.IsDBNull(0) ? null : r.GetString(0),
            cd_marital_status = r.IsDBNull(1) ? null : r.GetString(1),
            cd_education_status = r.IsDBNull(2) ? null : r.GetString(2),
            cnt1 = r.GetInt64(3),
            cd_purchase_estimate = r.IsDBNull(4) ? null : (long?)r.GetInt64(4),
            cnt2 = r.GetInt64(5),
            cd_credit_rating = r.IsDBNull(6) ? null : r.GetString(6),
            cnt3 = r.GetInt64(7),
            cd_dep_count = r.IsDBNull(8) ? null : (long?)r.GetInt64(8),
            cnt4 = r.GetInt64(9),
            cd_dep_employed_count = r.IsDBNull(10) ? null : (long?)r.GetInt64(10),
            cnt5 = r.GetInt64(11),
            cd_dep_college_count = r.IsDBNull(12) ? null : (int?)r.GetInt32(12),
            cnt6 = r.GetInt64(13),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.cd_gender, (string?)p.cd_gender);
            Assert.Equal(d.cd_marital_status, (string?)p.cd_marital_status);
            Assert.Equal(d.cd_education_status, (string?)p.cd_education_status);
            Assert.Equal(d.cnt1, p.cnt1);
            Assert.Equal(d.cd_purchase_estimate, p.cd_purchase_estimate);
            Assert.Equal(d.cnt2, p.cnt2);
            Assert.Equal(d.cd_credit_rating, (string?)p.cd_credit_rating);
            Assert.Equal(d.cnt3, p.cnt3);
            Assert.Equal(d.cd_dep_count, p.cd_dep_count);
            Assert.Equal(d.cnt4, p.cnt4);
            Assert.Equal(d.cd_dep_employed_count, p.cd_dep_employed_count);
            Assert.Equal(d.cnt5, p.cnt5);
            Assert.Equal(d.cd_dep_college_count, p.cd_dep_college_count);
            Assert.Equal(d.cnt6, p.cnt6);
        }
    }
}
