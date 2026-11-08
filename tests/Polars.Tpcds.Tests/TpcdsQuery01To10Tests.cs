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
        var limitedQ1 = TpcdsQueries.Q1();

        AssertPlan(limitedQ1, NativePushdown);

        var polarsResult = limitedQ1.ToList();
        Assert.NotEmpty(polarsResult);

        // Ground-truth cross validation with DuckDB
        var duckDbSql = TpcdsSql.Q1();

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
                     r1 = (yItem.sun_sales1 == null || zItem.sun_sales2 == null || zItem.sun_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.sun_sales1 / (double)zItem.sun_sales2, 2),
                     r2 = (yItem.mon_sales1 == null || zItem.mon_sales2 == null || zItem.mon_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.mon_sales1 / (double)zItem.mon_sales2, 2),
                     r3 = (yItem.tue_sales1 == null || zItem.tue_sales2 == null || zItem.tue_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.tue_sales1 / (double)zItem.tue_sales2, 2),
                     r4 = (yItem.wed_sales1 == null || zItem.wed_sales2 == null || zItem.wed_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.wed_sales1 / (double)zItem.wed_sales2, 2),
                     r5 = (yItem.thu_sales1 == null || zItem.thu_sales2 == null || zItem.thu_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.thu_sales1 / (double)zItem.thu_sales2, 2),
                     r6 = (yItem.fri_sales1 == null || zItem.fri_sales2 == null || zItem.fri_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.fri_sales1 / (double)zItem.fri_sales2, 2),
                     r7 = (yItem.sat_sales1 == null || zItem.sat_sales2 == null || zItem.sat_sales2 == 0m)
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

        var standardQuery = TpcdsQueries.Q3();
        AssertPlan(standardQuery, NativePushdown);

        var standardResults = standardQuery.ToList();

        // DuckDB Cross Validation
        var duckDbSql = TpcdsSql.Q3();

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            d_year = r.GetInt64(0),
            brand_id = r.GetInt64(1),
            brand = r.GetString(2),
            sum_agg = r.GetDecimal(3)
        });

        Assert.Equal(duckDbResults.Count, standardResults.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = standardResults[i];
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

        Assert.NotEmpty(polarsResult);
        TpcdsAssert.RowsEqual(TpcdsAssert.DuckDbRows(duckDbSql), polarsResult);
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
        var q5 = TpcdsQueries.Q5();

        AssertPlan(q5, NativePushdown);

        var polarsResult = q5.ToList();
        Assert.NotEmpty(polarsResult);

        // DuckDB Ground Truth Cross-Validation
        var duckDbSql = TpcdsSql.Q5();

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
            Assert.Equal(d.sales, (decimal?)p.sales);
            Assert.Equal(d.returns_, (decimal?)p.returns_);
            Assert.Equal(d.profit, (decimal?)p.profit);
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
        var q6 = TpcdsQueries.Q6();

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
                                                        where j.i_category != null && j.i_category == i.i_category // SQL = never matches NULL
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
                              cnt = g.LongCount()
                          } into r
                          orderby r.cnt, r.state
                          select r).Take(100);

        AssertPlan(q6SelfJoin, NativePushdown);

        using var dfOver = q6.ToDataFrame();
        using var dfSelfJoin = q6SelfJoin.ToDataFrame();
        Assert.True(dfOver.Equals(dfSelfJoin));

        // DuckDB Cross-Validation
        var duckDbSql = TpcdsSql.Q6();

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            state = r.IsDBNull(0) ? null : r.GetString(0),
            cnt = r.GetInt64(1)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int idx = 0; idx < duckDbResults.Count; idx++)
        {
            var d = duckDbResults[idx];
            var p = polarsResult[idx];
            Assert.Equal(d.state, (string?)p.state);
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

    // The 400 zip codes of the official TPC-DS Q8 (DuckDB tpcds_queries()).
    private static readonly string[] Q8TargetZips = [
        "24128", "76232", "65084", "87816", "83926", "77556", "20548", "26231",
        "43848", "15126", "91137", "61265", "98294", "25782", "17920", "18426",
        "98235", "40081", "84093", "28577", "55565", "17183", "54601", "67897",
        "22752", "86284", "18376", "38607", "45200", "21756", "29741", "96765",
        "23932", "89360", "29839", "25989", "28898", "91068", "72550", "10390",
        "18845", "47770", "82636", "41367", "76638", "86198", "81312", "37126",
        "39192", "88424", "72175", "81426", "53672", "10445", "42666", "66864",
        "66708", "41248", "48583", "82276", "18842", "78890", "49448", "14089",
        "38122", "34425", "79077", "19849", "43285", "39861", "66162", "77610",
        "13695", "99543", "83444", "83041", "12305", "57665", "68341", "25003",
        "57834", "62878", "49130", "81096", "18840", "27700", "23470", "50412",
        "21195", "16021", "76107", "71954", "68309", "18119", "98359", "64544",
        "10336", "86379", "27068", "39736", "98569", "28915", "24206", "56529",
        "57647", "54917", "42961", "91110", "63981", "14922", "36420", "23006",
        "67467", "32754", "30903", "20260", "31671", "51798", "72325", "85816",
        "68621", "13955", "36446", "41766", "68806", "16725", "15146", "22744",
        "35850", "88086", "51649", "18270", "52867", "39972", "96976", "63792",
        "11376", "94898", "13595", "10516", "90225", "58943", "39371", "94945",
        "28587", "96576", "57855", "28488", "26105", "83933", "25858", "34322",
        "44438", "73171", "30122", "34102", "22685", "71256", "78451", "54364",
        "13354", "45375", "40558", "56458", "28286", "45266", "47305", "69399",
        "83921", "26233", "11101", "15371", "69913", "35942", "15882", "25631",
        "24610", "44165", "99076", "33786", "70738", "26653", "14328", "72305",
        "62496", "22152", "10144", "64147", "48425", "14663", "21076", "18799",
        "30450", "63089", "81019", "68893", "24996", "51200", "51211", "45692",
        "92712", "70466", "79994", "22437", "25280", "38935", "71791", "73134",
        "56571", "14060", "19505", "72425", "56575", "74351", "68786", "51650",
        "20004", "18383", "76614", "11634", "18906", "15765", "41368", "73241",
        "76698", "78567", "97189", "28545", "76231", "75691", "22246", "51061",
        "90578", "56691", "68014", "51103", "94167", "57047", "14867", "73520",
        "15734", "63435", "25733", "35474", "24676", "94627", "53535", "17879",
        "15559", "53268", "59166", "11928", "59402", "33282", "45721", "43933",
        "68101", "33515", "36634", "71286", "19736", "58058", "55253", "67473",
        "41918", "19515", "36495", "19430", "22351", "77191", "91393", "49156",
        "50298", "87501", "18652", "53179", "18767", "63193", "23968", "65164",
        "68880", "21286", "72823", "58470", "67301", "13394", "31016", "70372",
        "67030", "40604", "24317", "45748", "39127", "26065", "77721", "31029",
        "31880", "60576", "24671", "45549", "13376", "50016", "33123", "19769",
        "22927", "97789", "46081", "72151", "15723", "46136", "51949", "68100",
        "96888", "64528", "14171", "79777", "28709", "11489", "25103", "32213",
        "78668", "22245", "15798", "27156", "37930", "62971", "21337", "51622",
        "67853", "10567", "38415", "15455", "58263", "42029", "60279", "37125",
        "56240", "88190", "50308", "26859", "64457", "89091", "82136", "62377",
        "36233", "63837", "58078", "17043", "30010", "60099", "28810", "98025",
        "29178", "87343", "73273", "30469", "64034", "39516", "86057", "21309",
        "90257", "67875", "40162", "11356", "73650", "61810", "72013", "30431",
        "22461", "19512", "13375", "55307", "30625", "83849", "68908", "26689",
        "96451", "38193", "46820", "88885", "84935", "69035", "83144", "47537",
        "56616", "94983", "48033", "69952", "25486", "61547", "27385", "61860",
        "58048", "56910", "16807", "17871", "35258", "31387", "35458", "35576"
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

        var standardQuery = BuildQuery(Q8TargetZips, 10);
        AssertPlan(standardQuery, NativePushdown);

        var standardResult = standardQuery.ToList();
        Assert.NotEmpty(standardResult);

        var zipList = string.Join(", ", Q8TargetZips.Select(z => $"'{z}'"));
        var duckDbSql = $@"
            SELECT s_store_name,
                   sum(ss_net_profit)
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.StorePath}'),
              (SELECT ca_zip
               FROM
                 (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip
                  FROM read_parquet('{TpcdsData.CustomerAddressPath}')
                  WHERE SUBSTRING(ca_zip, 1, 5) IN ({zipList})
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
                     HAVING count(*) > 10)A1)A2) V1
            WHERE ss_store_sk = s_store_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_qoy = 2
              AND d_year = 1998
              AND (SUBSTRING(s_zip, 1, 2) = SUBSTRING(V1.ca_zip, 1, 2))
            GROUP BY s_store_name
            ORDER BY s_store_name NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            s_store_name = r.GetString(0),
            net_profit = r.GetDecimal(1)
        });

        Assert.Equal(duckDbResults.Count, standardResult.Count);
        for (int idx = 0; idx < duckDbResults.Count; idx++)
        {
            var d = duckDbResults[idx];
            var p = standardResult[idx];
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

        TpcdsAssert.Close(d.bucket1, p.bucket1);
        TpcdsAssert.Close(d.bucket2, p.bucket2);
        TpcdsAssert.Close(d.bucket3, p.bucket3);
        TpcdsAssert.Close(d.bucket4, p.bucket4);
        TpcdsAssert.Close(d.bucket5, p.bucket5);
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
                       g.Key.cd_gender,
                       g.Key.cd_marital_status,
                       g.Key.cd_education_status,
                       cnt1 = g.LongCount(),
                       g.Key.cd_purchase_estimate,
                       cnt2 = g.LongCount(),
                       g.Key.cd_credit_rating,
                       cnt3 = g.LongCount(),
                       g.Key.cd_dep_count,
                       cnt4 = g.LongCount(),
                       g.Key.cd_dep_employed_count,
                       cnt5 = g.LongCount(),
                       g.Key.cd_dep_college_count,
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
