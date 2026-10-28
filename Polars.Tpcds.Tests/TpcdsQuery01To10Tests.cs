using DuckDB.NET.Data;

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
        using var lfStoreReturns = TpcdsData.StoreReturns();
        using var lfDateDim = TpcdsData.DateDim();
        using var lfStore = TpcdsData.Store();
        using var lfCustomer = TpcdsData.Customer();

        var returnsQuery = lfStoreReturns.AsQueryable<StoreReturns>();
        var dateDimQuery = lfDateDim.AsQueryable<DateDim>();
        var storeQuery = lfStore.AsQueryable<Store>();
        var customerQuery = lfCustomer.AsQueryable<Customer>();

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
        using var lfWebSales = TpcdsData.WebSales();
        using var lfCatalogSales = TpcdsData.CatalogSales();
        using var lfDateDim = TpcdsData.DateDim();

        var webSalesQuery = lfWebSales.AsQueryable<WebSales>();
        var catalogSalesQuery = lfCatalogSales.AsQueryable<CatalogSales>();
        var dateDimQuery = lfDateDim.AsQueryable<DateDim>();

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
        q2.InspectPlan();
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
        using var lfDateDim = TpcdsData.DateDim();
        using var lfStoreSales = TpcdsData.StoreSales();
        using var lfItem = TpcdsData.Item();

        var dateDimQuery = lfDateDim.AsQueryable<DateDim>();
        var storeSalesQuery = lfStoreSales.AsQueryable<StoreSales>();
        var itemQuery = lfItem.AsQueryable<Item>();

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
        using var lfCustomer = TpcdsData.Customer();
        using var lfStoreSales = TpcdsData.StoreSales();
        using var lfCatalogSales = TpcdsData.CatalogSales();
        using var lfWebSales = TpcdsData.WebSales();
        using var lfDateDim = TpcdsData.DateDim();

        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var storeSalesQuery = lfStoreSales.AsQueryable<StoreSales>();
        var catalogSalesQuery = lfCatalogSales.AsQueryable<CatalogSales>();
        var webSalesQuery = lfWebSales.AsQueryable<WebSales>();
        var dateDimQuery = lfDateDim.AsQueryable<DateDim>();

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
}
