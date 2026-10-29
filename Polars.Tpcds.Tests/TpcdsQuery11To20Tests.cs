namespace Polars.Tpcds.Tests;

public class TpcdsQuery11To20Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 11
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
     *           sum(ss_ext_list_price-ss_ext_discount_amt) year_total,
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
     *                     sum(ws_ext_list_price-ws_ext_discount_amt) year_total,
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
     *      year_total t_w_firstyear,
     *      year_total t_w_secyear
     * WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
     *   AND t_s_firstyear.customer_id = t_w_secyear.customer_id
     *   AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
     *   AND t_s_firstyear.sale_type = 's'
     *   AND t_w_firstyear.sale_type = 'w'
     *   AND t_s_secyear.sale_type = 's'
     *   AND t_w_secyear.sale_type = 'w'
     *   AND t_s_firstyear.dyear = 2001
     *   AND t_s_secyear.dyear = 2001+1
     *   AND t_w_firstyear.dyear = 2001
     *   AND t_w_secyear.dyear = 2001+1
     *   AND t_s_firstyear.year_total > 0
     *   AND t_w_firstyear.year_total > 0
     *   AND CASE
     *           WHEN t_w_firstyear.year_total > 0 THEN (t_w_secyear.year_total*1.0000) / t_w_firstyear.year_total
     *           ELSE 0.0
     *       END > CASE
     *                 WHEN t_s_firstyear.year_total > 0 THEN (t_s_secyear.year_total*1.0000) / t_s_firstyear.year_total
     *                 ELSE 0.0
     *             END
     * ORDER BY t_s_secyear.customer_id NULLS FIRST,
     *          t_s_secyear.customer_first_name NULLS FIRST,
     *          t_s_secyear.customer_last_name NULLS FIRST,
     *          t_s_secyear.customer_preferred_cust_flag NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q11")]
    public void Test_Q11()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
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
                            year_total = g.Sum(x => x.ss.ss_ext_list_price - x.ss.ss_ext_discount_amt),
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
                          year_total = g.Sum(x => x.ws.ws_ext_list_price - x.ws.ws_ext_discount_amt),
                      };

        // 2. Filter year and positive sales
        var ts1 = storeYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var ts2 = storeYear.Where(x => x.dyear == 2002);
        var tw1 = webYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var tw2 = webYear.Where(x => x.dyear == 2002);

        // 3. Multi-channel customer self-join and relative growth filter
        var q11 = from s1 in ts1
                  join s2 in ts2 on s1.customer_id equals s2.customer_id
                  join w1 in tw1 on s1.customer_id equals w1.customer_id
                  join w2 in tw2 on s1.customer_id equals w2.customer_id
                  where (double)w2.year_total! / (double)w1.year_total! > (double)s2.year_total! / (double)s1.year_total!
                  orderby s2.customer_id, s2.customer_first_name, s2.customer_last_name, s2.customer_preferred_cust_flag
                  select new
                  {
                      s2.customer_id,
                      s2.customer_first_name,
                      s2.customer_last_name,
                      s2.customer_preferred_cust_flag
                  };

        var limitedQ11 = q11.Take(100);

        AssertPlan(limitedQ11, NativePushdown);

        var polarsResult = limitedQ11.ToList();

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
                      sum(ss_ext_list_price-ss_ext_discount_amt) year_total,
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
                                sum(ws_ext_list_price-ws_ext_discount_amt) year_total,
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
                 year_total t_w_firstyear,
                 year_total t_w_secyear
            WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
              AND t_s_firstyear.customer_id = t_w_secyear.customer_id
              AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
              AND t_s_firstyear.sale_type = 's'
              AND t_w_firstyear.sale_type = 'w'
              AND t_s_secyear.sale_type = 's'
              AND t_w_secyear.sale_type = 'w'
              AND t_s_firstyear.dyear = 2001
              AND t_s_secyear.dyear = 2001+1
              AND t_w_firstyear.dyear = 2001
              AND t_w_secyear.dyear = 2001+1
              AND t_s_firstyear.year_total > 0
              AND t_w_firstyear.year_total > 0
              AND CASE
                      WHEN t_w_firstyear.year_total > 0 THEN (t_w_secyear.year_total*1.0000) / t_w_firstyear.year_total
                      ELSE 0.0
                  END > CASE
                            WHEN t_s_firstyear.year_total > 0 THEN (t_s_secyear.year_total*1.0000) / t_s_firstyear.year_total
                            ELSE 0.0
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
            customer_first_name = r.IsDBNull(1) ? null : r.GetString(1),
            customer_last_name = r.IsDBNull(2) ? null : r.GetString(2),
            customer_preferred_cust_flag = r.IsDBNull(3) ? null : r.GetString(3),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.customer_id, p.customer_id);
            Assert.Equal(d.customer_first_name, (string?)p.customer_first_name);
            Assert.Equal(d.customer_last_name, (string?)p.customer_last_name);
            Assert.Equal(d.customer_preferred_cust_flag, (string?)p.customer_preferred_cust_flag);
        }
    }

    private static readonly string[] Q12TargetCategories = ["Sports", "Books", "Home"];

    /*
     * TPC-DS Query 12
     *
     * SELECT i_item_id,
     *        i_item_desc,
     *        i_category,
     *        i_class,
     *        i_current_price,
     *        sum(ws_ext_sales_price) AS itemrevenue,
     *        sum(ws_ext_sales_price)*100.0000/sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
     * FROM web_sales,
     *      item,
     *      date_dim
     * WHERE ws_item_sk = i_item_sk
     *   AND i_category IN ('Sports',
     *                      'Books',
     *                      'Home')
     *   AND ws_sold_date_sk = d_date_sk
     *   AND d_date BETWEEN cast('1999-02-22' AS date) AND cast('1999-03-24' AS date)
     * GROUP BY i_item_id,
     *          i_item_desc,
     *          i_category,
     *          i_class,
     *          i_current_price
     * ORDER BY i_category,
     *          i_class,
     *          i_item_id,
     *          i_item_desc,
     *          revenueratio
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q12")]
    public void Test_Q12()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var minDate = new DateOnly(1999, 2, 22);
        var maxDate = new DateOnly(1999, 3, 24);

        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredItems = itemQuery.Where(i => Q12TargetCategories.Contains(i.i_category));

        var baseGroup = from ws in webSalesQuery
                        join d in filteredDates on ws.ws_sold_date_sk equals d.d_date_sk
                        join i in filteredItems on ws.ws_item_sk equals i.i_item_sk
                        group ws by new
                        {
                            i.i_item_id,
                            i.i_item_desc,
                            i.i_category,
                            i.i_class,
                            i.i_current_price
                        } into g
                        select new
                        {
                            g.Key.i_item_id,
                            g.Key.i_item_desc,
                            g.Key.i_category,
                            g.Key.i_class,
                            g.Key.i_current_price,
                            itemrevenue = g.Sum(x => x.ws_ext_sales_price),
                        };

        var withRatio = from r in baseGroup
                        select new
                        {
                            r.i_item_id,
                            r.i_item_desc,
                            r.i_category,
                            r.i_class,
                            r.i_current_price,
                            r.itemrevenue,
                            revenueratio = (double?)(r.itemrevenue * 100.0m) / (double?)r.itemrevenue.Sum().Over(r.i_class)
                        };

        var q12 = from r in withRatio
                  orderby r.i_category, r.i_class, r.i_item_id, r.i_item_desc, r.revenueratio
                  select r;

        var limitedQ12 = q12.Take(100);

        AssertPlan(limitedQ12, NativePushdown);

        var polarsResult = limitedQ12.ToList();
        Assert.NotEmpty(polarsResult);

        // Ground-truth cross validation with DuckDB
        var duckDbSql = $@"
            SELECT i_item_id,
                   i_item_desc,
                   i_category,
                   i_class,
                   i_current_price,
                   sum(ws_ext_sales_price) AS itemrevenue,
                   sum(ws_ext_sales_price)*100.0000/sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
            FROM read_parquet('{TpcdsData.WebSalesPath}'),
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.DateDimPath}')
            WHERE ws_item_sk = i_item_sk
              AND i_category IN ('Sports',
                                 'Books',
                                 'Home')
              AND ws_sold_date_sk = d_date_sk
              AND d_date BETWEEN cast('1999-02-22' AS date) AND cast('1999-03-24' AS date)
            GROUP BY i_item_id,
                     i_item_desc,
                     i_category,
                     i_class,
                     i_current_price
            ORDER BY i_category,
                     i_class,
                     i_item_id,
                     i_item_desc,
                     revenueratio
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            i_category = r.IsDBNull(2) ? null : r.GetString(2),
            i_class = r.IsDBNull(3) ? null : r.GetString(3),
            i_current_price = r.IsDBNull(4) ? null : (decimal?)r.GetDecimal(4),
            itemrevenue = r.IsDBNull(5) ? null : (decimal?)r.GetDecimal(5),
            revenueratio = r.IsDBNull(6) ? null : (double?)r.GetDouble(6),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.i_item_desc, (string?)p.i_item_desc);
            Assert.Equal(d.i_category, (string?)p.i_category);
            Assert.Equal(d.i_class, (string?)p.i_class);
            Assert.Equal(d.i_current_price, p.i_current_price);
            Assert.Equal(d.itemrevenue, p.itemrevenue);
            Assert.Equal(d.revenueratio!.Value, p.revenueratio!.Value, 4);
        }
    }

    /*
     * TPC-DS Query 13
     *
     * SELECT avg(ss_quantity) avg1,
     *        avg(ss_ext_sales_price) avg2,
     *        avg(ss_ext_wholesale_cost) avg3,
     *        sum(ss_ext_wholesale_cost)
     * FROM store_sales ,
     *      store ,
     *      customer_demographics ,
     *      household_demographics ,
     *      customer_address ,
     *      date_dim
     * WHERE s_store_sk = ss_store_sk
     *   AND ss_sold_date_sk = d_date_sk
     *   AND d_year = 2001
     *   AND (
     *         (ss_hdemo_sk = hd_demo_sk
     *          AND cd_demo_sk = ss_cdemo_sk
     *          AND cd_marital_status = 'M'
     *          AND cd_education_status = 'Advanced Degree'
     *          AND ss_sales_price BETWEEN 100.00 AND 150.00
     *          AND hd_dep_count = 3)
     *      OR (ss_hdemo_sk = hd_demo_sk
     *          AND cd_demo_sk = ss_cdemo_sk
     *          AND cd_marital_status = 'S'
     *          AND cd_education_status = 'College'
     *          AND ss_sales_price BETWEEN 50.00 AND 100.00
     *          AND hd_dep_count = 1)
     *      OR (ss_hdemo_sk = hd_demo_sk
     *          AND cd_demo_sk = ss_cdemo_sk
     *          AND cd_marital_status = 'W'
     *          AND cd_education_status = '2 yr Degree'
     *          AND ss_sales_price BETWEEN 150.00 AND 200.00
     *          AND hd_dep_count = 1)
     *   )
     *   AND (
     *         (ss_addr_sk = ca_address_sk
     *          AND ca_country = 'United States'
     *          AND ca_state IN ('TX', 'OH', 'TX')
     *          AND ss_net_profit BETWEEN 100 AND 200)
     *      OR (ss_addr_sk = ca_address_sk
     *          AND ca_country = 'United States'
     *          AND ca_state IN ('OR', 'NM', 'KY')
     *          AND ss_net_profit BETWEEN 150 AND 300)
     *      OR (ss_addr_sk = ca_address_sk
     *          AND ca_country = 'United States'
     *          AND ca_state IN ('VA', 'TX', 'MS')
     *          AND ss_net_profit BETWEEN 50 AND 250)
     *   );
     */
    [Fact]
    [Trait("TPCDS", "Q13")]
    public void Test_Q13()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var householdDemographicsQuery = TpcdsData.HouseholdDemographicsQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var q13 = from ss in storeSalesQuery
                  join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                  join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                  join cd in customerDemographicsQuery on ss.ss_cdemo_sk equals cd.cd_demo_sk
                  join hd in householdDemographicsQuery on ss.ss_hdemo_sk equals hd.hd_demo_sk
                  join ca in customerAddressQuery on ss.ss_addr_sk equals ca.ca_address_sk
                  where d.d_year == 2001
                     && ca.ca_country == "United States"
                     && (
                            (cd.cd_marital_status == "M" && cd.cd_education_status == "Advanced Degree" && ss.ss_sales_price >= 100.00m && ss.ss_sales_price <= 150.00m && hd.hd_dep_count == 3)
                         || (cd.cd_marital_status == "S" && cd.cd_education_status == "College" && ss.ss_sales_price >= 50.00m && ss.ss_sales_price <= 100.00m && hd.hd_dep_count == 1)
                         || (cd.cd_marital_status == "W" && cd.cd_education_status == "2 yr Degree" && ss.ss_sales_price >= 150.00m && ss.ss_sales_price <= 200.00m && hd.hd_dep_count == 1)
                     )
                     && (
                            ((ca.ca_state == "TX" || ca.ca_state == "OH") && ss.ss_net_profit >= 100m && ss.ss_net_profit <= 200m)
                         || ((ca.ca_state == "OR" || ca.ca_state == "NM" || ca.ca_state == "KY") && ss.ss_net_profit >= 150m && ss.ss_net_profit <= 300m)
                         || ((ca.ca_state == "VA" || ca.ca_state == "TX" || ca.ca_state == "MS") && ss.ss_net_profit >= 50m && ss.ss_net_profit <= 250m)
                     )
                  group ss by 1 into g
                  select new
                  {
                      avg1 = g.Average(x => (double?)x.ss_quantity),
                      avg2 = g.Average(x => (double?)x.ss_ext_sales_price),
                      avg3 = g.Average(x => (double?)x.ss_ext_wholesale_cost),
                      sum4 = g.Sum(x => x.ss_ext_wholesale_cost)
                  };

        AssertPlan(q13, NativePushdown);

        var polarsResult = q13.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT avg(ss_quantity) avg1,
                   avg(ss_ext_sales_price) avg2,
                   avg(ss_ext_wholesale_cost) avg3,
                   sum(ss_ext_wholesale_cost)
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.StorePath}'),
                 read_parquet('{TpcdsData.CustomerDemographicsPath}'),
                 read_parquet('{TpcdsData.HouseholdDemographicsPath}'),
                 read_parquet('{TpcdsData.CustomerAddressPath}'),
                 read_parquet('{TpcdsData.DateDimPath}')
            WHERE s_store_sk = ss_store_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_year = 2001 and((ss_hdemo_sk=hd_demo_sk
                                     AND cd_demo_sk = ss_cdemo_sk
                                     AND cd_marital_status = 'M'
                                     AND cd_education_status = 'Advanced Degree'
                                     AND ss_sales_price BETWEEN 100.00 AND 150.00
                                     AND hd_dep_count = 3)
                                    OR (ss_hdemo_sk=hd_demo_sk
                                        AND cd_demo_sk = ss_cdemo_sk
                                        AND cd_marital_status = 'S'
                                        AND cd_education_status = 'College'
                                        AND ss_sales_price BETWEEN 50.00 AND 100.00
                                        AND hd_dep_count = 1 )
                                    OR (ss_hdemo_sk=hd_demo_sk
                                        AND cd_demo_sk = ss_cdemo_sk
                                        AND cd_marital_status = 'W'
                                        AND cd_education_status = '2 yr Degree'
                                        AND ss_sales_price BETWEEN 150.00 AND 200.00
                                        AND hd_dep_count = 1)) and((ss_addr_sk = ca_address_sk
                                                                    AND ca_country = 'United States'
                                                                    AND ca_state IN ('TX', 'OH', 'TX')
                                                                    AND ss_net_profit BETWEEN 100 AND 200)
                                                                   OR (ss_addr_sk = ca_address_sk
                                                                       AND ca_country = 'United States'
                                                                       AND ca_state IN ('OR', 'NM', 'KY')
                                                                       AND ss_net_profit BETWEEN 150 AND 300)
                                                                   OR (ss_addr_sk = ca_address_sk
                                                                       AND ca_country = 'United States'
                                                                       AND ca_state IN ('VA', 'TX', 'MS')
                                                                       AND ss_net_profit BETWEEN 50 AND 250)) ;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            avg1 = r.IsDBNull(0) ? null : (double?)r.GetDouble(0),
            avg2 = r.IsDBNull(1) ? null : (double?)r.GetDouble(1),
            avg3 = r.IsDBNull(2) ? null : (double?)r.GetDouble(2),
            sum4 = r.IsDBNull(3) ? null : (decimal?)r.GetDecimal(3),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.avg1, p.avg1);
            Assert.Equal(d.avg2, p.avg2);
            Assert.Equal(d.avg3, p.avg3);
            if (d.sum4 == null && p.sum4 == 0m)
            {
                // In Polars, sum on an empty group produces 0, whereas SQL produces NULL
            }
            else
            {
                Assert.Equal(d.sum4, p.sum4);
            }
        }
    }

    /*
     * TPC-DS Query 14
     *
     * WITH cross_items AS
     *   (SELECT i_item_sk ss_item_sk
     *    FROM item,
     *      (SELECT iss.i_brand_id brand_id,
     *              iss.i_class_id class_id,
     *              iss.i_category_id category_id
     *       FROM store_sales,
     *            item iss,
     *            date_dim d1
     *       WHERE ss_item_sk = iss.i_item_sk
     *         AND ss_sold_date_sk = d1.d_date_sk
     *         AND d1.d_year BETWEEN 1999 AND 1999 + 2 INTERSECT
     *         SELECT ics.i_brand_id,
     *                ics.i_class_id,
     *                ics.i_category_id
     *         FROM catalog_sales,
     *              item ics,
     *              date_dim d2 WHERE cs_item_sk = ics.i_item_sk
     *         AND cs_sold_date_sk = d2.d_date_sk
     *         AND d2.d_year BETWEEN 1999 AND 1999 + 2 INTERSECT
     *         SELECT iws.i_brand_id,
     *                iws.i_class_id,
     *                iws.i_category_id
     *         FROM web_sales,
     *              item iws,
     *              date_dim d3 WHERE ws_item_sk = iws.i_item_sk
     *         AND ws_sold_date_sk = d3.d_date_sk
     *         AND d3.d_year BETWEEN 1999 AND 1999 + 2) sq1
     *    WHERE i_brand_id = brand_id
     *      AND i_class_id = class_id
     *      AND i_category_id = category_id ),
     *      avg_sales AS
     *   (SELECT avg(quantity*list_price) average_sales
     *    FROM
     *      (SELECT ss_quantity quantity,
     *              ss_list_price list_price
     *       FROM store_sales,
     *            date_dim
     *       WHERE ss_sold_date_sk = d_date_sk
     *         AND d_year BETWEEN 1999 AND 1999 + 2
     *       UNION ALL SELECT cs_quantity quantity,
     *                        cs_list_price list_price
     *       FROM catalog_sales,
     *            date_dim
     *       WHERE cs_sold_date_sk = d_date_sk
     *         AND d_year BETWEEN 1999 AND 1999 + 2
     *       UNION ALL SELECT ws_quantity quantity,
     *                        ws_list_price list_price
     *       FROM web_sales,
     *            date_dim
     *       WHERE ws_sold_date_sk = d_date_sk
     *         AND d_year BETWEEN 1999 AND 1999 + 2) sq2)
     * SELECT channel,
     *        i_brand_id,
     *        i_class_id,
     *        i_category_id,
     *        sum(sales) AS sum_sales,
     *        sum(number_sales) AS sum_number_sales
     * FROM
     *   (SELECT 'store' channel,
     *                   i_brand_id,
     *                   i_class_id,
     *                   i_category_id,
     *                   sum(ss_quantity*ss_list_price) sales,
     *                   count(*) number_sales
     *    FROM store_sales,
     *         item,
     *         date_dim
     *    WHERE ss_item_sk IN
     *        (SELECT ss_item_sk
     *         FROM cross_items)
     *      AND ss_item_sk = i_item_sk
     *      AND ss_sold_date_sk = d_date_sk
     *      AND d_year = 1999+2
     *      AND d_moy = 11
     *    GROUP BY i_brand_id,
     *             i_class_id,
     *             i_category_id
     *    HAVING sum(ss_quantity*ss_list_price) >
     *      (SELECT average_sales
     *       FROM avg_sales)
     *    UNION ALL SELECT 'catalog' channel,
     *                               i_brand_id,
     *                               i_class_id,
     *                               i_category_id,
     *                               sum(cs_quantity*cs_list_price) sales,
     *                               count(*) number_sales
     *    FROM catalog_sales,
     *         item,
     *         date_dim
     *    WHERE cs_item_sk IN
     *        (SELECT ss_item_sk
     *         FROM cross_items)
     *      AND cs_item_sk = i_item_sk
     *      AND cs_sold_date_sk = d_date_sk
     *      AND d_year = 1999+2
     *      AND d_moy = 11
     *    GROUP BY i_brand_id,
     *             i_class_id,
     *             i_category_id
     *    HAVING sum(cs_quantity*cs_list_price) >
     *      (SELECT average_sales
     *       FROM avg_sales)
     *    UNION ALL SELECT 'web' channel,
     *                           i_brand_id,
     *                           i_class_id,
     *                           i_category_id,
     *                           sum(ws_quantity*ws_list_price) sales,
     *                           count(*) number_sales
     *    FROM web_sales,
     *         item,
     *         date_dim
     *    WHERE ws_item_sk IN
     *        (SELECT ss_item_sk
     *         FROM cross_items)
     *      AND ws_item_sk = i_item_sk
     *      AND ws_sold_date_sk = d_date_sk
     *      AND d_year = 1999+2
     *      AND d_moy = 11
     *    GROUP BY i_brand_id,
     *             i_class_id,
     *             i_category_id
     *    HAVING sum(ws_quantity*ws_list_price) >
     *      (SELECT average_sales
     *       FROM avg_sales)) y
     * GROUP BY ROLLUP (channel,
     *                  i_brand_id,
     *                  i_class_id,
     *                  i_category_id)
     * ORDER BY channel NULLS FIRST,
     *          i_brand_id NULLS FIRST,
     *          i_class_id NULLS FIRST,
     *          i_category_id NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q14")]
    public void Test_Q14()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var dates3Years = dateDimQuery.Where(d => d.d_year >= 1999 && d.d_year <= 2001);

        // 1. cross_items CTE: items whose (brand, class, category) exist in all 3 sales channels
        var storeBCC = (from ss in storeSalesQuery
                        join iss in itemQuery on ss.ss_item_sk equals iss.i_item_sk
                        join d1 in dates3Years on ss.ss_sold_date_sk equals d1.d_date_sk
                        select new
                        {
                            brand_id = iss.i_brand_id,
                            class_id = iss.i_class_id,
                            category_id = iss.i_category_id
                        }).Distinct();

        var catalogBCC = (from cs in catalogSalesQuery
                          join ics in itemQuery on cs.cs_item_sk equals ics.i_item_sk
                          join d2 in dates3Years on cs.cs_sold_date_sk equals d2.d_date_sk
                          select new
                          {
                              brand_id = ics.i_brand_id,
                              class_id = ics.i_class_id,
                              category_id = ics.i_category_id
                          }).Distinct();

        var webBCC = (from ws in webSalesQuery
                      join iws in itemQuery on ws.ws_item_sk equals iws.i_item_sk
                      join d3 in dates3Years on ws.ws_sold_date_sk equals d3.d_date_sk
                      select new
                      {
                          brand_id = iws.i_brand_id,
                          class_id = iws.i_class_id,
                          category_id = iws.i_category_id
                      }).Distinct();

        var crossBCC = storeBCC.Intersect(catalogBCC).Intersect(webBCC);

        var crossItems = from i in itemQuery
                         join sq in crossBCC on new { brand_id = i.i_brand_id, class_id = i.i_class_id, category_id = i.i_category_id }
                                         equals new { sq.brand_id, sq.class_id, sq.category_id }
                         select new
                         {
                             i.i_item_sk,
                             i.i_brand_id,
                             i.i_class_id,
                             i.i_category_id
                         };

        // 2. avg_sales CTE: average sales across all 3 channels in 1999-2001
        var ssSales = from ss in storeSalesQuery
                      join d in dates3Years on ss.ss_sold_date_sk equals d.d_date_sk
                      select new { sales = (double?)(ss.ss_quantity * ss.ss_list_price) };

        var csSales = from cs in catalogSalesQuery
                      join d in dates3Years on cs.cs_sold_date_sk equals d.d_date_sk
                      select new { sales = (double?)(cs.cs_quantity * cs.cs_list_price) };

        var wsSales = from ws in webSalesQuery
                      join d in dates3Years on ws.ws_sold_date_sk equals d.d_date_sk
                      select new { sales = (double?)(ws.ws_quantity * ws.ws_list_price) };

        var avgSales = ssSales.Concat(csSales).Concat(wsSales).Average(x => x.sales);

        // 3. Channel streams in year 2001, month 11 with having filter > avgSales
        var targetDates = dateDimQuery.Where(d => d.d_year == 2001 && d.d_moy == 11);

        var storeStream = from ss in storeSalesQuery
                          join i in crossItems on ss.ss_item_sk equals i.i_item_sk
                          join d in targetDates on ss.ss_sold_date_sk equals d.d_date_sk
                          group ss by new
                          {
                              i.i_brand_id,
                              i.i_class_id,
                              i.i_category_id
                          } into g
                          where g.Sum(x => x.ss_quantity * x.ss_list_price) > (decimal?)avgSales
                          select new
                          {
                              channel = "store",
                              g.Key.i_brand_id,
                              g.Key.i_class_id,
                              g.Key.i_category_id,
                              sales = g.Sum(x => x.ss_quantity * x.ss_list_price),
                              number_sales = g.LongCount()
                          };

        var catalogStream = from cs in catalogSalesQuery
                            join i in crossItems on cs.cs_item_sk equals i.i_item_sk
                            join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                            group cs by new
                            {
                                i.i_brand_id,
                                i.i_class_id,
                                i.i_category_id
                            } into g
                            where g.Sum(x => x.cs_quantity * x.cs_list_price) > (decimal?)avgSales
                            select new
                            {
                                channel = "catalog",
                                g.Key.i_brand_id,
                                g.Key.i_class_id,
                                g.Key.i_category_id,
                                sales = g.Sum(x => x.cs_quantity * x.cs_list_price),
                                number_sales = g.LongCount()
                            };

        var webStream = from ws in webSalesQuery
                        join i in crossItems on ws.ws_item_sk equals i.i_item_sk
                        join d in targetDates on ws.ws_sold_date_sk equals d.d_date_sk
                        group ws by new
                        {
                            i.i_brand_id,
                            i.i_class_id,
                            i.i_category_id
                        } into g
                        where g.Sum(x => x.ws_quantity * x.ws_list_price) > (decimal?)avgSales
                        select new
                        {
                            channel = "web",
                            g.Key.i_brand_id,
                            g.Key.i_class_id,
                            g.Key.i_category_id,
                            sales = g.Sum(x => x.ws_quantity * x.ws_list_price),
                            number_sales = g.LongCount()
                        };

        var y = storeStream.Concat(catalogStream).Concat(webStream);

        // 4. 5-Level ROLLUP (channel, i_brand_id, i_class_id, i_category_id) via fluent GroupByRollup
        var q14 = y
            .GroupByRollup(
                r => new { r.channel, r.i_brand_id, r.i_class_id, r.i_category_id },
                (key, g) => new
                {
                    channel = (string?)key.channel,
                    i_brand_id = (long?)key.i_brand_id,
                    i_class_id = (long?)key.i_class_id,
                    i_category_id = (long?)key.i_category_id,
                    sum_sales = g.Sum(x => x.sales),
                    sum_number_sales = g.Sum(x => x.number_sales)
                }
            )
            .OrderBy(r => r.channel)
            .ThenBy(r => r.i_brand_id)
            .ThenBy(r => r.i_class_id)
            .ThenBy(r => r.i_category_id)
            .Take(100);

        AssertPlan(q14, NativePushdown);

        var polarsResult = q14.ToList();
        Assert.NotEmpty(polarsResult);

        // Ground-truth cross validation with DuckDB
        var duckDbSql = $@"
            WITH cross_items AS
              (SELECT i_item_sk ss_item_sk
               FROM read_parquet('{TpcdsData.ItemPath}'),
                 (SELECT iss.i_brand_id brand_id,
                         iss.i_class_id class_id,
                         iss.i_category_id category_id
                  FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                       read_parquet('{TpcdsData.ItemPath}') iss,
                       read_parquet('{TpcdsData.DateDimPath}') d1
                  WHERE ss_item_sk = iss.i_item_sk
                    AND ss_sold_date_sk = d1.d_date_sk
                    AND d1.d_year BETWEEN 1999 AND 1999 + 2 INTERSECT
                    SELECT ics.i_brand_id,
                           ics.i_class_id,
                           ics.i_category_id
                    FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                         read_parquet('{TpcdsData.ItemPath}') ics,
                         read_parquet('{TpcdsData.DateDimPath}') d2 WHERE cs_item_sk = ics.i_item_sk
                    AND cs_sold_date_sk = d2.d_date_sk
                    AND d2.d_year BETWEEN 1999 AND 1999 + 2 INTERSECT
                    SELECT iws.i_brand_id,
                           iws.i_class_id,
                           iws.i_category_id
                    FROM read_parquet('{TpcdsData.WebSalesPath}'),
                         read_parquet('{TpcdsData.ItemPath}') iws,
                         read_parquet('{TpcdsData.DateDimPath}') d3 WHERE ws_item_sk = iws.i_item_sk
                    AND ws_sold_date_sk = d3.d_date_sk
                    AND d3.d_year BETWEEN 1999 AND 1999 + 2) sq1
               WHERE i_brand_id = brand_id
                 AND i_class_id = class_id
                 AND i_category_id = category_id ),
                 avg_sales AS
              (SELECT avg(quantity*list_price) average_sales
               FROM
                 (SELECT ss_quantity quantity,
                         ss_list_price list_price
                  FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                       read_parquet('{TpcdsData.DateDimPath}')
                  WHERE ss_sold_date_sk = d_date_sk
                    AND d_year BETWEEN 1999 AND 1999 + 2
                  UNION ALL SELECT cs_quantity quantity,
                                   cs_list_price list_price
                  FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                       read_parquet('{TpcdsData.DateDimPath}')
                  WHERE cs_sold_date_sk = d_date_sk
                    AND d_year BETWEEN 1999 AND 1999 + 2
                  UNION ALL SELECT ws_quantity quantity,
                                   ws_list_price list_price
                  FROM read_parquet('{TpcdsData.WebSalesPath}'),
                       read_parquet('{TpcdsData.DateDimPath}')
                  WHERE ws_sold_date_sk = d_date_sk
                    AND d_year BETWEEN 1999 AND 1999 + 2) sq2)
            SELECT channel,
                   i_brand_id,
                   i_class_id,
                   i_category_id,
                   sum(sales) AS sum_sales,
                   sum(number_sales) AS sum_number_sales
            FROM
              (SELECT 'store' channel,
                              i_brand_id,
                              i_class_id,
                              i_category_id,
                              sum(ss_quantity*ss_list_price) sales,
                              count(*) number_sales
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE ss_item_sk IN
                   (SELECT ss_item_sk
                    FROM cross_items)
                 AND ss_item_sk = i_item_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND d_year = 1999+2
                 AND d_moy = 11
               GROUP BY i_brand_id,
                        i_class_id,
                        i_category_id
               HAVING sum(ss_quantity*ss_list_price) >
                 (SELECT average_sales
                  FROM avg_sales)
               UNION ALL SELECT 'catalog' channel,
                                          i_brand_id,
                                          i_class_id,
                                          i_category_id,
                                          sum(cs_quantity*cs_list_price) sales,
                                          count(*) number_sales
               FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE cs_item_sk IN
                   (SELECT ss_item_sk
                    FROM cross_items)
                 AND cs_item_sk = i_item_sk
                 AND cs_sold_date_sk = d_date_sk
                 AND d_year = 1999+2
                 AND d_moy = 11
               GROUP BY i_brand_id,
                        i_class_id,
                        i_category_id
               HAVING sum(cs_quantity*cs_list_price) >
                 (SELECT average_sales
                  FROM avg_sales)
               UNION ALL SELECT 'web' channel,
                                      i_brand_id,
                                      i_class_id,
                                      i_category_id,
                                      sum(ws_quantity*ws_list_price) sales,
                                      count(*) number_sales
               FROM read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE ws_item_sk IN
                   (SELECT ss_item_sk
                    FROM cross_items)
                 AND ws_item_sk = i_item_sk
                 AND ws_sold_date_sk = d_date_sk
                 AND d_year = 1999+2
                 AND d_moy = 11
               GROUP BY i_brand_id,
                        i_class_id,
                        i_category_id
               HAVING sum(ws_quantity*ws_list_price) >
                 (SELECT average_sales
                  FROM avg_sales)) y
            GROUP BY ROLLUP (channel,
                             i_brand_id,
                             i_class_id,
                             i_category_id)
            ORDER BY channel NULLS FIRST,
                     i_brand_id NULLS FIRST,
                     i_class_id NULLS FIRST,
                     i_category_id NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            channel = r.IsDBNull(0) ? null : r.GetString(0),
            i_brand_id = r.IsDBNull(1) ? null : (long?)r.GetInt64(1),
            i_class_id = r.IsDBNull(2) ? null : (long?)r.GetInt64(2),
            i_category_id = r.IsDBNull(3) ? null : (long?)r.GetInt64(3),
            sum_sales = r.IsDBNull(4) ? null : (decimal?)r.GetDecimal(4),
            sum_number_sales = r.IsDBNull(5) ? null : (long?)r.GetInt64(5),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.channel, (string?)p.channel);
            Assert.Equal(d.i_brand_id, (long?)p.i_brand_id);
            Assert.Equal(d.i_class_id, (long?)p.i_class_id);
            Assert.Equal(d.i_category_id, (long?)p.i_category_id);
            Assert.Equal(d.sum_sales, p.sum_sales);
            Assert.Equal(d.sum_number_sales, p.sum_number_sales);
        }
    }

    /**
     * TPC-DS Query 15
     *
     * select  ca_zip
     *        ,sum(cs_sales_price)
     * from catalog_sales
     *      ,customer
     *      ,customer_address
     *      ,date_dim
     * where cs_bill_customer_sk = c_customer_sk
     *   and c_current_addr_sk = ca_address_sk
     *   and ( substr(ca_zip,1,5) in ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
     *         or ca_state in ('CA','WA','GA')
     *         or cs_sales_price > 500)
     *   and cs_sold_date_sk = d_date_sk
     *   and d_qoy = 2
     *   and d_year = 2002
     * group by ca_zip
     * order by ca_zip
     * limit 100;
     */
    [Fact]
    [Trait("TPCDS", "Q15")]
    public void Test_Q15()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        string[] targetZips = ["85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792"];
        string[] targetStates = ["CA", "WA", "GA"];

        var filteredDates = dateDimQuery.Where(d => d.d_qoy == 2 && d.d_year == 2002);

        var q15 = (from cs in catalogSalesQuery
                   join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                   join c in customerQuery on cs.cs_bill_customer_sk equals c.c_customer_sk
                   join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                   where targetZips.Contains(ca.ca_zip!.Substring(0, 5)) ||
                         targetStates.Contains(ca.ca_state!) ||
                         cs.cs_sales_price > 500m
                   group cs by ca.ca_zip into g
                   orderby g.Key
                   select new
                   {
                       ca_zip = g.Key,
                       sum_sales_price = g.Sum(x => x.cs_sales_price)
                   }).Take(100);

        AssertPlan(q15, NativePushdown);

        var polarsResult = q15.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT ca_zip,
                   sum(cs_sales_price)
            FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs,
                 read_parquet('{TpcdsData.CustomerPath}') c,
                 read_parquet('{TpcdsData.CustomerAddressPath}') ca,
                 read_parquet('{TpcdsData.DateDimPath}') d
            WHERE cs.cs_bill_customer_sk = c.c_customer_sk
              AND c.c_current_addr_sk = ca.ca_address_sk
              AND ( substr(ca_zip,1,5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
                    OR ca_state IN ('CA','WA','GA')
                    OR cs_sales_price > 500)
              AND cs.cs_sold_date_sk = d.d_date_sk
              AND d.d_qoy = 2
              AND d.d_year = 2002
            GROUP BY ca_zip
            ORDER BY ca_zip
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            ca_zip = r.IsDBNull(0) ? null : r.GetString(0),
            sum_sales_price = r.IsDBNull(1) ? null : (decimal?)r.GetDecimal(1)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.ca_zip, p.ca_zip);
            Assert.Equal(d.sum_sales_price, p.sum_sales_price);
        }
    }

    /**
     * TPC-DS Query 16
     *
     * SELECT count(DISTINCT cs_order_number) AS "order count",
     *        sum(cs_ext_ship_cost) AS "total shipping cost",
     *        sum(cs_net_profit) AS "total net profit"
     * FROM catalog_sales cs1,
     *      date_dim,
     *      customer_address,
     *      call_center
     * WHERE d_date BETWEEN '2002-02-01' AND cast('2002-04-02' AS date)
     *   AND cs1.cs_ship_date_sk = d_date_sk
     *   AND cs1.cs_ship_addr_sk = ca_address_sk
     *   AND ca_state = 'GA'
     *   AND cs1.cs_call_center_sk = cc_call_center_sk
     *   AND cc_county = 'Williamson County'
     *   AND EXISTS
     *     (SELECT *
     *      FROM catalog_sales cs2
     *      WHERE cs1.cs_order_number = cs2.cs_order_number
     *        AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
     *   AND NOT EXISTS
     *     (SELECT *
     *      FROM catalog_returns cr1
     *      WHERE cs1.cs_order_number = cr1.cr_order_number)
     * ORDER BY count(DISTINCT cs_order_number)
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q16")]
    public void Test_Q16()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var callCenterQuery = TpcdsData.CallCenterQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();

        var minDate = new DateOnly(2002, 2, 1);
        var maxDate = new DateOnly(2002, 4, 2);

        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredAddr = customerAddressQuery.Where(ca => ca.ca_state == "GA");
        var filteredCallCenter = callCenterQuery.Where(cc => cc.cc_county == "Williamson County");

        var multiWarehouseOrders = (from cs1 in catalogSalesQuery
                                    join cs2 in catalogSalesQuery on cs1.cs_order_number equals cs2.cs_order_number
                                    where cs1.cs_warehouse_sk != cs2.cs_warehouse_sk
                                    select cs1.cs_order_number).Distinct();

        var returnedOrders = catalogReturnsQuery.Select(cr => cr.cr_order_number);

        var baseSales = from cs in catalogSalesQuery
                        join d in filteredDates on cs.cs_ship_date_sk equals d.d_date_sk
                        join ca in filteredAddr on cs.cs_ship_addr_sk equals ca.ca_address_sk
                        join cc in filteredCallCenter on cs.cs_call_center_sk equals cc.cc_call_center_sk
                        select cs;

        var eligibleSales = (from cs in baseSales
                             join o in multiWarehouseOrders on cs.cs_order_number equals o
                             select cs).ExceptBy(returnedOrders, cs => cs.cs_order_number);

        var q16 = from cs in eligibleSales
                  group cs by 1 into g
                  select new
                  {
                      order_count = g.Select(x => x.cs_order_number).Distinct().LongCount(),
                      total_shipping_cost = g.Sum(x => x.cs_ext_ship_cost),
                      total_net_profit = g.Sum(x => x.cs_net_profit)
                  };

        AssertPlan(baseSales, NativePushdown);
        AssertPlan(multiWarehouseOrders, NativePushdown);
        AssertPlan(eligibleSales, NativePushdown);
        AssertPlan(q16, NativePushdown);

        var polarsResult = q16.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT count(DISTINCT cs1.cs_order_number) AS ""order count"",
                   sum(cs1.cs_ext_ship_cost) AS ""total shipping cost"",
                   sum(cs1.cs_net_profit) AS ""total net profit""
            FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs1,
                 read_parquet('{TpcdsData.DateDimPath}') d,
                 read_parquet('{TpcdsData.CustomerAddressPath}') ca,
                 read_parquet('{TpcdsData.CallCenterPath}') cc
            WHERE d.d_date BETWEEN '2002-02-01' AND cast('2002-04-02' AS date)
              AND cs1.cs_ship_date_sk = d.d_date_sk
              AND cs1.cs_ship_addr_sk = ca.ca_address_sk
              AND ca.ca_state = 'GA'
              AND cs1.cs_call_center_sk = cc.cc_call_center_sk
              AND cc.cc_county = 'Williamson County'
              AND EXISTS
                (SELECT *
                 FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs2
                 WHERE cs1.cs_order_number = cs2.cs_order_number
                   AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
              AND NOT EXISTS
                (SELECT *
                 FROM read_parquet('{TpcdsData.CatalogReturnsPath}') cr1
                 WHERE cs1.cs_order_number = cr1.cr_order_number)
            ORDER BY count(DISTINCT cs1.cs_order_number)
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            order_count = r.IsDBNull(0) ? 0 : r.GetInt64(0),
            total_shipping_cost = r.IsDBNull(1) ? null : (decimal?)r.GetDecimal(1),
            total_net_profit = r.IsDBNull(2) ? null : (decimal?)r.GetDecimal(2)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.order_count, p.order_count);

            if (d.total_shipping_cost == null && p.total_shipping_cost == 0m)
            {
                // In Polars, sum on an empty group produces 0, whereas SQL produces NULL
            }
            else
            {
                Assert.Equal(d.total_shipping_cost, p.total_shipping_cost);
            }

            if (d.total_net_profit == null && p.total_net_profit == 0m)
            {
                // In Polars, sum on an empty group produces 0, whereas SQL produces NULL
            }
            else
            {
                Assert.Equal(d.total_net_profit, p.total_net_profit);
            }
        }
    }

    /**
     * TPC-DS Query 17
     *
     * SELECT i_item_id,
     *        i_item_desc,
     *        s_state,
     *        count(ss_quantity) AS store_sales_quantitycount,
     *        avg(ss_quantity) AS store_sales_quantityave,
     *        stddev_samp(ss_quantity) AS store_sales_quantitystdev,
     *        stddev_samp(ss_quantity)/avg(ss_quantity) AS store_sales_quantitycov,
     *        count(sr_return_quantity) AS store_returns_quantitycount,
     *        avg(sr_return_quantity) AS store_returns_quantityave,
     *        stddev_samp(sr_return_quantity) AS store_returns_quantitystdev,
     *        stddev_samp(sr_return_quantity)/avg(sr_return_quantity) AS store_returns_quantitycov,
     *        count(cs_quantity) AS catalog_sales_quantitycount,
     *        avg(cs_quantity) AS catalog_sales_quantityave,
     *        stddev_samp(cs_quantity) AS catalog_sales_quantitystdev,
     *        stddev_samp(cs_quantity)/avg(cs_quantity) AS catalog_sales_quantitycov
     * FROM store_sales,
     *      store_returns,
     *      catalog_sales,
     *      date_dim d1,
     *      date_dim d2,
     *      date_dim d3,
     *      store,
     *      item
     * WHERE d1.d_quarter_name = '2001Q1'
     *   AND d1.d_date_sk = ss_sold_date_sk
     *   AND i_item_sk = ss_item_sk
     *   AND s_store_sk = ss_store_sk
     *   AND ss_customer_sk = sr_customer_sk
     *   AND ss_item_sk = sr_item_sk
     *   AND ss_ticket_number = sr_ticket_number
     *   AND sr_returned_date_sk = d2.d_date_sk
     *   AND d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
     *   AND sr_customer_sk = cs_bill_customer_sk
     *   AND sr_item_sk = cs_item_sk
     *   AND cs_sold_date_sk = d3.d_date_sk
     *   AND d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
     * GROUP BY i_item_id,
     *          i_item_desc,
     *          s_state
     * ORDER BY i_item_id NULLS FIRST,
     *          i_item_desc NULLS FIRST,
     *          s_state NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q17")]
    public void Test_Q17()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery1 = TpcdsData.DateDimQuery();
        var dateDimQuery2 = TpcdsData.DateDimQuery();
        var dateDimQuery3 = TpcdsData.DateDimQuery();

        string[] targetQuarters = ["2001Q1", "2001Q2", "2001Q3"];

        var filteredDates1 = dateDimQuery1.Where(d => d.d_quarter_name == "2001Q1");
        var filteredDates2 = dateDimQuery2.Where(d => targetQuarters.Contains(d.d_quarter_name!));
        var filteredDates3 = dateDimQuery3.Where(d => targetQuarters.Contains(d.d_quarter_name!));

        var joined = from ss in storeSalesQuery
                     join d1 in filteredDates1 on ss.ss_sold_date_sk equals d1.d_date_sk
                     join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                     join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                     join sr in storeReturnsQuery on new { customer_sk = ss.ss_customer_sk, item_sk = ss.ss_item_sk, ticket_number = ss.ss_ticket_number }
                                              equals new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk, ticket_number = sr.sr_ticket_number }
                     join d2 in filteredDates2 on sr.sr_returned_date_sk equals d2.d_date_sk
                     join cs in catalogSalesQuery on new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk }
                                              equals new { customer_sk = cs.cs_bill_customer_sk, item_sk = cs.cs_item_sk }
                     join d3 in filteredDates3 on cs.cs_sold_date_sk equals d3.d_date_sk
                     select new
                     {
                         i.i_item_id,
                         i.i_item_desc,
                         s.s_state,
                         ss.ss_quantity,
                         sr.sr_return_quantity,
                         cs.cs_quantity
                     };

        var q17 = (from r in joined
                   group r by new { r.i_item_id, r.i_item_desc, r.s_state } into g
                   orderby g.Key.i_item_id, g.Key.i_item_desc, g.Key.s_state
                   select new
                   {
                       g.Key.i_item_id,
                       g.Key.i_item_desc,
                       g.Key.s_state,
                       store_sales_quantitycount = g.Select(x => x.ss_quantity).Count(),
                       store_sales_quantityave = g.Average(x => (double?)x.ss_quantity),
                       store_sales_quantitystdev = g.Std(x => (double?)x.ss_quantity),
                       store_sales_quantitycov = g.Std(x => (double?)x.ss_quantity) / g.Average(x => (double?)x.ss_quantity),
                       store_returns_quantitycount = g.Select(x => x.sr_return_quantity).Count(),
                       store_returns_quantityave = g.Average(x => (double?)x.sr_return_quantity),
                       store_returns_quantitystdev = g.Std(x => (double?)x.sr_return_quantity),
                       store_returns_quantitycov = g.Std(x => (double?)x.sr_return_quantity) / g.Average(x => (double?)x.sr_return_quantity),
                       catalog_sales_quantitycount = g.Select(x => x.cs_quantity).Count(),
                       catalog_sales_quantityave = g.Average(x => (double?)x.cs_quantity),
                       catalog_sales_quantitystdev = g.Std(x => (double?)x.cs_quantity),
                       catalog_sales_quantitycov = g.Std(x => (double?)x.cs_quantity) / g.Average(x => (double?)x.cs_quantity),
                   }).Take(100);

        AssertPlan(q17, NativePushdown);

        var polarsResult = q17.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT i_item_id,
                   i_item_desc,
                   s_state,
                   count(ss_quantity) AS store_sales_quantitycount,
                   avg(ss_quantity) AS store_sales_quantityave,
                   stddev_samp(ss_quantity) AS store_sales_quantitystdev,
                   stddev_samp(ss_quantity)/avg(ss_quantity) AS store_sales_quantitycov,
                   count(sr_return_quantity) AS store_returns_quantitycount,
                   avg(sr_return_quantity) AS store_returns_quantityave,
                   stddev_samp(sr_return_quantity) AS store_returns_quantitystdev,
                   stddev_samp(sr_return_quantity)/avg(sr_return_quantity) AS store_returns_quantitycov,
                   count(cs_quantity) AS catalog_sales_quantitycount,
                   avg(cs_quantity) AS catalog_sales_quantityave,
                   stddev_samp(cs_quantity) AS catalog_sales_quantitystdev,
                   stddev_samp(cs_quantity)/avg(cs_quantity) AS catalog_sales_quantitycov
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.StoreReturnsPath}'),
                 read_parquet('{TpcdsData.CatalogSalesPath}'),
                 read_parquet('{TpcdsData.DateDimPath}') d1,
                 read_parquet('{TpcdsData.DateDimPath}') d2,
                 read_parquet('{TpcdsData.DateDimPath}') d3,
                 read_parquet('{TpcdsData.StorePath}'),
                 read_parquet('{TpcdsData.ItemPath}')
            WHERE d1.d_quarter_name = '2001Q1'
              AND d1.d_date_sk = ss_sold_date_sk
              AND i_item_sk = ss_item_sk
              AND s_store_sk = ss_store_sk
              AND ss_customer_sk = sr_customer_sk
              AND ss_item_sk = sr_item_sk
              AND ss_ticket_number = sr_ticket_number
              AND sr_returned_date_sk = d2.d_date_sk
              AND d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
              AND sr_customer_sk = cs_bill_customer_sk
              AND sr_item_sk = cs_item_sk
              AND cs_sold_date_sk = d3.d_date_sk
              AND d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
            GROUP BY i_item_id,
                     i_item_desc,
                     s_state
            ORDER BY i_item_id NULLS FIRST,
                     i_item_desc NULLS FIRST,
                     s_state NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.IsDBNull(0) ? null : r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            s_state = r.IsDBNull(2) ? null : r.GetString(2),
            store_sales_quantitycount = r.GetInt64(3),
            store_sales_quantityave = r.IsDBNull(4) ? null : (double?)r.GetDouble(4),
            store_sales_quantitystdev = r.IsDBNull(5) ? null : (double?)r.GetDouble(5),
            store_sales_quantitycov = r.IsDBNull(6) ? null : (double?)r.GetDouble(6),
            store_returns_quantitycount = r.GetInt64(7),
            store_returns_quantityave = r.IsDBNull(8) ? null : (double?)r.GetDouble(8),
            store_returns_quantitystdev = r.IsDBNull(9) ? null : (double?)r.GetDouble(9),
            store_returns_quantitycov = r.IsDBNull(10) ? null : (double?)r.GetDouble(10),
            catalog_sales_quantitycount = r.GetInt64(11),
            catalog_sales_quantityave = r.IsDBNull(12) ? null : (double?)r.GetDouble(12),
            catalog_sales_quantitystdev = r.IsDBNull(13) ? null : (double?)r.GetDouble(13),
            catalog_sales_quantitycov = r.IsDBNull(14) ? null : (double?)r.GetDouble(14),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.i_item_desc, p.i_item_desc);
            Assert.Equal(d.s_state, p.s_state);

            Assert.Equal(d.store_sales_quantitycount, p.store_sales_quantitycount);
            AssertNullableDouble(d.store_sales_quantityave, p.store_sales_quantityave);
            AssertNullableDouble(d.store_sales_quantitystdev, p.store_sales_quantitystdev);
            AssertNullableDouble(d.store_sales_quantitycov, p.store_sales_quantitycov);

            Assert.Equal(d.store_returns_quantitycount, p.store_returns_quantitycount);
            AssertNullableDouble(d.store_returns_quantityave, p.store_returns_quantityave);
            AssertNullableDouble(d.store_returns_quantitystdev, p.store_returns_quantitystdev);
            AssertNullableDouble(d.store_returns_quantitycov, p.store_returns_quantitycov);

            Assert.Equal(d.catalog_sales_quantitycount, p.catalog_sales_quantitycount);
            AssertNullableDouble(d.catalog_sales_quantityave, p.catalog_sales_quantityave);
            AssertNullableDouble(d.catalog_sales_quantitystdev, p.catalog_sales_quantitystdev);
            AssertNullableDouble(d.catalog_sales_quantitycov, p.catalog_sales_quantitycov);
        }
    }

    /**
     * TPC-DS Query 18
     *
     * SELECT i_item_id,
     *        ca_country,
     *        ca_state,
     *        ca_county,
     *        avg(cast(cs_quantity AS decimal(12, 2)))      agg1,
     *        avg(cast(cs_list_price AS decimal(12, 2)))    agg2,
     *        avg(cast(cs_coupon_amt AS decimal(12, 2)))    agg3,
     *        avg(cast(cs_sales_price AS decimal(12, 2)))   agg4,
     *        avg(cast(cs_net_profit AS decimal(12, 2)))    agg5,
     *        avg(cast(c_birth_year AS decimal(12, 2)))     agg6,
     *        avg(cast(cd1.cd_dep_count AS decimal(12, 2))) agg7
     * FROM catalog_sales,
     *      customer_demographics cd1,
     *      customer_demographics cd2,
     *      customer,
     *      customer_address,
     *      date_dim,
     *      item
     * WHERE cs_sold_date_sk = d_date_sk
     *   AND cs_item_sk = i_item_sk
     *   AND cs_bill_cdemo_sk = cd1.cd_demo_sk
     *   AND cs_bill_customer_sk = c_customer_sk
     *   AND cd1.cd_gender = 'F'
     *   AND cd1.cd_education_status = 'Unknown'
     *   AND c_current_cdemo_sk = cd2.cd_demo_sk
     *   AND c_current_addr_sk = ca_address_sk
     *   AND c_birth_month IN (1, 6, 8, 9, 12, 2)
     *   AND d_year = 1998
     *   AND ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS')
     * GROUP BY ROLLUP (i_item_id,
     *     ca_country,
     *     ca_state,
     *     ca_county)
     * ORDER BY ca_country NULLS FIRST,
     *     ca_state NULLS FIRST,
     *     ca_county NULLS FIRST,
     *     i_item_id NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q18")]
    public void Test_Q18()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var customerDemographicsQuery1 = TpcdsData.CustomerDemographicsQuery();
        var customerDemographicsQuery2 = TpcdsData.CustomerDemographicsQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_year == 1998);
        var filteredCD1 = customerDemographicsQuery1.Where(cd1 => cd1.cd_gender == "F" && cd1.cd_education_status == "Unknown");

        long[] birthMonths = [1, 6, 8, 9, 12, 2];
        var filteredCustomers = customerQuery.Where(c => birthMonths.Contains(c.c_birth_month!.Value));

        string[] targetStates = ["MS", "IN", "ND", "OK", "NM", "VA"];
        var filteredAddresses = customerAddressQuery.Where(ca => targetStates.Contains(ca.ca_state!));

        var joined = from cs in catalogSalesQuery
                     join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                     join i in itemQuery on cs.cs_item_sk equals i.i_item_sk
                     join cd1 in filteredCD1 on cs.cs_bill_cdemo_sk equals cd1.cd_demo_sk
                     join c in filteredCustomers on cs.cs_bill_customer_sk equals c.c_customer_sk
                     join cd2 in customerDemographicsQuery2 on c.c_current_cdemo_sk equals cd2.cd_demo_sk
                     join ca in filteredAddresses on c.c_current_addr_sk equals ca.ca_address_sk
                     select new
                     {
                         i.i_item_id,
                         ca.ca_country,
                         ca.ca_state,
                         ca.ca_county,
                         cs.cs_quantity,
                         cs.cs_list_price,
                         cs.cs_coupon_amt,
                         cs.cs_sales_price,
                         cs.cs_net_profit,
                         c.c_birth_year,
                         cd1.cd_dep_count
                     };

        var q18 = joined
            .GroupByRollup(
                x => new { x.i_item_id, x.ca_country, x.ca_state, x.ca_county },
                (key, g) => new
                {
                    key.i_item_id,
                    key.ca_country,
                    key.ca_state,
                    key.ca_county,
                    agg1 = g.Average(x => (double?)x.cs_quantity),
                    agg2 = g.Average(x => (double?)x.cs_list_price),
                    agg3 = g.Average(x => (double?)x.cs_coupon_amt),
                    agg4 = g.Average(x => (double?)x.cs_sales_price),
                    agg5 = g.Average(x => (double?)x.cs_net_profit),
                    agg6 = g.Average(x => (double?)x.c_birth_year),
                    agg7 = g.Average(x => (double?)x.cd_dep_count)
                }
            )
            .OrderBy(x => x.ca_country)
            .ThenBy(x => x.ca_state)
            .ThenBy(x => x.ca_county)
            .ThenBy(x => x.i_item_id)
            .Take(100);

        AssertPlan(q18, NativePushdown);

        var polarsResult = q18.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT i_item_id,
                   ca_country,
                   ca_state,
                   ca_county,
                   avg(cast(cs_quantity AS decimal(12, 2)))      agg1,
                   avg(cast(cs_list_price AS decimal(12, 2)))    agg2,
                   avg(cast(cs_coupon_amt AS decimal(12, 2)))    agg3,
                   avg(cast(cs_sales_price AS decimal(12, 2)))   agg4,
                   avg(cast(cs_net_profit AS decimal(12, 2)))    agg5,
                   avg(cast(c_birth_year AS decimal(12, 2)))     agg6,
                   avg(cast(cd1.cd_dep_count AS decimal(12, 2))) agg7
            FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}') cd1,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}') cd2,
                 read_parquet('{TpcdsData.CustomerPath}') c,
                 read_parquet('{TpcdsData.CustomerAddressPath}') ca,
                 read_parquet('{TpcdsData.DateDimPath}') d,
                 read_parquet('{TpcdsData.ItemPath}') i
            WHERE cs_sold_date_sk = d_date_sk
              AND cs_item_sk = i_item_sk
              AND cs_bill_cdemo_sk = cd1.cd_demo_sk
              AND cs_bill_customer_sk = c_customer_sk
              AND cd1.cd_gender = 'F'
              AND cd1.cd_education_status = 'Unknown'
              AND c_current_cdemo_sk = cd2.cd_demo_sk
              AND c_current_addr_sk = ca_address_sk
              AND c_birth_month IN (1, 6, 8, 9, 12, 2)
              AND d_year = 1998
              AND ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS')
            GROUP BY ROLLUP (i_item_id,
                ca_country,
                ca_state,
                ca_county)
            ORDER BY ca_country NULLS FIRST,
                ca_state NULLS FIRST,
                ca_county NULLS FIRST,
                i_item_id NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.IsDBNull(0) ? null : r.GetString(0),
            ca_country = r.IsDBNull(1) ? null : r.GetString(1),
            ca_state = r.IsDBNull(2) ? null : r.GetString(2),
            ca_county = r.IsDBNull(3) ? null : r.GetString(3),
            agg1 = r.IsDBNull(4) ? null : (double?)r.GetDouble(4),
            agg2 = r.IsDBNull(5) ? null : (double?)r.GetDouble(5),
            agg3 = r.IsDBNull(6) ? null : (double?)r.GetDouble(6),
            agg4 = r.IsDBNull(7) ? null : (double?)r.GetDouble(7),
            agg5 = r.IsDBNull(8) ? null : (double?)r.GetDouble(8),
            agg6 = r.IsDBNull(9) ? null : (double?)r.GetDouble(9),
            agg7 = r.IsDBNull(10) ? null : (double?)r.GetDouble(10),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.ca_country, p.ca_country);
            Assert.Equal(d.ca_state, p.ca_state);
            Assert.Equal(d.ca_county, p.ca_county);

            AssertNullableDouble(d.agg1, p.agg1);
            AssertNullableDouble(d.agg2, p.agg2);
            AssertNullableDouble(d.agg3, p.agg3);
            AssertNullableDouble(d.agg4, p.agg4);
            AssertNullableDouble(d.agg5, p.agg5);
            AssertNullableDouble(d.agg6, p.agg6);
            AssertNullableDouble(d.agg7, p.agg7);
        }
    }

    private static void AssertNullableDouble(double? expected, double? actual, double tolerance = 0.001)
    {
        if (expected == null)
        {
            // In SQL stddev_samp/cov on a group of size 1 returns NULL, whereas Polars returns 0.0 or NaN
            Assert.True(actual == null || double.IsNaN(actual.Value) || actual.Value == 0.0, $"Expected null or 0.0 but got {actual}");
        }
        else
        {
            Assert.NotNull(actual);
            Assert.Equal(expected.Value, actual.Value, tolerance);
        }
    }
}
