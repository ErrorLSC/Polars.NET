namespace Polars.Tpcds;

public static partial class TpcdsCases
{
    private static readonly string[] Q12TargetCategories = ["Sports", "Books", "Home"];

    private static readonly string[] Q20TargetCategories = ["Sports", "Books", "Home"];

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
    public static IQueryable<object> Q11()
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

        return limitedQ11;
    }

    public static string Q11Sql() =>
        $@"
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
    public static IQueryable<object> Q12()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var minDate = new DateOnly(1999, 2, 22);
        var maxDate = new DateOnly(1999, 3, 24);

        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredItems = itemQuery.Where(i => Q12TargetCategories.Contains(i.i_category));

        var q12 = from ws in webSalesQuery
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
                  let itemrevenue = g.Sum(x => x.ws_ext_sales_price)
                  let revenueratio = (double?)(itemrevenue * 100.0m) / (double?)itemrevenue.Sum().Over(g.Key.i_class)
                  orderby g.Key.i_category, g.Key.i_class, g.Key.i_item_id, g.Key.i_item_desc, revenueratio
                  select new
                  {
                      g.Key.i_item_id,
                      g.Key.i_item_desc,
                      g.Key.i_category,
                      g.Key.i_class,
                      g.Key.i_current_price,
                      itemrevenue,
                      revenueratio
                  };

        var limitedQ12 = q12.Take(100);

        return limitedQ12;
    }

    public static string Q12Sql() =>
        $@"
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
            ORDER BY i_category NULLS FIRST,
                     i_class NULLS FIRST,
                     i_item_id NULLS FIRST,
                     i_item_desc NULLS FIRST,
                     revenueratio NULLS FIRST
            LIMIT 100;
        ";

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
    public static IQueryable<object> Q13()
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
                      // SQL sum() is NULL for a group without non-null values, while LINQ Sum() is 0.
                     sum4 = g.Count(x => x.ss_ext_wholesale_cost != null) > 0 ? g.Sum(x => x.ss_ext_wholesale_cost) : null
                  };

        return q13;
    }

    public static string Q13Sql() =>
        $@"
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
     *   and d_year = 2001
     * group by ca_zip
     * order by ca_zip
     * limit 100;
     */
    public static IQueryable<object> Q15()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        string[] targetZips = ["85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792"];
        string[] targetStates = ["CA", "WA", "GA"];

        var filteredDates = dateDimQuery.Where(d => d.d_qoy == 2 && d.d_year == 2001);

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

        return q15;
    }

    public static string Q15Sql() =>
        $@"
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
              AND d.d_year = 2001
            GROUP BY ca_zip
            ORDER BY ca_zip NULLS FIRST
            LIMIT 100;
        ";

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
    public static IQueryable<object> Q17()
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

        var q17 = (from ss in storeSalesQuery
                   join d1 in filteredDates1 on ss.ss_sold_date_sk equals d1.d_date_sk
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                   join sr in storeReturnsQuery on new { customer_sk = ss.ss_customer_sk, item_sk = ss.ss_item_sk, ticket_number = ss.ss_ticket_number }
                                            equals new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk, ticket_number = sr.sr_ticket_number }
                   join d2 in filteredDates2 on sr.sr_returned_date_sk equals d2.d_date_sk
                   join cs in catalogSalesQuery on new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk }
                                            equals new { customer_sk = cs.cs_bill_customer_sk, item_sk = cs.cs_item_sk }
                   join d3 in filteredDates3 on cs.cs_sold_date_sk equals d3.d_date_sk
                   group new { ss, sr, cs } by new { i.i_item_id, i.i_item_desc, s.s_state } into g
                   orderby g.Key.i_item_id, g.Key.i_item_desc, g.Key.s_state
                   select new
                   {
                       g.Key.i_item_id,
                       g.Key.i_item_desc,
                       g.Key.s_state,
                       // SQL count(col) skips NULLs.
                       store_sales_quantitycount = g.Count(x => x.ss.ss_quantity != null),
                       store_sales_quantityave = g.Average(x => (double?)x.ss.ss_quantity),
                       store_sales_quantitystdev = g.Std(x => (double?)x.ss.ss_quantity),
                       store_sales_quantitycov = g.Std(x => (double?)x.ss.ss_quantity) / g.Average(x => (double?)x.ss.ss_quantity),
                       store_returns_quantitycount = g.Count(x => x.sr.sr_return_quantity != null),
                       store_returns_quantityave = g.Average(x => (double?)x.sr.sr_return_quantity),
                       store_returns_quantitystdev = g.Std(x => (double?)x.sr.sr_return_quantity),
                       store_returns_quantitycov = g.Std(x => (double?)x.sr.sr_return_quantity) / g.Average(x => (double?)x.sr.sr_return_quantity),
                       catalog_sales_quantitycount = g.Count(x => x.cs.cs_quantity != null),
                       catalog_sales_quantityave = g.Average(x => (double?)x.cs.cs_quantity),
                       catalog_sales_quantitystdev = g.Std(x => (double?)x.cs.cs_quantity),
                       catalog_sales_quantitycov = g.Std(x => (double?)x.cs.cs_quantity) / g.Average(x => (double?)x.cs.cs_quantity),
                   }).Take(100);

        return q17;
    }

    public static string Q17Sql() =>
        $@"
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
    public static IQueryable<object> Q18()
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
            .GroupBy(x => new { x.i_item_id, x.ca_country, x.ca_state, x.ca_county })
            .RollUp(
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

        return q18;
    }

    public static string Q18Sql() =>
        $@"
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

    /*
     * TPC-DS Query 19
     *
     * SELECT i_brand_id brand_id,
     *        i_brand brand,
     *        i_manufact_id,
     *        i_manufact,
     *        sum(ss_ext_sales_price) ext_price
     * FROM date_dim,
     *      store_sales,
     *      item,
     *      customer,
     *      customer_address,
     *      store
     * WHERE d_date_sk = ss_sold_date_sk
     *   AND ss_item_sk = i_item_sk
     *   AND i_manager_id = 8
     *   AND d_moy = 11
     *   AND d_year = 1998
     *   AND ss_customer_sk = c_customer_sk
     *   AND c_current_addr_sk = ca_address_sk
     *   AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
     *   AND ss_store_sk = s_store_sk
     * GROUP BY i_brand,
     *          i_brand_id,
     *          i_manufact_id,
     *          i_manufact
     * ORDER BY ext_price DESC,
     *          brand,
     *          brand_id,
     *          i_manufact_id,
     *          i_manufact
     * LIMIT 100;
     */
    public static IQueryable<object> Q19()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_year == 1998 && d.d_moy == 11);
        var filteredItems = itemQuery.Where(i => i.i_manager_id == 8);

        var q19 = from ss in storeSalesQuery
                  join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                  join i in filteredItems on ss.ss_item_sk equals i.i_item_sk
                  join c in customerQuery on ss.ss_customer_sk equals c.c_customer_sk
                  join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                  join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                  // SQL <> / = is unknown when either side is NULL; C# null semantics would compare NULLs.
                  where ca.ca_zip != null && s.s_zip != null && ca.ca_zip.Substring(0, 5) != s.s_zip.Substring(0, 5)
                  group ss by new
                  {
                      i.i_brand,
                      i.i_brand_id,
                      i.i_manufact_id,
                      i.i_manufact
                  } into g
                  let ext_price = g.Sum(x => x.ss_ext_sales_price)
                  orderby ext_price descending,
                          g.Key.i_brand,
                          g.Key.i_brand_id,
                          g.Key.i_manufact_id,
                          g.Key.i_manufact
                  select new
                  {
                      brand_id = g.Key.i_brand_id,
                      brand = g.Key.i_brand,
                      g.Key.i_manufact_id,
                      g.Key.i_manufact,
                      ext_price
                  };

        var limitedQ19 = q19.Take(100);

        return limitedQ19;
    }

    public static string Q19Sql() =>
        $@"
            SELECT i_brand_id brand_id,
                   i_brand brand,
                   i_manufact_id,
                   i_manufact,
                   sum(ss_ext_sales_price) ext_price
            FROM read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.CustomerPath}'),
                 read_parquet('{TpcdsData.CustomerAddressPath}'),
                 read_parquet('{TpcdsData.StorePath}')
            WHERE d_date_sk = ss_sold_date_sk
              AND ss_item_sk = i_item_sk
              AND i_manager_id = 8
              AND d_moy = 11
              AND d_year = 1998
              AND ss_customer_sk = c_customer_sk
              AND c_current_addr_sk = ca_address_sk
              AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
              AND ss_store_sk = s_store_sk
            GROUP BY i_brand,
                     i_brand_id,
                     i_manufact_id,
                     i_manufact
            ORDER BY ext_price DESC,
                     brand,
                     brand_id,
                     i_manufact_id,
                     i_manufact
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 20
     *
     * SELECT i_item_id,
     *        i_item_desc,
     *        i_category,
     *        i_class,
     *        i_current_price,
     *        sum(cs_ext_sales_price) AS itemrevenue,
     *        sum(cs_ext_sales_price)*100.0000/sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
     * FROM catalog_sales,
     *      item,
     *      date_dim
     * WHERE cs_item_sk = i_item_sk
     *   AND i_category IN ('Sports',
     *                      'Books',
     *                      'Home')
     *   AND cs_sold_date_sk = d_date_sk
     *   AND d_date BETWEEN cast('1999-02-22' AS date) AND (cast('1999-02-22' AS date) + interval 30 day)
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
    public static IQueryable<object> Q20()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var minDate = new DateOnly(1999, 2, 22);
        var maxDate = new DateOnly(1999, 3, 24);

        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredItems = itemQuery.Where(i => Q20TargetCategories.Contains(i.i_category));

        var q20 = from cs in catalogSalesQuery
                  join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                  join i in filteredItems on cs.cs_item_sk equals i.i_item_sk
                  group cs by new
                  {
                      i.i_item_id,
                      i.i_item_desc,
                      i.i_category,
                      i.i_class,
                      i.i_current_price
                  } into g
                  let itemrevenue = g.Sum(x => x.cs_ext_sales_price)
                  let revenueratio = (double?)(itemrevenue * 100.0m) / (double?)itemrevenue.Sum().Over(g.Key.i_class)
                  orderby g.Key.i_category, g.Key.i_class, g.Key.i_item_id, g.Key.i_item_desc, revenueratio
                  select new
                  {
                      g.Key.i_item_id,
                      g.Key.i_item_desc,
                      g.Key.i_category,
                      g.Key.i_class,
                      g.Key.i_current_price,
                      itemrevenue,
                      revenueratio
                  };

        var limitedQ20 = q20.Take(100);

        return limitedQ20;
    }

    public static string Q20Sql() =>
        $@"
            SELECT i_item_id,
                   i_item_desc,
                   i_category,
                   i_class,
                   i_current_price,
                   sum(cs_ext_sales_price) AS itemrevenue,
                   sum(cs_ext_sales_price)*100.0000/sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
            FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.DateDimPath}')
            WHERE cs_item_sk = i_item_sk
              AND i_category IN ('Sports',
                                 'Books',
                                 'Home')
              AND cs_sold_date_sk = d_date_sk
              AND d_date BETWEEN cast('1999-02-22' AS date) AND (cast('1999-02-22' AS date) + interval 30 day)
            GROUP BY i_item_id,
                     i_item_desc,
                     i_category,
                     i_class,
                     i_current_price
            ORDER BY i_category NULLS FIRST,
                     i_class NULLS FIRST,
                     i_item_id NULLS FIRST,
                     i_item_desc NULLS FIRST,
                     revenueratio NULLS FIRST
            LIMIT 100;
        ";
}
