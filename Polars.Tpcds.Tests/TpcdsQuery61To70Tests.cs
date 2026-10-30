namespace Polars.Tpcds.Tests;

public class TpcdsQuery61To70Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 61
     *
     * SELECT promotions,
     *        total,
     *        cast(promotions AS decimal(15,4))/cast(total AS decimal(15,4))*100
     * FROM
     *   (SELECT sum(ss_ext_sales_price) promotions
     *    FROM store_sales,
     *         store,
     *         promotion,
     *         date_dim,
     *         customer,
     *         customer_address,
     *         item
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND ss_store_sk = s_store_sk
     *      AND ss_promo_sk = p_promo_sk
     *      AND ss_customer_sk= c_customer_sk
     *      AND ca_address_sk = c_current_addr_sk
     *      AND ss_item_sk = i_item_sk
     *      AND ca_gmt_offset = -5
     *      AND i_category = 'Jewelry'
     *      AND (p_channel_dmail = 'Y'
     *           OR p_channel_email = 'Y'
     *           OR p_channel_tv = 'Y')
     *      AND s_gmt_offset = -5
     *      AND d_year = 1998
     *      AND d_moy = 11) promotional_sales,
     *   (SELECT sum(ss_ext_sales_price) total
     *    FROM store_sales,
     *         store,
     *         date_dim,
     *         customer,
     *         customer_address,
     *         item
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND ss_store_sk = s_store_sk
     *      AND ss_customer_sk= c_customer_sk
     *      AND ca_address_sk = c_current_addr_sk
     *      AND ss_item_sk = i_item_sk
     *      AND ca_gmt_offset = -5
     *      AND i_category = 'Jewelry'
     *      AND s_gmt_offset = -5
     *      AND d_year = 1998
     *      AND d_moy = 11) all_sales
     * ORDER BY promotions,
     *          total
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q61()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var promotionQuery = TpcdsData.PromotionQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var baseSales = from ss in storeSalesQuery
                        join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                        join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                        join c in customerQuery on ss.ss_customer_sk equals c.c_customer_sk
                        join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                        join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                        where ca.ca_gmt_offset == -5m && i.i_category == "Jewelry"
                           && s.s_gmt_offset == -5m && d.d_year == 1998 && d.d_moy == 11
                        select new
                        {
                            ss.ss_promo_sk,
                            ss.ss_ext_sales_price
                        };

        var promoSales = from ss in baseSales
                         join p in promotionQuery on ss.ss_promo_sk equals p.p_promo_sk
                         where p.p_channel_dmail == "Y" || p.p_channel_email == "Y" || p.p_channel_tv == "Y"
                         group ss by 1 into g
                         select new
                         {
                             key = g.Key,
                             promotions = g.Sum(x => x.ss_ext_sales_price)
                         };

        var allSales = from ss in baseSales
                       group ss by 1 into g
                       select new
                       {
                           key = g.Key,
                           total = g.Sum(x => x.ss_ext_sales_price)
                       };

        var q61 = (from ps in promoSales
                   join als in allSales on ps.key equals als.key
                   orderby ps.promotions, als.total
                   select new
                   {
                       ps.promotions,
                       als.total,
                       pct = (double)ps.promotions! / (double)als.total! * 100.0
                   })
                  .Take(100);

        AssertPlan(q61, NativePushdown);

        var polarsResult = q61.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT promotions,
                   total,
                   cast(promotions AS decimal(15,4))/cast(total AS decimal(15,4))*100
            FROM
              (SELECT sum(ss_ext_sales_price) promotions
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.StorePath}'),
                    read_parquet('{TpcdsData.PromotionPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE ss_sold_date_sk = d_date_sk
                 AND ss_store_sk = s_store_sk
                 AND ss_promo_sk = p_promo_sk
                 AND ss_customer_sk= c_customer_sk
                 AND ca_address_sk = c_current_addr_sk
                 AND ss_item_sk = i_item_sk
                 AND ca_gmt_offset = -5
                 AND i_category = 'Jewelry'
                 AND (p_channel_dmail = 'Y'
                      OR p_channel_email = 'Y'
                      OR p_channel_tv = 'Y')
                 AND s_gmt_offset = -5
                 AND d_year = 1998
                 AND d_moy = 11) promotional_sales,
              (SELECT sum(ss_ext_sales_price) total
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.StorePath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE ss_sold_date_sk = d_date_sk
                 AND ss_store_sk = s_store_sk
                 AND ss_customer_sk= c_customer_sk
                 AND ca_address_sk = c_current_addr_sk
                 AND ss_item_sk = i_item_sk
                 AND ca_gmt_offset = -5
                 AND i_category = 'Jewelry'
                 AND s_gmt_offset = -5
                 AND d_year = 1998
                 AND d_moy = 11) all_sales
            ORDER BY promotions,
                     total
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            promotions = r.IsDBNull(0) ? (decimal?)null : r.GetDecimal(0),
            total = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1),
            pct = r.IsDBNull(2) ? (double?)null : (double)r.GetDecimal(2)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.promotions, p.promotions);
            Assert.Equal(d.total, p.total);
            Assert.Equal(d.pct ?? 0.0, p.pct, 4);
        }
    }

    /*
     * TPC-DS Query 62
     *
     * SELECT w_substr,
     *        sm_type,
     *        web_name,
     *        sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
     *        sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 30) AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days",
     *        sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 60) AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days",
     *        sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 90) AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days",
     *        sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
     * FROM web_sales,
     *   (SELECT SUBSTRING(w_warehouse_name,1,20) w_substr,
     *           *
     *    FROM warehouse) sq1,
     *      ship_mode,
     *      web_site,
     *      date_dim
     * WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
     *   AND ws_ship_date_sk = d_date_sk
     *   AND ws_warehouse_sk = w_warehouse_sk
     *   AND ws_ship_mode_sk = sm_ship_mode_sk
     *   AND ws_web_site_sk = web_site_sk
     * GROUP BY w_substr,
     *          sm_type,
     *          web_name
     * ORDER BY 1 NULLS FIRST,
     *          2 NULLS FIRST,
     *          3 NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q62()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var shipModeQuery = TpcdsData.ShipModeQuery();
        var webSiteQuery = TpcdsData.WebSiteQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var q62 = (from ws in webSalesQuery
                   join d in dateDimQuery on ws.ws_ship_date_sk equals d.d_date_sk
                   join w in warehouseQuery on ws.ws_warehouse_sk equals w.w_warehouse_sk
                   join sm in shipModeQuery on ws.ws_ship_mode_sk equals sm.sm_ship_mode_sk
                   join web in webSiteQuery on ws.ws_web_site_sk equals web.web_site_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                   let w_substr = w.w_warehouse_name!.Substring(0, 20)
                   group ws by new { w_substr, sm.sm_type, web.web_name } into g
                   orderby g.Key.w_substr, g.Key.sm_type, g.Key.web_name
                   select new
                   {
                       g.Key.w_substr,
                       g.Key.sm_type,
                       g.Key.web_name,
                       days_30 = g.Sum(x => x.ws_ship_date_sk - x.ws_sold_date_sk <= 30 ? 1 : 0),
                       days_31_60 = g.Sum(x => x.ws_ship_date_sk - x.ws_sold_date_sk > 30 && x.ws_ship_date_sk - x.ws_sold_date_sk <= 60 ? 1 : 0),
                       days_61_90 = g.Sum(x => x.ws_ship_date_sk - x.ws_sold_date_sk > 60 && x.ws_ship_date_sk - x.ws_sold_date_sk <= 90 ? 1 : 0),
                       days_91_120 = g.Sum(x => x.ws_ship_date_sk - x.ws_sold_date_sk > 90 && x.ws_ship_date_sk - x.ws_sold_date_sk <= 120 ? 1 : 0),
                       days_over_120 = g.Sum(x => x.ws_ship_date_sk - x.ws_sold_date_sk > 120 ? 1 : 0)
                   })
                  .Take(100);

        AssertPlan(q62, NativePushdown);

        var polarsResult = q62.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT w_substr,
                   sm_type,
                   web_name,
                   sum(CASE
                           WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1
                           ELSE 0
                       END) AS ""30 days"",
                   sum(CASE
                           WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
                                AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1
                           ELSE 0
                       END) AS ""31-60 days"",
                   sum(CASE
                           WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
                                AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1
                           ELSE 0
                       END) AS ""61-90 days"",
                   sum(CASE
                           WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
                                AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1
                           ELSE 0
                       END) AS ""91-120 days"",
                   sum(CASE
                           WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1
                           ELSE 0
                       END) AS "">120 days""
            FROM read_parquet('{TpcdsData.WebSalesPath}'),
              (SELECT SUBSTRING(w_warehouse_name,1,20) w_substr,
                      *
               FROM read_parquet('{TpcdsData.WarehousePath}')) sq1,
                 read_parquet('{TpcdsData.ShipModePath}'),
                 read_parquet('{TpcdsData.WebSitePath}'),
                 read_parquet('{TpcdsData.DateDimPath}')
            WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
              AND ws_ship_date_sk = d_date_sk
              AND ws_warehouse_sk = w_warehouse_sk
              AND ws_ship_mode_sk = sm_ship_mode_sk
              AND ws_web_site_sk = web_site_sk
            GROUP BY w_substr,
                     sm_type,
                     web_name
            ORDER BY 1 NULLS FIRST,
                     2 NULLS FIRST,
                     3 NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            w_substr = r.IsDBNull(0) ? null : r.GetString(0),
            sm_type = r.IsDBNull(1) ? null : r.GetString(1),
            web_name = r.IsDBNull(2) ? null : r.GetString(2),
            days_30 = r.IsDBNull(3) ? (long?)null : r.GetInt64(3),
            days_31_60 = r.IsDBNull(4) ? (long?)null : r.GetInt64(4),
            days_61_90 = r.IsDBNull(5) ? (long?)null : r.GetInt64(5),
            days_91_120 = r.IsDBNull(6) ? (long?)null : r.GetInt64(6),
            days_over_120 = r.IsDBNull(7) ? (long?)null : r.GetInt64(7)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.w_substr, p.w_substr);
            Assert.Equal(d.sm_type, p.sm_type);
            Assert.Equal(d.web_name, p.web_name);
            Assert.Equal(d.days_30, p.days_30);
            Assert.Equal(d.days_31_60, p.days_31_60);
            Assert.Equal(d.days_61_90, p.days_61_90);
            Assert.Equal(d.days_91_120, p.days_91_120);
            Assert.Equal(d.days_over_120, p.days_over_120);
        }
    }

    /*
     * TPC-DS Query 63
     *
     * SELECT *
     * FROM
     *   (SELECT i_manager_id,
     *           sum(ss_sales_price) sum_sales,
     *           avg(sum(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
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
     *    GROUP BY i_manager_id,
     *             d_moy) tmp1
     * WHERE CASE
     *           WHEN avg_monthly_sales > 0 THEN ABS (sum_sales - avg_monthly_sales) / avg_monthly_sales
     *           ELSE NULL
     *       END > 0.1
     * ORDER BY i_manager_id,
     *          avg_monthly_sales,
     *          sum_sales
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q63()
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

        var q63 = (from ss in storeSalesQuery
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                   join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                      && ((cat1.Contains(i.i_category) && class1.Contains(i.i_class) && brand1.Contains(i.i_brand))
                          || (cat2.Contains(i.i_category) && class2.Contains(i.i_class) && brand2.Contains(i.i_brand)))
                   group ss by new { i.i_manager_id, d.d_moy } into g
                   let sum_sales = g.Sum(x => x.ss_sales_price)
                   let avg_monthly_sales = sum_sales.Average().Over(g.Key.i_manager_id)
                   where avg_monthly_sales > 0.0
                      && (Math.Abs((double)sum_sales! - avg_monthly_sales) / avg_monthly_sales > 0.1)
                   orderby g.Key.i_manager_id, avg_monthly_sales, sum_sales
                   select new
                   {
                       g.Key.i_manager_id,
                       sum_sales,
                       avg_monthly_sales
                   })
                  .Take(100);

        AssertPlan(q63, NativePushdown);

        var polarsResult = q63.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT *
            FROM
              (SELECT i_manager_id,
                      sum(ss_sales_price) sum_sales,
                      avg(sum(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
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
               GROUP BY i_manager_id,
                        d_moy) tmp1
            WHERE CASE
                      WHEN avg_monthly_sales > 0 THEN ABS (sum_sales - avg_monthly_sales) / avg_monthly_sales
                      ELSE NULL
                  END > 0.1
            ORDER BY i_manager_id,
                     avg_monthly_sales,
                     sum_sales
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_manager_id = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            sum_sales = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1),
            avg_monthly_sales = r.IsDBNull(2) ? (double?)null : (double)r.GetDecimal(2)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.i_manager_id, p.i_manager_id);
            Assert.Equal(d.sum_sales, p.sum_sales);
            Assert.Equal(d.avg_monthly_sales ?? 0.0, p.avg_monthly_sales, 4);
        }
    }

    /*
     * TPC-DS Query 64
     *
     * WITH cs_ui AS
     *   (SELECT cs_item_sk,
     *           sum(cs_ext_list_price) AS sale,
     *           sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) AS refund
     *    FROM catalog_sales,
     *         catalog_returns
     *    WHERE cs_item_sk = cr_item_sk
     *      AND cs_order_number = cr_order_number
     *    GROUP BY cs_item_sk
     *    HAVING sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
     *      cross_sales AS
     *   (SELECT i_product_name product_name,
     *           i_item_sk item_sk,
     *           s_store_name store_name,
     *           s_zip store_zip,
     *           ad1.ca_street_number b_street_number,
     *           ad1.ca_street_name b_street_name,
     *           ad1.ca_city b_city,
     *           ad1.ca_zip b_zip,
     *           ad2.ca_street_number c_street_number,
     *           ad2.ca_street_name c_street_name,
     *           ad2.ca_city c_city,
     *           ad2.ca_zip c_zip,
     *           d1.d_year AS syear,
     *           d2.d_year AS fsyear,
     *           d3.d_year s2year,
     *           count(*) cnt,
     *           sum(ss_wholesale_cost) s1,
     *           sum(ss_list_price) s2,
     *           sum(ss_coupon_amt) s3
     *    FROM store_sales,
     *         store_returns,
     *         cs_ui,
     *         date_dim d1,
     *         date_dim d2,
     *         date_dim d3,
     *         store,
     *         customer,
     *         customer_demographics cd1,
     *         customer_demographics cd2,
     *         promotion,
     *         household_demographics hd1,
     *         household_demographics hd2,
     *         customer_address ad1,
     *         customer_address ad2,
     *         income_band ib1,
     *         income_band ib2,
     *         item
     *    WHERE ...
     *    GROUP BY ...)
     * SELECT ...
     * FROM cross_sales cs1,
     *      cross_sales cs2
     * WHERE cs1.item_sk=cs2.item_sk
     *   AND cs1.syear = 1999
     *   AND cs2.syear = 1999 + 1
     *   AND cs2.cnt <= cs1.cnt
     *   AND cs1.store_name = cs2.store_name
     *   AND cs1.store_zip = cs2.store_zip
     * ORDER BY cs1.product_name,
     *          cs1.store_name,
     *          cs2.cnt,
     *          cs1.s1,
     *          cs2.s1;
     */
    [Fact]
    public void Test_Q64()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var promotionQuery = TpcdsData.PromotionQuery();
        var householdDemographicsQuery = TpcdsData.HouseholdDemographicsQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var incomeBandQuery = TpcdsData.IncomeBandQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var colors = new[] { "purple", "burlywood", "indian", "spring", "floral", "medium" };

        var csUiBase = from cs in catalogSalesQuery
                       join cr in catalogReturnsQuery on new { item_sk = cs.cs_item_sk, order_number = cs.cs_order_number }
                                                  equals new { item_sk = cr.cr_item_sk, order_number = cr.cr_order_number }
                       group new { cs, cr } by cs.cs_item_sk into g
                       select new
                       {
                           cs_item_sk = g.Key,
                           sale = g.Sum(x => x.cs.cs_ext_list_price),
                           refund = g.Sum(x => x.cr.cr_refunded_cash + x.cr.cr_reversed_charge + x.cr.cr_store_credit)
                       };

        var csUi = from x in csUiBase
                   where x.sale > 2m * x.refund
                   select x;

        var crossSales = from ss in storeSalesQuery
                         join sr in storeReturnsQuery on new { item_sk = ss.ss_item_sk, ticket_number = ss.ss_ticket_number }
                                                  equals new { item_sk = sr.sr_item_sk, ticket_number = sr.sr_ticket_number }
                         join cu in csUi on ss.ss_item_sk equals cu.cs_item_sk
                         join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                         where colors.Contains(i.i_color) && i.i_current_price >= 65m && i.i_current_price <= 74m
                         join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                         join d1 in dateDimQuery on ss.ss_sold_date_sk equals d1.d_date_sk
                         join c in customerQuery on ss.ss_customer_sk equals c.c_customer_sk
                         join cd1 in customerDemographicsQuery on ss.ss_cdemo_sk equals cd1.cd_demo_sk
                         join cd2 in customerDemographicsQuery on c.c_current_cdemo_sk equals cd2.cd_demo_sk
                         where cd1.cd_marital_status != cd2.cd_marital_status
                         join hd1 in householdDemographicsQuery on ss.ss_hdemo_sk equals hd1.hd_demo_sk
                         join hd2 in householdDemographicsQuery on c.c_current_hdemo_sk equals hd2.hd_demo_sk
                         join ib1 in incomeBandQuery on hd1.hd_income_band_sk equals ib1.ib_income_band_sk
                         join ib2 in incomeBandQuery on hd2.hd_income_band_sk equals ib2.ib_income_band_sk
                         join ad1 in customerAddressQuery on ss.ss_addr_sk equals ad1.ca_address_sk
                         join ad2 in customerAddressQuery on c.c_current_addr_sk equals ad2.ca_address_sk
                         join d2 in dateDimQuery on c.c_first_sales_date_sk equals d2.d_date_sk
                         join d3 in dateDimQuery on c.c_first_shipto_date_sk equals d3.d_date_sk
                         join p in promotionQuery on ss.ss_promo_sk equals p.p_promo_sk
                         group ss by new
                         {
                             product_name = i.i_product_name,
                             item_sk = i.i_item_sk,
                             store_name = s.s_store_name,
                             store_zip = s.s_zip,
                             b_street_number = ad1.ca_street_number,
                             b_street_name = ad1.ca_street_name,
                             b_city = ad1.ca_city,
                             b_zip = ad1.ca_zip,
                             c_street_number = ad2.ca_street_number,
                             c_street_name = ad2.ca_street_name,
                             c_city = ad2.ca_city,
                             c_zip = ad2.ca_zip,
                             syear = d1.d_year,
                             fsyear = d2.d_year,
                             s2year = d3.d_year
                         } into g
                         select new
                         {
                             g.Key.product_name,
                             g.Key.item_sk,
                             g.Key.store_name,
                             g.Key.store_zip,
                             g.Key.b_street_number,
                             g.Key.b_street_name,
                             g.Key.b_city,
                             g.Key.b_zip,
                             g.Key.c_street_number,
                             g.Key.c_street_name,
                             g.Key.c_city,
                             g.Key.c_zip,
                             g.Key.syear,
                             g.Key.fsyear,
                             g.Key.s2year,
                             cnt = (long)g.Count(),
                             s1 = g.Sum(x => x.ss_wholesale_cost),
                             s2 = g.Sum(x => x.ss_list_price),
                             s3 = g.Sum(x => x.ss_coupon_amt)
                         };

        var cs1Query = crossSales.Where(x => x.syear == 1999);
        var cs2Query = crossSales.Where(x => x.syear == 2000);

        var q64 = from cs1 in cs1Query
                  join cs2 in cs2Query on new { cs1.item_sk, cs1.store_name, cs1.store_zip }
                                   equals new { cs2.item_sk, cs2.store_name, cs2.store_zip }
                  where cs2.cnt <= cs1.cnt
                  orderby cs1.product_name, cs1.store_name, cs2.cnt, cs1.s1, cs2.s1
                  select new
                  {
                      cs1.product_name,
                      cs1.store_name,
                      cs1.store_zip,
                      cs1.b_street_number,
                      cs1.b_street_name,
                      cs1.b_city,
                      cs1.b_zip,
                      cs1.c_street_number,
                      cs1.c_street_name,
                      cs1.c_city,
                      cs1.c_zip,
                      cs1syear = cs1.syear,
                      cs1cnt = cs1.cnt,
                      s11 = cs1.s1,
                      s21 = cs1.s2,
                      s31 = cs1.s3,
                      s12 = cs2.s1,
                      s22 = cs2.s2,
                      s32 = cs2.s3,
                      cs2.syear,
                      cs2.cnt
                  };

        AssertPlan(q64, NativePushdown);

        var polarsResult = q64.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH cs_ui AS
              (SELECT cs_item_sk,
                      sum(cs_ext_list_price) AS sale,
                      sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) AS refund
               FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.CatalogReturnsPath}')
               WHERE cs_item_sk = cr_item_sk
                 AND cs_order_number = cr_order_number
               GROUP BY cs_item_sk
               HAVING sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
                 cross_sales AS
              (SELECT i_product_name product_name,
                      i_item_sk item_sk,
                      s_store_name store_name,
                      s_zip store_zip,
                      ad1.ca_street_number b_street_number,
                      ad1.ca_street_name b_street_name,
                      ad1.ca_city b_city,
                      ad1.ca_zip b_zip,
                      ad2.ca_street_number c_street_number,
                      ad2.ca_street_name c_street_name,
                      ad2.ca_city c_city,
                      ad2.ca_zip c_zip,
                      d1.d_year AS syear,
                      d2.d_year AS fsyear,
                      d3.d_year s2year,
                      count(*) cnt,
                      sum(ss_wholesale_cost) s1,
                      sum(ss_list_price) s2,
                      sum(ss_coupon_amt) s3
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.StoreReturnsPath}'),
                    cs_ui,
                    read_parquet('{TpcdsData.DateDimPath}') d1,
                    read_parquet('{TpcdsData.DateDimPath}') d2,
                    read_parquet('{TpcdsData.DateDimPath}') d3,
                    read_parquet('{TpcdsData.StorePath}'),
                    read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.CustomerDemographicsPath}') cd1,
                    read_parquet('{TpcdsData.CustomerDemographicsPath}') cd2,
                    read_parquet('{TpcdsData.PromotionPath}'),
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') hd1,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') hd2,
                    read_parquet('{TpcdsData.CustomerAddressPath}') ad1,
                    read_parquet('{TpcdsData.CustomerAddressPath}') ad2,
                    read_parquet('{TpcdsData.IncomeBandPath}') ib1,
                    read_parquet('{TpcdsData.IncomeBandPath}') ib2,
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE ss_store_sk = s_store_sk
                 AND ss_sold_date_sk = d1.d_date_sk
                 AND ss_customer_sk = c_customer_sk
                 AND ss_cdemo_sk= cd1.cd_demo_sk
                 AND ss_hdemo_sk = hd1.hd_demo_sk
                 AND ss_addr_sk = ad1.ca_address_sk
                 AND ss_item_sk = i_item_sk
                 AND ss_item_sk = sr_item_sk
                 AND ss_ticket_number = sr_ticket_number
                 AND ss_item_sk = cs_ui.cs_item_sk
                 AND c_current_cdemo_sk = cd2.cd_demo_sk
                 AND c_current_hdemo_sk = hd2.hd_demo_sk
                 AND c_current_addr_sk = ad2.ca_address_sk
                 AND c_first_sales_date_sk = d2.d_date_sk
                 AND c_first_shipto_date_sk = d3.d_date_sk
                 AND ss_promo_sk = p_promo_sk
                 AND hd1.hd_income_band_sk = ib1.ib_income_band_sk
                 AND hd2.hd_income_band_sk = ib2.ib_income_band_sk
                 AND cd1.cd_marital_status <> cd2.cd_marital_status
                 AND i_color IN ('purple',
                                 'burlywood',
                                 'indian',
                                 'spring',
                                 'floral',
                                 'medium')
                 AND i_current_price BETWEEN 64 AND 64 + 10
                 AND i_current_price BETWEEN 64 + 1 AND 64 + 15
               GROUP BY i_product_name,
                        i_item_sk,
                        s_store_name,
                        s_zip,
                        ad1.ca_street_number,
                        ad1.ca_street_name,
                        ad1.ca_city,
                        ad1.ca_zip,
                        ad2.ca_street_number,
                        ad2.ca_street_name,
                        ad2.ca_city,
                        ad2.ca_zip,
                        d1.d_year,
                        d2.d_year,
                        d3.d_year)
            SELECT cs1.product_name,
                   cs1.store_name,
                   cs1.store_zip,
                   cs1.b_street_number,
                   cs1.b_street_name,
                   cs1.b_city,
                   cs1.b_zip,
                   cs1.c_street_number,
                   cs1.c_street_name,
                   cs1.c_city,
                   cs1.c_zip,
                   cs1.syear cs1syear,
                   cs1.cnt cs1cnt,
                   cs1.s1 AS s11,
                   cs1.s2 AS s21,
                   cs1.s3 AS s31,
                   cs2.s1 AS s12,
                   cs2.s2 AS s22,
                   cs2.s3 AS s32,
                   cs2.syear,
                   cs2.cnt
            FROM cross_sales cs1,
                 cross_sales cs2
            WHERE cs1.item_sk=cs2.item_sk
              AND cs1.syear = 1999
              AND cs2.syear = 1999 + 1
              AND cs2.cnt <= cs1.cnt
              AND cs1.store_name = cs2.store_name
              AND cs1.store_zip = cs2.store_zip
            ORDER BY cs1.product_name,
                     cs1.store_name,
                     cs2.cnt,
                     cs1.s1,
                     cs2.s1;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            product_name = r.IsDBNull(0) ? null : r.GetString(0),
            store_name = r.IsDBNull(1) ? null : r.GetString(1),
            cnt = r.GetInt64(20)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
    }

    /*
     * TPC-DS Query 65
     *
     * SELECT s_store_name,
     *        i_item_desc,
     *        sc.revenue,
     *        i_current_price,
     *        i_wholesale_cost,
     *        i_brand
     * FROM store,
     *      item,
     *   (SELECT ss_store_sk,
     *           avg(revenue) AS ave
     *    FROM
     *      (SELECT ss_store_sk,
     *              ss_item_sk,
     *              sum(ss_sales_price) AS revenue
     *       FROM store_sales,
     *            date_dim
     *       WHERE ss_sold_date_sk = d_date_sk
     *         AND d_month_seq BETWEEN 1176 AND 1176+11
     *       GROUP BY ss_store_sk,
     *                ss_item_sk) sa
     *    GROUP BY ss_store_sk) sb,
     *   (SELECT ss_store_sk,
     *           ss_item_sk,
     *           sum(ss_sales_price) AS revenue
     *    FROM store_sales,
     *         date_dim
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND d_month_seq BETWEEN 1176 AND 1176+11
     *    GROUP BY ss_store_sk,
     *             ss_item_sk) sc
     * WHERE sb.ss_store_sk = sc.ss_store_sk
     *   AND sc.revenue <= 0.1 * sb.ave
     *   AND s_store_sk = sc.ss_store_sk
     *   AND i_item_sk = sc.ss_item_sk
     * ORDER BY s_store_name NULLS FIRST,
     *          i_item_desc NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q65()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var sc = from ss in storeSalesQuery
                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                 where d.d_month_seq >= 1176 && d.d_month_seq <= 1187
                 group ss by new { ss.ss_store_sk, ss.ss_item_sk } into g
                 select new
                 {
                     g.Key.ss_store_sk,
                     g.Key.ss_item_sk,
                     revenue = g.Sum(x => x.ss_sales_price)
                 };

        var sb = from sa in sc
                 group sa by sa.ss_store_sk into g
                 select new
                 {
                     ss_store_sk = g.Key,
                     ave = g.Average(x => (double?)x.revenue)
                 };

        var q65 = (from c in sc
                   join b in sb on c.ss_store_sk equals b.ss_store_sk
                   where (double?)c.revenue <= 0.1 * b.ave
                   join s in storeQuery on c.ss_store_sk equals s.s_store_sk
                   join i in itemQuery on c.ss_item_sk equals i.i_item_sk
                   orderby s.s_store_name, i.i_item_desc
                   select new
                   {
                       s.s_store_name,
                       i.i_item_desc,
                       c.revenue,
                       i.i_current_price,
                       i.i_wholesale_cost,
                       i.i_brand
                   })
                  .Take(100);

        AssertPlan(q65, NativePushdown);

        var polarsResult = q65.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT s_store_name,
                   i_item_desc,
                   sc.revenue,
                   i_current_price,
                   i_wholesale_cost,
                   i_brand
            FROM read_parquet('{TpcdsData.StorePath}') store,
                 read_parquet('{TpcdsData.ItemPath}') item,
              (SELECT ss_store_sk,
                      avg(revenue) AS ave
               FROM
                 (SELECT ss_store_sk,
                         ss_item_sk,
                         sum(ss_sales_price) AS revenue
                  FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                       read_parquet('{TpcdsData.DateDimPath}') date_dim
                  WHERE ss_sold_date_sk = d_date_sk
                    AND d_month_seq BETWEEN 1176 AND 1176+11
                  GROUP BY ss_store_sk,
                           ss_item_sk) sa
               GROUP BY ss_store_sk) sb,
              (SELECT ss_store_sk,
                      ss_item_sk,
                      sum(ss_sales_price) AS revenue
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE ss_sold_date_sk = d_date_sk
                 AND d_month_seq BETWEEN 1176 AND 1176+11
               GROUP BY ss_store_sk,
                        ss_item_sk) sc
            WHERE sb.ss_store_sk = sc.ss_store_sk
              AND sc.revenue <= 0.1 * sb.ave
              AND s_store_sk = sc.ss_store_sk
              AND i_item_sk = sc.ss_item_sk
            ORDER BY s_store_name NULLS FIRST,
                     i_item_desc NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            s_store_name = r.IsDBNull(0) ? null : r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            revenue = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2),
            i_current_price = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3),
            i_wholesale_cost = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4),
            i_brand = r.IsDBNull(5) ? null : r.GetString(5)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.s_store_name, p.s_store_name);
            Assert.Equal(d.i_item_desc, p.i_item_desc);
            Assert.Equal(d.revenue, p.revenue);
            Assert.Equal(d.i_current_price, p.i_current_price);
            Assert.Equal(d.i_wholesale_cost, p.i_wholesale_cost);
            Assert.Equal(d.i_brand, p.i_brand);
        }
    }

    /*
     * TPC-DS Query 66
     *
     * select
     *   w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country,
     *   ship_carriers, year_,
     *   sum(jan_sales) as jan_sales, ..., sum(dec_sales) as dec_sales,
     *   sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot, ...,
     *   sum(jan_net) as jan_net, ..., sum(dec_net) as dec_net
     * from (web_sales union all catalog_sales) x
     * group by w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year_
     * order by w_warehouse_name NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q66()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var timeDimQuery = TpcdsData.TimeDimQuery();
        var shipModeQuery = TpcdsData.ShipModeQuery();

        var carriers = new[] { "DHL", "BARIAN" };

        var webPart = from ws in webSalesQuery
                      join w in warehouseQuery on ws.ws_warehouse_sk equals w.w_warehouse_sk
                      join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                      join t in timeDimQuery on ws.ws_sold_time_sk equals t.t_time_sk
                      join sm in shipModeQuery on ws.ws_ship_mode_sk equals sm.sm_ship_mode_sk
                      where d.d_year == 2001
                         && t.t_time >= 30838 && t.t_time <= 30838 + 28800
                         && carriers.Contains(sm.sm_carrier)
                      group new { ws, d } by new
                      {
                          w.w_warehouse_name,
                          w.w_warehouse_sq_ft,
                          w.w_city,
                          w.w_county,
                          w.w_state,
                          w.w_country,
                          d.d_year
                      } into g
                      select new
                      {
                          g.Key.w_warehouse_name,
                          g.Key.w_warehouse_sq_ft,
                          g.Key.w_city,
                          g.Key.w_county,
                          g.Key.w_state,
                          g.Key.w_country,
                          ship_carriers = "DHL,BARIAN",
                          year_ = g.Key.d_year,
                          jan_sales = g.Sum(x => x.d.d_moy == 1 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          feb_sales = g.Sum(x => x.d.d_moy == 2 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          mar_sales = g.Sum(x => x.d.d_moy == 3 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          apr_sales = g.Sum(x => x.d.d_moy == 4 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          may_sales = g.Sum(x => x.d.d_moy == 5 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          jun_sales = g.Sum(x => x.d.d_moy == 6 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          jul_sales = g.Sum(x => x.d.d_moy == 7 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          aug_sales = g.Sum(x => x.d.d_moy == 8 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          sep_sales = g.Sum(x => x.d.d_moy == 9 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          oct_sales = g.Sum(x => x.d.d_moy == 10 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          nov_sales = g.Sum(x => x.d.d_moy == 11 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          dec_sales = g.Sum(x => x.d.d_moy == 12 ? x.ws.ws_ext_sales_price * x.ws.ws_quantity : 0.00m),
                          jan_net = g.Sum(x => x.d.d_moy == 1 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          feb_net = g.Sum(x => x.d.d_moy == 2 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          mar_net = g.Sum(x => x.d.d_moy == 3 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          apr_net = g.Sum(x => x.d.d_moy == 4 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          may_net = g.Sum(x => x.d.d_moy == 5 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          jun_net = g.Sum(x => x.d.d_moy == 6 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          jul_net = g.Sum(x => x.d.d_moy == 7 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          aug_net = g.Sum(x => x.d.d_moy == 8 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          sep_net = g.Sum(x => x.d.d_moy == 9 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          oct_net = g.Sum(x => x.d.d_moy == 10 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          nov_net = g.Sum(x => x.d.d_moy == 11 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m),
                          dec_net = g.Sum(x => x.d.d_moy == 12 ? x.ws.ws_net_paid * x.ws.ws_quantity : 0.00m)
                      };

        var catPart = from cs in catalogSalesQuery
                      join w in warehouseQuery on cs.cs_warehouse_sk equals w.w_warehouse_sk
                      join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                      join t in timeDimQuery on cs.cs_sold_time_sk equals t.t_time_sk
                      join sm in shipModeQuery on cs.cs_ship_mode_sk equals sm.sm_ship_mode_sk
                      where d.d_year == 2001
                         && t.t_time >= 30838 && t.t_time <= 30838 + 28800
                         && carriers.Contains(sm.sm_carrier)
                      group new { cs, d } by new
                      {
                          w.w_warehouse_name,
                          w.w_warehouse_sq_ft,
                          w.w_city,
                          w.w_county,
                          w.w_state,
                          w.w_country,
                          d.d_year
                      } into g
                      select new
                      {
                          g.Key.w_warehouse_name,
                          g.Key.w_warehouse_sq_ft,
                          g.Key.w_city,
                          g.Key.w_county,
                          g.Key.w_state,
                          g.Key.w_country,
                          ship_carriers = "DHL,BARIAN",
                          year_ = g.Key.d_year,
                          jan_sales = g.Sum(x => x.d.d_moy == 1 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          feb_sales = g.Sum(x => x.d.d_moy == 2 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          mar_sales = g.Sum(x => x.d.d_moy == 3 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          apr_sales = g.Sum(x => x.d.d_moy == 4 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          may_sales = g.Sum(x => x.d.d_moy == 5 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          jun_sales = g.Sum(x => x.d.d_moy == 6 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          jul_sales = g.Sum(x => x.d.d_moy == 7 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          aug_sales = g.Sum(x => x.d.d_moy == 8 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          sep_sales = g.Sum(x => x.d.d_moy == 9 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          oct_sales = g.Sum(x => x.d.d_moy == 10 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          nov_sales = g.Sum(x => x.d.d_moy == 11 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          dec_sales = g.Sum(x => x.d.d_moy == 12 ? x.cs.cs_sales_price * x.cs.cs_quantity : 0.00m),
                          jan_net = g.Sum(x => x.d.d_moy == 1 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          feb_net = g.Sum(x => x.d.d_moy == 2 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          mar_net = g.Sum(x => x.d.d_moy == 3 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          apr_net = g.Sum(x => x.d.d_moy == 4 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          may_net = g.Sum(x => x.d.d_moy == 5 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          jun_net = g.Sum(x => x.d.d_moy == 6 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          jul_net = g.Sum(x => x.d.d_moy == 7 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          aug_net = g.Sum(x => x.d.d_moy == 8 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          sep_net = g.Sum(x => x.d.d_moy == 9 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          oct_net = g.Sum(x => x.d.d_moy == 10 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          nov_net = g.Sum(x => x.d.d_moy == 11 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m),
                          dec_net = g.Sum(x => x.d.d_moy == 12 ? x.cs.cs_net_paid_inc_tax * x.cs.cs_quantity : 0.00m)
                      };

        var q66 = (from x in webPart.Concat(catPart)
                   group x by new
                   {
                       x.w_warehouse_name,
                       x.w_warehouse_sq_ft,
                       x.w_city,
                       x.w_county,
                       x.w_state,
                       x.w_country,
                       x.ship_carriers,
                       x.year_
                   } into g
                   orderby g.Key.w_warehouse_name
                   select new
                   {
                       g.Key.w_warehouse_name,
                       g.Key.w_warehouse_sq_ft,
                       g.Key.w_city,
                       g.Key.w_county,
                       g.Key.w_state,
                       g.Key.w_country,
                       g.Key.ship_carriers,
                       g.Key.year_,
                       jan_sales = g.Sum(x => x.jan_sales),
                       feb_sales = g.Sum(x => x.feb_sales),
                       mar_sales = g.Sum(x => x.mar_sales),
                       apr_sales = g.Sum(x => x.apr_sales),
                       may_sales = g.Sum(x => x.may_sales),
                       jun_sales = g.Sum(x => x.jun_sales),
                       jul_sales = g.Sum(x => x.jul_sales),
                       aug_sales = g.Sum(x => x.aug_sales),
                       sep_sales = g.Sum(x => x.sep_sales),
                       oct_sales = g.Sum(x => x.oct_sales),
                       nov_sales = g.Sum(x => x.nov_sales),
                       dec_sales = g.Sum(x => x.dec_sales),
                       jan_sales_per_sq_foot = g.Sum(x => (double?)x.jan_sales / (double?)x.w_warehouse_sq_ft),
                       feb_sales_per_sq_foot = g.Sum(x => (double?)x.feb_sales / (double?)x.w_warehouse_sq_ft),
                       mar_sales_per_sq_foot = g.Sum(x => (double?)x.mar_sales / (double?)x.w_warehouse_sq_ft),
                       apr_sales_per_sq_foot = g.Sum(x => (double?)x.apr_sales / (double?)x.w_warehouse_sq_ft),
                       may_sales_per_sq_foot = g.Sum(x => (double?)x.may_sales / (double?)x.w_warehouse_sq_ft),
                       jun_sales_per_sq_foot = g.Sum(x => (double?)x.jun_sales / (double?)x.w_warehouse_sq_ft),
                       jul_sales_per_sq_foot = g.Sum(x => (double?)x.jul_sales / (double?)x.w_warehouse_sq_ft),
                       aug_sales_per_sq_foot = g.Sum(x => (double?)x.aug_sales / (double?)x.w_warehouse_sq_ft),
                       sep_sales_per_sq_foot = g.Sum(x => (double?)x.sep_sales / (double?)x.w_warehouse_sq_ft),
                       oct_sales_per_sq_foot = g.Sum(x => (double?)x.oct_sales / (double?)x.w_warehouse_sq_ft),
                       nov_sales_per_sq_foot = g.Sum(x => (double?)x.nov_sales / (double?)x.w_warehouse_sq_ft),
                       dec_sales_per_sq_foot = g.Sum(x => (double?)x.dec_sales / (double?)x.w_warehouse_sq_ft),
                       jan_net = g.Sum(x => x.jan_net),
                       feb_net = g.Sum(x => x.feb_net),
                       mar_net = g.Sum(x => x.mar_net),
                       apr_net = g.Sum(x => x.apr_net),
                       may_net = g.Sum(x => x.may_net),
                       jun_net = g.Sum(x => x.jun_net),
                       jul_net = g.Sum(x => x.jul_net),
                       aug_net = g.Sum(x => x.aug_net),
                       sep_net = g.Sum(x => x.sep_net),
                       oct_net = g.Sum(x => x.oct_net),
                       nov_net = g.Sum(x => x.nov_net),
                       dec_net = g.Sum(x => x.dec_net)
                   })
                  .Take(100);

        AssertPlan(q66, NativePushdown);

        var polarsResult = q66.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            select
                     w_warehouse_name
              ,w_warehouse_sq_ft
              ,w_city
              ,w_county
              ,w_state
              ,w_country
              ,ship_carriers
              ,year_
              ,sum(jan_sales) as jan_sales
              ,sum(feb_sales) as feb_sales
              ,sum(mar_sales) as mar_sales
              ,sum(apr_sales) as apr_sales
              ,sum(may_sales) as may_sales
              ,sum(jun_sales) as jun_sales
              ,sum(jul_sales) as jul_sales
              ,sum(aug_sales) as aug_sales
              ,sum(sep_sales) as sep_sales
              ,sum(oct_sales) as oct_sales
              ,sum(nov_sales) as nov_sales
              ,sum(dec_sales) as dec_sales
              ,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
              ,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
              ,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
              ,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
              ,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
              ,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
              ,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
              ,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
              ,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
              ,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
              ,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
              ,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
              ,sum(jan_net) as jan_net
              ,sum(feb_net) as feb_net
              ,sum(mar_net) as mar_net
              ,sum(apr_net) as apr_net
              ,sum(may_net) as may_net
              ,sum(jun_net) as jun_net
              ,sum(jul_net) as jul_net
              ,sum(aug_net) as aug_net
              ,sum(sep_net) as sep_net
              ,sum(oct_net) as oct_net
              ,sum(nov_net) as nov_net
              ,sum(dec_net) as dec_net
             from (
                 select
              w_warehouse_name
              ,w_warehouse_sq_ft
              ,w_city
              ,w_county
              ,w_state
              ,w_country
              ,'DHL,BARIAN' as ship_carriers
              ,d_year as year_
              ,sum(case when d_moy = 1 then ws_ext_sales_price* ws_quantity else 0 end) as jan_sales
              ,sum(case when d_moy = 2 then ws_ext_sales_price* ws_quantity else 0 end) as feb_sales
              ,sum(case when d_moy = 3 then ws_ext_sales_price* ws_quantity else 0 end) as mar_sales
              ,sum(case when d_moy = 4 then ws_ext_sales_price* ws_quantity else 0 end) as apr_sales
              ,sum(case when d_moy = 5 then ws_ext_sales_price* ws_quantity else 0 end) as may_sales
              ,sum(case when d_moy = 6 then ws_ext_sales_price* ws_quantity else 0 end) as jun_sales
              ,sum(case when d_moy = 7 then ws_ext_sales_price* ws_quantity else 0 end) as jul_sales
              ,sum(case when d_moy = 8 then ws_ext_sales_price* ws_quantity else 0 end) as aug_sales
              ,sum(case when d_moy = 9 then ws_ext_sales_price* ws_quantity else 0 end) as sep_sales
              ,sum(case when d_moy = 10 then ws_ext_sales_price* ws_quantity else 0 end) as oct_sales
              ,sum(case when d_moy = 11 then ws_ext_sales_price* ws_quantity else 0 end) as nov_sales
              ,sum(case when d_moy = 12 then ws_ext_sales_price* ws_quantity else 0 end) as dec_sales
              ,sum(case when d_moy = 1 then ws_net_paid * ws_quantity else 0 end) as jan_net
              ,sum(case when d_moy = 2 then ws_net_paid * ws_quantity else 0 end) as feb_net
              ,sum(case when d_moy = 3 then ws_net_paid * ws_quantity else 0 end) as mar_net
              ,sum(case when d_moy = 4 then ws_net_paid * ws_quantity else 0 end) as apr_net
              ,sum(case when d_moy = 5 then ws_net_paid * ws_quantity else 0 end) as may_net
              ,sum(case when d_moy = 6 then ws_net_paid * ws_quantity else 0 end) as jun_net
              ,sum(case when d_moy = 7 then ws_net_paid * ws_quantity else 0 end) as jul_net
              ,sum(case when d_moy = 8 then ws_net_paid * ws_quantity else 0 end) as aug_net
              ,sum(case when d_moy = 9 then ws_net_paid * ws_quantity else 0 end) as sep_net
              ,sum(case when d_moy = 10 then ws_net_paid * ws_quantity else 0 end) as oct_net
              ,sum(case when d_moy = 11 then ws_net_paid * ws_quantity else 0 end) as nov_net
              ,sum(case when d_moy = 12 then ws_net_paid * ws_quantity else 0 end) as dec_net
                 from
                      read_parquet('{TpcdsData.WebSalesPath}') web_sales
                     ,read_parquet('{TpcdsData.WarehousePath}') warehouse
                     ,read_parquet('{TpcdsData.DateDimPath}') date_dim
                     ,read_parquet('{TpcdsData.TimeDimPath}') time_dim
                     ,read_parquet('{TpcdsData.ShipModePath}') ship_mode
                 where
                        ws_warehouse_sk =  w_warehouse_sk
                    and ws_sold_date_sk = d_date_sk
                    and ws_sold_time_sk = t_time_sk
                    and ws_ship_mode_sk = sm_ship_mode_sk
                    and d_year = 2001
                    and t_time between 30838 and 30838+28800
                    and sm_carrier in ('DHL','BARIAN')
                 group by
                    w_warehouse_name
              ,w_warehouse_sq_ft
              ,w_city
              ,w_county
              ,w_state
              ,w_country
              ,d_year
             union all
                 select
              w_warehouse_name
              ,w_warehouse_sq_ft
              ,w_city
              ,w_county
              ,w_state
              ,w_country
              ,'DHL,BARIAN' as ship_carriers
              ,d_year as year_
              ,sum(case when d_moy = 1 then cs_sales_price* cs_quantity else 0 end) as jan_sales
              ,sum(case when d_moy = 2 then cs_sales_price* cs_quantity else 0 end) as feb_sales
              ,sum(case when d_moy = 3 then cs_sales_price* cs_quantity else 0 end) as mar_sales
              ,sum(case when d_moy = 4 then cs_sales_price* cs_quantity else 0 end) as apr_sales
              ,sum(case when d_moy = 5 then cs_sales_price* cs_quantity else 0 end) as may_sales
              ,sum(case when d_moy = 6 then cs_sales_price* cs_quantity else 0 end) as jun_sales
              ,sum(case when d_moy = 7 then cs_sales_price* cs_quantity else 0 end) as jul_sales
              ,sum(case when d_moy = 8 then cs_sales_price* cs_quantity else 0 end) as aug_sales
              ,sum(case when d_moy = 9 then cs_sales_price* cs_quantity else 0 end) as sep_sales
              ,sum(case when d_moy = 10 then cs_sales_price* cs_quantity else 0 end) as oct_sales
              ,sum(case when d_moy = 11 then cs_sales_price* cs_quantity else 0 end) as nov_sales
              ,sum(case when d_moy = 12 then cs_sales_price* cs_quantity else 0 end) as dec_sales
              ,sum(case when d_moy = 1 then cs_net_paid_inc_tax * cs_quantity else 0 end) as jan_net
              ,sum(case when d_moy = 2 then cs_net_paid_inc_tax * cs_quantity else 0 end) as feb_net
              ,sum(case when d_moy = 3 then cs_net_paid_inc_tax * cs_quantity else 0 end) as mar_net
              ,sum(case when d_moy = 4 then cs_net_paid_inc_tax * cs_quantity else 0 end) as apr_net
              ,sum(case when d_moy = 5 then cs_net_paid_inc_tax * cs_quantity else 0 end) as may_net
              ,sum(case when d_moy = 6 then cs_net_paid_inc_tax * cs_quantity else 0 end) as jun_net
              ,sum(case when d_moy = 7 then cs_net_paid_inc_tax * cs_quantity else 0 end) as jul_net
              ,sum(case when d_moy = 8 then cs_net_paid_inc_tax * cs_quantity else 0 end) as aug_net
              ,sum(case when d_moy = 9 then cs_net_paid_inc_tax * cs_quantity else 0 end) as sep_net
              ,sum(case when d_moy = 10 then cs_net_paid_inc_tax * cs_quantity else 0 end) as oct_net
              ,sum(case when d_moy = 11 then cs_net_paid_inc_tax * cs_quantity else 0 end) as nov_net
              ,sum(case when d_moy = 12 then cs_net_paid_inc_tax * cs_quantity else 0 end) as dec_net
                 from
                      read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales
                     ,read_parquet('{TpcdsData.WarehousePath}') warehouse
                     ,read_parquet('{TpcdsData.DateDimPath}') date_dim
                     ,read_parquet('{TpcdsData.TimeDimPath}') time_dim
                     ,read_parquet('{TpcdsData.ShipModePath}') ship_mode
                 where
                        cs_warehouse_sk =  w_warehouse_sk
                    and cs_sold_date_sk = d_date_sk
                    and cs_sold_time_sk = t_time_sk
                    and cs_ship_mode_sk = sm_ship_mode_sk
                    and d_year = 2001
                    and t_time between 30838 AND 30838+28800
                    and sm_carrier in ('DHL','BARIAN')
                 group by
                    w_warehouse_name
              ,w_warehouse_sq_ft
              ,w_city
              ,w_county
              ,w_state
              ,w_country
              ,d_year
             ) x
             group by
                    w_warehouse_name
              ,w_warehouse_sq_ft
              ,w_city
              ,w_county
              ,w_state
              ,w_country
              ,ship_carriers
              ,year_
             order by w_warehouse_name NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            w_warehouse_name = r.IsDBNull(0) ? null : r.GetString(0),
            w_warehouse_sq_ft = r.IsDBNull(1) ? (long?)null : r.GetInt64(1),
            jan_sales = r.IsDBNull(8) ? (decimal?)null : r.GetDecimal(8),
            jan_sales_per_sq_foot = r.IsDBNull(20) ? (double?)null : (double)r.GetDecimal(20),
            jan_net = r.IsDBNull(32) ? (decimal?)null : r.GetDecimal(32)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.w_warehouse_name, p.w_warehouse_name);
            Assert.Equal(d.w_warehouse_sq_ft, p.w_warehouse_sq_ft);
            Assert.Equal(d.jan_sales, p.jan_sales);
            Assert.Equal(d.jan_sales_per_sq_foot ?? 0.0, p.jan_sales_per_sq_foot ?? 0.0, 4);
            Assert.Equal(d.jan_net, p.jan_net);
        }
    }

    /*
     * TPC-DS Query 67
     *
     * SELECT *
     * FROM
     *   (SELECT i_category,
     *           i_class,
     *           i_brand,
     *           i_product_name,
     *           d_year,
     *           d_qoy,
     *           d_moy,
     *           s_store_id,
     *           sumsales,
     *           rank() OVER (PARTITION BY i_category
     *                        ORDER BY sumsales DESC) rk
     *    FROM
     *      (SELECT i_category,
     *              i_class,
     *              i_brand,
     *              i_product_name,
     *              d_year,
     *              d_qoy,
     *              d_moy,
     *              s_store_id,
     *              sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
     *       FROM store_sales,
     *            date_dim,
     *            store,
     *            item
     *       WHERE ss_sold_date_sk=d_date_sk
     *         AND ss_item_sk=i_item_sk
     *         AND ss_store_sk = s_store_sk
     *         AND d_month_seq BETWEEN 1200 AND 1200+11
     *       GROUP BY rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
     * WHERE rk <= 100
     * ORDER BY i_category NULLS FIRST,
     *          i_class NULLS FIRST,
     *          i_brand NULLS FIRST,
     *          i_product_name NULLS FIRST,
     *          d_year NULLS FIRST,
     *          d_qoy NULLS FIRST,
     *          d_moy NULLS FIRST,
     *          s_store_id NULLS FIRST,
     *          sumsales NULLS FIRST,
     *          rk NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q67()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var joined = from ss in storeSalesQuery
                     join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                     join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                     join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                     where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                     select new
                     {
                         i.i_category,
                         i.i_class,
                         i.i_brand,
                         i.i_product_name,
                         d.d_year,
                         d.d_qoy,
                         d.d_moy,
                         s.s_store_id,
                         line_sales = (ss.ss_sales_price * ss.ss_quantity) ?? 0.00m
                     };

        var dw1 = joined.GroupByRollup(
            x => new
            {
                x.i_category,
                x.i_class,
                x.i_brand,
                x.i_product_name,
                x.d_year,
                x.d_qoy,
                x.d_moy,
                x.s_store_id
            },
            (key, g) => new
            {
                key.i_category,
                key.i_class,
                key.i_brand,
                key.i_product_name,
                key.d_year,
                key.d_qoy,
                key.d_moy,
                key.s_store_id,
                sumsales = g.Sum(x => x.line_sales)
            }
        );

        var dw2 = from x in dw1
                  select new
                  {
                      x.i_category,
                      x.i_class,
                      x.i_brand,
                      x.i_product_name,
                      x.d_year,
                      x.d_qoy,
                      x.d_moy,
                      x.s_store_id,
                      x.sumsales,
                      rk = (long)x.sumsales.Rank(RankMethod.Min, true).Over(x.i_category)
                  };

        var q67 = (from x in dw2
                   where x.rk <= 100
                   orderby x.i_category, x.i_class, x.i_brand, x.i_product_name,
                           x.d_year, x.d_qoy, x.d_moy, x.s_store_id,
                           x.sumsales, x.rk
                   select x)
                  .Take(100);

        AssertPlan(q67, NativePushdown);

        var polarsResult = q67.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT *
            FROM
              (SELECT i_category,
                      i_class,
                      i_brand,
                      i_product_name,
                      d_year,
                      d_qoy,
                      d_moy,
                      s_store_id,
                      sumsales,
                      rank() OVER (PARTITION BY i_category
                                   ORDER BY sumsales DESC) rk
               FROM
                 (SELECT i_category,
                         i_class,
                         i_brand,
                         i_product_name,
                         d_year,
                         d_qoy,
                         d_moy,
                         s_store_id,
                         sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
                  FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                       read_parquet('{TpcdsData.DateDimPath}'),
                       read_parquet('{TpcdsData.StorePath}'),
                       read_parquet('{TpcdsData.ItemPath}')
                  WHERE ss_sold_date_sk=d_date_sk
                    AND ss_item_sk=i_item_sk
                    AND ss_store_sk = s_store_sk
                    AND d_month_seq BETWEEN 1200 AND 1200+11
                  GROUP BY rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
            WHERE rk <= 100
            ORDER BY i_category NULLS FIRST,
                     i_class NULLS FIRST,
                     i_brand NULLS FIRST,
                     i_product_name NULLS FIRST,
                     d_year NULLS FIRST,
                     d_qoy NULLS FIRST,
                     d_moy NULLS FIRST,
                     s_store_id NULLS FIRST,
                     sumsales NULLS FIRST,
                     rk NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_category = r.IsDBNull(0) ? null : r.GetString(0),
            i_class = r.IsDBNull(1) ? null : r.GetString(1),
            i_brand = r.IsDBNull(2) ? null : r.GetString(2),
            i_product_name = r.IsDBNull(3) ? null : r.GetString(3),
            d_year = r.IsDBNull(4) ? (long?)null : r.GetInt64(4),
            d_qoy = r.IsDBNull(5) ? (long?)null : r.GetInt64(5),
            d_moy = r.IsDBNull(6) ? (long?)null : r.GetInt64(6),
            s_store_id = r.IsDBNull(7) ? null : r.GetString(7),
            sumsales = r.GetDecimal(8),
            rk = r.GetInt64(9)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.i_category, p.i_category);
            Assert.Equal(d.i_class, p.i_class);
            Assert.Equal(d.i_brand, p.i_brand);
            Assert.Equal(d.i_product_name, p.i_product_name);
            Assert.Equal(d.d_year, p.d_year);
            Assert.Equal(d.d_qoy, p.d_qoy);
            Assert.Equal(d.d_moy, p.d_moy);
            Assert.Equal(d.s_store_id, p.s_store_id);
            Assert.Equal(d.sumsales, p.sumsales);
            Assert.Equal(d.rk, p.rk);
        }
    }

    /*
     * TPC-DS Query 68
     *
     * SELECT c_last_name,
     *        c_first_name,
     *        ca_city,
     *        bought_city,
     *        ss_ticket_number,
     *        extended_price,
     *        extended_tax,
     *        list_price
     * FROM
     *   (SELECT ss_ticket_number,
     *           ss_customer_sk,
     *           ca_city bought_city,
     *           sum(ss_ext_sales_price) extended_price,
     *           sum(ss_ext_list_price) list_price,
     *           sum(ss_ext_tax) extended_tax
     *    FROM store_sales,
     *         date_dim,
     *         store,
     *         household_demographics,
     *         customer_address
     *    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     *      AND store_sales.ss_store_sk = store.s_store_sk
     *      AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     *      AND store_sales.ss_addr_sk = customer_address.ca_address_sk
     *      AND date_dim.d_dom BETWEEN 1 AND 2
     *      AND (household_demographics.hd_dep_count = 4
     *           OR household_demographics.hd_vehicle_count= 3)
     *      AND date_dim.d_year IN (1999,
     *                              1999+1,
     *                              1999+2)
     *      AND store.s_city IN ('Fairview',
     *                           'Midway')
     *    GROUP BY ss_ticket_number,
     *             ss_customer_sk,
     *             ss_addr_sk,
     *             ca_city) dn,
     *      customer,
     *      customer_address current_addr
     * WHERE ss_customer_sk = c_customer_sk
     *   AND customer.c_current_addr_sk = current_addr.ca_address_sk
     *   AND current_addr.ca_city <> bought_city
     * ORDER BY c_last_name NULLS FIRST,
     *          ss_ticket_number NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q68()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var householdDemographicsQuery = TpcdsData.HouseholdDemographicsQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        long[] years = [1999, 2000, 2001];
        string[] cities = ["Fairview", "Midway"];

        var dn = from ss in storeSalesQuery
                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                 join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                 join hd in householdDemographicsQuery on ss.ss_hdemo_sk equals hd.hd_demo_sk
                 join ca in customerAddressQuery on ss.ss_addr_sk equals ca.ca_address_sk
                 where d.d_dom >= 1 && d.d_dom <= 2
                    && (hd.hd_dep_count == 4 || hd.hd_vehicle_count == 3)
                    && years.Contains(d.d_year!.Value)
                    && cities.Contains(s.s_city)
                 group ss by new
                 {
                     ss.ss_ticket_number,
                     ss.ss_customer_sk,
                     ss.ss_addr_sk,
                     bought_city = ca.ca_city
                 } into g
                 select new
                 {
                     g.Key.ss_ticket_number,
                     g.Key.ss_customer_sk,
                     g.Key.bought_city,
                     extended_price = g.Sum(x => x.ss_ext_sales_price),
                     list_price = g.Sum(x => x.ss_ext_list_price),
                     extended_tax = g.Sum(x => x.ss_ext_tax)
                 };

        var q68 = (from x in dn
                   join c in customerQuery on x.ss_customer_sk equals c.c_customer_sk
                   join current_addr in customerAddressQuery on c.c_current_addr_sk equals current_addr.ca_address_sk
                   where current_addr.ca_city != x.bought_city
                   orderby c.c_last_name, x.ss_ticket_number
                   select new
                   {
                       c.c_last_name,
                       c.c_first_name,
                       current_addr.ca_city,
                       x.bought_city,
                       x.ss_ticket_number,
                       x.extended_price,
                       x.extended_tax,
                       x.list_price
                   })
                  .Take(100);

        AssertPlan(q68, NativePushdown);

        var polarsResult = q68.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT c_last_name,
                   c_first_name,
                   ca_city,
                   bought_city,
                   ss_ticket_number,
                   extended_price,
                   extended_tax,
                   list_price
            FROM
              (SELECT ss_ticket_number,
                      ss_customer_sk,
                      ca_city bought_city,
                      sum(ss_ext_sales_price) extended_price,
                      sum(ss_ext_list_price) list_price,
                      sum(ss_ext_tax) extended_tax
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.CustomerAddressPath}') customer_address
               WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
                 AND store_sales.ss_store_sk = store.s_store_sk
                 AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND store_sales.ss_addr_sk = customer_address.ca_address_sk
                 AND date_dim.d_dom BETWEEN 1 AND 2
                 AND (household_demographics.hd_dep_count = 4
                      OR household_demographics.hd_vehicle_count= 3)
                 AND date_dim.d_year IN (1999,
                                         1999+1,
                                         1999+2)
                 AND store.s_city IN ('Fairview',
                                      'Midway')
               GROUP BY ss_ticket_number,
                        ss_customer_sk,
                        ss_addr_sk,
                        ca_city) dn,
                 read_parquet('{TpcdsData.CustomerPath}') customer,
                 read_parquet('{TpcdsData.CustomerAddressPath}') current_addr
            WHERE ss_customer_sk = c_customer_sk
              AND customer.c_current_addr_sk = current_addr.ca_address_sk
              AND current_addr.ca_city <> bought_city
            ORDER BY c_last_name NULLS FIRST,
                     ss_ticket_number NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            c_last_name = r.IsDBNull(0) ? null : r.GetString(0),
            c_first_name = r.IsDBNull(1) ? null : r.GetString(1),
            ca_city = r.IsDBNull(2) ? null : r.GetString(2),
            bought_city = r.IsDBNull(3) ? null : r.GetString(3),
            ss_ticket_number = r.GetInt64(4),
            extended_price = r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5),
            extended_tax = r.IsDBNull(6) ? (decimal?)null : r.GetDecimal(6),
            list_price = r.IsDBNull(7) ? (decimal?)null : r.GetDecimal(7)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.c_last_name, p.c_last_name);
            Assert.Equal(d.c_first_name, p.c_first_name);
            Assert.Equal(d.ca_city, p.ca_city);
            Assert.Equal(d.bought_city, p.bought_city);
            Assert.Equal(d.ss_ticket_number, p.ss_ticket_number);
            Assert.Equal(d.extended_price, p.extended_price);
            Assert.Equal(d.extended_tax, p.extended_tax);
            Assert.Equal(d.list_price, p.list_price);
        }
    }

    /*
     * TPC-DS Query 69
     *
     * SELECT cd_gender,
     *        cd_marital_status,
     *        cd_education_status,
     *        count(*) cnt1,
     *        cd_purchase_estimate,
     *        count(*) cnt2,
     *        cd_credit_rating,
     *        count(*) cnt3
     * FROM customer c,
     *      customer_address ca,
     *      customer_demographics
     * WHERE c.c_current_addr_sk = ca.ca_address_sk
     *   AND ca_state IN ('KY',
     *                    'GA',
     *                    'NM')
     *   AND cd_demo_sk = c.c_current_cdemo_sk
     *   AND EXISTS
     *     (SELECT *
     *      FROM store_sales,
     *           date_dim
     *      WHERE c.c_customer_sk = ss_customer_sk
     *        AND ss_sold_date_sk = d_date_sk
     *        AND d_year = 2001
     *        AND d_moy BETWEEN 4 AND 4+2)
     *   AND (NOT EXISTS
     *          (SELECT *
     *           FROM web_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = ws_bill_customer_sk
     *             AND ws_sold_date_sk = d_date_sk
     *             AND d_year = 2001
     *             AND d_moy BETWEEN 4 AND 4+2)
     *        AND NOT EXISTS
     *          (SELECT *
     *           FROM catalog_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = cs_ship_customer_sk
     *             AND cs_sold_date_sk = d_date_sk
     *             AND d_year = 2001
     *             AND d_moy BETWEEN 4 AND 4+2))
     * GROUP BY cd_gender,
     *          cd_marital_status,
     *          cd_education_status,
     *          cd_purchase_estimate,
     *          cd_credit_rating
     * ORDER BY cd_gender,
     *          cd_marital_status,
     *          cd_education_status,
     *          cd_purchase_estimate,
     *          cd_credit_rating
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q69()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var states = new[] { "KY", "GA", "NM" };
        var targetDates = dateDimQuery.Where(d => d.d_year == 2001 && d.d_moy >= 4 && d.d_moy <= 6);

        var storeSalesInPeriod = from ss in storeSalesQuery
                                 join d in targetDates on ss.ss_sold_date_sk equals d.d_date_sk
                                 select ss;

        var webSalesInPeriod = from ws in webSalesQuery
                               join d in targetDates on ws.ws_sold_date_sk equals d.d_date_sk
                               select ws;

        var catSalesInPeriod = from cs in catalogSalesQuery
                               join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                               select cs;

        var q69 = (from c in customerQuery
                   join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                   where states.Contains(ca.ca_state)
                   join cd in customerDemographicsQuery on c.c_current_cdemo_sk equals cd.cd_demo_sk
                   where storeSalesInPeriod.Any(ss => ss.ss_customer_sk == c.c_customer_sk)
                      && !webSalesInPeriod.Any(ws => ws.ws_bill_customer_sk == c.c_customer_sk)
                      && !catSalesInPeriod.Any(cs => cs.cs_ship_customer_sk == c.c_customer_sk)
                   group c by new
                   {
                       cd.cd_gender,
                       cd.cd_marital_status,
                       cd.cd_education_status,
                       cd.cd_purchase_estimate,
                       cd.cd_credit_rating
                   } into g
                   let cnt = (long)g.Count()
                   orderby g.Key.cd_gender,
                           g.Key.cd_marital_status,
                           g.Key.cd_education_status,
                           g.Key.cd_purchase_estimate,
                           g.Key.cd_credit_rating
                   select new
                   {
                       g.Key.cd_gender,
                       g.Key.cd_marital_status,
                       g.Key.cd_education_status,
                       cnt1 = cnt,
                       g.Key.cd_purchase_estimate,
                       cnt2 = cnt,
                       g.Key.cd_credit_rating,
                       cnt3 = cnt
                   })
                  .Take(100);

        AssertPlan(q69, NativePushdown);

        var polarsResult = q69.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT cd_gender,
                   cd_marital_status,
                   cd_education_status,
                   count(*) cnt1,
                   cd_purchase_estimate,
                   count(*) cnt2,
                   cd_credit_rating,
                   count(*) cnt3
            FROM read_parquet('{TpcdsData.CustomerPath}') c,
                 read_parquet('{TpcdsData.CustomerAddressPath}') ca,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}')
            WHERE c.c_current_addr_sk = ca.ca_address_sk
              AND ca_state IN ('KY',
                               'GA',
                               'NM')
              AND cd_demo_sk = c.c_current_cdemo_sk
              AND EXISTS
                (SELECT *
                 FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                      read_parquet('{TpcdsData.DateDimPath}')
                 WHERE c.c_customer_sk = ss_customer_sk
                   AND ss_sold_date_sk = d_date_sk
                   AND d_year = 2001
                   AND d_moy BETWEEN 4 AND 4+2)
              AND (NOT EXISTS
                     (SELECT *
                      FROM read_parquet('{TpcdsData.WebSalesPath}'),
                           read_parquet('{TpcdsData.DateDimPath}')
                      WHERE c.c_customer_sk = ws_bill_customer_sk
                        AND ws_sold_date_sk = d_date_sk
                        AND d_year = 2001
                        AND d_moy BETWEEN 4 AND 4+2)
                   AND NOT EXISTS
                     (SELECT *
                      FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                           read_parquet('{TpcdsData.DateDimPath}')
                      WHERE c.c_customer_sk = cs_ship_customer_sk
                        AND cs_sold_date_sk = d_date_sk
                        AND d_year = 2001
                        AND d_moy BETWEEN 4 AND 4+2))
            GROUP BY cd_gender,
                     cd_marital_status,
                     cd_education_status,
                     cd_purchase_estimate,
                     cd_credit_rating
            ORDER BY cd_gender,
                     cd_marital_status,
                     cd_education_status,
                     cd_purchase_estimate,
                     cd_credit_rating
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            cd_gender = r.IsDBNull(0) ? null : r.GetString(0),
            cd_marital_status = r.IsDBNull(1) ? null : r.GetString(1),
            cd_education_status = r.IsDBNull(2) ? null : r.GetString(2),
            cnt1 = r.GetInt64(3),
            cd_purchase_estimate = r.IsDBNull(4) ? (long?)null : r.GetInt64(4),
            cnt2 = r.GetInt64(5),
            cd_credit_rating = r.IsDBNull(6) ? null : r.GetString(6),
            cnt3 = r.GetInt64(7)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.cd_gender, p.cd_gender);
            Assert.Equal(d.cd_marital_status, p.cd_marital_status);
            Assert.Equal(d.cd_education_status, p.cd_education_status);
            Assert.Equal(d.cnt1, p.cnt1);
            Assert.Equal(d.cd_purchase_estimate, p.cd_purchase_estimate);
            Assert.Equal(d.cnt2, p.cnt2);
            Assert.Equal(d.cd_credit_rating, p.cd_credit_rating);
            Assert.Equal(d.cnt3, p.cnt3);
        }
    }

    /*
     * TPC-DS Query 70
     *
     * SELECT sum(ss_net_profit) AS total_sum,
     *        s_state,
     *        s_county,
     *        grouping(s_state)+grouping(s_county) AS lochierarchy,
     *        rank() OVER (PARTITION BY grouping(s_state)+grouping(s_county),
     *                                  CASE
     *                                      WHEN grouping(s_county) = 0 THEN s_state
     *                                  END
     *                     ORDER BY sum(ss_net_profit) DESC) AS rank_within_parent
     * FROM store_sales,
     *      date_dim d1,
     *      store
     * WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
     *   AND d1.d_date_sk = ss_sold_date_sk
     *   AND s_store_sk = ss_store_sk
     *   AND s_state IN
     *     (SELECT s_state
     *      FROM
     *        (SELECT s_state AS s_state,
     *                rank() OVER (PARTITION BY s_state
     *                             ORDER BY sum(ss_net_profit) DESC) AS ranking
     *         FROM store_sales,
     *              store,
     *              date_dim
     *         WHERE d_month_seq BETWEEN 1200 AND 1200+11
     *           AND d_date_sk = ss_sold_date_sk
     *           AND s_store_sk = ss_store_sk
     *         GROUP BY s_state) tmp1
     *      WHERE ranking <= 5 )
     * GROUP BY rollup(s_state,s_county)
     * ORDER BY lochierarchy DESC ,
     *          CASE
     *              WHEN grouping(s_state)+grouping(s_county) = 0 THEN s_state
     *          END ,
     *          rank_within_parent
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q70()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_month_seq >= 1200 && d.d_month_seq <= 1211);

        var topStates = from ss in storeSalesQuery
                        join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                        join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                        group ss by s.s_state into g
                        let ranking = (long)g.Sum(x => x.ss_net_profit).Rank(RankMethod.Min, true).Over(g.Key)
                        where ranking <= 5
                        select new { s_state = g.Key };


        var baseSales = from ss in storeSalesQuery
                        join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                        join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                        join ts in topStates on s.s_state equals ts.s_state
                        select new
                        {
                            s.s_state,
                            s.s_county,
                            ss.ss_net_profit
                        };

        var level0 = from x in baseSales
                     group x by new { x.s_state, x.s_county } into g
                     select new
                     {
                         total_sum = g.Sum(x => x.ss_net_profit),
                         g.Key.s_state,
                         g.Key.s_county,
                         g_county = 0,
                         lochierarchy = 0
                     };

        var level1 = from x in baseSales
                     group x by x.s_state into g
                     select new
                     {
                         total_sum = g.Sum(x => x.ss_net_profit),
                         s_state = g.Key,
                         s_county = (string?)null,
                         g_county = 1,
                         lochierarchy = 1
                     };

        var level2 = from x in baseSales
                     group x by 1 into g
                     select new
                     {
                         total_sum = g.Sum(x => x.ss_net_profit),
                         s_state = (string?)null,
                         s_county = (string?)null,
                         g_county = 1,
                         lochierarchy = 2
                     };

        var rollup = level0.Concat(level1).Concat(level2);

        var q70 = (from r in rollup
                   select new
                   {
                       r.total_sum,
                       r.s_state,
                       r.s_county,
                       r.lochierarchy,
                       rank_within_parent = (long)r.total_sum.Rank(RankMethod.Min, true).Over(new { r.lochierarchy, parent = r.g_county == 0 ? r.s_state : null })
                   } into res
                   orderby res.lochierarchy descending,
                           res.lochierarchy == 0 ? res.s_state : null,
                           res.rank_within_parent
                   select new
                   {
                       res.total_sum,
                       res.s_state,
                       res.s_county,
                       res.lochierarchy,
                       res.rank_within_parent
                   })
                  .Take(100);

        AssertPlan(q70, NativePushdown);

        var polarsResult = q70.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT sum(ss_net_profit) AS total_sum,
                   s_state,
                   s_county,
                   grouping(s_state)+grouping(s_county) AS lochierarchy,
                   rank() OVER (PARTITION BY grouping(s_state)+grouping(s_county),
                                             CASE
                                                 WHEN grouping(s_county) = 0 THEN s_state
                                             END
                                ORDER BY sum(ss_net_profit) DESC) AS rank_within_parent
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.DateDimPath}') d1,
                 read_parquet('{TpcdsData.StorePath}')
            WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
              AND d1.d_date_sk = ss_sold_date_sk
              AND s_store_sk = ss_store_sk
              AND s_state IN
                (SELECT s_state
                 FROM
                   (SELECT s_state AS s_state,
                           rank() OVER (PARTITION BY s_state
                                        ORDER BY sum(ss_net_profit) DESC) AS ranking
                    FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                         read_parquet('{TpcdsData.StorePath}'),
                         read_parquet('{TpcdsData.DateDimPath}')
                    WHERE d_month_seq BETWEEN 1200 AND 1200+11
                      AND d_date_sk = ss_sold_date_sk
                      AND s_store_sk = ss_store_sk
                    GROUP BY s_state) tmp1
                 WHERE ranking <= 5 )
            GROUP BY rollup(s_state,s_county)
            ORDER BY lochierarchy DESC NULLS FIRST,
                     CASE
                         WHEN grouping(s_state)+grouping(s_county) = 0 THEN s_state
                     END NULLS FIRST,
                     rank_within_parent NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            total_sum = r.IsDBNull(0) ? (decimal?)null : r.GetDecimal(0),
            s_state = r.IsDBNull(1) ? null : r.GetString(1),
            s_county = r.IsDBNull(2) ? null : r.GetString(2),
            lochierarchy = r.GetInt32(3),
            rank_within_parent = r.GetInt64(4)
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];
            Assert.Equal(d.total_sum, p.total_sum);
            Assert.Equal(d.s_state, p.s_state);
            Assert.Equal(d.s_county, p.s_county);
            Assert.Equal(d.lochierarchy, p.lochierarchy);
            Assert.Equal(d.rank_within_parent, p.rank_within_parent);
        }
    }
}

