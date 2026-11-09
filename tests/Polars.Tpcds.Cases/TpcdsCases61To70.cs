namespace Polars.Tpcds;

public static partial class TpcdsCases
{
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
    public static IQueryable<object> Q61()
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

        return q61;
    }

    public static string Q61Sql() =>
        $@"
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
    public static IQueryable<object> Q62()
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

        return q62;
    }

    public static string Q62Sql() =>
        $@"
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
    public static IQueryable<object> Q63()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        string[] cat1 = [ "Books", "Children", "Electronics" ];
        string[] class1 = ["personal", "portable", "reference", "self-help"];
        string[] brand1 = ["scholaramalgamalg #14", "scholaramalgamalg #7", "exportiunivamalg #9", "scholaramalgamalg #9"];

        string[] cat2 = ["Women", "Music", "Men"];
        string[] class2 = ["accessories", "classical", "fragrances", "pants"];
        string[] brand2 = ["amalgimporto #1", "edu packscholar #1", "exportiimporto #1", "importoamalg #1"];

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

        return q63;
    }

    public static string Q63Sql() =>
        $@"
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
    public static IQueryable<object> Q65()
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
                     // SQL sum() is NULL for a group without non-null values, while LINQ Sum() is 0.
                     revenue = g.Count(x => x.ss_sales_price != null) > 0 ? g.Sum(x => x.ss_sales_price) : null
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

        return q65;
    }

    public static string Q65Sql() =>
        $@"
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
    public static IQueryable<object> Q66()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var timeDimQuery = TpcdsData.TimeDimQuery();
        var shipModeQuery = TpcdsData.ShipModeQuery();

        string[] carriers = [ "DHL", "BARIAN" ];

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
                       // SQL sum() is NULL for a group without non-null values, while LINQ Sum() is 0.
                       jan_sales_per_sq_foot = g.Count(x => (double?)x.jan_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.jan_sales / (double?)x.w_warehouse_sq_ft) : null,
                       feb_sales_per_sq_foot = g.Count(x => (double?)x.feb_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.feb_sales / (double?)x.w_warehouse_sq_ft) : null,
                       mar_sales_per_sq_foot = g.Count(x => (double?)x.mar_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.mar_sales / (double?)x.w_warehouse_sq_ft) : null,
                       apr_sales_per_sq_foot = g.Count(x => (double?)x.apr_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.apr_sales / (double?)x.w_warehouse_sq_ft) : null,
                       may_sales_per_sq_foot = g.Count(x => (double?)x.may_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.may_sales / (double?)x.w_warehouse_sq_ft) : null,
                       jun_sales_per_sq_foot = g.Count(x => (double?)x.jun_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.jun_sales / (double?)x.w_warehouse_sq_ft) : null,
                       jul_sales_per_sq_foot = g.Count(x => (double?)x.jul_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.jul_sales / (double?)x.w_warehouse_sq_ft) : null,
                       aug_sales_per_sq_foot = g.Count(x => (double?)x.aug_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.aug_sales / (double?)x.w_warehouse_sq_ft) : null,
                       sep_sales_per_sq_foot = g.Count(x => (double?)x.sep_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.sep_sales / (double?)x.w_warehouse_sq_ft) : null,
                       oct_sales_per_sq_foot = g.Count(x => (double?)x.oct_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.oct_sales / (double?)x.w_warehouse_sq_ft) : null,
                       nov_sales_per_sq_foot = g.Count(x => (double?)x.nov_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.nov_sales / (double?)x.w_warehouse_sq_ft) : null,
                       dec_sales_per_sq_foot = g.Count(x => (double?)x.dec_sales / (double?)x.w_warehouse_sq_ft != null) > 0 ? g.Sum(x => (double?)x.dec_sales / (double?)x.w_warehouse_sq_ft) : null,
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

        return q66;
    }

    public static string Q66Sql() =>
        $@"
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
    public static IQueryable<object> Q68()
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
                   // SQL <> / = is unknown when either side is NULL; C# null semantics would compare NULLs.
                   where current_addr.ca_city != null && x.bought_city != null && current_addr.ca_city != x.bought_city
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

        return q68;
    }

    public static string Q68Sql() =>
        $@"
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
    public static IQueryable<object> Q69()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        string[] states = [ "KY", "GA", "NM" ];
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
                   let cnt = g.LongCount()
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

        return q69;
    }

    public static string Q69Sql() =>
        $@"
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
    public static IQueryable<object> Q70()
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

        var rollup = baseSales
            .GroupBy(x => new { x.s_state, x.s_county })
            .RollUp(
                (key, g) => new
                {
                    total_sum = g.Sum(x => x.ss_net_profit),
                    key.s_state,
                    key.s_county,
                    g_county = key.s_county.Grouping(),
                    lochierarchy = key.s_state.Grouping() + key.s_county.Grouping()
                }
            );

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

        return q70;
    }

    public static string Q70Sql() =>
        $@"
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
}
