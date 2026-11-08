namespace Polars.Tpcds;

/// <summary>DuckDB reference SQL for the shared TPC-DS queries; Parquet paths are resolved at call time.</summary>
public static partial class TpcdsSql
{
    public static string Q1()
    {
        return $@"
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
    }

    public static string Q3()
    {
        return $@"
            SELECT dt.d_year,
                   item.i_brand_id brand_id,
                   item.i_brand brand,
                   sum(ss_ext_sales_price) sum_agg
            FROM read_parquet('{TpcdsData.DateDimPath}') dt,
                 read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                 read_parquet('{TpcdsData.ItemPath}') item
            WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
              AND store_sales.ss_item_sk = item.i_item_sk
              AND item.i_manufact_id = 128
              AND dt.d_moy = 11
            GROUP BY dt.d_year,
                     item.i_brand,
                     item.i_brand_id
            ORDER BY dt.d_year,
                     sum_agg DESC,
                     brand_id
            LIMIT 100;
        ";
    }

    public static string Q5()
    {
        return $@"
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
    }

    public static string Q6()
    {
        return $@"
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
    }

    public static string Q14()
    {
        return $@"
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
    }

    public static string Q16()
    {
        return $@"
            SELECT count(DISTINCT cs1.cs_order_number) AS order_count,
                   sum(cs1.cs_ext_ship_cost) AS total_shipping_cost,
                   sum(cs1.cs_net_profit) AS total_net_profit
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
    }

    public static string Q36()
    {
        return $@"
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
    }

    public static string Q38()
    {
        return $@"
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
    }

    public static string Q47()
    {
        return $@"
            WITH v1 AS
              (SELECT i_category,
                      i_brand,
                      s_store_name,
                      s_company_name,
                      d_year,
                      d_moy,
                      sum(ss_sales_price) sum_sales,
                      avg(sum(ss_sales_price)) OVER (PARTITION BY i_category,
                                                                  i_brand,
                                                                  s_store_name,
                                                                  s_company_name,
                                                                  d_year) avg_monthly_sales,
                      rank() OVER (PARTITION BY i_category,
                                                i_brand,
                                                s_store_name,
                                                s_company_name
                                   ORDER BY d_year,
                                            d_moy) rn
               FROM read_parquet('{TpcdsData.ItemPath}'),
                    read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.StorePath}')
               WHERE ss_item_sk = i_item_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND ss_store_sk = s_store_sk
                 AND (d_year = 1999
                      OR (d_year = 1999-1
                          AND d_moy =12)
                      OR (d_year = 1999+1
                          AND d_moy =1))
               GROUP BY i_category,
                        i_brand,
                        s_store_name,
                        s_company_name,
                        d_year,
                        d_moy),
                 v2 AS
              (SELECT v1.i_category,
                      v1.i_brand,
                      v1.s_store_name,
                      v1.s_company_name,
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
                 AND v1.s_store_name = v1_lag.s_store_name
                 AND v1.s_store_name = v1_lead.s_store_name
                 AND v1.s_company_name = v1_lag.s_company_name
                 AND v1.s_company_name = v1_lead.s_company_name
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
            ORDER BY sum_sales - avg_monthly_sales, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            LIMIT 100;
        ";
    }

    public static string Q51()
    {
        return $@"
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
    }

    public static string Q55()
    {
        return $@"
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
    }

    public static string Q64()
    {
        return $@"
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
            ORDER BY cs1.product_name NULLS FIRST,
                     cs1.store_name NULLS FIRST,
                     cs2.cnt NULLS FIRST,
                     cs1.s1 NULLS FIRST,
                     cs2.s1 NULLS FIRST;
        ";
    }

    public static string Q67()
    {
        return $@"
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
    }

    public static string Q72()
    {
        return $@"
            SELECT i_item_desc,
                   w_warehouse_name,
                   d1.d_week_seq,
                   sum(CASE
                           WHEN p_promo_sk IS NULL THEN 1
                           ELSE 0
                       END) no_promo,
                   sum(CASE
                           WHEN p_promo_sk IS NOT NULL THEN 1
                           ELSE 0
                       END) promo,
                   count(*) total_cnt
            FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales
            JOIN read_parquet('{TpcdsData.InventoryPath}') inventory ON (cs_item_sk = inv_item_sk)
            JOIN read_parquet('{TpcdsData.WarehousePath}') warehouse ON (w_warehouse_sk=inv_warehouse_sk)
            JOIN read_parquet('{TpcdsData.ItemPath}') item ON (i_item_sk = cs_item_sk)
            JOIN read_parquet('{TpcdsData.CustomerDemographicsPath}') customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
            JOIN read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
            JOIN read_parquet('{TpcdsData.DateDimPath}') d1 ON (cs_sold_date_sk = d1.d_date_sk)
            JOIN read_parquet('{TpcdsData.DateDimPath}') d2 ON (inv_date_sk = d2.d_date_sk)
            JOIN read_parquet('{TpcdsData.DateDimPath}') d3 ON (cs_ship_date_sk = d3.d_date_sk)
            LEFT OUTER JOIN read_parquet('{TpcdsData.PromotionPath}') promotion ON (cs_promo_sk=p_promo_sk)
            LEFT OUTER JOIN read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns ON (cr_item_sk = cs_item_sk
                                                AND cr_order_number = cs_order_number)
            WHERE d1.d_week_seq = d2.d_week_seq
              AND inv_quantity_on_hand < cs_quantity
              AND d3.d_date > d1.d_date + 5
              AND hd_buy_potential = '>10000'
              AND d1.d_year = 1999
              AND cd_marital_status = 'D'
            GROUP BY i_item_desc,
                     w_warehouse_name,
                     d1.d_week_seq
            ORDER BY total_cnt DESC NULLS FIRST,
                     i_item_desc NULLS FIRST,
                     w_warehouse_name NULLS FIRST,
                     d1.d_week_seq NULLS FIRST
            LIMIT 100;
        ";
    }

    public static string Q86()
    {
        return $@"
            SELECT sum(ws_net_paid) AS total_sum ,
                   i_category ,
                   i_class ,
                   grouping(i_category)+grouping(i_class) AS lochierarchy ,
                   rank() OVER ( PARTITION BY grouping(i_category)+grouping(i_class),
                                              CASE
                                                  WHEN grouping(i_class) = 0 THEN i_category
                                              END
                                ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
            FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales ,
                 read_parquet('{TpcdsData.DateDimPath}') d1 ,
                 read_parquet('{TpcdsData.ItemPath}') item
            WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
              AND d1.d_date_sk = ws_sold_date_sk
              AND i_item_sk = ws_item_sk
            GROUP BY rollup(i_category,i_class)
            ORDER BY lochierarchy DESC NULLS FIRST,
                     CASE
                         WHEN grouping(i_category)+grouping(i_class) = 0 THEN i_category
                     END NULLS FIRST,
                     rank_within_parent NULLS FIRST,
                     i_class NULLS FIRST
            LIMIT 100;
        ";
    }

    public static string Q87()
    {
        return $@"
            SELECT count(*)
            FROM ((SELECT DISTINCT c_last_name,
                                     c_first_name,
                                     d_date
                     FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                          read_parquet('{TpcdsData.DateDimPath}') date_dim,
                          read_parquet('{TpcdsData.CustomerPath}') customer
                     WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
                       AND store_sales.ss_customer_sk = customer.c_customer_sk
                       AND d_month_seq BETWEEN 1200 AND 1200+11)
                  EXCEPT
                    (SELECT DISTINCT c_last_name,
                                     c_first_name,
                                     d_date
                     FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales,
                          read_parquet('{TpcdsData.DateDimPath}') date_dim,
                          read_parquet('{TpcdsData.CustomerPath}') customer
                     WHERE catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
                       AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
                       AND d_month_seq BETWEEN 1200 AND 1200+11)
                  EXCEPT
                    (SELECT DISTINCT c_last_name,
                                     c_first_name,
                                     d_date
                     FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                          read_parquet('{TpcdsData.DateDimPath}') date_dim,
                          read_parquet('{TpcdsData.CustomerPath}') customer
                     WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk
                       AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
                       AND d_month_seq BETWEEN 1200 AND 1200+11)) cool_cust ;
        ";
    }

    public static string Q94()
    {
        return $@"
SELECT
   count(distinct ws_order_number) as order_count
  ,sum(ws_ext_ship_cost) as total_shipping_cost
  ,sum(ws_net_profit) as total_net_profit
FROM
   read_parquet('{TpcdsData.WebSalesPath}') ws1
  ,read_parquet('{TpcdsData.DateDimPath}') date_dim
  ,read_parquet('{TpcdsData.CustomerAddressPath}') customer_address
  ,read_parquet('{TpcdsData.WebSitePath}') web_site
WHERE
    d_date between '1999-2-01' and cast('1999-4-02' as date)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'IL'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and exists (select *
            from read_parquet('{TpcdsData.WebSalesPath}') ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from read_parquet('{TpcdsData.WebReturnsPath}') wr1
               where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
limit 100;
";
    }

    public static string Q98()
    {
        return $@"
SELECT i_item_id
      ,i_item_desc
      ,i_category
      ,i_class
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue
      ,sum(ss_ext_sales_price)*100.0000/sum(sum(ss_ext_sales_price)) over
          (partition by i_class) as revenueratio
FROM
         read_parquet('{TpcdsData.StoreSalesPath}') store_sales
        ,read_parquet('{TpcdsData.ItemPath}') item
        ,read_parquet('{TpcdsData.DateDimPath}') date_dim
WHERE
        ss_item_sk = i_item_sk
        and i_category in ('Sports', 'Books', 'Home')
        and ss_sold_date_sk = d_date_sk
        and d_date between cast('1999-02-22' as date)
                                and cast('1999-03-24' as date)
GROUP BY
        i_item_id
        ,i_item_desc
        ,i_category
        ,i_class
        ,i_current_price
ORDER BY
        i_category NULLS FIRST
        ,i_class NULLS FIRST
        ,i_item_id NULLS FIRST
        ,i_item_desc NULLS FIRST
        ,revenueratio NULLS FIRST;
";
    }
}
