using Polars.CSharp.Linq;

namespace Polars.Tpcds.Tests;

public class TpcdsQuery71To80Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 71
     *
     * SELECT i_brand_id brand_id,
     *        i_brand brand,
     *        t_hour,
     *        t_minute,
     *        sum(ext_price) ext_price
     * FROM item,
     *   (SELECT ws_ext_sales_price AS ext_price,
     *           ws_sold_date_sk AS sold_date_sk,
     *           ws_item_sk AS sold_item_sk,
     *           ws_sold_time_sk AS time_sk
     *    FROM web_sales,
     *         date_dim
     *    WHERE d_date_sk = ws_sold_date_sk
     *      AND d_moy=11
     *      AND d_year=1999
     *    UNION ALL SELECT cs_ext_sales_price AS ext_price,
     *                     cs_sold_date_sk AS sold_date_sk,
     *                     cs_item_sk AS sold_item_sk,
     *                     cs_sold_time_sk AS time_sk
     *    FROM catalog_sales,
     *         date_dim
     *    WHERE d_date_sk = cs_sold_date_sk
     *      AND d_moy=11
     *      AND d_year=1999
     *    UNION ALL SELECT ss_ext_sales_price AS ext_price,
     *                     ss_sold_date_sk AS sold_date_sk,
     *                     ss_item_sk AS sold_item_sk,
     *                     ss_sold_time_sk AS time_sk
     *    FROM store_sales,
     *         date_dim
     *    WHERE d_date_sk = ss_sold_date_sk
     *      AND d_moy=11
     *      AND d_year=1999 ) tmp,
     *      time_dim
     * WHERE sold_item_sk = i_item_sk
     *   AND i_manager_id=1
     *   AND time_sk = t_time_sk
     *   AND (t_meal_time = 'breakfast'
     *        OR t_meal_time = 'dinner')
     * GROUP BY i_brand,
     *          i_brand_id,
     *          t_hour,
     *          t_minute
     * ORDER BY ext_price DESC NULLS FIRST,
     *          i_brand_id NULLS FIRST,
     *          t_hour NULLS FIRST;
     */
    [Fact]
    public void Test_Q71()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var timeDimQuery = TpcdsData.TimeDimQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_moy == 11 && d.d_year == 1999);

        var wsTmp = from ws in webSalesQuery
                    join d in filteredDates on ws.ws_sold_date_sk equals d.d_date_sk
                    select new
                    {
                        ext_price = ws.ws_ext_sales_price,
                        sold_date_sk = ws.ws_sold_date_sk,
                        sold_item_sk = ws.ws_item_sk,
                        time_sk = ws.ws_sold_time_sk
                    };

        var csTmp = from cs in catalogSalesQuery
                    join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                    select new
                    {
                        ext_price = cs.cs_ext_sales_price,
                        sold_date_sk = cs.cs_sold_date_sk,
                        sold_item_sk = cs.cs_item_sk,
                        time_sk = cs.cs_sold_time_sk
                    };

        var ssTmp = from ss in storeSalesQuery
                    join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                    select new
                    {
                        ext_price = ss.ss_ext_sales_price,
                        sold_date_sk = ss.ss_sold_date_sk,
                        sold_item_sk = ss.ss_item_sk,
                        time_sk = ss.ss_sold_time_sk
                    };

        var tmp = wsTmp.Concat(csTmp).Concat(ssTmp);

        var q71 = from s in tmp
                  join i in itemQuery on s.sold_item_sk equals i.i_item_sk
                  join t in timeDimQuery on s.time_sk equals t.t_time_sk
                  where i.i_manager_id == 1
                     && (t.t_meal_time == "breakfast" || t.t_meal_time == "dinner")
                  group s by new { i.i_brand, i.i_brand_id, t.t_hour, t.t_minute } into g
                  orderby g.Sum(x => x.ext_price) descending, g.Key.i_brand_id, g.Key.t_hour, g.Key.t_minute
                  select new
                  {
                      brand_id = g.Key.i_brand_id,
                      brand = g.Key.i_brand,
                      g.Key.t_hour,
                      g.Key.t_minute,
                      ext_price = g.Sum(x => x.ext_price)
                  };

        AssertPlan(q71, NativePushdown);

        var polarsResult = q71.ToList();

        var duckDbSql = $@"
            SELECT i_brand_id brand_id,
                   i_brand brand,
                   t_hour,
                   t_minute,
                   sum(ext_price) ext_price
            FROM read_parquet('{TpcdsData.ItemPath}') item,
              (SELECT ws_ext_sales_price AS ext_price,
                      ws_sold_date_sk AS sold_date_sk,
                      ws_item_sk AS sold_item_sk,
                      ws_sold_time_sk AS time_sk
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE d_date_sk = ws_sold_date_sk
                 AND d_moy=11
                 AND d_year=1999
               UNION ALL SELECT cs_ext_sales_price AS ext_price,
                                cs_sold_date_sk AS sold_date_sk,
                                cs_item_sk AS sold_item_sk,
                                cs_sold_time_sk AS time_sk
               FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE d_date_sk = cs_sold_date_sk
                 AND d_moy=11
                 AND d_year=1999
               UNION ALL SELECT ss_ext_sales_price AS ext_price,
                                ss_sold_date_sk AS sold_date_sk,
                                ss_item_sk AS sold_item_sk,
                                ss_sold_time_sk AS time_sk
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE d_date_sk = ss_sold_date_sk
                 AND d_moy=11
                 AND d_year=1999 ) tmp,
                 read_parquet('{TpcdsData.TimeDimPath}') time_dim
            WHERE sold_item_sk = i_item_sk
              AND i_manager_id=1
              AND time_sk = t_time_sk
              AND (t_meal_time = 'breakfast'
                   OR t_meal_time = 'dinner')
            GROUP BY i_brand,
                     i_brand_id,
                     t_hour,
                     t_minute
            ORDER BY ext_price DESC NULLS FIRST,
                     i_brand_id NULLS FIRST,
                     t_hour NULLS FIRST,
                     t_minute NULLS FIRST;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            brand_id = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            brand = r.IsDBNull(1) ? null : r.GetString(1),
            t_hour = r.IsDBNull(2) ? (long?)null : r.GetInt64(2),
            t_minute = r.IsDBNull(3) ? (long?)null : r.GetInt64(3),
            ext_price = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].brand_id, polarsResult[i].brand_id);
            Assert.Equal(duckDbResult[i].brand, polarsResult[i].brand);
            Assert.Equal(duckDbResult[i].t_hour, polarsResult[i].t_hour);
            Assert.Equal(duckDbResult[i].t_minute, polarsResult[i].t_minute);
            Assert.Equal(duckDbResult[i].ext_price ?? 0m, polarsResult[i].ext_price ?? 0m, 2);
        }
    }

    /*
     * TPC-DS Query 72
     *
     * SELECT i_item_desc,
     *        w_warehouse_name,
     *        d1.d_week_seq,
     *        sum(CASE
     *                WHEN p_promo_sk IS NULL THEN 1
     *                ELSE 0
     *            END) no_promo,
     *        sum(CASE
     *                WHEN p_promo_sk IS NOT NULL THEN 1
     *                ELSE 0
     *            END) promo,
     *        count(*) total_cnt
     * FROM catalog_sales
     * JOIN inventory ON (cs_item_sk = inv_item_sk)
     * JOIN warehouse ON (w_warehouse_sk=inv_warehouse_sk)
     * JOIN item ON (i_item_sk = cs_item_sk)
     * JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
     * JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
     * JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
     * JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
     * JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
     * LEFT OUTER JOIN promotion ON (cs_promo_sk=p_promo_sk)
     * LEFT OUTER JOIN catalog_returns ON (cr_item_sk = cs_item_sk
     *                                     AND cr_order_number = cs_order_number)
     * WHERE d1.d_week_seq = d2.d_week_seq
     *   AND inv_quantity_on_hand < cs_quantity
     *   AND d3.d_date > d1.d_date + 5 -- SQL Server: DATEADD(day, 5, d1.d_date)
     *   AND hd_buy_potential = '>10000'
     *   AND d1.d_year = 1999
     *   AND cd_marital_status = 'D'
     * GROUP BY i_item_desc,
     *          w_warehouse_name,
     *          d1.d_week_seq
     * ORDER BY total_cnt DESC NULLS FIRST,
     *          i_item_desc NULLS FIRST,
     *          w_warehouse_name NULLS FIRST,
     *          d1.d_week_seq NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q72()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var inventoryQuery = TpcdsData.InventoryQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var cdemoQuery = TpcdsData.CustomerDemographicsQuery();
        var hdemoQuery = TpcdsData.HouseholdDemographicsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var promotionQuery = TpcdsData.PromotionQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();

        var invWithWeek = from inv in inventoryQuery
                          join w in warehouseQuery on inv.inv_warehouse_sk equals w.w_warehouse_sk
                          join d2 in dateDimQuery on inv.inv_date_sk equals d2.d_date_sk
                          select new
                          {
                              inv.inv_item_sk,
                              inv.inv_quantity_on_hand,
                              w.w_warehouse_name,
                              d2.d_week_seq
                          };

        var q72 = (from cs in catalogSalesQuery
                   join d1 in dateDimQuery on cs.cs_sold_date_sk equals d1.d_date_sk
                   join d3 in dateDimQuery on cs.cs_ship_date_sk equals d3.d_date_sk
                   join cd in cdemoQuery on cs.cs_bill_cdemo_sk equals cd.cd_demo_sk
                   join hd in hdemoQuery on cs.cs_bill_hdemo_sk equals hd.hd_demo_sk
                   join i in itemQuery on cs.cs_item_sk equals i.i_item_sk
                   join inv in invWithWeek on new { item_sk = cs.cs_item_sk, week_seq = d1.d_week_seq }
                                       equals new { item_sk = inv.inv_item_sk, week_seq = inv.d_week_seq }
                   join p in promotionQuery on cs.cs_promo_sk equals p.p_promo_sk into pGroup
                   from p in pGroup.DefaultIfEmpty()
                   join cr in catalogReturnsQuery on new { item_sk = cs.cs_item_sk, order_number = cs.cs_order_number }
                                              equals new { item_sk = cr.cr_item_sk, order_number = cr.cr_order_number } into crGroup
                   from cr in crGroup.DefaultIfEmpty()
                   where d1.d_year == 1999
                      && hd.hd_buy_potential == ">10000"
                      && cd.cd_marital_status == "D"
                      && inv.inv_quantity_on_hand < cs.cs_quantity
                      && d3.d_date > d1.d_date!.Value.AddDays(5)
#pragma warning disable CS0472
                   group p by new { i.i_item_desc, inv.w_warehouse_name, d1.d_week_seq } into g
                   orderby g.LongCount() descending, g.Key.i_item_desc, g.Key.w_warehouse_name, g.Key.d_week_seq
                   select new
                   {
                       g.Key.i_item_desc,
                       g.Key.w_warehouse_name,
                       g.Key.d_week_seq,
                       no_promo = g.Sum(x => (long?)x.p_promo_sk == null ? 1L : 0L),
                       promo = g.Sum(x => (long?)x.p_promo_sk != null ? 1L : 0L),
                       total_cnt = g.LongCount()
                   }).Take(100);
#pragma warning restore CS0472

        AssertPlan(q72, NativePushdown);

        var polarsResult = q72.ToList();

        var duckDbSql = $@"
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

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_desc = r.IsDBNull(0) ? null : r.GetString(0),
            w_warehouse_name = r.IsDBNull(1) ? null : r.GetString(1),
            d_week_seq = r.IsDBNull(2) ? (long?)null : r.GetInt64(2),
            no_promo = r.GetInt64(3),
            promo = r.GetInt64(4),
            total_cnt = r.GetInt64(5)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].i_item_desc, polarsResult[i].i_item_desc);
            Assert.Equal(duckDbResult[i].w_warehouse_name, polarsResult[i].w_warehouse_name);
            Assert.Equal(duckDbResult[i].d_week_seq, polarsResult[i].d_week_seq);
            Assert.Equal(duckDbResult[i].no_promo, polarsResult[i].no_promo);
            Assert.Equal(duckDbResult[i].promo, polarsResult[i].promo);
            Assert.Equal(duckDbResult[i].total_cnt, polarsResult[i].total_cnt);
        }
    }

    /*
     * TPC-DS Query 73
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
     *      AND date_dim.d_dom BETWEEN 1 AND 2
     *      AND (household_demographics.hd_buy_potential = 'Unknown'
     *           OR household_demographics.hd_buy_potential = '>10000')
     *      AND household_demographics.hd_vehicle_count > 0
     *      AND CASE
     *              WHEN household_demographics.hd_vehicle_count > 0 THEN (household_demographics.hd_dep_count*1.000)/ household_demographics.hd_vehicle_count
     *              ELSE NULL
     *          END > 1
     *      AND date_dim.d_year IN (1999,
     *                              1999+1,
     *                              1999+2)
     *      AND store.s_county IN ('Orange County',
     *                             'Bronx County',
     *                             'Franklin Parish',
     *                             'Williamson County')
     *    GROUP BY ss_ticket_number,
     *             ss_customer_sk) dj,
     *      customer
     * WHERE ss_customer_sk = c_customer_sk
     *   AND cnt BETWEEN 1 AND 5
     * ORDER BY cnt DESC,
     *          c_last_name ASC;
     */
    [Fact]
    public void Test_Q73()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var hdemoQuery = TpcdsData.HouseholdDemographicsQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        long[] years = [1999, 2000, 2001];
        string[] counties = ["Orange County", "Bronx County", "Franklin Parish", "Williamson County"];

        var dj = from ss in storeSalesQuery
                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                 join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                 join hd in hdemoQuery on ss.ss_hdemo_sk equals hd.hd_demo_sk
                 where d.d_dom >= 1 && d.d_dom <= 2
                    && (hd.hd_buy_potential == "Unknown" || hd.hd_buy_potential == ">10000")
                    && hd.hd_vehicle_count > 0
                    && ((double)hd.hd_dep_count! / (double)hd.hd_vehicle_count!) > 1.0
                    && years.Contains(d.d_year!.Value)
                    && counties.Contains(s.s_county!)
                 group ss by new { ss.ss_ticket_number, ss.ss_customer_sk } into g
                 select new
                 {
                     g.Key.ss_ticket_number,
                     g.Key.ss_customer_sk,
                     cnt = g.LongCount()
                 };

        var q73 = from x in dj
                  join c in customerQuery on x.ss_customer_sk equals c.c_customer_sk
                  where x.cnt >= 1 && x.cnt <= 5
                  orderby x.cnt descending, c.c_last_name, x.ss_ticket_number
                  select new
                  {
                      c.c_last_name,
                      c.c_first_name,
                      c.c_salutation,
                      c.c_preferred_cust_flag,
                      x.ss_ticket_number,
                      x.cnt
                  };

        AssertPlan(q73, NativePushdown);

        var polarsResult = q73.ToList();

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
                 AND date_dim.d_dom BETWEEN 1 AND 2
                 AND (household_demographics.hd_buy_potential = 'Unknown'
                      OR household_demographics.hd_buy_potential = '>10000')
                 AND household_demographics.hd_vehicle_count > 0
                 AND CASE
                         WHEN household_demographics.hd_vehicle_count > 0 THEN (household_demographics.hd_dep_count*1.000)/ household_demographics.hd_vehicle_count
                         ELSE NULL
                     END > 1
                 AND date_dim.d_year IN (1999,
                                         1999+1,
                                         1999+2)
                 AND store.s_county IN ('Orange County',
                                        'Bronx County',
                                        'Franklin Parish',
                                        'Williamson County')
               GROUP BY ss_ticket_number,
                        ss_customer_sk) dj,
                 read_parquet('{TpcdsData.CustomerPath}') customer
            WHERE ss_customer_sk = c_customer_sk
              AND cnt BETWEEN 1 AND 5
            ORDER BY cnt DESC NULLS FIRST,
                     c_last_name ASC NULLS FIRST,
                     ss_ticket_number ASC NULLS FIRST;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            c_last_name = r.IsDBNull(0) ? null : r.GetString(0),
            c_first_name = r.IsDBNull(1) ? null : r.GetString(1),
            c_salutation = r.IsDBNull(2) ? null : r.GetString(2),
            c_preferred_cust_flag = r.IsDBNull(3) ? null : r.GetString(3),
            ss_ticket_number = r.GetInt64(4),
            cnt = r.GetInt64(5)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].c_last_name, polarsResult[i].c_last_name);
            Assert.Equal(duckDbResult[i].c_first_name, polarsResult[i].c_first_name);
            Assert.Equal(duckDbResult[i].c_salutation, polarsResult[i].c_salutation);
            Assert.Equal(duckDbResult[i].c_preferred_cust_flag, polarsResult[i].c_preferred_cust_flag);
            Assert.Equal(duckDbResult[i].ss_ticket_number, polarsResult[i].ss_ticket_number);
            Assert.Equal(duckDbResult[i].cnt, polarsResult[i].cnt);
        }
    }

    /*
     * TPC-DS Query 74
     *
     * WITH year_total AS
     *   (SELECT c_customer_id customer_id,
     *           c_first_name customer_first_name,
     *           c_last_name customer_last_name,
     *           d_year AS year_,
     *           sum(ss_net_paid) year_total,
     *           's' sale_type
     *    FROM customer,
     *         store_sales,
     *         date_dim
     *    WHERE c_customer_sk = ss_customer_sk
     *      AND ss_sold_date_sk = d_date_sk
     *      AND d_year IN (2001,
     *                     2001+1)
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             d_year
     *    UNION ALL SELECT c_customer_id customer_id,
     *                     c_first_name customer_first_name,
     *                     c_last_name customer_last_name,
     *                     d_year AS year_,
     *                     sum(ws_net_paid) year_total,
     *                     'w' sale_type
     *    FROM customer,
     *         web_sales,
     *         date_dim
     *    WHERE c_customer_sk = ws_bill_customer_sk
     *      AND ws_sold_date_sk = d_date_sk
     *      AND d_year IN (2001,
     *                     2001+1)
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             d_year)
     * SELECT t_s_secyear.customer_id,
     *        t_s_secyear.customer_first_name,
     *        t_s_secyear.customer_last_name
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
     *   AND t_s_firstyear.year_ = 2001
     *   AND t_s_secyear.year_ = 2001+1
     *   AND t_w_firstyear.year_ = 2001
     *   AND t_w_secyear.year_ = 2001+1
     *   AND t_s_firstyear.year_total > 0
     *   AND t_w_firstyear.year_total > 0
     *   AND CASE
     *           WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
     *           ELSE NULL
     *       END > CASE
     *                 WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
     *                 ELSE NULL
     *             END
     * ORDER BY 1 NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q74()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var storeYear = from c in customerQuery
                        join ss in storeSalesQuery on c.c_customer_sk equals ss.ss_customer_sk
                        join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                        where d.d_year == 2001 || d.d_year == 2002
                        group ss by new
                        {
                            c.c_customer_id,
                            c.c_first_name,
                            c.c_last_name,
                            d.d_year
                        } into g
                        select new
                        {
                            customer_id = g.Key.c_customer_id,
                            customer_first_name = g.Key.c_first_name,
                            customer_last_name = g.Key.c_last_name,
                            year_ = g.Key.d_year,
                            year_total = g.Sum(x => x.ss_net_paid)
                        };

        var webYear = from c in customerQuery
                      join ws in webSalesQuery on c.c_customer_sk equals ws.ws_bill_customer_sk
                      join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                      where d.d_year == 2001 || d.d_year == 2002
                      group ws by new
                      {
                          c.c_customer_id,
                          d.d_year
                      } into g
                      select new
                      {
                          customer_id = g.Key.c_customer_id,
                          year_ = g.Key.d_year,
                          year_total = g.Sum(x => x.ws_net_paid)
                      };

        var ts1 = storeYear.Where(x => x.year_ == 2001 && x.year_total > 0m);
        var ts2 = storeYear.Where(x => x.year_ == 2002);
        var tw1 = webYear.Where(x => x.year_ == 2001 && x.year_total > 0m);
        var tw2 = webYear.Where(x => x.year_ == 2002);

        var q74 = (from s1 in ts1
                   join s2 in ts2 on s1.customer_id equals s2.customer_id
                   join w1 in tw1 on s1.customer_id equals w1.customer_id
                   join w2 in tw2 on s1.customer_id equals w2.customer_id
                   where (double)w2.year_total! / (double)w1.year_total! > (double)s2.year_total! / (double)s1.year_total!
                   orderby s2.customer_id, s2.customer_first_name, s2.customer_last_name
                   select new
                   {
                       s2.customer_id,
                       s2.customer_first_name,
                       s2.customer_last_name
                   }).Take(100);

        AssertPlan(q74, NativePushdown);

        var polarsResult = q74.ToList();

        var duckDbSql = $@"
            WITH year_total AS
              (SELECT c_customer_id customer_id,
                      c_first_name customer_first_name,
                      c_last_name customer_last_name,
                      d_year AS year_,
                      sum(ss_net_paid) year_total,
                      's' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}') customer,
                    read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE c_customer_sk = ss_customer_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND d_year IN (2001,
                                2001+1)
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        d_year
               UNION ALL SELECT c_customer_id customer_id,
                                c_first_name customer_first_name,
                                c_last_name customer_last_name,
                                d_year AS year_,
                                sum(ws_net_paid) year_total,
                                'w' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}') customer,
                    read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE c_customer_sk = ws_bill_customer_sk
                 AND ws_sold_date_sk = d_date_sk
                 AND d_year IN (2001,
                                2001+1)
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        d_year)
            SELECT t_s_secyear.customer_id,
                   t_s_secyear.customer_first_name,
                   t_s_secyear.customer_last_name
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
              AND t_s_firstyear.year_ = 2001
              AND t_s_secyear.year_ = 2001+1
              AND t_w_firstyear.year_ = 2001
              AND t_w_secyear.year_ = 2001+1
              AND t_s_firstyear.year_total > 0
              AND t_w_firstyear.year_total > 0
              AND CASE
                      WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                      ELSE NULL
                  END > CASE
                            WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                            ELSE NULL
                        END
            ORDER BY 1 NULLS FIRST, 2 NULLS FIRST, 3 NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            customer_id = r.IsDBNull(0) ? null : r.GetString(0),
            customer_first_name = r.IsDBNull(1) ? null : r.GetString(1),
            customer_last_name = r.IsDBNull(2) ? null : r.GetString(2)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].customer_id, polarsResult[i].customer_id);
            Assert.Equal(duckDbResult[i].customer_first_name, polarsResult[i].customer_first_name);
            Assert.Equal(duckDbResult[i].customer_last_name, polarsResult[i].customer_last_name);
        }
    }

    /*
     * TPC-DS Query 75
     *
     * WITH all_sales AS
     *   ( SELECT d_year ,
     *            i_brand_id ,
     *            i_class_id ,
     *            i_category_id ,
     *            i_manufact_id ,
     *            SUM(sales_cnt) AS sales_cnt ,
     *            SUM(sales_amt) AS sales_amt
     *    FROM
     *      (SELECT d_year ,
     *              i_brand_id ,
     *              i_class_id ,
     *              i_category_id ,
     *              i_manufact_id ,
     *              cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt ,
     *              cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
     *       FROM catalog_sales
     *       JOIN item ON i_item_sk=cs_item_sk
     *       JOIN date_dim ON d_date_sk=cs_sold_date_sk
     *       LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number
     *                                     AND cs_item_sk=cr_item_sk)
     *       WHERE i_category='Books'
     *       UNION SELECT d_year ,
     *                    i_brand_id ,
     *                    i_class_id ,
     *                    i_category_id ,
     *                    i_manufact_id ,
     *                    ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt ,
     *                    ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
     *       FROM store_sales
     *       JOIN item ON i_item_sk=ss_item_sk
     *       JOIN date_dim ON d_date_sk=ss_sold_date_sk
     *       LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number
     *                                   AND ss_item_sk=sr_item_sk)
     *       WHERE i_category='Books'
     *       UNION SELECT d_year ,
     *                    i_brand_id ,
     *                    i_class_id ,
     *                    i_category_id ,
     *                    i_manufact_id ,
     *                    ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt ,
     *                    ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
     *       FROM web_sales
     *       JOIN item ON i_item_sk=ws_item_sk
     *       JOIN date_dim ON d_date_sk=ws_sold_date_sk
     *       LEFT JOIN web_returns ON (ws_order_number=wr_order_number
     *                                 AND ws_item_sk=wr_item_sk)
     *       WHERE i_category='Books') sales_detail
     *    GROUP BY d_year,
     *             i_brand_id,
     *             i_class_id,
     *             i_category_id,
     *             i_manufact_id)
     * SELECT prev_yr.d_year AS prev_year ,
     *        curr_yr.d_year AS year_ ,
     *        curr_yr.i_brand_id ,
     *        curr_yr.i_class_id ,
     *        curr_yr.i_category_id ,
     *        curr_yr.i_manufact_id ,
     *        prev_yr.sales_cnt AS prev_yr_cnt ,
     *        curr_yr.sales_cnt AS curr_yr_cnt ,
     *        curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff ,
     *        curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
     * FROM all_sales curr_yr,
     *      all_sales prev_yr
     * WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
     *   AND curr_yr.i_class_id=prev_yr.i_class_id
     *   AND curr_yr.i_category_id=prev_yr.i_category_id
     *   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
     *   AND curr_yr.d_year=2002
     *   AND prev_yr.d_year=2002-1
     *   AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
     * ORDER BY sales_cnt_diff,
     *          sales_amt_diff
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q75()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var booksItems = itemQuery.Where(i => i.i_category == "Books");

        var csDetail = from cs in catalogSalesQuery
                       join i in booksItems on cs.cs_item_sk equals i.i_item_sk
                       join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                       join cr in catalogReturnsQuery on new { order_number = cs.cs_order_number, item_sk = cs.cs_item_sk }
                                                  equals new { order_number = cr.cr_order_number, item_sk = cr.cr_item_sk } into crGroup
                       from cr in crGroup.DefaultIfEmpty()
                       select new
                       {
                           d.d_year,
                           i.i_brand_id,
                           i.i_class_id,
                           i.i_category_id,
                           i.i_manufact_id,
                           sales_cnt = cs.cs_quantity - (cr.cr_return_quantity ?? 0L),
                           sales_amt = cs.cs_ext_sales_price - (cr.cr_return_amount ?? 0.00m)
                       };

        var ssDetail = from ss in storeSalesQuery
                       join i in booksItems on ss.ss_item_sk equals i.i_item_sk
                       join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                       join sr in storeReturnsQuery on new { ticket_number = ss.ss_ticket_number, item_sk = ss.ss_item_sk }
                                                equals new { ticket_number = sr.sr_ticket_number, item_sk = sr.sr_item_sk } into srGroup
                       from sr in srGroup.DefaultIfEmpty()
                       select new
                       {
                           d.d_year,
                           i.i_brand_id,
                           i.i_class_id,
                           i.i_category_id,
                           i.i_manufact_id,
                           sales_cnt = ss.ss_quantity - (sr.sr_return_quantity ?? 0L),
                           sales_amt = ss.ss_ext_sales_price - (sr.sr_return_amt ?? 0.00m)
                       };

        var wsDetail = from ws in webSalesQuery
                       join i in booksItems on ws.ws_item_sk equals i.i_item_sk
                       join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                       join wr in webReturnsQuery on new { order_number = ws.ws_order_number, item_sk = ws.ws_item_sk }
                                              equals new { order_number = wr.wr_order_number, item_sk = wr.wr_item_sk } into wrGroup
                       from wr in wrGroup.DefaultIfEmpty()
                       select new
                       {
                           d.d_year,
                           i.i_brand_id,
                           i.i_class_id,
                           i.i_category_id,
                           i.i_manufact_id,
                           sales_cnt = ws.ws_quantity - (wr.wr_return_quantity ?? 0L),
                           sales_amt = ws.ws_ext_sales_price - (wr.wr_return_amt ?? 0.00m)
                       };

        var salesDetail = csDetail.Concat(ssDetail).Concat(wsDetail).Distinct();

        var allSales = from x in salesDetail
                       group x by new
                       {
                           x.d_year,
                           x.i_brand_id,
                           x.i_class_id,
                           x.i_category_id,
                           x.i_manufact_id
                       } into g
                       select new
                       {
                           g.Key.d_year,
                           g.Key.i_brand_id,
                           g.Key.i_class_id,
                           g.Key.i_category_id,
                           g.Key.i_manufact_id,
                           sales_cnt = g.Sum(x => x.sales_cnt),
                           sales_amt = g.Sum(x => x.sales_amt)
                       };

        var currYr = allSales.Where(x => x.d_year == 2002);
        var prevYr = allSales.Where(x => x.d_year == 2001);

        var q75 = (from curr in currYr
                   join prev in prevYr on new
                   {
                       curr.i_brand_id,
                       curr.i_class_id,
                       curr.i_category_id,
                       curr.i_manufact_id
                   } equals new
                   {
                       prev.i_brand_id,
                       prev.i_class_id,
                       prev.i_category_id,
                       prev.i_manufact_id
                   }
                   where (double)curr.sales_cnt! / (double)prev.sales_cnt! < 0.9
                   orderby curr.sales_cnt - prev.sales_cnt, curr.sales_amt - prev.sales_amt
                   select new
                   {
                       prev_year = prev.d_year,
                       year_ = curr.d_year,
                       curr.i_brand_id,
                       curr.i_class_id,
                       curr.i_category_id,
                       curr.i_manufact_id,
                       prev_yr_cnt = prev.sales_cnt,
                       curr_yr_cnt = curr.sales_cnt,
                       sales_cnt_diff = curr.sales_cnt - prev.sales_cnt,
                       sales_amt_diff = curr.sales_amt - prev.sales_amt
                   }).Take(100);

        AssertPlan(q75, NativePushdown);

        var polarsResult = q75.ToList();

        var duckDbSql = $@"
            WITH all_sales AS
              ( SELECT d_year ,
                       i_brand_id ,
                       i_class_id ,
                       i_category_id ,
                       i_manufact_id ,
                       SUM(sales_cnt) AS sales_cnt ,
                       SUM(sales_amt) AS sales_amt
               FROM
                 (SELECT d_year ,
                         i_brand_id ,
                         i_class_id ,
                         i_category_id ,
                         i_manufact_id ,
                         cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt ,
                         cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
                  FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales
                  JOIN read_parquet('{TpcdsData.ItemPath}') item ON i_item_sk=cs_item_sk
                  JOIN read_parquet('{TpcdsData.DateDimPath}') date_dim ON d_date_sk=cs_sold_date_sk
                  LEFT JOIN read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns ON (cs_order_number=cr_order_number
                                                AND cs_item_sk=cr_item_sk)
                  WHERE i_category='Books'
                  UNION SELECT d_year ,
                               i_brand_id ,
                               i_class_id ,
                               i_category_id ,
                               i_manufact_id ,
                               ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt ,
                               ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
                  FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales
                  JOIN read_parquet('{TpcdsData.ItemPath}') item ON i_item_sk=ss_item_sk
                  JOIN read_parquet('{TpcdsData.DateDimPath}') date_dim ON d_date_sk=ss_sold_date_sk
                  LEFT JOIN read_parquet('{TpcdsData.StoreReturnsPath}') store_returns ON (ss_ticket_number=sr_ticket_number
                                              AND ss_item_sk=sr_item_sk)
                  WHERE i_category='Books'
                  UNION SELECT d_year ,
                               i_brand_id ,
                               i_class_id ,
                               i_category_id ,
                               i_manufact_id ,
                               ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt ,
                               ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
                  FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales
                  JOIN read_parquet('{TpcdsData.ItemPath}') item ON i_item_sk=ws_item_sk
                  JOIN read_parquet('{TpcdsData.DateDimPath}') date_dim ON d_date_sk=ws_sold_date_sk
                  LEFT JOIN read_parquet('{TpcdsData.WebReturnsPath}') web_returns ON (ws_order_number=wr_order_number
                                            AND ws_item_sk=wr_item_sk)
                  WHERE i_category='Books') sales_detail
               GROUP BY d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id)
            SELECT prev_yr.d_year AS prev_year ,
                   curr_yr.d_year AS year_ ,
                   curr_yr.i_brand_id ,
                   curr_yr.i_class_id ,
                   curr_yr.i_category_id ,
                   curr_yr.i_manufact_id ,
                   prev_yr.sales_cnt AS prev_yr_cnt ,
                   curr_yr.sales_cnt AS curr_yr_cnt ,
                   curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff ,
                   curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
            FROM all_sales curr_yr,
                 all_sales prev_yr
            WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
              AND curr_yr.i_class_id=prev_yr.i_class_id
              AND curr_yr.i_category_id=prev_yr.i_category_id
              AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
              AND curr_yr.d_year=2002
              AND prev_yr.d_year=2002-1
              AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
            ORDER BY sales_cnt_diff NULLS FIRST,
                     sales_amt_diff NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            prev_year = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            year_ = r.IsDBNull(1) ? (long?)null : r.GetInt64(1),
            i_brand_id = r.IsDBNull(2) ? (long?)null : r.GetInt64(2),
            i_class_id = r.IsDBNull(3) ? (long?)null : r.GetInt64(3),
            i_category_id = r.IsDBNull(4) ? (long?)null : r.GetInt64(4),
            i_manufact_id = r.IsDBNull(5) ? (long?)null : r.GetInt64(5),
            prev_yr_cnt = r.IsDBNull(6) ? (long?)null : (long)r.GetFieldValue<System.Numerics.BigInteger>(6),
            curr_yr_cnt = r.IsDBNull(7) ? (long?)null : (long)r.GetFieldValue<System.Numerics.BigInteger>(7),
            sales_cnt_diff = r.IsDBNull(8) ? (long?)null : (long)r.GetFieldValue<System.Numerics.BigInteger>(8),
            sales_amt_diff = r.IsDBNull(9) ? (decimal?)null : r.GetDecimal(9)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].prev_year, polarsResult[i].prev_year);
            Assert.Equal(duckDbResult[i].year_, polarsResult[i].year_);
            Assert.Equal(duckDbResult[i].i_brand_id, polarsResult[i].i_brand_id);
            Assert.Equal(duckDbResult[i].i_class_id, polarsResult[i].i_class_id);
            Assert.Equal(duckDbResult[i].i_category_id, polarsResult[i].i_category_id);
            Assert.Equal(duckDbResult[i].i_manufact_id, polarsResult[i].i_manufact_id);
            Assert.Equal(duckDbResult[i].prev_yr_cnt, polarsResult[i].prev_yr_cnt);
            Assert.Equal(duckDbResult[i].curr_yr_cnt, polarsResult[i].curr_yr_cnt);
            Assert.Equal(duckDbResult[i].sales_cnt_diff, polarsResult[i].sales_cnt_diff);
            Assert.Equal(duckDbResult[i].sales_amt_diff ?? 0m, polarsResult[i].sales_amt_diff ?? 0m, 2);
        }
    }

    /*
     * TPC-DS Query 76
     *
     * SELECT channel,
     *        col_name,
     *        d_year,
     *        d_qoy,
     *        i_category,
     *        COUNT(*) sales_cnt,
     *        SUM(ext_sales_price) sales_amt
     * FROM
     *   ( SELECT 'store' AS channel,
     *            'ss_store_sk' col_name,
     *                          d_year,
     *                          d_qoy,
     *                          i_category,
     *                          ss_ext_sales_price ext_sales_price
     *    FROM store_sales,
     *         item,
     *         date_dim
     *    WHERE ss_store_sk IS NULL
     *      AND ss_sold_date_sk=d_date_sk
     *      AND ss_item_sk=i_item_sk
     *    UNION ALL SELECT 'web' AS channel,
     *                     'ws_ship_customer_sk' col_name,
     *                                           d_year,
     *                                           d_qoy,
     *                                           i_category,
     *                                           ws_ext_sales_price ext_sales_price
     *    FROM web_sales,
     *         item,
     *         date_dim
     *    WHERE ws_ship_customer_sk IS NULL
     *      AND ws_sold_date_sk=d_date_sk
     *      AND ws_item_sk=i_item_sk
     *    UNION ALL SELECT 'catalog' AS channel,
     *                     'cs_ship_addr_sk' col_name,
     *                                       d_year,
     *                                       d_qoy,
     *                                       i_category,
     *                                       cs_ext_sales_price ext_sales_price
     *    FROM catalog_sales,
     *         item,
     *         date_dim
     *    WHERE cs_ship_addr_sk IS NULL
     *      AND cs_sold_date_sk=d_date_sk
     *      AND cs_item_sk=i_item_sk) foo
     * GROUP BY channel,
     *          col_name,
     *          d_year,
     *          d_qoy,
     *          i_category
     * ORDER BY channel NULLS FIRST,
     *          col_name NULLS FIRST,
     *          d_year NULLS FIRST,
     *          d_qoy NULLS FIRST,
     *          i_category NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q76()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var storeFoo = from ss in storeSalesQuery
                       where ss.ss_store_sk == null
                       join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                       join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                       select new
                       {
                           channel = "store",
                           col_name = "ss_store_sk",
                           d.d_year,
                           d.d_qoy,
                           i.i_category,
                           ext_sales_price = ss.ss_ext_sales_price
                       };

        var webFoo = from ws in webSalesQuery
                     where ws.ws_ship_customer_sk == null
                     join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                     join i in itemQuery on ws.ws_item_sk equals i.i_item_sk
                     select new
                     {
                         channel = "web",
                         col_name = "ws_ship_customer_sk",
                         d.d_year,
                         d.d_qoy,
                         i.i_category,
                         ext_sales_price = ws.ws_ext_sales_price
                     };

        var catalogFoo = from cs in catalogSalesQuery
                         where cs.cs_ship_addr_sk == null
                         join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                         join i in itemQuery on cs.cs_item_sk equals i.i_item_sk
                         select new
                         {
                             channel = "catalog",
                             col_name = "cs_ship_addr_sk",
                             d.d_year,
                             d.d_qoy,
                             i.i_category,
                             ext_sales_price = cs.cs_ext_sales_price
                         };

        var foo = storeFoo.Concat(webFoo).Concat(catalogFoo);

        var q76 = (from x in foo
                   group x by new
                   {
                       x.channel,
                       x.col_name,
                       x.d_year,
                       x.d_qoy,
                       x.i_category
                   } into g
                   orderby g.Key.channel, g.Key.col_name, g.Key.d_year, g.Key.d_qoy, g.Key.i_category
                   select new
                   {
                       g.Key.channel,
                       g.Key.col_name,
                       g.Key.d_year,
                       g.Key.d_qoy,
                       g.Key.i_category,
                       sales_cnt = g.LongCount(),
                       sales_amt = g.Sum(x => x.ext_sales_price)
                   }).Take(100);

        AssertPlan(q76, NativePushdown);

        var polarsResult = q76.ToList();

        var duckDbSql = $@"
            SELECT channel,
                   col_name,
                   d_year,
                   d_qoy,
                   i_category,
                   COUNT(*) sales_cnt,
                   SUM(ext_sales_price) sales_amt
            FROM
              ( SELECT 'store' AS channel,
                       'ss_store_sk' col_name,
                                     d_year,
                                     d_qoy,
                                     i_category,
                                     ss_ext_sales_price ext_sales_price
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE ss_store_sk IS NULL
                 AND ss_sold_date_sk=d_date_sk
                 AND ss_item_sk=i_item_sk
               UNION ALL SELECT 'web' AS channel,
                                'ws_ship_customer_sk' col_name,
                                                      d_year,
                                                      d_qoy,
                                                      i_category,
                                                      ws_ext_sales_price ext_sales_price
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                    read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE ws_ship_customer_sk IS NULL
                 AND ws_sold_date_sk=d_date_sk
                 AND ws_item_sk=i_item_sk
               UNION ALL SELECT 'catalog' AS channel,
                                'cs_ship_addr_sk' col_name,
                                                  d_year,
                                                  d_qoy,
                                                  i_category,
                                                  cs_ext_sales_price ext_sales_price
               FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales,
                    read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE cs_ship_addr_sk IS NULL
                 AND cs_sold_date_sk=d_date_sk
                 AND cs_item_sk=i_item_sk) foo
            GROUP BY channel,
                     col_name,
                     d_year,
                     d_qoy,
                     i_category
            ORDER BY channel NULLS FIRST,
                     col_name NULLS FIRST,
                     d_year NULLS FIRST,
                     d_qoy NULLS FIRST,
                     i_category NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            channel = r.GetString(0),
            col_name = r.GetString(1),
            d_year = r.IsDBNull(2) ? (long?)null : r.GetInt64(2),
            d_qoy = r.IsDBNull(3) ? (long?)null : r.GetInt64(3),
            i_category = r.IsDBNull(4) ? null : r.GetString(4),
            sales_cnt = r.GetInt64(5),
            sales_amt = r.IsDBNull(6) ? (decimal?)null : r.GetDecimal(6)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].channel, polarsResult[i].channel);
            Assert.Equal(duckDbResult[i].col_name, polarsResult[i].col_name);
            Assert.Equal(duckDbResult[i].d_year, polarsResult[i].d_year);
            Assert.Equal(duckDbResult[i].d_qoy, polarsResult[i].d_qoy);
            Assert.Equal(duckDbResult[i].i_category, polarsResult[i].i_category);
            Assert.Equal(duckDbResult[i].sales_cnt, polarsResult[i].sales_cnt);
            Assert.Equal(duckDbResult[i].sales_amt ?? 0m, polarsResult[i].sales_amt ?? 0m, 2);
        }
    }

    /*
     * TPC-DS Query 77
     *
     * WITH ss AS
     *   (SELECT s_store_sk,
     *           sum(ss_ext_sales_price) AS sales,
     *           sum(ss_net_profit) AS profit
     *    FROM store_sales,
     *         date_dim,
     *         store
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *      AND ss_store_sk = s_store_sk
     *    GROUP BY s_store_sk) ,
     *      sr AS
     *   (SELECT s_store_sk,
     *           sum(sr_return_amt) AS returns_,
     *           sum(sr_net_loss) AS profit_loss
     *    FROM store_returns,
     *         date_dim,
     *         store
     *    WHERE sr_returned_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *      AND sr_store_sk = s_store_sk
     *    GROUP BY s_store_sk),
     *      cs AS
     *   (SELECT cs_call_center_sk,
     *           sum(cs_ext_sales_price) AS sales,
     *           sum(cs_net_profit) AS profit
     *    FROM catalog_sales,
     *         date_dim
     *    WHERE cs_sold_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *    GROUP BY cs_call_center_sk),
     *      cr AS
     *   (SELECT cr_call_center_sk,
     *           sum(cr_return_amount) AS returns_,
     *           sum(cr_net_loss) AS profit_loss
     *    FROM catalog_returns,
     *         date_dim
     *    WHERE cr_returned_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *    GROUP BY cr_call_center_sk ),
     *      ws AS
     *   (SELECT wp_web_page_sk,
     *           sum(ws_ext_sales_price) AS sales,
     *           sum(ws_net_profit) AS profit
     *    FROM web_sales,
     *         date_dim,
     *         web_page
     *    WHERE ws_sold_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *      AND ws_web_page_sk = wp_web_page_sk
     *    GROUP BY wp_web_page_sk),
     *      wr AS
     *   (SELECT wp_web_page_sk,
     *           sum(wr_return_amt) AS returns_,
     *           sum(wr_net_loss) AS profit_loss
     *    FROM web_returns,
     *         date_dim,
     *         web_page
     *    WHERE wr_returned_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *      AND wr_web_page_sk = wp_web_page_sk
     *    GROUP BY wp_web_page_sk)
     * SELECT channel ,
     *        id ,
     *        sum(sales) AS sales ,
     *        sum(returns_) AS returns_ ,
     *        sum(profit) AS profit
     * FROM
     *   (SELECT 'store channel' AS channel ,
     *           ss.s_store_sk AS id ,
     *           sales ,
     *           coalesce(returns_, 0) AS returns_ ,
     *           (profit - coalesce(profit_loss,0)) AS profit
     *    FROM ss
     *    LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
     *    UNION ALL SELECT 'catalog channel' AS channel ,
     *                     cs_call_center_sk AS id ,
     *                     sales ,
     *                     returns_ ,
     *                     (profit - profit_loss) AS profit
     *    FROM cs ,
     *         cr
     *    UNION ALL SELECT 'web channel' AS channel ,
     *                     ws.wp_web_page_sk AS id ,
     *                     sales ,
     *                     coalesce(returns_, 0) returns_ ,
     *                     (profit - coalesce(profit_loss,0)) AS profit
     *    FROM ws
     *    LEFT JOIN wr ON ws.wp_web_page_sk = wr.wp_web_page_sk ) x
     * GROUP BY ROLLUP (channel,
     *                  id)
     * ORDER BY channel NULLS FIRST,
     *          id NULLS FIRST,
     *          returns_ DESC
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q77()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var webPageQuery = TpcdsData.WebPageQuery();

        var minDate = new DateOnly(2000, 8, 23);
        var maxDate = new DateOnly(2000, 9, 22);
        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);

        var ss = from s in storeSalesQuery
                 join d in filteredDates on s.ss_sold_date_sk equals d.d_date_sk
                 join st in storeQuery on s.ss_store_sk equals st.s_store_sk
                 group s by st.s_store_sk into g
                 select new
                 {
                     s_store_sk = (long?)g.Key,
                     sales = g.Sum(x => x.ss_ext_sales_price),
                     profit = g.Sum(x => x.ss_net_profit)
                 };

        var sr = from r in storeReturnsQuery
                 join d in filteredDates on r.sr_returned_date_sk equals d.d_date_sk
                 join st in storeQuery on r.sr_store_sk equals st.s_store_sk
                 group r by st.s_store_sk into g
                 select new
                 {
                     s_store_sk = (long?)g.Key,
                     returns_ = g.Sum(x => x.sr_return_amt),
                     profit_loss = g.Sum(x => x.sr_net_loss)
                 };

        var storeChan = from s in ss
                        join r in sr on s.s_store_sk equals r.s_store_sk into rGroup
                        from r in rGroup.DefaultIfEmpty()
                        select new
                        {
                            channel = "store channel",
                            id = s.s_store_sk,
                            sales = s.sales,
                            returns_ = (decimal?)(r.returns_ ?? 0.00m),
                            profit = s.profit - (r.profit_loss ?? 0.00m)
                        };

        var cs = from s in catalogSalesQuery
                 join d in filteredDates on s.cs_sold_date_sk equals d.d_date_sk
                 group s by s.cs_call_center_sk into g
                 select new
                 {
                     key = 1,
                     cs_call_center_sk = g.Key,
                     sales = g.Sum(x => x.cs_ext_sales_price),
                     profit = g.Sum(x => x.cs_net_profit)
                 };

        var cr = from r in catalogReturnsQuery
                 join d in filteredDates on r.cr_returned_date_sk equals d.d_date_sk
                 group r by r.cr_call_center_sk into g
                 select new
                 {
                     key = 1,
                     cr_call_center_sk = g.Key,
                     returns_ = g.Sum(x => x.cr_return_amount),
                     profit_loss = g.Sum(x => x.cr_net_loss)
                 };

        var catalogChan = from s in cs
                          join r in cr on s.key equals r.key
                          select new
                          {
                              channel = "catalog channel",
                              id = s.cs_call_center_sk,
                              sales = s.sales,
                              returns_ = r.returns_,
                              profit = s.profit - r.profit_loss
                          };

        var ws = from s in webSalesQuery
                 join d in filteredDates on s.ws_sold_date_sk equals d.d_date_sk
                 join wp in webPageQuery on s.ws_web_page_sk equals wp.wp_web_page_sk
                 group s by wp.wp_web_page_sk into g
                 select new
                 {
                     wp_web_page_sk = (long?)g.Key,
                     sales = g.Sum(x => x.ws_ext_sales_price),
                     profit = g.Sum(x => x.ws_net_profit)
                 };

        var wr = from r in webReturnsQuery
                 join d in filteredDates on r.wr_returned_date_sk equals d.d_date_sk
                 join wp in webPageQuery on r.wr_web_page_sk equals wp.wp_web_page_sk
                 group r by wp.wp_web_page_sk into g
                 select new
                 {
                     wp_web_page_sk = (long?)g.Key,
                     returns_ = g.Sum(x => x.wr_return_amt),
                     profit_loss = g.Sum(x => x.wr_net_loss)
                 };

        var webChan = from s in ws
                      join r in wr on s.wp_web_page_sk equals r.wp_web_page_sk into rGroup
                      from r in rGroup.DefaultIfEmpty()
                      select new
                      {
                          channel = "web channel",
                          id = s.wp_web_page_sk,
                          sales = s.sales,
                          returns_ = (decimal?)(r.returns_ ?? 0.00m),
                          profit = s.profit - (r.profit_loss ?? 0.00m)
                      };

        var x = storeChan.Concat(catalogChan).Concat(webChan);

        var q77 = x
            .GroupByRollup(
                r => new { r.channel, r.id },
                (key, g) => new
                {
                    channel = (string?)key.channel,
                    id = (long?)key.id,
                    sales = g.Sum(v => v.sales),
                    returns_ = g.Sum(v => v.returns_),
                    profit = g.Sum(v => v.profit)
                }
            )
            .OrderBy(r => r.channel)
            .ThenBy(r => r.id)
            .ThenByDescending(r => r.returns_)
            .Take(100);

        AssertPlan(q77, NativePushdown);

        var polarsResult = q77.ToList();

        var duckDbSql = $@"
            WITH ss AS
              (SELECT s_store_sk,
                      sum(ss_ext_sales_price) AS sales,
                      sum(ss_net_profit) AS profit
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
                 AND ss_store_sk = s_store_sk
               GROUP BY s_store_sk) ,
                 sr AS
              (SELECT s_store_sk,
                      sum(sr_return_amt) AS returns_,
                      sum(sr_net_loss) AS profit_loss
               FROM read_parquet('{TpcdsData.StoreReturnsPath}') store_returns,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE sr_returned_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
                 AND sr_store_sk = s_store_sk
               GROUP BY s_store_sk),
                 cs AS
              (SELECT cs_call_center_sk,
                      sum(cs_ext_sales_price) AS sales,
                      sum(cs_net_profit) AS profit
               FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE cs_sold_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
               GROUP BY cs_call_center_sk),
                 cr AS
              (SELECT cr_call_center_sk,
                      sum(cr_return_amount) AS returns_,
                      sum(cr_net_loss) AS profit_loss
               FROM read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE cr_returned_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
               GROUP BY cr_call_center_sk ),
                 ws AS
              (SELECT wp_web_page_sk,
                      sum(ws_ext_sales_price) AS sales,
                      sum(ws_net_profit) AS profit
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.WebPagePath}') web_page
               WHERE ws_sold_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
                 AND ws_web_page_sk = wp_web_page_sk
               GROUP BY wp_web_page_sk),
                 wr AS
              (SELECT wp_web_page_sk,
                      sum(wr_return_amt) AS returns_,
                      sum(wr_net_loss) AS profit_loss
               FROM read_parquet('{TpcdsData.WebReturnsPath}') web_returns,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.WebPagePath}') web_page
               WHERE wr_returned_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
                 AND wr_web_page_sk = wp_web_page_sk
               GROUP BY wp_web_page_sk)
            SELECT channel ,
                   id ,
                   sum(sales) AS sales ,
                   sum(returns_) AS returns_ ,
                   sum(profit) AS profit
            FROM
              (SELECT 'store channel' AS channel ,
                      ss.s_store_sk AS id ,
                      sales ,
                      coalesce(returns_, 0) AS returns_ ,
                      (profit - coalesce(profit_loss,0)) AS profit
               FROM ss
               LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
               UNION ALL SELECT 'catalog channel' AS channel ,
                                cs_call_center_sk AS id ,
                                sales ,
                                returns_ ,
                                (profit - profit_loss) AS profit
               FROM cs ,
                    cr
               UNION ALL SELECT 'web channel' AS channel ,
                                ws.wp_web_page_sk AS id ,
                                sales ,
                                coalesce(returns_, 0) returns_ ,
                                (profit - coalesce(profit_loss,0)) AS profit
               FROM ws
               LEFT JOIN wr ON ws.wp_web_page_sk = wr.wp_web_page_sk ) x
            GROUP BY ROLLUP (channel,
                             id)
            ORDER BY channel NULLS FIRST,
                     id NULLS FIRST,
                     returns_ DESC NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            channel = r.IsDBNull(0) ? null : r.GetString(0),
            id = r.IsDBNull(1) ? (long?)null : r.GetInt64(1),
            sales = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2),
            returns_ = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3),
            profit = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].channel, polarsResult[i].channel);
            Assert.Equal(duckDbResult[i].id, polarsResult[i].id);
            Assert.Equal(duckDbResult[i].sales ?? 0m, polarsResult[i].sales ?? 0m, 2);
            Assert.Equal(duckDbResult[i].returns_ ?? 0m, polarsResult[i].returns_ ?? 0m, 2);
            Assert.Equal(duckDbResult[i].profit ?? 0m, polarsResult[i].profit ?? 0m, 2);
        }
    }

    /*
     * TPC-DS Query 78
     *
     * WITH ws AS
     *   (SELECT d_year AS ws_sold_year,
     *           ws_item_sk,
     *           ws_bill_customer_sk ws_customer_sk,
     *           sum(ws_quantity) ws_qty,
     *           sum(ws_wholesale_cost) ws_wc,
     *           sum(ws_sales_price) ws_sp
     *    FROM web_sales
     *    LEFT JOIN web_returns ON wr_order_number=ws_order_number
     *    AND ws_item_sk=wr_item_sk
     *    JOIN date_dim ON ws_sold_date_sk = d_date_sk
     *    WHERE wr_order_number IS NULL
     *    GROUP BY d_year,
     *             ws_item_sk,
     *             ws_bill_customer_sk ),
     *      cs AS
     *   (SELECT d_year AS cs_sold_year,
     *           cs_item_sk,
     *           cs_bill_customer_sk cs_customer_sk,
     *           sum(cs_quantity) cs_qty,
     *           sum(cs_wholesale_cost) cs_wc,
     *           sum(cs_sales_price) cs_sp
     *    FROM catalog_sales
     *    LEFT JOIN catalog_returns ON cr_order_number=cs_order_number
     *    AND cs_item_sk=cr_item_sk
     *    JOIN date_dim ON cs_sold_date_sk = d_date_sk
     *    WHERE cr_order_number IS NULL
     *    GROUP BY d_year,
     *             cs_item_sk,
     *             cs_bill_customer_sk ),
     *      ss AS
     *   (SELECT d_year AS ss_sold_year,
     *           ss_item_sk,
     *           ss_customer_sk,
     *           sum(ss_quantity) ss_qty,
     *           sum(ss_wholesale_cost) ss_wc,
     *           sum(ss_sales_price) ss_sp
     *    FROM store_sales
     *    LEFT JOIN store_returns ON sr_ticket_number=ss_ticket_number
     *    AND ss_item_sk=sr_item_sk
     *    JOIN date_dim ON ss_sold_date_sk = d_date_sk
     *    WHERE sr_ticket_number IS NULL
     *    GROUP BY d_year,
     *             ss_item_sk,
     *             ss_customer_sk )
     * SELECT ss_sold_year,
     *        ss_item_sk,
     *        ss_customer_sk,
     *        round((ss_qty*1.00)/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
     *        ss_qty store_qty,
     *        ss_wc store_wholesale_cost,
     *        ss_sp store_sales_price,
     *        coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
     *        coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
     *        coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
     * FROM ss
     * LEFT JOIN ws ON (ws_sold_year=ss_sold_year
     *                  AND ws_item_sk=ss_item_sk
     *                  AND ws_customer_sk=ss_customer_sk)
     * LEFT JOIN cs ON (cs_sold_year=ss_sold_year
     *                  AND cs_item_sk=ss_item_sk
     *                  AND cs_customer_sk=ss_customer_sk)
     * WHERE (coalesce(ws_qty,0)>0
     *        OR coalesce(cs_qty, 0)>0)
     *   AND ss_sold_year=2000
     * ORDER BY ss_sold_year,
     *          ss_item_sk,
     *          ss_customer_sk,
     *          ss_qty DESC,
     *          ss_wc DESC,
     *          ss_sp DESC,
     *          other_chan_qty,
     *          other_chan_wholesale_cost,
     *          other_chan_sales_price,
     *          ratio
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q78()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var dates2000 = dateDimQuery.Where(d => d.d_year == 2000);

#pragma warning disable CS0472
        var ws = from w in webSalesQuery
                 join r in webReturnsQuery on new { order_number = (long?)w.ws_order_number, item_sk = (long?)w.ws_item_sk }
                                       equals new { order_number = (long?)r.wr_order_number, item_sk = (long?)r.wr_item_sk } into rGroup
                 from r in rGroup.DefaultIfEmpty()
                 join d in dates2000 on w.ws_sold_date_sk equals d.d_date_sk
                 where (long?)r.wr_order_number == null
                 group w by new { d.d_year, w.ws_item_sk, w.ws_bill_customer_sk } into g
                 select new
                 {
                     ws_sold_year = g.Key.d_year,
                     ws_item_sk = (long?)g.Key.ws_item_sk,
                     ws_customer_sk = g.Key.ws_bill_customer_sk,
                     ws_qty = g.Sum(x => x.ws_quantity),
                     ws_wc = g.Sum(x => x.ws_wholesale_cost),
                     ws_sp = g.Sum(x => x.ws_sales_price)
                 };

        var cs = from c in catalogSalesQuery
                 join r in catalogReturnsQuery on new { order_number = (long?)c.cs_order_number, item_sk = (long?)c.cs_item_sk }
                                           equals new { order_number = (long?)r.cr_order_number, item_sk = (long?)r.cr_item_sk } into rGroup
                 from r in rGroup.DefaultIfEmpty()
                 join d in dates2000 on c.cs_sold_date_sk equals d.d_date_sk
                 where (long?)r.cr_order_number == null
                 group c by new { d.d_year, c.cs_item_sk, c.cs_bill_customer_sk } into g
                 select new
                 {
                     cs_sold_year = g.Key.d_year,
                     cs_item_sk = (long?)g.Key.cs_item_sk,
                     cs_customer_sk = g.Key.cs_bill_customer_sk,
                     cs_qty = g.Sum(x => x.cs_quantity),
                     cs_wc = g.Sum(x => x.cs_wholesale_cost),
                     cs_sp = g.Sum(x => x.cs_sales_price)
                 };

        var ss = from s in storeSalesQuery
                 join r in storeReturnsQuery on new { ticket_number = (long?)s.ss_ticket_number, item_sk = (long?)s.ss_item_sk }
                                         equals new { ticket_number = (long?)r.sr_ticket_number, item_sk = (long?)r.sr_item_sk } into rGroup
                 from r in rGroup.DefaultIfEmpty()
                 join d in dates2000 on s.ss_sold_date_sk equals d.d_date_sk
                 where (long?)r.sr_ticket_number == null
                 group s by new { d.d_year, s.ss_item_sk, s.ss_customer_sk } into g
                 select new
                 {
                     ss_sold_year = g.Key.d_year,
                     ss_item_sk = (long?)g.Key.ss_item_sk,
                     ss_customer_sk = g.Key.ss_customer_sk,
                     ss_qty = g.Sum(x => x.ss_quantity),
                     ss_wc = g.Sum(x => x.ss_wholesale_cost),
                     ss_sp = g.Sum(x => x.ss_sales_price)
                 };
#pragma warning restore CS0472

        var joined = from s in ss
                     join w in ws on new { year = s.ss_sold_year, item = s.ss_item_sk, cust = s.ss_customer_sk }
                              equals new { year = w.ws_sold_year, item = w.ws_item_sk, cust = w.ws_customer_sk } into wGroup
                     from w in wGroup.DefaultIfEmpty()
                     join c in cs on new { year = s.ss_sold_year, item = s.ss_item_sk, cust = s.ss_customer_sk }
                              equals new { year = c.cs_sold_year, item = c.cs_item_sk, cust = c.cs_customer_sk } into cGroup
                     from c in cGroup.DefaultIfEmpty()
                     where (w.ws_qty ?? 0L) > 0L || (c.cs_qty ?? 0L) > 0L
                     select new
                     {
                         s.ss_sold_year,
                         s.ss_item_sk,
                         s.ss_customer_sk,
                         ratio = Math.Round((double)s.ss_qty! / (double)((w.ws_qty ?? 0L) + (c.cs_qty ?? 0L)), 2, MidpointRounding.AwayFromZero),
                         store_qty = s.ss_qty,
                         store_wholesale_cost = s.ss_wc,
                         store_sales_price = s.ss_sp,
                         other_chan_qty = (w.ws_qty ?? 0L) + (c.cs_qty ?? 0L),
                         other_chan_wholesale_cost = (w.ws_wc ?? 0.00m) + (c.cs_wc ?? 0.00m),
                         other_chan_sales_price = (w.ws_sp ?? 0.00m) + (c.cs_sp ?? 0.00m)
                     };

        var q78 = (from x in joined
                   orderby x.ss_sold_year,
                           x.ss_item_sk,
                           x.ss_customer_sk,
                           x.store_qty descending,
                           x.store_wholesale_cost descending,
                           x.store_sales_price descending,
                           x.other_chan_qty,
                           x.other_chan_wholesale_cost,
                           x.other_chan_sales_price,
                           x.ratio
                   select x).Take(100);

        AssertPlan(q78, NativePushdown);

        var polarsResult = q78.ToList();

        var duckDbSql = $@"
            WITH ws AS
              (SELECT d_year AS ws_sold_year,
                      ws_item_sk,
                      ws_bill_customer_sk ws_customer_sk,
                      sum(ws_quantity) ws_qty,
                      sum(ws_wholesale_cost) ws_wc,
                      sum(ws_sales_price) ws_sp
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales
               LEFT JOIN read_parquet('{TpcdsData.WebReturnsPath}') web_returns ON wr_order_number=ws_order_number
               AND ws_item_sk=wr_item_sk
               JOIN read_parquet('{TpcdsData.DateDimPath}') date_dim ON ws_sold_date_sk = d_date_sk
               WHERE wr_order_number IS NULL
               GROUP BY d_year,
                        ws_item_sk,
                        ws_bill_customer_sk ),
                 cs AS
              (SELECT d_year AS cs_sold_year,
                      cs_item_sk,
                      cs_bill_customer_sk cs_customer_sk,
                      sum(cs_quantity) cs_qty,
                      sum(cs_wholesale_cost) cs_wc,
                      sum(cs_sales_price) cs_sp
               FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales
               LEFT JOIN read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns ON cr_order_number=cs_order_number
               AND cs_item_sk=cr_item_sk
               JOIN read_parquet('{TpcdsData.DateDimPath}') date_dim ON cs_sold_date_sk = d_date_sk
               WHERE cr_order_number IS NULL
               GROUP BY d_year,
                        cs_item_sk,
                        cs_bill_customer_sk ),
                 ss AS
              (SELECT d_year AS ss_sold_year,
                      ss_item_sk,
                      ss_customer_sk,
                      sum(ss_quantity) ss_qty,
                      sum(ss_wholesale_cost) ss_wc,
                      sum(ss_sales_price) ss_sp
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales
               LEFT JOIN read_parquet('{TpcdsData.StoreReturnsPath}') store_returns ON sr_ticket_number=ss_ticket_number
               AND ss_item_sk=sr_item_sk
               JOIN read_parquet('{TpcdsData.DateDimPath}') date_dim ON ss_sold_date_sk = d_date_sk
               WHERE sr_ticket_number IS NULL
               GROUP BY d_year,
                        ss_item_sk,
                        ss_customer_sk )
            SELECT ss_sold_year,
                   ss_item_sk,
                   ss_customer_sk,
                   round((ss_qty*1.00)/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
                   ss_qty store_qty,
                   ss_wc store_wholesale_cost,
                   ss_sp store_sales_price,
                   coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
                   coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
                   coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
            FROM ss
            LEFT JOIN ws ON (ws_sold_year=ss_sold_year
                             AND ws_item_sk=ss_item_sk
                             AND ws_customer_sk=ss_customer_sk)
            LEFT JOIN cs ON (cs_sold_year=ss_sold_year
                             AND cs_item_sk=ss_item_sk
                             AND cs_customer_sk=ss_customer_sk)
            WHERE (coalesce(ws_qty,0)>0
                   OR coalesce(cs_qty, 0)>0)
              AND ss_sold_year=2000
            ORDER BY ss_sold_year NULLS FIRST,
                     ss_item_sk NULLS FIRST,
                     ss_customer_sk NULLS FIRST,
                     ss_qty DESC NULLS FIRST,
                     ss_wc DESC NULLS FIRST,
                     ss_sp DESC NULLS FIRST,
                     other_chan_qty NULLS FIRST,
                     other_chan_wholesale_cost NULLS FIRST,
                     other_chan_sales_price NULLS FIRST,
                     ratio NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            ss_sold_year = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            ss_item_sk = r.IsDBNull(1) ? (long?)null : r.GetInt64(1),
            ss_customer_sk = r.IsDBNull(2) ? (long?)null : r.GetInt64(2),
            ratio = r.IsDBNull(3) ? 0.0 : r.GetDouble(3),
            store_qty = r.IsDBNull(4) ? (long?)null : (long)r.GetFieldValue<System.Numerics.BigInteger>(4),
            store_wholesale_cost = r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5),
            store_sales_price = r.IsDBNull(6) ? (decimal?)null : r.GetDecimal(6),
            other_chan_qty = r.IsDBNull(7) ? 0L : (long)r.GetFieldValue<System.Numerics.BigInteger>(7),
            other_chan_wholesale_cost = r.IsDBNull(8) ? 0m : r.GetDecimal(8),
            other_chan_sales_price = r.IsDBNull(9) ? 0m : r.GetDecimal(9)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].ss_sold_year, polarsResult[i].ss_sold_year);
            Assert.Equal(duckDbResult[i].ss_item_sk, polarsResult[i].ss_item_sk);
            Assert.Equal(duckDbResult[i].ss_customer_sk, polarsResult[i].ss_customer_sk);
            Assert.Equal(duckDbResult[i].ratio, polarsResult[i].ratio, 2);
            Assert.Equal(duckDbResult[i].store_qty ?? 0L, polarsResult[i].store_qty ?? 0L);
            Assert.Equal(duckDbResult[i].store_wholesale_cost ?? 0m, polarsResult[i].store_wholesale_cost ?? 0m, 2);
            Assert.Equal(duckDbResult[i].store_sales_price ?? 0m, polarsResult[i].store_sales_price ?? 0m, 2);
            Assert.Equal(duckDbResult[i].other_chan_qty, polarsResult[i].other_chan_qty);
            Assert.Equal(duckDbResult[i].other_chan_wholesale_cost, polarsResult[i].other_chan_wholesale_cost, 2);
            Assert.Equal(duckDbResult[i].other_chan_sales_price, polarsResult[i].other_chan_sales_price, 2);
        }
    }

    /*
     * TPC-DS Query 79
     *
     * SELECT c_last_name,
     *        c_first_name,
     *        SUBSTRING(s_city,1,30),
     *        ss_ticket_number,
     *        amt,
     *        profit
     * FROM
     *   (SELECT ss_ticket_number ,
     *           ss_customer_sk ,
     *           store.s_city ,
     *           sum(ss_coupon_amt) amt ,
     *           sum(ss_net_profit) profit
     *    FROM store_sales,
     *         date_dim,
     *         store,
     *         household_demographics
     *    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     *      AND store_sales.ss_store_sk = store.s_store_sk
     *      AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     *      AND (household_demographics.hd_dep_count = 6
     *           OR household_demographics.hd_vehicle_count > 2)
     *      AND date_dim.d_dow = 1
     *      AND date_dim.d_year IN (1999,
     *                              1999+1,
     *                              1999+2)
     *      AND store.s_number_employees BETWEEN 200 AND 295
     *    GROUP BY ss_ticket_number,
     *             ss_customer_sk,
     *             ss_addr_sk,
     *             store.s_city) ms,
     *      customer
     * WHERE ss_customer_sk = c_customer_sk
     * ORDER BY c_last_name  NULLS FIRST,
     *          c_first_name  NULLS FIRST,
     *          SUBSTRING(s_city,1,30)  NULLS FIRST,
     *          profit NULLS FIRST,
     *          ss_ticket_number
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q79()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var hdemoQuery = TpcdsData.HouseholdDemographicsQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        long[] years = [1999, 2000, 2001];

        var ms = from ss in storeSalesQuery
                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                 join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                 join hd in hdemoQuery on ss.ss_hdemo_sk equals hd.hd_demo_sk
                 where (hd.hd_dep_count == 6 || hd.hd_vehicle_count > 2)
                    && d.d_dow == 1
                    && years.Contains(d.d_year!.Value)
                    && s.s_number_employees >= 200 && s.s_number_employees <= 295
                 group ss by new
                 {
                     ss.ss_ticket_number,
                     ss.ss_customer_sk,
                     ss.ss_addr_sk,
                     s.s_city
                 } into g
                 select new
                 {
                     g.Key.ss_ticket_number,
                     g.Key.ss_customer_sk,
                     g.Key.s_city,
                     amt = g.Sum(x => x.ss_coupon_amt),
                     profit = g.Sum(x => x.ss_net_profit)
                 };

        var q79 = (from m in ms
                   join c in customerQuery on m.ss_customer_sk equals c.c_customer_sk
                   let s_city_sub = m.s_city!.Substring(0, 30)
                   orderby c.c_last_name, c.c_first_name, s_city_sub, m.profit, m.ss_ticket_number, m.amt
                   select new
                   {
                       c.c_last_name,
                       c.c_first_name,
                       s_city_sub,
                       m.ss_ticket_number,
                       m.amt,
                       m.profit
                   }).Take(100);

        AssertPlan(q79, NativePushdown);

        var polarsResult = q79.ToList();

        var duckDbSql = $@"
            SELECT c_last_name,
                   c_first_name,
                   SUBSTRING(s_city,1,30),
                   ss_ticket_number,
                   amt,
                   profit
            FROM
              (SELECT ss_ticket_number ,
                      ss_customer_sk ,
                      store.s_city ,
                      sum(ss_coupon_amt) amt ,
                      sum(ss_net_profit) profit
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics
               WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
                 AND store_sales.ss_store_sk = store.s_store_sk
                 AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND (household_demographics.hd_dep_count = 6
                      OR household_demographics.hd_vehicle_count > 2)
                 AND date_dim.d_dow = 1
                 AND date_dim.d_year IN (1999,
                                         1999+1,
                                         1999+2)
                 AND store.s_number_employees BETWEEN 200 AND 295
               GROUP BY ss_ticket_number,
                        ss_customer_sk,
                        ss_addr_sk,
                        store.s_city) ms,
                 read_parquet('{TpcdsData.CustomerPath}') customer
            WHERE ss_customer_sk = c_customer_sk
            ORDER BY c_last_name NULLS FIRST,
                     c_first_name NULLS FIRST,
                     SUBSTRING(s_city,1,30) NULLS FIRST,
                     profit NULLS FIRST,
                     ss_ticket_number NULLS FIRST,
                     amt NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            c_last_name = r.IsDBNull(0) ? null : r.GetString(0),
            c_first_name = r.IsDBNull(1) ? null : r.GetString(1),
            s_city_sub = r.IsDBNull(2) ? null : r.GetString(2),
            ss_ticket_number = r.GetInt64(3),
            amt = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4),
            profit = r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].c_last_name, polarsResult[i].c_last_name);
            Assert.Equal(duckDbResult[i].c_first_name, polarsResult[i].c_first_name);
            Assert.Equal(duckDbResult[i].s_city_sub, polarsResult[i].s_city_sub);
            Assert.Equal(duckDbResult[i].ss_ticket_number, polarsResult[i].ss_ticket_number);
            Assert.Equal(duckDbResult[i].amt ?? 0m, polarsResult[i].amt ?? 0m, 2);
            Assert.Equal(duckDbResult[i].profit ?? 0m, polarsResult[i].profit ?? 0m, 2);
        }
    }

    /*
     * TPC-DS Query 80
     *
     * WITH ssr AS
     *   (SELECT s_store_id AS store_id,
     *           sum(ss_ext_sales_price) AS sales,
     *           sum(coalesce(sr_return_amt, 0)) AS returns_,
     *           sum(ss_net_profit - coalesce(sr_net_loss, 0)) AS profit
     *    FROM store_sales
     *    LEFT OUTER JOIN store_returns ON (ss_item_sk = sr_item_sk
     *                                      AND ss_ticket_number = sr_ticket_number), date_dim,
     *                                                                                store,
     *                                                                                item,
     *                                                                                promotion
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *      AND ss_store_sk = s_store_sk
     *      AND ss_item_sk = i_item_sk
     *      AND i_current_price > 50
     *      AND ss_promo_sk = p_promo_sk
     *      AND p_channel_tv = 'N'
     *    GROUP BY s_store_id) ,
     *      csr AS
     *   (SELECT cp_catalog_page_id AS catalog_page_id,
     *           sum(cs_ext_sales_price) AS sales,
     *           sum(coalesce(cr_return_amount, 0)) AS returns_,
     *           sum(cs_net_profit - coalesce(cr_net_loss, 0)) AS profit
     *    FROM catalog_sales
     *    LEFT OUTER JOIN catalog_returns ON (cs_item_sk = cr_item_sk
     *                                        AND cs_order_number = cr_order_number), date_dim,
     *                                                                                catalog_page,
     *                                                                                item,
     *                                                                                promotion
     *    WHERE cs_sold_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *      AND cs_catalog_page_sk = cp_catalog_page_sk
     *      AND cs_item_sk = i_item_sk
     *      AND i_current_price > 50
     *      AND cs_promo_sk = p_promo_sk
     *      AND p_channel_tv = 'N'
     *    GROUP BY cp_catalog_page_id) ,
     *      wsr AS
     *   (SELECT web_site_id,
     *           sum(ws_ext_sales_price) AS sales,
     *           sum(coalesce(wr_return_amt, 0)) AS returns_,
     *           sum(ws_net_profit - coalesce(wr_net_loss, 0)) AS profit
     *    FROM web_sales
     *    LEFT OUTER JOIN web_returns ON (ws_item_sk = wr_item_sk
     *                                    AND ws_order_number = wr_order_number), date_dim,
     *                                                                            web_site,
     *                                                                            item,
     *                                                                            promotion
     *    WHERE ws_sold_date_sk = d_date_sk
     *      AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     *      AND ws_web_site_sk = web_site_sk
     *      AND ws_item_sk = i_item_sk
     *      AND i_current_price > 50
     *      AND ws_promo_sk = p_promo_sk
     *      AND p_channel_tv = 'N'
     *    GROUP BY web_site_id)
     * SELECT channel ,
     *        id ,
     *        sum(sales) AS sales ,
     *        sum(returns_) AS returns_ ,
     *        sum(profit) AS profit
     * FROM
     *   (SELECT 'store channel' AS channel ,
     *           concat('store', store_id) AS id ,
     *           sales ,
     *           returns_ ,
     *           profit
     *    FROM ssr
     *    UNION ALL SELECT 'catalog channel' AS channel ,
     *                     concat('catalog_page', catalog_page_id) AS id ,
     *                     sales ,
     *                     returns_ ,
     *                     profit
     *    FROM csr
     *    UNION ALL SELECT 'web channel' AS channel ,
     *                     concat('web_site', web_site_id) AS id ,
     *                     sales ,
     *                     returns_ ,
     *                     profit
     *    FROM wsr ) x
     * GROUP BY ROLLUP (channel,
     *                  id)
     * ORDER BY channel NULLS FIRST,
     *          id NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q80()
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
        var itemQuery = TpcdsData.ItemQuery();
        var promotionQuery = TpcdsData.PromotionQuery();

        var minDate = new DateOnly(2000, 8, 23);
        var maxDate = new DateOnly(2000, 9, 22);
        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredItems = itemQuery.Where(i => i.i_current_price > 50m);
        var filteredPromos = promotionQuery.Where(p => p.p_channel_tv == "N");

        var ssr = from ss in storeSalesQuery
                  join sr in storeReturnsQuery on new { item_sk = (long?)ss.ss_item_sk, ticket_number = (long?)ss.ss_ticket_number }
                                           equals new { item_sk = (long?)sr.sr_item_sk, ticket_number = (long?)sr.sr_ticket_number } into srGroup
                  from sr in srGroup.DefaultIfEmpty()
                  join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                  join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                  join i in filteredItems on ss.ss_item_sk equals i.i_item_sk
                  join p in filteredPromos on ss.ss_promo_sk equals p.p_promo_sk
                  group new { ss, sr } by s.s_store_id into g
                  select new
                  {
                      channel = "store channel",
                      id = "store" + g.Key,
                      sales = g.Sum(x => x.ss.ss_ext_sales_price),
                      returns_ = g.Sum(x => (decimal?)(x.sr.sr_return_amt ?? 0.00m)),
                      profit = g.Sum(x => x.ss.ss_net_profit - (x.sr.sr_net_loss ?? 0.00m))
                  };

        var csr = from cs in catalogSalesQuery
                  join cr in catalogReturnsQuery on new { item_sk = (long?)cs.cs_item_sk, order_number = (long?)cs.cs_order_number }
                                             equals new { item_sk = (long?)cr.cr_item_sk, order_number = (long?)cr.cr_order_number } into crGroup
                  from cr in crGroup.DefaultIfEmpty()
                  join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                  join cp in catalogPageQuery on cs.cs_catalog_page_sk equals cp.cp_catalog_page_sk
                  join i in filteredItems on cs.cs_item_sk equals i.i_item_sk
                  join p in filteredPromos on cs.cs_promo_sk equals p.p_promo_sk
                  group new { cs, cr } by cp.cp_catalog_page_id into g
                  select new
                  {
                      channel = "catalog channel",
                      id = "catalog_page" + g.Key,
                      sales = g.Sum(x => x.cs.cs_ext_sales_price),
                      returns_ = g.Sum(x => (decimal?)(x.cr.cr_return_amount ?? 0.00m)),
                      profit = g.Sum(x => x.cs.cs_net_profit - (x.cr.cr_net_loss ?? 0.00m))
                  };

        var wsr = from ws in webSalesQuery
                  join wr in webReturnsQuery on new { item_sk = (long?)ws.ws_item_sk, order_number = (long?)ws.ws_order_number }
                                         equals new { item_sk = (long?)wr.wr_item_sk, order_number = (long?)wr.wr_order_number } into wrGroup
                  from wr in wrGroup.DefaultIfEmpty()
                  join d in filteredDates on ws.ws_sold_date_sk equals d.d_date_sk
                  join w in webSiteQuery on ws.ws_web_site_sk equals w.web_site_sk
                  join i in filteredItems on ws.ws_item_sk equals i.i_item_sk
                  join p in filteredPromos on ws.ws_promo_sk equals p.p_promo_sk
                  group new { ws, wr } by w.web_site_id into g
                  select new
                  {
                      channel = "web channel",
                      id = "web_site" + g.Key,
                      sales = g.Sum(x => x.ws.ws_ext_sales_price),
                      returns_ = g.Sum(x => (decimal?)(x.wr.wr_return_amt ?? 0.00m)),
                      profit = g.Sum(x => x.ws.ws_net_profit - (x.wr.wr_net_loss ?? 0.00m))
                  };

        var allChan = ssr.Concat(csr).Concat(wsr);

        var q80 = allChan
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

        AssertPlan(q80, NativePushdown);

        var polarsResult = q80.ToList();

        var duckDbSql = $@"
            WITH ssr AS
              (SELECT s_store_id AS store_id,
                      sum(ss_ext_sales_price) AS sales,
                      sum(coalesce(sr_return_amt, 0)) AS returns_,
                      sum(ss_net_profit - coalesce(sr_net_loss, 0)) AS profit
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales
               LEFT OUTER JOIN read_parquet('{TpcdsData.StoreReturnsPath}') store_returns ON (ss_item_sk = sr_item_sk
                                                 AND ss_ticket_number = sr_ticket_number), read_parquet('{TpcdsData.DateDimPath}') date_dim,
                                                                                           read_parquet('{TpcdsData.StorePath}') store,
                                                                                           read_parquet('{TpcdsData.ItemPath}') item,
                                                                                           read_parquet('{TpcdsData.PromotionPath}') promotion
               WHERE ss_sold_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
                 AND ss_store_sk = s_store_sk
                 AND ss_item_sk = i_item_sk
                 AND i_current_price > 50
                 AND ss_promo_sk = p_promo_sk
                 AND p_channel_tv = 'N'
               GROUP BY s_store_id) ,
                 csr AS
              (SELECT cp_catalog_page_id AS catalog_page_id,
                      sum(cs_ext_sales_price) AS sales,
                      sum(coalesce(cr_return_amount, 0)) AS returns_,
                      sum(cs_net_profit - coalesce(cr_net_loss, 0)) AS profit
               FROM read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales
               LEFT OUTER JOIN read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns ON (cs_item_sk = cr_item_sk
                                                   AND cs_order_number = cr_order_number), read_parquet('{TpcdsData.DateDimPath}') date_dim,
                                                                                           read_parquet('{TpcdsData.CatalogPagePath}') catalog_page,
                                                                                           read_parquet('{TpcdsData.ItemPath}') item,
                                                                                           read_parquet('{TpcdsData.PromotionPath}') promotion
               WHERE cs_sold_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
                 AND cs_catalog_page_sk = cp_catalog_page_sk
                 AND cs_item_sk = i_item_sk
                 AND i_current_price > 50
                 AND cs_promo_sk = p_promo_sk
                 AND p_channel_tv = 'N'
               GROUP BY cp_catalog_page_id) ,
                 wsr AS
              (SELECT web_site_id,
                      sum(ws_ext_sales_price) AS sales,
                      sum(coalesce(wr_return_amt, 0)) AS returns_,
                      sum(ws_net_profit - coalesce(wr_net_loss, 0)) AS profit
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales
               LEFT OUTER JOIN read_parquet('{TpcdsData.WebReturnsPath}') web_returns ON (ws_item_sk = wr_item_sk
                                               AND ws_order_number = wr_order_number), read_parquet('{TpcdsData.DateDimPath}') date_dim,
                                                                                       read_parquet('{TpcdsData.WebSitePath}') web_site,
                                                                                       read_parquet('{TpcdsData.ItemPath}') item,
                                                                                       read_parquet('{TpcdsData.PromotionPath}') promotion
               WHERE ws_sold_date_sk = d_date_sk
                 AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
                 AND ws_web_site_sk = web_site_sk
                 AND ws_item_sk = i_item_sk
                 AND i_current_price > 50
                 AND ws_promo_sk = p_promo_sk
                 AND p_channel_tv = 'N'
               GROUP BY web_site_id)
            SELECT channel ,
                   id ,
                   sum(sales) AS sales ,
                   sum(returns_) AS returns_ ,
                   sum(profit) AS profit
            FROM
              (SELECT 'store channel' AS channel ,
                      concat('store', store_id) AS id ,
                      sales ,
                      returns_ ,
                      profit
               FROM ssr
               UNION ALL SELECT 'catalog channel' AS channel ,
                                concat('catalog_page', catalog_page_id) AS id ,
                                sales ,
                                returns_ ,
                                profit
               FROM csr
               UNION ALL SELECT 'web channel' AS channel ,
                                concat('web_site', web_site_id) AS id ,
                                sales ,
                                returns_ ,
                                profit
               FROM wsr ) x
            GROUP BY ROLLUP (channel,
                             id)
            ORDER BY channel NULLS FIRST,
                     id NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            channel = r.IsDBNull(0) ? null : r.GetString(0),
            id = r.IsDBNull(1) ? null : r.GetString(1),
            sales = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2),
            returns_ = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3),
            profit = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].channel, polarsResult[i].channel);
            Assert.Equal(duckDbResult[i].id, polarsResult[i].id);
            Assert.Equal(duckDbResult[i].sales ?? 0m, polarsResult[i].sales ?? 0m, 2);
            Assert.Equal(duckDbResult[i].returns_ ?? 0m, polarsResult[i].returns_ ?? 0m, 2);
            Assert.Equal(duckDbResult[i].profit ?? 0m, polarsResult[i].profit ?? 0m, 2);
        }
    }
}

