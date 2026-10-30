namespace Polars.Tpcds.Tests;

public class TpcdsQuery21To30Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 21
     *
     * SELECT *
     * FROM (
     *     SELECT w_warehouse_name,
     *            i_item_id,
     *            sum(CASE WHEN (cast(d_date AS date) < cast('2000-03-11' AS date))
     *                     THEN inv_quantity_on_hand ELSE 0 END) AS inv_before,
     *            sum(CASE WHEN (cast(d_date AS date) >= cast('2000-03-11' AS date))
     *                     THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
     *     FROM inventory,
     *          warehouse,
     *          item,
     *          date_dim
     *     WHERE i_current_price BETWEEN 0.99 AND 1.49
     *       AND i_item_sk = inv_item_sk
     *       AND inv_warehouse_sk = w_warehouse_sk
     *       AND inv_date_sk = d_date_sk
     *       AND d_date BETWEEN (cast('2000-03-11' AS date) - interval 30 day)
     *                      AND (cast('2000-03-11' AS date) + interval 30 day)
     *     GROUP BY w_warehouse_name,
     *              i_item_id
     * ) x
     * WHERE (CASE WHEN inv_before > 0 THEN cast(inv_after AS double) / cast(inv_before AS double) ELSE NULL END) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
     * ORDER BY w_warehouse_name,
     *          i_item_id
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q21")]
    public void Test_Q21()
    {
        var inventoryQuery = TpcdsData.InventoryQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var pivotDate = new DateOnly(2000, 3, 11);
        var minDate = pivotDate.AddDays(-30);
        var maxDate = pivotDate.AddDays(30);

        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredItems = itemQuery.Where(i => i.i_current_price >= 0.99m && i.i_current_price <= 1.49m);

        var q21 = from inv in inventoryQuery
                  join w in warehouseQuery on inv.inv_warehouse_sk equals w.w_warehouse_sk
                  join i in filteredItems on inv.inv_item_sk equals i.i_item_sk
                  join d in filteredDates on inv.inv_date_sk equals d.d_date_sk
                  group new { inv, d } by new
                  {
                      w.w_warehouse_name,
                      i.i_item_id
                  } into g
                  let inv_before = g.Sum(x => x.d.d_date < pivotDate ? (long)x.inv.inv_quantity_on_hand! : 0L)
                  let inv_after = g.Sum(x => x.d.d_date >= pivotDate ? (long)x.inv.inv_quantity_on_hand! : 0L)
                  where inv_before > 0 &&
                        (double)inv_after / (double)inv_before >= 2.0 / 3.0 &&
                        (double)inv_after / (double)inv_before <= 3.0 / 2.0
                  orderby g.Key.w_warehouse_name, g.Key.i_item_id
                  select new
                  {
                      g.Key.w_warehouse_name,
                      g.Key.i_item_id,
                      inv_before,
                      inv_after
                  };

        var limitedQ21 = q21.Take(100);

        AssertPlan(limitedQ21, NativePushdown);

        var polarsResult = limitedQ21.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT *
            FROM (
                SELECT w_warehouse_name,
                       i_item_id,
                       cast(sum(CASE WHEN (cast(d_date AS date) < cast('2000-03-11' AS date))
                                     THEN inv_quantity_on_hand ELSE 0 END) AS bigint) AS inv_before,
                       cast(sum(CASE WHEN (cast(d_date AS date) >= cast('2000-03-11' AS date))
                                     THEN inv_quantity_on_hand ELSE 0 END) AS bigint) AS inv_after
                FROM read_parquet('{TpcdsData.InventoryPath}'),
                     read_parquet('{TpcdsData.WarehousePath}'),
                     read_parquet('{TpcdsData.ItemPath}'),
                     read_parquet('{TpcdsData.DateDimPath}')
                WHERE i_current_price BETWEEN 0.99 AND 1.49
                  AND i_item_sk = inv_item_sk
                  AND inv_warehouse_sk = w_warehouse_sk
                  AND inv_date_sk = d_date_sk
                  AND d_date BETWEEN (cast('2000-03-11' AS date) - interval 30 day)
                                 AND (cast('2000-03-11' AS date) + interval 30 day)
                GROUP BY w_warehouse_name,
                         i_item_id
            ) x
            WHERE (CASE WHEN inv_before > 0 THEN cast(inv_after AS double) / cast(inv_before AS double) ELSE NULL END) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
            ORDER BY w_warehouse_name,
                     i_item_id
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            w_warehouse_name = r.IsDBNull(0) ? null : r.GetString(0),
            i_item_id = r.GetString(1),
            inv_before = r.IsDBNull(2) ? 0L : r.GetInt64(2),
            inv_after = r.IsDBNull(3) ? 0L : r.GetInt64(3),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.w_warehouse_name, p.w_warehouse_name);
            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.inv_before, p.inv_before);
            Assert.Equal(d.inv_after, p.inv_after);
        }
    }

    /*
     * TPC-DS Query 22
     *
     * SELECT i_product_name,
     *        i_brand,
     *        i_class,
     *        i_category,
     *        avg(inv_quantity_on_hand) AS qoh
     * FROM inventory,
     *      date_dim,
     *      item
     * WHERE inv_date_sk = d_date_sk
     *   AND inv_item_sk = i_item_sk
     *   AND d_month_seq BETWEEN 1201 AND 1201 + 11
     * GROUP BY rollup (i_product_name, i_brand, i_class, i_category)
     * ORDER BY qoh,
     *          i_product_name,
     *          i_brand,
     *          i_class,
     *          i_category
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q22")]
    public void Test_Q22()
    {
        var inventoryQuery = TpcdsData.InventoryQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_month_seq >= 1201 && d.d_month_seq <= 1201 + 11);

        var q22 = (from inv in inventoryQuery
                   join d in filteredDates on inv.inv_date_sk equals d.d_date_sk
                   join i in itemQuery on inv.inv_item_sk equals i.i_item_sk
                   select new { inv, i })
            .GroupByRollup(
                x => new { x.i.i_product_name, x.i.i_brand, x.i.i_class, x.i.i_category },
                (key, g) => new
                {
                    key.i_product_name,
                    key.i_brand,
                    key.i_class,
                    key.i_category,
                    qoh = g.Average(x => (double?)x.inv.inv_quantity_on_hand)
                }
            )
            .OrderBy(x => x.qoh)
            .ThenBy(x => x.i_product_name)
            .ThenBy(x => x.i_brand)
            .ThenBy(x => x.i_class)
            .ThenBy(x => x.i_category)
            .Take(100);

        AssertPlan(q22, NativePushdown);

        var polarsResult = q22.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT i_product_name,
                   i_brand,
                   i_class,
                   i_category,
                   avg(inv_quantity_on_hand) AS qoh
            FROM read_parquet('{TpcdsData.InventoryPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.ItemPath}')
            WHERE inv_date_sk = d_date_sk
              AND inv_item_sk = i_item_sk
              AND d_month_seq BETWEEN 1201 AND 1201 + 11
            GROUP BY rollup (i_product_name, i_brand, i_class, i_category)
            ORDER BY qoh,
                     i_product_name NULLS FIRST,
                     i_brand NULLS FIRST,
                     i_class NULLS FIRST,
                     i_category NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_product_name = r.IsDBNull(0) ? null : r.GetString(0),
            i_brand = r.IsDBNull(1) ? null : r.GetString(1),
            i_class = r.IsDBNull(2) ? null : r.GetString(2),
            i_category = r.IsDBNull(3) ? null : r.GetString(3),
            qoh = r.IsDBNull(4) ? null : (double?)r.GetDouble(4),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_product_name, p.i_product_name);
            Assert.Equal(d.i_brand, p.i_brand);
            Assert.Equal(d.i_class, p.i_class);
            Assert.Equal(d.i_category, p.i_category);
            if (d.qoh == null)
            {
                Assert.Null(p.qoh);
            }
            else
            {
                Assert.NotNull(p.qoh);
                Assert.Equal(d.qoh.Value, p.qoh.Value, 4);
            }
        }
    }

    /*
     * TPC-DS Query 23
     *
     * WITH frequent_ss_items AS
     *   (SELECT substr(i_item_desc, 1, 30) itemdesc,
     *           i_item_sk item_sk,
     *           d_date solddate,
     *           count(*) cnt
     *    FROM store_sales,
     *         date_dim,
     *         item
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND ss_item_sk = i_item_sk
     *      AND d_year IN (1999, 1999+1, 1999+2, 1999+3)
     *    GROUP BY substr(i_item_desc, 1, 30),
     *             i_item_sk,
     *             d_date
     *    HAVING count(*) > 4),
     *      max_store_sales AS
     *   (SELECT max(csales) tpcds_cmax
     *    FROM
     *      (SELECT c_customer_sk,
     *              sum(ss_quantity*ss_sales_price) csales
     *       FROM store_sales,
     *            customer,
     *            date_dim
     *       WHERE ss_customer_sk = c_customer_sk
     *         AND ss_sold_date_sk = d_date_sk
     *         AND d_year IN (1999, 1999+1, 1999+2, 1999+3)
     *       GROUP BY c_customer_sk)),
     *      best_ss_customer AS
     *   (SELECT c_customer_sk,
     *           sum(ss_quantity*ss_sales_price) ssales
     *    FROM store_sales,
     *         customer
     *    WHERE ss_customer_sk = c_customer_sk
     *    GROUP BY c_customer_sk
     *    HAVING sum(ss_quantity*ss_sales_price) > (95/100.0) *
     *      (SELECT *
     *       FROM max_store_sales))
     * SELECT c_last_name,
     *        c_first_name,
     *        sales
     * FROM
     *   (SELECT c_last_name,
     *           c_first_name,
     *           sum(cs_quantity*cs_list_price) sales
     *    FROM catalog_sales,
     *         customer,
     *         date_dim
     *    WHERE d_year = 1999
     *      AND d_moy = 1
     *      AND cs_sold_date_sk = d_date_sk
     *      AND cs_item_sk IN
     *        (SELECT item_sk
     *         FROM frequent_ss_items)
     *      AND cs_bill_customer_sk IN
     *        (SELECT c_customer_sk
     *         FROM best_ss_customer)
     *      AND cs_bill_customer_sk = c_customer_sk
     *    GROUP BY c_last_name,
     *             c_first_name
     *    UNION ALL SELECT c_last_name,
     *                     c_first_name,
     *                     sum(ws_quantity*ws_list_price) sales
     *    FROM web_sales,
     *         customer,
     *         date_dim
     *    WHERE d_year = 1999
     *      AND d_moy = 1
     *      AND ws_sold_date_sk = d_date_sk
     *      AND ws_item_sk IN
     *        (SELECT item_sk
     *         FROM frequent_ss_items)
     *      AND ws_bill_customer_sk IN
     *        (SELECT c_customer_sk
     *         FROM best_ss_customer)
     *      AND ws_bill_customer_sk = c_customer_sk
     *    GROUP BY c_last_name,
     *             c_first_name)
     * ORDER BY c_last_name,
     *          c_first_name,
     *          sales
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q23")]
    public void Test_Q23()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var dates4Years = dateDimQuery.Where(d => d.d_year >= 1999 && d.d_year <= 2002);

        // 1. frequent_ss_items CTE
        var frequentItems = (from ss in storeSalesQuery
                             join d in dates4Years on ss.ss_sold_date_sk equals d.d_date_sk
                             join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                             group ss by new
                             {
                                 itemdesc = i.i_item_desc!.Substring(0, 30),
                                 i.i_item_sk,
                                 d.d_date
                             } into g
                             where g.LongCount() > 4
                             select new { item_sk = g.Key.i_item_sk }).Distinct();

        // 2. max_store_sales CTE
        var maxStoreSalesQuery = from ss in storeSalesQuery
                                 join d in dates4Years on ss.ss_sold_date_sk equals d.d_date_sk
                                 group ss by ss.ss_customer_sk into g
                                 select new { csales = g.Sum(x => x.ss_quantity * x.ss_sales_price) };

        // 3. best_ss_customer CTE
        var bestCustomers = (from ss in storeSalesQuery
                             group ss by ss.ss_customer_sk into g
                             where g.Sum(x => x.ss_quantity * x.ss_sales_price) > maxStoreSalesQuery.Max(x => x.csales) * 0.95m
                             select new { c_customer_sk = g.Key }).Distinct();

        // 4. Final selection: Catalog & Web sales for best customers & frequent items in 1999 month 1
        var targetDates = dateDimQuery.Where(d => d.d_year == 1999 && d.d_moy == 1);

        var catalogStream = from cs in catalogSalesQuery
                            join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                            join c in customerQuery on cs.cs_bill_customer_sk equals c.c_customer_sk
                            join fi in frequentItems on cs.cs_item_sk equals fi.item_sk
                            join bc in bestCustomers on cs.cs_bill_customer_sk equals bc.c_customer_sk
                            group cs by new { c.c_last_name, c.c_first_name } into g
                            select new
                            {
                                g.Key.c_last_name,
                                g.Key.c_first_name,
                                sales = g.Sum(x => x.cs_quantity * x.cs_list_price)
                            };

        var webStream = from ws in webSalesQuery
                        join d in targetDates on ws.ws_sold_date_sk equals d.d_date_sk
                        join c in customerQuery on ws.ws_bill_customer_sk equals c.c_customer_sk
                        join fi in frequentItems on ws.ws_item_sk equals fi.item_sk
                        join bc in bestCustomers on ws.ws_bill_customer_sk equals bc.c_customer_sk
                        group ws by new { c.c_last_name, c.c_first_name } into g
                        select new
                        {
                            g.Key.c_last_name,
                            g.Key.c_first_name,
                            sales = g.Sum(x => x.ws_quantity * x.ws_list_price)
                        };

        var q23 = (from x in catalogStream.Concat(webStream)
                   orderby x.c_last_name, x.c_first_name, x.sales
                   select x).Take(100);

        AssertPlan(q23, NativePushdown);

        var polarsResult = q23.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            with frequent_ss_items as 
             (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
              from read_parquet('{TpcdsData.StoreSalesPath}')
                  ,read_parquet('{TpcdsData.DateDimPath}')
                  ,read_parquet('{TpcdsData.ItemPath}')
              where ss_sold_date_sk = d_date_sk
                and ss_item_sk = i_item_sk
                and d_year in (1999,1999+1,1999+2,1999+3)
              group by substr(i_item_desc,1,30),i_item_sk,d_date
              having count(*) > 4),
             max_store_sales as
             (select max(csales) tpcds_cmax
              from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
                    from read_parquet('{TpcdsData.StoreSalesPath}')
                        ,read_parquet('{TpcdsData.CustomerPath}')
                        ,read_parquet('{TpcdsData.DateDimPath}')
                    where ss_customer_sk = c_customer_sk
                     and ss_sold_date_sk = d_date_sk
                     and d_year in (1999,1999+1,1999+2,1999+3) 
                    group by c_customer_sk)),
             best_ss_customer as
             (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
              from read_parquet('{TpcdsData.StoreSalesPath}')
                  ,read_parquet('{TpcdsData.CustomerPath}')
              where ss_customer_sk = c_customer_sk
              group by c_customer_sk
              having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select * from max_store_sales))
             select c_last_name,c_first_name,sales
             from (select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
                    from read_parquet('{TpcdsData.CatalogSalesPath}')
                        ,read_parquet('{TpcdsData.CustomerPath}')
                        ,read_parquet('{TpcdsData.DateDimPath}')
                    where d_year = 1999 
                     and d_moy = 1 
                     and cs_sold_date_sk = d_date_sk 
                     and cs_item_sk in (select item_sk from frequent_ss_items)
                     and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
                     and cs_bill_customer_sk = c_customer_sk 
                    group by c_last_name,c_first_name
                  union all
                  select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
                    from read_parquet('{TpcdsData.WebSalesPath}')
                        ,read_parquet('{TpcdsData.CustomerPath}')
                        ,read_parquet('{TpcdsData.DateDimPath}')
                    where d_year = 1999 
                     and d_moy = 1 
                     and ws_sold_date_sk = d_date_sk 
                     and ws_item_sk in (select item_sk from frequent_ss_items)
                     and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
                     and ws_bill_customer_sk = c_customer_sk
                    group by c_last_name,c_first_name) 
             order by c_last_name,c_first_name,sales
             LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            c_last_name = r.IsDBNull(0) ? null : r.GetString(0),
            c_first_name = r.IsDBNull(1) ? null : r.GetString(1),
            sales = r.IsDBNull(2) ? null : (decimal?)r.GetDecimal(2),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.c_last_name, p.c_last_name);
            Assert.Equal(d.c_first_name, p.c_first_name);
            Assert.Equal(d.sales, p.sales);
        }
    }

    /*
     * TPC-DS Query 24
     *
     * WITH ssales AS
     *   (SELECT c_last_name,
     *           c_first_name,
     *           s_store_name,
     *           ca_state,
     *           s_state,
     *           i_color,
     *           i_current_price,
     *           i_manager_id,
     *           i_units,
     *           i_size,
     *           sum(ss_net_paid) AS netpaid
     *    FROM store_sales,
     *         store_returns,
     *         store,
     *         item,
     *         customer,
     *         customer_address
     *    WHERE ss_ticket_number = sr_ticket_number
     *      AND ss_item_sk = sr_item_sk
     *      AND ss_customer_sk = c_customer_sk
     *      AND ss_item_sk = i_item_sk
     *      AND ss_store_sk = s_store_sk
     *      AND c_current_addr_sk = ca_address_sk
     *      AND c_birth_country <> upper(ca_country)
     *      AND s_zip = ca_zip
     *      AND s_market_id = 8
     *    GROUP BY c_last_name,
     *             c_first_name,
     *             s_store_name,
     *             ca_state,
     *             s_state,
     *             i_color,
     *             i_current_price,
     *             i_manager_id,
     *             i_units,
     *             i_size)
     * SELECT c_last_name,
     *        c_first_name,
     *        s_store_name,
     *        sum(netpaid) AS paid
     * FROM ssales
     * WHERE i_color = 'peach'
     * GROUP BY c_last_name,
     *          c_first_name,
     *          s_store_name
     * HAVING sum(netpaid) >
     *   (SELECT 0.05 * avg(netpaid)
     *    FROM ssales)
     * ORDER BY c_last_name,
     *          c_first_name,
     *          s_store_name;
     */
    [Fact]
    [Trait("TPCDS", "Q24")]
    public void Test_Q24()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();

        var filteredStores = storeQuery.Where(s => s.s_market_id == 8);

        // 1. ssales CTE
        var ssales = from ss in storeSalesQuery
                     join sr in storeReturnsQuery on new { ticket_number = ss.ss_ticket_number, item_sk = ss.ss_item_sk }
                                              equals new { ticket_number = sr.sr_ticket_number, item_sk = sr.sr_item_sk }
                     join s in filteredStores on ss.ss_store_sk equals s.s_store_sk
                     join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                     join c in customerQuery on ss.ss_customer_sk equals c.c_customer_sk
                     join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                     where c.c_birth_country != ca.ca_country!.ToUpper() &&
                           s.s_zip == ca.ca_zip
                     group ss by new
                     {
                         c.c_last_name,
                         c.c_first_name,
                         s.s_store_name,
                         ca.ca_state,
                         s.s_state,
                         i.i_color,
                         i.i_current_price,
                         i.i_manager_id,
                         i.i_units,
                         i.i_size
                     } into g
                     select new
                     {
                         g.Key.c_last_name,
                         g.Key.c_first_name,
                         g.Key.s_store_name,
                         g.Key.ca_state,
                         g.Key.s_state,
                         g.Key.i_color,
                         g.Key.i_current_price,
                         g.Key.i_manager_id,
                         g.Key.i_units,
                         g.Key.i_size,
                         netpaid = g.Sum(x => x.ss_net_paid)
                     };

        // 2. Final Selection
        var q24 = from x in ssales
                  where x.i_color == "peach"
                  group x by new
                  {
                      x.c_last_name,
                      x.c_first_name,
                      x.s_store_name
                  } into g
                  where (double?)g.Sum(x => x.netpaid) > 0.05 * ssales.Average(x => (double?)x.netpaid)
                  orderby g.Key.c_last_name, g.Key.c_first_name, g.Key.s_store_name
                  select new
                  {
                      g.Key.c_last_name,
                      g.Key.c_first_name,
                      g.Key.s_store_name,
                      paid = g.Sum(x => x.netpaid)
                  };

        AssertPlan(q24, NativePushdown);

        var polarsResult = q24.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH ssales AS (
                SELECT 
                    c_last_name, 
                    c_first_name, 
                    s_store_name, 
                    ca_state, 
                    s_state, 
                    i_color, 
                    i_current_price, 
                    i_manager_id, 
                    i_units, 
                    i_size, 
                    sum(ss_net_paid) AS netpaid 
                FROM 
                    read_parquet('{TpcdsData.StoreSalesPath}'), 
                    read_parquet('{TpcdsData.StoreReturnsPath}'), 
                    read_parquet('{TpcdsData.StorePath}'), 
                    read_parquet('{TpcdsData.ItemPath}'), 
                    read_parquet('{TpcdsData.CustomerPath}'), 
                    read_parquet('{TpcdsData.CustomerAddressPath}') 
                WHERE 
                    ss_ticket_number = sr_ticket_number 
                    AND ss_item_sk = sr_item_sk 
                    AND ss_customer_sk = c_customer_sk 
                    AND ss_item_sk = i_item_sk 
                    AND ss_store_sk = s_store_sk 
                    AND c_current_addr_sk = ca_address_sk 
                    AND c_birth_country <> upper(ca_country) 
                    AND s_zip = ca_zip 
                    AND s_market_id = 8 
                GROUP BY 
                    c_last_name, c_first_name, s_store_name, ca_state, 
                    s_state, i_color, i_current_price, i_manager_id, i_units, i_size
            )
            SELECT 
                c_last_name, 
                c_first_name, 
                s_store_name, 
                sum(netpaid) AS paid 
            FROM ssales 
            WHERE i_color = 'peach'
            GROUP BY c_last_name, c_first_name, s_store_name 
            HAVING sum(netpaid) > (SELECT 0.05 * avg(netpaid) FROM ssales) 
            ORDER BY c_last_name, c_first_name, s_store_name;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            last_name = r.IsDBNull(0) ? null : r.GetString(0),
            first_name = r.IsDBNull(1) ? null : r.GetString(1),
            store_name = r.IsDBNull(2) ? null : r.GetString(2),
            paid = r.IsDBNull(3) ? null : (decimal?)r.GetDecimal(3),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.last_name, p.c_last_name);
            Assert.Equal(d.first_name, p.c_first_name);
            Assert.Equal(d.store_name, p.s_store_name);
            Assert.Equal(d.paid, p.paid);
        }
    }

    /*
     * TPC-DS Query 25
     *
     * SELECT i_item_id,
     *        i_item_desc,
     *        s_store_id,
     *        s_store_name,
     *        sum(ss_net_profit) AS store_sales_profit,
     *        sum(sr_net_loss) AS store_returns_loss,
     *        sum(cs_net_profit) AS catalog_sales_profit
     * FROM store_sales,
     *      store_returns,
     *      catalog_sales,
     *      date_dim d1,
     *      date_dim d2,
     *      date_dim d3,
     *      store,
     *      item
     * WHERE d1.d_moy = 4
     *   AND d1.d_year = 2000
     *   AND d1.d_date_sk = ss_sold_date_sk
     *   AND i_item_sk = ss_item_sk
     *   AND s_store_sk = ss_store_sk
     *   AND ss_customer_sk = sr_customer_sk
     *   AND ss_item_sk = sr_item_sk
     *   AND ss_ticket_number = sr_ticket_number
     *   AND sr_returned_date_sk = d2.d_date_sk
     *   AND d2.d_moy BETWEEN 4 AND 10
     *   AND d2.d_year = 2000
     *   AND sr_customer_sk = cs_bill_customer_sk
     *   AND sr_item_sk = cs_item_sk
     *   AND cs_sold_date_sk = d3.d_date_sk
     *   AND d3.d_moy BETWEEN 4 AND 10
     *   AND d3.d_year = 2000
     * GROUP BY i_item_id,
     *          i_item_desc,
     *          s_store_id,
     *          s_store_name
     * ORDER BY i_item_id,
     *          i_item_desc,
     *          s_store_id,
     *          s_store_name
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q25")]
    public void Test_Q25()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery1 = TpcdsData.DateDimQuery();
        var dateDimQuery2 = TpcdsData.DateDimQuery();
        var dateDimQuery3 = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var d1 = dateDimQuery1.Where(d => d.d_year == 2000 && d.d_moy == 4);
        var d2 = dateDimQuery2.Where(d => d.d_year == 2000 && d.d_moy >= 4 && d.d_moy <= 10);
        var d3 = dateDimQuery3.Where(d => d.d_year == 2000 && d.d_moy >= 4 && d.d_moy <= 10);

        var q25 = (from ss in storeSalesQuery
                   join d_1 in d1 on ss.ss_sold_date_sk equals d_1.d_date_sk
                   join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   join sr in storeReturnsQuery on new { customer_sk = ss.ss_customer_sk, item_sk = ss.ss_item_sk, ticket_number = ss.ss_ticket_number }
                                            equals new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk, ticket_number = sr.sr_ticket_number }
                   join d_2 in d2 on sr.sr_returned_date_sk equals d_2.d_date_sk
                   join cs in catalogSalesQuery on new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk }
                                            equals new { customer_sk = cs.cs_bill_customer_sk, item_sk = cs.cs_item_sk }
                   join d_3 in d3 on cs.cs_sold_date_sk equals d_3.d_date_sk
                   group new { ss, sr, cs } by new
                   {
                       i.i_item_id,
                       i.i_item_desc,
                       s.s_store_id,
                       s.s_store_name
                   } into g
                   orderby g.Key.i_item_id,
                           g.Key.i_item_desc,
                           g.Key.s_store_id,
                           g.Key.s_store_name
                   select new
                   {
                       g.Key.i_item_id,
                       g.Key.i_item_desc,
                       g.Key.s_store_id,
                       g.Key.s_store_name,
                       store_sales_profit = g.Sum(x => x.ss.ss_net_profit),
                       store_returns_loss = g.Sum(x => x.sr.sr_net_loss),
                       catalog_sales_profit = g.Sum(x => x.cs.cs_net_profit)
                   }).Take(100);

        AssertPlan(q25, NativePushdown);

        var polarsResult = q25.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT 
                i_item_id
                , i_item_desc
                , s_store_id
                , s_store_name
                , sum(ss_net_profit) as store_sales_profit
                , sum(sr_net_loss) as store_returns_loss
                , sum(cs_net_profit) as catalog_sales_profit
            FROM 
                read_parquet('{TpcdsData.StoreSalesPath}')
                , read_parquet('{TpcdsData.StoreReturnsPath}')
                , read_parquet('{TpcdsData.CatalogSalesPath}')
                , read_parquet('{TpcdsData.DateDimPath}') d1
                , read_parquet('{TpcdsData.DateDimPath}') d2
                , read_parquet('{TpcdsData.DateDimPath}') d3
                , read_parquet('{TpcdsData.StorePath}')
                , read_parquet('{TpcdsData.ItemPath}')
            WHERE 
                d1.d_moy = 4 
                and d1.d_year = 2000 
                and d1.d_date_sk = ss_sold_date_sk 
                and i_item_sk = ss_item_sk 
                and s_store_sk = ss_store_sk 
                and ss_customer_sk = sr_customer_sk 
                and ss_item_sk = sr_item_sk 
                and ss_ticket_number = sr_ticket_number 
                and sr_returned_date_sk = d2.d_date_sk 
                and d2.d_moy between 4 and 10 
                and d2.d_year = 2000 
                and sr_customer_sk = cs_bill_customer_sk 
                and sr_item_sk = cs_item_sk 
                and cs_sold_date_sk = d3.d_date_sk 
                and d3.d_moy between 4 and 10 
                and d3.d_year = 2000
            GROUP BY 
                i_item_id
                , i_item_desc
                , s_store_id
                , s_store_name
            ORDER BY 
                i_item_id
                , i_item_desc
                , s_store_id
                , s_store_name
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            s_store_id = r.GetString(2),
            s_store_name = r.IsDBNull(3) ? null : r.GetString(3),
            store_sales_profit = r.IsDBNull(4) ? null : (decimal?)r.GetDecimal(4),
            store_returns_loss = r.IsDBNull(5) ? null : (decimal?)r.GetDecimal(5),
            catalog_sales_profit = r.IsDBNull(6) ? null : (decimal?)r.GetDecimal(6),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.i_item_desc, p.i_item_desc);
            Assert.Equal(d.s_store_id, p.s_store_id);
            Assert.Equal(d.s_store_name, p.s_store_name);
            Assert.Equal(d.store_sales_profit, p.store_sales_profit);
            Assert.Equal(d.store_returns_loss, p.store_returns_loss);
            Assert.Equal(d.catalog_sales_profit, p.catalog_sales_profit);
        }
    }

    /*
     * TPC-DS Query 26
     *
     * SELECT i_item_id,
     *        avg(cs_quantity) agg1,
     *        avg(cs_list_price) agg2,
     *        avg(cs_coupon_amt) agg3,
     *        avg(cs_sales_price) agg4
     * FROM catalog_sales,
     *      customer_demographics,
     *      date_dim,
     *      item,
     *      promotion
     * WHERE cs_sold_date_sk = d_date_sk
     *   AND cs_item_sk = i_item_sk
     *   AND cs_bill_cdemo_sk = cd_demo_sk
     *   AND cs_promo_sk = p_promo_sk
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
    [Trait("TPCDS", "Q26")]
    public void Test_Q26()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var promotionQuery = TpcdsData.PromotionQuery();

        var filteredCd = customerDemographicsQuery.Where(cd => cd.cd_gender == "M"
                                                            && cd.cd_marital_status == "S"
                                                            && cd.cd_education_status == "College");

        var filteredPromo = promotionQuery.Where(p => p.p_channel_email == "N" || p.p_channel_event == "N");

        var filteredDates = dateDimQuery.Where(d => d.d_year == 2000);

        var q26 = (from cs in catalogSalesQuery
                   join d in filteredDates on cs.cs_sold_date_sk equals d.d_date_sk
                   join i in itemQuery on cs.cs_item_sk equals i.i_item_sk
                   join cd in filteredCd on cs.cs_bill_cdemo_sk equals cd.cd_demo_sk
                   join p in filteredPromo on cs.cs_promo_sk equals p.p_promo_sk
                   group cs by i.i_item_id into g
                   select new
                   {
                       i_item_id = g.Key,
                       agg1 = g.Average(x => (double?)x.cs_quantity),
                       agg2 = g.Average(x => (double?)x.cs_list_price),
                       agg3 = g.Average(x => (double?)x.cs_coupon_amt),
                       agg4 = g.Average(x => (double?)x.cs_sales_price),
                   } into r
                   orderby r.i_item_id
                   select r).Take(100);

        AssertPlan(q26, NativePushdown);

        var polarsResult = q26.ToList();
        Assert.NotEmpty(polarsResult);

        // DuckDB Cross-Validation
        var duckDbSql = $@"
            SELECT i_item_id,
                   avg(cs_quantity) agg1,
                   avg(cs_list_price) agg2,
                   avg(cs_coupon_amt) agg3,
                   avg(cs_sales_price) agg4
            FROM read_parquet('{TpcdsData.CatalogSalesPath}'),
                 read_parquet('{TpcdsData.CustomerDemographicsPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.PromotionPath}')
            WHERE cs_sold_date_sk = d_date_sk
              AND cs_item_sk = i_item_sk
              AND cs_bill_cdemo_sk = cd_demo_sk
              AND cs_promo_sk = p_promo_sk
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

    /*
     * TPC-DS Query 27
     *
     * WITH results AS
     *   (SELECT i_item_id,
     *           s_state,
     *           0 AS g_state,
     *           ss_quantity agg1,
     *           ss_list_price agg2,
     *           ss_coupon_amt agg3,
     *           ss_sales_price agg4
     *    FROM store_sales,
     *         customer_demographics,
     *         date_dim,
     *         store,
     *         item
     *    WHERE ss_sold_date_sk = d_date_sk
     *      AND ss_item_sk = i_item_sk
     *      AND ss_store_sk = s_store_sk
     *      AND ss_cdemo_sk = cd_demo_sk
     *      AND cd_gender = 'M'
     *      AND cd_marital_status = 'S'
     *      AND cd_education_status = 'College'
     *      AND d_year = 2002
     *      AND s_state = 'TN' )
     * SELECT i_item_id,
     *        s_state,
     *        g_state,
     *        agg1,
     *        agg2,
     *        agg3,
     *        agg4
     * FROM
     *   ( SELECT i_item_id,
     *            s_state,
     *            0 AS g_state,
     *            avg(agg1) agg1,
     *            avg(agg2) agg2,
     *            avg(agg3) agg3,
     *            avg(agg4) agg4
     *    FROM results
     *    GROUP BY i_item_id,
     *             s_state
     *    UNION ALL SELECT i_item_id,
     *                     NULL AS s_state,
     *                     1 AS g_state,
     *                     avg(agg1) agg1,
     *                     avg(agg2) agg2,
     *                     avg(agg3) agg3,
     *                     avg(agg4) agg4
     *    FROM results
     *    GROUP BY i_item_id
     *    UNION ALL SELECT NULL AS i_item_id,
     *                     NULL AS s_state,
     *                     1 AS g_state,
     *                     avg(agg1) agg1,
     *                     avg(agg2) agg2,
     *                     avg(agg3) agg3,
     *                     avg(agg4) agg4
     *    FROM results ) foo
     * ORDER BY i_item_id NULLS FIRST,
     *          s_state NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q27")]
    public void Test_Q27()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var filteredCd = customerDemographicsQuery.Where(cd => cd.cd_gender == "M"
                                                            && cd.cd_marital_status == "S"
                                                            && cd.cd_education_status == "College");
        var filteredDates = dateDimQuery.Where(d => d.d_year == 2002);
        var filteredStores = storeQuery.Where(s => s.s_state == "TN");

        var results = from ss in storeSalesQuery
                      join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                      join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                      join s in filteredStores on ss.ss_store_sk equals s.s_store_sk
                      join cd in filteredCd on ss.ss_cdemo_sk equals cd.cd_demo_sk
                      select new
                      {
                          i.i_item_id,
                          s.s_state,
                          agg1 = ss.ss_quantity,
                          agg2 = ss.ss_list_price,
                          agg3 = ss.ss_coupon_amt,
                          agg4 = ss.ss_sales_price
                      };

        var q27 = results
            .GroupByRollup(
                r => new { r.i_item_id, r.s_state },
                (key, g) => new
                {
                    key.i_item_id,
                    key.s_state,
                    agg1 = g.Average(x => (double?)x.agg1),
                    agg2 = g.Average(x => (double?)x.agg2),
                    agg3 = g.Average(x => (double?)x.agg3),
                    agg4 = g.Average(x => (double?)x.agg4)
                }
            )
            .Select(x => new
            {
                x.i_item_id,
                x.s_state,
                g_state = x.s_state == null ? 1 : 0,
                x.agg1,
                x.agg2,
                x.agg3,
                x.agg4
            })
            .OrderBy(x => x.i_item_id)
            .ThenBy(x => x.s_state)
            .Take(100);

        AssertPlan(q27, NativePushdown);

        var polarsResult = q27.ToList();
        Assert.NotEmpty(polarsResult);

        // DuckDB Cross-Validation
        var duckDbSql = $@"
            WITH results AS
              (SELECT i_item_id,
                      s_state,
                      0 AS g_state,
                      ss_quantity agg1,
                      ss_list_price agg2,
                      ss_coupon_amt agg3,
                      ss_sales_price agg4
               FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.CustomerDemographicsPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.StorePath}'),
                    read_parquet('{TpcdsData.ItemPath}')
               WHERE ss_sold_date_sk = d_date_sk
                 AND ss_item_sk = i_item_sk
                 AND ss_store_sk = s_store_sk
                 AND ss_cdemo_sk = cd_demo_sk
                 AND cd_gender = 'M'
                 AND cd_marital_status = 'S'
                 AND cd_education_status = 'College'
                 AND d_year = 2002
                 AND s_state = 'TN' )
            SELECT i_item_id,
                   s_state,
                   g_state,
                   agg1,
                   agg2,
                   agg3,
                   agg4
            FROM
              ( SELECT i_item_id,
                       s_state,
                       0 AS g_state,
                       avg(agg1) agg1,
                       avg(agg2) agg2,
                       avg(agg3) agg3,
                       avg(agg4) agg4
               FROM results
               GROUP BY i_item_id,
                        s_state
               UNION ALL SELECT i_item_id,
                                NULL AS s_state,
                                1 AS g_state,
                                avg(agg1) agg1,
                                avg(agg2) agg2,
                                avg(agg3) agg3,
                                avg(agg4) agg4
               FROM results
               GROUP BY i_item_id
               UNION ALL SELECT NULL AS i_item_id,
                                NULL AS s_state,
                                1 AS g_state,
                                avg(agg1) agg1,
                                avg(agg2) agg2,
                                avg(agg3) agg3,
                                avg(agg4) agg4
               FROM results ) foo
            ORDER BY i_item_id NULLS FIRST,
                     s_state NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.IsDBNull(0) ? null : r.GetString(0),
            s_state = r.IsDBNull(1) ? null : r.GetString(1),
            g_state = r.GetInt32(2),
            agg1 = r.IsDBNull(3) ? (double?)null : r.GetDouble(3),
            agg2 = r.IsDBNull(4) ? (double?)null : r.GetDouble(4),
            agg3 = r.IsDBNull(5) ? (double?)null : r.GetDouble(5),
            agg4 = r.IsDBNull(6) ? (double?)null : r.GetDouble(6),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int idx = 0; idx < duckDbResults.Count; idx++)
        {
            var d = duckDbResults[idx];
            var p = polarsResult[idx];
            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.s_state, p.s_state);
            Assert.Equal(d.g_state, p.g_state);
            if (d.agg1 == null) Assert.Null(p.agg1); else Assert.Equal(d.agg1.Value, (double)p.agg1!, 4);
            if (d.agg2 == null) Assert.Null(p.agg2); else Assert.Equal(d.agg2.Value, (double)p.agg2!, 4);
            if (d.agg3 == null) Assert.Null(p.agg3); else Assert.Equal(d.agg3.Value, (double)p.agg3!, 4);
            if (d.agg4 == null) Assert.Null(p.agg4); else Assert.Equal(d.agg4.Value, (double)p.agg4!, 4);
        }
    }

    /*
     * TPC-DS Query 28
     *
     * SELECT *
     * FROM
     *   (SELECT avg(ss_list_price) B1_LP,
     *           count(ss_list_price) B1_CNT,
     *           count(DISTINCT ss_list_price) B1_CNTD
     *    FROM store_sales
     *    WHERE ss_quantity BETWEEN 0 AND 5
     *      AND (ss_list_price BETWEEN 8 AND 8+10
     *           OR ss_coupon_amt BETWEEN 459 AND 459+1000
     *           OR ss_wholesale_cost BETWEEN 57 AND 57+20)) B1,
     *   (SELECT avg(ss_list_price) B2_LP,
     *           count(ss_list_price) B2_CNT,
     *           count(DISTINCT ss_list_price) B2_CNTD
     *    FROM store_sales
     *    WHERE ss_quantity BETWEEN 6 AND 10
     *      AND (ss_list_price BETWEEN 90 AND 90+10
     *           OR ss_coupon_amt BETWEEN 2323 AND 2323+1000
     *           OR ss_wholesale_cost BETWEEN 31 AND 31+20)) B2,
     *   (SELECT avg(ss_list_price) B3_LP,
     *           count(ss_list_price) B3_CNT,
     *           count(DISTINCT ss_list_price) B3_CNTD
     *    FROM store_sales
     *    WHERE ss_quantity BETWEEN 11 AND 15
     *      AND (ss_list_price BETWEEN 142 AND 142+10
     *           OR ss_coupon_amt BETWEEN 12214 AND 12214+1000
     *           OR ss_wholesale_cost BETWEEN 79 AND 79+20)) B3,
     *   (SELECT avg(ss_list_price) B4_LP,
     *           count(ss_list_price) B4_CNT,
     *           count(DISTINCT ss_list_price) B4_CNTD
     *    FROM store_sales
     *    WHERE ss_quantity BETWEEN 16 AND 20
     *      AND (ss_list_price BETWEEN 135 AND 135+10
     *           OR ss_coupon_amt BETWEEN 6071 AND 6071+1000
     *           OR ss_wholesale_cost BETWEEN 38 AND 38+20)) B4,
     *   (SELECT avg(ss_list_price) B5_LP,
     *           count(ss_list_price) B5_CNT,
     *           count(DISTINCT ss_list_price) B5_CNTD
     *    FROM store_sales
     *    WHERE ss_quantity BETWEEN 21 AND 25
     *      AND (ss_list_price BETWEEN 122 AND 122+10
     *           OR ss_coupon_amt BETWEEN 836 AND 836+1000
     *           OR ss_wholesale_cost BETWEEN 17 AND 17+20)) B5,
     *   (SELECT avg(ss_list_price) B6_LP,
     *           count(ss_list_price) B6_CNT,
     *           count(DISTINCT ss_list_price) B6_CNTD
     *    FROM store_sales
     *    WHERE ss_quantity BETWEEN 26 AND 30
     *      AND (ss_list_price BETWEEN 154 AND 154+10
     *           OR ss_coupon_amt BETWEEN 7326 AND 7326+1000
     *           OR ss_wholesale_cost BETWEEN 7 AND 7+20)) B6
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q28")]
    public void Test_Q28()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();

        var b1 = storeSalesQuery
            .Where(ss => ss.ss_quantity >= 0 && ss.ss_quantity <= 5 &&
                         ((ss.ss_list_price >= 8m && ss.ss_list_price <= 18m) ||
                          (ss.ss_coupon_amt >= 459m && ss.ss_coupon_amt <= 1459m) ||
                          (ss.ss_wholesale_cost >= 57m && ss.ss_wholesale_cost <= 77m)))
            .GroupBy(_ => 1)
            .Select(g => new
            {
                B1_LP = g.Average(x => (double?)x.ss_list_price),
                B1_CNT = g.LongCount(x => x.ss_list_price != null),
                B1_CNTD = g.Where(x => x.ss_list_price != null).Select(x => x.ss_list_price).Distinct().LongCount()
            });

        var b2 = storeSalesQuery
            .Where(ss => ss.ss_quantity >= 6 && ss.ss_quantity <= 10 &&
                         ((ss.ss_list_price >= 90m && ss.ss_list_price <= 100m) ||
                          (ss.ss_coupon_amt >= 2323m && ss.ss_coupon_amt <= 3323m) ||
                          (ss.ss_wholesale_cost >= 31m && ss.ss_wholesale_cost <= 51m)))
            .GroupBy(_ => 1)
            .Select(g => new
            {
                B2_LP = g.Average(x => (double?)x.ss_list_price),
                B2_CNT = g.LongCount(x => x.ss_list_price != null),
                B2_CNTD = g.Where(x => x.ss_list_price != null).Select(x => x.ss_list_price).Distinct().LongCount()
            });

        var b3 = storeSalesQuery
            .Where(ss => ss.ss_quantity >= 11 && ss.ss_quantity <= 15 &&
                         ((ss.ss_list_price >= 142m && ss.ss_list_price <= 152m) ||
                          (ss.ss_coupon_amt >= 12214m && ss.ss_coupon_amt <= 13214m) ||
                          (ss.ss_wholesale_cost >= 79m && ss.ss_wholesale_cost <= 99m)))
            .GroupBy(_ => 1)
            .Select(g => new
            {
                B3_LP = g.Average(x => (double?)x.ss_list_price),
                B3_CNT = g.LongCount(x => x.ss_list_price != null),
                B3_CNTD = g.Where(x => x.ss_list_price != null).Select(x => x.ss_list_price).Distinct().LongCount()
            });

        var b4 = storeSalesQuery
            .Where(ss => ss.ss_quantity >= 16 && ss.ss_quantity <= 20 &&
                         ((ss.ss_list_price >= 135m && ss.ss_list_price <= 145m) ||
                          (ss.ss_coupon_amt >= 6071m && ss.ss_coupon_amt <= 7071m) ||
                          (ss.ss_wholesale_cost >= 38m && ss.ss_wholesale_cost <= 58m)))
            .GroupBy(_ => 1)
            .Select(g => new
            {
                B4_LP = g.Average(x => (double?)x.ss_list_price),
                B4_CNT = g.LongCount(x => x.ss_list_price != null),
                B4_CNTD = g.Where(x => x.ss_list_price != null).Select(x => x.ss_list_price).Distinct().LongCount()
            });

        var b5 = storeSalesQuery
            .Where(ss => ss.ss_quantity >= 21 && ss.ss_quantity <= 25 &&
                         ((ss.ss_list_price >= 122m && ss.ss_list_price <= 132m) ||
                          (ss.ss_coupon_amt >= 836m && ss.ss_coupon_amt <= 1836m) ||
                          (ss.ss_wholesale_cost >= 17m && ss.ss_wholesale_cost <= 37m)))
            .GroupBy(_ => 1)
            .Select(g => new
            {
                B5_LP = g.Average(x => (double?)x.ss_list_price),
                B5_CNT = g.LongCount(x => x.ss_list_price != null),
                B5_CNTD = g.Where(x => x.ss_list_price != null).Select(x => x.ss_list_price).Distinct().LongCount()
            });

        var b6 = storeSalesQuery
            .Where(ss => ss.ss_quantity >= 26 && ss.ss_quantity <= 30 &&
                         ((ss.ss_list_price >= 154m && ss.ss_list_price <= 164m) ||
                          (ss.ss_coupon_amt >= 7326m && ss.ss_coupon_amt <= 8326m) ||
                          (ss.ss_wholesale_cost >= 7m && ss.ss_wholesale_cost <= 27m)))
            .GroupBy(_ => 1)
            .Select(g => new
            {
                B6_LP = g.Average(x => (double?)x.ss_list_price),
                B6_CNT = g.LongCount(x => x.ss_list_price != null),
                B6_CNTD = g.Where(x => x.ss_list_price != null).Select(x => x.ss_list_price).Distinct().LongCount()
            });

        var q28 = (from x1 in b1
                   from x2 in b2
                   from x3 in b3
                   from x4 in b4
                   from x5 in b5
                   from x6 in b6
                   select new
                   {
                       x1.B1_LP,
                       x1.B1_CNT,
                       x1.B1_CNTD,
                       x2.B2_LP,
                       x2.B2_CNT,
                       x2.B2_CNTD,
                       x3.B3_LP,
                       x3.B3_CNT,
                       x3.B3_CNTD,
                       x4.B4_LP,
                       x4.B4_CNT,
                       x4.B4_CNTD,
                       x5.B5_LP,
                       x5.B5_CNT,
                       x5.B5_CNTD,
                       x6.B6_LP,
                       x6.B6_CNT,
                       x6.B6_CNTD,
                   }).Take(100);

        AssertPlan(q28, NativePushdown);

        var polarsResult = q28.ToList();
        Assert.Single(polarsResult);

        // DuckDB Cross-Validation
        var duckDbSql = $@"
            SELECT *
            FROM
              (SELECT avg(ss_list_price) B1_LP,
                      count(ss_list_price) B1_CNT,
                      count(DISTINCT ss_list_price) B1_CNTD
               FROM read_parquet('{TpcdsData.StoreSalesPath}')
               WHERE ss_quantity BETWEEN 0 AND 5
                 AND (ss_list_price BETWEEN 8 AND 8+10
                      OR ss_coupon_amt BETWEEN 459 AND 459+1000
                      OR ss_wholesale_cost BETWEEN 57 AND 57+20)) B1,
              (SELECT avg(ss_list_price) B2_LP,
                      count(ss_list_price) B2_CNT,
                      count(DISTINCT ss_list_price) B2_CNTD
               FROM read_parquet('{TpcdsData.StoreSalesPath}')
               WHERE ss_quantity BETWEEN 6 AND 10
                 AND (ss_list_price BETWEEN 90 AND 90+10
                      OR ss_coupon_amt BETWEEN 2323 AND 2323+1000
                      OR ss_wholesale_cost BETWEEN 31 AND 31+20)) B2,
              (SELECT avg(ss_list_price) B3_LP,
                      count(ss_list_price) B3_CNT,
                      count(DISTINCT ss_list_price) B3_CNTD
               FROM read_parquet('{TpcdsData.StoreSalesPath}')
               WHERE ss_quantity BETWEEN 11 AND 15
                 AND (ss_list_price BETWEEN 142 AND 142+10
                      OR ss_coupon_amt BETWEEN 12214 AND 12214+1000
                      OR ss_wholesale_cost BETWEEN 79 AND 79+20)) B3,
              (SELECT avg(ss_list_price) B4_LP,
                      count(ss_list_price) B4_CNT,
                      count(DISTINCT ss_list_price) B4_CNTD
               FROM read_parquet('{TpcdsData.StoreSalesPath}')
               WHERE ss_quantity BETWEEN 16 AND 20
                 AND (ss_list_price BETWEEN 135 AND 135+10
                      OR ss_coupon_amt BETWEEN 6071 AND 6071+1000
                      OR ss_wholesale_cost BETWEEN 38 AND 38+20)) B4,
              (SELECT avg(ss_list_price) B5_LP,
                      count(ss_list_price) B5_CNT,
                      count(DISTINCT ss_list_price) B5_CNTD
               FROM read_parquet('{TpcdsData.StoreSalesPath}')
               WHERE ss_quantity BETWEEN 21 AND 25
                 AND (ss_list_price BETWEEN 122 AND 122+10
                      OR ss_coupon_amt BETWEEN 836 AND 836+1000
                      OR ss_wholesale_cost BETWEEN 17 AND 17+20)) B5,
              (SELECT avg(ss_list_price) B6_LP,
                      count(ss_list_price) B6_CNT,
                      count(DISTINCT ss_list_price) B6_CNTD
               FROM read_parquet('{TpcdsData.StoreSalesPath}')
               WHERE ss_quantity BETWEEN 26 AND 30
                 AND (ss_list_price BETWEEN 154 AND 154+10
                      OR ss_coupon_amt BETWEEN 7326 AND 7326+1000
                      OR ss_wholesale_cost BETWEEN 7 AND 7+20)) B6
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            B1_LP = r.IsDBNull(0) ? (double?)null : r.GetDouble(0),
            B1_CNT = r.GetInt64(1),
            B1_CNTD = r.GetInt64(2),
            B2_LP = r.IsDBNull(3) ? (double?)null : r.GetDouble(3),
            B2_CNT = r.GetInt64(4),
            B2_CNTD = r.GetInt64(5),
            B3_LP = r.IsDBNull(6) ? (double?)null : r.GetDouble(6),
            B3_CNT = r.GetInt64(7),
            B3_CNTD = r.GetInt64(8),
            B4_LP = r.IsDBNull(9) ? (double?)null : r.GetDouble(9),
            B4_CNT = r.GetInt64(10),
            B4_CNTD = r.GetInt64(11),
            B5_LP = r.IsDBNull(12) ? (double?)null : r.GetDouble(12),
            B5_CNT = r.GetInt64(13),
            B5_CNTD = r.GetInt64(14),
            B6_LP = r.IsDBNull(15) ? (double?)null : r.GetDouble(15),
            B6_CNT = r.GetInt64(16),
            B6_CNTD = r.GetInt64(17),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        var d = duckDbResults[0];
        var p = polarsResult[0];

        if (d.B1_LP == null) Assert.Null(p.B1_LP); else Assert.Equal(d.B1_LP.Value, (double)p.B1_LP!, 4);
        Assert.Equal(d.B1_CNT, p.B1_CNT);
        Assert.Equal(d.B1_CNTD, p.B1_CNTD);

        if (d.B2_LP == null) Assert.Null(p.B2_LP); else Assert.Equal(d.B2_LP.Value, (double)p.B2_LP!, 4);
        Assert.Equal(d.B2_CNT, p.B2_CNT);
        Assert.Equal(d.B2_CNTD, p.B2_CNTD);

        if (d.B3_LP == null) Assert.Null(p.B3_LP); else Assert.Equal(d.B3_LP.Value, (double)p.B3_LP!, 4);
        Assert.Equal(d.B3_CNT, p.B3_CNT);
        Assert.Equal(d.B3_CNTD, p.B3_CNTD);

        if (d.B4_LP == null) Assert.Null(p.B4_LP); else Assert.Equal(d.B4_LP.Value, (double)p.B4_LP!, 4);
        Assert.Equal(d.B4_CNT, p.B4_CNT);
        Assert.Equal(d.B4_CNTD, p.B4_CNTD);

        if (d.B5_LP == null) Assert.Null(p.B5_LP); else Assert.Equal(d.B5_LP.Value, (double)p.B5_LP!, 4);
        Assert.Equal(d.B5_CNT, p.B5_CNT);
        Assert.Equal(d.B5_CNTD, p.B5_CNTD);

        if (d.B6_LP == null) Assert.Null(p.B6_LP); else Assert.Equal(d.B6_LP.Value, (double)p.B6_LP!, 4);
        Assert.Equal(d.B6_CNT, p.B6_CNT);
        Assert.Equal(d.B6_CNTD, p.B6_CNTD);
    }

    /*
     * TPC-DS Query 29
     *
     * SELECT i_item_id,
     *        i_item_desc,
     *        s_store_id,
     *        s_store_name,
     *        sum(ss_quantity) AS store_sales_quantity,
     *        sum(sr_return_quantity) AS store_returns_quantity,
     *        sum(cs_quantity) AS catalog_sales_quantity
     * FROM store_sales,
     *      store_returns,
     *      catalog_sales,
     *      date_dim d1,
     *      date_dim d2,
     *      date_dim d3,
     *      store,
     *      item
     * WHERE d1.d_moy = 9
     *   AND d1.d_year = 1999
     *   AND d1.d_date_sk = ss_sold_date_sk
     *   AND i_item_sk = ss_item_sk
     *   AND s_store_sk = ss_store_sk
     *   AND ss_customer_sk = sr_customer_sk
     *   AND ss_item_sk = sr_item_sk
     *   AND ss_ticket_number = sr_ticket_number
     *   AND sr_returned_date_sk = d2.d_date_sk
     *   AND d2.d_moy BETWEEN 9 AND 9 + 3
     *   AND d2.d_year = 1999
     *   AND sr_customer_sk = cs_bill_customer_sk
     *   AND sr_item_sk = cs_item_sk
     *   AND cs_sold_date_sk = d3.d_date_sk
     *   AND d3.d_year IN (1999,
     *                     1999+1,
     *                     1999+2)
     * GROUP BY i_item_id,
     *          i_item_desc,
     *          s_store_id,
     *          s_store_name
     * ORDER BY i_item_id,
     *          i_item_desc,
     *          s_store_id,
     *          s_store_name
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q29")]
    public void Test_Q29()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery1 = TpcdsData.DateDimQuery();
        var dateDimQuery2 = TpcdsData.DateDimQuery();
        var dateDimQuery3 = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var d1 = dateDimQuery1.Where(d => d.d_year == 1999 && d.d_moy == 9);
        var d2 = dateDimQuery2.Where(d => d.d_year == 1999 && d.d_moy >= 9 && d.d_moy <= 12);
        var d3 = dateDimQuery3.Where(d => d.d_year >= 1999 && d.d_year <= 2001);

        var q29 = (from ss in storeSalesQuery
                   join d_1 in d1 on ss.ss_sold_date_sk equals d_1.d_date_sk
                   join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   join sr in storeReturnsQuery on new { customer_sk = ss.ss_customer_sk, item_sk = ss.ss_item_sk, ticket_number = ss.ss_ticket_number }
                                            equals new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk, ticket_number = sr.sr_ticket_number }
                   join d_2 in d2 on sr.sr_returned_date_sk equals d_2.d_date_sk
                   join cs in catalogSalesQuery on new { customer_sk = sr.sr_customer_sk, item_sk = sr.sr_item_sk }
                                            equals new { customer_sk = cs.cs_bill_customer_sk, item_sk = cs.cs_item_sk }
                   join d_3 in d3 on cs.cs_sold_date_sk equals d_3.d_date_sk
                   group new { ss, sr, cs } by new
                   {
                       i.i_item_id,
                       i.i_item_desc,
                       s.s_store_id,
                       s.s_store_name
                   } into g
                   orderby g.Key.i_item_id,
                           g.Key.i_item_desc,
                           g.Key.s_store_id,
                           g.Key.s_store_name
                   select new
                   {
                       g.Key.i_item_id,
                       g.Key.i_item_desc,
                       g.Key.s_store_id,
                       g.Key.s_store_name,
                       store_sales_quantity = g.Sum(x => x.ss.ss_quantity),
                       store_returns_quantity = g.Sum(x => x.sr.sr_return_quantity),
                       catalog_sales_quantity = g.Sum(x => x.cs.cs_quantity)
                   }).Take(100);

        AssertPlan(q29, NativePushdown);

        var polarsResult = q29.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            SELECT 
                i_item_id
                , i_item_desc
                , s_store_id
                , s_store_name
                , cast(sum(ss_quantity) as bigint) as store_sales_quantity
                , cast(sum(sr_return_quantity) as bigint) as store_returns_quantity
                , cast(sum(cs_quantity) as bigint) as catalog_sales_quantity
            FROM 
                read_parquet('{TpcdsData.StoreSalesPath}')
                , read_parquet('{TpcdsData.StoreReturnsPath}')
                , read_parquet('{TpcdsData.CatalogSalesPath}')
                , read_parquet('{TpcdsData.DateDimPath}') d1
                , read_parquet('{TpcdsData.DateDimPath}') d2
                , read_parquet('{TpcdsData.DateDimPath}') d3
                , read_parquet('{TpcdsData.StorePath}')
                , read_parquet('{TpcdsData.ItemPath}')
            WHERE 
                d1.d_moy = 9 
                and d1.d_year = 1999 
                and d1.d_date_sk = ss_sold_date_sk 
                and i_item_sk = ss_item_sk 
                and s_store_sk = ss_store_sk 
                and ss_customer_sk = sr_customer_sk 
                and ss_item_sk = sr_item_sk 
                and ss_ticket_number = sr_ticket_number 
                and sr_returned_date_sk = d2.d_date_sk 
                and d2.d_moy between 9 and 9 + 3 
                and d2.d_year = 1999 
                and sr_customer_sk = cs_bill_customer_sk 
                and sr_item_sk = cs_item_sk 
                and cs_sold_date_sk = d3.d_date_sk 
                and d3.d_year in (1999, 1999+1, 1999+2)
            GROUP BY 
                i_item_id
                , i_item_desc
                , s_store_id
                , s_store_name
            ORDER BY 
                i_item_id
                , i_item_desc
                , s_store_id
                , s_store_name
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            s_store_id = r.GetString(2),
            s_store_name = r.IsDBNull(3) ? null : r.GetString(3),
            store_sales_quantity = r.IsDBNull(4) ? null : (long?)r.GetInt64(4),
            store_returns_quantity = r.IsDBNull(5) ? null : (long?)r.GetInt64(5),
            catalog_sales_quantity = r.IsDBNull(6) ? null : (long?)r.GetInt64(6),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.i_item_id, p.i_item_id);
            Assert.Equal(d.i_item_desc, p.i_item_desc);
            Assert.Equal(d.s_store_id, p.s_store_id);
            Assert.Equal(d.s_store_name, p.s_store_name);
            Assert.Equal(d.store_sales_quantity, p.store_sales_quantity);
            Assert.Equal(d.store_returns_quantity, p.store_returns_quantity);
            Assert.Equal(d.catalog_sales_quantity, p.catalog_sales_quantity);
        }
    }

    /*
     * TPC-DS Query 30
     *
     * WITH customer_total_return AS
     *   (SELECT wr_returning_customer_sk AS ctr_customer_sk,
     *           ca_state AS ctr_state,
     *           sum(wr_return_amt) AS ctr_total_return
     *    FROM web_returns,
     *         date_dim,
     *         customer_address
     *    WHERE wr_returned_date_sk = d_date_sk
     *      AND d_year = 2002
     *      AND wr_returning_addr_sk = ca_address_sk
     *    GROUP BY wr_returning_customer_sk,
     *             ca_state)
     * SELECT c_customer_id,
     *        c_salutation,
     *        c_first_name,
     *        c_last_name,
     *        c_preferred_cust_flag,
     *        c_birth_day,
     *        c_birth_month,
     *        c_birth_year,
     *        c_birth_country,
     *        c_login,
     *        c_email_address,
     *        c_last_review_date_sk,
     *        ctr_total_return
     * FROM customer_total_return ctr1,
     *      customer_address,
     *      customer
     * WHERE ctr1.ctr_total_return >
     *     (SELECT avg(ctr_total_return)*1.2
     *      FROM customer_total_return ctr2
     *      WHERE ctr1.ctr_state = ctr2.ctr_state)
     *   AND ca_address_sk = c_current_addr_sk
     *   AND ca_state = 'GA'
     *   AND ctr1.ctr_customer_sk = c_customer_sk
     * ORDER BY c_customer_id NULLS FIRST,
     *          c_salutation NULLS FIRST,
     *          c_first_name NULLS FIRST,
     *          c_last_name NULLS FIRST,
     *          c_preferred_cust_flag NULLS FIRST,
     *          c_birth_day NULLS FIRST,
     *          c_birth_month NULLS FIRST,
     *          c_birth_year NULLS FIRST,
     *          c_birth_country NULLS FIRST,
     *          c_login NULLS FIRST,
     *          c_email_address NULLS FIRST,
     *          c_last_review_date_sk NULLS FIRST,
     *          ctr_total_return NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q30")]
    public void Test_Q30()
    {
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_year == 2002);

        var ctr = from wr in webReturnsQuery
                  join d in filteredDates on wr.wr_returned_date_sk equals d.d_date_sk
                  join ca in customerAddressQuery on wr.wr_returning_addr_sk equals ca.ca_address_sk
                  group wr by new { wr.wr_returning_customer_sk, ca.ca_state } into g
                  select new
                  {
                      ctr_customer_sk = g.Key.wr_returning_customer_sk,
                      ctr_state = g.Key.ca_state,
                      ctr_total_return = g.Sum(x => x.wr_return_amt)
                  };

        var filteredCa = customerAddressQuery.Where(ca => ca.ca_state == "GA");

        var q30 = (from ctr1 in ctr
                   where ctr1.ctr_total_return > ctr.Where(ctr2 => ctr2.ctr_state == ctr1.ctr_state)
                                                    .Average(ctr2 => ctr2.ctr_total_return) * 1.2m
                   join c in customerQuery on ctr1.ctr_customer_sk equals c.c_customer_sk
                   join ca in filteredCa on c.c_current_addr_sk equals ca.ca_address_sk
                   orderby c.c_customer_id,
                           c.c_salutation,
                           c.c_first_name,
                           c.c_last_name,
                           c.c_preferred_cust_flag,
                           c.c_birth_day,
                           c.c_birth_month,
                           c.c_birth_year,
                           c.c_birth_country,
                           c.c_login,
                           c.c_email_address,
                           c.c_last_review_date_sk,
                           ctr1.ctr_total_return
                   select new
                   {
                       c.c_customer_id,
                       c.c_salutation,
                       c.c_first_name,
                       c.c_last_name,
                       c.c_preferred_cust_flag,
                       c.c_birth_day,
                       c.c_birth_month,
                       c.c_birth_year,
                       c.c_birth_country,
                       c.c_login,
                       c.c_email_address,
                       c.c_last_review_date_sk,
                       ctr1.ctr_total_return
                   }).Take(100);

        AssertPlan(q30, NativePushdown);

        var polarsResult = q30.ToList();

        // DuckDB Cross Validation
        var duckDbSql = $@"
            WITH customer_total_return AS
              (SELECT wr_returning_customer_sk AS ctr_customer_sk,
                      ca_state AS ctr_state,
                      sum(wr_return_amt) AS ctr_total_return
               FROM read_parquet('{TpcdsData.WebReturnsPath}'),
                    read_parquet('{TpcdsData.DateDimPath}'),
                    read_parquet('{TpcdsData.CustomerAddressPath}')
               WHERE wr_returned_date_sk = d_date_sk
                 AND d_year = 2002
                 AND wr_returning_addr_sk = ca_address_sk
               GROUP BY wr_returning_customer_sk,
                        ca_state)
            SELECT c_customer_id,
                   c_salutation,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_day,
                   c_birth_month,
                   c_birth_year,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   c_last_review_date_sk,
                   ctr_total_return
            FROM customer_total_return ctr1,
                 read_parquet('{TpcdsData.CustomerAddressPath}'),
                 read_parquet('{TpcdsData.CustomerPath}')
            WHERE ctr1.ctr_total_return >
                (SELECT avg(ctr_total_return)*1.2
                 FROM customer_total_return ctr2
                 WHERE ctr1.ctr_state = ctr2.ctr_state)
              AND ca_address_sk = c_current_addr_sk
              AND ca_state = 'GA'
              AND ctr1.ctr_customer_sk = c_customer_sk
            ORDER BY c_customer_id NULLS FIRST,
                     c_salutation NULLS FIRST,
                     c_first_name NULLS FIRST,
                     c_last_name NULLS FIRST,
                     c_preferred_cust_flag NULLS FIRST,
                     c_birth_day NULLS FIRST,
                     c_birth_month NULLS FIRST,
                     c_birth_year NULLS FIRST,
                     c_birth_country NULLS FIRST,
                     c_login NULLS FIRST,
                     c_email_address NULLS FIRST,
                     c_last_review_date_sk NULLS FIRST,
                     ctr_total_return NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResults = DuckDbFixture.Query(duckDbSql, r => new
        {
            c_customer_id = r.IsDBNull(0) ? null : r.GetString(0),
            c_salutation = r.IsDBNull(1) ? null : r.GetString(1),
            c_first_name = r.IsDBNull(2) ? null : r.GetString(2),
            c_last_name = r.IsDBNull(3) ? null : r.GetString(3),
            c_preferred_cust_flag = r.IsDBNull(4) ? null : r.GetString(4),
            c_birth_day = r.IsDBNull(5) ? null : (long?)r.GetInt64(5),
            c_birth_month = r.IsDBNull(6) ? null : (long?)r.GetInt64(6),
            c_birth_year = r.IsDBNull(7) ? null : (long?)r.GetInt64(7),
            c_birth_country = r.IsDBNull(8) ? null : r.GetString(8),
            c_login = r.IsDBNull(9) ? null : r.GetString(9),
            c_email_address = r.IsDBNull(10) ? null : r.GetString(10),
            c_last_review_date_sk = r.IsDBNull(11) ? null : (int?)r.GetInt32(11),
            ctr_total_return = r.IsDBNull(12) ? null : (decimal?)r.GetDecimal(12),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.c_customer_id, p.c_customer_id);
            Assert.Equal(d.c_salutation, p.c_salutation);
            Assert.Equal(d.c_first_name, p.c_first_name);
            Assert.Equal(d.c_last_name, p.c_last_name);
            Assert.Equal(d.c_preferred_cust_flag, p.c_preferred_cust_flag);
            Assert.Equal(d.c_birth_day, p.c_birth_day);
            Assert.Equal(d.c_birth_month, p.c_birth_month);
            Assert.Equal(d.c_birth_year, p.c_birth_year);
            Assert.Equal(d.c_birth_country, p.c_birth_country);
            Assert.Equal(d.c_login, p.c_login);
            Assert.Equal(d.c_email_address, p.c_email_address);
            Assert.Equal(d.c_last_review_date_sk, p.c_last_review_date_sk);
            Assert.Equal(d.ctr_total_return, p.ctr_total_return);
        }
    }
}

