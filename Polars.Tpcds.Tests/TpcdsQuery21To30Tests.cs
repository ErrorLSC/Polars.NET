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

        var joined = from inv in inventoryQuery
                     join w in warehouseQuery on inv.inv_warehouse_sk equals w.w_warehouse_sk
                     join i in filteredItems on inv.inv_item_sk equals i.i_item_sk
                     join d in filteredDates on inv.inv_date_sk equals d.d_date_sk
                     select new
                     {
                         w.w_warehouse_name,
                         i.i_item_id,
                         inv.inv_quantity_on_hand,
                         d.d_date
                     };

        var grouped = from x in joined
                      group x by new
                      {
                          x.w_warehouse_name,
                          x.i_item_id
                      } into g
                      select new
                      {
                          g.Key.w_warehouse_name,
                          g.Key.i_item_id,
                          inv_before = g.Sum(x => x.d_date < pivotDate ? (long)x.inv_quantity_on_hand! : 0L),
                          inv_after = g.Sum(x => x.d_date >= pivotDate ? (long)x.inv_quantity_on_hand! : 0L)
                      };

        var q21 = from x in grouped
                  where x.inv_before > 0 &&
                        (double)x.inv_after / (double)x.inv_before >= 2.0 / 3.0 &&
                        (double)x.inv_after / (double)x.inv_before <= 3.0 / 2.0
                  orderby x.w_warehouse_name, x.i_item_id
                  select x;

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

        var joined = from inv in inventoryQuery
                     join d in filteredDates on inv.inv_date_sk equals d.d_date_sk
                     join i in itemQuery on inv.inv_item_sk equals i.i_item_sk
                     select new
                     {
                         i.i_product_name,
                         i.i_brand,
                         i.i_class,
                         i.i_category,
                         inv.inv_quantity_on_hand
                     };

        var q22 = joined
            .GroupByRollup(
                x => new { x.i_product_name, x.i_brand, x.i_class, x.i_category },
                (key, g) => new
                {
                    key.i_product_name,
                    key.i_brand,
                    key.i_class,
                    key.i_category,
                    qoh = g.Average(x => (double?)x.inv_quantity_on_hand)
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
        var maxSales = (from ss in storeSalesQuery
                        join d in dates4Years on ss.ss_sold_date_sk equals d.d_date_sk
                        group ss by ss.ss_customer_sk into g
                        select g.Sum(x => x.ss_quantity * x.ss_sales_price)).Max();

        var threshold = (decimal?)(maxSales * 0.95m);

        // 3. best_ss_customer CTE
        var bestCustomers = (from ss in storeSalesQuery
                             group ss by ss.ss_customer_sk into g
                             where g.Sum(x => x.ss_quantity * x.ss_sales_price) > threshold
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
}

