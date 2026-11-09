namespace Polars.Tpcds;

public static partial class TpcdsCases
{
    /*
     * TPC-DS Query 41
     *
     * SELECT distinct(i_product_name)
     * FROM item i1
     * WHERE i_manufact_id BETWEEN 738 AND 738+40
     *   AND
     *     (SELECT count(*) AS item_cnt
     *      FROM item
     *      WHERE (i_manufact = i1.i_manufact
     *             AND ((i_category = 'Women'
     *                   AND (i_color = 'powder'
     *                        OR i_color = 'khaki')
     *                   AND (i_units = 'Ounce'
     *                        OR i_units = 'Oz')
     *                   AND (i_size = 'medium'
     *                        OR i_size = 'extra large'))
     *                  OR (i_category = 'Women'
     *                      AND (i_color = 'brown'
     *                           OR i_color = 'honeydew')
     *                      AND (i_units = 'Bunch'
     *                           OR i_units = 'Ton')
     *                      AND (i_size = 'N/A'
     *                           OR i_size = 'small'))
     *                  OR (i_category = 'Men'
     *                      AND (i_color = 'floral'
     *                           OR i_color = 'deep')
     *                      AND (i_units = 'N/A'
     *                           OR i_units = 'Dozen')
     *                      AND (i_size = 'petite'
     *                           OR i_size = 'petite'))
     *                  OR (i_category = 'Men'
     *                      AND (i_color = 'light'
     *                           OR i_color = 'cornflower')
     *                      AND (i_units = 'Box'
     *                           OR i_units = 'Pound')
     *                      AND (i_size = 'medium'
     *                           OR i_size = 'extra large'))))
     *        OR (i_manufact = i1.i_manufact
     *            AND ((i_category = 'Women'
     *                  AND (i_color = 'midnight'
     *                       OR i_color = 'snow')
     *                  AND (i_units = 'Pallet'
     *                       OR i_units = 'Gross')
     *                  AND (i_size = 'medium'
     *                       OR i_size = 'extra large'))
     *                 OR (i_category = 'Women'
     *                     AND (i_color = 'cyan'
     *                          OR i_color = 'papaya')
     *                     AND (i_units = 'Cup'
     *                          OR i_units = 'Dram')
     *                     AND (i_size = 'N/A'
     *                          OR i_size = 'small'))
     *                 OR (i_category = 'Men'
     *                     AND (i_color = 'orange'
     *                          OR i_color = 'frosted')
     *                     AND (i_units = 'Each'
     *                          OR i_units = 'Tbl')
     *                     AND (i_size = 'petite'
     *                          OR i_size = 'petite'))
     *                 OR (i_category = 'Men'
     *                     AND (i_color = 'forest'
     *                          OR i_color = 'ghost')
     *                     AND (i_units = 'Lb'
     *                          OR i_units = 'Bundle')
     *                     AND (i_size = 'medium'
     *                          OR i_size = 'extra large'))))) > 0
     * ORDER BY i_product_name
     * LIMIT 100;
     */
    public static IQueryable<object> Q41()
    {
        var itemQuery = TpcdsData.ItemQuery();

        var validMfg = itemQuery
            .Where(i =>
                (i.i_category == "Women" && (i.i_color == "powder" || i.i_color == "khaki") && (i.i_units == "Ounce" || i.i_units == "Oz") && (i.i_size == "medium" || i.i_size == "extra large")) ||
                (i.i_category == "Women" && (i.i_color == "brown" || i.i_color == "honeydew") && (i.i_units == "Bunch" || i.i_units == "Ton") && (i.i_size == "N/A" || i.i_size == "small")) ||
                (i.i_category == "Men" && (i.i_color == "floral" || i.i_color == "deep") && (i.i_units == "N/A" || i.i_units == "Dozen") && (i.i_size == "petite" || i.i_size == "petite")) ||
                (i.i_category == "Men" && (i.i_color == "light" || i.i_color == "cornflower") && (i.i_units == "Box" || i.i_units == "Pound") && (i.i_size == "medium" || i.i_size == "extra large")) ||
                (i.i_category == "Women" && (i.i_color == "midnight" || i.i_color == "snow") && (i.i_units == "Pallet" || i.i_units == "Gross") && (i.i_size == "medium" || i.i_size == "extra large")) ||
                (i.i_category == "Women" && (i.i_color == "cyan" || i.i_color == "papaya") && (i.i_units == "Cup" || i.i_units == "Dram") && (i.i_size == "N/A" || i.i_size == "small")) ||
                (i.i_category == "Men" && (i.i_color == "orange" || i.i_color == "frosted") && (i.i_units == "Each" || i.i_units == "Tbl") && (i.i_size == "petite" || i.i_size == "petite")) ||
                (i.i_category == "Men" && (i.i_color == "forest" || i.i_color == "ghost") && (i.i_units == "Lb" || i.i_units == "Bundle") && (i.i_size == "medium" || i.i_size == "extra large"))
            )
            .Select(i => new { i.i_manufact })
            .Distinct();

        var q41 = (from i1 in itemQuery
                   where i1.i_manufact_id >= 738 && i1.i_manufact_id <= 738 + 40
                   join mfg in validMfg on i1.i_manufact equals mfg.i_manufact
                   select i1.i_product_name)
                  .Distinct()
                  .OrderBy(p => p)
                  .Take(100);

        return q41;
    }

    public static string Q41Sql() =>
        $@"
            SELECT distinct(i_product_name)
            FROM read_parquet('{TpcdsData.ItemPath}') i1
            WHERE i_manufact_id BETWEEN 738 AND 738+40
              AND
                (SELECT count(*) AS item_cnt
                 FROM read_parquet('{TpcdsData.ItemPath}')
                 WHERE (i_manufact = i1.i_manufact
                        AND ((i_category = 'Women'
                              AND (i_color = 'powder'
                                   OR i_color = 'khaki')
                              AND (i_units = 'Ounce'
                                   OR i_units = 'Oz')
                              AND (i_size = 'medium'
                                   OR i_size = 'extra large'))
                             OR (i_category = 'Women'
                                 AND (i_color = 'brown'
                                      OR i_color = 'honeydew')
                                 AND (i_units = 'Bunch'
                                      OR i_units = 'Ton')
                                 AND (i_size = 'N/A'
                                      OR i_size = 'small'))
                             OR (i_category = 'Men'
                                 AND (i_color = 'floral'
                                      OR i_color = 'deep')
                                 AND (i_units = 'N/A'
                                      OR i_units = 'Dozen')
                                 AND (i_size = 'petite'
                                      OR i_size = 'petite'))
                             OR (i_category = 'Men'
                                 AND (i_color = 'light'
                                      OR i_color = 'cornflower')
                                 AND (i_units = 'Box'
                                      OR i_units = 'Pound')
                                 AND (i_size = 'medium'
                                      OR i_size = 'extra large'))))
                   OR (i_manufact = i1.i_manufact
                       AND ((i_category = 'Women'
                             AND (i_color = 'midnight'
                                  OR i_color = 'snow')
                             AND (i_units = 'Pallet'
                                  OR i_units = 'Gross')
                             AND (i_size = 'medium'
                                  OR i_size = 'extra large'))
                            OR (i_category = 'Women'
                                AND (i_color = 'cyan'
                                     OR i_color = 'papaya')
                                AND (i_units = 'Cup'
                                     OR i_units = 'Dram')
                                AND (i_size = 'N/A'
                                     OR i_size = 'small'))
                            OR (i_category = 'Men'
                                AND (i_color = 'orange'
                                     OR i_color = 'frosted')
                                AND (i_units = 'Each'
                                     OR i_units = 'Tbl')
                                AND (i_size = 'petite'
                                     OR i_size = 'petite'))
                            OR (i_category = 'Men'
                                AND (i_color = 'forest'
                                     OR i_color = 'ghost')
                                AND (i_units = 'Lb'
                                     OR i_units = 'Bundle')
                                AND (i_size = 'medium'
                                     OR i_size = 'extra large'))))) > 0
            ORDER BY i_product_name
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 42
     *
     * SELECT dt.d_year,
     *        item.i_category_id,
     *        item.i_category,
     *        sum(ss_ext_sales_price)
     * FROM date_dim dt,
     *      store_sales,
     *      item
     * WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
     *   AND store_sales.ss_item_sk = item.i_item_sk
     *   AND item.i_manager_id = 1
     *   AND dt.d_moy=11
     *   AND dt.d_year=2000
     * GROUP BY dt.d_year,
     *          item.i_category_id,
     *          item.i_category
     * ORDER BY sum(ss_ext_sales_price) DESC,dt.d_year,
     *                                       item.i_category_id,
     *                                       item.i_category
     * LIMIT 100;
     */
    public static IQueryable<object> Q42()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var q42 = (from dt in dateDimQuery
                   join ss in storeSalesQuery on dt.d_date_sk equals ss.ss_sold_date_sk
                   join item in itemQuery on ss.ss_item_sk equals item.i_item_sk
                   where item.i_manager_id == 1 && dt.d_moy == 11 && dt.d_year == 2000
                   group ss by new { dt.d_year, item.i_category_id, item.i_category } into g
                   orderby g.Sum(x => x.ss_ext_sales_price) descending, g.Key.d_year, g.Key.i_category_id, g.Key.i_category
                   select new
                   {
                       g.Key.d_year,
                       g.Key.i_category_id,
                       g.Key.i_category,
                       total_sales = g.Sum(x => x.ss_ext_sales_price)
                   })
                  .Take(100);

        return q42;
    }

    public static string Q42Sql() =>
        $@"
            SELECT dt.d_year,
                   item.i_category_id,
                   item.i_category,
                   sum(ss_ext_sales_price)
            FROM read_parquet('{TpcdsData.DateDimPath}') dt,
                 read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                 read_parquet('{TpcdsData.ItemPath}') item
            WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
              AND store_sales.ss_item_sk = item.i_item_sk
              AND item.i_manager_id = 1
              AND dt.d_moy=11
              AND dt.d_year=2000
            GROUP BY dt.d_year,
                     item.i_category_id,
                     item.i_category
            ORDER BY sum(ss_ext_sales_price) DESC,dt.d_year,
                                                  item.i_category_id,
                                                  item.i_category
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 43
     *
     * SELECT s_store_name,
     *        s_store_id,
     *        sum(CASE
     *                WHEN (d_day_name='Sunday') THEN ss_sales_price
     *                ELSE NULL
     *            END) sun_sales,
     *        sum(CASE
     *                WHEN (d_day_name='Monday') THEN ss_sales_price
     *                ELSE NULL
     *            END) mon_sales,
     *        sum(CASE
     *                WHEN (d_day_name='Tuesday') THEN ss_sales_price
     *                ELSE NULL
     *            END) tue_sales,
     *        sum(CASE
     *                WHEN (d_day_name='Wednesday') THEN ss_sales_price
     *                ELSE NULL
     *            END) wed_sales,
     *        sum(CASE
     *                WHEN (d_day_name='Thursday') THEN ss_sales_price
     *                ELSE NULL
     *            END) thu_sales,
     *        sum(CASE
     *                WHEN (d_day_name='Friday') THEN ss_sales_price
     *                ELSE NULL
     *            END) fri_sales,
     *        sum(CASE
     *                WHEN (d_day_name='Saturday') THEN ss_sales_price
     *                ELSE NULL
     *            END) sat_sales
     * FROM date_dim,
     *      store_sales,
     *      store
     * WHERE d_date_sk = ss_sold_date_sk
     *   AND s_store_sk = ss_store_sk
     *   AND s_gmt_offset = -5
     *   AND d_year = 2000
     * GROUP BY s_store_name,
     *          s_store_id
     * ORDER BY s_store_name,
     *          s_store_id,
     *          sun_sales,
     *          mon_sales,
     *          tue_sales,
     *          wed_sales,
     *          thu_sales,
     *          fri_sales,
     *          sat_sales
     * LIMIT 100;
     */
    public static IQueryable<object> Q43()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var q43 = (from dt in dateDimQuery
                   join ss in storeSalesQuery on dt.d_date_sk equals ss.ss_sold_date_sk
                   join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                   where s.s_gmt_offset == -5m && dt.d_year == 2000
                   group new { dt, ss } by new { s.s_store_name, s.s_store_id } into g
                   select new
                   {
                       g.Key.s_store_name,
                       g.Key.s_store_id,
                       sun_sales = g.Sum(x => x.dt.d_day_name == "Sunday" ? x.ss.ss_sales_price : null),
                       mon_sales = g.Sum(x => x.dt.d_day_name == "Monday" ? x.ss.ss_sales_price : null),
                       tue_sales = g.Sum(x => x.dt.d_day_name == "Tuesday" ? x.ss.ss_sales_price : null),
                       wed_sales = g.Sum(x => x.dt.d_day_name == "Wednesday" ? x.ss.ss_sales_price : null),
                       thu_sales = g.Sum(x => x.dt.d_day_name == "Thursday" ? x.ss.ss_sales_price : null),
                       fri_sales = g.Sum(x => x.dt.d_day_name == "Friday" ? x.ss.ss_sales_price : null),
                       sat_sales = g.Sum(x => x.dt.d_day_name == "Saturday" ? x.ss.ss_sales_price : null),
                   })
                  .OrderBy(x => x.s_store_name)
                  .ThenBy(x => x.s_store_id)
                  .ThenBy(x => x.sun_sales)
                  .ThenBy(x => x.mon_sales)
                  .ThenBy(x => x.tue_sales)
                  .ThenBy(x => x.wed_sales)
                  .ThenBy(x => x.thu_sales)
                  .ThenBy(x => x.fri_sales)
                  .ThenBy(x => x.sat_sales)
                  .Take(100);

        return q43;
    }

    public static string Q43Sql() =>
        $@"
            SELECT s_store_name,
                   s_store_id,
                   sum(CASE
                           WHEN (d_day_name='Sunday') THEN ss_sales_price
                           ELSE NULL
                       END) sun_sales,
                   sum(CASE
                           WHEN (d_day_name='Monday') THEN ss_sales_price
                           ELSE NULL
                       END) mon_sales,
                   sum(CASE
                           WHEN (d_day_name='Tuesday') THEN ss_sales_price
                           ELSE NULL
                       END) tue_sales,
                   sum(CASE
                           WHEN (d_day_name='Wednesday') THEN ss_sales_price
                           ELSE NULL
                       END) wed_sales,
                   sum(CASE
                           WHEN (d_day_name='Thursday') THEN ss_sales_price
                           ELSE NULL
                       END) thu_sales,
                   sum(CASE
                           WHEN (d_day_name='Friday') THEN ss_sales_price
                           ELSE NULL
                       END) fri_sales,
                   sum(CASE
                           WHEN (d_day_name='Saturday') THEN ss_sales_price
                           ELSE NULL
                       END) sat_sales
            FROM read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.StorePath}')
            WHERE d_date_sk = ss_sold_date_sk
              AND s_store_sk = ss_store_sk
              AND s_gmt_offset = -5
              AND d_year = 2000
            GROUP BY s_store_name,
                     s_store_id
            ORDER BY s_store_name,
                     s_store_id,
                     sun_sales,
                     mon_sales,
                     tue_sales,
                     wed_sales,
                     thu_sales,
                     fri_sales,
                     sat_sales
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 44
     *
     * SELECT asceding.rnk,
     *        i1.i_product_name best_performing,
     *        i2.i_product_name worst_performing
     * FROM
     *   (SELECT *
     *    FROM
     *      (SELECT item_sk,
     *              rank() OVER (
     *                           ORDER BY rank_col ASC) rnk
     *       FROM
     *         (SELECT ss_item_sk item_sk,
     *                 avg(ss_net_profit) rank_col
     *          FROM store_sales ss1
     *          WHERE ss_store_sk = 4
     *          GROUP BY ss_item_sk
     *          HAVING avg(ss_net_profit) > 0.9*
     *            (SELECT avg(ss_net_profit) rank_col
     *             FROM store_sales
     *             WHERE ss_store_sk = 4
     *               AND ss_addr_sk IS NULL
     *             GROUP BY ss_store_sk))V1)V11
     *    WHERE rnk < 11) asceding,
     *   (SELECT *
     *    FROM
     *      (SELECT item_sk,
     *              rank() OVER (
     *                           ORDER BY rank_col DESC) rnk
     *       FROM
     *         (SELECT ss_item_sk item_sk,
     *                 avg(ss_net_profit) rank_col
     *          FROM store_sales ss1
     *          WHERE ss_store_sk = 4
     *          GROUP BY ss_item_sk
     *          HAVING avg(ss_net_profit) > 0.9*
     *            (SELECT avg(ss_net_profit) rank_col
     *             FROM store_sales
     *             WHERE ss_store_sk = 4
     *               AND ss_addr_sk IS NULL
     *             GROUP BY ss_store_sk))V2)V21
     *    WHERE rnk < 11) descending,
     *      item i1,
     *      item i2
     * WHERE asceding.rnk = descending.rnk
     *   AND i1.i_item_sk=asceding.item_sk
     *   AND i2.i_item_sk=descending.item_sk
     * ORDER BY asceding.rnk
     * LIMIT 100;
     */
    public static IQueryable<object> Q44()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var storeBenchmark = storeSalesQuery
            .Where(ss => ss.ss_store_sk == 4 && ss.ss_addr_sk == null);

        var ascQuery = from ss in storeSalesQuery
                       where ss.ss_store_sk == 4
                       group ss by ss.ss_item_sk into g
                       let rank_col = g.Average(x => (double?)x.ss_net_profit)
                       where rank_col > 0.9 * storeBenchmark.Average(ss => (double?)ss.ss_net_profit)
                       select new
                       {
                           item_sk = g.Key,
                           rnk = (long)rank_col.Rank(RankMethod.Min, false)
                       } into a
                       where a.rnk < 11
                       select a;

        var descQuery = from ss in storeSalesQuery
                        where ss.ss_store_sk == 4
                        group ss by ss.ss_item_sk into g
                        let rank_col = g.Average(x => (double?)x.ss_net_profit)
                        where rank_col > 0.9 * storeBenchmark.Average(ss => (double?)ss.ss_net_profit)
                        select new
                        {
                            item_sk = g.Key,
                            rnk = (long)rank_col.Rank(RankMethod.Min, true)
                        } into d
                        where d.rnk < 11
                        select d;

        var q44 = (from a in ascQuery
                   join d in descQuery on a.rnk equals d.rnk
                   join i1 in itemQuery on a.item_sk equals i1.i_item_sk
                   join i2 in itemQuery on d.item_sk equals i2.i_item_sk
                   orderby a.rnk, i1.i_product_name, i2.i_product_name // tie-breakers for equal ranks
                   select new
                   {
                       a.rnk,
                       best_performing = i1.i_product_name,
                       worst_performing = i2.i_product_name
                   })
                  .Take(100);

        return q44;
    }

    public static string Q44Sql() =>
        $@"
            SELECT asceding.rnk,
                   i1.i_product_name best_performing,
                   i2.i_product_name worst_performing
            FROM
              (SELECT *
               FROM
                 (SELECT item_sk,
                         rank() OVER (
                                      ORDER BY rank_col ASC) rnk
                  FROM
                    (SELECT ss_item_sk item_sk,
                            avg(ss_net_profit) rank_col
                     FROM read_parquet('{TpcdsData.StoreSalesPath}') ss1
                     WHERE ss_store_sk = 4
                     GROUP BY ss_item_sk
                     HAVING avg(ss_net_profit) > 0.9*
                       (SELECT avg(ss_net_profit) rank_col
                        FROM read_parquet('{TpcdsData.StoreSalesPath}')
                        WHERE ss_store_sk = 4
                          AND ss_addr_sk IS NULL
                        GROUP BY ss_store_sk))V1)V11
               WHERE rnk < 11) asceding,
              (SELECT *
               FROM
                 (SELECT item_sk,
                         rank() OVER (
                                      ORDER BY rank_col DESC) rnk
                  FROM
                    (SELECT ss_item_sk item_sk,
                            avg(ss_net_profit) rank_col
                     FROM read_parquet('{TpcdsData.StoreSalesPath}') ss1
                     WHERE ss_store_sk = 4
                     GROUP BY ss_item_sk
                     HAVING avg(ss_net_profit) > 0.9*
                       (SELECT avg(ss_net_profit) rank_col
                        FROM read_parquet('{TpcdsData.StoreSalesPath}')
                        WHERE ss_store_sk = 4
                          AND ss_addr_sk IS NULL
                        GROUP BY ss_store_sk))V2)V21
               WHERE rnk < 11) descending,
                 read_parquet('{TpcdsData.ItemPath}') i1,
                 read_parquet('{TpcdsData.ItemPath}') i2
            WHERE asceding.rnk = descending.rnk
              AND i1.i_item_sk=asceding.item_sk
              AND i2.i_item_sk=descending.item_sk
            ORDER BY asceding.rnk,
                     best_performing NULLS FIRST,
                     worst_performing NULLS FIRST
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 45
     *
     * SELECT ca_zip,
     *        ca_city,
     *        sum(ws_sales_price)
     * FROM web_sales,
     *      customer,
     *      customer_address,
     *      date_dim,
     *      item
     * WHERE ws_bill_customer_sk = c_customer_sk
     *   AND c_current_addr_sk = ca_address_sk
     *   AND ws_item_sk = i_item_sk
     *   AND (SUBSTRING(ca_zip,1,5) IN ('85669',
     *                               '86197',
     *                               '88274',
     *                               '83405',
     *                               '86475',
     *                               '85392',
     *                               '85460',
     *                               '80348',
     *                               '81792')
     *        OR i_item_id IN
     *          (SELECT i_item_id
     *           FROM item
     *           WHERE i_item_sk IN (2,
     *                               3,
     *                               5,
     *                               7,
     *                               11,
     *                               13,
     *                               17,
     *                               19,
     *                               23,
     *                               29) ))
     *   AND ws_sold_date_sk = d_date_sk
     *   AND d_qoy = 2
     *   AND d_year = 2001
     * GROUP BY ca_zip,
     *          ca_city
     * ORDER BY ca_zip,
     *          ca_city
     * LIMIT 100;
     */
    public static IQueryable<object> Q45()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();

        string[] zipList = ["85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792"];
        long[] itemSkList = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29];

        var targetItemIds = itemQuery
            .Where(i => itemSkList.Contains(i.i_item_sk))
            .Select(i => i.i_item_id);

        var q45 = (from ws in webSalesQuery
                   join c in customerQuery on ws.ws_bill_customer_sk equals c.c_customer_sk
                   join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                   join i in itemQuery on ws.ws_item_sk equals i.i_item_sk
                   join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                   where d.d_qoy == 2 && d.d_year == 2001
                      && (zipList.Contains(ca.ca_zip!.Substring(0, 5)) || targetItemIds.Contains(i.i_item_id))
                   group ws by new { ca.ca_zip, ca.ca_city } into g
                   orderby g.Key.ca_zip, g.Key.ca_city
                   select new
                   {
                       g.Key.ca_zip,
                       g.Key.ca_city,
                       total_sales = g.Sum(x => x.ws_sales_price)
                   })
                  .Take(100);

        return q45;
    }

    public static string Q45Sql() =>
        $@"
            SELECT ca_zip,
                   ca_city,
                   sum(ws_sales_price)
            FROM read_parquet('{TpcdsData.WebSalesPath}'),
                 read_parquet('{TpcdsData.CustomerPath}'),
                 read_parquet('{TpcdsData.CustomerAddressPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.ItemPath}')
            WHERE ws_bill_customer_sk = c_customer_sk
              AND c_current_addr_sk = ca_address_sk
              AND ws_item_sk = i_item_sk
              AND (SUBSTRING(ca_zip,1,5) IN ('85669',
                                          '86197',
                                          '88274',
                                          '83405',
                                          '86475',
                                          '85392',
                                          '85460',
                                          '80348',
                                          '81792')
                   OR i_item_id IN
                     (SELECT i_item_id
                      FROM read_parquet('{TpcdsData.ItemPath}')
                      WHERE i_item_sk IN (2,
                                          3,
                                          5,
                                          7,
                                          11,
                                          13,
                                          17,
                                          19,
                                          23,
                                          29) ))
              AND ws_sold_date_sk = d_date_sk
              AND d_qoy = 2
              AND d_year = 2001
            GROUP BY ca_zip,
                     ca_city
            ORDER BY ca_zip NULLS FIRST,
                     ca_city NULLS FIRST
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 46
     *
     * SELECT c_last_name,
     *        c_first_name,
     *        ca_city,
     *        bought_city,
     *        ss_ticket_number,
     *        amt,
     *        profit
     * FROM
     *   (SELECT ss_ticket_number,
     *           ss_customer_sk,
     *           ca_city bought_city,
     *           sum(ss_coupon_amt) amt,
     *           sum(ss_net_profit) profit
     *    FROM store_sales,
     *         date_dim,
     *         store,
     *         household_demographics,
     *         customer_address
     *    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     *      AND store_sales.ss_store_sk = store.s_store_sk
     *      AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     *      AND store_sales.ss_addr_sk = customer_address.ca_address_sk
     *      AND (household_demographics.hd_dep_count = 4
     *           OR household_demographics.hd_vehicle_count= 3)
     *      AND date_dim.d_dow IN (6,
     *                             0)
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
     *          c_first_name NULLS FIRST,
     *          ca_city NULLS FIRST,
     *          bought_city NULLS FIRST,
     *          ss_ticket_number NULLS FIRST
     * LIMIT 100;
     */
    public static IQueryable<object> Q46()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var hdQuery = TpcdsData.HouseholdDemographicsQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        long[] dowList = [ 6, 0 ];
        long[] yearList = [ 1999, 2000, 2001 ];
        string[] cityList = [ "Fairview", "Midway" ];

        var dn = from ss in storeSalesQuery
                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                 join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                 join hd in hdQuery on ss.ss_hdemo_sk equals hd.hd_demo_sk
                 join ca in customerAddressQuery on ss.ss_addr_sk equals ca.ca_address_sk
                 where (hd.hd_dep_count == 4 || hd.hd_vehicle_count == 3)
                    && dowList.Contains(d.d_dow!.Value)
                    && yearList.Contains(d.d_year!.Value)
                    && cityList.Contains(s.s_city)
                 group ss by new { ss.ss_ticket_number, ss.ss_customer_sk, ss.ss_addr_sk, ca.ca_city } into g
                 select new
                 {
                     g.Key.ss_ticket_number,
                     g.Key.ss_customer_sk,
                     bought_city = g.Key.ca_city,
                     amt = g.Sum(x => x.ss_coupon_amt),
                     profit = g.Sum(x => x.ss_net_profit)
                 };

        var q46 = (from d in dn
                   join c in customerQuery on d.ss_customer_sk equals c.c_customer_sk
                   join current_addr in customerAddressQuery on c.c_current_addr_sk equals current_addr.ca_address_sk
                   // SQL <> / = is unknown when either side is NULL; C# null semantics would compare NULLs.
                   where current_addr.ca_city != null && d.bought_city != null && current_addr.ca_city != d.bought_city
                   orderby c.c_last_name, c.c_first_name, current_addr.ca_city, d.bought_city, d.ss_ticket_number
                   select new
                   {
                       c.c_last_name,
                       c.c_first_name,
                       current_addr.ca_city,
                       d.bought_city,
                       d.ss_ticket_number,
                       d.amt,
                       d.profit
                   })
                  .Take(100);

        return q46;
    }

    public static string Q46Sql() =>
        $@"
            SELECT c_last_name,
                   c_first_name,
                   ca_city,
                   bought_city,
                   ss_ticket_number,
                   amt,
                   profit
            FROM
              (SELECT ss_ticket_number,
                      ss_customer_sk,
                      ca_city bought_city,
                      sum(ss_coupon_amt) amt,
                      sum(ss_net_profit) profit
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.CustomerAddressPath}') customer_address
               WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
                 AND store_sales.ss_store_sk = store.s_store_sk
                 AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND store_sales.ss_addr_sk = customer_address.ca_address_sk
                 AND (household_demographics.hd_dep_count = 4
                      OR household_demographics.hd_vehicle_count= 3)
                 AND date_dim.d_dow IN (6,
                                        0)
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
                     c_first_name NULLS FIRST,
                     ca_city NULLS FIRST,
                     bought_city NULLS FIRST,
                     ss_ticket_number NULLS FIRST
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 48
     *
     * SELECT SUM (ss_quantity)
     * FROM store_sales,
     *      store,
     *      customer_demographics,
     *      customer_address,
     *      date_dim
     * WHERE s_store_sk = ss_store_sk
     *   AND ss_sold_date_sk = d_date_sk
     *   AND d_year = 2000
     *   AND ((cd_demo_sk = ss_cdemo_sk
     *         AND cd_marital_status = 'M'
     *         AND cd_education_status = '4 yr Degree'
     *         AND ss_sales_price BETWEEN 100.00 AND 150.00)
     *        OR (cd_demo_sk = ss_cdemo_sk
     *            AND cd_marital_status = 'D'
     *            AND cd_education_status = '2 yr Degree'
     *            AND ss_sales_price BETWEEN 50.00 AND 100.00)
     *        OR (cd_demo_sk = ss_cdemo_sk
     *            AND cd_marital_status = 'S'
     *            AND cd_education_status = 'College'
     *            AND ss_sales_price BETWEEN 150.00 AND 200.00))
     *   AND ((ss_addr_sk = ca_address_sk
     *         AND ca_country = 'United States'
     *         AND ca_state IN ('CO',
     *                          'OH',
     *                          'TX')
     *         AND ss_net_profit BETWEEN 0 AND 2000)
     *        OR (ss_addr_sk = ca_address_sk
     *            AND ca_country = 'United States'
     *            AND ca_state IN ('OR',
     *                             'MN',
     *                             'KY')
     *            AND ss_net_profit BETWEEN 150 AND 3000)
     *        OR (ss_addr_sk = ca_address_sk
     *            AND ca_country = 'United States'
     *            AND ca_state IN ('VA',
     *                             'CA',
     *                             'MS')
     *            AND ss_net_profit BETWEEN 50 AND 25000)) ;
     */
    public static IQueryable<long?> Q48()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var cdQuery = TpcdsData.CustomerDemographicsQuery();
        var caQuery = TpcdsData.CustomerAddressQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        string[] stateList1 = ["CO", "OH", "TX"];
        string[] stateList2 = ["OR", "MN", "KY"];
        string[] stateList3 = ["VA", "CA", "MS"];

        var q48 = from ss in storeSalesQuery
                  join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                  join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                  join cd in cdQuery on ss.ss_cdemo_sk equals cd.cd_demo_sk
                  join ca in caQuery on ss.ss_addr_sk equals ca.ca_address_sk
                  where d.d_year == 2000
                     && ((cd.cd_marital_status == "M" && cd.cd_education_status == "4 yr Degree" && ss.ss_sales_price >= 100.00m && ss.ss_sales_price <= 150.00m)
                      || (cd.cd_marital_status == "D" && cd.cd_education_status == "2 yr Degree" && ss.ss_sales_price >= 50.00m && ss.ss_sales_price <= 100.00m)
                      || (cd.cd_marital_status == "S" && cd.cd_education_status == "College" && ss.ss_sales_price >= 150.00m && ss.ss_sales_price <= 200.00m))
                     && ((ca.ca_country == "United States" && stateList1.Contains(ca.ca_state) && ss.ss_net_profit >= 0m && ss.ss_net_profit <= 2000m)
                      || (ca.ca_country == "United States" && stateList2.Contains(ca.ca_state) && ss.ss_net_profit >= 150m && ss.ss_net_profit <= 3000m)
                      || (ca.ca_country == "United States" && stateList3.Contains(ca.ca_state) && ss.ss_net_profit >= 50m && ss.ss_net_profit <= 25000m))
                  select ss.ss_quantity;

        return q48;
    }

    public static string Q48Sql() =>
        $@"
            SELECT SUM (ss_quantity)
            FROM read_parquet('{TpcdsData.StoreSalesPath}') ss,
                 read_parquet('{TpcdsData.StorePath}') s,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}') cd,
                 read_parquet('{TpcdsData.CustomerAddressPath}') ca,
                 read_parquet('{TpcdsData.DateDimPath}') d
            WHERE s_store_sk = ss_store_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_year = 2000
              AND ((cd_demo_sk = ss_cdemo_sk
                    AND cd_marital_status = 'M'
                    AND cd_education_status = '4 yr Degree'
                    AND ss_sales_price BETWEEN 100.00 AND 150.00)
                   OR (cd_demo_sk = ss_cdemo_sk
                       AND cd_marital_status = 'D'
                       AND cd_education_status = '2 yr Degree'
                       AND ss_sales_price BETWEEN 50.00 AND 100.00)
                   OR (cd_demo_sk = ss_cdemo_sk
                       AND cd_marital_status = 'S'
                       AND cd_education_status = 'College'
                       AND ss_sales_price BETWEEN 150.00 AND 200.00))
              AND ((ss_addr_sk = ca_address_sk
                    AND ca_country = 'United States'
                    AND ca_state IN ('CO',
                                     'OH',
                                     'TX')
                    AND ss_net_profit BETWEEN 0 AND 2000)
                   OR (ss_addr_sk = ca_address_sk
                       AND ca_country = 'United States'
                       AND ca_state IN ('OR',
                                        'MN',
                                        'KY')
                       AND ss_net_profit BETWEEN 150 AND 3000)
                   OR (ss_addr_sk = ca_address_sk
                       AND ca_country = 'United States'
                       AND ca_state IN ('VA',
                                        'CA',
                                        'MS')
                       AND ss_net_profit BETWEEN 50 AND 25000));
        ";

    /*
     * TPC-DS Query 49
     *
     * SELECT channel,
     *        item,
     *        return_ratio,
     *        return_rank,
     *        currency_rank
     * FROM
     *   (SELECT 'web' AS channel,
     *           web.item,
     *           web.return_ratio,
     *           web.return_rank,
     *           web.currency_rank
     *    FROM
     *      (SELECT item,
     *              return_ratio,
     *              currency_ratio,
     *              rank() OVER (
     *                           ORDER BY return_ratio) AS return_rank,
     *                          rank() OVER (
     *                                       ORDER BY currency_ratio) AS currency_rank
     *       FROM
     *         (SELECT ws.ws_item_sk AS item,
     *                 (cast(sum(coalesce(wr.wr_return_quantity,0)) AS decimal(15,4))/ cast(sum(coalesce(ws.ws_quantity,0)) AS decimal(15,4))) AS return_ratio,
     *                 (cast(sum(coalesce(wr.wr_return_amt,0)) AS decimal(15,4))/ cast(sum(coalesce(ws.ws_net_paid,0)) AS decimal(15,4))) AS currency_ratio
     *          FROM web_sales ws
     *          LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number
     *                                             AND ws.ws_item_sk = wr.wr_item_sk) ,date_dim
     *          WHERE wr.wr_return_amt > 10000
     *            AND ws.ws_net_profit > 1
     *            AND ws.ws_net_paid > 0
     *            AND ws.ws_quantity > 0
     *            AND ws_sold_date_sk = d_date_sk
     *            AND d_year = 2001
     *            AND d_moy = 12
     *          GROUP BY ws.ws_item_sk) in_web) web
     *    WHERE (web.return_rank <= 10
     *           OR web.currency_rank <= 10)
     *    UNION SELECT 'catalog' AS channel,
     *                 catalog.item,
     *                 catalog.return_ratio,
     *                 catalog.return_rank,
     *                 catalog.currency_rank
     *    FROM
     *      (SELECT item,
     *              return_ratio,
     *              currency_ratio,
     *              rank() OVER (
     *                           ORDER BY return_ratio) AS return_rank,
     *                          rank() OVER (
     *                                       ORDER BY currency_ratio) AS currency_rank
     *       FROM
     *         (SELECT cs.cs_item_sk AS item,
     *                 (cast(sum(coalesce(cr.cr_return_quantity,0)) AS decimal(15,4))/ cast(sum(coalesce(cs.cs_quantity,0)) AS decimal(15,4))) AS return_ratio,
     *                 (cast(sum(coalesce(cr.cr_return_amount,0)) AS decimal(15,4))/ cast(sum(coalesce(cs.cs_net_paid,0)) AS decimal(15,4))) AS currency_ratio
     *          FROM catalog_sales cs
     *          LEFT OUTER JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number
     *                                                 AND cs.cs_item_sk = cr.cr_item_sk) ,date_dim
     *          WHERE cr.cr_return_amount > 10000
     *            AND cs.cs_net_profit > 1
     *            AND cs.cs_net_paid > 0
     *            AND cs.cs_quantity > 0
     *            AND cs_sold_date_sk = d_date_sk
     *            AND d_year = 2001
     *            AND d_moy = 12
     *          GROUP BY cs.cs_item_sk) in_cat) CATALOG
     *    WHERE (catalog.return_rank <= 10
     *           OR catalog.currency_rank <=10)
     *    UNION SELECT 'store' AS channel,
     *                 store.item,
     *                 store.return_ratio,
     *                 store.return_rank,
     *                 store.currency_rank
     *    FROM
     *      (SELECT item,
     *              return_ratio,
     *              currency_ratio,
     *              rank() OVER (
     *                           ORDER BY return_ratio) AS return_rank,
     *                          rank() OVER (
     *                                       ORDER BY currency_ratio) AS currency_rank
     *       FROM
     *         (SELECT sts.ss_item_sk AS item,
     *                 (cast(sum(coalesce(sr.sr_return_quantity,0)) AS decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) AS decimal(15,4))) AS return_ratio,
     *                 (cast(sum(coalesce(sr.sr_return_amt,0)) AS decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) AS decimal(15,4))) AS currency_ratio
     *          FROM store_sales sts
     *          LEFT OUTER JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number
     *                                               AND sts.ss_item_sk = sr.sr_item_sk) ,date_dim
     *          WHERE sr.sr_return_amt > 10000
     *            AND sts.ss_net_profit > 1
     *            AND sts.ss_net_paid > 0
     *            AND sts.ss_quantity > 0
     *            AND ss_sold_date_sk = d_date_sk
     *            AND d_year = 2001
     *            AND d_moy = 12
     *          GROUP BY sts.ss_item_sk) in_store) store
     *    WHERE (store.return_rank <= 10
     *           OR store.currency_rank <= 10) ) sq1
     * ORDER BY 1 NULLS FIRST,
     *          4 NULLS FIRST,
     *          5 NULLS FIRST,
     *          2 NULLS FIRST
     * LIMIT 100;
     */
    public static IQueryable<object> Q49()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var datesDec2001 = dateDimQuery.Where(d => d.d_year == 2001 && d.d_moy == 12);

        // Web
        var webRanked = from ws in webSalesQuery
                        join wr in webReturnsQuery on new { order_number = ws.ws_order_number, item_sk = ws.ws_item_sk }
                                               equals new { order_number = wr.wr_order_number, item_sk = wr.wr_item_sk }
                        join d in datesDec2001 on ws.ws_sold_date_sk equals d.d_date_sk
                        where wr.wr_return_amt > 10000m && ws.ws_net_profit > 1m && ws.ws_net_paid > 0m && ws.ws_quantity > 0
                        group new { ws, wr } by ws.ws_item_sk into g
                        let return_ratio = (double)g.Sum(x => x.wr.wr_return_quantity)! / (double)g.Sum(x => x.ws.ws_quantity)!
                        let currency_ratio = (double)g.Sum(x => x.wr.wr_return_amt)! / (double)g.Sum(x => x.ws.ws_net_paid)!
                        select new
                        {
                            channel = "web",
                            item = g.Key,
                            return_ratio,
                            return_rank = (long)return_ratio.Rank(RankMethod.Min, false),
                            currency_rank = (long)currency_ratio.Rank(RankMethod.Min, false)
                        } into res
                        where res.return_rank <= 10 || res.currency_rank <= 10
                        select res;

        // Catalog
        var catRanked = from cs in catalogSalesQuery
                        join cr in catalogReturnsQuery on new { order_number = cs.cs_order_number, item_sk = cs.cs_item_sk }
                                               equals new { order_number = cr.cr_order_number, item_sk = cr.cr_item_sk }
                        join d in datesDec2001 on cs.cs_sold_date_sk equals d.d_date_sk
                        where cr.cr_return_amount > 10000m && cs.cs_net_profit > 1m && cs.cs_net_paid > 0m && cs.cs_quantity > 0
                        group new { cs, cr } by cs.cs_item_sk into g
                        let return_ratio = (double)g.Sum(x => x.cr.cr_return_quantity)! / (double)g.Sum(x => x.cs.cs_quantity)!
                        let currency_ratio = (double)g.Sum(x => x.cr.cr_return_amount)! / (double)g.Sum(x => x.cs.cs_net_paid)!
                        select new
                        {
                            channel = "catalog",
                            item = g.Key,
                            return_ratio,
                            return_rank = (long)return_ratio.Rank(RankMethod.Min, false),
                            currency_rank = (long)currency_ratio.Rank(RankMethod.Min, false)
                        } into res
                        where res.return_rank <= 10 || res.currency_rank <= 10
                        select res;

        // Store
        var storeRanked = from sts in storeSalesQuery
                          join sr in storeReturnsQuery on new { ticket_number = sts.ss_ticket_number, item_sk = sts.ss_item_sk }
                                                 equals new { ticket_number = sr.sr_ticket_number, item_sk = sr.sr_item_sk }
                          join d in datesDec2001 on sts.ss_sold_date_sk equals d.d_date_sk
                          where sr.sr_return_amt > 10000m && sts.ss_net_profit > 1m && sts.ss_net_paid > 0m && sts.ss_quantity > 0
                          group new { sts, sr } by sts.ss_item_sk into g
                          let return_ratio = (double)g.Sum(x => x.sr.sr_return_quantity)! / (double)g.Sum(x => x.sts.ss_quantity)!
                          let currency_ratio = (double)g.Sum(x => x.sr.sr_return_amt)! / (double)g.Sum(x => x.sts.ss_net_paid)!
                          select new
                          {
                              channel = "store",
                              item = g.Key,
                              return_ratio,
                              return_rank = (long)return_ratio.Rank(RankMethod.Min, false),
                              currency_rank = (long)currency_ratio.Rank(RankMethod.Min, false)
                          } into res
                          where res.return_rank <= 10 || res.currency_rank <= 10
                          select res;

        var q49 = (from r in webRanked.Concat(catRanked).Concat(storeRanked).Distinct()
                   orderby r.channel, r.return_rank, r.currency_rank, r.item
                   select r)
                  .Take(100);

        return q49;
    }

    public static string Q49Sql() =>
        $@"
            SELECT channel,
                   item,
                   return_ratio,
                   return_rank,
                   currency_rank
            FROM
              (SELECT 'web' AS channel,
                      web.item,
                      web.return_ratio,
                      web.return_rank,
                      web.currency_rank
               FROM
                 (SELECT item,
                         return_ratio,
                         currency_ratio,
                         rank() OVER (
                                      ORDER BY return_ratio) AS return_rank,
                                     rank() OVER (
                                                  ORDER BY currency_ratio) AS currency_rank
                  FROM
                    (SELECT ws.ws_item_sk AS item,
                            (cast(sum(coalesce(wr.wr_return_quantity,0)) AS decimal(15,4))/ cast(sum(coalesce(ws.ws_quantity,0)) AS decimal(15,4))) AS return_ratio,
                            (cast(sum(coalesce(wr.wr_return_amt,0)) AS decimal(15,4))/ cast(sum(coalesce(ws.ws_net_paid,0)) AS decimal(15,4))) AS currency_ratio
                     FROM read_parquet('{TpcdsData.WebSalesPath}') ws
                     LEFT OUTER JOIN read_parquet('{TpcdsData.WebReturnsPath}') wr ON (ws.ws_order_number = wr.wr_order_number
                                                        AND ws.ws_item_sk = wr.wr_item_sk) ,read_parquet('{TpcdsData.DateDimPath}')
                     WHERE wr.wr_return_amt > 10000
                       AND ws.ws_net_profit > 1
                       AND ws.ws_net_paid > 0
                       AND ws.ws_quantity > 0
                       AND ws_sold_date_sk = d_date_sk
                       AND d_year = 2001
                       AND d_moy = 12
                     GROUP BY ws.ws_item_sk) in_web) web
               WHERE (web.return_rank <= 10
                      OR web.currency_rank <= 10)
               UNION SELECT 'catalog' AS channel,
                            catalog.item,
                            catalog.return_ratio,
                            catalog.return_rank,
                            catalog.currency_rank
               FROM
                 (SELECT item,
                         return_ratio,
                         currency_ratio,
                         rank() OVER (
                                      ORDER BY return_ratio) AS return_rank,
                                     rank() OVER (
                                                  ORDER BY currency_ratio) AS currency_rank
                  FROM
                    (SELECT cs.cs_item_sk AS item,
                            (cast(sum(coalesce(cr.cr_return_quantity,0)) AS decimal(15,4))/ cast(sum(coalesce(cs.cs_quantity,0)) AS decimal(15,4))) AS return_ratio,
                            (cast(sum(coalesce(cr.cr_return_amount,0)) AS decimal(15,4))/ cast(sum(coalesce(cs.cs_net_paid,0)) AS decimal(15,4))) AS currency_ratio
                     FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs
                     LEFT OUTER JOIN read_parquet('{TpcdsData.CatalogReturnsPath}') cr ON (cs.cs_order_number = cr.cr_order_number
                                                            AND cs.cs_item_sk = cr.cr_item_sk) ,read_parquet('{TpcdsData.DateDimPath}')
                     WHERE cr.cr_return_amount > 10000
                       AND cs.cs_net_profit > 1
                       AND cs.cs_net_paid > 0
                       AND cs.cs_quantity > 0
                       AND cs_sold_date_sk = d_date_sk
                       AND d_year = 2001
                       AND d_moy = 12
                     GROUP BY cs.cs_item_sk) in_cat) CATALOG
               WHERE (catalog.return_rank <= 10
                      OR catalog.currency_rank <=10)
               UNION SELECT 'store' AS channel,
                            store.item,
                            store.return_ratio,
                            store.return_rank,
                            store.currency_rank
               FROM
                 (SELECT item,
                         return_ratio,
                         currency_ratio,
                         rank() OVER (
                                      ORDER BY return_ratio) AS return_rank,
                                     rank() OVER (
                                                  ORDER BY currency_ratio) AS currency_rank
                  FROM
                    (SELECT sts.ss_item_sk AS item,
                            (cast(sum(coalesce(sr.sr_return_quantity,0)) AS decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) AS decimal(15,4))) AS return_ratio,
                            (cast(sum(coalesce(sr.sr_return_amt,0)) AS decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) AS decimal(15,4))) AS currency_ratio
                     FROM read_parquet('{TpcdsData.StoreSalesPath}') sts
                     LEFT OUTER JOIN read_parquet('{TpcdsData.StoreReturnsPath}') sr ON (sts.ss_ticket_number = sr.sr_ticket_number
                                                          AND sts.ss_item_sk = sr.sr_item_sk) ,read_parquet('{TpcdsData.DateDimPath}')
                     WHERE sr.sr_return_amt > 10000
                       AND sts.ss_net_profit > 1
                       AND sts.ss_net_paid > 0
                       AND sts.ss_quantity > 0
                       AND ss_sold_date_sk = d_date_sk
                       AND d_year = 2001
                       AND d_moy = 12
                     GROUP BY sts.ss_item_sk) in_store) store
               WHERE (store.return_rank <= 10
                      OR store.currency_rank <= 10) ) sq1
            ORDER BY 1 NULLS FIRST,
                     4 NULLS FIRST,
                     5 NULLS FIRST,
                     2 NULLS FIRST
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 50
     *
     * SELECT s_store_name,
     *        s_company_id,
     *        s_street_number,
     *        s_street_name,
     *        s_street_type,
     *        s_suite_number,
     *        s_city,
     *        s_county,
     *        s_state,
     *        s_zip,
     *        sum(CASE
     *                WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1
     *                ELSE 0
     *            END) AS "30 days",
     *        sum(CASE
     *                WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
     *                     AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
     *                ELSE 0
     *            END) AS "31-60 days",
     *        sum(CASE
     *                WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
     *                     AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
     *                ELSE 0
     *            END) AS "61-90 days",
     *        sum(CASE
     *                WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
     *                     AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
     *                ELSE 0
     *            END) AS "91-120 days",
     *        sum(CASE
     *                WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1
     *                ELSE 0
     *            END) AS ">120 days"
     * FROM store_sales,
     *      store_returns,
     *      store,
     *      date_dim d1,
     *      date_dim d2
     * WHERE d2.d_year = 2001
     *   AND d2.d_moy = 8
     *   AND ss_ticket_number = sr_ticket_number
     *   AND ss_item_sk = sr_item_sk
     *   AND ss_sold_date_sk = d1.d_date_sk
     *   AND sr_returned_date_sk = d2.d_date_sk
     *   AND ss_customer_sk = sr_customer_sk
     *   AND ss_store_sk = s_store_sk
     * GROUP BY s_store_name,
     *          s_company_id,
     *          s_street_number,
     *          s_street_name,
     *          s_street_type,
     *          s_suite_number,
     *          s_city,
     *          s_county,
     *          s_state,
     *          s_zip
     * ORDER BY s_store_name,
     *          s_company_id,
     *          s_street_number,
     *          s_street_name,
     *          s_street_type,
     *          s_suite_number,
     *          s_city,
     *          s_county,
     *          s_state,
     *          s_zip
     * LIMIT 100;
     */
    public static IQueryable<object> Q50()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var d2Filtered = dateDimQuery.Where(d => d.d_year == 2001 && d.d_moy == 8);

        var q50 = (from ss in storeSalesQuery
                   join sr in storeReturnsQuery on new { ticket_number = ss.ss_ticket_number, item_sk = ss.ss_item_sk, customer_sk = ss.ss_customer_sk }
                                            equals new { ticket_number = sr.sr_ticket_number, item_sk = sr.sr_item_sk, customer_sk = sr.sr_customer_sk }
                   join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                   join d1 in dateDimQuery on ss.ss_sold_date_sk equals d1.d_date_sk
                   join d2 in d2Filtered on sr.sr_returned_date_sk equals d2.d_date_sk
                   group new { ss, sr } by new
                   {
                       s.s_store_name,
                       s.s_company_id,
                       s.s_street_number,
                       s.s_street_name,
                       s.s_street_type,
                       s.s_suite_number,
                       s.s_city,
                       s.s_county,
                       s.s_state,
                       s.s_zip
                   } into g
                   orderby g.Key.s_store_name,
                           g.Key.s_company_id,
                           g.Key.s_street_number,
                           g.Key.s_street_name,
                           g.Key.s_street_type,
                           g.Key.s_suite_number,
                           g.Key.s_city,
                           g.Key.s_county,
                           g.Key.s_state,
                           g.Key.s_zip
                   select new
                   {
                       g.Key.s_store_name,
                       g.Key.s_company_id,
                       g.Key.s_street_number,
                       g.Key.s_street_name,
                       g.Key.s_street_type,
                       g.Key.s_suite_number,
                       g.Key.s_city,
                       g.Key.s_county,
                       g.Key.s_state,
                       g.Key.s_zip,
                       days_30 = g.Sum(x => x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk <= 30 ? 1 : 0),
                       days_31_60 = g.Sum(x => x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk > 30 && x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk <= 60 ? 1 : 0),
                       days_61_90 = g.Sum(x => x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk > 60 && x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk <= 90 ? 1 : 0),
                       days_91_120 = g.Sum(x => x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk > 90 && x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk <= 120 ? 1 : 0),
                       days_over_120 = g.Sum(x => x.sr.sr_returned_date_sk - x.ss.ss_sold_date_sk > 120 ? 1 : 0),
                   })
                  .Take(100);

        return q50;
    }

    public static string Q50Sql() =>
        $@"
            SELECT s_store_name,
                   s_company_id,
                   s_street_number,
                   s_street_name,
                   s_street_type,
                   s_suite_number,
                   s_city,
                   s_county,
                   s_state,
                   s_zip,
                   sum(CASE
                           WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1
                           ELSE 0
                       END) AS ""30 days"",
                   sum(CASE
                           WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
                                AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
                           ELSE 0
                       END) AS ""31-60 days"",
                   sum(CASE
                           WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
                                AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
                           ELSE 0
                       END) AS ""61-90 days"",
                   sum(CASE
                           WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
                                AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
                           ELSE 0
                       END) AS ""91-120 days"",
                   sum(CASE
                           WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1
                           ELSE 0
                       END) AS "">120 days""
            FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                 read_parquet('{TpcdsData.StoreReturnsPath}') store_returns,
                 read_parquet('{TpcdsData.StorePath}') store,
                 read_parquet('{TpcdsData.DateDimPath}') d1,
                 read_parquet('{TpcdsData.DateDimPath}') d2
            WHERE d2.d_year = 2001
              AND d2.d_moy = 8
              AND ss_ticket_number = sr_ticket_number
              AND ss_item_sk = sr_item_sk
              AND ss_sold_date_sk = d1.d_date_sk
              AND sr_returned_date_sk = d2.d_date_sk
              AND ss_customer_sk = sr_customer_sk
              AND ss_store_sk = s_store_sk
            GROUP BY s_store_name,
                     s_company_id,
                     s_street_number,
                     s_street_name,
                     s_street_type,
                     s_suite_number,
                     s_city,
                     s_county,
                     s_state,
                     s_zip
            ORDER BY s_store_name,
                     s_company_id,
                     s_street_number,
                     s_street_name,
                     s_street_type,
                     s_suite_number,
                     s_city,
                     s_county,
                     s_state,
                     s_zip
            LIMIT 100;
        ";
}
