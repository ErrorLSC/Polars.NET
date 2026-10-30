using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Tpcds.Tests;

public class TpcdsQuery81To90Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 81
     *
     * WITH customer_total_return AS
     *   (SELECT cr_returning_customer_sk AS ctr_customer_sk ,
     *           ca_state AS ctr_state,
     *           sum(cr_return_amt_inc_tax) AS ctr_total_return
     *    FROM catalog_returns ,
     *         date_dim ,
     *         customer_address
     *    WHERE cr_returned_date_sk = d_date_sk
     *      AND d_year = 2000
     *      AND cr_returning_addr_sk = ca_address_sk
     *    GROUP BY cr_returning_customer_sk ,
     *             ca_state)
     * SELECT c_customer_id,
     *        c_salutation,
     *        c_first_name,
     *        c_last_name,
     *        ca_street_number,
     *        ca_street_name ,
     *        ca_street_type,
     *        ca_suite_number,
     *        ca_city,
     *        ca_county,
     *        ca_state,
     *        ca_zip,
     *        ca_country,
     *        ca_gmt_offset ,
     *        ca_location_type,
     *        ctr_total_return
     * FROM customer_total_return ctr1 ,
     *      customer_address ,
     *      customer
     * WHERE ctr1.ctr_total_return >
     *     (SELECT avg(ctr_total_return)*1.2
     *      FROM customer_total_return ctr2
     *      WHERE ctr1.ctr_state = ctr2.ctr_state)
     *   AND ca_address_sk = c_current_addr_sk
     *   AND ca_state = 'GA'
     *   AND ctr1.ctr_customer_sk = c_customer_sk
     * ORDER BY c_customer_id,
     *          c_salutation,
     *          c_first_name,
     *          c_last_name,
     *          ca_street_number,
     *          ca_street_name ,
     *          ca_street_type,
     *          ca_suite_number,
     *          ca_city,
     *          ca_county,
     *          ca_state,
     *          ca_zip,
     *          ca_country,
     *          ca_gmt_offset ,
     *          ca_location_type,
     *          ctr_total_return
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q81()
    {
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        var ctr = from cr in catalogReturnsQuery
                  join d in dateDimQuery on cr.cr_returned_date_sk equals d.d_date_sk
                  join ca in customerAddressQuery on cr.cr_returning_addr_sk equals ca.ca_address_sk
                  where d.d_year == 2000
                  group cr by new { cr.cr_returning_customer_sk, ca.ca_state } into g
                  select new
                  {
                      ctr_customer_sk = g.Key.cr_returning_customer_sk,
                      ctr_state = g.Key.ca_state,
                      ctr_total_return = g.Sum(x => x.cr_return_amt_inc_tax)
                  };

        var filteredCa = customerAddressQuery.Where(ca => ca.ca_state == "GA");

        var q81 = (from ctr1 in ctr
                   where ctr1.ctr_total_return > ctr.Where(ctr2 => ctr2.ctr_state == ctr1.ctr_state)
                                                    .Average(ctr2 => ctr2.ctr_total_return) * 1.2m
                   join c in customerQuery on ctr1.ctr_customer_sk equals c.c_customer_sk
                   join ca in filteredCa on c.c_current_addr_sk equals ca.ca_address_sk
                   orderby c.c_customer_id,
                           c.c_salutation,
                           c.c_first_name,
                           c.c_last_name,
                           ca.ca_street_number,
                           ca.ca_street_name,
                           ca.ca_street_type,
                           ca.ca_suite_number,
                           ca.ca_city,
                           ca.ca_county,
                           ca.ca_state,
                           ca.ca_zip,
                           ca.ca_country,
                           ca.ca_gmt_offset,
                           ca.ca_location_type,
                           ctr1.ctr_total_return
                   select new
                   {
                       c.c_customer_id,
                       c.c_salutation,
                       c.c_first_name,
                       c.c_last_name,
                       ca.ca_street_number,
                       ca.ca_street_name,
                       ca.ca_street_type,
                       ca.ca_suite_number,
                       ca.ca_city,
                       ca.ca_county,
                       ca.ca_state,
                       ca.ca_zip,
                       ca.ca_country,
                       ca.ca_gmt_offset,
                       ca.ca_location_type,
                       ctr1.ctr_total_return
                   }).Take(100);

        AssertPlan(q81, NativePushdown);

        var polarsResult = q81.ToList();

        var duckDbSql = $@"
            WITH customer_total_return AS
              (SELECT cr_returning_customer_sk AS ctr_customer_sk ,
                      ca_state AS ctr_state,
                      sum(cr_return_amt_inc_tax) AS ctr_total_return
               FROM read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns ,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim ,
                    read_parquet('{TpcdsData.CustomerAddressPath}') customer_address
               WHERE cr_returned_date_sk = d_date_sk
                 AND d_year = 2000
                 AND cr_returning_addr_sk = ca_address_sk
               GROUP BY cr_returning_customer_sk ,
                        ca_state)
            SELECT c_customer_id,
                   c_salutation,
                   c_first_name,
                   c_last_name,
                   ca_street_number,
                   ca_street_name ,
                   ca_street_type,
                   ca_suite_number,
                   ca_city,
                   ca_county,
                   ca_state,
                   ca_zip,
                   ca_country,
                   ca_gmt_offset ,
                   ca_location_type,
                   ctr_total_return
            FROM customer_total_return ctr1 ,
                 read_parquet('{TpcdsData.CustomerAddressPath}') customer_address ,
                 read_parquet('{TpcdsData.CustomerPath}') customer
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
                     ca_street_number NULLS FIRST,
                     ca_street_name NULLS FIRST,
                     ca_street_type NULLS FIRST,
                     ca_suite_number NULLS FIRST,
                     ca_city NULLS FIRST,
                     ca_county NULLS FIRST,
                     ca_state NULLS FIRST,
                     ca_zip NULLS FIRST,
                     ca_country NULLS FIRST,
                     ca_gmt_offset NULLS FIRST,
                     ca_location_type NULLS FIRST,
                     ctr_total_return NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            c_customer_id = r.IsDBNull(0) ? null : r.GetString(0),
            c_salutation = r.IsDBNull(1) ? null : r.GetString(1),
            c_first_name = r.IsDBNull(2) ? null : r.GetString(2),
            c_last_name = r.IsDBNull(3) ? null : r.GetString(3),
            ca_street_number = r.IsDBNull(4) ? null : r.GetString(4),
            ca_street_name = r.IsDBNull(5) ? null : r.GetString(5),
            ca_street_type = r.IsDBNull(6) ? null : r.GetString(6),
            ca_suite_number = r.IsDBNull(7) ? null : r.GetString(7),
            ca_city = r.IsDBNull(8) ? null : r.GetString(8),
            ca_county = r.IsDBNull(9) ? null : r.GetString(9),
            ca_state = r.IsDBNull(10) ? null : r.GetString(10),
            ca_zip = r.IsDBNull(11) ? null : r.GetString(11),
            ca_country = r.IsDBNull(12) ? null : r.GetString(12),
            ca_gmt_offset = r.IsDBNull(13) ? (decimal?)null : r.GetDecimal(13),
            ca_location_type = r.IsDBNull(14) ? null : r.GetString(14),
            ctr_total_return = r.IsDBNull(15) ? (decimal?)null : r.GetDecimal(15)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].c_customer_id, polarsResult[i].c_customer_id);
            Assert.Equal(duckDbResult[i].c_salutation, polarsResult[i].c_salutation);
            Assert.Equal(duckDbResult[i].c_first_name, polarsResult[i].c_first_name);
            Assert.Equal(duckDbResult[i].c_last_name, polarsResult[i].c_last_name);
            Assert.Equal(duckDbResult[i].ca_street_number, polarsResult[i].ca_street_number);
            Assert.Equal(duckDbResult[i].ca_street_name, polarsResult[i].ca_street_name);
            Assert.Equal(duckDbResult[i].ca_street_type, polarsResult[i].ca_street_type);
            Assert.Equal(duckDbResult[i].ca_suite_number, polarsResult[i].ca_suite_number);
            Assert.Equal(duckDbResult[i].ca_city, polarsResult[i].ca_city);
            Assert.Equal(duckDbResult[i].ca_county, polarsResult[i].ca_county);
            Assert.Equal(duckDbResult[i].ca_state, polarsResult[i].ca_state);
            Assert.Equal(duckDbResult[i].ca_zip, polarsResult[i].ca_zip);
            Assert.Equal(duckDbResult[i].ca_country, polarsResult[i].ca_country);
            Assert.Equal(duckDbResult[i].ca_gmt_offset, polarsResult[i].ca_gmt_offset);
            Assert.Equal(duckDbResult[i].ca_location_type, polarsResult[i].ca_location_type);
            Assert.Equal(duckDbResult[i].ctr_total_return ?? 0m, polarsResult[i].ctr_total_return ?? 0m, 2);
        }
    }

    /*
     * TPC-DS Query 82
     *
     * SELECT i_item_id ,
     *        i_item_desc ,
     *        i_current_price
     * FROM item,
     *      inventory,
     *      date_dim,
     *      store_sales
     * WHERE i_current_price BETWEEN 62 AND 62+30
     *   AND inv_item_sk = i_item_sk
     *   AND d_date_sk=inv_date_sk
     *   AND d_date BETWEEN cast('2000-05-25' AS date) AND cast('2000-07-24' AS date)
     *   AND i_manufact_id IN (129,
     *                         270,
     *                         821,
     *                         423)
     *   AND inv_quantity_on_hand BETWEEN 100 AND 500
     *   AND ss_item_sk = i_item_sk
     * GROUP BY i_item_id,
     *          i_item_desc,
     *          i_current_price
     * ORDER BY i_item_id
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q82()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var inventoryQuery = TpcdsData.InventoryQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();

        var minDate = new DateOnly(2000, 5, 25);
        var maxDate = new DateOnly(2000, 7, 24);
        long[] manufacts = [129, 270, 821, 423];

        var q82 = (from i in itemQuery
                   join inv in inventoryQuery on i.i_item_sk equals inv.inv_item_sk
                   join d in dateDimQuery on inv.inv_date_sk equals d.d_date_sk
                   join ss in storeSalesQuery on i.i_item_sk equals ss.ss_item_sk
                   where i.i_current_price >= 62m && i.i_current_price <= 92m
                      && d.d_date >= minDate && d.d_date <= maxDate
                      && manufacts.Contains(i.i_manufact_id!.Value)
                      && inv.inv_quantity_on_hand >= 100 && inv.inv_quantity_on_hand <= 500
                   group i by new { i.i_item_id, i.i_item_desc, i.i_current_price } into g
                   orderby g.Key.i_item_id, g.Key.i_item_desc, g.Key.i_current_price
                   select new
                   {
                       g.Key.i_item_id,
                       g.Key.i_item_desc,
                       g.Key.i_current_price
                   }).Take(100);

        AssertPlan(q82, NativePushdown);

        var polarsResult = q82.ToList();

        var duckDbSql = $@"
            SELECT i_item_id ,
                   i_item_desc ,
                   i_current_price
            FROM read_parquet('{TpcdsData.ItemPath}') item,
                 read_parquet('{TpcdsData.InventoryPath}') inventory,
                 read_parquet('{TpcdsData.DateDimPath}') date_dim,
                 read_parquet('{TpcdsData.StoreSalesPath}') store_sales
            WHERE i_current_price BETWEEN 62 AND 62+30
              AND inv_item_sk = i_item_sk
              AND d_date_sk=inv_date_sk
              AND d_date BETWEEN cast('2000-05-25' AS date) AND cast('2000-07-24' AS date)
              AND i_manufact_id IN (129,
                                    270,
                                    821,
                                    423)
              AND inv_quantity_on_hand BETWEEN 100 AND 500
              AND ss_item_sk = i_item_sk
            GROUP BY i_item_id,
                     i_item_desc,
                     i_current_price
            ORDER BY i_item_id NULLS FIRST,
                     i_item_desc NULLS FIRST,
                     i_current_price NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.IsDBNull(0) ? null : r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            i_current_price = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].i_item_id, polarsResult[i].i_item_id);
            Assert.Equal(duckDbResult[i].i_item_desc, polarsResult[i].i_item_desc);
            Assert.Equal(duckDbResult[i].i_current_price, polarsResult[i].i_current_price);
        }
    }

    /*
     * TPC-DS Query 83
     *
     * WITH sr_items AS
     *   (SELECT i_item_id item_id,
     *           sum(sr_return_quantity) sr_item_qty
     *    FROM store_returns,
     *         item,
     *         date_dim
     *    WHERE sr_item_sk = i_item_sk
     *      AND d_date IN
     *        (SELECT d_date
     *         FROM date_dim
     *         WHERE d_week_seq IN
     *             (SELECT d_week_seq
     *              FROM date_dim
     *              WHERE d_date IN ('2000-06-30',
     *                               '2000-09-27',
     *                               '2000-11-17')))
     *      AND sr_returned_date_sk = d_date_sk
     *    GROUP BY i_item_id),
     *      cr_items AS
     *   (SELECT i_item_id item_id,
     *           sum(cr_return_quantity) cr_item_qty
     *    FROM catalog_returns,
     *         item,
     *         date_dim
     *    WHERE cr_item_sk = i_item_sk
     *      AND d_date IN
     *        (SELECT d_date
     *         FROM date_dim
     *         WHERE d_week_seq IN
     *             (SELECT d_week_seq
     *              FROM date_dim
     *              WHERE d_date IN ('2000-06-30',
     *                               '2000-09-27',
     *                               '2000-11-17')))
     *      AND cr_returned_date_sk = d_date_sk
     *    GROUP BY i_item_id),
     *      wr_items AS
     *   (SELECT i_item_id item_id,
     *           sum(wr_return_quantity) wr_item_qty
     *    FROM web_returns,
     *         item,
     *         date_dim
     *    WHERE wr_item_sk = i_item_sk
     *      AND d_date IN
     *        (SELECT d_date
     *         FROM date_dim
     *         WHERE d_week_seq IN
     *             (SELECT d_week_seq
     *              FROM date_dim
     *              WHERE d_date IN ('2000-06-30',
     *                               '2000-09-27',
     *                               '2000-11-17')))
     *      AND wr_returned_date_sk = d_date_sk
     *    GROUP BY i_item_id)
     * SELECT sr_items.item_id ,
     *        sr_item_qty ,
     *        (sr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 sr_dev ,
     *        cr_item_qty ,
     *        (cr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 cr_dev ,
     *        wr_item_qty ,
     *        (wr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 wr_dev ,
     *        (sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
     * FROM sr_items ,
     *      cr_items ,
     *      wr_items
     * WHERE sr_items.item_id=cr_items.item_id
     *   AND sr_items.item_id=wr_items.item_id
     * ORDER BY sr_items.item_id NULLS FIRST,
     *          sr_item_qty NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q83()
    {
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        DateOnly[] targetDates = [new DateOnly(2000, 6, 30), new DateOnly(2000, 9, 27), new DateOnly(2000, 11, 17)];

        var targetWeeks = (from d in dateDimQuery
                           where targetDates.Contains(d.d_date!.Value)
                           select d.d_week_seq).Distinct();

        var validDateSks = (from d in dateDimQuery
                            join w in targetWeeks on d.d_week_seq equals w
                            select d.d_date_sk).Distinct();

        var srItems = from sr in storeReturnsQuery
                      join i in itemQuery on sr.sr_item_sk equals i.i_item_sk
                      join dsk in validDateSks on sr.sr_returned_date_sk equals dsk
                      group sr by i.i_item_id into g
                      select new
                      {
                          item_id = g.Key,
                          sr_item_qty = g.Sum(x => x.sr_return_quantity)
                      };

        var crItems = from cr in catalogReturnsQuery
                      join i in itemQuery on cr.cr_item_sk equals i.i_item_sk
                      join dsk in validDateSks on cr.cr_returned_date_sk equals dsk
                      group cr by i.i_item_id into g
                      select new
                      {
                          item_id = g.Key,
                          cr_item_qty = g.Sum(x => x.cr_return_quantity)
                      };

        var wrItems = from wr in webReturnsQuery
                      join i in itemQuery on wr.wr_item_sk equals i.i_item_sk
                      join dsk in validDateSks on wr.wr_returned_date_sk equals dsk
                      group wr by i.i_item_id into g
                      select new
                      {
                          item_id = g.Key,
                          wr_item_qty = g.Sum(x => x.wr_return_quantity)
                      };

        var q83 = (from sr in srItems
                   join cr in crItems on sr.item_id equals cr.item_id
                   join wr in wrItems on sr.item_id equals wr.item_id
                   orderby sr.item_id, sr.sr_item_qty
                   select new
                   {
                       sr.item_id,
                       sr.sr_item_qty,
                       sr_dev = (double)sr.sr_item_qty! / (double)(sr.sr_item_qty! + cr.cr_item_qty! + wr.wr_item_qty!) / 3.0 * 100.0,
                       cr.cr_item_qty,
                       cr_dev = (double)cr.cr_item_qty! / (double)(sr.sr_item_qty! + cr.cr_item_qty! + wr.wr_item_qty!) / 3.0 * 100.0,
                       wr.wr_item_qty,
                       wr_dev = (double)wr.wr_item_qty! / (double)(sr.sr_item_qty! + cr.cr_item_qty! + wr.wr_item_qty!) / 3.0 * 100.0,
                       average = (double)(sr.sr_item_qty! + cr.cr_item_qty! + wr.wr_item_qty!) / 3.0
                   }).Take(100);

        AssertPlan(q83, NativePushdown);

        var polarsResult = q83.ToList();

        var duckDbSql = $@"
            WITH sr_items AS
              (SELECT i_item_id item_id,
                      sum(sr_return_quantity) sr_item_qty
               FROM read_parquet('{TpcdsData.StoreReturnsPath}') store_returns,
                    read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE sr_item_sk = i_item_sk
                 AND d_date IN
                   (SELECT d_date
                    FROM read_parquet('{TpcdsData.DateDimPath}') date_dim
                    WHERE d_week_seq IN
                        (SELECT d_week_seq
                         FROM read_parquet('{TpcdsData.DateDimPath}') date_dim
                         WHERE d_date IN ('2000-06-30',
                                          '2000-09-27',
                                          '2000-11-17')))
                 AND sr_returned_date_sk = d_date_sk
               GROUP BY i_item_id),
                 cr_items AS
              (SELECT i_item_id item_id,
                      sum(cr_return_quantity) cr_item_qty
               FROM read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns,
                    read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE cr_item_sk = i_item_sk
                 AND d_date IN
                   (SELECT d_date
                    FROM read_parquet('{TpcdsData.DateDimPath}') date_dim
                    WHERE d_week_seq IN
                        (SELECT d_week_seq
                         FROM read_parquet('{TpcdsData.DateDimPath}') date_dim
                         WHERE d_date IN ('2000-06-30',
                                          '2000-09-27',
                                          '2000-11-17')))
                 AND cr_returned_date_sk = d_date_sk
               GROUP BY i_item_id),
                 wr_items AS
              (SELECT i_item_id item_id,
                      sum(wr_return_quantity) wr_item_qty
               FROM read_parquet('{TpcdsData.WebReturnsPath}') web_returns,
                    read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE wr_item_sk = i_item_sk
                 AND d_date IN
                   (SELECT d_date
                    FROM read_parquet('{TpcdsData.DateDimPath}') date_dim
                    WHERE d_week_seq IN
                        (SELECT d_week_seq
                         FROM read_parquet('{TpcdsData.DateDimPath}') date_dim
                         WHERE d_date IN ('2000-06-30',
                                          '2000-09-27',
                                          '2000-11-17')))
                 AND wr_returned_date_sk = d_date_sk
               GROUP BY i_item_id)
            SELECT sr_items.item_id ,
                   sr_item_qty ,
                   (sr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 sr_dev ,
                   cr_item_qty ,
                   (cr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 cr_dev ,
                   wr_item_qty ,
                   (wr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 wr_dev ,
                   (sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
            FROM sr_items ,
                 cr_items ,
                 wr_items
            WHERE sr_items.item_id=cr_items.item_id
              AND sr_items.item_id=wr_items.item_id
            ORDER BY sr_items.item_id NULLS FIRST,
                     sr_item_qty NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            item_id = r.IsDBNull(0) ? null : r.GetString(0),
            sr_item_qty = r.IsDBNull(1) ? (long?)null : (long)r.GetFieldValue<System.Numerics.BigInteger>(1),
            sr_dev = r.IsDBNull(2) ? 0.0 : r.GetDouble(2),
            cr_item_qty = r.IsDBNull(3) ? (long?)null : (long)r.GetFieldValue<System.Numerics.BigInteger>(3),
            cr_dev = r.IsDBNull(4) ? 0.0 : r.GetDouble(4),
            wr_item_qty = r.IsDBNull(5) ? (long?)null : (long)r.GetFieldValue<System.Numerics.BigInteger>(5),
            wr_dev = r.IsDBNull(6) ? 0.0 : r.GetDouble(6),
            average = r.IsDBNull(7) ? 0.0 : r.GetDouble(7)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].item_id, polarsResult[i].item_id);
            Assert.Equal(duckDbResult[i].sr_item_qty ?? 0L, polarsResult[i].sr_item_qty ?? 0L);
            Assert.Equal(duckDbResult[i].sr_dev, polarsResult[i].sr_dev, 2);
            Assert.Equal(duckDbResult[i].cr_item_qty ?? 0L, polarsResult[i].cr_item_qty ?? 0L);
            Assert.Equal(duckDbResult[i].cr_dev, polarsResult[i].cr_dev, 2);
            Assert.Equal(duckDbResult[i].wr_item_qty ?? 0L, polarsResult[i].wr_item_qty ?? 0L);
            Assert.Equal(duckDbResult[i].wr_dev, polarsResult[i].wr_dev, 2);
            Assert.Equal(duckDbResult[i].average, polarsResult[i].average, 2);
        }
    }

    /*
     * TPC-DS Query 84
     *
     * SELECT c_customer_id AS customer_id ,
     *        concat(concat(coalesce(c_last_name, '') , ', '), coalesce(c_first_name, '')) AS customername
     * FROM customer ,
     *      customer_address ,
     *      customer_demographics ,
     *      household_demographics ,
     *      income_band ,
     *      store_returns
     * WHERE ca_city = 'Edgewood'
     *   AND c_current_addr_sk = ca_address_sk
     *   AND ib_lower_bound >= 38128
     *   AND ib_upper_bound <= 38128 + 50000
     *   AND ib_income_band_sk = hd_income_band_sk
     *   AND cd_demo_sk = c_current_cdemo_sk
     *   AND hd_demo_sk = c_current_hdemo_sk
     *   AND sr_cdemo_sk = cd_demo_sk
     * ORDER BY c_customer_id NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q84()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var cdemoQuery = TpcdsData.CustomerDemographicsQuery();
        var hdemoQuery = TpcdsData.HouseholdDemographicsQuery();
        var incomeBandQuery = TpcdsData.IncomeBandQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();

        var q84 = (from c in customerQuery
                   join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                   join cd in cdemoQuery on c.c_current_cdemo_sk equals cd.cd_demo_sk
                   join hd in hdemoQuery on c.c_current_hdemo_sk equals hd.hd_demo_sk
                   join ib in incomeBandQuery on hd.hd_income_band_sk equals ib.ib_income_band_sk
                   join sr in storeReturnsQuery on cd.cd_demo_sk equals sr.sr_cdemo_sk
                   where ca.ca_city == "Edgewood"
                      && ib.ib_lower_bound >= 38128
                      && ib.ib_upper_bound <= 88128
                   orderby c.c_customer_id
                   select new
                   {
                       customer_id = c.c_customer_id,
                       customername = (c.c_last_name ?? "") + ", " + (c.c_first_name ?? "")
                   }).Take(100);

        AssertPlan(q84, NativePushdown);

        var polarsResult = q84.ToList();

        var duckDbSql = $@"
            SELECT c_customer_id AS customer_id ,
                   concat(concat(coalesce(c_last_name, '') , ', '), coalesce(c_first_name, '')) AS customername
            FROM read_parquet('{TpcdsData.CustomerPath}') customer ,
                 read_parquet('{TpcdsData.CustomerAddressPath}') customer_address ,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}') customer_demographics ,
                 read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics ,
                 read_parquet('{TpcdsData.IncomeBandPath}') income_band ,
                 read_parquet('{TpcdsData.StoreReturnsPath}') store_returns
            WHERE ca_city = 'Edgewood'
              AND c_current_addr_sk = ca_address_sk
              AND ib_lower_bound >= 38128
              AND ib_upper_bound <= 38128 + 50000
              AND ib_income_band_sk = hd_income_band_sk
              AND cd_demo_sk = c_current_cdemo_sk
              AND hd_demo_sk = c_current_hdemo_sk
              AND sr_cdemo_sk = cd_demo_sk
            ORDER BY c_customer_id NULLS FIRST,
                     customername NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            customer_id = r.IsDBNull(0) ? null : r.GetString(0),
            customername = r.IsDBNull(1) ? null : r.GetString(1)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].customer_id, polarsResult[i].customer_id);
            Assert.Equal(duckDbResult[i].customername, polarsResult[i].customername);
        }
    }

    /*
     * TPC-DS Query 85
     *
     * SELECT SUBSTRING(r_reason_desc,1,20) ,
     *        avg(ws_quantity) avg1,
     *        avg(wr_refunded_cash) avg2,
     *        avg(wr_fee)
     * FROM web_sales,
     *      web_returns,
     *      web_page,
     *      customer_demographics cd1,
     *      customer_demographics cd2,
     *      customer_address,
     *      date_dim,
     *      reason
     * WHERE ws_web_page_sk = wp_web_page_sk
     *   AND ws_item_sk = wr_item_sk
     *   AND ws_order_number = wr_order_number
     *   AND ws_sold_date_sk = d_date_sk
     *   AND d_year = 2000
     *   AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
     *   AND cd2.cd_demo_sk = wr_returning_cdemo_sk
     *   AND ca_address_sk = wr_refunded_addr_sk
     *   AND r_reason_sk = wr_reason_sk
     *   AND ( ( cd1.cd_marital_status = 'M'
     *          AND cd1.cd_marital_status = cd2.cd_marital_status
     *          AND cd1.cd_education_status = 'Advanced Degree'
     *          AND cd1.cd_education_status = cd2.cd_education_status
     *          AND ws_sales_price BETWEEN 100.00 AND 150.00 )
     *        OR ( cd1.cd_marital_status = 'S'
     *            AND cd1.cd_marital_status = cd2.cd_marital_status
     *            AND cd1.cd_education_status = 'College'
     *            AND cd1.cd_education_status = cd2.cd_education_status
     *            AND ws_sales_price BETWEEN 50.00 AND 100.00 )
     *        OR ( cd1.cd_marital_status = 'W'
     *            AND cd1.cd_marital_status = cd2.cd_marital_status
     *            AND cd1.cd_education_status = '2 yr Degree'
     *            AND cd1.cd_education_status = cd2.cd_education_status
     *            AND ws_sales_price BETWEEN 150.00 AND 200.00 ) )
     *   AND ( ( ca_country = 'United States'
     *          AND ca_state IN ('IN',
     *                           'OH',
     *                           'NJ')
     *          AND ws_net_profit BETWEEN 100 AND 200)
     *        OR ( ca_country = 'United States'
     *            AND ca_state IN ('WI',
     *                             'CT',
     *                             'KY')
     *            AND ws_net_profit BETWEEN 150 AND 300)
     *        OR ( ca_country = 'United States'
     *            AND ca_state IN ('LA',
     *                             'IA',
     *                             'AR')
     *            AND ws_net_profit BETWEEN 50 AND 250) )
     * GROUP BY r_reason_desc
     * ORDER BY SUBSTRING(r_reason_desc,1,20) ,
     *          avg(ws_quantity) ,
     *          avg(wr_refunded_cash) ,
     *          avg(wr_fee)
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q85()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var webPageQuery = TpcdsData.WebPageQuery();
        var cdemoQuery = TpcdsData.CustomerDemographicsQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var reasonQuery = TpcdsData.ReasonQuery();

        string[] states1 = ["IN", "OH", "NJ"];
        string[] states2 = ["WI", "CT", "KY"];
        string[] states3 = ["LA", "IA", "AR"];

        var baseQuery = from ws in webSalesQuery
                        join wr in webReturnsQuery on new { item_sk = ws.ws_item_sk, order_number = ws.ws_order_number }
                                               equals new { item_sk = wr.wr_item_sk, order_number = wr.wr_order_number }
                        join wp in webPageQuery on ws.ws_web_page_sk equals wp.wp_web_page_sk
                        join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                        join cd1 in cdemoQuery on wr.wr_refunded_cdemo_sk equals cd1.cd_demo_sk
                        join cd2 in cdemoQuery on wr.wr_returning_cdemo_sk equals cd2.cd_demo_sk
                        join ca in customerAddressQuery on wr.wr_refunded_addr_sk equals ca.ca_address_sk
                        join r in reasonQuery on wr.wr_reason_sk equals r.r_reason_sk
                        where d.d_year == 2000
                           && cd1.cd_marital_status == cd2.cd_marital_status
                           && cd1.cd_education_status == cd2.cd_education_status
                           && ((cd1.cd_marital_status == "M" && cd1.cd_education_status == "Advanced Degree" && ws.ws_sales_price >= 100.00m && ws.ws_sales_price <= 150.00m)
                               || (cd1.cd_marital_status == "S" && cd1.cd_education_status == "College" && ws.ws_sales_price >= 50.00m && ws.ws_sales_price <= 100.00m)
                               || (cd1.cd_marital_status == "W" && cd1.cd_education_status == "2 yr Degree" && ws.ws_sales_price >= 150.00m && ws.ws_sales_price <= 200.00m))
                           && ca.ca_country == "United States"
                           && ((states1.Contains(ca.ca_state!) && ws.ws_net_profit >= 100m && ws.ws_net_profit <= 200m)
                               || (states2.Contains(ca.ca_state!) && ws.ws_net_profit >= 150m && ws.ws_net_profit <= 300m)
                               || (states3.Contains(ca.ca_state!) && ws.ws_net_profit >= 50m && ws.ws_net_profit <= 250m))
                        select new
                        {
                            r.r_reason_desc,
                            ws.ws_quantity,
                            wr.wr_refunded_cash,
                            wr.wr_fee
                        };

        var grouped = from x in baseQuery
                      group x by x.r_reason_desc into g
                      select new
                      {
                          r_reason_desc = g.Key,
                          avg1 = g.Average(x => (double?)x.ws_quantity),
                          avg2 = g.Average(x => (double?)x.wr_refunded_cash),
                          avg3 = g.Average(x => (double?)x.wr_fee)
                      };

        var q85 = (from g in grouped
                   let sub_reason = g.r_reason_desc!.Substring(0, 20)
                   orderby sub_reason, g.avg1, g.avg2, g.avg3
                   select new
                   {
                       sub_reason,
                       g.avg1,
                       g.avg2,
                       g.avg3
                   }).Take(100);

        AssertPlan(q85, NativePushdown);

        var polarsResult = q85.ToList();

        var duckDbSql = $@"
            SELECT SUBSTRING(r_reason_desc,1,20) ,
                   avg(ws_quantity) avg1,
                   avg(wr_refunded_cash) avg2,
                   avg(wr_fee)
            FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                 read_parquet('{TpcdsData.WebReturnsPath}') web_returns,
                 read_parquet('{TpcdsData.WebPagePath}') web_page,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}') cd1,
                 read_parquet('{TpcdsData.CustomerDemographicsPath}') cd2,
                 read_parquet('{TpcdsData.CustomerAddressPath}') customer_address,
                 read_parquet('{TpcdsData.DateDimPath}') date_dim,
                 read_parquet('{TpcdsData.ReasonPath}') reason
            WHERE ws_web_page_sk = wp_web_page_sk
              AND ws_item_sk = wr_item_sk
              AND ws_order_number = wr_order_number
              AND ws_sold_date_sk = d_date_sk
              AND d_year = 2000
              AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
              AND cd2.cd_demo_sk = wr_returning_cdemo_sk
              AND ca_address_sk = wr_refunded_addr_sk
              AND r_reason_sk = wr_reason_sk
              AND ( ( cd1.cd_marital_status = 'M'
                     AND cd1.cd_marital_status = cd2.cd_marital_status
                     AND cd1.cd_education_status = 'Advanced Degree'
                     AND cd1.cd_education_status = cd2.cd_education_status
                     AND ws_sales_price BETWEEN 100.00 AND 150.00 )
                   OR ( cd1.cd_marital_status = 'S'
                       AND cd1.cd_marital_status = cd2.cd_marital_status
                       AND cd1.cd_education_status = 'College'
                       AND cd1.cd_education_status = cd2.cd_education_status
                       AND ws_sales_price BETWEEN 50.00 AND 100.00 )
                   OR ( cd1.cd_marital_status = 'W'
                       AND cd1.cd_marital_status = cd2.cd_marital_status
                       AND cd1.cd_education_status = '2 yr Degree'
                       AND cd1.cd_education_status = cd2.cd_education_status
                       AND ws_sales_price BETWEEN 150.00 AND 200.00 ) )
              AND ( ( ca_country = 'United States'
                     AND ca_state IN ('IN',
                                      'OH',
                                      'NJ')
                     AND ws_net_profit BETWEEN 100 AND 200)
                   OR ( ca_country = 'United States'
                       AND ca_state IN ('WI',
                                        'CT',
                                        'KY')
                       AND ws_net_profit BETWEEN 150 AND 300)
                   OR ( ca_country = 'United States'
                       AND ca_state IN ('LA',
                                        'IA',
                                        'AR')
                       AND ws_net_profit BETWEEN 50 AND 250) )
            GROUP BY r_reason_desc
            ORDER BY SUBSTRING(r_reason_desc,1,20) NULLS FIRST,
                     avg(ws_quantity) NULLS FIRST,
                     avg(wr_refunded_cash) NULLS FIRST,
                     avg(wr_fee) NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            sub_reason = r.IsDBNull(0) ? null : r.GetString(0),
            avg1 = r.IsDBNull(1) ? (double?)null : r.GetDouble(1),
            avg2 = r.IsDBNull(2) ? (double?)null : r.GetDouble(2),
            avg3 = r.IsDBNull(3) ? (double?)null : r.GetDouble(3)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].sub_reason, polarsResult[i].sub_reason);
            Assert.Equal(duckDbResult[i].avg1 ?? 0.0, polarsResult[i].avg1 ?? 0.0, 2);
            Assert.Equal(duckDbResult[i].avg2 ?? 0.0, polarsResult[i].avg2 ?? 0.0, 2);
            Assert.Equal(duckDbResult[i].avg3 ?? 0.0, polarsResult[i].avg3 ?? 0.0, 2);
        }
    }

    /*
     * TPC-DS Query 86
     *
     * SELECT sum(ws_net_paid) AS total_sum ,
     *        i_category ,
     *        i_class ,
     *        grouping(i_category)+grouping(i_class) AS lochierarchy ,
     *        rank() OVER ( PARTITION BY grouping(i_category)+grouping(i_class),
     *                                   CASE
     *                                       WHEN grouping(i_class) = 0 THEN i_category
     *                                   END
     *                     ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
     * FROM web_sales ,
     *      date_dim d1 ,
     *      item
     * WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
     *   AND d1.d_date_sk = ws_sold_date_sk
     *   AND i_item_sk = ws_item_sk
     * GROUP BY rollup(i_category,i_class)
     * ORDER BY lochierarchy DESC NULLS FIRST,
     *          CASE
     *              WHEN grouping(i_category)+grouping(i_class) = 0 THEN i_category
     *          END NULLS FIRST,
     *          rank_within_parent NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q86()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_month_seq >= 1200 && d.d_month_seq <= 1211);

        var baseSales = from ws in webSalesQuery
                        join d in filteredDates on ws.ws_sold_date_sk equals d.d_date_sk
                        join i in itemQuery on ws.ws_item_sk equals i.i_item_sk
                        group ws by new { i.i_category, i.i_class } into g
                        select new
                        {
                            g.Key.i_category,
                            g.Key.i_class,
                            total_sum = g.Sum(x => x.ws_net_paid)
                        };

        var level0 = from r in baseSales
                     select new
                     {
                         total_sum = r.total_sum,
                         i_category = r.i_category,
                         i_class = r.i_class,
                         t_class = 0,
                         lochierarchy = 0
                     };

        var level1 = from r in baseSales
                     group r by r.i_category into g
                     select new
                     {
                         total_sum = g.Sum(x => x.total_sum),
                         i_category = g.Key,
                         i_class = (string?)null,
                         t_class = 1,
                         lochierarchy = 1
                     };

        var level2 = from r in baseSales
                     group r by 1 into g
                     select new
                     {
                         total_sum = g.Sum(x => x.total_sum),
                         i_category = (string?)null,
                         i_class = (string?)null,
                         t_class = 1,
                         lochierarchy = 2
                     };

        var rollup = level0.Concat(level1).Concat(level2);

        var q86 = (from r in rollup
                   select new
                   {
                       r.total_sum,
                       r.i_category,
                       r.i_class,
                       r.lochierarchy,
                       rank_within_parent = (long)r.total_sum.Rank(RankMethod.Min, true).Over(new { r.lochierarchy, parent = r.t_class == 0 ? r.i_category : null })
                   } into res
                   orderby res.lochierarchy descending,
                           res.lochierarchy == 0 ? res.i_category : null,
                           res.rank_within_parent
                   select res).Take(100);

        AssertPlan(q86, NativePushdown);

        var polarsResult = q86.ToList();

        var duckDbSql = $@"
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

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            total_sum = r.IsDBNull(0) ? (decimal?)null : r.GetDecimal(0),
            i_category = r.IsDBNull(1) ? null : r.GetString(1),
            i_class = r.IsDBNull(2) ? null : r.GetString(2),
            lochierarchy = r.GetInt32(3),
            rank_within_parent = r.GetInt64(4)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].total_sum ?? 0m, polarsResult[i].total_sum ?? 0m, 2);
            Assert.Equal(duckDbResult[i].i_category, polarsResult[i].i_category);
            Assert.Equal(duckDbResult[i].i_class, polarsResult[i].i_class);
            Assert.Equal(duckDbResult[i].lochierarchy, polarsResult[i].lochierarchy);
            Assert.Equal(duckDbResult[i].rank_within_parent, polarsResult[i].rank_within_parent);
        }
    }

    /*
     * TPC-DS Query 87
     *
     * SELECT count(*)
     * FROM ((SELECT DISTINCT c_last_name,
     *                          c_first_name,
     *                          d_date
     *          FROM store_sales,
     *               date_dim,
     *               customer
     *          WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     *            AND store_sales.ss_customer_sk = customer.c_customer_sk
     *            AND d_month_seq BETWEEN 1200 AND 1200+11)
     *       EXCEPT
     *         (SELECT DISTINCT c_last_name,
     *                          c_first_name,
     *                          d_date
     *          FROM catalog_sales,
     *               date_dim,
     *               customer
     *          WHERE catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
     *            AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     *            AND d_month_seq BETWEEN 1200 AND 1200+11)
     *       EXCEPT
     *         (SELECT DISTINCT c_last_name,
     *                          c_first_name,
     *                          d_date
     *          FROM web_sales,
     *               date_dim,
     *               customer
     *          WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk
     *            AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
     *            AND d_month_seq BETWEEN 1200 AND 1200+11)) cool_cust ;
     */
    [Fact]
    public void Test_Q87()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        var dates = dateDimQuery.Where(d => d.d_month_seq >= 1200 && d.d_month_seq <= 1211);

        var storeStream = (from ss in storeSalesQuery
                           join d in dates on ss.ss_sold_date_sk equals d.d_date_sk
                           join c in customerQuery on ss.ss_customer_sk equals c.c_customer_sk
                           select new { c.c_last_name, c.c_first_name, d.d_date }).Distinct();

        var catalogStream = (from cs in catalogSalesQuery
                             join d in dates on cs.cs_sold_date_sk equals d.d_date_sk
                             join c in customerQuery on cs.cs_bill_customer_sk equals c.c_customer_sk
                             select new { c.c_last_name, c.c_first_name, d.d_date }).Distinct();

        var webStream = (from ws in webSalesQuery
                         join d in dates on ws.ws_sold_date_sk equals d.d_date_sk
                         join c in customerQuery on ws.ws_bill_customer_sk equals c.c_customer_sk
                         select new { c.c_last_name, c.c_first_name, d.d_date }).Distinct();

        var coolCust = storeStream.Except(catalogStream).Except(webStream);

        AssertPlan(coolCust, NativePushdown);

        var polarsCount = coolCust.LongCount();

        var duckDbSql = $@"
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

        var duckDbCount = DuckDbFixture.Query(duckDbSql, r => r.GetInt64(0))[0];

        Assert.Equal(duckDbCount, polarsCount);
    }

    /*
     * TPC-DS Query 88
     *
     * SELECT *
     * FROM
     *   (SELECT count(*) h8_30_to_9 ... ) s1,
     *   ...
     *   (SELECT count(*) h12_to_12_30 ... ) s8 ;
     */
    [Fact]
    public void Test_Q88()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var hdemoQuery = TpcdsData.HouseholdDemographicsQuery();
        var timeDimQuery = TpcdsData.TimeDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var baseSales = from ss in storeSalesQuery
                        join hd in hdemoQuery on ss.ss_hdemo_sk equals hd.hd_demo_sk
                        join t in timeDimQuery on ss.ss_sold_time_sk equals t.t_time_sk
                        join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                        where s.s_store_name == "ese"
                           && ((hd.hd_dep_count == 4 && hd.hd_vehicle_count <= 6)
                               || (hd.hd_dep_count == 2 && hd.hd_vehicle_count <= 4)
                               || (hd.hd_dep_count == 0 && hd.hd_vehicle_count <= 2))
                        select new
                        {
                            t.t_hour,
                            t.t_minute
                        };

        var q88 = from x in baseSales
                  group x by 1 into g
                  select new
                  {
                      h8_30_to_9 = g.Sum(x => x.t_hour == 8 && x.t_minute >= 30 ? 1L : 0L),
                      h9_to_9_30 = g.Sum(x => x.t_hour == 9 && x.t_minute < 30 ? 1L : 0L),
                      h9_30_to_10 = g.Sum(x => x.t_hour == 9 && x.t_minute >= 30 ? 1L : 0L),
                      h10_to_10_30 = g.Sum(x => x.t_hour == 10 && x.t_minute < 30 ? 1L : 0L),
                      h10_30_to_11 = g.Sum(x => x.t_hour == 10 && x.t_minute >= 30 ? 1L : 0L),
                      h11_to_11_30 = g.Sum(x => x.t_hour == 11 && x.t_minute < 30 ? 1L : 0L),
                      h11_30_to_12 = g.Sum(x => x.t_hour == 11 && x.t_minute >= 30 ? 1L : 0L),
                      h12_to_12_30 = g.Sum(x => x.t_hour == 12 && x.t_minute < 30 ? 1L : 0L)
                  };

        AssertPlan(q88, NativePushdown);

        var polarsResult = q88.ToList();

        var duckDbSql = $@"
            SELECT *
            FROM
              (SELECT count(*) h8_30_to_9
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 8
                 AND time_dim.t_minute >= 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s1,
              (SELECT count(*) h9_to_9_30
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 9
                 AND time_dim.t_minute < 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s2,
              (SELECT count(*) h9_30_to_10
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 9
                 AND time_dim.t_minute >= 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s3,
              (SELECT count(*) h10_to_10_30
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 10
                 AND time_dim.t_minute < 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s4,
              (SELECT count(*) h10_30_to_11
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 10
                 AND time_dim.t_minute >= 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s5,
              (SELECT count(*) h11_to_11_30
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 11
                 AND time_dim.t_minute < 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s6,
              (SELECT count(*) h11_30_to_12
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 11
                 AND time_dim.t_minute >= 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s7,
              (SELECT count(*) h12_to_12_30
               FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_sold_time_sk = time_dim.t_time_sk
                 AND ss_hdemo_sk = household_demographics.hd_demo_sk
                 AND ss_store_sk = s_store_sk
                 AND time_dim.t_hour = 12
                 AND time_dim.t_minute < 30
                 AND ((household_demographics.hd_dep_count = 4
                       AND household_demographics.hd_vehicle_count<=4+2)
                      OR (household_demographics.hd_dep_count = 2
                          AND household_demographics.hd_vehicle_count<=2+2)
                      OR (household_demographics.hd_dep_count = 0
                          AND household_demographics.hd_vehicle_count<=0+2))
                 AND store.s_store_name = 'ese') s8 ;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            h8_30_to_9 = r.GetInt64(0),
            h9_to_9_30 = r.GetInt64(1),
            h9_30_to_10 = r.GetInt64(2),
            h10_to_10_30 = r.GetInt64(3),
            h10_30_to_11 = r.GetInt64(4),
            h11_to_11_30 = r.GetInt64(5),
            h11_30_to_12 = r.GetInt64(6),
            h12_to_12_30 = r.GetInt64(7)
        })[0];

        var p = polarsResult.Count > 0 ? polarsResult[0] : new
        {
            h8_30_to_9 = 0L,
            h9_to_9_30 = 0L,
            h9_30_to_10 = 0L,
            h10_to_10_30 = 0L,
            h10_30_to_11 = 0L,
            h11_to_11_30 = 0L,
            h11_30_to_12 = 0L,
            h12_to_12_30 = 0L
        };

        Assert.Equal(duckDbResult.h8_30_to_9, p.h8_30_to_9);
        Assert.Equal(duckDbResult.h9_to_9_30, p.h9_to_9_30);
        Assert.Equal(duckDbResult.h9_30_to_10, p.h9_30_to_10);
        Assert.Equal(duckDbResult.h10_to_10_30, p.h10_to_10_30);
        Assert.Equal(duckDbResult.h10_30_to_11, p.h10_30_to_11);
        Assert.Equal(duckDbResult.h11_to_11_30, p.h11_to_11_30);
        Assert.Equal(duckDbResult.h11_30_to_12, p.h11_30_to_12);
        Assert.Equal(duckDbResult.h12_to_12_30, p.h12_to_12_30);
    }

    /*
     * TPC-DS Query 89
     *
     * SELECT * from
     *   (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
     *    FROM item, store_sales, date_dim, store
     *    WHERE ss_item_sk = i_item_sk
     *      AND ss_sold_date_sk = d_date_sk
     *      AND ss_store_sk = s_store_sk
     *      AND d_year = 1999
     *      AND ((i_category IN ('Books','Electronics','Sports')
     *            AND i_class IN ('computers','stereo','football') )
     *           OR (i_category IN ('Men','Jewelry','Women')
     *               AND i_class IN ('shirts','birdal','dresses')))
     *    GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy) tmp1
     * WHERE CASE
     *           WHEN (avg_monthly_sales <> 0) THEN (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
     *           ELSE NULL
     *       END > 0.1
     * ORDER BY sum_sales - avg_monthly_sales,
     *          s_store_name, 1, 2, 3, 5, 6, 7, 8
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q89()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        string[] cat1 = ["Books", "Electronics", "Sports"];
        string[] class1 = ["computers", "stereo", "football"];
        string[] cat2 = ["Men", "Jewelry", "Women"];
        string[] class2 = ["shirts", "birdal", "dresses"];

        var grouped = from ss in storeSalesQuery
                      join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                      join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                      join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                      where d.d_year == 1999
                         && ((cat1.Contains(i.i_category!) && class1.Contains(i.i_class!))
                             || (cat2.Contains(i.i_category!) && class2.Contains(i.i_class!)))
                      group ss by new
                      {
                          i.i_category,
                          i.i_class,
                          i.i_brand,
                          s.s_store_name,
                          s.s_company_name,
                          d.d_moy
                      } into g
                      select new
                      {
                          g.Key.i_category,
                          g.Key.i_class,
                          g.Key.i_brand,
                          g.Key.s_store_name,
                          g.Key.s_company_name,
                          g.Key.d_moy,
                          sum_sales = g.Sum(x => x.ss_sales_price)
                      };

        var tmp1 = from g in grouped
                   select new
                   {
                       g.i_category,
                       g.i_class,
                       g.i_brand,
                       g.s_store_name,
                       g.s_company_name,
                       g.d_moy,
                       g.sum_sales,
                       avg_monthly_sales = g.sum_sales.Average().Over(new
                       {
                           g.i_category,
                           g.i_brand,
                           g.s_store_name,
                           g.s_company_name
                       })
                   };

        var q89 = (from t in tmp1
                   where t.avg_monthly_sales != 0.0
                      && (Math.Abs((double)t.sum_sales! - t.avg_monthly_sales) / t.avg_monthly_sales > 0.1)
                   orderby (double)t.sum_sales! - t.avg_monthly_sales,
                           t.s_store_name,
                           t.i_category,
                           t.i_class,
                           t.i_brand,
                           t.s_company_name,
                           t.d_moy,
                           t.sum_sales,
                           t.avg_monthly_sales
                   select t).Take(100);

        AssertPlan(q89, NativePushdown);

        var polarsResult = q89.ToList();

        var duckDbSql = $@"
            SELECT * from
              (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
               FROM read_parquet('{TpcdsData.ItemPath}') item,
                    read_parquet('{TpcdsData.StoreSalesPath}') store_sales,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim,
                    read_parquet('{TpcdsData.StorePath}') store
               WHERE ss_item_sk = i_item_sk
                 AND ss_sold_date_sk = d_date_sk
                 AND ss_store_sk = s_store_sk
                 AND d_year = 1999
                 AND ((i_category IN ('Books','Electronics','Sports')
                       AND i_class IN ('computers','stereo','football') )
                      OR (i_category IN ('Men','Jewelry','Women')
                          AND i_class IN ('shirts','birdal','dresses')))
               GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy) tmp1
            WHERE CASE
                      WHEN (avg_monthly_sales <> 0) THEN (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
                      ELSE NULL
                  END > 0.1
            ORDER BY sum_sales - avg_monthly_sales NULLS FIRST,
                     s_store_name NULLS FIRST,
                     1 NULLS FIRST,
                     2 NULLS FIRST,
                     3 NULLS FIRST,
                     5 NULLS FIRST,
                     6 NULLS FIRST,
                     7 NULLS FIRST,
                     8 NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_category = r.IsDBNull(0) ? null : r.GetString(0),
            i_class = r.IsDBNull(1) ? null : r.GetString(1),
            i_brand = r.IsDBNull(2) ? null : r.GetString(2),
            s_store_name = r.IsDBNull(3) ? null : r.GetString(3),
            s_company_name = r.IsDBNull(4) ? null : r.GetString(4),
            d_moy = r.IsDBNull(5) ? (long?)null : r.GetInt64(5),
            sum_sales = r.IsDBNull(6) ? (decimal?)null : r.GetDecimal(6),
            avg_monthly_sales = r.IsDBNull(7) ? 0.0 : r.GetDouble(7)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].i_category, polarsResult[i].i_category);
            Assert.Equal(duckDbResult[i].i_class, polarsResult[i].i_class);
            Assert.Equal(duckDbResult[i].i_brand, polarsResult[i].i_brand);
            Assert.Equal(duckDbResult[i].s_store_name, polarsResult[i].s_store_name);
            Assert.Equal(duckDbResult[i].s_company_name, polarsResult[i].s_company_name);
            Assert.Equal(duckDbResult[i].d_moy, polarsResult[i].d_moy);
            Assert.Equal(duckDbResult[i].sum_sales ?? 0m, polarsResult[i].sum_sales ?? 0m, 2);
            Assert.Equal(duckDbResult[i].avg_monthly_sales, polarsResult[i].avg_monthly_sales, 2);
        }
    }

    /*
     * TPC-DS Query 90
     *
     * SELECT case when pmc=0 then null else cast(amc AS decimal(15,4))/cast(pmc AS decimal(15,4)) end am_pm_ratio
     * FROM
     *   (SELECT count(*) amc
     *    FROM web_sales,
     *         household_demographics,
     *         time_dim,
     *         web_page
     *    WHERE ws_sold_time_sk = time_dim.t_time_sk
     *      AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
     *      AND ws_web_page_sk = web_page.wp_web_page_sk
     *      AND time_dim.t_hour BETWEEN 8 AND 8+1
     *      AND household_demographics.hd_dep_count = 6
     *      AND web_page.wp_char_count BETWEEN 5000 AND 5200) at_,
     *   (SELECT count(*) pmc
     *    FROM web_sales,
     *         household_demographics,
     *         time_dim,
     *         web_page
     *    WHERE ws_sold_time_sk = time_dim.t_time_sk
     *      AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
     *      AND ws_web_page_sk = web_page.wp_web_page_sk
     *      AND time_dim.t_hour BETWEEN 19 AND 19+1
     *      AND household_demographics.hd_dep_count = 6
     *      AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
     * ORDER BY am_pm_ratio
     * LIMIT 100;
     */
    [Fact]
    public void Test_Q90()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var hdemoQuery = TpcdsData.HouseholdDemographicsQuery();
        var timeDimQuery = TpcdsData.TimeDimQuery();
        var webPageQuery = TpcdsData.WebPageQuery();

        var baseSales = from ws in webSalesQuery
                        join hd in hdemoQuery on ws.ws_ship_hdemo_sk equals hd.hd_demo_sk
                        join t in timeDimQuery on ws.ws_sold_time_sk equals t.t_time_sk
                        join wp in webPageQuery on ws.ws_web_page_sk equals wp.wp_web_page_sk
                        where hd.hd_dep_count == 6
                           && wp.wp_char_count >= 5000 && wp.wp_char_count <= 5200
                        select new
                        {
                            t.t_hour
                        };

        var counts = from x in baseSales
                     group x by 1 into g
                     select new
                     {
                         amc = g.Sum(x => x.t_hour >= 8 && x.t_hour <= 9 ? 1L : 0L),
                         pmc = g.Sum(x => x.t_hour >= 19 && x.t_hour <= 20 ? 1L : 0L)
                     };

        var q90 = (from c in counts
                   let am_pm_ratio = c.pmc == 0L ? (double?)null : (double)c.amc / (double)c.pmc
                   orderby am_pm_ratio
                   select new
                   {
                       am_pm_ratio
                   }).Take(100);

        AssertPlan(q90, NativePushdown);

        var polarsResult = q90.ToList();

        var duckDbSql = $@"
            SELECT case when pmc=0 then null else cast(amc AS decimal(15,4))/cast(pmc AS decimal(15,4)) end am_pm_ratio
            FROM
              (SELECT count(*) amc
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.WebPagePath}') web_page
               WHERE ws_sold_time_sk = time_dim.t_time_sk
                 AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
                 AND ws_web_page_sk = web_page.wp_web_page_sk
                 AND time_dim.t_hour BETWEEN 8 AND 8+1
                 AND household_demographics.hd_dep_count = 6
                 AND web_page.wp_char_count BETWEEN 5000 AND 5200) at_,
              (SELECT count(*) pmc
               FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales,
                    read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics,
                    read_parquet('{TpcdsData.TimeDimPath}') time_dim,
                    read_parquet('{TpcdsData.WebPagePath}') web_page
               WHERE ws_sold_time_sk = time_dim.t_time_sk
                 AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
                 AND ws_web_page_sk = web_page.wp_web_page_sk
                 AND time_dim.t_hour BETWEEN 19 AND 19+1
                 AND household_demographics.hd_dep_count = 6
                 AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
            ORDER BY am_pm_ratio NULLS FIRST
            LIMIT 100;
        ";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            am_pm_ratio = r.IsDBNull(0) ? (double?)null : (double)r.GetDecimal(0)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            var expected = duckDbResult[i].am_pm_ratio;
            var actual = polarsResult[i]?.am_pm_ratio;
            if (expected == null || actual == null || double.IsNaN(actual.Value) || double.IsInfinity(actual.Value))
            {
                Assert.True(expected == null && (actual == null || double.IsNaN(actual.Value) || double.IsInfinity(actual.Value)));
            }
            else
            {
                Assert.Equal(expected.Value, actual.Value, 4);
            }
        }
    }
}

