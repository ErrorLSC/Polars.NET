using LinqToDB;
using LinqToDB.Data;
using Polars.Tpcds;

namespace Polars.Benchmarks.Tpcds;

internal static partial class TpcdsLinq2DbQueries
{
    public static IQueryable<string> Q1(DataConnection db)
    {
        var ctr = (from sr in db.GetTable<StoreReturns>()
                   join d in db.GetTable<DateDim>() on sr.sr_returned_date_sk equals d.d_date_sk
                   where d.d_year == 2000
                   group sr by new { sr.sr_customer_sk, sr.sr_store_sk } into g
                   select new
                   {
                       ctr_customer_sk = g.Key.sr_customer_sk,
                       ctr_store_sk = g.Key.sr_store_sk,
                       ctr_total_return = g.Sum(x => x.sr_return_amt)
                   }).AsCte("customer_total_return");

        return (from ctr1 in ctr
                join s in db.GetTable<Store>() on ctr1.ctr_store_sk equals s.s_store_sk
                join c in db.GetTable<Polars.Tpcds.Customer>() on ctr1.ctr_customer_sk equals c.c_customer_sk
                where s.s_state == "TN"
                   && ctr1.ctr_total_return > (from ctr2 in ctr
                                               where ctr2.ctr_store_sk == ctr1.ctr_store_sk
                                               select ctr2.ctr_total_return).Average() * 1.2m
                orderby c.c_customer_id
                select c.c_customer_id!).Take(100);
    }

    public static IQueryable<Q3Row> Q3(DataConnection db) =>
        (from ss in db.GetTable<StoreSales>()
         join dt in db.GetTable<DateDim>() on ss.ss_sold_date_sk equals dt.d_date_sk
         join i in db.GetTable<Item>() on ss.ss_item_sk equals i.i_item_sk
         where i.i_manufact_id == 128 && dt.d_moy == 11
         group ss by new { dt.d_year, i.i_brand, i.i_brand_id } into g
         select new Q3Row
         {
             d_year = g.Key.d_year,
             brand_id = g.Key.i_brand_id,
             brand = g.Key.i_brand,
             sum_agg = g.Sum(x => x.ss_ext_sales_price)
         })
        .OrderBy(r => r.d_year).ThenByDescending(r => r.sum_agg).ThenBy(r => r.brand_id)
        .Take(100);

    public static IQueryable<Q16Row> Q16(DataConnection db)
    {
        var cs1s = db.GetTable<CatalogSales>();
        var minDate = new DateOnly(2002, 2, 1);
        var maxDate = new DateOnly(2002, 4, 2);

        return from cs1 in cs1s
               join d in db.GetTable<DateDim>() on cs1.cs_ship_date_sk equals d.d_date_sk
               join ca in db.GetTable<CustomerAddress>() on cs1.cs_ship_addr_sk equals ca.ca_address_sk
               join cc in db.GetTable<CallCenter>() on cs1.cs_call_center_sk equals cc.cc_call_center_sk
               where d.d_date >= minDate && d.d_date <= maxDate
                  && ca.ca_state == "GA"
                  && cc.cc_county == "Williamson County"
                  && db.GetTable<CatalogSales>().Any(cs2 => cs1.cs_order_number == cs2.cs_order_number
                                                            && cs1.cs_warehouse_sk != cs2.cs_warehouse_sk)
                  && !db.GetTable<CatalogReturns>().Any(cr1 => cs1.cs_order_number == cr1.cr_order_number)
               group cs1 by 1 into g
               select new Q16Row
               {
                   order_count = g.Select(x => x.cs_order_number).Distinct().LongCount(),
                   total_shipping_cost = g.Sum(x => x.cs_ext_ship_cost),
                   total_net_profit = g.Sum(x => x.cs_net_profit)
               };
    }

    public static IQueryable<Q36Row> Q36(DataConnection db)
    {
        var rolled = (from ss in db.GetTable<StoreSales>()
                      join d in db.GetTable<DateDim>() on ss.ss_sold_date_sk equals d.d_date_sk
                      join i in db.GetTable<Item>() on ss.ss_item_sk equals i.i_item_sk
                      join s in db.GetTable<Store>() on ss.ss_store_sk equals s.s_store_sk
                      where d.d_year == 2001 && s.s_state == "TN"
                      group ss by Sql.GroupBy.Rollup(new { i.i_category, i.i_class }) into g
                      select new
                      {
                          g.Key.i_category,
                          g.Key.i_class,
                          net_profit = g.Sum(x => x.ss_net_profit),
                          ext_sales_price = g.Sum(x => x.ss_ext_sales_price),
                          g_category = Sql.Grouping(g.Key.i_category),
                          g_class = Sql.Grouping(g.Key.i_class)
                      }).AsCte("rolled");

        return (from r in rolled
                let gross_margin = (double)(r.net_profit / r.ext_sales_price)!
                let lochierarchy = r.g_category + r.g_class
                select new
                {
                    gross_margin,
                    r.i_category,
                    r.i_class,
                    lochierarchy,
                    rank_within_parent = Sql.Window.Rank(f => f.PartitionBy(lochierarchy, r.g_class == 0 ? r.i_category : null).OrderBy(gross_margin))
                } into res
                orderby res.lochierarchy descending, res.lochierarchy == 0 ? res.i_category : null, res.rank_within_parent
                select new Q36Row
                {
                    gross_margin = res.gross_margin,
                    i_category = res.i_category,
                    i_class = res.i_class,
                    lochierarchy = res.lochierarchy,
                    rank_within_parent = res.rank_within_parent
                }).Take(100);
    }

    public static IQueryable<Q98Row> Q98(DataConnection db)
    {
        var startDate = new DateOnly(1999, 2, 22);
        var endDate = new DateOnly(1999, 3, 24);
        string[] categories = ["Sports", "Books", "Home"];

        var grouped = from ss in db.GetTable<StoreSales>()
                      join i in db.GetTable<Item>() on ss.ss_item_sk equals i.i_item_sk
                      join d in db.GetTable<DateDim>() on ss.ss_sold_date_sk equals d.d_date_sk
                      where categories.Contains(i.i_category!) && d.d_date >= startDate && d.d_date <= endDate
                      group ss by new { i.i_item_id, i.i_item_desc, i.i_category, i.i_class, i.i_current_price } into g
                      select new
                      {
                          g.Key.i_item_id,
                          g.Key.i_item_desc,
                          g.Key.i_category,
                          g.Key.i_class,
                          g.Key.i_current_price,
                          itemrevenue = g.Sum(x => x.ss_ext_sales_price)
                      };

        return from r in grouped
               orderby r.i_category, r.i_class, r.i_item_id, r.i_item_desc,
                       (double?)r.itemrevenue * 100.0 / (double?)Sql.Window.Sum(r.itemrevenue, f => f.PartitionBy(r.i_class))
               select new Q98Row
               {
                   i_item_id = r.i_item_id,
                   i_item_desc = r.i_item_desc,
                   i_category = r.i_category,
                   i_class = r.i_class,
                   i_current_price = r.i_current_price,
                   itemrevenue = r.itemrevenue,
                   revenueratio = (double?)r.itemrevenue * 100.0 / (double?)Sql.Ext.Sum(r.itemrevenue).Over().PartitionBy(r.i_class).ToValue()
               };
    }
}
