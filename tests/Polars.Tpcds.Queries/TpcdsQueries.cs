using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Tpcds;

/// <summary>TPC-DS LINQ queries shared by the xunit tests and the benchmarks.</summary>
public static partial class TpcdsQueries
{
    public static IQueryable<string> Q1()
    {
        var returnsQuery = TpcdsData.StoreReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var customerQuery = TpcdsData.CustomerQuery();

        // Step 1: customer_total_return CTE
        var ctr = from sr in returnsQuery
                  join d in dateDimQuery on sr.sr_returned_date_sk equals d.d_date_sk
                  where d.d_year == 2000
                  group sr by new { sr.sr_customer_sk, sr.sr_store_sk } into g
                  select new
                  {
                      ctr_customer_sk = g.Key.sr_customer_sk,
                      ctr_store_sk = g.Key.sr_store_sk,
                      // SQL sum() is NULL for a group without non-null values, while LINQ Sum() is 0.
                      ctr_total_return = g.Count(x => x.sr_return_amt != null) > 0 ? g.Sum(x => x.sr_return_amt) : null
                  };

        // Step 2: Main query with subquery
        var q1 = from ctr1 in ctr
                 where ctr1.ctr_total_return > ctr.Where(ctr2 => ctr2.ctr_store_sk == ctr1.ctr_store_sk)
                                                  .Average(ctr2 => ctr2.ctr_total_return) * 1.2m
                 join s in storeQuery.Where(s => s.s_state == "TN") on ctr1.ctr_store_sk equals s.s_store_sk
                 join c in customerQuery on ctr1.ctr_customer_sk equals c.c_customer_sk
                 orderby c.c_customer_id
                 select c.c_customer_id;

        var limitedQ1 = q1.Take(100);

        return limitedQ1;
    }

    public static IQueryable<Q3Row> Q3()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();

        IQueryable<Q3Row> BuildQuery(long manufactId)
        {
            var baseQuery = from ss in storeSalesQuery
                            join dt in dateDimQuery.Where(d => d.d_moy == 11) on ss.ss_sold_date_sk equals dt.d_date_sk
                            join i in itemQuery.Where(i => i.i_manufact_id == manufactId) on ss.ss_item_sk equals i.i_item_sk
                            group ss by new { dt.d_year, i.i_brand, i.i_brand_id } into g
                            select new
                            {
                                g.Key.d_year,
                                brand_id = g.Key.i_brand_id,
                                brand = g.Key.i_brand,
                                sum_agg = g.Sum(x => x.ss_ext_sales_price)
                            };

            return (from r in baseQuery
                    orderby r.d_year, r.sum_agg descending, r.brand_id
                    select new Q3Row
                    {
                        d_year = r.d_year,
                        brand_id = r.brand_id,
                        brand = r.brand,
                        sum_agg = r.sum_agg
                    }).Take(100);
        }

        // 1. Standard parameter (i_manufact_id = 128, yields 0 rows on SF=0.01)
        var standardQuery = BuildQuery(128);

        return standardQuery;
    }

    public static IQueryable<Q5Row> Q5()
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

        var minDate = new DateOnly(2000, 8, 23);
        var maxDate = new DateOnly(2000, 9, 6);
        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);

        // 1. Store Channel
        var ssUnion = storeSalesQuery.Select(ss => new
        {
            store_sk = ss.ss_store_sk,
            date_sk = ss.ss_sold_date_sk,
            sales_price = ss.ss_ext_sales_price,
            profit = ss.ss_net_profit,
            return_amt = (decimal?)0.00m,
            net_loss = (decimal?)0.00m
        }).Concat(storeReturnsQuery.Select(sr => new
        {
            store_sk = sr.sr_store_sk,
            date_sk = sr.sr_returned_date_sk,
            sales_price = (decimal?)0.00m,
            profit = (decimal?)0.00m,
            return_amt = sr.sr_return_amt,
            net_loss = sr.sr_net_loss
        }));

        var ssr = from sr in ssUnion
                  join d in filteredDates on sr.date_sk equals d.d_date_sk
                  join s in storeQuery on sr.store_sk equals s.s_store_sk
                  group sr by s.s_store_id into g
                  select new
                  {
                      channel = "store channel",
                      id = "store" + g.Key,
                      sales = g.Sum(x => x.sales_price),
                      returns_ = g.Sum(x => x.return_amt),
                      profit = g.Sum(x => x.profit) - g.Sum(x => x.net_loss)
                  };

        // 2. Catalog Channel
        var csUnion = catalogSalesQuery.Select(cs => new
        {
            page_sk = cs.cs_catalog_page_sk,
            date_sk = cs.cs_sold_date_sk,
            sales_price = cs.cs_ext_sales_price,
            profit = cs.cs_net_profit,
            return_amt = (decimal?)0.00m,
            net_loss = (decimal?)0.00m
        }).Concat(catalogReturnsQuery.Select(cr => new
        {
            page_sk = cr.cr_catalog_page_sk,
            date_sk = cr.cr_returned_date_sk,
            sales_price = (decimal?)0.00m,
            profit = (decimal?)0.00m,
            return_amt = cr.cr_return_amount,
            net_loss = cr.cr_net_loss
        }));

        var csr = from cr in csUnion
                  join d in filteredDates on cr.date_sk equals d.d_date_sk
                  join cp in catalogPageQuery on cr.page_sk equals cp.cp_catalog_page_sk
                  group cr by cp.cp_catalog_page_id into g
                  select new
                  {
                      channel = "catalog channel",
                      id = "catalog_page" + g.Key,
                      sales = g.Sum(x => x.sales_price),
                      returns_ = g.Sum(x => x.return_amt),
                      profit = g.Sum(x => x.profit) - g.Sum(x => x.net_loss)
                  };

        // 3. Web Channel
        var wsPart = webSalesQuery.Select(ws => new
        {
            wsr_web_site_sk = ws.ws_web_site_sk,
            date_sk = ws.ws_sold_date_sk,
            sales_price = ws.ws_ext_sales_price,
            profit = ws.ws_net_profit,
            return_amt = (decimal?)0.00m,
            net_loss = (decimal?)0.00m
        });

        var wrJoined = from wr in webReturnsQuery
                       join ws in webSalesQuery on new { item_sk = wr.wr_item_sk, order_num = wr.wr_order_number }
                                            equals new { item_sk = ws.ws_item_sk, order_num = ws.ws_order_number }
                       select new
                       {
                           wsr_web_site_sk = ws.ws_web_site_sk,
                           date_sk = wr.wr_returned_date_sk,
                           sales_price = (decimal?)0.00m,
                           profit = (decimal?)0.00m,
                           return_amt = wr.wr_return_amt,
                           net_loss = wr.wr_net_loss
                       };

        var wsUnion = wsPart.Concat(wrJoined);

        var wsr = from w in wsUnion
                  join d in filteredDates on w.date_sk equals d.d_date_sk
                  join ws in webSiteQuery on w.wsr_web_site_sk equals ws.web_site_sk
                  group w by ws.web_site_id into g
                  select new
                  {
                      channel = "web channel",
                      id = "web_site" + g.Key,
                      sales = g.Sum(x => x.sales_price),
                      returns_ = g.Sum(x => x.return_amt),
                      profit = g.Sum(x => x.profit) - g.Sum(x => x.net_loss)
                  };

        // Combined stream x
        var x = ssr.Concat(csr).Concat(wsr);

        // 3-Level ROLLUP via fluent GroupBy followed by RollUp
        var q5 = x
            .GroupBy(r => new { r.channel, r.id })
            .RollUp(
                (key, g) => new Q5Row

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

        return q5;
    }

    public static IQueryable<Q6Row> Q6()
    {
        var addressQuery = TpcdsData.CustomerAddressQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var dateDimSubQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();

        // 1. Month sequence subquery
        var targetMonths = (from d in dateDimSubQuery
                            where d.d_year == 2001 && d.d_moy == 1
                            select d.d_month_seq).Distinct();

        var filteredDates = from d in dateDimQuery
                            join m in targetMonths on d.d_month_seq equals m
                            select d;

        // 2. Item partition window condition using Over(). A window partitions NULL categories together,
        //    while the SQL's correlated `j.i_category = i.i_category` never matches a NULL category, so
        //    NULL categories are excluded to keep the two forms equivalent.
        var filteredItems = from i in itemQuery
                            where i.i_category != null
                               && (double)i.i_current_price! > 1.2 * i.i_current_price.Average().Over(i.i_category)
                            select i;

        // 3. Multi-table join + GroupBy state + Having count >= 10
        var q6 = (from a in addressQuery
                  join c in customerQuery on a.ca_address_sk equals c.c_current_addr_sk
                  join s in storeSalesQuery on c.c_customer_sk equals s.ss_customer_sk
                  join d in filteredDates on s.ss_sold_date_sk equals d.d_date_sk
                  join i in filteredItems on s.ss_item_sk equals i.i_item_sk
                  group a by a.ca_state into g
                  where g.Count() >= 10
                  select new Q6Row
                  {
                      state = g.Key,
                      cnt = g.LongCount()
                  } into r
                  orderby r.cnt, r.state
                  select r).Take(100);

        return q6;
    }

    public static IQueryable<Q14Row> Q14()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var dates3Years = dateDimQuery.Where(d => d.d_year >= 1999 && d.d_year <= 2001);

        // 1. cross_items CTE: items whose (brand, class, category) exist in all 3 sales channels
        var storeBCC = (from ss in storeSalesQuery
                        join iss in itemQuery on ss.ss_item_sk equals iss.i_item_sk
                        join d1 in dates3Years on ss.ss_sold_date_sk equals d1.d_date_sk
                        select new
                        {
                            brand_id = iss.i_brand_id,
                            class_id = iss.i_class_id,
                            category_id = iss.i_category_id
                        }).Distinct();

        var catalogBCC = (from cs in catalogSalesQuery
                          join ics in itemQuery on cs.cs_item_sk equals ics.i_item_sk
                          join d2 in dates3Years on cs.cs_sold_date_sk equals d2.d_date_sk
                          select new
                          {
                              brand_id = ics.i_brand_id,
                              class_id = ics.i_class_id,
                              category_id = ics.i_category_id
                          }).Distinct();

        var webBCC = (from ws in webSalesQuery
                      join iws in itemQuery on ws.ws_item_sk equals iws.i_item_sk
                      join d3 in dates3Years on ws.ws_sold_date_sk equals d3.d_date_sk
                      select new
                      {
                          brand_id = iws.i_brand_id,
                          class_id = iws.i_class_id,
                          category_id = iws.i_category_id
                      }).Distinct();

        var crossBCC = storeBCC.Intersect(catalogBCC).Intersect(webBCC);

        var crossItems = from i in itemQuery
                         join sq in crossBCC on new { brand_id = i.i_brand_id, class_id = i.i_class_id, category_id = i.i_category_id }
                                         equals new { sq.brand_id, sq.class_id, sq.category_id }
                         select new
                         {
                             i.i_item_sk,
                             i.i_brand_id,
                             i.i_class_id,
                             i.i_category_id
                         };

        // 2. avg_sales CTE: average sales across all 3 channels in 1999-2001
        var ssSales = from ss in storeSalesQuery
                      join d in dates3Years on ss.ss_sold_date_sk equals d.d_date_sk
                      select new { sales = (double?)(ss.ss_quantity * ss.ss_list_price) };

        var csSales = from cs in catalogSalesQuery
                      join d in dates3Years on cs.cs_sold_date_sk equals d.d_date_sk
                      select new { sales = (double?)(cs.cs_quantity * cs.cs_list_price) };

        var wsSales = from ws in webSalesQuery
                      join d in dates3Years on ws.ws_sold_date_sk equals d.d_date_sk
                      select new { sales = (double?)(ws.ws_quantity * ws.ws_list_price) };

        var allSales = ssSales.Concat(csSales).Concat(wsSales);

        // 3. Channel streams in year 2001, month 11 with having filter > avgSales
        var targetDates = dateDimQuery.Where(d => d.d_year == 2001 && d.d_moy == 11);

        var storeStream = from ss in storeSalesQuery
                          join i in crossItems on ss.ss_item_sk equals i.i_item_sk
                          join d in targetDates on ss.ss_sold_date_sk equals d.d_date_sk
                          group ss by new
                          {
                              i.i_brand_id,
                              i.i_class_id,
                              i.i_category_id
                          } into g
                          where g.Sum(x => x.ss_quantity * x.ss_list_price) > (decimal?)allSales.Average(x => x.sales)
                          select new
                          {
                              channel = "store",
                              g.Key.i_brand_id,
                              g.Key.i_class_id,
                              g.Key.i_category_id,
                              sales = g.Sum(x => x.ss_quantity * x.ss_list_price),
                              number_sales = g.LongCount()
                          };

        var catalogStream = from cs in catalogSalesQuery
                            join i in crossItems on cs.cs_item_sk equals i.i_item_sk
                            join d in targetDates on cs.cs_sold_date_sk equals d.d_date_sk
                            group cs by new
                            {
                                i.i_brand_id,
                                i.i_class_id,
                                i.i_category_id
                            } into g
                            where g.Sum(x => x.cs_quantity * x.cs_list_price) > (decimal?)allSales.Average(x => x.sales)
                            select new
                            {
                                channel = "catalog",
                                g.Key.i_brand_id,
                                g.Key.i_class_id,
                                g.Key.i_category_id,
                                sales = g.Sum(x => x.cs_quantity * x.cs_list_price),
                                number_sales = g.LongCount()
                            };

        var webStream = from ws in webSalesQuery
                        join i in crossItems on ws.ws_item_sk equals i.i_item_sk
                        join d in targetDates on ws.ws_sold_date_sk equals d.d_date_sk
                        group ws by new
                        {
                            i.i_brand_id,
                            i.i_class_id,
                            i.i_category_id
                        } into g
                        where g.Sum(x => x.ws_quantity * x.ws_list_price) > (decimal?)allSales.Average(x => x.sales)
                        select new
                        {
                            channel = "web",
                            g.Key.i_brand_id,
                            g.Key.i_class_id,
                            g.Key.i_category_id,
                            sales = g.Sum(x => x.ws_quantity * x.ws_list_price),
                            number_sales = g.LongCount()
                        };

        var y = storeStream.Concat(catalogStream).Concat(webStream);

        // 4. 5-Level ROLLUP (channel, i_brand_id, i_class_id, i_category_id) via fluent GroupBy followed by RollUp
        var q14 = y
            .GroupBy(r => new { r.channel, r.i_brand_id, r.i_class_id, r.i_category_id })
            .RollUp(
                (key, g) => new Q14Row

                {
                    channel = (string?)key.channel,
                    i_brand_id = (long?)key.i_brand_id,
                    i_class_id = (long?)key.i_class_id,
                    i_category_id = (long?)key.i_category_id,
                    sum_sales = g.Sum(x => x.sales),
                    sum_number_sales = g.Sum(x => x.number_sales)
                }
            )
            .OrderBy(r => r.channel)
            .ThenBy(r => r.i_brand_id)
            .ThenBy(r => r.i_class_id)
            .ThenBy(r => r.i_category_id)
            .Take(100);

        return q14;
    }

    public static IQueryable<Q16Row> Q16()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var callCenterQuery = TpcdsData.CallCenterQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();

        var minDate = new DateOnly(2002, 2, 1);
        var maxDate = new DateOnly(2002, 4, 2);

        var filteredDates = dateDimQuery.Where(d => d.d_date >= minDate && d.d_date <= maxDate);
        var filteredAddr = customerAddressQuery.Where(ca => ca.ca_state == "GA");
        var filteredCallCenter = callCenterQuery.Where(cc => cc.cc_county == "Williamson County");

        var baseSales = from cs in catalogSalesQuery
                        join d in filteredDates on cs.cs_ship_date_sk equals d.d_date_sk
                        join ca in filteredAddr on cs.cs_ship_addr_sk equals ca.ca_address_sk
                        join cc in filteredCallCenter on cs.cs_call_center_sk equals cc.cc_call_center_sk
                        select cs;

        // EXISTS (another row of the order from a different warehouse) AND NOT EXISTS (a return of the order)
        var eligibleSales = from cs in baseSales
                            // SQL <> is unknown when either warehouse is NULL
                            where catalogSalesQuery.Any(cs2 => cs2.cs_order_number == cs.cs_order_number
                                                               && cs.cs_warehouse_sk != null
                                                               && cs2.cs_warehouse_sk != null
                                                               && cs2.cs_warehouse_sk != cs.cs_warehouse_sk)
                               && !catalogReturnsQuery.Any(cr => cr.cr_order_number == cs.cs_order_number)
                            select cs;

        var q16 = from cs in eligibleSales
                  group cs by 1 into g
                  select new Q16Row
                  {
                      order_count = g.Select(x => x.cs_order_number).Distinct().LongCount(),
                      total_shipping_cost = g.Count(x => x.cs_ext_ship_cost != null) > 0 ? g.Sum(x => x.cs_ext_ship_cost) : null,
                      total_net_profit = g.Count(x => x.cs_net_profit != null) > 0 ? g.Sum(x => x.cs_net_profit) : null
                  };

        return q16;
    }

    public static IQueryable<Q36Row> Q36()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_year == 2001);
        var filteredStores = storeQuery.Where(s => s.s_state == "TN");

        var baseSales = from ss in storeSalesQuery
                        join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                        join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                        join s in filteredStores on ss.ss_store_sk equals s.s_store_sk
                        select new
                        {
                            i.i_category,
                            i.i_class,
                            ss.ss_net_profit,
                            ss.ss_ext_sales_price
                        };

        var results = from x in baseSales
                      group x by new { x.i_category, x.i_class } into g
                      select new
                      {
                          g.Key.i_category,
                          g.Key.i_class,
                          ss_net_profit = g.Sum(x => x.ss_net_profit),
                          ss_ext_sales_price = g.Sum(x => x.ss_ext_sales_price),
                          gross_margin = (double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!
                      };

        var level0 = from r in results
                     select new
                     {
                         r.gross_margin,
                         r.i_category,
                         r.i_class,
                         t_category = 0,
                         t_class = 0,
                         lochierarchy = 0
                     };

        var level1 = from r in results
                     group r by r.i_category into g
                     select new
                     {
                         gross_margin = (double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!,
                         i_category = g.Key,
                         i_class = (string?)null,
                         t_category = 0,
                         t_class = 1,
                         lochierarchy = 1
                     };

        var level2 = from r in results
                     group r by 1 into g
                     select new
                     {
                         gross_margin = (double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!,
                         i_category = (string?)null,
                         i_class = (string?)null,
                         t_category = 1,
                         t_class = 1,
                         lochierarchy = 2
                     };

        var resultsRollup = level0.Concat(level1).Concat(level2);

        var q36 = (from r in resultsRollup
                   select new
                   {
                       r.gross_margin,
                       r.i_category,
                       r.i_class,
                       r.lochierarchy,
                       parent = r.t_class == 0 ? r.i_category : null,
                       rank_within_parent = (long)r.gross_margin.Rank(RankMethod.Min).Over(new { r.lochierarchy, parent = r.t_class == 0 ? r.i_category : null })
                   } into res
                   orderby res.lochierarchy descending,
                           res.lochierarchy == 0 ? res.i_category : null,
                           res.rank_within_parent
                   select new Q36Row
                    {
                        gross_margin = res.gross_margin,
                        i_category = res.i_category,
                        i_class = res.i_class,
                        lochierarchy = res.lochierarchy,
                        rank_within_parent = res.rank_within_parent
                    }).Take(100);

        return q36;
    }

    public static IQueryable<Q38Row> Q38()
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

        var hotCust = storeStream
            .Intersect(catalogStream)
            .Intersect(webStream)
            .Select(r => new Q38Row { c_last_name = r.c_last_name, c_first_name = r.c_first_name, d_date = r.d_date });

        return hotCust;
    }

    public static IQueryable<Q47Row> Q47()
    {
        var itemQuery = TpcdsData.ItemQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var v1 = from ss in storeSalesQuery
                 join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                 join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                 where d.d_year == 1999 || (d.d_year == 1998 && d.d_moy == 12) || (d.d_year == 2000 && d.d_moy == 1)
                 group ss by new
                 {
                     i.i_category,
                     i.i_brand,
                     s.s_store_name,
                     s.s_company_name,
                     d.d_year,
                     d.d_moy
                 } into g
                 orderby g.Key.d_year, g.Key.d_moy
                 let sum_sales = g.Sum(x => x.ss_sales_price)
                 select new
                 {
                     g.Key.i_category,
                     g.Key.i_brand,
                     g.Key.s_store_name,
                     g.Key.s_company_name,
                     g.Key.d_year,
                     g.Key.d_moy,
                     avg_monthly_sales = sum_sales.Average().Over(new { g.Key.i_category, g.Key.i_brand, g.Key.s_store_name, g.Key.s_company_name, g.Key.d_year }),
                     sum_sales,
                     psum = sum_sales.Shift(1).Over(new { g.Key.i_category, g.Key.i_brand, g.Key.s_store_name, g.Key.s_company_name }),
                     nsum = sum_sales.Shift(-1).Over(new { g.Key.i_category, g.Key.i_brand, g.Key.s_store_name, g.Key.s_company_name })
                 };

        var q47 = (from x in v1
                   where x.d_year == 1999 && x.avg_monthly_sales > 0.0
                      && x.psum != null && x.nsum != null
                      && (Math.Abs((double)x.sum_sales! - x.avg_monthly_sales) / x.avg_monthly_sales > 0.1)
                   orderby (double?)x.sum_sales - x.avg_monthly_sales,
                           x.i_category,
                           x.i_brand,
                           x.s_store_name,
                           x.s_company_name,
                           x.d_year,
                           x.d_moy,
                           x.avg_monthly_sales,
                           x.sum_sales,
                           x.psum,
                           x.nsum
                   select x)
                  .Take(100)
                  .Select(x => new Q47Row
                  {
                      i_category = x.i_category,
                      i_brand = x.i_brand,
                      s_store_name = x.s_store_name,
                      s_company_name = x.s_company_name,
                      d_year = x.d_year,
                      d_moy = x.d_moy,
                      avg_monthly_sales = x.avg_monthly_sales,
                      sum_sales = x.sum_sales,
                      psum = x.psum,
                      nsum = x.nsum
                  });

        return q47;
    }

    public static IQueryable<Q51Row> Q51()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var webDaily = from ws in webSalesQuery
                       join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                       where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                       group ws by new { item_sk = ws.ws_item_sk, d.d_date } into g
                       select new
                       {
                           g.Key.item_sk,
                           g.Key.d_date,
                           web_daily = g.Sum(x => x.ws_sales_price) ?? 0.00m,
                           store_daily = 0.00m,
                           has_web = 1L,
                           has_web_val = g.Max(x => x.ws_sales_price != null ? 1L : 0L),
                           has_store = 0L,
                           has_store_val = 0L
                       };

        var storeDaily = from ss in storeSalesQuery
                         join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                         where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                         group ss by new { item_sk = ss.ss_item_sk, d.d_date } into g
                         select new
                         {
                             g.Key.item_sk,
                             g.Key.d_date,
                             web_daily = 0.00m,
                             store_daily = g.Sum(x => x.ss_sales_price) ?? 0.00m,
                             has_web = 0L,
                             has_web_val = 0L,
                             has_store = 1L,
                             has_store_val = g.Max(x => x.ss_sales_price != null ? 1L : 0L)
                         };

        var mergedDaily = from x in webDaily.Concat(storeDaily)
                          group x by new { x.item_sk, x.d_date } into g
                          select new
                          {
                              g.Key.item_sk,
                              g.Key.d_date,
                              web_daily = g.Sum(x => x.web_daily),
                              store_daily = g.Sum(x => x.store_daily),
                              has_web = g.Max(x => x.has_web),
                              has_web_val = g.Max(x => x.has_web_val),
                              has_store = g.Max(x => x.has_store),
                              has_store_val = g.Max(x => x.has_store_val)
                          };

        var withCume = from m in mergedDaily
                       orderby m.item_sk, m.d_date
                       select new
                       {
                           m.item_sk,
                           m.d_date,
                           m.has_web,
                           m.has_store,
                           web_cume_raw = m.web_daily.CumSum().Over(m.item_sk),
                           store_cume_raw = m.store_daily.CumSum().Over(m.item_sk),
                           seen_web = m.has_web_val.CumSum().Over(m.item_sk),
                           seen_store = m.has_store_val.CumSum().Over(m.item_sk)
                       };

        var projected = from c in withCume
                        select new
                        {
                            c.item_sk,
                            c.d_date,
                            web_sales = c.has_web > 0 && c.seen_web > 0 ? (decimal?)c.web_cume_raw : null,
                            store_sales = c.has_store > 0 && c.seen_store > 0 ? (decimal?)c.store_cume_raw : null,
                            web_cumulative = c.seen_web > 0 ? (decimal?)c.web_cume_raw : null,
                            store_cumulative = c.seen_store > 0 ? (decimal?)c.store_cume_raw : null
                        };

        var q51 = (from y in projected
                   where y.web_cumulative > y.store_cumulative
                   orderby y.item_sk, y.d_date
                   select y)
                  .Take(100)
                  .Select(y => new Q51Row
                  {
                      item_sk = y.item_sk,
                      d_date = y.d_date,
                      web_sales = y.web_sales,
                      store_sales = y.store_sales,
                      web_cumulative = y.web_cumulative,
                      store_cumulative = y.store_cumulative
                  });

        return q51;
    }

    public static IQueryable<Q55Row> Q55()
    {
        var dateDimQuery = TpcdsData.DateDimQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var q55 = (from d in dateDimQuery
                   join ss in storeSalesQuery on d.d_date_sk equals ss.ss_sold_date_sk
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   where i.i_manager_id == 28 && d.d_moy == 11 && d.d_year == 1999
                   group ss by new { i.i_brand, i.i_brand_id } into g
                   let ext_price = g.Sum(x => x.ss_ext_sales_price)
                   orderby ext_price descending, g.Key.i_brand_id
                   select new Q55Row
                   {
                       brand_id = g.Key.i_brand_id,
                       brand = g.Key.i_brand,
                       ext_price = ext_price
                   })
                  .Take(100);

        return q55;
    }

    public static IQueryable<Q64Row> Q64()
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

        IReadOnlyList<string> colors = [ "purple", "burlywood", "indian", "spring", "floral", "medium" ];

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
                             cnt = g.LongCount(),
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
                  select new Q64Row
                  {
                      product_name = cs1.product_name,
                      store_name = cs1.store_name,
                      store_zip = cs1.store_zip,
                      b_street_number = cs1.b_street_number,
                      b_street_name = cs1.b_street_name,
                      b_city = cs1.b_city,
                      b_zip = cs1.b_zip,
                      c_street_number = cs1.c_street_number,
                      c_street_name = cs1.c_street_name,
                      c_city = cs1.c_city,
                      c_zip = cs1.c_zip,
                      cs1syear = cs1.syear,
                      cs1cnt = cs1.cnt,
                      s11 = cs1.s1,
                      s21 = cs1.s2,
                      s31 = cs1.s3,
                      s12 = cs2.s1,
                      s22 = cs2.s2,
                      s32 = cs2.s3,
                      syear = cs2.syear,
                      cnt = cs2.cnt
                  };

        return q64;
    }

    public static IQueryable<Q67Row> Q67()
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

        var dw1 = joined
            .GroupBy(x => new
            {
                x.i_category,
                x.i_class,
                x.i_brand,
                x.i_product_name,
                x.d_year,
                x.d_qoy,
                x.d_moy,
                x.s_store_id
            })
            .RollUp(
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
                  .Take(100)
                  .Select(x => new Q67Row
                  {
                      i_category = x.i_category,
                      i_class = x.i_class,
                      i_brand = x.i_brand,
                      i_product_name = x.i_product_name,
                      d_year = x.d_year,
                      d_qoy = x.d_qoy,
                      d_moy = x.d_moy,
                      s_store_id = x.s_store_id,
                      sumsales = x.sumsales,
                      rk = x.rk
                  });

        return q67;
    }

    public static IQueryable<Q72Row> Q72()
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
                   group p by new { i.i_item_desc, inv.w_warehouse_name, d1.d_week_seq } into g
                   orderby g.LongCount() descending, g.Key.i_item_desc, g.Key.w_warehouse_name, g.Key.d_week_seq
                   select new Q72Row
                   {
                       i_item_desc = g.Key.i_item_desc,
                       w_warehouse_name = g.Key.w_warehouse_name,
                       d_week_seq = g.Key.d_week_seq,
                       no_promo = g.Sum(x => x == null ? 1L : 0L),
                       promo = g.Sum(x => x != null ? 1L : 0L),
                       total_cnt = g.LongCount()
                   }).Take(100);

        return q72;
    }

    public static IQueryable<Q86Row> Q86()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();

        var filteredDates = dateDimQuery.Where(d => d.d_month_seq >= 1200 && d.d_month_seq <= 1211);

        var baseSales = from ws in webSalesQuery
                        join d in filteredDates on ws.ws_sold_date_sk equals d.d_date_sk
                        join i in itemQuery on ws.ws_item_sk equals i.i_item_sk
                        select new
                        {
                            i.i_category,
                            i.i_class,
                            ws.ws_net_paid
                        };

        var rollup = baseSales
            .GroupBy(x => new { x.i_category, x.i_class })
            .RollUp(
                (key, g) => new
                {
                    total_sum = g.Sum(x => x.ws_net_paid),
                    key.i_category,
                    key.i_class,
                    t_class = key.i_class.Grouping(),
                    lochierarchy = key.i_category.Grouping() + key.i_class.Grouping()
                }
            );

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
                           res.rank_within_parent,
                           res.i_class // tie-breaker for equal ranks, as in the DuckDB SQL
                   select new Q86Row
                   {
                       total_sum = res.total_sum,
                       i_category = res.i_category,
                       i_class = res.i_class,
                       lochierarchy = res.lochierarchy,
                       rank_within_parent = res.rank_within_parent
                   }).Take(100);

        return q86;
    }

    public static IQueryable<Q87Row> Q87()
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

        var coolCust = storeStream.Except(catalogStream).Except(webStream)
            .Select(r => new Q87Row { c_last_name = r.c_last_name, c_first_name = r.c_first_name, d_date = r.d_date });

        return coolCust;
    }

    public static IQueryable<Q94Row> Q94()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var webSiteQuery = TpcdsData.WebSiteQuery();

        var startDate = new DateOnly(1999, 2, 1);
        var endDate = new DateOnly(1999, 4, 2);

        var baseSales = (from ws1 in webSalesQuery
                         join d in dateDimQuery on ws1.ws_ship_date_sk equals d.d_date_sk
                         join ca in customerAddressQuery on ws1.ws_ship_addr_sk equals ca.ca_address_sk
                         join web in webSiteQuery on ws1.ws_web_site_sk equals web.web_site_sk
                         where d.d_date >= startDate && d.d_date <= endDate
                            && ca.ca_state == "IL"
                            && web.web_company_name == "pri"
                            // EXISTS (another row of the order from a different warehouse)
                            // SQL <> is unknown when either warehouse is NULL
                            && webSalesQuery.Any(ws2 => ws2.ws_order_number == ws1.ws_order_number
                                                        && ws1.ws_warehouse_sk != null
                                                        && ws2.ws_warehouse_sk != null
                                                        && ws2.ws_warehouse_sk != ws1.ws_warehouse_sk)
                            // NOT EXISTS (a return of the order)
                            && !webReturnsQuery.Any(wr => wr.wr_order_number == ws1.ws_order_number)
                         select new
                         {
                             ws1.ws_order_number,
                             ws1.ws_ext_ship_cost,
                             ws1.ws_net_profit
                         });

        var q94 = (from x in baseSales
                   group x by 1 into g
                   select new Q94Row
                   {
                       order_count = g.Select(x => x.ws_order_number).Distinct().LongCount(),
                       total_shipping_cost = g.Sum(x => x.ws_ext_ship_cost),
                       total_net_profit = g.Sum(x => x.ws_net_profit)
                   })
                  .OrderBy(x => x.order_count)
                  .Take(100);

        return q94;
    }

    public static IQueryable<Q98Row> Q98()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var startDate = new DateOnly(1999, 2, 22);
        var endDate = new DateOnly(1999, 3, 24);
        var categories = new[] { "Sports", "Books", "Home" };

        var q98 = (from ss in storeSalesQuery
                   join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                   join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                   where categories.Contains(i.i_category)
                      && d.d_date >= startDate && d.d_date <= endDate
                   group ss by new
                   {
                       i.i_item_id,
                       i.i_item_desc,
                       i.i_category,
                       i.i_class,
                       i.i_current_price
                   } into g
                   // SQL sum() is NULL for a group without non-null values, while LINQ Sum() is 0.
                   let itemrevenue = g.Count(x => x.ss_ext_sales_price != null) > 0 ? g.Sum(x => x.ss_ext_sales_price) : null
                   select new Q98Row
                   {
                       i_item_id = g.Key.i_item_id,
                       i_item_desc = g.Key.i_item_desc,
                       i_category = g.Key.i_category,
                       i_class = g.Key.i_class,
                       i_current_price = g.Key.i_current_price,
                       itemrevenue = itemrevenue,
                       revenueratio = (double?)itemrevenue * 100.0 / (double?)itemrevenue.Sum().Over(g.Key.i_class)
                   })
                  .OrderBy(x => x.i_category)
                  .ThenBy(x => x.i_class)
                  .ThenBy(x => x.i_item_id)
                  .ThenBy(x => x.i_item_desc)
                  .ThenBy(x => x.revenueratio);

        return q98;
    }
}
