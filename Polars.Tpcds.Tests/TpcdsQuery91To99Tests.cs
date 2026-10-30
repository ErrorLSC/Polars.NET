using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Xunit;

namespace Polars.Tpcds.Tests;

public class TpcdsQuery91To99Tests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    [Fact]
    [Trait("TPCDS","Q91")]
    public void Test_Q91()
    {
        var callCenterQuery = TpcdsData.CallCenterQuery();
        var catalogReturnsQuery = TpcdsData.CatalogReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var householdDemographicsQuery = TpcdsData.HouseholdDemographicsQuery();

        var q91 = (from cr in catalogReturnsQuery
                   join cc in callCenterQuery on cr.cr_call_center_sk equals cc.cc_call_center_sk
                   join d in dateDimQuery on cr.cr_returned_date_sk equals d.d_date_sk
                   join c in customerQuery on cr.cr_returning_customer_sk equals c.c_customer_sk
                   join cd in customerDemographicsQuery on c.c_current_cdemo_sk equals cd.cd_demo_sk
                   join hd in householdDemographicsQuery on c.c_current_hdemo_sk equals hd.hd_demo_sk
                   join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                   where d.d_year == 1998
                      && d.d_moy == 11
                      && ((cd.cd_marital_status == "M" && cd.cd_education_status == "Unknown")
                          || (cd.cd_marital_status == "W" && cd.cd_education_status == "Advanced Degree"))
                      && hd.hd_buy_potential!.StartsWith("Unknown")
                      && ca.ca_gmt_offset == -7m
                   group cr by new
                   {
                       cc.cc_call_center_id,
                       cc.cc_name,
                       cc.cc_manager,
                       cd.cd_marital_status,
                       cd.cd_education_status
                   } into g
                   select new
                   {
                       Call_Center = g.Key.cc_call_center_id,
                       Call_Center_Name = g.Key.cc_name,
                       Manager = g.Key.cc_manager,
                       Returns_Loss = g.Sum(x => x.cr_net_loss)
                   })
                  .OrderByDescending(x => x.Returns_Loss)
                  .ThenBy(x => x.Call_Center);

        AssertPlan(q91, NativePushdown);

        var polarsResult = q91.ToList();

        var duckDbSql = $@"
SELECT
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
FROM
        read_parquet('{TpcdsData.CallCenterPath}') call_center,
        read_parquet('{TpcdsData.CatalogReturnsPath}') catalog_returns,
        read_parquet('{TpcdsData.DateDimPath}') date_dim,
        read_parquet('{TpcdsData.CustomerPath}') customer,
        read_parquet('{TpcdsData.CustomerAddressPath}') customer_address,
        read_parquet('{TpcdsData.CustomerDemographicsPath}') customer_demographics,
        read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics
WHERE
        cr_call_center_sk       = cc_call_center_sk
and     cr_returned_date_sk     = d_date_sk
and     cr_returning_customer_sk= c_customer_sk
and     cd_demo_sk              = c_current_cdemo_sk
and     hd_demo_sk              = c_current_hdemo_sk
and     ca_address_sk           = c_current_addr_sk
and     d_year                  = 1998
and     d_moy                   = 11
and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
and     hd_buy_potential like 'Unknown%'
and     ca_gmt_offset           = -7
GROUP BY cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
ORDER BY sum(cr_net_loss) DESC, cc_call_center_id NULLS FIRST;
";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            Call_Center = r.IsDBNull(0) ? null : r.GetString(0),
            Call_Center_Name = r.IsDBNull(1) ? null : r.GetString(1),
            Manager = r.IsDBNull(2) ? null : r.GetString(2),
            Returns_Loss = r.IsDBNull(3) ? (decimal?)null : r.GetDecimal(3)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].Call_Center, polarsResult[i].Call_Center);
            Assert.Equal(duckDbResult[i].Call_Center_Name, polarsResult[i].Call_Center_Name);
            Assert.Equal(duckDbResult[i].Manager, polarsResult[i].Manager);
            Assert.Equal(duckDbResult[i].Returns_Loss ?? 0m, polarsResult[i].Returns_Loss ?? 0m);
        }
    }

    [Fact]
    [Trait("TPCDS","Q92")]
    public void Test_Q92()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var startDate = new DateOnly(2000, 1, 27);
        var endDate = new DateOnly(2000, 4, 26);

        var itemAvgDiscount = from ws in webSalesQuery
                              join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                              where d.d_date >= startDate && d.d_date <= endDate
                              group ws by ws.ws_item_sk into g
                              select new
                              {
                                  ws_item_sk = g.Key,
                                  threshold = (decimal?)(g.Average(x => (double?)x.ws_ext_discount_amt) * 1.3)
                              };

        var q92 = (from ws in webSalesQuery
                   join i in itemQuery on ws.ws_item_sk equals i.i_item_sk
                   join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                   join avg in itemAvgDiscount on i.i_item_sk equals avg.ws_item_sk
                   where i.i_manufact_id == 350
                      && d.d_date >= startDate && d.d_date <= endDate
                      && ws.ws_ext_discount_amt > avg.threshold
                   group ws by 1 into g
                   select new
                   {
                       excess_discount_amount = g.Sum(x => x.ws_ext_discount_amt)
                   })
                  .OrderBy(x => x.excess_discount_amount)
                  .Take(100);

        AssertPlan(q92, NativePushdown);

        var polarsResult = q92.ToList();

        var duckDbSql = $@"
SELECT
   sum(ws_ext_discount_amt)  as ""Excess Discount Amount""
FROM
    read_parquet('{TpcdsData.WebSalesPath}') web_sales
   ,read_parquet('{TpcdsData.ItemPath}') item
   ,read_parquet('{TpcdsData.DateDimPath}') date_dim
WHERE
i_manufact_id = 350
and i_item_sk = ws_item_sk
and d_date between '2000-01-27' and cast('2000-04-26' as date)
and d_date_sk = ws_sold_date_sk
and ws_ext_discount_amt
     > (
         SELECT
            1.3 * avg(ws_ext_discount_amt)
         FROM
            read_parquet('{TpcdsData.WebSalesPath}') web_sales
           ,read_parquet('{TpcdsData.DateDimPath}') date_dim
         WHERE
              ws_item_sk = i_item_sk
          and d_date between '2000-01-27' and cast('2000-04-26' as date)
          and d_date_sk = ws_sold_date_sk
      )
order by sum(ws_ext_discount_amt)
limit 100;
";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            excess_discount_amount = r.IsDBNull(0) ? (decimal?)null : r.GetDecimal(0)
        });

        decimal expectedSum = duckDbResult.FirstOrDefault()?.excess_discount_amount ?? 0m;
        decimal actualSum = polarsResult.FirstOrDefault()?.excess_discount_amount ?? 0m;
        Assert.Equal(expectedSum, actualSum);
    }

    [Fact]
    [Trait("TPCDS","Q93")]
    public void Test_Q93()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeReturnsQuery = TpcdsData.StoreReturnsQuery();
        var reasonQuery = TpcdsData.ReasonQuery();

        var q93 = (from ss in storeSalesQuery
                   join sr in storeReturnsQuery
                     on new { item_sk = ss.ss_item_sk, ticket = ss.ss_ticket_number }
                     equals new { item_sk = sr.sr_item_sk, ticket = sr.sr_ticket_number }
                     into srGroup
                   from sr in srGroup.DefaultIfEmpty()
                   join r in reasonQuery on sr.sr_reason_sk equals r.r_reason_sk
                   where r.r_reason_desc == "reason 28"
                   group new { ss, sr } by ss.ss_customer_sk into g
                   let sumsales = g.Sum(x => x.sr.sr_return_quantity != null
                       ? (decimal?)(x.ss.ss_quantity - x.sr.sr_return_quantity) * x.ss.ss_sales_price
                       : (decimal?)x.ss.ss_quantity * x.ss.ss_sales_price)
                   orderby sumsales, g.Key
                   select new
                   {
                       ss_customer_sk = g.Key,
                       sumsales
                   })
                  .Take(100);

        AssertPlan(q93, NativePushdown);

        var polarsResult = q93.ToList();

        var duckDbSql = $@"
SELECT ss_customer_sk
      ,sum(act_sales) sumsales
FROM (SELECT ss_item_sk
            ,ss_ticket_number
            ,ss_customer_sk
            ,CASE WHEN sr_return_quantity IS NOT NULL THEN (ss_quantity-sr_return_quantity)*ss_sales_price
                                                      ELSE (ss_quantity*ss_sales_price) END act_sales
      FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales
           LEFT OUTER JOIN read_parquet('{TpcdsData.StoreReturnsPath}') store_returns ON (sr_item_sk = ss_item_sk
                                                     AND sr_ticket_number = ss_ticket_number)
          ,read_parquet('{TpcdsData.ReasonPath}') reason
      WHERE sr_reason_sk = r_reason_sk
        AND r_reason_desc = 'reason 28') t
GROUP BY ss_customer_sk
ORDER BY sumsales NULLS FIRST, ss_customer_sk NULLS FIRST
LIMIT 100;
";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            ss_customer_sk = r.IsDBNull(0) ? (long?)null : r.GetInt64(0),
            sumsales = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].ss_customer_sk, polarsResult[i].ss_customer_sk);
            Assert.Equal(duckDbResult[i].sumsales ?? 0m, polarsResult[i].sumsales ?? 0m);
        }
    }

    [Fact]
    [Trait("TPCDS","Q94")]
    public void Test_Q94()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var webSiteQuery = TpcdsData.WebSiteQuery();

        var startDate = new DateOnly(1999, 2, 1);
        var endDate = new DateOnly(1999, 4, 2);

        var multiWarehouseOrders = (from ws1 in webSalesQuery
                                    join ws2 in webSalesQuery on ws1.ws_order_number equals ws2.ws_order_number
                                    where ws1.ws_warehouse_sk != ws2.ws_warehouse_sk
                                    select ws1.ws_order_number).Distinct();

        var returnedOrders = webReturnsQuery.Select(wr => wr.wr_order_number);

        var baseSales = (from ws1 in webSalesQuery
                         join d in dateDimQuery on ws1.ws_ship_date_sk equals d.d_date_sk
                         join ca in customerAddressQuery on ws1.ws_ship_addr_sk equals ca.ca_address_sk
                         join web in webSiteQuery on ws1.ws_web_site_sk equals web.web_site_sk
                         join mwo in multiWarehouseOrders on ws1.ws_order_number equals mwo
                         where d.d_date >= startDate && d.d_date <= endDate
                            && ca.ca_state == "IL"
                            && web.web_company_name == "pri"
                         select new
                         {
                             ws1.ws_order_number,
                             ws1.ws_ext_ship_cost,
                             ws1.ws_net_profit
                         })
                        .ExceptBy(returnedOrders, ws => ws.ws_order_number);

        var q94 = (from x in baseSales
                   group x by 1 into g
                   select new
                   {
                       order_count = g.Select(x => x.ws_order_number).Distinct().LongCount(),
                       total_shipping_cost = g.Sum(x => x.ws_ext_ship_cost),
                       total_net_profit = g.Sum(x => x.ws_net_profit)
                   })
                  .OrderBy(x => x.order_count)
                  .Take(100);

        AssertPlan(q94, NativePushdown);

        var polarsResult = q94.ToList();

        var duckDbSql = $@"
SELECT
   count(distinct ws_order_number) as ""order count""
  ,sum(ws_ext_ship_cost) as ""total shipping cost""
  ,sum(ws_net_profit) as ""total net profit""
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

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            order_count = r.GetInt64(0),
            total_shipping_cost = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1),
            total_net_profit = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2)
        });

        var expected = duckDbResult.FirstOrDefault();
        var actual = polarsResult.FirstOrDefault();
        Assert.Equal(expected?.order_count ?? 0L, actual?.order_count ?? 0L);
        Assert.Equal(expected?.total_shipping_cost ?? 0m, actual?.total_shipping_cost ?? 0m);
        Assert.Equal(expected?.total_net_profit ?? 0m, actual?.total_net_profit ?? 0m);
    }

    [Fact]
    [Trait("TPCDS","Q95")]
    public void Test_Q95()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var webReturnsQuery = TpcdsData.WebReturnsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var webSiteQuery = TpcdsData.WebSiteQuery();

        var startDate = new DateOnly(1999, 2, 1);
        var endDate = new DateOnly(1999, 4, 2);

        var multiWarehouseOrders = (from ws1 in webSalesQuery
                                    join ws2 in webSalesQuery on ws1.ws_order_number equals ws2.ws_order_number
                                    where ws1.ws_warehouse_sk != ws2.ws_warehouse_sk
                                    select ws1.ws_order_number).Distinct();

        var returnedOrders = webReturnsQuery.Select(wr => wr.wr_order_number).Distinct();

        var q95 = (from ws1 in webSalesQuery
                   join d in dateDimQuery on ws1.ws_ship_date_sk equals d.d_date_sk
                   join ca in customerAddressQuery on ws1.ws_ship_addr_sk equals ca.ca_address_sk
                   join web in webSiteQuery on ws1.ws_web_site_sk equals web.web_site_sk
                   join mwo in multiWarehouseOrders on ws1.ws_order_number equals mwo
                   join ro in returnedOrders on ws1.ws_order_number equals ro
                   where d.d_date >= startDate && d.d_date <= endDate
                      && ca.ca_state == "IL"
                      && web.web_company_name == "pri"
                   group ws1 by 1 into g
                   select new
                   {
                       order_count = g.Select(x => x.ws_order_number).Distinct().LongCount(),
                       total_shipping_cost = g.Sum(x => x.ws_ext_ship_cost),
                       total_net_profit = g.Sum(x => x.ws_net_profit)
                   })
                  .OrderBy(x => x.order_count)
                  .Take(100);

        AssertPlan(q95, NativePushdown);

        var polarsResult = q95.ToList();

        var duckDbSql = $@"
WITH ws_wh AS
(SELECT ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 FROM read_parquet('{TpcdsData.WebSalesPath}') ws1,read_parquet('{TpcdsData.WebSalesPath}') ws2
 WHERE ws1.ws_order_number = ws2.ws_order_number
   AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
SELECT
   count(distinct ws_order_number) as ""order count""
  ,sum(ws_ext_ship_cost) as ""total shipping cost""
  ,sum(ws_net_profit) as ""total net profit""
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
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from read_parquet('{TpcdsData.WebReturnsPath}') web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
limit 100;
";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            order_count = r.GetInt64(0),
            total_shipping_cost = r.IsDBNull(1) ? (decimal?)null : r.GetDecimal(1),
            total_net_profit = r.IsDBNull(2) ? (decimal?)null : r.GetDecimal(2)
        });

        var expected = duckDbResult.FirstOrDefault();
        var actual = polarsResult.FirstOrDefault();
        Assert.Equal(expected?.order_count ?? 0L, actual?.order_count ?? 0L);
        Assert.Equal(expected?.total_shipping_cost ?? 0m, actual?.total_shipping_cost ?? 0m);
        Assert.Equal(expected?.total_net_profit ?? 0m, actual?.total_net_profit ?? 0m);
    }

    [Fact]
    [Trait("TPCDS","Q96")]
    public void Test_Q96()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var householdDemographicsQuery = TpcdsData.HouseholdDemographicsQuery();
        var timeDimQuery = TpcdsData.TimeDimQuery();
        var storeQuery = TpcdsData.StoreQuery();

        var q96 = from ss in storeSalesQuery
                  join hd in householdDemographicsQuery on ss.ss_hdemo_sk equals hd.hd_demo_sk
                  join t in timeDimQuery on ss.ss_sold_time_sk equals t.t_time_sk
                  join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                  where t.t_hour == 20
                     && t.t_minute >= 30
                     && hd.hd_dep_count == 7
                     && s.s_store_name == "ese"
                  select ss.ss_item_sk;

        AssertPlan(q96, NativePushdown);

        long polarsCount = q96.LongCount();

        var duckDbSql = $@"
SELECT count(*)
FROM read_parquet('{TpcdsData.StoreSalesPath}') store_sales
    ,read_parquet('{TpcdsData.HouseholdDemographicsPath}') household_demographics
    ,read_parquet('{TpcdsData.TimeDimPath}') time_dim
    ,read_parquet('{TpcdsData.StorePath}') store
WHERE ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 20
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 7
    and store.s_store_name = 'ese'
order by count(*)
limit 100;
";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => r.GetInt64(0));

        Assert.Equal(duckDbResult.Single(), polarsCount);
    }

    [Fact]
    [Trait("TPCDS","Q97")]
    public void Test_Q97()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var ssci = from ss in storeSalesQuery
                   join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                      && ss.ss_customer_sk != null
                   group ss by new
                   {
                       customer_sk = ss.ss_customer_sk,
                       item_sk = ss.ss_item_sk
                   } into g
                   select new
                   {
                       g.Key.customer_sk,
                       g.Key.item_sk,
                       in_store = 1L,
                       in_cat = 0L
                   };

        var csci = from cs in catalogSalesQuery
                   join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                   where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                      && cs.cs_bill_customer_sk != null
                   group cs by new
                   {
                       customer_sk = cs.cs_bill_customer_sk,
                       item_sk = cs.cs_item_sk
                   } into g
                   select new
                   {
                       g.Key.customer_sk,
                       g.Key.item_sk,
                       in_store = 0L,
                       in_cat = 1L
                   };

        var merged = from x in ssci.Concat(csci)
                     group x by new { x.customer_sk, x.item_sk } into g
                     select new
                     {
                         in_store = g.Max(x => x.in_store),
                         in_cat = g.Max(x => x.in_cat)
                     };

        var q97 = (from x in merged
                   group x by 1 into g
                   select new
                   {
                       store_only = g.Sum(x => x.in_store == 1L && x.in_cat == 0L ? 1L : 0L),
                       catalog_only = g.Sum(x => x.in_store == 0L && x.in_cat == 1L ? 1L : 0L),
                       store_and_catalog = g.Sum(x => x.in_store == 1L && x.in_cat == 1L ? 1L : 0L)
                   })
                  .Take(100);

        AssertPlan(q97, NativePushdown);

        var polarsResult = q97.ToList();

        var duckDbSql = $@"
WITH ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from read_parquet('{TpcdsData.StoreSalesPath}') store_sales,read_parquet('{TpcdsData.DateDimPath}') date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1200 and 1200 + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales,read_parquet('{TpcdsData.DateDimPath}') date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1200 and 1200 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;
";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            store_only = r.IsDBNull(0) ? 0L : (long)r.GetFieldValue<BigInteger>(0),
            catalog_only = r.IsDBNull(1) ? 0L : (long)r.GetFieldValue<BigInteger>(1),
            store_and_catalog = r.IsDBNull(2) ? 0L : (long)r.GetFieldValue<BigInteger>(2)
        });

        var expected = duckDbResult.FirstOrDefault();
        var actual = polarsResult.FirstOrDefault();
        Assert.Equal(expected?.store_only ?? 0L, actual?.store_only ?? 0L);
        Assert.Equal(expected?.catalog_only ?? 0L, actual?.catalog_only ?? 0L);
        Assert.Equal(expected?.store_and_catalog ?? 0L, actual?.store_and_catalog ?? 0L);
    }

    [Fact]
    [Trait("TPCDS","Q98")]
    public void Test_Q98()
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
                   select new
                   {
                       g.Key.i_item_id,
                       g.Key.i_item_desc,
                       g.Key.i_category,
                       g.Key.i_class,
                       g.Key.i_current_price,
                       itemrevenue = g.Sum(x => x.ss_ext_sales_price),
                       revenueratio = (double)g.Sum(x => x.ss_ext_sales_price)! * 100.0 / (double)g.Sum(x => x.ss_ext_sales_price).Sum().Over(g.Key.i_class)!
                   })
                  .OrderBy(x => x.i_category)
                  .ThenBy(x => x.i_class)
                  .ThenBy(x => x.i_item_id)
                  .ThenBy(x => x.i_item_desc)
                  .ThenBy(x => x.revenueratio);

        AssertPlan(q98, NativePushdown);

        var polarsResult = q98.ToList();

        var duckDbSql = $@"
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

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            i_item_id = r.IsDBNull(0) ? null : r.GetString(0),
            i_item_desc = r.IsDBNull(1) ? null : r.GetString(1),
            i_category = r.IsDBNull(2) ? null : r.GetString(2),
            i_class = r.IsDBNull(3) ? null : r.GetString(3),
            i_current_price = r.IsDBNull(4) ? (decimal?)null : r.GetDecimal(4),
            itemrevenue = r.IsDBNull(5) ? (decimal?)null : r.GetDecimal(5),
            revenueratio = r.IsDBNull(6) ? (double?)null : (double)r.GetDecimal(6)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].i_item_id, polarsResult[i].i_item_id);
            Assert.Equal(duckDbResult[i].i_item_desc, polarsResult[i].i_item_desc);
            Assert.Equal(duckDbResult[i].i_category, polarsResult[i].i_category);
            Assert.Equal(duckDbResult[i].i_class, polarsResult[i].i_class);
            Assert.Equal(duckDbResult[i].i_current_price, polarsResult[i].i_current_price);
            Assert.Equal(duckDbResult[i].itemrevenue, polarsResult[i].itemrevenue);
            Assert.Equal(duckDbResult[i].revenueratio ?? 0.0, polarsResult[i].revenueratio, 4);
        }
    }

    [Fact]
    [Trait("TPCDS","Q99")]
    public void Test_Q99()
    {
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var warehouseQuery = TpcdsData.WarehouseQuery();
        var shipModeQuery = TpcdsData.ShipModeQuery();
        var callCenterQuery = TpcdsData.CallCenterQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var joined = from cs in catalogSalesQuery
                     join d in dateDimQuery on cs.cs_ship_date_sk equals d.d_date_sk
                     join w in warehouseQuery on cs.cs_warehouse_sk equals w.w_warehouse_sk
                     join sm in shipModeQuery on cs.cs_ship_mode_sk equals sm.sm_ship_mode_sk
                     join cc in callCenterQuery on cs.cs_call_center_sk equals cc.cc_call_center_sk
                     where d.d_month_seq >= 1200 && d.d_month_seq <= 1211
                     select new
                     {
                         w_substr = w.w_warehouse_name!.Substring(0, 20),
                         sm.sm_type,
                         cc.cc_name,
                         diff = cs.cs_ship_date_sk - cs.cs_sold_date_sk
                     };

        var q99 = (from x in joined
                   group x by new { x.w_substr, x.sm_type, x.cc_name } into g
                   select new
                   {
                       g.Key.w_substr,
                       g.Key.sm_type,
                       cc_name_lower = g.Key.cc_name!.ToLower(),
                       d30 = g.Sum(x => x.diff <= 30 ? 1L : 0L),
                       d60 = g.Sum(x => x.diff > 30 && x.diff <= 60 ? 1L : 0L),
                       d90 = g.Sum(x => x.diff > 60 && x.diff <= 90 ? 1L : 0L),
                       d120 = g.Sum(x => x.diff > 90 && x.diff <= 120 ? 1L : 0L),
                       dmore = g.Sum(x => x.diff > 120 ? 1L : 0L)
                   })
                  .OrderBy(x => x.w_substr)
                  .ThenBy(x => x.sm_type)
                  .ThenBy(x => x.cc_name_lower)
                  .Take(100);

        AssertPlan(q99, NativePushdown);

        var polarsResult = q99.ToList();

        var duckDbSql = $@"
SELECT
   w_substr
  ,sm_type
  ,LOWER(cc_name) cc_name_lower
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as ""30 days""
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as ""31-60 days""
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as ""61-90 days""
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as ""91-120 days""
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as "">120 days""
FROM
   read_parquet('{TpcdsData.CatalogSalesPath}') catalog_sales
  ,(SELECT SUBSTRING(w_warehouse_name,1,20) w_substr, * FROM read_parquet('{TpcdsData.WarehousePath}') warehouse) AS sq1
  ,read_parquet('{TpcdsData.ShipModePath}') ship_mode
  ,read_parquet('{TpcdsData.CallCenterPath}') call_center
  ,read_parquet('{TpcdsData.DateDimPath}') date_dim
WHERE
    d_month_seq between 1200 and 1200 + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   w_substr
  ,sm_type
  ,cc_name
order by w_substr NULLS FIRST
        ,sm_type NULLS FIRST
        ,cc_name_lower NULLS FIRST
limit 100;
";

        var duckDbResult = DuckDbFixture.Query(duckDbSql, r => new
        {
            w_substr = r.IsDBNull(0) ? null : r.GetString(0),
            sm_type = r.IsDBNull(1) ? null : r.GetString(1),
            cc_name_lower = r.IsDBNull(2) ? null : r.GetString(2),
            d30 = r.IsDBNull(3) ? 0L : (long)r.GetFieldValue<BigInteger>(3),
            d60 = r.IsDBNull(4) ? 0L : (long)r.GetFieldValue<BigInteger>(4),
            d90 = r.IsDBNull(5) ? 0L : (long)r.GetFieldValue<BigInteger>(5),
            d120 = r.IsDBNull(6) ? 0L : (long)r.GetFieldValue<BigInteger>(6),
            dmore = r.IsDBNull(7) ? 0L : (long)r.GetFieldValue<BigInteger>(7)
        });

        Assert.Equal(duckDbResult.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResult.Count; i++)
        {
            Assert.Equal(duckDbResult[i].w_substr, polarsResult[i].w_substr);
            Assert.Equal(duckDbResult[i].sm_type, polarsResult[i].sm_type);
            Assert.Equal(duckDbResult[i].cc_name_lower, polarsResult[i].cc_name_lower);
            Assert.Equal(duckDbResult[i].d30, polarsResult[i].d30);
            Assert.Equal(duckDbResult[i].d60, polarsResult[i].d60);
            Assert.Equal(duckDbResult[i].d90, polarsResult[i].d90);
            Assert.Equal(duckDbResult[i].d120, polarsResult[i].d120);
            Assert.Equal(duckDbResult[i].dmore, polarsResult[i].dmore);
        }
    }
}
