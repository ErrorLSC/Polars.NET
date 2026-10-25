using System.Text.RegularExpressions;
using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.TPCH.Tests;

/// <summary>
/// TPC-DS Boss Rush: Testing advanced analytical patterns in standard LINQ.
/// </summary>
public class TpcdsBossRushTests
{
    private const string NativePushdown = "[Polars.NET Query Plan - 100% Native Pushdown]";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var explain = query.Explain();
        Assert.True(
            explain.Contains(expectedPlan),
            $"assertPlan failed! Expected to contain: '{expectedPlan}'\nActual plan:\n{explain}"
        );
    }

    [Fact, Trait("TPCDS", "BOSS1")]
    public void Boss_01_Multi_Dimensional_Rollup_Via_Concat()
    {
        // Equivalent to TPC-DS Style:
        // SELECT l_returnflag, l_linestatus, SUM(l_quantity) as total_qty
        // FROM lineitem
        // GROUP BY ROLLUP(l_returnflag, l_linestatus)
        //
        // In standard C# LINQ, expressed as 3-level grouping unioned together:
        // 1. Group by (ReturnFlag, LineStatus)
        // 2. Group by ReturnFlag (LineStatus = null)
        // 3. Grand total (ReturnFlag = null, LineStatus = null)

        using var lf1 = TpchData.LineItem();
        using var lf2 = TpchData.LineItem();
        using var lf3 = TpchData.LineItem();

        // Level 1: Sub-totals by (returnflag, linestatus)
        var level1 = from l in lf1.AsQueryable<LineItem>()
                     group l by new { l.l_returnflag, l.l_linestatus } into g
                     select new
                     {
                         ReturnFlag = g.Key.l_returnflag,
                         LineStatus = g.Key.l_linestatus,
                         TotalQty = g.Sum(x => x.l_quantity)
                     };

        // Level 2: Sub-totals by returnflag
        var level2 = from l in lf2.AsQueryable<LineItem>()
                     group l by l.l_returnflag into g
                     select new
                     {
                         ReturnFlag = g.Key,
                         LineStatus = (string?)null,
                         TotalQty = g.Sum(x => x.l_quantity)
                     };

        // Level 3: Grand total
        var level3 = from l in lf3.AsQueryable<LineItem>()
                     group l by 1 into g
                     select new
                     {
                         ReturnFlag = (string?)null,
                         LineStatus = (string?)null,
                         TotalQty = g.Sum(x => x.l_quantity)
                     };

        var rollupQuery = level1.Concat(level2).Concat(level3);

        AssertPlan(rollupQuery, NativePushdown);

        var results = rollupQuery.ToList();
        Assert.NotEmpty(results);

        // Verify grand total equals sum of level 2 totals
        var grandTotal = results.Single(r => r.ReturnFlag == null && r.LineStatus == null).TotalQty;
        var sumLevel2 = results.Where(r => r.ReturnFlag != null && r.LineStatus == null).Sum(r => r.TotalQty);
        Assert.Equal(grandTotal, sumLevel2);
    }

    [Fact, Trait("TPCDS", "BOSS2")]
    public void Boss_02_Top_N_Per_Group_Slicing()
    {
        // Equivalent to TPC-DS Style:
        // Top 3 highest priced lineitems for each linestatus
        // SELECT * FROM (
        //   SELECT l_linestatus, l_extendedprice, l_quantity,
        //          ROW_NUMBER() OVER (PARTITION BY l_linestatus ORDER BY l_extendedprice DESC) as rn
        //   FROM lineitem
        // ) WHERE rn <= 3

        using var lf = TpchData.LineItem();

        var query = from l in lf.AsQueryable<LineItem>()
                    group l by l.l_linestatus into g
                    from top in g.OrderByDescending(x => x.l_extendedprice).Take(3)
                    select new
                    {
                        LineStatus = g.Key,
                        ExtendedPrice = top.l_extendedprice,
                        Quantity = top.l_quantity
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();
        Assert.NotEmpty(results);

        // Group by linestatus and ensure each group has at most 3 items
        var grouped = results.GroupBy(r => r.LineStatus).ToList();
        Assert.All(grouped, grp => Assert.True(grp.Count() <= 3));
    }

    [Fact, Trait("TPCDS", "BOSS3")]
    public void Boss_03_Deep_Snowflake_Joins_With_Disambiguation()
    {
        // Deep snowflake schema with 8 joins (LineItem -> Orders -> Customer -> Supplier -> Part -> CustNation -> SuppNation -> Region)
        // With duplicate tables (CustNation vs SuppNation both scan Nation, colliding column names like n_name, n_comment).
        using var lineItemLf = TpchData.LineItem();
        using var ordersLf = TpchData.Orders();
        using var customerLf = TpchData.Customer();
        using var supplierLf = TpchData.Supplier();
        using var partLf = TpchData.Part();
        using var custNationLf = TpchData.Nation();
        using var suppNationLf = TpchData.Nation();
        using var regionLf = TpchData.Region();

        var query = from l in lineItemLf.AsQueryable<LineItem>()
                    join o in ordersLf.AsQueryable<Orders>() on l.l_orderkey equals o.o_orderkey
                    join c in customerLf.AsQueryable<Customer>() on o.o_custkey equals c.c_custkey
                    join s in supplierLf.AsQueryable<Supplier>() on l.l_suppkey equals s.s_suppkey
                    join p in partLf.AsQueryable<Part>() on l.l_partkey equals p.p_partkey
                    join cn in custNationLf.AsQueryable<Nation>() on c.c_nationkey equals cn.n_nationkey
                    join sn in suppNationLf.AsQueryable<Nation>() on s.s_nationkey equals sn.n_nationkey
                    join r in regionLf.AsQueryable<Region>() on cn.n_regionkey equals r.r_regionkey
                    where cn.n_name != sn.n_name
                    select new
                    {
                        OrderKey = l.l_orderkey,
                        CustomerName = c.c_name,
                        SupplierName = s.s_name,
                        CustomerNation = cn.n_name,
                        SupplierNation = sn.n_name,
                        RegionName = r.r_name,
                        PartName = p.p_name
                    };

        AssertPlan(query, NativePushdown);

        var results = query.Take(10).ToList();
        Assert.NotEmpty(results);
        Assert.All(results, r => Assert.NotEqual(r.CustomerNation, r.SupplierNation));
    }

    [Fact, Trait("TPCDS", "BOSS4")]
    public void Boss_04_Correlated_Scalar_Subquery_And_Semi_Join()
    {
        // Equivalent to TPC-DS Query 32 / Query 92 / Query 10 pattern:
        // Complex decorrelation combining:
        // 1. Semi-Join via Any (EXISTS): Customer has open orders
        // 2. Anti-Join via !Any (NOT EXISTS): Customer has no urgent orders
        // 3. Semi-Join via Contains (IN subquery): Customer nation is in target regions
        // 4. Multi-key correlated scalar aggregate subquery with scaling factor in WHERE:
        //    Customer account balance > 1.2 * average account balance of customers in the same nation & segment
        // 5. Composite condition correlated scalar aggregate subquery (equi-key + inequality theta condition in WHERE):
        //    Customer account balance < 2.0 * average balance of peers in the same nation with balance >= current customer
        // 6. Snowflake Dimension Joins (Customer -> Nation -> Region)
        // 7. Correlated scalar aggregate subqueries in SELECT projection after joins:
        //    NationAvgBalance = average balance of all customers in the same nation
        //    NationMaxBalance = maximum balance of all customers in the same nation
        // All with 100% Native Pushdown.

        using var customerLf = TpchData.Customer();
        using var customerAggLf = TpchData.Customer();
        using var customerThetaLf = TpchData.Customer();
        using var customerProjLf = TpchData.Customer();
        using var customerProjLf2 = TpchData.Customer();
        using var ordersLf = TpchData.Orders();
        using var urgentOrdersLf = TpchData.Orders();
        using var filterNationLf = TpchData.Nation();
        using var nationLf = TpchData.Nation();
        using var regionLf = TpchData.Region();

        var query = from c in customerLf.AsQueryable<Customer>()
                    where ordersLf.AsQueryable<Orders>()
                        .Any(o => o.o_custkey == c.c_custkey && o.o_orderstatus == "O")
                    where !urgentOrdersLf.AsQueryable<Orders>()
                        .Any(o => o.o_custkey == c.c_custkey && o.o_orderpriority == "1-URGENT")
                    where filterNationLf.AsQueryable<Nation>()
                        .Where(fn => fn.n_regionkey >= 1)
                        .Select(fn => fn.n_nationkey)
                        .Contains(c.c_nationkey)
                    where c.c_acctbal > (from c2 in customerAggLf.AsQueryable<Customer>()
                                         where c2.c_nationkey == c.c_nationkey
                                            && c2.c_mktsegment == c.c_mktsegment
                                         select c2.c_acctbal).Average() * 1.2m
                    where c.c_acctbal < (from c5 in customerThetaLf.AsQueryable<Customer>()
                                         where c5.c_nationkey == c.c_nationkey
                                            && c5.c_acctbal >= c.c_acctbal
                                         select c5.c_acctbal).Average() * 2.0m
                    join n in nationLf.AsQueryable<Nation>() on c.c_nationkey equals n.n_nationkey
                    join r in regionLf.AsQueryable<Region>() on n.n_regionkey equals r.r_regionkey
                    select new
                    {
                        CustomerKey = c.c_custkey,
                        CustomerName = c.c_name,
                        NationName = n.n_name,
                        RegionName = r.r_name,
                        AccountBalance = c.c_acctbal,
                        NationAvgBalance = (from c3 in customerProjLf.AsQueryable<Customer>()
                                            where c3.c_nationkey == c.c_nationkey
                                            select c3.c_acctbal).Average(),
                        NationMaxBalance = (from c4 in customerProjLf2.AsQueryable<Customer>()
                                            where c4.c_nationkey == c.c_nationkey
                                            select c4.c_acctbal).Max()
                    };

        AssertPlan(query, NativePushdown);

        var results = query.Take(10).ToList();
        Assert.NotEmpty(results);
        Assert.All(results, r =>
        {
            Assert.True(r.CustomerKey > 0);
            Assert.False(string.IsNullOrWhiteSpace(r.CustomerName));
            Assert.False(string.IsNullOrWhiteSpace(r.NationName));
            Assert.False(string.IsNullOrWhiteSpace(r.RegionName));
            Assert.True(r.AccountBalance > 0m);
            Assert.True(r.NationAvgBalance > 0m);
            Assert.True(r.NationMaxBalance >= r.NationAvgBalance);
        });
    }

    [Fact, Trait("TPCDS", "BOSS5")]
    public void Boss_05_Diamond_Cte_Self_Join_And_Branch_Reunite()
    {
        // =========================================================================
        // Part A: Direct CTE Self-Join with Colliding Column Disambiguation
        // WITH base_cte AS (
        //     SELECT o_orderkey, o_custkey, o_totalprice, o_orderdate FROM orders
        // )
        // SELECT o1.o_custkey, o1.o_orderkey AS k1, o2.o_orderkey AS k2,
        //        (o1.o_totalprice - o2.o_totalprice) AS price_diff
        // FROM base_cte o1
        // JOIN base_cte o2 ON o1.o_custkey = o2.o_custkey
        // WHERE o1.o_orderkey != o2.o_orderkey AND o1.o_totalprice > o2.o_totalprice
        // =========================================================================
        using (var ordersSelfLf = TpchData.Orders())
        {
            var baseOrdersCte = from o in ordersSelfLf.AsQueryable<Orders>()
                                select new
                                {
                                    o.o_orderkey,
                                    o.o_custkey,
                                    o.o_totalprice,
                                    o.o_orderdate
                                };

            var selfJoinQuery = from o1 in baseOrdersCte
                                join o2 in baseOrdersCte on o1.o_custkey equals o2.o_custkey
                                where o1.o_orderkey != o2.o_orderkey && o1.o_totalprice > o2.o_totalprice
                                select new
                                {
                                    CustKey = o1.o_custkey,
                                    Order1 = o1.o_orderkey,
                                    Order2 = o2.o_orderkey,
                                    Price1 = o1.o_totalprice,
                                    Price2 = o2.o_totalprice,
                                    PriceDiff = o1.o_totalprice - o2.o_totalprice
                                };

            AssertPlan(selfJoinQuery, NativePushdown);

            var selfResults = selfJoinQuery.Take(10).ToList();
            Assert.NotEmpty(selfResults);
            Assert.All(selfResults, r =>
            {
                Assert.True(r.CustKey > 0);
                Assert.NotEqual(r.Order1, r.Order2);
                Assert.True(r.Price1 > r.Price2);
                Assert.Equal(r.PriceDiff, r.Price1 - r.Price2);
            });
        }

        // =========================================================================
        // Part B: Diamond Graph Reuniting with Aggregated Branch + Dimension Join
        // Equivalent to TPC-DS Style Diamond Graph / Shared CTE Reuniting:
        // WITH base_cte AS (
        //     SELECT o_custkey, o_orderstatus, SUM(o_totalprice) AS total_spent, COUNT(*) AS order_cnt
        //     FROM orders
        //     GROUP BY o_custkey, o_orderstatus
        // ),
        // high_value AS (
        //     SELECT * FROM base_cte WHERE total_spent > 1000.0
        // ),
        // status_stats AS (
        //     SELECT o_orderstatus, AVG(total_spent) AS avg_spent, MAX(total_spent) AS max_spent
        //     FROM base_cte
        //     GROUP BY o_orderstatus
        // )
        // SELECT c.c_custkey, c.c_name, h.o_orderstatus, h.total_spent, s.avg_spent, s.max_spent,
        //        (h.total_spent - s.avg_spent) AS diff_from_avg
        // FROM high_value h
        // JOIN status_stats s ON h.o_orderstatus = s.o_orderstatus
        // WHERE h.total_spent > s.avg_spent
        // JOIN customer c ON h.o_custkey = c.c_custkey
        // =========================================================================

        using var ordersLf = TpchData.Orders();
        using var customerLf = TpchData.Customer();

        // 1. Shared Base Query (Diamond Node)
        var baseCte = from o in ordersLf.AsQueryable<Orders>()
                      group o by new { o.o_custkey, o.o_orderstatus } into g
                      select new
                      {
                          CustKey = g.Key.o_custkey,
                          Status = g.Key.o_orderstatus,
                          TotalSpent = g.Sum(x => x.o_totalprice),
                          OrderCount = g.Count()
                      };

        // 2. Branch 1: Filtered detail branch
        var branch1 = from b in baseCte
                      where b.TotalSpent > 1000m
                      select b;

        // 3. Branch 2: Aggregate summary branch derived from same base CTE
        var branch2 = from b in baseCte
                      group b by b.Status into g
                      select new
                      {
                          Status = g.Key,
                          AvgSpent = g.Average(x => x.TotalSpent),
                          MaxSpent = g.Max(x => x.TotalSpent)
                      };

        // 4. Reunite (Diamond Join) + Snowflake Join with Customer dimension
        var query = from b1 in branch1
                    join b2 in branch2 on b1.Status equals b2.Status
                    where b1.TotalSpent > b2.AvgSpent
                    join c in customerLf.AsQueryable<Customer>() on b1.CustKey equals c.c_custkey
                    select new
                    {
                        CustomerKey = c.c_custkey,
                        CustomerName = c.c_name,
                        OrderStatus = b1.Status,
                        TotalSpent = b1.TotalSpent,
                        AvgSpent = b2.AvgSpent,
                        MaxSpent = b2.MaxSpent,
                        DiffFromAvg = b1.TotalSpent - b2.AvgSpent
                    };

        AssertPlan(query, NativePushdown);

        var results = query.Take(10).ToList();
        Assert.NotEmpty(results);
        Assert.All(results, r =>
        {
            Assert.True(r.CustomerKey > 0);
            Assert.False(string.IsNullOrWhiteSpace(r.CustomerName));
            Assert.False(string.IsNullOrWhiteSpace(r.OrderStatus));
            Assert.True(r.TotalSpent > r.AvgSpent);
            Assert.True(r.MaxSpent >= r.AvgSpent);
            Assert.True(Math.Abs(r.DiffFromAvg - (r.TotalSpent - r.AvgSpent)) < 0.01m);
        });
    }

    [Fact, Trait("TPCDS", "BOSS6")]
    public void Boss_06_Group_Partition_Slice_And_TopN()
    {
        // =========================================================================
        // BOSS 6: Group Partition Slicing & Top-N per Group
        //
        // Part A: Sliced Group Aggregation & List-Agg inside GroupBy.Select
        // Equivalent to TPC-DS / Analytical Group Slicing:
        // SELECT o_custkey,
        //        LIST(o_totalprice ORDER BY o_totalprice DESC LIMIT 2) AS top2_prices,
        //        SUM(o_totalprice ORDER BY o_totalprice DESC LIMIT 2) AS top2_sum,
        //        FIRST_VALUE(o_orderkey ORDER BY o_totalprice DESC) AS max_orderkey,
        //        MAX(o_totalprice) AS max_price,
        //        COUNT(*) AS total_count,
        //        SUM(o_totalprice) AS total_sum
        // FROM orders
        // GROUP BY o_custkey
        // HAVING COUNT(*) >= 2
        // =========================================================================
        using (var ordersLf = TpchData.Orders())
        {
            var groupSliceQuery = from o in ordersLf.AsQueryable<Orders>()
                                  group o by o.o_custkey into g
                                  where g.Count() >= 2
                                  select new
                                  {
                                      CustKey = g.Key,
                                      Top2Prices = g.OrderByDescending(x => x.o_totalprice).Take(2).Select(x => x.o_totalprice).ToList(),
                                      Top2Sum = g.OrderByDescending(x => x.o_totalprice).Take(2).Sum(x => x.o_totalprice),
                                      MaxOrderKey = g.OrderByDescending(x => x.o_totalprice).Select(x => x.o_orderkey).First(),
                                      MaxPrice = g.OrderByDescending(x => x.o_totalprice).First().o_totalprice,
                                      TotalCount = g.Count(),
                                      TotalSum = g.Sum(x => x.o_totalprice)
                                  };

            AssertPlan(groupSliceQuery, NativePushdown);

            var sliceResults = groupSliceQuery.Take(10).ToList();
            Assert.NotEmpty(sliceResults);
            Assert.All(sliceResults, r =>
            {
                Assert.True(r.CustKey > 0);
                Assert.True(r.TotalCount >= 2);
                Assert.Equal(2, r.Top2Prices.Count);
                Assert.True(r.Top2Prices[0] >= r.Top2Prices[1]);
                Assert.Equal(r.Top2Sum, r.Top2Prices[0] + r.Top2Prices[1]);
                Assert.Equal(r.MaxPrice, r.Top2Prices[0]);
                Assert.True(r.TotalSum >= r.Top2Sum);
                Assert.True(r.MaxOrderKey > 0);
            });
        }

        // =========================================================================
        // Part B: Flattened Group Partition Slice via SelectMany (Top-2 per Group)
        // Equivalent to SQL Window Ranking filter:
        // WITH ranked AS (
        //     SELECT o_custkey, o_orderkey, o_totalprice, o_orderstatus,
        //            ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) AS rn
        //     FROM orders
        // )
        // SELECT o_custkey, o_orderkey, o_totalprice, o_orderstatus
        // FROM ranked
        // WHERE rn <= 2
        // =========================================================================
        using var ordersFlatLf = TpchData.Orders();
        var flatTopNQuery = from g in ordersFlatLf.AsQueryable<Orders>().GroupBy(o => o.o_custkey)
                            from top in g.OrderByDescending(o => o.o_totalprice).Take(2)
                            select new
                            {
                                CustKey = g.Key,
                                OrderKey = top.o_orderkey,
                                TotalPrice = top.o_totalprice,
                                OrderStatus = top.o_orderstatus
                            };

        AssertPlan(flatTopNQuery, NativePushdown);

        var flatResults = flatTopNQuery.Take(10).ToList();
        Assert.NotEmpty(flatResults);
        Assert.All(flatResults, r =>
        {
            Assert.True(r.CustKey > 0);
            Assert.True(r.OrderKey > 0);
            Assert.True(r.TotalPrice > 0m);
            Assert.False(string.IsNullOrWhiteSpace(r.OrderStatus));
        });
    }

    [Fact, Trait("TPCDS", "BOSS7")]
    public void Boss_07_Period_Over_Period_Window_Shift_And_Diff()
    {
        // =========================================================================
        // BOSS 7: Inter-Row Period-over-Period Window Analytics (Lead / Lag / Diff / CumSum)
        // Equivalent to TPC-DS Time-Series Window Queries (e.g. Q77, Q80, Q94):
        // SELECT o_custkey, o_orderkey, o_orderdate, o_totalprice,
        //        LAG(o_totalprice, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS prev_price,
        //        LEAD(o_totalprice, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS next_price,
        //        DIFF(o_totalprice, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS price_diff,
        //        SUM(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS running_total,
        //        SUM(o_totalprice) OVER (PARTITION BY o_custkey) AS cust_total,
        //        (o_totalprice - AVG(o_totalprice) OVER (PARTITION BY o_custkey)) AS diff_from_cust_avg
        // FROM orders
        // =========================================================================
        using var ordersLf = TpchData.Orders();
        var windowQuery = from o in ordersLf.AsQueryable<Orders>()
                          select new
                          {
                              CustKey = o.o_custkey,
                              OrderKey = o.o_orderkey,
                              OrderDate = o.o_orderdate,
                              Price = o.o_totalprice,
                              PrevPrice = ((decimal?)o.o_totalprice).Shift(1).Over(o.o_custkey, o.o_orderdate),
                              NextPrice = ((decimal?)o.o_totalprice).Shift(-1).Over(o.o_custkey, o.o_orderdate),
                              DescNextPrice = ((decimal?)o.o_totalprice).Shift(-1).Over(o.o_custkey, o.o_orderdate, true),
                              PriceDiff = o.o_totalprice.Diff(1).Over(o.o_custkey, o.o_orderdate),
                              RunningTotal = o.o_totalprice.CumSum().Over(o.o_custkey, o.o_orderdate),
                              CustTotal = o.o_totalprice.Sum().Over(o.o_custkey),
                              DiffFromAvg = o.o_totalprice - (decimal)o.o_totalprice.Average().Over(o.o_custkey)
                          };

        AssertPlan(windowQuery, NativePushdown);

        var results = windowQuery.Take(20).ToList();
        Assert.NotEmpty(results);
        Assert.All(results, r =>
        {
            Assert.True(r.CustKey > 0);
            Assert.True(r.OrderKey > 0);
            Assert.True(r.Price > 0m);
            Assert.True(r.RunningTotal >= r.Price);
            Assert.True(r.CustTotal >= r.Price);

            // When previous price exists, Diff must match Price - PrevPrice
            if (r.PrevPrice.HasValue)
            {
                Assert.Equal(r.PriceDiff, r.Price - r.PrevPrice.Value);
                Assert.Equal(r.PrevPrice, r.DescNextPrice);
            }
        });
    }

    public record Boss8Trade(DateTime Time, string Ticker, double Price, long Volume);
    public record Boss8Quote(DateTime Time, string Ticker, double Bid, double Ask);

    [Fact, Trait("TPCDS", "BOSS8")]
    public void Boss_08_AsOf_Time_Series_Tolerance_Join()
    {
        // =========================================================================
        // BOSS 8: High-Frequency Financial Time-Series Join (AsOf Join with By and Tolerance)
        // Equivalent to kdb+ / q style asof join / Polars join_asof:
        // SELECT t.Time, t.Ticker, t.Price, t.Volume, q.Bid, q.Ask,
        //        (t.Price - q.Ask) AS Slippage
        // FROM trades t
        // ASOF JOIN quotes q
        //   ON t.Time = q.Time
        //   BY t.Ticker = q.Ticker
        //   TOLERANCE '2m'
        //   STRATEGY 'backward'
        // WHERE t.Price > 100.0
        // ORDER BY t.Time
        // =========================================================================

        var tradesData = new
        {
            Time = new[]
            {
                new DateTime(2024, 1, 1, 10, 0, 0),
                new DateTime(2024, 1, 1, 10, 1, 0),
                new DateTime(2024, 1, 1, 10, 2, 0),
                new DateTime(2024, 1, 1, 10, 5, 0),
                new DateTime(2024, 1, 1, 10, 6, 0)
            },
            Ticker = new[] { "AAPL", "MSFT", "AAPL", "AAPL", "MSFT" },
            Price = new[] { 150.25, 380.50, 151.30, 152.00, 382.10 },
            Volume = new long[] { 100, 300, 200, 150, 250 }
        };

        var quotesData = new
        {
            Time = new[]
            {
                new DateTime(2024, 1, 1, 9, 59, 0),
                new DateTime(2024, 1, 1, 10, 0, 30),
                new DateTime(2024, 1, 1, 10, 1, 0),
                new DateTime(2024, 1, 1, 10, 3, 0),
                new DateTime(2024, 1, 1, 10, 6, 0)
            },
            Ticker = new[] { "AAPL", "MSFT", "AAPL", "MSFT", "AAPL" },
            Bid = new[] { 150.00, 380.00, 151.00, 381.50, 152.50 },
            Ask = new[] { 150.50, 380.80, 151.50, 382.00, 153.00 }
        };

        using var dfTrades = DataFrame.FromColumns(tradesData);
        using var dfQuotes = DataFrame.FromColumns(quotesData);

        var asofQuery = dfTrades.AsQueryable<Boss8Trade>()
            .JoinAsOf(
                dfQuotes.AsQueryable<Boss8Quote>(),
                t => t.Time,
                q => q.Time,
                t => t.Ticker,
                q => q.Ticker,
                static (t, q) => new
                {
                    t.Time,
                    t.Ticker,
                    t.Price,
                    t.Volume,
                    QuoteBid = (double?)q.Bid,
                    QuoteAsk = (double?)q.Ask,
                    Slippage = t.Price - (double?)q.Ask
                },
                tolerance: TimeSpan.FromMinutes(2),
                strategy: AsofStrategy.Backward
            )
            .Where(x => x.Price > 100.0)
            .OrderBy(x => x.Time);

        AssertPlan(asofQuery, NativePushdown);

        var results = asofQuery.ToList();
        Assert.Equal(5, results.Count);

        // Row 0: AAPL 10:00 -> Quote AAPL 09:59 (diff 1m <= 2m) -> Matched
        Assert.Equal("AAPL", results[0].Ticker);
        Assert.Equal(150.25, results[0].Price);
        Assert.Equal(150.00, results[0].QuoteBid);
        Assert.Equal(150.50, results[0].QuoteAsk);
        Assert.True(Math.Abs(results[0].Slippage!.Value - (150.25 - 150.50)) < 1e-6);

        // Row 1: MSFT 10:01 -> Quote MSFT 10:00:30 (diff 30s <= 2m) -> Matched
        Assert.Equal("MSFT", results[1].Ticker);
        Assert.Equal(380.50, results[1].Price);
        Assert.Equal(380.00, results[1].QuoteBid);
        Assert.Equal(380.80, results[1].QuoteAsk);

        // Row 2: AAPL 10:02 -> Quote AAPL 10:01 (diff 1m <= 2m) -> Matched
        Assert.Equal("AAPL", results[2].Ticker);
        Assert.Equal(151.30, results[2].Price);
        Assert.Equal(151.00, results[2].QuoteBid);
        Assert.Equal(151.50, results[2].QuoteAsk);

        // Row 3: AAPL 10:05 -> Quote AAPL 10:01 (diff 4m > 2m) -> Unmatched (null)
        Assert.Equal("AAPL", results[3].Ticker);
        Assert.Equal(152.00, results[3].Price);
        Assert.Null(results[3].QuoteBid);
        Assert.Null(results[3].QuoteAsk);
        Assert.Null(results[3].Slippage);

        // Row 4: MSFT 10:06 -> Quote MSFT 10:03 (diff 3m > 2m) -> Unmatched (null)
        Assert.Equal("MSFT", results[4].Ticker);
        Assert.Equal(382.10, results[4].Price);
        Assert.Null(results[4].QuoteBid);
        Assert.Null(results[4].QuoteAsk);
        Assert.Null(results[4].Slippage);
    }

    [Fact, Trait("TPCDS", "BOSS10")]
    public void Boss_10_Cross_Tab_Pivot_Case_When_Matrix()
    {
        // =========================================================================
        // BOSS 10: Cross-Tab / Pivot CASE-WHEN Matrix
        // Equivalent to TPC-DS Cross-Tab / Dimension Pivot Queries (e.g. Q44, Q70, Q80, Q86):
        // SELECT l_returnflag,
        //        COUNT(*) AS TotalCount,
        //        SUM(l_quantity) AS TotalQty,
        //        SUM(l_extendedprice * (1 - l_discount)) AS TotalNetRevenue,
        //        COUNT(CASE WHEN l_linestatus = 'O' THEN 1 END) AS StatusO_Count,
        //        COUNT(CASE WHEN l_linestatus = 'F' THEN 1 END) AS StatusF_Count,
        //        SUM(CASE WHEN l_linestatus = 'O' THEN l_quantity ELSE 0 END) AS StatusO_Qty,
        //        SUM(CASE WHEN l_linestatus = 'F' THEN l_quantity ELSE 0 END) AS StatusF_Qty,
        //        SUM(CASE WHEN l_linestatus = 'O' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS StatusO_Revenue,
        //        SUM(CASE WHEN l_linestatus = 'F' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS StatusF_Revenue,
        //        SUM(CASE WHEN l_shipmode IN ('AIR', 'REG AIR') THEN l_quantity ELSE 0 END) AS Air_Qty,
        //        SUM(CASE WHEN l_shipmode = 'MAIL' THEN l_quantity ELSE 0 END) AS Mail_Qty,
        //        SUM(CASE WHEN l_shipmode = 'SHIP' THEN l_quantity ELSE 0 END) AS Ship_Qty,
        //        SUM(CASE WHEN l_shipmode = 'TRUCK' THEN l_quantity ELSE 0 END) AS Truck_Qty,
        //        SUM(CASE WHEN l_shipmode = 'RAIL' THEN l_quantity ELSE 0 END) AS Rail_Qty,
        //        SUM(CASE WHEN l_shipmode = 'FOB' THEN l_quantity ELSE 0 END) AS Fob_Qty,
        //        SUM(CASE WHEN l_discount > 0.05 THEN l_extendedprice ELSE 0 END) AS HighDiscount_Revenue,
        //        SUM(CASE WHEN l_linestatus = 'O' AND (l_shipmode = 'AIR' OR l_shipmode = 'REG AIR') THEN l_quantity ELSE 0 END) AS StatusO_Air_Qty,
        //        AVG(CASE WHEN l_linestatus = 'F' THEN l_discount END) AS StatusF_AvgDiscount,
        //        MAX(CASE WHEN l_linestatus = 'F' THEN l_extendedprice END) AS StatusF_MaxPrice
        // FROM lineitem
        // GROUP BY l_returnflag
        // ORDER BY l_returnflag
        // =========================================================================

        using var lineitemLf = TpchData.LineItem();

        var matrixQuery =
            from l in lineitemLf.AsQueryable<LineItem>()
            group l by l.l_returnflag into g
            orderby g.Key
            select new
            {
                ReturnFlag = g.Key,
                TotalCount = g.Count(),
                TotalQty = g.Sum(x => x.l_quantity),
                TotalNetRevenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount)),

                // Cross-Tab dimension 1: LineStatus ('O' vs 'F')
                StatusO_Count = g.Count(x => x.l_linestatus == "O"),
                StatusF_Count = g.Count(x => x.l_linestatus == "F"),
                StatusO_Qty = g.Where(x => x.l_linestatus == "O").Sum(x => x.l_quantity),
                StatusF_Qty = g.Where(x => x.l_linestatus == "F").Sum(x => x.l_quantity),
                StatusO_Revenue = g.Where(x => x.l_linestatus == "O").Sum(x => x.l_extendedprice * (1m - x.l_discount)),
                StatusF_Revenue = g.Where(x => x.l_linestatus == "F").Sum(x => x.l_extendedprice * (1m - x.l_discount)),

                // Cross-Tab dimension 2: ShipMode Matrix
                Air_Qty = g.Where(x => x.l_shipmode == "AIR" || x.l_shipmode == "REG AIR").Sum(x => x.l_quantity),
                Mail_Qty = g.Where(x => x.l_shipmode == "MAIL").Sum(x => x.l_quantity),
                Ship_Qty = g.Where(x => x.l_shipmode == "SHIP").Sum(x => x.l_quantity),
                Truck_Qty = g.Where(x => x.l_shipmode == "TRUCK").Sum(x => x.l_quantity),
                Rail_Qty = g.Where(x => x.l_shipmode == "RAIL").Sum(x => x.l_quantity),
                Fob_Qty = g.Where(x => x.l_shipmode == "FOB").Sum(x => x.l_quantity),

                // Ternary conditional aggregation
                HighDiscount_Revenue = g.Sum(x => x.l_discount > 0.05m ? x.l_extendedprice : 0m),

                // Composite multi-condition aggregation (LineStatus AND ShipMode)
                StatusO_Air_Qty = g.Where(x => x.l_linestatus == "O" && (x.l_shipmode == "AIR" || x.l_shipmode == "REG AIR")).Sum(x => x.l_quantity),

                // Conditional Statistical metrics
                StatusF_AvgDiscount = (double)g.Where(x => x.l_linestatus == "F").Average(x => x.l_discount),
                StatusF_MaxPrice = g.Where(x => x.l_linestatus == "F").Max(x => x.l_extendedprice),

                // Derived cross-column metric calculations
                Status_Diff_Qty = g.Where(x => x.l_linestatus == "O").Sum(x => x.l_quantity) - g.Where(x => x.l_linestatus == "F").Sum(x => x.l_quantity),
                StatusO_Qty_Ratio = (double)g.Where(x => x.l_linestatus == "O").Sum(x => x.l_quantity) / (double)g.Sum(x => x.l_quantity)
            };

        AssertPlan(matrixQuery, NativePushdown);

        var results = matrixQuery.ToList();
        Assert.NotEmpty(results);

        foreach (var r in results)
        {
            Assert.False(string.IsNullOrEmpty(r.ReturnFlag));
            Assert.True(r.TotalCount > 0);
            Assert.True(r.TotalQty > 0m);
            Assert.True(r.TotalNetRevenue > 0m);

            // Mathematical consistency invariants across the cross-tab matrix:
            // 1. LineStatus partitions partition the entire group
            Assert.Equal(r.TotalCount, r.StatusO_Count + r.StatusF_Count);
            Assert.Equal(r.TotalQty, r.StatusO_Qty + r.StatusF_Qty);
            Assert.Equal(r.TotalNetRevenue, r.StatusO_Revenue + r.StatusF_Revenue);

            // 2. ShipMode partition sums match TotalQty
            var shipModeSum = r.Air_Qty + r.Mail_Qty + r.Ship_Qty + r.Truck_Qty + r.Rail_Qty + r.Fob_Qty;
            Assert.Equal(r.TotalQty, shipModeSum);

            // 3. Composite subset invariant: StatusO_Air_Qty <= StatusO_Qty and <= Air_Qty
            Assert.True(r.StatusO_Air_Qty <= r.StatusO_Qty);
            Assert.True(r.StatusO_Air_Qty <= r.Air_Qty);

            // 4. Conditional stats
            Assert.True(r.StatusF_AvgDiscount >= 0.0 && r.StatusF_AvgDiscount <= 1.0);
            if (r.StatusF_Qty > 0m)
            {
                Assert.True(r.StatusF_MaxPrice > 0m);
            }

            // 5. Cross-column derived metrics
            Assert.Equal(r.Status_Diff_Qty, r.StatusO_Qty - r.StatusF_Qty);
            Assert.True(r.StatusO_Qty_Ratio >= 0.0 && r.StatusO_Qty_Ratio <= 1.0);
        }

        // =========================================================================
        // Verification 2: Composite Key Cross-Tab Matrix (Multi-Dimensional Pivot)
        // Group by (l_returnflag, l_linestatus) and pivot across ShipModes
        // =========================================================================
        using var lf2 = TpchData.LineItem();
        var multiKeyMatrixQuery =
            from l in lf2.AsQueryable<LineItem>()
            group l by new { l.l_returnflag, l.l_linestatus } into g
            orderby g.Key.l_returnflag, g.Key.l_linestatus
            select new
            {
                ReturnFlag = g.Key.l_returnflag,
                LineStatus = g.Key.l_linestatus,
                TotalQty = g.Sum(x => x.l_quantity),
                TotalCount = g.Count(),
                Air_Qty = g.Where(x => x.l_shipmode == "AIR" || x.l_shipmode == "REG AIR").Sum(x => x.l_quantity),
                Mail_Qty = g.Where(x => x.l_shipmode == "MAIL").Sum(x => x.l_quantity),
                Ship_Qty = g.Where(x => x.l_shipmode == "SHIP").Sum(x => x.l_quantity),
                Truck_Qty = g.Where(x => x.l_shipmode == "TRUCK").Sum(x => x.l_quantity),
                Rail_Qty = g.Where(x => x.l_shipmode == "RAIL").Sum(x => x.l_quantity),
                Fob_Qty = g.Where(x => x.l_shipmode == "FOB").Sum(x => x.l_quantity)
            };

        AssertPlan(multiKeyMatrixQuery, NativePushdown);

        var multiResults = multiKeyMatrixQuery.ToList();
        Assert.NotEmpty(multiResults);
        foreach (var r in multiResults)
        {
            Assert.False(string.IsNullOrEmpty(r.ReturnFlag));
            Assert.False(string.IsNullOrEmpty(r.LineStatus));
            Assert.True(r.TotalQty > 0m);
            Assert.True(r.TotalCount > 0);

            var shipSum = r.Air_Qty + r.Mail_Qty + r.Ship_Qty + r.Truck_Qty + r.Rail_Qty + r.Fob_Qty;
            Assert.Equal(r.TotalQty, shipSum);
        }
    }

    public record WideQuarterlySales(string Department, double Q1, double Q2, double Q3);

    [Fact, Trait("TPCDS", "SelectManyUnpivot")]
    public void Test_SelectMany_Unpivot_Execution_Plan()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Department", ["Electronics", "Clothing"]),
            Series.From("Q1", [100.0, 50.0]),
            Series.From("Q2", [120.0, 60.0]),
            Series.From("Q3", [140.0, 70.0])
        ]);

        var query =
            from d in df.AsQueryable<WideQuarterlySales>()
            from q in new[]
            {
                new { Quarter = "Q1", Sales = d.Q1 },
                new { Quarter = "Q2", Sales = d.Q2 },
                new { Quarter = "Q3", Sales = d.Q3 }
            }
            select new
            {
                d.Department,
                q.Quarter,
                q.Sales
            };

        var plan = query.Explain();
        AssertPlan(query, NativePushdown);
        Assert.Contains("UNPIVOT", plan.ToUpper());

        var results = query.ToList();
        Assert.Equal(6, results.Count);

        // Polars UNPIVOT concatenates unpivoted columns column-by-column:
        // First all Q1 rows, then all Q2 rows, then all Q3 rows:
        Assert.Equal("Electronics", results[0].Department);
        Assert.Equal("Q1", results[0].Quarter);
        Assert.Equal(100.0, results[0].Sales);

        Assert.Equal("Clothing", results[1].Department);
        Assert.Equal("Q1", results[1].Quarter);
        Assert.Equal(50.0, results[1].Sales);

        Assert.Equal("Electronics", results[2].Department);
        Assert.Equal("Q2", results[2].Quarter);
        Assert.Equal(120.0, results[2].Sales);

        Assert.Equal("Clothing", results[3].Department);
        Assert.Equal("Q2", results[3].Quarter);
        Assert.Equal(60.0, results[3].Sales);

        Assert.Equal("Electronics", results[4].Department);
        Assert.Equal("Q3", results[4].Quarter);
        Assert.Equal(140.0, results[4].Sales);

        Assert.Equal("Clothing", results[5].Department);
        Assert.Equal("Q3", results[5].Quarter);
        Assert.Equal(70.0, results[5].Sales);

        // =========================================================================
        // Pattern 2: ValueTuple syntax with renaming and downstream Filter + OrderBy
        // =========================================================================
        using var df2 = DataFrame.FromColumns(
        [
            Series.From("Department", ["Electronics", "Clothing"]),
            Series.From("Q1", [100.0, 50.0]),
            Series.From("Q2", [120.0, 60.0]),
            Series.From("Q3", [140.0, 70.0])
        ]);

        var tupleQuery =
            from d in df2.AsQueryable<WideQuarterlySales>()
            from q in new[]
            {
                Tuple.Create("Q1", d.Q1),
                Tuple.Create("Q2", d.Q2),
                Tuple.Create("Q3", d.Q3)
            }
            where q.Item2 > 65.0
            orderby q.Item2 descending
            select new
            {
                d.Department,
                Quarter = q.Item1,
                Sales = q.Item2
            };

        AssertPlan(tupleQuery, NativePushdown);
        var tuplePlan = tupleQuery.Explain();
        Assert.Contains("UNPIVOT", tuplePlan.ToUpper());

        var tupleResults = tupleQuery.ToList();
        // Matching rows with Sales > 65:
        // Electronics Q3 (140.0)
        // Electronics Q2 (120.0)
        // Electronics Q1 (100.0)
        // Clothing Q3 (70.0)
        Assert.Equal(4, tupleResults.Count);
        Assert.Equal("Electronics", tupleResults[0].Department);
        Assert.Equal("Q3", tupleResults[0].Quarter);
        Assert.Equal(140.0, tupleResults[0].Sales);

        Assert.Equal("Electronics", tupleResults[1].Department);
        Assert.Equal("Q2", tupleResults[1].Quarter);
        Assert.Equal(120.0, tupleResults[1].Sales);

        Assert.Equal("Electronics", tupleResults[2].Department);
        Assert.Equal("Q1", tupleResults[2].Quarter);
        Assert.Equal(100.0, tupleResults[2].Sales);

        Assert.Equal("Clothing", tupleResults[3].Department);
        Assert.Equal("Q3", tupleResults[3].Quarter);
        Assert.Equal(70.0, tupleResults[3].Sales);

        // =========================================================================
        // Pattern 3: Single-Argument SelectMany with Explicit Index Column
        // =========================================================================
        using var df3 = DataFrame.FromColumns(
        [
            Series.From("Department", ["Electronics", "Clothing"]),
            Series.From("Q1", [100.0, 50.0]),
            Series.From("Q2", [120.0, 60.0]),
            Series.From("Q3", [140.0, 70.0])
        ]);

        var directQuery = df3.AsQueryable<WideQuarterlySales>()
            .SelectMany(d => new[]
            {
                new { d.Department, Quarter = "Q1", Sales = d.Q1 },
                new { d.Department, Quarter = "Q2", Sales = d.Q2 },
                new { d.Department, Quarter = "Q3", Sales = d.Q3 }
            });

        AssertPlan(directQuery, NativePushdown);
        var directPlan = directQuery.Explain();
        Assert.Contains("UNPIVOT", directPlan.ToUpper());

        var directResults = directQuery.ToList();
        Assert.Equal(6, directResults.Count);

        // =========================================================================
        // Pattern 4: Multi-Column Composite Index Unpivot
        // Preserve both Department and Region while unpivoting Q1, Q2, Q3
        // =========================================================================
        using var df4 = DataFrame.FromColumns(
        [
            Series.From("Department", ["Electronics", "Clothing"]),
            Series.From("Region", ["North", "South"]),
            Series.From("Q1", [100.0, 50.0]),
            Series.From("Q2", [120.0, 60.0]),
            Series.From("Q3", [140.0, 70.0])
        ]);

        var multiIndexQuery =
            from d in df4.AsQueryable<MultiIndexSales>()
            from q in new[]
            {
                new { Quarter = "Q1", Sales = d.Q1 },
                new { Quarter = "Q2", Sales = d.Q2 },
                new { Quarter = "Q3", Sales = d.Q3 }
            }
            select new
            {
                d.Department,
                d.Region,
                q.Quarter,
                q.Sales
            };

        AssertPlan(multiIndexQuery, NativePushdown);
        var multiPlan = multiIndexQuery.Explain();
        Assert.Contains("UNPIVOT", multiPlan.ToUpper());

        var multiResults = multiIndexQuery.ToList();
        Assert.Equal(6, multiResults.Count);
        Assert.Equal("Electronics", multiResults[0].Department);
        Assert.Equal("North", multiResults[0].Region);
        Assert.Equal("Q1", multiResults[0].Quarter);
        Assert.Equal(100.0, multiResults[0].Sales);
    }

    public record MultiIndexSales(string Department, string Region, double Q1, double Q2, double Q3);
    public record EmployeeSalary(string Department, string Name, double Salary);

    [Fact, Trait("TPCDS", "BOSS9")]
    public void Boss_09_Window_Ranking_And_Analytical_Predicate_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "IT", "IT", "HR", "HR", "HR"]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]),
            Series.From("Salary", [100.0, 90.0, 80.0, 70.0, 60.0, 50.0])
        ]);

        var query =
            from e in df.AsQueryable<EmployeeSalary>()
            where e.Salary.Rank(true).Over(e.Department) <= 2.0
            select e;

        var plan = query.Explain();
        AssertPlan(query, NativePushdown);

        var results = query.ToList();
        Assert.Equal(4, results.Count);
        Assert.Contains(results, r => r.Name == "Alice" && r.Department == "IT");
        Assert.Contains(results, r => r.Name == "Bob" && r.Department == "IT");
        Assert.Contains(results, r => r.Name == "David" && r.Department == "HR");
        Assert.Contains(results, r => r.Name == "Eve" && r.Department == "HR");
        Assert.DoesNotContain(results, r => r.Name == "Charlie");
        Assert.DoesNotContain(results, r => r.Name == "Frank");

        // =========================================================================
        // Variant 2: Let clause (Roslyn Transparent Identifier) with rank projection
        // =========================================================================
        var queryWithLet =
            from e in df.AsQueryable<EmployeeSalary>()
            let rk = e.Salary.Rank(true).Over(e.Department)
            where rk <= 2.0
            orderby e.Department, rk
            select new
            {
                e.Department,
                e.Name,
                e.Salary,
                Rank = rk
            };

        var letPlan = queryWithLet.Explain();
        AssertPlan(queryWithLet, NativePushdown);

        var letResults = queryWithLet.ToList();
        Assert.Equal(4, letResults.Count);

        // HR Rank 1 (David 70.0)
        Assert.Equal("HR", letResults[0].Department);
        Assert.Equal("David", letResults[0].Name);
        Assert.Equal(1.0, letResults[0].Rank);

        // HR Rank 2 (Eve 60.0)
        Assert.Equal("HR", letResults[1].Department);
        Assert.Equal("Eve", letResults[1].Name);
        Assert.Equal(2.0, letResults[1].Rank);

        // IT Rank 1 (Alice 100.0)
        Assert.Equal("IT", letResults[2].Department);
        Assert.Equal("Alice", letResults[2].Name);
        Assert.Equal(1.0, letResults[2].Rank);

        // IT Rank 2 (Bob 90.0)
        Assert.Equal("IT", letResults[3].Department);
        Assert.Equal("Bob", letResults[3].Name);
        Assert.Equal(2.0, letResults[3].Rank);

        // =========================================================================
        // Variant 3: Dense Rank with tie handling (TPC-DS Q44/Q49 pattern)
        // =========================================================================
        using var dfTies = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "IT", "IT", "IT"]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Salary", [100.0, 100.0, 80.0, 70.0])
        ]);

        var denseQuery =
            from e in dfTies.AsQueryable<EmployeeSalary>()
            where e.Salary.Rank(RankMethod.Dense, true).Over(e.Department) <= 2.0
            orderby e.Salary descending
            select e;

        AssertPlan(denseQuery, NativePushdown);
        var denseResults = denseQuery.ToList();
        // Alice (100, dense rank 1), Bob (100, dense rank 1), Charlie (80, dense rank 2)
        Assert.Equal(3, denseResults.Count);
        Assert.Contains(denseResults, r => r.Name == "Alice");
        Assert.Contains(denseResults, r => r.Name == "Bob");
        Assert.Contains(denseResults, r => r.Name == "Charlie");
        Assert.DoesNotContain(denseResults, r => r.Name == "David");

        // =========================================================================
        // Variant 4: Multi-Column Composite Partitioning in Over clause
        // =========================================================================
        using var dfRoles = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "IT", "IT", "IT"]),
            Series.From("Role", ["Dev", "Dev", "QA", "QA"]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Salary", [100.0, 90.0, 80.0, 70.0])
        ]);

        var multiKeyQuery =
            from e in dfRoles.AsQueryable<EmployeeRoleSalary>()
            where e.Salary.Rank(true).Over(new { e.Department, e.Role }) <= 1.0
            select e;

        AssertPlan(multiKeyQuery, NativePushdown);
        var multiKeyResults = multiKeyQuery.ToList();
        // Top 1 in Dev (Alice), Top 1 in QA (Charlie)
        Assert.Equal(2, multiKeyResults.Count);
        Assert.Contains(multiKeyResults, r => r.Name == "Alice" && r.Role == "Dev");
        Assert.Contains(multiKeyResults, r => r.Name == "Charlie" && r.Role == "QA");
        Assert.DoesNotContain(multiKeyResults, r => r.Name == "Bob");
        Assert.DoesNotContain(multiKeyResults, r => r.Name == "David");

        // =========================================================================
        // Variant 5: Cumulative Window Functions with PartitionBy + OrderBy in Where
        // =========================================================================
        var cumQuery =
            from e in dfRoles.AsQueryable<EmployeeRoleSalary>()
            where e.Salary.CumSum().Over(e.Department, e.Salary) <= 170.0
            orderby e.Salary
            select e;

        AssertPlan(cumQuery, NativePushdown);
        var cumResults = cumQuery.ToList();
        // IT department sorted by Salary ascending:
        // David (70.0, cumsum=70.0 <= 170)
        // Charlie (80.0, cumsum=150.0 <= 170)
        // Bob (90.0, cumsum=240.0 > 170 - filtered)
        // Alice (100.0, cumsum=340.0 > 170 - filtered)
        Assert.Equal(2, cumResults.Count);
        Assert.Equal("David", cumResults[0].Name);
        Assert.Equal(70.0, cumResults[0].Salary);
        Assert.Equal("Charlie", cumResults[1].Name);
        Assert.Equal(80.0, cumResults[1].Salary);

        // =========================================================================
        // Variant 6: Filtering by Window Aggregate directly in Where!
        // In standard SQL, window functions are strictly prohibited in WHERE clause!
        // (SQL throws "ERROR: window functions are not allowed in WHERE").
        // In Polars.NET, it pushes down natively!
        // =========================================================================
        var aboveAvgQuery =
            from e in dfRoles.AsQueryable<EmployeeRoleSalary>()
            where e.Salary > e.Salary.Average().Over(e.Department)
            orderby e.Salary descending
            select e;

        AssertPlan(aboveAvgQuery, NativePushdown);
        var aboveAvgResults = aboveAvgQuery.ToList();
        // IT salaries: 100, 90, 80, 70 -> Avg = 85.0. Above average: Alice (100) and Bob (90)
        Assert.Equal(2, aboveAvgResults.Count);
        Assert.Equal("Alice", aboveAvgResults[0].Name);
        Assert.Equal("Bob", aboveAvgResults[1].Name);
    }

    public record EmployeeRoleSalary(string Department, string Role, string Name, double Salary);
}
