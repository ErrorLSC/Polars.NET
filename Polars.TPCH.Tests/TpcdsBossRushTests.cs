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
}
