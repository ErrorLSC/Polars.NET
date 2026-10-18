using System.Text.RegularExpressions;

namespace Polars.TPCH.Tests;

#region TPC-H Schema Records

public record Customer(
    long c_custkey,
    string c_name,
    string c_address,
    int c_nationkey,
    string c_phone,
    decimal c_acctbal,
    string c_mktsegment,
    string c_comment
);

public record LineItem(
    long l_orderkey,
    long l_partkey,
    long l_suppkey,
    long l_linenumber,
    decimal l_quantity,
    decimal l_extendedprice,
    decimal l_discount,
    decimal l_tax,
    string l_returnflag,
    string l_linestatus,
    DateOnly l_shipdate,
    DateOnly l_commitdate,
    DateOnly l_receiptdate,
    string l_shipinstruct,
    string l_shipmode,
    string l_comment
);

public record Nation(
    int n_nationkey,
    string n_name,
    int n_regionkey,
    string n_comment
);

public record Orders(
    long o_orderkey,
    long o_custkey,
    string o_orderstatus,
    decimal o_totalprice,
    DateOnly o_orderdate,
    string o_orderpriority,
    string o_clerk,
    int o_shippriority,
    string o_comment
);

public record Part(
    long p_partkey,
    string p_name,
    string p_mfgr,
    string p_brand,
    string p_type,
    int p_size,
    string p_container,
    decimal p_retailprice,
    string p_comment
);

public record PartSupp(
    long ps_partkey,
    long ps_suppkey,
    long ps_availqty,
    decimal ps_supplycost,
    string ps_comment
);

public record Region(
    int r_regionkey,
    string r_name,
    string r_comment
);

public record Supplier(
    long s_suppkey,
    string s_name,
    string s_address,
    int s_nationkey,
    string s_phone,
    decimal s_acctbal,
    string s_comment
);

#endregion

public partial class TpchLinqTests
{
    private static void AssertPlan<T>(
        IQueryable<T> query,
        string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    private const string NativePushdown = "100% Native Pushdown";
    /*
     * TPC-H Query 1: Pricing Summary Report Query
     *
     * SELECT
     *     l_returnflag,
     *     l_linestatus,
     *     sum(l_quantity) AS sum_qty,
     *     sum(l_extendedprice) AS sum_base_price,
     *     sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
     *     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
     *     avg(l_quantity) AS avg_qty,
     *     avg(l_extendedprice) AS avg_price,
     *     avg(l_discount) AS avg_disc,
     *     count(*) AS count_order
     * FROM
     *     lineitem
     * WHERE
     *     l_shipdate <= CAST('1998-09-02' AS date)
     * GROUP BY
     *     l_returnflag,
     *     l_linestatus
     * ORDER BY
     *     l_returnflag,
     *     l_linestatus;
     */
    [Fact]
    [Trait("TPCH", "Q1")]
    public void Test_Q1_Pricing_Summary_Report()
    {
        using var lf = TpchData.LineItem();
        var cutoff = new DateOnly(1998, 9, 2);

        var query = from l in lf.AsQueryable<LineItem>()
                    where l.l_shipdate <= cutoff
                    group l by new { l.l_returnflag, l.l_linestatus } into g
                    orderby g.Key.l_returnflag, g.Key.l_linestatus
                    select new
                    {
                        g.Key.l_returnflag,
                        g.Key.l_linestatus,
                        sum_qty = g.Sum(x => x.l_quantity),
                        sum_base_price = g.Sum(x => x.l_extendedprice),
                        sum_disc_price = g.Sum(x => x.l_extendedprice * (1m - x.l_discount)),
                        sum_charge = g.Sum(x => x.l_extendedprice * (1m - x.l_discount) * (1m + x.l_tax)),
                        avg_qty = g.Average(x => (double)x.l_quantity),
                        avg_price = g.Average(x => (double)x.l_extendedprice),
                        avg_disc = g.Average(x => (double)x.l_discount),
                        count_order = g.Count()
                    };

        var results = query.ToList();
        AssertPlan(query, NativePushdown);
        Assert.Equal(4, results.Count);

        // Ground truth verification against DuckDB TPCH (scale factor = 0.01)
        var row0 = results[0];
        Assert.Equal("A", row0.l_returnflag);
        Assert.Equal("F", row0.l_linestatus);
        Assert.Equal(14876, row0.count_order);
        Assert.Equal(380456.00m, row0.sum_qty);
        Assert.Equal(532348211.65m, row0.sum_base_price);
        Assert.Equal(25.575154611454693, row0.avg_qty, 6);
        Assert.Equal(35785.70930693735, row0.avg_price, 6);
        Assert.Equal(0.05008133906964238, row0.avg_disc, 6);

        var row1 = results[1];
        Assert.Equal("N", row1.l_returnflag);
        Assert.Equal("F", row1.l_linestatus);
        Assert.Equal(348, row1.count_order);
        Assert.Equal(8971.00m, row1.sum_qty);
        Assert.Equal(12384801.37m, row1.sum_base_price);

        var row2 = results[2];
        Assert.Equal("N", row2.l_returnflag);
        Assert.Equal("O", row2.l_linestatus);
        Assert.Equal(29181, row2.count_order);
        Assert.Equal(742802.00m, row2.sum_qty);
        Assert.Equal(1041502841.45m, row2.sum_base_price);

        var row3 = results[3];
        Assert.Equal("R", row3.l_returnflag);
        Assert.Equal("F", row3.l_linestatus);
        Assert.Equal(14902, row3.count_order);
        Assert.Equal(381449.00m, row3.sum_qty);
        Assert.Equal(534594445.35m, row3.sum_base_price);
    }

    /*
     * TPC-H Query 2: Minimum Cost Supplier Query
     *
     * SELECT
     *     s_acctbal,
     *     s_name,
     *     n_name,
     *     p_partkey,
     *     p_mfgr,
     *     s_address,
     *     s_phone,
     *     s_comment
     * FROM
     *     part,
     *     supplier,
     *     partsupp,
     *     nation,
     *     region
     * WHERE
     *     p_partkey = ps_partkey
     *     AND s_suppkey = ps_suppkey
     *     AND p_size = 15
     *     AND p_type LIKE '%BRASS'
     *     AND s_nationkey = n_nationkey
     *     AND n_regionkey = r_regionkey
     *     AND r_name = 'EUROPE'
     *     AND ps_supplycost = (
     *         SELECT
     *             min(ps_supplycost)
     *         FROM
     *             partsupp,
     *             supplier,
     *             nation,
     *             region
     *         WHERE
     *             p_partkey = ps_partkey
     *             AND s_suppkey = ps_suppkey
     *             AND s_nationkey = n_nationkey
     *             AND n_regionkey = r_regionkey
     *             AND r_name = 'EUROPE'
     *     )
     * ORDER BY
     *     s_acctbal DESC,
     *     n_name,
     *     s_name,
     *     p_partkey
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCH", "Q2")]
    public void Test_Q2_Minimum_Cost_Supplier()
    {
        using var lfPart = TpchData.Part();
        using var lfSupplier = TpchData.Supplier();
        using var lfPartSupp = TpchData.PartSupp();
        using var lfNation = TpchData.Nation();
        using var lfRegion = TpchData.Region();

        var partQuery = lfPart.AsQueryable<Part>();
        var supplierQuery = lfSupplier.AsQueryable<Supplier>();
        var partsuppQuery = lfPartSupp.AsQueryable<PartSupp>();
        var nationQuery = lfNation.AsQueryable<Nation>();
        var regionQuery = lfRegion.AsQueryable<Region>();

        var query = (from p in partQuery
                     where p.p_size == 15 && p.p_type.EndsWith("BRASS")
                     join ps in partsuppQuery on p.p_partkey equals ps.ps_partkey
                     join mc in
                        from ps in partsuppQuery
                        join s in supplierQuery on ps.ps_suppkey equals s.s_suppkey
                        join n in nationQuery on s.s_nationkey equals n.n_nationkey
                        join r in regionQuery on n.n_regionkey equals r.r_regionkey
                        where r.r_name == "EUROPE"
                        group ps by ps.ps_partkey into g
                        select new
                        {
                            ps_partkey = g.Key,
                            min_cost = g.Min(x => x.ps_supplycost)
                        }
                     on new { ps.ps_partkey, ps.ps_supplycost } equals new { mc.ps_partkey, ps_supplycost = mc.min_cost }
                     join s in supplierQuery on ps.ps_suppkey equals s.s_suppkey
                     join n in nationQuery on s.s_nationkey equals n.n_nationkey
                     join r in regionQuery on n.n_regionkey equals r.r_regionkey
                     where r.r_name == "EUROPE"
                     orderby s.s_acctbal descending, n.n_name, s.s_name, p.p_partkey
                     select new
                     {
                         s.s_acctbal,
                         s.s_name,
                         n.n_name,
                         p.p_partkey,
                         p.p_mfgr,
                         s.s_address,
                         s.s_phone,
                         s.s_comment
                     }).Take(100);

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(4, results.Count);

        var row0 = results[0];
        Assert.Equal(4186.95m, row0.s_acctbal);
        Assert.Equal("Supplier#000000077", row0.s_name);
        Assert.Equal("GERMANY", row0.n_name);
        Assert.Equal(249, row0.p_partkey);
        Assert.Equal("Manufacturer#4", row0.p_mfgr);

        var row1 = results[1];
        Assert.Equal(1883.37m, row1.s_acctbal);
        Assert.Equal("Supplier#000000086", row1.s_name);
        Assert.Equal("ROMANIA", row1.n_name);
        Assert.Equal(1015, row1.p_partkey);
        Assert.Equal("Manufacturer#4", row1.p_mfgr);

        var row2 = results[2];
        Assert.Equal(1687.81m, row2.s_acctbal);
        Assert.Equal("Supplier#000000017", row2.s_name);
        Assert.Equal("ROMANIA", row2.n_name);
        Assert.Equal(1634, row2.p_partkey);
        Assert.Equal("Manufacturer#2", row2.p_mfgr);

        var row3 = results[3];
        Assert.Equal(287.16m, row3.s_acctbal);
        Assert.Equal("Supplier#000000052", row3.s_name);
        Assert.Equal("ROMANIA", row3.n_name);
        Assert.Equal(323, row3.p_partkey);
        Assert.Equal("Manufacturer#4", row3.p_mfgr);
    }

    /*
     * TPC-H Query 3: Shipping Priority Query
     *
     * SELECT
     *     l_orderkey,
     *     sum(l_extendedprice * (1 - l_discount)) AS revenue,
     *     o_orderdate,
     *     o_shippriority
     * FROM
     *     customer,
     *     orders,
     *     lineitem
     * WHERE
     *     c_mktsegment = 'BUILDING'
     *     AND c_custkey = o_custkey
     *     AND l_orderkey = o_orderkey
     *     AND o_orderdate < CAST('1995-03-15' AS date)
     *     AND l_shipdate > CAST('1995-03-15' AS date)
     * GROUP BY
     *     l_orderkey,
     *     o_orderdate,
     *     o_shippriority
     * ORDER BY
     *     revenue DESC,
     *     o_orderdate
     * LIMIT 10;
     */
    [Fact]
    [Trait("TPCH", "Q3")]
    public void Test_Q3_Shipping_Priority()
    {
        using var lfCustomer = TpchData.Customer();
        using var lfOrders = TpchData.Orders();
        using var lfLineItem = TpchData.LineItem();

        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();

        var cutoff = new DateOnly(1995, 3, 15);

        var query = (from c in customerQuery
                     where c.c_mktsegment == "BUILDING"
                     join o in ordersQuery on c.c_custkey equals o.o_custkey
                     where o.o_orderdate < cutoff
                     join l in lineitemQuery on o.o_orderkey equals l.l_orderkey
                     where l.l_shipdate > cutoff
                     group l by new { l.l_orderkey, o.o_orderdate, o.o_shippriority } into g
                     select new
                     {
                         g.Key.l_orderkey,
                         revenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount)),
                         g.Key.o_orderdate,
                         g.Key.o_shippriority
                     })
                     .OrderByDescending(x => x.revenue)
                     .ThenBy(x => x.o_orderdate)
                     .Take(10);

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(10, results.Count);

        var row0 = results[0];
        Assert.Equal(47714, row0.l_orderkey);
        Assert.Equal(267010.59m, row0.revenue);
        Assert.Equal(new DateOnly(1995, 3, 11), row0.o_orderdate);
        Assert.Equal(0, row0.o_shippriority);

        var row1 = results[1];
        Assert.Equal(22276, row1.l_orderkey);
        Assert.Equal(266351.57m, row1.revenue);
        Assert.Equal(new DateOnly(1995, 1, 29), row1.o_orderdate);
        Assert.Equal(0, row1.o_shippriority);

        var row9 = results[9];
        Assert.Equal(9696, row9.l_orderkey);
        Assert.Equal(201502.21m, row9.revenue);
        Assert.Equal(new DateOnly(1995, 2, 20), row9.o_orderdate);
        Assert.Equal(0, row9.o_shippriority);
    }

    /*
     * TPC-H Query 4: Order Priority Checking Query
     *
     * SELECT
     *     o_orderpriority,
     *     count(*) AS order_count
     * FROM
     *     orders
     * WHERE
     *     o_orderdate >= CAST('1993-07-01' AS date)
     *     AND o_orderdate < CAST('1993-10-01' AS date)
     *     AND EXISTS (
     *         SELECT
     *             *
     *         FROM
     *             lineitem
     *         WHERE
     *             l_orderkey = o_orderkey
     *             AND l_commitdate < l_receiptdate
     *     )
     * GROUP BY
     *     o_orderpriority
     * ORDER BY
     *     o_orderpriority;
     */
    [Fact]
    [Trait("TPCH", "Q4")]
    public void Test_Q4_Order_Priority_Checking()
    {
        using var lfOrders = TpchData.Orders();
        using var lfLineItem = TpchData.LineItem();

        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();

        var startDate = new DateOnly(1993, 7, 1);
        var endDate = new DateOnly(1993, 10, 1);

        var query = from o in ordersQuery
                    where o.o_orderdate >= startDate && o.o_orderdate < endDate
                       && lineitemQuery.Any(l => l.l_orderkey == o.o_orderkey && l.l_commitdate < l.l_receiptdate)
                    group o by o.o_orderpriority into g
                    orderby g.Key
                    select new
                    {
                        o_orderpriority = g.Key,
                        order_count = g.Count()
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(5, results.Count);

        var row0 = results[0];
        Assert.Equal("1-URGENT", row0.o_orderpriority);
        Assert.Equal(93, row0.order_count);

        var row1 = results[1];
        Assert.Equal("2-HIGH", row1.o_orderpriority);
        Assert.Equal(103, row1.order_count);

        var row2 = results[2];
        Assert.Equal("3-MEDIUM", row2.o_orderpriority);
        Assert.Equal(109, row2.order_count);

        var row3 = results[3];
        Assert.Equal("4-NOT SPECIFIED", row3.o_orderpriority);
        Assert.Equal(102, row3.order_count);

        var row4 = results[4];
        Assert.Equal("5-LOW", row4.o_orderpriority);
        Assert.Equal(128, row4.order_count);
    }

    /*
     * TPC-H Query 5: Local Supplier Volume Query
     *
     * SELECT
     *     n_name,
     *     sum(l_extendedprice * (1 - l_discount)) AS revenue
     * FROM
     *     customer,
     *     orders,
     *     lineitem,
     *     supplier,
     *     nation,
     *     region
     * WHERE
     *     c_custkey = o_custkey
     *     AND l_orderkey = o_orderkey
     *     AND l_suppkey = s_suppkey
     *     AND c_nationkey = s_nationkey
     *     AND s_nationkey = n_nationkey
     *     AND n_regionkey = r_regionkey
     *     AND r_name = 'ASIA'
     *     AND o_orderdate >= CAST('1994-01-01' AS date)
     *     AND o_orderdate < CAST('1995-01-01' AS date)
     * GROUP BY
     *     n_name
     * ORDER BY
     *     revenue DESC;
     */
    [Fact]
    [Trait("TPCH", "Q5")]
    public void Test_Q5_Local_Supplier_Volume()
    {
        using var lfCustomer = TpchData.Customer();
        using var lfOrders = TpchData.Orders();
        using var lfLineItem = TpchData.LineItem();
        using var lfSupplier = TpchData.Supplier();
        using var lfNation = TpchData.Nation();
        using var lfRegion = TpchData.Region();

        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var supplierQuery = lfSupplier.AsQueryable<Supplier>();
        var nationQuery = lfNation.AsQueryable<Nation>();
        var regionQuery = lfRegion.AsQueryable<Region>();

        var startDate = new DateOnly(1994, 1, 1);
        var endDate = new DateOnly(1995, 1, 1);

        var query = (from c in customerQuery
                     join o in ordersQuery on c.c_custkey equals o.o_custkey
                     where o.o_orderdate >= startDate && o.o_orderdate < endDate
                     join l in lineitemQuery on o.o_orderkey equals l.l_orderkey
                     join s in supplierQuery on new { l.l_suppkey, c.c_nationkey }
                        equals new { l_suppkey = s.s_suppkey, c_nationkey = s.s_nationkey }
                     join n in nationQuery on s.s_nationkey equals n.n_nationkey
                     join r in regionQuery on n.n_regionkey equals r.r_regionkey
                     where r.r_name == "ASIA"
                     group l by n.n_name into g
                     select new
                     {
                         n_name = g.Key,
                         revenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount))
                     })
                     .OrderByDescending(x => x.revenue);

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(5, results.Count);

        var row0 = results[0];
        Assert.Equal("VIETNAM", row0.n_name);
        Assert.Equal(1000926.71m, row0.revenue);

        var row1 = results[1];
        Assert.Equal("CHINA", row1.n_name);
        Assert.Equal(740210.76m, row1.revenue);

        var row2 = results[2];
        Assert.Equal("JAPAN", row2.n_name);
        Assert.Equal(660651.24m, row2.revenue);

        var row3 = results[3];
        Assert.Equal("INDONESIA", row3.n_name);
        Assert.Equal(566379.52m, row3.revenue);

        var row4 = results[4];
        Assert.Equal("INDIA", row4.n_name);
        Assert.Equal(422874.70m, row4.revenue);
    }

    /*
     * TPC-H Query 6: Forecasting Revenue Change Query
     *
     * SELECT
     *     sum(l_extendedprice * l_discount) AS revenue
     * FROM
     *     lineitem
     * WHERE
     *     l_shipdate >= CAST('1994-01-01' AS date)
     *     AND l_shipdate < CAST('1995-01-01' AS date)
     *     AND l_discount BETWEEN 0.05 AND 0.07
     *     AND l_quantity < 24;
     */
    [Fact]
    [Trait("TPCH", "Q6")]
    public void Test_Q6_Forecasting_Revenue_Change()
    {
        using var lfLineItem = TpchData.LineItem();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();

        var startDate = new DateOnly(1994, 1, 1);
        var endDate = new DateOnly(1995, 1, 1);

        var query = from l in lineitemQuery
                    where l.l_shipdate >= startDate && l.l_shipdate < endDate
                       && l.l_discount >= 0.05m && l.l_discount <= 0.07m
                       && l.l_quantity < 24m
                    group l by 1 into g
                    select new
                    {
                        revenue = g.Sum(x => x.l_extendedprice * x.l_discount)
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Single(results);

        var row0 = results[0];
        Assert.Equal(1193053.17m, row0.revenue);
    }

    /*
     * TPC-H Query 7: Volume Shipping Query
     *
     * SELECT
     *     supp_nation,
     *     cust_nation,
     *     l_year,
     *     sum(volume) AS revenue
     * FROM (
     *     SELECT
     *         n1.n_name AS supp_nation,
     *         n2.n_name AS cust_nation,
     *         extract(year FROM l_shipdate) AS l_year,
     *         l_extendedprice * (1 - l_discount) AS volume
     *     FROM
     *         supplier,
     *         lineitem,
     *         orders,
     *         customer,
     *         nation n1,
     *         nation n2
     *     WHERE
     *         s_suppkey = l_suppkey
     *         AND o_orderkey = l_orderkey
     *         AND c_custkey = o_custkey
     *         AND s_nationkey = n1.n_nationkey
     *         AND c_nationkey = n2.n_nationkey
     *         AND (
     *             (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
     *             OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
     *         )
     *         AND l_shipdate BETWEEN CAST('1995-01-01' AS date) AND CAST('1996-12-31' AS date)
     * ) AS shipping
     * GROUP BY
     *     supp_nation,
     *     cust_nation,
     *     l_year
     * ORDER BY
     *     supp_nation,
     *     cust_nation,
     *     l_year;
     */
    [Fact]
    [Trait("TPCH", "Q7")]
    public void Test_Q7_Volume_Shipping()
    {
        using var lfSupplier = TpchData.Supplier();
        using var lfLineItem = TpchData.LineItem();
        using var lfOrders = TpchData.Orders();
        using var lfCustomer = TpchData.Customer();
        using var lfNation = TpchData.Nation();

        var supplierQuery = lfSupplier.AsQueryable<Supplier>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var nationQuery = lfNation.AsQueryable<Nation>();

        var startDate = new DateOnly(1995, 1, 1);
        var endDate = new DateOnly(1996, 12, 31);

        var query = from s in supplierQuery
                    join n1 in nationQuery.Select(n => new { supp_nationkey = n.n_nationkey, supp_nation = n.n_name })
                        on s.s_nationkey equals n1.supp_nationkey
                    join l in lineitemQuery on s.s_suppkey equals l.l_suppkey
                    where l.l_shipdate >= startDate && l.l_shipdate <= endDate
                    join o in ordersQuery on l.l_orderkey equals o.o_orderkey
                    join c in customerQuery on o.o_custkey equals c.c_custkey
                    join n2 in nationQuery.Select(n => new { cust_nationkey = n.n_nationkey, cust_nation = n.n_name })
                        on c.c_nationkey equals n2.cust_nationkey
                    where (n1.supp_nation == "FRANCE" && n2.cust_nation == "GERMANY")
                       || (n1.supp_nation == "GERMANY" && n2.cust_nation == "FRANCE")
                    group l by new
                    {
                        n1.supp_nation,
                        n2.cust_nation,
                        l_year = l.l_shipdate.Year
                    } into g
                    orderby g.Key.supp_nation, g.Key.cust_nation, g.Key.l_year
                    select new
                    {
                        g.Key.supp_nation,
                        g.Key.cust_nation,
                        g.Key.l_year,
                        revenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount))
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(4, results.Count);

        var row0 = results[0];
        Assert.Equal("FRANCE", row0.supp_nation);
        Assert.Equal("GERMANY", row0.cust_nation);
        Assert.Equal(1995, row0.l_year);
        Assert.Equal(268068.57m, row0.revenue);

        var row1 = results[1];
        Assert.Equal("FRANCE", row1.supp_nation);
        Assert.Equal("GERMANY", row1.cust_nation);
        Assert.Equal(1996, row1.l_year);
        Assert.Equal(303862.29m, row1.revenue);

        var row2 = results[2];
        Assert.Equal("GERMANY", row2.supp_nation);
        Assert.Equal("FRANCE", row2.cust_nation);
        Assert.Equal(1995, row2.l_year);
        Assert.Equal(621159.50m, row2.revenue);

        var row3 = results[3];
        Assert.Equal("GERMANY", row3.supp_nation);
        Assert.Equal("FRANCE", row3.cust_nation);
        Assert.Equal(1996, row3.l_year);
        Assert.Equal(379095.89m, row3.revenue);
    }

    /*
     * TPC-H Query 8: National Market Share Query
     *
     * SELECT
     *     o_year,
     *     sum(CASE
     *         WHEN nation = 'BRAZIL' THEN volume
     *         ELSE 0
     *     END) / sum(volume) AS mkt_share
     * FROM (
     *     SELECT
     *         extract(year FROM o_orderdate) AS o_year,
     *         l_extendedprice * (1 - l_discount) AS volume,
     *         n2.n_name AS nation
     *     FROM
     *         part,
     *         supplier,
     *         lineitem,
     *         orders,
     *         customer,
     *         nation n1,
     *         nation n2,
     *         region
     *     WHERE
     *         p_partkey = l_partkey
     *         AND s_suppkey = l_suppkey
     *         AND l_orderkey = o_orderkey
     *         AND o_custkey = c_custkey
     *         AND c_nationkey = n1.n_nationkey
     *         AND n1.n_regionkey = r_regionkey
     *         AND r_name = 'AMERICA'
     *         AND s_nationkey = n2.n_nationkey
     *         AND o_orderdate BETWEEN CAST('1995-01-01' AS date) AND CAST('1996-12-31' AS date)
     *         AND p_type = 'ECONOMY ANODIZED STEEL'
     * ) AS all_nations
     * GROUP BY
     *     o_year
     * ORDER BY
     *     o_year;
     */
    [Fact]
    [Trait("TPCH", "Q8")]
    public void Test_Q8_National_Market_Share()
    {
        using var lfPart = TpchData.Part();
        using var lfSupplier = TpchData.Supplier();
        using var lfLineItem = TpchData.LineItem();
        using var lfOrders = TpchData.Orders();
        using var lfCustomer = TpchData.Customer();
        using var lfNation = TpchData.Nation();
        using var lfRegion = TpchData.Region();

        var partQuery = lfPart.AsQueryable<Part>();
        var supplierQuery = lfSupplier.AsQueryable<Supplier>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var nationQuery = lfNation.AsQueryable<Nation>();
        var regionQuery = lfRegion.AsQueryable<Region>();

        var startDate = new DateOnly(1995, 1, 1);
        var endDate = new DateOnly(1996, 12, 31);

        var query = (from a in from p in partQuery
                               where p.p_type == "ECONOMY ANODIZED STEEL"
                               join l in lineitemQuery on p.p_partkey equals l.l_partkey
                               join s in supplierQuery on l.l_suppkey equals s.s_suppkey
                               join n2 in nationQuery.Select(n => new { supp_nationkey = n.n_nationkey, supp_nation = n.n_name })
                                   on s.s_nationkey equals n2.supp_nationkey
                               join o in ordersQuery on l.l_orderkey equals o.o_orderkey
                               where o.o_orderdate >= startDate && o.o_orderdate <= endDate
                               join c in customerQuery on o.o_custkey equals c.c_custkey
                               join n1 in nationQuery on c.c_nationkey equals n1.n_nationkey
                               join r in regionQuery on n1.n_regionkey equals r.r_regionkey
                               where r.r_name == "AMERICA"
                               select new
                               {
                                   o_year = o.o_orderdate.Year,
                                   volume = l.l_extendedprice * (1m - l.l_discount),
                                   nation = n2.supp_nation
                               }
                     group a by a.o_year into g
                     orderby g.Key
                     select new
                     {
                         o_year = g.Key,
                         brazil_volume = g.Sum(x => x.nation == "BRAZIL" ? x.volume : 0m),
                         total_volume = g.Sum(x => x.volume)
                     })
                     .Select(x => new
                     {
                         x.o_year,
                         mkt_share = (double)(x.brazil_volume / x.total_volume)
                     });

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(2, results.Count);

        var row0 = results[0];
        Assert.Equal(1995, row0.o_year);
        Assert.Equal(0.0, row0.mkt_share, precision: 4);

        var row1 = results[1];
        Assert.Equal(1996, row1.o_year);
        Assert.Equal(0.0, row1.mkt_share, precision: 4);
    }

    /*
     * TPC-H Query 9: Product Type Profit Measure Query
     *
     * SELECT
     *     nation,
     *     o_year,
     *     sum(amount) AS sum_profit
     * FROM (
     *     SELECT
     *         n_name AS nation,
     *         extract(year FROM o_orderdate) AS o_year,
     *         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
     *     FROM
     *         part,
     *         supplier,
     *         lineitem,
     *         partsupp,
     *         orders,
     *         nation
     *     WHERE
     *         s_suppkey = l_suppkey
     *         AND ps_suppkey = l_suppkey
     *         AND ps_partkey = l_partkey
     *         AND p_partkey = l_partkey
     *         AND o_orderkey = l_orderkey
     *         AND s_nationkey = n_nationkey
     *         AND p_name LIKE '%green%'
     * ) AS profit
     * GROUP BY
     *     nation,
     *     o_year
     * ORDER BY
     *     nation,
     *     o_year DESC;
     */
    [Fact]
    [Trait("TPCH", "Q9")]
    public void Test_Q9_Product_Type_Profit_Measure()
    {
        using var lfPart = TpchData.Part();
        using var lfSupplier = TpchData.Supplier();
        using var lfLineItem = TpchData.LineItem();
        using var lfPartSupp = TpchData.PartSupp();
        using var lfOrders = TpchData.Orders();
        using var lfNation = TpchData.Nation();

        var partQuery = lfPart.AsQueryable<Part>();
        var supplierQuery = lfSupplier.AsQueryable<Supplier>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var partsuppQuery = lfPartSupp.AsQueryable<PartSupp>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var nationQuery = lfNation.AsQueryable<Nation>();

        var query = from pr in from p in partQuery
                               where p.p_name.Contains("green")
                               join l in lineitemQuery on p.p_partkey equals l.l_partkey
                               join ps in partsuppQuery on new { l.l_partkey, l.l_suppkey } equals new { l_partkey = ps.ps_partkey, l_suppkey = ps.ps_suppkey }
                               join s in supplierQuery on l.l_suppkey equals s.s_suppkey
                               join o in ordersQuery on l.l_orderkey equals o.o_orderkey
                               join n in nationQuery on s.s_nationkey equals n.n_nationkey
                               select new
                               {
                                   nation = n.n_name,
                                   o_year = o.o_orderdate.Year,
                                   amount = (l.l_extendedprice * (1m - l.l_discount)) - (ps.ps_supplycost * l.l_quantity)
                               }
                    group pr by new { pr.nation, pr.o_year } into g
                    orderby g.Key.nation, g.Key.o_year descending
                    select new
                    {
                        g.Key.nation,
                        g.Key.o_year,
                        sum_profit = g.Sum(x => x.amount)
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(173, results.Count);

        var row0 = results[0];
        Assert.Equal("ALGERIA", row0.nation);
        Assert.Equal(1998, row0.o_year);
        Assert.Equal(97864.58m, row0.sum_profit);

        var row1 = results[1];
        Assert.Equal("ALGERIA", row1.nation);
        Assert.Equal(1997, row1.o_year);
        Assert.Equal(368231.67m, row1.sum_profit);

        var row2 = results[2];
        Assert.Equal("ALGERIA", row2.nation);
        Assert.Equal(1996, row2.o_year);
        Assert.Equal(196525.81m, row2.sum_profit);

        var last = results[^1];
        Assert.Equal("VIETNAM", last.nation);
        Assert.Equal(1992, last.o_year);
        Assert.Equal(716126.53m, last.sum_profit);
    }

    /*
     * TPC-H Query 10: Returned Item Reporting Query
     *
     * SELECT
     *     c_custkey,
     *     c_name,
     *     sum(l_extendedprice * (1 - l_discount)) AS revenue,
     *     c_acctbal,
     *     n_name,
     *     c_address,
     *     c_phone,
     *     c_comment
     * FROM
     *     customer,
     *     orders,
     *     lineitem,
     *     nation
     * WHERE
     *     c_custkey = o_custkey
     *     AND l_orderkey = o_orderkey
     *     AND o_orderdate >= CAST('1993-10-01' AS date)
     *     AND o_orderdate < CAST('1994-01-01' AS date)
     *     AND l_returnflag = 'R'
     *     AND c_nationkey = n_nationkey
     * GROUP BY
     *     c_custkey,
     *     c_name,
     *     c_acctbal,
     *     c_phone,
     *     n_name,
     *     c_address,
     *     c_comment
     * ORDER BY
     *     revenue DESC
     * LIMIT 20;
     */
    [Fact]
    [Trait("TPCH", "Q10")]
    public void Test_Q10_Returned_Item_Reporting()
    {
        using var lfCustomer = TpchData.Customer();
        using var lfOrders = TpchData.Orders();
        using var lfLineItem = TpchData.LineItem();
        using var lfNation = TpchData.Nation();

        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var nationQuery = lfNation.AsQueryable<Nation>();

        var startDate = new DateOnly(1993, 10, 1);
        var endDate = new DateOnly(1994, 1, 1);

        var query = (from c in customerQuery
                     join o in ordersQuery on c.c_custkey equals o.o_custkey
                     where o.o_orderdate >= startDate && o.o_orderdate < endDate
                     join l in lineitemQuery on o.o_orderkey equals l.l_orderkey
                     where l.l_returnflag == "R"
                     join n in nationQuery on c.c_nationkey equals n.n_nationkey
                     group l by new
                     {
                         c.c_custkey,
                         c.c_name,
                         c.c_acctbal,
                         n.n_name,
                         c.c_address,
                         c.c_phone,
                         c.c_comment
                     } into g
                     select new
                     {
                         g.Key.c_custkey,
                         g.Key.c_name,
                         revenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount)),
                         g.Key.c_acctbal,
                         g.Key.n_name,
                         g.Key.c_address,
                         g.Key.c_phone,
                         g.Key.c_comment
                     })
                     .OrderByDescending(x => x.revenue)
                     .Take(20);

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(20, results.Count);

        var row0 = results[0];
        Assert.Equal(679, row0.c_custkey);
        Assert.Equal("Customer#000000679", row0.c_name);
        Assert.Equal(378211.32m, row0.revenue);
        Assert.Equal(1394.44m, row0.c_acctbal);
        Assert.Equal("IRAN", row0.n_name);
        Assert.Equal("jhLZlG1fRiSE0 wDLOCguVB96dH6a5", row0.c_address);
        Assert.Equal("20-146-696-9508", row0.c_phone);
        Assert.Equal("oys. furiously silent pinto beans", row0.c_comment);

        var row1 = results[1];
        Assert.Equal(1201, row1.c_custkey);
        Assert.Equal("Customer#000001201", row1.c_name);
        Assert.Equal(374331.55m, row1.revenue);
        Assert.Equal(5165.39m, row1.c_acctbal);
        Assert.Equal("IRAN", row1.n_name);
    }

    /*
     * TPC-H Query 11: Important Stock Identification Query
     *
     * SELECT
     *     ps_partkey,
     *     sum(ps_supplycost * ps_availqty) AS value
     * FROM
     *     partsupp,
     *     supplier,
     *     nation
     * WHERE
     *     ps_suppkey = s_suppkey
     *     AND s_nationkey = n_nationkey
     *     AND n_name = 'GERMANY'
     * GROUP BY
     *     ps_partkey
     * HAVING
     *     sum(ps_supplycost * ps_availqty) > (
     *         SELECT
     *             sum(ps_supplycost * ps_availqty) * 0.0001000000
     *         FROM
     *             partsupp,
     *             supplier,
     *             nation
     *         WHERE
     *             ps_suppkey = s_suppkey
     *             AND s_nationkey = n_nationkey
     *             AND n_name = 'GERMANY'
     *     )
     * ORDER BY
     *     value DESC;
     */
    [Fact]
    [Trait("TPCH", "Q11")]
    public void Test_Q11_Important_Stock_Identification()
    {
        using var lfPartSupp = TpchData.PartSupp();
        using var lfSupplier = TpchData.Supplier();
        using var lfNation = TpchData.Nation();

        var partsuppQuery = lfPartSupp.AsQueryable<PartSupp>();
        var supplierQuery = lfSupplier.AsQueryable<Supplier>();
        var nationQuery = lfNation.AsQueryable<Nation>();

        var query = (from ps in partsuppQuery
                     join s in supplierQuery on ps.ps_suppkey equals s.s_suppkey
                     join n in nationQuery on s.s_nationkey equals n.n_nationkey
                     where n.n_name == "GERMANY"
                     group ps by ps.ps_partkey into g
                     select new
                     {
                         ps_partkey = g.Key,
                         value = g.Sum(x => x.ps_supplycost * x.ps_availqty)
                     })
                     .Where(x => x.value > (from ps2 in partsuppQuery
                                            join s2 in supplierQuery on ps2.ps_suppkey equals s2.s_suppkey
                                            join n2 in nationQuery on s2.s_nationkey equals n2.n_nationkey
                                            where n2.n_name == "GERMANY"
                                            select ps2.ps_supplycost * ps2.ps_availqty).Sum() * 0.0001m)
                     .OrderByDescending(x => x.value);

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(359, results.Count);

        var row0 = results[0];
        Assert.Equal(1376, row0.ps_partkey);
        Assert.Equal(13271249.89m, row0.value);

        var row1 = results[1];
        Assert.Equal(788, row1.ps_partkey);
        Assert.Equal(9498648.06m, row1.value);

        var last = results[^1];
        Assert.Equal(295, last.ps_partkey);
        Assert.Equal(97604.25m, last.value);
    }

    /*
     * TPC-H Query 12: Shipping Modes and Order Priority Query
     *
     * SELECT
     *     l_shipmode,
     *     sum(
     *         CASE WHEN o_orderpriority = '1-URGENT'
     *             OR o_orderpriority = '2-HIGH' THEN
     *             1
     *         ELSE
     *             0
     *         END) AS high_line_count,
     *     sum(
     *         CASE WHEN o_orderpriority <> '1-URGENT'
     *             AND o_orderpriority <> '2-HIGH' THEN
     *             1
     *         ELSE
     *             0
     *         END) AS low_line_count
     * FROM
     *     orders,
     *     lineitem
     * WHERE
     *     o_orderkey = l_orderkey
     *     AND l_shipmode IN ('MAIL', 'SHIP')
     *     AND l_commitdate < l_receiptdate
     *     AND l_shipdate < l_commitdate
     *     AND l_receiptdate >= CAST('1994-01-01' AS date)
     *     AND l_receiptdate < CAST('1995-01-01' AS date)
     * GROUP BY
     *     l_shipmode
     * ORDER BY
     *     l_shipmode;
     */
    [Fact]
    [Trait("TPCH", "Q12")]
    public void Test_Q12_Shipping_Modes_and_Order_Priority()
    {
        using var lfOrders = TpchData.Orders();
        using var lfLineItem = TpchData.LineItem();

        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();

        var startDate = new DateOnly(1994, 1, 1);
        var endDate = new DateOnly(1995, 1, 1);

        var query = from l in lineitemQuery
                    where (l.l_shipmode == "MAIL" || l.l_shipmode == "SHIP")
                       && l.l_commitdate < l.l_receiptdate
                       && l.l_shipdate < l.l_commitdate
                       && l.l_receiptdate >= startDate && l.l_receiptdate < endDate
                    join o in ordersQuery on l.l_orderkey equals o.o_orderkey
                    group o by l.l_shipmode into g
                    orderby g.Key
                    select new
                    {
                        l_shipmode = g.Key,
                        high_line_count = g.Count(x => x.o_orderpriority == "1-URGENT" || x.o_orderpriority == "2-HIGH"),
                        low_line_count = g.Count(x => x.o_orderpriority != "1-URGENT" && x.o_orderpriority != "2-HIGH")
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(2, results.Count);

        var row0 = results[0];
        Assert.Equal("MAIL", row0.l_shipmode);
        Assert.Equal(64, row0.high_line_count);
        Assert.Equal(86, row0.low_line_count);

        var row1 = results[1];
        Assert.Equal("SHIP", row1.l_shipmode);
        Assert.Equal(61, row1.high_line_count);
        Assert.Equal(96, row1.low_line_count);
    }

    /*
     * TPC-H Query 13: Customer Distribution Query
     *
     * SELECT
     *     c_count,
     *     count(*) AS custdist
     * FROM (
     *     SELECT
     *         c_custkey,
     *         count(o_orderkey) AS c_count
     *     FROM
     *         customer
     *     LEFT OUTER JOIN orders ON c_custkey = o_custkey
     *         AND o_comment NOT LIKE '%special%requests%'
     *     GROUP BY
     *         c_custkey
     * ) AS c_orders (c_custkey, c_count)
     * GROUP BY
     *     c_count
     * ORDER BY
     *     custdist DESC,
     *     c_count DESC;
     */
    [GeneratedRegex("special.*requests")]
    private static partial Regex SpecialRequestsRegex();

    [Fact]
    [Trait("TPCH", "Q13")]
    public void Test_Q13_Customer_Distribution()
    {
        using var lfCustomer = TpchData.Customer();
        using var lfOrders = TpchData.Orders();

        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();

        var query = from co in from c in customerQuery
                               join o in ordersQuery.Where(o => !SpecialRequestsRegex().IsMatch(o.o_comment)) on c.c_custkey equals o.o_custkey into g
                               select new
                               {
                                   c.c_custkey,
                                   c_count = g.Count()
                               }
                    group co by co.c_count into g
                    orderby g.Count() descending, g.Key descending
                    select new
                    {
                        c_count = g.Key,
                        custdist = g.Count()
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(32, results.Count);

        var row0 = results[0];
        Assert.Equal(0, row0.c_count);
        Assert.Equal(500, row0.custdist);

        var row1 = results[1];
        Assert.Equal(10, row1.c_count);
        Assert.Equal(69, row1.custdist);

        var row2 = results[2];
        Assert.Equal(9, row2.c_count);
        Assert.Equal(66, row2.custdist);

        var row3 = results[3];
        Assert.Equal(11, row3.c_count);
        Assert.Equal(63, row3.custdist);
    }

    /*
     * TPC-H Query 14: Promotion Effect Query
     *
     * SELECT
     *     100.00 * sum(
     *         CASE WHEN p_type LIKE 'PROMO%' THEN
     *             l_extendedprice * (1 - l_discount)
     *         ELSE
     *             0
     *         END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
     * FROM
     *     lineitem,
     *     part
     * WHERE
     *     l_partkey = p_partkey
     *     AND l_shipdate >= date '1995-09-01'
     *     AND l_shipdate < CAST('1995-10-01' AS date);
     */
    [Fact]
    [Trait("TPCH", "Q14")]
    public void Test_Q14_Promotion_Effect()
    {
        using var lfLineItem = TpchData.LineItem();
        using var lfPart = TpchData.Part();

        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var partQuery = lfPart.AsQueryable<Part>();

        var startDate = new DateOnly(1995, 9, 1);
        var endDate = new DateOnly(1995, 10, 1);

        var query = from l in lineitemQuery
                    where l.l_shipdate >= startDate && l.l_shipdate < endDate
                    join p in partQuery on l.l_partkey equals p.p_partkey
                    group new { l, p } by 1 into g
                    select new
                    {
                        promo_revenue = 100.0 * (double)g.Sum(x => x.p.p_type.StartsWith("PROMO") ? x.l.l_extendedprice * (1m - x.l.l_discount) : 0m)
                                        / (double)g.Sum(x => x.l.l_extendedprice * (1m - x.l.l_discount))
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Single(results);
        Assert.Equal(15.4865, results[0].promo_revenue, precision: 4);
    }

    /*
     * TPC-H Query 15: Top Supplier Query
     *
     * WITH revenue AS (
     *     SELECT
     *         l_suppkey AS supplier_no,
     *         sum(l_extendedprice * (1 - l_discount)) AS total_revenue
     *     FROM
     *         lineitem
     *     WHERE
     *         l_shipdate >= CAST('1996-01-01' AS date)
     *         AND l_shipdate < CAST('1996-04-01' AS date)
     *     GROUP BY
     *         l_suppkey
     * )
     * SELECT
     *     s_suppkey,
     *     s_name,
     *     s_address,
     *     s_phone,
     *     total_revenue
     * FROM
     *     supplier,
     *     revenue
     * WHERE
     *     s_suppkey = supplier_no
     *     AND total_revenue = (
     *         SELECT
     *             max(total_revenue)
     *         FROM
     *             revenue
     *     )
     * ORDER BY
     *     s_suppkey;
     */
    [Fact]
    [Trait("TPCH", "Q15")]
    public void Test_Q15_Top_Supplier()
    {
        using var lfSupplier = TpchData.Supplier();
        using var lfLineItem = TpchData.LineItem();

        var supplierQuery = lfSupplier.AsQueryable<Supplier>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();

        var startDate = new DateOnly(1996, 1, 1);
        var endDate = new DateOnly(1996, 4, 1);

        var revenue = from l in lineitemQuery
                      where l.l_shipdate >= startDate && l.l_shipdate < endDate
                      group l by l.l_suppkey into g
                      select new
                      {
                          supplier_no = g.Key,
                          total_revenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount))
                      };

        var query = from s in supplierQuery
                    join r in revenue on s.s_suppkey equals r.supplier_no
                    where r.total_revenue == revenue.Select(x => x.total_revenue).Max()
                    orderby s.s_suppkey
                    select new
                    {
                        s.s_suppkey,
                        s.s_name,
                        s.s_address,
                        s.s_phone,
                        r.total_revenue
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Single(results);
        var row0 = results[0];
        Assert.Equal(21, row0.s_suppkey);
        Assert.Equal("Supplier#000000021", row0.s_name);
        Assert.Equal("TZoQwNFFO i,baXpbpin02,hvuhE,GRVIKm ", row0.s_address);
        Assert.Equal("12-253-590-5816", row0.s_phone);
        Assert.Equal(1161099.49m, row0.total_revenue);
    }

    /*
     * TPC-H Query 16: Parts/Supplier Relationship Query
     *
     * SELECT
     *     p_brand,
     *     p_type,
     *     p_size,
     *     count(DISTINCT ps_suppkey) AS supplier_cnt
     * FROM
     *     partsupp,
     *     part
     * WHERE
     *     p_partkey = ps_partkey
     *     AND p_brand <> 'Brand#45'
     *     AND p_type NOT LIKE 'MEDIUM POLISHED%'
     *     AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
     *     AND ps_suppkey NOT IN (
     *         SELECT
     *             s_suppkey
     *         FROM
     *             supplier
     *         WHERE
     *             s_comment LIKE '%Customer%Complaints%'
     *     )
     * GROUP BY
     *     p_brand,
     *     p_type,
     *     p_size
     * ORDER BY
     *     supplier_cnt DESC,
     *     p_brand,
     *     p_type,
     *     p_size;
     */
    [GeneratedRegex("Customer.*Complaints")]
    private static partial Regex CustomerComplaintsRegex();

    [Fact]
    [Trait("TPCH", "Q16")]
    public void Test_Q16_Parts_Supplier_Relationship()
    {
        using var lfPartSupp = TpchData.PartSupp();
        using var lfPart = TpchData.Part();
        using var lfSupplier = TpchData.Supplier();

        var partsuppQuery = lfPartSupp.AsQueryable<PartSupp>();
        var partQuery = lfPart.AsQueryable<Part>();
        var supplierQuery = lfSupplier.AsQueryable<Supplier>();

        int[] targetSizes = [49, 14, 23, 45, 19, 3, 36, 9];

        var complaintsSuppliers = supplierQuery
            .Where(s => CustomerComplaintsRegex().IsMatch(s.s_comment))
            .Select(s => s.s_suppkey);

        var filteredParts = partQuery.Where(p => p.p_brand != "Brand#45"
                                              && !p.p_type.StartsWith("MEDIUM POLISHED")
                                              && targetSizes.Contains(p.p_size));

        var partPartSupp = from ps in partsuppQuery
                           where !complaintsSuppliers.Contains(ps.ps_suppkey)
                           join p in filteredParts on ps.ps_partkey equals p.p_partkey
                           select new { ps.ps_suppkey, p.p_brand, p.p_type, p.p_size };

        var query = from x in partPartSupp
                    group x by new { x.p_brand, x.p_type, x.p_size } into g
                    orderby g.Select(s => s.ps_suppkey).Distinct().Count() descending,
                            g.Key.p_brand,
                            g.Key.p_type,
                            g.Key.p_size
                    select new
                    {
                        g.Key.p_brand,
                        g.Key.p_type,
                        g.Key.p_size,
                        supplier_cnt = g.Select(s => s.ps_suppkey).Distinct().Count()
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(296, results.Count);

        var row0 = results[0];
        Assert.Equal("Brand#14", row0.p_brand);
        Assert.Equal("PROMO BRUSHED STEEL", row0.p_type);
        Assert.Equal(9, row0.p_size);
        Assert.Equal(8, row0.supplier_cnt);

        var row1 = results[1];
        Assert.Equal("Brand#35", row1.p_brand);
        Assert.Equal("SMALL POLISHED COPPER", row1.p_type);
        Assert.Equal(14, row1.p_size);
        Assert.Equal(8, row1.supplier_cnt);

        var row2 = results[2];
        Assert.Equal("Brand#22", row2.p_brand);
        Assert.Equal("LARGE BURNISHED TIN", row2.p_type);
        Assert.Equal(36, row2.p_size);
        Assert.Equal(6, row2.supplier_cnt);
    }

    /*
     * TPC-H Query 17: Small-Quantity-Order Revenue Query
     *
     * SELECT
     *     sum(l_extendedprice) / 7.0 AS avg_yearly
     * FROM
     *     lineitem,
     *     part
     * WHERE
     *     p_partkey = l_partkey
     *     AND p_brand = 'Brand#23'
     *     AND p_container = 'MED BOX'
     *     AND l_quantity < (
     *         SELECT
     *             0.2 * avg(l_quantity)
     *         FROM
     *             lineitem
     *         WHERE
     *             l_partkey = p_partkey
     *     );
     */
    [Fact]
    [Trait("TPCH", "Q17")]
    public void Test_Q17_Small_Quantity_Order_Revenue()
    {
        using var lfLineItem = TpchData.LineItem();
        using var lfPart = TpchData.Part();

        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var partQuery = lfPart.AsQueryable<Part>();

        // 1. Standard TPC-H Spec Parameters: Brand#23, MED BOX (Matches 0 parts in SF=0.01)
        var standardQuery = from l in lineitemQuery
                            where l.l_quantity < 0.2m * lineitemQuery
                                                     .Where(l2 => l2.l_partkey == l.l_partkey)
                                                     .Average(l2 => l2.l_quantity)
                            join p in partQuery.Where(p => p.p_brand == "Brand#23" && p.p_container == "MED BOX")
                                                     on l.l_partkey equals p.p_partkey
                            group l by 1 into g
                            select new
                            {
                                avg_yearly = (double)g.Sum(x => x.l_extendedprice) / 7.0
                            };

        AssertPlan(standardQuery, NativePushdown);

        var standardResults = standardQuery.ToList();
        Assert.Single(standardResults);
        Assert.Equal(0.0, standardResults[0].avg_yearly);

        // 2. Active Dataset Case on SF=0.01: Brand#23, MED DRUM (Has 11 matching lineitems, verified by DuckDB)
        var activeQuery = from l in lineitemQuery
                          where l.l_quantity < 0.2m * lineitemQuery
                                                   .Where(l2 => l2.l_partkey == l.l_partkey)
                                                   .Average(l2 => l2.l_quantity)
                          join p in partQuery.Where(p => p.p_brand == "Brand#23" && p.p_container == "MED DRUM")
                                                   on l.l_partkey equals p.p_partkey
                          group l by 1 into g
                          select new
                          {
                              avg_yearly = (double)g.Sum(x => x.l_extendedprice) / 7.0
                          };

        AssertPlan(activeQuery, NativePushdown);

        var activeResults = activeQuery.ToList();
        Assert.Single(activeResults);
        Assert.Equal(7590.6514, activeResults[0].avg_yearly, precision: 4);
    }

    /*
     * TPC-H Query 18: Large Volume Customer Query
     *
     * SELECT
     *     c_name,
     *     c_custkey,
     *     o_orderkey,
     *     o_orderdate,
     *     o_totalprice,
     *     sum(l_quantity)
     * FROM
     *     customer,
     *     orders,
     *     lineitem
     * WHERE
     *     o_orderkey IN (
     *         SELECT
     *             l_orderkey
     *         FROM
     *             lineitem
     *         GROUP BY
     *             l_orderkey
     *         HAVING
     *             sum(l_quantity) > 300
     *     )
     *     AND c_custkey = o_custkey
     *     AND o_orderkey = l_orderkey
     * GROUP BY
     *     c_name,
     *     c_custkey,
     *     o_orderkey,
     *     o_orderdate,
     *     o_totalprice
     * ORDER BY
     *     o_totalprice DESC,
     *     o_orderdate
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCH", "Q18")]
    public void Test_Q18_Large_Volume_Customer()
    {
        using var lfCustomer = TpchData.Customer();
        using var lfOrders = TpchData.Orders();
        using var lfLineItem = TpchData.LineItem();

        var customerQuery = lfCustomer.AsQueryable<Customer>();
        var ordersQuery = lfOrders.AsQueryable<Orders>();
        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();

        var bigOrders = from l in lineitemQuery
                        group l by l.l_orderkey into g
                        select new
                        {
                            l_orderkey = g.Key,
                            total_qty = g.Sum(x => x.l_quantity)
                        } into og
                        where og.total_qty > 300m
                        select og.l_orderkey;

        AssertPlan(bigOrders, NativePushdown);

        var filteredOrders = from o in ordersQuery
                             where bigOrders.Contains(o.o_orderkey)
                             select o;

        var query = (from c in customerQuery
                     join o in filteredOrders on c.c_custkey equals o.o_custkey
                     join l in lineitemQuery on o.o_orderkey equals l.l_orderkey
                     group l by new { c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice } into g
                     orderby g.Key.o_totalprice descending, g.Key.o_orderdate
                     select new
                     {
                         g.Key.c_name,
                         g.Key.c_custkey,
                         g.Key.o_orderkey,
                         g.Key.o_orderdate,
                         g.Key.o_totalprice,
                         sum_qty = g.Sum(x => x.l_quantity)
                     }).Take(100);

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Equal(2, results.Count);

        var row0 = results[0];
        Assert.Equal("Customer#000000667", row0.c_name);
        Assert.Equal(667, row0.c_custkey);
        Assert.Equal(29158, row0.o_orderkey);
        Assert.Equal(new DateOnly(1995, 10, 21), row0.o_orderdate);
        Assert.Equal(439687.23m, row0.o_totalprice);
        Assert.Equal(305.00m, row0.sum_qty);

        var row1 = results[1];
        Assert.Equal("Customer#000000178", row1.c_name);
        Assert.Equal(178, row1.c_custkey);
        Assert.Equal(6882, row1.o_orderkey);
        Assert.Equal(new DateOnly(1997, 4, 9), row1.o_orderdate);
        Assert.Equal(422359.65m, row1.o_totalprice);
        Assert.Equal(303.00m, row1.sum_qty);
    }

    /*
     * TPC-H Q19: Discounted Revenue Query
     *
     * SELECT
     *     sum(l_extendedprice * (1 - l_discount)) AS revenue
     * FROM
     *     lineitem,
     *     part
     * WHERE
     *     (
     *         p_partkey = l_partkey
     *         AND p_brand = 'Brand#12'
     *         AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
     *         AND l_quantity >= 1 AND l_quantity <= 1 + 10
     *         AND p_size BETWEEN 1 AND 5
     *         AND l_shipmode IN ('AIR', 'AIR REG')
     *         AND l_shipinstruct = 'DELIVER IN PERSON'
     *     )
     *     OR
     *     (
     *         p_partkey = l_partkey
     *         AND p_brand = 'Brand#23'
     *         AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
     *         AND l_quantity >= 10 AND l_quantity <= 10 + 10
     *         AND p_size BETWEEN 1 AND 10
     *         AND l_shipmode IN ('AIR', 'AIR REG')
     *         AND l_shipinstruct = 'DELIVER IN PERSON'
     *     )
     *     OR
     *     (
     *         p_partkey = l_partkey
     *         AND p_brand = 'Brand#34'
     *         AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
     *         AND l_quantity >= 20 AND l_quantity <= 20 + 10
     *         AND p_size BETWEEN 1 AND 15
     *         AND l_shipmode IN ('AIR', 'AIR REG')
     *         AND l_shipinstruct = 'DELIVER IN PERSON'
     *     );
     */
    [Fact]
    [Trait("TPCH", "Q19")]
    public void Test_Q19_Discounted_Revenue()
    {
        using var lfLineItem = TpchData.LineItem();
        using var lfPart = TpchData.Part();

        var lineitemQuery = lfLineItem.AsQueryable<LineItem>();
        var partQuery = lfPart.AsQueryable<Part>();

        var smContainers = new[] { "SM CASE", "SM BOX", "SM PACK", "SM PKG" };
        var medContainers = new[] { "MED BAG", "MED BOX", "MED PKG", "MED PACK" };
        var lgContainers = new[] { "LG CASE", "LG BOX", "LG PACK", "LG PKG" };
        var shipModes = new[] { "AIR", "AIR REG" };

        var query = from l in lineitemQuery
                    join p in partQuery on l.l_partkey equals p.p_partkey
                    where ((p.p_brand == "Brand#12"
                            && smContainers.Contains(p.p_container)
                            && l.l_quantity >= 1m && l.l_quantity <= 11m
                            && p.p_size >= 1 && p.p_size <= 5
                            && shipModes.Contains(l.l_shipmode)
                            && l.l_shipinstruct == "DELIVER IN PERSON")
                        || (p.p_brand == "Brand#23"
                            && medContainers.Contains(p.p_container)
                            && l.l_quantity >= 10m && l.l_quantity <= 20m
                            && p.p_size >= 1 && p.p_size <= 10
                            && shipModes.Contains(l.l_shipmode)
                            && l.l_shipinstruct == "DELIVER IN PERSON")
                        || (p.p_brand == "Brand#34"
                            && lgContainers.Contains(p.p_container)
                            && l.l_quantity >= 20m && l.l_quantity <= 30m
                            && p.p_size >= 1 && p.p_size <= 15
                            && shipModes.Contains(l.l_shipmode)
                            && l.l_shipinstruct == "DELIVER IN PERSON"))
                    group l by 1 into g
                    select new
                    {
                        revenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount))
                    };

        AssertPlan(query, NativePushdown);

        var results = query.ToList();

        Assert.Single(results);
        // Ground truth verification against DuckDB TPCH (SF = 0.01):
        // DuckDB DECIMAL(15,2) * DECIMAL(15,2) produces DECIMAL(18,4) = 22923.0280.
        // Polars native Decimal arithmetic retains scale 2 (Decimal(38, 2)), rounding to 22923.03m.
        Assert.Equal(22923.03m, results[0].revenue);
        Assert.Equal(22923.0280, (double)results[0].revenue, precision: 1);
    }
}










