
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

public class TpchLinqTests
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
}

