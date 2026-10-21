namespace Polars.FSharp.Tests

open System
open System.IO
open System.Linq
open System.Text.RegularExpressions
open Xunit
open Polars.FSharp
open Polars.FSharp.Query

// =========================================================================
// TPC-H Schema Records
// =========================================================================

type Customer =
    {
        c_custkey: int64
        c_name: string
        c_address: string
        c_nationkey: int
        c_phone: string
        c_acctbal: decimal
        c_mktsegment: string
        c_comment: string
    }

type LineItem =
    {
        l_orderkey: int64
        l_partkey: int64
        l_suppkey: int64
        l_linenumber: int64
        l_quantity: decimal
        l_extendedprice: decimal
        l_discount: decimal
        l_tax: decimal
        l_returnflag: string
        l_linestatus: string
        l_shipdate: DateOnly
        l_commitdate: DateOnly
        l_receiptdate: DateOnly
        l_shipinstruct: string
        l_shipmode: string
        l_comment: string
    }

type Nation =
    {
        n_nationkey: int
        n_name: string
        n_regionkey: int
        n_comment: string
    }

type Orders =
    {
        o_orderkey: int64
        o_custkey: int64
        o_orderstatus: string
        o_totalprice: decimal
        o_orderdate: DateOnly
        o_orderpriority: string
        o_clerk: string
        o_shippriority: int
        o_comment: string
    }

type Part =
    {
        p_partkey: int64
        p_name: string
        p_mfgr: string
        p_brand: string
        p_type: string
        p_size: int
        p_container: string
        p_retailprice: decimal
        p_comment: string
    }

type PartSupp =
    {
        ps_partkey: int64
        ps_suppkey: int64
        ps_availqty: int64
        ps_supplycost: decimal
        ps_comment: string
    }

type Region =
    {
        r_regionkey: int
        r_name: string
        r_comment: string
    }

type Supplier =
    {
        s_suppkey: int64
        s_name: string
        s_address: string
        s_nationkey: int
        s_phone: string
        s_acctbal: decimal
        s_comment: string
    }

module TpchData =
    let private dataDir =
        lazy
            let appBase = AppContext.BaseDirectory
            let candidate = Path.Combine(appBase, "Data")
            if Directory.Exists candidate && File.Exists(Path.Combine(candidate, "lineitem.parquet")) then
                Path.GetFullPath candidate
            else
                let rec findUp (d: DirectoryInfo) =
                    if isNull d then
                        failwith "Could not find TPC-H parquet data directory. Run 'Polars.TPCH.Tests/generate_tpch_data.sh' to generate test data."
                    let projData = Path.Combine(d.FullName, "Polars.TPCH.Tests", "Data")
                    if Directory.Exists projData && File.Exists(Path.Combine(projData, "lineitem.parquet")) then
                        Path.GetFullPath projData
                    else
                        let direct = Path.Combine(d.FullName, "Data")
                        if Directory.Exists direct && File.Exists(Path.Combine(direct, "lineitem.parquet")) then
                            Path.GetFullPath direct
                        else
                            findUp d.Parent
                findUp (DirectoryInfo appBase)

    let DataDir = dataDir.Value

    let CustomerPath = Path.Combine(DataDir, "customer.parquet")
    let LineItemPath = Path.Combine(DataDir, "lineitem.parquet")
    let NationPath = Path.Combine(DataDir, "nation.parquet")
    let OrdersPath = Path.Combine(DataDir, "orders.parquet")
    let PartPath = Path.Combine(DataDir, "part.parquet")
    let PartSuppPath = Path.Combine(DataDir, "partsupp.parquet")
    let RegionPath = Path.Combine(DataDir, "region.parquet")
    let SupplierPath = Path.Combine(DataDir, "supplier.parquet")

    let Customer () = LazyFrame.ScanParquet CustomerPath
    let LineItem () = LazyFrame.ScanParquet LineItemPath
    let Nation () = LazyFrame.ScanParquet NationPath
    let Orders () = LazyFrame.ScanParquet OrdersPath
    let Part () = LazyFrame.ScanParquet PartPath
    let PartSupp () = LazyFrame.ScanParquet PartSuppPath
    let Region () = LazyFrame.ScanParquet RegionPath
    let Supplier () = LazyFrame.ScanParquet SupplierPath

module TpchLinqTests =

    let private assertPlan (expectedPlan: string) (query: IQueryable<'T>) =
        let actualPlan = query.Explain()
        if not (actualPlan.Contains expectedPlan) then
            failwithf "Plan mismatch:\nExpected: %s\nActual:\n%s\nExpression:\n%s\nProvider: %A" expectedPlan actualPlan (query.Expression.ToString()) (box query.Provider)

    [<Literal>]
    let NativePushdown = "100% Native Pushdown"

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q1")>]
    let ``Test_Q1_Pricing_Summary_Report`` () =
        use lf = TpchData.LineItem()
        let cutoff = DateOnly(1998, 9, 2)

        let query =
            query {
                for l in lf.AsQueryable<LineItem>() do
                where (l.l_shipdate <= cutoff)
                groupBy (l.l_returnflag, l.l_linestatus) into g
                sortBy (fst g.Key)
                thenBy (snd g.Key)
                select
                    {|
                        l_returnflag = fst g.Key
                        l_linestatus = snd g.Key
                        sum_qty = g.Sum(fun x -> x.l_quantity)
                        sum_base_price = g.Sum(fun x -> x.l_extendedprice)
                        sum_disc_price = g.Sum(fun x -> x.l_extendedprice * (1.0m - x.l_discount))
                        sum_charge = g.Sum(fun x -> x.l_extendedprice * (1.0m - x.l_discount) * (1.0m + x.l_tax))
                        avg_qty = g.Average(fun x -> double x.l_quantity)
                        avg_price = g.Average(fun x -> double x.l_extendedprice)
                        avg_disc = g.Average(fun x -> double x.l_discount)
                        count_order = g.Count()
                    |}
            }

        assertPlan NativePushdown query
        let results = query.ToList()
        Assert.Equal(4, results.Count)

        // Ground truth verification against DuckDB TPCH (scale factor = 0.01)
        let row0 = results.[0]
        Assert.Equal("A", row0.l_returnflag)
        Assert.Equal("F", row0.l_linestatus)
        Assert.Equal(14876, row0.count_order)
        Assert.Equal(380456.00m, row0.sum_qty)
        Assert.Equal(532348211.65m, row0.sum_base_price)
        Assert.Equal(25.575154611454693, row0.avg_qty, 6)
        Assert.Equal(35785.70930693735, row0.avg_price, 6)
        Assert.Equal(0.05008133906964238, row0.avg_disc, 6)

        let row1 = results.[1]
        Assert.Equal("N", row1.l_returnflag)
        Assert.Equal("F", row1.l_linestatus)
        Assert.Equal(348, row1.count_order)
        Assert.Equal(8971.00m, row1.sum_qty)
        Assert.Equal(12384801.37m, row1.sum_base_price)

        let row2 = results.[2]
        Assert.Equal("N", row2.l_returnflag)
        Assert.Equal("O", row2.l_linestatus)
        Assert.Equal(29181, row2.count_order)
        Assert.Equal(742802.00m, row2.sum_qty)
        Assert.Equal(1041502841.45m, row2.sum_base_price)

        let row3 = results.[3]
        Assert.Equal("R", row3.l_returnflag)
        Assert.Equal("F", row3.l_linestatus)
        Assert.Equal(14902, row3.count_order)
        Assert.Equal(381449.00m, row3.sum_qty)
        Assert.Equal(534594445.35m, row3.sum_base_price)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q2")>]
    let ``Test_Q2_Minimum_Cost_Supplier`` () =
        use lfPart = TpchData.Part()
        use lfSupplier = TpchData.Supplier()
        use lfPartSupp = TpchData.PartSupp()
        use lfNation = TpchData.Nation()
        use lfRegion = TpchData.Region()

        let partQuery = lfPart.AsQueryable<Part>()
        let supplierQuery = lfSupplier.AsQueryable<Supplier>()
        let partsuppQuery = lfPartSupp.AsQueryable<PartSupp>()
        let nationQuery = lfNation.AsQueryable<Nation>()
        let regionQuery = lfRegion.AsQueryable<Region>()

        let minCostQuery =
            query {
                for ps in partsuppQuery do
                join s in supplierQuery on (ps.ps_suppkey = s.s_suppkey)
                join n in nationQuery on (s.s_nationkey = n.n_nationkey)
                join r in regionQuery on (n.n_regionkey = r.r_regionkey)
                where (r.r_name = "EUROPE")
                groupValBy ps ps.ps_partkey into g
                select
                    {|
                        ps_partkey = g.Key
                        min_cost = g.Min(fun (x: PartSupp) -> x.ps_supplycost)
                    |}
            }

        assertPlan NativePushdown minCostQuery

        let query =
            query {
                for p in partQuery do
                where (p.p_size = 15 && p.p_type.EndsWith("BRASS"))
                join ps in partsuppQuery on (p.p_partkey = ps.ps_partkey)
                join mc in minCostQuery on ((ps.ps_partkey, ps.ps_supplycost) = (mc.ps_partkey, mc.min_cost))
                join s in supplierQuery on (ps.ps_suppkey = s.s_suppkey)
                join n in nationQuery on (s.s_nationkey = n.n_nationkey)
                join r in regionQuery on (n.n_regionkey = r.r_regionkey)
                where (r.r_name = "EUROPE")
                sortByDescending s.s_acctbal
                thenBy n.n_name
                thenBy s.s_name
                thenBy p.p_partkey
                take 100
                select
                    {|
                        s_acctbal = s.s_acctbal
                        s_name = s.s_name
                        n_name = n.n_name
                        p_partkey = p.p_partkey
                        p_mfgr = p.p_mfgr
                        s_address = s.s_address
                        s_phone = s.s_phone
                        s_comment = s.s_comment
                    |}
            }

        assertPlan NativePushdown query
        let results = query.ToList()
        Assert.Equal(4, results.Count)

        let row0 = results.[0]
        Assert.Equal(4186.95m, row0.s_acctbal)
        Assert.Equal("Supplier#000000077", row0.s_name)
        Assert.Equal("GERMANY", row0.n_name)
        Assert.Equal(249L, row0.p_partkey)
        Assert.Equal("Manufacturer#4", row0.p_mfgr)

        let row1 = results.[1]
        Assert.Equal(1883.37m, row1.s_acctbal)
        Assert.Equal("Supplier#000000086", row1.s_name)
        Assert.Equal("ROMANIA", row1.n_name)
        Assert.Equal(1015L, row1.p_partkey)
        Assert.Equal("Manufacturer#4", row1.p_mfgr)

        let row2 = results.[2]
        Assert.Equal(1687.81m, row2.s_acctbal)
        Assert.Equal("Supplier#000000017", row2.s_name)
        Assert.Equal("ROMANIA", row2.n_name)
        Assert.Equal(1634L, row2.p_partkey)
        Assert.Equal("Manufacturer#2", row2.p_mfgr)

        let row3 = results.[3]
        Assert.Equal(287.16m, row3.s_acctbal)
        Assert.Equal("Supplier#000000052", row3.s_name)
        Assert.Equal("ROMANIA", row3.n_name)
        Assert.Equal(323L, row3.p_partkey)
        Assert.Equal("Manufacturer#4", row3.p_mfgr)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q3")>]
    let ``Test_Q3_Shipping_Priority`` () =
        use lfCustomer = TpchData.Customer()
        use lfOrders = TpchData.Orders()
        use lfLineItem = TpchData.LineItem()

        let customerQuery = lfCustomer.AsQueryable<Customer>()
        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()

        let cutoff = DateOnly(1995, 3, 15)

        let baseQuery =
            query {
                for c in customerQuery do
                where (c.c_mktsegment = "BUILDING")
                join o in ordersQuery on (c.c_custkey = o.o_custkey)
                where (o.o_orderdate < cutoff)
                join l in lineitemQuery on (o.o_orderkey = l.l_orderkey)
                where (l.l_shipdate > cutoff)
                groupValBy l {| l_orderkey = l.l_orderkey; o_orderdate = o.o_orderdate; o_shippriority = o.o_shippriority |} into g
                select
                    {|
                        l_orderkey = g.Key.l_orderkey
                        revenue = g.Sum(fun x -> x.l_extendedprice * (1.0m - x.l_discount))
                        o_orderdate = g.Key.o_orderdate
                        o_shippriority = g.Key.o_shippriority
                    |}
            }

        assertPlan NativePushdown baseQuery

        let query =
            query {
                for x in baseQuery do
                sortByDescending x.revenue
                thenBy x.o_orderdate
                take 10
            }

        assertPlan NativePushdown query
        let results = query.ToList()

        let row0 = results.[0]
        Assert.Equal(47714L, row0.l_orderkey)
        Assert.Equal(267010.59m, row0.revenue)
        Assert.Equal(DateOnly(1995, 3, 11), row0.o_orderdate)
        Assert.Equal(0, row0.o_shippriority)

        let row1 = results.[1]
        Assert.Equal(22276L, row1.l_orderkey)
        Assert.Equal(266351.57m, row1.revenue)
        Assert.Equal(DateOnly(1995, 1, 29), row1.o_orderdate)
        Assert.Equal(0, row1.o_shippriority)

        let row9 = results.[9]
        Assert.Equal(9696L, row9.l_orderkey)
        Assert.Equal(201502.21m, row9.revenue)
        Assert.Equal(DateOnly(1995, 2, 20), row9.o_orderdate)
        Assert.Equal(0, row9.o_shippriority)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q4")>]
    let ``Test_Q4_Order_Priority_Checking`` () =
        use lfOrders = TpchData.Orders()
        use lfLineItem = TpchData.LineItem()

        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()

        let startDate = DateOnly(1993, 7, 1)
        let endDate = DateOnly(1993, 10, 1)

        let query =
            query {
                for o in ordersQuery do
                where (o.o_orderdate >= startDate && o.o_orderdate < endDate
                       && query {
                           for l in lineitemQuery do
                           exists (l.l_orderkey = o.o_orderkey && l.l_commitdate < l.l_receiptdate)
                       })
                groupBy o.o_orderpriority into g
                sortBy g.Key
                select
                    {|
                        o_orderpriority = g.Key
                        order_count = g.Count()
                    |}
            }

        assertPlan NativePushdown query
        let results = query.ToList()
        Assert.Equal(5, results.Count)

        let row0 = results.[0]
        Assert.Equal("1-URGENT", row0.o_orderpriority)
        Assert.Equal(93, row0.order_count)

        let row1 = results.[1]
        Assert.Equal("2-HIGH", row1.o_orderpriority)
        Assert.Equal(103, row1.order_count)

        let row2 = results.[2]
        Assert.Equal("3-MEDIUM", row2.o_orderpriority)
        Assert.Equal(109, row2.order_count)

        let row3 = results.[3]
        Assert.Equal("4-NOT SPECIFIED", row3.o_orderpriority)
        Assert.Equal(102, row3.order_count)

        let row4 = results.[4]
        Assert.Equal("5-LOW", row4.o_orderpriority)
        Assert.Equal(128, row4.order_count)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q5")>]
    let ``Test_Q5_Local_Supplier_Volume`` () : unit =
        use lfCustomer = TpchData.Customer()
        use lfOrders = TpchData.Orders()
        use lfLineItem = TpchData.LineItem()
        use lfSupplier = TpchData.Supplier()
        use lfNation = TpchData.Nation()
        use lfRegion = TpchData.Region()

        let customerQuery = lfCustomer.AsQueryable<Customer>()
        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()
        let supplierQuery = lfSupplier.AsQueryable<Supplier>()
        let nationQuery = lfNation.AsQueryable<Nation>()
        let regionQuery = lfRegion.AsQueryable<Region>()

        let startDate = DateOnly(1994, 1, 1)
        let endDate = DateOnly(1995, 1, 1)

        let query =
            query {
                for c in customerQuery do
                join o in ordersQuery on (c.c_custkey = o.o_custkey)
                where (o.o_orderdate >= startDate && o.o_orderdate < endDate)
                join l in lineitemQuery on (o.o_orderkey = l.l_orderkey)
                join s in supplierQuery on ((l.l_suppkey, c.c_nationkey) = (s.s_suppkey, s.s_nationkey))
                join n in nationQuery on (s.s_nationkey = n.n_nationkey)
                join r in regionQuery on (n.n_regionkey = r.r_regionkey)
                where (r.r_name = "ASIA")
                groupValBy l n.n_name into g
                sortByDescending (g.Sum(fun x -> x.l_extendedprice * (1.0m - x.l_discount)))
                select
                    {|
                        n_name = g.Key
                        revenue = g.Sum(fun x -> x.l_extendedprice * (1.0m - x.l_discount))
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(5, results.Count)

        let row0 = results.[0]
        Assert.Equal("VIETNAM", row0.n_name)
        Assert.Equal(1000926.71m, row0.revenue)

        let row1 = results.[1]
        Assert.Equal("CHINA", row1.n_name)
        Assert.Equal(740210.76m, row1.revenue)

        let row2 = results.[2]
        Assert.Equal("JAPAN", row2.n_name)
        Assert.Equal(660651.24m, row2.revenue)

        let row3 = results.[3]
        Assert.Equal("INDONESIA", row3.n_name)
        Assert.Equal(566379.52m, row3.revenue)

        let row4 = results.[4]
        Assert.Equal("INDIA", row4.n_name)
        Assert.Equal(422874.70m, row4.revenue)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q6")>]
    let ``Test_Q6_Forecasting_Revenue_Change`` () =
        use lfLineItem = TpchData.LineItem()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()

        let startDate = DateOnly(1994, 1, 1)
        let endDate = DateOnly(1995, 1, 1)

        let query =
            query {
                for l in lineitemQuery do
                where (l.l_shipdate >= startDate && l.l_shipdate < endDate
                       && l.l_discount >= 0.05m && l.l_discount <= 0.07m
                       && l.l_quantity < 24m)
                groupBy 1 into g
                select
                    {|
                        revenue = g.Sum(fun x -> x.l_extendedprice * x.l_discount)
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Single(results) |> ignore

        let row0 = results.[0]
        Assert.Equal(1193053.17m, row0.revenue)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q7")>]
    let ``Test_Q7_Volume_Shipping`` () =
        use lfSupplier = TpchData.Supplier()
        use lfLineItem = TpchData.LineItem()
        use lfOrders = TpchData.Orders()
        use lfCustomer = TpchData.Customer()
        use lfNation = TpchData.Nation()

        let supplierQuery = lfSupplier.AsQueryable<Supplier>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()
        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let customerQuery = lfCustomer.AsQueryable<Customer>()
        let nationQuery = lfNation.AsQueryable<Nation>()

        let startDate = DateOnly(1995, 1, 1)
        let endDate = DateOnly(1996, 12, 31)

        let n1Query = nationQuery.Select(fun n -> {| supp_nationkey = n.n_nationkey; supp_nation = n.n_name |})
        let n2Query = nationQuery.Select(fun n -> {| cust_nationkey = n.n_nationkey; cust_nation = n.n_name |})

        let query =
            query {
                for s in supplierQuery do
                join n1 in n1Query on (s.s_nationkey = n1.supp_nationkey)
                join l in lineitemQuery on (s.s_suppkey = l.l_suppkey)
                where (l.l_shipdate >= startDate && l.l_shipdate <= endDate)
                join o in ordersQuery on (l.l_orderkey = o.o_orderkey)
                join c in customerQuery on (o.o_custkey = c.c_custkey)
                join n2 in n2Query on (c.c_nationkey = n2.cust_nationkey)
                where (n1.supp_nation = "FRANCE" && n2.cust_nation = "GERMANY"
                       || n1.supp_nation = "GERMANY" && n2.cust_nation = "FRANCE")
                groupValBy l {| supp_nation = n1.supp_nation; cust_nation = n2.cust_nation; l_year = l.l_shipdate.Year |} into g
                sortBy g.Key.supp_nation
                thenBy g.Key.cust_nation
                thenBy g.Key.l_year
                select
                    {|
                        supp_nation = g.Key.supp_nation
                        cust_nation = g.Key.cust_nation
                        l_year = g.Key.l_year
                        revenue = g.Sum(fun x -> x.l_extendedprice * (1.0m - x.l_discount))
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(4, results.Count)

        let row0 = results.[0]
        Assert.Equal("FRANCE", row0.supp_nation)
        Assert.Equal("GERMANY", row0.cust_nation)
        Assert.Equal(1995, row0.l_year)
        Assert.Equal(268068.57m, row0.revenue)

        let row1 = results.[1]
        Assert.Equal("FRANCE", row1.supp_nation)
        Assert.Equal("GERMANY", row1.cust_nation)
        Assert.Equal(1996, row1.l_year)
        Assert.Equal(303862.29m, row1.revenue)

        let row2 = results.[2]
        Assert.Equal("GERMANY", row2.supp_nation)
        Assert.Equal("FRANCE", row2.cust_nation)
        Assert.Equal(1995, row2.l_year)
        Assert.Equal(621159.50m, row2.revenue)

        let row3 = results.[3]
        Assert.Equal("GERMANY", row3.supp_nation)
        Assert.Equal("FRANCE", row3.cust_nation)
        Assert.Equal(1996, row3.l_year)
        Assert.Equal(379095.89m, row3.revenue)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q8")>]
    let ``Test_Q8_National_Market_Share`` () =
        use lfPart = TpchData.Part()
        use lfSupplier = TpchData.Supplier()
        use lfLineItem = TpchData.LineItem()
        use lfOrders = TpchData.Orders()
        use lfCustomer = TpchData.Customer()
        use lfNation = TpchData.Nation()
        use lfRegion = TpchData.Region()

        let partQuery = lfPart.AsQueryable<Part>()
        let supplierQuery = lfSupplier.AsQueryable<Supplier>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()
        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let customerQuery = lfCustomer.AsQueryable<Customer>()
        let nationQuery = lfNation.AsQueryable<Nation>()
        let regionQuery = lfRegion.AsQueryable<Region>()

        let startDate = DateOnly(1995, 1, 1)
        let endDate = DateOnly(1996, 12, 31)

        let n2Query = nationQuery.Select(fun n -> {| supp_nationkey = n.n_nationkey; supp_nation = n.n_name |})

        let subQuery =
            query {
                for p in partQuery do
                where (p.p_type = "ECONOMY ANODIZED STEEL")
                join l in lineitemQuery on (p.p_partkey = l.l_partkey)
                join s in supplierQuery on (l.l_suppkey = s.s_suppkey)
                join n2 in n2Query on (s.s_nationkey = n2.supp_nationkey)
                join o in ordersQuery on (l.l_orderkey = o.o_orderkey)
                where (o.o_orderdate >= startDate && o.o_orderdate <= endDate)
                join c in customerQuery on (o.o_custkey = c.c_custkey)
                join n1 in nationQuery on (c.c_nationkey = n1.n_nationkey)
                join r in regionQuery on (n1.n_regionkey = r.r_regionkey)
                where (r.r_name = "AMERICA")
                select
                    {|
                        o_year = o.o_orderdate.Year
                        volume = l.l_extendedprice * (1.0m - l.l_discount)
                        nation = n2.supp_nation
                    |}
            }

        assertPlan NativePushdown subQuery

        let baseQuery =
            query {
                for a in subQuery do
                groupBy a.o_year into g
                sortBy g.Key
                select
                    {|
                        o_year = g.Key
                        brazil_volume = g.Sum(fun x -> if x.nation = "BRAZIL" then x.volume else 0.0m)
                        total_volume = g.Sum(fun x -> x.volume)
                    |}
            }

        assertPlan NativePushdown baseQuery

        let query =
            query {
                for x in baseQuery do
                select
                    {|
                        o_year = x.o_year
                        mkt_share = double (x.brazil_volume / x.total_volume)
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(2, results.Count)

        let row0 = results.[0]
        Assert.Equal(1995, row0.o_year)
        Assert.Equal(0.0, row0.mkt_share, precision = 4)

        let last = results.[1]
        Assert.Equal(1996, last.o_year)
        Assert.Equal(0.0, last.mkt_share, precision = 4)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q9")>]
    let ``Test_Q9_Product_Type_Profit_Measure`` () =
        use lfPart = TpchData.Part()
        use lfSupplier = TpchData.Supplier()
        use lfLineItem = TpchData.LineItem()
        use lfPartSupp = TpchData.PartSupp()
        use lfOrders = TpchData.Orders()
        use lfNation = TpchData.Nation()

        let partQuery = lfPart.AsQueryable<Part>()
        let supplierQuery = lfSupplier.AsQueryable<Supplier>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()
        let partsuppQuery = lfPartSupp.AsQueryable<PartSupp>()
        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let nationQuery = lfNation.AsQueryable<Nation>()

        let subQuery =
            query {
                for p in partQuery do
                where (p.p_name.Contains "green")
                join l in lineitemQuery on (p.p_partkey = l.l_partkey)
                join ps in partsuppQuery on ((l.l_partkey, l.l_suppkey) = (ps.ps_partkey, ps.ps_suppkey))
                join s in supplierQuery on (l.l_suppkey = s.s_suppkey)
                join o in ordersQuery on (l.l_orderkey = o.o_orderkey)
                join n in nationQuery on (s.s_nationkey = n.n_nationkey)
                select
                    {|
                        nation = n.n_name
                        o_year = o.o_orderdate.Year
                        amount = l.l_extendedprice * (1.0m - l.l_discount) - ps.ps_supplycost * l.l_quantity
                    |}
            }

        assertPlan NativePushdown subQuery

        let query =
            query {
                for pr in subQuery do
                groupBy (pr.nation, pr.o_year) into g
                sortBy (fst g.Key)
                thenByDescending (snd g.Key)
                select
                    {|
                        nation = fst g.Key
                        o_year = snd g.Key
                        sum_profit = g.Sum(fun x -> x.amount)
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(173, results.Count)

        let row0 = results.[0]
        Assert.Equal("ALGERIA", row0.nation)
        Assert.Equal(1998, row0.o_year)
        Assert.Equal(97864.58m, row0.sum_profit)

        let row1 = results.[1]
        Assert.Equal("ALGERIA", row1.nation)
        Assert.Equal(1997, row1.o_year)
        Assert.Equal(368231.67m, row1.sum_profit)

        let row2 = results.[2]
        Assert.Equal("ALGERIA", row2.nation)
        Assert.Equal(1996, row2.o_year)
        Assert.Equal(196525.81m, row2.sum_profit)

        let last = results.[results.Count - 1]
        Assert.Equal("VIETNAM", last.nation)
        Assert.Equal(1992, last.o_year)
        Assert.Equal(716126.53m, last.sum_profit)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q10")>]
    let ``Test_Q10_Returned_Item_Reporting`` () =
        use lfCustomer = TpchData.Customer()
        use lfOrders = TpchData.Orders()
        use lfLineItem = TpchData.LineItem()
        use lfNation = TpchData.Nation()

        let customerQuery = lfCustomer.AsQueryable<Customer>()
        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()
        let nationQuery = lfNation.AsQueryable<Nation>()

        let startDate = DateOnly(1993, 10, 1)
        let endDate = DateOnly(1994, 1, 1)

        let baseQuery =
            query {
                for c in customerQuery do
                join o in ordersQuery on (c.c_custkey = o.o_custkey)
                where (o.o_orderdate >= startDate && o.o_orderdate < endDate)
                join l in lineitemQuery on (o.o_orderkey = l.l_orderkey)
                where (l.l_returnflag = "R")
                join n in nationQuery on (c.c_nationkey = n.n_nationkey)
                groupValBy l
                    {|
                        c_custkey = c.c_custkey
                        c_name = c.c_name
                        c_acctbal = c.c_acctbal
                        n_name = n.n_name
                        c_address = c.c_address
                        c_phone = c.c_phone
                        c_comment = c.c_comment
                    |} into g
                select
                    {|
                        c_custkey = g.Key.c_custkey
                        c_name = g.Key.c_name
                        revenue = g.Sum(fun x -> x.l_extendedprice * (1.0m - x.l_discount))
                        c_acctbal = g.Key.c_acctbal
                        n_name = g.Key.n_name
                        c_address = g.Key.c_address
                        c_phone = g.Key.c_phone
                        c_comment = g.Key.c_comment
                    |}
            }

        assertPlan NativePushdown baseQuery

        let query =
            query {
                for x in baseQuery do
                sortByDescending x.revenue
                take 20
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(20, results.Count)

        let row0 = results.[0]
        Assert.Equal(679L, row0.c_custkey)
        Assert.Equal("Customer#000000679", row0.c_name)
        Assert.Equal(378211.32m, row0.revenue)
        Assert.Equal(1394.44m, row0.c_acctbal)
        Assert.Equal("IRAN", row0.n_name)
        Assert.Equal("jhLZlG1fRiSE0 wDLOCguVB96dH6a5", row0.c_address)
        Assert.Equal("20-146-696-9508", row0.c_phone)
        Assert.Equal("oys. furiously silent pinto beans", row0.c_comment)

        let row1 = results.[1]
        Assert.Equal(1201L, row1.c_custkey)
        Assert.Equal("Customer#000001201", row1.c_name)
        Assert.Equal(374331.55m, row1.revenue)
        Assert.Equal(5165.39m, row1.c_acctbal)
        Assert.Equal("IRAN", row1.n_name)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q11")>]
    let ``Test_Q11_Important_Stock_Identification`` () =
        use lfPartSupp = TpchData.PartSupp()
        use lfSupplier = TpchData.Supplier()
        use lfNation = TpchData.Nation()

        let partsuppQuery = lfPartSupp.AsQueryable<PartSupp>()
        let supplierQuery = lfSupplier.AsQueryable<Supplier>()
        let nationQuery = lfNation.AsQueryable<Nation>()

        let totalCostQuery =
            query {
                for ps2 in partsuppQuery do
                join s2 in supplierQuery on (ps2.ps_suppkey = s2.s_suppkey)
                join n2 in nationQuery on (s2.s_nationkey = n2.n_nationkey)
                where (n2.n_name = "GERMANY")
                select (ps2.ps_supplycost * decimal ps2.ps_availqty)
            }

        let threshold = totalCostQuery.Sum() * 0.0001m

        let query =
            query {
                for ps in partsuppQuery do
                join s in supplierQuery on (ps.ps_suppkey = s.s_suppkey)
                join n in nationQuery on (s.s_nationkey = n.n_nationkey)
                where (n.n_name = "GERMANY")
                groupValBy (ps.ps_supplycost * decimal ps.ps_availqty) ps.ps_partkey into g
                where (g.Sum() > threshold)
                sortByDescending (g.Sum())
                select
                    {|
                        ps_partkey = g.Key
                        value = g.Sum()
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(359, results.Count)

        let row0 = results.[0]
        Assert.Equal(1376L, row0.ps_partkey)
        Assert.Equal(13271249.89m, row0.value)

        let row1 = results.[1]
        Assert.Equal(788L, row1.ps_partkey)
        Assert.Equal(9498648.06m, row1.value)

        let last = results.[results.Count - 1]
        Assert.Equal(295L, last.ps_partkey)
        Assert.Equal(97604.25m, last.value)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q12")>]
    let ``Test_Q12_Shipping_Modes_and_Order_Priority`` () =
        use lfOrders = TpchData.Orders()
        use lfLineItem = TpchData.LineItem()

        let ordersQuery = lfOrders.AsQueryable<Orders>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()

        let startDate = DateOnly(1994, 1, 1)
        let endDate = DateOnly(1995, 1, 1)

        let query =
            query {
                for l in lineitemQuery do
                where (
                    (l.l_shipmode = "MAIL" || l.l_shipmode = "SHIP")
                    && l.l_commitdate < l.l_receiptdate
                    && l.l_shipdate < l.l_commitdate
                    && l.l_receiptdate >= startDate
                    && l.l_receiptdate < endDate
                )
                join o in ordersQuery on (l.l_orderkey = o.o_orderkey)
                groupValBy o l.l_shipmode into g
                sortBy g.Key
                select
                    {|
                        l_shipmode = g.Key
                        high_line_count = g.Count(fun x -> x.o_orderpriority = "1-URGENT" || x.o_orderpriority = "2-HIGH")
                        low_line_count = g.Count(fun x -> x.o_orderpriority <> "1-URGENT" && x.o_orderpriority <> "2-HIGH")
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(2, results.Count)

        let row0 = results.[0]
        Assert.Equal("MAIL", row0.l_shipmode)
        Assert.Equal(64, row0.high_line_count)
        Assert.Equal(86, row0.low_line_count)

        let row1 = results.[1]
        Assert.Equal("SHIP", row1.l_shipmode)
        Assert.Equal(61, row1.high_line_count)
        Assert.Equal(96, row1.low_line_count)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q13")>]
    let ``Test_Q13_Customer_Distribution`` () =
        use lfCustomer = TpchData.Customer()
        use lfOrders = TpchData.Orders()

        let customerQuery = lfCustomer.AsQueryable<Customer>()
        let ordersQuery = lfOrders.AsQueryable<Orders>()

        let specialRequestsRegex = Regex("special.*requests", RegexOptions.Compiled)

        let customerOrders =
            query {
                for c in customerQuery do
                groupJoin o in ordersQuery.Where(fun o -> not (specialRequestsRegex.IsMatch(o.o_comment)))
                    on (c.c_custkey = o.o_custkey) into g
                select
                    {|
                        c_custkey = c.c_custkey
                        c_count = g.Count()
                    |}
            }

        let query =
            query {
                for co in customerOrders do
                groupBy co.c_count into g
                sortByDescending (g.Count())
                thenByDescending g.Key
                select
                    {|
                        c_count = g.Key
                        custdist = g.Count()
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(32, results.Count)

        let row0 = results.[0]
        Assert.Equal(0, row0.c_count)
        Assert.Equal(500, row0.custdist)

        let row1 = results.[1]
        Assert.Equal(10, row1.c_count)
        Assert.Equal(69, row1.custdist)

        let row2 = results.[2]
        Assert.Equal(9, row2.c_count)
        Assert.Equal(66, row2.custdist)

        let row3 = results.[3]
        Assert.Equal(11, row3.c_count)
        Assert.Equal(63, row3.custdist)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q14")>]
    let ``Test_Q14_Promotion_Effect`` () =
        use lfLineItem = TpchData.LineItem()
        use lfPart = TpchData.Part()

        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()
        let partQuery = lfPart.AsQueryable<Part>()

        let startDate = DateOnly(1995, 9, 1)
        let endDate = DateOnly(1995, 10, 1)

        let query =
            query {
                for l in lineitemQuery do
                where (l.l_shipdate >= startDate && l.l_shipdate < endDate)
                join p in partQuery on (l.l_partkey = p.p_partkey)
                groupValBy {| l = l; p = p |} 1 into g
                select
                    {|
                        promo_revenue =
                            100.0 * double (g.Sum(fun x -> if x.p.p_type.StartsWith "PROMO" then x.l.l_extendedprice * (1.0m - x.l.l_discount) else 0.0m))
                            / double (g.Sum(fun x -> x.l.l_extendedprice * (1.0m - x.l.l_discount)))
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Single(results) |> ignore
        Assert.Equal(15.4865, results.[0].promo_revenue, precision = 4)

    (*
     * TPC-H Query 15: Top Supplier Query
     *
     * WITH revenue AS (
     *     SELECT
     *         l_suppkey AS supplier_no,
     *         sum(l_extendedprice * (1 - l_discount)) AS total_revenue
     *     FROM
     *         lineitem
     *     WHERE
     *         l_shipdate >= date '1996-01-01'
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q15")>]
    let ``Test_Q15_Top_Supplier`` () =
        use lfSupplier = TpchData.Supplier()
        use lfLineItem = TpchData.LineItem()

        let supplierQuery = lfSupplier.AsQueryable<Supplier>()
        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()

        let startDate = DateOnly(1996, 1, 1)
        let endDate = DateOnly(1996, 4, 1)

        let revenue =
            query {
                for l in lineitemQuery do
                where (l.l_shipdate >= startDate && l.l_shipdate < endDate)
                groupValBy (l.l_extendedprice * (1.0m - l.l_discount)) l.l_suppkey into g
                select
                    {|
                        supplier_no = g.Key
                        total_revenue = g.Sum()
                    |}
            }

        let query =
            query {
                for s in supplierQuery do
                join r in revenue on (s.s_suppkey = r.supplier_no)
                where (r.total_revenue = revenue.Select(fun x -> x.total_revenue).Max())
                sortBy s.s_suppkey
                select
                    {|
                        s_suppkey = s.s_suppkey
                        s_name = s.s_name
                        s_address = s.s_address
                        s_phone = s.s_phone
                        total_revenue = r.total_revenue
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Single(results) |> ignore
        let row0 = results.[0]
        Assert.Equal(21L, row0.s_suppkey)
        Assert.Equal("Supplier#000000021", row0.s_name)
        Assert.Equal("TZoQwNFFO i,baXpbpin02,hvuhE,GRVIKm ", row0.s_address)
        Assert.Equal("12-253-590-5816", row0.s_phone)
        Assert.Equal(1161099.49m, row0.total_revenue)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q16")>]
    let ``Test_Q16_Parts_Supplier_Relationship`` () =
        use lfPartSupp = TpchData.PartSupp()
        use lfPart = TpchData.Part()
        use lfSupplier = TpchData.Supplier()

        let partsuppQuery = lfPartSupp.AsQueryable<PartSupp>()
        let partQuery = lfPart.AsQueryable<Part>()
        let supplierQuery = lfSupplier.AsQueryable<Supplier>()

        let targetSizes = [| 49; 14; 23; 45; 19; 3; 36; 9 |]
        let complaintsRegex = Regex("Customer.*Complaints", RegexOptions.Compiled)

        let complaintsSuppliers =
            query {
                for s in supplierQuery do
                where (complaintsRegex.IsMatch(s.s_comment))
                select s.s_suppkey
            }

        let filteredParts =
            query {
                for p in partQuery do
                where (p.p_brand <> "Brand#45"
                       && not (p.p_type.StartsWith "MEDIUM POLISHED")
                       && targetSizes.Contains(p.p_size))
            }

        let query =
            query {
                for ps in partsuppQuery do
                where (not (complaintsSuppliers.Contains(ps.ps_suppkey)))
                join p in filteredParts on (ps.ps_partkey = p.p_partkey)
                groupValBy ps.ps_suppkey
                    {|
                        p_brand = p.p_brand
                        p_type = p.p_type
                        p_size = p.p_size
                    |} into g
                sortByDescending (g.Distinct().Count())
                thenBy g.Key.p_brand
                thenBy g.Key.p_type
                thenBy g.Key.p_size
                select
                    {|
                        p_brand = g.Key.p_brand
                        p_type = g.Key.p_type
                        p_size = g.Key.p_size
                        supplier_cnt = g.Distinct().Count()
                    |}
            }

        assertPlan NativePushdown query

        let results = query.ToList()
        Assert.Equal(296, results.Count)

        let row0 = results.[0]
        Assert.Equal("Brand#14", row0.p_brand)
        Assert.Equal("PROMO BRUSHED STEEL", row0.p_type)
        Assert.Equal(9, row0.p_size)
        Assert.Equal(8, row0.supplier_cnt)

        let row1 = results.[1]
        Assert.Equal("Brand#35", row1.p_brand)
        Assert.Equal("SMALL POLISHED COPPER", row1.p_type)
        Assert.Equal(14, row1.p_size)
        Assert.Equal(8, row1.supplier_cnt)

        let row2 = results.[2]
        Assert.Equal("Brand#22", row2.p_brand)
        Assert.Equal("LARGE BURNISHED TIN", row2.p_type)
        Assert.Equal(36, row2.p_size)
        Assert.Equal(6, row2.supplier_cnt)

    (*
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
     *)
    [<Fact>]
    [<Trait("TPCH", "Q17")>]
    let ``Test_Q17_Small_Quantity_Order_Revenue`` () =
        use lfLineItem = TpchData.LineItem()
        use lfPart = TpchData.Part()

        let lineitemQuery = lfLineItem.AsQueryable<LineItem>()
        let partQuery = lfPart.AsQueryable<Part>()

        let queryQ17 (brand: string) (container: string) =
            let filteredParts =
                query {
                    for p in partQuery do
                    where (p.p_brand = brand && p.p_container = container)
                }

            query {
                for l in lineitemQuery do
                where (
                    l.l_quantity < 0.2m * lineitemQuery
                                            .Where(fun l2 -> l2.l_partkey = l.l_partkey)
                                            .Average(fun l2 -> l2.l_quantity)
                )
                join p in filteredParts on (l.l_partkey = p.p_partkey)
                groupValBy l 1 into g
                select
                    {|
                        avg_yearly = (double (g.Sum(fun x -> x.l_extendedprice))) / 7.0
                    |}
            }

        // 1. Standard TPC-H Spec Parameters: Brand#23, MED BOX (Matches 0 parts in SF=0.01)
        let standardQuery = queryQ17 "Brand#23" "MED BOX"
        assertPlan NativePushdown standardQuery

        let standardResults = standardQuery.ToList()
        Assert.Single(standardResults) |> ignore
        Assert.Equal(0.0, standardResults.[0].avg_yearly)

        // 2. Active Dataset Case on SF=0.01: Brand#23, MED DRUM (Has 11 matching lineitems, verified by DuckDB)
        let activeQuery = queryQ17 "Brand#23" "MED DRUM"
        assertPlan NativePushdown activeQuery

        let activeResults = activeQuery.ToList()
        Assert.Single(activeResults) |> ignore
        Assert.Equal(7590.6514, activeResults.[0].avg_yearly, 4)










