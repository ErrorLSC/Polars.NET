using System.Text.RegularExpressions;
using BenchmarkDotNet.Attributes;
using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Benchmarks.Tpch;

/// <summary>
/// TPC-H queries through the LINQ provider, end to end (compile, execute natively, materialize), over tables held
/// in memory. A representative subset: a wide aggregation (Q1), join chains with grouping and top-N (Q3, Q5, Q10),
/// a group join with a second grouping (Q13) and a semi join on a grouped subquery (Q18).
/// </summary>
/// <remarks>
/// Data: run <c>Polars.Benchmarks/generate_tpch_data.sh</c> (scale factor 1 by default), or point
/// <c>POLARS_BENCH_TPCH_DIR</c> at a directory of TPC-H Parquet files. Native timings are only meaningful with an
/// optimized native library; see Program.cs.
/// </remarks>
[MemoryDiagnoser]
public partial class TpchBenchmarks
{
    private DataFrame _customer = null!;
    private DataFrame _orders = null!;
    private DataFrame _lineitem = null!;
    private DataFrame _nation = null!;
    private DataFrame _region = null!;
    private DataFrame _supplier = null!;

    [GeneratedRegex("special.*requests")]
    private static partial Regex SpecialRequestsRegex();

    private static string DataDir()
    {
        var configured = Environment.GetEnvironmentVariable("POLARS_BENCH_TPCH_DIR");
        if (!string.IsNullOrEmpty(configured)) return configured;

        // From the build output (bin/<Configuration>/<tfm>) up to the project directory.
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "Data", "tpch-sf1");
            if (File.Exists(Path.Combine(candidate, "lineitem.parquet"))) return candidate;
        }

        throw new InvalidOperationException(
            "TPC-H data not found. Run Polars.Benchmarks/generate_tpch_data.sh, or set POLARS_BENCH_TPCH_DIR.");
    }

    [GlobalSetup]
    public void Setup()
    {
        var dir = DataDir();
        DataFrame Load(string table) => DataFrame.ReadParquet(Path.Combine(dir, table + ".parquet"));
        _customer = Load("customer");
        _orders = Load("orders");
        _lineitem = Load("lineitem");
        _nation = Load("nation");
        _region = Load("region");
        _supplier = Load("supplier");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _customer.Dispose();
        _orders.Dispose();
        _lineitem.Dispose();
        _nation.Dispose();
        _region.Dispose();
        _supplier.Dispose();
    }

    private IQueryable<Customer> Customers => _customer.AsQueryable<Customer>();
    private IQueryable<Orders> Orders => _orders.AsQueryable<Orders>();
    private IQueryable<LineItem> LineItems => _lineitem.AsQueryable<LineItem>();
    private IQueryable<Nation> Nations => _nation.AsQueryable<Nation>();
    private IQueryable<Region> Regions => _region.AsQueryable<Region>();
    private IQueryable<Supplier> Suppliers => _supplier.AsQueryable<Supplier>();

    [Benchmark]
    public int Q1_PricingSummary()
    {
        var cutoff = new DateOnly(1998, 9, 2);
        return (from l in LineItems
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
                }).ToList().Count;
    }

    [Benchmark]
    public int Q3_ShippingPriority()
    {
        var cutoff = new DateOnly(1995, 3, 15);
        return (from c in Customers
                where c.c_mktsegment == "BUILDING"
                join o in Orders on c.c_custkey equals o.o_custkey
                where o.o_orderdate < cutoff
                join l in LineItems on o.o_orderkey equals l.l_orderkey
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
                .Take(10)
                .ToList().Count;
    }

    [Benchmark]
    public int Q5_LocalSupplierVolume()
    {
        var startDate = new DateOnly(1994, 1, 1);
        var endDate = new DateOnly(1995, 1, 1);
        return (from c in Customers
                join o in Orders on c.c_custkey equals o.o_custkey
                where o.o_orderdate >= startDate && o.o_orderdate < endDate
                join l in LineItems on o.o_orderkey equals l.l_orderkey
                join s in Suppliers on new { l.l_suppkey, c.c_nationkey }
                    equals new { l_suppkey = s.s_suppkey, c_nationkey = s.s_nationkey }
                join n in Nations on s.s_nationkey equals n.n_nationkey
                join r in Regions on n.n_regionkey equals r.r_regionkey
                where r.r_name == "ASIA"
                group l by n.n_name into g
                select new { n_name = g.Key, revenue = g.Sum(x => x.l_extendedprice * (1m - x.l_discount)) })
                .OrderByDescending(x => x.revenue)
                .ToList().Count;
    }

    [Benchmark]
    public int Q10_ReturnedItemReporting()
    {
        var startDate = new DateOnly(1993, 10, 1);
        var endDate = new DateOnly(1994, 1, 1);
        return (from c in Customers
                join o in Orders on c.c_custkey equals o.o_custkey
                where o.o_orderdate >= startDate && o.o_orderdate < endDate
                join l in LineItems on o.o_orderkey equals l.l_orderkey
                where l.l_returnflag == "R"
                join n in Nations on c.c_nationkey equals n.n_nationkey
                group l by new { c.c_custkey, c.c_name, c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment } into g
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
                .Take(20)
                .ToList().Count;
    }

    [Benchmark]
    public int Q13_CustomerDistribution()
    {
        return (from co in from c in Customers
                           join o in Orders.Where(o => !SpecialRequestsRegex().IsMatch(o.o_comment)) on c.c_custkey equals o.o_custkey into g
                           select new { c.c_custkey, c_count = g.Count() }
                group co by co.c_count into g
                orderby g.Count() descending, g.Key descending
                select new { c_count = g.Key, custdist = g.Count() }).ToList().Count;
    }

    [Benchmark]
    public int Q18_LargeVolumeCustomer()
    {
        var bigOrders = from l in LineItems
                        group l by l.l_orderkey into g
                        select new { l_orderkey = g.Key, total_qty = g.Sum(x => x.l_quantity) } into og
                        where og.total_qty > 300m
                        select og.l_orderkey;

        var filteredOrders = from o in Orders where bigOrders.Contains(o.o_orderkey) select o;

        return (from c in Customers
                join o in filteredOrders on c.c_custkey equals o.o_custkey
                join l in LineItems on o.o_orderkey equals l.l_orderkey
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
                })
                .Take(100)
                .ToList().Count;
    }
}
