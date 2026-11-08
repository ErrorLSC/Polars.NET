using System.Diagnostics;
using System.Linq.Expressions;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;
using Polars.NET.Core.Wrappers;
using Polars.NET.Linq;
using Polars.NET.Linq.Backend.Polars;

namespace Polars.Benchmarks.Tpcds;

// Throwaway probe: where does materializing the Wide result spend its time? Delete after the analysis.
public sealed class ZzLongRow
{
    public long ss_ticket_number { get; init; }
    public long ss_item_sk { get; init; }
    public long? ss_customer_sk { get; init; }
    public long? ss_sold_date_sk { get; init; }
    public long? ss_quantity { get; init; }
}

public sealed class ZzDecRow
{
    public decimal? ss_sales_price { get; init; }
    public decimal? ss_ext_sales_price { get; init; }
    public decimal? ss_net_profit { get; init; }
}

public sealed class ZzStrRow
{
    public string? i_item_id { get; init; }
    public string? i_category { get; init; }
}

internal static class ZzMaterializeProbe
{
    public static void Run()
    {
        TpcdsBenchEnvironment.UseBenchmarkData();
        var wide = Collect(WideResult.Polars());
        var longs = Collect(WideResult.Polars().Select(r => new ZzLongRow { ss_ticket_number = r.ss_ticket_number, ss_item_sk = r.ss_item_sk, ss_customer_sk = r.ss_customer_sk, ss_sold_date_sk = r.ss_sold_date_sk, ss_quantity = r.ss_quantity }));
        var decs = Collect(WideResult.Polars().Select(r => new ZzDecRow { ss_sales_price = r.ss_sales_price, ss_ext_sales_price = r.ss_ext_sales_price, ss_net_profit = r.ss_net_profit }));
        var strs = Collect(WideResult.Polars().Select(r => new ZzStrRow { i_item_id = r.i_item_id, i_category = r.i_category }));
        Console.WriteLine($"rows: {PolarsWrapper.DataFrameHeight(wide)}");
        var uniq = Collect(Polars.Tpcds.TpcdsData.CustomerQuery().Select(c => new ZzStrRow { i_item_id = c.c_customer_id, i_category = c.c_email_address }));
        Console.WriteLine($"customer rows: {PolarsWrapper.DataFrameHeight(uniq)}");

        using (var b = ArrowFfiBridge.ExportDataFrame(PolarsWrapper.CloneDataFrame(wide)))
        {
            Console.WriteLine("schema: " + string.Join(", ", b.Schema.FieldsList.Select(f => $"{f.Name}:{f.DataType.Name}")));
            Console.WriteLine("arrays: " + string.Join(", ", Enumerable.Range(0, b.ColumnCount).Select(i => b.Column(i).GetType().Name)));
        }

        Time("export only (wide)", () => { using var b = ArrowFfiBridge.ExportDataFrame(PolarsWrapper.CloneDataFrame(wide)); return b.Length; });
        Time("WideRow  full (10 cols)", () => Hydrate<WideRow>(wide));
        Time("LongRow  5 x long", () => Hydrate<ZzLongRow>(longs));
        Time("DecRow   3 x decimal", () => Hydrate<ZzDecRow>(decs));
        Time("StrRow   2 x string", () => Hydrate<ZzStrRow>(strs));
        Time("raw Arrow loop -> WideRow", () => RawWide(wide));
        Time("unique strings (customer)", () => Hydrate<ZzStrRow>(uniq), PolarsWrapper.DataFrameHeight(uniq));
        Time("object alloc only", () => { var l = new List<WideRow>(1_099_596); for (int i = 0; i < 1_099_596; i++) l.Add(new WideRow()); return l.Count; });
    }

    private static DataFrameHandle Collect<T>(IQueryable<T> q)
    {
        using var p = ((PolarsQuery<T>)q).Prepare();
        return p.Collect();
    }

    private static int Hydrate<T>(DataFrameHandle h)
    {
        using var cursor = ArrowRowMapper<T>.CreateCursor(PolarsWrapper.CloneDataFrame(h));
        var list = new List<T>(cursor.Length);
        for (int i = 0; i < cursor.Length; i++) list.Add(cursor.ReadRow(i));
        return list.Count;
    }

    private static int RawWide(DataFrameHandle h)
    {
        using var b = ArrowFfiBridge.ExportDataFrame(PolarsWrapper.CloneDataFrame(h));
        var c = Enumerable.Range(0, b.ColumnCount).Select(b.Column).ToArray();
        var t = (Int64Array)c[0]; var it = (Int64Array)c[1]; var cu = (Int64Array)c[2]; var sd = (Int64Array)c[3]; var q = (Int64Array)c[4];
        var sp = (Decimal128Array)c[5]; var esp = (Decimal128Array)c[6]; var np = (Decimal128Array)c[7];
        var id = (StringViewArray)c[8]; var cat = (StringViewArray)c[9];
        var list = new List<WideRow>(b.Length);
        for (int i = 0; i < b.Length; i++)
            list.Add(new WideRow
            {
                ss_ticket_number = t.GetValue(i)!.Value, ss_item_sk = it.GetValue(i)!.Value, ss_customer_sk = cu.GetValue(i),
                ss_sold_date_sk = sd.GetValue(i), ss_quantity = q.GetValue(i),
                ss_sales_price = sp.GetValue(i), ss_ext_sales_price = esp.GetValue(i), ss_net_profit = np.GetValue(i),
                i_item_id = id.GetString(i), i_category = cat.GetString(i),
            });
        return list.Count;
    }

    private static void Time(string what, Func<int> f, long rows = 1_099_596)
    {
        f();
        var best = double.MaxValue;
        for (int r = 0; r < 5; r++)
        {
            GC.Collect();
            var w = Stopwatch.StartNew();
            f();
            best = Math.Min(best, w.Elapsed.TotalMilliseconds);
        }

        Console.WriteLine($"{what,-28} {best,8:F1} ms  ({best * 1e6 / rows,6:F0} ns/row)");
    }
}

// Throwaway probe: plan cache behaviour of repeated translations of one query (sources reused, as in the
// translation benchmark). Delete after the analysis.
internal static class ZzCacheProbe
{
    public static void Run(string name)
    {
        TpcdsBenchEnvironment.UseBenchmarkData();
        Polars.Tpcds.TpcdsData.ReuseSources = true;
        QueryPlanCache.Enabled = true;
        QueryPlanCache.Clear();
        var q = TpcdsBenchCatalog.Get(name);
        for (int i = 1; i <= 6; i++)
        {
            long h = QueryPlanCache.Hits, m = QueryPlanCache.Misses;
            var w = Stopwatch.StartNew();
            q.PolarsTranslate();
            Console.WriteLine($"run {i}: {w.Elapsed.TotalMicroseconds,9:F0} us  hits +{QueryPlanCache.Hits - h}  misses +{QueryPlanCache.Misses - m}  entries {QueryPlanCache.Count}");
        }
    }
}

internal static class ZzCacheSteps
{
    public static void Run(string name)
    {
        TpcdsBenchEnvironment.UseBenchmarkData();
        Polars.Tpcds.TpcdsData.ReuseSources = true;
        QueryPlanCache.Enabled = true;
        QueryPlanCache.Clear();
        Func<IQueryable> make = name switch
        {
            "Q3" => () => Polars.Tpcds.TpcdsQueries.Q3(),
            "Q5" => () => Polars.Tpcds.TpcdsQueries.Q5(),
            "Q14" => () => Polars.Tpcds.TpcdsQueries.Q14(),
            "Q64" => () => Polars.Tpcds.TpcdsQueries.Q64(),
            "Q98" => () => Polars.Tpcds.TpcdsQueries.Q98(),
            _ => throw new ArgumentException(name),
        };
        for (int i = 0; i < 3; i++) Prep(make());

        var q = make();
        var expr = q.Expression;
        var elem = q.ElementType;
        Step("build query (factory)", () => make());
        Step("expression.ToString()", () => expr.ToString());
        Expression keyExpr = null!;
        Step("InlineComposedQueries", () => keyExpr = QueryPlanCache.InlineComposedQueries(expr));
        ExtractionResult ex = null!;
        Step("PartialEvaluator.extract", () => ex = PartialEvaluator.extract(keyExpr));
        Step("TryCreateProbe (key)", () => QueryPlanCache.TryCreateProbe(elem, ex.NormalizedExpression, ex.Parameters));
        foreach (var kv in ex.Parameters)
            Console.WriteLine($"  param {kv.Key}: declared {kv.Value.Type.Name}, value {kv.Value.Value?.GetType().Name ?? "null"}");
        var probe = QueryPlanCache.TryCreateProbe(elem, ex.NormalizedExpression, ex.Parameters);
        Console.WriteLine($"  top-level probe: {(probe is null ? "NONE (not cacheable)" : "ok")}");
        if (probe is not null)
        {
            Step("TryGetBound", () => QueryPlanCache.TryGetBound(probe.Value));
            var pipeline = QueryPlanCache.TryGetBound(probe.Value)!.Value;
            Polars.NET.Linq.IR.CompiledLogicalQuery packed = null!;
            Step("forMaterialization", () => packed = RowMaterialization.forMaterialization(elem, pipeline.Stages, pipeline.Compiled));
            Step("lowerPlan (LazyFrame/FFI)", () => PolarsPhysicalLowerer.lowerPlan(packed.Sources, packed.Plan).Dispose());
            Console.WriteLine($"  plan nodes: {CountNodes(packed.Plan)}, sources: {packed.Sources.Length}");
        }
        Step("full Prepare (translate)", () => Prep(make()));
    }

    private static int CountNodes(Polars.NET.Linq.IR.LogicalPlan plan) =>
        1 + Polars.NET.Linq.IR.LogicalPlanOps.getInputs(plan).Sum(CountNodes);

    private static void Prep(IQueryable q)
    {
        var m = q.GetType().GetMethod("Prepare", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public)!;
        ((IDisposable)m.Invoke(q, null)!).Dispose();
    }

    private static void Step(string what, Action a)
    {
        a();
        long h = QueryPlanCache.Hits;
        var w = Stopwatch.StartNew();
        const int n = 5;
        for (int i = 0; i < n; i++) a();
        Console.WriteLine($"{what,-28} {w.Elapsed.TotalMicroseconds / n,10:F0} us   cache hits/run {(QueryPlanCache.Hits - h) / (double)n:F0}");
    }
}
