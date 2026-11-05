using BenchmarkDotNet.Attributes;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Linq;

namespace Polars.Benchmarks;

public record Order(int Id, string Customer, int Qty, double Price, DateTime When);

/// <summary>
/// Cost of turning a LINQ query into IR with and without the query plan cache.
/// BuildExpression is the caller's own cost of constructing the expression tree, which no cache can remove.
/// </summary>
[MemoryDiagnoser]
public class LinqCompileBenchmarks
{
    private DataFrame _df = null!;
    private Func<int, IQueryable<object>> _make = null!;
    private IQueryable<object> _query = null!;
    private int _freshValue;

    [Params("FilterSortTake", "GroupByAggregate")]
    public string Shape { get; set; } = "";

    [Params(false, true)]
    public bool CacheEnabled { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var rows = Enumerable.Range(0, 1000)
            .Select(i => new Order(i, "C" + (i % 50), i % 7, i * 1.5, new DateTime(2024, 1, 1).AddDays(i % 365)))
            .ToArray();
        _df = DataFrame.FromRows(rows);

        _make = Shape switch
        {
            "FilterSortTake" => minQty => _df.AsQueryable<Order>()
                .Where(o => o.Qty > minQty && o.Customer.StartsWith("C1"))
                .Select(o => new { o.Customer, Total = o.Qty * o.Price })
                .OrderByDescending(x => x.Total)
                .Take(10)
                .Cast<object>(),
            "GroupByAggregate" => minQty => _df.AsQueryable<Order>()
                .Where(o => o.Qty > minQty && o.When.Year == 2024)
                .GroupBy(o => o.Customer)
                .Select(g => new { g.Key, Sum = g.Sum(x => x.Qty * x.Price), Count = g.Count() })
                .OrderBy(x => x.Key)
                .Cast<object>(),
            _ => throw new ArgumentOutOfRangeException(nameof(Shape)),
        };

        QueryPlanCache.Enabled = CacheEnabled;
        QueryPlanCache.Clear();
        _query = _make(3);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        QueryPlanCache.Enabled = true;
        QueryPlanCache.Clear();
        _df.Dispose();
    }

    /// Constructing the expression tree (caller side).
    [Benchmark]
    public System.Linq.Expressions.Expression BuildExpression() => _make(3).Expression;

    /// Compiling a prebuilt query to IR; a cache hit after the first iteration when the cache is enabled.
    [Benchmark]
    public object Compile() => ((IPolarsExplainable)_query).GetLogicalPlan();

    /// Compiling with a parameter value never seen before: always a cache miss, including the insertion
    /// (and, once MaxCapacity is reached, eviction) cost when the cache is enabled.
    [Benchmark]
    public object Compile_FreshParameterValue() => ((IPolarsExplainable)_make(++_freshValue)).GetLogicalPlan();

    /// End to end: compile, lower, collect and materialize.
    [Benchmark]
    public int ToList() => _query.ToList().Count;
}
