using BenchmarkDotNet.Attributes;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.Benchmarks;

// Overhead of the checked integer Sum compiled by the LINQ provider.
// An int Sum is lowered to an exact Int64 sum, a long Sum to an exact Decimal(38,0) sum, followed by an
// overflow check on the aggregated value. The baseline is the plain wrapping Polars sum written by hand.
// Every pair is (baseline, compensated) over the same data; the baseline carries [Benchmark(Baseline = true)],
// so the Ratio column reads compensated / baseline.
//
// Filter: --filter '*CheckedSum*' (all classes in this file are named accordingly).

public class CheckedSumRow
{
    public int K { get; set; }
    public int I { get; set; }
    public long L { get; set; }
}

/// Shared deterministic data, built once per row count and reused by every class in this file.
/// Values stay in [-1000, 1000] so no sum can overflow; K has 1000 distinct groups.
internal static class CheckedSumData
{
    private static readonly Dictionary<int, DataFrame> Cache = new();

    public static DataFrame Get(int n)
    {
        if (Cache.TryGetValue(n, out var df)) return df;

        var rng = new Random(42);
        var k = new int[n];
        var i = new int[n];
        var l = new long[n];
        for (int r = 0; r < n; r++)
        {
            k[r] = rng.Next(0, 1000);
            i[r] = rng.Next(-1000, 1001);
            l[r] = rng.NextInt64(-1000, 1001);
        }
        df = DataFrame.FromColumns(("K", k), ("I", i), ("L", l));
        Cache[n] = df;
        return df;
    }
}

// ---------------------------------------------------------------------------
// 1. Scalar Sum over int (I)
// ---------------------------------------------------------------------------

[ShortRunJob]
[MemoryDiagnoser]
public class CheckedSumScalarIntBenchmarks
{
    [Params(1_000_000, 10_000_000)]
    public int N;

    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = CheckedSumData.Get(N);

    [Benchmark(Baseline = true)]
    public long ScalarInt_Baseline()
    {
        using var lf = _df.Lazy().Select(Pl.Col("I").Sum());
        using var result = lf.Collect();
        return Convert.ToInt64(result[0, "I"]);
    }

    [Benchmark]
    public long ScalarInt_Compensated() =>
        (long)_df.AsQueryable<CheckedSumRow>().Sum(x => x.I);

    // Same LINQ query with UDF pushdown disallowed: the check becomes a strict cast, not a managed callback.
    [Benchmark]
    public long ScalarInt_Compensated_NoAssert()
    {
        using var _ = Pl.Config.SetDisallowUdfPushdown(true).BeginScope();
        return (long)_df.AsQueryable<CheckedSumRow>().Sum(x => x.I);
    }

    // What the provider emits for an int sum, without the callback: widen, sum, strict cast back.
    [Benchmark]
    public long ScalarInt_Rewritten()
    {
        using var lf = _df.Lazy().Select(Pl.Col("I").Cast<long>().Sum().Cast<int>());
        using var result = lf.Collect();
        return Convert.ToInt64(result[0, "I"]);
    }

    // Fixed cost of a LINQ scalar terminal, with no overflow check.
    [Benchmark]
    public double ScalarInt_LinqPlainDouble() =>
        _df.AsQueryable<CheckedSumRow>().Sum(x => (double)x.I);
}

// ---------------------------------------------------------------------------
// 2. Scalar Sum over long (L)
// ---------------------------------------------------------------------------

[ShortRunJob]
[MemoryDiagnoser]
public class CheckedSumScalarLongBenchmarks
{
    [Params(1_000_000, 10_000_000)]
    public int N;

    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = CheckedSumData.Get(N);

    [Benchmark(Baseline = true)]
    public long ScalarLong_Baseline()
    {
        using var lf = _df.Lazy().Select(Pl.Col("L").Sum());
        using var result = lf.Collect();
        return Convert.ToInt64(result[0, "L"]);
    }

    [Benchmark]
    public long ScalarLong_Compensated() =>
        (long)_df.AsQueryable<CheckedSumRow>().Sum(x => x.L);

    // Same LINQ query with UDF pushdown disallowed: the check becomes a strict cast, not a managed callback.
    [Benchmark]
    public long ScalarLong_Compensated_NoAssert()
    {
        using var _ = Pl.Config.SetDisallowUdfPushdown(true).BeginScope();
        return (long)_df.AsQueryable<CheckedSumRow>().Sum(x => x.L);
    }

    // What the provider emits for a long sum, without the callback: the two 32-bit half sums in one select.
    [Benchmark]
    public long ScalarLong_Rewritten()
    {
        var l = Pl.Col("L");
        var q = Pl.Lit(4294967296L);
        using var lf = _df.Lazy().Select(
            l.FloorDiv(q).Sum().Alias("hi"),
            (l - l.FloorDiv(q) * q).Sum().Alias("lo"));
        using var result = lf.Collect();
        return Convert.ToInt64(result[0, "hi"]) + Convert.ToInt64(result[0, "lo"]);
    }

    // Fixed cost of a LINQ scalar terminal, with no overflow check.
    [Benchmark]
    public double ScalarLong_LinqPlainDouble() =>
        _df.AsQueryable<CheckedSumRow>().Sum(x => (double)x.L);
}

// ---------------------------------------------------------------------------
// 3. Grouped Sum over long (L), about 1000 groups
// ---------------------------------------------------------------------------

[ShortRunJob]
[MemoryDiagnoser]
public class CheckedSumGroupedLongBenchmarks
{
    [Params(1_000_000, 10_000_000)]
    public int N;

    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = CheckedSumData.Get(N);

    [Benchmark(Baseline = true)]
    public long GroupedLong_Baseline()
    {
        using var lf = _df.Lazy().GroupBy("K").Agg(Pl.Col("L").Sum());
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public int GroupedLong_Compensated() =>
        _df.AsQueryable<CheckedSumRow>()
            .GroupBy(x => x.K)
            .Select(g => new { g.Key, S = g.Sum(x => x.L) })
            .ToList().Count;
}

// ---------------------------------------------------------------------------
// 4. Grouped Sum over int (I), about 1000 groups
// ---------------------------------------------------------------------------

[ShortRunJob]
[MemoryDiagnoser]
public class CheckedSumGroupedIntBenchmarks
{
    [Params(1_000_000, 10_000_000)]
    public int N;

    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = CheckedSumData.Get(N);

    [Benchmark(Baseline = true)]
    public long GroupedInt_Baseline()
    {
        using var lf = _df.Lazy().GroupBy("K").Agg(Pl.Col("I").Sum());
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public int GroupedInt_Compensated() =>
        _df.AsQueryable<CheckedSumRow>()
            .GroupBy(x => x.K)
            .Select(g => new { g.Key, S = g.Sum(x => x.I) })
            .ToList().Count;
}
