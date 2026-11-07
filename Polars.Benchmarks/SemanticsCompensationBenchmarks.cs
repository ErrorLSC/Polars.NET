using System.Reflection;
using BenchmarkDotNet.Attributes;
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;
using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.Benchmarks;

// Overhead of the semantic compensations Polars.NET adds to match C# behaviour.
// Every pair is (baseline, compensated) over the same data. The baseline carries [Benchmark(Baseline = true)],
// so the Ratio column reads compensated / baseline.
//
// Filter: --filter '*SemanticsCompensation*' (all classes in this file are named accordingly).

public class SemanticsNullRow
{
    public long Id { get; set; }
    public long? A { get; set; }
    public long? B { get; set; }
    public long? V { get; set; }
}

public class SemanticsGuardRow
{
    public long Id { get; set; }
    public int N { get; set; }
}

/// <summary>
/// The shim kernels the LINQ backend lowers to, as Exprs (Expr's handle constructor is internal).
internal static class SemanticsKernels
{
    private static readonly ConstructorInfo ExprCtor =
        typeof(Expr).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, [typeof(ExprHandle)])!;
    private static readonly MethodInfo CloneHandle =
        typeof(Expr).GetMethod("CloneHandle", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static ExprHandle H(Expr e) => (ExprHandle)CloneHandle.Invoke(e, null)!;
    private static Expr E(ExprHandle h) => (Expr)ExprCtor.Invoke([h]);

    public static Expr FloatOrderKey(Expr e) => E(PolarsWrapper.DotNetFloatOrderKey(H(e)));
    public static Expr Div(Expr a, Expr b) => E(PolarsWrapper.DotNetDiv(H(a), H(b)));
    public static Expr Rem(Expr a, Expr b) => E(PolarsWrapper.DotNetRem(H(a), H(b)));
}

/// Shared deterministic data, built once per process and reused by every class in this file.
/// </summary>
internal static class SemanticsCompensationData
{
    public const int N = 1_000_000;

    private static DataFrame? _nulls;
    private static DataFrame? _nan;
    private static DataFrame? _guard;

    public static DataFrame Nulls => _nulls ??= BuildNulls();
    public static DataFrame Nan => _nan ??= BuildNan();
    public static DataFrame Guard => _guard ??= BuildGuard();

    private static long? MaybeNull(Random rng, long value) => rng.NextDouble() < 0.2 ? null : value;

    private static DataFrame BuildNulls()
    {
        var rng = new Random(42);
        var rows = new SemanticsNullRow[N];
        for (int i = 0; i < N; i++)
        {
            rows[i] = new SemanticsNullRow
            {
                Id = i,
                A = MaybeNull(rng, rng.NextInt64(-50, 51)),
                B = MaybeNull(rng, rng.NextInt64(-50, 51)),
                V = MaybeNull(rng, rng.NextInt64(0, 21)),
            };
        }
        return DataFrame.From(rows);
    }

    private static DataFrame BuildNan()
    {
        var rng = new Random(42);
        var d = new double?[N];
        var i = new int[N];
        var j = new int[N];
        for (int k = 0; k < N; k++)
        {
            double u = rng.NextDouble();
            if (u < 0.05) d[k] = null;
            else if (u < 0.06) d[k] = double.NaN;
            else d[k] = rng.NextDouble() * 2 - 1;

            i[k] = rng.Next(-1000, 1001);

            int jv = 0;
            while (jv == 0) jv = rng.Next(-1000, 1001);
            j[k] = jv;
        }
        return DataFrame.FromColumns(("D", d), ("I", i), ("J", j));
    }

    private static DataFrame BuildGuard()
    {
        var rng = new Random(42);
        var rows = new SemanticsGuardRow[N];
        for (int i = 0; i < N; i++)
        {
            int n = 0;
            while (n == 0) n = rng.Next(-1000, 1001);
            rows[i] = new SemanticsGuardRow { Id = i, N = n };
        }
        return DataFrame.From(rows);
    }
}

// ---------------------------------------------------------------------------
// A. Null-semantics compensation (LINQ end-to-end, global config toggled)
// ---------------------------------------------------------------------------

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationNullProjectionBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nulls;

    [Benchmark(Baseline = true)]
    public long Projection_Baseline()
    {
        using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
        {
            return Project();
        }
    }

    [Benchmark]
    public long Projection_Compensated() => Project();

    private int Project() =>
        _df.AsQueryable<SemanticsNullRow>()
            .Select(x => new { x.Id, eq = (bool?)(x.A == x.B), gt = (bool?)(x.A > x.B) })
            .ToList().Count;
}

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationNullFilterNotEqualBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nulls;

    [Benchmark(Baseline = true)]
    public long FilterNotEqual_Baseline()
    {
        using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
        {
            return Filter();
        }
    }

    [Benchmark]
    public long FilterNotEqual_Compensated() => Filter();

    private int Filter() =>
        _df.AsQueryable<SemanticsNullRow>()
            .Where(x => x.V != 10)
            .Select(x => x.Id)
            .ToList().Count;
}

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationNullFilterEqualBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nulls;

    [Benchmark(Baseline = true)]
    public long FilterEqual_Baseline()
    {
        using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
        {
            return Filter();
        }
    }

    [Benchmark]
    public long FilterEqual_Compensated() => Filter();

    private int Filter() =>
        _df.AsQueryable<SemanticsNullRow>()
            .Where(x => x.V == 10)
            .Select(x => x.Id)
            .ToList().Count;
}

// ---------------------------------------------------------------------------
// B. NaN compensation and truncating arithmetic (raw Polars expressions, no LINQ)
// ---------------------------------------------------------------------------

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationNanSortBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nan;

    [Benchmark(Baseline = true)]
    public long Sort_Baseline()
    {
        using var lf = _df.Lazy().Sort("D");
        using var result = lf.Collect();
        return result.Height;
    }

    // Helper key "__nn" = NOT(is_nan(D)); it is dropped again after the sort.
    [Benchmark]
    public long Sort_Compensated()
    {
        using var lf = _df.Lazy()
            .WithColumns((!Pl.Col("D").IsNan()).Alias("__nn"))
            .Sort(new IntoSelector[] { "__nn", "D" },
                descending: new[] { false, false },
                nullsLast: new[] { false, false })
            .Drop("__nn");
        using var result = lf.Collect();
        return result.Height;
    }

    // What the backend now emits: one Int64 key with .NET's float order.
    [Benchmark]
    public long Sort_Kernel()
    {
        using var lf = _df.Lazy()
            .WithColumns(SemanticsKernels.FloatOrderKey(Pl.Col("D")).Alias("__k"))
            .Sort("__k")
            .Drop("__k");
        using var result = lf.Collect();
        return result.Height;
    }
}

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationNanFilterBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nan;

    [Benchmark(Baseline = true)]
    public long Filter_Baseline()
    {
        using var lf = _df.Lazy().Filter(Pl.Col("D") > 0.5);
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public long Filter_Compensated()
    {
        using var lf = _df.Lazy()
            .Filter((Pl.Col("D") > 0.5) & !Pl.Col("D").IsNan().FillNull(Pl.Lit(false)));
        using var result = lf.Collect();
        return result.Height;
    }

    // What the backend now emits: `D <= +inf` excludes NaN with one plain comparison.
    [Benchmark]
    public long Filter_Cheap()
    {
        using var lf = _df.Lazy().Filter((Pl.Col("D") > 0.5) & (Pl.Col("D") <= double.PositiveInfinity));
        using var result = lf.Collect();
        return result.Height;
    }
}

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationNanMinBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nan;

    [Benchmark(Baseline = true)]
    public long Min_Baseline()
    {
        using var lf = _df.Lazy().Select(Pl.Col("D").Min());
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public long Min_Compensated()
    {
        using var lf = _df.Lazy().Select(Pl.Col("D").NanMin());
        using var result = lf.Collect();
        return result.Height;
    }
}

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationIntFloorDivBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nan;

    [Benchmark(Baseline = true)]
    public long FloorDiv_Baseline()
    {
        var i = Pl.Col("I");
        var j = Pl.Col("J");
        using var lf = _df.Lazy().WithColumns(i.FloorDiv(j).Alias("R"));
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public long FloorDiv_Compensated()
    {
        var i = Pl.Col("I");
        var j = Pl.Col("J");
        var cond = (i % j != 0) & ((i < 0) != (j < 0));
        var expr = Pl.When(cond).Then(i.FloorDiv(j) + 1).Otherwise(i.FloorDiv(j));
        using var lf = _df.Lazy().WithColumns(expr.Alias("R"));
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public long FloorDiv_Kernel()
    {
        using var lf = _df.Lazy().WithColumns(SemanticsKernels.Div(Pl.Col("I"), Pl.Col("J")).Alias("R"));
        using var result = lf.Collect();
        return result.Height;
    }
}

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationIntRemBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Nan;

    [Benchmark(Baseline = true)]
    public long Rem_Baseline()
    {
        var i = Pl.Col("I");
        var j = Pl.Col("J");
        using var lf = _df.Lazy().WithColumns((i % j).Alias("R"));
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public long Rem_Compensated()
    {
        var i = Pl.Col("I");
        var j = Pl.Col("J");
        var cond = (i % j != 0) & ((i < 0) != (j < 0));
        var expr = Pl.When(cond).Then((i % j) - j).Otherwise(i % j);
        using var lf = _df.Lazy().WithColumns(expr.Alias("R"));
        using var result = lf.Collect();
        return result.Height;
    }

    [Benchmark]
    public long Rem_Kernel()
    {
        using var lf = _df.Lazy().WithColumns(SemanticsKernels.Rem(Pl.Col("I"), Pl.Col("J")).Alias("R"));
        using var result = lf.Collect();
        return result.Height;
    }
}

// ---------------------------------------------------------------------------
// C. Guarded UDF (LINQ end-to-end, default settings)
// ---------------------------------------------------------------------------

[ShortRunJob]
[MemoryDiagnoser]
public class SemanticsCompensationGuardedUdfBenchmarks
{
    private DataFrame _df = null!;

    [GlobalSetup]
    public void Setup() => _df = SemanticsCompensationData.Guard;

    public static int F(int n) => 100 / n;

    [Benchmark(Baseline = true)]
    public long Udf_Baseline() =>
        _df.AsQueryable<SemanticsGuardRow>()
            .Select(x => F(x.N))
            .ToList().Count;

    [Benchmark]
    public long Udf_Compensated() =>
        _df.AsQueryable<SemanticsGuardRow>()
            .Select(x => x.N != 0 ? F(x.N) : 0)
            .ToList().Count;
}
