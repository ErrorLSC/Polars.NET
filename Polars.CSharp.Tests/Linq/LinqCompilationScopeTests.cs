using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Polars.NET.Linq.Backend.Polars;
using Polars.NET.Linq.IR;

namespace Polars.CSharp.Tests;

/// <summary>
/// Each compilation collects its diagnostics in its own scope: nested compilations do not leak into the
/// enclosing one unless it absorbs them, and one query's diagnostics never show up on another query.
/// </summary>
public class LinqCompilationScopeTests
{
    [Fact]
    public void Nested_Scope_Is_Isolated_And_Restores_The_Enclosing_One()
    {
        using var outer = CompilationScope.Enter(PolarsQueryBackend.Instance);
        outer.RecordUdf("outer");

        using (var inner = CompilationScope.Enter(PolarsQueryBackend.Instance))
        {
            Assert.Same(inner, CompilationScope.Current);
            inner.RecordUdf("inner");
            inner.FallbackReason = Microsoft.FSharp.Core.FSharpOption<string>.Some("inner reason");
        }

        Assert.Same(outer, CompilationScope.Current);
        Assert.Equal(["outer"], outer.Udfs);
        Assert.Null(outer.FallbackReason);
    }

    [Fact]
    public void Absorb_Keeps_Udf_Order_Without_Duplicates_And_Prefers_The_Nested_Reason()
    {
        using var scope = CompilationScope.Enter(PolarsQueryBackend.Instance);
        scope.RecordUdf("a");
        scope.FallbackReason = Microsoft.FSharp.Core.FSharpOption<string>.Some("outer reason");

        scope.Absorb(Microsoft.FSharp.Core.FSharpOption<string>.Some("nested reason"), Microsoft.FSharp.Collections.ListModule.OfArray(["b", "a"]));
        Assert.Equal(["a", "b"], scope.Udfs);
        Assert.Equal("nested reason", scope.FallbackReason!.Value);

        scope.Absorb(null, Microsoft.FSharp.Collections.FSharpList<string>.Empty);
        Assert.Equal("nested reason", scope.FallbackReason!.Value);
    }

    [Fact]
    public void Translation_Outside_A_Scope_Is_Reported_As_A_Bug()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => CompilationScope.Current);
        Assert.Contains("outside a compilation scope", ex.Message);
    }

    public record ScopeRow(string Name, int Value);

    private static string Shout(string s) => s.ToUpperInvariant() + "!";

    [Fact]
    public void Diagnostics_Belong_To_Their_Own_Query()
    {
        using var df = DataFrame.FromRows([new ScopeRow("a", 1), new ScopeRow("b", 2)]);
        var withUdf = df.AsQueryable<ScopeRow>().Select(r => Shout(r.Name));
        var plain = df.AsQueryable<ScopeRow>().Select(r => r.Name.ToUpper());

        Assert.True(((IPolarsExplainable)withUdf).InspectPlan(true).HasUdf);
        Assert.False(((IPolarsExplainable)plain).InspectPlan(true).HasUdf);
        Assert.True(((IPolarsExplainable)withUdf).InspectPlan(true).HasUdf);
        Assert.Equal(["A!", "B!"], withUdf.ToList());
    }
}
