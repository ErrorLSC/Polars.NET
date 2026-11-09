using System.Collections;
using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Polars.NET.Linq;
using Kaminari.Samidare;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

public record FallbackDept(string Name, string Dept);

/// <summary>
/// Pipelines that need a full LINQ-to-Objects fallback partway through must return exactly what
/// LINQ-to-Objects returns for the whole pipeline, whatever native prefix runs before the fallback point.
/// </summary>
public class LinqFallbackPrefixTests
{
    private static readonly UdfEmployee[] Rows =
    [
        new("Alice", 30, 60000),
        new("Bob", 25, 45000),
        new("Charlie", 40, 80000),
        new("David", 22, 35000),
        new("Eve", 35, 90000),
        new("Frank", 28, 52000),
    ];

    // Deliberately different from Rows so that confusing the two sources changes the result.
    private static readonly FallbackDept[] Depts =
    [
        new("Alice", "Eng"),
        new("Charlie", "Ops"),
        new("Eve", "Eng"),
    ];

    private delegate IEnumerable Pipeline(IQueryable<UdfEmployee> employees, IQueryable<FallbackDept> depts);

    private static readonly Dictionary<string, Pipeline> Pipelines = new()
    {
        // A client ordering key cannot be a row step and forces full fallback; native Where + Select before it.
        ["NativeWhere_AnonSelect_ClientOrderBy"] = (q, _) => q
            .Where(e => e.Age > 24)
            .Select(e => new { e.Name, e.Salary })
            .OrderBy(x => TestUdfMethods.MaskName(x.Name) + x.Name)
            .Where(x => x.Salary > 40000)
            .Select(x => x.Name),

        // Row-op client Where followed by paging: native prefix, client Where, then Take.
        ["NativeWhere_OrderBy_ClientWhere_Take"] = (q, _) => q
            .Where(e => e.Age > 20)
            .OrderBy(e => e.Age)
            .Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary))
            .Take(1)
            .Select(e => e.Name),

        // Untranslatable ThenBy key: the fallback point may not split OrderBy from ThenBy.
        ["NativeWhere_OrderBy_ClientThenBy"] = (q, _) => q
            .Where(e => e.Age > 20)
            .OrderBy(e => e.Age > 27)
            .ThenBy(e => TestUdfMethods.MaskName(e.Name) + e.Name)
            .Select(e => e.Name),

        ["NativeWhere_OrderByDescending_ClientThenByDescending_Skip"] = (q, _) => q
            .Where(e => e.Salary > 40000)
            .OrderByDescending(e => e.Age > 27)
            .ThenByDescending(e => TestUdfMethods.MaskName(e.Name) + e.Name)
            .Skip(1)
            .Select(e => e.Name),

        // Prefix ends in a GroupBy; the client projection runs over the groups.
        ["NativeWhere_GroupBy_ClientSelect_OrderBy"] = (q, _) => q
            .Where(e => e.Age > 20)
            .GroupBy(e => e.Age > 27)
            .Select(g => new { g.Key, Masked = TestUdfMethods.MaskName(g.OrderBy(x => x.Name).First().Name) })
            .OrderBy(x => x.Key)
            .Select(x => x.Key + ":" + x.Masked),

        // Native join with a second source, then a client row step over the joined anonymous rows;
        // the OrderBy after it cannot follow a client row step.
        ["NativeJoin_ClientWhere"] = (q, d) => q
            .Join(d, e => e.Name, x => x.Name, (e, x) => new { e.Name, e.Salary, x.Dept })
            .Where(r => TestUdfMethods.CheckSecurity(r.Name, r.Salary))
            .OrderBy(r => r.Name)
            .Select(r => r.Name + "@" + r.Dept),

        // The LINQ-to-Objects part references the second source inside its lambda.
        ["NativeWhere_ClientOrderBy_ReferencesSecondSource"] = (q, d) => q
            .Where(e => e.Age > 20)
            .Select(e => new { e.Name, e.Salary })
            .OrderBy(x => TestUdfMethods.CheckSecurity(x.Name, x.Salary) && d.Any(o => o.Name == x.Name && o.Dept == "Eng"))
            .ThenBy(x => x.Name)
            .Select(x => x.Name),

        // Client Where then Chunk escalates row ops to full fallback.
        ["NativeWhere_ClientWhere_Chunk"] = (q, _) => q
            .Where(e => e.Age > 24)
            .Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary))
            .Select(e => e.Name)
            .Chunk(1),

        // Only a native projection precedes the client row step; the OrderByDescending after it falls back.
        ["AnonSelect_ClientWhere_OrderByDescending"] = (q, _) => q
            .Select(e => new { e.Name, e.Salary })
            .Where(x => TestUdfMethods.CheckSecurity(x.Name, x.Salary))
            .OrderByDescending(x => x.Salary)
            .Select(x => x.Name),

        // Fallback at the very first stage: nothing runs natively.
        ["ClientOrderBy_First_Stage"] = (q, _) => q
            .OrderBy(e => TestUdfMethods.MaskName(e.Name) + e.Name)
            .Select(e => e.Name),
    };

    /// The LINQ method at which LINQ-to-Objects takes over; null when no prefix runs natively.
    private static readonly Dictionary<string, string?> ExpectedBoundary = new()
    {
        ["NativeWhere_AnonSelect_ClientOrderBy"] = "OrderBy",
        ["NativeWhere_OrderBy_ClientWhere_Take"] = "Take",
        ["NativeWhere_OrderBy_ClientThenBy"] = "OrderBy",
        ["NativeWhere_OrderByDescending_ClientThenByDescending_Skip"] = "OrderByDescending",
        ["NativeWhere_GroupBy_ClientSelect_OrderBy"] = "Select",
        ["NativeJoin_ClientWhere"] = "OrderBy",
        ["NativeWhere_ClientOrderBy_ReferencesSecondSource"] = "OrderBy",
        ["NativeWhere_ClientWhere_Chunk"] = "Chunk",
        ["AnonSelect_ClientWhere_OrderByDescending"] = "OrderByDescending",
        ["ClientOrderBy_First_Stage"] = null,
    };

    public static TheoryData<string> PipelineNames() => [.. Pipelines.Keys];

    private static string Render(object item) =>
        item is IEnumerable seq and not string
            ? "[" + string.Join("|", seq.Cast<object>().Select(Render)) + "]"
            : item?.ToString() ?? "null";

    private static List<string> RenderAll(IEnumerable rows) => rows.Cast<object>().Select(Render).ToList();

    private static IDisposable ClientEvaluationScope()
    {
        var udf = Pl.Config.SetDisallowUdfPushdown(true).BeginScope();
        var policy = Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope();
        return new CompositeScope(policy, udf);
    }

    private sealed class CompositeScope(params IDisposable[] scopes) : IDisposable
    {
        public void Dispose()
        {
            foreach (var s in scopes) s.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(PipelineNames))]
    [Trait("LINQ", "FallbackPrefix")]
    public void Fallback_Pipeline_Matches_LinqToObjects(string name)
    {
        var pipeline = Pipelines[name];
        var expected = RenderAll(pipeline(Rows.AsQueryable(), Depts.AsQueryable()));
        Assert.NotEmpty(expected);

        using var employees = DataFrame.FromRows(Rows);
        using var depts = DataFrame.FromRows(Depts);
        using (ClientEvaluationScope())
        {
            var query = pipeline(employees.AsQueryable<UdfEmployee>(), depts.AsQueryable<FallbackDept>());
            Assert.True(((IQueryExplainable)query).InspectPlan(true).IsFullFallback, $"{name} is expected to need full fallback.");
            Assert.Equal(expected, RenderAll(query));
        }
    }

    [Theory]
    [MemberData(nameof(PipelineNames))]
    [Trait("LINQ", "FallbackPrefix")]
    public void Fallback_Boundary_Is_The_Longest_Native_Prefix(string name)
    {
        using var employees = DataFrame.FromRows(Rows);
        using var depts = DataFrame.FromRows(Depts);
        using (ClientEvaluationScope())
        {
            var query = Pipelines[name](employees.AsQueryable<UdfEmployee>(), depts.AsQueryable<FallbackDept>());
            var inspection = ((IQueryExplainable)query).InspectPlan(true);
            Assert.Equal(ExpectedBoundary[name], inspection.FallbackBoundary?.Value);
        }
    }

    [Fact]
    [Trait("LINQ", "FallbackPrefix")]
    public void Scalar_Terminals_After_Fallback_Match_LinqToObjects()
    {
        static IQueryable<string> Shape(IQueryable<UdfEmployee> q) => q
            .Where(e => e.Age > 24)
            .Select(e => new { e.Name, e.Salary })
            .Where(x => TestUdfMethods.CheckSecurity(x.Name, x.Salary))
            .Select(x => x.Name);

        var expected = Shape(Rows.AsQueryable());

        using var df = DataFrame.FromRows(Rows);
        using (ClientEvaluationScope())
        {
            var polars = Shape(df.AsQueryable<UdfEmployee>());
            Assert.Equal(expected.Count(), polars.Count());
            Assert.Equal(expected.First(), polars.First());
            Assert.Equal(expected.Any(n => n.StartsWith('E')), polars.Any(n => n.StartsWith('E')));
            Assert.Equal(expected.Max(), polars.Max());
        }
    }

    [Fact]
    [Trait("LINQ", "FallbackPrefix")]
    public async Task Async_Enumeration_After_Fallback_Matches_LinqToObjects()
    {
        static IQueryable<string> Shape(IQueryable<UdfEmployee> q) => q
            .Where(e => e.Age > 24)
            .Select(e => new { e.Name, e.Salary })
            .Where(x => TestUdfMethods.CheckSecurity(x.Name, x.Salary))
            .OrderBy(x => x.Name)
            .Select(x => x.Name);

        var expected = Shape(Rows.AsQueryable()).ToList();

        using var df = DataFrame.FromRows(Rows);
        using (ClientEvaluationScope())
        {
            var actual = new List<string>();
            await foreach (var name in (IAsyncEnumerable<string>)Shape(df.AsQueryable<UdfEmployee>()))
                actual.Add(name);
            Assert.Equal(expected, actual);
        }
    }
    [Fact]
    [Trait("LINQ", "FallbackPrefix")]
    public void Native_Prefix_Runs_In_Polars_Not_In_LinqToObjects()
    {
        // Integer division by zero throws in .NET but not in Polars, so the pipeline only succeeds
        // when the Where before the fallback point is executed natively. The client Where is a row step;
        // LINQ-to-Objects takes over at the OrderBy after it.
        static IQueryable<string> Shape(IQueryable<UdfEmployee> q) => q
            .Where(e => e.Salary / (e.Age - 25) > 1000)
            .Select(e => new { e.Name, e.Salary })
            .Where(x => TestUdfMethods.CheckSecurity(x.Name, x.Salary))
            .OrderBy(x => x.Name)
            .Select(x => x.Name);

        Assert.Throws<DivideByZeroException>(() => Shape(Rows.AsQueryable()).ToList());

        using var df = DataFrame.FromRows(Rows);
        using (ClientEvaluationScope())
        {
            var query = Shape(df.AsQueryable<UdfEmployee>());
            Assert.Equal("OrderBy", ((IQueryExplainable)query).InspectPlan(true).FallbackBoundary?.Value);
            Assert.Equal(["Alice", "Charlie"], query.ToList());
            Assert.Equal(2, query.Count());
        }
    }
}
