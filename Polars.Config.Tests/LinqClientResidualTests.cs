using System.Collections;
using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

public record MaskedView(string Masked, int Salary);

/// <summary>
/// Pipelines that continue after a client-evaluated step (an untranslatable Where / Select) must keep
/// LINQ-to-Objects semantics: operations apply in pipeline order, and later stages (Chunk, AggregateBy,
/// DefaultIfEmpty, paging, ordering) see the client-filtered rows.
/// </summary>
public class LinqClientResidualTests
{
    private static readonly UdfEmployee[] Rows =
    [
        new("Alice", 30, 60000),
        new("Bob", 25, 45000),
        new("Charlie", 40, 80000),
        new("David", 22, 35000),
    ];

    private static readonly Dictionary<string, Func<IQueryable<UdfEmployee>, IEnumerable>> Pipelines = new()
    {
        ["ClientSelect_Where"] = q => q.Select(e => new MaskedView(TestUdfMethods.MaskName(e.Name), e.Salary)).Where(v => v.Salary > 50000),
        ["ClientSelect_Where_Select"] = q => q.Select(e => new MaskedView(TestUdfMethods.MaskName(e.Name), e.Salary)).Where(v => v.Salary > 50000).Select(v => v.Masked),
        ["ClientWhere_Select_Where"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).Select(e => e.Salary).Where(s => s > 70000),
        ["ClientWhere_ClientSelect"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).Select(e => TestUdfMethods.MaskName(e.Name)),
        ["ClientWhere_Take"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).Take(1),
        ["ClientWhere_OrderByDescending"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).OrderByDescending(e => e.Age).Select(e => e.Name),
        ["NativeWhere_ClientWhere_Skip"] = q => q.Where(e => e.Age > 20).Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).Skip(1).Select(e => e.Name),
        ["ClientWhere_Chunk"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).Chunk(5),
        ["ClientWhere_AggregateBy"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).AggregateBy(e => e.Age > 35, 0, (acc, e) => acc + 1),
        ["ClientWhere_DefaultIfEmpty"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary) && e.Age > 100).DefaultIfEmpty(new UdfEmployee("none", 0, 0)),
        ["ClientWhere_OfType"] = q => q.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary)).OfType<UdfEmployee>(),
        // Client HAVING over the groups themselves (single surviving group, so group order is irrelevant).
        ["Grouping_ClientHaving"] = q => q.GroupBy(e => e.Age > 26).Where(g => TestUdfMethods.CheckSecurity(g.First().Name, g.Max(x => x.Salary))),
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
    [Trait("LINQ", "ClientResidual")]
    public void Enumeration_After_Client_Step_Matches_LinqToObjects(string name)
    {
        var pipeline = Pipelines[name];
        var expected = RenderAll(pipeline(Rows.AsQueryable()));

        using var df = DataFrame.FromRows(Rows);
        using (ClientEvaluationScope())
        {
            Assert.Equal(expected, RenderAll(pipeline(df.AsQueryable<UdfEmployee>())));
        }
    }

    [Fact]
    [Trait("LINQ", "ClientResidual")]
    public void Scalar_Terminals_After_Client_Select_Then_Where()
    {
        static IQueryable<MaskedView> Shape(IQueryable<UdfEmployee> q) =>
            q.Select(e => new MaskedView(TestUdfMethods.MaskName(e.Name), e.Salary)).Where(v => v.Salary > 50000);

        using var df = DataFrame.FromRows(Rows);
        using (ClientEvaluationScope())
        {
            var polars = Shape(df.AsQueryable<UdfEmployee>());
            Assert.Equal(2, polars.Count());
            Assert.True(polars.Any(v => v.Masked == "C***"));
            Assert.Equal("A***", polars.First().Masked);
            Assert.Equal(140000, polars.Sum(v => v.Salary));
        }
    }

    [Fact]
    [Trait("LINQ", "ClientResidual")]
    public void ToDataFrame_After_Client_Select_Then_Where()
    {
        using var df = DataFrame.FromRows(Rows);
        using (ClientEvaluationScope())
        {
            using var result = df.AsQueryable<UdfEmployee>()
                .Select(e => new MaskedView(TestUdfMethods.MaskName(e.Name), e.Salary))
                .Where(v => v.Salary > 50000)
                .ToDataFrame();

            Assert.Equal(2, result.Height);
            Assert.True(result.ColumnNames.SequenceEqual(["Masked", "Salary"]));
        }
    }
}
