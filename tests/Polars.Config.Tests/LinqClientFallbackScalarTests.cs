using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

/// <summary>
/// Scalar / single-row LINQ terminals (Count, Any, First, Min, MinBy, ...) applied on top of a query that
/// already required client-side evaluation must agree with LINQ-to-Objects over the same rows.
/// Scenario A: an untranslatable Where becomes a client predicate.
/// Scenario B: an untranslatable OrderBy forces full client fallback for the remaining stages (here Take).
/// </summary>
public class LinqClientFallbackScalarTests
{
    private static readonly UdfEmployee[] Rows =
    [
        new("Alice", 30, 60000),
        new("Bob", 25, 45000),
        new("Charlie", 40, 80000),
        new("David", 22, 35000),
    ];

    private static readonly Dictionary<string, Func<IQueryable<UdfEmployee>, object?>> Terminals = new()
    {
        ["Count"] = q => q.Count(),
        ["LongCount"] = q => q.LongCount(),
        ["CountPredicate"] = q => q.Count(e => e.Age > 35),
        ["Any"] = q => q.Any(),
        ["AnyPredicate"] = q => q.Any(e => e.Name == "Bob"),
        ["AllPredicate"] = q => q.All(e => e.Salary > 50000),
        ["First"] = q => q.First(),
        ["FirstPredicate"] = q => q.First(e => e.Age < 35),
        ["FirstOrDefaultPredicate"] = q => q.FirstOrDefault(e => e.Age < 23),
        ["Last"] = q => q.Last(),
        ["ElementAt1"] = q => q.ElementAt(1),
        ["ElementAtOrDefault3"] = q => q.ElementAtOrDefault(3),
        ["MinSelector"] = q => q.Min(e => e.Age),
        ["MaxSelector"] = q => q.Max(e => e.Salary),
        ["SumSelector"] = q => q.Sum(e => e.Salary),
        ["AverageSelector"] = q => q.Average(e => e.Age),
        ["MinBy"] = q => q.MinBy(e => e.Age),
        ["MaxBy"] = q => q.MaxBy(e => e.Salary),
        ["ContainsExcluded"] = q => q.Contains(new UdfEmployee("Bob", 25, 45000)),
        ["ContainsIncluded"] = q => q.Contains(new UdfEmployee("Alice", 30, 60000)),
    };

    public static TheoryData<string> TerminalNames() => [.. Terminals.Keys];

    private static IQueryable<UdfEmployee> ClientPredicateQuery(IQueryable<UdfEmployee> source) =>
        source.Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary));

    private static IQueryable<UdfEmployee> ClientFallbackQuery(IQueryable<UdfEmployee> source) =>
        source.OrderByDescending(e => TestUdfMethods.MaskName(e.Name)).Take(2);

    private static void AssertMatchesLinqToObjects(
        Func<IQueryable<UdfEmployee>, IQueryable<UdfEmployee>> shape,
        string terminalName)
    {
        var terminal = Terminals[terminalName];
        var expected = terminal(shape(Rows.AsQueryable()));

        using var df = DataFrame.FromRows(Rows);

        using (Pl.Config.SetDisallowUdfPushdown(true).BeginScope())
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            var actual = terminal(shape(df.AsQueryable<UdfEmployee>()));
            Assert.Equal(expected, actual);
        }
    }

    [Theory]
    [MemberData(nameof(TerminalNames))]
    [Trait("LINQ", "ClientFallbackScalar")]
    public void Scalar_Terminal_After_Client_Predicate_Matches_LinqToObjects(string terminalName) =>
        AssertMatchesLinqToObjects(ClientPredicateQuery, terminalName);

    [Theory]
    [MemberData(nameof(TerminalNames))]
    [Trait("LINQ", "ClientFallbackScalar")]
    public void Scalar_Terminal_After_Client_Fallback_Matches_LinqToObjects(string terminalName) =>
        AssertMatchesLinqToObjects(ClientFallbackQuery, terminalName);
}
