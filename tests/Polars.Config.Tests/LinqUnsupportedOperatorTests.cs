using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

/// <summary>
/// A query operator the pipeline does not translate (an overload with a comparer) used to be skipped together
/// with every stage beneath it, silently. It now sends the query to LINQ to Objects under the client
/// evaluation policy: Strict raises, Allow evaluates it exactly.
/// </summary>
public class LinqUnsupportedOperatorTests
{
    public class Word
    {
        public long Id { get; set; }
        public string S { get; set; } = "";
    }

    private static readonly Word[] Words = [new() { Id = 1, S = "b" }, new() { Id = 2, S = "A" }, new() { Id = 3, S = "a" }, new() { Id = 4, S = "B" }];

    private static IEnumerable<Func<IQueryable<Word>, IEnumerable<long>>> Queries()
    {
        yield return q => q.Where(x => x.Id > 2).OrderBy(x => x.S, StringComparer.Ordinal).Select(x => x.Id);
        yield return q => q.OrderBy(x => x.Id % 2).ThenBy(x => x.S, StringComparer.OrdinalIgnoreCase).ThenBy(x => x.Id).Select(x => x.Id);
        yield return q => q.Where(x => x.Id > 1).DistinctBy(x => x.S, StringComparer.OrdinalIgnoreCase).Select(x => x.Id);
        yield return q => q.Where(x => x.Id > 1).GroupBy(x => x.S, StringComparer.OrdinalIgnoreCase).Select(g => g.LongCount());
        yield return q => q.Where(x => x.Id > 1).Select(x => x.S).Distinct(StringComparer.OrdinalIgnoreCase).Select(s => (long)s.Length);
    }

    [Fact]
    public void Strict_Policy_Raises_Instead_Of_Skipping_The_Operator()
    {
        using var df = DataFrame.From(Words);
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Strict).BeginScope())
        {
            foreach (var query in Queries())
                Assert.Throws<PolarsPushdownException>(() => query(df.AsQueryable<Word>()).ToList());
        }
    }

    [Fact]
    public void Allow_Policy_Evaluates_The_Operator_Exactly()
    {
        using var df = DataFrame.From(Words);
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            foreach (var query in Queries())
                Assert.Equal(query(Words.AsQueryable()).ToList(), [.. query(df.AsQueryable<Word>())]);
        }
    }

    [Fact]
    public void Unknown_Query_Operator_In_The_Chain_Is_Never_Skipped()
    {
        using var df = DataFrame.From(Words);
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Strict).BeginScope())
        {
            var query = df.AsQueryable<Word>().Where(x => x.Id > 2).Tagged().Select(x => x.Id);
            Assert.Throws<PolarsPushdownException>(() => query.ToList());
        }
    }
}

public static class CustomQueryOperators
{
    /// <summary>A query operator the provider does not know, as libraries add (a hint, a tag).</summary>
    public static IQueryable<T> Tagged<T>(this IQueryable<T> source) =>
        source.Provider.CreateQuery<T>(System.Linq.Expressions.Expression.Call(
            null,
            ((System.Reflection.MethodInfo)System.Reflection.MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(T)),
            source.Expression));
}
