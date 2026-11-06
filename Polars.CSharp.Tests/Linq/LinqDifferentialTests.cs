using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public record DtRow(int Id, int A, int B, int? C, string S, double V);

/// <summary>
/// Differential tests: combinations of filters, orderings, paging and result shapes, each run natively and with
/// LINQ to Objects, compared element by element in order. The data has many duplicate keys, nulls, and groups whose
/// first appearance differs from their key order, so order guarantees (stable sorts, group order) are observable.
/// </summary>
public class LinqDifferentialTests
{
    private static readonly DtRow[] Rows = Generate(20_000);

    private static DtRow[] Generate(int count)
    {
        var random = new Random(20261107);
        string[] words = ["x1", "y2", "x3", "z4", "w5"];
        return Enumerable.Range(0, count)
            .Select(i => new DtRow(
                i,
                (i * 7 + random.Next(3)) % 5,
                random.Next(3),
                random.Next(4) == 0 ? null : random.Next(10),
                words[random.Next(words.Length)],
                Math.Round(random.NextDouble() * 100, 1)))
            .ToArray();
    }

    private static readonly (string Name, Func<IQueryable<DtRow>, IQueryable<DtRow>> Apply)[] Filters =
    [
        ("all", q => q),
        ("A!=2", q => q.Where(r => r.A != 2)),
        ("C>3", q => q.Where(r => r.C != null && r.C > 3)),
        ("S^x", q => q.Where(r => r.S.StartsWith("x"))),
    ];

    private static readonly (string Name, Func<IQueryable<DtRow>, IQueryable<DtRow>> Apply)[] Orderings =
    [
        ("unordered", q => q),
        ("A", q => q.OrderBy(r => r.A)),
        ("A,B desc", q => q.OrderBy(r => r.A).ThenByDescending(r => r.B)),
        ("V desc", q => q.OrderByDescending(r => r.V)),
        ("C", q => q.OrderBy(r => r.C)),
        ("S,Id", q => q.OrderBy(r => r.S).ThenBy(r => r.Id)),
    ];

    private static readonly (string Name, Func<IQueryable<DtRow>, IQueryable<DtRow>> Apply)[] Pagings =
    [
        ("whole", q => q),
        ("take 100", q => q.Take(100)),
        ("skip 50 take 30", q => q.Skip(50).Take(30)),
    ];

    private static readonly (string Name, Func<IQueryable<DtRow>, IEnumerable<string>> Run)[] Shapes =
    [
        ("ids", q => q.Select(r => r.Id).ToList().Select(x => x.ToString())),
        ("anonymous", q => q.Select(r => new { r.Id, r.A, r.C }).ToList().Select(x => x.ToString())),
        ("groups", q => q.GroupBy(r => r.A).Select(g => new { g.Key, N = g.Count(), S = g.Sum(x => x.B) }).ToList().Select(x => x.ToString())),
        ("groups by count", q => q.GroupBy(r => new { r.A, r.B }).Select(g => new { g.Key.A, g.Key.B, N = g.Count() }).OrderBy(x => x.N).ToList().Select(x => x.ToString())),
        ("groups by key", q => q.GroupBy(r => r.A).Select(g => new { g.Key, M = g.Max(x => x.C) }).OrderByDescending(x => x.Key).ToList().Select(x => x.ToString())),
        ("distinct", q => q.Select(r => r.S).Distinct().ToList()),
        ("scalars", q => [q.Count().ToString(), q.Sum(r => r.B).ToString(), q.Any(r => r.B == 2).ToString(), q.Select(r => r.Id).FirstOrDefault().ToString()]),
    ];

    /// <summary>Subqueries producing keys, used as IN / NOT IN filters over the outer query.</summary>
    private static readonly (string Name, Func<IQueryable<DtRow>, IQueryable<int>> Keys, System.Linq.Expressions.Expression<Func<DtRow, int>> OuterKey)[] Subqueries =
    [
        ("where-select", i => i.Where(x => x.V > 99).Select(x => x.Id), r => r.Id),
        ("having", i => i.GroupBy(x => x.Id % 97).Where(g => g.Sum(x => x.V) > 10400).Select(g => g.Key), r => r.Id % 97),
        ("distinct", i => i.Where(x => x.S == "z4" && x.B == 1).Select(x => x.Id % 300).Distinct(), r => r.Id % 300),
        ("top-n", i => i.OrderByDescending(x => x.V).ThenBy(x => x.Id).Take(300).Select(x => x.Id), r => r.Id),
        ("top-groups", i => i.GroupBy(x => x.Id % 97).Select(g => new { g.Key, T = g.Sum(x => x.V) }).OrderByDescending(t => t.T).Take(20).Select(t => t.Key), r => r.Id % 97),
        ("first-n", i => i.Where(x => x.B == 2).Take(150).Select(x => x.Id), r => r.Id),
    ];

    /// <summary>
    /// The outer filter/ordering/paging combinations under an IN or NOT IN subquery filter. The native side captures
    /// the Polars subquery (grafted as a semi or anti join); the oracle evaluates the subquery once into a set.
    /// </summary>
    [Fact]
    public void Subquery_Filter_Combinations_Agree_With_Linq_To_Objects()
    {
        using var df = DataFrame.FromRows(Rows);
        using var dfInner = DataFrame.FromRows(Rows);
        var failures = new List<string>();
        int checkedCount = 0;

        foreach (var sub in Subqueries)
        foreach (var negate in new[] { false, true })
        {
            var keyOf = sub.OuterKey.Compile();
            var expectedKeys = sub.Keys(Rows.AsQueryable()).ToHashSet();
            var nativeKeys = sub.Keys(dfInner.AsQueryable<DtRow>());
            var p = sub.OuterKey.Parameters[0];
            var contains = System.Linq.Expressions.Expression.Call(
                typeof(Queryable), nameof(Queryable.Contains), [typeof(int)],
                System.Linq.Expressions.Expression.Constant(nativeKeys, typeof(IQueryable<int>)), sub.OuterKey.Body);
            var nativePredicate = System.Linq.Expressions.Expression.Lambda<Func<DtRow, bool>>(
                negate ? System.Linq.Expressions.Expression.Not(contains) : contains, p);

            foreach (var filter in Filters)
            foreach (var ordering in Orderings)
            foreach (var paging in Pagings)
            {
                var name = $"{(negate ? "not in" : "in")} {sub.Name} | {filter.Name} | {ordering.Name} | {paging.Name}";
                checkedCount++;
                try
                {
                    var expected = paging.Apply(ordering.Apply(filter.Apply(Rows.AsQueryable())
                            .Where(r => expectedKeys.Contains(keyOf(r)) != negate)))
                        .Select(r => r.Id).ToList();
                    var actual = paging.Apply(ordering.Apply(filter.Apply(df.AsQueryable<DtRow>()).Where(nativePredicate)))
                        .Select(r => r.Id).ToList();
                    if (!expected.SequenceEqual(actual))
                    {
                        int at = expected.Zip(actual).TakeWhile(x => x.First == x.Second).Count();
                        failures.Add($"{name}: first difference at {at} (counts {expected.Count}/{actual.Count})");
                    }
                }
                catch (Exception ex)
                {
                    failures.Add($"{name}: {ex.GetType().Name}: {ex.Message.Split('\n')[0]}");
                }
            }
        }

        Assert.True(failures.Count == 0, $"{failures.Count} of {checkedCount} combinations differ:\n" + string.Join("\n", failures.Take(40)));
    }

    [Fact]
    public void Query_Combinations_Agree_With_Linq_To_Objects()
    {
        using var df = DataFrame.FromRows(Rows);
        var failures = new List<string>();
        int checkedCount = 0;

        foreach (var filter in Filters)
        foreach (var ordering in Orderings)
        foreach (var paging in Pagings)
        foreach (var shape in Shapes)
        {
            IQueryable<DtRow> Build(IQueryable<DtRow> source) => paging.Apply(ordering.Apply(filter.Apply(source)));
            var name = $"{filter.Name} | {ordering.Name} | {paging.Name} | {shape.Name}";
            checkedCount++;
            try
            {
                var expected = shape.Run(Build(Rows.AsQueryable())).ToList();
                var actual = shape.Run(Build(df.AsQueryable<DtRow>())).ToList();
                if (!expected.SequenceEqual(actual))
                {
                    int at = expected.Zip(actual).TakeWhile(p => p.First == p.Second).Count();
                    failures.Add($"{name}: first difference at {at} (expected {expected.ElementAtOrDefault(at)}, actual {actual.ElementAtOrDefault(at)}; counts {expected.Count}/{actual.Count})");
                }
            }
            catch (Exception ex)
            {
                failures.Add($"{name}: {ex.GetType().Name}: {ex.Message.Split('\n')[0]}");
            }
        }

        Assert.True(failures.Count == 0, $"{failures.Count} of {checkedCount} combinations differ:\n" + string.Join("\n", failures.Take(40)));
    }
}
