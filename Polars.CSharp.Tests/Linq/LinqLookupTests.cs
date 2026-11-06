using System.Linq.Expressions;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public enum LkShade { Red = 5, Green = 1 }
public record LkRow(int Id, string Name, int Age, int? Opt, LkShade Shade);

/// <summary>
/// ToLookup partitions natively on any translatable key: a column, a computed value (added as a temporary column),
/// or a composite key (one temporary column per member). Groups, their order (first appearance), null keys and
/// elements agree with LINQ to Objects.
/// </summary>
public class LinqLookupTests
{
    private static readonly LkRow[] Rows =
    [
        new(1, "a", 30, null, LkShade.Red), new(2, "b", 25, 1, LkShade.Green),
        new(3, "c", 30, 1, LkShade.Red), new(4, "d", 40, null, LkShade.Green)
    ];

    private static string Format<TKey, TElement>(IEnumerable<IGrouping<TKey, TElement>> lookup) =>
        string.Join(" | ", lookup.Select(g => $"{g.Key}:[{string.Join(",", g)}]"));

    private static void AssertMatches<TKey, TElement>(
        Expression<Func<LkRow, TKey>> key,
        Expression<Func<LkRow, TElement>> element)
    {
        using var df = DataFrame.FromRows(Rows);
        Assert.Equal(
            Format(Rows.ToLookup(key.Compile(), element.Compile())),
            Format(df.AsQueryable<LkRow>().ToLookup(key, element)));
    }

    private static string Upper(string s) => s.ToUpperInvariant() + "!";

    [Fact]
    public void Column_Keys()
    {
        AssertMatches(r => r.Age, r => r.Id);
        AssertMatches(r => r.Shade, r => r.Id);
        using var df = DataFrame.FromRows(Rows);
        Assert.Equal(Format(Rows.ToLookup(r => r.Age)), Format(df.AsQueryable<LkRow>().ToLookup(r => r.Age)));
    }

    [Fact]
    public void Computed_Keys()
    {
        AssertMatches(r => r.Id % 2, r => r.Name);
        AssertMatches(r => r.Age > 26, r => r.Id);
        AssertMatches(r => r.Name + "x", r => r.Id);
        AssertMatches(r => (int)r.Shade, r => r.Id);
        AssertMatches(r => Upper(r.Name), r => r.Id);

        using var df = DataFrame.FromRows(Rows);
        Assert.Equal(Format(Rows.ToLookup(r => r.Id % 2)), Format(df.AsQueryable<LkRow>().ToLookup(r => r.Id % 2)));
    }

    [Fact]
    public void Composite_Keys()
    {
        AssertMatches(r => new { r.Age, r.Shade }, r => r.Id);
        AssertMatches(r => new { Even = r.Id % 2 == 0, r.Age }, r => r.Name);
        AssertMatches(r => new { Age = r.Id }, r => r.Age);
        AssertMatches(r => Tuple.Create(r.Age, r.Shade), r => r.Id);
        AssertMatches(r => new { r.Opt }, r => r.Name);
    }

    [Fact]
    public void Null_Keys_Form_Their_Own_Group()
    {
        AssertMatches(r => r.Opt, r => r.Name);

        using var df = DataFrame.FromRows(Rows);
        var lookup = df.AsQueryable<LkRow>().ToLookup(r => r.Opt);
        Assert.True(lookup.Contains(null));
        Assert.Equal([1, 4], lookup[null].Select(r => r.Id));
        Assert.Empty(lookup[7]);
        Assert.Equal(2, lookup.Count);
    }

    [Fact]
    public void Element_Selectors()
    {
        AssertMatches(r => r.Age, r => r.Id * 10);
        AssertMatches(r => r.Age, r => new { r.Id, r.Name });
        AssertMatches(r => r.Age, r => Tuple.Create(r.Id, r.Name));
        AssertMatches(r => r.Age, r => Upper(r.Name));
    }

    [Fact]
    public void Comparer_Applies_To_Lookups()
    {
        using var df = DataFrame.FromRows(Rows);
        var lookup = df.AsQueryable<LkRow>().ToLookup(r => r.Name, r => r.Id, StringComparer.OrdinalIgnoreCase);

        Assert.True(lookup.Contains("A"));
        Assert.Equal([2], lookup["B"]);
    }
}
