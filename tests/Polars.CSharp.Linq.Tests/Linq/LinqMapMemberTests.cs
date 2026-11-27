#nullable enable

namespace Polars.CSharp.Linq.Tests;

/// <summary>
/// Dictionary members of map columns in LINQ queries, run in the engine: as LINQ to Objects where .NET does not throw
/// (guarded lookups, GetValueOrDefault, ContainsKey, Count, Keys, Values), null where it throws on a missing key, and a
/// null dictionary null for every member.
/// </summary>
public class LinqMapMemberTests
{
    public sealed record Bag(int Id, Dictionary<string, int>? M, IReadOnlyDictionary<string, string?>? S);

    private static readonly Bag[] Rows =
    [
        new(1, new() { ["a"] = 1, ["b"] = 2 }, new Dictionary<string, string?> { ["a"] = "x" }),
        new(2, new() { ["c"] = 3 }, new Dictionary<string, string?> { ["c"] = null }),
        new(3, null, null),
        new(4, [], new Dictionary<string, string?>()),
    ];

    private static readonly Bag[] NonNull = [.. Rows.Where(static r => r.M is not null)];

    private static void Same<T>(Func<IQueryable<Bag>, IQueryable<T>> query, Bag[] rows, params string[] inPlan)
    {
        using var df = DataFrame.FromRows(rows);
        IQueryable<T> native = query(df.AsQueryable(rows));
        Assert.Equal(query(rows.AsQueryable()).ToList(), native.ToList());
        string plan = native.Explain();
        foreach (string function in inPlan) Assert.True(plan.Contains(function), plan);
    }

    [Fact]
    public void A_Guarded_Lookup_Runs_As_In_Dotnet()
    {
        Same(static q => q.Select(r => r.M != null && r.M.ContainsKey("a") ? r.M["a"] : -1), Rows, "map.get", "map.contains_key");
        Same(static q => q.Select(r => r.S != null && r.S.ContainsKey("c") ? r.S["c"] : "none"), Rows, "map.get");
    }

    [Fact]
    public void GetValueOrDefault_Gives_The_Default_For_A_Missing_Key()
    {
        Same(static q => q.Select(r => r.M!.GetValueOrDefault("a")), NonNull, "map.get");
        Same(static q => q.Select(r => r.M!.GetValueOrDefault("a", -1)), NonNull, "map.get");
        Same(static q => q.Select(r => r.S!.GetValueOrDefault("a")), NonNull, "map.get");
        // A key held with a null value gives the null, not the default (as .NET).
        Same(static q => q.Select(r => r.S!.GetValueOrDefault("c", "none")), NonNull, "map.contains_key");

        // A null dictionary (.NET: ArgumentNullException) gives null, as every member of one.
        using var df = DataFrame.FromRows(Rows);
        Assert.Equal([1, -1, null, -1], df.AsQueryable(Rows).Select(static r => (int?)r.M!.GetValueOrDefault("a", -1)).ToList());
        Assert.Equal([1, 0, null, 0], df.AsQueryable(Rows).Select(static r => (int?)r.M!.GetValueOrDefault("a")).ToList());
    }

    [Fact]
    public void Count_Keys_And_Values()
    {
        Same(static q => q.Select(r => r.M!.Count), NonNull, "map.len");
        Same(static q => q.Select(r => r.M!.Keys.Count), NonNull, "map.keys");
        Same(static q => q.Select(r => r.M!.Values.Sum()), NonNull, "map.values");
        Same(static q => q.Select(r => r.M!.Keys.Contains("b")), NonNull, "map.keys");

        using var df = DataFrame.FromRows(NonNull);
        var keys = df.AsQueryable(NonNull).Select(static r => r.M!.Keys.ToList()).ToList();
        Assert.Equal([["a", "b"], ["c"], []], keys);
    }

    [Fact]
    public void A_Filter_On_A_Map_Runs_In_The_Engine()
    {
        Same(static q => q.Where(r => r.M != null && r.M.ContainsKey("a")).Select(r => r.Id), Rows, "map.contains_key");
        Same(static q => q.Where(r => r.M != null && r.M.ContainsKey("c") && r.M["c"] > 2).Select(r => r.Id), Rows, "map.get");
        Same(static q => q.Where(r => r.M != null && r.M.Count > 1).Select(r => r.Id), Rows, "map.len");
    }

    [Fact]
    public void A_Missing_Key_Reads_Null_Where_Dotnet_Throws()
    {
        using var df = DataFrame.FromRows(Rows);
        // .NET: KeyNotFoundException (and NullReferenceException for row 3); the engine: null.
        var values = df.AsQueryable(Rows).Select(static r => (int?)r.M!["a"]).ToList();
        Assert.Equal([1, null, null, null], values);
        var texts = df.AsQueryable(Rows).Select(static r => r.S!["a"]).ToList();
        Assert.Equal(["x", null, null, null], texts);

        // A null dictionary: null for every member, no row in a filter.
        var counts = df.AsQueryable(Rows).Select(static r => (int?)r.M!.Count).ToList();
        Assert.Equal([2, 1, null, 0], counts);
        Assert.Equal([1], df.AsQueryable(Rows).Where(static r => r.M!.ContainsKey("a")).Select(static r => r.Id).ToList());
    }

    [Fact]
    public void A_Missing_Key_In_A_Non_Nullable_Result_Fails_At_Materialization()
    {
        using var df = DataFrame.FromRows(NonNull);
        // The row whose map lacks the key reads null, which an int cannot hold: the message says how to receive it.
        var error = Assert.Throws<InvalidOperationException>(() => df.AsQueryable(NonNull).Select(static r => r.M!["a"]).ToList());
        Assert.Contains("Int32?", error.Message);
    }
}
