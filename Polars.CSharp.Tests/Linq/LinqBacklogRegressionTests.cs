using Polars.CSharp.Linq;
using Polars.NET.Linq;

namespace Polars.CSharp.Tests.Linq;

public enum NestedHue { Red = 1, Green = 2, Blue = 4 }

/// <summary>
/// Regression tests for backlog items: indexing a dictionary a UDF returns, a right join after a transparent
/// join, and enums nested in struct and list columns. Results are compared with LINQ to Objects.
/// </summary>
public class LinqBacklogRegressionTests
{
    public class Text
    {
        public long Id { get; set; }
        public string? Value { get; set; }
    }

    public static Dictionary<string, long> Lengths(string? s) => new() { ["len"] = s?.Length ?? -1, ["zero"] = 0 };

    [Fact]
    public void Indexing_A_Dictionary_Is_Not_A_List_Operation()
    {
        Text[] rows = [new() { Id = 1, Value = "abc" }, new() { Id = 2, Value = null }];
        using var df = DataFrame.From(rows);

        Assert.Equal(rows.Select(x => Lengths(x.Value)["len"]), df.AsQueryable<Text>().Select(x => Lengths(x.Value)["len"]).ToList());
        Assert.Equal(
            rows.Select(x => new { x.Id, n = Lengths(x.Value).Count, has = Lengths(x.Value).ContainsKey("len") }),
            df.AsQueryable<Text>().Select(x => new { x.Id, n = Lengths(x.Value).Count, has = Lengths(x.Value).ContainsKey("len") }).ToList());
    }

    public class A { public long K { get; set; } public string Va { get; set; } = ""; }
    public class B { public long K { get; set; } public string Vb { get; set; } = ""; }
    public class C { public long K { get; set; } public string Vc { get; set; } = ""; }

    [Fact]
    public void Right_Join_After_A_Transparent_Join_Reads_Both_Scopes()
    {
        A[] a = [new() { K = 1, Va = "a1" }, new() { K = 2, Va = "a2" }];
        B[] b = [new() { K = 1, Vb = "b1" }, new() { K = 2, Vb = "b2" }];
        C[] c = [new() { K = 2, Vc = "c2" }, new() { K = 3, Vc = "c3" }];
        using var adf = DataFrame.From(a);
        using var bdf = DataFrame.From(b);
        using var cdf = DataFrame.From(c);

        var other = (from x in adf.AsQueryable<A>() join y in bdf.AsQueryable<B>() on x.K equals y.K select new { x, y })
            .RightJoin(cdf.AsQueryable<C>(), t => t.x.K, z => z.K, (t, z) => new { Va = t == null ? null : t.x.Va, Vb = t == null ? null : t.y.Vb, z.Vc });
        var sameNames = (from x in adf.AsQueryable<A>() join y in bdf.AsQueryable<B>() on x.K equals y.K select new { x, y })
            .RightJoin(adf.AsQueryable<A>(), t => t.x.K, z => z.K, (t, z) => new { L = t == null ? null : t.x.Va, R = z.Va, Kl = t == null ? -1 : t.x.K, Kr = z.K });

        Assert.Equal(
            (from x in a join y in b on x.K equals y.K select new { x, y })
                .RightJoin(c, t => t.x.K, z => z.K, (t, z) => new { Va = t == null ? null : t.x.Va, Vb = t == null ? null : t.y.Vb, z.Vc }),
            other.ToList());
        Assert.Equal(
            (from x in a join y in b on x.K equals y.K select new { x, y })
                .RightJoin(a, t => t.x.K, z => z.K, (t, z) => new { L = t == null ? null : t.x.Va, R = z.Va, Kl = t == null ? -1 : t.x.K, Kr = z.K }),
            sameNames.ToList());
    }

    public class Paint
    {
        public NestedHue Hue { get; set; }
        public NestedHue? Maybe { get; set; }
    }

    public class Item
    {
        public long Id { get; set; }
        public Paint Paint { get; set; } = new();
        public List<NestedHue> Hues { get; set; } = [];
    }

    private static readonly Item[] Items =
    [
        new() { Id = 1, Paint = new() { Hue = NestedHue.Green, Maybe = null }, Hues = [NestedHue.Red, NestedHue.Blue] },
        new() { Id = 2, Paint = new() { Hue = NestedHue.Red, Maybe = NestedHue.Blue }, Hues = [NestedHue.Green] },
        new() { Id = 3, Paint = new() { Hue = NestedHue.Blue, Maybe = NestedHue.Red }, Hues = [] },
    ];

    [Fact]
    public void Enums_Nested_In_Struct_And_List_Columns_Translate()
    {
        using var df = DataFrame.From(Items);
        var q = df.AsQueryable<Item>();

        Assert.Equal(Items.Where(x => x.Paint.Hue == NestedHue.Green).Select(x => x.Id), q.Where(x => x.Paint.Hue == NestedHue.Green).Select(x => x.Id).ToList());
        Assert.Equal(Items.Where(x => x.Paint.Hue > NestedHue.Red).Select(x => x.Id), q.Where(x => x.Paint.Hue > NestedHue.Red).Select(x => x.Id).ToList());
        Assert.Equal(Items.Where(x => x.Paint.Maybe == null).Select(x => x.Id), q.Where(x => x.Paint.Maybe == null).Select(x => x.Id).ToList());
        Assert.Equal(Items.Where(x => x.Hues.Contains(NestedHue.Blue)).Select(x => x.Id), q.Where(x => x.Hues.Contains(NestedHue.Blue)).Select(x => x.Id).ToList());
        Assert.Equal(Items.OrderBy(x => x.Paint.Hue).Select(x => x.Id), q.OrderBy(x => x.Paint.Hue).Select(x => x.Id).ToList());
        Assert.Equal(
            Items.Select(x => new { x.Id, name = x.Paint.Hue.ToString(), value = (int)x.Paint.Hue, all = string.Join(",", x.Hues), n = x.Hues.Count }),
            q.Select(x => new { x.Id, name = x.Paint.Hue.ToString(), value = (int)x.Paint.Hue, all = string.Join(",", x.Hues), n = x.Hues.Count }).ToList());
        Assert.Equal(
            Items.Where(x => x.Hues.Count > 0).Select(x => new { x.Id, first = x.Hues[0] }),
            q.Where(x => x.Hues.Count > 0).Select(x => new { x.Id, first = x.Hues[0] }).ToList());
        Assert.Equal(
            Items.GroupBy(x => x.Paint.Hue).Select(g => new { g.Key, n = g.Count() }).OrderBy(r => r.Key),
            q.GroupBy(x => x.Paint.Hue).Select(g => new { g.Key, n = g.Count() }).OrderBy(r => r.Key).ToList());
    }
}
