using Polars.CSharp.Linq;
using Polars.NET.Linq;

namespace Polars.CSharp.Tests;

/// <summary>
/// System tuples (Tuple.Create, ValueTuple.Create, new ValueTuple) are rows like anonymous types: their
/// entity elements are packed into struct columns Item1..ItemN, so they are read whole rather than guessed
/// from suffixed column names. Every result is compared, in order, with LINQ-to-Objects.
/// </summary>
public class LinqTupleRowTests
{
    private static readonly NeEmp[] Emps = [new("a", "X", 1), new("b", "Y", 2), new("c", "Z", 3)];
    private static readonly NeDept[] Depts = [new("X", "dx", "1"), new("Y", "dy", "2")];
    private static readonly NeLoc[] Locs = [new("1", "P"), new("2", "Q")];

    private static void AssertNativeMatchesLinq<T>(Func<IQueryable<NeEmp>, IQueryable<NeDept>, IQueryable<NeLoc>, IQueryable<T>> query)
    {
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts);
        using var l = DataFrame.FromRows(Locs);
        var native = query(e.AsQueryable<NeEmp>(), d.AsQueryable<NeDept>(), l.AsQueryable<NeLoc>());
        var expected = query(Emps.AsQueryable(), Depts.AsQueryable(), Locs.AsQueryable()).Select(r => r!.ToString()).ToList();

        Assert.False(((IPolarsExplainable)native).InspectPlan(true).IsFullFallback);
        Assert.Equal(expected, native.ToList().Select(r => r!.ToString()));
    }

    [Fact]
    public void Scalar_Tuple_Create() =>
        AssertNativeMatchesLinq((e, d, l) => e.Select(x => Tuple.Create(x.Name, x.Salary)));

    [Fact]
    public void Scalar_ValueTuple_Create() =>
        AssertNativeMatchesLinq((e, d, l) => e.Select(x => ValueTuple.Create(x.Name, x.Salary)));

    [Fact]
    public void Scalar_New_ValueTuple_Then_Filter() =>
        AssertNativeMatchesLinq((e, d, l) => e.Select(x => new ValueTuple<string, int>(x.Name, x.Salary)).Where(t => t.Item2 > 1));

    [Fact]
    public void Row_Entity_Next_To_Its_Field() =>
        AssertNativeMatchesLinq((e, d, l) => e.Select(x => Tuple.Create(x, x.Name)));

    [Fact]
    public void Join_Tuple_Pair() =>
        AssertNativeMatchesLinq((e, d, l) => e.Join(d, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y)));

    [Fact]
    public void Join_ValueTuple_Pair() =>
        AssertNativeMatchesLinq((e, d, l) => e.Join(d, x => x.Dept, y => y.Dept, (x, y) => ValueTuple.Create(x, y)));

    [Fact]
    public void Join_Tuple_Pair_Filtered_On_Colliding_Column() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y)).Where(t => t.Item2.Name == "dy"));

    [Fact]
    public void Join_Tuple_Of_Entity_And_Field() =>
        AssertNativeMatchesLinq((e, d, l) => e.Join(d, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y.Name)));

    [Fact]
    public void Left_Join_Tuple_Pair_Unmatched_Side_Is_Null() =>
        AssertNativeMatchesLinq((e, d, l) => e.LeftJoin(d, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y)));

    [Fact]
    public void Three_Way_Tuple() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y))
                .Join(l, t => t.Item2.Floor, z => z.Floor, (t, z) => Tuple.Create(t.Item1, t.Item2, z)));

    [Fact]
    public void Nested_Tuple() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y))
                .Join(l, t => t.Item2.Floor, z => z.Floor, (t, z) => Tuple.Create(t, z)));

    [Fact]
    public void Tuple_Pair_Projected_To_Fields() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y)).Select(t => new { t.Item1.Name, D = t.Item2.Name }));

    [Fact]
    public void Tuple_Pair_Plan_Packs_Each_Element()
    {
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts);
        var query = e.AsQueryable<NeEmp>().Join(d.AsQueryable<NeDept>(), x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y));

        using var frame = query.ToDataFrame();
        Assert.Equal(["Item1", "Item2"], frame.ColumnNames);
    }
}
