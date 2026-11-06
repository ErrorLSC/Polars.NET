using Polars.CSharp.Linq;
using Polars.NET.Core.Exceptions;
using Polars.NET.Linq;

namespace Polars.CSharp.Tests;

public record NeEmp(string Name, string Dept, int Salary);
public record NeDept(string Dept, string Name, string Floor);
public record NeLoc(string Floor, string City);

/// <summary>
/// Whole entities projected as members (a join's <c>new { x, y }</c>, or <c>new { x, x.Name }</c> over one
/// table) are packed into struct columns and materialized as nested objects, natively. Every result is
/// compared with LINQ-to-Objects over the same rows.
/// </summary>
public class LinqNestedEntityTests
{
    // NeDept.Name collides with NeEmp.Name, and Dept is the join key on both sides.
    private static readonly NeEmp[] Emps = [new("a", "X", 1), new("b", "Y", 2), new("c", "Z", 3)];
    private static readonly NeDept[] Depts = [new("X", "dx", "1"), new("Y", "dy", "2"), new("W", "dw", "9")];
    private static readonly NeLoc[] Locs = [new("1", "P"), new("2", "Q")];

    private static bool IsNative<T>(IQueryable<T> query) =>
        !((IPolarsExplainable)query).InspectPlan(true).IsFullFallback;

    /// <summary>Compares results, in order, by their deep string form.</summary>
    private static void AssertSameRows<T>(IEnumerable<T> expected, IEnumerable<T> actual) =>
        Assert.Equal(
            expected.Select(r => r?.ToString() ?? "<null>"),
            actual.Select(r => r?.ToString() ?? "<null>"));

    private sealed class Sources : IDisposable
    {
        public DataFrame E { get; } = DataFrame.FromRows(Emps);
        public DataFrame D { get; } = DataFrame.FromRows(Depts);
        public DataFrame L { get; } = DataFrame.FromRows(Locs);

        public void Dispose()
        {
            E.Dispose();
            D.Dispose();
            L.Dispose();
        }
    }

    /// <summary>Runs the same query natively and over in-memory arrays, and compares the rows.</summary>
    private static void AssertNativeMatchesLinq<T>(
        Func<IQueryable<NeEmp>, IQueryable<NeDept>, IQueryable<NeLoc>, IQueryable<T>> query,
        bool expectNative = true)
    {
        using var s = new Sources();
        var native = query(s.E.AsQueryable<NeEmp>(), s.D.AsQueryable<NeDept>(), s.L.AsQueryable<NeLoc>());
        var expected = query(Emps.AsQueryable(), Depts.AsQueryable(), Locs.AsQueryable()).ToList();

        Assert.Equal(expectNative, IsNative(native));
        AssertSameRows(expected, native.ToList());
    }

    [Fact]
    public void Join_Entity_Pair_Materializes_Both_Entities() =>
        AssertNativeMatchesLinq((e, d, l) => e.Join(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y }));

    [Fact]
    public void Join_Entity_Pair_Plan_Packs_Each_Side_Into_A_Struct()
    {
        using var s = new Sources();
        var query = s.E.AsQueryable<NeEmp>().Join(s.D.AsQueryable<NeDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y });

        var plan = ((IPolarsExplainable)query).InspectPlan(true).LogicalPlan!.Value;
        Assert.Contains("as_struct", plan);

        using var frame = query.ToDataFrame();
        Assert.Equal(["x", "y"], frame.ColumnNames);
    }

    [Fact]
    public void Renamed_Members_Resolve_To_Their_Own_Side() =>
        // t.D.Name must read the right side's Name, not the left side's column of the same name.
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => new { E = x, D = y }).Where(t => t.D.Name == "dy"));

    [Fact]
    public void Result_Selector_Parameters_May_Differ_From_Key_Selectors() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, k => k.Dept, k => k.Dept, (emp, dept) => new { emp, dept }).Where(t => t.dept.Name == "dx"));

    [Fact]
    public void Entity_Next_To_Scalar_Members() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y.Floor, Tag = y.Name + "!" }));

    [Fact]
    public void Filtered_And_Ordered_Pairs() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y })
                .Where(t => t.y.Floor == "2" || t.x.Salary == 1)
                .OrderByDescending(t => t.x.Name)
                .Take(1));

    [Fact]
    public void Left_Join_Unmatched_Side_Is_Null() =>
        AssertNativeMatchesLinq((e, d, l) => e.LeftJoin(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y }));

    [Fact]
    public void Right_Join_Unmatched_Side_Is_Null() =>
        AssertNativeMatchesLinq((e, d, l) => e.RightJoin(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y }));

    [Fact]
    public void Query_Syntax_Left_Join_Pair() =>
        AssertNativeMatchesLinq((e, d, l) =>
            from x in e
            join y in d on x.Dept equals y.Dept into ys
            from y in ys.DefaultIfEmpty()
            select new { x, y });

    [Fact]
    public void Query_Syntax_Two_Left_Joins() =>
        AssertNativeMatchesLinq((e, d, l) =>
            from x in e
            join y in d on x.Dept equals y.Dept into ys
            from y in ys.DefaultIfEmpty()
            join z in l on x.Name equals z.City into zs
            from z in zs.DefaultIfEmpty()
            select new { x, y, z });

    [Fact]
    public void Query_Syntax_Left_Join_Keeps_Unmatched_Rows() =>
        AssertNativeMatchesLinq((e, d, l) =>
            from x in e
            join y in d on x.Dept equals y.Dept into ys
            from y in ys.DefaultIfEmpty()
            where y == null
            select new { x, y });

    [Fact]
    public void Left_Join_Null_Check_On_Entity() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.LeftJoin(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Where(t => t.y == null));

    [Fact]
    public void Three_Way_Query_Syntax() =>
        AssertNativeMatchesLinq((e, d, l) =>
            from x in e
            join y in d on x.Dept equals y.Dept
            join z in l on y.Floor equals z.Floor
            select new { x, y, z });

    [Fact]
    public void Nested_Transparent_Identifier() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.Join(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y })
                .Join(l, t => t.y.Floor, z => z.Floor, (t, z) => new { t, z }));

    [Fact]
    public void Nested_Left_Joins_Fold_Each_Missing_Entity() =>
        AssertNativeMatchesLinq((e, d, l) =>
            e.LeftJoin(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y })
                .LeftJoin(l, t => t.y == null ? null : t.y.Floor, z => z.Floor, (t, z) => new { t, z }));

    [Fact]
    public void Query_Let_Then_Select_Entity() =>
        AssertNativeMatchesLinq((e, d, l) =>
            from x in e
            join y in d on x.Dept equals y.Dept
            let f = y.Floor + "!"
            select new { x, f });

    [Fact]
    public void Select_Many_Pair() =>
        AssertNativeMatchesLinq((e, d, l) =>
            from x in e
            from y in d
            where x.Dept == y.Dept
            select new { x, y });

    [Fact]
    public void Single_Table_Row_Parameter_As_Member() =>
        AssertNativeMatchesLinq((e, d, l) => e.Select(x => new { x, x.Name, Upper = x.Name.ToUpper() }));

    [Fact]
    public void Computed_Member_Shadowing_A_Column_Does_Not_Change_The_Entity() =>
        // Name is recomputed, but x must keep the original Name.
        AssertNativeMatchesLinq((e, d, l) => e.Select(x => new { x, Name = x.Name.ToUpper() }));

    [Fact]
    public void Grouped_Elements_Are_Pairs()
    {
        using var s = new Sources();
        var groups = s.E.AsQueryable<NeEmp>()
            .Join(s.D.AsQueryable<NeDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y })
            .GroupBy(t => t.y.Floor)
            .ToList();

        var expected = Emps.Join(Depts, x => x.Dept, y => y.Dept, (x, y) => new { x, y }).GroupBy(t => t.y.Floor).ToList();

        Assert.Equal(expected.Select(g => g.Key), groups.Select(g => g.Key));
        foreach (var g in groups)
            AssertSameRows(expected.Single(e => e.Key == g.Key), g);
    }

    private static bool IsEven(NeEmp emp) => emp.Salary % 2 == 0;

    [Fact]
    public void Client_Predicate_Over_Pairs_Is_A_Row_Step_Not_A_Full_Fallback()
    {
        // Executing it needs client evaluation, which is allowed in Polars.Config.Tests (LinqClientRowPackingTests).
        using var s = new Sources();
        var query = s.E.AsQueryable<NeEmp>()
            .Join(s.D.AsQueryable<NeDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y })
            .Where(t => IsEven(t.x));

        var inspection = ((IPolarsExplainable)query).InspectPlan(true);
        Assert.False(inspection.IsFullFallback, inspection.FormatReport());
        Assert.True(inspection.HasClientPredicates);
    }

    private static string Describe(NeDept dept) => $"{dept.Name}@{dept.Floor}";

    [Fact]
    public void Untranslatable_Member_Is_Not_Materialized_From_Guessed_Columns()
    {
        // `Where` has no column; the rows must not be read natively. The default policy rejects the full
        // fallback instead of returning rows with a missing member.
        using var s = new Sources();
        var query = s.E.AsQueryable<NeEmp>()
            .Join(s.D.AsQueryable<NeDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, Where = Describe(y) });

        Assert.False(IsNative(query));
        Assert.Throws<PolarsPushdownException>(() => query.ToList());
    }

    [Fact]
    public void Scalar_Terminals_Over_Pairs()
    {
        using var s = new Sources();
        var pairs = s.E.AsQueryable<NeEmp>().Join(s.D.AsQueryable<NeDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y });
        var expected = Emps.Join(Depts, x => x.Dept, y => y.Dept, (x, y) => new { x, y }).ToList();

        Assert.Equal(expected.OrderBy(t => t.x.Name).First().ToString(), pairs.OrderBy(t => t.x.Name).First().ToString());
        Assert.Equal(expected.Single(t => t.y.Name == "dy").ToString(), pairs.Single(t => t.y.Name == "dy").ToString());
        Assert.Equal(expected.Count, pairs.Count());
        Assert.True(pairs.Any());
        Assert.Equal(expected.Max(t => t.x.Salary), pairs.Max(t => t.x.Salary));
    }

    [Fact]
    public async Task Async_Enumeration_Of_Pairs()
    {
        using var s = new Sources();
        var pairs = s.E.AsQueryable<NeEmp>().Join(s.D.AsQueryable<NeDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y });

        var rows = new List<string>();
        await foreach (var pair in (IAsyncEnumerable<object>)pairs)
            rows.Add(pair.ToString()!);

        var expected = Emps.Join(Depts, x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Select(t => t.ToString()!);
        Assert.Equal(expected, rows);
    }

    [Fact]
    public void Pair_Query_Grafted_Into_Another_Join_Reads_Flat_Columns() =>
        // The inner pair query is grafted as a plan; the outer key selector reads its entities' columns.
        AssertNativeMatchesLinq((e, d, l) =>
            l.Join(
                e.Join(d, x => x.Dept, y => y.Dept, (x, y) => new { x, y }),
                z => z.Floor,
                p => p.y.Floor,
                (z, p) => new { z.City, p.x.Name }));
}
