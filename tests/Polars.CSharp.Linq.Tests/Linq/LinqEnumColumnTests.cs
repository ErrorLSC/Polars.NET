using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Tests;

public enum LeShade { Red = 5, Green = 1, Blue = 3, Black = -2 }

[Flags]
public enum LeAccess { None = 0, Read = 1, Write = 2, Exec = 4 }

public record LeRow(int Id, LeShade Shade, LeShade? Opt, int Code);
public record LeIntRow(int Id, int Shade, int? Opt, int Code);
public record LeFlagRow(int Id, LeAccess Access);

/// <summary>
/// Queries over enum members, against Polars Enum columns (what FromRows writes) and against integer columns of
/// underlying values (data written elsewhere). Enum comparisons are on underlying values in .NET; Enum categories
/// follow value order, so both layouts must agree with LINQ to Objects, natively and without UDFs.
/// </summary>
public class LinqEnumColumnTests
{
    // Declaration order differs from value order, and Black is negative.
    private static readonly LeRow[] Rows =
    [
        new(1, LeShade.Red, null, 1), new(2, LeShade.Blue, LeShade.Green, 3), new(3, LeShade.Black, LeShade.Red, 5),
        new(4, LeShade.Green, null, -2), new(5, LeShade.Blue, LeShade.Black, 3)
    ];

    private static readonly LeShade[] RedOrGreen = [LeShade.Red, LeShade.Green];

    public static TheoryData<bool> Layouts => new() { false, true };

    private static DataFrame Frame(bool integerColumns) =>
        integerColumns
            ? DataFrame.FromRows(Rows.Select(r => new LeIntRow(r.Id, (int)r.Shade, (int?)r.Opt, r.Code)).ToArray())
            : DataFrame.FromRows(Rows);

    private static void AssertNative<T>(bool integerColumns, Func<IQueryable<LeRow>, IQueryable<T>> query)
    {
        using var df = Frame(integerColumns);
        var native = query(df.AsQueryable<LeRow>());

        var inspection = ((IQueryExplainable)native).InspectPlan(true);
        Assert.False(inspection.HasUdf, inspection.FormatReport());
        Assert.False(inspection.HasClientPredicates, inspection.FormatReport());
        Assert.False(inspection.IsFullFallback, inspection.FormatReport());
        Assert.Equal(query(Rows.AsQueryable()).ToList(), native.ToList());
    }

    [Fact]
    public void FromRows_Writes_An_Enum_Column_That_The_Query_Reads()
    {
        using var df = Frame(integerColumns: false);
        Assert.True(df.Schema["Shade"].IsEnum);
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Equality(bool integerColumns)
    {
        AssertNative(integerColumns, q => q.Where(r => r.Shade == LeShade.Blue).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => r.Shade != LeShade.Blue).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => LeShade.Blue == r.Shade).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Ordering_Comparisons_Follow_Underlying_Values(bool integerColumns)
    {
        AssertNative(integerColumns, q => q.Where(r => r.Shade < LeShade.Blue).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => r.Shade >= LeShade.Green).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => LeShade.Green > r.Shade).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Comparisons_With_Undefined_Values(bool integerColumns)
    {
        AssertNative(integerColumns, q => q.Where(r => r.Shade == (LeShade)42).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => r.Shade < (LeShade)2).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Nullable_Members(bool integerColumns)
    {
        AssertNative(integerColumns, q => q.Where(r => r.Opt == LeShade.Red).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => r.Opt == null).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => r.Opt == r.Shade).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Select(r => r.Opt ?? LeShade.Green));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Captured_Values_And_Membership(bool integerColumns)
    {
        var green = LeShade.Green;
        AssertNative(integerColumns, q => q.Where(r => r.Shade == green).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => RedOrGreen.Contains(r.Shade)).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Projection_Sorting_And_Grouping(bool integerColumns)
    {
        AssertNative(integerColumns, q => q.Select(r => r.Shade));
        AssertNative(integerColumns, q => q.Select(r => new { r.Id, r.Shade, r.Opt }));
        AssertNative(integerColumns, q => q.OrderBy(r => r.Shade).ThenBy(r => r.Id).Select(r => r.Id));
        AssertNative(integerColumns, q => q.GroupBy(r => r.Shade).Select(g => new { g.Key, Count = g.Count() }).OrderBy(x => x.Key));
        AssertNative(integerColumns, q => q.GroupBy(r => 1).Select(g => g.Max(r => r.Shade)));
        AssertNative(integerColumns, q => q.Select(r => r.Id > 2 ? r.Shade : LeShade.Red));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Conversions_Between_Enum_And_Value(bool integerColumns)
    {
        AssertNative(integerColumns, q => q.Select(r => (int)r.Shade));
        AssertNative(integerColumns, q => q.Where(r => (int)r.Shade > 2).Select(r => r.Id));
        AssertNative(integerColumns, q => q.GroupBy(r => 1).Select(g => g.Sum(r => (int)r.Shade)));
        AssertNative(integerColumns, q => q.Select(r => (LeShade)r.Code));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Enum_Methods(bool integerColumns)
    {
        AssertNative(integerColumns, q => q.Select(r => r.Shade.ToString()));
        AssertNative(integerColumns, q => q.Where(r => r.Opt != null).Select(r => r.Opt!.Value.ToString()));
        AssertNative(integerColumns, q => q.Where(r => r.Shade.Equals(LeShade.Blue)).Select(r => r.Id));
        AssertNative(integerColumns, q => q.Where(r => r.Shade.HasFlag(LeShade.Green)).Select(r => r.Id));
    }

    [Fact]
    public void Value_To_Enum_Conversion_Yields_An_Enum_Column()
    {
        using var df = Frame(integerColumns: true);
        using var result = df.AsQueryable<LeRow>().Select(r => new { Shade = (LeShade)r.Code }).ToDataFrame();

        Assert.True(result.Schema["Shade"].IsEnum);
    }

    private static int Weight(LeShade s) => (int)s * 10;

    [Fact]
    public void Enum_Arguments_Reach_Udfs()
    {
        bool[] integerColumnsValues = [false, true];
        foreach (var integerColumns in integerColumnsValues)
        {
            using var df = Frame(integerColumns);
            var query = df.AsQueryable<LeRow>().Select(r => Weight(r.Shade));

            Assert.True(((IQueryExplainable)query).InspectPlan(true).HasUdf);
            Assert.Equal(Rows.Select(r => Weight(r.Shade)), query.ToList());
        }
    }

    private static readonly LeFlagRow[] FlagRows =
    [
        new(1, LeAccess.Read | LeAccess.Write), new(2, LeAccess.None), new(3, LeAccess.Exec | LeAccess.Read)
    ];

    private static void AssertFlags<T>(Func<IQueryable<LeFlagRow>, IQueryable<T>> query, bool expectUdf = false)
    {
        using var df = DataFrame.FromRows(FlagRows);
        var native = query(df.AsQueryable<LeFlagRow>());

        var inspection = ((IQueryExplainable)native).InspectPlan(true);
        Assert.Equal(expectUdf, inspection.HasUdf);
        Assert.False(inspection.IsFullFallback, inspection.FormatReport());
        Assert.Equal(query(FlagRows.AsQueryable()).ToList(), native.ToList());
    }

    [Fact]
    public void Flags_Enums_Are_Integer_Columns()
    {
        AssertFlags(q => q.Where(r => r.Access.HasFlag(LeAccess.Read)).Select(r => r.Id));
        AssertFlags(q => q.Where(r => r.Access == (LeAccess.Read | LeAccess.Write)).Select(r => r.Id));
        AssertFlags(q => q.Where(r => (r.Access & LeAccess.Exec) != 0).Select(r => r.Id));
        AssertFlags(q => q.Select(r => r.Access));
        AssertFlags(q => q.Select(r => (int)r.Access));
        // .NET formats combined flags ("Read, Write"), which has no native counterpart.
        AssertFlags(q => q.Select(r => r.Access.ToString()), expectUdf: true);
    }
}
