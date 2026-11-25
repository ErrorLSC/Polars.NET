using Polars.CSharp.Linq;
using Polars.NET.Core.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Tests;

public enum LeShade { Red = 5, Green = 1, Blue = 3, Black = -2 }

[Flags]
public enum LeAccess { None = 0, Read = 1, Write = 2, Exec = 4 }

public record LeRow(int Id, LeShade Shade, LeShade? Opt, int Code);
public record LeIntRow(int Id, int Shade, int? Opt, int Code);
public record LeFlagRow(int Id, LeAccess Access);
public record LeNameRow(int Id, string Shade, string Opt, int Code);

/// <summary>
/// Rows of enum members as each layout stores them: a Polars Enum (what FromRows writes), integers of the underlying
/// values, the member names as strings (files, JSON) and as a Categorical.
/// </summary>
internal static class LeFrames
{
    public static DataFrame Of(LeRow[] rows, string layout)
    {
        switch (layout)
        {
            case "enum":
                return DataFrame.FromRows(rows);
            case "integer":
                return DataFrame.FromRows([.. rows.Select(r => new LeIntRow(r.Id, (int)r.Shade, (int?)r.Opt, r.Code))]);
            default:
                DataFrame names = DataFrame.FromRows([.. rows.Select(r => new LeNameRow(r.Id, r.Shade.ToString(), r.Opt?.ToString(), r.Code))]);
                if (layout == "names") return names;
                using (names)
                    return names.WithColumns(Polars.Col("Shade").Cast(DataType.Categorical()), Polars.Col("Opt").Cast(DataType.Categorical()));
        }
    }
}

/// <summary>
/// Queries over enum members, against Polars Enum columns (what FromRows writes), integer columns of underlying values
/// and columns of member names, as strings or Categoricals (data written elsewhere). Enum comparisons are on
/// underlying values in .NET; Enum categories follow value order, and comparisons over names compare the names whose
/// values satisfy them, so every layout must agree with LINQ to Objects, natively and without UDFs.
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

    public static TheoryData<string> Layouts => ["enum", "integer", "names", "categorical"];

    private static DataFrame Frame(string layout) => LeFrames.Of(Rows, layout);

    private static void AssertNative<T>(string layout, Func<IQueryable<LeRow>, IQueryable<T>> query)
    {
        using var df = Frame(layout);
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
        using var df = Frame(layout: "enum");
        Assert.True(df.Schema["Shade"].IsEnum);
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Equality(string layout)
    {
        AssertNative(layout, q => q.Where(r => r.Shade == LeShade.Blue).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => r.Shade != LeShade.Blue).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => LeShade.Blue == r.Shade).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Ordering_Comparisons_Follow_Underlying_Values(string layout)
    {
        AssertNative(layout, q => q.Where(r => r.Shade < LeShade.Blue).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => r.Shade >= LeShade.Green).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => LeShade.Green > r.Shade).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Comparisons_With_Undefined_Values(string layout)
    {
        AssertNative(layout, q => q.Where(r => r.Shade == (LeShade)42).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => r.Shade < (LeShade)2).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Nullable_Members(string layout)
    {
        AssertNative(layout, q => q.Where(r => r.Opt == LeShade.Red).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => r.Opt == null).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => r.Opt == r.Shade).Select(r => r.Id));
        AssertNative(layout, q => q.Select(r => r.Opt ?? LeShade.Green));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Captured_Values_And_Membership(string layout)
    {
        var green = LeShade.Green;
        AssertNative(layout, q => q.Where(r => r.Shade == green).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => RedOrGreen.Contains(r.Shade)).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Projection_Sorting_And_Grouping(string layout)
    {
        AssertNative(layout, q => q.Select(r => r.Shade));
        AssertNative(layout, q => q.Select(r => new { r.Id, r.Shade, r.Opt }));
        AssertNative(layout, q => q.OrderBy(r => r.Shade).ThenBy(r => r.Id).Select(r => r.Id));
        AssertNative(layout, q => q.GroupBy(r => r.Shade).Select(g => new { g.Key, Count = g.Count() }).OrderBy(x => x.Key));
        AssertNative(layout, q => q.GroupBy(r => 1).Select(g => g.Max(r => r.Shade)));
        AssertNative(layout, q => q.Select(r => r.Id > 2 ? r.Shade : LeShade.Red));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Conversions_Between_Enum_And_Value(string layout)
    {
        AssertNative(layout, q => q.Select(r => (int)r.Shade));
        AssertNative(layout, q => q.Where(r => (int)r.Shade > 2).Select(r => r.Id));
        AssertNative(layout, q => q.GroupBy(r => 1).Select(g => g.Sum(r => (int)r.Shade)));
        AssertNative(layout, q => q.Select(r => (LeShade)r.Code));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Enum_Methods(string layout)
    {
        AssertNative(layout, q => q.Select(r => r.Shade.ToString()));
        AssertNative(layout, q => q.Where(r => r.Opt != null).Select(r => r.Opt!.Value.ToString()));
        AssertNative(layout, q => q.Where(r => r.Shade.Equals(LeShade.Blue)).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => r.Shade.HasFlag(LeShade.Green)).Select(r => r.Id));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void CompareTo_And_Formats(string layout)
    {
        AssertNative(layout, q => q.Select(r => r.Shade.CompareTo(LeShade.Blue)));
        AssertNative(layout, q => q.Where(r => r.Shade.CompareTo(LeShade.Blue) > 0).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => r.Opt != null).Select(r => r.Shade.CompareTo(r.Opt!.Value)));
        AssertNative(layout, q => q.Select(r => r.Shade.ToString("G")));
        AssertNative(layout, q => q.Select(r => r.Shade.ToString("d")));
        // An undefined value made from a number has its number as its name.
        AssertNative(layout, q => q.Select(r => ((LeShade)(r.Code + 1)).ToString()));
    }

    [Fact]
    public void Formats_Without_A_Native_Form_Run_As_Udfs()
    {
        foreach (string layout in (string[])["enum", "integer", "names", "categorical"])
        {
            using var df = Frame(layout);
            var query = df.AsQueryable<LeRow>().Select(r => r.Shade.ToString("X"));

            Assert.True(((IQueryExplainable)query).InspectPlan(true).HasUdf);
            Assert.Equal(Rows.Select(r => r.Shade.ToString("X")), query.ToList());
        }
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Static_Enum_Methods(string layout)
    {
        AssertNative(layout, q => q.Select(r => Enum.GetName(r.Shade)));
        AssertNative(layout, q => q.Select(r => Enum.GetName(typeof(LeShade), r.Shade)));
        AssertNative(layout, q => q.Select(r => Enum.GetName((LeShade)(r.Code + 1))));
        AssertNative(layout, q => q.Select(r => Enum.GetName(typeof(LeShade), r.Code + 1)));
        AssertNative(layout, q => q.Where(r => Enum.IsDefined(r.Shade)).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => Enum.IsDefined((LeShade)(r.Code + 1))).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => Enum.IsDefined(typeof(LeShade), r.Code + 1)).Select(r => r.Id));
        AssertNative(layout, q => q.Where(r => Enum.IsDefined(typeof(LeShade), r.Shade.ToString().ToLower())).Select(r => r.Id));
        AssertNative(layout, q => q.Select(r => Enum.Parse<LeShade>(r.Shade.ToString())));
        AssertNative(layout, q => q.Select(r => Enum.Parse<LeShade>(" " + r.Shade.ToString().ToLower() + " ", true)));
        AssertNative(layout, q => q.Select(r => (LeShade)Enum.Parse(typeof(LeShade), r.Code.ToString())));
        AssertNative(layout, q => q.Where(r => Enum.Parse<LeShade>(r.Code.ToString()) == r.Shade).Select(r => r.Id));
        AssertNative(layout, q => q.Select(r => (int)Enum.Parse<LeShade>((r.Code + 1).ToString())));
        AssertNative(layout, q => q.Select(r => Convert.ToInt32(r.Shade)));
        AssertNative(layout, q => q.Select(r => Convert.ToInt64(r.Opt)));
        AssertNative(layout, q => q.Select(r => Convert.ToString(r.Shade)));
        AssertNative(layout, q => q.Select(r => Convert.ToString(r.Opt)));
    }

    /// <summary>Where .NET throws (text that is no member), the parse gives null, as the provider's other conversions.</summary>
    [Theory, MemberData(nameof(Layouts))]
    public void Parse_Of_Text_That_Is_No_Member_Gives_Null(string layout)
    {
        using var df = Frame(layout);
        var query = df.AsQueryable<LeRow>().Select(r => (LeShade?)Enum.Parse<LeShade>(r.Shade.ToString().ToLower()));

        Assert.False(((IQueryExplainable)query).InspectPlan(true).HasUdf);
        Assert.All(query.ToList(), Assert.Null);
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Min_And_Max_Follow_Underlying_Values(string layout)
    {
        AssertNative(layout, q => q.GroupBy(r => r.Code).Select(g => new { g.Key, Min = g.Min(r => r.Shade), Max = g.Max(r => r.Shade) }).OrderBy(x => x.Key));
        AssertNative(layout, q => q.GroupBy(r => r.Code).Select(g => new { g.Key, Max = g.Max(r => r.Opt) }).OrderBy(x => x.Key));
        AssertNative(layout, q => q.OrderByDescending(r => r.Opt).ThenBy(r => r.Id).Select(r => r.Id));
        AssertNative(layout, q => q.Select(r => r.Shade).Order());
        AssertNative(layout, q => q.Select(r => r.Opt).OrderDescending());
        AssertNative(layout, q => q.GroupBy(r => r.Code).Select(g => new { g.Key, g.MaxBy(r => r.Shade)!.Id }).OrderBy(x => x.Key));
        AssertNative(layout, q => q.GroupBy(r => r.Id % 2).Select(g => new { g.Key, Max = g.MaxBy(r => r.Shade)!.Id, Min = g.MinBy(r => r.Shade)!.Id }).OrderBy(x => x.Key));
        AssertNative(layout, q => q.GroupBy(r => r.Code).Select(g => new { g.Key, g.OrderBy(r => r.Shade).First().Id }).OrderBy(x => x.Key));

        using var df = Frame(layout);
        Assert.Equal(Rows.Max(r => r.Shade), df.AsQueryable<LeRow>().Max(r => r.Shade));
        Assert.Equal(Rows.Min(r => r.Shade), df.AsQueryable<LeRow>().Min(r => r.Shade));
        Assert.Equal(Rows.Min(r => r.Opt), df.AsQueryable<LeRow>().Min(r => r.Opt));
    }

    [Fact]
    public void Value_To_Enum_Conversion_Yields_An_Enum_Column()
    {
        using var df = Frame(layout: "integer");
        using var result = df.AsQueryable<LeRow>().Select(r => new { Shade = (LeShade)r.Code }).ToDataFrame();

        Assert.True(result.Schema["Shade"].IsEnum);
    }

    private static int Weight(LeShade s) => (int)s * 10;

    [Fact]
    public void Enum_Arguments_Reach_Udfs()
    {
        foreach (string layout in (string[])["enum", "integer", "names", "categorical"])
        {
            using var df = Frame(layout);
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
