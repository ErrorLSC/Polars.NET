using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

/// <summary>
/// Expressions compiled outside a query pipeline (typed merge conditions and setters, ToLookup selectors) read the
/// frames' column types, so enum members work against Polars Enum columns and integer columns alike, and a setter
/// keeps the target column's representation.
/// </summary>
public class LinqEnumStandaloneTests
{
    private static readonly LeRow[] Target =
    [
        new(1, LeShade.Red, null, 0), new(2, LeShade.Blue, null, 0), new(3, LeShade.Black, null, 0)
    ];

    private static readonly LeRow[] Source =
    [
        new(1, LeShade.Green, null, 0), new(2, LeShade.Red, null, 0), new(3, LeShade.Blue, null, 0)
    ];

    public static TheoryData<bool> Layouts => new() { false, true };

    private static DataFrame Frame(LeRow[] rows, bool integerColumns) =>
        integerColumns
            ? DataFrame.FromRows(rows.Select(r => new LeIntRow(r.Id, (int)r.Shade, (int?)r.Opt, r.Code)).ToArray())
            : DataFrame.FromRows(rows);

    private static List<(int Id, LeShade Shade)> Shades(DataFrame df)
    {
        using var sorted = df.Sort("Id");
        return sorted.Rows<LeRow>().ToList().Select(r => (r.Id, r.Shade)).ToList();
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Merge_Condition_Compares_Enum_Values(bool integerColumns)
    {
        using var target = Frame(Target, integerColumns);
        using var source = Frame(Source, integerColumns);

        // Source shade below the target's (by underlying value) replaces it: Id 2 (Red 5 > Blue 3 is false),
        // Id 1 (Green 1 < Red 5) and Id 3 (Blue 3 > Black -2 is false).
        using var result = target.AsQueryable<LeRow>()
            .Merge(source.AsQueryable<LeRow>(), t => t.Id, s => s.Id)
            .WhenMatched((t, s) => s.Shade < t.Shade, then => then.Update(set => set.Set(t => t.Shade, (t, s) => s.Shade)))
            .ToDataFrame();

        Assert.Equal([(1, LeShade.Green), (2, LeShade.Blue), (3, LeShade.Black)], Shades(result));
        Assert.Equal(!integerColumns, result.Schema["Shade"].IsEnum);
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Merge_Condition_With_An_Enum_Constant(bool integerColumns)
    {
        using var target = Frame(Target, integerColumns);
        using var source = Frame(Source, integerColumns);

        using var result = target.AsQueryable<LeRow>()
            .Merge(source.AsQueryable<LeRow>(), t => t.Id, s => s.Id)
            .WhenMatched((t, s) => s.Shade == LeShade.Blue, then => then.Delete())
            .ToDataFrame();

        Assert.Equal([(1, LeShade.Red), (2, LeShade.Blue)], Shades(result));
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Merge_Constant_Setter_Keeps_The_Column_Representation(bool integerColumns)
    {
        using var target = Frame(Target, integerColumns);
        using var source = Frame(Source, integerColumns);

        using var result = target.AsQueryable<LeRow>()
            .Merge(source.AsQueryable<LeRow>(), t => t.Id, s => s.Id)
            .WhenMatched((t, s) => t.Id == 2, then => then.Update(set => set.Set(t => t.Shade, LeShade.Green)))
            .ToDataFrame();

        Assert.Equal([(1, LeShade.Red), (2, LeShade.Green), (3, LeShade.Black)], Shades(result));
        Assert.Equal(!integerColumns, result.Schema["Shade"].IsEnum);
    }

    [Theory, MemberData(nameof(Layouts))]
    public void Merge_Expression_Setter_With_An_Enum_Constant(bool integerColumns)
    {
        using var target = Frame(Target, integerColumns);
        using var source = Frame(Source, integerColumns);

        using var result = target.AsQueryable<LeRow>()
            .Merge(source.AsQueryable<LeRow>(), t => t.Id, s => s.Id)
            .WhenMatched((t, s) => t.Id == 3, then => then.Update(set => set.Set(t => t.Shade, (t, s) => LeShade.Green)))
            .ToDataFrame();

        Assert.Equal([(1, LeShade.Red), (2, LeShade.Blue), (3, LeShade.Green)], Shades(result));
        Assert.Equal(!integerColumns, result.Schema["Shade"].IsEnum);
    }

    [Theory, MemberData(nameof(Layouts))]
    public void ToLookup_Selectors_Over_Enum_Members(bool integerColumns)
    {
        using var df = Frame(Target, integerColumns);

        var byShade = df.AsQueryable<LeRow>().ToLookup(r => r.Shade, r => (int)r.Shade);
        Assert.Equal([3], byShade[LeShade.Blue]);
        Assert.Equal([-2], byShade[LeShade.Black]);

        var names = df.AsQueryable<LeRow>().ToLookup(r => r.Id, r => r.Shade.ToString());
        Assert.Equal(["Blue"], names[2]);
    }
}
