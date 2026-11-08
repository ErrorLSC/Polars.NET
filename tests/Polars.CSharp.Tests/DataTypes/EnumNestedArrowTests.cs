using System.Text.Json;
using Polars.NET.Core.Exceptions;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public enum NaShade { Red = 5, Green = 1, Black = -2 }

[Flags]
public enum NaAccess { None = 0, Read = 1, Write = 2 }

public record NaInner(int X, NaShade Shade, NaShade? Opt, NaAccess Access);

public record NaRow(int Id, NaInner Inner, List<NaShade> Shades, NaShade[] Arr, List<NaShade?> OptShades, List<NaInner> Inners);

public class NaMutableRow
{
    public int Id { get; set; }
    public NaShade Shade { get; set; }
    public List<NaShade> Shades { get; set; } = [];
    public NaInner Inner { get; set; }
}

/// <summary>
/// Enums nested in struct members and list elements are written as Arrow dictionaries carrying Polars' Enum
/// metadata, so they become Enum fields (categories in underlying-value order) wherever they sit. [Flags] enums
/// stay their underlying integers.
/// </summary>
public class EnumNestedArrowTests
{
    private static readonly string[] Categories = ["Black", "Green", "Red"];

    private static readonly NaRow[] Rows =
    [
        new(1, new(10, NaShade.Red, null, NaAccess.Read | NaAccess.Write), [NaShade.Green, NaShade.Black], [NaShade.Red],
            [null, NaShade.Red], [new(1, NaShade.Black, NaShade.Green, NaAccess.None)]),
        new(2, new(20, NaShade.Black, NaShade.Green, NaAccess.None), [], [NaShade.Black, NaShade.Green], [NaShade.Green], []),
    ];

    private static void AssertEnum(DataType dtype)
    {
        Assert.True(dtype.IsEnum, dtype.ToString());
        Assert.Equal(Categories, dtype.EnumCategories!.GetCategories());
    }

    private static DataType StructField(DataType dtype, string name) =>
        dtype.StructFields!.Single(f => f.Name == name).DataType;

    private static void AssertNestedEnumSchema(DataFrame df)
    {
        AssertEnum(StructField(df.Schema["Inner"], "Shade"));
        AssertEnum(StructField(df.Schema["Inner"], "Opt"));
        Assert.Equal(DataType.Int32, StructField(df.Schema["Inner"], "Access"));
        AssertEnum(df.Schema["Shades"].InnerType!);
        AssertEnum(df.Schema["Arr"].InnerType!);
        AssertEnum(df.Schema["OptShades"].InnerType!);
        AssertEnum(StructField(df.Schema["Inners"].InnerType!, "Shade"));
    }

    private static string Json<T>(IEnumerable<T> rows) => JsonSerializer.Serialize(rows.ToList());

    [Fact]
    public void FromRows_Writes_Nested_Enums_And_Reads_Them_Back()
    {
        using var df = DataFrame.FromRows(Rows);

        AssertNestedEnumSchema(df);
        Assert.Equal(Json(Rows), Json(df.Rows<NaRow>().ToList()));
    }

    [Fact]
    public void FromRows_Streaming_Path_Writes_Nested_Enums()
    {
        using var df = DataFrame.FromRows(Rows.Where(_ => true));

        AssertNestedEnumSchema(df);
        Assert.Equal(Json(Rows), Json(df.Rows<NaRow>().ToList()));
    }

    [Fact]
    public void FromEnumerable_Writes_Nested_Enums()
    {
        using var df = DataFrame.FromEnumerable(Rows);
        AssertNestedEnumSchema(df);

        // A class whose default enum value (0) is not a member: the schema is not inferred from sample instances.
        using var mutable = DataFrame.FromEnumerable(new[]
        {
            new NaMutableRow { Id = 1, Shade = NaShade.Red, Shades = [NaShade.Black], Inner = Rows[0].Inner }
        });
        AssertEnum(mutable.Schema["Shade"]);
        AssertEnum(mutable.Schema["Shades"].InnerType!);
        AssertEnum(StructField(mutable.Schema["Inner"], "Shade"));
    }

    [Fact]
    public void Series_Of_Lists_And_Structs_Hold_Enums()
    {
        using var lists = Series.From("s", new List<NaShade>[] { [NaShade.Red], [] });
        AssertEnum(lists.DataType.InnerType!);

        using var structs = Series.From("s", new[] { Rows[0].Inner });
        AssertEnum(StructField(structs.DataType, "Shade"));
    }

    [Fact]
    public void Undefined_Nested_Value_Is_Rejected()
    {
        NaRow[] rows = [new(1, new(1, (NaShade)42, null, NaAccess.None), [], [], [], [])];

        var error = Assert.Throws<ArgumentException>(() => DataFrame.FromRows(rows));
        Assert.Contains("42", error.Message);
        Assert.Contains(nameof(NaShade), error.Message);
    }

    [Fact]
    public void Nested_Enums_Survive_An_Arrow_Round_Trip()
    {
        using var df = DataFrame.FromRows(Rows);
        using var back = DataFrame.FromArrow(df.ToArrow());

        AssertNestedEnumSchema(back);
        Assert.Equal(Json(Rows), Json(back.Rows<NaRow>().ToList()));
    }

    [Fact]
    public void DataType_From_Net_Type_Maps_Enums()
    {
        AssertEnum(DataType.FromNetType<NaShade>());
        AssertEnum(DataType.FromNetType<NaShade?>());
        Assert.Equal(DataType.Int32, DataType.FromNetType<NaAccess>());
    }

    [Fact]
    public void Udf_Results_Of_Enum_Type()
    {
        using var df = new DataFrame(Series.From("i", new[] { 1, 2, 3 }));
        using var result = df.Select(
            Pl.Col("i").MapBatches<int, NaShade>(i => i == 1 ? NaShade.Red : NaShade.Black).Alias("e"),
            Pl.Col("i").MapBatches<int, NaShade?>(i => i == 2 ? null : NaShade.Green).Alias("n"),
            Pl.Col("i").MapBatches<int, NaAccess>(i => (NaAccess)i).Alias("f"));

        AssertEnum(result.Schema["e"]);
        AssertEnum(result.Schema["n"]);
        Assert.Equal(DataType.Int32, result.Schema["f"]);
        Assert.Equal(["Red", "Black", "Black"], result["e"].Cast(DataType.String).ToArray<string>());
        Assert.Equal(["Green", null, "Green"], result["n"].Cast(DataType.String).ToArray<string>());
        Assert.Equal([1, 2, 3], result["f"].ToArray<int>());
    }

    [Fact]
    public void Udf_Result_Of_An_Undefined_Enum_Value_Is_Rejected()
    {
        using var df = new DataFrame(Series.From("i", new[] { 1 }));

        var error = Assert.Throws<PolarsException>(() => df.Select(Pl.Col("i").MapBatches<int, NaShade>(i => (NaShade)42)));
        Assert.Contains("not a defined member", error.Message);
    }
}
