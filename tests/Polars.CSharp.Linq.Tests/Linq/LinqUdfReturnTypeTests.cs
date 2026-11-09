using Polars.CSharp.Linq;
using Polars.NET.Core.Exceptions;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Tests;

public enum UrColor { Red, Green }
public record UrRow(int Id);

/// <summary>
/// A method whose result has no Arrow representation (object, Int128, a class without readable members) is not
/// pushed down as a UDF, whose output column could not hold it; it is evaluated on the client. An enum result is
/// pushed down: the UDF yields underlying values, which become the enum's Polars Enum categories.
/// </summary>
public class LinqUdfReturnTypeTests
{
    private static readonly UrRow[] Rows = [new(1), new(2)];

    private sealed class MemberLess
    {
        public override string ToString() => "member-less";
    }

    private static UrColor ToColor(int i) => i % 2 == 0 ? UrColor.Red : UrColor.Green;
    private static object ToObject(int i) => i % 2 == 0 ? "s" + i : i;
    private static Int128 ToInt128(int i) => (Int128)i * 1_000_000_000_000L;
    private static MemberLess ToMemberLess(int i) => new();

    private static void AssertClientProjectionMatchesLinq<T>(Func<IQueryable<UrRow>, IQueryable<T>> query)
    {
        using var df = DataFrame.FromRows(Rows);
        var native = query(df.AsQueryable<UrRow>());

        var inspection = ((IQueryExplainable)native).InspectPlan(true);
        Assert.False(inspection.HasUdf, inspection.FormatReport());
        Assert.Equal(
            query(Rows.AsQueryable()).Select(r => r!.ToString()),
            native.ToList().Select(r => r!.ToString()));
    }

    [Fact]
    public void Enum_Result_Is_Pushed_Down_As_An_Enum_Column()
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<UrRow>().Select(r => new { r.Id, C = ToColor(r.Id) });

        Assert.True(((IQueryExplainable)query).InspectPlan(true).HasUdf);
        Assert.Equal(Rows.Select(r => new { r.Id, C = ToColor(r.Id) }), query.ToList());

        using var result = query.ToDataFrame();
        Assert.True(result.Schema["C"].IsEnum);
    }

    [Fact]
    public void Object_Result() =>
        AssertClientProjectionMatchesLinq(q => q.Select(r => new { r.Id, C = ToObject(r.Id) }));

    [Fact]
    public void Int128_Result() =>
        AssertClientProjectionMatchesLinq(q => q.Select(r => new { r.Id, C = ToInt128(r.Id) }));

    [Fact]
    public void Member_Less_Class_Result() =>
        AssertClientProjectionMatchesLinq(q => q.Select(r => new { r.Id, C = ToMemberLess(r.Id) }));

    [Fact]
    public void Cast_To_Enum_Maps_Values_To_Categories()
    {
        // Not a cast of the column: Polars casts integers to an Enum by category index, not by value.
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<UrRow>().Select(r => new { r.Id, C = (UrColor)(r.Id % 2) });

        var inspection = ((IQueryExplainable)query).InspectPlan(true);
        Assert.False(inspection.HasUdf, inspection.FormatReport());
        Assert.False(inspection.HasClientProjection, inspection.FormatReport());
        Assert.False(inspection.IsFullFallback, inspection.FormatReport());
        Assert.Equal(Rows.Select(r => new { r.Id, C = (UrColor)(r.Id % 2) }), query.ToList());
    }

    [Fact]
    public void Cast_Of_An_Undefined_Value_To_Enum_Is_Rejected()
    {
        // Id 2 is not a member of UrColor; an Enum column cannot hold it.
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<UrRow>().Select(r => (UrColor)r.Id);

        var error = Assert.Throws<PolarsException>(() => query.ToList());
        Assert.Contains($"Value '2' is not a defined member of enum {typeof(UrColor).FullName}", error.Message);
    }

    [Fact]
    public void Enum_Result_In_A_Predicate_Is_Pushed_Down()
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<UrRow>().Where(r => ToColor(r.Id) == UrColor.Green).Select(r => r.Id);

        var inspection = ((IQueryExplainable)query).InspectPlan(true);
        Assert.True(inspection.HasUdf, inspection.FormatReport());
        Assert.False(inspection.HasClientPredicates, inspection.FormatReport());
        Assert.Equal(Rows.Where(r => ToColor(r.Id) == UrColor.Green).Select(r => r.Id), query.ToList());
    }
}
