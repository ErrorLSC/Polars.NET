using Polars.CSharp.Linq;
using Polars.NET.Core.Exceptions;
using Polars.NET.Linq;

namespace Polars.CSharp.Tests;

public enum UrColor { Red, Green }
public record UrRow(int Id);

/// <summary>
/// A method whose result has no Arrow representation (an enum, object, Int128, a class without readable
/// members) is not pushed down as a UDF, whose output column could not hold it; it is evaluated on the client.
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

        var inspection = ((IPolarsExplainable)native).InspectPlan(true);
        Assert.False(inspection.HasUdf, inspection.FormatReport());
        Assert.Equal(
            query(Rows.AsQueryable()).Select(r => r!.ToString()),
            native.ToList().Select(r => r!.ToString()));
    }

    [Fact]
    public void Enum_Result() =>
        AssertClientProjectionMatchesLinq(q => q.Select(r => new { r.Id, C = ToColor(r.Id) }));

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
    public void Cast_To_Enum_Is_Not_A_Cast_To_String()
    {
        // An enum has no column type yet, so the cast is evaluated on the client rather than translated as a
        // cast of the column to strings (which only happened to read back as the enum's numeric name).
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<UrRow>().Select(r => new { r.Id, C = (UrColor)r.Id });

        Assert.DoesNotContain("CAST", ((IPolarsExplainable)query).InspectPlan(true).LogicalPlan!.Value);
        AssertClientProjectionMatchesLinq(q => q.Select(r => new { r.Id, C = (UrColor)r.Id }));
    }

    [Fact]
    public void Enum_Result_In_A_Predicate_Is_A_Client_Predicate()
    {
        // The default policy rejects client predicates with a pushdown error naming the predicate, instead of a
        // cast failure inside a native UDF callback. Evaluating it is covered in Polars.Config.Tests.
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<UrRow>().Where(r => ToColor(r.Id) == UrColor.Green).Select(r => r.Id);

        Assert.True(((IPolarsExplainable)query).InspectPlan(true).HasClientPredicates);
        var ex = Assert.Throws<PolarsPushdownException>(() => query.ToList());
        Assert.Contains("ToColor", ex.Message);
    }
}
