using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;
using Kaminari.Sazanami.Arrow;

namespace Polars.CSharp.Tests;

/// <summary>
/// The strict CLR-to-Arrow mapping reports types without an Arrow representation (null), including through
/// nested element, key, value and member types; the lenient mapping keeps mapping them to strings.
/// </summary>
public class ArrowTypeResolutionTests
{
    private enum Shade { Light, Dark }
    private sealed class MemberLess;
    private sealed class Point { public int X { get; set; } public int Y { get; set; } }
    private sealed class Tagged { public int Id { get; set; } public Shade Shade { get; set; } }

    public static TheoryData<Type> Unrepresentable() =>
    [
        typeof(object),
        typeof(Shade),
        typeof(Shade?),
        typeof(Int128),
        typeof(MemberLess),
        typeof(List<object>),
        typeof(Dictionary<string, Shade>),
        typeof(Tagged),
    ];

    [Theory]
    [MemberData(nameof(Unrepresentable))]
    public void Strict_Mapping_Reports_Types_Without_An_Arrow_Representation(Type type) =>
        Assert.Null(ArrowTypeResolver.TryGetArrowTypeFromNetType(type));

    [Fact]
    public void Strict_Mapping_Agrees_With_The_Lenient_One_For_Representable_Types()
    {
        Type[] types = [typeof(int), typeof(string), typeof(DateTime?), typeof(List<double>), typeof(Point), typeof(Dictionary<string, int>)];
        foreach (var type in types)
            Assert.Equal(
                ArrowTypeResolver.GetArrowTypeFromNetType(type).ToString(),
                ArrowTypeResolver.TryGetArrowTypeFromNetType(type)!.ToString());
    }

    [Fact]
    public void Lenient_Mapping_Still_Maps_Each_Unrepresentable_Part_To_A_String()
    {
        Assert.IsType<StringViewType>(ArrowTypeResolver.GetArrowTypeFromNetType(typeof(Shade)));

        var list = Assert.IsType<LargeListType>(ArrowTypeResolver.GetArrowTypeFromNetType(typeof(List<object>)));
        Assert.IsType<StringViewType>(list.ValueDataType);

        var tagged = Assert.IsType<StructType>(ArrowTypeResolver.GetArrowTypeFromNetType(typeof(Tagged)));
        Assert.IsType<StringViewType>(tagged.Fields.Single(f => f.Name == "Shade").DataType);
    }
}
