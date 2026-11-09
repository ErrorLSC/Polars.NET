using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text.Json;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public record AsOfTrade(long Time, string Sym);
public record AsOfQuote(long Time, string Sym, int? Price);
public record AsOfRow(long Time, int? Price);
public record StampRow(int Id, DateTime Ts);
public record JsonRow(int Seq, string Json);
public record OrderRow(int Seq, string Key, int Amount);
public record TaggedRow(int Id, List<string> Tags, int[] Scores);

/// <summary>
/// Pins LINQ behaviors whose IR representation depends on type / join / strategy vocabulary:
/// AsOf join strategies, by-keys and tolerance, and casts to DateTimeOffset / decimal.
/// </summary>
public class LinqIrVocabularyTests
{
    // Outer:  1(A)  7(A)  10(B)
    // Inner:  0(A)=10  4(A)=40  6(B)=60  9(B)=90   (sorted by Time)
    private static DataFrame Trades(params long[] times) =>
        DataFrame.FromRows(times.Select((t, i) => new AsOfTrade(t, i < 2 ? "A" : "B")).ToArray());

    private static DataFrame Quotes() =>
        DataFrame.FromRows([
            new AsOfQuote(0, "A", 10),
            new AsOfQuote(4, "A", 40),
            new AsOfQuote(6, "B", 60),
            new AsOfQuote(9, "B", 90),
        ]);

    private static int?[] RunAsOf(DataFrame trades, DataFrame quotes, AsofStrategy strategy, AsOfTolerance? tolerance = null)
    {
        var query = trades.AsQueryable<AsOfTrade>()
            .JoinAsOf(
                quotes.AsQueryable<AsOfQuote>(),
                t => t.Time,
                q => q.Time,
                (t, q) => new AsOfRow(t.Time, q.Price),
                tolerance,
                strategy)
            .OrderBy(r => r.Time)
            .Select(r => r.Price);

        Assert.Contains("100% Native Pushdown", query.Explain());
        return query.ToArray();
    }

    [Theory]
    [InlineData(AsofStrategy.Backward, new[] { 10, 60, 90 })]
    [InlineData(AsofStrategy.Nearest, new[] { 10, 60, 90 })]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Strategy_Matches_Expected(AsofStrategy strategy, int[] expected)
    {
        using var trades = Trades(1, 7, 10);
        using var quotes = Quotes();

        Assert.Equal(expected.Select(v => (int?)v), RunAsOf(trades, quotes, strategy));
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Forward_Leaves_Unmatched_As_Null()
    {
        using var trades = Trades(1, 7, 10);
        using var quotes = Quotes();

        Assert.Equal([40, 90, null], RunAsOf(trades, quotes, AsofStrategy.Forward));
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Backward_With_Integer_Tolerance()
    {
        using var trades = Trades(2, 7, 10);
        using var quotes = Quotes();

        // 2 -> 0 is 2 apart (> tolerance 1) -> null; 7 -> 6 and 10 -> 9 are within tolerance.
        Assert.Equal([null, 60, 90], RunAsOf(trades, quotes, AsofStrategy.Backward, 1L));
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Backward_By_Group_Key()
    {
        using var trades = Trades(1, 7, 10);
        using var quotes = Quotes();

        var prices = trades.AsQueryable<AsOfTrade>()
            .JoinAsOf(
                quotes.AsQueryable<AsOfQuote>(),
                t => t.Time,
                q => q.Time,
                t => t.Sym,
                q => q.Sym,
                (t, q) => new AsOfRow(t.Time, q.Price))
            .OrderBy(r => r.Time)
            .Select(r => r.Price)
            .ToArray();

        // 7(A) must not see 6(B): latest A quote at or before 7 is 4 -> 40.
        Assert.Equal([10, 40, 90], prices);
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_DateTime_To_DateTimeOffset_Is_Utc()
    {
        var ts = new DateTime(2025, 3, 4, 5, 6, 7);
        using var df = DataFrame.FromRows([new StampRow(1, ts)]);

        var result = df.AsQueryable<StampRow>()
            .Select(s => new { s.Id, Dto = (DateTimeOffset)s.Ts })
            .ToList();

        Assert.Single(result);
        Assert.Equal(TimeSpan.Zero, result[0].Dto.Offset);
        Assert.Equal(new DateTimeOffset(ts, TimeSpan.Zero), result[0].Dto);
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_Int_To_Decimal()
    {
        using var df = DataFrame.FromRows([new StampRow(7, DateTime.UnixEpoch)]);

        var result = df.AsQueryable<StampRow>()
            .Select(s => new { Value = (decimal)s.Id })
            .ToList();

        Assert.Equal(7m, result[0].Value);
    }

    private static DataFrame TaggedRows() =>
        DataFrame.FromRows([
            new TaggedRow(1, ["a", "b"], [10, 20, 30]),
            new TaggedRow(2, [], [5]),
            new TaggedRow(3, ["c"], []),
        ]);

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_Collection_Upcast_To_Interface_Preserves_Elements()
    {
        using var df = TaggedRows();

        var query = df.AsQueryable<TaggedRow>()
            .Select(r => new
            {
                r.Id,
                Tags = (IEnumerable<string>)r.Tags,
                Scores = (IReadOnlyList<int>)r.Scores
            })
            .OrderBy(r => r.Id);

        // The upcasts must reach the IR as casts (not be dropped or evaluated client-side).
        Assert.Contains("CAST(", query.ExplainLogicalPlan());
        Assert.Contains("100% Native Pushdown", query.Explain());

        var result = query.ToList();

        Assert.Equal(3, result.Count);
        Assert.Equal(["a", "b"], result[0].Tags);
        Assert.Empty(result[1].Tags);
        Assert.Equal(["c"], result[2].Tags);
        Assert.Equal([10, 20, 30], result[0].Scores);
        Assert.Equal([5], result[1].Scores);
        Assert.Empty(result[2].Scores);
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_Collection_Upcast_Then_Count()
    {
        using var df = TaggedRows();

        var counts = df.AsQueryable<TaggedRow>()
            .OrderBy(r => r.Id)
            .Select(r => ((IEnumerable<string>)r.Tags).Count())
            .ToList();

        Assert.Equal([2, 0, 1], counts);
    }

    // Keys first appear in the order c, a, d, b: neither sorted nor insertion-hash friendly.
    private static DataFrame OrderRows() =>
        DataFrame.FromRows([
            new OrderRow(1, "c", 10), new OrderRow(2, "a", 20), new OrderRow(3, "c", 30),
            new OrderRow(4, "d", 40), new OrderRow(5, "a", 50), new OrderRow(6, "b", 60),
            new OrderRow(7, "d", 70), new OrderRow(8, "b", 80), new OrderRow(9, "c", 90),
        ]);

    private static readonly string[] FirstAppearance = ["c", "a", "d", "b"];

    // LINQ GroupBy / CountBy yield groups in order of first key appearance. Native group-by order is
    // otherwise nondeterministic, so repeat the query to make an unordered implementation fail reliably.
    private static void AssertStableFirstAppearance(Func<IQueryable<OrderRow>, IEnumerable<string>> keysOf)
    {
        using var df = OrderRows();
        for (int run = 0; run < 30; run++)
            Assert.Equal(FirstAppearance, keysOf(df.AsQueryable<OrderRow>()).ToArray());
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void GroupBy_Yields_Groups_In_First_Appearance_Order() =>
        AssertStableFirstAppearance(q => q.GroupBy(r => r.Key).Select(g => new { g.Key, Total = g.Sum(r => r.Amount) }).Select(x => x.Key));

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void GroupBy_ResultSelector_Yields_Groups_In_First_Appearance_Order() =>
        AssertStableFirstAppearance(q => q.GroupBy(r => r.Key, (k, rows) => new { Key = k, Count = rows.Count() }).Select(x => x.Key));

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void CountBy_Yields_Keys_In_First_Appearance_Order() =>
        AssertStableFirstAppearance(q => q.CountBy(r => r.Key).Select(kv => kv.Key));

    // A DTO emitted into a dynamic assembly, like types compiled by LINQPad or scripting hosts:
    // Type.GetType(AssemblyQualifiedName) cannot resolve it, so the IR must not round-trip it by name.
    private static Type EmitDynamicDto()
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("PolarsDynamicDtos_" + Guid.NewGuid().ToString("N")), AssemblyBuilderAccess.Run);
        var type = assembly.DefineDynamicModule("Main").DefineType("DynamicJsonDto", TypeAttributes.Public | TypeAttributes.Class);
        type.DefineDefaultConstructor(MethodAttributes.Public);
        AddAutoProperty(type, "Id", typeof(int));
        AddAutoProperty(type, "Name", typeof(string));
        return type.CreateType();
    }

    private static void AddAutoProperty(TypeBuilder type, string name, Type propertyType)
    {
        const MethodAttributes accessor = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig;
        var field = type.DefineField("_" + name, propertyType, FieldAttributes.Private);

        var getter = type.DefineMethod("get_" + name, accessor, propertyType, Type.EmptyTypes);
        var gil = getter.GetILGenerator();
        gil.Emit(OpCodes.Ldarg_0);
        gil.Emit(OpCodes.Ldfld, field);
        gil.Emit(OpCodes.Ret);

        var setter = type.DefineMethod("set_" + name, accessor, null, [propertyType]);
        var sil = setter.GetILGenerator();
        sil.Emit(OpCodes.Ldarg_0);
        sil.Emit(OpCodes.Ldarg_1);
        sil.Emit(OpCodes.Stfld, field);
        sil.Emit(OpCodes.Ret);

        var property = type.DefineProperty(name, PropertyAttributes.None, propertyType, null);
        property.SetGetMethod(getter);
        property.SetSetMethod(setter);
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JsonDeserialize_Of_Dynamically_Emitted_Dto_Pushes_Down()
    {
        var dtoType = EmitDynamicDto();
        Assert.Null(Type.GetType(dtoType.AssemblyQualifiedName!)); // precondition: not resolvable by name

        using var df = DataFrame.FromRows([
            new JsonRow(1, """{"Id":1,"Name":"alpha"}"""),
            new JsonRow(2, """{"Id":2,"Name":"beta"}"""),
        ]);

        // r => JsonSerializer.Deserialize<DynamicJsonDto>(r.Json, (JsonSerializerOptions)null).Name
        var deserialize = typeof(JsonSerializer).GetMethods()
            .Single(m => m.Name == nameof(JsonSerializer.Deserialize)
                && m.IsGenericMethodDefinition
                && m.GetParameters() is [{ ParameterType: var p0 }, { ParameterType: var p1 }]
                && p0 == typeof(string) && p1 == typeof(JsonSerializerOptions))
            .MakeGenericMethod(dtoType);
        var row = Expression.Parameter(typeof(JsonRow), "r");
        var name = Expression.Property(
            Expression.Call(deserialize, Expression.Property(row, nameof(JsonRow.Json)), Expression.Constant(null, typeof(JsonSerializerOptions))),
            "Name");
        var selector = Expression.Lambda<Func<JsonRow, string>>(name, row);

        var query = df.AsQueryable<JsonRow>().OrderBy(r => r.Seq).Select(selector);

        Assert.Contains("100% Native Pushdown", query.Explain());
        Assert.Equal(["alpha", "beta"], query.ToList());
    }

    // LINQ Distinct / DistinctBy / Union keep the first occurrence of each element, in source order.
    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Distinct_Yields_Elements_In_First_Appearance_Order() =>
        AssertStableFirstAppearance(q => q.Select(r => r.Key).Distinct());

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void DistinctBy_Keeps_First_Row_Per_Key_In_Source_Order()
    {
        using var df = OrderRows();
        for (int run = 0; run < 30; run++)
        {
            var firstRows = df.AsQueryable<OrderRow>().DistinctBy(r => r.Key).Select(r => r.Seq).ToArray();
            Assert.Equal([1, 2, 4, 6], firstRows);
        }
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Union_Yields_Elements_In_First_Appearance_Order() =>
        AssertStableFirstAppearance(q =>
            q.Where(r => r.Seq <= 3).Select(r => r.Key)
                .Union(q.Where(r => r.Seq > 3).Select(r => r.Key)));

    private static void AssertDecodedByMemberName<TEnum>(Func<object, object> decode) where TEnum : struct, Enum
    {
        foreach (var member in Enum.GetValues<TEnum>())
            Assert.Equal(member.ToString(), decode(member).ToString());
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Method_Enum_Arguments_Decode_To_The_Member_Of_The_Same_Name()
    {
        AssertDecodedByMemberName<RankMethod>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.rankMethodOf(v));
        AssertDecodedByMemberName<QuantileMethod>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.quantileMethodOf(v));
        AssertDecodedByMemberName<NullBehavior>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.nullBehaviorOf(v));
        AssertDecodedByMemberName<InterpolationMethod>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.interpolationMethodOf(v));

        AssertDecodedByMemberName<global::Polars.NET.Core.Enums.PlRankMethod>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.rankMethodOf(v));
        AssertDecodedByMemberName<global::Polars.NET.Core.Enums.PlQuantileMethod>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.quantileMethodOf(v));
        AssertDecodedByMemberName<global::Polars.NET.Core.Enums.PlNullBehavior>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.nullBehaviorOf(v));
        AssertDecodedByMemberName<global::Polars.NET.Core.Enums.PlInterpolationMethod>(v => global::Kaminari.Samidare.WindowAndMethodTranslator.interpolationMethodOf(v));
    }

    private record StampQuote(DateTime Ts, int? Price);

    private static int? AsOfPriceWithin(AsOfTolerance tolerance)
    {
        var at = new DateTime(2024, 1, 1, 12, 0, 0);
        using var left = DataFrame.FromRows([new StampQuote(at, 0)]);
        using var right = DataFrame.FromRows([new StampQuote(at.AddHours(-1), 7)]);

        return left.AsQueryable<StampQuote>()
            .JoinAsOf(right.AsQueryable<StampQuote>(), l => l.Ts, r => r.Ts, (l, r) => r.Price, tolerance, AsofStrategy.Backward)
            .ToArray()[0];
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Tolerance_As_TimeSpan_And_As_Engine_Text()
    {
        Assert.Equal(7, AsOfPriceWithin(TimeSpan.FromHours(2)));
        Assert.Null(AsOfPriceWithin(TimeSpan.FromMinutes(30)));
        Assert.Equal(7, AsOfPriceWithin("2h"));
        Assert.Null(AsOfPriceWithin("30m"));
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void DateTime_Add_Methods_Follow_DotNet_For_Negative_Fractional_And_Large_Offsets()
    {
        var ts = new DateTime(2024, 3, 1, 6, 0, 0);
        using var df = DataFrame.FromRows([new StampRow(2024, ts)]);

        var row = df.AsQueryable<StampRow>()
            .Select(r => new
            {
                Negative = r.Ts.AddDays(-30),
                Fractional = r.Ts.AddHours(1.5),
                Large = r.Ts.AddSeconds(1000000),
                Leap = DateTime.IsLeapYear(r.Id)
            })
            .ToArray()[0];

        Assert.Equal(ts.AddDays(-30), row.Negative);
        Assert.Equal(ts.AddHours(1.5), row.Fractional);
        Assert.Equal(ts.AddSeconds(1000000), row.Large);
        Assert.True(row.Leap);
    }
}
