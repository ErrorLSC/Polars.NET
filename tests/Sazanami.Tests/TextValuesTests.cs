using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary><see cref="TextValues"/>: kinds inferred and values parsed as Polars' CSV inference does (probed with Polars 2.1).</summary>
public class TextValuesTests
{
    [Theory]
    [InlineData("true", "Boolean")]
    [InlineData("FALSE", "Boolean")]
    [InlineData("True", "Boolean")]
    [InlineData("1", "Int64")]
    [InlineData("-5", "Int64")]
    [InlineData("007", "Int64")]
    [InlineData("+5", "String")]
    [InlineData("99999999999999999999", "Int128")]
    [InlineData("1.5", "Float64")]
    [InlineData(".5", "Float64")]
    [InlineData("1.", "Float64")]
    [InlineData("-1e3", "Float64")]
    [InlineData("+7e+05", "Float64")]
    [InlineData("inf", "Float64")]
    [InlineData("-inf", "Float64")]
    [InlineData("NaN", "Float64")]
    [InlineData("nan", "String")]
    [InlineData("1e", "String")]
    [InlineData(".", "String")]
    [InlineData("x", "String")]
    [InlineData("2024-01-15", "Date")]
    [InlineData("2024/01/15", "Date")]
    [InlineData("2024.01.15", "Date")]
    [InlineData("15-01-2024", "Date")]
    [InlineData("2024-1-5", "Date")]
    [InlineData("2024-13-45", "String")]
    [InlineData("2024-02-30", "String")]
    [InlineData("2024-01/15", "String")]
    [InlineData("2024-01-15 10:00:00", "Datetime")]
    [InlineData("2024-01-15T10:00:00.5", "Datetime")]
    [InlineData("2024-01-15 10:00", "Datetime")]
    [InlineData("2024-01-15 1000", "Datetime")]
    [InlineData("20240115T100000", "Datetime")]
    [InlineData("15-01-2024 10:00", "Datetime")]
    [InlineData("2024-01-15T10:00:00Z", "DatetimeUtc")]
    [InlineData("2024-01-15T10:00:00+02:00", "DatetimeUtc")]
    [InlineData("2024-01-15T10:00:00-0530", "DatetimeUtc")]
    [InlineData("10:00:00", "String")]
    [InlineData("2024-01-15 25:00", "String")]
    public void Kinds_AreInferredAsPolarsInfersThem(string text, string kind) => Assert.Equal(Enum.Parse<TextKind>(kind), TextValues.Infer(text, parseDates: true));

    [Theory]
    [InlineData("2024-01-15")]
    [InlineData("2024-01-15 10:00:00")]
    [InlineData("2024-01-15T10:00:00Z")]
    public void WithoutDateParsing_TemporalTextIsAString(string text) => Assert.Equal(TextKind.String, TextValues.Infer(text, parseDates: false));

    [Theory]
    [InlineData("Int64", "Int64")]
    [InlineData("Int64,Float64", "Float64")]
    [InlineData("Int64,Int128", "Int128")]
    [InlineData("Int128,Float64", "Float64")]
    [InlineData("Int64,Boolean", "String")]
    [InlineData("Date,Datetime", "String")]
    [InlineData("Int64,Float64,Boolean", "String")]
    public void Conflicts_FinishAsPolarsFinishesThem(string kinds, string kind) =>
        Assert.Equal(Enum.Parse<TextKind>(kind), TextValues.Finish(kinds.Split(',').Select(Enum.Parse<TextKind>).ToHashSet()));

    [Fact]
    public void Values_Parse()
    {
        Assert.True(TextValues.TryParseFloat64("-inf", out double d) && double.IsNegativeInfinity(d));
        Assert.True(TextValues.TryParseFloat64("NaN", out d) && double.IsNaN(d));
        Assert.True(TextValues.TryParseFloat64("3", out d) && d == 3);
        Assert.True(TextValues.TryParseInt64("007", out long l) && l == 7);
        Assert.False(TextValues.TryParseInt64("+7", out _));
        Assert.True(TextValues.TryParseInt128("99999999999999999999", out Int128 big) && big == Int128.Parse("99999999999999999999"));
        Assert.True(TextValues.TryParseDate("15.01.2024", out DateOnly date) && date == new DateOnly(2024, 1, 15));
        Assert.True(TextValues.TryParseDatetime("2024-01-15T10:00:00.25+02:00", out DateTime t, out bool zoned) && zoned);
        Assert.Equal(new DateTime(2024, 1, 15, 8, 0, 0, DateTimeKind.Utc).AddMilliseconds(250), t);
        Assert.True(TextValues.TryParseDatetime("20240115T100000Z", out t, out zoned) && zoned && t == new DateTime(2024, 1, 15, 10, 0, 0, DateTimeKind.Utc));
        Assert.True(TextValues.TryParseDatetime("2024-01-15 10:00-05:30", out t, out zoned) && zoned && t == new DateTime(2024, 1, 15, 15, 30, 0, DateTimeKind.Utc));
        Assert.True(TextValues.TryParseDatetime("2024-01-15", out t, out zoned) && !zoned && t == new DateTime(2024, 1, 15));
        Assert.True(TextValues.TryParseBoolean("TRUE", out bool b) && b);
        Assert.False(TextValues.TryParseBoolean("yes", out _));
    }

    [Fact]
    public void Repeat_MakesAColumnOfOneValue_ForEveryFlatType()
    {
        var ints = new Int64Array.Builder().Append(5).AppendNull().Append(7).Build();
        Assert.Equal([7L, 7, 7], ((Int64Array)ConstantArrays.Repeat(ints, 2, 3)).Values.ToArray());
        IArrowArray nulls = ConstantArrays.Repeat(ints, 1, 4);
        Assert.Equal(4, nulls.NullCount);
        Assert.Equal(ArrowTypeId.Int64, nulls.Data.DataType.TypeId);
        var bools = new BooleanArray.Builder().Append(false).Append(true).Build();
        Assert.All(Enumerable.Range(0, 9), i => Assert.True(((BooleanArray)ConstantArrays.Repeat(bools, 1, 9)).GetValue(i)));
        Assert.All(Enumerable.Range(0, 9), i => Assert.False(((BooleanArray)ConstantArrays.Repeat(bools, 0, 9)).GetValue(i)));
        var stamps = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC")).Append(DateTimeOffset.UnixEpoch.AddDays(1)).Build();
        var repeated = (TimestampArray)ConstantArrays.Repeat(stamps, 0, 2);
        Assert.Equal("UTC", ((TimestampType)repeated.Data.DataType).Timezone);
        Assert.Equal(DateTimeOffset.UnixEpoch.AddDays(1), repeated.GetTimestamp(1));
        var strings = new StringArray.Builder().Append("short").Append("a value longer than twelve").AppendNull().Build();
        Assert.Equal(["a value longer than twelve", "a value longer than twelve"], Strings(ConstantArrays.Repeat(strings, 1, 2)));
        var views = new StringViewArray.Builder().Append("short").Append("a value longer than twelve").Build();
        Assert.Equal(["short", "short", "short"], Strings(ConstantArrays.Repeat(views, 0, 3)));
        Assert.Equal(["a value longer than twelve"], Strings(ConstantArrays.Repeat(views, 1, 1)));
        Assert.Equal(2, ConstantArrays.Repeat(strings, 2, 2).NullCount);
        // A sliced source: its offset counts.
        Assert.Equal([7L, 7], ((Int64Array)ConstantArrays.Repeat(ints.Slice(1, 2), 1, 2)).Values.ToArray());
        Assert.Throws<NotSupportedException>(() => ConstantArrays.Repeat(new ListArray.Builder(Int32Type.Default).Build(), 0, 1));

        static string[] Strings(IArrowArray array) => array switch
        {
            StringArray s => [.. Enumerable.Range(0, s.Length).Select(i => s.GetString(i))],
            StringViewArray v => [.. Enumerable.Range(0, v.Length).Select(i => v.GetString(i))],
            _ => throw new InvalidOperationException(),
        };
    }
}
