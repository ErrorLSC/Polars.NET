namespace Polars.CSharp.Tests;

/// <summary>
/// Rows of objects reach the engine through Sazanami's row builder: the layout of each .NET type (128-bit integers as the
/// engine's own, byte arrays as binary, decimals as decimal(38, 18), nested values) and the values back.
/// </summary>
public class RowIngestionLayoutTests
{
    public sealed record Wide(Int128 Big, UInt128? UBig, byte[]? Blob, decimal Price, Holder? Nested);

    public sealed record Holder(Int128 Inner, List<UInt128> Many);

    private static readonly Int128 Huge = (Int128)1 << 120;

    private static Wide[] Rows() =>
    [
        new(Huge + 1, UInt128.MaxValue, [1, 2, 3], 1.5m, new Holder(-Huge, [UInt128.MaxValue, 0])),
        new(-Huge, null, null, -0.000000000000000001m, null),
    ];

    [Fact]
    public void From_LaysOutEachType()
    {
        using var df = DataFrame.From(Rows());
        Assert.Equal("i128", df.Schema["Big"].ToString());
        Assert.Equal("u128", df.Schema["UBig"].ToString());
        Assert.Equal("binary", df.Schema["Blob"].ToString());
        Assert.Equal("decimal[38,18]", df.Schema["Price"].ToString());
        Assert.Equal("struct[Inner: i128, Many: list[u128]]", df.Schema["Nested"].ToString());
    }

    [Fact]
    public void From_ThenRows_KeepsTheValues()
    {
        Wide[] rows = Rows();
        using var df = DataFrame.From(rows);
        Assert.Equal(rows[0].Big, df["Big"].GetValue<Int128>(0));
        Wide[] read = [.. df.Rows<Wide>()];
        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].Big, read[i].Big);
            Assert.Equal(rows[i].UBig, read[i].UBig); // UInt128.MaxValue: its bits through the engine and back
            Assert.Equal(rows[i].Blob, read[i].Blob);
            Assert.Equal(rows[i].Price, read[i].Price);
            Assert.Equal(rows[i].Nested?.Inner, read[i].Nested?.Inner);
            Assert.Equal(rows[i].Nested?.Many, read[i].Nested?.Many);
        }
    }

    public sealed record Plain(byte[]? Blob, decimal Price);

    [Fact]
    public void WideIntegers_ThroughStreams()
    {
        Wide[] rows = Rows();
        using var eager = DataFrame.FromEnumerable(rows, batchSize: 1);
        using var lazy = LazyFrame.ScanEnumerable(rows, batchSize: 1).Collect();
        foreach (DataFrame df in new[] { eager, lazy })
        {
            Assert.Equal("i128", df.Schema["Big"].ToString());
            Assert.Equal("struct[Inner: i128, Many: list[u128]]", df.Schema["Nested"].ToString());
            Assert.Equal(rows[0].Big, df["Big"].GetValue<Int128>(0));
            Assert.Equal(rows[1].Big, df["Big"].GetValue<Int128>(1));
        }
    }

    [Fact]
    public void Objects_AreReadAsTheirClass_OrTheirText()
    {
        using var records = Series.From("s", new object?[] { new Plain([1], 1m), null, new Plain(null, 2m) });
        Assert.StartsWith("struct", records.DataType.ToString());
        using var mixed = Series.From("s", new object?[] { new Plain([1], 1m), "text", 3 });
        Assert.Equal("str", mixed.DataType.ToString());
    }

    [Fact]
    public void Decimals_ThatDoNotFitExactly_AreRefused() =>
        Assert.Throws<OverflowException>(() => DataFrame.From([new Wide(0, null, null, 1m / 3, null)]));

    [Fact]
    public void MapRows_ToValues_IsOneValueColumn()
    {
        using var df = DataFrame.From(Rows()).Select("Blob", "Price");
        using var mapped = df.MapRows<Plain, string>(r => r.Price.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(["value"], mapped.Columns);
        Assert.Equal(2, mapped.Height);
    }
}
