using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public partial class NativeMemoryTests
{
    // ---------------- JSON ----------------

    [Theory]
    [InlineData(JsonFormat.Json)]
    [InlineData(JsonFormat.JsonLines)]
    public void WriteJsonNativeMemory_MatchesByteArray(JsonFormat format)
    {
        using var df = SampleFrame();

        using var native = df.WriteJsonNativeMemory(format);
        byte[] managed = df.WriteJsonMemory(format);

        Assert.True(native.Length > 0);
        Assert.Equal(managed, native.ToArray());
    }

    [Fact]
    public void SinkJsonNativeMemory_MatchesByteArray()
    {
        using var df = SampleFrame();

        using var native = df.Lazy().Filter(Col("id") > 2).SinkJsonNativeMemory();
        byte[] managed = df.Lazy().Filter(Col("id") > 2).SinkJsonMemory();

        Assert.True(native.Length > 0);
        Assert.Equal(managed, native.ToArray());
    }

    [Theory]
    [InlineData(JsonFormat.Json)]
    [InlineData(JsonFormat.JsonLines)]
    public void WriteJsonNativeMemory_RoundTrip(JsonFormat format)
    {
        using var df = SampleFrame();

        using var native = df.WriteJsonNativeMemory(format);
        using var back = DataFrame.ReadJson(native.AsStream(), jsonFormat: format);

        Assert.Equal(5, back.Height);
        Assert.Equal(1L, back.GetValue<long>(0, "id"));
        Assert.Equal("Alice", back.GetValue<string>(0, "name"));
        Assert.Null(back.GetValue<string>(2, "name"));
        Assert.Equal(5.5, back.GetValue<double>(4, "score"));
    }

    [Fact]
    public void SinkJsonNativeMemory_RoundTrip()
    {
        using var df = SampleFrame();

        using var native = df.Lazy().Filter(Col("id") > 2).SinkJsonNativeMemory();
        using var back = DataFrame.ReadJson(native.ToArray(), jsonFormat: JsonFormat.JsonLines);

        Assert.Equal(3, back.Height);
        Assert.Equal(3L, back.GetValue<long>(0, "id"));
        Assert.Equal("David", back.GetValue<string>(1, "name"));
        Assert.Equal(5.5, back.GetValue<double>(2, "score"));
    }
}
