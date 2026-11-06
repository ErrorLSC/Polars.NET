using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public partial class NativeMemoryTests
{
    // ---------------- CSV ----------------

    [Fact]
    public void WriteCsvNativeMemory_MatchesByteArray()
    {
        using var df = SampleFrame();

        using var native = df.WriteCsvNativeMemory();
        byte[] managed = df.WriteCsvMemory();

        Assert.True(native.Length > 0);
        Assert.Equal(managed, native.ToArray());
    }

    [Fact]
    public void SinkCsvNativeMemory_MatchesByteArray()
    {
        using var df = SampleFrame();

        using var native = df.Lazy().Filter(Col("id") > 2).SinkCsvNativeMemory();
        byte[] managed = df.Lazy().Filter(Col("id") > 2).SinkCsvMemory();

        Assert.True(native.Length > 0);
        Assert.Equal(managed, native.ToArray());
    }

    [Fact]
    public void WriteCsvNativeMemory_RoundTrip()
    {
        using var df = SampleFrame();

        using var native = df.WriteCsvNativeMemory();
        using var back = DataFrame.ReadCsv(native.AsStream());

        Assert.Equal(5, back.Height);
        for (long i = 0; i < 5; i++)
        {
            Assert.Equal(df.GetValue<int>(i, "id"), (int)back.GetValue<long>(i, "id"));
            Assert.Equal(df.GetValue<double>(i, "score"), back.GetValue<double>(i, "score"));
        }
        Assert.Equal("Alice", back.GetValue<string>(0, "name"));
        // CSV has no null marker by default: the null name is written as an empty field and read back as "".
        Assert.Equal("", back.GetValue<string>(2, "name"));
        Assert.Equal("Eve", back.GetValue<string>(4, "name"));
    }

    [Fact]
    public void SinkCsvNativeMemory_RoundTrip()
    {
        using var df = SampleFrame();

        using var native = df.Lazy().Filter(Col("id") > 2).SinkCsvNativeMemory();
        using var back = DataFrame.ReadCsv(native.ToArray());

        Assert.Equal(3, back.Height);
        Assert.Equal(3L, back.GetValue<long>(0, "id"));
        // CSV has no null marker by default: the null name is written as an empty field and read back as "".
        Assert.Equal("", back.GetValue<string>(0, "name"));
        Assert.Equal("David", back.GetValue<string>(1, "name"));
        Assert.Equal(5.5, back.GetValue<double>(2, "score"));
    }
}
