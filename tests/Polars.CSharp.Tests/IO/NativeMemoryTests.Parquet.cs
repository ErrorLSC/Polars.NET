using Polars.NET.Core;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public partial class NativeMemoryTests
{
    // ---------------- Parquet ----------------

    [Fact]
    public void WriteParquetNativeMemory_RoundTrip_MatchesByteArray()
    {
        using var df = SampleFrame();

        using var native = df.WriteParquetNativeMemory();
        byte[] managed = df.WriteParquetMemory();

        Assert.True(native.Length > 0);
        Assert.Equal(managed, native.ToArray());

        using var back = DataFrame.ReadParquet(native.AsStream());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void SinkParquetNativeMemory_RoundTrip_MatchesByteArray()
    {
        using var df = SampleFrame();

        using var native = df.Lazy().Filter(Col("id") > 2).SinkParquetNativeMemory();
        byte[] managed = df.Lazy().Filter(Col("id") > 2).SinkParquetMemory();

        Assert.True(native.Length > 0);
        Assert.Equal(managed, native.ToArray());

        using var back = DataFrame.ReadParquet(native.AsStream());
        Assert.Equal(3, back.Height);
        Assert.Equal(3, back.GetValue<int>(0, "id"));
    }
}
