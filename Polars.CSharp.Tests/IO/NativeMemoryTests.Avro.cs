using Polars.NET.Core;

namespace Polars.CSharp.Tests;

public partial class NativeMemoryTests
{
    // ---------------- Avro ----------------

    [Fact]
    public void WriteAvroNativeMemory_RoundTrip()
    {
        using var df = SampleFrame();

        // Avro output is not byte-deterministic (header metadata map order and the sync marker
        // vary per writer), so compare via round-trip rather than raw bytes.
        using var native = df.WriteAvroNativeMemory(AvroCompression.Snappy, "sample");
        Assert.True(native.Length > 0);

        using var back = DataFrame.ReadAvro(native.AsStream());
        Assert.True(df.Equals(back));

        using var backManaged = DataFrame.ReadAvro(df.WriteAvroMemory(AvroCompression.Snappy, "sample"));
        Assert.True(back.Equals(backManaged));
    }
}
