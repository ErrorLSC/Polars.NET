namespace Polars.CSharp.Tests;

/// <summary>
/// Avro stream and PipeWriter writers.
/// Avro output is not byte-deterministic (header metadata map order and sync marker vary per writer),
/// so these tests compare by reading the output back instead of comparing raw bytes.
/// </summary>
public partial class StreamWriteTests
{
    [Theory]
    [InlineData(AvroCompression.Uncompressed)]
    [InlineData(AvroCompression.Deflate)]
    [InlineData(AvroCompression.Snappy)]
    public void WriteAvro_ToStream_RoundTrip(AvroCompression compression)
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteAvro(ms, compression, "sample");

        using var back = DataFrame.ReadAvro(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task WriteAvroAsync_ToStream_RoundTrip()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.WriteAvroAsync(ms, AvroCompression.Snappy, "sample");

        Assert.Equal(1, ms.FlushCalls);
        using var back = DataFrame.ReadAvro(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task WriteAvroAsync_ToPipeWriter_RoundTrip()
    {
        using var df = SampleFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.WriteAvroAsync(w, AvroCompression.Deflate, "sample"));

        using var back = DataFrame.ReadAvro(bytes);
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void WriteAvro_LargeOutput_IsWrittenInMultipleChunks()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream();

        df.WriteAvro(ms, AvroCompression.Snappy);

        Assert.True(ms.WriteCalls > 1, $"expected multiple chunks, got {ms.WriteCalls}");
        using var back = DataFrame.ReadAvro(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task WriteAvroAsync_ToPipeWriter_LargeOutput_Backpressure()
    {
        using var df = LargeFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.WriteAvroAsync(w), pauseThreshold: 16 * 1024);

        using var back = DataFrame.ReadAvro(bytes);
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void WriteAvro_DestinationFailure_PropagatesOriginalException()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream(failAfterBytes: 100_000);

        var ex = Assert.Throws<IOException>(() => df.WriteAvro(ms));
        Assert.Equal("boom: destination failed", ex.Message);
    }

    [Fact]
    public void WriteAvro_NonWritableStream_Throws()
    {
        using var df = SampleFrame();
        using var ro = new MemoryStream([], writable: false);

        Assert.Throws<ArgumentException>(() => df.WriteAvro(ro));
    }
}
