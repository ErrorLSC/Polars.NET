namespace Polars.CSharp.Tests;

/// <summary>Parquet stream and PipeWriter writers. Output must be byte-identical to the in-memory writers.</summary>
public partial class StreamWriteTests
{
    [Fact]
    public void WriteParquet_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteParquet(ms);

        Assert.Equal(df.WriteParquetMemory(), ms.ToArray());
    }

    [Fact]
    public async Task WriteParquetAsync_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.WriteParquetAsync(ms);

        Assert.Equal(df.WriteParquetMemory(), ms.ToArray());
        Assert.Equal(1, ms.FlushCalls);
    }

    [Fact]
    public async Task WriteParquetAsync_ToPipeWriter_MatchesMemory()
    {
        using var df = SampleFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.WriteParquetAsync(w));

        Assert.Equal(df.WriteParquetMemory(), bytes);
    }

    [Fact]
    public void SinkParquet_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.Lazy().SinkParquet(ms);

        Assert.Equal(df.Lazy().SinkParquetMemory(), ms.ToArray());
    }

    [Fact]
    public async Task SinkParquetAsync_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.Lazy().SinkParquetAsync(ms);

        Assert.Equal(df.Lazy().SinkParquetMemory(), ms.ToArray());
    }

    [Fact]
    public async Task SinkParquetAsync_ToPipeWriter_MatchesMemory()
    {
        using var df = SampleFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.Lazy().SinkParquetAsync(w));

        Assert.Equal(df.Lazy().SinkParquetMemory(), bytes);
    }

    [Fact]
    public void WriteParquet_ToStream_RoundTrip()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteParquet(ms, ParquetCompression.Snappy, statistics: true);

        using var back = DataFrame.ReadParquet(ms.ToArray());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public async Task SinkParquetAsync_ToStream_LargeOutput_RoundTrip()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream(allowSyncWrites: false, asyncDelayMs: 1);

        await df.Lazy().SinkParquetAsync(ms);

        Assert.True(ms.WriteCalls > 1);
        using var back = DataFrame.ReadParquet(ms.ToArray());
        Assert.True(df.Equals(back));
    }
}
