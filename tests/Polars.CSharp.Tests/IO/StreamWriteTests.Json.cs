using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>JSON / NDJSON stream and PipeWriter writers (DataFrame and LazyFrame sink).</summary>
public partial class StreamWriteTests
{
    // ---------------- DataFrame JSON ----------------

    [Theory]
    [InlineData(JsonFormat.Json)]
    [InlineData(JsonFormat.JsonLines)]
    public void WriteJson_ToStream_MatchesMemory(JsonFormat format)
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteJson(ms, format);

        Assert.Equal(df.WriteJsonMemory(format), ms.ToArray());
    }

    [Theory]
    [InlineData(JsonFormat.Json)]
    [InlineData(JsonFormat.JsonLines)]
    public async Task WriteJsonAsync_ToStream_MatchesMemory(JsonFormat format)
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.WriteJsonAsync(ms, format);

        Assert.Equal(df.WriteJsonMemory(format), ms.ToArray());
        Assert.Equal(1, ms.FlushCalls);
    }

    [Theory]
    [InlineData(JsonFormat.Json)]
    [InlineData(JsonFormat.JsonLines)]
    public async Task WriteJsonAsync_ToPipeWriter_MatchesMemory(JsonFormat format)
    {
        using var df = SampleFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.WriteJsonAsync(w, format));

        Assert.Equal(df.WriteJsonMemory(format), bytes);
    }

    [Fact]
    public void SinkJson_LargeOutput_IsWrittenInMultipleChunks()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream();

        df.Lazy().SinkJson(ms);

        Assert.True(ms.WriteCalls > 1, $"expected multiple chunks, got {ms.WriteCalls}");
        Assert.Equal(df.Lazy().SinkJsonMemory(), ms.ToArray());
    }

    [Fact]
    public async Task WriteJsonAsync_ToPipeWriter_LargeOutput_Backpressure()
    {
        using var df = LargeFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.WriteJsonAsync(w, JsonFormat.JsonLines), pauseThreshold: 16 * 1024);

        Assert.Equal(df.WriteJsonMemory(JsonFormat.JsonLines), bytes);
    }

    [Fact]
    public void WriteJson_DestinationFailure_PropagatesOriginalException()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream(failAfterBytes: 100_000);

        var ex = Assert.Throws<IOException>(() => df.WriteJson(ms, JsonFormat.JsonLines));
        Assert.Equal("boom: destination failed", ex.Message);
    }

    [Fact]
    public void WriteJson_NonWritableStream_Throws()
    {
        using var df = SampleFrame();
        using var ro = new MemoryStream([], writable: false);

        Assert.Throws<ArgumentException>(() => df.WriteJson(ro));
    }

    // ---------------- LazyFrame JSON sink ----------------

    [Fact]
    public void SinkJson_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var lf = df.Lazy().Filter(Col("id") > 2);
        using var ms = new MemoryStream();

        lf.SinkJson(ms);

        Assert.Equal(lf.SinkJsonMemory(), ms.ToArray());
    }

    [Fact]
    public async Task SinkJsonAsync_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var lf = df.Lazy().Filter(Col("id") > 2);
        using var ms = new RecordingStream(allowSyncWrites: false);

        await lf.SinkJsonAsync(ms);

        Assert.Equal(lf.SinkJsonMemory(), ms.ToArray());
        Assert.Equal(1, ms.FlushCalls);
    }

    [Fact]
    public async Task SinkJsonAsync_ToPipeWriter_MatchesMemory()
    {
        using var df = SampleFrame();
        using var lf = df.Lazy().Filter(Col("id") > 2);

        byte[] bytes = await CollectFromPipeAsync(w => lf.SinkJsonAsync(w));

        Assert.Equal(lf.SinkJsonMemory(), bytes);
    }

    [Fact]
    public async Task SinkJsonAsync_ToPipeWriter_LargeOutput_Backpressure()
    {
        using var df = LargeFrame();
        using var lf = df.Lazy();

        byte[] bytes = await CollectFromPipeAsync(w => lf.SinkJsonAsync(w), pauseThreshold: 16 * 1024);

        Assert.Equal(lf.SinkJsonMemory(), bytes);
    }

    [Fact]
    public void SinkJson_DestinationFailure_PropagatesOriginalException()
    {
        using var df = LargeFrame();
        using var lf = df.Lazy();
        using var ms = new RecordingStream(failAfterBytes: 100_000);

        var ex = Assert.Throws<IOException>(() => lf.SinkJson(ms));
        Assert.Equal("boom: destination failed", ex.Message);
    }

    [Fact]
    public async Task SinkJsonAsync_QueryFailure_PropagatesPolarsError()
    {
        using var df = SampleFrame();
        using var lf = df.Lazy().Select(Col("does_not_exist"));
        using var ms = new MemoryStream();

        await Assert.ThrowsAnyAsync<Exception>(() => lf.SinkJsonAsync(ms));
    }
}
