namespace Polars.CSharp.Tests;

/// <summary>CSV stream and PipeWriter writers. Output must be byte-identical to the in-memory writers.</summary>
public partial class StreamWriteTests
{
    [Fact]
    public void WriteCsv_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteCsv(ms);

        Assert.Equal(df.WriteCsvMemory(), ms.ToArray());
    }

    [Fact]
    public async Task WriteCsvAsync_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.WriteCsvAsync(ms);

        Assert.Equal(df.WriteCsvMemory(), ms.ToArray());
        Assert.Equal(1, ms.FlushCalls);
    }

    [Fact]
    public async Task WriteCsvAsync_ToPipeWriter_MatchesMemory()
    {
        using var df = SampleFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.WriteCsvAsync(w));

        Assert.Equal(df.WriteCsvMemory(), bytes);
    }

    [Fact]
    public void SinkCsv_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.Lazy().SinkCsv(ms);

        Assert.Equal(df.Lazy().SinkCsvMemory(), ms.ToArray());
    }

    [Fact]
    public async Task SinkCsvAsync_ToStream_MatchesMemory()
    {
        using var df = SampleFrame();
        using var ms = new RecordingStream(allowSyncWrites: false);

        await df.Lazy().SinkCsvAsync(ms);

        Assert.Equal(df.Lazy().SinkCsvMemory(), ms.ToArray());
    }

    [Fact]
    public async Task SinkCsvAsync_ToPipeWriter_MatchesMemory()
    {
        using var df = SampleFrame();

        byte[] bytes = await CollectFromPipeAsync(w => df.Lazy().SinkCsvAsync(w));

        Assert.Equal(df.Lazy().SinkCsvMemory(), bytes);
    }

    [Fact]
    public void WriteCsv_ToStream_RoundTrip()
    {
        using var df = SampleFrame();
        using var ms = new MemoryStream();

        df.WriteCsv(ms, includeHeader: true, separator: (byte)';');

        // CSV carries no type information (ints come back as Int64), so compare the content instead of Equals.
        using var back = DataFrame.ReadCsv(ms.ToArray(), separator: ';');
        Assert.Equal(5, back.Height);
        Assert.Equal("Alice", back.GetValue<string>(0, "name"));
        Assert.True(string.IsNullOrEmpty(back.GetValue<string>(2, "name"))); // null is written as an empty field
    }

    [Fact]
    public async Task SinkCsvAsync_ToStream_LargeOutput_RoundTrip()
    {
        using var df = LargeFrame();
        using var ms = new RecordingStream(allowSyncWrites: false, asyncDelayMs: 1);

        await df.Lazy().SinkCsvAsync(ms);

        Assert.True(ms.WriteCalls > 1);
        using var back = DataFrame.ReadCsv(ms.ToArray());
        Assert.Equal(df.Height, back.Height);
        Assert.Equal(df.Width, back.Width);
    }

    [Theory]
    [InlineData(QuoteStyle.Necessary, "id,name,score\n1,Alice,1.5\n")]
    [InlineData(QuoteStyle.Always, "\"id\",\"name\",\"score\"\n\"1\",\"Alice\",\"1.5\"\n")]
    [InlineData(QuoteStyle.NonNumeric, "\"id\",\"name\",\"score\"\n1,\"Alice\",1.5\n")]
    [InlineData(QuoteStyle.Never, "id,name,score\n1,Alice,1.5\n")]
    public void CsvQuoteStyle_IsMappedCorrectly(QuoteStyle style, string expected)
    {
        using var df = SampleFrame().Head(1);

        string memory = System.Text.Encoding.UTF8.GetString(df.WriteCsvMemory(quoteStyle: style));
        Assert.Equal(expected, memory);

        using var ms = new MemoryStream();
        df.WriteCsv(ms, quoteStyle: style);
        Assert.Equal(expected, System.Text.Encoding.UTF8.GetString(ms.ToArray()));
    }
}
