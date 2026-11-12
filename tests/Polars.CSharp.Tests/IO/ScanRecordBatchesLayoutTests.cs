using Apache.Arrow;

namespace Polars.CSharp.Tests;

public class ScanRecordBatchesLayoutTests
{
    [Fact]
    public void ScanRecordBatches_converts_standard_arrow_layouts()
    {
        // Utf8 (not Utf8View) and Time32[ms] (not Time64[ns]) used to be taken as the engine's own layouts.
        var schema = new Schema(
        [
            new Apache.Arrow.Field("s", Apache.Arrow.Types.StringType.Default, true),
            new Apache.Arrow.Field("t", new Apache.Arrow.Types.Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond), true),
        ], null);
        var batch = new RecordBatch(schema,
        [
            new StringArray.Builder().Append("Alice").Append("Bob").Build(),
            new Time32Array.Builder(new Apache.Arrow.Types.Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond)).Append(1000).Append(2000).Build(),
        ], 2);
        using var df = LazyFrame.ScanRecordBatches([batch], schema).Collect();
        Assert.Equal("Bob", df.GetValue<string>(1, "s"));
        Assert.Equal(new TimeOnly(0, 0, 2), df.GetValue<TimeOnly>(1, "t"));
    }
}
