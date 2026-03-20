using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests;

public class HttpTests : IClassFixture<HttpFixture>
{
    private readonly HttpFixture _http;

    public HttpTests(HttpFixture http)
    {
        _http = http;
    }

    [Fact]
    public void Test_ScanParquet_From_Http_Nginx()
    {
        var httpUrl = $"{_http.BaseUrl}/data.parquet";

        var options = CloudOptions.Http(new Dictionary<string, string>
        {
            { "Authorization", "Bearer fake-token" },
            { "User-Agent", "Polars.NET-Test-Client" }
        });

        using var lf = LazyFrame.ScanParquet(httpUrl, cloudOptions: options);
        using var df = lf.Collect();

        // shape: (2, 2)
        // ┌──────┬───────┐
        // │ Name ┆ Value │
        // │ ---  ┆ ---   │
        // │ str  ┆ i32   │
        // ╞══════╪═══════╡
        // │ HTTP ┆ 123   │
        // │ Test ┆ 456   │
        // └──────┴───────┘

        Assert.Equal(2, df.Height);
        Assert.Equal("HTTP", df["Name"][0]);
        Assert.Equal(123, df["Value"][0]);
        
    }
}