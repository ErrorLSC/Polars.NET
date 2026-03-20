using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests;

public class AzureTests : IClassFixture<AzuriteFixture>
{
    private readonly AzuriteFixture _azurite;

    public AzureTests(AzuriteFixture azurite)
    {
        _azurite = azurite;
    }

    [Fact]
    public void Test_RoundTrip_Parquet_Azure_Azurite()
    {
        var azureUrl = $"az://{_azurite.ContainerName}/test_azure.parquet";

        var options = CloudOptions.Azure(
            accountName: AzuriteFixture.AccountName,
            accessKey: AzuriteFixture.AccountKey,
            endpoint: _azurite.BlobEndpoint 
        );


        if (options.Credentials != null)
        {
            options.Credentials["azure_allow_http"] = "true";
            options.Credentials["azure_use_emulator"] = "true"; 
        }

        using var df = DataFrame.FromColumns(new
        {
            City = new[] { "Seattle", "Redmond", "Bellevue" },
            Temp = new[] { 15.5, 16.0, 15.2 }
        });
        
        // Write (Sink)
        df.Lazy().SinkParquet(azureUrl, cloudOptions: options);

        // Read (Scan)
        using var lfRead = LazyFrame.ScanParquet(azureUrl, cloudOptions: options);
        using var dfRead = lfRead.Collect();

        Assert.Equal(df.Height, dfRead.Height);
        Assert.Equal("Seattle", dfRead["City"][0]);
    }
}