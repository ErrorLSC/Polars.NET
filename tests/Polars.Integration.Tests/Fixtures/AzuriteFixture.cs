using Azure.Storage.Blobs;
using Testcontainers.Azurite;

namespace Polars.Integration.Tests.Fixtures;

public class AzuriteFixture : IAsyncLifetime
{
    private readonly AzuriteContainer _azuriteContainer;

    public const string AccountName = "devstoreaccount1";
    // Key: 
    public const string AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    
    public string ContainerName => "polars-azure-test";

    public AzuriteFixture()
    {
        _azuriteContainer = new AzuriteBuilder("mcr.microsoft.com/azure-storage/azurite:latest")
            .Build();
    }

    public string ConnectionString => _azuriteContainer.GetConnectionString();
    
    public string BlobEndpoint 
    {
        get
        {

            var parts = ConnectionString.Split(';');
            foreach (var part in parts)
            {
                if (part.StartsWith("BlobEndpoint="))
                {
                    return part["BlobEndpoint=".Length..];
                }
            }
            throw new Exception("Could not find BlobEndpoint in connection string");
        }
    }

    public async Task InitializeAsync()
    {
        await _azuriteContainer.StartAsync();

        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2025_11_05);
        
        var blobServiceClient = new BlobServiceClient(ConnectionString, options);
        var containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        await containerClient.CreateIfNotExistsAsync();
    }

    public Task DisposeAsync()
    {
        return _azuriteContainer.DisposeAsync().AsTask();
    }
}