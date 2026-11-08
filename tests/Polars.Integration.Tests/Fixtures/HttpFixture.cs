using Polars.CSharp;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Polars.Integration.Tests.Fixtures;

public class HttpFixture : IAsyncLifetime
{
    private readonly IContainer _nginxContainer;
    private string _localParquetPath = null!;

    public const int ContainerPort = 80;

    public HttpFixture()
    {
        _nginxContainer = new ContainerBuilder("nginx:alpine")
            .WithPortBinding(ContainerPort, true)
            .Build();
    }

    public string BaseUrl => $"http://{_nginxContainer.Hostname}:{_nginxContainer.GetMappedPublicPort(ContainerPort)}";

    public async Task InitializeAsync()
    {
        await _nginxContainer.StartAsync();

        _localParquetPath = Path.GetTempFileName();
        
        using var df = DataFrame.FromColumns(new
        {
            Name = new[] { "HTTP", "Test" },
            Value = new[] { 123, 456 }
        });
        df.WriteParquet(_localParquetPath);

        var fileContent = await File.ReadAllBytesAsync(_localParquetPath);
        
        await _nginxContainer.ExecAsync(
        [
            "sh", "-c", $"echo 'Hello' > /usr/share/nginx/html/index.html" 
        ]);
        
        await _nginxContainer.CopyAsync(fileContent, "/usr/share/nginx/html/data.parquet");
    }

    public Task DisposeAsync()
    {
        if (File.Exists(_localParquetPath)) File.Delete(_localParquetPath);
        return _nginxContainer.DisposeAsync().AsTask();
    }
}