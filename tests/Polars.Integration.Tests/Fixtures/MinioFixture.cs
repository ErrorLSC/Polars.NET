using Amazon.S3;
using Testcontainers.Minio;

namespace Polars.Integration.Tests.Fixtures;

public class MinioFixture : IAsyncLifetime
{
    private readonly MinioContainer _minioContainer;

    public string AccessKey => "admin";
    public string SecretKey => "password";
    public string BucketName => "polars-test";
    public string Region => "us-east-1";

    public MinioFixture()
    {
        _minioContainer = new MinioBuilder("minio/minio:latest")
            .WithUsername(AccessKey)
            .WithPassword(SecretKey)
            .Build();
    }

    public string Endpoint => _minioContainer.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _minioContainer.StartAsync();

        var s3Config = new AmazonS3Config
        {
            ServiceURL = Endpoint,
            ForcePathStyle = true, 
            UseHttp = true
        };

        using var s3Client = new AmazonS3Client(AccessKey, SecretKey, s3Config);

        try 
        {
            await s3Client.PutBucketAsync(BucketName);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error creating bucket: {ex.Message}");
            throw;
        }
    }

    /// <summary>Adds a user that may read and write every bucket; returns its access key and secret.</summary>
    public async Task<(string AccessKey, string SecretKey)> AddUserAsync()
    {
        string key = "u" + Guid.NewGuid().ToString("N")[..15];
        string secret = Guid.NewGuid().ToString("N");
        await Mc($"admin user add local {key} {secret}");
        await Mc($"admin policy attach local readwrite --user {key}");
        return (key, secret);
    }

    /// <summary>Disables a user: requests signed with its keys are refused from now on.</summary>
    public Task DisableUserAsync(string accessKey) => Mc($"admin user disable local {accessKey}");

    private async Task Mc(string arguments)
    {
        var result = await _minioContainer.ExecAsync(
            ["sh", "-c", $"mc alias set local http://127.0.0.1:9000 {AccessKey} {SecretKey} >/dev/null && mc {arguments}"]);
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"mc {arguments} failed: {result.Stderr}{result.Stdout}");
    }

    public Task DisposeAsync()
    {
        return _minioContainer.DisposeAsync().AsTask();
    }
}