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

    public Task DisposeAsync()
    {
        return _minioContainer.DisposeAsync().AsTask();
    }
}