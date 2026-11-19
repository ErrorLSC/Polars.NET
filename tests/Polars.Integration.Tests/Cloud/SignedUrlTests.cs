using Amazon.S3;
using Amazon.S3.Model;
using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests.Cloud;

/// <summary>Files of a dataset given as http(s) URLs (no store given): read by ranged GETs, read again with kept footers.</summary>
internal static class UrlDatasets
{
    public const int Files = 4, Rows = 20_000;

    /// <summary>File <paramref name="f"/>: ids from f × Rows; even files uncompressed (beyond the first read of a tail).</summary>
    public static byte[] File(string format, int f) => DatasetFormats.File(format, (long)f * Rows, Rows, large: f % 2 == 0);

    public static void ReadTwice(string format, IReadOnlyList<string> urls)
    {
        using DatasetFormats.Opened dataset = DatasetFormats.Open(format, urls);
        long[] expected = [.. Enumerable.Range(0, Files * Rows).Select(i => (long)i)];
        Assert.Equal(expected, dataset.Ids(1));
        Assert.Equal(expected, dataset.Ids(8)); // the large files reopened from their kept footers, their versions required (If-Match)
    }
}

/// <summary>Presigned GET URLs of a MinIO bucket (signed for GET only: no HEAD may be sent).</summary>
[Trait("Category", "Integration")]
public sealed class MinioSignedUrlTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public async Task ADatasetOfPresignedUrls_ReadsThem(string format)
    {
        using var s3 = new AmazonS3Client(minio.AccessKey, minio.SecretKey, new AmazonS3Config { ServiceURL = minio.Endpoint, ForcePathStyle = true, UseHttp = true });
        List<string> urls = [];
        for (int f = 0; f < UrlDatasets.Files; f++)
        {
            string key = $"signed/f{f}.{DatasetFormats.Extension(format)}";
            using var content = new MemoryStream(UrlDatasets.File(format, f));
            await s3.PutObjectAsync(new PutObjectRequest { BucketName = minio.BucketName, Key = key, InputStream = content });
            urls.Add(await s3.GetPreSignedURLAsync(new GetPreSignedUrlRequest
            {
                BucketName = minio.BucketName,
                Key = key,
                Verb = HttpVerb.GET,
                Protocol = Protocol.HTTP,
                Expires = DateTime.UtcNow.AddHours(1),
            }));
        }
        UrlDatasets.ReadTwice(format, urls);
    }
}

/// <summary>SAS URLs of an Azurite container (no suffix ranges there: files are opened by explicit ones).</summary>
[Trait("Category", "Integration")]
public sealed class AzuriteSasUrlTests(AzuriteFixture azurite) : IClassFixture<AzuriteFixture>
{
    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public async Task ADatasetOfSasUrls_ReadsThem(string format)
    {
        var container = new BlobContainerClient(azurite.ConnectionString, azurite.ContainerName, new BlobClientOptions(BlobClientOptions.ServiceVersion.V2025_11_05));
        await container.CreateIfNotExistsAsync();
        List<string> urls = [];
        for (int f = 0; f < UrlDatasets.Files; f++)
        {
            BlobClient blob = container.GetBlobClient($"signed/f{f}.{DatasetFormats.Extension(format)}");
            await blob.UploadAsync(new BinaryData(UrlDatasets.File(format, f)), overwrite: true);
            urls.Add(blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddHours(1)).ToString());
        }
        UrlDatasets.ReadTwice(format, urls);
    }
}

/// <summary>A plain web server (nginx): a file by its URL.</summary>
[Trait("Category", "Integration")]
public sealed class WebServerUrlTests(HttpFixture http) : IClassFixture<HttpFixture>
{
    [Fact]
    public void AFileOfAWebServer_ReadsByItsUrl()
    {
        using var dataset = ParquetDataset.Open($"{http.BaseUrl}/data.parquet");
        using var reader = dataset.OpenReader();
        List<string> names = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                var column = (StringViewArray)batch.Column(batch.Schema.GetFieldIndex("Name"));
                for (int i = 0; i < batch.Length; i++) names.Add(column.GetString(i)!);
            }
        }
        Assert.Equal(["HTTP", "Test"], names);
    }

    /// <summary>Files on the web server given as URLs, read twice (the large ones' footers kept, their ETags required).</summary>
    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public async Task ADatasetOfWebServerUrls_ReadsThem(string format)
    {
        List<string> urls = [];
        for (int f = 0; f < UrlDatasets.Files; f++)
        {
            string name = $"set_{format}_f{f}.{DatasetFormats.Extension(format)}";
            await http.PutAsync(name, UrlDatasets.File(format, f));
            urls.Add($"{http.BaseUrl}/{name}");
        }
        UrlDatasets.ReadTwice(format, urls);
    }
}
