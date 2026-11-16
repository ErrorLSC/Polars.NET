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
    public static byte[] File(int f)
    {
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("s", StringType.Default, true)], null);
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(f * Rows, Rows).Select(i => (long)i)).Build();
        var s = new StringArray.Builder().AppendRange(Enumerable.Range(f * Rows, Rows).Select(i => $"value {i}")).Build();
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions
        {
            RowGroupRows = Rows / 4,
            Compression = f % 2 == 0 ? NoCompression.Instance : new ZstandardCompression(),
        }))
        {
            writer.Write(new RecordBatch(schema, [ids, s], Rows));
            writer.Complete();
        }
        return stream.ToArray();
    }

    public static List<long> Ids(ParquetDataset dataset, int threads)
    {
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        List<long> ids = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
        }
        return ids;
    }

    public static void ReadTwice(IReadOnlyList<string> urls)
    {
        using var dataset = ParquetDataset.Open(urls);
        long[] expected = [.. Enumerable.Range(0, Files * Rows).Select(i => (long)i)];
        Assert.Equal(expected, Ids(dataset, 1));
        Assert.Equal(expected, Ids(dataset, 8)); // the large files reopened from their kept footers, their versions required (If-Match)
    }
}

/// <summary>Presigned GET URLs of a MinIO bucket (signed for GET only: no HEAD may be sent).</summary>
[Trait("Category", "Integration")]
public sealed class MinioSignedUrlTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    [Fact]
    public async Task ADatasetOfPresignedUrls_ReadsThem()
    {
        using var s3 = new AmazonS3Client(minio.AccessKey, minio.SecretKey, new AmazonS3Config { ServiceURL = minio.Endpoint, ForcePathStyle = true, UseHttp = true });
        List<string> urls = [];
        for (int f = 0; f < UrlDatasets.Files; f++)
        {
            string key = $"signed/f{f}.parquet";
            using var content = new MemoryStream(UrlDatasets.File(f));
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
        UrlDatasets.ReadTwice(urls);
    }
}

/// <summary>SAS URLs of an Azurite container (no suffix ranges there: files are opened by explicit ones).</summary>
[Trait("Category", "Integration")]
public sealed class AzuriteSasUrlTests(AzuriteFixture azurite) : IClassFixture<AzuriteFixture>
{
    [Fact]
    public async Task ADatasetOfSasUrls_ReadsThem()
    {
        var container = new BlobContainerClient(azurite.ConnectionString, azurite.ContainerName, new BlobClientOptions(BlobClientOptions.ServiceVersion.V2025_11_05));
        await container.CreateIfNotExistsAsync();
        List<string> urls = [];
        for (int f = 0; f < UrlDatasets.Files; f++)
        {
            BlobClient blob = container.GetBlobClient($"signed/f{f}.parquet");
            await blob.UploadAsync(new BinaryData(UrlDatasets.File(f)), overwrite: true);
            urls.Add(blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddHours(1)).ToString());
        }
        UrlDatasets.ReadTwice(urls);
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
}
