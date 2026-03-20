using Polars.Integration.Tests.Fixtures;
using Polars.CSharp;

namespace Polars.Integration.Tests;

public class AwsTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    [Fact]
    public void Test_RoundTrip_Parquet_AWS()
    {
        var s3Url = $"s3://{minio.BucketName}/test_roundtrip.parquet";
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: minio.Endpoint
        );
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true"; 

        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3, 4, 5 },
            Name = new[] { "Alice", "Bob", "Charlie", "David", "Eve" },
            Score = new[] { 99.5, 88.0, 75.5, 60.0, 100.0 }
        });

        df.WriteParquet(s3Url, cloudOptions: options);

        using var dfRead = DataFrame.ReadParquet(s3Url, cloudOptions: options);

        Assert.Equal(df.Height, dfRead.Height);
        
        var originalNames = df["Name"].ToArray<string>();
        var readNames = dfRead["Name"].ToArray<string>();
        
        Assert.Equal(originalNames, readNames);

    }
}