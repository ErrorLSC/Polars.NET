using WireMock.Server;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using Polars.CSharp;
using Polars.Integration.Tests.Fixtures; // 假设你的 Polars.NET 命名空间

namespace Polars.Integration.Tests;

// [Collection("MinioCollection")] // 假设你已经配置了 CollectionFixture
public class CatalogIntegrationTests(MinioFixture _minio) : IAsyncLifetime, IClassFixture<MinioFixture>
{
    private WireMockServer _catalogMockServer = null!;

    public Task InitializeAsync()
    {
        // 1. 启动本地的 HTTP Mock 服务器，充当 Unity Catalog
        _catalogMockServer = WireMockServer.Start();
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _catalogMockServer?.Stop();
        _catalogMockServer?.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    [Trait("Catalog","Scan")]
    public void Test_ScanDeltaCatalog_With_Minio_And_WireMock()
    {
        // ==========================================
        // 1. 准备物理路径和 Endpoint (针对 MinIO)
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_test_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        // ==========================================
        // 2. 真实写入阶段：使用已有验证通过的 WriteDelta
        // ==========================================
        // 写入用的 Options，需要直接包含 AccessKey 和 SecretKey
        var writeOptions = CloudOptions.Aws(
            region: _minio.Region,
            accessKey: _minio.AccessKey,
            secretKey: _minio.SecretKey,
            endpoint: polarsEndpoint
        );
        writeOptions.Credentials!["AWS_ALLOW_HTTP"] = "true";
        writeOptions.Credentials!["aws_allow_http"] = "true";
        writeOptions.Credentials!["AWS_S3_FORCE_PATH_STYLE"] = "true";
        writeOptions.Credentials!["aws_s3_force_path_style"] = "true";
        writeOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        Console.WriteLine("Step 1: Real Write (Create Table) directly to MinIO...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 },
            Msg = new[] { "Alice", "Bob", "Charlie" }
        }))
        {
            // 写入真实的 Parquet 和 _delta_log 文件夹
            df.WriteDelta(s3StorageLocation, mode: DeltaSaveMode.Append, cloudOptions: writeOptions);
        }

        // ==========================================
        // 3. 配置 WireMock 扮演 Databricks Unity Catalog
        // ==========================================
        Console.WriteLine("Step 2: Setup WireMock to act as Unity Catalog...");
        var expectedToken = "dapi-super-secret-token";
        var dummyTableId = Guid.NewGuid().ToString();
        // A. 伪造获取表信息接口 -> 返回刚才写入的真实 MinIO S3 路径
        _catalogMockServer
            .Given(
                Request.Create()
                    .WithPath($"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}")
                    .UsingGet()
                    .WithHeader("Authorization", $"Bearer {expectedToken}")
            )
            .RespondWith(
                Response.Create()
                    .WithStatusCode(200)
                    .WithBodyAsJson(new 
                    {
                        name = table,
                        catalog_name = catalog,
                        schema_name = schema,
                        table_type = "EXTERNAL",
                        storage_location = s3StorageLocation,
                        table_id = dummyTableId,
                        
                        // 【填坑】：这四个字段没有 default，必须出现！
                        created_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        created_by = "test_admin",
                        updated_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        updated_by = "test_admin"
                    })
            );

        // B. 伪造获取临时凭证接口 -> 同样补齐其他云平台的 null 字段
        _catalogMockServer
            .Given(
                Request.Create()
                    .WithPath("/api/2.1/unity-catalog/temporary-table-credentials")
                    .UsingPost()
                    .WithHeader("Authorization", $"Bearer {expectedToken}")
            )
            .RespondWith(
                Response.Create()
                    .WithStatusCode(200)
                    .WithBodyAsJson(new 
                    {
                        aws_temp_credentials = new 
                        {
                            access_key_id = _minio.AccessKey,
                            secret_access_key = _minio.SecretKey,
                            session_token = (string?)null,
                            access_point = (string?)null // 这里可选
                        },
                        // 【填坑】：必须显式输出 null，否则 serde 会报 missing field
                        azure_user_delegation_sas = (object?)null,
                        gcp_oauth_token = (object?)null,
                        
                        expiration_time = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds()
                    })
            );

        // ==========================================
        // 4. Catalog 读取阶段：使用 ScanDeltaCatalog
        // ==========================================
        Console.WriteLine("Step 3: Scan data using Catalog mechanism...");
        
        // 读取用的 Options，不需要（也不应该）包含长期 Key，它们会由 WireMock 动态下发！
        // 我们只保留 MinIO 需要的 Endpoint 和基础 HTTP 设置
        var readOptions = CloudOptions.Aws(
            region: _minio.Region,
            endpoint: polarsEndpoint
        );
        readOptions.Credentials!["aws_allow_http"] = "true";
        readOptions.Credentials!["aws_s3_force_path_style"] = "true";

        // 发起请求：向本地的 WireMock 请求表信息，底层自动拿凭证去连 MinIO
        using var lf = LazyFrame.ScanDelta(
            workspaceUrl: _catalogMockServer.Urls[0],
            bearerToken: expectedToken,
            catalogName: catalog,
            schemaName: schema,
            tableName: table,
            cloudOptions: readOptions
        );

        using var resultDf = lf.Collect();
        resultDf.Show();

        // ==========================================
        // 5. 验证闭环结果
        // ==========================================
        Assert.Equal(3, resultDf.Height);
        Assert.Contains("Alice", resultDf["Msg"].ToArray<string>());
        Assert.Contains("Charlie", resultDf["Msg"].ToArray<string>());

        Console.WriteLine("Success! Data successfully retrieved via Unity Catalog Mock.");
    }
}