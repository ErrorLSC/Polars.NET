using System.Net;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Catalog;
using Kaminari.Seseragi.Lakehouse.Catalog.Unity;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Answers requests from a script and records what was sent. Serves both the blocking and the async path.</summary>
internal sealed class StubHandler(Func<StubHandler.Recorded, (HttpStatusCode Status, string Body)> respond) : HttpMessageHandler
{
    internal sealed record Recorded(HttpMethod Method, string PathAndQuery, string? Authorization, string? Body, bool Sync);

    public List<Recorded> Requests { get; } = [];

    private HttpResponseMessage Handle(HttpRequestMessage request, string? body, bool sync)
    {
        var recorded = new Recorded(request.Method, request.RequestUri!.PathAndQuery, request.Headers.Authorization?.ToString(), body, sync);
        lock (Requests) Requests.Add(recorded);
        var (status, text) = respond(recorded);
        return new HttpResponseMessage(status) { Content = new StringContent(text, Encoding.UTF8, "application/json") };
    }

    protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        string? body = null;
        if (request.Content is not null)
        {
            using var reader = new StreamReader(request.Content.ReadAsStream(cancellationToken));
            body = reader.ReadToEnd();
        }
        return Handle(request, body, sync: true);
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        string? body = request.Content is null ? null : await request.Content.ReadAsStringAsync(cancellationToken);
        return Handle(request, body, sync: false);
    }
}

public class UnityCatalogClientTests
{
    private const string TableJson = """
        {
          "name": "t", "catalog_name": "main", "schema_name": "default",
          "table_type": "EXTERNAL", "data_source_format": "DELTA",
          "storage_location": "s3://bucket/t", "table_id": "id-1",
          "properties": null,
          "columns": [{"name": "Id", "type_name": "INT", "type_text": "int", "type_json": "{}", "position": 0}],
          "created_at": 1700000000000, "created_by": "admin"
        }
        """;

    private const string AwsCredentialsJson = """
        {
          "aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK", "session_token": null, "access_point": null},
          "azure_user_delegation_sas": null, "gcp_oauth_token": null,
          "expiration_time": 1700000360000
        }
        """;

    private static (UnityCatalogClient Client, StubHandler Handler) Make(Func<StubHandler.Recorded, (HttpStatusCode, string)> respond)
    {
        var handler = new StubHandler(respond);
        return (new UnityCatalogClient("http://uc.test/", "tok", new HttpClient(handler)), handler);
    }

    [Fact]
    public void GetTable_SendsBearerTokenToTheTablePath_AndReadsTheTable()
    {
        var (client, handler) = Make(_ => (HttpStatusCode.OK, TableJson));

        TableInfo table = client.GetTable("main", "default", "t");

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Get, request.Method);
        Assert.Equal("/api/2.1/unity-catalog/tables/main.default.t", request.PathAndQuery);
        Assert.Equal("Bearer tok", request.Authorization);
        Assert.True(request.Sync);

        Assert.Equal("t", table.Name);
        Assert.Equal("id-1", table.TableId);
        Assert.Equal(UnityTableTypes.External, table.TableType);
        Assert.Equal(UnityDataSourceFormats.Delta, table.DataSourceFormat);
        Assert.Equal("s3://bucket/t", table.StorageLocation);
        Assert.Empty(table.Properties);
        Assert.Equal("Id", Assert.Single(table.Columns!).Name);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), table.CreatedAt);
        Assert.Null(table.UpdatedAt);
    }

    [Fact]
    public async Task GetTableAsync_GoesThroughTheAsyncPath()
    {
        var (client, handler) = Make(_ => (HttpStatusCode.OK, TableJson));

        TableInfo table = await client.GetTableAsync("main", "default", "t");

        Assert.Equal("id-1", table.TableId);
        Assert.False(Assert.Single(handler.Requests).Sync);
    }

    [Fact]
    public void GetTable_EscapesNameParts()
    {
        var (client, handler) = Make(_ => (HttpStatusCode.OK, TableJson));

        client.GetTable("ma/in", "de fault", "t");

        Assert.Equal("/api/2.1/unity-catalog/tables/ma%2Fin.de%20fault.t", Assert.Single(handler.Requests).PathAndQuery);
    }

    [Theory]
    [InlineData(false, "READ")]
    [InlineData(true, "READ_WRITE")]
    public void GetTableCredentials_PostsTableIdAndOperation(bool write, string operation)
    {
        var (client, handler) = Make(_ => (HttpStatusCode.OK, AwsCredentialsJson));

        TemporaryCredentials credentials = client.GetTableCredentials("id-1", write);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("/api/2.1/unity-catalog/temporary-table-credentials", request.PathAndQuery);
        using var body = JsonDocument.Parse(request.Body!);
        Assert.Equal("id-1", body.RootElement.GetProperty("table_id").GetString());
        Assert.Equal(operation, body.RootElement.GetProperty("operation").GetString());

        Assert.Equal(CredentialKind.Aws, credentials.Kind);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000360000), credentials.ExpiresAt);
        var options = credentials.ToStorageOptions();
        Assert.Equal("AK", options["aws_access_key_id"]);
        Assert.Equal("SK", options["aws_secret_access_key"]);
        Assert.False(options.ContainsKey("aws_session_token"));
    }

    [Fact]
    public void GetTableCredentials_ReadsAzureAndGcp()
    {
        var (azure, _) = Make(_ => (HttpStatusCode.OK, """{"azure_user_delegation_sas": {"sas_token": "sas"}, "expiration_time": 1}"""));
        Assert.Equal("sas", azure.GetTableCredentials("id", false).ToStorageOptions()["azure_storage_sas_token"]);

        var (gcp, _) = Make(_ => (HttpStatusCode.OK, """{"gcp_oauth_token": {"oauth_token": "oa"}}"""));
        TemporaryCredentials credentials = gcp.GetTableCredentials("id", false);
        Assert.Equal("oa", credentials.ToStorageOptions()["google_oauth_token"]);
        Assert.Null(credentials.ExpiresAt);
    }

    [Fact]
    public void GetTableCredentials_WithoutAnyCredentials_Throws()
    {
        var (client, _) = Make(_ => (HttpStatusCode.OK, """{"expiration_time": 1}"""));

        Assert.Throws<CatalogException>(() => client.GetTableCredentials("id", false));
    }

    [Fact]
    public void ResolveTableAccess_LaysVendedCredentialsOverBaseOptions()
    {
        var (client, handler) = Make(r => (HttpStatusCode.OK, r.Method == HttpMethod.Get ? TableJson : AwsCredentialsJson));

        TableAccess access = client.ResolveTableAccess("main", "default", "t", write: true);

        Assert.Equal(2, handler.Requests.Count);
        Assert.Contains("READ_WRITE", handler.Requests[1].Body);
        Assert.Equal("s3://bucket/t", access.Location);

        var merged = access.MergeStorageOptions(new Dictionary<string, string>
        {
            ["aws_region"] = "us-east-1",
            ["aws_access_key_id"] = "stale",
        });
        Assert.Equal("us-east-1", merged["aws_region"]);
        Assert.Equal("AK", merged["aws_access_key_id"]);
        Assert.Equal("SK", merged["aws_secret_access_key"]);
    }

    [Fact]
    public void ResolveTableAccess_WithoutLocation_Throws()
    {
        var (client, _) = Make(r => (HttpStatusCode.OK, r.Method == HttpMethod.Get
            ? """{"name": "v", "table_id": "id", "table_type": "VIEW"}"""
            : AwsCredentialsJson));

        var e = Assert.Throws<CatalogException>(() => client.ResolveTableAccess("main", "default", "v", false));
        Assert.Contains("no storage location", e.Message);
    }

    [Fact]
    public void ErrorStatus_CarriesStatusAndBody()
    {
        var (client, _) = Make(_ => (HttpStatusCode.NotFound, """{"error_code": "TABLE_DOES_NOT_EXIST"}"""));

        var e = Assert.Throws<CatalogException>(() => client.GetTable("main", "default", "missing"));

        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
        Assert.Contains("TABLE_DOES_NOT_EXIST", e.ResponseBody);
        Assert.Contains("404", e.Message);
    }

    [Fact]
    public void InvalidJson_Throws()
    {
        var (client, _) = Make(_ => (HttpStatusCode.OK, "<html>"));

        Assert.Throws<CatalogException>(() => client.GetTable("main", "default", "t"));
    }

    [Fact]
    public void ListTables_FollowsPageTokens()
    {
        var (client, handler) = Make(r => r.PathAndQuery.Contains("page_token=p%2F2")
            ? (HttpStatusCode.OK, """{"tables": [{"name": "b", "table_id": "2", "table_type": "MANAGED"}], "next_page_token": null}""")
            : (HttpStatusCode.OK, """{"tables": [{"name": "a", "table_id": "1", "table_type": "MANAGED"}], "next_page_token": "p/2"}"""));

        List<TableInfo> tables = client.ListTables("main", "default");

        Assert.Equal(["a", "b"], tables.Select(t => t.Name));
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal("/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=default", handler.Requests[0].PathAndQuery);
        Assert.Equal("/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=default&page_token=p%2F2", handler.Requests[1].PathAndQuery);
    }

    [Fact]
    public void ListCatalogs_And_ListSchemas_ReadTheirArrays()
    {
        var (client, handler) = Make(r => r.PathAndQuery.Contains("/schemas")
            ? (HttpStatusCode.OK, """{"schemas": [{"name": "default", "properties": {"k": "v"}}]}""")
            : (HttpStatusCode.OK, """{}"""));

        Assert.Empty(client.ListCatalogs());
        SchemaInfo schema = Assert.Single(client.ListSchemas("main"));

        Assert.Equal("default", schema.Name);
        Assert.Equal("v", schema.Properties["k"]);
        Assert.Equal("/api/2.1/unity-catalog/schemas?catalog_name=main", handler.Requests[1].PathAndQuery);
    }

    [Fact]
    public void CreateTable_PostsTheFullBody()
    {
        var (client, handler) = Make(_ => (HttpStatusCode.OK, TableJson));

        client.CreateTable(
            "main", "default", "t",
            UnityTableTypes.External,
            UnityDataSourceFormats.Delta,
            [ColumnInfo.Create("Id", UnityDataType.Int, position: 0), ColumnInfo.Create("Msg", UnityDataType.String, position: 1)],
            storageLocation: "s3://bucket/t");

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("/api/2.1/unity-catalog/tables", request.PathAndQuery);

        using var body = JsonDocument.Parse(request.Body!);
        JsonElement root = body.RootElement;
        Assert.Equal("t", root.GetProperty("name").GetString());
        Assert.Equal("main", root.GetProperty("catalog_name").GetString());
        Assert.Equal("default", root.GetProperty("schema_name").GetString());
        Assert.Equal("EXTERNAL", root.GetProperty("table_type").GetString());
        Assert.Equal("DELTA", root.GetProperty("data_source_format").GetString());
        Assert.Equal("s3://bucket/t", root.GetProperty("storage_location").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("comment").ValueKind);
        Assert.Equal(JsonValueKind.Object, root.GetProperty("properties").ValueKind);

        JsonElement id = root.GetProperty("columns")[0];
        Assert.Equal("Id", id.GetProperty("name").GetString());
        Assert.Equal("INT", id.GetProperty("type_name").GetString());
        Assert.Equal("int", id.GetProperty("type_text").GetString());
        Assert.Equal("""{"name":"Id","type":"integer","nullable":true,"metadata":{}}""", id.GetProperty("type_json").GetString());
        Assert.Equal(0, id.GetProperty("position").GetInt32());
        Assert.Equal(JsonValueKind.Null, id.GetProperty("partition_index").ValueKind);
        Assert.Equal(1, root.GetProperty("columns")[1].GetProperty("position").GetInt32());
    }

    [Fact]
    public void Deletes_UseTheRightPaths()
    {
        var (client, handler) = Make(_ => (HttpStatusCode.OK, ""));

        client.DeleteTable("main", "default", "t");
        client.DeleteSchema("main", "default", force: true);
        client.DeleteCatalog("main");

        Assert.All(handler.Requests, r => Assert.Equal(HttpMethod.Delete, r.Method));
        Assert.Equal(
            [
                "/api/2.1/unity-catalog/tables/main.default.t",
                "/api/2.1/unity-catalog/schemas/main.default?force=true",
                "/api/2.1/unity-catalog/catalogs/main?force=false",
            ],
            handler.Requests.Select(r => r.PathAndQuery));
    }

    [Fact]
    public void NoToken_SendsNoAuthorizationHeader()
    {
        var handler = new StubHandler(_ => (HttpStatusCode.OK, TableJson));
        using var client = new UnityCatalogClient("http://uc.test", null, new HttpClient(handler));

        client.GetTable("main", "default", "t");

        Assert.Null(Assert.Single(handler.Requests).Authorization);
    }
}

public class UnityDataTypeTests
{
    [Fact]
    public void Primitives_RenderAllThreeForms()
    {
        Assert.Equal(("LONG", "bigint"), (UnityDataType.Long.TypeName, UnityDataType.Long.TypeText));
        Assert.Equal(("BYTE", "tinyint"), (UnityDataType.Byte.TypeName, UnityDataType.Byte.TypeText));
        Assert.Equal(("TIMESTAMP_NTZ", "timestamp_ntz"), (UnityDataType.TimestampNtz.TypeName, UnityDataType.TimestampNtz.TypeText));
        Assert.Equal("""{"name":"c","type":"long","nullable":true,"metadata":{}}""", UnityDataType.Long.ToTypeJson("c"));
    }

    [Fact]
    public void Decimal_UsesTypeTextInJson()
    {
        UnityDataType type = UnityDataType.Decimal(18, 2);

        Assert.Equal("DECIMAL", type.TypeName);
        Assert.Equal("decimal(18,2)", type.TypeText);
        Assert.Equal("""{"name":"d","type":"decimal(18,2)","nullable":true,"metadata":{}}""", type.ToTypeJson("d"));
    }

    [Fact]
    public void Array_Map_Struct_Nest()
    {
        UnityDataType type = UnityDataType.Array(UnityDataType.Struct(
        [
            new UnityField("x", UnityDataType.Long),
            new UnityField("m", UnityDataType.Map(UnityDataType.String, UnityDataType.Double)),
        ]));

        Assert.Equal("ARRAY", type.TypeName);
        Assert.Equal("array<struct<x:bigint,m:map<string,double>>>", type.TypeText);
        Assert.Equal(
            """{"name":"a","type":{"type":"array","elementType":{"type":"struct","fields":[""" +
            """{"name":"x","type":"long","nullable":true,"metadata":{}},""" +
            """{"name":"m","type":{"type":"map","keyType":"string","valueType":"double","valueContainsNull":true},"nullable":true,"metadata":{}}""" +
            """]},"containsNull":true},"nullable":true,"metadata":{}}""",
            type.ToTypeJson("a"));
    }
}
