using System.Net;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Catalog;
using Kaminari.Seseragi.Lakehouse.Catalog.Unity;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Creating managed tables through Unity Catalog's Delta API, as Delta Spark 4.4 does with UC 0.7.</summary>
public class UnityManagedTablesTests
{
    // The staging-tables response of Unity Catalog 0.7.0 to Spark's CREATE TABLE (location shortened).
    private const string StagingResponse = """
        {"table-id":"a33b079e-85cb-4ee4-9352-85dfff7be0fb","table-type":"MANAGED","location":"file:///uc/tables/a33b079e",
         "storage-credentials":[{"prefix":"file:///uc/tables/a33b079e","operation":"READ_WRITE","config":{},"expiration-time-ms":null}],
         "required-protocol":{"min-reader-version":3,"min-writer-version":7,
           "reader-features":["catalogManaged","v2Checkpoint","vacuumProtocolCheck","deletionVectors"],
           "writer-features":["catalogManaged","v2Checkpoint","vacuumProtocolCheck","deletionVectors","inCommitTimestamp"]},
         "suggested-protocol":{"reader-features":["columnMapping"],"writer-features":["columnMapping","domainMetadata","rowTracking"]},
         "required-properties":{"delta.enableInCommitTimestamps":"true","delta.checkpoint.writeStatsAsJson":"true","delta.checkpointPolicy":"v2",
           "delta.checkpoint.writeStatsAsStruct":"true","delta.enableDeletionVectors":"true","io.unitycatalog.tableId":"a33b079e-85cb-4ee4-9352-85dfff7be0fb"},
         "suggested-properties":{"delta.enableRowTracking":"true","delta.rowTracking.materializedRowIdColumnName":null,
           "delta.columnMapping.maxColumnId":null,"delta.randomizeFilePrefixes":"true","delta.parquet.compression.codec":"zstd",
           "delta.rowTracking.materializedRowCommitVersionColumnName":null,"delta.columnMapping.mode":"name"}}
        """;

    private static UnityStagingTable Staging(string json = StagingResponse)
    {
        using JsonDocument doc = JsonDocument.Parse(json);
        return UnityCatalogClient.ReadStagingTable(doc.RootElement);
    }

    [Fact]
    public void TheStagingResponse_IsRead()
    {
        UnityStagingTable staging = Staging();
        Assert.Equal("a33b079e-85cb-4ee4-9352-85dfff7be0fb", staging.TableId);
        Assert.Equal("file:///uc/tables/a33b079e", staging.Location);
        Assert.Equal(CredentialKind.None, staging.Credentials.Kind);
        Assert.Equal((3, 7), (staging.RequiredProtocol.MinReaderVersion, staging.RequiredProtocol.MinWriterVersion));
        Assert.Equal(["columnMapping", "domainMetadata", "rowTracking"], staging.SuggestedWriterFeatures);
        Assert.Null(staging.SuggestedProperties["delta.columnMapping.maxColumnId"]);
    }

    [Fact]
    public void Properties_AreTheSuggestedOnes_TheCallersOverThem_ThenTheRequiredOnes_AsSpark()
    {
        Dictionary<string, string> properties = Staging().TableProperties(new Dictionary<string, string>
        {
            ["delta.columnMapping.mode"] = "none", // overrides a suggestion
            ["delta.enableChangeDataFeed"] = "true",
            ["delta.enableDeletionVectors"] = "true", // the required value: fine
        });
        Assert.Equal("none", properties["delta.columnMapping.mode"]);
        Assert.Equal("true", properties["delta.enableChangeDataFeed"]);
        Assert.Equal("true", properties["delta.randomizeFilePrefixes"]);
        Assert.Equal("true", properties["delta.enableRowTracking"]);
        Assert.Equal("v2", properties["delta.checkpointPolicy"]);
        Assert.Equal("a33b079e-85cb-4ee4-9352-85dfff7be0fb", properties["io.unitycatalog.tableId"]);
        // Suggestions without a value are the writer's to fill; Spark does not take the codec.
        Assert.DoesNotContain("delta.columnMapping.maxColumnId", properties.Keys);
        Assert.DoesNotContain("delta.parquet.compression.codec", properties.Keys);
    }

    [Fact]
    public void APropertyContradictingARequiredOne_IsRefused_AsSpark()
    {
        var e = Assert.Throws<CatalogException>(() => Staging().TableProperties(new Dictionary<string, string> { ["delta.enableDeletionVectors"] = "false" }));
        Assert.Contains("'delta.enableDeletionVectors'='true'", e.Message);
    }

    [Fact]
    public void Features_AreTheRequiredAndTheSuggestedOnes_AndAnUnknownRequiredOneIsRefused()
    {
        Assert.Equal(
            ["catalogManaged", "v2Checkpoint", "vacuumProtocolCheck", "deletionVectors", "inCommitTimestamp", "columnMapping", "domainMetadata", "rowTracking"],
            Staging().Features());
        UnityStagingTable odd = Staging() with
        {
            RequiredProtocol = new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = ["catalogManaged"], WriterFeatures = ["catalogManaged", "someFutureFeature"] },
        };
        Assert.Contains("someFutureFeature", Assert.Throws<CatalogException>(odd.Features).Message);
    }

    [Fact]
    public void Credentials_AreThoseOfTheLongestCoveringPrefix_InTheDeltaApisKeys()
    {
        string json = StagingResponse.Replace(
            """[{"prefix":"file:///uc/tables/a33b079e","operation":"READ_WRITE","config":{},"expiration-time-ms":null}]""",
            """
            [
              {"prefix":"file:///uc","operation":"READ_WRITE","config":{"s3.access-key-id":"wide","s3.secret-access-key":"w"},"expiration-time-ms":1},
              {"prefix":"file:///uc/tables/a33b079e","operation":"READ_WRITE","config":{"s3.access-key-id":"AK","s3.secret-access-key":"SK","s3.session-token":"ST"},"expiration-time-ms":1794461524990},
              {"prefix":"file:///other/tables/a33b079e/longer","operation":"READ_WRITE","config":{"azure.sas-token":"x"},"expiration-time-ms":1}]
            """);
        TemporaryCredentials credentials = Staging(json).Credentials;
        Assert.Equal(CredentialKind.Aws, credentials.Kind);
        Assert.Equal(("AK", "SK", "ST"), (credentials.AwsAccessKeyId, credentials.AwsSecretAccessKey, credentials.AwsSessionToken));
        Assert.Equal(1794461524990, credentials.ExpiresAt!.Value.ToUnixTimeMilliseconds());

        Assert.Equal("t", Staging(StagingResponse.Replace("\"config\":{}", "\"config\":{\"gcs.oauth-token\":\"t\"}")).Credentials.GcpOauthToken);
        Assert.Equal("s", Staging(StagingResponse.Replace("\"config\":{}", "\"config\":{\"azure.sas-token\":\"s\"}")).Credentials.AzureSasToken);
    }

    [Fact]
    public void TheCreateRequest_IsShapedAsSparks()
    {
        const string schema = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-1"}}]}""";
        var metadata = new MetadataAction
        {
            Id = "m", SchemaString = schema, PartitionColumns = ["p"],
            Configuration = new Dictionary<string, string> { ["delta.enableDeletionVectors"] = "true" },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = ["catalogManaged"], WriterFeatures = ["catalogManaged", "clustering"],
        };
        DomainMetadataAction[] domains =
        [
            new() { Domain = "delta.clustering", Configuration = """{"clusteringColumns":[["col-1"]],"domainName":"delta.clustering"}""" },
            new() { Domain = "delta.rowTracking", Configuration = """{"rowIdHighWaterMark":2,"domainName":"delta.rowTracking"}""" },
            new() { Domain = "custom", Configuration = "{}" },
        ];

        byte[] request = UnityManagedTables.CreateRequest("plain", "file:///uc/t", metadata, protocol, domains, 1794461521171);

        using JsonDocument doc = JsonDocument.Parse(request);
        JsonElement r = doc.RootElement;
        Assert.Equal(["name", "location", "table-type", "columns", "partition-columns", "protocol", "properties", "domain-metadata", "last-commit-timestamp-ms"],
            r.EnumerateObject().Select(p => p.Name));
        Assert.Equal("MANAGED", r.GetProperty("table-type").GetString());
        Assert.Equal("col-1", r.GetProperty("columns").GetProperty("fields")[0].GetProperty("metadata").GetProperty("delta.columnMapping.physicalName").GetString());
        Assert.Equal("p", r.GetProperty("partition-columns")[0].GetString());
        Assert.Equal(7, r.GetProperty("protocol").GetProperty("min-writer-version").GetInt32());
        Assert.Equal("""{"delta.clustering":{"clusteringColumns":[["col-1"]]},"delta.rowTracking":{"rowIdHighWaterMark":2}}""",
            r.GetProperty("domain-metadata").GetRawText());
        Assert.Equal(1794461521171, r.GetProperty("last-commit-timestamp-ms").GetInt64());

        // Without such domains, no domain-metadata (as Spark's plain CREATE TABLE).
        using JsonDocument plain = JsonDocument.Parse(UnityManagedTables.CreateRequest("plain", "file:///uc/t", metadata, protocol, [], 1));
        Assert.False(plain.RootElement.TryGetProperty("domain-metadata", out _));
    }

    [Fact]
    public void TheClient_ChecksTheName_StagesTheTable_AndCreatesIt_AtTheDeltaApi()
    {
        var handler = new StubHandler(r => (r.Method.Method, r.PathAndQuery) switch
        {
            ("GET", _) => (HttpStatusCode.NotFound, """{"error_code":"NOT_FOUND","message":"no"}"""),
            ("POST", var p) when p.EndsWith("/staging-tables", StringComparison.Ordinal) => (HttpStatusCode.OK, StagingResponse),
            _ => (HttpStatusCode.OK, "{}"),
        });
        var client = new UnityCatalogClient("http://uc.test/", "tok", new HttpClient(handler));

        Assert.False(client.DeltaTableExists("probe", "s", "plain"));
        Assert.Equal("a33b079e-85cb-4ee4-9352-85dfff7be0fb", client.CreateStagingTable("probe", "s", "plain").TableId);
        client.CreateDeltaTable("probe", "s", "{}"u8.ToArray());

        Assert.Equal(
            [
                "GET /api/2.1/unity-catalog/delta/v1/catalogs/probe/schemas/s/tables/plain",
                "POST /api/2.1/unity-catalog/delta/v1/catalogs/probe/schemas/s/staging-tables",
                "POST /api/2.1/unity-catalog/delta/v1/catalogs/probe/schemas/s/tables",
            ],
            handler.Requests.Select(r => $"{r.Method} {r.PathAndQuery}"));
        Assert.Equal("""{"name":"plain"}""", handler.Requests[1].Body);
    }

    [Fact]
    public void StagingCredentials_AreRenewed_AtTheDeltaApi()
    {
        var handler = new StubHandler(_ => (HttpStatusCode.OK, """
            {"storage-credentials":[
              {"prefix":"s3://b/","operation":"READ_WRITE","config":{"s3.access-key-id":"wide","s3.secret-access-key":"w"},"expiration-time-ms":1},
              {"prefix":"s3://b/uc/t1","operation":"READ_WRITE","config":{"s3.access-key-id":"AK2","s3.secret-access-key":"SK2","s3.session-token":"ST2"},"expiration-time-ms":1794461600000}]}
            """));
        var client = new UnityCatalogClient("http://uc.test/", "tok", new HttpClient(handler));

        TemporaryCredentials renewed = client.GetStagingTableCredentials("a33b079e", "s3://b/uc/t1");

        Assert.Equal(("AK2", "SK2", "ST2"), (renewed.AwsAccessKeyId, renewed.AwsSecretAccessKey, renewed.AwsSessionToken));
        Assert.Equal(1794461600000, renewed.ExpiresAt!.Value.ToUnixTimeMilliseconds());
        Assert.Equal("GET /api/2.1/unity-catalog/delta/v1/staging-tables/a33b079e/credentials",
            handler.Requests.Select(r => $"{r.Method} {r.PathAndQuery}").Single());
    }
}
