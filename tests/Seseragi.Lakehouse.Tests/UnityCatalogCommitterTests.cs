using System.Net;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Catalog.Unity;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Unity Catalog's Delta API as the managing catalog: requests shaped as Delta Spark 4.4 sends them to UC 0.7.</summary>
public class UnityCatalogCommitterTests
{
    // Trimmed from a load-table response of Unity Catalog 0.7.0 for a table Spark created.
    private const string LoadResponse = """
        {"metadata":{"etag":"etag-1","table-type":"MANAGED","table-uuid":"267b000a-169e-46a5-8bc7-18be1468efcc","location":"file:///t",
         "properties":{"delta.feature.catalogManaged":"supported"},"last-commit-version":1},
         "commits":[{"version":1,"timestamp":1794455332145,"file-name":"00000000000000000001.3225533a-ba0b-4eaa-906d-ee156343a614.json","file-size":1759,"file-modification-timestamp":1794455332158}],
         "latest-table-version":1,"allowed-maintenance-operations":["DATA_REORGANIZATION"]}
        """;

    private static readonly ProtocolAction Protocol = new()
    {
        MinReaderVersion = 3, MinWriterVersion = 7,
        ReaderFeatures = ["catalogManaged", "deletionVectors"], WriterFeatures = ["catalogManaged", "deletionVectors", "inCommitTimestamp"],
    };

    private static MetadataAction Metadata(string schema, params (string Key, string Value)[] properties) => new()
    {
        Id = "t", SchemaString = schema, Configuration = properties.ToDictionary(p => p.Key, p => p.Value),
    };

    private const string Schema = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""";

    private static CatalogCommit Commit(
        long version, MetadataAction? metadata = null, ProtocolAction? protocol = null, IReadOnlyList<DomainMetadataAction>? domains = null,
        MetadataAction? read = null) =>
        new(version, $"{version:D20}.0e0e0e0e-0000-0000-0000-000000000000.json", 1759, DateTimeOffset.FromUnixTimeMilliseconds(1794455332158),
            1794455332145, version - 1, Protocol, read ?? Metadata(Schema, ("a", "1"), ("gone", "x")), protocol, metadata, domains ?? []);

    private static (UnityCatalogCommitter Committer, StubHandler Handler) Committer(Func<StubHandler.Recorded, (HttpStatusCode, string)> update)
    {
        var handler = new StubHandler(r => r.Method == HttpMethod.Get ? (HttpStatusCode.OK, LoadResponse) : update(r));
        var client = new UnityCatalogClient("http://uc.test/", "tok", new HttpClient(handler));
        return (new UnityCatalogCommitter(client, "probe", "s", "t3"), handler);
    }

    [Fact]
    public void TheLog_IsTheCatalogsLatestVersion_AndTheCommitsItHolds()
    {
        var (committer, handler) = Committer(_ => throw new InvalidOperationException());
        CatalogLog log = committer.GetLog();
        Assert.Equal(1, log.LatestVersion);
        RatifiedCommit commit = Assert.Single(log.Commits);
        Assert.Equal((1L, "00000000000000000001.3225533a-ba0b-4eaa-906d-ee156343a614.json", 1759L, 1794455332158L),
            (commit.Version, commit.FileName, commit.Size, commit.ModificationTime.ToUnixTimeMilliseconds()));
        Assert.Equal("/api/2.1/unity-catalog/delta/v1/catalogs/probe/schemas/s/tables/t3", Assert.Single(handler.Requests).PathAndQuery);
    }

    [Fact]
    public void AnAppend_SendsTheCommitAndHowFarTheLogIsPublished_AsSparkDoes()
    {
        byte[] request = UnityCatalogCommitter.UpdateRequest("267b000a-169e-46a5-8bc7-18be1468efcc", Commit(2));
        // As captured from Delta Spark 4.4.1 (file name and numbers aside).
        Assert.Equal(
            """{"requirements":[{"type":"assert-table-uuid","uuid":"267b000a-169e-46a5-8bc7-18be1468efcc"}],"updates":[{"action":"add-commit","commit":{"version":2,"timestamp":1794455332145,"file-name":"00000000000000000002.0e0e0e0e-0000-0000-0000-000000000000.json","file-size":1759,"file-modification-timestamp":1794455332158}},{"action":"set-latest-backfilled-version","latest-published-version":1}]}""",
            Encoding.UTF8.GetString(request));
    }

    [Fact]
    public void MetadataProtocolAndClusteringChanges_AreSentAsTheirUpdates()
    {
        const string wider = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"n","type":"integer","nullable":true,"metadata":{}}]}""";
        ProtocolAction clustered = new()
        {
            MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = Protocol.ReaderFeatures,
            WriterFeatures = [.. Protocol.WriterFeatures!, "clustering", "domainMetadata"],
        };
        DomainMetadataAction clustering = new()
        {
            Domain = "delta.clustering", Configuration = """{"clusteringColumns":[["col-1"]],"domainName":"delta.clustering"}""",
        };
        CatalogCommit commit = Commit(5, Metadata(wider, ("a", "2"), ("new", "y")), clustered, [clustering]);

        using JsonDocument doc = JsonDocument.Parse(UnityCatalogCommitter.UpdateRequest("u", commit));
        Dictionary<string, string> updates = doc.RootElement.GetProperty("updates").EnumerateArray()
            .ToDictionary(u => u.GetProperty("action").GetString()!, u => u.GetRawText());

        Assert.Equal(["add-commit", "set-latest-backfilled-version", "set-properties", "remove-properties", "set-columns", "set-protocol", "set-domain-metadata"],
            updates.Keys);
        Assert.Equal("""{"action":"set-properties","updates":{"a":"2","new":"y"}}""", updates["set-properties"]);
        Assert.Equal("""{"action":"remove-properties","removals":["gone"]}""", updates["remove-properties"]);
        Assert.Equal("""{"action":"set-columns","columns":""" + wider + "}", updates["set-columns"]);
        Assert.Contains("""writer-features":["catalogManaged","deletionVectors","inCommitTimestamp","clustering","domainMetadata"]""", updates["set-protocol"]);
        Assert.Equal("""{"action":"set-domain-metadata","updates":{"delta.clustering":{"clusteringColumns":[["col-1"]]}}}""", updates["set-domain-metadata"]);
    }

    [Fact]
    public void AnUnchangedProtocolOrMetadata_SendsNoUpdateForThem()
    {
        MetadataAction same = Metadata(Schema, ("a", "1"), ("gone", "x"));
        using JsonDocument doc = JsonDocument.Parse(UnityCatalogCommitter.UpdateRequest("u", Commit(2, same, Protocol, read: same)));
        Assert.Equal(2, doc.RootElement.GetProperty("updates").GetArrayLength());
    }

    [Theory]
    [InlineData(HttpStatusCode.OK, CatalogCommitOutcome.Ratified)]
    [InlineData(HttpStatusCode.Conflict, CatalogCommitOutcome.Conflict)]
    public void TheCatalogsAnswer_DecidesTheOutcome(HttpStatusCode status, CatalogCommitOutcome expected)
    {
        var (committer, _) = Committer(_ => (status, status == HttpStatusCode.OK ? LoadResponse : """{"error":{"type":"CommitVersionConflictException"}}"""));
        Assert.Equal(expected, committer.Ratify(Commit(2)));
    }

    [Fact]
    public void ARefusal_IsDefinite()
    {
        var (committer, handler) = Committer(_ => (HttpStatusCode.BadRequest, """{"error":{"message":"Commit version must be the next version"}}"""));
        Assert.Throws<CatalogCommitRejectedException>(() => committer.Ratify(Commit(2)));
        Assert.Single(handler.Requests, r => r.Method == HttpMethod.Post);
    }

    [Fact]
    public void ALostAnswer_IsAskedAgainWithTheSameRequest_AndThatAnswerCounts()
    {
        int posts = 0;
        var (committer, handler) = Committer(_ => ++posts == 1 ? throw new HttpRequestException("connection reset") : (HttpStatusCode.OK, LoadResponse));
        Assert.Equal(CatalogCommitOutcome.Ratified, committer.Ratify(Commit(2)));
        StubHandler.Recorded[] sent = [.. handler.Requests.Where(r => r.Method == HttpMethod.Post)];
        Assert.Equal(2, sent.Length);
        Assert.Equal(sent[0].Body, sent[1].Body);
    }

    [Fact]
    public void TwoLostAnswers_LeaveTheOutcomeUnknown()
    {
        var (committer, _) = Committer(_ => (HttpStatusCode.ServiceUnavailable, "{}"));
        Assert.Throws<CatalogCommitOutcomeUnknownException>(() => committer.Ratify(Commit(2)));
    }
}
