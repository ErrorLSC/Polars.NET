using System.Text.Json;
using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;
using static Polars.CSharp.Polars;

namespace Polars.Integration.Tests;

/// <summary>
/// Catalog-managed Delta tables (catalogManaged) on an open-source Unity Catalog 0.7 server: Spark (Delta 4.4 with
/// the UC connector) creates them, the kernel reads and writes them through the catalog, and each sees the other's
/// commits.
/// </summary>
[Trait("DeltaKernel", "UnityManaged")]
public class UnityManagedTableTests(UnityCatalogFixture uc) : IClassFixture<UnityCatalogFixture>
{
    private sealed record Row(long id, string s);

    private sealed record WideRow(long id, string s, long n);

    /// <summary>A managed table Spark created in a new catalog, with rows (1, a) and (2, b).</summary>
    private async Task<(string Catalog, string Table)> SparksTableAsync(params string[] more)
    {
        string catalog = await uc.NewCatalogAsync();
        string table = $"{catalog}.s.t";
        LocalSpark.Run(["uc_sql", uc.Url, catalog,
            $"CREATE TABLE {table} (id BIGINT, s STRING) USING delta TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')",
            $"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')", .. more]);
        return (catalog, table);
    }

    /// <summary>Spark's rows for a query, as compact JSON.</summary>
    private string SparkRows(string catalog, string query)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
            LocalSpark.Run("uc_query", uc.Url, catalog, query)[0].RootElement.GetProperty("rows").WriteTo(writer);
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static string Rows(DataFrame df) =>
        string.Join(" ", Enumerable.Range(0, (int)df.Height).Select(i => $"({df["id"][i]}, {df["s"][i]})"));

    /// <summary>What the catalog's Delta API says about the table.</summary>
    private JsonDocument Load(string catalog, string name = "t")
    {
        using var http = new HttpClient();
        return JsonDocument.Parse(http.GetStringAsync($"{uc.Url}/api/2.1/unity-catalog/delta/v1/catalogs/{catalog}/schemas/s/tables/{name}").Result);
    }

    /// <summary>The actions of a table's version 0, from its published log.</summary>
    private JsonElement[] Version0Actions(string catalog, string name)
    {
        using JsonDocument state = Load(catalog, name);
        string location = new Uri(state.RootElement.GetProperty("metadata").GetProperty("location").GetString()!).LocalPath;
        return [.. File.ReadAllLines(Path.Combine(location, "_delta_log", $"{0:D20}.json")).Select(l => JsonDocument.Parse(l).RootElement)];
    }

    /// <summary>The protocol and metaData of a table's version 0, from its published log.</summary>
    private (JsonElement Protocol, JsonElement Metadata) Version0(string catalog, string name)
    {
        using JsonDocument state = Load(catalog, name);
        string location = new Uri(state.RootElement.GetProperty("metadata").GetProperty("location").GetString()!).LocalPath;
        JsonElement[] actions = [.. File.ReadAllLines(Path.Combine(location, "_delta_log", $"{0:D20}.json")).Select(l => JsonDocument.Parse(l).RootElement)];
        return (actions.Single(a => a.TryGetProperty("protocol", out _)).GetProperty("protocol"),
                actions.Single(a => a.TryGetProperty("metaData", out _)).GetProperty("metaData"));
    }

    [SparkFact]
    public async Task SparksTable_IsReadAndWrittenThroughTheCatalog_AndSparkSeesEveryCommit()
    {
        var (catalog, table) = await SparksTableAsync();
        using var client = new UnityCatalog(uc.Url, "");

        using (DataFrame read = client.ScanCatalogTable(catalog, "s", "t").Sort("id").Collect())
            Assert.Equal("(1, a) (2, b)", Rows(read));

        // Append, delete and merge through the catalog.
        using (DataFrame more = DataFrame.From<Row>([new(3, "c")])) more.Lazy().SinkCatalogTable(client, catalog, "s", "t");
        client.DeleteCatalogRecords(catalog, "s", "t", Col("id") == 1);
        using (DataFrame source = DataFrame.From<Row>([new(2, "m"), new(4, "d")]))
            source.Lazy().MergeCatalogRecords(client, catalog, "s", "t", ["id"]).WhenMatchedUpdate().WhenNotMatchedInsert().Execute();

        Assert.Equal("""[{"id":2,"s":"m"},{"id":3,"s":"c"},{"id":4,"s":"d"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));

        // Spark commits after the kernel's commits, and the kernel reads Spark's.
        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {table} VALUES (5, 'e')");
        using (DataFrame read = client.ScanCatalogTable(catalog, "s", "t").Sort("id").Collect())
            Assert.Equal("(2, m) (3, c) (4, d) (5, e)", Rows(read));

        // The catalog ratified every commit (create, Spark's insert, the kernel's three, Spark's insert), and the log is
        // published without a gap.
        using JsonDocument state = Load(catalog);
        long latest = state.RootElement.GetProperty("latest-table-version").GetInt64();
        Assert.Equal(5, latest);
        string location = new Uri(state.RootElement.GetProperty("metadata").GetProperty("location").GetString()!).LocalPath;
        Assert.All(Enumerable.Range(0, (int)latest + 1), v => Assert.True(File.Exists(Path.Combine(location, "_delta_log", $"{v:D20}.json")), $"version {v} published"));
        using JsonDocument history = JsonDocument.Parse(SparkRows(catalog, $"DESCRIBE HISTORY {table}"));
        Assert.Equal(3, history.RootElement.EnumerateArray().Count(r => r.GetProperty("engineInfo").GetString() == "Kaminari.Seseragi.Lakehouse"));
    }

    [SparkFact]
    public async Task ByPath_TheTableIsRefused_AndMaintenanceSparkRefusesIsRefusedThroughTheCatalog()
    {
        var (catalog, _) = await SparksTableAsync();
        using var client = new UnityCatalog(uc.Url, "");
        using JsonDocument state = Load(catalog);
        string location = state.RootElement.GetProperty("metadata").GetProperty("location").GetString()!;

        Assert.Contains("catalog-managed", Assert.ThrowsAny<Exception>(() => LazyFrame.ScanDelta(location).Collect()).Message);
        Assert.Contains("OPTIMIZE", Assert.ThrowsAny<Exception>(() => client.OptimizeCatalogTable(catalog, "s", "t")).Message);
        Assert.Contains("VACUUM", Assert.ThrowsAny<Exception>(() => client.DeltaVacuum(catalog, "s", "t")).Message);
        Assert.Contains("RENAME COLUMN", Assert.ThrowsAny<Exception>(() => client.RenameCatalogColumn(catalog, "s", "t", "s", "x")).Message);
        Assert.Contains("DROP FEATURE", Assert.ThrowsAny<Exception>(() => client.DropFeature(catalog, "s", "t", "deletionVectors")).Message);
        Assert.Contains("REORG", Assert.ThrowsAny<Exception>(() => client.PurgeCatalogTable(catalog, "s", "t")).Message);
        // Nothing was committed.
        using JsonDocument after = Load(catalog);
        Assert.Equal(1, after.RootElement.GetProperty("latest-table-version").GetInt64());
    }

    [SparkFact]
    public async Task SchemaAndClusteringChanges_ReachTheCatalog_AndSpark()
    {
        var (catalog, table) = await SparksTableAsync();
        using var client = new UnityCatalog(uc.Url, "");

        // A new column through schema evolution: the catalog's copy of the columns follows.
        using (DataFrame wider = DataFrame.From<WideRow>([new(3, "c", 30)]))
            wider.Lazy().SinkCatalogTable(client, catalog, "s", "t", canEvolve: true);
        client.ClusterCatalogTable(catalog, "s", "t", ["id"]);
        // A comment and a new place for a column: the catalog's columns follow too.
        client.SetCatalogColumnComment(catalog, "s", "t", "s", "the label");
        client.MoveCatalogColumn(catalog, "s", "t", "n", after: "id");
        client.DropCatalogColumnNotNull(catalog, "s", "t", "n");

        using (HttpClient http = new())
        {
            using JsonDocument info = JsonDocument.Parse(await http.GetStringAsync($"{uc.Url}/api/2.1/unity-catalog/tables/{table}"));
            JsonElement[] columns = [.. info.RootElement.GetProperty("columns").EnumerateArray()];
            Assert.Equal(["id", "n", "s"], columns.Select(c => c.GetProperty("name").GetString()));
            Assert.Equal("the label", columns[2].GetProperty("comment").GetString());
        }
        Assert.Equal("""[{"id":1,"n":null,"s":"a"},{"id":2,"n":null,"s":"b"},{"id":3,"n":30,"s":"c"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
        Assert.Contains("the label", SparkRows(catalog, $"DESCRIBE TABLE {table}"));
        using JsonDocument detail = JsonDocument.Parse(SparkRows(catalog, $"DESCRIBE DETAIL {table}"));
        Assert.Equal("""["id"]""", detail.RootElement[0].GetProperty("clusteringColumns").GetRawText());
    }

    [SparkFact]
    public async Task CreatedHere_TheTableIsCatalogManaged_AsSparksCreate_AndBothWriteIt()
    {
        string catalog = await uc.NewCatalogAsync();
        string table = $"{catalog}.s.t";
        using var client = new UnityCatalog(uc.Url, "");
        using PolarsSchema schema = PolarsSchema.From<Row>(); // borrowed by every call below
        client.CreateCatalogTable(catalog, "s", "t", schema, partitionBy: ["s"], properties: new() { ["delta.enableChangeDataFeed"] = "true" });
        // Spark's CREATE of the same table, to compare with.
        LocalSpark.Run("uc_sql", uc.Url, catalog,
            $"CREATE TABLE {catalog}.s.spark (id BIGINT, s STRING) USING delta PARTITIONED BY (s) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')");

        // The catalog knows it as a catalog-managed table at version 0 (so later operations go through the catalog).
        using (JsonDocument state = Load(catalog))
        {
            Assert.Equal(0, state.RootElement.GetProperty("latest-table-version").GetInt64());
            JsonElement properties = state.RootElement.GetProperty("metadata").GetProperty("properties");
            Assert.Equal("supported", properties.GetProperty("delta.feature.catalogManaged").GetString());
            Assert.Equal(["s"], state.RootElement.GetProperty("metadata").GetProperty("partition-columns").EnumerateArray().Select(c => c.GetString()));
        }
        // Version 0 has Spark's protocol and the same properties (ids and generated names aside).
        var (ours, ourMetadata) = Version0(catalog, "t");
        var (sparks, sparkMetadata) = Version0(catalog, "spark");
        foreach (string list in (string[])["readerFeatures", "writerFeatures"])
            Assert.Equal(sparks.GetProperty(list).EnumerateArray().Select(f => f.GetString()).Order(),
                ours.GetProperty(list).EnumerateArray().Select(f => f.GetString()).Order());
        static string[] Keys(JsonElement metadata) =>
            [.. metadata.GetProperty("configuration").EnumerateObject().Select(p => p.Name).Order()];
        Assert.Equal(Keys(sparkMetadata), Keys(ourMetadata));
        Assert.Equal("name", ourMetadata.GetProperty("configuration").GetProperty("delta.columnMapping.mode").GetString());

        // Spark writes it, the kernel writes it, and each reads the other's rows.
        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')");
        using (DataFrame more = DataFrame.From<Row>([new(3, "c")])) more.Lazy().SinkCatalogTable(client, catalog, "s", "t");
        client.DeleteCatalogRecords(catalog, "s", "t", Col("id") == 1);
        Assert.Equal("""[{"id":2,"s":"b"},{"id":3,"s":"c"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
        using (DataFrame read = client.ScanCatalogTable(catalog, "s", "t").Sort("id").Collect())
            Assert.Equal("(2, b) (3, c)", Rows(read));

        // A name that is taken, or a property contradicting the catalog's, is refused before anything is written.
        Assert.Contains("already exists", Assert.ThrowsAny<Exception>(() => client.CreateCatalogTable(catalog, "s", "t", schema)).Message);
        Assert.Contains("delta.enableDeletionVectors", Assert.ThrowsAny<Exception>(() => client.CreateCatalogTable(
            catalog, "s", "refused", schema, properties: new() { ["delta.enableDeletionVectors"] = "false" })).Message);
        using var http = new HttpClient();
        Assert.Equal(System.Net.HttpStatusCode.NotFound,
            (await http.GetAsync($"{uc.Url}/api/2.1/unity-catalog/delta/v1/catalogs/{catalog}/schemas/s/tables/refused")).StatusCode);
    }

    [SparkFact]
    public async Task CreatedHere_Clustered_WithoutColumnMapping_SparkSeesTheClustering()
    {
        string catalog = await uc.NewCatalogAsync();
        string table = $"{catalog}.s.t";
        using var client = new UnityCatalog(uc.Url, "");
        using PolarsSchema schema = PolarsSchema.From<Row>();
        client.CreateCatalogTable(catalog, "s", "t", schema, clusterBy: ["id"], properties: new() { ["delta.columnMapping.mode"] = "none" });

        var (protocol, metadata) = Version0(catalog, "t");
        Assert.Equal("none", metadata.GetProperty("configuration").GetProperty("delta.columnMapping.mode").GetString());
        // As Spark: the suggested feature stays in the protocol although the property turns its use off.
        Assert.Contains("columnMapping", protocol.GetProperty("writerFeatures").EnumerateArray().Select(f => f.GetString()));

        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {table} VALUES (1, 'a')");
        using (DataFrame more = DataFrame.From<Row>([new(2, "b")])) more.Lazy().SinkCatalogTable(client, catalog, "s", "t");
        Assert.Equal("""[{"id":1,"s":"a"},{"id":2,"s":"b"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
        using JsonDocument detail = JsonDocument.Parse(SparkRows(catalog, $"DESCRIBE DETAIL {table}"));
        Assert.Equal("""["id"]""", detail.RootElement[0].GetProperty("clusteringColumns").GetRawText());
    }

    [SparkFact]
    public async Task CreatedHere_WithAnIdentityColumn_BothGenerateItsValues()
    {
        string catalog = await uc.NewCatalogAsync();
        string table = $"{catalog}.s.t";
        using var client = new UnityCatalog(uc.Url, "");
        using PolarsSchema schema = PolarsSchema.From<Row>();
        client.CreateCatalogTable(catalog, "s", "t", schema, identityColumns: [new DeltaIdentityColumn("id", Start: 100, Step: 10)]);

        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {table} (s) VALUES ('a')");
        using (DataFrame more = DataFrame.FromColumns(new { s = new[] { "b" } })) more.Lazy().SinkCatalogTable(client, catalog, "s", "t");
        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {table} (s) VALUES ('c')");
        Assert.Equal("""[{"id":100,"s":"a"},{"id":110,"s":"b"},{"id":120,"s":"c"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
    }

    [SparkFact]
    public async Task SinkingToAMissingTable_CreatesItWithTheRowsAsVersion0_AsSparksCreateTableAsSelect()
    {
        string catalog = await uc.NewCatalogAsync();
        string table = $"{catalog}.s.t";
        using var client = new UnityCatalog(uc.Url, "");
        using (DataFrame rows = DataFrame.From<Row>([new(1, "a"), new(2, "b")]))
            rows.Lazy().SinkCatalogTable(client, catalog, "s", "t", partitionBy: Polars.CSharp.Polars.Selectors.ByName("s"));
        LocalSpark.Run("uc_sql", uc.Url, catalog,
            $"CREATE TABLE {catalog}.s.spark USING delta PARTITIONED BY (s) AS SELECT CAST(id AS BIGINT) AS id, s FROM VALUES (1, 'a'), (2, 'b') AS v(id, s)");

        // Registered as a catalog-managed table at version 0, which holds the rows, as Spark's.
        using (JsonDocument state = Load(catalog))
        {
            Assert.Equal(0, state.RootElement.GetProperty("latest-table-version").GetInt64());
            Assert.Equal("supported", state.RootElement.GetProperty("metadata").GetProperty("properties").GetProperty("delta.feature.catalogManaged").GetString());
            Assert.Equal(["s"], state.RootElement.GetProperty("metadata").GetProperty("partition-columns").EnumerateArray().Select(c => c.GetString()));
        }
        JsonElement[] ours = Version0Actions(catalog, "t"), sparks = Version0Actions(catalog, "spark");
        static string[] Kinds(JsonElement[] actions) => [.. actions.Select(a => a.EnumerateObject().First().Name).Distinct().Order()];
        Assert.Equal(Kinds(sparks), Kinds(ours));
        Assert.Equal("CREATE TABLE AS SELECT", ours[0].GetProperty("commitInfo").GetProperty("operation").GetString());
        Assert.Contains(ours, a => a.TryGetProperty("domainMetadata", out JsonElement d) && d.GetProperty("domain").GetString() == "delta.rowTracking");
        var (protocol, metadata) = Version0(catalog, "t");
        var (sparkProtocol, sparkMetadata) = Version0(catalog, "spark");
        foreach (string list in (string[])["readerFeatures", "writerFeatures"])
            Assert.Equal(sparkProtocol.GetProperty(list).EnumerateArray().Select(f => f.GetString()).Order(),
                protocol.GetProperty(list).EnumerateArray().Select(f => f.GetString()).Order());
        static string[] Keys(JsonElement m) => [.. m.GetProperty("configuration").EnumerateObject().Select(p => p.Name).Order()];
        Assert.Equal(Keys(sparkMetadata), Keys(metadata));
        // Its data files are where the catalog's suggested properties put them (random prefixes).
        Assert.All(ours.Where(a => a.TryGetProperty("add", out _)), a =>
            Assert.Matches("^[0-9A-Za-z]{2}/part-", a.GetProperty("add").GetProperty("path").GetString()));

        // Then a table like any other: the kernel appends through the catalog, Spark inserts, both read it.
        using (DataFrame more = DataFrame.From<Row>([new(3, "c")])) more.Lazy().SinkCatalogTable(client, catalog, "s", "t");
        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {table} VALUES (4, 'd')");
        Assert.Equal("""[{"id":1,"s":"a"},{"id":2,"s":"b"},{"id":3,"s":"c"},{"id":4,"s":"d"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
        using (DataFrame read = client.ScanCatalogTable(catalog, "s", "t").Sort("id").Collect())
            Assert.Equal("(1, a) (2, b) (3, c) (4, d)", Rows(read));
        using JsonDocument after = Load(catalog);
        Assert.Equal(2, after.RootElement.GetProperty("latest-table-version").GetInt64());
    }

    [Theory]
    [InlineData(DeltaSaveMode.Append)]
    [InlineData(DeltaSaveMode.Overwrite)]
    [InlineData(DeltaSaveMode.ErrorIfExists)]
    [InlineData(DeltaSaveMode.Ignore)]
    public async Task SinkingToAMissingTable_CreatesIt_WhateverTheMode(DeltaSaveMode mode)
    {
        string catalog = await uc.NewCatalogAsync();
        using var client = new UnityCatalog(uc.Url, "");
        using (DataFrame rows = DataFrame.From<Row>([new(1, "a"), new(2, "b")]))
            rows.Lazy().SinkCatalogTable(client, catalog, "s", "t", mode: mode);

        using JsonDocument state = Load(catalog);
        Assert.Equal(0, state.RootElement.GetProperty("latest-table-version").GetInt64());
        using DataFrame read = client.ScanCatalogTable(catalog, "s", "t").Sort("id").Collect();
        Assert.Equal("(1, a) (2, b)", Rows(read));
        // The operation Spark's saveAsTable records in each mode (delta-spark 4.4 with the UC connector 0.7).
        Assert.Equal(mode == DeltaSaveMode.Overwrite ? "CREATE OR REPLACE TABLE AS SELECT" : "CREATE TABLE AS SELECT",
            Version0Actions(catalog, "t")[0].GetProperty("commitInfo").GetProperty("operation").GetString());
    }

    [SparkFact]
    public async Task SinkingToAMissingTable_WithClusterBy_SparkSeesTheClustering()
    {
        string catalog = await uc.NewCatalogAsync();
        string table = $"{catalog}.s.t";
        using var client = new UnityCatalog(uc.Url, "");
        using (DataFrame rows = DataFrame.From<Row>([new(1, "a"), new(2, "b")]))
            rows.Lazy().SinkCatalogTable(client, catalog, "s", "t", clusterBy: ["id"]);

        Assert.Contains(Version0Actions(catalog, "t"), a => a.TryGetProperty("domainMetadata", out JsonElement d) && d.GetProperty("domain").GetString() == "delta.clustering");
        using JsonDocument detail = JsonDocument.Parse(SparkRows(catalog, $"DESCRIBE DETAIL {table}"));
        Assert.Equal("""["id"]""", detail.RootElement[0].GetProperty("clusteringColumns").GetRawText());
        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {table} VALUES (3, 'c')");
        Assert.Equal("""[{"id":1,"s":"a"},{"id":2,"s":"b"},{"id":3,"s":"c"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
    }

    [SparkFact]
    public async Task Updates_ByPredicate_AndByMatchedRows_CommitThroughTheCatalog()
    {
        var (catalog, table) = await SparksTableAsync();
        using var client = new UnityCatalog(uc.Url, "");

        client.UpdateCatalogRecords(catalog, "s", "t", Col("id") == 1, new Dictionary<string, Expr> { ["s"] = Lit("A") });
        using (DataFrame changes = DataFrame.From<Row>([new(2, "B"), new(9, "never inserted")]))
            changes.UpdateCatalogRecords(client, catalog, "s", "t", ["id"]);

        Assert.Equal("""[{"id":1,"s":"A"},{"id":2,"s":"B"}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
        using JsonDocument state = Load(catalog);
        Assert.Equal(3, state.RootElement.GetProperty("latest-table-version").GetInt64()); // create, insert, two updates
        using JsonDocument history = JsonDocument.Parse(SparkRows(catalog, $"DESCRIBE HISTORY {table}"));
        Assert.Equal(["MERGE", "UPDATE"], history.RootElement.EnumerateArray().Take(2).Select(r => r.GetProperty("operation").GetString()));
    }
}
