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
    public async Task LinqQueries_OverACatalogTableScan_LeaveOutTheFilesTheFilterRulesOut()
    {
        // Three inserts: the files of the middle one (ids 11..13) are then damaged.
        string catalog = await uc.NewCatalogAsync();
        string table = $"{catalog}.s.t";
        LocalSpark.Run(["uc_sql", uc.Url, catalog,
            $"CREATE TABLE {table} (id BIGINT, s STRING) USING delta TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')"]);
        string location;
        using (JsonDocument state = Load(catalog))
            location = new Uri(state.RootElement.GetProperty("metadata").GetProperty("location").GetString()!).LocalPath;
        string[] DataFiles() => Directory.GetFiles(location, "*.parquet", SearchOption.AllDirectories);
        string[] middle = [];
        foreach (int first in (int[])[1, 11, 21])
        {
            string[] before = DataFiles();
            LocalSpark.Run(["uc_sql", uc.Url, catalog, $"INSERT INTO {table} VALUES ({first}, 'x'), ({first + 1}, 'y'), ({first + 2}, 'z')"]);
            if (first == 11) middle = [.. DataFiles().Except(before)];
        }
        Assert.NotEmpty(middle);
        foreach (string file in middle) File.WriteAllBytes(file, "not a parquet file"u8.ToArray());

        using var client = new UnityCatalog(uc.Url, "");
        using LazyFrame lf = client.ScanCatalogTable(catalog, "s", "t");
        var rows = Polars.CSharp.Linq.LinqExtensions.AsQueryable<Row>(lf);
        Assert.Equal([21L, 22L, 23L], rows.Where(r => r.id > 20).Select(r => r.id).ToList().Order());
        Assert.Equal([1L, 2L, 3L], rows.Where(r => r.id < 5).Select(r => r.id).ToList().Order());
        Assert.ThrowsAny<Exception>(() => rows.Where(r => r.id == 12).ToList());
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

    // ---- REPLACE TABLE (Spark: managed, catalog-managed tables only) ----

    private sealed record ReplacedRow(long id, int? w);

    /// <summary>The actions of a table's version, from its published log.</summary>
    private JsonElement[] VersionActions(string catalog, string name, long version)
    {
        using JsonDocument state = Load(catalog, name);
        string location = new Uri(state.RootElement.GetProperty("metadata").GetProperty("location").GetString()!).LocalPath;
        return [.. File.ReadAllLines(Path.Combine(location, "_delta_log", $"{version:D20}.json")).Select(l => JsonDocument.Parse(l).RootElement.Clone())];
    }

    private static JsonElement Single(JsonElement[] actions, string kind) => actions.Single(a => a.TryGetProperty(kind, out _)).GetProperty(kind);

    /// <summary>A table's properties, with the values that differ table by table (generated names and ids) left out.</summary>
    private static string Comparable(JsonElement configuration) => string.Join(",", configuration.EnumerateObject()
        .Select(p => p.Name is "io.unitycatalog.tableId" || p.Name.StartsWith("delta.rowTracking.materialized", StringComparison.Ordinal)
            ? p.Name : $"{p.Name}={p.Value.GetString()}")
        .Order(StringComparer.Ordinal));

    private static string Features(JsonElement protocol) =>
        string.Join(",", protocol.GetProperty("writerFeatures").EnumerateArray().Select(f => f.GetString()).Order(StringComparer.Ordinal));

    [SparkFact]
    public async Task Replaced_TheTableKeepsWhatSparkKeeps_AndTheCatalogAndSparkSeeTheNewDefinition()
    {
        // Two equal tables Spark made: Spark replaces one, the kernel the other, with the same definition.
        string catalog = await uc.NewCatalogAsync();
        string[] tables = ["sp", "ours"];
        LocalSpark.Run(["uc_sql", uc.Url, catalog, .. tables.SelectMany(t => (string[])[
            $"CREATE TABLE {catalog}.s.{t} (id BIGINT, s STRING) USING delta TBLPROPERTIES ('foo' = 'bar', 'delta.checkpointInterval' = '7', 'delta.appendOnly' = 'false')",
            $"INSERT INTO {catalog}.s.{t} VALUES (1, 'a'), (2, 'b')"]),
            $"REPLACE TABLE {catalog}.s.sp (id BIGINT, w INT) USING delta TBLPROPERTIES ('k2' = 'v2')"]);
        using var client = new UnityCatalog(uc.Url, "");
        client.ReplaceCatalogTable(catalog, "s", "ours", PolarsSchema.From<ReplacedRow>(), properties: new() { ["k2"] = "v2" });

        JsonElement[] spark = VersionActions(catalog, "sp", 2), ours = VersionActions(catalog, "ours", 2);
        Assert.Equal("REPLACE TABLE", Single(ours, "commitInfo").GetProperty("operation").GetString());
        Assert.Equal(2, ours.Count(a => a.TryGetProperty("remove", out _)));
        JsonElement metadata = Single(ours, "metaData");
        // Every delta.* property kept (foo dropped), as Spark; the table id too.
        Assert.Equal(Comparable(Single(spark, "metaData").GetProperty("configuration")), Comparable(metadata.GetProperty("configuration")));
        Assert.Equal(Single(VersionActions(catalog, "ours", 0), "metaData").GetProperty("id").GetString(), metadata.GetProperty("id").GetString());
        // Under column mapping, id keeps its column id and w takes the next one, as Spark numbers them.
        string Ids(JsonElement m) => string.Join(",", JsonDocument.Parse(m.GetProperty("schemaString").GetString()!).RootElement.GetProperty("fields")
            .EnumerateArray().Select(f => $"{f.GetProperty("name").GetString()}:{f.GetProperty("metadata").GetProperty("delta.columnMapping.id").GetInt32()}"));
        Assert.Equal(Ids(Single(spark, "metaData")), Ids(metadata));
        Assert.Equal("id:1,w:3", Ids(metadata));

        // The catalog has the new columns and properties.
        using (JsonDocument state = Load(catalog, "ours"))
        {
            JsonElement m = state.RootElement.GetProperty("metadata");
            Assert.Equal(["id", "w"], m.GetProperty("columns").GetProperty("fields").EnumerateArray().Select(f => f.GetProperty("name").GetString()));
            Assert.Equal("v2", m.GetProperty("properties").GetProperty("k2").GetString());
            Assert.False(m.GetProperty("properties").TryGetProperty("foo", out _));
            Assert.Equal(2, state.RootElement.GetProperty("latest-table-version").GetInt64());
        }
        // Spark writes and reads it; time travel reads the old definition.
        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {catalog}.s.ours VALUES (5, 50)");
        Assert.Equal("""[{"id":5,"w":50}]""", SparkRows(catalog, $"SELECT * FROM {catalog}.s.ours"));
        using (DataFrame read = client.ScanCatalogTable(catalog, "s", "ours").Collect())
            Assert.Equal((1L, 5L, 50), (read.Height, (long)read["id"][0]!, (int)read["w"][0]!));
        Assert.Equal("""[{"id":1,"s":"a"},{"id":2,"s":"b"}]""", SparkRows(catalog, $"SELECT * FROM {catalog}.s.ours VERSION AS OF 1 ORDER BY id"));
    }

    [SparkFact]
    public async Task Replace_IsRefusedWhereSparkRefusesIt_OrWhereItWouldBreakTheTable_AndOrCreateCreates()
    {
        string catalog = await uc.NewCatalogAsync();
        string external = Path.Combine(uc.Storage, $"ext_{Guid.NewGuid():N}");
        LocalSpark.Run(["uc_sql", uc.Url, catalog,
            $"CREATE TABLE {catalog}.s.t (id BIGINT, s STRING) USING delta TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            $"INSERT INTO {catalog}.s.t VALUES (1, 'a')",
            $"ALTER TABLE {catalog}.s.t ADD CONSTRAINT pos CHECK (s <> 'x')",
            $"CREATE TABLE {catalog}.s.e (id BIGINT) USING delta LOCATION 'file://{external}'"]);
        using var client = new UnityCatalog(uc.Url, "");
        string Refusal(Action action) => Assert.ThrowsAny<Exception>(action).Message;
        PolarsSchema replaced = PolarsSchema.From<ReplacedRow>(), same = PolarsSchema.From<Row>();

        // As Spark: an external table, and a missing one without orCreate.
        Assert.Contains("Only a managed, catalog-managed table can be replaced", Refusal(() => client.ReplaceCatalogTable(catalog, "s", "e", replaced)));
        Assert.Contains($"The catalog table {catalog}.s.none does not exist; use orCreate", Refusal(() => client.ReplaceCatalogTable(catalog, "s", "none", replaced)));
        // A kept constraint the new schema cannot hold (Spark keeps it, and every insert then fails).
        Assert.Contains("CHECK constraint 'pos' (s <> 'x')", Refusal(() => client.ReplaceCatalogTable(catalog, "s", "t", replaced)));
        client.DropCatalogConstraint(catalog, "s", "t", "pos");
        // As Spark: the change data feed (kept), column mapping and rows, and a column dropped.
        Assert.Contains("change data feed (CDF)", Refusal(() => client.ReplaceCatalogTable(catalog, "s", "t", replaced)));
        // What the catalog requires of a managed table, by any operation.
        Assert.Contains("'delta.enableDeletionVectors'='true'", Refusal(() => client.ReplaceCatalogTable(
            catalog, "s", "t", same, properties: new() { ["delta.enableDeletionVectors"] = "false" })));
        Assert.Contains("'delta.enableDeletionVectors'='true'", Refusal(() => client.UnsetCatalogTableProperties(catalog, "s", "t", ["delta.enableDeletionVectors"])));
        using (JsonDocument state = Load(catalog))
            Assert.Equal(3, state.RootElement.GetProperty("latest-table-version").GetInt64());

        // The same columns replace it (the change data feed stays on); orCreate names the operation.
        client.ReplaceCatalogTable(catalog, "s", "t", same, orCreate: true);
        JsonElement[] actions = VersionActions(catalog, "t", 4);
        Assert.Equal("CREATE OR REPLACE TABLE", Single(actions, "commitInfo").GetProperty("operation").GetString());
        Assert.Equal("true", Single(actions, "metaData").GetProperty("configuration").GetProperty("delta.enableChangeDataFeed").GetString());

        // A missing table, with orCreate: created managed, as Spark's CREATE OR REPLACE TABLE.
        client.ReplaceCatalogTable(catalog, "s", "n", replaced, orCreate: true);
        Assert.Equal("CREATE OR REPLACE TABLE", Single(Version0Actions(catalog, "n"), "commitInfo").GetProperty("operation").GetString());
        LocalSpark.Run("uc_sql", uc.Url, catalog, $"INSERT INTO {catalog}.s.n VALUES (7, 70)");
        Assert.Equal("""[{"id":7,"w":70}]""", SparkRows(catalog, $"SELECT * FROM {catalog}.s.n"));
    }
}
