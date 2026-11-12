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
    private JsonDocument Load(string catalog)
    {
        using var http = new HttpClient();
        return JsonDocument.Parse(http.GetStringAsync($"{uc.Url}/api/2.1/unity-catalog/delta/v1/catalogs/{catalog}/schemas/s/tables/t").Result);
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

        using (HttpClient http = new())
        {
            using JsonDocument info = JsonDocument.Parse(await http.GetStringAsync($"{uc.Url}/api/2.1/unity-catalog/tables/{table}"));
            Assert.Equal(["id", "s", "n"], info.RootElement.GetProperty("columns").EnumerateArray().Select(c => c.GetProperty("name").GetString()));
        }
        Assert.Equal("""[{"id":1,"s":"a","n":null},{"id":2,"s":"b","n":null},{"id":3,"s":"c","n":30}]""", SparkRows(catalog, $"SELECT * FROM {table} ORDER BY id"));
        using JsonDocument detail = JsonDocument.Parse(SparkRows(catalog, $"DESCRIBE DETAIL {table}"));
        Assert.Equal("""["id"]""", detail.RootElement[0].GetProperty("clusteringColumns").GetRawText());
    }
}
