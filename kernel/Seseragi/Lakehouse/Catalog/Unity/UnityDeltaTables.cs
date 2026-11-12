using System.Net;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Kaminari.Seseragi.Lakehouse.Catalog.Unity;

/// <summary>A Delta table as Unity Catalog's Delta API (<c>/delta/v1</c>) describes it.</summary>
/// <param name="TableUuid">The table's id in the catalog (also its <c>io.unitycatalog.tableId</c> property).</param>
/// <param name="LatestVersion">The table's latest version.</param>
/// <param name="Commits">The ratified commits the catalog holds.</param>
public sealed record UnityDeltaTable(string TableUuid, long LatestVersion, IReadOnlyList<RatifiedCommit> Commits);

public sealed partial class UnityCatalogClient
{
    // ------------------------------------------------------------------
    // Delta API (catalog-managed tables)
    // ------------------------------------------------------------------

    /// <summary>Loads a Delta table through the Delta API: its id, latest version and the commits the catalog holds.</summary>
    public UnityDeltaTable LoadDeltaTable(string catalogName, string schemaName, string tableName)
    {
        byte[] bytes = SendCore(HttpMethod.Get, DeltaTablePath(catalogName, schemaName, tableName), null, false, default).GetAwaiter().GetResult();
        using JsonDocument doc = Parse(bytes);
        return ReadDeltaTable(doc.RootElement);
    }

    /// <summary>Sends a Delta API table update (<c>requirements</c> and <c>updates</c>, as JSON).</summary>
    /// <exception cref="CatalogException">The catalog refused it (with its status code) or could not be reached.</exception>
    public void UpdateDeltaTable(string catalogName, string schemaName, string tableName, byte[] update) =>
        SendCore(HttpMethod.Post, DeltaTablePath(catalogName, schemaName, tableName), update, false, default).GetAwaiter().GetResult();

    private static string DeltaTablePath(string catalogName, string schemaName, string tableName) =>
        $"/delta/v1/catalogs/{Escape(catalogName)}/schemas/{Escape(schemaName)}/tables/{Escape(tableName)}";

    private static UnityDeltaTable ReadDeltaTable(JsonElement root)
    {
        JsonElement metadata = root.TryGetProperty("metadata", out JsonElement m) && m.ValueKind == JsonValueKind.Object
            ? m
            : throw new CatalogException("The catalog response has no 'metadata' object.");
        long latest = root.TryGetProperty("latest-table-version", out JsonElement v) && v.TryGetInt64(out long version)
            ? version
            : throw new CatalogException("The catalog response has no 'latest-table-version'.");
        List<RatifiedCommit> commits = [];
        if (root.TryGetProperty("commits", out JsonElement list) && list.ValueKind == JsonValueKind.Array)
        {
            foreach (JsonElement c in list.EnumerateArray())
            {
                commits.Add(new RatifiedCommit(
                    Long(c, "version"), Required(c, "file-name"), Long(c, "file-size"),
                    DateTimeOffset.FromUnixTimeMilliseconds(Long(c, "file-modification-timestamp"))));
            }
        }
        return new UnityDeltaTable(Required(metadata, "table-uuid"), latest, commits);

        static long Long(JsonElement obj, string name) =>
            obj.TryGetProperty(name, out JsonElement value) && value.TryGetInt64(out long n)
                ? n
                : throw new CatalogException($"The catalog response has no '{name}' number.");
    }
}

/// <summary>
/// Unity Catalog as the managing catalog of one catalog-managed Delta table: reads its commits and ratifies new ones
/// through the Delta API, as Delta Spark does.
/// </summary>
/// <param name="client">The catalog client.</param>
/// <param name="catalogName">The table's catalog.</param>
/// <param name="schemaName">The table's schema.</param>
/// <param name="tableName">The table's name.</param>
public sealed class UnityCatalogCommitter(UnityCatalogClient client, string catalogName, string schemaName, string tableName) : ICatalogCommitter
{
    /// <summary>The domains whose configuration the catalog keeps a copy of (Spark sends these).</summary>
    private const string ClusteringDomain = "delta.clustering";

    private string? _tableUuid;

    /// <inheritdoc/>
    public CatalogLog GetLog()
    {
        UnityDeltaTable table = Load();
        return new CatalogLog(table.LatestVersion, table.Commits);
    }

    private UnityDeltaTable Load()
    {
        UnityDeltaTable table = client.LoadDeltaTable(catalogName, schemaName, tableName);
        _tableUuid = table.TableUuid;
        return table;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A request whose answer is lost (a broken connection, a server error) is sent again as it was: the catalog takes
    /// the same commit twice as once. When that does not settle it either, the outcome is unknown.
    /// </remarks>
    public CatalogCommitOutcome Ratify(CatalogCommit commit)
    {
        ArgumentNullException.ThrowIfNull(commit);
        byte[] request = UpdateRequest(_tableUuid ?? Load().TableUuid, commit);
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                client.UpdateDeltaTable(catalogName, schemaName, tableName, request);
                return CatalogCommitOutcome.Ratified;
            }
            catch (CatalogException e) when (e.StatusCode == HttpStatusCode.Conflict)
            {
                return CatalogCommitOutcome.Conflict;
            }
            catch (CatalogException e) when (attempt == 0 && e.StatusCode is { } status && (int)status is >= 400 and < 500)
            {
                throw new CatalogCommitRejectedException(
                    $"Unity Catalog refused the commit of version {commit.Version} of {catalogName}.{schemaName}.{tableName}: {e.Message}", e);
            }
            catch (CatalogException e) when (attempt == 0)
            {
                System.Diagnostics.Trace.TraceWarning($"The commit of version {commit.Version} got no answer ({e.Message}); sending it again.");
            }
            catch (CatalogException e)
            {
                throw new CatalogCommitOutcomeUnknownException(
                    $"Whether Unity Catalog took the commit of version {commit.Version} of {catalogName}.{schemaName}.{tableName} is not known: {e.Message}", e);
            }
        }
    }

    /// <summary>
    /// The Delta API update that ratifies the commit: the commit, how far the log is published, and what the commit
    /// changes of what the catalog keeps (properties, columns, partitioning, comment, protocol, clustering).
    /// </summary>
    internal static byte[] UpdateRequest(string tableUuid, CatalogCommit commit)
    {
        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartObject();
            w.WriteStartArray("requirements");
            w.WriteStartObject();
            w.WriteString("type", "assert-table-uuid");
            w.WriteString("uuid", tableUuid);
            w.WriteEndObject();
            w.WriteEndArray();

            w.WriteStartArray("updates");
            w.WriteStartObject();
            w.WriteString("action", "add-commit");
            w.WriteStartObject("commit");
            w.WriteNumber("version", commit.Version);
            w.WriteNumber("timestamp", commit.Timestamp);
            w.WriteString("file-name", commit.FileName);
            w.WriteNumber("file-size", commit.Size);
            w.WriteNumber("file-modification-timestamp", commit.ModificationTime.ToUnixTimeMilliseconds());
            w.WriteEndObject();
            w.WriteEndObject();

            if (commit.LatestPublishedVersion >= 0)
            {
                w.WriteStartObject();
                w.WriteString("action", "set-latest-backfilled-version");
                w.WriteNumber("latest-published-version", commit.LatestPublishedVersion);
                w.WriteEndObject();
            }
            if (commit.Metadata is { } metadata) WriteMetadataChanges(w, commit.ReadMetadata, metadata);
            if (commit.Protocol is { } protocol && !SameProtocol(commit.ReadProtocol, protocol)) WriteProtocol(w, protocol);
            WriteDomainChanges(w, commit.Domains);
            w.WriteEndArray();
            w.WriteEndObject();
        }
        return stream.ToArray();
    }

    private static void WriteMetadataChanges(Utf8JsonWriter w, MetadataAction before, MetadataAction after)
    {
        List<KeyValuePair<string, string>> set = [.. after.Configuration.Where(p => !before.Configuration.TryGetValue(p.Key, out string? old) || old != p.Value)];
        if (set.Count > 0)
        {
            w.WriteStartObject();
            w.WriteString("action", "set-properties");
            w.WriteStartObject("updates");
            foreach (var (key, value) in set) w.WriteString(key, value);
            w.WriteEndObject();
            w.WriteEndObject();
        }
        List<string> removed = [.. before.Configuration.Keys.Where(k => !after.Configuration.ContainsKey(k))];
        if (removed.Count > 0)
        {
            w.WriteStartObject();
            w.WriteString("action", "remove-properties");
            w.WriteStartArray("removals");
            foreach (string key in removed) w.WriteStringValue(key);
            w.WriteEndArray();
            w.WriteEndObject();
        }
        if (after.SchemaString != before.SchemaString)
        {
            w.WriteStartObject();
            w.WriteString("action", "set-columns");
            w.WritePropertyName("columns");
            w.WriteRawValue(after.SchemaString);
            w.WriteEndObject();
        }
        if (!after.PartitionColumns.SequenceEqual(before.PartitionColumns, StringComparer.Ordinal))
        {
            w.WriteStartObject();
            w.WriteString("action", "set-partition-columns");
            w.WriteStartArray("partition-columns");
            foreach (string column in after.PartitionColumns) w.WriteStringValue(column);
            w.WriteEndArray();
            w.WriteEndObject();
        }
        if (after.Description != before.Description && after.Description is not null)
        {
            w.WriteStartObject();
            w.WriteString("action", "set-table-comment");
            w.WriteString("comment", after.Description);
            w.WriteEndObject();
        }
    }

    private static bool SameProtocol(ProtocolAction a, ProtocolAction b) =>
        a.MinReaderVersion == b.MinReaderVersion && a.MinWriterVersion == b.MinWriterVersion
        && (a.ReaderFeatures ?? []).Order(StringComparer.Ordinal).SequenceEqual((b.ReaderFeatures ?? []).Order(StringComparer.Ordinal), StringComparer.Ordinal)
        && (a.WriterFeatures ?? []).Order(StringComparer.Ordinal).SequenceEqual((b.WriterFeatures ?? []).Order(StringComparer.Ordinal), StringComparer.Ordinal);

    private static void WriteProtocol(Utf8JsonWriter w, ProtocolAction protocol)
    {
        w.WriteStartObject();
        w.WriteString("action", "set-protocol");
        w.WriteStartObject("protocol");
        w.WriteNumber("min-reader-version", protocol.MinReaderVersion);
        w.WriteNumber("min-writer-version", protocol.MinWriterVersion);
        w.WriteStartArray("reader-features");
        foreach (string feature in protocol.ReaderFeatures ?? []) w.WriteStringValue(feature);
        w.WriteEndArray();
        w.WriteStartArray("writer-features");
        foreach (string feature in protocol.WriterFeatures ?? []) w.WriteStringValue(feature);
        w.WriteEndArray();
        w.WriteEndObject();
        w.WriteEndObject();
    }

    /// <summary>The clustering domain, as Spark sends it: its configuration as an object, without the domain name.</summary>
    private static void WriteDomainChanges(Utf8JsonWriter w, IReadOnlyList<DomainMetadataAction> domains)
    {
        DomainMetadataAction? clustering = domains.LastOrDefault(d => d.Domain == ClusteringDomain);
        if (clustering is null) return;
        w.WriteStartObject();
        if (clustering.Removed)
        {
            w.WriteString("action", "remove-domain-metadata");
            w.WriteStartArray("domains");
            w.WriteStringValue(ClusteringDomain);
            w.WriteEndArray();
        }
        else
        {
            w.WriteString("action", "set-domain-metadata");
            w.WriteStartObject("updates");
            w.WriteStartObject(ClusteringDomain);
            using JsonDocument config = JsonDocument.Parse(clustering.Configuration);
            foreach (JsonProperty property in config.RootElement.EnumerateObject())
            {
                if (property.Name != "domainName") property.WriteTo(w);
            }
            w.WriteEndObject();
            w.WriteEndObject();
        }
        w.WriteEndObject();
    }
}
