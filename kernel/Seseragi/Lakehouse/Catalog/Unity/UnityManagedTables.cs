using System.Net;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Kaminari.Seseragi.Lakehouse.Catalog.Unity;

/// <summary>
/// A managed table the catalog staged (Delta API <c>staging-tables</c>): where to write its version 0, with what, and
/// what the catalog asks of that version. The table exists for the catalog once it is created
/// (<see cref="UnityCatalogClient.CreateDeltaTable"/>) from the version 0 written at <see cref="Location"/>.
/// </summary>
/// <param name="TableId">The table's id (also its <c>io.unitycatalog.tableId</c> property).</param>
/// <param name="Location">Where the table lives.</param>
/// <param name="Credentials">Credentials for writing version 0 there.</param>
/// <param name="RequiredProtocol">The protocol version 0 must have at least.</param>
/// <param name="SuggestedReaderFeatures">Reader features the catalog suggests.</param>
/// <param name="SuggestedWriterFeatures">Writer features the catalog suggests.</param>
/// <param name="RequiredProperties">Properties version 0 must set (a null value: any value).</param>
/// <param name="SuggestedProperties">Properties the catalog suggests (a null value: the writer picks it).</param>
public sealed record UnityStagingTable(
    string TableId,
    string Location,
    TemporaryCredentials Credentials,
    ProtocolAction RequiredProtocol,
    IReadOnlyList<string> SuggestedReaderFeatures,
    IReadOnlyList<string> SuggestedWriterFeatures,
    IReadOnlyDictionary<string, string?> RequiredProperties,
    IReadOnlyDictionary<string, string?> SuggestedProperties)
{
    /// <summary>
    /// A suggestion Delta Spark does not take: the property is not one Delta defines, and the files keep the writer's
    /// codec (Spark's files stay Snappy).
    /// </summary>
    private const string CompressionCodecKey = "delta.parquet.compression.codec";

    /// <summary>
    /// The properties of version 0, as Delta Spark sets them: the suggested ones (those with a value), the caller's
    /// over them, then the required ones.
    /// </summary>
    /// <exception cref="CatalogException">A property of the caller contradicts a required one.</exception>
    public Dictionary<string, string> TableProperties(IReadOnlyDictionary<string, string>? properties)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in SuggestedProperties)
        {
            if (value is not null && key != CompressionCodecKey) result[key] = value;
        }
        foreach (var (key, value) in properties ?? EmptyMap.Instance) result[key] = value;
        foreach (var (key, value) in RequiredProperties)
        {
            if (value is null) continue;
            if (properties is not null && properties.TryGetValue(key, out string? given) && given != value)
                throw new CatalogException(
                    $"The catalog requires the table property '{key}'='{value}', but '{key}'='{given}' was given. Leave it out.");
            result[key] = value;
        }
        return result;
    }

    /// <summary>
    /// The features version 0 needs on top of what its schema and properties bring: the required ones, and the
    /// suggested ones this kernel writes (Delta Spark lists the suggested features even when a property turns their
    /// use off, such as column mapping mode none).
    /// </summary>
    /// <exception cref="CatalogException">The catalog requires a feature this kernel does not write.</exception>
    public IReadOnlyList<string> Features()
    {
        string[] required = [.. TableFeatures.ReaderFeaturesOf(RequiredProtocol), .. TableFeatures.WriterFeaturesOf(RequiredProtocol)];
        string[] unsupported = [.. required.Distinct().Where(f => !TableFeatures.SupportedWriterFeatures.Contains(f))];
        if (unsupported.Length > 0)
            throw new CatalogException($"The catalog requires table features this kernel does not write: {string.Join(", ", unsupported)}.");
        IEnumerable<string> suggested = SuggestedReaderFeatures.Concat(SuggestedWriterFeatures).Where(TableFeatures.SupportedWriterFeatures.Contains);
        return [.. required.Concat(suggested).Distinct(StringComparer.Ordinal)];
    }
}

public sealed partial class UnityCatalogClient
{
    /// <summary>Whether the Delta API knows the table.</summary>
    /// <exception cref="CatalogException">The catalog could not answer.</exception>
    public bool DeltaTableExists(string catalogName, string schemaName, string tableName)
    {
        try
        {
            SendCore(HttpMethod.Get, DeltaTablePath(catalogName, schemaName, tableName), null, false, default).GetAwaiter().GetResult();
            return true;
        }
        catch (CatalogException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    /// <summary>Stages a managed table (Delta API <c>staging-tables</c>): its id, location and what version 0 needs.</summary>
    /// <exception cref="CatalogException">The catalog refused it or could not be reached.</exception>
    public UnityStagingTable CreateStagingTable(string catalogName, string schemaName, string tableName)
    {
        byte[] body = WriteJson(w => w.WriteString("name", tableName));
        byte[] bytes = SendCore(HttpMethod.Post, $"{DeltaSchemaPath(catalogName, schemaName)}/staging-tables", body, false, default)
            .GetAwaiter().GetResult();
        using JsonDocument doc = Parse(bytes);
        return ReadStagingTable(doc.RootElement);
    }

    /// <summary>Creates (registers) a managed table whose version 0 was written at its staged location.</summary>
    /// <param name="catalogName">The table's catalog.</param>
    /// <param name="schemaName">The table's schema.</param>
    /// <param name="request">The request, from <see cref="UnityManagedTables.CreateRequest"/>.</param>
    /// <exception cref="CatalogException">The catalog refused it (with its status code) or could not be reached.</exception>
    public void CreateDeltaTable(string catalogName, string schemaName, byte[] request) =>
        SendCore(HttpMethod.Post, $"{DeltaSchemaPath(catalogName, schemaName)}/tables", request, false, default).GetAwaiter().GetResult();

    private static string DeltaSchemaPath(string catalogName, string schemaName) =>
        $"/delta/v1/catalogs/{Escape(catalogName)}/schemas/{Escape(schemaName)}";

    internal static UnityStagingTable ReadStagingTable(JsonElement root)
    {
        string location = Required(root, "location");
        JsonElement protocol = root.TryGetProperty("required-protocol", out JsonElement p) && p.ValueKind == JsonValueKind.Object
            ? p
            : throw new CatalogException("The catalog response has no 'required-protocol' object.");
        JsonElement suggested = root.TryGetProperty("suggested-protocol", out JsonElement s) && s.ValueKind == JsonValueKind.Object ? s : default;
        return new UnityStagingTable(
            Required(root, "table-id"),
            location,
            StagingCredentials(root, location),
            new ProtocolAction
            {
                MinReaderVersion = Int(protocol, "min-reader-version") ?? 1,
                MinWriterVersion = Int(protocol, "min-writer-version") ?? 2,
                ReaderFeatures = Strings(protocol, "reader-features"),
                WriterFeatures = Strings(protocol, "writer-features"),
            },
            suggested.ValueKind == JsonValueKind.Object ? Strings(suggested, "reader-features") : [],
            suggested.ValueKind == JsonValueKind.Object ? Strings(suggested, "writer-features") : [],
            NullableMap(root, "required-properties"),
            NullableMap(root, "suggested-properties"));

        static string[] Strings(JsonElement obj, string name) =>
            obj.TryGetProperty(name, out JsonElement array) && array.ValueKind == JsonValueKind.Array
                ? [.. array.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!)]
                : [];

        static Dictionary<string, string?> NullableMap(JsonElement obj, string name)
        {
            var map = new Dictionary<string, string?>(StringComparer.Ordinal);
            if (obj.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty property in value.EnumerateObject())
                    map[property.Name] = property.Value.ValueKind == JsonValueKind.String ? property.Value.GetString() : null;
            }
            return map;
        }
    }

    /// <summary>
    /// The Delta API's credentials for <paramref name="location"/>: of those whose prefix covers it, the one with the
    /// longest prefix (as the API asks); none on local storage, where the catalog vends an empty configuration.
    /// </summary>
    private static TemporaryCredentials StagingCredentials(JsonElement root, string location)
    {
        JsonElement? best = null;
        int bestLength = -1;
        if (root.TryGetProperty("storage-credentials", out JsonElement list) && list.ValueKind == JsonValueKind.Array)
        {
            foreach (JsonElement credential in list.EnumerateArray())
            {
                string prefix = Str(credential, "prefix") ?? "";
                if (location.StartsWith(prefix, StringComparison.Ordinal) && prefix.Length > bestLength)
                {
                    best = credential;
                    bestLength = prefix.Length;
                }
            }
        }
        if (best is not { } chosen || !chosen.TryGetProperty("config", out JsonElement config) || config.ValueKind != JsonValueKind.Object)
            return new TemporaryCredentials { Kind = CredentialKind.None };
        DateTimeOffset? expiresAt = Millis(chosen, "expiration-time-ms");
        if (Str(config, "s3.access-key-id") is { } keyId)
        {
            return new TemporaryCredentials
            {
                Kind = CredentialKind.Aws,
                AwsAccessKeyId = keyId,
                AwsSecretAccessKey = Required(config, "s3.secret-access-key"),
                AwsSessionToken = Str(config, "s3.session-token"),
                ExpiresAt = expiresAt,
            };
        }
        if (Str(config, "azure.sas-token") is { } sas)
            return new TemporaryCredentials { Kind = CredentialKind.Azure, AzureSasToken = sas, ExpiresAt = expiresAt };
        if (Str(config, "gcs.oauth-token") is { } token)
            return new TemporaryCredentials { Kind = CredentialKind.Gcp, GcpOauthToken = token, ExpiresAt = expiresAt };
        return new TemporaryCredentials { Kind = CredentialKind.None, ExpiresAt = expiresAt };
    }
}

/// <summary>Requests of the Delta API that create managed tables.</summary>
public static class UnityManagedTables
{
    /// <summary>The domains whose configuration the catalog keeps (the Delta API names these two).</summary>
    private static readonly string[] KeptDomains = ["delta.clustering", "delta.rowTracking"];

    /// <summary>
    /// The request that creates a table from the version 0 written at <paramref name="location"/>, as Delta Spark sends
    /// it: the columns, partitioning, protocol and properties of that version, the configuration of its clustering and
    /// row tracking domains, and its in-commit timestamp.
    /// </summary>
    public static byte[] CreateRequest(
        string tableName, string location, MetadataAction metadata, ProtocolAction protocol,
        IEnumerable<DomainMetadataAction> domains, long lastCommitTimestamp)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(protocol);
        ArgumentNullException.ThrowIfNull(domains);
        DomainMetadataAction[] kept = [.. domains.Where(d => !d.Removed && KeptDomains.Contains(d.Domain))];
        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartObject();
            w.WriteString("name", tableName);
            w.WriteString("location", location);
            w.WriteString("table-type", "MANAGED");
            if (metadata.Description is { } comment) w.WriteString("comment", comment);
            w.WritePropertyName("columns");
            w.WriteRawValue(metadata.SchemaString);
            w.WriteStartArray("partition-columns");
            foreach (string column in metadata.PartitionColumns) w.WriteStringValue(column);
            w.WriteEndArray();
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
            w.WriteStartObject("properties");
            foreach (var (key, value) in metadata.Configuration) w.WriteString(key, value);
            w.WriteEndObject();
            if (kept.Length > 0)
            {
                w.WriteStartObject("domain-metadata");
                foreach (DomainMetadataAction domain in kept)
                {
                    // The configuration as an object, without the domain name (as Spark sends it).
                    w.WriteStartObject(domain.Domain);
                    using JsonDocument config = JsonDocument.Parse(domain.Configuration);
                    foreach (JsonProperty property in config.RootElement.EnumerateObject())
                    {
                        if (property.Name != "domainName") property.WriteTo(w);
                    }
                    w.WriteEndObject();
                }
                w.WriteEndObject();
            }
            w.WriteNumber("last-commit-timestamp-ms", lastCommitTimestamp);
            w.WriteEndObject();
        }
        return stream.ToArray();
    }
}
