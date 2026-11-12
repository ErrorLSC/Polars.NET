using System.Buffers;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Catalog.Http;

namespace Kaminari.Seseragi.Lakehouse.Catalog.Unity;

/// <summary>
/// A client for the Unity Catalog REST API: catalogs, schemas, tables and temporary table credentials.
/// Every operation has a blocking form and an <c>Async</c> form. The client is safe to share between threads.
/// </summary>
public sealed partial class UnityCatalogClient : IDisposable
{
    private const string ApiRoot = "/api/2.1/unity-catalog";

    private readonly string _baseUrl;
    private readonly JsonHttpTransport _transport;

    /// <summary>Creates a client for one workspace.</summary>
    /// <param name="workspaceUrl">The workspace or server URL, for example <c>https://adb-123.azuredatabricks.net</c>.</param>
    /// <param name="bearerToken">A personal access token or OAuth token; null or empty for an open server.</param>
    /// <param name="httpClient">An HTTP client to send through. The caller keeps ownership of it. When null the client makes its own.</param>
    public UnityCatalogClient(string workspaceUrl, string? bearerToken, HttpClient? httpClient = null)
    {
        if (string.IsNullOrWhiteSpace(workspaceUrl))
            throw new ArgumentException("The workspace URL is required.", nameof(workspaceUrl));

        _baseUrl = workspaceUrl.TrimEnd('/') + ApiRoot;
        _transport = new JsonHttpTransport(bearerToken, httpClient);
    }

    // ------------------------------------------------------------------
    // Catalogs
    // ------------------------------------------------------------------

    /// <summary>Lists all catalogs.</summary>
    public List<CatalogInfo> ListCatalogs() =>
        ListCore("/catalogs", "catalogs", ReadCatalog, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="ListCatalogs()"/>
    public Task<List<CatalogInfo>> ListCatalogsAsync(CancellationToken cancellationToken = default) =>
        ListCore("/catalogs", "catalogs", ReadCatalog, true, cancellationToken).AsTask();

    /// <summary>Creates a catalog.</summary>
    public CatalogInfo CreateCatalog(string catalogName, string? comment = null, string? storageRoot = null) =>
        CreateCatalogCore(catalogName, comment, storageRoot, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="CreateCatalog(string, string?, string?)"/>
    public Task<CatalogInfo> CreateCatalogAsync(string catalogName, string? comment = null, string? storageRoot = null, CancellationToken cancellationToken = default) =>
        CreateCatalogCore(catalogName, comment, storageRoot, true, cancellationToken).AsTask();

    /// <summary>Deletes a catalog. With <paramref name="force"/> it is deleted even when not empty.</summary>
    public void DeleteCatalog(string catalogName, bool force = false) =>
        SendCore(HttpMethod.Delete, $"/catalogs/{Escape(catalogName)}?force={Bool(force)}", null, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="DeleteCatalog(string, bool)"/>
    public Task DeleteCatalogAsync(string catalogName, bool force = false, CancellationToken cancellationToken = default) =>
        SendCore(HttpMethod.Delete, $"/catalogs/{Escape(catalogName)}?force={Bool(force)}", null, true, cancellationToken).AsTask();

    // ------------------------------------------------------------------
    // Schemas
    // ------------------------------------------------------------------

    /// <summary>Lists the schemas of a catalog.</summary>
    public List<SchemaInfo> ListSchemas(string catalogName) =>
        ListCore($"/schemas?catalog_name={Escape(catalogName)}", "schemas", ReadSchema, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="ListSchemas(string)"/>
    public Task<List<SchemaInfo>> ListSchemasAsync(string catalogName, CancellationToken cancellationToken = default) =>
        ListCore($"/schemas?catalog_name={Escape(catalogName)}", "schemas", ReadSchema, true, cancellationToken).AsTask();

    /// <summary>Creates a schema in a catalog.</summary>
    public SchemaInfo CreateSchema(string catalogName, string schemaName, string? comment = null, string? storageRoot = null) =>
        CreateSchemaCore(catalogName, schemaName, comment, storageRoot, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="CreateSchema(string, string, string?, string?)"/>
    public Task<SchemaInfo> CreateSchemaAsync(string catalogName, string schemaName, string? comment = null, string? storageRoot = null, CancellationToken cancellationToken = default) =>
        CreateSchemaCore(catalogName, schemaName, comment, storageRoot, true, cancellationToken).AsTask();

    /// <summary>Deletes a schema. With <paramref name="force"/> it is deleted even when not empty.</summary>
    public void DeleteSchema(string catalogName, string schemaName, bool force = false) =>
        SendCore(HttpMethod.Delete, $"/schemas/{FullName(catalogName, schemaName)}?force={Bool(force)}", null, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="DeleteSchema(string, string, bool)"/>
    public Task DeleteSchemaAsync(string catalogName, string schemaName, bool force = false, CancellationToken cancellationToken = default) =>
        SendCore(HttpMethod.Delete, $"/schemas/{FullName(catalogName, schemaName)}?force={Bool(force)}", null, true, cancellationToken).AsTask();

    // ------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------

    /// <summary>Lists the tables of a schema.</summary>
    public List<TableInfo> ListTables(string catalogName, string schemaName) =>
        ListCore(TablesQuery(catalogName, schemaName), "tables", ReadTable, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="ListTables(string, string)"/>
    public Task<List<TableInfo>> ListTablesAsync(string catalogName, string schemaName, CancellationToken cancellationToken = default) =>
        ListCore(TablesQuery(catalogName, schemaName), "tables", ReadTable, true, cancellationToken).AsTask();

    /// <summary>Gets one table.</summary>
    public TableInfo GetTable(string catalogName, string schemaName, string tableName) =>
        GetTableCore(catalogName, schemaName, tableName, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="GetTable(string, string, string)"/>
    public Task<TableInfo> GetTableAsync(string catalogName, string schemaName, string tableName, CancellationToken cancellationToken = default) =>
        GetTableCore(catalogName, schemaName, tableName, true, cancellationToken).AsTask();

    /// <summary>Registers a table.</summary>
    /// <param name="catalogName">The catalog to create the table in.</param>
    /// <param name="schemaName">The schema to create the table in.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableType">See <see cref="UnityTableTypes"/>.</param>
    /// <param name="dataSourceFormat">See <see cref="UnityDataSourceFormats"/>; null leaves it out.</param>
    /// <param name="columns">The columns; null leaves the schema to the service.</param>
    /// <param name="storageLocation">The data location, required for external tables.</param>
    /// <param name="comment">A comment for the table.</param>
    /// <param name="properties">Table properties.</param>
    public TableInfo CreateTable(
        string catalogName, string schemaName, string tableName,
        string tableType,
        string? dataSourceFormat = null,
        IReadOnlyList<ColumnInfo>? columns = null,
        string? storageLocation = null,
        string? comment = null,
        IEnumerable<KeyValuePair<string, string>>? properties = null) =>
        CreateTableCore(catalogName, schemaName, tableName, tableType, dataSourceFormat, columns, storageLocation, comment, properties, false, default)
            .GetAwaiter().GetResult();

    /// <inheritdoc cref="CreateTable"/>
    public Task<TableInfo> CreateTableAsync(
        string catalogName, string schemaName, string tableName,
        string tableType,
        string? dataSourceFormat = null,
        IReadOnlyList<ColumnInfo>? columns = null,
        string? storageLocation = null,
        string? comment = null,
        IEnumerable<KeyValuePair<string, string>>? properties = null,
        CancellationToken cancellationToken = default) =>
        CreateTableCore(catalogName, schemaName, tableName, tableType, dataSourceFormat, columns, storageLocation, comment, properties, true, cancellationToken)
            .AsTask();

    /// <summary>Removes a table from the catalog.</summary>
    public void DeleteTable(string catalogName, string schemaName, string tableName) =>
        SendCore(HttpMethod.Delete, $"/tables/{FullName(catalogName, schemaName, tableName)}", null, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="DeleteTable(string, string, string)"/>
    public Task DeleteTableAsync(string catalogName, string schemaName, string tableName, CancellationToken cancellationToken = default) =>
        SendCore(HttpMethod.Delete, $"/tables/{FullName(catalogName, schemaName, tableName)}", null, true, cancellationToken).AsTask();

    // ------------------------------------------------------------------
    // Credentials
    // ------------------------------------------------------------------

    /// <summary>Asks for temporary storage credentials for a table.</summary>
    /// <param name="tableId">The <see cref="TableInfo.TableId"/> of the table.</param>
    /// <param name="write">True for read and write access, false for read only.</param>
    public TemporaryCredentials GetTableCredentials(string tableId, bool write) =>
        GetTableCredentialsCore(tableId, write, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="GetTableCredentials(string, bool)"/>
    public Task<TemporaryCredentials> GetTableCredentialsAsync(string tableId, bool write, CancellationToken cancellationToken = default) =>
        GetTableCredentialsCore(tableId, write, true, cancellationToken).AsTask();

    /// <summary>Looks a table up and asks for temporary credentials for it, in one step.</summary>
    /// <exception cref="CatalogException">The table has no storage location, or the service vended no usable credentials.</exception>
    public TableAccess ResolveTableAccess(string catalogName, string schemaName, string tableName, bool write) =>
        ResolveTableAccessCore(catalogName, schemaName, tableName, write, false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="ResolveTableAccess(string, string, string, bool)"/>
    public Task<TableAccess> ResolveTableAccessAsync(string catalogName, string schemaName, string tableName, bool write, CancellationToken cancellationToken = default) =>
        ResolveTableAccessCore(catalogName, schemaName, tableName, write, true, cancellationToken).AsTask();

    /// <summary>Releases the HTTP client when this client made it.</summary>
    public void Dispose() => _transport.Dispose();

    // ------------------------------------------------------------------
    // Cores
    // ------------------------------------------------------------------

    private ValueTask<byte[]> SendCore(HttpMethod method, string pathAndQuery, byte[]? body, bool async, CancellationToken ct) =>
        _transport.SendAsync(method, _baseUrl + pathAndQuery, body, async, ct);

    private async ValueTask<List<T>> ListCore<T>(
        string pathAndQuery, string arrayName, Func<JsonElement, T> read, bool async, CancellationToken ct)
    {
        var result = new List<T>();
        string? pageToken = null;
        do
        {
            string url = pageToken is null
                ? pathAndQuery
                : $"{pathAndQuery}{(pathAndQuery.Contains('?') ? '&' : '?')}page_token={Escape(pageToken)}";

            byte[] bytes = await SendCore(HttpMethod.Get, url, null, async, ct).ConfigureAwait(false);
            using JsonDocument doc = Parse(bytes);
            JsonElement root = doc.RootElement;

            if (root.TryGetProperty(arrayName, out JsonElement items) && items.ValueKind == JsonValueKind.Array)
            {
                foreach (JsonElement item in items.EnumerateArray()) result.Add(read(item));
            }

            pageToken = Str(root, "next_page_token");
        }
        while (!string.IsNullOrEmpty(pageToken));

        return result;
    }

    private async ValueTask<CatalogInfo> CreateCatalogCore(string catalogName, string? comment, string? storageRoot, bool async, CancellationToken ct)
    {
        byte[] body = WriteJson(w =>
        {
            w.WriteString("name", catalogName);
            w.WriteString("comment", comment);
            w.WriteString("storage_root", storageRoot);
        });
        byte[] bytes = await SendCore(HttpMethod.Post, "/catalogs", body, async, ct).ConfigureAwait(false);
        using JsonDocument doc = Parse(bytes);
        return ReadCatalog(doc.RootElement);
    }

    private async ValueTask<SchemaInfo> CreateSchemaCore(string catalogName, string schemaName, string? comment, string? storageRoot, bool async, CancellationToken ct)
    {
        byte[] body = WriteJson(w =>
        {
            w.WriteString("name", schemaName);
            w.WriteString("catalog_name", catalogName);
            w.WriteString("comment", comment);
            w.WriteString("storage_root", storageRoot);
        });
        byte[] bytes = await SendCore(HttpMethod.Post, "/schemas", body, async, ct).ConfigureAwait(false);
        using JsonDocument doc = Parse(bytes);
        return ReadSchema(doc.RootElement);
    }

    private async ValueTask<TableInfo> GetTableCore(string catalogName, string schemaName, string tableName, bool async, CancellationToken ct)
    {
        byte[] bytes = await SendCore(HttpMethod.Get, $"/tables/{FullName(catalogName, schemaName, tableName)}", null, async, ct).ConfigureAwait(false);
        using JsonDocument doc = Parse(bytes);
        return ReadTable(doc.RootElement);
    }

    private async ValueTask<TableInfo> CreateTableCore(
        string catalogName, string schemaName, string tableName,
        string tableType, string? dataSourceFormat, IReadOnlyList<ColumnInfo>? columns,
        string? storageLocation, string? comment, IEnumerable<KeyValuePair<string, string>>? properties,
        bool async, CancellationToken ct)
    {
        byte[] body = WriteJson(w =>
        {
            w.WriteString("name", tableName);
            w.WriteString("catalog_name", catalogName);
            w.WriteString("schema_name", schemaName);
            w.WriteString("comment", comment);
            w.WriteString("table_type", tableType);
            if (dataSourceFormat is not null) w.WriteString("data_source_format", dataSourceFormat);

            if (columns is null)
            {
                w.WriteNull("columns");
            }
            else
            {
                w.WriteStartArray("columns");
                foreach (ColumnInfo column in columns)
                {
                    w.WriteStartObject();
                    w.WriteString("name", column.Name);
                    w.WriteString("type_name", column.TypeName);
                    w.WriteString("type_text", column.TypeText);
                    w.WriteString("type_json", column.TypeJson);
                    WriteNullableInt(w, "position", column.Position);
                    w.WriteString("comment", column.Comment);
                    WriteNullableInt(w, "partition_index", column.PartitionIndex);
                    w.WriteEndObject();
                }
                w.WriteEndArray();
            }

            w.WriteString("storage_location", storageLocation);

            w.WriteStartObject("properties");
            if (properties is not null)
            {
                foreach (var (key, value) in properties) w.WriteString(key, value);
            }
            w.WriteEndObject();
        });
        byte[] bytes = await SendCore(HttpMethod.Post, "/tables", body, async, ct).ConfigureAwait(false);
        using JsonDocument doc = Parse(bytes);
        return ReadTable(doc.RootElement);
    }

    private async ValueTask<TemporaryCredentials> GetTableCredentialsCore(string tableId, bool write, bool async, CancellationToken ct)
    {
        byte[] body = WriteJson(w =>
        {
            w.WriteString("table_id", tableId);
            w.WriteString("operation", write ? "READ_WRITE" : "READ");
        });
        byte[] bytes = await SendCore(HttpMethod.Post, "/temporary-table-credentials", body, async, ct).ConfigureAwait(false);
        using JsonDocument doc = Parse(bytes);
        return ReadCredentials(doc.RootElement);
    }

    private async ValueTask<TableAccess> ResolveTableAccessCore(string catalogName, string schemaName, string tableName, bool write, bool async, CancellationToken ct)
    {
        TableInfo table = await GetTableCore(catalogName, schemaName, tableName, async, ct).ConfigureAwait(false);
        TemporaryCredentials credentials = await GetTableCredentialsCore(table.TableId, write, async, ct).ConfigureAwait(false);

        if (string.IsNullOrEmpty(table.StorageLocation))
            throw new CatalogException($"Table {catalogName}.{schemaName}.{tableName} has no storage location.");

        return new TableAccess { Table = table, Location = table.StorageLocation, Credentials = credentials };
    }

    // ------------------------------------------------------------------
    // JSON
    // ------------------------------------------------------------------

    private static byte[] WriteJson(Action<Utf8JsonWriter> writeProperties)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writeProperties(writer);
            writer.WriteEndObject();
        }
        return buffer.WrittenSpan.ToArray();
    }

    private static void WriteNullableInt(Utf8JsonWriter writer, string name, int? value)
    {
        if (value.HasValue) writer.WriteNumber(name, value.Value);
        else writer.WriteNull(name);
    }

    private static JsonDocument Parse(byte[] bytes)
    {
        try
        {
            JsonDocument doc = JsonDocument.Parse(bytes);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                doc.Dispose();
                throw new CatalogException("The catalog response is not a JSON object.");
            }
            return doc;
        }
        catch (JsonException e)
        {
            throw new CatalogException($"The catalog response is not valid JSON: {e.Message}", e);
        }
    }

    private static string? Str(JsonElement obj, string name) =>
        obj.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static string Required(JsonElement obj, string name) =>
        Str(obj, name) ?? throw new CatalogException($"The catalog response has no '{name}' field.");

    private static int? Int(JsonElement obj, string name) =>
        obj.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out int n) ? n : null;

    private static DateTimeOffset? Millis(JsonElement obj, string name) =>
        obj.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out long ms)
            ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
            : null;

    private static IReadOnlyDictionary<string, string> Map(JsonElement obj, string name)
    {
        if (!obj.TryGetProperty(name, out JsonElement value) || value.ValueKind != JsonValueKind.Object)
            return EmptyMap.Instance;

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (JsonProperty property in value.EnumerateObject())
        {
            map[property.Name] = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString()!
                : property.Value.GetRawText();
        }
        return map;
    }

    private static CatalogInfo ReadCatalog(JsonElement e) => new()
    {
        Name = Required(e, "name"),
        Comment = Str(e, "comment"),
        StorageLocation = Str(e, "storage_location"),
        Properties = Map(e, "properties"),
        Options = Map(e, "options"),
        CreatedAt = Millis(e, "created_at"),
        CreatedBy = Str(e, "created_by"),
        UpdatedAt = Millis(e, "updated_at"),
        UpdatedBy = Str(e, "updated_by"),
    };

    private static SchemaInfo ReadSchema(JsonElement e) => new()
    {
        Name = Required(e, "name"),
        Comment = Str(e, "comment"),
        StorageLocation = Str(e, "storage_location"),
        Properties = Map(e, "properties"),
        CreatedAt = Millis(e, "created_at"),
        CreatedBy = Str(e, "created_by"),
        UpdatedAt = Millis(e, "updated_at"),
        UpdatedBy = Str(e, "updated_by"),
    };

    private static TableInfo ReadTable(JsonElement e)
    {
        List<ColumnInfo>? columns = null;
        if (e.TryGetProperty("columns", out JsonElement cols) && cols.ValueKind == JsonValueKind.Array)
        {
            columns = [];
            foreach (JsonElement c in cols.EnumerateArray())
            {
                columns.Add(new ColumnInfo
                {
                    Name = Required(c, "name"),
                    TypeName = Str(c, "type_name") ?? "",
                    TypeText = Str(c, "type_text") ?? "",
                    TypeJson = Str(c, "type_json") ?? "",
                    Position = Int(c, "position"),
                    Comment = Str(c, "comment"),
                    PartitionIndex = Int(c, "partition_index"),
                });
            }
        }

        return new TableInfo
        {
            Name = Required(e, "name"),
            TableId = Required(e, "table_id"),
            TableType = Required(e, "table_type"),
            Comment = Str(e, "comment"),
            StorageLocation = Str(e, "storage_location"),
            DataSourceFormat = Str(e, "data_source_format"),
            Columns = columns,
            Properties = Map(e, "properties"),
            CreatedAt = Millis(e, "created_at"),
            CreatedBy = Str(e, "created_by"),
            UpdatedAt = Millis(e, "updated_at"),
            UpdatedBy = Str(e, "updated_by"),
        };
    }

    private static TemporaryCredentials ReadCredentials(JsonElement e)
    {
        DateTimeOffset? expiresAt = Millis(e, "expiration_time");

        if (e.TryGetProperty("aws_temp_credentials", out JsonElement aws) && aws.ValueKind == JsonValueKind.Object)
        {
            return new TemporaryCredentials
            {
                Kind = CredentialKind.Aws,
                AwsAccessKeyId = Required(aws, "access_key_id"),
                AwsSecretAccessKey = Required(aws, "secret_access_key"),
                AwsSessionToken = Str(aws, "session_token"),
                ExpiresAt = expiresAt,
            };
        }

        if (e.TryGetProperty("azure_user_delegation_sas", out JsonElement azure) && azure.ValueKind == JsonValueKind.Object)
        {
            return new TemporaryCredentials
            {
                Kind = CredentialKind.Azure,
                AzureSasToken = Required(azure, "sas_token"),
                ExpiresAt = expiresAt,
            };
        }

        if (e.TryGetProperty("gcp_oauth_token", out JsonElement gcp) && gcp.ValueKind == JsonValueKind.Object)
        {
            return new TemporaryCredentials
            {
                Kind = CredentialKind.Gcp,
                GcpOauthToken = Required(gcp, "oauth_token"),
                ExpiresAt = expiresAt,
            };
        }

        // Local storage needs no credentials, and the catalog vends none for it; anywhere else they must be there.
        if (Str(e, "url") is string url && url.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
            return new TemporaryCredentials { Kind = CredentialKind.None, ExpiresAt = expiresAt };
        throw new CatalogException("Unsupported or missing credentials in the catalog response.");
    }

    // ------------------------------------------------------------------
    // URLs
    // ------------------------------------------------------------------

    private static string Escape(string value) => Uri.EscapeDataString(value ?? throw new ArgumentNullException(nameof(value)));

    private static string Bool(bool value) => value ? "true" : "false";

    private static string FullName(string a, string b) => $"{Escape(a)}.{Escape(b)}";

    private static string FullName(string a, string b, string c) => $"{Escape(a)}.{Escape(b)}.{Escape(c)}";

    private static string TablesQuery(string catalogName, string schemaName) =>
        $"/tables?catalog_name={Escape(catalogName)}&schema_name={Escape(schemaName)}";
}
