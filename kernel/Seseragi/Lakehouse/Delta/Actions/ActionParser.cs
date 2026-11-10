using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>Reads the actions of a JSON commit file: one action per line.</summary>
public static class ActionParser
{
    /// <summary>Reads all actions of a commit file. Lines holding an action this kernel does not know are skipped.</summary>
    /// <exception cref="DeltaException">A line is not valid JSON or an action lacks a required field.</exception>
    public static List<DeltaAction> ParseCommit(ReadOnlyMemory<byte> content) => ParseLines(content, checkpointRows: false);

    /// <summary>
    /// Reads the actions of checkpoint rows given as JSON lines (see <see cref="Engine.IParquetReader"/>).
    /// Each row has one column per action kind; the columns that do not apply are null or hold only nulls.
    /// </summary>
    public static List<DeltaAction> ParseCheckpointRows(ReadOnlyMemory<byte> content) => ParseLines(content, checkpointRows: true);

    private static List<DeltaAction> ParseLines(ReadOnlyMemory<byte> content, bool checkpointRows)
    {
        var actions = new List<DeltaAction>();
        ReadOnlySpan<byte> span = content.Span;
        int start = 0;
        while (start < span.Length)
        {
            int newline = span[start..].IndexOf((byte)'\n');
            int end = newline < 0 ? span.Length : start + newline;

            ReadOnlyMemory<byte> line = content[start..end];
            if (!IsBlank(line.Span))
            {
                DeltaAction? action = ParseLine(line, checkpointRows);
                if (action is not null) actions.Add(action);
            }
            start = end + 1;
        }
        return actions;
    }

    private static bool IsBlank(ReadOnlySpan<byte> line)
    {
        foreach (byte b in line)
        {
            if (b != ' ' && b != '\r' && b != '\t') return false;
        }
        return true;
    }

    /// <summary>Reads one action. Returns null for an action this kernel does not know.</summary>
    public static DeltaAction? ParseLine(ReadOnlyMemory<byte> line) => ParseLine(line, checkpointRow: false);

    private static DeltaAction? ParseLine(ReadOnlyMemory<byte> line, bool checkpointRow)
    {
        try
        {
            using JsonDocument doc = JsonDocument.Parse(line);
            JsonElement root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                throw new DeltaException("A log line is not a JSON object.");

            foreach (JsonProperty p in root.EnumerateObject())
            {
                if (p.Value.ValueKind != JsonValueKind.Object) continue;
                // In a checkpoint row an action that is absent may arrive as an object whose fields are all null.
                if (checkpointRow && KeyField(p.Name) is string key
                    && (!p.Value.TryGetProperty(key, out JsonElement keyValue) || keyValue.ValueKind == JsonValueKind.Null))
                    continue;
                switch (p.Name)
                {
                    case "sidecar": return ReadSidecar(p.Value);
                    case "checkpointMetadata":
                        return new CheckpointMetadataAction { Version = Long(p.Value, "version") ?? throw Missing("version", "checkpointMetadata") };
                    case "add": return ReadAdd(p.Value);
                    case "remove": return ReadRemove(p.Value);
                    case "metaData": return ReadMetadata(p.Value);
                    case "protocol": return ReadProtocol(p.Value);
                    case "txn": return ReadTxn(p.Value);
                    case "cdc": return ReadCdc(p.Value);
                    case "domainMetadata": return ReadDomainMetadata(p.Value);
                    case "commitInfo": return ReadCommitInfo(p.Value);
                }
            }
            return null;
        }
        catch (JsonException e)
        {
            throw new DeltaException($"A log line is not valid JSON: {e.Message}", e);
        }
        catch (Exception e) when (e is InvalidOperationException or FormatException)
        {
            throw new DeltaException($"A log action has a field of the wrong type: {e.Message}", e);
        }
    }

    private static string? KeyField(string action) => action switch
    {
        "add" or "remove" or "cdc" or "sidecar" => "path",
        "metaData" => "id",
        "protocol" => "minReaderVersion",
        "txn" => "appId",
        "domainMetadata" => "domain",
        "checkpointMetadata" => "version",
        _ => null,
    };

    private static SidecarAction ReadSidecar(JsonElement e) => new()
    {
        Path = Required(e, "path", "sidecar"),
        SizeInBytes = Long(e, "sizeInBytes") ?? 0,
        ModificationTime = Long(e, "modificationTime") ?? 0,
    };

    private static AddAction ReadAdd(JsonElement e) => new()
    {
        Path = Required(e, "path", "add"),
        PartitionValues = NullableMap(e, "partitionValues") ?? EmptyMap.Nullable,
        Size = Long(e, "size") ?? 0,
        ModificationTime = Long(e, "modificationTime") ?? 0,
        DataChange = Bool(e, "dataChange", true),
        // A checkpoint may carry the statistics as a struct (stats_parsed) instead of, or beside, the JSON text.
        // The struct has the shape of the text, so its JSON form reads the same way.
        Stats = Str(e, "stats") ?? ParsedStats(e),
        Tags = NullableMap(e, "tags"),
        DeletionVector = ReadDeletionVector(e),
        BaseRowId = Long(e, "baseRowId"),
        DefaultRowCommitVersion = Long(e, "defaultRowCommitVersion"),
        ClusteringProvider = Str(e, "clusteringProvider"),
    };

    private static RemoveAction ReadRemove(JsonElement e) => new()
    {
        Path = Required(e, "path", "remove"),
        DeletionTimestamp = Long(e, "deletionTimestamp"),
        DataChange = Bool(e, "dataChange", true),
        ExtendedFileMetadata = Bool(e, "extendedFileMetadata", false),
        PartitionValues = NullableMap(e, "partitionValues"),
        Size = Long(e, "size"),
        DeletionVector = ReadDeletionVector(e),
        BaseRowId = Long(e, "baseRowId"),
        DefaultRowCommitVersion = Long(e, "defaultRowCommitVersion"),
    };

    private static CdcAction ReadCdc(JsonElement e) => new()
    {
        Path = Required(e, "path", "cdc"),
        PartitionValues = NullableMap(e, "partitionValues") ?? EmptyMap.Nullable,
        Size = Long(e, "size") ?? 0,
        Tags = NullableMap(e, "tags"),
    };

    private static MetadataAction ReadMetadata(JsonElement e)
    {
        string provider = "parquet";
        IReadOnlyDictionary<string, string> formatOptions = EmptyMap.Instance;
        if (e.TryGetProperty("format", out JsonElement format) && format.ValueKind == JsonValueKind.Object)
        {
            provider = Str(format, "provider") ?? provider;
            formatOptions = Map(format, "options");
        }

        var partitionColumns = new List<string>();
        if (e.TryGetProperty("partitionColumns", out JsonElement pc) && pc.ValueKind == JsonValueKind.Array)
        {
            foreach (JsonElement c in pc.EnumerateArray()) partitionColumns.Add(c.GetString()!);
        }

        return new MetadataAction
        {
            Id = Required(e, "id", "metaData"),
            Name = Str(e, "name"),
            Description = Str(e, "description"),
            FormatProvider = provider,
            FormatOptions = formatOptions,
            SchemaString = Required(e, "schemaString", "metaData"),
            PartitionColumns = partitionColumns,
            Configuration = Map(e, "configuration"),
            CreatedTime = Long(e, "createdTime"),
        };
    }

    private static ProtocolAction ReadProtocol(JsonElement e) => new()
    {
        MinReaderVersion = (int)(Long(e, "minReaderVersion") ?? throw Missing("minReaderVersion", "protocol")),
        MinWriterVersion = (int)(Long(e, "minWriterVersion") ?? throw Missing("minWriterVersion", "protocol")),
        ReaderFeatures = Strings(e, "readerFeatures"),
        WriterFeatures = Strings(e, "writerFeatures"),
    };

    private static TxnAction ReadTxn(JsonElement e) => new()
    {
        AppId = Required(e, "appId", "txn"),
        Version = Long(e, "version") ?? throw Missing("version", "txn"),
        LastUpdated = Long(e, "lastUpdated"),
    };

    private static DomainMetadataAction ReadDomainMetadata(JsonElement e) => new()
    {
        Domain = Required(e, "domain", "domainMetadata"),
        Configuration = Str(e, "configuration") ?? "",
        Removed = Bool(e, "removed", false),
    };

    private static CommitInfoAction ReadCommitInfo(JsonElement e) => new()
    {
        Json = e.GetRawText(),
        Timestamp = Long(e, "timestamp"),
        InCommitTimestamp = Long(e, "inCommitTimestamp"),
        Operation = Str(e, "operation"),
    };

    private static DeletionVectorDescriptor? ReadDeletionVector(JsonElement parent)
    {
        if (!parent.TryGetProperty("deletionVector", out JsonElement e) || e.ValueKind != JsonValueKind.Object) return null;
        return new DeletionVectorDescriptor
        {
            StorageType = Required(e, "storageType", "deletionVector"),
            PathOrInlineDv = Required(e, "pathOrInlineDv", "deletionVector"),
            Offset = (int?)Long(e, "offset"),
            SizeInBytes = (int)(Long(e, "sizeInBytes") ?? throw Missing("sizeInBytes", "deletionVector")),
            Cardinality = Long(e, "cardinality") ?? throw Missing("cardinality", "deletionVector"),
        };
    }

    // ------------------------------------------------------------------

    private static DeltaException Missing(string field, string action) =>
        new($"A '{action}' action lacks the required field '{field}'.");

    private static string? ParsedStats(JsonElement e) =>
        e.TryGetProperty("stats_parsed", out JsonElement v) && v.ValueKind == JsonValueKind.Object ? v.GetRawText() : null;

    private static string? Str(JsonElement e, string name) =>
        e.TryGetProperty(name, out JsonElement v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static string Required(JsonElement e, string name, string action) => Str(e, name) ?? throw Missing(name, action);

    private static long? Long(JsonElement e, string name) =>
        e.TryGetProperty(name, out JsonElement v) && v.ValueKind == JsonValueKind.Number ? v.GetInt64() : null;

    private static bool Bool(JsonElement e, string name, bool fallback) =>
        e.TryGetProperty(name, out JsonElement v)
            ? v.ValueKind switch { JsonValueKind.True => true, JsonValueKind.False => false, _ => fallback }
            : fallback;

    private static List<string>? Strings(JsonElement e, string name)
    {
        if (!e.TryGetProperty(name, out JsonElement v) || v.ValueKind != JsonValueKind.Array) return null;
        var list = new List<string>(v.GetArrayLength());
        foreach (JsonElement item in v.EnumerateArray()) list.Add(item.GetString()!);
        return list;
    }

    private static IReadOnlyDictionary<string, string> Map(JsonElement e, string name)
    {
        Dictionary<string, string?>? map = NullableMap(e, name);
        if (map is null || map.Count == 0) return EmptyMap.Instance;
        var result = new Dictionary<string, string>(map.Count, StringComparer.Ordinal);
        foreach (var (key, value) in map)
        {
            if (value is not null) result[key] = value;
        }
        return result;
    }

    // A map is an object, or (from a Parquet reader) an array of {"key": ..., "value": ...} entries.
    private static Dictionary<string, string?>? NullableMap(JsonElement e, string name)
    {
        if (!e.TryGetProperty(name, out JsonElement v)) return null;

        if (v.ValueKind == JsonValueKind.Object)
        {
            var map = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (JsonProperty p in v.EnumerateObject()) map[p.Name] = Text(p.Value);
            return map;
        }
        if (v.ValueKind == JsonValueKind.Array)
        {
            var map = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (JsonElement entry in v.EnumerateArray())
            {
                if (entry.ValueKind != JsonValueKind.Object || !entry.TryGetProperty("key", out JsonElement key) || key.ValueKind != JsonValueKind.String)
                    throw new DeltaException($"The map '{name}' has an entry without a string key.");
                map[key.GetString()!] = entry.TryGetProperty("value", out JsonElement value) ? Text(value) : null;
            }
            return map;
        }
        return null;
    }

    private static string? Text(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => value.GetString(),
        _ => value.GetRawText(),
    };
}
