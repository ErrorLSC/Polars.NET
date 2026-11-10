using System.Buffers;
using System.Text.Json;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>Writes actions as the lines of a commit file, in the JSON form the protocol defines.</summary>
public static class ActionWriter
{
    /// <summary>
    /// Writer options for log JSON: characters are written as UTF-8 rather than escaped (the log is never embedded in
    /// HTML), as other Delta writers do.
    /// </summary>
    public static JsonWriterOptions JsonOptions { get; } = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>The commit file content: one JSON object per action, each on its own line.</summary>
    public static byte[] WriteCommit(IEnumerable<DeltaAction> actions)
    {
        ArgumentNullException.ThrowIfNull(actions);
        var buffer = new ArrayBufferWriter<byte>();
        bool first = true;
        foreach (DeltaAction action in actions)
        {
            if (!first) buffer.Write("\n"u8);
            first = false;
            using (var json = new Utf8JsonWriter(buffer, JsonOptions))
            {
                Write(json, action);
            }
        }
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Writes one action as a JSON object, such as <c>{"add":{...}}</c>.</summary>
    public static void Write(Utf8JsonWriter json, DeltaAction action)
    {
        ArgumentNullException.ThrowIfNull(json);
        json.WriteStartObject();
        switch (action)
        {
            case CommitInfoAction info:
                json.WritePropertyName("commitInfo");
                json.WriteRawValue(info.Json);
                break;

            case ProtocolAction p:
                json.WriteStartObject("protocol");
                json.WriteNumber("minReaderVersion", p.MinReaderVersion);
                json.WriteNumber("minWriterVersion", p.MinWriterVersion);
                if (p.ReaderFeatures is not null) WriteStrings(json, "readerFeatures", p.ReaderFeatures);
                if (p.WriterFeatures is not null) WriteStrings(json, "writerFeatures", p.WriterFeatures);
                json.WriteEndObject();
                break;

            case MetadataAction m:
                json.WriteStartObject("metaData");
                json.WriteString("id", m.Id);
                if (m.Name is not null) json.WriteString("name", m.Name);
                if (m.Description is not null) json.WriteString("description", m.Description);
                json.WriteStartObject("format");
                json.WriteString("provider", m.FormatProvider);
                WriteMap(json, "options", m.FormatOptions);
                json.WriteEndObject();
                json.WriteString("schemaString", m.SchemaString);
                WriteStrings(json, "partitionColumns", m.PartitionColumns);
                WriteMap(json, "configuration", m.Configuration);
                if (m.CreatedTime is long created) json.WriteNumber("createdTime", created);
                json.WriteEndObject();
                break;

            case AddAction a:
                json.WriteStartObject("add");
                json.WriteString("path", a.Path);
                WriteNullableMap(json, "partitionValues", a.PartitionValues);
                json.WriteNumber("size", a.Size);
                json.WriteNumber("modificationTime", a.ModificationTime);
                json.WriteBoolean("dataChange", a.DataChange);
                if (a.Stats is not null) json.WriteString("stats", a.Stats);
                if (a.Tags is not null) WriteNullableMap(json, "tags", a.Tags);
                if (a.DeletionVector is not null) WriteDeletionVector(json, a.DeletionVector);
                if (a.BaseRowId is long baseRowId) json.WriteNumber("baseRowId", baseRowId);
                if (a.DefaultRowCommitVersion is long rowVersion) json.WriteNumber("defaultRowCommitVersion", rowVersion);
                if (a.ClusteringProvider is not null) json.WriteString("clusteringProvider", a.ClusteringProvider);
                json.WriteEndObject();
                break;

            case RemoveAction r:
                json.WriteStartObject("remove");
                json.WriteString("path", r.Path);
                if (r.DeletionTimestamp is long deleted) json.WriteNumber("deletionTimestamp", deleted);
                json.WriteBoolean("dataChange", r.DataChange);
                if (r.ExtendedFileMetadata)
                {
                    json.WriteBoolean("extendedFileMetadata", true);
                    if (r.PartitionValues is not null) WriteNullableMap(json, "partitionValues", r.PartitionValues);
                    if (r.Size is long size) json.WriteNumber("size", size);
                }
                if (r.DeletionVector is not null) WriteDeletionVector(json, r.DeletionVector);
                if (r.BaseRowId is long removedBaseRowId) json.WriteNumber("baseRowId", removedBaseRowId);
                if (r.DefaultRowCommitVersion is long removedRowVersion) json.WriteNumber("defaultRowCommitVersion", removedRowVersion);
                json.WriteEndObject();
                break;

            case CdcAction c:
                json.WriteStartObject("cdc");
                json.WriteString("path", c.Path);
                WriteNullableMap(json, "partitionValues", c.PartitionValues);
                json.WriteNumber("size", c.Size);
                json.WriteBoolean("dataChange", false);
                if (c.Tags is not null) WriteNullableMap(json, "tags", c.Tags);
                json.WriteEndObject();
                break;

            case TxnAction t:
                json.WriteStartObject("txn");
                json.WriteString("appId", t.AppId);
                json.WriteNumber("version", t.Version);
                if (t.LastUpdated is long updated) json.WriteNumber("lastUpdated", updated);
                json.WriteEndObject();
                break;

            case DomainMetadataAction d:
                json.WriteStartObject("domainMetadata");
                json.WriteString("domain", d.Domain);
                json.WriteString("configuration", d.Configuration);
                json.WriteBoolean("removed", d.Removed);
                json.WriteEndObject();
                break;

            default:
                throw new DeltaException($"Actions of type {action.GetType().Name} are not written to commits.");
        }
        json.WriteEndObject();
    }

    private static void WriteStrings(Utf8JsonWriter json, string name, IEnumerable<string> values)
    {
        json.WriteStartArray(name);
        foreach (string value in values) json.WriteStringValue(value);
        json.WriteEndArray();
    }

    private static void WriteMap(Utf8JsonWriter json, string name, IReadOnlyDictionary<string, string> map)
    {
        json.WriteStartObject(name);
        foreach (var (key, value) in map) json.WriteString(key, value);
        json.WriteEndObject();
    }

    private static void WriteNullableMap(Utf8JsonWriter json, string name, IReadOnlyDictionary<string, string?> map)
    {
        json.WriteStartObject(name);
        foreach (var (key, value) in map)
        {
            if (value is null) json.WriteNull(key);
            else json.WriteString(key, value);
        }
        json.WriteEndObject();
    }

    private static void WriteDeletionVector(Utf8JsonWriter json, DeletionVectorDescriptor dv)
    {
        json.WriteStartObject("deletionVector");
        json.WriteString("storageType", dv.StorageType);
        json.WriteString("pathOrInlineDv", dv.PathOrInlineDv);
        if (dv.Offset is int offset) json.WriteNumber("offset", offset);
        json.WriteNumber("sizeInBytes", dv.SizeInBytes);
        json.WriteNumber("cardinality", dv.Cardinality);
        json.WriteEndObject();
    }
}
