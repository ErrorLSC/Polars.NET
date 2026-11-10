using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Restore and history of a Delta table through the managed kernel (see <see cref="Kaminari.Seseragi.Lakehouse.Delta.Restore"/>
/// and <see cref="Kaminari.Seseragi.Lakehouse.Delta.History"/>). Not reachable from the public API yet.
/// </summary>
internal static class DeltaKernelHistory
{
    /// <summary>Brings the table back to a version, or to the version current at a time.</summary>
    public static RestoreResult Restore(
        string tablePath, long? version = null, DateTimeOffset? timestamp = null, bool ignoreMissingFiles = false,
        bool protocolDowngradeAllowed = false, CloudArgs? cloud = null)
    {
        if (version.HasValue == timestamp.HasValue) throw new ArgumentException("Give a version or a time to restore to, not both.");
        try
        {
            using DmlTable table = DmlTable.Open(tablePath, cloud);
            var parquet = new EngineParquetReader(table.Root, cloud);
            Snapshot target = version is long v
                ? Snapshot.Load(table.FileSystem, v, parquet)
                : Snapshot.LoadAsOf(table.FileSystem, timestamp!.Value, parquet);
            RestoreResult result = Kaminari.Seseragi.Lakehouse.Delta.Restore.Run(
                table.FileSystem, table.Snapshot, target, ignoreMissingFiles, protocolDowngradeAllowed);
            if (result.Version is long committed) DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, committed, cloud);
            return result;
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }

    /// <summary>The newest <paramref name="limit"/> commits (0: all), newest first.</summary>
    public static List<HistoryEntry> Read(string tablePath, int limit = 0, CloudArgs? cloud = null)
    {
        bool isUrl = tablePath.Contains("://", StringComparison.Ordinal);
        IFileSystem fileSystem = isUrl ? new ObjectStoreFileSystem(tablePath.TrimEnd('/'), cloud) : new LocalFileSystem(tablePath);
        try
        {
            string root = fileSystem is LocalFileSystem local ? local.Root : tablePath.TrimEnd('/');
            return History.Read(fileSystem, limit, new EngineParquetReader(root, cloud));
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            (fileSystem as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// The history as a JSON array, one object per commit: its <c>commitInfo</c> fields, with <c>version</c> and
    /// <c>timestamp</c> (milliseconds, the protocol's commit time) set by the log.
    /// </summary>
    /// <param name="entries">The commits.</param>
    /// <param name="renameCollisions">Rename an <c>operationParameters</c> / <c>operationMetrics</c> field that has
    /// the name of a top-level field (a RESTORE's <c>version</c>) to <c>operationParameters.version</c>, so that the
    /// objects can be flattened into one row.</param>
    public static string ToJson(IEnumerable<HistoryEntry> entries, bool renameCollisions = false)
    {
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, Kaminari.Seseragi.Lakehouse.Delta.Actions.ActionWriter.JsonOptions))
        {
            json.WriteStartArray();
            foreach (HistoryEntry entry in entries)
            {
                json.WriteStartObject();
                json.WriteNumber("version", entry.Version);
                json.WriteNumber("timestamp", entry.Timestamp.ToUnixTimeMilliseconds());
                if (entry.CommitInfo is not null)
                {
                    using JsonDocument info = JsonDocument.Parse(entry.CommitInfo.Json);
                    var topLevel = new HashSet<string>(StringComparer.Ordinal) { "version", "timestamp" };
                    foreach (JsonProperty property in info.RootElement.EnumerateObject()) topLevel.Add(property.Name);
                    foreach (JsonProperty property in info.RootElement.EnumerateObject())
                    {
                        if (property.NameEquals("version") || property.NameEquals("timestamp")) continue;
                        bool nested = property.NameEquals("operationParameters") || property.NameEquals("operationMetrics");
                        if (!renameCollisions || !nested || property.Value.ValueKind != JsonValueKind.Object)
                        {
                            property.WriteTo(json);
                            continue;
                        }
                        json.WriteStartObject(property.Name);
                        foreach (JsonProperty field in property.Value.EnumerateObject())
                        {
                            json.WritePropertyName(topLevel.Contains(field.Name) ? property.Name + "." + field.Name : field.Name);
                            field.Value.WriteTo(json);
                        }
                        json.WriteEndObject();
                    }
                }
                json.WriteEndObject();
            }
            json.WriteEndArray();
        }
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }
}
