using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Row tracking (the <c>rowTracking</c> writer feature): every row has a stable row id and the version that last
/// changed it. A data file records the id of its first row (<c>baseRowId</c>) and the version that added it
/// (<c>defaultRowCommitVersion</c>); a row's id is <c>baseRowId</c> plus its position in the file, unless the file
/// materializes it in a hidden column, as rewritten files do. The highest id assigned is the
/// <c>rowIdHighWaterMark</c> of the <c>delta.rowTracking</c> domain.
/// </summary>
/// <remarks>
/// While the feature is supported, every new file gets fresh ids (the transaction assigns them when it commits,
/// see <see cref="AssignFresh"/>), unless <see cref="SuspendedKey"/> is on: then new files get none. Only once
/// <see cref="EnableKey"/> is on are ids stable: rewritten rows then keep theirs in the materialized columns. The two
/// properties exclude each other.
/// </remarks>
public static class RowTracking
{
    /// <summary>The writer feature.</summary>
    public const string Feature = "rowTracking";

    /// <summary>The table property that makes row ids stable (every file has its ids, rewrites keep them).</summary>
    public const string EnableKey = "delta.enableRowTracking";

    /// <summary>
    /// The table property that stops assigning row ids to new files (the first step of turning row tracking off);
    /// cannot be on together with <see cref="EnableKey"/>.
    /// </summary>
    public const string SuspendedKey = "delta.rowTrackingSuspended";

    /// <summary>The table property naming the hidden column of materialized row ids.</summary>
    public const string MaterializedRowIdKey = "delta.rowTracking.materializedRowIdColumnName";

    /// <summary>The table property naming the hidden column of materialized row commit versions.</summary>
    public const string MaterializedRowCommitVersionKey = "delta.rowTracking.materializedRowCommitVersionColumnName";

    /// <summary>The domain holding the row id high water mark.</summary>
    public const string Domain = "delta.rowTracking";

    /// <summary>Whether the protocol supports row tracking: new files must then get row ids.</summary>
    public static bool IsSupported(ProtocolAction protocol) =>
        TableFeatures.WriterFeaturesOf(protocol).Contains(Feature);

    /// <summary>Whether row ids are stable: the feature is supported and turned on.</summary>
    public static bool IsEnabled(ProtocolAction protocol, MetadataAction metadata) =>
        IsSupported(protocol) && IsTrue(metadata, EnableKey);

    /// <summary>Whether new files get no row ids although the feature is supported.</summary>
    /// <exception cref="DeltaProtocolException">Row tracking is both turned on and suspended.</exception>
    public static bool IsSuspended(MetadataAction metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        bool suspended = IsTrue(metadata, SuspendedKey);
        if (suspended && IsTrue(metadata, EnableKey))
            throw new DeltaProtocolException(
                $"Illegal table state: the table properties {EnableKey} and {SuspendedKey} are both set to true. " +
                "Set either of them to false.");
        return suspended;
    }

    private static bool IsTrue(MetadataAction metadata, string key) =>
        metadata.Configuration.TryGetValue(key, out string? value) && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>The names of the hidden columns of materialized row ids and commit versions, when the table names them.</summary>
    public static (string RowId, string CommitVersion)? MaterializedColumns(MetadataAction metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return metadata.Configuration.TryGetValue(MaterializedRowIdKey, out string? id)
            && metadata.Configuration.TryGetValue(MaterializedRowCommitVersionKey, out string? version)
            ? (id, version)
            : null;
    }

    /// <summary>
    /// The configuration with names for the materialized columns, as Spark gives them when the feature comes in
    /// (<c>_row-id-col-UUID</c>, <c>_row-commit-version-col-UUID</c>); names already set are kept.
    /// </summary>
    public static Dictionary<string, string> WithMaterializedColumnNames(IReadOnlyDictionary<string, string> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var result = new Dictionary<string, string>(configuration, StringComparer.Ordinal);
        result.TryAdd(MaterializedRowIdKey, $"_row-id-col-{Guid.NewGuid()}");
        result.TryAdd(MaterializedRowCommitVersionKey, $"_row-commit-version-col-{Guid.NewGuid()}");
        return result;
    }

    /// <summary>The row id high water mark of the domains of a table; null when no id was assigned yet.</summary>
    public static long? HighWaterMark(IReadOnlyDictionary<string, DomainMetadataAction> domains)
    {
        ArgumentNullException.ThrowIfNull(domains);
        return domains.TryGetValue(Domain, out DomainMetadataAction? domain) ? HighWaterMark(domain) : null;
    }

    /// <summary>The row id high water mark a <c>delta.rowTracking</c> domain action records; null when removed.</summary>
    /// <exception cref="DeltaProtocolException">The configuration is not the domain's JSON.</exception>
    public static long? HighWaterMark(DomainMetadataAction domain)
    {
        ArgumentNullException.ThrowIfNull(domain);
        if (domain.Removed) return null;
        try
        {
            using JsonDocument doc = JsonDocument.Parse(domain.Configuration);
            return doc.RootElement.TryGetProperty("rowIdHighWaterMark", out JsonElement mark) ? mark.GetInt64() : null;
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException or FormatException)
        {
            throw new DeltaProtocolException($"The {Domain} domain holds '{domain.Configuration}', which is not a row id high water mark.");
        }
    }

    /// <summary>The domain action recording a high water mark, as Spark writes it.</summary>
    public static DomainMetadataAction DomainFor(long highWaterMark)
    {
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteNumber("rowIdHighWaterMark", highWaterMark);
            json.WriteString("domainName", Domain);
            json.WriteEndObject();
        }
        return new DomainMetadataAction { Domain = Domain, Configuration = Encoding.UTF8.GetString(stream.ToArray()) };
    }

    /// <summary>
    /// The actions with fresh row ids for every added file that has none: consecutive ranges after
    /// <paramref name="highWaterMark"/>, each file taking as many ids as it has rows; such files, and every added
    /// file without one, get <paramref name="version"/> as their default row commit version. Files that keep their
    /// ids (re-added with a deletion vector, restored) are left as they are.
    /// </summary>
    /// <param name="actions">The actions of a commit.</param>
    /// <param name="highWaterMark">The highest id assigned so far (null for none); advanced past the ids given.</param>
    /// <param name="version">The version being committed.</param>
    /// <returns>The actions, and whether any id was assigned.</returns>
    /// <exception cref="DeltaProtocolException">An added file without ids has no row count in its statistics.</exception>
    public static (List<DeltaAction> Actions, bool Assigned) AssignFresh(IEnumerable<DeltaAction> actions, ref long? highWaterMark, long version)
    {
        ArgumentNullException.ThrowIfNull(actions);
        var result = new List<DeltaAction>();
        bool assigned = false;
        foreach (DeltaAction action in actions)
        {
            if (action is not AddAction add || (add.BaseRowId is not null && add.DefaultRowCommitVersion is not null))
            {
                result.Add(action);
                continue;
            }
            long? baseRowId = add.BaseRowId;
            if (baseRowId is null)
            {
                long rows = add.ParsedStats?.NumRecords
                    ?? throw new DeltaProtocolException($"The file {add.Path} has no row count in its statistics, which row tracking needs.");
                baseRowId = (highWaterMark ?? -1) + 1;
                if (rows > 0) highWaterMark = baseRowId + rows - 1;
                assigned = true;
            }
            result.Add(new AddAction
            {
                Path = add.Path,
                PartitionValues = add.PartitionValues,
                Size = add.Size,
                ModificationTime = add.ModificationTime,
                DataChange = add.DataChange,
                Stats = add.Stats,
                Tags = add.Tags,
                DeletionVector = add.DeletionVector,
                BaseRowId = baseRowId,
                DefaultRowCommitVersion = add.DefaultRowCommitVersion ?? version,
                ClusteringProvider = add.ClusteringProvider,
            });
        }
        return (result, assigned);
    }
}
