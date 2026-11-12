using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Removes log files that are past the log retention (the protocol's metadata cleanup), as delta-spark 4.4 does:
/// the cutoff is <c>delta.logRetentionDuration</c> (30 days by default) before now, rounded down to midnight UTC;
/// everything before the newest checkpoint whose preceding files are all older than the cutoff goes (file times
/// made increasing with the version, as Spark's <c>BufferingLogDeletionIterator</c> makes them). That checkpoint and
/// every later file stay. Under checkpoint protection, history before the protected version is removed only all at
/// once.
/// </summary>
public static class LogCleanup
{
    /// <summary>The table property giving how long log files are kept.</summary>
    public const string RetentionKey = "delta.logRetentionDuration";

    /// <summary>The table property that turns the cleanup off when "false".</summary>
    public const string EnabledKey = "delta.enableExpiredLogCleanup";

    /// <summary>The table property giving the log retention of <c>DROP FEATURE … TRUNCATE HISTORY</c>.</summary>
    public const string TruncateHistoryRetentionKey = "delta.dropFeatureTruncateHistory.retentionDuration";

    private static readonly TimeSpan DefaultRetention = TimeSpan.FromDays(30);
    private static readonly TimeSpan DefaultTruncateHistoryRetention = TimeSpan.FromHours(24);

    /// <summary>Deletes the expired log files; returns their paths.</summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="snapshot">The latest state of the table; its properties decide the retention.</param>
    /// <param name="now">The clock; the current time when null.</param>
    public static IReadOnlyList<string> Run(IWritableFileSystem fileSystem, Snapshot snapshot, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.Metadata.Configuration.TryGetValue(EnabledKey, out string? enabled)
            && string.Equals(enabled, "false", StringComparison.OrdinalIgnoreCase))
            return [];
        // The log of a catalog-managed table is cleaned up only where its catalog allows it (by default nowhere).
        if (CatalogManaged.IsCatalogManaged(snapshot.Protocol)) return [];
        DateTimeOffset cutoff = Floor((now ?? DateTimeOffset.UtcNow) - Retention(snapshot.Metadata), TimeSpan.FromDays(1));
        return Clean(fileSystem, snapshot, cutoff);
    }

    /// <summary>
    /// The cleanup of <c>DROP FEATURE … TRUNCATE HISTORY</c>, as Spark runs it: the cutoff is
    /// <c>delta.dropFeatureTruncateHistory.retentionDuration</c> (24 hours by default) before now, rounded down to the
    /// minute, and it runs even where <c>delta.enableExpiredLogCleanup</c> turns the regular cleanup off. Returns the
    /// paths deleted.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="snapshot">The latest state of the table, with the checkpoint the history may be truncated to.</param>
    /// <param name="now">The clock; the current time when null.</param>
    public static IReadOnlyList<string> Truncate(IWritableFileSystem fileSystem, Snapshot snapshot, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(snapshot);
        if (CatalogManaged.IsCatalogManaged(snapshot.Protocol)) return [];
        DateTimeOffset cutoff = Floor((now ?? DateTimeOffset.UtcNow) - TruncateHistoryRetention(snapshot.Metadata), TimeSpan.FromMinutes(1));
        return Clean(fileSystem, snapshot, cutoff);
    }

    private static IReadOnlyList<string> Clean(IWritableFileSystem fileSystem, Snapshot snapshot, DateTimeOffset cutoff)
    {
        List<LogFile> files = LogSegment.ListLog(fileSystem, null);
        long keepFrom = Boundary(files, snapshot.Version, cutoff);
        if (keepFrom <= 0) return [];

        // Checkpoint protection (a feature was dropped at the boundary version): history before the boundary goes
        // only in one go, all of it, so the protected checkpoints keep hiding the commits that need the dropped
        // feature. Until the retention reaches the boundary nothing is cleaned up.
        if (TableFeatures.CheckpointProtectionBoundary(snapshot.Protocol, snapshot.Metadata) is long boundary && keepFrom < boundary) return [];

        // Commits (and the files beside them) go before the checkpoints, so a checkpoint never disappears while a
        // commit it hides is still there.
        List<string> deleted = [];
        foreach (LogFile file in files.Where(f => !IsCheckpoint(f)).Concat(files.Where(IsCheckpoint)))
        {
            bool before = file.Kind switch
            {
                LogFileKind.Commit or LogFileKind.Checksum or LogFileKind.ClassicCheckpoint or LogFileKind.MultiPartCheckpoint
                    or LogFileKind.V2Checkpoint => file.Version < keepFrom,
                LogFileKind.CompactedCommit => file.EndVersion < keepFrom,
                _ => false, // _last_checkpoint and anything unknown stay
            };
            if (before && fileSystem.Delete(file.Entry.Path)) deleted.Add(file.Entry.Path);
        }
        deleted.AddRange(CleanSidecars(fileSystem, files.Where(f => f.Kind == LogFileKind.V2Checkpoint && f.Version >= keepFrom), cutoff));
        return deleted;
    }

    /// <summary>
    /// The version the log can be cut at, as Spark's <c>BufferingLogDeletionIterator</c> finds it: the files of the
    /// commits, checksums and checkpoints in version order (by name within a version), their times made increasing;
    /// at each complete checkpoint up to the latest one, everything before it may go when the last file before it is
    /// not later than the cutoff. Returns the newest such checkpoint, or -1.
    /// </summary>
    private static long Boundary(List<LogFile> files, long latestVersion, DateTimeOffset cutoff)
    {
        HashSet<long> complete = [.. files
            .Where(f => IsCheckpoint(f) && f.Version <= latestVersion)
            .GroupBy(f => f.Version)
            .Where(g => LogSegment.ChooseComplete([.. g]) is not null)
            .Select(g => g.Key)];
        if (complete.Count == 0) return -1;

        long keepFrom = -1;
        DateTimeOffset? lastTime = null;
        long lastVersion = -1;
        foreach (LogFile file in files
            .Where(f => f.Kind is LogFileKind.Commit or LogFileKind.Checksum || IsCheckpoint(f))
            .OrderBy(f => f.Version).ThenBy(f => f.Entry.Path, StringComparer.Ordinal))
        {
            // The first file of a complete checkpoint: the files before it go if the last of them has expired.
            if (IsCheckpoint(file) && complete.Contains(file.Version) && lastVersion >= 0 && lastVersion < file.Version
                && lastTime <= cutoff)
                keepFrom = file.Version;
            DateTimeOffset time = file.Entry.LastModified;
            if (lastTime is { } previous && file.Version > lastVersion && previous >= time) time = previous.AddMilliseconds(1);
            lastTime = time;
            lastVersion = file.Version;
        }
        return keepFrom;
    }

    /// <summary>The time rounded down to a whole <paramref name="unit"/> (a day: midnight UTC), as Spark's <c>truncateDate</c>.</summary>
    private static DateTimeOffset Floor(DateTimeOffset time, TimeSpan unit)
    {
        long ticks = time.UtcTicks;
        return new DateTimeOffset(ticks - ticks % unit.Ticks, TimeSpan.Zero);
    }

    private static bool IsCheckpoint(LogFile file) =>
        file.Kind is LogFileKind.ClassicCheckpoint or LogFileKind.MultiPartCheckpoint or LogFileKind.V2Checkpoint;

    /// <summary>
    /// Deletes sidecar files no remaining V2 checkpoint refers to, once they are past the retention. Only when every
    /// remaining V2 checkpoint is JSON, so its references can be read here; otherwise sidecars are left alone.
    /// </summary>
    private static List<string> CleanSidecars(IWritableFileSystem fileSystem, IEnumerable<LogFile> remaining, DateTimeOffset cutoff)
    {
        var deleted = new List<string>();
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        foreach (LogFile checkpoint in remaining)
        {
            if (!checkpoint.Entry.Path.EndsWith(".json", StringComparison.Ordinal)) return deleted;
            byte[]? content = fileSystem.ReadAll(checkpoint.Entry.Path);
            if (content is null) return deleted;
            foreach (SidecarAction sidecar in ActionParser.ParseCommit(content).OfType<SidecarAction>())
                referenced.Add(sidecar.Path);
        }
        foreach (FileEntry entry in fileSystem.List($"{LogFile.LogDirectory}/_sidecars"))
        {
            string name = entry.Path[(LogFile.LogDirectory.Length + "/_sidecars/".Length)..];
            if (referenced.Contains(name) || entry.LastModified > cutoff) continue;
            if (fileSystem.Delete(entry.Path)) deleted.Add(entry.Path);
        }
        return deleted;
    }

    /// <summary>The log retention: <c>interval N unit</c> as Delta writes it, 30 days when unset or unreadable.</summary>
    internal static TimeSpan Retention(MetadataAction metadata) =>
        metadata.Configuration.TryGetValue(RetentionKey, out string? text) && Checkpoints.TryParseInterval(text, out TimeSpan value)
            ? value
            : DefaultRetention;

    /// <summary>The retention of <c>TRUNCATE HISTORY</c>: <c>interval N unit</c>, 24 hours when unset or unreadable.</summary>
    internal static TimeSpan TruncateHistoryRetention(MetadataAction metadata) =>
        metadata.Configuration.TryGetValue(TruncateHistoryRetentionKey, out string? text) && Checkpoints.TryParseInterval(text, out TimeSpan value)
            ? value
            : DefaultTruncateHistoryRetention;
}
