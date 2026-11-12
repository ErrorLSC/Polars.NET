using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Removes log files that are past the log retention (the protocol's metadata cleanup): everything before the
/// newest checkpoint whose commit is older than <c>delta.logRetentionDuration</c> (30 days by default). That
/// checkpoint and every later file stay, so each version still inside the retention can be read. Under checkpoint
/// protection, history before the protected version is removed only all at once.
/// </summary>
public static class LogCleanup
{
    /// <summary>The table property giving how long log files are kept.</summary>
    public const string RetentionKey = "delta.logRetentionDuration";

    /// <summary>The table property that turns the cleanup off when "false".</summary>
    public const string EnabledKey = "delta.enableExpiredLogCleanup";

    private static readonly TimeSpan DefaultRetention = TimeSpan.FromDays(30);

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
        DateTimeOffset cutoff = (now ?? DateTimeOffset.UtcNow) - Retention(snapshot.Metadata);
        List<LogFile> files = LogSegment.ListLog(fileSystem, null);
        CommitTimeline timeline = CommitTimeline.Load(fileSystem, snapshot.Metadata);

        // The newest complete checkpoint, at or before the snapshot, whose commit is past the retention.
        long keepFrom = -1;
        foreach (IGrouping<long, LogFile> checkpoint in files
            .Where(f => f.Kind is LogFileKind.ClassicCheckpoint or LogFileKind.MultiPartCheckpoint or LogFileKind.V2Checkpoint
                && f.Version <= snapshot.Version)
            .GroupBy(f => f.Version)
            .OrderByDescending(g => g.Key))
        {
            if (LogSegment.ChooseComplete([.. checkpoint]) is null) continue;
            if (!TryTimestamp(timeline, checkpoint.Key, out DateTimeOffset at) || at > cutoff) continue;
            keepFrom = checkpoint.Key;
            break;
        }
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

    private static bool TryTimestamp(CommitTimeline timeline, long version, out DateTimeOffset at)
    {
        try
        {
            at = timeline.TimestampOf(version);
            return true;
        }
        catch (DeltaException)
        {
            at = default; // the commit of that version is already gone: its checkpoint is not a safe boundary to judge
            return false;
        }
    }

    /// <summary>The log retention: <c>interval N unit</c> as Delta writes it, 30 days when unset or unreadable.</summary>
    internal static TimeSpan Retention(MetadataAction metadata) =>
        metadata.Configuration.TryGetValue(RetentionKey, out string? text) && Checkpoints.TryParseInterval(text, out TimeSpan value)
            ? value
            : DefaultRetention;
}
