using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta.Log;

/// <summary>
/// The log files that together give the state of a table at one version: at most one checkpoint, and the commits
/// after it up to the version.
/// </summary>
internal sealed class LogSegment
{
    /// <summary>The version the segment ends at.</summary>
    public required long Version { get; init; }
    /// <summary>The files of the checkpoint the segment starts from; empty when it starts from commit 0.</summary>
    public required IReadOnlyList<LogFile> Checkpoint { get; init; }
    /// <summary>The version of the checkpoint, or -1.</summary>
    public required long CheckpointVersion { get; init; }
    /// <summary>
    /// The commits after the checkpoint, oldest first. A run of them may be replaced by a compacted commit
    /// (<c>N.M.compacted.json</c>) that holds their reconciled actions; the last commit is always its own file.
    /// </summary>
    public required IReadOnlyList<LogFile> Commits { get; init; }
    /// <summary>The modification time of the newest file of the segment.</summary>
    public required DateTimeOffset LastModified { get; init; }

    /// <summary>Finds the segment for the latest version, or for <paramref name="version"/>.</summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="version">The version wanted, or null for the latest.</param>
    /// <param name="useCheckpoints">False to start from commit 0 whatever the checkpoints (tests compare the two).</param>
    public static LogSegment Find(IFileSystem fileSystem, long? version, bool useCheckpoints)
    {
        // A catalog-managed table is what its catalog says: asked once, before listing.
        CatalogLog? log = CatalogManaged.CatalogOf(fileSystem)?.GetLog();

        // The hint lets a long log be listed from its last checkpoint on. It may be stale or missing, and it is of
        // no use for a version before it; then the whole log is listed.
        long? hint = useCheckpoints ? ReadLastCheckpointHint(fileSystem) : null;
        if (hint is long from && (version is null || version >= from))
        {
            List<LogFile> tail = Listed(fileSystem, $"{from:D20}", log);
            LogSegment? segment = TryBuild(tail, version, useCheckpoints, requireCheckpoint: true, out _);
            if (segment is not null) return segment;
        }

        List<LogFile> all = Listed(fileSystem, null, log);
        return TryBuild(all, version, useCheckpoints, requireCheckpoint: false, out string? error)
            ?? throw (all.Count == 0 || error is null
                ? new DeltaTableNotFoundException("No Delta table found: the log directory holds no commit.")
                : new DeltaException(error));
    }

    /// <summary>The commit files still present in the log, oldest first (for a catalog-managed table, as its catalog has them).</summary>
    public static List<LogFile> ListCommits(IFileSystem fileSystem)
    {
        List<LogFile> commits = Listed(fileSystem, null, CatalogManaged.CatalogOf(fileSystem)?.GetLog());
        commits.RemoveAll(static f => f.Kind != LogFileKind.Commit);
        commits.Sort(static (a, b) => a.Version.CompareTo(b.Version));
        return commits;
    }

    private static List<LogFile> Listed(IFileSystem fileSystem, string? startFrom, CatalogLog? log) =>
        log is null ? ListLog(fileSystem, startFrom) : CatalogManaged.Apply(ListLog(fileSystem, startFrom), log);

    internal static List<LogFile> ListLog(IFileSystem fileSystem, string? startFrom)
    {
        var files = new List<LogFile>();
        foreach (FileEntry entry in fileSystem.List(LogFile.LogDirectory, startFrom))
        {
            if (LogFile.TryParse(entry, out LogFile file)) files.Add(file);
        }
        return files;
    }

    private static long? ReadLastCheckpointHint(IFileSystem fileSystem)
    {
        byte[]? bytes = fileSystem.ReadAll($"{LogFile.LogDirectory}/_last_checkpoint");
        if (bytes is null) return null;
        try
        {
            using JsonDocument doc = JsonDocument.Parse(bytes);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                && doc.RootElement.TryGetProperty("version", out JsonElement v) && v.TryGetInt64(out long version)
                ? version
                : null;
        }
        catch (JsonException)
        {
            return null; // a corrupt hint is ignored, as the protocol allows
        }
    }

    private static LogSegment? TryBuild(List<LogFile> files, long? version, bool useCheckpoints, bool requireCheckpoint, out string? error)
    {
        error = null;
        var commits = new SortedDictionary<long, LogFile>();
        var compactions = new Dictionary<long, List<LogFile>>();
        var checkpoints = new SortedDictionary<long, List<LogFile>>();
        foreach (LogFile file in files)
        {
            switch (file.Kind)
            {
                case LogFileKind.Commit:
                    commits[file.Version] = file;
                    break;
                case LogFileKind.CompactedCommit:
                    if (!compactions.TryGetValue(file.Version, out List<LogFile>? starting)) compactions[file.Version] = starting = [];
                    starting.Add(file);
                    break;
                case LogFileKind.ClassicCheckpoint or LogFileKind.MultiPartCheckpoint or LogFileKind.V2Checkpoint:
                    if (!checkpoints.TryGetValue(file.Version, out List<LogFile>? list)) checkpoints[file.Version] = list = [];
                    list.Add(file);
                    break;
            }
        }

        // Complete checkpoints, newest first.
        var complete = new List<(long Version, List<LogFile> Files)>();
        if (useCheckpoints)
        {
            foreach (var (v, candidates) in checkpoints.Reverse())
            {
                List<LogFile>? chosen = ChooseComplete(candidates);
                if (chosen is not null) complete.Add((v, chosen));
            }
        }

        long latest = -1;
        if (commits.Count > 0) latest = commits.Keys.Max();
        if (complete.Count > 0) latest = Math.Max(latest, complete[0].Version);
        if (latest < 0) return null;

        long target = version ?? latest;
        if (target < 0 || target > latest)
        {
            error = $"Version {target} does not exist; the latest version is {latest}.";
            return null;
        }

        long checkpointVersion = -1;
        List<LogFile> checkpointFiles = [];
        foreach (var (v, chosen) in complete)
        {
            if (v <= target)
            {
                checkpointVersion = v;
                checkpointFiles = chosen;
                break;
            }
        }
        if (requireCheckpoint && checkpointVersion < 0) return null;

        var needed = new List<LogFile>();
        for (long v = checkpointVersion + 1; v <= target; v++)
        {
            if (!commits.TryGetValue(v, out LogFile commit))
            {
                error = checkpointVersion < 0 && v == 0
                    ? (checkpoints.Count > 0
                        ? "The log does not start at version 0: reading it needs a checkpoint, and checkpoints were not used."
                        : $"The log starts at version {(commits.Count > 0 ? commits.Keys.Min() : 0)} and has no checkpoint to start from.")
                    : $"The log is not contiguous: the commit of version {v} is missing.";
                return null;
            }
            needed.Add(commit);
        }
        needed = UseCompactions(needed, compactions, target);

        DateTimeOffset lastModified = needed.Count > 0
            ? needed[^1].Entry.LastModified
            : commits.TryGetValue(target, out LogFile own) ? own.Entry.LastModified : checkpointFiles[0].Entry.LastModified;

        return new LogSegment
        {
            Version = target,
            Checkpoint = checkpointFiles,
            CheckpointVersion = checkpointVersion,
            Commits = needed,
            LastModified = lastModified,
        };
    }

    /// <summary>
    /// Replaces runs of commits by the compacted commits that cover them, taking the longest run at each step. A
    /// compaction is used only when it ends before <paramref name="target"/>: the last commit is read on its own,
    /// because its file time is the time of the snapshot.
    /// </summary>
    private static List<LogFile> UseCompactions(List<LogFile> commits, Dictionary<long, List<LogFile>> compactions, long target)
    {
        if (compactions.Count == 0 || commits.Count == 0) return commits;
        var result = new List<LogFile>(commits.Count);
        long first = commits[0].Version;
        int i = 0;
        while (i < commits.Count)
        {
            long v = commits[i].Version;
            LogFile? best = null;
            if (compactions.TryGetValue(v, out List<LogFile>? starting))
            {
                foreach (LogFile compaction in starting)
                {
                    long end = compaction.EndVersion;
                    if (end > v && end < target && (best is null || end > best.Value.EndVersion)) best = compaction;
                }
            }
            if (best is LogFile chosen)
            {
                result.Add(chosen);
                i = (int)(chosen.EndVersion - first) + 1;
            }
            else
            {
                result.Add(commits[i]);
                i++;
            }
        }
        return result;
    }

    /// <summary>Of the checkpoint files of one version, picks a set that forms a whole checkpoint, or null.</summary>
    internal static List<LogFile>? ChooseComplete(List<LogFile> candidates)
    {
        foreach (LogFile file in candidates)
        {
            if (file.Kind == LogFileKind.V2Checkpoint) return [file];
        }
        foreach (LogFile file in candidates)
        {
            if (file.Kind == LogFileKind.ClassicCheckpoint) return [file];
        }
        // A multi-part checkpoint counts only when every part of one writer's set is there.
        foreach (IGrouping<int, LogFile> group in candidates.Where(f => f.Kind == LogFileKind.MultiPartCheckpoint).GroupBy(f => f.NumParts))
        {
            List<LogFile> parts = [.. group.DistinctBy(f => f.Part).OrderBy(f => f.Part)];
            if (parts.Count == group.Key) return parts;
        }
        return null;
    }
}
