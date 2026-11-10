using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>One commit of a table's history.</summary>
/// <param name="Version">The version.</param>
/// <param name="Timestamp">When it was committed, as the protocol defines it (in-commit timestamp, else file time).</param>
/// <param name="CommitInfo">Its provenance (operation, parameters, metrics), when the writer recorded one.</param>
public sealed record HistoryEntry(long Version, DateTimeOffset Timestamp, CommitInfoAction? CommitInfo);

/// <summary>The commits of a table, newest first, as far back as the log still holds them.</summary>
public static class History
{
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="limit">The number of newest commits; 0 for all.</param>
    /// <param name="parquet">Reads Parquet checkpoints; needed when the earliest commits were cleaned up.</param>
    public static List<HistoryEntry> Read(IFileSystem fileSystem, int limit = 0, IParquetReader? parquet = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit), "The limit cannot be negative.");
        CommitTimeline timeline = CommitTimeline.Load(fileSystem, parquet);
        var entries = new List<HistoryEntry>();
        for (long version = timeline.LatestVersion; version >= timeline.EarliestVersion; version--)
        {
            if (limit > 0 && entries.Count == limit) break;
            byte[]? commit = fileSystem.ReadAll(LogFile.CommitPath(version));
            if (commit is null) continue; // only a checkpoint left for it
            CommitInfoAction? info = ActionParser.ParseCommit(commit).OfType<CommitInfoAction>().FirstOrDefault();
            entries.Add(new HistoryEntry(version, timeline.TimestampOf(version), info));
        }
        return entries;
    }
}
