using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>Which files a vacuum considers.</summary>
public enum VacuumMode
{
    /// <summary>Only files the log says were removed (and their deletion vectors): no listing of the storage.</summary>
    Lite,
    /// <summary>Every file under the table, so files no commit ever referenced (failed writes) go too.</summary>
    Full,
}

/// <summary>What a vacuum deleted, or would delete in a dry run.</summary>
/// <param name="Files">The files, relative to the table root.</param>
/// <param name="Bytes">Their total size, when known (a lite vacuum knows the size the log recorded).</param>
/// <param name="DryRun">Whether nothing was deleted.</param>
public sealed record VacuumResult(IReadOnlyList<string> Files, long Bytes, bool DryRun);

/// <summary>
/// Deletes data files the table no longer needs: not part of the latest version, and removed longer ago than the
/// retention (<c>delta.deletedFileRetentionDuration</c>, 7 days by default), so time travel within the retention
/// keeps working. Files written more recently than the retention are never deleted, since a writer may be about
/// to commit them. The log itself is not touched (see <see cref="LogCleanup"/>).
/// </summary>
public static class Vacuum
{
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="snapshot">The latest state of the table.</param>
    /// <param name="retention">How long removed files are kept; the table's retention when null.</param>
    /// <param name="enforceRetention">Refuse a retention shorter than the table's (which would break time travel and
    /// may delete files concurrent writers are still committing).</param>
    /// <param name="dryRun">Only report what would be deleted.</param>
    /// <param name="mode">Which files to consider.</param>
    /// <param name="now">The current time; the clock when null.</param>
    /// <exception cref="DeltaException">The retention is too short, or the table cannot be written by this kernel.</exception>
    public static VacuumResult Run(
        IWritableFileSystem fileSystem, Snapshot snapshot, TimeSpan? retention = null, bool enforceRetention = true, bool dryRun = false,
        VacuumMode mode = VacuumMode.Lite, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(snapshot);
        // vacuumProtocolCheck: a vacuum is a write, so every writer feature of the table must be understood.
        TableFeatures.EnsureWritable(snapshot.Protocol, snapshot.Metadata);

        TimeSpan tableRetention = Checkpoints.Retention(snapshot.Metadata);
        TimeSpan keep = retention ?? tableRetention;
        if (keep < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(retention), "The retention cannot be negative.");
        if (enforceRetention && keep < tableRetention)
            throw new DeltaException(
                $"A retention of {keep.TotalHours:0.##} hours is shorter than the table's {tableRetention.TotalHours:0.##} hours " +
                $"({Checkpoints.TombstoneRetentionKey}). It would break time travel and may delete files of writes still in progress. " +
                "Turn the retention check off to do it anyway.");
        DateTimeOffset clock = now ?? DateTimeOffset.UtcNow;
        long cutoff = (clock - keep).ToUnixTimeMilliseconds();

        // Everything the latest version and unexpired removes still point at.
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        foreach (AddAction file in snapshot.Files) Reference(referenced, file.DecodedPath, file.DeletionVector);
        foreach (RemoveAction removed in snapshot.Tombstones)
        {
            if (removed.DeletionTimestamp is null || removed.DeletionTimestamp >= cutoff)
                Reference(referenced, Uri.UnescapeDataString(removed.Path), removed.DeletionVector);
        }

        var victims = new List<(string Path, long Size)>();
        if (mode == VacuumMode.Full)
        {
            foreach (FileEntry entry in fileSystem.ListAll(""))
            {
                if (IsHidden(entry.Path) || referenced.Contains(entry.Path)) continue;
                if (entry.LastModified.ToUnixTimeMilliseconds() >= cutoff) continue; // maybe a write about to commit
                victims.Add((entry.Path, entry.Size));
            }
        }
        else
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (RemoveAction removed in snapshot.Tombstones)
            {
                if (removed.DeletionTimestamp is null || removed.DeletionTimestamp >= cutoff) continue;
                string path = Uri.UnescapeDataString(removed.Path);
                if (!IsInside(path)) continue;
                if (!referenced.Contains(path) && seen.Add(path) && Exists(fileSystem, path)) victims.Add((path, removed.Size ?? 0));
                if (VectorFile(removed.DeletionVector) is string vector && !referenced.Contains(vector) && seen.Add(vector) && Exists(fileSystem, vector))
                    victims.Add((vector, 0));
            }
        }
        victims.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        long bytes = victims.Sum(v => v.Size);
        if (dryRun || victims.Count == 0) return new VacuumResult([.. victims.Select(v => v.Path)], bytes, dryRun);

        // As Spark: a commit before and after, without actions, so the history shows the vacuum.
        string Millis(TimeSpan t) => ((long)t.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
        Transaction.Start(fileSystem, snapshot, "VACUUM START")
            .WithParameter("retentionCheckEnabled", enforceRetention ? "true" : "false")
            .WithParameter("specifiedRetentionMillis", Millis(keep))
            .WithParameter("defaultRetentionMillis", Millis(tableRetention))
            .WithMetric("numFilesToDelete", victims.Count)
            .WithMetric("sizeOfDataToDelete", bytes)
            .Commit(clock);

        var deleted = new List<string>(victims.Count);
        long deletedBytes = 0;
        foreach (var (path, size) in victims)
        {
            if (!fileSystem.Delete(path)) continue;
            deleted.Add(path);
            deletedBytes += size;
        }

        Transaction.Start(fileSystem, snapshot, "VACUUM END")
            .WithParameter("status", "COMPLETED")
            .WithMetric("numDeletedFiles", deleted.Count)
            .WithMetric("numVacuumedDirectories", 0)
            .Commit(clock);
        return new VacuumResult(deleted, deletedBytes, DryRun: false);
    }

    private static void Reference(HashSet<string> referenced, string path, DeletionVectorDescriptor? vector)
    {
        referenced.Add(path);
        if (VectorFile(vector) is string file) referenced.Add(file);
    }

    /// <summary>The table-relative file of a deletion vector stored in a file under the table (storage type u).</summary>
    private static string? VectorFile(DeletionVectorDescriptor? vector) =>
        vector is { StorageType: "u" } ? vector.FilePath() : null;

    /// <summary>A path under the table root (not an absolute URI pointing elsewhere).</summary>
    private static bool IsInside(string path) => !path.Contains("://", StringComparison.Ordinal) && !path.StartsWith('/');

    /// <summary>
    /// Files a vacuum never touches: the log, and anything in a directory or with a name starting with <c>_</c> or
    /// <c>.</c>, except the change data directory and partition directories (<c>name=value</c>).
    /// </summary>
    internal static bool IsHidden(string path)
    {
        string[] segments = path.Split('/');
        if (segments[0] == "_delta_log") return true;
        for (int i = 0; i < segments.Length; i++)
        {
            string segment = segments[i];
            if (segment.Length == 0 || segment[0] is not ('_' or '.')) continue;
            bool isDirectory = i < segments.Length - 1;
            if (isDirectory && (segment.Contains('=') || (i == 0 && segment == "_change_data"))) continue;
            return true;
        }
        return false;
    }

    private static bool Exists(IFileSystem fileSystem, string path) => fileSystem.ReadRange(path, 0, 1) is not null;
}
