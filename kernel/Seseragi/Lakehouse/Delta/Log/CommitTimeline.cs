using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta.Log;

/// <summary>
/// The commit time of every version still in the log. The time of a commit is the modification time of its file,
/// adjusted so that times strictly increase with the version; or, from the version where in-commit timestamps
/// were enabled, the timestamp written inside the commit.
/// </summary>
public sealed class CommitTimeline
{
    /// <summary>The table property that turns in-commit timestamps on.</summary>
    public const string InCommitTimestampsKey = "delta.enableInCommitTimestamps";
    private const string EnablementVersionKey = "delta.inCommitTimestampEnablementVersion";

    private readonly IFileSystem _fileSystem;
    private readonly List<LogFile> _commits;
    private readonly long[] _fileTimes; // milliseconds, strictly increasing
    private readonly long _inCommitFrom;  // first version with in-commit timestamps; long.MaxValue when off
    private readonly Dictionary<long, long> _inCommitCache = [];

    private CommitTimeline(IFileSystem fileSystem, List<LogFile> commits, long inCommitFrom)
    {
        _fileSystem = fileSystem;
        _commits = commits;
        _inCommitFrom = inCommitFrom;
        _fileTimes = new long[commits.Count];
        for (int i = 0; i < commits.Count; i++)
        {
            long time = commits[i].Entry.LastModified.ToUnixTimeMilliseconds();
            _fileTimes[i] = i > 0 && time <= _fileTimes[i - 1] ? _fileTimes[i - 1] + 1 : time;
        }
    }

    /// <summary>The earliest version whose commit is still in the log.</summary>
    public long EarliestVersion => _commits[0].Version;

    /// <summary>The latest version.</summary>
    public long LatestVersion => _commits[^1].Version;

    /// <summary>Reads the timeline of a table.</summary>
    /// <exception cref="DeltaTableNotFoundException">The location holds no Delta table.</exception>
    public static CommitTimeline Load(IFileSystem fileSystem, IParquetReader? parquet = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);

        List<LogFile> commits = LogSegment.ListCommits(fileSystem);
        if (commits.Count == 0)
            throw new DeltaTableNotFoundException("No Delta table found: the log directory holds no commit.");

        // Keep the contiguous run that ends at the latest version: a gap means older commits were cleaned up.
        int first = commits.Count - 1;
        while (first > 0 && commits[first - 1].Version == commits[first].Version - 1) first--;
        if (first > 0) commits.RemoveRange(0, first);

        long inCommitFrom = long.MaxValue;
        Snapshot latest = Snapshot.Load(fileSystem, null, parquet);
        if (latest.Metadata.Configuration.TryGetValue(InCommitTimestampsKey, out string? enabled)
            && string.Equals(enabled, "true", StringComparison.OrdinalIgnoreCase))
        {
            // Without an enablement version the feature was on from the table's first commit.
            inCommitFrom = latest.Metadata.Configuration.TryGetValue(EnablementVersionKey, out string? v) && long.TryParse(v, out long from)
                ? from
                : 0;
        }

        return new CommitTimeline(fileSystem, commits, inCommitFrom);
    }

    /// <summary>The commit time of a version.</summary>
    /// <exception cref="DeltaException">The commit of the version is not in the log.</exception>
    public DateTimeOffset TimestampOf(long version) => DateTimeOffset.FromUnixTimeMilliseconds(MillisAt(IndexOf(version)));

    /// <summary>The latest version committed at or before a time; the latest version when the time is after it.</summary>
    /// <exception cref="DeltaException">The time is before the earliest commit still in the log.</exception>
    public long VersionAsOf(DateTimeOffset timestamp)
    {
        long target = timestamp.ToUnixTimeMilliseconds();
        if (MillisAt(0) > target)
            throw new DeltaException(
                $"The time {timestamp:O} is before the earliest version still in the log (version {EarliestVersion}, committed at {TimestampOf(EarliestVersion):O}).");

        // Times increase with the version, so the answer is the last index whose time is not after the target.
        int low = 0, high = _commits.Count - 1;
        while (low < high)
        {
            int mid = low + (high - low + 1) / 2;
            if (MillisAt(mid) <= target) low = mid;
            else high = mid - 1;
        }
        return _commits[low].Version;
    }

    /// <summary>The earliest version committed at or after a time, or null when the time is after the last commit.</summary>
    public long? VersionAtOrAfter(DateTimeOffset timestamp)
    {
        long target = timestamp.ToUnixTimeMilliseconds();
        if (MillisAt(_commits.Count - 1) < target) return null;

        int low = 0, high = _commits.Count - 1;
        while (low < high)
        {
            int mid = low + (high - low) / 2;
            if (MillisAt(mid) >= target) high = mid;
            else low = mid + 1;
        }
        return _commits[low].Version;
    }

    private int IndexOf(long version)
    {
        long index = version - EarliestVersion;
        if (index < 0 || index >= _commits.Count)
            throw new DeltaException($"The commit of version {version} is not in the log (versions {EarliestVersion} to {LatestVersion} are).");
        return (int)index;
    }

    private long MillisAt(int index)
    {
        long version = _commits[index].Version;
        if (version < _inCommitFrom) return _fileTimes[index];

        if (_inCommitCache.TryGetValue(version, out long cached)) return cached;
        byte[] content = _fileSystem.ReadAll(_commits[index].Entry.Path)
            ?? throw new DeltaException($"The commit of version {version} disappeared while reading the log.");
        foreach (DeltaAction action in ActionParser.ParseCommit(content))
        {
            if (action is CommitInfoAction { InCommitTimestamp: long time })
            {
                _inCommitCache[version] = time;
                return time;
            }
        }
        throw new DeltaException($"The commit of version {version} has no in-commit timestamp although the table has them enabled.");
    }
}
