using Kaminari.Seseragi.Delta.Engine;

namespace Kaminari.Seseragi.Delta.Log;

/// <summary>The kinds of file found in <c>_delta_log</c>.</summary>
public enum LogFileKind
{
    /// <summary><c>N.json</c>: the actions of one commit.</summary>
    Commit,
    /// <summary><c>N.checkpoint.parquet</c>.</summary>
    ClassicCheckpoint,
    /// <summary><c>N.checkpoint.P.T.parquet</c>: part P of T.</summary>
    MultiPartCheckpoint,
    /// <summary><c>N.checkpoint.UUID.json</c> or <c>.parquet</c>.</summary>
    V2Checkpoint,
    /// <summary><c>N.M.compacted.json</c>: the reconciled actions of commits N to M.</summary>
    CompactedCommit,
    /// <summary><c>N.crc</c>: a version checksum.</summary>
    Checksum,
    /// <summary><c>_last_checkpoint</c>.</summary>
    LastCheckpoint,
}

/// <summary>A file of the log directory with what its name says.</summary>
/// <param name="Kind">What the file is.</param>
/// <param name="Version">The version in the name; for a compacted commit the first version. -1 for <c>_last_checkpoint</c>.</param>
/// <param name="Entry">The listed file.</param>
/// <param name="Part">For a multi-part checkpoint, the one-based part number.</param>
/// <param name="NumParts">For a multi-part checkpoint, the number of parts.</param>
/// <param name="EndVersion">For a compacted commit, the last version it covers.</param>
public readonly record struct LogFile(
    LogFileKind Kind, long Version, FileEntry Entry, int Part = 0, int NumParts = 0, long EndVersion = -1)
{
    /// <summary>The name of the log directory inside a table.</summary>
    public const string LogDirectory = "_delta_log";

    /// <summary>The table-relative path of the commit file of a version.</summary>
    public static string CommitPath(long version) => $"{LogDirectory}/{version:D20}.json";

    /// <summary>Reads a log file name. Returns false for names the protocol does not define; those are to be ignored.</summary>
    public static bool TryParse(FileEntry entry, out LogFile file)
    {
        file = default;
        ReadOnlySpan<char> name = entry.Path.AsSpan();
        int slash = name.LastIndexOf('/');
        if (slash >= 0) name = name[(slash + 1)..];

        if (name.SequenceEqual("_last_checkpoint"))
        {
            file = new LogFile(LogFileKind.LastCheckpoint, -1, entry);
            return true;
        }

        if (!TryVersion(name, 20, out long version) || name.Length < 22 || name[20] != '.') return false;
        ReadOnlySpan<char> rest = name[21..];

        if (rest.SequenceEqual("json"))
        {
            file = new LogFile(LogFileKind.Commit, version, entry);
            return true;
        }
        if (rest.SequenceEqual("crc"))
        {
            file = new LogFile(LogFileKind.Checksum, version, entry);
            return true;
        }
        if (rest.SequenceEqual("checkpoint.parquet"))
        {
            file = new LogFile(LogFileKind.ClassicCheckpoint, version, entry);
            return true;
        }
        if (rest.EndsWith(".compacted.json"))
        {
            ReadOnlySpan<char> end = rest[..^".compacted.json".Length];
            if (end.Length == 20 && TryVersion(end, 20, out long endVersion) && endVersion >= version)
            {
                file = new LogFile(LogFileKind.CompactedCommit, version, entry, EndVersion: endVersion);
                return true;
            }
            return false;
        }
        if (rest.StartsWith("checkpoint."))
        {
            ReadOnlySpan<char> tail = rest["checkpoint.".Length..];
            if (tail.EndsWith(".parquet") && tail.Length == 10 + 1 + 10 + ".parquet".Length && tail[10] == '.'
                && TryVersion(tail, 10, out long part) && TryVersion(tail[11..], 10, out long parts)
                && part >= 1 && part <= parts)
            {
                file = new LogFile(LogFileKind.MultiPartCheckpoint, version, entry, Part: (int)part, NumParts: (int)parts);
                return true;
            }

            int dot = tail.LastIndexOf('.');
            if (dot > 0 && (tail[(dot + 1)..].SequenceEqual("json") || tail[(dot + 1)..].SequenceEqual("parquet"))
                && Guid.TryParse(tail[..dot], out _))
            {
                file = new LogFile(LogFileKind.V2Checkpoint, version, entry);
                return true;
            }
        }
        return false;
    }

    private static bool TryVersion(ReadOnlySpan<char> text, int digits, out long value)
    {
        value = 0;
        if (text.Length < digits) return false;
        for (int i = 0; i < digits; i++)
        {
            int d = text[i] - '0';
            if ((uint)d > 9) return false;
            value = value * 10 + d;
        }
        return true;
    }
}
