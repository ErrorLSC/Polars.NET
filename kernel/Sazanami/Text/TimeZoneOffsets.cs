using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// The UTC offset of a time zone at any instant, to the second: a zone named as Arrow timestamps name it (an IANA
/// name, <c>UTC</c>, or a fixed <c>±HH:MM</c>). IANA zones are read from the system's time zone database files
/// (TZif, as the reference dataframe engine's own database has them: local mean time with its seconds before the
/// first transition), and through <see cref="TimeZoneInfo"/> past the last transition or where there are no such
/// files (Windows: offsets in whole minutes).
/// </summary>
internal sealed class TimeZoneOffsets
{
    private static readonly ConcurrentDictionary<string, TimeZoneOffsets?> Zones = new(StringComparer.Ordinal);
    private static readonly string[] Directories = [Environment.GetEnvironmentVariable("TZDIR") ?? "", "/usr/share/zoneinfo", "/usr/lib/zoneinfo", "/usr/share/lib/zoneinfo"];

    private readonly int _fixed;
    private readonly long[] _transitions = [];  // UTC seconds, ascending
    private readonly int[] _offsets = [];       // the offset from each transition on
    private readonly int _before;               // the offset before the first transition
    private readonly TimeZoneInfo? _info;       // past the last transition, or the whole zone without files

    private const long Year2000 = 946_684_800, Year2400 = 13_569_465_600, FourCenturies = 12_622_780_800;

    private TimeZoneOffsets(int offset) => _fixed = offset;

    private TimeZoneOffsets(long[] transitions, int[] offsets, int before, TimeZoneInfo? info)
    {
        _transitions = transitions;
        _offsets = offsets;
        _before = before;
        _info = info;
    }

    private TimeZoneOffsets(TimeZoneInfo info) => _info = info;

    /// <summary>The zone named <paramref name="name"/>, or null when it is not known.</summary>
    public static TimeZoneOffsets? For(string name) => Zones.GetOrAdd(name, Load);

    /// <summary>The offset east of UTC, in seconds, at <paramref name="utcSeconds"/> since 1970-01-01T00:00:00Z.</summary>
    public int OffsetAt(long utcSeconds)
    {
        if (_transitions.Length > 0)
        {
            if (utcSeconds < _transitions[0]) return _before;
            if (_info is null || utcSeconds < _transitions[^1])
            {
                int i = Array.BinarySearch(_transitions, utcSeconds);
                return _offsets[i >= 0 ? i : ~i - 1];
            }
        }
        if (_info is null) return _fixed;
        // Rules repeat with the calendar, every 400 years (a whole number of weeks): instants past 2400 are read 400 years
        // at a time earlier (TimeZoneInfo also errs near the end of DateTime's range); before DateTime's, its start.
        if (utcSeconds >= Year2400) utcSeconds = Year2000 + (utcSeconds - Year2000) % FourCenturies;
        long ticks = Math.Max(utcSeconds, -62_135_596_800L) * TimeSpan.TicksPerSecond + DateTime.UnixEpoch.Ticks;
        return (int)_info.GetUtcOffset(new DateTime(ticks, DateTimeKind.Utc)).TotalSeconds;
    }

    private static TimeZoneOffsets? Load(string name)
    {
        if (name is "UTC" or "Z" or "Etc/UTC" or "utc") return new TimeZoneOffsets(0);
        if (FixedOffset(name) is int offset) return new TimeZoneOffsets(offset);
        if (name.Length == 0 || name.Contains("..", StringComparison.Ordinal) || name[0] == '/') return null;
        TimeZoneInfo? info = null;
        try { info = TimeZoneInfo.FindSystemTimeZoneById(name); }
        catch (Exception e) when (e is TimeZoneNotFoundException or InvalidTimeZoneException) { }
        foreach (string directory in Directories)
        {
            if (directory.Length == 0) continue;
            string path = Path.Combine(directory, name);
            if (!File.Exists(path)) continue;
            try
            {
                if (ReadTzif(File.ReadAllBytes(path), info) is { } zone) return zone;
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
        return info is null ? null : new TimeZoneOffsets(info);
    }

    // ±HH:MM (or ±HHMM, ±HH) as seconds east of UTC.
    private static int? FixedOffset(string name)
    {
        if (name.Length is not (3 or 5 or 6) || name[0] is not ('+' or '-')) return null;
        ReadOnlySpan<char> rest = name.AsSpan(1);
        if (!int.TryParse(rest[..2], System.Globalization.NumberStyles.None, null, out int hours)) return null;
        int minutes = 0;
        if (rest.Length > 2)
        {
            ReadOnlySpan<char> m = rest[^2..];
            if (rest.Length == 5 && rest[2] != ':' || !int.TryParse(m, System.Globalization.NumberStyles.None, null, out minutes)) return null;
        }
        if (hours > 23 || minutes > 59) return null;
        int seconds = hours * 3600 + minutes * 60;
        return name[0] == '-' ? -seconds : seconds;
    }

    // A TZif file (RFC 8536), version 2 or later: its 64-bit transitions and the offsets of their local time types.
    private static TimeZoneOffsets? ReadTzif(ReadOnlySpan<byte> file, TimeZoneInfo? info)
    {
        if (file.Length < 44 || !file.StartsWith("TZif"u8) || file[4] < (byte)'2') return null;
        // Skip the version 1 data block (32-bit times).
        var (isUt, isStd, leaps, times, types, chars) = Counts(file);
        int v1 = 44 + times * 4 + times + types * 6 + chars + leaps * 8 + isStd + isUt;
        file = file[v1..];
        if (file.Length < 44 || !file.StartsWith("TZif"u8)) return null;
        (_, _, _, times, types, _) = Counts(file);
        ReadOnlySpan<byte> data = file[44..];
        if (types == 0 || data.Length < times * 9 + types * 6) return null;
        var transitions = new long[times];
        var offsets = new int[times];
        if (types > 256) return null; // type indices are bytes
        Span<int> typeOffsets = stackalloc int[256];
        for (int t = 0; t < types; t++) typeOffsets[t] = BinaryPrimitives.ReadInt32BigEndian(data.Slice(times * 9 + t * 6, 4));
        for (int i = 0; i < times; i++)
        {
            transitions[i] = BinaryPrimitives.ReadInt64BigEndian(data.Slice(i * 8, 8));
            int type = data[times * 8 + i];
            if (type >= types) return null;
            offsets[i] = typeOffsets[type];
        }
        // Before the first transition: local time type 0 (local mean time, in practice).
        int before = typeOffsets[0];
        if (times == 0) return new TimeZoneOffsets([long.MaxValue], [before], before, info);
        return new TimeZoneOffsets(transitions, offsets, before, info);

        static (int, int, int, int, int, int) Counts(ReadOnlySpan<byte> header) =>
            (Count(header, 20), Count(header, 24), Count(header, 28), Count(header, 32), Count(header, 36), Count(header, 40));

        static int Count(ReadOnlySpan<byte> header, int at) => checked((int)BinaryPrimitives.ReadUInt32BigEndian(header.Slice(at, 4)));
    }
}
