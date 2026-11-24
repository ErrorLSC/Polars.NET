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
    // Abbreviations (TZif files): from each transition on, before the first, and past the last (the footer's rule:
    // standard time, daylight time).
    private readonly string[] _names = [];
    private readonly string? _beforeName, _standardName, _daylightName, _fixedName;

    private const long Year2000 = 946_684_800, Year2400 = 13_569_465_600, FourCenturies = 12_622_780_800;

    private TimeZoneOffsets(int offset, string name)
    {
        _fixed = offset;
        _fixedName = name;
    }

    private TimeZoneOffsets(long[] transitions, int[] offsets, int before, TimeZoneInfo? info, string[] names, string beforeName,
        string? standardName, string? daylightName)
    {
        _transitions = transitions;
        _offsets = offsets;
        _before = before;
        _info = info;
        _names = names;
        _beforeName = beforeName;
        _standardName = standardName;
        _daylightName = daylightName;
    }

    private TimeZoneOffsets(TimeZoneInfo info) => _info = info;

    /// <summary>The zone named <paramref name="name"/>, or null when it is not known.</summary>
    public static TimeZoneOffsets? For(string name) => Zones.GetOrAdd(name, Load);

    /// <summary>Whether the zone's abbreviations are known (<see cref="AbbreviationAt"/>): fixed offsets, and zones read from TZif files.</summary>
    public bool HasAbbreviations => _fixedName is not null || _names.Length > 0 && (_info is null || _standardName is not null);

    /// <summary>
    /// The zone's abbreviation at <paramref name="utcSeconds"/> (JST, EDT, LMT, or digits such as +1030 where the time
    /// zone database has no letters; UTC; a fixed offset's own text), or null when not known.
    /// </summary>
    public string? AbbreviationAt(long utcSeconds)
    {
        if (_fixedName is not null) return _fixedName;
        if (_names.Length == 0) return null;
        if (utcSeconds < _transitions[0]) return _beforeName;
        if (_info is null || utcSeconds < _transitions[^1])
        {
            int i = Array.BinarySearch(_transitions, utcSeconds);
            return _names[i >= 0 ? i : ~i - 1];
        }
        if (_daylightName is null) return _standardName;
        if (utcSeconds >= Year2400) utcSeconds = Year2000 + (utcSeconds - Year2000) % FourCenturies;
        long ticks = Math.Max(utcSeconds, -62_135_596_800L) * TimeSpan.TicksPerSecond + DateTime.UnixEpoch.Ticks;
        return _info.IsDaylightSavingTime(new DateTime(ticks, DateTimeKind.Utc)) ? _daylightName : _standardName;
    }

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
        if (name is "UTC" or "Z" or "Etc/UTC" or "utc") return new TimeZoneOffsets(0, "UTC");
        if (FixedOffset(name) is int offset) return new TimeZoneOffsets(offset, FixedName(offset));
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

    // A fixed offset's abbreviation, as the reference engine's time library writes it: +09:00.
    private static string FixedName(int seconds) => $"{(seconds < 0 ? '-' : '+')}{Math.Abs(seconds) / 3600:D2}:{Math.Abs(seconds) / 60 % 60:D2}";

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
        (isUt, isStd, leaps, times, types, chars) = Counts(file);
        ReadOnlySpan<byte> data = file[44..];
        if (types == 0 || data.Length < times * 9 + types * 6 + chars) return null;
        var transitions = new long[times];
        var offsets = new int[times];
        if (types > 256) return null; // type indices are bytes
        Span<int> typeOffsets = stackalloc int[256];
        for (int t = 0; t < types; t++) typeOffsets[t] = BinaryPrimitives.ReadInt32BigEndian(data.Slice(times * 9 + t * 6, 4));
        // Each type's abbreviation: NUL-terminated text in the designations after the types.
        ReadOnlySpan<byte> designations = data.Slice(times * 9 + types * 6, chars);
        var typeNames = new string[types];
        for (int t = 0; t < types; t++)
        {
            int at = Math.Min(data[times * 9 + t * 6 + 5], designations.Length);
            ReadOnlySpan<byte> text = designations[at..];
            int end = text.IndexOf((byte)0);
            typeNames[t] = System.Text.Encoding.ASCII.GetString(end < 0 ? text : text[..end]);
        }
        var names = new string[times];
        for (int i = 0; i < times; i++)
        {
            transitions[i] = BinaryPrimitives.ReadInt64BigEndian(data.Slice(i * 8, 8));
            int type = data[times * 8 + i];
            if (type >= types) return null;
            offsets[i] = typeOffsets[type];
            names[i] = typeNames[type];
        }
        // Before the first transition: local time type 0 (local mean time, in practice).
        int before = typeOffsets[0];
        // The footer (\n POSIX TZ rule \n): the abbreviations past the last transition.
        int footer = times * 9 + types * 6 + chars + leaps * 12 + isStd + isUt;
        (string? standard, string? daylight) = footer < data.Length ? FooterNames(data[footer..]) : (null, null);
        if (times == 0) return new TimeZoneOffsets([long.MaxValue], [before], before, info, [typeNames[0]], typeNames[0], standard ?? typeNames[0], daylight);
        return new TimeZoneOffsets(transitions, offsets, before, info, names, typeNames[0], standard ?? names[^1], daylight);

        // "\nEST5EDT,M3.2.0,M11.1.0\n" → (EST, EDT); "\n<+11>-11\n" → (+11, null).
        static (string?, string?) FooterNames(ReadOnlySpan<byte> footer)
        {
            if (footer.Length < 2 || footer[0] != '\n') return (null, null);
            footer = footer[1..];
            int end = footer.IndexOf((byte)'\n');
            if (end >= 0) footer = footer[..end];
            string? standard = Name(ref footer);
            if (standard is null) return (null, null);
            // The offset after the standard name, then the daylight name (if any).
            while (!footer.IsEmpty && (footer[0] is (byte)'+' or (byte)'-' or (byte)':' || char.IsAsciiDigit((char)footer[0]))) footer = footer[1..];
            return (standard, footer.IsEmpty || footer[0] == ',' ? null : Name(ref footer));
        }

        static string? Name(ref ReadOnlySpan<byte> rule)
        {
            if (rule.IsEmpty) return null;
            if (rule[0] == '<')
            {
                int close = rule.IndexOf((byte)'>');
                if (close < 0) return null;
                string quoted = System.Text.Encoding.ASCII.GetString(rule[1..close]);
                rule = rule[(close + 1)..];
                return quoted;
            }
            int n = 0;
            while (n < rule.Length && char.IsAsciiLetter((char)rule[n])) n++;
            if (n == 0) return null;
            string letters = System.Text.Encoding.ASCII.GetString(rule[..n]);
            rule = rule[n..];
            return letters;
        }

        static (int, int, int, int, int, int) Counts(ReadOnlySpan<byte> header) =>
            (Count(header, 20), Count(header, 24), Count(header, 28), Count(header, 32), Count(header, 36), Count(header, 40));

        static int Count(ReadOnlySpan<byte> header, int at) => checked((int)BinaryPrimitives.ReadUInt32BigEndian(header.Slice(at, 4)));
    }
}
