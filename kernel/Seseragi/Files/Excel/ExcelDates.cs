namespace Kaminari.Seseragi.Files.Excel;

/// <summary>
/// Excel serial numbers (days since the epoch, the time of day as the fraction) to and from Unix microseconds.
/// </summary>
/// <remarks>
/// In the 1900 system serial 1 is 1900-01-01 and serial 60 is the non-existent 1900-02-29 that Lotus 1-2-3 counted
/// (read as 1900-02-28, never written); serials from 61 on are days since 1899-12-30. In the 1904 system serial 0 is
/// 1904-01-01. The whole days and the fraction are converted separately, so that a microsecond written as the
/// nearest double reads back exactly while one unit in the last place of the serial stays below half a microsecond
/// (serials below 65,536, that is through 2079-06-05).
/// </remarks>
internal static class ExcelDates
{
    public const long MicrosPerDay = 86_400_000_000;

    // Days from 1970-01-01 back to the day serial 0 stands for, counting from serial 61 on in the 1900 system.
    private const long Epoch1900 = -25_569;  // 1899-12-30
    private const long Epoch1904 = -24_107;  // 1904-01-01

    /// <summary>The Unix day of 9999-12-31, the last day Excel shows.</summary>
    public const long MaxUnixDay = 2_932_896;

    /// <summary>
    /// Splits a serial number into Unix days and the microseconds of the day (rounded to the nearest microsecond).
    /// Returns false for values outside Excel's date range (negative, or past 9999-12-31).
    /// </summary>
    public static bool TrySplit(double serial, bool date1904, out long unixDays, out long microsOfDay)
    {
        unixDays = 0;
        microsOfDay = 0;
        if (!(serial >= 0) || serial >= 3_000_000) return false;
        double whole = Math.Floor(serial);
        long days = (long)whole;
        long micros = (long)Math.Round((serial - whole) * MicrosPerDay, MidpointRounding.AwayFromZero);
        if (micros == MicrosPerDay)
        {
            days++;
            micros = 0;
        }
        unixDays = UnixDays(days, date1904);
        microsOfDay = micros;
        return unixDays <= MaxUnixDay;
    }

    /// <summary>Microseconds since the Unix epoch for a serial number; false when out of range.</summary>
    public static bool TryToUnixMicros(double serial, bool date1904, out long micros)
    {
        if (!TrySplit(serial, date1904, out long days, out long ofDay))
        {
            micros = 0;
            return false;
        }
        micros = days * MicrosPerDay + ofDay;
        return true;
    }

    /// <summary>A duration in microseconds (the serial counts days; any sign), rounded to the nearest microsecond.</summary>
    public static bool TryToDurationMicros(double serial, out long micros)
    {
        micros = 0;
        if (!double.IsFinite(serial) || Math.Abs(serial) > 100_000_000) return false;
        double whole = Math.Truncate(serial);
        micros = (long)whole * MicrosPerDay + (long)Math.Round((serial - whole) * MicrosPerDay, MidpointRounding.AwayFromZero);
        return true;
    }

    /// <summary>Unix days of a whole-day serial number.</summary>
    public static long UnixDays(long serialDays, bool date1904)
    {
        if (date1904) return Epoch1904 + serialDays;
        // Serials 1..59 are 1900-01-01..1900-02-28 (one day later than days since 1899-12-30 would say); serial 60
        // (the fictitious 1900-02-29) reads as 1900-02-28; serial 0 is 1899-12-31.
        return serialDays switch
        {
            < 60 => Epoch1900 + serialDays + 1,
            60 => Epoch1900 + 60,
            _ => Epoch1900 + serialDays,
        };
    }

    /// <summary>
    /// The serial number of a moment (1900 system), given as Unix days and microseconds of the day. Returns false
    /// before 1900-01-01 or after 9999-12-31, which Excel cannot show as dates.
    /// </summary>
    public static bool TryToSerial(long unixDays, long microsOfDay, out double serial)
    {
        serial = 0;
        // 1900-01-01 is Unix day -25567; 1900-03-01 is -25508.
        if (unixDays < -25_567 || unixDays > MaxUnixDay) return false;
        long days = unixDays - Epoch1900;
        if (unixDays < -25_508) days--; // before 1900-03-01: the fictitious 1900-02-29 is not yet counted
        serial = days + microsOfDay / (double)MicrosPerDay;
        return true;
    }

    /// <summary>Splits Unix microseconds into whole days (floored) and microseconds of the day.</summary>
    public static (long Days, long MicrosOfDay) SplitMicros(long micros)
    {
        long days = Math.DivRem(micros, MicrosPerDay, out long rem);
        if (rem < 0)
        {
            days--;
            rem += MicrosPerDay;
        }
        return (days, rem);
    }
}
