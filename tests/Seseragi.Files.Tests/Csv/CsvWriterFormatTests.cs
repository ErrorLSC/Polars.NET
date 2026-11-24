using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// .NET date, time and timestamp formats: the writer writes what <see cref="DateOnly"/>, <see cref="TimeOnly"/>,
/// <see cref="DateTime"/> and <see cref="DateTimeOffset"/> write with the invariant culture, and refuses (when created)
/// the formats .NET refuses — and offsets of timestamps without a zone, which .NET would take from the machine.
/// </summary>
public class CsvWriterFormatTests
{
    private const int Rows = 300;

    private static readonly string[] Formats =
    [
        "yyyy-MM-dd", "dd/MM/yyyy", "d", "D", "M/d/yy", "MMM d, yyyy", "dddd, MMMM dd", "ddd", "yyy", "yyyyy", "y", "yy", "MM", "M", "m", "Y",
        "g", "gg yyyy", "o", "O", "r", "R", "s", "u", "f", "F", "G", "t", "T", "HH:mm:ss.fff", "h:mm tt", "hh:mm:ss.FFFFFFF", "H:m:s",
        "HH:mm:ss.FFF", "ss.ffffff", "s.F", "t tt", "yyyy-MM-ddTHH:mm:ss.fffffffzzz", "zz", "z", "zzz", "K", "'Q'yyyy\\'", "\"lit\" HH",
        "yyyy'-'MM", "\\d d", "hh 'h'", "fffffff", "FFFFFFF", "HH:mm:ss.FFFFFFF z", "yyyy:MM", "HH/mm", "yyyy-MM-ddTHH:mm:ssK", "MMMMM ddddd",
        "hhh HHH mmm sss", "tt", "U", "ffffffff",
    ];

    private static readonly TimeZoneInfo NewYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");

    public static TheoryData<string> Targets() => ["date", "time", "naive", "zoned"];

    [Theory]
    [MemberData(nameof(Targets))]
    public void NetFormats_WriteWhatNetWrites(string target)
    {
        var random = new Random(target.GetHashCode(StringComparison.Ordinal) ^ 20261124);
        (IArrowType type, long[] values) = target switch
        {
            "date" => ((IArrowType)Date32Type.Default, Enumerable.Range(0, Rows).Select(_ => (long)random.Next(DateOnly.MinValue.DayNumber, DateOnly.MaxValue.DayNumber + 1) - 719_162).ToArray()),
            "time" => (new Time64Type(TimeUnit.Nanosecond), Enumerable.Range(0, Rows).Select(_ => random.NextInt64(86_400_000_000_000L)).ToArray()),
            "naive" => (new TimestampType(TimeUnit.Nanosecond, (string?)null), Enumerable.Range(0, Rows).Select(_ => random.NextInt64(-2_208_988_800L, 7_258_118_400L) * 1_000_000_000 + random.Next(1_000_000_000)).ToArray()),
            _ => (new TimestampType(TimeUnit.Microsecond, "America/New_York"), Enumerable.Range(0, Rows).Select(_ => random.NextInt64(-631_152_000L, 4_102_444_800L) * 1_000_000 + random.Next(1_000_000)).ToArray()),
        };
        // A whole second and a fraction with trailing zeros first (F leaves out a zero fraction and its point).
        if (target != "date")
        {
            long unit = target == "zoned" ? 1_000_000 : 1_000_000_000;
            values[0] = values[0] / unit * unit;
            values[1] = values[1] / unit * unit + unit / 1000 * 120;
        }
        var schema = new Schema([new Field("v", type, false)], null);
        using var batch = new RecordBatch(schema, [Array(type, values)], Rows);
        List<string> failures = [];
        foreach (string format in Formats)
        {
            string[]? expected;
            try { expected = [.. values.Select(v => Net(target, v, format))]; }
            catch (FormatException) { expected = null; }
            // Offsets of timestamps without a zone, and their universal time (U): .NET takes the machine's zone; the writer
            // refuses them.
            if (target == "naive" && (format.Contains('z', StringComparison.Ordinal) && !format.Contains('\'', StringComparison.Ordinal) || format == "U")) expected = null;
            string[]? actual;
            try
            {
                var options = new CsvWriteOptions { IncludeHeader = false, Separator = '\t', DateFormat = format, TimeFormat = format, DatetimeFormat = format };
                actual = CsvWriter.WriteString(schema, [batch], options).Split('\n')[..Rows];
            }
            catch (ArgumentException) { actual = null; }
            if (expected is null != actual is null)
                failures.Add($"'{format}': .NET {(expected is null ? "refuses it" : "writes " + expected[0])}, the writer {(actual is null ? "refuses it" : "writes " + actual[0])}");
            else if (expected is not null && actual is not null)
            {
                int at = Enumerable.Range(0, Rows).FirstOrDefault(i => expected[i] != actual[i], -1);
                if (at >= 0) failures.Add($"'{format}' value {values[at]}: .NET '{expected[at]}', the writer '{actual[at]}'");
            }
        }
        Assert.True(failures.Count == 0, string.Join("\n", failures));
    }

    private static string Net(string target, long value, string format) => target switch
    {
        "date" => DateOnly.FromDayNumber((int)(value + 719_162)).ToString(format, CultureInfo.InvariantCulture),
        "time" => new TimeOnly(value / 100).ToString(format, CultureInfo.InvariantCulture),
        // Ticks: nanoseconds floored to 100 ns (before 1970 too).
        "naive" => new DateTime(DateTime.UnixEpoch.Ticks + Math.DivRem(value, 100, out long rest) - (rest < 0 ? 1 : 0), DateTimeKind.Unspecified).ToString(format, CultureInfo.InvariantCulture),
        _ => TimeZoneInfo.ConvertTime(new DateTimeOffset(DateTime.UnixEpoch.Ticks + value * 10, TimeSpan.Zero), NewYork).ToString(format, CultureInfo.InvariantCulture),
    };

    private static IArrowArray Array(IArrowType type, long[] values)
    {
        byte[] data = type is Date32Type
            ? [.. values.SelectMany(v => BitConverter.GetBytes((int)v))]
            : [.. values.SelectMany(BitConverter.GetBytes)];
        return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Length, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(data)]));
    }
}
