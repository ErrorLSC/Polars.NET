using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow;
/// <summary>
/// Extension methods for handling Apache Arrow Arrays.
/// Provides formatting and safe value extraction.
/// </summary>
public static partial class ArrowExtensions
{
    // ==========================================
    // 1. FormatValue
    // ==========================================
    /// <summary>
    /// Deal with Other Formats
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    public static string FormatValue(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return "null";

        return array switch
        {
            // Basic types
            Int8Array arr => arr.GetValue(index).ToString()!,
            Int16Array arr => arr.GetValue(index).ToString()!,
            Int32Array arr => arr.GetValue(index).ToString()!,
            Int64Array arr => arr.GetValue(index).ToString()!,
            UInt8Array arr => arr.GetValue(index).ToString()!,
            UInt16Array arr => arr.GetValue(index).ToString()!,
            UInt32Array arr => arr.GetValue(index).ToString()!,
            UInt64Array arr => arr.GetValue(index).ToString()!,
            HalfFloatArray arr => arr.GetValue(index).ToString()!,
            FloatArray arr => arr.GetValue(index).ToString()!,
            DoubleArray arr => arr.GetValue(index).ToString()!,
            Decimal128Array arr => arr.GetValue(index).ToString()!,
            Decimal256Array arr => arr.GetValue(index).ToString()!,
            DictionaryArray dictArr => $"\"{dictArr.GetStringValue(index)}\"",

            // Int128Array arr  => arr.GetValue(index).ToString()!,
            // UInt128Array arr  => arr.GetValue(index).ToString()!,

            // Strings
            StringArray sa => $"\"{sa.GetString(index)}\"",
            LargeStringArray lsa => $"\"{lsa.GetString(index)}\"",
            StringViewArray sva => $"\"{sva.GetString(index)}\"",

            // Bool
            BooleanArray arr => arr.GetValue(index).ToString()!.ToLower(),

            // Binary
            BinaryArray arr => FormatBinary(arr.GetBytes(index)),
            LargeBinaryArray arr => FormatBinary(arr.GetBytes(index)),

            // Datetime
            Date32Array arr => FormatDate32(arr, index),
            TimestampArray arr => FormatTimestamp(arr, index),
            Time32Array arr => FormatTime32(arr, index),
            Time64Array arr => FormatTime64(arr, index),
            DurationArray arr => FormatDuration(arr, index),

            // nested types
            ListArray arr => FormatList(arr, index),
            LargeListArray arr => FormatLargeList(arr, index),
            FixedSizeListArray arr => FormatFixedSizeList(arr, index),
            StructArray arr => FormatStruct(arr, index),

            _ => $"<{array.GetType().Name}>"
        };
    }

    // --- Helpers ---
    /// <summary>
    /// Deal with Values
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    public static long? GetInt64Value(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;
        return array switch
        {
            // Signed Integes
            Int8Array i8 => i8.GetValue(index),   // Polars Month/Day/Weekday is Int8
            Int16Array i16 => i16.GetValue(index),
            Int32Array i32 => i32.GetValue(index),
            Int64Array i64 => i64.GetValue(index),

            // Unsigned Integers
            UInt8Array u8 => u8.GetValue(index),
            UInt16Array u16 => u16.GetValue(index),
            UInt32Array u32 => u32.GetValue(index),
            UInt64Array u64 => (long?)u64.GetValue(index),

            // DateTime
            TimestampArray ts => ts.GetValue(index),
            Date32Array d32 => d32.GetValue(index), // Days
            Date64Array d64 => d64.GetValue(index), // Milliseconds
            Time32Array t32 => t32.GetValue(index),
            Time64Array t64 => t64.GetValue(index),
            DurationArray dur => dur.GetValue(index),
            _ => null
        };
    }

    /// <summary>
    /// Deal with Double Values
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    public static double? GetDoubleValue(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;
        return array switch
        {
            DoubleArray d => d.GetValue(index),
            FloatArray f => f.GetValue(index),
            HalfFloatArray h => (double?)h.GetValue(index),
            Int64Array i => i.GetValue(index),
            Int32Array i => i.GetValue(index),
            _ => null
        };
    }
    /// <summary>
    /// Deal with String Values
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    public static string? GetStringValue(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;
        return array switch
        {
            StringArray sa => sa.GetString(index),
            LargeStringArray lsa => lsa.GetString(index),
            StringViewArray sva => sva.GetString(index),
            DictionaryArray dictArr => UnpackDictionary(dictArr, index),
            _ => null
        };
    }
    /// <summary>
    /// Get Decimal value.
    /// Note: Arrow Decimal128 is converted to C# decimal.
    /// Use with caution if precision > 28.
    /// </summary>
    public static decimal? GetDecimalValue(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        return array switch
        {
            // Native Decimal (128-bit)
            Decimal128Array d128 => d128.GetValue(index),

            // Fallback: Cast other numerics to decimal
            DoubleArray d => (decimal)d.GetValue(index)!,
            FloatArray f => (decimal)f.GetValue(index)!,
            Int64Array i64 => (decimal)i64.GetValue(index)!,
            Int32Array i32 => (decimal)i32.GetValue(index)!,

            _ => null
        };
    }
    private static string? UnpackDictionary(DictionaryArray dictArr, int index)
    {
        // Get Key (Index)
        var keys = dictArr.Indices;
        long? key = keys.GetInt64Value(index);

        if (!key.HasValue) return null;

        // Get Dictionary (Values)
        var values = dictArr.Dictionary;
        return values.GetStringValue((int)key.Value);
    }
    private static readonly char[] HexLookup = new[]
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static string FormatBinary(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return "x''";

        bool truncated = false;
        ReadOnlySpan<byte> target = bytes;

        if (bytes.Length > 20)
        {
            target = bytes[..20];
            truncated = true;
        }

        char[] chars = new char[target.Length * 2];

        for (int i = 0; i < target.Length; i++)
        {
            byte b = target[i];
            chars[i * 2] = HexLookup[b >> 4];
            chars[i * 2 + 1] = HexLookup[b & 0xF];
        }

        string hex = new(chars);
        return truncated ? $"x'{hex}...'" : $"x'{hex}'";
    }

    private static string FormatDate32(Date32Array arr, int index)
    {
        int days = arr.GetValue(index) ?? 0;
        return new DateTime(1970, 1, 1).AddDays(days).ToString("yyyy-MM-dd");
    }

    private static string FormatTimestamp(TimestampArray arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as TimestampType)?.Unit;
        long ticks = unit switch
        {
            TimeUnit.Nanosecond => v / 100L,
            TimeUnit.Microsecond => v * 10L,
            TimeUnit.Millisecond => v * 10000L,
            TimeUnit.Second => v * 10000000L,
            _ => v
        };
        try { return DateTime.UnixEpoch.AddTicks(ticks).ToString("yyyy-MM-dd HH:mm:ss.ffffff"); }
        catch { return v.ToString(); }
    }

    private static string FormatTime32(Time32Array arr, int index)
    {
        int v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as Time32Type)?.Unit;
        var span = unit switch { TimeUnit.Millisecond => TimeSpan.FromMilliseconds(v), _ => TimeSpan.FromSeconds(v) };
        return span.ToString();
    }

    private static string FormatTime64(Time64Array arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as Time64Type)?.Unit;
        long ticks = unit switch { TimeUnit.Nanosecond => v / 100L, _ => v * 10L };
        return TimeSpan.FromTicks(ticks).ToString();
    }

    private static string FormatDuration(DurationArray arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as DurationType)?.Unit;
        string suffix = unit switch
        {
            TimeUnit.Nanosecond => "ns",
            TimeUnit.Microsecond => "us",
            TimeUnit.Millisecond => "ms",
            TimeUnit.Second => "s",
            _ => ""
        };
        return $"{v}{suffix}";
    }

    private static string FormatList(ListArray arr, int index)
    {
        int start = arr.ValueOffsets[index];
        int end = arr.ValueOffsets[index + 1];
        var items = Enumerable.Range(start, end - start).Select(i => arr.Values.FormatValue(i));
        return $"[{string.Join(", ", items)}]";
    }

    private static string FormatLargeList(LargeListArray arr, int index)
    {
        int start = (int)arr.ValueOffsets[index];
        int end = (int)arr.ValueOffsets[index + 1];
        var items = Enumerable.Range(start, end - start).Select(i => arr.Values.FormatValue(i));
        return $"[{string.Join(", ", items)}]";
    }
    private static string FormatFixedSizeList(FixedSizeListArray arr, int index)
    {
        var type = (FixedSizeListType)arr.Data.DataType;
        int width = type.ListSize;

        int start = index * width;
        int count = width;

        var items = Enumerable.Range(start, count)
            .Select(i => arr.Values.FormatValue(i));

        return $"[{string.Join(", ", items)}]";
    }
    private static string FormatStruct(StructArray arr, int index)
    {
        var structType = arr.Data.DataType as StructType;
        if (structType == null) return "{}";
        var fields = structType.Fields.Select((field, i) =>
            $"{field.Name}: {arr.Fields[i].FormatValue(index)}");
        return $"{{{string.Join(", ", fields)}}}";
    }

    // ==========================================
    // 3. Typed Accessors (Casting to C# Types)
    // ==========================================

    /// <summary>
    /// Fetch DateTime object. Automatically handles Arrow Time Unit.
    /// </summary>
    public static DateTime? GetDateTime(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        return array switch
        {
            // Timestamp
            TimestampArray tsArr => ConvertTimestamp(tsArr, index),

            // Date32 (Days since epoch)
            Date32Array d32 => new DateTime(1970, 1, 1).AddDays(d32.GetValue(index)!.Value),

            // Date64 (Milliseconds since epoch)
            Date64Array d64 => new DateTime(1970, 1, 1).AddMilliseconds(d64.GetValue(index)!.Value),

            _ => null
        };
    }

    /// <summary>
    /// Fetch TimeSpan object. Automatically handles Arrow Time Unit.
    /// </summary>
    public static TimeSpan? GetTimeSpan(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        return array switch
        {
            // Time32 (s or ms)
            Time32Array t32 => ConvertTime32(t32, index),

            // Time64 (us or ns)
            Time64Array t64 => ConvertTime64(t64, index),

            // Duration
            DurationArray dur => ConvertDuration(dur, index),

            _ => null
        };
    }
    private static readonly int UnixEpochDayNumber = new DateOnly(1970, 1, 1).DayNumber;

    public static DateOnly? GetDateOnly(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        if (array is Date32Array d32)
        {
            int daysSinceEpoch = d32.GetValue(index)!.Value;
            return DateOnly.FromDayNumber(UnixEpochDayNumber + daysSinceEpoch);
        }

        if (array is Date64Array d64)
        {
            var dt = new DateTime(1970, 1, 1).AddMilliseconds(d64.GetValue(index)!.Value);
            return DateOnly.FromDateTime(dt);
        }

        return null;
    }

    public static TimeOnly? GetTimeOnly(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        // Time32 (Seconds / Milliseconds)
        if (array is Time32Array t32)
        {
            var ms = t32.GetMilliSeconds(index); // Arrow Helper
            if (ms.HasValue)
                return new TimeOnly(0, 0, 0).Add(TimeSpan.FromMilliseconds(ms.Value));

            return null;
        }

        // Time64 (Microseconds / Nanoseconds)
        if (array is Time64Array t64)
        {
            long v = t64.GetValue(index)!.Value;
            var unit = (t64.Data.DataType as Time64Type)?.Unit;

            long ticks = unit switch
            {
                TimeUnit.Nanosecond => v / 100L, // 100ns = 1 tick
                _ => v * 10L // Microsecond -> 100ns
            };
            return new TimeOnly(ticks);
        }

        return null;
    }
    /// <summary>
    /// Use Pre-Set TimeZoneInfo
    /// </summary>
    public static DateTimeOffset? GetDateTimeOffsetOptimized(this IArrowArray array, int index, TimeZoneInfo? tzi)
    {
        if (array.IsNull(index)) return null;

        if (array is TimestampArray tsArr)
        {
            long? v = tsArr.GetValue(index);
            if (!v.HasValue) return null;

            var unit = (tsArr.Data.DataType as TimestampType)?.Unit;

            // A. Calculate UTC Ticks
            long ticks = unit switch
            {
                TimeUnit.Nanosecond => v.Value / 100L,
                TimeUnit.Microsecond => v.Value * 10L,
                TimeUnit.Millisecond => v.Value * 10000L,
                TimeUnit.Second => v.Value * 10000000L,
                _ => v.Value
            };
            long utcTicks = DateTime.UnixEpoch.Ticks + ticks;

            // Calculate Offset
            TimeSpan offset = TimeSpan.Zero;
            if (tzi != null)
            {
                offset = tzi.GetUtcOffset(new DateTime(utcTicks, DateTimeKind.Utc));
            }

            return new DateTimeOffset(utcTicks, TimeSpan.Zero).ToOffset(offset);
        }

        if (array is Date32Array d32)
        {
            int? days = d32.GetValue(index);
            if (!days.HasValue) return null;
            return new DateTimeOffset(new DateTime(1970, 1, 1).AddDays(days.Value), TimeSpan.Zero);
        }
        if (array is Date64Array d64)
        {
            long? ms = d64.GetValue(index);
            if (!ms.HasValue) return null;
            return new DateTimeOffset(new DateTime(1970, 1, 1).AddMilliseconds(ms.Value), TimeSpan.Zero);
        }


        return null;
    }

    // ==========================================
    // Internal Conversion Logic
    // ==========================================
    private static readonly DateTime EpochNaive = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static DateTime ConvertTimestamp(TimestampArray arr, int index)
    {
        long v = arr.GetValue(index).GetValueOrDefault();

        // Check Timezone
        var type = arr.Data.DataType as TimestampType;
        var unit = type?.Unit;

        // C# DateTime Ticks = 100ns
        long ticks = unit switch
        {
            TimeUnit.Nanosecond => v / 100L,
            TimeUnit.Microsecond => v * 10L,
            TimeUnit.Millisecond => v * 10000L,
            TimeUnit.Second => v * 10000000L,
            _ => v
        };

        try
        {
            // 1. Convert to Wall Time (Unspecified)
            var dt = EpochNaive.AddTicks(ticks);

            // 2. Check TimeZone

            if (!string.IsNullOrEmpty(type?.Timezone))
            {
                return DateTime.UnixEpoch.AddTicks(ticks);
            }

            return dt;
        }
        catch (ArgumentOutOfRangeException)
        {
            return v > 0 ? DateTime.MaxValue : DateTime.MinValue;
        }
    }

    private static TimeSpan ConvertTime32(Time32Array arr, int index)
    {
        int v = arr.GetValue(index).GetValueOrDefault();
        var unit = (arr.Data.DataType as Time32Type)?.Unit;
        return unit switch
        {
            TimeUnit.Millisecond => TimeSpan.FromMilliseconds(v),
            _ => TimeSpan.FromSeconds(v)
        };
    }

    private static TimeSpan ConvertTime64(Time64Array arr, int index)
    {
        long v = arr.GetValue(index).GetValueOrDefault();
        var unit = (arr.Data.DataType as Time64Type)?.Unit;

        long ticks = unit switch
        {
            TimeUnit.Nanosecond => v / 100L,
            _ => v * 10L // Microsecond
        };
        return TimeSpan.FromTicks(ticks);
    }

    private static TimeSpan ConvertDuration(DurationArray arr, int index)
    {
        long v = arr.GetValue(index).GetValueOrDefault();
        var unit = (arr.Data.DataType as DurationType)?.Unit;

        long ticks = unit switch
        {
            TimeUnit.Nanosecond => v / 100L,
            TimeUnit.Microsecond => v * 10L,
            TimeUnit.Millisecond => v * 10000L,
            TimeUnit.Second => v * 10000000L,
            _ => v
        };
        return TimeSpan.FromTicks(ticks);
    }
}