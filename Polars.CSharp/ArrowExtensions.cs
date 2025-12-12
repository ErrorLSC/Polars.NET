using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.CSharp;
/// <summary>
///  Deal types of Arrow and C#
/// </summary>
public static class ArrowExtensions
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
            // 基础数值
            Int8Array arr   => arr.GetValue(index).ToString()!,
            Int16Array arr  => arr.GetValue(index).ToString()!,
            Int32Array arr  => arr.GetValue(index).ToString()!,
            Int64Array arr  => arr.GetValue(index).ToString()!,
            UInt8Array arr  => arr.GetValue(index).ToString()!,
            UInt16Array arr => arr.GetValue(index).ToString()!,
            UInt32Array arr => arr.GetValue(index).ToString()!,
            UInt64Array arr => arr.GetValue(index).ToString()!,
            FloatArray arr  => arr.GetValue(index).ToString()!,
            DoubleArray arr => arr.GetValue(index).ToString()!,

            // Strings
            StringArray sa      => $"\"{sa.GetString(index)}\"",
            LargeStringArray lsa => $"\"{lsa.GetString(index)}\"",
            StringViewArray sva  => $"\"{sva.GetString(index)}\"",

            // 布尔
            BooleanArray arr => arr.GetValue(index).ToString()!.ToLower(),

            // Binary
            BinaryArray arr      => FormatBinary(arr.GetBytes(index)),
            LargeBinaryArray arr => FormatBinary(arr.GetBytes(index)),

            // 时间类型
            Date32Array arr => FormatDate32(arr, index),
            TimestampArray arr => FormatTimestamp(arr, index),
            Time32Array arr => FormatTime32(arr, index),
            Time64Array arr => FormatTime64(arr, index),
            DurationArray arr => FormatDuration(arr, index),

            // 嵌套类型
            ListArray arr      => FormatList(arr, index),
            LargeListArray arr => FormatLargeList(arr, index),
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
            Int8Array  i8  => i8.GetValue(index),   // Polars Month/Day/Weekday is Int8
            Int16Array i16 => i16.GetValue(index),
            Int32Array i32 => i32.GetValue(index),
            Int64Array i64 => i64.GetValue(index),
            
            // Unsigned Integers (注意 UInt64 转 long 可能溢出为负数，但在常规数值处理中通常够用)
            UInt8Array  u8  => u8.GetValue(index),
            UInt16Array u16 => u16.GetValue(index),
            UInt32Array u32 => u32.GetValue(index),
            UInt64Array u64 => (long?)u64.GetValue(index),
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
            // 也可以支持整型转浮点
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
            StringArray sa       => sa.GetString(index),
            LargeStringArray lsa => lsa.GetString(index),
            StringViewArray sva  => sva.GetString(index),
            _ => null
        };
    }

    private static string FormatBinary(ReadOnlySpan<byte> bytes)
    {
        string hex = BitConverter.ToString(bytes.ToArray()).Replace("-", "").ToLower();
        return hex.Length > 20 ? $"x'{hex.Substring(0, 20)}...'" : $"x'{hex}'";
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
        long ticks = unit switch {
            Apache.Arrow.Types.TimeUnit.Nanosecond => v / 100L, Apache.Arrow.Types.TimeUnit.Microsecond => v * 10L,
            Apache.Arrow.Types.TimeUnit.Millisecond => v * 10000L, Apache.Arrow.Types.TimeUnit.Second => v * 10000000L, _ => v
        };
        try { return DateTime.UnixEpoch.AddTicks(ticks).ToString("yyyy-MM-dd HH:mm:ss.ffffff"); }
        catch { return v.ToString(); }
    }

    private static string FormatTime32(Time32Array arr, int index)
    {
        int v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as Time32Type)?.Unit;
        var span = unit switch { Apache.Arrow.Types.TimeUnit.Millisecond => TimeSpan.FromMilliseconds(v), _ => TimeSpan.FromSeconds(v) };
        return span.ToString();
    }

    private static string FormatTime64(Time64Array arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as Time64Type)?.Unit;
        long ticks = unit switch { Apache.Arrow.Types.TimeUnit.Nanosecond => v / 100L, _ => v * 10L };
        return TimeSpan.FromTicks(ticks).ToString();
    }

    private static string FormatDuration(DurationArray arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as DurationType)?.Unit;
        string suffix = unit switch {
            Apache.Arrow.Types.TimeUnit.Nanosecond => "ns", Apache.Arrow.Types.TimeUnit.Microsecond => "us",
            Apache.Arrow.Types.TimeUnit.Millisecond => "ms", Apache.Arrow.Types.TimeUnit.Second => "s", _ => ""
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

    // ==========================================
    // Internal Conversion Logic
    // ==========================================

    private static DateTime ConvertTimestamp(TimestampArray arr, int index)
    {
        long v = arr.GetValue(index).GetValueOrDefault();
        var unit = (arr.Data.DataType as TimestampType)?.Unit;
        
        // C# DateTime 使用 Ticks (100ns)
        // Unix Epoch 是 1970-01-01
        long ticks = unit switch
        {
            Apache.Arrow.Types.TimeUnit.Nanosecond => v / 100L,        // ns -> 100ns
            Apache.Arrow.Types.TimeUnit.Microsecond => v * 10L,        // us -> 100ns
            Apache.Arrow.Types.TimeUnit.Millisecond => v * 10000L,     // ms -> 100ns
            Apache.Arrow.Types.TimeUnit.Second => v * 10000000L,       // s  -> 100ns
            _ => v // Should not happen
        };

        try 
        {
            return DateTime.UnixEpoch.AddTicks(ticks);
        }
        catch (ArgumentOutOfRangeException)
        {
            // 如果超出 C# DateTime 范围，返回 Min/Max 或抛出
            return v > 0 ? DateTime.MaxValue : DateTime.MinValue;
        }
    }

    private static TimeSpan ConvertTime32(Time32Array arr, int index)
    {
        int v = arr.GetValue(index).GetValueOrDefault();
        var unit = (arr.Data.DataType as Time32Type)?.Unit;
        return unit switch
        {
            Apache.Arrow.Types.TimeUnit.Millisecond => TimeSpan.FromMilliseconds(v),
            _ => TimeSpan.FromSeconds(v)
        };
    }

    private static TimeSpan ConvertTime64(Time64Array arr, int index)
    {
        long v = arr.GetValue(index).GetValueOrDefault();
        var unit = (arr.Data.DataType as Time64Type)?.Unit;
        
        long ticks = unit switch
        {
            Apache.Arrow.Types.TimeUnit.Nanosecond => v / 100L,
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
            Apache.Arrow.Types.TimeUnit.Nanosecond => v / 100L,
            Apache.Arrow.Types.TimeUnit.Microsecond => v * 10L,
            Apache.Arrow.Types.TimeUnit.Millisecond => v * 10000L,
            Apache.Arrow.Types.TimeUnit.Second => v * 10000000L,
            _ => v
        };
        return TimeSpan.FromTicks(ticks);
    }
}