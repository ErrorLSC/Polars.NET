using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow;
/// <summary>
/// Extension methods for handling Apache Arrow Arrays.
/// Provides formatting and safe value extraction.
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
            HalfFloatArray arr => arr.GetValue(index).ToString()!,
            FloatArray arr  => arr.GetValue(index).ToString()!,
            DoubleArray arr => arr.GetValue(index).ToString()!,
            DictionaryArray dictArr => $"\"{dictArr.GetStringValue(index)}\"",
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

            // [新增] 兼容时间类型 (返回 Raw Ticks / Days)
            // 这能防止 POCO 定义为 long 但数据是 Timestamp 时无法读取的问题
            TimestampArray ts => ts.GetValue(index),
            Date32Array d32   => d32.GetValue(index), // Days
            Date64Array d64   => d64.GetValue(index), // Milliseconds
            Time32Array t32   => t32.GetValue(index),
            Time64Array t64   => t64.GetValue(index),
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
            DictionaryArray dictArr => UnpackDictionary(dictArr, index),
            _ => null
        };
    }
    private static string? UnpackDictionary(DictionaryArray dictArr, int index)
    {
        // 1. 获取 Key (索引)
        // Indices 可能是 Int8, Int16, Int32 等
        var keys = dictArr.Indices;
        long? key = keys.GetInt64Value(index); // 复用我们写的通用 Int 获取器

        if (!key.HasValue) return null;

        // 2. 从 Dictionary (Values) 中查找对应的 String
        var values = dictArr.Dictionary;
        return values.GetStringValue((int)key.Value);
    }
    private static string FormatBinary(ReadOnlySpan<byte> bytes)
    {
        string hex = Convert.ToHexStringLower(bytes.ToArray());
        return hex.Length > 20 ? $"x'{hex[..20]}...'" : $"x'{hex}'";
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
            TimeUnit.Nanosecond => v / 100L,
            TimeUnit.Microsecond => v * 10L,
            TimeUnit.Millisecond => v * 10000L,
            TimeUnit.Second => v * 10000000L, _ => v
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
        string suffix = unit switch {
            TimeUnit.Nanosecond => "ns",
            TimeUnit.Microsecond => "us",
            TimeUnit.Millisecond => "ms",
            TimeUnit.Second => "s", _ => ""
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
    private static readonly int UnixEpochDayNumber = new DateOnly(1970, 1, 1).DayNumber;

    public static DateOnly? GetDateOnly(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        if (array is Date32Array d32)
        {
            int daysSinceEpoch = d32.GetValue(index)!.Value;
            // C# DateOnly.FromDayNumber 是从 0001-01-01 开始算的
            return DateOnly.FromDayNumber(UnixEpochDayNumber + daysSinceEpoch);
        }
        
        // Date64 是毫秒，也可以转
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
            
            // 如果单位是秒，可能要手动算，这里简化处理
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

    public static DateTimeOffset? GetDateTimeOffset(this IArrowArray array, int index)
        {
            // 1. TimestampArray (最常见的情况)
            if (array is TimestampArray tsArr)
            {
                long? v = tsArr.GetValue(index);
                if (!v.HasValue) return null;

                var type = tsArr.Data.DataType as TimestampType;
                var unit = type?.Unit;
                var timezoneId = type?.Timezone; // 获取 Arrow 里的时区 ID (e.g., "Asia/Shanghai")

                // A. 计算 UTC Ticks
                long ticks = unit switch
                {
                    TimeUnit.Nanosecond => v.Value / 100L,
                    TimeUnit.Microsecond => v.Value * 10L,
                    TimeUnit.Millisecond => v.Value * 10000L,
                    TimeUnit.Second => v.Value * 10000000L,
                    _ => v.Value
                };
                long utcTicks = DateTime.UnixEpoch.Ticks + ticks;

                // B. 计算 Offset
                TimeSpan offset = TimeSpan.Zero;
                
                if (!string.IsNullOrEmpty(timezoneId))
                {
                    try
                    {
                        // 尝试查找系统时区
                        // 注意：Polars 输出的是 IANA ID (如 "Asia/Shanghai")
                        // Windows 上需要 .NET 6+ 且启用 ICU 才能直接识别 IANA，否则可能报错
                        var tzi = TimeZoneInfo.FindSystemTimeZoneById(timezoneId);
                        
                        // 计算该时刻的 Offset (考虑夏令时)
                        offset = tzi.GetUtcOffset(new DateTime(utcTicks, DateTimeKind.Utc));
                    }
                    catch (TimeZoneNotFoundException)
                    {
                        // 如果找不到时区 (比如 Windows 上没装 ICU 且传了 IANA)，回退到 UTC 或抛出
                        // 这里为了稳健，保持 Zero，但在日志里可能需要记录
                        Console.WriteLine($"Warning: TimeZone '{timezoneId}' not found on this system.");
                    }
                }

                // C. 构造 DateTimeOffset (先造 UTC，再 ToOffset 转换视角)
                return new DateTimeOffset(utcTicks, TimeSpan.Zero).ToOffset(offset);
            }
            
            // 2. 兼容 Date32 (Days)
            if (array is Date32Array d32)
            {
                int? days = d32.GetValue(index);
                if (!days.HasValue) return null;
                return new DateTimeOffset(new DateTime(1970, 1, 1).AddDays(days.Value), TimeSpan.Zero);
            }
            
            // 3. 兼容 Date64 (Milliseconds)
            if (array is Date64Array d64)
            {
                long? ms = d64.GetValue(index);
                if (!ms.HasValue) return null;
                return new DateTimeOffset(new DateTime(1970, 1, 1).AddMilliseconds(ms.Value), TimeSpan.Zero);
            }
                
            return null;
        }
        /// <summary>
        /// 高性能版本：使用预先查找好的 TimeZoneInfo
        /// </summary>
        public static DateTimeOffset? GetDateTimeOffsetOptimized(this IArrowArray array, int index, TimeZoneInfo? tzi)
        {
            if (array.IsNull(index)) return null;

            if (array is TimestampArray tsArr)
            {
                long? v = tsArr.GetValue(index);
                if (!v.HasValue) return null;

                var unit = (tsArr.Data.DataType as TimestampType)?.Unit;
                
                // A. 计算 UTC Ticks
                long ticks = unit switch
                {
                    TimeUnit.Nanosecond => v.Value / 100L,
                    TimeUnit.Microsecond => v.Value * 10L,
                    TimeUnit.Millisecond => v.Value * 10000L,
                    TimeUnit.Second => v.Value * 10000000L,
                    _ => v.Value
                };
                long utcTicks = DateTime.UnixEpoch.Ticks + ticks;

                // B. 如果有时区信息，计算 Offset
                TimeSpan offset = TimeSpan.Zero;
                if (tzi != null)
                {
                    // TimeZoneInfo 已经缓存好了，直接算
                    offset = tzi.GetUtcOffset(new DateTime(utcTicks, DateTimeKind.Utc));
                }

                return new DateTimeOffset(utcTicks, TimeSpan.Zero).ToOffset(offset);
            }

            // 兼容 Date32/Date64 (它们通常没有时区，默认为 UTC)
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
        
        // 获取 Type 信息以检查 Timezone
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
            // 1. 还原为墙上时间 (Unspecified)
            // Arrow 里的 v 此时代表 "距离 1970-01-01 00:00:00 的墙上时间差"
            var dt = EpochNaive.AddTicks(ticks);

            // 2. 检查时区
            // 如果 Arrow Schema 里真的带了时区 (比如是通过 CSV 读取带时区的列，或者 dt.convert_time_zone 产生的)
            // 此时 Polars 物理存储的是 UTC。我们需要把这个 UTC 映射回 Local 吗？
            // 不，Polars 的逻辑是：有时区 -> 物理存UTC -> 显示时转换。
            // 但如果我们在 Converter 里存的是 Naive，这里 type.Timezone 应该是 null。
            
            if (!string.IsNullOrEmpty(type?.Timezone))
            {
                // 极端情况：如果 Arrow 里有时区，说明这是个"绝对时间"。
                // 此时我们需要返回 UTC 吗？
                // 为了保持一致性，如果 Polars 说是 Aware，我们最好返回 UTC (或者带 Offset)。
                // 但这里我们主要讨论 Naive 场景。
                return DateTime.UnixEpoch.AddTicks(ticks); 
            }

            // 绝大多数情况：Naive -> Unspecified
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