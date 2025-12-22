using System.ComponentModel.DataAnnotations;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C; 

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    private static byte[]? ToBytes(bool[]? bools)
    {
        if (bools == null) return null;
        var bytes = new byte[bools.Length];
        for (int i = 0; i < bools.Length; i++)
        {
            bytes[i] = bools[i] ? (byte)1 : (byte)0;
        }
        return bytes;
    }
    // --- Constructors ---

    public static SeriesHandle SeriesNew(string name, int[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i32(name, data, ToBytes(validity), (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, long[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i64(name, data, ToBytes(validity), (UIntPtr)data.Length));

    public static SeriesHandle SeriesNew(string name, double[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_f64(name, data, ToBytes(validity), (UIntPtr)data.Length));
        
    public static SeriesHandle SeriesNew(string name, bool[] data, bool[]? validity)
    {
        // 数据和 Validity 都需要转成 byte[]
        var dataBytes = ToBytes(data)!; // data 不可能为 null
        var validBytes = ToBytes(validity);
        return ErrorHelper.Check(NativeBindings.pl_series_new_bool(name, dataBytes, validBytes, (UIntPtr)data.Length));
    }

    // 字符串 Marshalling 比较特殊：需要手动分配非托管内存
    public static SeriesHandle SeriesNew(string name, string?[] data)
    {
        var len = data.Length;
        var ptrs = new IntPtr[len];
        
        try
        {
            for(int i=0; i<len; i++) 
            {
                if (data[i] == null)
                    ptrs[i] = IntPtr.Zero; // Rust 端会识别为 None
                else
                    ptrs[i] = Marshal.StringToCoTaskMemUTF8(data[i]);
            }
            
            return ErrorHelper.Check(NativeBindings.pl_series_new_str(name, ptrs, (UIntPtr)len));
        }
        finally 
        {
            // 必须释放刚刚分配的非托管字符串内存
            foreach(var p in ptrs) 
            {
                if (p != IntPtr.Zero) Marshal.FreeCoTaskMem(p);
            }
        }
    }
    // 预计算 10 的幂次，避免重复 Math.Pow
    private static readonly decimal[] PowersOf10;

    static PolarsWrapper()
    {
        PowersOf10 = new decimal[29]; // 0 .. 28
        PowersOf10[0] = 1m;
        for (int i = 1; i < PowersOf10.Length; i++)
        {
            PowersOf10[i] = PowersOf10[i - 1] * 10m;
        }
    }

    public static SeriesHandle SeriesNewDecimal(string name, decimal[] data, bool[]? validity, int scale)
    {
        if (scale < 0 || scale >= PowersOf10.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), $"Scale must be between 0 and {PowersOf10.Length - 1} for C# decimal conversion.");
        }
        var len = data.Length;
        var scaledValues = new Int128[len];
        var multiplier = PowersOf10[scale]; // 获取乘数 (例如 scale=2 -> 100)

        // 转换逻辑：将 C# decimal 变成纯整数 Int128
        for (int i = 0; i < len; i++)
        {
            // 注意：这里可能会溢出 decimal 的范围，但在金融场景通常还好
            // 直接乘法会自动处理符号
            scaledValues[i] = (Int128)(data[i] * multiplier);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_decimal(
            name, 
            scaledValues, 
            ToBytes(validity), 
            (UIntPtr)len, 
            (UIntPtr)scale
        ));
    }
    
    // 可空版本 (Option)
    public static SeriesHandle SeriesNewDecimal(string name, decimal?[] data, int scale)
    {
        if (scale < 0 || scale >= PowersOf10.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), $"Scale must be between 0 and {PowersOf10.Length - 1} for C# decimal conversion.");
        }
        var len = data.Length;
        var scaledValues = new Int128[len];
        var validity = new byte[len];
        var multiplier = PowersOf10[scale];

        for (int i = 0; i < len; i++)
        {
            var value = data[i];
            if (value.HasValue)
            {
                scaledValues[i] = (Int128)(value.Value * multiplier);
                validity[i] = 1;
            }
            else
            {
                scaledValues[i] = 0;
                validity[i] = 0;
            }
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_decimal(
            name, 
            scaledValues, 
            validity, 
            (UIntPtr)len, 
            (UIntPtr)scale
        ));
    }
    // --- Properties ---
    public static string GetSeriesDtypeString(SeriesHandle h)
    {
        var ptr = NativeBindings.pl_series_dtype_str(h);
        return ErrorHelper.CheckString(ptr);
    }
    /// <summary>
    /// 直接从 Series 获取 DataType Handle (Zero Parse)
    /// </summary>
    public static DataTypeHandle GetSeriesDataType(SeriesHandle handle)
    {
        return ErrorHelper.Check(NativeBindings.pl_series_get_dtype(handle));
    }
    public static long SeriesLen(SeriesHandle h) => (long)NativeBindings.pl_series_len(h);
    
    public static string SeriesName(SeriesHandle h) 
    {
        var ptr = NativeBindings.pl_series_name(h);
        try {
            return Marshal.PtrToStringUTF8(ptr) ?? "";
        } finally {
            NativeBindings.pl_free_c_string(ptr);
        }
    }
    
    public static void SeriesRename(SeriesHandle h, string name) => NativeBindings.pl_series_rename(h, name);

    // --- DataFrame Conversion ---
    public static DataFrameHandle SeriesToFrame(SeriesHandle h) 
    {
        return ErrorHelper.Check(NativeBindings.pl_series_to_frame(h));
    }
    public static long? SeriesGetInt(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_i64(s, (UIntPtr)idx, out long val)) return val;
        return null;
    }

    public static double? SeriesGetDouble(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_f64(s, (UIntPtr)idx, out double val)) return val;
        return null;
    }

    public static bool? SeriesGetBool(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_bool(s, (UIntPtr)idx, out bool val)) return val;
        return null;
    }

    public static string? SeriesGetString(SeriesHandle s, long idx)
    {
        IntPtr ptr = NativeBindings.pl_series_get_str(s, (UIntPtr)idx);
        return ErrorHelper.CheckString(ptr); // CheckString 会处理 IntPtr.Zero -> null
    }

    public static decimal? SeriesGetDecimal(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_decimal(s, (UIntPtr)idx, out Int128 val, out UIntPtr scalePtr))
        {
            int scale = (int)scalePtr;
            
            // Int128 -> Decimal 转换
            // Decimal 构造函数不支持 Int128，但支持 int[] bits
            // 简单做法：(decimal)val / 10^scale
            // 但这样会转 double 丢精度。
            
            // 正确做法：直接构造 decimal
            // decimal 布局: flags, hi, lo, mid
            // 我们需要先把 Int128 变成 decimal (纯整数)，然后设 scale
            
            // 既然 val 是 i128，我们显式强转 decimal (C# 11+ 支持显式转换)
            try 
            {
                decimal d = (decimal)val; 
                // 手动应用 scale: d / 10^scale
                // 或者更高效：创建一个新的 decimal 修改其 flags
                // 但 C# decimal 是 immutable struct，修改 flags 比较 hacky
                // 最稳妥： d / PowersOf10[scale] (我们之前算过这个表)
                
                if (scale >= 0 && scale < PowersOf10.Length)
                {
                    return d / PowersOf10[scale];
                }
                else
                {
                    // Fallback using double division if scale is huge (unlikely)
                    return d / (decimal)Math.Pow(10, scale);
                }
            }
            catch (OverflowException)
            {
                return null; // 超出 decimal 范围
            }
        }
        return null;
    }
    // Date: Days since 1970-01-01
    public static DateOnly? SeriesGetDate(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_date(s, (UIntPtr)idx, out int days))
        {
            return DateOnly.FromDayNumber(days + 719162); // 719162 is days from 0001-01-01 to 1970-01-01
        }
        return null;
    }

    // Time: Nanoseconds since midnight
    public static TimeOnly? SeriesGetTime(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_time(s, (UIntPtr)idx, out long ns))
        {
            // .NET Ticks = 100ns
            long ticks = ns / 100;
            return new TimeOnly(ticks);
        }
        return null;
    }

    // Datetime: Microseconds since 1970-01-01 (Assuming 'us' time unit)
    public static DateTime? SeriesGetDatetime(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_datetime(s, (UIntPtr)idx, out long us))
        {
            // .NET Ticks = 100ns. 1 us = 10 ticks.
            // Unix Epoch Ticks = 621355968000000000
            long ticks = (us * 10) + 621355968000000000L;
            return new DateTime(ticks, DateTimeKind.Utc); // 默认 UTC 语义
        }
        return null;
    }

    // Duration: Microseconds (Assuming 'us')
    public static TimeSpan? SeriesGetDuration(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_duration(s, (UIntPtr)idx, out long us))
        {
            // 1 us = 10 ticks
            return new TimeSpan(us * 10);
        }
        return null;
    }
    // --- Arrow Integration ---

    public static unsafe IArrowArray SeriesToArrow(SeriesHandle h)
    {
        // 1. 获取 Rust Context
        // 注意：ArrowArrayContextHandle 也是 PolarsHandle，可以被 ErrorHelper 检查（如果需要的话）
        // 这里 NativeBindings 直接返回句柄，通常不需要 Check，除非 Rust 可能返回空指针
        using var contextHandle = NativeBindings.pl_series_to_arrow(h);
        
        // 2. 准备 C Data Interface 结构体
        var cArray = new CArrowArray();
        var cSchema = new CArrowSchema();
        
        // 3. 导出 (填充结构体)
        NativeBindings.pl_arrow_array_export(contextHandle, &cArray);
        NativeBindings.pl_arrow_schema_export(contextHandle, &cSchema);
        bool ownershipTransferred = false;
        // 4. 导入 (两步走)
        try
        {
            // [修复] 第一步：先从 CSchema 导入字段定义 (Field)，获取 DataType
            var importedField = CArrowSchemaImporter.ImportField(&cSchema);
            
            // [修复] 第二步：使用 DataType 导入 Array
            // 这样编译器就能匹配到 ImportArray(CArrowArray*, IArrowType)
            var array = CArrowArrayImporter.ImportArray(&cArray, importedField.DataType);
            ownershipTransferred = true;
            return array;
        }
        finally
        {
            // 注意：CArrowArrayImporter 会接管 cArray 和 cSchema 指向的内部资源 (release 回调)
            // 所以我们不需要手动释放 cArray/cSchema 结构体本身的内容
            // 只需要释放 Rust 的 Context 壳子 (由 using contextHandle 自动完成)
            if (!ownershipTransferred)
            {
                CArrowArray.Free(&cArray);
                CArrowSchema.Free(&cSchema);
            }
        }
    }
    /// <summary>
    /// Imports an Arrow Array via C Data Interface.
    /// </summary>
    public static unsafe SeriesHandle SeriesFromArrow(string name, CArrowArray* cArray, CArrowSchema* cSchema)
    {
        return ErrorHelper.Check(NativeBindings.pl_arrow_to_series(name, cArray, cSchema));
    }
    public static SeriesHandle SeriesCast(SeriesHandle s, DataTypeHandle dtype)
    {
        return ErrorHelper.Check(NativeBindings.pl_series_cast(s, dtype));
    }
    public static SeriesHandle SeriesIsNull(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_null(s));
    
    public static SeriesHandle SeriesIsNotNull(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_not_null(s));

    public static bool SeriesIsNullAt(SeriesHandle s, long idx)
    {
        return NativeBindings.pl_series_is_null_at(s, (UIntPtr)idx);
    }
    public static SeriesHandle SeriesIsNan(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_nan(s));
    public static SeriesHandle SeriesIsNotNan(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_not_nan(s));
    public static SeriesHandle SeriesIsFinite(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_finite(s));
    public static SeriesHandle SeriesIsInfinite(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_infinite(s));
    public static long SeriesNullCount(SeriesHandle s)
    {
        return (long)NativeBindings.pl_series_null_count(s);
    }
    // Ops
    public static SeriesHandle SeriesAdd(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_add(s1, s2));
    public static SeriesHandle SeriesSub(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_sub(s1, s2));
    public static SeriesHandle SeriesMul(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_mul(s1, s2));
    public static SeriesHandle SeriesDiv(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_div(s1, s2));

    public static SeriesHandle SeriesEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_eq(s1, s2));
    public static SeriesHandle SeriesNeq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_neq(s1, s2));
    public static SeriesHandle SeriesGt(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_gt(s1, s2));
    public static SeriesHandle SeriesLt(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_lt(s1, s2));
    public static SeriesHandle SeriesGtEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_gt_eq(s1, s2));
    public static SeriesHandle SeriesLtEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_lt_eq(s1, s2));

    // Aggs
    public static SeriesHandle SeriesSum(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_sum(s));
    public static SeriesHandle SeriesMean(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_mean(s));
    public static SeriesHandle SeriesMin(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_min(s));
    public static SeriesHandle SeriesMax(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_max(s));
    // Slice
    public static SeriesHandle SeriesSlice(SeriesHandle handle, long offset, long length)
    {
        // 将 long 长度转为 UIntPtr
        return ErrorHelper.Check(NativeBindings.pl_series_slice(handle, offset, (UIntPtr)length));
    }
}