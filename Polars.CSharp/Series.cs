using Polars.NET.Core;
using Apache.Arrow;
using Polars.NET.Core.Arrow;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars Series.
/// </summary>
public class Series : IDisposable
{
    internal SeriesHandle Handle { get; }

    internal Series(SeriesHandle handle)
    {
        Handle = handle;
    }

    internal Series(string name, SeriesHandle handle)
    {
        PolarsWrapper.SeriesRename(handle, name);
        Handle = handle;
    }
    // ==========================================
    // Metadata
    // ==========================================

    /// <summary>
    /// Get the string representation of the Series data type (e.g. "i64", "str", "datetime(μs)").
    /// </summary>
    public string DataTypeName => PolarsWrapper.GetSeriesDtypeString(Handle);

    /// <summary>
    /// Gets the DataType of the Series.
    /// </summary>
    /// <remarks>
    /// This property creates a new DataType instance every time it is accessed.
    /// Since DataType wraps a native handle, consider caching it locally if accessed frequently in a loop.
    /// </remarks>
    public DataType DataType
    {
        get
        {
            // 1. 调用底层获取类型字符串 (例如 "i64", "date", "list[i64]")
            var dtypeStr = PolarsWrapper.GetSeriesDtypeString(Handle);
            
            // 2. 解析为 C# DataType 对象
            return DataType.Parse(dtypeStr);
        }
    }
    
    // ==========================================
    // Scalar Accessors (Native Speed ⚡)
    // ==========================================

    /// <summary>
    /// Get an item at the specified index.
    /// Supports: int, long, double, bool, string, decimal, DateTime, TimeSpan, DateOnly, TimeOnly.
    /// </summary>
    public T? GetValue<T>(long index)
    {
        var type = typeof(T);
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (index < 0 || index >= Length)
            throw new IndexOutOfRangeException($"Index {index} is out of bounds for Series length {Length}.");

        // 1. Numeric
        if (underlying == typeof(int)) 
            return (T?)(object?)(int?)PolarsWrapper.SeriesGetInt(Handle, index); // Long -> Int (Narrowing)
            
        if (underlying == typeof(long)) 
            return (T?)(object?)PolarsWrapper.SeriesGetInt(Handle, index);

        if (underlying == typeof(double)) 
            return (T?)(object?)PolarsWrapper.SeriesGetDouble(Handle, index);

        if (underlying == typeof(float)) 
            return (T?)(object?)(float?)PolarsWrapper.SeriesGetDouble(Handle, index);

        // 2. Boolean
        if (underlying == typeof(bool)) 
            return (T?)(object?)PolarsWrapper.SeriesGetBool(Handle, index);

        // 3. String
        if (underlying == typeof(string)) 
        {
            // 1. 先检查 Validity Bitmap (位图)
            if (PolarsWrapper.SeriesIsNullAt(Handle, index))
            {
                // 这里返回 default! 是为了压制 "可能返回 null" 的警告
                // 对于 string?，default 是 null；对于 string，default 也是 null (但在非空上下文中需要 !)
                return default!; 
            }

            // 2. 获取实际字符串
            var strVal = PolarsWrapper.SeriesGetString(Handle, index);
            
            // 3. 压制警告并返回
            // strVal! 告诉编译器：根据前面的 IsNullAt 检查，我确信这里 strVal 不会是 null
            return (T)(object)strVal!;
        }

        // 4. Decimal
        if (underlying == typeof(decimal))
            return (T?)(object?)PolarsWrapper.SeriesGetDecimal(Handle, index);

        // // 5. Temporal (Time)
        // if (underlying == typeof(DateTime))
        // {
        //     // [修复逻辑] 检查 Series 实际的 DataType
        //     // 如果底层是 Date 类型 (Int32)，不能调 GetDatetime (期望 Int64)
        //     // 而应该调 GetDate (得到 DateOnly)，再转为 DateTime
        //     if (this.DataTypeName == "date") 
        //     {
        //         var dateOnly = PolarsWrapper.SeriesGetDate(Handle, index);
        //         if (dateOnly == null) return default; // 处理空值
        //         return (T)(object)dateOnly.Value.ToDateTime(TimeOnly.MinValue);
        //     }

        //     // 只有当底层真的是 Datetime 类型时，才调这个
        //     return (T?)(object?)PolarsWrapper.SeriesGetDatetime(Handle, index);
        // }

        if (underlying == typeof(DateOnly))
            return (T?)(object?)PolarsWrapper.SeriesGetDate(Handle, index);
            
        if (underlying == typeof(TimeOnly))
            return (T?)(object?)PolarsWrapper.SeriesGetTime(Handle, index);
            
        if (underlying == typeof(TimeSpan))
            return (T?)(object?)PolarsWrapper.SeriesGetDuration(Handle, index);

        // ==============================================================
        // 🐢 慢车道 (Universal Path) - 使用 Arrow Infrastructure
        // 针对 Struct, List, F# Option, DateTimeOffset 等复杂类型
        // ==============================================================
        
        // 1. 切片：只取这一行
        using var slice = this.Slice(index, 1);
        
        // 2. 导出为 Arrow Array
        // 因为 ArrowReader 需要 IArrowArray，我们暂时没有 Series.ToArrow 直接绑定
        // 所以我们把它包在 DataFrame 里导出，然后取第一列
        using var df = new DataFrame(slice);
        using var batch = df.ToArrow(); // 调用 Core 层的 ExportDataFrame
        var column = batch.Column(0);

        // 3. 使用强大的 ArrowReader 解析
        // 这里会自动处理 Struct 递归、F# Option 解包、DateTimeOffset 时区归一化
        return ArrowReader.ReadItem<T>(column, 0);

        // throw new NotSupportedException($"Type {type.Name} is not supported for Series.GetValue.");
    }
    
    /// <summary>
    /// Get an item at the specified index as object.
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public object? this[long index]
    {
        get
        {
            // 根据 dtype 字符串动态决定返回类型 (稍微慢一点，适合调试)
            // 你也可以解析 DataTypeName 字符串，或者让用户必须用 GetValue<T>
            // 这里简单处理：
            var dtype = DataTypeName;
            if (dtype.Contains("i32") || dtype.Contains("i64")) return GetValue<long>(index);
            if (dtype.Contains("f32") || dtype.Contains("f64")) return GetValue<double>(index);
            if (dtype.Contains("str")) return GetValue<string>(index);
            if (dtype.Contains("bool")) return GetValue<bool>(index);
            if (dtype.Contains("decimal")) return GetValue<decimal>(index);
            if (dtype.Contains("datetime")) return GetValue<DateTime>(index);
            if (dtype.Contains("date")) return GetValue<DateOnly>(index);
            if (dtype.Contains("time")) return GetValue<TimeOnly>(index);
            if (dtype.Contains("duration")) return GetValue<TimeSpan>(index);
            
            return null; // Fallback
        }
    }
    // ==========================================
    // Arithmetic Operators (算术运算符)
    // ==========================================
    /// <summary>
    /// Add Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator +(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesAdd(left.Handle, right.Handle));
    }
    /// <summary>
    /// Minus Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator -(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesSub(left.Handle, right.Handle));
    }
    /// <summary>
    /// Multiple Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator *(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesMul(left.Handle, right.Handle));
    }
    /// <summary>
    /// Divide Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator /(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesDiv(left.Handle, right.Handle));
    }

    // ==========================================
    // Comparison Methods & Operators (比较)
    // ==========================================

    // C# 的 == 和 != 运算符重载有比较严格的限制（通常用于对象相等性），
    // 且必须成对重载并重写 Equals/GetHashCode。
    // 为了避免混淆（是比较引用还是生成布尔掩码？），我们推荐使用显式的 Eq/Neq 方法，
    // 或者在未来实现复杂的运算符重载策略。目前先暴露方法。
    /// <summary>
    /// Compare whether two Series is equal
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Eq(Series other) => new(PolarsWrapper.SeriesEq(Handle, other.Handle));
    /// <summary>
    /// Compare whether two Series is not equal
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Neq(Series other) => new(PolarsWrapper.SeriesNeq(Handle, other.Handle));
    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    // 大于小于可以用运算符重载，这在 C# 中比较常见用于自定义类型
    public static Series operator >(Series left, Series right) 
        => new(PolarsWrapper.SeriesGt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator <(Series left, Series right) 
        => new(PolarsWrapper.SeriesLt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator >=(Series left, Series right) 
        => new(PolarsWrapper.SeriesGtEq(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator <=(Series left, Series right) 
        => new(PolarsWrapper.SeriesLtEq(left.Handle, right.Handle));

    // 显式方法别名 (Fluent API 风格)
    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Gt(Series other) => this > other;
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Lt(Series other) => this < other;
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series GtEq(Series other) => this >= other;
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series LtEq(Series other) => this <= other;

    // ==========================================
    // Aggregations (聚合)
    // ==========================================

    // 注意：Polars 的 Series 聚合通常返回一个长度为 1 的新 Series (Scalar)
    /// <summary>
    /// Sum series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Sum() => new(PolarsWrapper.SeriesSum(Handle));
    /// <summary>
    /// Mean series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Mean() => new(PolarsWrapper.SeriesMean(Handle));
    /// <summary>
    /// Min series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Min() => new(PolarsWrapper.SeriesMin(Handle));
    /// <summary>
    /// Max series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Max() => new(PolarsWrapper.SeriesMax(Handle));

    // 泛型辅助方法：直接获取标量值
    /// <summary>
    /// Sum series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Sum<T>() => Sum().GetValue<T>(0);
    /// <summary>
    /// Mean series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Mean<T>() => Mean().GetValue<T>(0);
    /// <summary>
    /// Min series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Min<T>() => Min().GetValue<T>(0);
    /// <summary>
    /// Max series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Max<T>() => Max().GetValue<T>(0);
    // ==========================================
    // Helpers (时间转换逻辑)
    // ==========================================
    
    // Unix Epoch Ticks (1970-01-01)
    private const long UnixEpochTicks = 621355968000000000L;
    private const int DaysTo1970 = 719162;

    // DateTime -> Microseconds (Long)
    private static long ToMicros(DateTime dt) => (dt.Ticks - UnixEpochTicks) / 10L;
    
    // TimeSpan -> Microseconds (Long)
    private static long ToMicros(TimeSpan ts) => ts.Ticks / 10L;

    // TimeOnly -> Nanoseconds (Long)
    private static long ToNanos(TimeOnly t) => t.Ticks * 100L;

    // DateOnly -> Days (Int)
    private static int ToDays(DateOnly d) => d.DayNumber - DaysTo1970;
    // ==========================================
    // Constructors
    // ==========================================
    /// <summary>
    /// Create a Series from an array of integers.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, int[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of longs.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, long[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of doubles.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, double[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of booleans.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, bool[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of strings.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, string?[] data)
    {
        Handle = PolarsWrapper.SeriesNew(name, data);
    }
    /// <summary>
    /// Create a Series from an array of DateTime values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateTime[] data) : this(name, data, null) { }
    /// <summary>
    /// Create a Series from an array of DateTime values with validity mask.    
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, DateTime[] data, bool[]? validity)
    {
        var longArray = new long[data.Length];
        for (int i = 0; i < data.Length; i++) longArray[i] = ToMicros(data[i]);

        // 步骤: 创建 i64 -> Cast 为 Datetime
        using var hRaw = PolarsWrapper.SeriesNew(name, longArray, validity);
        using var dtype = DataType.Datetime; // 默认是 Microseconds
        Handle = PolarsWrapper.SeriesCast(hRaw, dtype.Handle);
    }
    
    /// <summary>
    /// Create a Series from an array of TimeSpan values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeSpan[] data) : this(name, data, null) { }
    /// <summary>
    /// Create a Series from an array of TimeSpan values with validity mask.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, TimeSpan[] data, bool[]? validity)
    {
        var longArray = new long[data.Length];
        for (int i = 0; i < data.Length; i++) longArray[i] = ToMicros(data[i]);

        using var hRaw = PolarsWrapper.SeriesNew(name, longArray, validity);
        using var dtype = DataType.Duration; 
        Handle = PolarsWrapper.SeriesCast(hRaw, dtype.Handle);
    }

    /// <summary>
    /// Create a Series from an array of DateOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateOnly[] data) : this(name, data, null) { }
    /// <summary>
    /// Create a Series from an array of DateOnly values with validity mask.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, DateOnly[] data, bool[]? validity)
    {
        var intArray = new int[data.Length];
        for (int i = 0; i < data.Length; i++) intArray[i] = ToDays(data[i]);

        using var hRaw = PolarsWrapper.SeriesNew(name, intArray, validity);
        using var dtype = DataType.Date;
        Handle = PolarsWrapper.SeriesCast(hRaw, dtype.Handle);
    }

    /// <summary>
    /// Create a Series from an array of TimeOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeOnly[] data) : this(name, data, null) { }
    /// <summary>
    /// Create a Series from an array of TimeOnly values with validity mask.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, TimeOnly[] data, bool[]? validity)
    {
        var longArray = new long[data.Length];
        for (int i = 0; i < data.Length; i++) longArray[i] = ToNanos(data[i]);

        using var hRaw = PolarsWrapper.SeriesNew(name, longArray, validity);
        using var dtype = DataType.Time;
        Handle = PolarsWrapper.SeriesCast(hRaw, dtype.Handle);
    }
    /// <summary>
    /// Create a Series from a collection of values. 
    /// Supports: int, long, double, bool, string, decimal, and their nullable variants.
    /// </summary>
    public static Series Create<T>(string name, IEnumerable<T> values)
    {
        var array = values as T[] ?? [.. values];
        var type = typeof(T);
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        // --- 1. Integers (Int32) ---
        if (underlyingType == typeof(int))
        {
            var (data, validity) = ToRawArrays(array, v => (int)(object)v!);
            return new Series(name, PolarsWrapper.SeriesNew(name, data, validity));
        }
        
        // --- 2. Long (Int64) ---
        if (underlyingType == typeof(long))
        {
            var (data, validity) = ToRawArrays(array, v => (long)(object)v!);
            return new Series(name, PolarsWrapper.SeriesNew(name, data, validity));
        }

        // --- 3. Double (Float64) ---
        if (underlyingType == typeof(double))
        {
            var (data, validity) = ToRawArrays(array, v => (double)(object)v!);
            return new Series(name, PolarsWrapper.SeriesNew(name, data, validity));
        }
        if (underlyingType == typeof(float))
        {
            // 策略：复用 SeriesNew(double[])，创建后 Cast 为 Float32
            // 这样不需要在底层 NativeBindings 加 pl_series_new_f32
            var (data, validity) = ToRawArrays(array, v => (double)(float)(object)v!);
            
            using var temp = new Series(name, PolarsWrapper.SeriesNew(name, data, validity));
            return temp.Cast(DataType.Float32);
        }
        // --- 4. Boolean ---
        if (underlyingType == typeof(bool))
        {
            var (data, validity) = ToRawArrays(array, v => (bool)(object)v!);
            return new Series(name, PolarsWrapper.SeriesNew(name, data, validity));
        }

        // --- 5. String (特殊处理) ---
        if (underlyingType == typeof(string))
        {
            // string 引用类型本身可空，直接传给 Wrapper
            var strArray = array as string[] ?? array.Select(x => x as string).ToArray();
            return new Series(name, PolarsWrapper.SeriesNew(name, strArray));
        }

        // --- 6. Decimal (高精度金融计算) ---
        if (underlyingType == typeof(decimal))
        {
            // 必须先计算 Scale，因为 Wrapper 需要它来做 Int128 乘法
            if (type == typeof(decimal))
            {
                // 非空 Decimal
                var decArray = array as decimal[] ?? [.. array.Cast<decimal>()];
                int scale = DetectMaxScale(decArray);
                // 调用你刚才写的 Wrapper (它内部会处理 * 10^scale 转 Int128)
                return new Series(name, PolarsWrapper.SeriesNewDecimal(name, decArray, null, scale));
            }
            else
            {
                if (array is not decimal?[] decArray)
                {
                    decArray = [.. array.Cast<decimal?>()];
                }

                int scale = DetectMaxScale(decArray);
                return new Series(name, PolarsWrapper.SeriesNewDecimal(name, decArray, scale));
            }
        }
        // --- 7. DateTime ---
        if (underlyingType == typeof(DateTime))
        {
            // 7.1 非空：直接调用构造函数 (复用 ToMicros + Cast 逻辑)
            if (type == typeof(DateTime))
            {
                return new Series(name, array.Cast<DateTime>().ToArray());
            }

            // 7.2 可空
            var dtArray = array.Cast<DateTime?>().ToArray();
            var longArray = new long[dtArray.Length];
            var validity = new bool[dtArray.Length];

            for (int i = 0; i < dtArray.Length; i++)
            {
                if (dtArray[i] is DateTime dt)
                {
                    longArray[i] = ToMicros(dt);
                    validity[i] = true;
                }
                else
                {
                    longArray[i] = 0; // validity=false 时值不重要
                    validity[i] = false;
                }
            }
            
            using var temp = new Series(name, longArray, validity);
            return temp.Cast(DataType.Datetime);
        }

        // --- 8. DateOnly ---
        if (underlyingType == typeof(DateOnly))
        {
            if (type == typeof(DateOnly))
            {
                return new Series(name, array.Cast<DateOnly>().ToArray());
            }

            // 可空
            var dArray = array.Cast<DateOnly?>().ToArray();
            var intArray = new int[dArray.Length];
            var validity = new bool[dArray.Length];
            const int DaysTo1970 = 719162;

            for (int i = 0; i < dArray.Length; i++)
            {
                if (dArray[i] is DateOnly d)
                {
                    intArray[i] = d.DayNumber - DaysTo1970;
                    validity[i] = true;
                }
                else
                {
                    intArray[i] = 0;
                    validity[i] = false;
                }
            }

            using var temp = new Series(name, intArray, validity);
            return temp.Cast(DataType.Date);
        }

        // --- 9. TimeOnly ---
        if (underlyingType == typeof(TimeOnly))
        {
            if (type == typeof(TimeOnly))
            {
                return new Series(name, array.Cast<TimeOnly>().ToArray());
            }

            // 可空
            var tArray = array.Cast<TimeOnly?>().ToArray();
            var longArray = new long[tArray.Length];
            var validity = new bool[tArray.Length];

            for (int i = 0; i < tArray.Length; i++)
            {
                if (tArray[i] is TimeOnly t)
                {
                    longArray[i] = ToNanos(t);
                    validity[i] = true;
                }
                else
                {
                    longArray[i] = 0;
                    validity[i] = false;
                }
            }

            using var temp = new Series(name, longArray, validity);
            return temp.Cast(DataType.Time);
        }

        // --- 10. TimeSpan (Duration) ---
        if (underlyingType == typeof(TimeSpan))
        {
            if (type == typeof(TimeSpan))
            {
                return new Series(name, array.Cast<TimeSpan>().ToArray());
            }

            // 可空
            var tsArray = array.Cast<TimeSpan?>().ToArray();
            var longArray = new long[tsArray.Length];
            var validity = new bool[tsArray.Length];

            for (int i = 0; i < tsArray.Length; i++)
            {
                if (tsArray[i] is TimeSpan ts)
                {
                    longArray[i] = ToMicros(ts);
                    validity[i] = true;
                }
                else
                {
                    longArray[i] = 0;
                    validity[i] = false;
                }
            }

            using var temp = new Series(name, longArray, validity);
            return temp.Cast(DataType.Duration);
        }

        throw new NotSupportedException($"Type {type.Name} is not supported for Series creation via Create<T>.");
    }
    /// <summary>
    /// Create a Series from an array of decimals.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, decimal[] data, bool[]? validity = null)
    {
        // 复用之前的自动精度推断逻辑
        int scale = DetectMaxScale(data);
        Handle = PolarsWrapper.SeriesNewDecimal(name, data, validity, scale);
    }

    /// <summary>
    /// Create a Series from an array of nullable decimals.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, decimal?[] data)
    {
        int scale = DetectMaxScale(data);
        Handle = PolarsWrapper.SeriesNewDecimal(name, data, scale);
    }
    // ==========================================
    // Internal Helpers
    // ==========================================

    /// <summary>
    /// 将 IEnumerable&lt;T&gt; (可能是 Nullable) 拆分为 数据数组 + ValidityMask
    /// </summary>
    private static (TPrimitive[] data, bool[]? validity) ToRawArrays<TInput, TPrimitive>(
        TInput[] input, 
        Func<TInput, TPrimitive> valueSelector) 
        where TPrimitive : struct
    {
        int len = input.Length;
        var data = new TPrimitive[len];
        
        // 只有当类型是 Nullable 或者是引用类型且有 null 时才需要 validity
        // 但为了通用性，我们这里先检查一下是否有 null，如果没有 null，validity 传 null 给 Rust 以节省内存
        
        // 快速路径：如果 TInput 是值类型且非 Nullable，直接 Copy
        // (省略优化，走通用路径以保证安全性)

        var validity = new bool[len];
        bool hasNull = false;

        for (int i = 0; i < len; i++)
        {
            var item = input[i];
            if (item == null)
            {
                hasNull = true;
                validity[i] = false;
                data[i] = default; // 0
            }
            else
            {
                validity[i] = true;
                data[i] = valueSelector(item);
            }
        }

        return (data, hasNull ? validity : null);
    }

    // --- Decimal Helpers ---

    private static int GetScale(decimal d)
    {
        // C# decimal bits: [0,1,2] = 96bit integer, [3] = flags (contains scale)
        int[] bits = decimal.GetBits(d);
        // Scale is in bits 16-23 of the 4th int
        return (bits[3] >> 16) & 0x7F;
    }

    private static int DetectMaxScale(IEnumerable<decimal> values)
    {
        int max = 0;
        foreach (var v in values)
        {
            int s = GetScale(v);
            if (s > max) max = s;
        }
        return max;
    }
    
    private static int DetectMaxScale(IEnumerable<decimal?> values)
    {
        int max = 0;
        foreach (var v in values)
        {
            if (v.HasValue)
            {
                int s = GetScale(v.Value);
                if (s > max) max = s;
            }
        }
        return max;
    }

    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Length of the Series.
    /// </summary>
    public long Length => PolarsWrapper.SeriesLen(Handle);
    /// <summary>
    /// Name of the Series.
    /// </summary>
    public string Name 
    {
        get => PolarsWrapper.SeriesName(Handle);
        set => PolarsWrapper.SeriesRename(Handle, value);
    }
    /// <summary>
    /// Get the number of null values in the Series.
    /// </summary>
    public long NullCount => PolarsWrapper.SeriesNullCount(Handle);

    // ==========================================
    // Operations
    // ==========================================

    /// <summary>
    /// Cast the Series to a different DataType.
    /// </summary>
    public Series Cast(DataType dtype)
    {
        // SeriesCast 返回一个新的 Series Handle
        return new Series(PolarsWrapper.SeriesCast(Handle, dtype.Handle));
    }
    /// <summary>
    /// Get a slice of this Series.
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Length of the slice.</param>
    public Series Slice(long offset, long length)
    {
        var newHandle = PolarsWrapper.SeriesSlice(Handle, offset, length);
        return new Series(newHandle);
    }
    /// <summary>
    /// Convert Series to Arrow Array
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T[] ToArray<T>()
    {
        // 1. 转为 DataFrame (为了用 ToArrow 导出 Batch)
        using var df = new DataFrame(this);
        using var batch = df.ToArrow();
        
        // 2. 取第一列
        var col = batch.Column(0);
        
        // 3. 读取
        return ArrowReader.ReadColumn<T>(col);
    }
    // ==========================================
    // Null Checks & Boolean Masks
    // ==========================================

    /// <summary>
    /// 检查指定索引处的值是否为 Null。
    /// </summary>
    public bool IsNullAt(long index)
    {
        return PolarsWrapper.SeriesIsNullAt(Handle, index);
    }

    /// <summary>
    /// 返回一个布尔 Series，如果元素为 Null 则为 True。
    /// </summary>
    public Series IsNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNull(Handle);
        return new Series(newHandle);
    }

    /// <summary>
    /// 返回一个布尔 Series，如果元素不为 Null 则为 True。
    /// </summary>
    public Series IsNotNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNotNull(Handle);
        return new Series(newHandle);
    }
    // ==========================================
    // Float Checks (数值检查)
    // ==========================================
    /// <summary>
    /// Check whether this series is NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNan() => new(PolarsWrapper.SeriesIsNan(Handle));
    /// <summary>
    /// Check whether this series is not NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNotNan() => new(PolarsWrapper.SeriesIsNotNan(Handle));
    /// <summary>
    /// Check whether this series is finite
    /// </summary>
    /// <returns></returns>
    public Series IsFinite() => new(PolarsWrapper.SeriesIsFinite(Handle));
    /// <summary>
    /// Check whether this series is infinite
    /// </summary>
    /// <returns></returns>
    public Series IsInfinite() => new(PolarsWrapper.SeriesIsInfinite(Handle));
    // ==========================================
    // Conversions (Arrow / DataFrame)
    // ==========================================

    /// <summary>
    /// Zero-copy convert to Apache Arrow Array.
    /// </summary>
    public IArrowArray ToArrow()
    {
        return PolarsWrapper.SeriesToArrow(Handle);
    }
    /// <summary>
    /// Low-level entry point: Create Series from existing Arrow Array.
    /// </summary>
    public static Series FromArrow(string name, IArrowArray arrowArray)
    {
        var handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
        return new Series(handle);
    }

    // ==========================================
    // High-Level Factories
    // ==========================================
    /// <summary>
    /// Create a Series from a list of objects, primitives, or nested lists.
    /// Uses Polars.NET.Core to handle Arrow conversion and FFI transfer.
    /// </summary>
    public static Series From<T>(string name, IEnumerable<T> data) 
    {
        // 1. 调用 Core 层的转换器：IEnumerable<T> -> IArrowArray
        // (原 ArrowArrayFactory.Build)
        using var arrowArray = ArrowConverter.Build(data);

        // 2. 调用 Core 层的 FFI 桥梁：IArrowArray -> SeriesHandle
        // (原 Series.FromArrow 的底层逻辑)
        var handle = ArrowFfiBridge.ImportSeries(name, arrowArray);

        // 3. 封装为 C# API 对象
        return new Series(handle);
    }
    /// <summary>
    /// Convert this single Series into a DataFrame.
    /// </summary>
    public DataFrame ToFrame()
    {
        return new DataFrame(PolarsWrapper.SeriesToFrame(Handle));
    }
    /// <summary>
    /// Dispose the underlying SeriesHandle.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }
}