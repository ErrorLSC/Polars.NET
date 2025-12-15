using Polars.NET.Core;
using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Apache.Arrow.Types;

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
    /// Get an item at the specified index as object (boxed).
    /// Note: For Struct/List types, please use <see cref="GetValue{T}(long)"/> explicitly.
    /// </summary>
    public object? this[long index]
    {
        get
        {
            // 获取 Polars 的类型字符串
            var dtype = DataTypeName;

            // =========================================================
            // 1. Integers
            // =========================================================
            if (dtype == "Int32") return GetValue<int>(index);
            if (dtype == "Int64") return GetValue<long>(index);
            if (dtype == "Int16") return GetValue<short>(index);
            if (dtype == "Int8")  return GetValue<sbyte>(index);
            
            if (dtype == "UInt32") return GetValue<uint>(index);
            if (dtype == "UInt64") return GetValue<ulong>(index);
            if (dtype == "UInt16") return GetValue<ushort>(index);
            if (dtype == "UInt8")  return GetValue<byte>(index);

            // =========================================================
            // 2. Floats & Decimal
            // =========================================================
            if (dtype == "Float64") return GetValue<double>(index);
            if (dtype == "Float32") return GetValue<float>(index);
            if (dtype.StartsWith("decimal")) return GetValue<decimal>(index);

            // =========================================================
            // 3. String & Bool
            // =========================================================
            if (dtype == "String") return GetValue<string>(index);
            if (dtype == "Boolean") return GetValue<bool>(index);
            if (dtype == "Binary") return GetValue<byte[]>(index);

            // =========================================================
            // 4. Temporal (我们新加的兄弟们)
            // =========================================================
            if (dtype == "date") return GetValue<DateOnly>(index);
            if (dtype == "time") return GetValue<TimeOnly>(index);
            
            // Duration 可能带有单位后缀 (e.g. "Duration(us)")，用 StartsWith
            if (dtype.StartsWith("duration")) return GetValue<TimeSpan>(index);
            
            // Datetime 可能带时区 (e.g. "Datetime(us, Asia/Shanghai)")
            if (dtype.StartsWith("datetime")) 
            {
                // 优先尝试返回 DateTimeOffset，因为它能携带时区信息
                // 我们的 ArrowReader 已经支持了自动处理 Datetime -> DateTimeOffset
                return GetValue<DateTimeOffset>(index);
            }

            // =========================================================
            // 5. Complex Types (Struct, List)
            // =========================================================
            // 对于复杂类型，我们无法推断用户想映射成什么 C# 类，所以抛错引导
            if (dtype.StartsWith("Struct") || dtype.StartsWith("List"))
            {
                throw new NotSupportedException(
                    $"Cannot access complex type '{dtype}' via non-generic indexer. " +
                    $"Please use series.GetValue<T>(index) to specify the target C# class or List type.");
            }

            // Fallback
            throw new NotSupportedException($"DataType '{dtype}' is not supported in the non-generic indexer.");
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

    // ------------------------------------------
    // 🚀 1. Fast Path (Primitives)
    // 直接走 P/Invoke，性能最高
    // ------------------------------------------
    
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

    // ------------------------------------------
    // 🐢 2. Universal Path (Complex Types)
    // 委托给 ArrowConverter，逻辑统一
    // ------------------------------------------

    /// <summary>
    /// Create a Series from an array of DateTime values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateTime[] data)
    {
        // 1. 转 Arrow
        using var arrowArray = ArrowConverter.Build(data);
        // 2. 导入 Handle (这一步会自动转移所有权给 Rust)
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    /// <summary>
    /// Create a Series from an array of Nullable DateTime values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateTime?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    
    /// <summary>
    /// Create a Series from an array of TimeSpan values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeSpan[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of Nullable TimeSpan values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeSpan?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    /// <summary>
    /// Create a Series from an array of DateOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateOnly[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of Nullable DateOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateOnly?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    /// <summary>
    /// Create a Series from an array of TimeOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeOnly[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of Nullable TimeOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeOnly?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of decimals.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, decimal[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of nullable decimals.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, decimal?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
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