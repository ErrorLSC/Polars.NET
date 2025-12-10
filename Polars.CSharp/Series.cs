using System;
using Polars.Native;
using Apache.Arrow;

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
                var decArray = array as decimal[] ?? array.Cast<decimal>().ToArray();
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

        throw new NotSupportedException($"Type {type.Name} is not supported for Series creation via Create<T>.");
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