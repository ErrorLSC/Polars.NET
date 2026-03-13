using Polars.NET.Core.Arrow;
using Microsoft.FSharp.Core;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
namespace Polars.NET.Core.Helpers;

/// <summary>
/// Central factory for creating Series handles from C# arrays.
/// Handles primitives, nullables, temporals, and multi-dimensional arrays.
/// </summary>
public static class SeriesFactory
{
    /// <summary>
    /// Generic type entry for Series create
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <returns></returns>
    public static SeriesHandle CreateGenericType<T>(string name, IEnumerable<T> data)
    {
        if (data is Array array)
        {
            return Create(name, array);
        }

        using var arrowArray = ArrowConverter.Build(data);
        return ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    // public static unsafe SeriesHandle Create<T>(string name, ReadOnlySpan<T> data) where T : unmanaged
    // {
    //     ref T srcRef = ref MemoryMarshal.GetReference(data);
    //     fixed (T* ptr = &srcRef)
    //     {
    //         // Call the FFI layer. Assuming PolarsWrapper handles the memory copy to Rust/Arrow buffer.
    //         return PolarsWrapper.SeriesNew(name, ptr, (nuint)data.Length);
    //     }
    // }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle CreateSpan<T>(string name, ReadOnlySpan<T> data)
    {
        if (data.IsEmpty)
            throw new ArgumentException("Cannot create Series from empty span.");

        Type t = typeof(T);

        // ==========================================
        // 1. Signed Integers 
        // ==========================================
        if (t == typeof(int))
        {
            var span = ReinterpretSpan<T, int>(data);
            // TODO: 这里需要对接接受 ref 指针和 length 的 FFI Wrapper，实现真正的零拷贝！
            return PolarsWrapper.SeriesNew(name, span.ToArray(), null); // 暂时先 ToArray，下面会讲 FFI 升级
        }
        else if (t == typeof(int?))
        {
            var span = ReinterpretSpan<T, int?>(data);
            // 完美接入我们刚升级的核动力 Helper！
            var (vals, mask) = ArrayHelper.UnzipNullable(span); 
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(long))
        {
            var span = ReinterpretSpan<T, long>(data);
            return PolarsWrapper.SeriesNew(name, span.ToArray(), null);
        }
        else if (t == typeof(long?))
        {
            var span = ReinterpretSpan<T, long?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }

        // ==========================================
        // F# ValueOption<T> Support
        // ==========================================
        else if (t == typeof(FSharpValueOption<int>))
        {
            var span = ReinterpretSpan<T, FSharpValueOption<int>>(data);
            // 完美接入刚改好的 FSharpHelper
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<DateTime>))
        {
            var span = ReinterpretSpan<T, FSharpValueOption<DateTime>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOptionDateTimeToUs(span);
            return PolarsWrapper.SeriesNewDatetime(name, vals, valid, null);
        }

        // ==========================================
        // String & Temporal
        // ==========================================
        else if (t == typeof(string))
        {
            // 对于引用类型，也可以用我们写的神器
            var span = ReinterpretSpan<T, string>(data);
            return PolarsWrapper.SeriesNewStringSimd(name, span.ToArray());
        }
        else if (t == typeof(DateTime))
        {
            var span = ReinterpretSpan<T, DateTime>(data);
            var vals = ArrayHelper.UnzipDateTimeToUs(span); // 假设你的 ArrayHelper 也增加了 Span 支持
            return PolarsWrapper.SeriesNewDatetime(name, vals, null, null);
        }

        // ... 继续添加其他类型 (byte, short, float, double 等等)

        throw new NotSupportedException($"Type {t} is not supported for Span creation.");
    }
    /// <summary>
    /// Creates a SeriesHandle from a generic Array.
    /// Uses pattern matching to dispatch to the correct PolarsWrapper method.
    /// </summary>
    public static SeriesHandle Create(string name, Array array)
    {
        var handle = array switch
        {

            // ==========================================
            // 1. Signed Integers 
            // ==========================================
            sbyte[] v when array.GetType() == typeof(sbyte[])   => PolarsWrapper.SeriesNew(name, v, null),
            sbyte?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),
            
            short[] v when array.GetType() == typeof(short[])  => PolarsWrapper.SeriesNew(name, v, null),
            short?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),
            
            int[] v  when array.GetType() == typeof(int[])  => PolarsWrapper.SeriesNew(name, v, null),
            int?[] v    => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),
            
            long[] v when array.GetType() == typeof(long[]) => PolarsWrapper.SeriesNew(name, v, null),
            long?[] v   => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            Int128[] v when array.GetType() == typeof(Int128[]) => PolarsWrapper.SeriesNew(name, v, null),
            Int128?[] v => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            // ==========================================
            // 2. Unsigned Integers
            // ==========================================
            byte[] v when array.GetType() == typeof(byte[])   => PolarsWrapper.SeriesNew(name, v, null),
            byte?[] v   => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            ushort[] v when array.GetType() == typeof(ushort[]) => PolarsWrapper.SeriesNew(name, v, null),
            ushort?[] v => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            uint[] v when array.GetType() == typeof(uint[])   => PolarsWrapper.SeriesNew(name, v, null),
            uint?[] v   => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            ulong[] v when array.GetType() == typeof(ulong[])  => PolarsWrapper.SeriesNew(name, v, null),
            ulong?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            UInt128[] v when array.GetType() == typeof(UInt128[]) => PolarsWrapper.SeriesNew(name, v, null),
            UInt128?[] v => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            // ==========================================
            // 3. Floating Point
            // ==========================================
            Half[] v   => PolarsWrapper.SeriesNew(name, v, null),
            Half?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr, Half.NaN), PolarsWrapper.SeriesNew),
            float[] v   => PolarsWrapper.SeriesNew(name, v, null),
            float?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr, float.NaN), PolarsWrapper.SeriesNew),

            double[] v  => PolarsWrapper.SeriesNew(name, v, null),
            double?[] v => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr, double.NaN), PolarsWrapper.SeriesNew),

            // ==========================================
            // 4. Decimal & Bool & String
            // ==========================================
            decimal[] v => PackDecimal(name, v),
            decimal?[] v => PackDecimalNullable(name, v),

            bool[] v => PackBool(name, v),
            bool?[] v => PackBoolNullable(name, v),
            
            string?[] v => PolarsWrapper.SeriesNewStringSimd(name, v),

            // ==========================================
            // 5. Temporal
            // ==========================================
            DateOnly[] v  => WithHelper(name, v, ArrayHelper.UnzipDateOnlyToInt32, PolarsWrapper.SeriesNewDate),
            DateOnly?[] v => WithHelperNullable(name, v, ArrayHelper.UnzipDateOnlyToInt32, PolarsWrapper.SeriesNewDate),

            TimeOnly[] v  => WithHelper(name, v, ArrayHelper.UnzipTimeOnlyToNs, PolarsWrapper.SeriesNewTime),
            TimeOnly?[] v => WithHelperNullable(name, v, ArrayHelper.UnzipTimeOnlyToNs, PolarsWrapper.SeriesNewTime),

            TimeSpan[] v  => WithHelper(name, v, ArrayHelper.UnzipTimeSpanToUs, PolarsWrapper.SeriesNewDuration),
            TimeSpan?[] v => WithHelperNullable(name, v, ArrayHelper.UnzipTimeSpanToUs, PolarsWrapper.SeriesNewDuration),

            DateTime[] v => CreateDateTime(name, v),
            DateTime?[] v => CreateDateTimeNullable(name, v),
            
            DateTimeOffset[] v => CreateDateTimeOffset(name, v),
            DateTimeOffset?[] v => CreateDateTimeOffsetNullable(name, v),

            // ==========================================
            // 6. Fixed Size Arrays (2D)
            // ==========================================
            sbyte[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            byte[,] v    => PolarsWrapper.SeriesNewFixedArray(name, v),
            short[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            ushort[,] v  => PolarsWrapper.SeriesNewFixedArray(name, v),
            int[,] v     => PolarsWrapper.SeriesNewFixedArray(name, v),
            uint[,] v    => PolarsWrapper.SeriesNewFixedArray(name, v),
            long[,] v    => PolarsWrapper.SeriesNewFixedArray(name, v),
            ulong[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            Half[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            float[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            double[,] v  => PolarsWrapper.SeriesNewFixedArray(name, v),
            decimal[,] v => PolarsWrapper.SeriesNewFixedArray(name, v),
            Int128[,] v  => PolarsWrapper.SeriesNewFixedArray(name, v),
            UInt128[,] v => PolarsWrapper.SeriesNewFixedArray(name, v),

            // ==========================================
            // F# Option<T> Support
            // ==========================================
            
            // 1. Primitives (Generic UnzipOption)
            FSharpOption<Half>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<byte>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<sbyte>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<short>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<ushort>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<int>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<uint>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<long>[] v   => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<ulong>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<Int128>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<UInt128>[] v    => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<double>[] v => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),
            FSharpOption<float>[] v  => CreateFSOpt(name, v, FSharpHelper.UnzipOption, PolarsWrapper.SeriesNew),

            // 2. Bool (Packed)
            FSharpOption<bool>[] v => CreateFSOptPacked(name, v, FSharpHelper.PackOptionBool),
            
            // 3. String
            FSharpOption<string>[] v => PolarsWrapper.SeriesNewStringSimd(name, FSharpHelper.UnwrapOptionString(v)),

            // 4. Temporals (Specific Unzippers)
            FSharpOption<DateTime>[] v       => CreateFSOpt(name, v, FSharpHelper.UnzipOptionDateTimeToUs, (n, vals, valid) => PolarsWrapper.SeriesNewDatetime(n, vals, valid, null)),
            FSharpOption<DateOnly>[] v       => CreateFSOpt(name, v, FSharpHelper.UnzipOptionDateOnlyToInt32, PolarsWrapper.SeriesNewDate),
            FSharpOption<TimeOnly>[] v       => CreateFSOpt(name, v, FSharpHelper.UnzipOptionTimeOnlyToNs, PolarsWrapper.SeriesNewTime),
            FSharpOption<TimeSpan>[] v       => CreateFSOpt(name, v, FSharpHelper.UnzipOptionTimeSpanToUs, PolarsWrapper.SeriesNewDuration),
            FSharpOption<DateTimeOffset>[] v => CreateFSOpt(name, v, FSharpHelper.UnzipOptionDateTimeOffsetToUs, (n, vals, valid) => PolarsWrapper.SeriesNewDatetime(n, vals, valid, "UTC")),

            // 5. Decimal
            FSharpOption<decimal>[] v      => CreateFSOptDecimal(name, v),
            
            // ==========================================
            // F# ValueOption<T> Support
            // ==========================================
            
            // 1. Primitives
            FSharpValueOption<Half>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<byte>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<sbyte>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<short>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<ushort>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<int>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<uint>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<long>[] v   => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<ulong>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<Int128>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<UInt128>[] v    => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<double>[] v => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),
            FSharpValueOption<float>[] v  => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOption, PolarsWrapper.SeriesNew),

            // 2. Bool
            FSharpValueOption<bool>[] v => CreateFSVOptPacked(name, v, FSharpHelper.PackValueOptionBool),

            // 3. String
            FSharpValueOption<string>[] v => PolarsWrapper.SeriesNewStringSimd(name, FSharpHelper.UnwrapValueOptionString(v)),

            // 4. Temporals
            FSharpValueOption<DateTime>[] v       => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOptionDateTimeToUs, (n, vals, valid) => PolarsWrapper.SeriesNewDatetime(n, vals, valid, null)),
            FSharpValueOption<DateOnly>[] v       => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOptionDateOnlyToInt32, PolarsWrapper.SeriesNewDate),
            FSharpValueOption<TimeOnly>[] v       => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOptionTimeOnlyToNs, PolarsWrapper.SeriesNewTime),
            FSharpValueOption<TimeSpan>[] v       => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOptionTimeSpanToUs, PolarsWrapper.SeriesNewDuration),
            FSharpValueOption<DateTimeOffset>[] v => CreateFSVOpt(name, v, FSharpHelper.UnzipValueOptionDateTimeOffsetToUs, (n, vals, valid) => PolarsWrapper.SeriesNewDatetime(n, vals, valid, "UTC")),
            
            // 5. Decimal
            FSharpValueOption<decimal>[] v => CreateFSVOptDecimal(name, v),

            _ => null
        };

        if (handle != null && !handle.IsInvalid)
        {
            return handle;
        }

        // Fallback to Arrow reflection for nested/complex types
        return CreateFromArrowViaReflection(name, array);
    }

    // --- Helpers ---

    private static SeriesHandle NewNullable<T, U>(
        string name, 
        T?[] data, 
        Func<T?[], (U[] values, byte[]? mask)> unzipFunc,
        Func<string, U[], byte[]?, SeriesHandle> factory)
        where T : struct 
    {
        var (vals, mask) = unzipFunc(data);
        return factory(name, vals, mask);
    }

    private static SeriesHandle WithHelper<T, U>(
        string name, 
        T[] data, 
        Func<T[], U[]> convFunc,
        Func<string, U[], byte[], SeriesHandle> wrapperFunc)
    {
        var vals = convFunc(data);
        return wrapperFunc(name, vals, null!);
    }

    private static SeriesHandle WithHelperNullable<T, U>(
        string name, 
        T?[] data, 
        Func<T?[], (U[] values, byte[]? mask)> convFunc,
        Func<string, U[], byte[]?, SeriesHandle> wrapperFunc)
        where T : struct
    {
        var (vals, mask) = convFunc(data);
        return wrapperFunc(name, vals, mask);
    }

    private static SeriesHandle CreateFromArrowViaReflection(string name, Array array)
    {
        using var arrowArray = ArrowConverter.Build((dynamic)array); 
        return ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    private static SeriesHandle PackDecimal(string name, decimal[] v)
    {
        var (vals, scale) = DecimalPacker.Pack(v);
        return PolarsWrapper.SeriesNewDecimal(name, vals, null, scale);
    }
    
    private static SeriesHandle PackDecimalNullable(string name, decimal?[] v)
    {
        var (vals, mask, scale) = DecimalPacker.Pack(v);
        return PolarsWrapper.SeriesNewDecimal(name, vals, mask, scale);
    }
    
    private static SeriesHandle PackBool(string name, bool[] v)
    {
         var packed = BoolPacker.Pack(v);
         return PolarsWrapper.SeriesNew(name, packed, null, (nuint)v.Length);
    }
    
    private static SeriesHandle PackBoolNullable(string name, bool?[] v)
    {
         var (packed, mask) = BoolPacker.PackNullable(v);
         return PolarsWrapper.SeriesNew(name, packed, mask, (nuint)v.Length);
    }
    
    private static SeriesHandle CreateDateTime(string name, DateTime[] v)
    {
        var vals = ArrayHelper.UnzipDateTimeToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, null, null);
    }
    
    private static SeriesHandle CreateDateTimeNullable(string name, DateTime?[] v)
    {
        var (vals, mask) = ArrayHelper.UnzipDateTimeToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, mask, null);
    }
    
    private static SeriesHandle CreateDateTimeOffset(string name, DateTimeOffset[] v)
    {
        var vals = ArrayHelper.UnzipDateTimeOffsetToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, null, "UTC"); 
    }
    
    private static SeriesHandle CreateDateTimeOffsetNullable(string name, DateTimeOffset?[] v)
    {
        var (vals, mask) = ArrayHelper.UnzipDateTimeOffsetToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, mask, "UTC");
    }
    // ==========================================
    // F# Helper Bridges
    // ==========================================

    // Option Helper
    private static SeriesHandle CreateFSOpt<T, U>(
        string name, 
        FSharpOption<T>[] data, 
        Func<FSharpOption<T>[], (U[], byte[]?)> unzip,
        Func<string, U[], byte[]?, SeriesHandle> factory)
    {
        var (vals, valid) = unzip(data);
        return factory(name, vals, valid);
    }

    // ValueOption Helper
    private static SeriesHandle CreateFSVOpt<T, U>(
        string name, 
        FSharpValueOption<T>[] data, 
        Func<FSharpValueOption<T>[], (U[], byte[]?)> unzip,
        Func<string, U[], byte[]?, SeriesHandle> factory)
    {
        var (vals, valid) = unzip(data);
        return factory(name, vals, valid);
    }

    // Bool Packed Helpers
    private static SeriesHandle CreateFSOptPacked(
        string name,
        FSharpOption<bool>[] data,
        Func<FSharpOption<bool>[], (byte[], byte[]?)> pack)
    {
        var (vals, valid) = pack(data);
        return PolarsWrapper.SeriesNew(name, vals, valid, (nuint)data.Length);
    }

    private static SeriesHandle CreateFSVOptPacked(
        string name,
        FSharpValueOption<bool>[] data,
        Func<FSharpValueOption<bool>[], (byte[], byte[]?)> pack)
    {
        var (vals, valid) = pack(data);
        return PolarsWrapper.SeriesNew(name, vals, valid, (nuint)data.Length);
    }

    private static SeriesHandle CreateFSOptDecimal(string name, FSharpOption<decimal>[] data)
    {
        // 1. Fast Unwrap
        var nullable = FSharpHelper.UnwrapOptionDecimal(data);

        // 2. Pack
        var (vals, valid, scale) = DecimalPacker.Pack(nullable);

        // 3. Create
        return PolarsWrapper.SeriesNewDecimal(name, vals, valid, scale);
    }

    private static SeriesHandle CreateFSVOptDecimal(string name, FSharpValueOption<decimal>[] data)
    {
        // 1. Fast Unwrap
        var nullable = FSharpHelper.UnwrapValueOptionDecimal(data);

        // 2. Pack
        var (vals, valid, scale) = DecimalPacker.Pack(nullable);

        // 3. Create
        return PolarsWrapper.SeriesNewDecimal(name, vals, valid, scale);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySpan<U> ReinterpretSpan<T, U>(ReadOnlySpan<T> span)
    {
        ref T srcRef = ref MemoryMarshal.GetReference(span);
        ref U dstRef = ref Unsafe.As<T, U>(ref srcRef);
        return MemoryMarshal.CreateReadOnlySpan(ref dstRef, span.Length);
    }
}