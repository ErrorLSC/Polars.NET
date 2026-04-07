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
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle CreateSpan<T>(string name, ReadOnlySpan<T> data)
    {
        Type t = typeof(T);

        // ==========================================
        // 1. Primitive Types
        // ==========================================
        if (t == typeof(sbyte)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, sbyte>(data));
        else if (t == typeof(short)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, short>(data));
        else if (t == typeof(int)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, int>(data));
        else if (t == typeof(long)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, long>(data));
        else if (t == typeof(Int128)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, Int128>(data));

        else if (t == typeof(byte)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, byte>(data));
        else if (t == typeof(ushort)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, ushort>(data));
        else if (t == typeof(uint)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, uint>(data));
        else if (t == typeof(ulong)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, ulong>(data));
        else if (t == typeof(UInt128)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, UInt128>(data));

        else if (t == typeof(Half)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, Half>(data));
        else if (t == typeof(float)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, float>(data));
        else if (t == typeof(double)) return PolarsWrapper.SeriesNew(name, ReinterpretSpan<T, double>(data));

        // ==========================================
        // 2. Special Packed Primitives (Decimal / Bool)
        // ==========================================
        else if (t == typeof(decimal))
        {
            var span = ReinterpretSpan<T, decimal>(data);
            var (vals, scale) = DecimalPacker.Pack(span); 
            return PolarsWrapper.SeriesNewDecimal(name, vals, default, scale);
        }
        else if (t == typeof(decimal?))
        {
            var span = ReinterpretSpan<T, decimal?>(data);
            var (vals,validity, scale) = DecimalPacker.Pack(span); 
            return PolarsWrapper.SeriesNewDecimal(name, vals, validity, scale);
        }
        else if (t == typeof(bool))
        {
            var span = ReinterpretSpan<T, bool>(data);
            var packed = BoolPacker.Pack(span); 
            return PolarsWrapper.SeriesNew(name, packed, default, (nuint)data.Length);
        }
        else if (t == typeof(bool?))
        {
            var span = ReinterpretSpan<T, bool?>(data);
            var (packed,validity) = BoolPacker.PackNullable(span); 
            return PolarsWrapper.SeriesNew(name, packed, validity, (nuint)data.Length);
        }

        // ==========================================
        // 3. Nullables (Unzip -> Span)
        // ==========================================
        else if (t == typeof(sbyte?))
        {
            var span = ReinterpretSpan<T, sbyte?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span); 
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(byte?))
        {
            var span = ReinterpretSpan<T, byte?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span); 
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(short?))
        {
            var span = ReinterpretSpan<T, short?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span); 
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(ushort?))
        {
            var span = ReinterpretSpan<T, ushort?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span); 
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(int?))
        {
            var span = ReinterpretSpan<T, int?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span); 
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(uint?))
        {
            var span = ReinterpretSpan<T, uint?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span); 
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(long?))
        {
            var span = ReinterpretSpan<T, long?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(ulong?))
        {
            var span = ReinterpretSpan<T, ulong?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(Int128?))
        {
            var span = ReinterpretSpan<T, Int128?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(UInt128?))
        {
            var span = ReinterpretSpan<T, UInt128?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(Half?))
        {
            var span = ReinterpretSpan<T, Half?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(float?))
        {
            var span = ReinterpretSpan<T, float?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }
        else if (t == typeof(double?))
        {
            var span = ReinterpretSpan<T, double?>(data);
            var (vals, mask) = ArrayHelper.UnzipNullable(span);
            return PolarsWrapper.SeriesNew(name, vals, mask);
        }

        // ==========================================
        // 4. String
        // ==========================================
        else if (t == typeof(string))
        {
            var span = ReinterpretSpan<T, string?>(data);

            return PolarsWrapper.SeriesNewStringSimd(name, span);
        }

        // ==========================================
        // 5. Temporal Types
        // ==========================================
        else if (t == typeof(DateTime))
        {
            var span = ReinterpretSpan<T, DateTime>(data);
            var vals = ArrayHelper.UnzipDateTimeToUs(span); 
            return PolarsWrapper.SeriesNewDatetime(name, vals, default, null);
        }
        else if (t == typeof(DateTimeOffset))
        {
            var span = ReinterpretSpan<T, DateTimeOffset>(data);
            var vals = ArrayHelper.UnzipDateTimeOffsetToUs(span);
            return PolarsWrapper.SeriesNewDatetime(name, vals, default, "UTC");
        }
        else if (t == typeof(TimeSpan))
        {
            var span = ReinterpretSpan<T, TimeSpan>(data);
            var vals = ArrayHelper.UnzipTimeSpanToUs(span);
            return PolarsWrapper.SeriesNewDuration(name, vals, default);
        }
        else if (t == typeof(TimeOnly))
        {
            var span = ReinterpretSpan<T, TimeOnly>(data);
            var vals = ArrayHelper.UnzipTimeOnlyToNs(span);
            return PolarsWrapper.SeriesNewTime(name, vals, default);
        }
        else if (t == typeof(DateOnly))
        {
            var span = ReinterpretSpan<T, DateOnly>(data);
            var vals = ArrayHelper.UnzipDateOnlyToInt32(span);
            return PolarsWrapper.SeriesNewDate(name, vals, default);
        }
        // ==========================================
        // Temporal Nullables (DateTime?, DateTimeOffset?, TimeSpan?)
        // ==========================================
        else if (t == typeof(DateTime?))
        {
            var span = ReinterpretSpan<T, DateTime?>(data);
            var (vals, mask) = ArrayHelper.UnzipDateTimeToUs(span); 
            return PolarsWrapper.SeriesNewDatetime(name, vals, mask, null);
        }
        else if (t == typeof(DateTimeOffset?))
        {
            var span = ReinterpretSpan<T, DateTimeOffset?>(data);
            var (vals, mask) = ArrayHelper.UnzipDateTimeOffsetToUs(span);
            return PolarsWrapper.SeriesNewDatetime(name, vals, mask, "UTC");
        }
        else if (t == typeof(TimeSpan?))
        {
            var span = ReinterpretSpan<T, TimeSpan?>(data);
            var (vals, mask) = ArrayHelper.UnzipTimeSpanToUs(span);
            return PolarsWrapper.SeriesNewDuration(name, vals, mask);
        }
        else if (t == typeof(TimeOnly?))
        {
            var span = ReinterpretSpan<T, TimeOnly?>(data);
            var (vals, mask) = ArrayHelper.UnzipTimeOnlyToNs(span);
            return PolarsWrapper.SeriesNewTime(name, vals, mask);
        }
        else if (t == typeof(DateOnly?))
        {
            var span = ReinterpretSpan<T, DateOnly?>(data);
            var (vals, mask) = ArrayHelper.UnzipDateOnlyToInt32(span);
            return PolarsWrapper.SeriesNewDate(name, vals, mask);
        }

        // ==========================================
        // F# ValueOption<T> Support (Struct Option)
        // ==========================================
        
        // --- 1. Primitives ---
        else if (t == typeof(FSharpValueOption<sbyte>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<sbyte>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<byte>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<byte>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<short>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<short>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<ushort>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<ushort>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<int>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<int>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<uint>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<uint>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<long>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<long>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<ulong>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<ulong>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<Int128>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<Int128>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<UInt128>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<UInt128>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<Half>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<Half>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<float>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<float>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<double>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<double>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }

        // --- 2. Bool (Packed) ---
        else if (t == typeof(FSharpValueOption<bool>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<bool>>(data);
            var (vals, valid) = FSharpHelper.PackValueOptionBool(span);
            return PolarsWrapper.SeriesNew(name, vals, valid, (nuint)data.Length);
        }

        // --- 3. String ---
        else if (t == typeof(FSharpValueOption<string>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<string>>(data);
            var unwrapped = FSharpHelper.UnwrapValueOptionString(span); 
            return PolarsWrapper.SeriesNewStringSimd(name, unwrapped);
        }

        // --- 4. Temporals ---
        else if (t == typeof(FSharpValueOption<DateTime>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<DateTime>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOptionDateTimeToUs(span);
            return PolarsWrapper.SeriesNewDatetime(name, vals, valid, null);
        }
        else if (t == typeof(FSharpValueOption<DateOnly>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<DateOnly>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOptionDateOnlyToInt32(span);
            return PolarsWrapper.SeriesNewDate(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<TimeOnly>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<TimeOnly>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOptionTimeOnlyToNs(span);
            return PolarsWrapper.SeriesNewTime(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<TimeSpan>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<TimeSpan>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOptionTimeSpanToUs(span);
            return PolarsWrapper.SeriesNewDuration(name, vals, valid);
        }
        else if (t == typeof(FSharpValueOption<DateTimeOffset>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<DateTimeOffset>>(data);
            var (vals, valid) = FSharpHelper.UnzipValueOptionDateTimeOffsetToUs(span);
            return PolarsWrapper.SeriesNewDatetime(name, vals, valid, "UTC");
        }

        // --- 5. Decimal ---
        else if (t == typeof(FSharpValueOption<decimal>)) {
            var span = ReinterpretSpan<T, FSharpValueOption<decimal>>(data);
            var nullable = FSharpHelper.UnwrapValueOptionDecimal(span); 
            var (vals, valid, scale) = DecimalPacker.Pack(nullable);
            return PolarsWrapper.SeriesNewDecimal(name, vals, valid, scale);
        }
        // ==========================================
        // F# Option<T> Support (Reference Option)
        // ==========================================
        
        // --- 1. Primitives ---
        else if (t == typeof(FSharpOption<sbyte>)) {
            var span = ReinterpretSpan<T, FSharpOption<sbyte>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<byte>)) {
            var span = ReinterpretSpan<T, FSharpOption<byte>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<short>)) {
            var span = ReinterpretSpan<T, FSharpOption<short>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<ushort>)) {
            var span = ReinterpretSpan<T, FSharpOption<ushort>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<int>)) {
            var span = ReinterpretSpan<T, FSharpOption<int>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<uint>)) {
            var span = ReinterpretSpan<T, FSharpOption<uint>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<long>)) {
            var span = ReinterpretSpan<T, FSharpOption<long>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<ulong>)) {
            var span = ReinterpretSpan<T, FSharpOption<ulong>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<Int128>)) {
            var span = ReinterpretSpan<T, FSharpOption<Int128>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<UInt128>)) {
            var span = ReinterpretSpan<T, FSharpOption<UInt128>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<Half>)) {
            var span = ReinterpretSpan<T, FSharpOption<Half>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<float>)) {
            var span = ReinterpretSpan<T, FSharpOption<float>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<double>)) {
            var span = ReinterpretSpan<T, FSharpOption<double>>(data);
            var (vals, valid) = FSharpHelper.UnzipOption(span);
            return PolarsWrapper.SeriesNew(name, vals, valid);
        }

        // --- 2. Bool (Packed) ---
        else if (t == typeof(FSharpOption<bool>)) {
            var span = ReinterpretSpan<T, FSharpOption<bool>>(data);
            var (vals, valid) = FSharpHelper.PackOptionBool(span);
            return PolarsWrapper.SeriesNew(name, vals, valid, (nuint)data.Length);
        }

        // --- 3. String ---
        else if (t == typeof(FSharpOption<string>)) {
            var span = ReinterpretSpan<T, FSharpOption<string>>(data);
            var unwrapped = FSharpHelper.UnwrapOptionString(span); 
            return PolarsWrapper.SeriesNewStringSimd(name, unwrapped);
        }

        // --- 4. Temporals ---
        else if (t == typeof(FSharpOption<DateTime>)) {
            var span = ReinterpretSpan<T, FSharpOption<DateTime>>(data);
            var (vals, valid) = FSharpHelper.UnzipOptionDateTimeToUs(span);
            return PolarsWrapper.SeriesNewDatetime(name, vals, valid, null);
        }
        else if (t == typeof(FSharpOption<DateOnly>)) {
            var span = ReinterpretSpan<T, FSharpOption<DateOnly>>(data);
            var (vals, valid) = FSharpHelper.UnzipOptionDateOnlyToInt32(span);
            return PolarsWrapper.SeriesNewDate(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<TimeOnly>)) {
            var span = ReinterpretSpan<T, FSharpOption<TimeOnly>>(data);
            var (vals, valid) = FSharpHelper.UnzipOptionTimeOnlyToNs(span);
            return PolarsWrapper.SeriesNewTime(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<TimeSpan>)) {
            var span = ReinterpretSpan<T, FSharpOption<TimeSpan>>(data);
            var (vals, valid) = FSharpHelper.UnzipOptionTimeSpanToUs(span);
            return PolarsWrapper.SeriesNewDuration(name, vals, valid);
        }
        else if (t == typeof(FSharpOption<DateTimeOffset>)) {
            var span = ReinterpretSpan<T, FSharpOption<DateTimeOffset>>(data);
            var (vals, valid) = FSharpHelper.UnzipOptionDateTimeOffsetToUs(span);
            return PolarsWrapper.SeriesNewDatetime(name, vals, valid, "UTC");
        }

        // --- 5. Decimal ---
        else if (t == typeof(FSharpOption<decimal>)) {
            var span = ReinterpretSpan<T, FSharpOption<decimal>>(data);
            var nullable = FSharpHelper.UnwrapOptionDecimal(span); 
            var (vals, valid, scale) = DecimalPacker.Pack(nullable);
            return PolarsWrapper.SeriesNewDecimal(name, vals, valid, scale);
        }
        return null!;
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
            // Fixed Size Arrays (2D)
            // ==========================================
            sbyte[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            byte[,] v    => PolarsWrapper.SeriesNewFixedArray(name, v),
            short[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            ushort[,] v  => PolarsWrapper.SeriesNewFixedArray(name, v),
            int[,] v     => PolarsWrapper.SeriesNewFixedArray(name, v),
            uint[,] v    => PolarsWrapper.SeriesNewFixedArray(name, v),
            long[,] v    => PolarsWrapper.SeriesNewFixedArray(name, v),
            ulong[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            Half[,] v    => PolarsWrapper.SeriesNewFixedArray(name, v),
            float[,] v   => PolarsWrapper.SeriesNewFixedArray(name, v),
            double[,] v  => PolarsWrapper.SeriesNewFixedArray(name, v),
            decimal[,] v => PolarsWrapper.SeriesNewFixedArray(name, v),
            Int128[,] v  => PolarsWrapper.SeriesNewFixedArray(name, v),
            UInt128[,] v => PolarsWrapper.SeriesNewFixedArray(name, v),

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
    private static SeriesHandle CreateFromArrowViaReflection(string name, Array array)
    {
        using var arrowArray = ArrowConverter.Build((dynamic)array); 
        return ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySpan<U> ReinterpretSpan<T, U>(ReadOnlySpan<T> span)
    {
        ref T srcRef = ref MemoryMarshal.GetReference(span);
        ref U dstRef = ref Unsafe.As<T, U>(ref srcRef);
        return MemoryMarshal.CreateReadOnlySpan(ref dstRef, span.Length);
    }
}