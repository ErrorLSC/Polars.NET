using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    /// <summary>
    /// [ILP Optimized] ReadOnlySpan<TimeSpan> -> Int64[] (Microseconds)
    /// Logic: Ticks / 10
    /// Use Unroll 8 to hide integer division latency.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe long[] UnzipTimeSpanToUs(ReadOnlySpan<TimeSpan> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);

        ref TimeSpan srcRef = ref MemoryMarshal.GetReference(data);

        fixed (TimeSpan* pSrc = &srcRef)
        fixed (long* pDst = values)
        {
            // TimeSpan is long (Ticks)
            long* pRawSrc = (long*)pSrc;
            
            int i = 0;
            int limit = len - 8;

            // =========================================================
            // Unroll 8 (ILP)
            // =========================================================
            for (; i <= limit; i += 8)
            {
                pDst[i]     = pRawSrc[i]     / 10;
                pDst[i + 1] = pRawSrc[i + 1] / 10;
                pDst[i + 2] = pRawSrc[i + 2] / 10;
                pDst[i + 3] = pRawSrc[i + 3] / 10;
                pDst[i + 4] = pRawSrc[i + 4] / 10;
                pDst[i + 5] = pRawSrc[i + 5] / 10;
                pDst[i + 6] = pRawSrc[i + 6] / 10;
                pDst[i + 7] = pRawSrc[i + 7] / 10;
            }

            // Tail
            for (; i < len; i++)
            {
                pDst[i] = pRawSrc[i] / 10;
            }
        }
        return values;
    }
    
    /// <summary>
    /// [Scalar Extreme] ReadOnlySpan<TimeSpan?> -> (Int64[], Validity)
    /// Layout: 16 Bytes [Bool(1), Pad(7), Ticks(8)]
    /// Logic: Ticks / 10 -> Microseconds
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (long[] values, byte[]? validity) UnzipTimeSpanToUs(ReadOnlySpan<TimeSpan?> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        ref TimeSpan? srcRef = ref MemoryMarshal.GetReference(data);

        fixed (TimeSpan?* pSrc = &srcRef)
        fixed (long* pDst = values)
        {
            // convert to 2 long* (16 bytes)
            long* pRawSrc = (long*)pSrc;
            
            int i = 0;
            int limit = len - 4; // Unroll 4 (Cache Line friendly for 16-byte items)

            for (; i <= limit; i += 4)
            {
                HandleTimeSpanItem(i,     pRawSrc, pDst, ref validity, ref validRef, byteLen);
                HandleTimeSpanItem(i + 1, pRawSrc, pDst, ref validity, ref validRef, byteLen);
                HandleTimeSpanItem(i + 2, pRawSrc, pDst, ref validity, ref validRef, byteLen);
                HandleTimeSpanItem(i + 3, pRawSrc, pDst, ref validity, ref validRef, byteLen);
            }

            for (; i < len; i++)
            {
                HandleTimeSpanItem(i, pRawSrc, pDst, ref validity, ref validRef, byteLen);
            }
        }

        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void HandleTimeSpanItem(
        int i,
        long* pRawSrc,
        long* pDst,
        ref byte[]? validity,
        ref byte validRef,
        int byteLen)
    {
        // Calc Current Item Address
        long* pItem = pRawSrc + (i * 2);
        
        // Header (Bool)
        byte hasValue = *(byte*)pItem; 

        if (hasValue != 0)
        {
            // Value (Offset 8 bytes)
            long ticks = *(pItem + 1);
            
            // Logic: Ticks / 10
            pDst[i] = ticks / 10;

            // Validity
            if (validity != null)
            {
                if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                target |= (byte)(1 << (i & 7));
            }
        }
        else
        {
            // Null Handling
            if (validity == null)
            {
                validity = new byte[byteLen];
                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                int bytesToFill = i >> 3;
                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                int remainingBits = i & 7;
                if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
            }
            pDst[i] = 0; 
        }
    }

    // ========================================================================
    // Array Overloads for backward API compatibility (Inlined)
    // ========================================================================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long[] UnzipTimeSpanToUs(TimeSpan[] data) => UnzipTimeSpanToUs(new ReadOnlySpan<TimeSpan>(data));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipTimeSpanToUs(TimeSpan?[] data) => UnzipTimeSpanToUs(new ReadOnlySpan<TimeSpan?>(data));
}

public static class DurationFormatter
{
    /// <summary>
    /// Format C# Duration to Polars duration string
    /// </summary>
    public static string ToPolarsString(TimeSpan ts)
    {
        if (ts == TimeSpan.Zero) return "0s";

        var sb = new StringBuilder();

        // Polars suffixs: ns, us, ms, s, m, h, d, w
        
        // 1. Days (d)
        if (ts.Days > 0) sb.Append($"{ts.Days}d");

        // 2. Hours (h)
        if (ts.Hours > 0) sb.Append($"{ts.Hours}h");

        // 3. Minutes (m)
        if (ts.Minutes > 0) sb.Append($"{ts.Minutes}m");

        // 4. Seconds (s)
        if (ts.Seconds > 0) sb.Append($"{ts.Seconds}s");

        // 5. Milliseconds (ms)
        if (ts.Milliseconds > 0) sb.Append($"{ts.Milliseconds}ms");

        // 6. Microseconds (us)
        if (ts.Microseconds > 0) sb.Append($"{ts.Microseconds}us");

        // 7. Nanoseconds (ns)
        if (ts.Nanoseconds > 0) sb.Append($"{ts.Nanoseconds}ns");

        return sb.ToString();
    }
    
    [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(ts))]
    public static string? ToPolarsString(TimeSpan? ts)
    {
        return ts.HasValue ? ToPolarsString(ts.Value) : null;
    }
}

public static class PolarsExtensions
{
    // ts.ToPolarsDuration()
    public static string ToPolarsDuration(this TimeSpan ts) 
        => DurationFormatter.ToPolarsString(ts);

    // tsNullable.ToPolarsDuration()
    public static string? ToPolarsDuration(this TimeSpan? ts) 
        => DurationFormatter.ToPolarsString(ts);
}