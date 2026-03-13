using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    /// <summary>
    /// [ILP Optimized] ReadOnlySpan<DateTimeOffset> -> UTC Microseconds (long[])
    /// Calc (UtcTicks - Epoch) / 10
    /// Unified to UTC。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static long[] UnzipDateTimeOffsetToUs(ReadOnlySpan<DateTimeOffset> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);

        long epoch = 621355968000000000; 

        // Pointer Ops
        // DateTimeOffset is a struct contains (DateTime DateTime, short OffsetMinutes)
        ref DateTimeOffset srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        int i = 0;
        // Unroll 8
        int limit = len - 8;
        for (; i <= limit; i += 8)
        {
            Unsafe.Add(ref dstRef, i)     = (Unsafe.Add(ref srcRef, i).UtcTicks     - epoch) / 10;
            Unsafe.Add(ref dstRef, i + 1) = (Unsafe.Add(ref srcRef, i + 1).UtcTicks - epoch) / 10;
            Unsafe.Add(ref dstRef, i + 2) = (Unsafe.Add(ref srcRef, i + 2).UtcTicks - epoch) / 10;
            Unsafe.Add(ref dstRef, i + 3) = (Unsafe.Add(ref srcRef, i + 3).UtcTicks - epoch) / 10;
            Unsafe.Add(ref dstRef, i + 4) = (Unsafe.Add(ref srcRef, i + 4).UtcTicks - epoch) / 10;
            Unsafe.Add(ref dstRef, i + 5) = (Unsafe.Add(ref srcRef, i + 5).UtcTicks - epoch) / 10;
            Unsafe.Add(ref dstRef, i + 6) = (Unsafe.Add(ref srcRef, i + 6).UtcTicks - epoch) / 10;
            Unsafe.Add(ref dstRef, i + 7) = (Unsafe.Add(ref srcRef, i + 7).UtcTicks - epoch) / 10;
        }

        // Tail
        for (; i < len; i++)
        {
            Unsafe.Add(ref dstRef, i) = (Unsafe.Add(ref srcRef, i).UtcTicks - epoch) / 10;
        }

        return values;
    }
    /// <summary>
    /// ReadOnlySpan<DateTimeOffset?> -> (Microseconds[], Validity[])
    /// Extracts UtcTicks and calculates microseconds since Unix epoch.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipDateTimeOffsetToUs(ReadOnlySpan<DateTimeOffset?> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long epoch = 621355968000000000;

        ref DateTimeOffset? srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            ref DateTimeOffset? item = ref Unsafe.Add(ref srcRef, i);
            
            if (item.HasValue)
            {
                long utcTicks = item.GetValueOrDefault().UtcTicks;
                Unsafe.Add(ref dstRef, i) = (utcTicks - epoch) / 10;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    validity = new byte[byteLen];
                    validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    int bytesToFill = i >> 3;
                    if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                    int remainingBits = i & 7;
                    if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        return (values, validity);
    }
}