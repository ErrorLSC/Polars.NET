using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    /// <summary>
    /// ReadOnlySpan<DateTime> -> Microseconds[] (long[])
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe long[] UnzipDateTimeToUs(ReadOnlySpan<DateTime> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);

        // Constants 
        long mask = 0x3FFFFFFFFFFFFFFF;  // Ticks Mask (no Kind bits)
        long epoch = 621355968000000000; // 1970-01-01 Ticks
        ref DateTime srcRef = ref MemoryMarshal.GetReference(data);
        fixed (DateTime* pSrc = &srcRef)
        fixed (long* pDst = values)
        {
            // DateTime in mem is ulong (private ulong _dateData)
            long* pRawSrc = (long*)pSrc;
            
            int i = 0;
            
            // ---------------------------------------------------------
            // Main Loop: Unroll 8
            // ---------------------------------------------------------
            int limit = len - 8;
            for (; i <= limit; i += 8)
            {
                pDst[i]     = ((pRawSrc[i]     & mask) - epoch) / 10;
                pDst[i + 1] = ((pRawSrc[i + 1] & mask) - epoch) / 10;
                pDst[i + 2] = ((pRawSrc[i + 2] & mask) - epoch) / 10;
                pDst[i + 3] = ((pRawSrc[i + 3] & mask) - epoch) / 10;
                pDst[i + 4] = ((pRawSrc[i + 4] & mask) - epoch) / 10;
                pDst[i + 5] = ((pRawSrc[i + 5] & mask) - epoch) / 10;
                pDst[i + 6] = ((pRawSrc[i + 6] & mask) - epoch) / 10;
                pDst[i + 7] = ((pRawSrc[i + 7] & mask) - epoch) / 10;
            }

            // Tail Loop
            for (; i < len; i++)
            {
                pDst[i] = ((pRawSrc[i] & mask) - epoch) / 10;
            }
        }

        return values;
    }
    /// <summary>
    /// DateTime?[] -> (Microseconds[], Validity[])
    /// Logic：Mask Kind -> Subtract Epoch -> Divide by 10
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (long[] values, byte[]? validity) UnzipDateTimeToUs(ReadOnlySpan<DateTime?> data)
    {
        int len = data.Length;
        // Alloc target mem
        var values = GC.AllocateUninitializedArray<long>(len);
        int byteLen = (len + 7) >> 3;
        
        byte[]? validity = null;
        // lazy loading
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long mask = 0x3FFFFFFFFFFFFFFF; // Ticks Mask
        long epoch = 621355968000000000; // 1970-01-01 Ticks

        ref DateTime? srcRef = ref MemoryMarshal.GetReference(data);

        fixed (DateTime?* pSrc = &srcRef) // DateTime? mem layout: [Bool(1), Pad(7), Val(8)] (Stride 16)
        fixed (long* pDst = values)
        {
            long* pRawSrc = (long*)pSrc;

            int i = 0;

            // ---------------------------------------------------------
            // Main Loop (Scalar Unroll 4)
            // ---------------------------------------------------------
            // Unroll Method
            int limit = len - 4;
            for (; i <= limit; i += 4)
            {
                // Item 0
                HandleSingleDate(i, pRawSrc, pDst, ref validity, ref validRef, byteLen, len, mask, epoch);
                // Item 1
                HandleSingleDate(i + 1, pRawSrc, pDst, ref validity, ref validRef, byteLen, len, mask, epoch);
                // Item 2
                HandleSingleDate(i + 2, pRawSrc, pDst, ref validity, ref validRef, byteLen, len, mask, epoch);
                // Item 3
                HandleSingleDate(i + 3, pRawSrc, pDst, ref validity, ref validRef, byteLen, len, mask, epoch);
            }

            // Tail
            for (; i < len; i++)
            {
                HandleSingleDate(i, pRawSrc, pDst, ref validity, ref validRef, byteLen, len, mask, epoch);
            }
        }

        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void HandleSingleDate(
        int i, 
        long* pRawSrc, // DateTime? Array
        long* pDst, 
        ref byte[]? validity, 
        ref byte validRef, 
        int byteLen, 
        int totalLen,
        long mask, 
        long epoch)
    {
        // DateTime? Struct：
        // Byte 0: HasValue (1=True, 0=False)
        // Byte 8-15: Ticks (ulong)
        // Stride = 16 bytes
        
        // Safe ptr calc：
        byte* ptrBase = (byte*)pRawSrc + (i * 16);
        bool hasValue = *ptrBase != 0; // first byte is bool

        if (hasValue)
        {
            long rawTicks = *(long*)(ptrBase + 8); // Offset 8 bytes to read Ticks
            
            // 1. Mask Kind
            long ticks = rawTicks & mask;
            // 2. Subtract Epoch
            long delta = ticks - epoch;
            // 3. Divide 10 (Ticks -> us)
            pDst[i] = delta / 10;

            // Fill Valid bit
            if (validity != null)
            {
                // Safety check
                if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                
                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                target |= (byte)(1 << (i & 7));
            }
        }
        else
        {
            // Null
            if (validity == null)
            {
                // Init Validity
                validity = new byte[byteLen];
                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                
                // Backfill:
                int bytesToFill = i >> 3;
                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                int remainingBits = i & 7;
                if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
            }
            
            pDst[i] = 0; 
        }
    }
}