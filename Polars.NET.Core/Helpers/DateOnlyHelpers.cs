using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe int[] UnzipDateOnlyToInt32(ReadOnlySpan<DateOnly> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        
        // 1970-01-01 DayNumber
        int epochShift = 719162; 

        // Get ref to the first element
        ref DateOnly srcRef = ref MemoryMarshal.GetReference(data);

        fixed (DateOnly* pSrc = &srcRef)
        fixed (int* pDst = values)
        {
            int* pRawSrc = (int*)pSrc;
            int i = 0;

            // =============================================================
            // AVX-512 Dual Turbo
            // =============================================================
            if (Vector512.IsHardwareAccelerated && len >= 16)
            {
                Vector512<int> vEpoch = Vector512.Create(epochShift);
                int limit = len - 16;

                for (; i <= limit; i += 16)
                {
                    Vector512<int> vData = Vector512.Load(pRawSrc + i);
                    Vector512<int> vResult = Vector512.Subtract(vData, vEpoch);
                    vResult.Store(pDst + i);
                }
            }
            // =============================================================
            // AVX-256 Turbocharger
            // =============================================================
            if (Vector256.IsHardwareAccelerated && (len - i) >= 8)
            {
                Vector256<int> vEpoch = Vector256.Create(epochShift);
                int limit = len - 8;

                for (; i <= limit; i += 8)
                {
                    Vector256<int> vData = Vector256.Load(pRawSrc + i);
                    Vector256<int> vResult = Vector256.Subtract(vData, vEpoch);
                    vResult.Store(pDst + i);
                }
            }

            // Scalar Tail
            for (; i < len; i++)
            {
                pDst[i] = pRawSrc[i] - epochShift;
            }
        }
        return values;
    }
    /// <summary>
    /// [Scalar Extreme] DateOnly?[] -> (Int32[], Validity)
    /// DateOnly? is 8 bytes, read it as long
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (int[] values, byte[]? validity) UnzipDateOnlyToInt32(ReadOnlySpan<DateOnly?> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        // Constant Value：1970 Epoch
        int epochShift = 719162;

        ref DateOnly? srcRef = ref MemoryMarshal.GetReference(data);

        fixed (DateOnly?* pSrc = &srcRef)
        fixed (int* pDst = values)
        {
            // Layout: [Bool(1), Pad(3), Int(4)]
            long* pRawSrc = (long*)pSrc;
            
            int i = 0;
            int limit = len - 8;

            // =========================================================
            // Unroll 8 
            // =========================================================
            for (; i <= limit; i += 8)
            {
                HandleDateOnlyItem(i,     pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
                HandleDateOnlyItem(i + 1, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
                HandleDateOnlyItem(i + 2, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
                HandleDateOnlyItem(i + 3, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
                HandleDateOnlyItem(i + 4, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
                HandleDateOnlyItem(i + 5, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
                HandleDateOnlyItem(i + 6, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
                HandleDateOnlyItem(i + 7, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
            }

            // Tail
            for (; i < len; i++)
            {
                HandleDateOnlyItem(i, pRawSrc, pDst, ref validity, ref validRef, byteLen, epochShift);
            }
        }

        return (values, validity);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void HandleDateOnlyItem(
        int i,
        long* pRawSrc,
        int* pDst,
        ref byte[]? validity,
        ref byte validRef,
        int byteLen,
        int epochShift)
    {
        // Read 8 bytes
        long raw = pRawSrc[i];

        // Check lower 8 bytes
        if ((byte)raw != 0)
        {
            // Extract upper 32 btis Value 
            int dayNumber = (int)(raw >> 32);
            
            // Sub epoch
            pDst[i] = dayNumber - epochShift;

            // Maintain Validity
            if (validity != null)
            {
                if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                target |= (byte)(1 << (i & 7));
            }
        }
        else
        {
            // Null handle
            if (validity == null)
            {
                // Init Validity
                validity = new byte[byteLen];
                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                
                // Backfill
                int bytesToFill = i >> 3;
                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                int remainingBits = i & 7;
                if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
            }
            
            pDst[i] = 0; // Default
        }
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int[] UnzipDateOnlyToInt32(DateOnly[] data) => UnzipDateOnlyToInt32(new ReadOnlySpan<DateOnly>(data));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int[] values, byte[]? validity) UnzipDateOnlyToInt32(DateOnly?[] data) => UnzipDateOnlyToInt32(new ReadOnlySpan<DateOnly?>(data));
}