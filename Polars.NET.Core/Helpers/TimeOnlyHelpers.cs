using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    /// <summary>
    /// Unzips a ReadOnlySpan of TimeOnly to Nanoseconds using SIMD.
    /// Memory Layout: TimeOnly is an 8-byte struct wrapping a long (Ticks).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe long[] UnzipTimeOnlyToNs(ReadOnlySpan<TimeOnly> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        
        long multiplier = 100;

        // Get ref to the first element
        ref TimeOnly srcRef = ref MemoryMarshal.GetReference(data);

        fixed (TimeOnly* pSrc = &srcRef)
        fixed (long* pDst = values)
        {
            long* pRawSrc = (long*)pSrc;
            int i = 0;

            // =============================================================
            // AVX-512 Dual Turbo
            // =============================================================
            if (Vector512.IsHardwareAccelerated && len >= 8)
            {
                Vector512<long> vMul = Vector512.Create(multiplier);
                int limit = len - 8;

                for (; i <= limit; i += 8)
                {
                    Vector512<long> vData = Vector512.Load(pRawSrc + i);
                    // .NET 8 JIT will generate vpmullq (AVX512DQ)
                    Vector512<long> vResult = Vector512.Multiply(vData, vMul);
                    vResult.Store(pDst + i);
                }
            }

            // =============================================================
            // AVX-256 Turbocharger
            // =============================================================
            if (Vector256.IsHardwareAccelerated && (len - i) >= 4)
            {
                Vector256<long> vMul = Vector256.Create(multiplier);
                int limit = len - 4;

                for (; i <= limit; i += 4)
                {
                    Vector256<long> vData = Vector256.Load(pRawSrc + i);
                    Vector256<long> vResult = Vector256.Multiply(vData, vMul);
                    vResult.Store(pDst + i);
                }
            }

            // Scalar Tail
            for (; i < len; i++)
            {
                pDst[i] = pRawSrc[i] * multiplier;
            }
        }
        return values;
    }

    /// <summary>
    /// [Scalar Extreme] ReadOnlySpan<TimeOnly?> -> (Int64[], Validity)
    /// Layout: 16 Bytes [Bool(1), Pad(7), Ticks(8)]
    /// Logic: Ticks * 100 -> Nanoseconds
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (long[] values, byte[]? validity) UnzipTimeOnlyToNs(ReadOnlySpan<TimeOnly?> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        // Ticks (100ns) -> ns
        long multiplier = 100;

        ref TimeOnly? srcRef = ref MemoryMarshal.GetReference(data);

        fixed (TimeOnly?* pSrc = &srcRef)
        fixed (long* pDst = values)
        {
            // To long*
            // Offset 0: Header (Bool)
            // Offset 1: Value (Ticks)
            long* pRawSrc = (long*)pSrc;
            
            int i = 0;
            int limit = len - 4;

            // =========================================================
            // Unroll 4
            // =========================================================
            for (; i <= limit; i += 4)
            {
                HandleTimeOnlyItem(i,     pRawSrc, pDst, ref validity, ref validRef, byteLen, multiplier);
                HandleTimeOnlyItem(i + 1, pRawSrc, pDst, ref validity, ref validRef, byteLen, multiplier);
                HandleTimeOnlyItem(i + 2, pRawSrc, pDst, ref validity, ref validRef, byteLen, multiplier);
                HandleTimeOnlyItem(i + 3, pRawSrc, pDst, ref validity, ref validRef, byteLen, multiplier);
            }

            // Tail
            for (; i < len; i++)
            {
                HandleTimeOnlyItem(i, pRawSrc, pDst, ref validity, ref validRef, byteLen, multiplier);
            }
        }

        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void HandleTimeOnlyItem(
        int i,
        long* pRawSrc, 
        long* pDst,
        ref byte[]? validity,
        ref byte validRef,
        int byteLen,
        long multiplier)
    {
        // Calc current element ptr location
        // i * 2 because long* ptr +1 will move 8 bytes，but Item is 16 bytes
        long* pItem = pRawSrc + (i * 2);
        
        // Read Header (Bool + Padding)
        // If Lower 8 bits != 0，that means HasValue
        byte hasValue = *(byte*)pItem; 

        if (hasValue != 0)
        {
            // Read Value 
            long ticks = *(pItem + 1);
            
            // Ticks * 100 = ns
            pDst[i] = ticks * multiplier;

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
            // Null Handle (Lazy Init Validity)
            if (validity == null)
            {
                validity = new byte[byteLen];
                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                
                // Backfill valid bits (0..i-1)
                int bytesToFill = i >> 3;
                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                int remainingBits = i & 7;
                if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
            }
            
            pDst[i] = 0; 
        }
    }
}