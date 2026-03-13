using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    // =========================================================================
    // [Stride 32] Int128 / UInt128 (32 Bytes -> 1 item)
    // Layout: [Value(16B), Bool(1B), Pad(15B)]
    // =========================================================================

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe (Int128[] values, byte[]? validity) UnzipInt128SIMD(ReadOnlySpan<Int128?> data, Int128 defaultValue)
    {
        int len = data.Length;
        // 1. Allocate Values (Uninitialized)
        var values = GC.AllocateUninitializedArray<Int128>(len);
        int byteLen = (len + 7) >> 3;

        byte[]? validity = null; 

        // Cache static field to local for speed
        bool isTypeA = IsInt128TypeA; 

        fixed (Int128?* pSrc = data)
        fixed (Int128* pDstVal = values)
        {
            ref byte validRef = ref Unsafe.NullRef<byte>();
            int i = 0;

            if (Vector256.IsHardwareAccelerated)
            {
                for (; i < len; i++)
                {
                    // 1. Load 32 bytes
                    Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 32));
                    
                    byte hasValue;

                    // 2. Get Value and Bool
                    if (isTypeA)
                    {
                        // Type A: [Value(0-15), Bool(16)]
                        raw.GetLower().Store((byte*)(pDstVal + i));
                        hasValue = raw.GetElement(16);
                    }
                    else
                    {
                        // Type B: [Bool(0), Pad, Value(16-31)]
                        raw.GetUpper().Store((byte*)(pDstVal + i));
                        hasValue = raw.GetElement(0);
                    }

                    // 3. Validity Check
                    if (hasValue != 1) 
                    {
                        if (validity == null)
                        {
                            validity = new byte[byteLen]; 
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            
                            // Backfill 1s
                            int bytesToFill = i >> 3;
                            if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                            int remainingBits = i & 7;
                            if (remainingBits > 0) 
                            {
                                Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                            }
                        }
                    }

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) 
                        {
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        }
                        if (hasValue != 0) // Valid
                        {
                            ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                            target |= (byte)(1 << (i & 7));
                        }
                        else // Null
                        {
                            pDstVal[i] = defaultValue;
                        }
                    }
                }
            }

            if (i < len)
            {
                UnzipScalarLoop(
                    ref Unsafe.AsRef<Int128?>(pSrc), 
                    ref Unsafe.AsRef<Int128>(pDstVal), 
                    ref validity, 
                    i, len, defaultValue
                );
            }
        }
        return (values, validity);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Int128[] values, byte[]? validity) UnzipInt128SIMD(Int128?[] data,Int128 defaultValue) => UnzipInt128SIMD(new ReadOnlySpan<Int128?>(data),defaultValue );
}

internal static unsafe class Int128Packer
{
    // AVX2 Permute4x64 Mask: 10_11_00_01 => 0xB1
    // Swaps [0,1] and [2,3] within the 256-bit vector
    private const byte SWAP_MASK_256 = 0xB1;

    /// <summary>
    /// Int128* -> Int128[]
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static Int128[] PackDense(Int128* pSrc, int len)
    {
        // 1. Allocate Result Array
        var result = GC.AllocateUninitializedArray<Int128>(len);
        
        fixed (Int128* pDst = result)
        {
            PackDenseInternal(pSrc, pDst, len);
        }

        return result;
    }

    /// <summary>
    /// UInt128* -> UInt128[]
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static UInt128[] PackDense(UInt128* pSrc, int len)
    {
        var result = GC.AllocateUninitializedArray<UInt128>(len);
        
        fixed (UInt128* pDst = result)
        {
            PackDenseInternal((Int128*)pSrc, (Int128*)pDst, len);
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void PackDenseInternal(Int128* pSrc, Int128* pDst, int len)
    {
        // Zero-Copy Path
        // if (!ArrayHelper.Int128NeedsSwap)
        // {
        long byteLen = (long)len * 16;
        Buffer.MemoryCopy(pSrc, pDst, byteLen, byteLen);
        return;
        // }

        // 2. Swap Path (Generic SIMD)
        // Treat Int128 as 2 Int64。
        // Vector256<long> contains 4 long: [Lo1, Hi1, Lo2, Hi2] (index 0, 1, 2, 3)
        // We need to swap to: [Hi1, Lo1, Hi2, Lo2] (index 1, 0, 3, 2)
        
        // long* pIn = (long*)pSrc;
        // long* pOut = (long*)pDst;
        // int i = 0;

        // // --- Vector256 (2 Int128) ---
        // if (Vector256.IsHardwareAccelerated && len >= 2)
        // {
        //     // 0,1,2,3 -> 1,0,3,2
        //     Vector256<long> mask = Vector256.Create(1L, 0, 3, 2);
            
        //     int limit = len - 2;
        //     for (; i <= limit; i += 2)
        //     {
        //         // Load
        //         Vector256<long> vec = Vector256.Load(pIn + (i * 2));
                
        //         // Shuffle (Generic Swap)
        //         Vector256<long> swapped = Vector256.Shuffle(vec, mask);
                
        //         // Store
        //         swapped.Store(pOut + (i * 2));
        //     }
        // }

        // // --- Vector128 (1 Int128) ---
        // if (Vector128.IsHardwareAccelerated && i < len)
        // {
        //     // Mask: 0,1 -> 1,0
        //     Vector128<long> mask = Vector128.Create(1L, 0);
            
        //     int limit = len - 1;
        //     for (; i <= limit; i++)
        //     {
        //         Vector128<long> vec = Vector128.Load(pIn + (i * 2));
        //         Vector128<long> swapped = Vector128.Shuffle(vec, mask);
        //         swapped.Store(pOut + (i * 2));
        //     }
        // }

        // --- Scalar Fallback ---
        // for (; i < len; i++)
        // {
        //     int baseIdx = i * 2;
        //     ulong lo = (ulong)pIn[baseIdx];
        //     ulong hi = (ulong)pIn[baseIdx + 1];
            
        //     pOut[baseIdx] = (long)hi;
        //     pOut[baseIdx + 1] = (long)lo;
        // }
    }
}