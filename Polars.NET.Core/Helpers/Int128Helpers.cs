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
}