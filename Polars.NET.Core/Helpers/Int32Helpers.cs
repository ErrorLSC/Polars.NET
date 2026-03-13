using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    // =================================================================================
    //  Int32? Shuffle Mask (32 Bytes -> 4 items)
    //  int? Memory Layout: [HasValue(1B), Pad(3B), Value(4B)] (x64/Arm64 .NET 8)
    // =================================================================================
    private static readonly Vector256<byte> Int32ShuffleMask = Vector256.Create(
        // --- Output Bytes 0-15: The Integers (4 * 4 bytes) ---
        4, 5, 6, 7,      // Item 0 Value
        12, 13, 14, 15,  // Item 1 Value
        20, 21, 22, 23,  // Item 2 Value
        28, 29, 30, 31,  // Item 3 Value
        // --- Output Bytes 16-19: The HasValues (4 * 1 byte) ---
        0,               // Item 0 HasValue
        8,               // Item 1 HasValue
        16,              // Item 2 HasValue
        24,              // Item 3 HasValue
        // --- Rest is padding/garbage ---
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 
    );
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe (int[] values, byte[]? validity) UnzipInt32SIMD(ReadOnlySpan<int?> data, int defaultValue)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null; 
        
        fixed (int?* pSrc = data)
        fixed (int* pDstVal = values)
        {
            ref byte validRef = ref Unsafe.NullRef<byte>();
            int i = 0;

            if (Vector256.IsHardwareAccelerated && len >= 4)
            {
                int limit = len - 4;
                Vector256<byte> mask = Int32ShuffleMask;

                for (; i <= limit; i += 4)
                {
                    Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 8));
                    Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);
                    *(Vector128<byte>*)(pDstVal + i) = shuffled.GetLower();

                    int v0 = shuffled.GetElement(16);
                    int v1 = shuffled.GetElement(17);
                    int v2 = shuffled.GetElement(18);
                    int v3 = shuffled.GetElement(19);
                    int validityCheck = v0 | (v1 << 8) | (v2 << 16) | (v3 << 24);

                    if (validityCheck != 0x01010101) 
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
                    }

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) 
                        {
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        }

                        if (v0 != 0) SetBitRef(ref validRef, i);
                        if (v1 != 0) SetBitRef(ref validRef, i + 1);
                        if (v2 != 0) SetBitRef(ref validRef, i + 2);
                        if (v3 != 0) SetBitRef(ref validRef, i + 3);

                        if (v0 == 0) pDstVal[i] = defaultValue;
                        if (v1 == 0) pDstVal[i + 1] = defaultValue;
                        if (v2 == 0) pDstVal[i + 2] = defaultValue;
                        if (v3 == 0) pDstVal[i + 3] = defaultValue;
                    }
                }
            }

            if (i < len)
            {
                // Fallback
                UnzipScalarLoop(
                    ref Unsafe.AsRef<int?>(pSrc), 
                    ref Unsafe.AsRef<int>(pDstVal), 
                    ref validity, 
                    i, len, defaultValue
                );
            }
        }
        return (values, validity);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int[] values, byte[]? validity) UnzipInt32SIMD(int?[] data,int defaultValue) => UnzipInt32SIMD(new ReadOnlySpan<int?>(data),defaultValue );
}