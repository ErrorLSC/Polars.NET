using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    // =================================================================================
    // Int64? Shuffle Mask (32 Bytes -> 2 items)
    // Int64? usually is 16 bytes: [HasValue(1B), Pad(7B), Value(8B)]
    // =================================================================================
    private static readonly Vector256<byte> Int64ShuffleMask = Vector256.Create(
        // --- Output Bytes 0-15: extract 2 double value (2 * 8B) ---
        // Item 0 Value (offset 8-15)
        8, 9, 10, 11, 12, 13, 14, 15,
        // Item 1 Value (offset 24-31)
        24, 25, 26, 27, 28, 29, 30, 31,

        // --- Output Bytes 16-17: extract 2 HasValue ---
        0,   // Item 0 HasValue
        (byte)16,  // Item 1 HasValue

        // --- Fill ---
        0,0,0,0,0,0,0,0,0,0,0,0,0,0
    );
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe (long[] values, byte[]? validity) UnzipInt64SIMD(ReadOnlySpan<long?> data, long defaultValue)
    {
        int len = data.Length;
        // 1. Allocate Values (Uninitialized)
        var values = GC.AllocateUninitializedArray<long>(len); 
        int byteLen = (len + 7) >> 3;
        
        byte[]? validity = null; 
        
        // 【Key Change 1】GC Safe ref
        

        fixed (long?* pSrc = data)
        fixed (long* pDstVal = values)
        {
            ref byte validRef = ref Unsafe.NullRef<byte>();
            int i = 0;

            // ---------------------------------------------------------
            // SIMD Loop: 2 items (32 Bytes)
            // ---------------------------------------------------------
            if (Vector256.IsHardwareAccelerated && len >= 2)
            {
                int limit = len - 2;
                // Reuse the Int64/Double Mask (same layout)
                Vector256<byte> mask = Int64ShuffleMask;

                for (; i <= limit; i += 2)
                {
                    // 1. Load: Load 32 bytes
                    Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 16)); 

                    // 2. Shuffle
                    Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);

                    // 3. Store Values: Write 128 bits (16 byte = 2 longs)
                    *(Vector128<byte>*)(pDstVal + i) = shuffled.GetLower();

                    // 4. Extract Validity
                    byte b0 = shuffled.GetElement(16);
                    byte b1 = shuffled.GetElement(17);

                    // Check Null
                    if ((b0 & b1) != 1) 
                    {
                        if (validity == null)
                        {
                            // 【Key Change 2】Allocate & Clear & Init
                            validity = new byte[byteLen];
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            
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
                        // Use SetBitRef
                        if (b0 != 0) SetBitRef(ref validRef, i);
                        if (b1 != 0) SetBitRef(ref validRef, i + 1);

                        // Handle Default Value
                        if (b0 == 0) pDstVal[i] = defaultValue;
                        if (b1 == 0) pDstVal[i + 1] = defaultValue;
                    }
                }
            }

            // ---------------------------------------------------------
            // Scalar Tail
            // ---------------------------------------------------------
            if (i < len)
            {
                UnzipScalarLoop(
                    ref Unsafe.AsRef<long?>(pSrc), 
                    ref Unsafe.AsRef<long>(pDstVal), 
                    ref validity, // auto sync
                    i, 
                    len, 
                    defaultValue
                );
            }
        }
        return (values, validity);
    }
}