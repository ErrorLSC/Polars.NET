using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    // =================================================================================
    //  Int8/Byte Shuffle Mask (32 Bytes -> 4 items)
    //  Layout: [Val(1), Bool(1)] or [Bool(1), Val(1)]
    // =================================================================================
    private static readonly Vector256<byte> Int8ShuffleMask = Vector256.Create(
        // --- Output 0-15: 16 Values  1, 3, 5...) ---
        1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31,
        
        // --- Output 16-31: 16 Bools  0, 2, 4...) ---
        (byte)0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30
    );
       // [Stride 2] Int8 / Byte
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe (byte[] values, byte[]? validity) UnzipInt8SIMD(ReadOnlySpan<byte?> data, byte defaultValue)
    {
        int len = data.Length;
        // 1. Allocate Values (Uninitialized)
        var values = GC.AllocateUninitializedArray<byte>(len);
        int byteLen = (len + 7) >> 3;
        
        byte[]? validity = null; 

        fixed (byte?* pSrc = data)
        fixed (byte* pDstVal = values)
        {
            ref byte validRef = ref Unsafe.NullRef<byte>();
            int i = 0;

            // ---------------------------------------------------------
            // SIMD Loop: 16 byte? items (32 Bytes)
            // ---------------------------------------------------------
            if (Vector256.IsHardwareAccelerated && len >= 16)
            {
                int limit = len - 16;
                Vector256<byte> mask = Int8ShuffleMask;
                Vector128<byte> zero = Vector128<byte>.Zero;

                for (; i <= limit; i += 16)
                {
                    // 1. Load 32 bytes (16 items)
                    Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 2));

                    // 2. Shuffle: 
                    // Lower 128 bits = 16 Values
                    // Upper 128 bits = 16 Bools
                    Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);

                    // 3. Store Values
                    shuffled.GetLower().Store(pDstVal + i);

                    // 4. Validity Check
                    Vector128<byte> bools = shuffled.GetUpper();

                    // Check Null (0x00)
                    // Equals(0, 0) -> 0xFF (Null)
                    // Equals(1, 0) -> 0x00 (Valid)
                    Vector128<byte> isNullVec = Vector128.Equals(bools, zero);

                    // Extract 16 bits mask (1 = Null, 0 = Valid)
                    uint nullMask = isNullVec.ExtractMostSignificantBits();

                    if (nullMask != 0)
                    {
                        if (validity == null)
                        {
                            validity = GC.AllocateUninitializedArray<byte>(byteLen);
                            Array.Clear(validity, 0, byteLen); 
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            
                            int bytesToFill = i >> 3;
                            if (bytesToFill > 0)
                            {
                                Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                            }
                        }
                    }

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) 
                        {
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        }
                        ushort validMask16 = (ushort)(~nullMask);

                        Unsafe.WriteUnaligned(
                            ref Unsafe.Add(ref validRef, i >> 3), 
                            validMask16
                        );

                        // Handle Default Value
                        if (nullMask != 0)
                        {
                            for (int k = 0; k < 16; k++)
                            {
                                if ((nullMask & (1 << k)) != 0) // Is Null
                                {
                                    pDstVal[i + k] = defaultValue;
                                }
                            }
                        }
                    }
                }
            }

            // ---------------------------------------------------------
            // Scalar Tail (Fallback)
            // ---------------------------------------------------------
            if (i < len)
            {
                UnzipScalarLoop(
                    ref Unsafe.AsRef<byte?>(pSrc), 
                    ref Unsafe.AsRef<byte>(pDstVal), 
                    ref validity, 
                    i, 
                    len, 
                    defaultValue
                );
            }
        }
        return (values, validity);
    }
}