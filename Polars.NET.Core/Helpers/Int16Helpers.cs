using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    // =================================================================================
    //  Int16/Short Shuffle Mask (32 Bytes -> 4 items)
    //  Layout: [Bool(1), Pad(1), Val(2)] -> Total 4
    // =================================================================================
    private static readonly Vector256<byte> Int16ShuffleMask = Vector256.Create(
        // Low 128: 8 Values (8 * 2 bytes)
        2, 3, 6, 7, 10, 11, 14, 15,
        18, 19, 22, 23, 26, 27, 30, 31,
        // High 128: 8 Bools (Offset 0)
        0, 4, 8, 12, 16, 20, 24, 28,
        // Padding
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF 
    );
       [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe (short[] values, byte[]? validity) UnzipInt16SIMD(ReadOnlySpan<short?> data, short defaultValue)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<short>(len);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null; 

        fixed (short?* pSrc = data)
        fixed (short* pDstVal = values)
        {
            ref byte validRef = ref Unsafe.NullRef<byte>();
            int i = 0;

            if (Vector256.IsHardwareAccelerated && len >= 8)
            {
                int limit = len - 8;
                Vector256<byte> mask = Int16ShuffleMask;

                for (; i <= limit; i += 8)
                {
                    Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 4));
                    Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);
                    shuffled.GetLower().Store((byte*)(pDstVal + i));

                    Vector128<byte> upper = shuffled.GetUpper();
                    ulong boolsChunk = upper.AsUInt64().GetElement(0);

                    if (boolsChunk != 0x0101010101010101UL)
                    {
                        if (validity == null)
                        {
                            validity = new byte[byteLen];
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            
                            int bytesToFill = i >> 3;
                            if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                        }
                    }

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) 
                        {
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        }

                        int packedByte = 0;
                        if (upper.GetElement(0) != 0) packedByte |= 1;
                        if (upper.GetElement(1) != 0) packedByte |= 2;
                        if (upper.GetElement(2) != 0) packedByte |= 4;
                        if (upper.GetElement(3) != 0) packedByte |= 8;
                        if (upper.GetElement(4) != 0) packedByte |= 16;
                        if (upper.GetElement(5) != 0) packedByte |= 32;
                        if (upper.GetElement(6) != 0) packedByte |= 64;
                        if (upper.GetElement(7) != 0) packedByte |= 128;

                        Unsafe.Add(ref validRef, i >> 3) = (byte)packedByte;

                        if (packedByte != 0xFF) 
                        {
                            if ((packedByte & 1) == 0) pDstVal[i] = defaultValue;
                            if ((packedByte & 2) == 0) pDstVal[i+1] = defaultValue;
                            if ((packedByte & 4) == 0) pDstVal[i+2] = defaultValue;
                            if ((packedByte & 8) == 0) pDstVal[i+3] = defaultValue;
                            if ((packedByte & 16) == 0) pDstVal[i+4] = defaultValue;
                            if ((packedByte & 32) == 0) pDstVal[i+5] = defaultValue;
                            if ((packedByte & 64) == 0) pDstVal[i+6] = defaultValue;
                            if ((packedByte & 128) == 0) pDstVal[i+7] = defaultValue;
                        }
                    }
                }
            }
            
            if (i < len)
            {
                UnzipScalarLoop(
                    ref Unsafe.AsRef<short?>(pSrc), 
                    ref Unsafe.AsRef<short>(pDstVal), 
                    ref validity, i, len, defaultValue
                );
            }
        }
        return (values, validity);
    }
}