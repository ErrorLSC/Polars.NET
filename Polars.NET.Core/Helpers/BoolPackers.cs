using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Polars.NET.Core.Helpers;

public static class BoolPacker
{
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static unsafe byte[] Pack(ReadOnlySpan<bool> data)
    {
        int len = data.Length;
        int byteLen = (len + 7) >> 3;
        var buffer = GC.AllocateUninitializedArray<byte>(byteLen);

        fixed (bool* pSrc = data)
        fixed (byte* pDst = buffer)
        {
            int i = 0;

            // =========================================================
            // Unified SIMD Path (.NET 8+)
            // =========================================================

            // =========================================================
            // AVX-512 Path (64 bool per cycle)
            // =========================================================
            if (Vector512.IsHardwareAccelerated && len >= 64)
            {
                int limit = len - 64;
                Vector512<byte> zero = Vector512<byte>.Zero;

                for (; i <= limit; i += 64)
                {
                    // 1. Load 64 bytes
                    Vector512<byte> vec = Vector512.Load((byte*)pSrc + i);
                    
                    // 2. Compare (0x00 or 0xFF)
                    // False(0) == Zero -> True(0xFF)
                    // True(1)  == Zero -> False(0x00)
                    Vector512<byte> cmp = Vector512.Equals(vec, zero);
                    
                    // 3. Compress to 64 bits (ulong)
                    ulong mask = Vector512.ExtractMostSignificantBits(cmp);
                    
                    // 4. Invert & Store
                    *(ulong*)(pDst + (i >> 3)) = ~mask;
                }
            }
            // =========================================================
            // AVX2 Path (32 bool per cycle)
            // =========================================================
            if (Vector256.IsHardwareAccelerated && len >= 32)
            {
                Vector256<byte> zero = Vector256<byte>.Zero;
                int limit = len - 32;

                for (; i <= limit; i += 32)
                {
                    Vector256<byte> vec = Vector256.Load((byte*)pSrc + i);
                    Vector256<byte> cmp = Vector256.Equals(vec, zero);
                    uint mask = Vector256.ExtractMostSignificantBits(cmp);
                    *(uint*)(pDst + (i >> 3)) = ~mask;
                }
            }
            // =========================================================
            // Scalar Path (Branchless Optimization)
            // =========================================================
            if (i < len)
            {
                int byteOffset = i >> 3;
                byte currentByte = 0;
                int bitOffset = 0;

                for (; i < len; i++)
                {
                    currentByte |= (byte)((*(byte*)(pSrc + i)) << bitOffset);
                    
                    bitOffset++;
                    if (bitOffset == 8)
                    {
                        pDst[byteOffset] = currentByte;
                        byteOffset++;
                        currentByte = 0;
                        bitOffset = 0;
                    }
                }
                // Last byte
                if (bitOffset > 0)
                {
                    pDst[byteOffset] = currentByte;
                }
            }
        }
        return buffer;
    }
    private static readonly Vector256<byte> DeinterleaveMask = Vector256.Create(
    (byte)0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, // Low 128: Values
    1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31       // High 128: Validity
    );

    /// <summary>
    /// Compress bool?[] into Values Bitmask & Validity Bitmask.
    /// Fixed: Uses ref byte for validity array to ensure GC safety.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static unsafe (byte[] values, byte[]? validity) PackNullable(ReadOnlySpan<bool?> data)
    {
        int len = data.Length;
        int byteLen = (len + 7) >> 3;

        var valuesBits = GC.AllocateUninitializedArray<byte>(byteLen);
        byte[]? validityBits = null; 

        ref byte validRef = ref Unsafe.NullRef<byte>();

        fixed (bool?* pSrc = data)
        fixed (byte* pDstVal = valuesBits)
        {
            int i = 0;

            // =========================================================
            // 1. Unified SIMD Path (.NET 8+) 
            // =========================================================
            if (Vector256.IsHardwareAccelerated && len >= 16)
            {
                Vector256<byte> zero = Vector256<byte>.Zero;
                int limit = len - 16;

                for (; i <= limit; i += 16)
                {
                    Vector256<byte> vec = Vector256.Load((byte*)pSrc + (i * 2));
                    Vector256<byte> shuffled = Vector256.Shuffle(vec, DeinterleaveMask);
                    Vector256<byte> cmp = Vector256.Equals(shuffled, zero);
                    uint mask = Vector256.ExtractMostSignificantBits(cmp);
                    uint finalMask = ~mask;

                    ushort valMask16 = (ushort)(finalMask & 0xFFFF);
                    ushort validMask16 = (ushort)(finalMask >>> 16);

                    *(ushort*)(pDstVal + (i >> 3)) = valMask16;

                    // Lazy Allocation Logic
                    if (validMask16 != 0xFFFF)
                    {
                        if (validityBits == null)
                        {
                            validityBits = GC.AllocateUninitializedArray<byte>(byteLen);
                            validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);

                            // Backfill 1s (All valid until now)
                            int bytesToFill = i >> 3;
                            if (bytesToFill > 0)
                            {
                                Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                            }
                        }
                    }

                    if (validityBits != null)
                    {
                        Unsafe.WriteUnaligned(
                            ref Unsafe.Add(ref validRef, i >> 3), 
                            validMask16
                        );
                    }
                }
            }

            // =========================================================
            // 2. Optimized Scalar Path
            // =========================================================
            if (i < len)
            {
                if (validityBits != null && Unsafe.IsNullRef(ref validRef))
                {
                        validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);
                }

                int byteOffset = i >> 3;
                byte curValByte = 0;
                byte curValidByte = 0; 
                int bitPos = 0;

                ref bool? srcRef = ref Unsafe.AsRef<bool?>(pSrc);

                for (; i < len; i++)
                {
                    bool? v = Unsafe.Add(ref srcRef, i);

                    if (v.HasValue)
                    {
                        curValidByte |= (byte)(1 << bitPos);
                        
                        if (v.GetValueOrDefault())
                        {
                            curValByte |= (byte)(1 << bitPos);
                        }
                    }
                    else
                    {
                        if (validityBits == null)
                        {
                            validityBits = GC.AllocateUninitializedArray<byte>(byteLen);
                            validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);
                            
                            if (byteOffset > 0)
                            {
                                Unsafe.InitBlock(ref validRef, 0xFF, (uint)byteOffset);
                            }
                        }
                    }

                    bitPos++;

                    if (bitPos == 8)
                    {
                        // Flush register to mem
                        pDstVal[byteOffset] = curValByte;
                        
                        if (validityBits != null)
                        {
                            Unsafe.Add(ref validRef, byteOffset) = curValidByte;
                        }

                        byteOffset++;
                        curValByte = 0;
                        curValidByte = 0;
                        bitPos = 0;
                    }
                }

                if (bitPos > 0)
                {
                    pDstVal[byteOffset] = curValByte;
                    if (validityBits != null)
                    {
                        Unsafe.Add(ref validRef, byteOffset) = curValidByte;
                    }
                }
            }
        }

        return (valuesBits, validityBits);
    }
    /// <summary>
    /// Overload for standard arrays to maintain API compatibility.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] Pack(bool[] data) => Pack(new ReadOnlySpan<bool>(data));
    /// <summary>
    /// Overload for standard arrays to maintain API compatibility.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (byte[] values, byte[]? validity) PackNullable(bool?[] data) => PackNullable(new ReadOnlySpan<bool?>(data));
}