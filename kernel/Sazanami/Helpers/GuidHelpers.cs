namespace Kaminari.Sazanami.Helpers;

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

public static partial class ArrayHelper
{
    /// <summary>
    /// Unzips a ReadOnlySpan of Guid? into contiguous 16-byte values and an Arrow validity bitmap.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (byte[] values, byte[]? validity) UnzipGuid(ReadOnlySpan<Guid?> data, Guid defaultValue = default)
    {
        int len = data.Length;
        int totalBytes = len * 16;
        var values = GC.AllocateUninitializedArray<byte>(totalBytes);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null;

        ref byte dstBytesRef = ref MemoryMarshal.GetArrayDataReference(values);
        ref byte validRef = ref Unsafe.NullRef<byte>();

        for (int i = 0; i < len; i++)
        {
            Guid? item = data[i];

            if (item.HasValue)
            {
                Guid val = item.GetValueOrDefault();
                // Copy 16 bytes directly into dstBytesRef + i * 16
                Unsafe.WriteUnaligned(ref Unsafe.Add(ref dstBytesRef, i * 16), val);

                if (validity != null)
                {
                    Unsafe.Add(ref validRef, i >> 3) |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                // First null encountered: allocate and backfill validity bitmap
                if (validity == null)
                {
                    validity = new byte[byteLen];
                    validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                    int fullBytes = i >> 3;
                    if (fullBytes > 0)
                    {
                        Unsafe.InitBlock(ref validRef, 0xFF, (uint)fullBytes);
                    }

                    int remainder = i & 7;
                    if (remainder > 0)
                    {
                        Unsafe.Add(ref validRef, fullBytes) = (byte)((1 << remainder) - 1);
                    }
                }

                // Write default placeholder value
                Unsafe.WriteUnaligned(ref Unsafe.Add(ref dstBytesRef, i * 16), defaultValue);
                // Bit at (i >> 3) remains 0
            }
        }

        return (values, validity);
    }
}
