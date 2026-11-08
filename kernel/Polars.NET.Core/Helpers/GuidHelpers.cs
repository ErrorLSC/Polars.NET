namespace Polars.NET.Core.Helpers;

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

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

    /// <summary>
    /// Creates a SeriesHandle from a nullable Guid span using the unzipped FixedSizeBinary pipeline.
    /// </summary>
    public static SeriesHandle CreateFromNullableGuidSpan(string name, ReadOnlySpan<Guid?> data)
    {
        if (data.IsEmpty)
        {
            return NativeBindings.pl_series_new_guid_fixed_binary(name, ref Unsafe.NullRef<byte>(), ref Unsafe.NullRef<byte>(), 0);
        }

        var (values, validity) = UnzipGuid(data);
        ref byte valuesRef = ref MemoryMarshal.GetArrayDataReference(values);
        ref byte validRef = ref (validity != null
            ? ref MemoryMarshal.GetArrayDataReference(validity)
            : ref Unsafe.NullRef<byte>());

        return NativeBindings.pl_series_new_guid_fixed_binary(
            name,
            ref valuesRef,
            ref validRef,
            (nuint)data.Length
        );
    }
}
