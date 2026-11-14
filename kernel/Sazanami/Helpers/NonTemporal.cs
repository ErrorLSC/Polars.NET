using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Writes to large output buffers that skip the caches (non-temporal stores, portable vector APIs): a normal store to
/// a line not in the cache first reads it from memory (write-allocate), so an output written once costs its size
/// twice in memory traffic; these stores do not. Worth it only for outputs larger than the caches, which no one reads
/// right away. A full fence ends each call, so the data is visible to other threads as with normal stores.
/// </summary>
internal static unsafe class NonTemporal
{
    /// <summary>Whether the platform has vector non-temporal stores (else the methods do plain writes).</summary>
    public static bool IsSupported => Vector256.IsHardwareAccelerated || Vector128.IsHardwareAccelerated;

    /// <summary>Copies <paramref name="source"/> to <paramref name="destination"/> (at least as long; the two must not overlap).</summary>
    public static void Copy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (destination.Length < source.Length) throw new ArgumentException("The destination is shorter than the source.", nameof(destination));
        fixed (byte* s = source)
        fixed (byte* d = destination)
            Copy(s, d, source.Length);
    }

    private static void Copy(byte* s, byte* d, nint length)
    {
        if (!IsSupported || length < 256)
        {
            Buffer.MemoryCopy(s, d, length, length);
            return;
        }
        int width = Vector256.IsHardwareAccelerated ? 32 : 16;
        // Up to an aligned destination with a plain copy, then whole vectors, then the rest plainly.
        nint head = (nint)(((nuint)width - ((nuint)d & (nuint)(width - 1))) & (nuint)(width - 1));
        Buffer.MemoryCopy(s, d, head, head);
        nint i = head;
        if (width == 32)
        {
            for (; i + 128 <= length; i += 128)
            {
                Vector256.StoreAlignedNonTemporal(Vector256.Load(s + i), d + i);
                Vector256.StoreAlignedNonTemporal(Vector256.Load(s + i + 32), d + i + 32);
                Vector256.StoreAlignedNonTemporal(Vector256.Load(s + i + 64), d + i + 64);
                Vector256.StoreAlignedNonTemporal(Vector256.Load(s + i + 96), d + i + 96);
            }
            for (; i + 32 <= length; i += 32) Vector256.StoreAlignedNonTemporal(Vector256.Load(s + i), d + i);
        }
        else
        {
            for (; i + 64 <= length; i += 64)
            {
                Vector128.StoreAlignedNonTemporal(Vector128.Load(s + i), d + i);
                Vector128.StoreAlignedNonTemporal(Vector128.Load(s + i + 16), d + i + 16);
                Vector128.StoreAlignedNonTemporal(Vector128.Load(s + i + 32), d + i + 32);
                Vector128.StoreAlignedNonTemporal(Vector128.Load(s + i + 48), d + i + 48);
            }
            for (; i + 16 <= length; i += 16) Vector128.StoreAlignedNonTemporal(Vector128.Load(s + i), d + i);
        }
        Buffer.MemoryCopy(s + i, d + i, length - i, length - i);
        Interlocked.MemoryBarrier();
    }

    /// <summary>
    /// Writes <c>dictionary[indices[i]]</c> to <c>destination[i]</c> for 4-, 8- or 16-byte values; the indices must
    /// already be checked against the dictionary.
    /// </summary>
    public static void Gather<T>(ReadOnlySpan<T> dictionary, ReadOnlySpan<int> indices, Span<T> destination) where T : unmanaged
    {
        if (destination.Length < indices.Length) throw new ArgumentException("The destination is shorter than the indices.", nameof(destination));
        ref T dict = ref MemoryMarshal.GetReference(dictionary);
        ref int index = ref MemoryMarshal.GetReference(indices);
        int n = indices.Length;
        fixed (T* destinationPointer = destination)
        {
            byte* d = (byte*)destinationPointer;
            int i = 0;
            if (IsSupported && n * sizeof(T) >= 256 && sizeof(T) is 4 or 8 or 16 && ((nuint)d & (nuint)(sizeof(T) - 1)) == 0)
            {
                // Plain writes up to a 16-byte aligned destination, then 16 bytes per non-temporal store.
                for (; i < n && ((nuint)(d + i * sizeof(T)) & 15) != 0; i++) Unsafe.Add(ref Unsafe.AsRef<T>(destinationPointer), i) = Unsafe.Add(ref dict, Unsafe.Add(ref index, i));
                int per = 16 / sizeof(T);
                for (; i + per <= n; i += per)
                {
                    Vector128<byte> v = sizeof(T) switch
                    {
                        16 => Unsafe.As<T, Vector128<byte>>(ref Unsafe.Add(ref dict, Unsafe.Add(ref index, i))),
                        8 => Vector128.Create(Unsafe.As<T, ulong>(ref Unsafe.Add(ref dict, Unsafe.Add(ref index, i))),
                            Unsafe.As<T, ulong>(ref Unsafe.Add(ref dict, Unsafe.Add(ref index, i + 1)))).AsByte(),
                        _ => Vector128.Create(Unsafe.As<T, uint>(ref Unsafe.Add(ref dict, Unsafe.Add(ref index, i))),
                            Unsafe.As<T, uint>(ref Unsafe.Add(ref dict, Unsafe.Add(ref index, i + 1))),
                            Unsafe.As<T, uint>(ref Unsafe.Add(ref dict, Unsafe.Add(ref index, i + 2))),
                            Unsafe.As<T, uint>(ref Unsafe.Add(ref dict, Unsafe.Add(ref index, i + 3)))).AsByte(),
                    };
                    Vector128.StoreAlignedNonTemporal(v, d + (nint)i * sizeof(T));
                }
            }
            for (; i < n; i++) Unsafe.Add(ref Unsafe.AsRef<T>(destinationPointer), i) = Unsafe.Add(ref dict, Unsafe.Add(ref index, i));
            if (IsSupported) Interlocked.MemoryBarrier();
        }
    }
}
