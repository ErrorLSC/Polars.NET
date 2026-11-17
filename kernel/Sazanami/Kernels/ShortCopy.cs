using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Kernels;

/// <summary>
/// Copies of short byte strings gathered one by one (string values out of Arrow arrays) without a call per value: a
/// few overlapping unaligned loads, then the stores. Inlined into the caller's loop, the loads of values scattered in
/// memory (a join's gathered string views) overlap their cache misses across values, where a call per value waits
/// for each. Longer strings go to the span copy.
/// </summary>
internal static class ShortCopy
{
    /// <summary>The longest string copied inline.</summary>
    public const int MaxInline = 64;

    /// <summary>Copies <paramref name="source"/> to the start of <paramref name="destination"/> (which must not overlap it).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Copy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int length = source.Length;
        if (length < 8 || length > MaxInline || destination.Length < length)
        {
            source.CopyTo(destination);
            return;
        }
        ref byte from = ref MemoryMarshal.GetReference(source);
        ref byte to = ref MemoryMarshal.GetReference(destination);
        if (length < 16)
        {
            ulong head = Unsafe.ReadUnaligned<ulong>(ref from), tail = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref from, length - 8));
            Unsafe.WriteUnaligned(ref to, head);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref to, length - 8), tail);
            return;
        }
        nuint last = (nuint)(length - 16);
        Vector128<byte> first = Vector128.LoadUnsafe(ref from), end = Vector128.LoadUnsafe(ref from, last);
        if (length > 32)
        {
            // 33 to 64 bytes: two more 16-byte pieces, from the start and to the end, overlapping as they must.
            nuint before = (nuint)(length - 32);
            Vector128<byte> second = Vector128.LoadUnsafe(ref from, 16), third = Vector128.LoadUnsafe(ref from, before);
            second.StoreUnsafe(ref to, 16);
            third.StoreUnsafe(ref to, before);
        }
        first.StoreUnsafe(ref to);
        end.StoreUnsafe(ref to, last);
    }
}
