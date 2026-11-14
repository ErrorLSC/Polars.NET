using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Filters;

/// <summary>A test of a string or binary value (unsigned byte order).</summary>
internal interface IBytesTest
{
    bool Test(ReadOnlySpan<byte> value);

    /// <summary>
    /// The outcome decided from a view alone (its length, its first four bytes, or its inline bytes when it has at
    /// most 12), or null when the value's bytes must be read.
    /// </summary>
    bool? FromView(ReadOnlySpan<byte> view);
}

/// <summary>The byte tests of filter leaves on strings and binaries.</summary>
internal static class BytesTests
{
    public const int MaxInline = 12;

    /// <summary>A view's length (bytes 0–3).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Length(ReadOnlySpan<byte> view) => BinaryPrimitives.ReadInt32LittleEndian(view);

    /// <summary>The first four bytes of a value (zero past its end) as a big-endian number: their order is the bytes' order.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Key(ReadOnlySpan<byte> value)
    {
        if (value.Length >= 4) return BinaryPrimitives.ReadUInt32BigEndian(value);
        uint key = 0;
        for (int i = 0; i < 4; i++) key = key << 8 | (i < value.Length ? value[i] : 0u);
        return key;
    }

    public static bool Holds(CompareOp op, int order) => op switch
    {
        CompareOp.Eq => order == 0,
        CompareOp.NotEq => order != 0,
        CompareOp.Lt => order < 0,
        CompareOp.LtEq => order <= 0,
        CompareOp.Gt => order > 0,
        _ => order >= 0,
    };

    /// <summary>Equality with a value.</summary>
    public readonly struct Equal(byte[] literal) : IBytesTest
    {
        public bool Test(ReadOnlySpan<byte> value) => value.SequenceEqual(literal);

        public bool? FromView(ReadOnlySpan<byte> view)
        {
            int length = Length(view);
            if (length != literal.Length) return false;
            if (length <= MaxInline) return view.Slice(4, length).SequenceEqual(literal);
            return view.Slice(4, 4).SequenceEqual(literal.AsSpan(0, 4)) ? null : false;
        }
    }

    /// <summary>An ordering comparison with a value (<see cref="CompareOp"/> other than equality, which <see cref="Equal"/> does faster).</summary>
    public readonly struct Order(CompareOp op, byte[] literal) : IBytesTest
    {
        private readonly uint _key = Key(literal);

        public bool Test(ReadOnlySpan<byte> value) => Holds(op, value.SequenceCompareTo(literal));

        public bool? FromView(ReadOnlySpan<byte> view)
        {
            int length = Length(view);
            if (length <= MaxInline) return Test(view.Slice(4, length));
            // A long value has four real bytes in front: different keys order the values as their bytes do.
            uint key = BinaryPrimitives.ReadUInt32BigEndian(view[4..]);
            return key == _key ? null : Holds(op, key < _key ? -1 : 1);
        }
    }

    /// <summary>The value starts with a prefix.</summary>
    public readonly struct Prefix(byte[] prefix) : IBytesTest
    {
        public bool Test(ReadOnlySpan<byte> value) => value.StartsWith(prefix);

        public bool? FromView(ReadOnlySpan<byte> view)
        {
            int length = Length(view);
            if (length < prefix.Length) return false;
            if (length <= MaxInline) return view.Slice(4, length).StartsWith(prefix);
            int head = Math.Min(4, prefix.Length);
            if (!view.Slice(4, head).SequenceEqual(prefix.AsSpan(0, head))) return false;
            return prefix.Length <= 4 ? true : null;
        }
    }

    /// <summary>Membership in a set of values, sorted and distinct (a binary search).</summary>
    public readonly struct Member(byte[][] sorted) : IBytesTest
    {
        public bool Test(ReadOnlySpan<byte> value)
        {
            int low = 0, high = sorted.Length - 1;
            while (low <= high)
            {
                int middle = (low + high) >>> 1;
                int order = value.SequenceCompareTo(sorted[middle]);
                if (order == 0) return true;
                if (order < 0) high = middle - 1;
                else low = middle + 1;
            }
            return false;
        }

        public bool? FromView(ReadOnlySpan<byte> view)
        {
            int length = Length(view);
            return length <= MaxInline ? Test(view.Slice(4, length)) : null;
        }
    }
}

/// <summary>
/// String and binary kernels for each Arrow layout: a test per value, written as a row mask. Slots that
/// <c>valid</c> marks null are not read (their bits stay clear); an empty <c>valid</c> marks every slot valid.
/// </summary>
internal static class BinaryKernels
{
    /// <summary>Utf8 / Binary: <paramref name="offsets"/> holds <paramref name="length"/> + 1 offsets into <paramref name="data"/>.</summary>
    public static void Offsets<TTest>(in TTest test, ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask)
        where TTest : struct, IBytesTest
    {
        int words = Masks.Words(length);
        for (int w = 0; w < words; w++)
        {
            int first = w << 6, n = Math.Min(64, length - first);
            ulong slots = valid.IsEmpty ? Masks.LastWord(n) : valid[w], word = 0;
            for (int k = 0; k < n; k++)
            {
                if ((slots >> k & 1) == 0) continue;
                int start = offsets[first + k], end = offsets[first + k + 1];
                if (test.Test(data[start..end])) word |= 1UL << k;
            }
            mask[w] = word;
        }
    }

    /// <summary>Utf8View / BinaryView: <paramref name="views"/> holds <paramref name="length"/> 16-byte views over <paramref name="buffers"/>.</summary>
    public static void Views<TTest>(in TTest test, ReadOnlySpan<byte> views, ReadOnlySpan<ReadOnlyMemory<byte>> buffers, int length, ReadOnlySpan<ulong> valid,
        Span<ulong> mask)
        where TTest : struct, IBytesTest
    {
        int words = Masks.Words(length);
        for (int w = 0; w < words; w++)
        {
            int first = w << 6, n = Math.Min(64, length - first);
            ulong slots = valid.IsEmpty ? Masks.LastWord(n) : valid[w], word = 0;
            for (int k = 0; k < n; k++)
            {
                if ((slots >> k & 1) == 0) continue;
                ReadOnlySpan<byte> view = views.Slice((first + k) << 4, 16);
                bool holds = test.FromView(view) ?? test.Test(Long(view, buffers));
                if (holds) word |= 1UL << k;
            }
            mask[w] = word;
        }
    }

    /// <summary>The bytes of a long view (more than 12 bytes) in its data buffer.</summary>
    public static ReadOnlySpan<byte> Long(ReadOnlySpan<byte> view, ReadOnlySpan<ReadOnlyMemory<byte>> buffers)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(view);
        int buffer = BinaryPrimitives.ReadInt32LittleEndian(view[8..]), offset = BinaryPrimitives.ReadInt32LittleEndian(view[12..]);
        if ((uint)buffer >= (uint)buffers.Length) throw new InvalidDataException($"A view points into data buffer {buffer} of {buffers.Length}.");
        return buffers[buffer].Span.Slice(offset, length);
    }

    /// <summary>FixedSizeBinary: <paramref name="length"/> values of <paramref name="width"/> bytes each.</summary>
    public static void Fixed<TTest>(in TTest test, ReadOnlySpan<byte> values, int width, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask)
        where TTest : struct, IBytesTest
    {
        int words = Masks.Words(length);
        for (int w = 0; w < words; w++)
        {
            int first = w << 6, n = Math.Min(64, length - first);
            ulong slots = valid.IsEmpty ? Masks.LastWord(n) : valid[w], word = 0;
            for (int k = 0; k < n; k++)
            {
                if ((slots >> k & 1) != 0 && test.Test(values.Slice((first + k) * width, width))) word |= 1UL << k;
            }
            mask[w] = word;
        }
    }
}
