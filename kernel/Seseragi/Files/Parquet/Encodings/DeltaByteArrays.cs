using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Seseragi.Files.Parquet.Encodings;

/// <summary>
/// DELTA_LENGTH_BYTE_ARRAY (the lengths as DELTA_BINARY_PACKED, then the bytes back to back) and DELTA_BYTE_ARRAY
/// (prefix lengths as DELTA_BINARY_PACKED, then the suffixes as DELTA_LENGTH_BYTE_ARRAY; each value is the previous
/// one's first <c>prefix</c> bytes and its suffix), turned into PLAIN layout: read the sections first (which sizes the
/// output), then write. A page starts afresh: its first value has no previous one, so its prefix must be 0.
/// </summary>
internal static class DeltaByteArrays
{
    /// <summary>A page's sections: where the value bytes start and how many of them the lengths take.</summary>
    public readonly record struct Sections(int DataStart, long DataBytes);

    /// <summary>
    /// Reads the length sections of <paramref name="encoded"/>: <paramref name="lengths"/> (the suffix lengths of
    /// DELTA_BYTE_ARRAY), and first <paramref name="prefixes"/> when there are (empty for DELTA_LENGTH_BYTE_ARRAY).
    /// </summary>
    /// <exception cref="ParquetFormatException">A section is damaged, a length negative, or the bytes fewer than the lengths take.</exception>
    public static Sections Read(ReadOnlySpan<byte> encoded, bool prefixed, Span<int> prefixes, Span<int> lengths, Span<ulong> scratch)
    {
        int position = 0;
        if (prefixed)
        {
            position = DeltaBinaryPacked.Decode(encoded, prefixes, scratch, toEnd: true);
            if (prefixes.IndexOfAnyInRange(int.MinValue, -1) is int p and >= 0) throw new ParquetFormatException($"a DELTA_BYTE_ARRAY prefix length is {prefixes[p]}.");
        }
        position += DeltaBinaryPacked.Decode(encoded[position..], lengths, scratch, toEnd: true);
        long bytes = 0;
        foreach (int length in lengths)
        {
            if (length < 0) throw new ParquetFormatException($"a byte array length is {length}.");
            bytes += length;
        }
        if (bytes > encoded.Length - position)
            throw new ParquetFormatException($"byte arrays of {bytes} bytes in all run past the page ({encoded.Length - position} bytes left).");
        // Prefixes are checked before anything is sized by them: the output is then as large as the values really are.
        long previous = 0;
        for (int i = 0; i < prefixes.Length; i++)
        {
            if (prefixes[i] > previous) throw LongPrefix(prefixes[i], previous);
            previous = (long)prefixes[i] + lengths[i];
        }
        return new Sections(position, bytes);
    }

    /// <summary>The bytes of the PLAIN layout: a 4-byte length and the bytes per value, or <paramref name="fixedLength"/> bytes per value.</summary>
    public static long PlainLength(ReadOnlySpan<int> prefixes, ReadOnlySpan<int> lengths, int fixedLength)
    {
        if (fixedLength > 0) return (long)lengths.Length * fixedLength;
        long bytes = 4L * lengths.Length;
        foreach (int length in lengths) bytes += length;
        foreach (int prefix in prefixes) bytes += prefix;
        return bytes;
    }

    /// <summary>
    /// Writes the values as PLAIN into <paramref name="plain"/> (<see cref="PlainLength"/> bytes): BYTE_ARRAY when
    /// <paramref name="fixedLength"/> is 0, else FIXED_LEN_BYTE_ARRAY of that length.
    /// </summary>
    /// <exception cref="ParquetFormatException">A prefix is longer than the previous value, or a value is not of the fixed length.</exception>
    public static void WritePlain(ReadOnlySpan<byte> data, ReadOnlySpan<int> prefixes, ReadOnlySpan<int> lengths, Span<byte> plain, int fixedLength)
    {
        int at = 0, read = 0;
        int previous = 0, previousLength = 0; // where the previous value's bytes start in plain, and their count
        for (int i = 0; i < lengths.Length; i++)
        {
            int prefix = prefixes.IsEmpty ? 0 : prefixes[i], suffix = lengths[i];
            if (prefix > previousLength) throw LongPrefix(prefix, previousLength);
            int length = prefix + suffix;
            if (fixedLength > 0 && length != fixedLength)
                throw new ParquetFormatException($"a FIXED_LEN_BYTE_ARRAY({fixedLength}) value is {length} bytes long.");
            if (fixedLength == 0)
            {
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(plain[at..], length);
                at += 4;
            }
            // The prefix comes from the previous value, already written before this one.
            Copy(plain, previous, prefix, plain, at);
            Copy(data, read, suffix, plain, at + prefix);
            read += suffix;
            previous = at;
            previousLength = length;
            at += length;
        }
    }

    /// <summary>The bytes the values of a DELTA_BYTE_ARRAY page take back to back (prefixes and suffixes).</summary>
    public static long ValuesLength(ReadOnlySpan<int> prefixes, ReadOnlySpan<int> lengths) => PlainLength(prefixes, lengths, 0) - 4L * lengths.Length;

    /// <summary>The bytes a value may be written past its end (short copies move up to 32 bytes): output buffers have this much room more.</summary>
    public const int Slack = 32;

    /// <summary>
    /// DELTA_BYTE_ARRAY values back to back into <paramref name="values"/> (<see cref="ValuesLength"/> bytes and
    /// <see cref="Slack"/> more), their starts into <paramref name="starts"/>; <paramref name="lengths"/> (the suffix
    /// lengths) become the values' lengths. For readers that take values with starts and lengths, not PLAIN.
    /// </summary>
    /// <exception cref="ParquetFormatException">A prefix is longer than the previous value.</exception>
    public static void WriteValues(ReadOnlySpan<byte> data, ReadOnlySpan<int> prefixes, Span<int> lengths, Span<byte> values, Span<int> starts)
    {
        int at = 0, read = 0, previous = 0, previousLength = 0;
        for (int i = 0; i < lengths.Length; i++)
        {
            int prefix = prefixes[i], suffix = lengths[i];
            if (prefix > previousLength) throw LongPrefix(prefix, previousLength);
            Copy(values, previous, prefix, values, at);
            Copy(data, read, suffix, values, at + prefix);
            read += suffix;
            starts[i] = previous = at;
            lengths[i] = previousLength = prefix + suffix;
            at += previousLength;
        }
    }

    /// <summary>The starts of DELTA_LENGTH_BYTE_ARRAY values in their data: where each one's predecessors end.</summary>
    public static void Starts(ReadOnlySpan<int> lengths, Span<int> starts)
    {
        int at = 0;
        for (int i = 0; i < lengths.Length; i++)
        {
            starts[i] = at;
            at += lengths[i];
        }
    }

    /// <summary>
    /// Copies <paramref name="count"/> bytes; a run of up to 32 moves one or two 16-byte vectors when both sides have
    /// room (the bytes past it are written over by what follows), instead of a call per value. Both vectors are loaded
    /// before either is stored: a prefix's source may run into its destination.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Copy(ReadOnlySpan<byte> from, int at, int count, Span<byte> to, int offset)
    {
        if (count <= 32 && from.Length - at >= 32 && to.Length - offset >= 32)
        {
            ref byte source = ref MemoryMarshal.GetReference(from);
            ref byte target = ref MemoryMarshal.GetReference(to);
            Vector128<byte> low = Vector128.LoadUnsafe(ref source, (nuint)at), high = Vector128.LoadUnsafe(ref source, (nuint)at + 16);
            low.StoreUnsafe(ref target, (nuint)offset);
            if (count > 16) high.StoreUnsafe(ref target, (nuint)offset + 16);
        }
        else from.Slice(at, count).CopyTo(to[offset..]);
    }

    private static ParquetFormatException LongPrefix(long prefix, long previous) =>
        new($"a DELTA_BYTE_ARRAY prefix of {prefix} bytes is longer than the previous value ({previous} bytes).");
}
