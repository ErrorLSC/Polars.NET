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
            plain.Slice(previous, prefix).CopyTo(plain[at..]);
            data.Slice(read, suffix).CopyTo(plain[(at + prefix)..]);
            read += suffix;
            previous = at;
            previousLength = length;
            at += length;
        }
    }

    private static ParquetFormatException LongPrefix(long prefix, long previous) =>
        new($"a DELTA_BYTE_ARRAY prefix of {prefix} bytes is longer than the previous value ({previous} bytes).");
}
