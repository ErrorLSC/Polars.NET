using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>Points at the deletion vector of a data file: the rows of the file that no longer exist.</summary>
public sealed class DeletionVectorDescriptor
{
    /// <summary><c>u</c>: a file named by a UUID under the table; <c>p</c>: a file at an absolute path; <c>i</c>: inline.</summary>
    public required string StorageType { get; init; }
    /// <summary>The encoded UUID with optional prefix, the absolute path, or the inline bytes in Z85.</summary>
    public required string PathOrInlineDv { get; init; }
    /// <summary>Where the vector starts in its file; null when inline or at the start.</summary>
    public int? Offset { get; init; }
    /// <summary>The size of the serialized vector in bytes.</summary>
    public required int SizeInBytes { get; init; }
    /// <summary>The number of deleted rows.</summary>
    public required long Cardinality { get; init; }

    /// <summary>
    /// Identifies this vector within the table. Together with the data file path it is the key of a file in log replay.
    /// </summary>
    public string UniqueId => Offset is int offset ? $"{StorageType}{PathOrInlineDv}@{offset}" : $"{StorageType}{PathOrInlineDv}";

    /// <summary>Whether the vector is stored in the descriptor itself.</summary>
    public bool IsInline => StorageType == "i";

    /// <summary>
    /// For storage type <c>u</c>, the table-relative path of the vector file,
    /// <c>[prefix/]deletion_vector_UUID.bin</c>. For <c>p</c>, the absolute path as stored.
    /// </summary>
    /// <exception cref="DeltaException">The vector is inline, or the descriptor is malformed.</exception>
    public string FilePath()
    {
        switch (StorageType)
        {
            case "p":
                return PathOrInlineDv;
            case "u":
            {
                if (PathOrInlineDv.Length < 20)
                    throw new DeltaException($"Malformed deletion vector descriptor: '{PathOrInlineDv}'.");
                string prefix = PathOrInlineDv[..^20];
                Span<byte> bytes = stackalloc byte[16];
                Z85.Decode(PathOrInlineDv.AsSpan(PathOrInlineDv.Length - 20), bytes);
                // The 16 bytes are the UUID in big-endian order.
                var uuid = new Guid(bytes, bigEndian: true);
                string name = $"deletion_vector_{uuid:D}.bin";
                return prefix.Length == 0 ? name : $"{prefix}/{name}";
            }
            default:
                throw new DeltaException($"Deletion vector of storage type '{StorageType}' has no file.");
        }
    }

    /// <summary>For storage type <c>i</c>, the bytes <see cref="InlineBytes"/> decodes into (the vector padded to whole 4-byte groups).</summary>
    /// <exception cref="DeltaException">The vector is not inline, or its text is not Z85.</exception>
    public int InlineBufferLength
    {
        get
        {
            if (!IsInline) throw new DeltaException($"Deletion vector of storage type '{StorageType}' is not inline.");
            return Z85.DecodedLength(PathOrInlineDv.Length);
        }
    }

    /// <summary>
    /// For storage type <c>i</c>, decodes the serialized vector into <paramref name="buffer"/> (at least
    /// <see cref="InlineBufferLength"/> bytes: stack or native memory) and returns it, without the padding.
    /// </summary>
    /// <exception cref="DeltaException">The vector is not inline, its text is not Z85, or it is shorter than its size.</exception>
    public ReadOnlySpan<byte> InlineBytes(Span<byte> buffer)
    {
        int padded = InlineBufferLength;
        if (buffer.Length < padded) throw new ArgumentException($"The buffer holds {buffer.Length} bytes, the vector needs {padded}.", nameof(buffer));
        // The encoder pads the vector to a multiple of four bytes; the descriptor carries the true size.
        if (SizeInBytes > padded)
            throw new DeltaException($"The inline deletion vector holds {padded} bytes but its descriptor says {SizeInBytes}.");
        Z85.Decode(PathOrInlineDv, buffer);
        return buffer[..SizeInBytes];
    }
}

/// <summary>The Z85 text encoding of binary data: every 4 bytes become 5 characters.</summary>
internal static class Z85
{
    private const string Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    private static readonly sbyte[] s_decode = BuildDecode();

    private static sbyte[] BuildDecode()
    {
        var table = new sbyte[128];
        Array.Fill(table, (sbyte)-1);
        for (int i = 0; i < Alphabet.Length; i++) table[Alphabet[i]] = (sbyte)i;
        return table;
    }

    /// <summary>The bytes <paramref name="textLength"/> characters decode into.</summary>
    /// <exception cref="DeltaException">The length is not a multiple of 5.</exception>
    public static int DecodedLength(int textLength) =>
        textLength % 5 == 0 ? textLength / 5 * 4 : throw new DeltaException("Z85 text length is not a multiple of 5.");

    /// <summary>Decodes <paramref name="text"/> into the start of <paramref name="bytes"/>; returns the bytes written.</summary>
    public static int Decode(ReadOnlySpan<char> text, Span<byte> bytes)
    {
        int length = DecodedLength(text.Length);
        if (bytes.Length < length) throw new ArgumentException($"Z85 text of {text.Length} characters needs {length} bytes.", nameof(bytes));
        for (int i = 0, o = 0; i < text.Length; i += 5, o += 4)
        {
            ulong value = 0;
            for (int j = 0; j < 5; j++)
            {
                char c = text[i + j];
                int digit = c < 128 ? s_decode[c] : -1;
                if (digit < 0) throw new DeltaException($"Invalid Z85 character '{c}'.");
                value = value * 85 + (uint)digit;
            }
            // A group above 2^32 - 1 is not valid Z85, but Delta's reference decoder (Java Base85Codec)
            // computes the group as a long and keeps the low 32 bits, and tables in the wild rely on it.
            // Follow the reference: the shifts below keep only the low 32 bits.
            bytes[o] = (byte)(value >> 24);
            bytes[o + 1] = (byte)(value >> 16);
            bytes[o + 2] = (byte)(value >> 8);
            bytes[o + 3] = (byte)value;
        }
        return length;
    }

    public static string Encode(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length % 4 != 0) throw new DeltaException("Z85 input length is not a multiple of 4.");
        int length = bytes.Length / 4 * 5;
        // The text is the only managed allocation: the characters are built on the stack or in native memory.
        using NativeArray<char>? native = length > StackChars ? new NativeArray<char>(length) : null;
        Span<char> chars = native is null ? stackalloc char[StackChars] : native.AsSpan(0, length);
        chars = chars[..length];
        for (int i = 0, o = 0; i < bytes.Length; i += 4, o += 5)
        {
            uint value = (uint)(bytes[i] << 24 | bytes[i + 1] << 16 | bytes[i + 2] << 8 | bytes[i + 3]);
            for (int j = 4; j >= 0; j--)
            {
                chars[o + j] = Alphabet[(int)(value % 85)];
                value /= 85;
            }
        }
        return new string(chars);
    }

    private const int StackChars = 1024;
}
