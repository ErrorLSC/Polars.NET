using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Compression;

/// <summary>
/// XXH32 (seed 0) over data appended piece by piece, as the LZ4 frame format's content checksum: a value type, so that
/// a decoder hashes each block right after writing it (still in cache) with no allocation and no per-call overhead
/// beyond a tail of fewer than 16 bytes carried to the next piece. Equal to <c>System.IO.Hashing.XxHash32</c>.
/// </summary>
internal struct XxHash32Stream
{
    private const uint Prime1 = 2654435761U, Prime2 = 2246822519U, Prime3 = 3266489917U, Prime4 = 668265263U, Prime5 = 374761393U;

    [InlineArray(16)]
    private struct Stripe
    {
        private byte _first;
    }

    private uint _v1, _v2, _v3, _v4;
    private long _length;
    private Stripe _tail;
    private int _tailLength;

    public XxHash32Stream()
    {
        _v1 = unchecked(Prime1 + Prime2);
        _v2 = Prime2;
        _v3 = 0;
        _v4 = unchecked(0 - Prime1);
    }

    public void Append(ReadOnlySpan<byte> data)
    {
        _length += data.Length;
        if (_tailLength > 0)
        {
            int take = Math.Min(16 - _tailLength, data.Length);
            data[..take].CopyTo(((Span<byte>)_tail)[_tailLength..]);
            _tailLength += take;
            data = data[take..];
            if (_tailLength < 16) return;
            Stripes(((ReadOnlySpan<byte>)_tail)[..16]);
            _tailLength = 0;
        }
        int whole = data.Length & ~15;
        if (whole > 0) Stripes(data[..whole]);
        data[whole..].CopyTo(_tail);
        _tailLength = data.Length - whole;
    }

    /// <summary>The hash of everything appended so far.</summary>
    public readonly uint Hash()
    {
        uint h = _length >= 16
            ? BitOperations.RotateLeft(_v1, 1) + BitOperations.RotateLeft(_v2, 7) + BitOperations.RotateLeft(_v3, 12) + BitOperations.RotateLeft(_v4, 18)
            : Prime5;
        h += (uint)_length;
        ReadOnlySpan<byte> rest = ((ReadOnlySpan<byte>)_tail)[.._tailLength];
        while (rest.Length >= 4)
        {
            h += BinaryPrimitives.ReadUInt32LittleEndian(rest) * Prime3;
            h = BitOperations.RotateLeft(h, 17) * Prime4;
            rest = rest[4..];
        }
        foreach (byte b in rest)
        {
            h += b * Prime5;
            h = BitOperations.RotateLeft(h, 11) * Prime1;
        }
        h ^= h >> 15;
        h *= Prime2;
        h ^= h >> 13;
        h *= Prime3;
        h ^= h >> 16;
        return h;
    }

    /// <summary>Whole 16-byte stripes, the four lanes in locals.</summary>
    private void Stripes(ReadOnlySpan<byte> data)
    {
        uint v1 = _v1, v2 = _v2, v3 = _v3, v4 = _v4;
        ref byte at = ref MemoryMarshal.GetReference(data);
        ref byte end = ref Unsafe.Add(ref at, data.Length);
        while (Unsafe.IsAddressLessThan(ref at, ref end))
        {
            v1 = Round(v1, Lane(ref at, 0));
            v2 = Round(v2, Lane(ref at, 4));
            v3 = Round(v3, Lane(ref at, 8));
            v4 = Round(v4, Lane(ref at, 12));
            at = ref Unsafe.Add(ref at, 16);
        }
        (_v1, _v2, _v3, _v4) = (v1, v2, v3, v4);

        static uint Lane(ref byte at, int offset)
        {
            uint lane = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref at, offset));
            return BitConverter.IsLittleEndian ? lane : BinaryPrimitives.ReverseEndianness(lane);
        }

        static uint Round(uint accumulator, uint lane) => BitOperations.RotateLeft(accumulator + lane * Prime2, 13) * Prime1;
    }
}
