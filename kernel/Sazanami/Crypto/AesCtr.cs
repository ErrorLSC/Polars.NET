using System.Runtime.Intrinsics;
using System.Security.Cryptography;

namespace Kaminari.Sazanami.Crypto;

/// <summary>
/// AES in counter mode (NIST SP 800-38A) over the BCL's AES block cipher, which offers no CTR mode: the key stream is
/// the encryption of successive counter blocks (the whole 128-bit block counts up, big-endian), XORed with the data.
/// Encrypting and decrypting are the same transform. An instance is used by one thread at a time.
/// </summary>
public sealed class AesCtr : IDisposable
{
    /// <summary>The bytes of key stream made per call of the block cipher.</summary>
    private const int Batch = 4096;

    private readonly Aes _aes;

    /// <param name="key">A 16-, 24- or 32-byte AES key.</param>
    /// <exception cref="CryptographicException">The key is not 16, 24 or 32 bytes long.</exception>
    public AesCtr(ReadOnlySpan<byte> key)
    {
        _aes = Aes.Create();
        _aes.Key = key.ToArray();
    }

    /// <summary>
    /// Transforms <paramref name="input"/> into <paramref name="output"/> (of the same length, or longer), starting
    /// from the 16-byte counter block <paramref name="counter"/>.
    /// </summary>
    public void Transform(ReadOnlySpan<byte> counter, ReadOnlySpan<byte> input, Span<byte> output)
    {
        if (counter.Length != 16) throw new ArgumentException("The counter block must be 16 bytes.", nameof(counter));
        if (output.Length < input.Length) throw new ArgumentException("The output is shorter than the input.", nameof(output));
        Span<byte> block = stackalloc byte[16];
        counter.CopyTo(block);
        Span<byte> counters = stackalloc byte[Batch];
        Span<byte> stream = stackalloc byte[Batch];
        for (int at = 0; at < input.Length; at += Batch)
        {
            int n = Math.Min(Batch, input.Length - at);
            int blocks = (n + 15) / 16;
            for (int b = 0; b < blocks; b++)
            {
                block.CopyTo(counters.Slice(b * 16, 16));
                Increment(block);
            }
            _aes.EncryptEcb(counters[..(blocks * 16)], stream[..(blocks * 16)], PaddingMode.None);
            Xor(input.Slice(at, n), stream[..n], output.Slice(at, n));
        }
    }

    /// <summary>Adds one to a big-endian 128-bit counter.</summary>
    private static void Increment(Span<byte> block)
    {
        for (int i = 15; i >= 0; i--)
        {
            if (++block[i] != 0) return;
        }
    }

    /// <summary><paramref name="output"/> = <paramref name="a"/> XOR <paramref name="b"/>.</summary>
    private static void Xor(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, Span<byte> output)
    {
        int i = 0;
        if (Vector256.IsHardwareAccelerated)
        {
            for (; i <= a.Length - Vector256<byte>.Count; i += Vector256<byte>.Count)
                (Vector256.Create(a[i..]) ^ Vector256.Create(b[i..])).CopyTo(output[i..]);
        }
        if (Vector128.IsHardwareAccelerated)
        {
            for (; i <= a.Length - Vector128<byte>.Count; i += Vector128<byte>.Count)
                (Vector128.Create(a[i..]) ^ Vector128.Create(b[i..])).CopyTo(output[i..]);
        }
        for (; i < a.Length; i++) output[i] = (byte)(a[i] ^ b[i]);
    }

    /// <inheritdoc/>
    public void Dispose() => _aes.Dispose();
}
