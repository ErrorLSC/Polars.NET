using System.Buffers.Binary;
using System.Security.Cryptography;
using Kaminari.Sazanami.Crypto;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="AesCtr"/>: the NIST SP 800-38A known answers (F.5.1, F.5.5), inputs spanning several key-stream batches
/// and of every tail length against a block-by-block reference, and the counter carrying across bytes and words.
/// </summary>
public sealed class AesCtrTests
{
    [Theory]
    // F.5.1 CTR-AES128.Encrypt and F.5.5 CTR-AES256.Encrypt: the counter's last byte carries (…feff, …ff00).
    [InlineData("2b7e151628aed2a6abf7158809cf4f3c",
        "874d6191b620e3261bef6864990db6ce9806f66b7970fdff8617187bb9fffdff5ae4df3edbd5d35e5b4f09020db03eab1e031dda2fbe03d1792170a0f3009cee")]
    [InlineData("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4",
        "601ec313775789a5b7a7f504bbf3d228f443e3ca4d62b59aca84e990cacaf5c52b0930daa23de94ce87017ba2d84988ddfc9c58db67aada613c2dd08457941a6")]
    public void Matches_The_NIST_Vectors(string key, string ciphertext)
    {
        byte[] counter = Convert.FromHexString("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");
        byte[] plain = Convert.FromHexString(
            "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710");
        using var ctr = new AesCtr(Convert.FromHexString(key));
        byte[] output = new byte[plain.Length];
        ctr.Transform(counter, plain, output);
        Assert.Equal(ciphertext, Convert.ToHexString(output).ToLowerInvariant());
        ctr.Transform(counter, output, output); // decrypting is the same transform, in place too
        Assert.Equal(plain, output);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(4095)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData(20_000)]
    public void Spans_Batches_And_Tails_As_Block_By_Block(int length)
    {
        byte[] key = [.. Enumerable.Range(0, 32).Select(i => (byte)(i * 7 + 1))];
        // A counter whose low 32 bits are about to wrap: the carry reaches the nonce's bytes, as a 128-bit counter does.
        byte[] counter = [.. Enumerable.Range(0, 12).Select(i => (byte)(0xA0 + i)), 0xFF, 0xFF, 0xFF, 0xFD];
        byte[] input = new byte[length];
        new Random(length).NextBytes(input);
        using var ctr = new AesCtr(key);
        byte[] output = new byte[length];
        ctr.Transform(counter, input, output);
        Assert.Equal(Reference(key, counter, input), output);
    }

    [Fact]
    public void Refuses_A_Bad_Counter_Or_A_Short_Output()
    {
        using var ctr = new AesCtr(new byte[16]);
        Assert.Throws<ArgumentException>(() => ctr.Transform(new byte[12], new byte[4], new byte[4]));
        Assert.Throws<ArgumentException>(() => ctr.Transform(new byte[16], new byte[4], new byte[3]));
        Assert.ThrowsAny<CryptographicException>(() => new AesCtr(new byte[20]));
    }

    /// <summary>CTR one block at a time: the key stream is AES of the counter, the counter a big-endian 128-bit number.</summary>
    private static byte[] Reference(byte[] key, byte[] counter, byte[] input)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        byte[] block = [.. counter], stream = new byte[16], output = new byte[input.Length];
        for (int at = 0; at < input.Length; at += 16)
        {
            aes.EncryptEcb(block, stream, PaddingMode.None);
            for (int i = 0; i < 16 && at + i < input.Length; i++) output[at + i] = (byte)(input[at + i] ^ stream[i]);
            UInt128 next = BinaryPrimitives.ReadUInt128BigEndian(block) + 1;
            BinaryPrimitives.WriteUInt128BigEndian(block, next);
        }
        return output;
    }
}
