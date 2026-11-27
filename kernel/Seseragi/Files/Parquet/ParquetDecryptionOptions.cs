namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// The keys to read Parquet files written with Parquet modular encryption: <c>AES_GCM_V1</c> and
/// <c>AES_GCM_CTR_V1</c>, 16-, 24- or 32-byte keys, encrypted or signed plaintext footers. Keys are given directly;
/// keys managed by a KMS (wrapped keys in the key metadata) are not unwrapped.
/// <para>
/// A column encrypted with a key of its own that is not given here can be left out of the read: the other columns
/// read, and reading that one fails, naming it. A file that is not encrypted reads as it is. A plaintext footer can be
/// read without any options: then the columns that are not encrypted read.
/// </para>
/// </summary>
/// <param name="footerKey">The key of the footer, and of the columns encrypted with it.</param>
public sealed class ParquetDecryptionOptions(ReadOnlyMemory<byte> footerKey)
{
    /// <summary>The key of the footer, and of the columns encrypted with it.</summary>
    public ReadOnlyMemory<byte> FooterKey { get; } = footerKey;

    /// <summary>The keys of the columns encrypted with a key of their own, by dot-separated Parquet path (<c>a.b</c>).</summary>
    public IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? ColumnKeys { get; init; }

    /// <summary>The AAD prefix the file was written with, needed when the file does not store it; empty means none.</summary>
    public ReadOnlyMemory<byte>? AadPrefix { get; init; }

    /// <summary>Check the signature of a plaintext footer with <see cref="FooterKey"/> (default true).</summary>
    public bool VerifyFooterSignature { get; init; } = true;

    /// <summary>Never shows the keys.</summary>
    public override string ToString() =>
        $"ParquetDecryptionOptions(footer key {FooterKey.Length} bytes, {ColumnKeys?.Count ?? 0} column keys)";
}
