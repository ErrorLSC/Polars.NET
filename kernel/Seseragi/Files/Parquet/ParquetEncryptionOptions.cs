namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>How the pages of an encrypted Parquet file are encrypted (other modules always use AES-GCM).</summary>
public enum ParquetEncryptionAlgorithm
{
    /// <summary><c>AES_GCM_V1</c>: every module, pages included, with AES-GCM (authenticated); what every reader of encrypted files reads.</summary>
    AesGcm,

    /// <summary>
    /// <c>AES_GCM_CTR_V1</c>: pages with AES-CTR (not authenticated, faster without AES hardware), the rest with
    /// AES-GCM. Not every reader takes it (pyarrow and Spark do).
    /// </summary>
    AesGcmCtr,
}

/// <summary>
/// How <see cref="ParquetWriter"/> encrypts a file (Parquet modular encryption), with keys given directly (16, 24 or
/// 32 bytes). Without <see cref="ColumnKeys"/> the footer and every column are encrypted with <see cref="FooterKey"/>
/// (uniform encryption); with them only those columns are encrypted, each with its own key, the others are written in
/// plain, and the footer is encrypted (or signed) with <see cref="FooterKey"/>. Each file gets a random file id and
/// random nonces.
/// </summary>
/// <param name="footerKey">The key of the footer, and of every column when no column has a key of its own.</param>
public sealed class ParquetEncryptionOptions(ReadOnlyMemory<byte> footerKey)
{
    /// <summary>The key of the footer, and of every column when no column has a key of its own.</summary>
    public ReadOnlyMemory<byte> FooterKey { get; } = footerKey;

    /// <summary>The columns to encrypt, each with a key of its own, by dot-separated Parquet path (<c>a.b</c>).</summary>
    public IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? ColumnKeys { get; init; }

    /// <summary>
    /// Leave the footer readable without a key, signed with <see cref="FooterKey"/>: readers see the schema and read
    /// the columns that are not encrypted. The statistics of encrypted columns are left out of it.
    /// </summary>
    public bool PlaintextFooter { get; init; }

    /// <summary>Additional authenticated data that binds the file to its context (a table name, a path); empty means none.</summary>
    public ReadOnlyMemory<byte>? AadPrefix { get; init; }

    /// <summary>Store <see cref="AadPrefix"/> in the file; when false (the default) readers must supply it.</summary>
    public bool StoreAadPrefix { get; init; }

    /// <summary>How pages are encrypted (default <see cref="ParquetEncryptionAlgorithm.AesGcm"/>).</summary>
    public ParquetEncryptionAlgorithm Algorithm { get; init; } = ParquetEncryptionAlgorithm.AesGcm;

    /// <summary>Never shows the keys.</summary>
    public override string ToString() =>
        $"ParquetEncryptionOptions({Algorithm}, footer key {FooterKey.Length} bytes, {ColumnKeys?.Count ?? 0} column keys{(PlaintextFooter ? ", plaintext footer" : "")})";
}
