using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

/// <summary>
/// How Parquet files are encrypted when written (Parquet modular encryption, AES-GCM), with keys given directly.
/// <para>
/// Without <see cref="ColumnKeys"/> the footer and every column are encrypted with <see cref="FooterKey"/> (uniform
/// encryption). With them, only those columns are encrypted, each with its own key; the other columns are written in
/// plain, and the footer is encrypted (or signed) with <see cref="FooterKey"/>. The engine takes 16-byte (AES-128)
/// and 32-byte (AES-256) keys; it copies them into native memory for the write and does not wipe them afterwards.
/// </para>
/// <para>
/// Experimental: the engine marks Parquet encryption unstable, and it may change in a minor release. Keys managed by
/// a KMS (wrapped keys) are not supported.
/// </para>
/// </summary>
/// <example>
/// <code>
/// var encryption = new ParquetEncryption("0123456789012345"u8.ToArray())
/// {
///     ColumnKeys = new Dictionary&lt;string, ReadOnlyMemory&lt;byte&gt;&gt; { ["ssn"] = "1234567890123450"u8.ToArray() },
/// };
/// df.WriteParquet("people.parquet", encryption: encryption);
/// </code>
/// </example>
/// <param name="footerKey">The key of the footer, and of every column when no column has a key of its own.</param>
public sealed class ParquetEncryption(ReadOnlyMemory<byte> footerKey)
{
    /// <summary>The key of the footer, and of every column when no column has a key of its own.</summary>
    public ReadOnlyMemory<byte> FooterKey { get; } = footerKey;

    /// <summary>
    /// The columns to encrypt, each with a key of its own (the others are written in plain), by dot-separated Parquet path (<c>a.b</c> for field <c>b</c> of
    /// struct column <c>a</c>). A partitioned write cannot partition by such a column.
    /// </summary>
    public IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? ColumnKeys { get; init; }

    /// <summary>
    /// Leave the footer readable without a key (it is signed instead): readers can see the schema and read the
    /// columns encrypted with no key of their own.
    /// </summary>
    public bool PlaintextFooter { get; init; }

    /// <summary>Additional authenticated data that binds the file to its context (a table name, a path); empty means none.</summary>
    public ReadOnlyMemory<byte>? AadPrefix { get; init; }

    /// <summary>Store <see cref="AadPrefix"/> in the file; when false (the default) readers must supply it.</summary>
    public bool StoreAadPrefix { get; init; }

    /// <summary>Never shows the keys.</summary>
    public override string ToString() =>
        $"ParquetEncryption(footer key {FooterKey.Length} bytes, {ColumnKeys?.Count ?? 0} column keys{(PlaintextFooter ? ", plaintext footer" : "")})";

    internal ParquetKeys Keys => new(FooterKey, ColumnKeys, AadPrefix);
}

/// <summary>
/// The keys to read Parquet files written with Parquet modular encryption (AES-GCM).
/// <para>
/// A column encrypted with a key of its own that is not given here can still be left out of the read; reading it
/// fails, naming the column. A file that is not encrypted reads as it is. The engine takes 16-byte (AES-128) and 32-byte (AES-256) keys and reads files encrypted
/// with <c>AES_GCM_V1</c>; files written with <c>AES_GCM_CTR_V1</c> are refused.
/// </para>
/// <para>Experimental: the engine marks Parquet encryption unstable. Keys managed by a KMS are not supported.</para>
/// </summary>
/// <param name="footerKey">The key of the footer, and of every column without its own key.</param>
public sealed class ParquetDecryption(ReadOnlyMemory<byte> footerKey)
{
    /// <summary>The key of the footer, and of every column without its own key.</summary>
    public ReadOnlyMemory<byte> FooterKey { get; } = footerKey;

    /// <summary>Keys of columns encrypted with a key of their own, by dot-separated Parquet path.</summary>
    public IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? ColumnKeys { get; init; }

    /// <summary>The AAD prefix the file was written with, when the file does not store it; empty means none.</summary>
    public ReadOnlyMemory<byte>? AadPrefix { get; init; }

    /// <summary>Check the signature of a plaintext footer (default true).</summary>
    public bool VerifyFooterSignature { get; init; } = true;

    /// <summary>Never shows the keys.</summary>
    public override string ToString() =>
        $"ParquetDecryption(footer key {FooterKey.Length} bytes, {ColumnKeys?.Count ?? 0} column keys)";

    internal ParquetKeys Keys => new(FooterKey, ColumnKeys, AadPrefix);
}

/// <summary>Builds the engine's Parquet option handles from the public options.</summary>
internal static class ParquetNativeOptions
{
    public static ParquetScanExtrasHandle? Scan(
        bool ignoreExtraColumns, ScanCastOptions? castOptions, string[]? hiddenFilePrefixes, ParquetDecryption? decryption) =>
        PolarsWrapper.ParquetScanExtras(
            ignoreExtraColumns,
            castOptions?.ToNative() ?? PlScanCastFlags.None,
            hiddenFilePrefixes,
            decryption?.Keys,
            decryption?.VerifyFooterSignature ?? true);

    public static ParquetWriteExtrasHandle? Write(
        IReadOnlyDictionary<string, string>? metadata, Apache.Arrow.Schema? arrowSchema, ParquetEncryption? encryption) =>
        PolarsWrapper.ParquetWriteExtras(
            metadata,
            arrowSchema,
            encryption?.Keys,
            encryption?.PlaintextFooter ?? false,
            encryption?.StoreAadPrefix ?? false);
}
