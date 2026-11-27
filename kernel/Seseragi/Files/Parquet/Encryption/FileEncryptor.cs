#pragma warning disable CS1573 // parameters documented where they need it
using System.Buffers.Binary;
using System.Security.Cryptography;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Crypto;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet.Encryption;

/// <summary>
/// The encryption of one Parquet file being written (Parquet modular encryption): its algorithm with a random file id,
/// its AAD and keys, and which columns are encrypted with which key. Made when the writer opens; a
/// <see cref="ColumnEncryptor"/> per column chunk encrypts its modules.
/// </summary>
internal sealed class FileEncryptor
{
    /// <summary>The bytes of the random file id in each module's AAD (parquet-mr and Arrow write 8).</summary>
    private const int FileIdLength = 8;

    private readonly byte[] _footerKey;
    private readonly Dictionary<string, byte[]>? _columnKeys;
    private readonly byte[] _fileAad;
    private readonly byte[] _cryptoMetadata; // the FileCryptoMetaData an encrypted footer follows

    private FileEncryptor(EncryptionAlgorithm algorithm, byte[] fileAad, byte[] footerKey, Dictionary<string, byte[]>? columnKeys, bool plaintextFooter)
    {
        Algorithm = algorithm;
        _fileAad = fileAad;
        _footerKey = footerKey;
        _columnKeys = columnKeys;
        PlaintextFooter = plaintextFooter;
        var thrift = new ThriftCompactWriter();
        new FileCryptoMetaData { EncryptionAlgorithm = algorithm }.Write(thrift);
        _cryptoMetadata = thrift.ToArray();
    }

    /// <summary>The algorithm as the file stores it (with its file id, and its AAD prefix when stored).</summary>
    public EncryptionAlgorithm Algorithm { get; }

    /// <summary>Whether the footer stays readable (signed).</summary>
    public bool PlaintextFooter { get; }

    private bool Ctr => Algorithm.Member == 2;

    /// <summary>The encryption of a file whose leaves have the dot-separated <paramref name="columnPaths"/>.</summary>
    /// <exception cref="ArgumentException">A key is not 16, 24 or 32 bytes long, or a column key names no leaf (or several).</exception>
    /// <exception cref="PlatformNotSupportedException">AES-GCM is not available on this platform.</exception>
    public static FileEncryptor Create(ParquetEncryptionOptions options, IReadOnlyList<string> columnPaths)
    {
        if (!AesGcm.IsSupported)
            throw new PlatformNotSupportedException("AES-GCM is not available on this platform, so encrypted Parquet files cannot be written here.");
        byte[] footerKey = Key(options.FooterKey, "the footer key");
        Dictionary<string, byte[]>? columnKeys = null;
        if (options.ColumnKeys is { Count: > 0 } keys)
        {
            var counts = columnPaths.GroupBy(p => p, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
            columnKeys = new(StringComparer.Ordinal);
            foreach (var (column, key) in keys)
            {
                if (!counts.TryGetValue(column, out int n))
                {
                    // Suggest the leaves under the same top-level column, else the first ones.
                    string top = column.Split('.')[0];
                    string[] near = [.. columnPaths.Where(p => p == top || p.StartsWith(top + ".", StringComparison.Ordinal)).Take(5)];
                    IEnumerable<string> examples = near.Length > 0 ? near : columnPaths.Take(5);
                    throw new ArgumentException($"Parquet encryption: a key is given for column '{column}', which the schema does not have "
                        + $"(columns are named by their dot-separated Parquet path: {string.Join(", ", examples.Select(p => $"'{p}'"))}).");
                }
                if (n > 1)
                    throw new ArgumentException($"Parquet encryption: '{column}' names {n} columns of the schema, which cannot be encrypted apart.");
                columnKeys[column] = Key(key, $"the key of column '{column}'");
            }
        }
        byte[] prefix = options.AadPrefix is { Length: > 0 } given ? given.ToArray() : [];
        byte[] fileId = RandomNumberGenerator.GetBytes(FileIdLength);
        var algorithm = new EncryptionAlgorithm
        {
            Member = options.Algorithm == ParquetEncryptionAlgorithm.AesGcmCtr ? (short)2 : (short)1,
            AadPrefix = prefix.Length > 0 && options.StoreAadPrefix ? prefix : null,
            AadFileUnique = fileId,
            SupplyAadPrefix = prefix.Length > 0 && !options.StoreAadPrefix ? true : null,
        };
        return new FileEncryptor(algorithm, [.. prefix, .. fileId], footerKey, columnKeys, options.PlaintextFooter);

        static byte[] Key(ReadOnlyMemory<byte> key, string which) => key.Length is 16 or 24 or 32
            ? key.ToArray()
            : throw new ArgumentException($"Parquet encryption: {which} is {key.Length} bytes; AES keys are 16, 24 or 32 bytes.");
    }

    /// <summary>Whether a column is encrypted: every one under uniform encryption, else those with a key.</summary>
    public bool Encrypts(string path) => _columnKeys is null || _columnKeys.ContainsKey(path);

    /// <summary>The <c>crypto_metadata</c> of a column's chunks; null for a column written in plain.</summary>
    public ColumnCryptoMetaData? CryptoMetaData(string path, string[] pathInSchema) =>
        _columnKeys is null ? new ColumnCryptoMetaData { WithFooterKey = true }
        : _columnKeys.ContainsKey(path) ? new ColumnCryptoMetaData { Path = pathInSchema }
        : null;

    /// <summary>
    /// Whether a column's metadata goes into <c>encrypted_column_metadata</c>: when its key is not the footer's
    /// (a column key), or the footer is not encrypted.
    /// </summary>
    public bool SeparatesMetadata(string path) => Encrypts(path) && (_columnKeys is not null || PlaintextFooter);

    /// <summary>The encryptor of a column chunk's modules; null for a column written in plain.</summary>
    public ColumnEncryptor? ForChunk(string path, int rowGroup, int column)
    {
        if (!Encrypts(path)) return null;
        if (rowGroup > short.MaxValue || column > short.MaxValue)
            throw new InvalidOperationException($"Parquet encryption names row groups and columns by 16-bit ordinals: {rowGroup}, {column} is beyond.");
        return new ColumnEncryptor(_columnKeys?[path] ?? _footerKey, Ctr, _fileAad, rowGroup, column);
    }

    /// <summary>The bytes after a footer of <paramref name="footer"/> bytes, before its length: the signature, or the crypto metadata and the sealed footer.</summary>
    public int StoredFooterLength(int footer) => PlaintextFooter
        ? footer + FileDecryptor.NonceLength + FileDecryptor.TagLength
        : _cryptoMetadata.Length + ModuleSeal.GcmLength(footer);

    /// <summary>
    /// How the file ends before the footer's length (<see cref="StoredFooterLength"/> bytes into <paramref name="stored"/>):
    /// a plaintext footer and its signature (a nonce and the AES-GCM tag of the footer, whose ciphertext is not kept),
    /// or the <c>FileCryptoMetaData</c> and the encrypted footer module of a <c>PARE</c> file.
    /// </summary>
    public void WriteStoredFooter(ReadOnlySpan<byte> footer, Span<byte> stored)
    {
        using var gcm = new AesGcm(_footerKey, FileDecryptor.TagLength);
        Span<byte> aad = stackalloc byte[_fileAad.Length + 1];
        int aadLength = ModuleCipher.Aad(aad, _fileAad, ParquetModule.Footer, -1, -1, -1);
        if (!PlaintextFooter)
        {
            _cryptoMetadata.CopyTo(stored);
            ModuleSeal.Gcm(gcm, footer, aad[..aadLength], stored[_cryptoMetadata.Length..]);
            return;
        }
        footer.CopyTo(stored);
        Span<byte> signature = stored.Slice(footer.Length, FileDecryptor.NonceLength + FileDecryptor.TagLength);
        RandomNumberGenerator.Fill(signature[..FileDecryptor.NonceLength]);
        using var scratch = new NativeByteBuffer(Math.Max(footer.Length, 1), pooled: true);
        gcm.Encrypt(signature[..FileDecryptor.NonceLength], footer, scratch.Span[..footer.Length], signature[FileDecryptor.NonceLength..], aad[..aadLength]);
    }
}

/// <summary>The modules of one column chunk, encrypted for one writer thread (pages by the file's algorithm, the rest with AES-GCM).</summary>
internal sealed class ColumnEncryptor : IDisposable
{
    private readonly AesGcm _gcm;
    private readonly AesCtr? _ctr;
    private readonly byte[] _fileAad;
    private readonly int _rowGroup, _column;

    public ColumnEncryptor(byte[] key, bool ctr, byte[] fileAad, int rowGroup, int column)
    {
        _gcm = new AesGcm(key, FileDecryptor.TagLength);
        _ctr = ctr ? new AesCtr(key) : null;
        _fileAad = fileAad;
        _rowGroup = rowGroup;
        _column = column;
    }

    private bool IsCtr(ParquetModule type) => _ctr is not null && type is ParquetModule.DataPage or ParquetModule.DictionaryPage;

    /// <summary>The bytes the module of <paramref name="plainLength"/> plaintext bytes takes (its length prefix included).</summary>
    public int ModuleLength(ParquetModule type, int plainLength) => IsCtr(type) ? ModuleSeal.CtrLength(plainLength) : ModuleSeal.GcmLength(plainLength);

    /// <summary>
    /// Encrypts <paramref name="plain"/> as a module into <paramref name="module"/> (<see cref="ModuleLength"/> bytes);
    /// in place when <paramref name="plain"/> is the module's ciphertext, <see cref="ModuleSeal.Prefix"/> bytes in.
    /// </summary>
    /// <param name="page">The data page's ordinal in the chunk (data pages and their headers), else negative.</param>
    public void Encrypt(ParquetModule type, int page, ReadOnlySpan<byte> plain, Span<byte> module)
    {
        if (IsCtr(type))
        {
            ModuleSeal.Ctr(_ctr!, plain, module);
            return;
        }
        if (page > short.MaxValue) throw new InvalidOperationException($"Parquet encryption names pages by 16-bit ordinals: a chunk cannot have {page + 1} pages.");
        Span<byte> aad = stackalloc byte[_fileAad.Length + 7];
        int length = ModuleCipher.Aad(aad, _fileAad, type, _rowGroup, _column, type is ParquetModule.DataPage or ParquetModule.DataPageHeader ? page : -1);
        ModuleSeal.Gcm(_gcm, plain, aad[..length], module);
    }

    /// <summary>Encrypts a module into a new array.</summary>
    public byte[] Encrypt(ParquetModule type, int page, ReadOnlySpan<byte> plain)
    {
        byte[] module = new byte[ModuleLength(type, plain.Length)];
        Encrypt(type, page, plain, module);
        return module;
    }

    public void Dispose()
    {
        _gcm.Dispose();
        _ctr?.Dispose();
    }
}

/// <summary>Writes encrypted modules: a 4-byte little-endian length, a random nonce, the ciphertext and (GCM) the tag.</summary>
internal static class ModuleSeal
{
    /// <summary>The bytes before a module's ciphertext: its length and nonce (a page sealed in place is compressed after them).</summary>
    public const int Prefix = 4 + FileDecryptor.NonceLength;

    /// <summary>The bytes after an AES-GCM module's ciphertext at most: its tag.</summary>
    public const int Suffix = FileDecryptor.TagLength;

    public static int GcmLength(int plain) => 4 + FileDecryptor.NonceLength + plain + FileDecryptor.TagLength;

    public static int CtrLength(int plain) => 4 + FileDecryptor.NonceLength + plain;

    public static void Gcm(AesGcm gcm, ReadOnlySpan<byte> plain, ReadOnlySpan<byte> aad, Span<byte> module)
    {
        int length = GcmLength(plain.Length);
        BinaryPrimitives.WriteInt32LittleEndian(module, length - 4);
        Span<byte> nonce = module.Slice(4, FileDecryptor.NonceLength);
        RandomNumberGenerator.Fill(nonce);
        gcm.Encrypt(nonce, plain, module.Slice(4 + FileDecryptor.NonceLength, plain.Length), module.Slice(length - FileDecryptor.TagLength, FileDecryptor.TagLength), aad);
    }

    public static void Ctr(AesCtr ctr, ReadOnlySpan<byte> plain, Span<byte> module)
    {
        BinaryPrimitives.WriteInt32LittleEndian(module, FileDecryptor.NonceLength + plain.Length);
        Span<byte> nonce = module.Slice(4, FileDecryptor.NonceLength);
        RandomNumberGenerator.Fill(nonce);
        Span<byte> counter = stackalloc byte[16];
        nonce.CopyTo(counter);
        BinaryPrimitives.WriteInt32BigEndian(counter[FileDecryptor.NonceLength..], 1);
        ctr.Transform(counter, plain, module[(4 + FileDecryptor.NonceLength)..]);
    }
}
