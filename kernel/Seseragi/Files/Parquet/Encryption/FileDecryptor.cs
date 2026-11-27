#pragma warning disable CS1573 // parameters documented where they need it
using System.Buffers.Binary;
using System.Security.Cryptography;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Crypto;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet.Encryption;

/// <summary>The modules of a Parquet file encrypted separately (the module type in their AAD).</summary>
internal enum ParquetModule : byte
{
    Footer = 0,
    ColumnMetaData = 1,
    DataPage = 2,
    DictionaryPage = 3,
    DataPageHeader = 4,
    DictionaryPageHeader = 5,
    ColumnIndex = 6,
    OffsetIndex = 7,
    BloomFilterHeader = 8,
    BloomFilterBitset = 9,
}

/// <summary>
/// The decryption of one Parquet file (Parquet modular encryption): its algorithm, its AAD (prefix and file id) and
/// the keys given. Made when the file is opened, and shared by its readers: each reader of a column chunk takes a
/// <see cref="ColumnDecryptor"/> of its own (an AES instance is used by one thread at a time).
/// </summary>
internal sealed class FileDecryptor
{
    public const int NonceLength = 12;
    public const int TagLength = 16;

    private readonly byte[] _fileAad;
    private readonly bool _prefixMissing;   // written with an AAD prefix it does not store, none given
    private readonly byte[]? _footerKey;
    private readonly Dictionary<string, byte[]>? _columnKeys;
    private readonly bool _verifySignature;

    private FileDecryptor(EncryptionAlgorithm algorithm, byte[] fileAad, bool prefixMissing, byte[]? footerKey, Dictionary<string, byte[]>? columnKeys,
        bool verifySignature)
    {
        Algorithm = algorithm;
        _fileAad = fileAad;
        _prefixMissing = prefixMissing;
        _footerKey = footerKey;
        _columnKeys = columnKeys;
        _verifySignature = verifySignature;
    }

    /// <summary>The file's algorithm (with its AAD file id), as stored in it.</summary>
    public EncryptionAlgorithm Algorithm { get; }

    /// <summary>Whether pages are encrypted with AES-CTR (<c>AES_GCM_CTR_V1</c>); other modules always with AES-GCM.</summary>
    public bool Ctr => Algorithm.Member == 2;

    /// <summary>Whether the footer key was given (without it only the columns that are not encrypted read).</summary>
    public bool HasFooterKey => _footerKey is not null;

    /// <summary>The decryption of a file encrypted with <paramref name="algorithm"/>, with the keys of <paramref name="options"/> (none: no keys).</summary>
    /// <exception cref="ParquetFormatException">The algorithm is unknown, or the AAD prefix given differs from the one the file stores.</exception>
    /// <exception cref="ArgumentException">A key is not 16, 24 or 32 bytes long.</exception>
    /// <exception cref="PlatformNotSupportedException">AES-GCM is not available on this platform.</exception>
    public static FileDecryptor Create(EncryptionAlgorithm algorithm, ParquetDecryptionOptions? options)
    {
        if (algorithm.Member is not (1 or 2))
            throw new ParquetFormatException($"The Parquet file is encrypted with {algorithm.Name}, which is not supported (AES_GCM_V1 and AES_GCM_CTR_V1 are).");
        if (algorithm.AadFileUnique is null)
            throw new ParquetFormatException("The Parquet file's encryption algorithm has no aad_file_unique.");
        if (options is not null && !AesGcm.IsSupported)
            throw new PlatformNotSupportedException("AES-GCM is not available on this platform, so encrypted Parquet files cannot be read here.");

        byte[]? supplied = options?.AadPrefix is { Length: > 0 } given ? given.ToArray() : null;
        byte[] prefix = [];
        bool missing = false;
        if (algorithm.AadPrefix is { Length: > 0 } stored)
        {
            if (supplied is not null && !supplied.AsSpan().SequenceEqual(stored))
                throw new ParquetFormatException("The AAD prefix given does not match the one the Parquet file stores.");
            prefix = stored;
        }
        else if (supplied is not null) prefix = supplied;
        else missing = algorithm.SupplyAadPrefix == true;

        byte[]? footerKey = null;
        Dictionary<string, byte[]>? columnKeys = null;
        if (options is not null)
        {
            footerKey = Key(options.FooterKey, "the footer key");
            if (options.ColumnKeys is { Count: > 0 } keys)
            {
                columnKeys = new(StringComparer.Ordinal);
                foreach (var (column, key) in keys) columnKeys[column] = Key(key, $"the key of column '{column}'");
            }
        }
        return new FileDecryptor(algorithm, [.. prefix, .. algorithm.AadFileUnique], missing, footerKey, columnKeys, options?.VerifyFooterSignature ?? true);

        static byte[] Key(ReadOnlyMemory<byte> key, string which) => key.Length is 16 or 24 or 32
            ? key.ToArray()
            : throw new ArgumentException($"Parquet decryption: {which} is {key.Length} bytes; AES keys are 16, 24 or 32 bytes.");
    }

    /// <summary>The footer of an encrypted footer module (<paramref name="module"/>: its length, nonce, ciphertext and tag), decrypted in native scratch and parsed.</summary>
    public FileMetaData DecryptFooter(ReadOnlySpan<byte> module, Schema.FooterInterner? interner)
    {
        byte[] key = _footerKey ?? throw new ParquetFormatException(
            "The Parquet file is encrypted (Parquet modular encryption, encrypted footer): open it with its keys (ParquetDecryptionOptions).");
        CheckPrefix();
        using var gcm = new AesGcm(key, TagLength);
        Span<byte> aad = stackalloc byte[_fileAad.Length + 1];
        Aad(aad, ParquetModule.Footer, -1, -1, -1);
        int length = ModuleCipher.GcmPlainLength(module, "the footer");
        using var plain = new NativeByteBuffer(Math.Max(length, 1), pooled: true);
        if (!ModuleCipher.TryGcm(gcm, module, aad, plain.Span))
            throw new ParquetFormatException("The footer key (or the AAD prefix) does not open this Parquet file: a wrong key, or a damaged footer.");
        return FileMetaData.Parse(plain.Span[..length], interner);
    }

    /// <summary>Checks the signature (nonce and tag) of a plaintext footer, when the footer key was given and checking is on.</summary>
    public void VerifyFooterSignature(ReadOnlySpan<byte> footer, ReadOnlySpan<byte> signature)
    {
        if (_footerKey is null || !_verifySignature) return;
        CheckPrefix();
        if (signature.Length != NonceLength + TagLength) throw new ParquetFormatException("The plaintext footer's signature is cut short.");
        using var gcm = new AesGcm(_footerKey, TagLength);
        Span<byte> aad = stackalloc byte[_fileAad.Length + 1];
        Aad(aad, ParquetModule.Footer, -1, -1, -1);
        Span<byte> tag = stackalloc byte[TagLength];
        using var scratch = new NativeByteBuffer(Math.Max(footer.Length, 1), pooled: true);
        gcm.Encrypt(signature[..NonceLength], footer, scratch.Span[..footer.Length], tag, aad);
        if (!CryptographicOperations.FixedTimeEquals(tag, signature[NonceLength..]))
            throw new ParquetFormatException(
                "The plaintext footer's signature does not match: a wrong footer key or AAD prefix, or the footer was changed.");
    }

    /// <summary>
    /// Puts the decrypted column metadata in the footer wherever the key is given: the chunks of columns encrypted
    /// with a key of their own (their metadata is not in the footer, or only stripped of statistics in a plaintext
    /// footer). Chunks whose key is not given keep what the footer has (nothing, under an encrypted footer).
    /// </summary>
    public void DecryptColumnMetadata(FileMetaData meta)
    {
        Span<byte> aad = stackalloc byte[_fileAad.Length + 5];
        using var plain = new NativeByteBuffer(0, pooled: true);
        for (int g = 0; g < meta.RowGroups.Length; g++)
        {
            RowGroup group = meta.RowGroups[g];
            int ordinal = group.Ordinal ?? g;
            for (int c = 0; c < group.Columns.Length; c++)
            {
                ColumnChunk chunk = group.Columns[c];
                if (chunk.EncryptedColumnMetadata is not { } encrypted || chunk.CryptoMetaData is not { } crypto || KeyFor(crypto) is not { } key) continue;
                CheckPrefix();
                using var gcm = new AesGcm(key, TagLength);
                Aad(aad, ParquetModule.ColumnMetaData, ordinal, c, -1);
                string name = NameOf(crypto, chunk);
                int length = ModuleCipher.GcmPlainLength(encrypted, $"the metadata of column '{name}'");
                plain.EnsureCapacity(Math.Max(length, 1));
                if (!ModuleCipher.TryGcm(gcm, encrypted, aad, plain.Span))
                    throw new ParquetFormatException($"The key given for column '{name}' does not open its metadata: a wrong key, or a damaged file.");
                var reader = new ThriftCompactReader(plain.Span[..length]);
                group.Columns[c] = new ColumnChunk
                {
                    FilePath = chunk.FilePath,
                    FileOffset = chunk.FileOffset,
                    MetaData = ColumnMetaData.Read(ref reader),
                    OffsetIndexOffset = chunk.OffsetIndexOffset,
                    OffsetIndexLength = chunk.OffsetIndexLength,
                    ColumnIndexOffset = chunk.ColumnIndexOffset,
                    ColumnIndexLength = chunk.ColumnIndexLength,
                    CryptoMetaData = crypto,
                    EncryptedColumnMetadata = encrypted,
                };
            }
        }
    }

    /// <summary>
    /// The decryptor of a column chunk's modules (pages, indexes, filter), for one reader; null when the chunk is not
    /// encrypted.
    /// </summary>
    /// <exception cref="ParquetFormatException">The chunk is encrypted with a key that was not given.</exception>
    public ColumnDecryptor? ForChunk(ColumnChunk chunk, int rowGroupOrdinal, int column, string name)
    {
        if (chunk.CryptoMetaData is not { } crypto) return null;
        byte[] key = KeyFor(crypto) ?? throw new ParquetFormatException(crypto.WithFooterKey || _footerKey is null && _columnKeys is null
            ? $"Column '{name}' is encrypted (Parquet modular encryption): open the file with its keys (ParquetDecryptionOptions)."
            : $"Column '{name}' is encrypted with a key of its own: give it in ParquetDecryptionOptions.ColumnKeys (by its dot-separated path).");
        CheckPrefix();
        return new ColumnDecryptor(key, Ctr, _fileAad, rowGroupOrdinal, column, name);
    }

    /// <summary>The decryptor of a chunk's modules when its key was given; null when it is not encrypted or the key is missing.</summary>
    public ColumnDecryptor? TryForChunk(ColumnChunk chunk, int rowGroupOrdinal, int column, out bool missing)
    {
        missing = false;
        if (chunk.CryptoMetaData is not { } crypto) return null;
        if (KeyFor(crypto) is not { } key || _prefixMissing)
        {
            missing = true;
            return null;
        }
        return new ColumnDecryptor(key, Ctr, _fileAad, rowGroupOrdinal, column, NameOf(crypto, chunk));
    }

    private byte[]? KeyFor(ColumnCryptoMetaData crypto) =>
        crypto.WithFooterKey ? _footerKey
        : crypto.Path is { } path && _columnKeys is { } keys && keys.TryGetValue(string.Join('.', path), out byte[]? key) ? key
        : null;

    private static string NameOf(ColumnCryptoMetaData crypto, ColumnChunk chunk) =>
        string.Join('.', crypto.Path ?? chunk.MetaData?.PathInSchema ?? ["?"]);

    private void CheckPrefix()
    {
        if (_prefixMissing)
            throw new ParquetFormatException("The Parquet file was encrypted with an AAD prefix it does not store: give it in ParquetDecryptionOptions.AadPrefix.");
    }

    /// <summary>Writes the AAD of a module (the file's AAD, the module type, then the ordinals that are not negative) into <paramref name="aad"/>.</summary>
    internal void Aad(Span<byte> aad, ParquetModule type, int rowGroup, int column, int page) => ModuleCipher.Aad(aad, _fileAad, type, rowGroup, column, page);
}

/// <summary>
/// The modules of one column chunk, decrypted for one reader: pages with AES-GCM or AES-CTR (by the file's
/// algorithm), the other modules with AES-GCM; each module's AAD names its type and ordinals.
/// </summary>
internal sealed class ColumnDecryptor : IDisposable
{
    private readonly AesGcm _gcm;
    private readonly AesCtr? _ctr;
    private readonly byte[] _fileAad;
    private readonly int _rowGroup, _column;

    public ColumnDecryptor(byte[] key, bool ctr, byte[] fileAad, int rowGroup, int column, string name)
    {
        if (rowGroup > short.MaxValue || column > short.MaxValue)
            throw new ParquetFormatException($"Column '{name}': encrypted modules beyond row group or column {short.MaxValue} cannot be named in an AAD.");
        _gcm = new AesGcm(key, FileDecryptor.TagLength);
        _ctr = ctr ? new AesCtr(key) : null;
        _fileAad = fileAad;
        _rowGroup = rowGroup;
        _column = column;
        Name = name;
    }

    /// <summary>The column's dot-separated path, for errors.</summary>
    public string Name { get; }

    /// <summary>Whether the module is a page body encrypted with AES-CTR.</summary>
    private bool IsCtr(ParquetModule type) => _ctr is not null && type is ParquetModule.DataPage or ParquetModule.DictionaryPage;

    /// <summary>The length of the plaintext of <paramref name="module"/> (its length, nonce, ciphertext and tag), checked against its bytes.</summary>
    public int PlainLength(ParquetModule type, ReadOnlySpan<byte> module) => IsCtr(type)
        ? ModuleCipher.CtrPlainLength(module, $"a page of column '{Name}'")
        : ModuleCipher.GcmPlainLength(module, $"a module of column '{Name}'");

    /// <summary>Decrypts a module into <paramref name="plain"/> (<see cref="PlainLength"/> bytes); false when it does not authenticate.</summary>
    /// <param name="page">The data page's ordinal in the chunk (data pages and their headers), else negative.</param>
    public bool TryDecrypt(ParquetModule type, int page, ReadOnlySpan<byte> module, Span<byte> plain)
    {
        if (IsCtr(type))
        {
            ModuleCipher.Ctr(_ctr!, module, plain);
            return true;
        }
        if (page > short.MaxValue) throw new ParquetFormatException($"Column '{Name}' has more than {short.MaxValue + 1} pages in a chunk, which encrypted files cannot.");
        Span<byte> aad = stackalloc byte[_fileAad.Length + 7];
        int length = ModuleCipher.Aad(aad, _fileAad, type, _rowGroup, _column, type is ParquetModule.DataPage or ParquetModule.DataPageHeader ? page : -1);
        return ModuleCipher.TryGcm(_gcm, module, aad[..length], plain);
    }

    /// <summary>Decrypts a module into a new array.</summary>
    /// <exception cref="ParquetFormatException">The module is damaged or does not authenticate (a wrong key).</exception>
    public byte[] Decrypt(ParquetModule type, int page, ReadOnlySpan<byte> module)
    {
        byte[] plain = new byte[PlainLength(type, module)];
        if (!TryDecrypt(type, page, module, plain)) throw WrongKey();
        return plain;
    }

    /// <summary>The error for a module that does not authenticate.</summary>
    public ParquetFormatException WrongKey() =>
        new($"The key given for column '{Name}' does not open its data: a wrong key, or a damaged file.");

    public void Dispose()
    {
        _gcm.Dispose();
        _ctr?.Dispose();
    }
}

/// <summary>The serialization of encrypted modules: a 4-byte little-endian length, then the nonce, the ciphertext and (GCM) the tag.</summary>
internal static class ModuleCipher
{
    private const int GcmOverhead = FileDecryptor.NonceLength + FileDecryptor.TagLength;

    /// <summary>The bytes a module takes in the file (its length included), from its first 4 bytes; checked against <paramref name="available"/>.</summary>
    public static int StoredLength(ReadOnlySpan<byte> start, long available, string what)
    {
        if (start.Length < 4 || available < 4) throw new ParquetFormatException($"The encrypted module of {what} is cut short.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(start);
        if (length < FileDecryptor.NonceLength || length > available - 4)
            throw new ParquetFormatException($"The encrypted module of {what} gives a length of {length} bytes, beyond the {available - 4} there are.");
        return length + 4;
    }

    public static int GcmPlainLength(ReadOnlySpan<byte> module, string what)
    {
        int stored = StoredLength(module, module.Length, what);
        if (stored - 4 < GcmOverhead) throw new ParquetFormatException($"The encrypted module of {what} is shorter than its nonce and tag.");
        return stored - 4 - GcmOverhead;
    }

    public static int CtrPlainLength(ReadOnlySpan<byte> module, string what) => StoredLength(module, module.Length, what) - 4 - FileDecryptor.NonceLength;

    /// <summary>Decrypts and authenticates an AES-GCM module; false when the tag does not match.</summary>
    public static bool TryGcm(AesGcm gcm, ReadOnlySpan<byte> module, ReadOnlySpan<byte> aad, Span<byte> plain)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(module);
        ReadOnlySpan<byte> body = module.Slice(4, length);
        int text = length - GcmOverhead;
        try
        {
            gcm.Decrypt(body[..FileDecryptor.NonceLength], body.Slice(FileDecryptor.NonceLength, text), body[^FileDecryptor.TagLength..], plain[..text], aad);
            return true;
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

    /// <summary>Decrypts an AES-CTR page: the counter starts at the nonce followed by a 32-bit 1.</summary>
    public static void Ctr(AesCtr ctr, ReadOnlySpan<byte> module, Span<byte> plain)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(module);
        ReadOnlySpan<byte> body = module.Slice(4, length);
        Span<byte> counter = stackalloc byte[16];
        body[..FileDecryptor.NonceLength].CopyTo(counter);
        BinaryPrimitives.WriteInt32BigEndian(counter[FileDecryptor.NonceLength..], 1);
        ctr.Transform(counter, body[FileDecryptor.NonceLength..], plain);
    }

    /// <summary>Writes a module's AAD into <paramref name="aad"/>; returns its length.</summary>
    public static int Aad(Span<byte> aad, ReadOnlySpan<byte> fileAad, ParquetModule type, int rowGroup, int column, int page)
    {
        fileAad.CopyTo(aad);
        int at = fileAad.Length;
        aad[at++] = (byte)type;
        foreach (int ordinal in (ReadOnlySpan<int>)[rowGroup, column, page])
        {
            if (ordinal < 0) continue;
            BinaryPrimitives.WriteInt16LittleEndian(aad[at..], (short)ordinal);
            at += 2;
        }
        return at;
    }
}
