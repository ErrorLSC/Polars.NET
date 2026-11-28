using System.Buffers;
using System.IO.Hashing;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Encryption;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>
/// A data page on its way: its levels (<see cref="Raw"/>; format 2 keeps them, the first <see cref="LevelsLength"/>
/// bytes, uncompressed), its values (<see cref="Values"/>: prepared, then encoded after the levels as the page is
/// compressed, unless encoded as it was laid out) and its header without sizes and checksum. Compressing it
/// (<see cref="ClaimedWork.Run"/>) depends on nothing else once laid out, so any thread may: the column writer claims a
/// page it needs and has not seen taken, or waits for the helper that took it. Bytes come out the same whichever thread
/// ran it. A page of an encrypted column is sealed as it is compressed (Parquet modular encryption): its body becomes
/// a module in place, its header a module of its own.
/// </summary>
internal sealed class PageJob(PageSettings settings) : ClaimedWork
{
    public NativeBufferWriter Raw { get; } = new(1 << 16);
    public int LevelsLength { get; set; }
    /// <summary>The page's header without sizes and checksum (set once its values are encoded).</summary>
    public PageHeader Header { get; set; } = null!;
    /// <summary>The page as the page index describes it (its place in the chunk set once committed).</summary>
    public required PageRecord Record { get; set; }

    /// <summary>An encrypted column's page: the encryptor that seals it (the job disposes it); null for a plain page.</summary>
    public ColumnEncryptor? Seal { get; init; }

    /// <summary>The data page's ordinal in its chunk (named in the AAD of its modules).</summary>
    public int Ordinal { get; init; }

    /// <summary>The upper bound of the compressed page in the file (header included), before it is compressed.</summary>
    public long Bound { get; set; }

    /// <summary>
    /// The page's values, encoded after its levels as it is compressed (as the chunk dictionary's indices of
    /// <see cref="BitWidth"/> bits, or plainly when that is -1), then given back to the column writer
    /// (<see cref="Return"/>); null once encoded, or when they were encoded as the page was laid out.
    /// </summary>
    public PageValues? Values { get; set; }

    public int BitWidth { get; set; } = -1;

    /// <summary>Lets the page's values go once they are encoded (any thread).</summary>
    public Action<PageValues>? Return { get; init; }

    /// <summary>The values' work before the writer lays the page out (mapping them to a dictionary of their own, bounds).</summary>
    public PagePreparation? Preparation { get; set; }

    /// <summary>Whether the values are the column writer's open page, encoded as the page is laid out (never handed on).</summary>
    public bool Inline { get; init; }

    /// <summary>Whether this page judges the chunk's dictionary (the first holding values).</summary>
    public bool Judges { get; init; }

    /// <summary>Whether a page judged the chunk's dictionary before this one.</summary>
    public bool AfterJudging { get; init; }

    /// <summary>The bytes the page may add to the chunk's dictionary until it is laid out (its values plainly: at most).</summary>
    public long DictionaryBound { get; set; }

    /// <summary>The page's levels and rows, for its header.</summary>
    public (int Levels, int Nulls, int Rows, int DefinitionBytes, int RepetitionBytes) Shape { get; set; }

    public byte[] HeaderBytes { get; private set; } = [];

    /// <summary>The compressed page (levels first in format 2), in native memory until <see cref="Release"/>.</summary>
    public NativeBufferWriter Body { get; } = new(1 << 16);

    public int BodyLength => Body.WrittenCount;
    public int Uncompressed { get; private set; }

    /// <summary>Compresses (and seals) the page and checksums it; its raw buffer goes back to the pool.</summary>
    protected override void Execute() => Compress();

    protected override void Finally() => Seal?.Dispose();

    private void Compress()
    {
        if (Values is { } pageValues)
        {
            if (BitWidth >= 0) pageValues.EncodeIndices(BitWidth, Raw);
            else pageValues.EncodePlain(Raw);
            Values = null;
            Return?.Invoke(pageValues);
        }
        ReadOnlySpan<byte> raw = Raw.WrittenSpan;
        bool v1 = Header.Type == PageType.DataPage;
        int levels = v1 ? 0 : LevelsLength;
        // Format 1 compresses levels and values together (an uncompressed codec copies them); format 2 only the
        // values, and only with a codec.
        bool compressed = v1 || settings.Codec != CompressionCodec.Uncompressed;
        ReadOnlySpan<byte> values = raw[levels..];
        // A sealed page is compressed after room for its module's length and nonce, and before room for its tag.
        int prefix = Seal is null ? 0 : ModuleSeal.Prefix, suffix = Seal is null ? 0 : ModuleSeal.Suffix;
        Span<byte> stored = Body.GetSpan(prefix + levels + (compressed ? PageCodecs.MaxCompressedLength(settings.Codec, values.Length) : values.Length) + suffix);
        Span<byte> body = stored[prefix..];
        raw[..levels].CopyTo(body);
        int length = levels + (compressed ? PageCodecs.Compress(settings.Codec, values, body[levels..], settings.Level) : Copy(values, body[levels..]));
        int storedLength = length;
        if (Seal is { } seal)
        {
            storedLength = seal.ModuleLength(ParquetModule.DataPage, length);
            seal.Encrypt(ParquetModule.DataPage, Ordinal, body[..length], stored[..storedLength]); // in place
        }
        Body.Advance(storedLength);
        PageHeader header = new()
        {
            Type = Header.Type,
            UncompressedPageSize = raw.Length,
            CompressedPageSize = storedLength,
            // An encrypted page's checksum covers its module, as parquet-mr and Arrow write it.
            Crc = settings.Checksums ? (int)Crc32.HashToUInt32(stored[..storedLength]) : null,
            DataPageHeader = Header.DataPageHeader,
            DataPageHeaderV2 = Header.DataPageHeaderV2,
        };
        ThriftCompactWriter thrift = ThriftCompactWriter.ThreadScratch();
        header.Write(thrift);
        HeaderBytes = Seal is null ? thrift.ToArray() : Seal.Encrypt(ParquetModule.DataPageHeader, Ordinal, thrift.WrittenSpan);
        Uncompressed = raw.Length;
        Raw.Dispose();

        static int Copy(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
    }

    /// <summary>Returns the compressed body to the pool (after the page is in the file).</summary>
    public void Release()
    {
        // Waited for when a helper runs them (they use the values and buffers), never run when none took them.
        Preparation?.Drop();
        Drop();
        Values?.Dispose();
        Values = null;
        Body.Dispose();
        Raw.Dispose();
        Seal?.Dispose();
    }
}
