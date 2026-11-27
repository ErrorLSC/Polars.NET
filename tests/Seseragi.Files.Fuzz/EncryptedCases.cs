using Apache.Arrow;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Seseragi.Files.Tests;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// The Parquet modular encryption cases (corpus TestData/parquet/encrypted: pyarrow and Polars files of every mode,
/// and their plain twin). Three in four take a fixture, damage it (in part where its modules start: lengths, nonces,
/// tags, ciphertext, the crypto metadata and the footer module) and read it with its keys (one in eight with a wrong
/// one) and random options: threads (pages read ahead), columns, rows, filters, checksums. AES-GCM modules must
/// refuse any damage; AES-CTR pages are not authenticated, so damage there reaches the decoders as garbage. The fourth
/// writes the plain twin's rows with random encryption (algorithm, key sizes, column keys, plaintext footer, AAD
/// stored or supplied) and random write options, reads them back with the keys (every row must come back) and with a
/// wrong footer key (refused). Contract: read to the end or refused with <see cref="ParquetFormatException"/>.
/// </summary>
internal sealed class EncryptedCases(IReadOnlyList<string> corpus) : IFuzzCases
{
    private static readonly byte[] K16 = "0123456789012345"u8.ToArray();
    private static readonly byte[] K24 = "012345678901234567890123"u8.ToArray();
    private static readonly byte[] K32 = "01234567890123456789012345678901"u8.ToArray();
    private static readonly byte[] Aad = "table-7"u8.ToArray();
    private static readonly int[] BatchSizes = [1, 7, 100, 1000, 65_536];

    private readonly Dictionary<string, (byte[] Bytes, long[] Hot, int Footer)> _files = [];
    private (Apache.Arrow.Schema Schema, List<RecordBatch> Batches, List<string> Rows)? _plain;

    public string Extension => ".parquet";

    public bool HasFile(long n) => Cases.IsFileCase(n);

    public Outcome Run(long seed, long n)
    {
        Random random = Cases.RandomFor(seed, n);
        try
        {
            if (!Cases.IsFileCase(n)) return RoundTrip(random);
            byte[] bytes = Mutate(random, out string source);
            return Read(bytes, random.Next(8) == 0 ? WrongKeys(random) : KeysOf(source), random);
        }
        catch (Exception e)
        {
            return Outcome.From(e);
        }
    }

    public byte[] FileOf(long seed, long n, out string source) => Mutate(Cases.RandomFor(seed, n), out source);

    /// <summary>The keys of a fixture (see TestData/parquet/writers/write_encrypted.py).</summary>
    private static ParquetDecryptionOptions KeysOf(string path)
    {
        string name = Path.GetFileName(path);
        return name switch
        {
            _ when name.StartsWith("pl_", StringComparison.Ordinal) => new(K16)
            {
                ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = "1234567890123450"u8.ToArray(), ["s.a"] = "1234567890123451"u8.ToArray() },
            },
            _ when name.Contains("192") => new(K24),
            _ when name.Contains("256") => new(K32),
            _ when name.Contains("aad_supplied") => new(K16) { AadPrefix = Aad },
            _ => new(K16),
        };
    }

    private static ParquetDecryptionOptions WrongKeys(Random random) => random.Next(3) switch
    {
        0 => new(K24),
        1 => new(K16) { AadPrefix = "other"u8.ToArray() },
        _ => new(K16) { ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = K16 } },
    };

    private byte[] Mutate(Random random, out string source)
    {
        source = corpus[random.Next(corpus.Count)];
        var (original, hot, footer) = Load(source);
        return Mutations.Apply(random, original, hot, footer, r => Load(corpus[r.Next(corpus.Count)]).Bytes);
    }

    /// <summary>A fixture's bytes, where its chunks' modules start (read with its keys), and its footer's length.</summary>
    private (byte[] Bytes, long[] Hot, int Footer) Load(string path)
    {
        if (_files.TryGetValue(path, out var known)) return known;
        byte[] bytes = File.ReadAllBytes(path);
        int footer = Math.Clamp(BitConverter.ToInt32(bytes, bytes.Length - 8), 0, bytes.Length - 12);
        List<long> hot = [];
        using (var file = ParquetFile.Open(bytes, KeysOf(path)))
        {
            foreach (RowGroup group in file.Thrift.RowGroups)
            {
                foreach (ColumnChunk chunk in group.Columns)
                {
                    if (chunk.MetaData is not { } meta) continue;
                    hot.Add(meta.DataPageOffset);
                    if (meta.DictionaryPageOffset is long dictionary) hot.Add(dictionary);
                    if (chunk.ColumnIndexOffset is long columnIndex) hot.Add(columnIndex);
                    if (chunk.OffsetIndexOffset is long offsetIndex) hot.Add(offsetIndex);
                    if (meta.BloomFilterOffset is long bloom) hot.Add(bloom);
                }
            }
        }
        return _files[path] = (bytes, [.. hot], footer);
    }

    private static Outcome Read(byte[] bytes, ParquetDecryptionOptions keys, Random random)
    {
        try
        {
            using var file = ParquetFile.Open(bytes, keys);
            string[] names = [.. file.Schema.FieldsList.Select(f => f.Name)];
            long rows = file.Metadata.RowGroups.Sum(g => Math.Max(0, g.RowCount));
            var options = new ParquetReadOptions
            {
                MaxDegreeOfParallelism = random.Next(3) == 0 ? 1 : 1 + random.Next(8),
                ViewTypes = random.Next(2) == 0,
                BatchSize = BatchSizes[random.Next(BatchSizes.Length)],
                VerifyChecksums = random.Next(2) == 0,
                LevelsAsBits = random.Next(4) != 0,
                Columns = random.Next(3) == 0 && names.Length > 1 ? [.. names.Where(_ => random.Next(2) == 0)] : null,
                Rows = random.Next(4) == 0 && rows > 0 ? RowRanges.Of([(random.NextInt64(rows), 1 + random.NextInt64(rows))]).Intersect(RowRanges.All(rows)) : null,
                Filter = random.Next(4) == 0 ? Cases.RandomFilter(random, file.Schema) : null,
                FilterRows = random.Next(2) == 0,
                BloomFilters = random.Next(2) == 0,
                MemoryMap = ParquetMemoryMap.Off,
            };
            using var reader = file.OpenReader(options);
            foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            return Outcome.Read;
        }
        catch (ParquetFormatException e)
        {
            return Outcome.Refused(e);
        }
    }

    /// <summary>The plain twin's rows, written encrypted with random options and read back.</summary>
    private Outcome RoundTrip(Random random)
    {
        var (schema, batches, expected) = _plain ??= Plain();
        byte[] footerKey = random.Next(3) switch { 0 => K16, 1 => K24, _ => K32 };
        bool columnKeys = random.Next(3) == 0, plaintextFooter = random.Next(3) == 0;
        int aad = random.Next(3); // none, stored, supplied
        var encryption = new ParquetEncryptionOptions(footerKey)
        {
            Algorithm = random.Next(2) == 0 ? ParquetEncryptionAlgorithm.AesGcm : ParquetEncryptionAlgorithm.AesGcmCtr,
            ColumnKeys = columnKeys ? new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = K16, ["s.a"] = K32 } : null,
            PlaintextFooter = plaintextFooter,
            AadPrefix = aad == 0 ? null : Aad,
            StoreAadPrefix = aad == 1,
        };
        var write = new ParquetWriteOptions
        {
            Encryption = encryption,
            Compression = random.Next(3) switch { 0 => NoCompression.Instance, 1 => SnappyCompression.Instance, _ => new ZstandardCompression() },
            DataPageVersion = random.Next(2) == 0 ? ParquetPageVersion.V1 : ParquetPageVersion.V2,
            RowGroupRows = 1 + random.Next(1500),
            DataPageRows = 1 + random.Next(300),
            WriteChecksums = random.Next(2) == 0,
            PageIndex = random.Next(2) == 0,
            MaxDegreeOfParallelism = random.Next(3) == 0 ? 1 : 1 + random.Next(8),
            Columns = random.Next(2) == 0 ? new Dictionary<string, ParquetColumnOptions> { ["str"] = new() { BloomFilter = new() } } : null,
            LeaveOpen = true,
        };
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, batches, write);
        byte[] bytes = stream.ToArray();
        var keys = new ParquetDecryptionOptions(footerKey)
        {
            ColumnKeys = encryption.ColumnKeys,
            AadPrefix = aad == 2 ? Aad : null,
        };
        var read = new ParquetReadOptions
        {
            MaxDegreeOfParallelism = random.Next(3) == 0 ? 1 : 1 + random.Next(8),
            VerifyChecksums = write.WriteChecksums,
            BatchSize = BatchSizes[random.Next(BatchSizes.Length)],
        };
        string what = $"{encryption}, aad {aad}, {write.Compression}, {write.DataPageVersion}, groups {write.RowGroupRows}, pages {write.DataPageRows}, threads {write.MaxDegreeOfParallelism}/{read.MaxDegreeOfParallelism}";
        using (var file = ParquetFile.Open(bytes, keys))
        using (var reader = file.OpenReader(read))
        {
            if (!expected.SequenceEqual(ArrowRender.Rows(reader.ReadBatches())))
                return new Outcome(true, "Encrypted rows do not come back", what);
        }
        // A wrong footer key: refused when the footer is encrypted or the columns use it; a plaintext footer with
        // column keys opens and reads its plain columns, refusing the encrypted ones.
        try
        {
            using var file = ParquetFile.Open(bytes, new ParquetDecryptionOptions(footerKey == K16 ? K24 : K16) { ColumnKeys = keys.ColumnKeys, AadPrefix = keys.AadPrefix });
            using var reader = file.OpenReader(read);
            foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            return new Outcome(true, "A wrong footer key reads an encrypted file", what);
        }
        catch (ParquetFormatException)
        {
            return Outcome.Read;
        }
    }

    private (Apache.Arrow.Schema, List<RecordBatch>, List<string>) Plain()
    {
        string path = corpus.First(f => Path.GetFileName(f) == "plain.parquet");
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(new ParquetReadOptions { ViewTypes = false });
        List<RecordBatch> batches = [.. reader.ReadBatches()];
        using var again = file.OpenReader(new ParquetReadOptions { ViewTypes = false });
        return (reader.Schema, batches, ArrowRender.Rows(again.ReadBatches()));
    }
}
