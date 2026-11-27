using System.Buffers.Binary;
using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.Parquet.ParquetPruningTests;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetWriter"/> with <see cref="ParquetWriteOptions.Encryption"/>: every mode (uniform or by column,
/// GCM or GCM-CTR, encrypted or plaintext footer, AAD prefix stored or not, 16/24/32-byte keys) read back by our reader
/// through every path (rows, filters, Bloom filters, offset index), datasets, and the opt-in interop with pyarrow and
/// Polars 2.2 (TestData/parquet/encrypted_interop.py, with SESERAGI_PARQUET_ENCRYPTION_PYTHON set to a Python with
/// pyarrow 26 and Polars 2.2, e.g. ~/.cache/seseragi/polars22-venv/bin/python).
/// </summary>
public sealed class ParquetEncryptionWriterTests : IDisposable
{
    private static readonly byte[] K16 = "0123456789012345"u8.ToArray();
    private static readonly byte[] K24 = "012345678901234567890123"u8.ToArray();
    private static readonly byte[] K32 = "01234567890123456789012345678901"u8.ToArray();
    private static readonly byte[] ColI64 = "1234567890123450"u8.ToArray();
    private static readonly byte[] ColSa = "1234567890123451"u8.ToArray();
    private static readonly byte[] Aad = "table-7"u8.ToArray();

    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-encryption").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    /// <summary>A mode of encryption: how it is written, and the keys that read it.</summary>
    public sealed record Mode(string Name, ParquetEncryptionOptions Encryption, ParquetDecryptionOptions Decryption, bool PagesV2 = false)
    {
        public override string ToString() => Name;
    }

    private static Dictionary<string, ReadOnlyMemory<byte>> ColumnKeys() => new() { ["i64"] = ColI64, ["s.a"] = ColSa };

    private static readonly Mode[] AllModes =
    [
        new("uniform_gcm", new(K16), new(K16)),
        new("uniform_gcm_pages_v2", new(K16), new(K16), PagesV2: true),
        new("columns_gcm_plaintext_footer", new(K16) { ColumnKeys = ColumnKeys(), PlaintextFooter = true }, new(K16) { ColumnKeys = ColumnKeys() }),
        new("uniform_ctr_256", new(K32) { Algorithm = ParquetEncryptionAlgorithm.AesGcmCtr }, new(K32)),
        new("uniform_gcm_192", new(K24), new(K24)),
        new("columns_gcm", new(K16) { ColumnKeys = ColumnKeys() }, new(K16) { ColumnKeys = ColumnKeys() }),
        new("columns_ctr_plaintext_footer", new(K16) { ColumnKeys = ColumnKeys(), PlaintextFooter = true, Algorithm = ParquetEncryptionAlgorithm.AesGcmCtr },
            new(K16) { ColumnKeys = ColumnKeys() }),
        new("uniform_plaintext_footer_aad_stored", new(K16) { PlaintextFooter = true, AadPrefix = Aad, StoreAadPrefix = true }, new(K16)),
        new("uniform_aad_supplied", new(K32) { AadPrefix = Aad }, new(K32) { AadPrefix = Aad }),
    ];

    public static TheoryData<Mode> Modes => [.. AllModes];

    /// <summary>The rows of the plain fixture (nested and flat types, nulls), as batches.</summary>
    private static List<RecordBatch> PlainBatches(out Apache.Arrow.Schema schema)
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(Path.Combine("encrypted", "plain.parquet")));
        using var reader = file.OpenReader();
        schema = reader.Schema;
        return [.. reader.ReadBatches()];
    }

    private static ParquetWriteOptions Options(ParquetEncryptionOptions? encryption, int threads = 4, bool pagesV2 = false) => new()
    {
        DataPageVersion = pagesV2 ? ParquetPageVersion.V2 : ParquetPageVersion.V1,
        RowGroupRows = 400,
        DataPageRows = 64,
        WriteChecksums = true,
        Encryption = encryption,
        MaxDegreeOfParallelism = threads,
        Columns = new Dictionary<string, ParquetColumnOptions> { ["str"] = new() { BloomFilter = new() } },
    };

    private static byte[] Write(ParquetWriteOptions options)
    {
        List<RecordBatch> batches = PlainBatches(out Apache.Arrow.Schema schema);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, options))
        {
            foreach (RecordBatch batch in batches) writer.Write(batch);
            writer.Complete();
        }
        foreach (RecordBatch batch in batches) batch.Dispose();
        return stream.ToArray();
    }

    private static List<string> Read(byte[] file, ParquetDecryptionOptions? keys, ParquetReadOptions? options = null)
    {
        using var parquet = ParquetFile.Open(file, keys);
        using var reader = parquet.OpenReader(options);
        return ArrowRender.Rows(reader.ReadBatches());
    }

    private static int PagesSkipped(byte[] file, ParquetDecryptionOptions? keys, ParquetReadOptions options)
    {
        using var parquet = ParquetFile.Open(file, keys);
        using var reader = parquet.OpenReader(options);
        foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
        Assert.True(reader.PagesSkipped > 0);
        return reader.PagesSkipped;
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Every_Mode_Reads_Back_As_Written(Mode mode)
    {
        byte[] plain = Write(Options(null));
        List<string> expected = Read(plain, null);
        Assert.Equal(1200, expected.Count);
        foreach (int threads in (int[])[1, 4])
        {
            byte[] file = Write(Options(mode.Encryption, threads, mode.PagesV2));
            Assert.Equal(expected, Read(file, mode.Decryption));
            Assert.Equal(expected, Read(file, mode.Decryption, new ParquetReadOptions { MaxDegreeOfParallelism = 1, VerifyChecksums = true }));
            // Rows by the offset index, filters by statistics and rows, Bloom filters.
            var ranges = new ParquetReadOptions { Rows = RowRanges.Of([(3, 70), (500, 150), (1190, 10)]) };
            Assert.Equal(Read(plain, null, ranges), Read(file, mode.Decryption, ranges));
            // The offset index points at the sealed pages: they are skipped as the plain file's are (an index that does
            // not fit its chunk would be dropped, and every page read).
            Assert.Equal(PagesSkipped(plain, null, ranges), PagesSkipped(file, mode.Decryption, ranges));
            var filtered = new ParquetReadOptions { Filter = Bin(BinaryOp.Lt, Col("i32"), Lit(LiteralValue.NewInt32(-1450))), FilterRows = true };
            Assert.Equal(Read(plain, null, filtered), Read(file, mode.Decryption, filtered));
            using var parquet = ParquetFile.Open(file, mode.Decryption);
            using (var reader = parquet.OpenReader(filtered)) Assert.Equal([0], reader.RowGroupsRead);
            ScalarExpr absent = Bin(BinaryOp.Eq, Col("str"), Lit(LiteralValue.NewString("value-1000")));
            using (var reader = parquet.OpenReader(new ParquetReadOptions { Filter = absent })) Assert.Empty(reader.RowGroupsRead);
            using (var reader = parquet.OpenReader(new ParquetReadOptions { Filter = absent, BloomFilters = false })) Assert.Equal([0, 1, 2], reader.RowGroupsRead);
        }
    }

    /// <summary>
    /// Row groups encoded side by side from batches the writer owns are sealed in parallel for their own ordinals (in
    /// every module's AAD): a row group sealed as another's does not authenticate.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void Row_Groups_Encoded_Side_By_Side_Are_Sealed_For_Their_Ordinals(Mode mode)
    {
        List<string> expected = Read(Write(Options(null)), null);
        foreach (int threads in (int[])[1, 4])
        {
            List<RecordBatch> batches = PlainBatches(out Apache.Arrow.Schema schema);
            using var stream = new MemoryStream();
            using (var writer = ParquetWriter.Create(stream, schema, Options(mode.Encryption, threads, mode.PagesV2) with { BatchOwnership = BatchOwnership.Writer }))
            {
                foreach (RecordBatch batch in batches) writer.Write(batch); // the writer disposes them
                writer.Complete();
            }
            byte[] file = stream.ToArray();
            using (var parquet = ParquetFile.Open(file, mode.Decryption)) Assert.Equal(3, parquet.Metadata.RowGroups.Count);
            Assert.Equal(expected, Read(file, mode.Decryption, new ParquetReadOptions { VerifyChecksums = true }));
        }
    }

    /// <summary>
    /// The checksum of an encrypted page covers its module (as parquet-mr and Arrow write it): a damaged page fails its
    /// checksum before it is decrypted; written without checksums, AES-GCM catches the damage instead.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Encrypted_Pages_Are_Checksummed_Over_Their_Modules(bool checksums)
    {
        byte[] file = Write(Options(new(K16)) with { WriteChecksums = checksums, Dictionary = false });
        // The first chunk follows the magic: its first data page's header module, then its body module.
        int header = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(4));
        file[4 + 4 + header + 4 + 12 + 3] ^= 0x10; // a byte of the body's ciphertext
        var error = Assert.Throws<ParquetFormatException>(() => Read(file, new(K16), new ParquetReadOptions { VerifyChecksums = true }));
        Assert.Contains(checksums ? "fails its CRC check" : "does not open its data", error.Message);
    }

    [Fact]
    public void Files_Are_Laid_Out_By_Mode_And_Hide_Their_Values()
    {
        byte[] encryptedFooter = Write(Options(new(K16)));
        Assert.True(encryptedFooter.AsSpan(0, 4).SequenceEqual("PARE"u8) && encryptedFooter.AsSpan()[^4..].SequenceEqual("PARE"u8));
        byte[] plaintextFooter = Write(Options(new(K16) { PlaintextFooter = true }));
        Assert.True(plaintextFooter.AsSpan(0, 4).SequenceEqual("PAR1"u8) && plaintextFooter.AsSpan()[^4..].SequenceEqual("PAR1"u8));
        // Uniformly encrypted, no value is in plain anywhere (the strings repeat across the file).
        byte[] value = "value-42"u8.ToArray();
        Assert.True(Write(Options(null)).AsSpan().IndexOf(value) >= 0);
        Assert.True(encryptedFooter.AsSpan().IndexOf(value) < 0);
        Assert.True(plaintextFooter.AsSpan().IndexOf(value) < 0);
        // Each file gets its own file id and nonces.
        Assert.False(encryptedFooter.AsSpan().SequenceEqual(Write(Options(new(K16)))));
    }

    [Fact]
    public void A_Plaintext_Footer_Shows_The_Plain_Columns_And_Hides_The_Statistics_Of_Encrypted_Ones()
    {
        byte[] file = Write(Options(new(K16) { ColumnKeys = ColumnKeys(), PlaintextFooter = true }));
        using (var keyless = ParquetFile.Open(file))
        {
            Assert.True(keyless.Metadata.IsEncrypted);
            ParquetColumnChunk Chunk(string path) => keyless.Metadata.RowGroups[0].Columns.Single(c => string.Join('.', c.Path) == path);
            Assert.Null(Chunk("i64").Statistics);
            Assert.NotNull(Chunk("i32").Statistics);
            Assert.Equal(Read(Write(Options(null)), null, new ParquetReadOptions { Columns = ["i32", "str"] }),
                Read(file, null, new ParquetReadOptions { Columns = ["i32", "str"] }));
            Assert.Contains("'i64'", Assert.Throws<ParquetFormatException>(() => Read(file, null, new ParquetReadOptions { Columns = ["i64"] })).Message);
        }
        using var keyed = ParquetFile.Open(file, new ParquetDecryptionOptions(K16) { ColumnKeys = ColumnKeys() });
        Assert.NotNull(keyed.Metadata.RowGroups[0].Columns.Single(c => string.Join('.', c.Path) == "i64").Statistics);
    }

    [Fact]
    public void Bad_Options_Are_Refused_Before_Anything_Is_Written()
    {
        var unknown = Assert.Throws<ArgumentException>(() => Write(Options(new(K16) { ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["s.z"] = ColSa } })));
        Assert.Contains("'s.z'", unknown.Message);
        Assert.Contains("'s.a'", unknown.Message); // a path the schema has, as an example
        Assert.Contains("AES keys are 16, 24 or 32 bytes", Assert.Throws<ArgumentException>(() => Write(Options(new(new byte[20])))).Message);
        Assert.Contains("ParquetDecryptionOptions", Assert.Throws<ParquetFormatException>(() => Read(Write(Options(new(K16))), null)).Message);
    }

    [Fact]
    public void Datasets_Write_And_Read_Encrypted_Files()
    {
        List<RecordBatch> batches = PlainBatches(out Apache.Arrow.Schema schema);
        string path = Path.Combine(_dir, "dataset");
        var options = new ParquetDatasetWriteOptions { Writer = Options(new(K16) { ColumnKeys = ColumnKeys() }), PartitionBy = ["b"] };
        using (var writer = ParquetDatasetWriter.Create(path, schema, options))
        {
            foreach (RecordBatch batch in batches) writer.Write(batch);
            writer.Complete();
        }
        foreach (RecordBatch batch in batches) batch.Dispose();
        using var dataset = ParquetDataset.Open(path + "/", new ParquetDatasetOptions { Decryption = new ParquetDecryptionOptions(K16) { ColumnKeys = ColumnKeys() } });
        using var reader = dataset.OpenReader();
        Assert.Equal(1200, reader.ReadBatches().Sum(b => b.Length));
    }

    [Fact]
    [Trait("Category", "Interop")]
    public async Task PyArrow_And_Polars_Read_Our_Encrypted_Files()
    {
        File.WriteAllBytes(Path.Combine(_dir, "plain.parquet"), Write(Options(null)));
        List<object> manifest = [];
        foreach (Mode mode in AllModes)
        {
            File.WriteAllBytes(Path.Combine(_dir, mode.Name + ".parquet"), Write(Options(mode.Encryption, pagesV2: mode.PagesV2)));
            manifest.Add(new Dictionary<string, object?>
            {
                ["file"] = mode.Name + ".parquet",
                ["footer_key"] = Convert.ToHexString(mode.Decryption.FooterKey.Span),
                ["column_keys"] = mode.Decryption.ColumnKeys?.ToDictionary(k => k.Key, k => Convert.ToHexString(k.Value.Span)),
                ["aad_prefix"] = mode.Decryption.AadPrefix is { } aad ? Convert.ToHexString(aad.Span) : null,
                ["ctr"] = mode.Encryption.Algorithm == ParquetEncryptionAlgorithm.AesGcmCtr,
                ["key_bytes"] = mode.Encryption.FooterKey.Length,
            });
        }
        File.WriteAllText(Path.Combine(_dir, "manifest.json"), JsonSerializerManifest(manifest));
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_ENCRYPTION_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        var psi = new ProcessStartInfo(python, ["-I", Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "encrypted_interop.py"), _dir])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(psi)!;
        Task<string> stdout = process.StandardOutput.ReadToEndAsync(), stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        string output = await stdout + await stderr;
        Assert.True(process.ExitCode == 0, output);
        Assert.DoesNotContain("FAIL", output);
        // pyarrow reads every uniform file; Polars every GCM file of 16- or 32-byte keys.
        Assert.Contains("ok pyarrow uniform_ctr_256.parquet", output);
        Assert.Contains("ok pyarrow uniform_aad_supplied.parquet", output);
        Assert.Contains("ok polars columns_gcm.parquet", output);
        Assert.Contains("ok polars uniform_plaintext_footer_aad_stored.parquet", output);
        Assert.Contains("ok polars columns_gcm_plaintext_footer.parquet", output);
        Assert.Contains("ok pyarrow uniform_gcm_pages_v2.parquet", output);
        Assert.Contains("ok polars uniform_gcm_pages_v2.parquet", output);
    }

    /// <summary>The manifest as JSON, written without reflection (Utf8JsonWriter).</summary>
    private static string JsonSerializerManifest(List<object> entries)
    {
        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartArray();
            foreach (Dictionary<string, object?> entry in entries.Cast<Dictionary<string, object?>>())
            {
                w.WriteStartObject();
                foreach (var (key, value) in entry)
                {
                    switch (value)
                    {
                        case null: w.WriteNull(key); break;
                        case string s: w.WriteString(key, s); break;
                        case bool b: w.WriteBoolean(key, b); break;
                        case int n: w.WriteNumber(key, n); break;
                        case Dictionary<string, string> map:
                            w.WriteStartObject(key);
                            foreach (var (k, v) in map) w.WriteString(k, v);
                            w.WriteEndObject();
                            break;
                    }
                }
                w.WriteEndObject();
            }
            w.WriteEndArray();
        }
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }
}
