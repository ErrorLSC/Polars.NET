using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.Parquet.ParquetPruningTests;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The managed reader of files written with Parquet modular encryption: pyarrow and Polars 2.2 files of
/// <c>TestData/parquet/encrypted</c> (each read against its plain twin) and the parquet-testing files (keys in their
/// README), through every way a read can go (row ranges, offset-index skipping, filters, Bloom filters, row groups,
/// one thread or many, datasets, typed rows), and every way a read can fail.
/// </summary>
public sealed class ParquetDecryptionTests
{
    private static readonly byte[] K16 = "0123456789012345"u8.ToArray();
    private static readonly byte[] K24 = "012345678901234567890123"u8.ToArray();
    private static readonly byte[] K32 = "01234567890123456789012345678901"u8.ToArray();
    private static readonly byte[] Aad = "table-7"u8.ToArray();

    private static string Ours(string name) => ParquetFixtures.FullPath(Path.Combine("encrypted", name));

    private static string Apache(string name) => ParquetFixtures.FullPath(Path.Combine("apache", "data", name));

    /// <summary>The keys of our fixtures (see writers/write_encrypted.py).</summary>
    private static ParquetDecryptionOptions OurKeys(string name, bool columnKeys = true) => name switch
    {
        _ when name.StartsWith("pl_", StringComparison.Ordinal) => new(K16)
        {
            ColumnKeys = columnKeys ? new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = "1234567890123450"u8.ToArray(), ["s.a"] = "1234567890123451"u8.ToArray() } : null,
        },
        _ when name.Contains("192") => new(K24),
        _ when name.Contains("256") => new(K32),
        _ when name.Contains("aad_supplied") => new(K16) { AadPrefix = Aad },
        _ => new(K16),
    };

    /// <summary>The keys of the parquet-testing files.</summary>
    private static ParquetDecryptionOptions ApacheKeys(string name, bool columnKeys = true)
    {
        bool aes256 = name.StartsWith("aes256", StringComparison.Ordinal);
        Dictionary<string, ReadOnlyMemory<byte>> keys = aes256
            ? new()
            {
                ["double_field"] = "12345678901234567890123456789012"u8.ToArray(),
                ["float_field"] = "12345678901234567890123456789013"u8.ToArray(),
                ["boolean_field"] = "12345678901234567890123456789014"u8.ToArray(),
                ["int32_field"] = "12345678901234567890123456789015"u8.ToArray(),
                ["ba_field"] = "12345678901234567890123456789016"u8.ToArray(),
                ["flba_field"] = "12345678901234567890123456789017"u8.ToArray(),
                ["int64_field.list.element"] = "12345678901234567890123456789018"u8.ToArray(),
                ["int96_field"] = "12345678901234567890123456789019"u8.ToArray(),
            }
            : new() { ["double_field"] = "1234567890123450"u8.ToArray(), ["float_field"] = "1234567890123451"u8.ToArray() };
        return new ParquetDecryptionOptions(aes256 ? K32 : K16)
        {
            ColumnKeys = columnKeys ? keys : null,
            AadPrefix = name.Contains("aad") ? "tester"u8.ToArray() : null,
        };
    }

    private static List<string> Read(string path, ParquetDecryptionOptions? keys, ParquetReadOptions? options = null)
    {
        using var file = ParquetFile.Open(path, keys);
        using var reader = file.OpenReader(options);
        return ArrowRender.Rows(reader.ReadBatches());
    }

    public static TheoryData<string> OurFiles => [.. Directory.GetFiles(ParquetFixtures.FullPath("encrypted"), "*.parquet")
        .Select(p => Path.GetFileName(p)).Where(n => n != "plain.parquet").Order(StringComparer.Ordinal)];

    public static TheoryData<string> ApacheFiles =>
    [
        "uniform_encryption.parquet.encrypted",
        "encrypt_columns_and_footer.parquet.encrypted",
        "encrypt_columns_plaintext_footer.parquet.encrypted",
        "encrypt_columns_and_footer_ctr.parquet.encrypted",
        "encrypt_columns_plaintext_footer_ctr.parquet.encrypted",
        "encrypt_columns_and_footer_aad.parquet.encrypted",
        "encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted",
        "aes256/uniform_encryption.parquet.encrypted",
        "aes256/encrypt_columns_and_footer.parquet.encrypted",
        "aes256/encrypt_columns_plaintext_footer.parquet.encrypted",
        "aes256/encrypt_columns_and_footer_ctr.parquet.encrypted",
        "aes256/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted",
    ];

    // ---------------------------------------------------------------- reading

    [Theory]
    [MemberData(nameof(OurFiles))]
    public void Our_Files_Read_As_Their_Plain_Twin(string name)
    {
        List<string> plain = Read(Ours("plain.parquet"), null);
        Assert.Equal(1200, plain.Count);
        Assert.Equal(plain, Read(Ours(name), OurKeys(name)));
        // One thread and many, every row group alone, rows in ranges (by the offset index where there is one).
        Assert.Equal(plain, Read(Ours(name), OurKeys(name), new ParquetReadOptions { MaxDegreeOfParallelism = 1 }));
        Assert.Equal(plain[600..], Read(Ours(name), OurKeys(name), new ParquetReadOptions { RowGroups = [1] }));
        var ranges = new ParquetReadOptions { Rows = RowRanges.Of([(5, 7), (590, 30), (1100, 50)]) };
        Assert.Equal(Read(Ours("plain.parquet"), null, ranges), Read(Ours(name), OurKeys(name), ranges));
        // pyarrow wrote page indexes (Polars none): ranges are read by the decrypted offset index, pages before them skipped unread.
        if (name.StartsWith("pl_", StringComparison.Ordinal)) return;
        using var file = ParquetFile.Open(Ours(name), OurKeys(name));
        using var reader = file.OpenReader(new ParquetReadOptions { Rows = RowRanges.Of([(450, 7)]), Columns = ["f64"] }); // pages of at most 100 rows
        Assert.Equal(7, reader.ReadBatches().Sum(b => b.Length));
        Assert.NotNull(file.PageIndexes.Get(0, 2, withColumns: false));
        Assert.True(reader.PagesSkipped > 0);
    }

    [Theory]
    [MemberData(nameof(OurFiles))]
    public void Filters_Prune_And_Filter_Rows_As_On_The_Plain_Twin(string name)
    {
        ScalarExpr filter = Bin(BinaryOp.And, Bin(BinaryOp.Gt, Col("i32"), Lit(LiteralValue.NewInt32(-1000))), Bin(BinaryOp.Lt, Col("i32"), Lit(LiteralValue.NewInt32(-950))));
        foreach (ParquetReadOptions options in (ParquetReadOptions[])
        [
            new() { Filter = filter, FilterRows = true },
            new() { Filter = filter, FilterRows = true, MaxDegreeOfParallelism = 1 },
            new() { Filter = Bin(BinaryOp.Eq, Col("str"), Lit(LiteralValue.NewString("value-5"))), FilterRows = true },
        ])
        {
            List<string> expected = Read(Ours("plain.parquet"), null, options);
            Assert.NotEmpty(expected);
            Assert.Equal(expected, Read(Ours(name), OurKeys(name), options));
        }
        using var file = ParquetFile.Open(Ours(name), OurKeys(name));
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = Bin(BinaryOp.Lt, Col("i32"), Lit(LiteralValue.NewInt32(-1400))), FilterRows = true });
        Assert.Equal([0], reader.RowGroupsRead); // pruned by the (decrypted) statistics
        Assert.Equal(94, reader.ReadBatches().Sum(b => b.Length)); // rows 0..99 but the 6 nulls
    }

    [Theory]
    [MemberData(nameof(ApacheFiles))]
    public void The_Parquet_Testing_Files_Read_Alike(string name)
    {
        string reference = name.StartsWith("aes256", StringComparison.Ordinal) ? "aes256/uniform_encryption.parquet.encrypted" : "uniform_encryption.parquet.encrypted";
        List<string> expected = Read(Apache(reference), ApacheKeys(reference));
        Assert.Equal(50, expected.Count);
        Assert.Equal(expected, Read(Apache(name), ApacheKeys(name)));
        Assert.Equal(expected[10..30], Read(Apache(name), ApacheKeys(name), new ParquetReadOptions { Rows = RowRanges.Of([(10, 20)]), MaxDegreeOfParallelism = 1 }));
    }

    [Fact]
    public void Encrypted_Bloom_Filters_Are_Read_And_Prune()
    {
        string path = Apache("encrypt_columns_and_footer_bloom_filter.parquet.encrypted");
        var keys = ApacheKeys("encrypt_columns_and_footer_bloom_filter.parquet.encrypted");
        // 0.75 lies within every range (values are i + 0.5): only the Bloom filter rules it out.
        ScalarExpr absent = Bin(BinaryOp.Eq, Col("double_field"), Lit(LiteralValue.NewFloat64(0.75)));
        using var file = ParquetFile.Open(path, keys);
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = absent })) Assert.Empty(reader.RowGroupsRead);
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = absent, BloomFilters = false })) Assert.NotEmpty(reader.RowGroupsRead);
        ScalarExpr present = Bin(BinaryOp.Eq, Col("double_field"), Lit(LiteralValue.NewFloat64(1.5)));
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = present, FilterRows = true }))
            Assert.Equal(["1.5 | 1.25 | 1 | \"name_1\""], ArrowRender.Rows(reader.ReadBatches()));
        Assert.Equal(2000, Read(path, keys).Count);
    }

    [Fact]
    public void A_Column_Without_Its_Key_Can_Be_Left_Out_And_Fails_By_Name_When_Read()
    {
        foreach (string name in (string[])["pl_columns.parquet", "pl_columns_plaintext_footer.parquet"])
        {
            ParquetDecryptionOptions footerOnly = OurKeys(name, columnKeys: false);
            List<string> expected = Read(Ours("plain.parquet"), null, new ParquetReadOptions { Columns = ["i32", "str", "lst"] });
            Assert.Equal(expected, Read(Ours(name), footerOnly, new ParquetReadOptions { Columns = ["i32", "str", "lst"] }));
            var e = Assert.Throws<ParquetFormatException>(() => Read(Ours(name), footerOnly, new ParquetReadOptions { Columns = ["i64"] }));
            Assert.Contains("'i64'", e.Message);
            Assert.Contains("ColumnKeys", e.Message);
            // A struct whose field has its own key: reading the struct needs it.
            Assert.Contains("'s.a'", Assert.Throws<ParquetFormatException>(() => Read(Ours(name), footerOnly, new ParquetReadOptions { Columns = ["s"] })).Message);
            // Under an encrypted footer its metadata stays encrypted; a plaintext footer keeps it without statistics.
            using var file = ParquetFile.Open(Ours(name), footerOnly);
            ParquetColumnChunk chunk = file.Metadata.RowGroups[0].Columns.Single(c => c.Path.SequenceEqual(["i64"]));
            Assert.Equal(name == "pl_columns.parquet" ? -1 : 600, chunk.ValueCount);
            Assert.Null(chunk.Statistics);
        }
    }

    [Fact]
    public void A_Plaintext_Footer_Opens_Without_Keys_And_Reads_The_Columns_That_Are_Not_Encrypted()
    {
        using var file = ParquetFile.Open(Ours("pl_columns_plaintext_footer.parquet"));
        Assert.True(file.Metadata.IsEncrypted);
        Assert.Equal(1200, file.Metadata.RowCount);
        using (var reader = file.OpenReader(new ParquetReadOptions { Columns = ["i32", "d"] }))
            Assert.Equal(Read(Ours("plain.parquet"), null, new ParquetReadOptions { Columns = ["i32", "d"] }), ArrowRender.Rows(reader.ReadBatches()));
        var e = Assert.Throws<ParquetFormatException>(() => Read(Ours("pl_columns_plaintext_footer.parquet"), null, new ParquetReadOptions { Columns = ["i64"] }));
        Assert.Contains("encrypted", e.Message);
        Assert.Contains("ParquetDecryptionOptions", e.Message);
        // Uniformly encrypted under a plaintext footer: every column needs the key.
        using var uniform = ParquetFile.Open(Ours("pa_gcm128_plaintext_footer.parquet"));
        Assert.Contains("'i32'", Assert.Throws<ParquetFormatException>(() => ArrowRender.Rows(uniform.OpenReader(new ParquetReadOptions { Columns = ["i32"] }).ReadBatches())).Message);
    }

    [Fact]
    public void A_File_That_Is_Not_Encrypted_Reads_With_Keys_As_It_Is()
    {
        Assert.Equal(Read(Ours("plain.parquet"), null), Read(Ours("plain.parquet"), new ParquetDecryptionOptions(K16)));
        using var file = ParquetFile.Open(Ours("plain.parquet"), new ParquetDecryptionOptions(K16));
        Assert.False(file.Metadata.IsEncrypted);
    }

    // ---------------------------------------------------------------- refusals

    [Fact]
    public void Missing_Or_Wrong_Keys_Are_Refused_With_The_Reason()
    {
        Assert.Contains("ParquetDecryptionOptions", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(Ours("pa_gcm128.parquet"))).Message);
        Assert.Contains("footer key", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(Ours("pa_gcm128.parquet"), new ParquetDecryptionOptions(K32))).Message);
        Assert.Contains("AadPrefix", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(Ours("pa_gcm128_aad_supplied.parquet"), new ParquetDecryptionOptions(K16))).Message);
        Assert.Contains("does not match", Assert.Throws<ParquetFormatException>(() =>
            ParquetFile.Open(Ours("pa_gcm256_aad_stored.parquet"), new ParquetDecryptionOptions(K32) { AadPrefix = "other"u8.ToArray() })).Message);
        // A plaintext footer with a wrong key: its signature does not match.
        Assert.Contains("signature", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(Ours("pa_gcm128_plaintext_footer.parquet"), new ParquetDecryptionOptions(K24))).Message);
        // Unchecked, the wrong key fails on the first module it should open: the columns' encrypted metadata.
        Assert.Contains("does not open", Assert.Throws<ParquetFormatException>(() =>
            ParquetFile.Open(Ours("pa_gcm128_plaintext_footer.parquet"), new ParquetDecryptionOptions(K24) { VerifyFooterSignature = false })).Message);
        // A wrong column key.
        var wrong = new ParquetDecryptionOptions(K16) { ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = K16, ["s.a"] = K16 } };
        Assert.Contains("'i64'", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(Ours("pl_columns.parquet"), wrong)).Message);
        Assert.Contains("AES keys are 16, 24 or 32 bytes", Assert.Throws<ArgumentException>(() => ParquetFile.Open(Ours("pa_gcm128.parquet"), new ParquetDecryptionOptions(new byte[20]))).Message);
        // An empty AAD prefix means none.
        Assert.Equal(1200, Read(Ours("pa_gcm256_aad_stored.parquet"), new ParquetDecryptionOptions(K32) { AadPrefix = Aad.AsMemory(0, 0) }).Count);
    }

    [Fact]
    public void Damaged_Bytes_Fail_Only_With_ParquetFormatException()
    {
        var random = new Random(20261127);
        foreach (string name in (string[])["pa_gcm128.parquet", "pa_ctr128.parquet", "pl_columns_plaintext_footer.parquet"])
        {
            byte[] good = File.ReadAllBytes(Ours(name));
            for (int round = 0; round < 60; round++)
            {
                byte[] bad = (byte[])good.Clone();
                for (int k = 0; k < 1 + round % 3; k++) bad[random.Next(4, bad.Length - 8)] ^= (byte)(1 << random.Next(8));
                Exception? e = Record.Exception(() =>
                {
                    using var file = ParquetFile.Open(bad, OurKeys(name));
                    using var reader = file.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 1 });
                    _ = ArrowRender.Rows(reader.ReadBatches());
                });
                if (e is not null) Assert.IsType<ParquetFormatException>(e);
            }
        }
    }

    // ---------------------------------------------------------------- other ways in

    [Fact]
    public async Task Async_Opens_Reopens_From_Metadata_Datasets_And_Typed_Rows_Take_The_Keys()
    {
        List<string> plain = Read(Ours("plain.parquet"), null);
        await using (var file = await ParquetFile.OpenAsync(Ours("pa_ctr256.parquet"), OurKeys("pa_ctr256.parquet")))
        {
            await using (var again = ParquetFile.Open(Ours("pa_ctr256.parquet"), file.Metadata, OurKeys("pa_ctr256.parquet")))
            using (var reader = again.OpenReader())
                Assert.Equal(plain, ArrowRender.Rows(reader.ReadBatches()));
            using var keyless = ParquetFile.Open(Ours("pa_ctr256.parquet"), file.Metadata);
            Assert.Contains("ParquetDecryptionOptions", Assert.Throws<ParquetFormatException>(() => ArrowRender.Rows(keyless.OpenReader().ReadBatches())).Message);
        }

        using var dataset = ParquetDataset.Open([Ours("pa_gcm128.parquet"), Ours("pa_ctr128.parquet"), Ours("plain.parquet")],
            new ParquetDatasetOptions { Decryption = new ParquetDecryptionOptions(K16) });
        using (var reader = dataset.OpenReader())
            Assert.Equal([.. plain, .. plain, .. plain], ArrowRender.Rows(reader.ReadBatches()));

        List<Row> rows = ParquetFile.ReadList<Row>(Ours("pl_columns.parquet"), r => r.i32 < -1490, decryption: OurKeys("pl_columns.parquet"));
        Assert.Equal(9, rows.Count); // rows 0..9 but the null at 3
        Assert.All(rows, r => Assert.True(r.i32 < -1490));
    }

    [Fact]
    public void The_Footer_View_Of_An_Encrypted_File_Is_Public_Only_Through_Open()
    {
        // The bytes of a plaintext footer parse, but a footer alone has no keys: ParquetMetadata.Parse refuses it.
        const string name = "apache/data/encrypt_columns_plaintext_footer.parquet.encrypted";
        Assert.Contains("encrypted", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.Parse(ParquetFixtures.FooterBytes(name))).Message);
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name), ApacheKeys("encrypt_columns_plaintext_footer"));
        Assert.True(file.Metadata.IsEncrypted);
        Assert.Equal(50, file.Metadata.RowCount);
    }

    private sealed record Row(int? i32, long? i64, string? str);
}
