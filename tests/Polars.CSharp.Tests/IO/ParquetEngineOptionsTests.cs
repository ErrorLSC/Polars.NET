using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests.IO;

/// <summary>
/// The engine's Parquet options beyond the plain ones: scan casts, extra columns, hidden files, key-value metadata, a
/// stored Arrow schema, statistics by default, and Parquet modular encryption (fixtures written by pyarrow, Polars 2.2
/// and parquet-testing).
/// </summary>
public sealed class ParquetEngineOptionsTests : IDisposable
{
    private static readonly byte[] K16 = "0123456789012345"u8.ToArray();
    private static readonly byte[] K32 = "01234567890123456789012345678901"u8.ToArray();
    private static readonly byte[] ColI64 = "1234567890123450"u8.ToArray();
    private static readonly byte[] ColSa = "1234567890123451"u8.ToArray();
    private static readonly byte[] Aad = "table-7"u8.ToArray();

    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"polars_parquet_options_{Guid.NewGuid():N}");

    public ParquetEngineOptionsTests() => Directory.CreateDirectory(_dir);

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Temp(string name) => Path.Combine(_dir, name);

    private static string Encrypted(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "encrypted", name);

    private static string Apache(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "apache", "data", name);

    private static DataFrame Plain() => DataFrame.ReadParquet(Encrypted("plain.parquet"));

    private static ParquetDecryption PolarsKeys(bool withColumnKeys = true) => new(K16)
    {
        ColumnKeys = withColumnKeys ? new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = ColI64, ["s.a"] = ColSa } : null,
    };

    // ---------------------------------------------------------------- scan options

    [Fact]
    public void Extra_Columns_Raise_By_Default_And_Are_Left_Out_When_Ignored()
    {
        using (var a = DataFrame.From<Narrow>([new(1, "x")])) a.WriteParquet(Temp("a.parquet"));
        using (var b = DataFrame.From<Wide>([new(2, "y", 3.5)])) b.WriteParquet(Temp("b.parquet"));
        string glob = Path.Combine(_dir, "*.parquet");

        Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(glob).Dispose());

        using var df = DataFrame.ReadParquet(glob, ignoreExtraColumns: true);
        Assert.Equal(["id", "name"], df.Columns);
        Assert.Equal([1L, 2L], df["id"].ToArray<long>());
    }

    [Fact]
    public void Integer_And_Datetime_Casts_Apply_Only_When_Allowed()
    {
        using (var a = DataFrame.From<Int32Value>([new(1), new(2)])) a.WriteParquet(Temp("a.parquet"));
        using (var b = DataFrame.From<Wide64>([new(3_000_000_000)])) b.WriteParquet(Temp("b.parquet"));
        string glob = Path.Combine(_dir, "*.parquet");
        var schema = PolarsSchema.From<Wide64>();

        Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(glob, schema: schema).Dispose());

        using var df = DataFrame.ReadParquet(glob, schema: PolarsSchema.From<Wide64>(), castOptions: new ScanCastOptions { Integers = IntegerCasts.Upcast });
        Assert.Equal([1L, 2L, 3_000_000_000L], df["v"].ToArray<long>());

        // Floats are a separate permission: an integer upcast does not let an int read as a double.
        Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(glob, schema: PolarsSchema.From<AsDouble>(),
            castOptions: new ScanCastOptions { Integers = IntegerCasts.Upcast }).Dispose());
        using var asDouble = DataFrame.ReadParquet(glob, schema: PolarsSchema.From<AsDouble>(),
            castOptions: new ScanCastOptions { Integers = IntegerCasts.ToFloat });
        Assert.Equal([1.0, 2.0, 3e9], asDouble["v"].ToArray<double>());
    }

    [Fact]
    public void Hidden_File_Prefixes_Skip_Files_In_A_Directory_Scan()
    {
        using (var a = DataFrame.From<Narrow>([new(1, "x")])) a.WriteParquet(Temp("part-0.parquet"));
        using (var b = DataFrame.From<Narrow>([new(2, "y")])) b.WriteParquet(Temp("tmp_part-1.parquet"));

        using var all = DataFrame.ReadParquet(_dir);
        Assert.Equal(2, all.Height);
        using var visible = DataFrame.ReadParquet(_dir, hiddenFilePrefixes: ["tmp_"]);
        Assert.Equal([1L], visible["id"].ToArray<long>());
    }

    // ---------------------------------------------------------------- write options

    [Fact]
    public void Statistics_Are_Written_By_Default()
    {
        using var df = DataFrame.From<Narrow>([new(1, "x"), new(5, null)]);
        df.WriteParquet(Temp("stats.parquet"));
        using var off = DataFrame.From<Narrow>([new(1, "x")]);
        off.WriteParquet(Temp("nostats.parquet"), statistics: false);

        using var file = ParquetFile.Open(Temp("stats.parquet"));
        var id = file.Metadata.RowGroups[0].Columns.Single(c => c.Path[0] == "id").Statistics;
        Assert.NotNull(id?.Min);
        Assert.Equal(1L, BitConverter.ToInt64(id!.Min));
        Assert.Equal(5L, BitConverter.ToInt64(id.Max));
        Assert.Equal(1L, file.Metadata.RowGroups[0].Columns.Single(c => c.Path[0] == "name").Statistics?.NullCount);

        using var bare = ParquetFile.Open(Temp("nostats.parquet"));
        Assert.Null(bare.Metadata.RowGroups[0].Columns.Single(c => c.Path[0] == "id").Statistics?.Min);
    }

    [Fact]
    public void Key_Value_Metadata_Is_Written_To_The_Footer_On_Every_Sink()
    {
        Dictionary<string, string> metadata = new() { ["owner"] = "team-a", ["note"] = "日本語" };
        using var df = DataFrame.From<Narrow>([new(1, "x")]);
        df.WriteParquet(Temp("file.parquet"), metadata: metadata);
        byte[] memory = df.WriteParquetMemory(metadata: metadata);
        using var stream = new MemoryStream();
        df.WriteParquet(stream, metadata: metadata);

        foreach (ParquetFile file in (ParquetFile[])[ParquetFile.Open(Temp("file.parquet")), ParquetFile.Open(memory), ParquetFile.Open(stream.ToArray())])
        {
            using (file)
            {
                var kv = file.Metadata.KeyValueMetadata.ToDictionary(p => p.Key, p => p.Value);
                Assert.Equal("team-a", kv["owner"]);
                Assert.Equal("日本語", kv["note"]);
                Assert.True(kv.ContainsKey("ARROW:schema"));
            }
        }
    }

    [Fact]
    public void A_Given_Arrow_Schema_Is_Stored_With_Its_Field_Metadata()
    {
        using var df = DataFrame.From<Narrow>([new(1, "x")]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Int64Type.Default, true, [new("unit", "cm")]))
            .Field(new Apache.Arrow.Field("name", StringViewType.Default, true))
            .Metadata("table", "people")
            .Build();
        df.WriteParquet(Temp("arrow.parquet"), arrowSchema: schema);

        using var file = ParquetFile.Open(Temp("arrow.parquet"));
        Assert.Equal("cm", file.Schema.GetFieldByName("id").Metadata["unit"]);

        var wrong = new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("other", Int64Type.Default, true)).Field(new Apache.Arrow.Field("name", StringViewType.Default, true)).Build();
        Assert.ThrowsAny<Exception>(() => df.WriteParquet(Temp("wrong.parquet"), arrowSchema: wrong));
    }

    // ---------------------------------------------------------------- encryption

    [Theory]
    [InlineData("pa_gcm128.parquet", 16, false)]
    [InlineData("pa_gcm256_aad_stored.parquet", 32, false)]
    [InlineData("pa_gcm128_aad_supplied.parquet", 16, true)]
    [InlineData("pa_gcm128_plaintext_footer.parquet", 16, false)]
    public void Reads_Pyarrow_Files_Encrypted_With_AES_GCM(string name, int keyLength, bool supplyAad)
    {
        var decryption = new ParquetDecryption(keyLength == 16 ? K16 : K32) { AadPrefix = supplyAad ? Aad : null };
        using var df = DataFrame.ReadParquet(Encrypted(name), decryption: decryption);
        using var plain = Plain();
        Assert.True(plain.Equals(df), name);
    }

    [Fact]
    public void An_Empty_AAD_Prefix_Means_None()
    {
        // An empty slice of a longer array: its memory pins to a real pointer.
        var decryption = new ParquetDecryption(K32) { AadPrefix = Aad.AsMemory(0, 0) };
        using var df = DataFrame.ReadParquet(Encrypted("pa_gcm256_aad_stored.parquet"), decryption: decryption);
        using var plain = Plain();
        Assert.True(plain.Equals(df));
    }

    [Fact]
    public void Reads_Polars_Files_With_Column_Keys_And_Projects_Around_A_Missing_Key()
    {
        using var plain = Plain();
        foreach (string name in (string[])["pl_columns.parquet", "pl_columns_plaintext_footer.parquet"])
        {
            using var all = DataFrame.ReadParquet(Encrypted(name), decryption: PolarsKeys());
            Assert.True(plain.Equals(all), name);

            // Without the column keys, the other columns still read; the encrypted column fails, naming it.
            using var others = DataFrame.ReadParquet(Encrypted(name), columns: ["i32", "str"], decryption: PolarsKeys(withColumnKeys: false));
            Assert.Equal(plain["str"].ToArray<string>(), others["str"].ToArray<string>());
            var e = Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(Encrypted(name), columns: ["i64"], decryption: PolarsKeys(withColumnKeys: false)).Dispose());
            Assert.Contains("i64", e.Message);
        }
    }

    [Fact]
    public void Wrong_Or_Missing_Keys_Fail()
    {
        Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(Encrypted("pa_gcm128.parquet")).Dispose());
        Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(Encrypted("pa_gcm128.parquet"), decryption: new ParquetDecryption(ColI64)).Dispose());
        // The AAD prefix was not stored: reading needs it.
        Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(Encrypted("pa_gcm128_aad_supplied.parquet"), decryption: new ParquetDecryption(K16)).Dispose());
    }

    [Fact]
    public void The_Engine_Refuses_AES_GCM_CTR_And_192_Bit_Keys_With_A_Clear_Message()
    {
        var ctr = Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(Encrypted("pa_ctr128.parquet"), decryption: new ParquetDecryption(K16)).Dispose());
        Assert.Contains("AES_GCM_CTR_V1", ctr.Message);

        var e = Assert.Throws<ArgumentException>(() => DataFrame.ReadParquet(Encrypted("pa_gcm192.parquet"),
            decryption: new ParquetDecryption("012345678901234567890123"u8.ToArray())));
        Assert.Contains("24 bytes", e.Message);
        Assert.Contains("16-byte (AES-128) or 32-byte (AES-256)", e.Message);
        using var one = DataFrame.From<Narrow>([new(1, "x")]);
        var column = Assert.Throws<ArgumentException>(() => one.WriteParquetMemory(encryption: new(K16)
        {
            ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["id"] = new byte[20] },
        }));
        Assert.Contains("the key of column 'id' is 20 bytes", column.Message);
    }

    [Fact]
    public void Reads_The_Parquet_Testing_Files_Encrypted_With_AES_GCM()
    {
        var keys = new ParquetDecryption(K16)
        {
            ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>>
            {
                ["double_field"] = "1234567890123450"u8.ToArray(),
                ["float_field"] = "1234567890123451"u8.ToArray(),
            },
        };
        using var reference = DataFrame.ReadParquet(Apache("uniform_encryption.parquet.encrypted"), decryption: new ParquetDecryption(K16));
        Assert.Equal(50, reference.Height);
        foreach (string name in (string[])["encrypt_columns_and_footer.parquet.encrypted", "encrypt_columns_plaintext_footer.parquet.encrypted"])
        {
            using var df = DataFrame.ReadParquet(Apache(name), decryption: keys);
            Assert.True(reference.Equals(df), name);
        }
        foreach (string name in (string[])["encrypt_columns_and_footer_aad.parquet.encrypted", "encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted"])
        {
            using var df = DataFrame.ReadParquet(Apache(name), decryption: new(K16) { ColumnKeys = keys.ColumnKeys, AadPrefix = "tester"u8.ToArray() });
            Assert.True(reference.Equals(df), name);
        }
        using var bloom = DataFrame.ReadParquet(Apache("encrypt_columns_and_footer_bloom_filter.parquet.encrypted"), decryption: keys);
        Assert.Equal(2000, bloom.Height);
    }

    [Fact]
    public void Encrypted_Writes_Round_Trip_Through_Every_Sink()
    {
        using var plain = Plain();
        var encryption = new ParquetEncryption(K16)
        {
            ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = ColI64, ["s.a"] = ColSa },
            AadPrefix = Aad,
        };
        var decryption = new ParquetDecryption(K16)
        {
            ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["i64"] = ColI64, ["s.a"] = ColSa },
            AadPrefix = Aad,
        };

        plain.WriteParquet(Temp("file.parquet"), encryption: encryption);
        byte[] memory = plain.WriteParquetMemory(encryption: encryption);
        using var stream = new MemoryStream();
        plain.WriteParquet(stream, encryption: encryption);

        foreach (var (name, read) in ((string, Func<ParquetDecryption, DataFrame>)[])
        [
            ("file", d => DataFrame.ReadParquet(Temp("file.parquet"), decryption: d)),
            ("memory", d => DataFrame.ReadParquet(memory, decryption: d)),
            ("stream", d => DataFrame.ReadParquet(stream.ToArray(), decryption: d)),
        ])
        {
            using var back = read(decryption);
            Assert.True(plain.Equals(back), name);
            Assert.ThrowsAny<Exception>(() => read(null).Dispose());
            Assert.ThrowsAny<Exception>(() => read(new ParquetDecryption(K16) { ColumnKeys = decryption.ColumnKeys }).Dispose()); // AAD prefix not stored
        }

        // The columns with keys of their own need them: the footer key alone reads the others only.
        var footerOnly = new ParquetDecryption(K16) { AadPrefix = Aad };
        using var others = DataFrame.ReadParquet(memory, columns: ["i32"], decryption: footerOnly);
        Assert.Equal(plain["i32"].ToArray<int?>(), others["i32"].ToArray<int?>());
        var e = Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(memory, columns: ["i64"], decryption: footerOnly).Dispose());
        Assert.Contains("i64", e.Message);
    }

    [Fact]
    public async Task Async_Sinks_Encrypt_And_Keep_The_Options_Alive()
    {
        using var plain = Plain();
        using var stream = new MemoryStream();
        await plain.WriteParquetAsync(stream, encryption: new ParquetEncryption(K32) { StoreAadPrefix = true, AadPrefix = Aad });
        GC.Collect();
        GC.WaitForPendingFinalizers();
        using var back = DataFrame.ReadParquet(stream.ToArray(), decryption: new ParquetDecryption(K32));
        Assert.True(plain.Equals(back));
    }

    [Fact]
    public async Task Every_Sink_Entry_Point_Passes_The_Write_Options()
    {
        var encryption = new ParquetEncryption(K16) { PlaintextFooter = true };
        var decryption = new ParquetDecryption(K16);
        Dictionary<string, string> metadata = new() { ["owner"] = "team-a" };
        using var df = DataFrame.From<Narrow>([new(1, "a"), new(2, "b"), new(3, "a")]);

        // Partitioned: every file is encrypted and carries the metadata.
        df.WriteParquetPartitioned(Temp("parts"), Pl.Col("name"), metadata: metadata, encryption: encryption);
        string[] files = Directory.GetFiles(Temp("parts"), "*.parquet", SearchOption.AllDirectories);
        Assert.Equal(2, files.Length);
        foreach (string file in files)
        {
            Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(file).Dispose()); // the columns are encrypted
            using var part = DataFrame.ReadParquet(file, decryption: decryption);
            Assert.True(part.Height > 0);
        }
        using (var parts = DataFrame.ReadParquet(Path.Combine(Temp("parts"), "**", "*.parquet"), decryption: decryption))
            Assert.Equal(3, parts.Height);

        // Native memory.
        using (var native = df.WriteParquetNativeMemory(metadata: metadata, encryption: encryption))
        {
            byte[] copy = native.GetSpan().ToArray();
            Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(copy).Dispose());
            using var back = DataFrame.ReadParquet(copy, decryption: decryption);
            Assert.True(df.Equals(back));
        }

        // A pipe writer.
        var pipe = new System.IO.Pipelines.Pipe();
        using var sink = new MemoryStream();
        Task reading = pipe.Reader.CopyToAsync(sink);
        await df.WriteParquetAsync(pipe.Writer, metadata: metadata, encryption: encryption);
        await pipe.Writer.CompleteAsync();
        await reading;
        using (var fromPipe = DataFrame.ReadParquet(sink.ToArray(), decryption: decryption)) Assert.True(df.Equals(fromPipe));

        // The 1.1 signature, with the token in its place.
        using var plain = new MemoryStream();
        await df.WriteParquetAsync(plain, ParquetCompression.ZSTD, 3, true, 0, 0, -1, true, CancellationToken.None);
        using (var fromOld = DataFrame.ReadParquet(plain.ToArray())) Assert.True(df.Equals(fromOld));
    }

    [Fact]
    public void A_Plaintext_Footer_Reads_The_Columns_Without_Their_Own_Key_With_The_Footer_Key_Alone()
    {
        using var df = DataFrame.From<Narrow>([new(1, "x"), new(2, "y")]);
        byte[] bytes = df.WriteParquetMemory(encryption: new ParquetEncryption(K16)
        {
            PlaintextFooter = true,
            ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["name"] = ColI64 },
        });
        using var ids = DataFrame.ReadParquet(bytes, columns: ["id"], decryption: new ParquetDecryption(K16));
        Assert.Equal([1L, 2L], ids["id"].ToArray<long>());

        // A plaintext footer ends in PAR1; an encrypted one in PARE.
        Assert.True(bytes.AsSpan()[^4..].SequenceEqual("PAR1"u8));
        byte[] hidden = df.WriteParquetMemory(encryption: new ParquetEncryption(K16));
        Assert.True(hidden.AsSpan()[^4..].SequenceEqual("PARE"u8));
    }

    [Fact]
    public void Keys_Never_Show_In_ToString()
    {
        string text = new ParquetEncryption(K16) { ColumnKeys = new Dictionary<string, ReadOnlyMemory<byte>> { ["a"] = ColI64 } }.ToString()
            + new ParquetDecryption(K16).ToString();
        Assert.DoesNotContain("0123456789", text);
        Assert.Contains("16 bytes", text);
    }

    private sealed record Narrow(long id, string name);

    private sealed record Wide(long id, string name, double score);

    private sealed record Wide64(long v);

    private sealed record Int32Value(int v);

    private sealed record AsDouble(double v);
}
