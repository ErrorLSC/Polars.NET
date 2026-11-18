using Kaminari.Sazanami.Arrow.Ipc;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// What the fuzzer (tests/Seseragi.Files.Fuzz) found: damaged files are read or refused with
/// <see cref="ParquetFormatException"/>, nothing else; a damaged stored Arrow schema is ignored without the
/// Apache.Arrow reader allocating from its lengths.
/// </summary>
public class ParquetFuzzRegressionTests
{
    private static string Dir(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", name);

    public static TheoryData<string> FuzzFiles => [.. Directory.GetFiles(Dir("parquet-fuzz"), "*.parquet").Select(Path.GetFileName).Order()!];

    /// <summary>Every way of reading a found file ends in batches or a format error.</summary>
    [Theory]
    [MemberData(nameof(FuzzFiles))]
    public void FoundFiles_ReadOrAreRefused_AsFormatErrors(string name)
    {
        byte[] file = File.ReadAllBytes(Path.Combine(Dir("parquet-fuzz"), name));
        foreach (bool views in (bool[])[true, false])
        {
            foreach (int parallelism in (int[])[1, 2])
            {
                try
                {
                    using var parquet = ParquetFile.Open(new MemoryStream(file));
                    using var reader = parquet.OpenReader(new() { ViewTypes = views, MaxDegreeOfParallelism = parallelism });
                    foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
                }
                catch (ParquetFormatException)
                {
                }
            }
        }
    }

    /// <summary>A dictionary page that declares more values than it holds is a format error, in both string layouts.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DictionaryPages_DeclaringMoreValuesThanTheyHold_AreRefused(bool viewTypes)
    {
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 1, repetition: null),
            HandBuiltParquet.Leaf("s", ParquetType.ByteArray, FieldRepetition.Optional, ConvertedType.Utf8),
        ];
        // Two long entries (not inline in views) where four are declared: the room for four runs out on the second.
        byte[] file = HandBuiltParquet.Build(schema, 2,
            new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 1, 0, [1, 1], [], HandBuiltParquet.Indices(1, 0, 1),
                Encoding: ParquetEncoding.RleDictionary, Dictionary: new(4, HandBuiltParquet.Strings("the first long entry, thirty b", "the second long entry, thirty "))));
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader(new() { ViewTypes = viewTypes });
        var e = Assert.Throws<ParquetFormatException>(() =>
        {
            foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
        });
        Assert.Contains("fewer than its 4", e.Message);
    }

    /// <summary>
    /// A row group claiming far more rows than its column chunks hold values is refused before anything is sized by
    /// that count (batches of one row planned up front once took all memory), with the columns read or none.
    /// </summary>
    [Fact]
    public void RowCounts_BeyondTheColumnChunks_AreRefused()
    {
        SchemaElement[] schema = [HandBuiltParquet.Group("schema", 1, repetition: null), HandBuiltParquet.Leaf("x", ParquetType.Int32, FieldRepetition.Required)];
        byte[] file = HandBuiltParquet.Build(schema, 1L << 40, new HandBuiltParquet.Column(["x"], ParquetType.Int32, 0, 0, [], [0, 0, 0], HandBuiltParquet.Int32s(1, 2, 3)));
        foreach (string[]? columns in (string[]?[])[null, []])
        {
            using var parquet = ParquetFile.Open(new MemoryStream(file));
            using var reader = parquet.OpenReader(new() { BatchSize = 1, Columns = columns });
            Assert.Contains("more than any of its column chunks holds values (3)", Assert.Throws<ParquetFormatException>(() =>
            {
                foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            }).Message);
        }
    }

    /// <summary>
    /// A row group and its column chunk claiming 2^62 rows or more (only 3 there), read with selections that would make
    /// billions of steps: two rows far apart (2^31 skips of int.MaxValue rows), nearly every row a row per batch, or many
    /// ranges each a little within bounds. The steps are not planned up front: the read fails where the data ends.
    /// </summary>
    [Theory]
    [InlineData("far", 1L << 62)]
    [InlineData("far", long.MaxValue)]
    [InlineData("rowPerBatch", long.MaxValue)]
    [InlineData("manyFar", long.MaxValue)]
    public void Selections_OverRowsTheFooterClaims_AreNotPlannedUpFront(string selection, long rows)
    {
        SchemaElement[] schema = [HandBuiltParquet.Group("schema", 1, repetition: null), HandBuiltParquet.Leaf("x", ParquetType.Int32, FieldRepetition.Required)];
        byte[] file = HandBuiltParquet.Build(schema, rows,
            new HandBuiltParquet.Column(["x"], ParquetType.Int32, 0, 0, [], [0, 0, 0], HandBuiltParquet.Int32s(1, 2, 3), FooterValues: rows));
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader(selection switch
        {
            "far" => new() { Rows = RowRanges.Of([(0, 1), (rows - 2, 1)]) },
            "rowPerBatch" => new() { BatchSize = 1, Rows = RowRanges.Of([(1, rows - 1)]) },
            // 20 000 rows 80 000 skips apart: 1.6 billion steps in all.
            _ => new() { Rows = RowRanges.Of([.. Enumerable.Range(0, 20_000).Select(i => (i * 80_000L * int.MaxValue, 1L))]) },
        });
        Assert.Contains("ends before its row group does", Assert.Throws<ParquetFormatException>(() =>
        {
            foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
        }).Message);
    }

    /// <summary>The stored Arrow schemas of the corpus (pyarrow, Polars, DuckDB, Spark writers) are all read.</summary>
    [Fact]
    public void StoredArrowSchemas_OfTheCorpus_AreSound()
    {
        List<string> checkedFiles = [];
        foreach (string path in Directory.EnumerateFiles(Dir("parquet"), "*.parquet", SearchOption.AllDirectories))
        {
            byte[]? message = StoredSchema(path);
            if (message is null) continue;
            Assert.NotNull(IpcSchemaMessage.TryRead(message));
            checkedFiles.Add(path);
        }
        Assert.True(checkedFiles.Count >= 20, $"only {checkedFiles.Count} files store an Arrow schema");
    }

    /// <summary>Damaged schemas (bytes changed, cut short) are read or refused (null), never thrown on.</summary>
    [Fact]
    public void DamagedStoredArrowSchemas_AreCheckedWithoutThrowing()
    {
        var random = new Random(23);
        int sound = 0, unsound = 0;
        foreach (string path in Directory.EnumerateFiles(Dir("parquet"), "*.parquet", SearchOption.AllDirectories).Order().Take(60))
        {
            if (StoredSchema(path) is not { } message) continue;
            for (int round = 0; round < 200; round++)
            {
                byte[] damaged = random.Next(10) == 0 ? message[..random.Next(message.Length)] : [.. message];
                for (int k = random.Next(1, 4); k > 0 && damaged.Length > 0; k--)
                {
                    int at = random.Next(damaged.Length);
                    damaged[at] = random.Next(2) == 0 ? (byte)random.Next(256) : (byte)(damaged[at] ^ (1 << random.Next(8)));
                }
                if (IpcSchemaMessage.TryRead(damaged) is null) unsound++;
                else sound++;
            }
        }
        Assert.True(sound > 0 && unsound > 0, $"{sound} sound, {unsound} unsound");
    }

    /// <summary>
    /// Each kind of damage the reader must refuse, made at a known place of a sound schema: a vector longer than the
    /// buffer (a string's bytes are not walked: only its length guards them), an offset past its end or beyond 2^31,
    /// a type tag from the future, a message that is not a schema.
    /// </summary>
    [Fact]
    public void StoredArrowSchema_DamagedAtKnownPlaces_IsUnsound()
    {
        byte[] message = Directory.EnumerateFiles(Dir("parquet"), "*.parquet", SearchOption.AllDirectories).Order()
            .Select(StoredSchema).First(m => m is not null)!;
        Assert.NotNull(IpcSchemaMessage.TryRead(message));
        int body = BitConverter.ToUInt32(message, 0) == 0xFFFFFFFF ? 8 : 4;
        int root = U32(0);
        int schema = Target(root, 2);
        int fieldsAt = Field(schema, 1), fields = Target(schema, 1);
        int first = fields + 4 + U32(fields + 4);

        Assert.True(IpcSchemaMessage.TryRead(With(fields, 0x7FFFFFFF)) is null, "a vector longer than the buffer");
        Assert.True(IpcSchemaMessage.TryRead(With(fieldsAt, message.Length)) is null, "an offset past the end");
        Assert.True(IpcSchemaMessage.TryRead(With(fieldsAt, int.MinValue)) is null, "an offset beyond 2^31 (negative as an int)");
        int name = Target(first, 0);
        Assert.True(IpcSchemaMessage.TryRead(With(name, 0x7FFFFFF0)) is null, "a string longer than the buffer");
        Assert.True(IpcSchemaMessage.TryRead(WithByte(Field(first, 2), 99)) is null, "a type tag from the future");
        Assert.True(IpcSchemaMessage.TryRead(WithByte(Field(root, 1), 2)) is null, "a record batch message");

        // FlatBuffer navigation over the message's buffer (positions relative to its start).
        int U32(int at) => (int)BitConverter.ToUInt32(message, body + at);
        int Field(int table, int slot)
        {
            int vtable = table - BitConverter.ToInt32(message, body + table);
            return table + BitConverter.ToUInt16(message, body + vtable + 4 + 2 * slot);
        }
        int Target(int table, int slot) => Field(table, slot) + U32(Field(table, slot));
        byte[] With(int at, int value)
        {
            byte[] copy = [.. message];
            BitConverter.TryWriteBytes(copy.AsSpan(body + at), value);
            return copy;
        }
        byte[] WithByte(int at, byte value)
        {
            byte[] copy = [.. message];
            copy[body + at] = value;
            return copy;
        }
    }

    private static byte[]? StoredSchema(string path)
    {
        try
        {
            using var parquet = ParquetFile.Open(path);
            string? encoded = parquet.Thrift.KeyValueMetadata?.LastOrDefault(kv => kv.Key == "ARROW:schema")?.Value;
            return string.IsNullOrEmpty(encoded) ? null : Convert.FromBase64String(encoded);
        }
        catch (ParquetFormatException)
        {
            return null;
        }
    }
}
