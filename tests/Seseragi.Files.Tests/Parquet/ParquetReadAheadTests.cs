using Apache.Arrow;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Pages read ahead (helpers read, check, decrypt and decompress the next pages of a chunk read whole while it decodes):
/// every codec, page format and encryption, from a file, from memory and from a mapping, reads what one thread reads;
/// a damaged page fails as it does read in order; a read stopped early lets its pages go.
/// </summary>
public sealed class ParquetReadAheadTests : IDisposable
{
    private static readonly byte[] Key = "0123456789012345"u8.ToArray();

    /// <summary>Fewer columns than the threads that read them (a column for every thread reads nothing ahead).</summary>
    private static readonly string[] Few = ["i64", "str", "dec30", "dict_str", "req_str"];

    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-ahead").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    public static TheoryData<string, ParquetPageVersion, string> Cases()
    {
        TheoryData<string, ParquetPageVersion, string> cases = [];
        foreach (string codec in (string[])["none", "snappy", "zstd", "gzip"])
        foreach (ParquetPageVersion version in (ParquetPageVersion[])[ParquetPageVersion.V1, ParquetPageVersion.V2])
        foreach (string encryption in (string[])["plain", "gcm", "ctr"])
            cases.Add(codec, version, encryption);
        return cases;
    }

    private static ParquetEncryptionOptions? Encryption(string encryption) => encryption switch
    {
        "gcm" => new ParquetEncryptionOptions(Key),
        "ctr" => new ParquetEncryptionOptions(Key) { Algorithm = ParquetEncryptionAlgorithm.AesGcmCtr },
        _ => null,
    };

    /// <summary>One row group of many small pages (every column's chunk read ahead).</summary>
    private static byte[] Write(string codec, ParquetPageVersion version, string encryption, int rows = 20_000)
    {
        using RecordBatch batch = ParquetWriteData.Wide(7, rows);
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, batch.Schema, [batch], new ParquetWriteOptions
        {
            Compression = codec switch
            {
                "none" => NoCompression.Instance,
                "zstd" => new ZstandardCompression(),
                "gzip" => new GzipCompression(),
                _ => SnappyCompression.Instance,
            },
            DataPageVersion = version,
            DataPageRows = 256,
            Encryption = Encryption(encryption),
            LeaveOpen = true,
        });
        return stream.ToArray();
    }

    private static (List<string> Rows, int Ahead, int Pages) Read(Func<ParquetFile> open, ParquetReadOptions options)
    {
        using ParquetFile file = open();
        using var reader = file.OpenReader(options);
        List<string> rows = ArrowRender.Rows(reader.ReadBatches());
        return (rows, reader.PagesReadAhead, reader.PagesRead);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Pages_Read_Ahead_Read_As_One_Thread_Reads(string codec, ParquetPageVersion version, string encryption)
    {
        byte[] bytes = Write(codec, version, encryption);
        string path = Path.Combine(_dir, "f.parquet");
        File.WriteAllBytes(path, bytes);
        ParquetDecryptionOptions? keys = encryption == "plain" ? null : new(Key);
        foreach (bool checksums in (bool[])[false, true])
        {
            var one = new ParquetReadOptions { MaxDegreeOfParallelism = 1, VerifyChecksums = checksums, Columns = Few };
            var (expected, none, pagesRead) = Read(() => ParquetFile.Open(bytes, keys), one);
            Assert.Equal(20_000, expected.Count);
            Assert.Equal(0, none); // one thread: nothing to read ahead with
            // Pages take work besides decoding (decompressing, decrypting, checksums): read ahead.
            bool work = codec != "none" || encryption != "plain" || checksums;
            foreach (ParquetMemoryMap map in (ParquetMemoryMap[])[ParquetMemoryMap.Off, ParquetMemoryMap.On])
            {
                var many = new ParquetReadOptions { MaxDegreeOfParallelism = 8, VerifyChecksums = checksums, MemoryMap = map, Columns = Few };
                foreach (Func<ParquetFile> open in (Func<ParquetFile>[])[() => ParquetFile.Open(path, keys), () => ParquetFile.Open(bytes, keys)])
                {
                    var (rows, ahead, pages) = Read(open, many);
                    Assert.Equal(expected, rows);
                    Assert.Equal(pagesRead, pages); // pages read ahead count as read
                    if (work) Assert.True(ahead > 0, "no page was read ahead");
                    else Assert.Equal(0, ahead);
                }
            }
        }
        // Every column (45, more than the threads): each thread decodes a column, nothing is read ahead.
        var all = Read(() => ParquetFile.Open(bytes, keys), new ParquetReadOptions { MaxDegreeOfParallelism = 8 });
        Assert.Equal(Read(() => ParquetFile.Open(bytes, keys), new ParquetReadOptions { MaxDegreeOfParallelism = 1 }).Rows, all.Rows);
        Assert.Equal(0, all.Ahead);
    }

    /// <summary>
    /// Rows selected in a file without page indexes skip pages by their headers, in order: such reads never read ahead,
    /// on any number of threads.
    /// </summary>
    [Theory]
    [InlineData("zstd", "plain")]
    [InlineData("snappy", "gcm")]
    public void Rows_Selected_By_Headers_Are_Read_In_Order(string codec, string encryption)
    {
        using RecordBatch batch = ParquetWriteData.Wide(7, 20_000);
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, batch.Schema, [batch], new ParquetWriteOptions
        {
            Compression = codec == "zstd" ? new ZstandardCompression() : SnappyCompression.Instance,
            DataPageRows = 256,
            PageIndex = false,
            Encryption = Encryption(encryption),
            LeaveOpen = true,
        });
        byte[] bytes = stream.ToArray();
        ParquetDecryptionOptions? keys = encryption == "plain" ? null : new(Key);
        // From the first row on (pages are taken before the first skip), and after a skip.
        foreach (RowRanges rows in (RowRanges[])[RowRanges.Of([(0, 300), (3000, 500), (9000, 4000), (19_990, 10)]), RowRanges.Of([(10, 300), (9000, 4000)])])
        {
            var (expected, _, _) = Read(() => ParquetFile.Open(bytes, keys), new ParquetReadOptions { MaxDegreeOfParallelism = 1, Rows = rows });
            Assert.Equal(rows.RowCount, expected.Count);
            using ParquetFile file = ParquetFile.Open(bytes, keys);
            using var reader = file.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 8, Rows = rows });
            Assert.Equal(expected, ArrowRender.Rows(reader.ReadBatches()));
            Assert.True(reader.PagesSkipped > 0);
            Assert.Equal(0, reader.PagesReadAhead);
        }
    }


    /// <summary>A damaged chunk fails read ahead as it fails read in order: the same error, from the same page.</summary>
    [Theory]
    [InlineData("snappy", "plain")]
    [InlineData("zstd", "gcm")]
    [InlineData("none", "ctr")]
    public void A_Damaged_Page_Fails_As_It_Does_Read_In_Order(string codec, string encryption)
    {
        byte[] bytes = Write(codec, ParquetPageVersion.V1, encryption);
        ParquetDecryptionOptions? keys = encryption == "plain" ? null : new(Key);
        long start, length;
        using (ParquetFile file = ParquetFile.Open(bytes, keys))
        {
            var chunk = file.Thrift.RowGroups[0].Columns[0].MetaData!;
            (start, length) = (chunk.DictionaryPageOffset ?? chunk.DataPageOffset, chunk.TotalCompressedSize);
        }
        var random = new Random(3);
        int failed = 0;
        for (int damage = 0; damage < 12; damage++)
        {
            byte[] damaged = [.. bytes];
            damaged[start + length / 4 + random.NextInt64(length / 2)] ^= (byte)(1 << random.Next(8));
            string Failure(int threads)
            {
                try
                {
                    Read(() => ParquetFile.Open(damaged, keys), new ParquetReadOptions { MaxDegreeOfParallelism = threads, VerifyChecksums = true, Columns = [Name0()] });
                    return "no failure";
                }
                catch (Exception e)
                {
                    return $"{e.GetType().Name}: {e.Message}";
                }
            }
            string inOrder = Failure(1);
            Assert.Equal(inOrder, Failure(8));
            if (inOrder != "no failure") failed++;
        }
        Assert.True(failed >= 6, $"only {failed} of 12 damages were caught");
    }

    private static string Name0()
    {
        using RecordBatch batch = ParquetWriteData.Wide(7, 1);
        return batch.Schema.FieldsList[0].Name;
    }

    /// <summary>
    /// Bytes past a chunk's last page (padding, damage) are never reached when the chunk is read to its last value:
    /// headers parsed ahead that fail, fail only when their page is taken, as reading in order does.
    /// </summary>
    [Theory]
    [InlineData("plain")]
    [InlineData("gcm")]
    public void What_Lies_Past_The_Last_Page_Fails_Only_When_Reached(string encryption)
    {
        byte[] bytes = Write("snappy", ParquetPageVersion.V1, encryption);
        ParquetDecryptionOptions? keys = encryption == "plain" ? null : new(Key);
        using ParquetFile file = ParquetFile.Open(bytes, keys);
        var group = file.Thrift.RowGroups[0];
        var meta = group.Columns[0].MetaData!;
        long start = meta.DictionaryPageOffset ?? meta.DataPageOffset;
        byte[] chunk = [.. bytes.AsSpan((int)start, (int)meta.TotalCompressedSize), .. Enumerable.Repeat((byte)0xEE, 300)];
        string name = Name0();

        (int Pages, string Failure) Walk(bool ahead)
        {
            using var pages = new Kaminari.Seseragi.Files.Parquet.Reading.PageReader(chunk, meta.Codec, verifyChecksums: true);
            if (file.Decryptor?.ForChunk(group.Columns[0], 0, 0, name) is { } crypto) pages.UseDecryption(crypto);
            if (ahead) pages.UseReadAhead(new Kaminari.Sazanami.Concurrency.WorkBudget(8));
            Assert.Equal(ahead, pages.ReadsAhead);
            int read = 0;
            try
            {
                while (pages.TryNext(out _)) read++;
                return (read, "no failure");
            }
            catch (Exception e)
            {
                return (read, $"{e.GetType().Name}: {e.Message}");
            }
        }

        var inOrder = Walk(ahead: false);
        Assert.True(inOrder.Pages > 20, $"{inOrder.Pages} pages");
        Assert.NotEqual("no failure", inOrder.Failure); // the padding is taken for a header once reached
        Assert.Equal(inOrder, Walk(ahead: true));
    }

    /// <summary>A read stopped after its first batch lets the pages read ahead go (helpers still at work included).</summary>
    [Theory]
    [InlineData("zstd", "plain")]
    [InlineData("snappy", "gcm")]
    public void A_Read_Stopped_Early_Lets_Its_Pages_Go(string codec, string encryption)
    {
        byte[] bytes = Write(codec, ParquetPageVersion.V2, encryption, rows: 100_000);
        ParquetDecryptionOptions? keys = encryption == "plain" ? null : new(Key);
        var options = new ParquetReadOptions { MaxDegreeOfParallelism = 8, BatchSize = 1000, Columns = Few };
        for (int i = 0; i < 20; i++)
        {
            using ParquetFile file = ParquetFile.Open(bytes, keys);
            using var reader = file.OpenReader(options);
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                Assert.Equal(1000, batch.Length);
                batch.Dispose();
                break;
            }
        }
        Assert.Equal(100_000, Read(() => ParquetFile.Open(bytes, keys), options).Rows.Count);
    }
}
