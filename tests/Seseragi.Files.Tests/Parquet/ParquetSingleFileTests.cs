using System.Buffers.Binary;
using Apache.Arrow;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// One stream holds one Parquet file: files joined together, data before the file and files cut at the start are
/// refused on open (the footer's offsets would otherwise point at other bytes), while the padding and metadata
/// copies writers leave before the footer are accepted.
/// </summary>
public class ParquetSingleFileTests
{
    /// <summary>Several row groups and pages, larger than what opening reads at once.</summary>
    private static readonly byte[] Large = File.ReadAllBytes(ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet"));

    /// <summary>Smaller than what opening reads at once.</summary>
    private static readonly byte[] Small = File.ReadAllBytes(ParquetFixtures.FullPath("matrix/pa_plain_v1.parquet"));

    private static List<string> Rows(ParquetFile file)
    {
        using var reader = file.OpenReader();
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), i))));
            }
        }
        return rows;
    }

    private static int FooterLength(byte[] file) => BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 8));

    /// <summary>The file with <paramref name="inserted"/> put right before its footer (offsets stay valid).</summary>
    private static byte[] BeforeTheFooter(byte[] file, byte[] inserted)
    {
        int footerStart = file.Length - 8 - FooterLength(file);
        return [.. file.AsSpan(0, footerStart), .. inserted, .. file.AsSpan(footerStart)];
    }

    public static TheoryData<string> Joined => ["large+small", "small+large", "large+large", "small+small"];

    private static byte[] Join(string which) => which switch
    {
        "large+small" => [.. Large, .. Small],
        "small+large" => [.. Small, .. Large],
        "large+large" => [.. Large, .. Large],
        "small+small" => [.. Small, .. Small],
        _ => throw new ArgumentOutOfRangeException(nameof(which)),
    };

    [Theory]
    [MemberData(nameof(Joined))]
    public void FilesJoinedTogether_AreRefused(string which)
    {
        byte[] joined = Join(which);
        var error = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(joined.AsMemory()));
        Assert.Contains("not a single Parquet file", error.Message);
        // Also from a store (only what opening reads is looked at).
        error = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new Remote(joined)));
        Assert.Contains("not a single Parquet file", error.Message);
    }

    /// <summary>
    /// Bytes before the file: without the magic, refused where the start is cheap to read; with it (a file joined
    /// after another), refused once they outgrow the footer (fewer could be a metadata copy older writers leave).
    /// </summary>
    [Fact]
    public void BytesBeforeTheFile_AreRefused()
    {
        byte[] prefixed = [.. "PAR1"u8, .. new byte[FooterLength(Large) + 1], .. Large];
        byte[] garbage = [.. new byte[100], .. Large];
        foreach (byte[] bytes in (byte[][])[prefixed, garbage])
        {
            var error = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(bytes.AsMemory()));
            Assert.Contains("not a single Parquet file", error.Message);
        }
        string path = Path.GetTempFileName();
        try
        {
            File.WriteAllBytes(path, garbage);
            var error = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(path));
            Assert.Contains("does not start with PAR1", error.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void AFileCutAtTheStart_IsRefused()
    {
        var error = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(Large.AsMemory(1000)));
        Assert.Contains("not a single Parquet file", error.Message);
    }

    /// <summary>Zeros padding the file before the footer (as block-aligned writers leave) are accepted, however many.</summary>
    [Theory]
    [InlineData(100)]
    [InlineData(5_000)]
    [InlineData(200_000)]
    public void ZerosBeforeTheFooter_AreAccepted(int count)
    {
        byte[] padded = BeforeTheFooter(Large, new byte[count]);
        using var original = ParquetFile.Open(Large.AsMemory());
        using var file = ParquetFile.Open(padded.AsMemory());
        Assert.Equal(Rows(original), Rows(file));
    }

    /// <summary>A copy of metadata no longer than the footer is accepted (older writers leave one); more data is not.</summary>
    [Fact]
    public void DataBeforeTheFooter_IsAcceptedUpToTheFootersLength()
    {
        int footer = FooterLength(Large);
        byte[] copy = [.. Enumerable.Repeat((byte)0x15, footer)];
        using (var file = ParquetFile.Open(BeforeTheFooter(Large, copy).AsMemory())) Assert.NotEmpty(Rows(file));
        // A copy followed by padding.
        using (var file = ParquetFile.Open(BeforeTheFooter(Large, [.. copy, .. new byte[3000]]).AsMemory())) Assert.NotEmpty(Rows(file));
        byte[] more = [.. Enumerable.Repeat((byte)0x15, footer + 1)];
        var error = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(BeforeTheFooter(Large, more).AsMemory()));
        Assert.Contains($"{footer + 1} bytes before the Parquet footer", error.Message);
    }

    /// <summary>A column chunk reaching into the footer is refused when read (the footer itself stays readable).</summary>
    [Fact]
    public void AChunkReachingIntoTheFooter_IsRefusedWhenRead()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("apache/bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet"));
        Assert.NotEmpty(file.Metadata.RowGroups);
        var error = Assert.Throws<ParquetFormatException>(() => Rows(file));
        Assert.Contains("before the footer", error.Message);
    }

    /// <summary>
    /// The last chunk cut short (bytes before the footer removed): it now reaches into the footer, still within the
    /// file. The file opens (a few bytes go unaccounted for) and reading that column is refused.
    /// </summary>
    [Fact]
    public void AChunkCutShortBeforeTheFooter_IsRefusedWhenRead()
    {
        int footerStart = Small.Length - 8 - FooterLength(Small);
        byte[] cut = [.. Small.AsSpan(0, footerStart - 8), .. Small.AsSpan(footerStart)];
        using var file = ParquetFile.Open(cut.AsMemory());
        foreach (ChunkReading reading in (ChunkReading[])[ChunkReading.Slice, ChunkReading.Fetch, ChunkReading.Stream])
        {
            using var reader = file.OpenReader(new ParquetReadOptions { Reading = reading });
            var error = Assert.Throws<ParquetFormatException>(() => reader.ReadBatches().ToList().ForEach(b => b.Dispose()));
            Assert.Contains("before the footer", error.Message);
        }
    }

    /// <summary>A source that is neither memory nor a local file: its start is not read on open.</summary>
    private sealed class Remote(byte[] bytes) : IRandomAccess
    {
        public long Length => bytes.Length;

        public int ReadAt(long offset, Span<byte> destination)
        {
            int count = (int)Math.Clamp(bytes.Length - offset, 0, destination.Length);
            bytes.AsSpan((int)offset, count).CopyTo(destination);
            return count;
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ReadAt(offset, destination.Span));
    }
}
