using System.Buffers.Binary;
using Apache.Arrow;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The footer <see cref="ParquetWriter"/> assembles from the Thrift bytes it keeps per column chunk (the metadata's
/// fields around the Bloom filter's, the page indexes): the same bytes as the footer's model writes, whichever chunks
/// have a Bloom filter, page indexes or size statistics; and <see cref="ParquetWriter.Metadata"/> describes that footer.
/// </summary>
public sealed class ParquetWriterFooterTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-footer").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static readonly ParquetColumnOptions Bloom = new() { BloomFilter = new ParquetBloomFilterOptions() };

    public static TheoryData<string, bool, bool, ParquetPageVersion> Layouts => new()
    {
        { "none", true, true, ParquetPageVersion.V1 },
        { "all", true, true, ParquetPageVersion.V2 },
        { "some", true, false, ParquetPageVersion.V1 }, // chunks with a filter next to chunks without
        { "some", false, true, ParquetPageVersion.V2 },
        { "all", false, false, ParquetPageVersion.V1 },
    };

    [Theory]
    [MemberData(nameof(Layouts))]
    public void TheFooter_IsWhatItsModelWrites_AndMetadataDescribesIt(string bloom, bool pageIndex, bool sizeStatistics, ParquetPageVersion version)
    {
        RecordBatch batch = ParquetWriterBloomTests.Mixed(3000, 11);
        string path = System.IO.Path.Combine(_dir, "footer.parquet");
        ParquetMetadata written;
        using (var writer = ParquetWriter.Create(path, batch.Schema, new ParquetWriteOptions
        {
            RowGroupRows = 1000, DataPageRows = 100, DataPageVersion = version, PageIndex = pageIndex, SizeStatistics = sizeStatistics,
            Columns = bloom switch
            {
                "all" => ParquetWriterBloomTests.AllColumns(Bloom),
                "some" => new() { ["few"] = Bloom, ["dbl"] = Bloom, ["s.b"] = Bloom },
                _ => null,
            },
        }))
        {
            writer.Write(batch.Slice(0, 1700));
            writer.Write(batch.Slice(1700, 1300));
            writer.Complete();
            written = writer.Metadata;
        }

        byte[] file = File.ReadAllBytes(path);
        int length = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 8));
        ReadOnlySpan<byte> footer = file.AsSpan(file.Length - 8 - length, length);
        FileMetaData model = FileMetaData.Parse(footer);
        var thrift = new ThriftCompactWriter();
        model.Write(thrift);
        Assert.Equal(footer.ToArray(), thrift.WrittenSpan.ToArray());

        Assert.Equal(3, model.RowGroups.Length);
        ColumnMetaData[] chunks = [.. model.RowGroups.SelectMany(g => g.Columns).Select(c => c.MetaData!)];
        Assert.Equal(bloom != "none", chunks.Any(c => c.BloomFilterOffset is not null));
        Assert.Equal(bloom == "some", chunks.Any(c => c.BloomFilterOffset is null) && chunks.Any(c => c.BloomFilterOffset is not null));
        Assert.Equal(pageIndex, model.RowGroups.SelectMany(g => g.Columns).All(c => c.OffsetIndexOffset is not null));

        var again = new ThriftCompactWriter();
        written.Thrift!.Write(again);
        Assert.Equal(footer.ToArray(), again.WrittenSpan.ToArray());
    }
}
