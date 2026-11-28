using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.Parquet.ParquetPruningTests;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Column chunks read page by page from a file (<see cref="ChunkReading.Stream"/>): each byte of the file is read
/// once, whatever the pages' sizes against the read window (pages of about the window's size were read twice: the
/// window filled at a header, then again at the body it cut short).
/// </summary>
public class ParquetPageWindowTests
{
    private static byte[] File(int rows, int pageBytes)
    {
        var names = new StringArray.Builder();
        var ids = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            names.Append($"v{i:D10}");
            ids.Append(i);
        }
        var schema = new Apache.Arrow.Schema([new Field("s", StringType.Default, false), new Field("id", Int64Type.Default, false)], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions
        {
            Compression = NoCompression.Instance,
            Dictionary = false,
            DataPageBytes = pageBytes,
            DataPageRows = int.MaxValue,
            RowGroupRows = rows,
        }))
        {
            writer.Write(new RecordBatch(schema, [names.Build(), ids.Build()], rows));
            writer.Complete();
        }
        return stream.ToArray();
    }

    [Theory]
    [InlineData(1 << 20)]           // pages of the window's size
    [InlineData((1 << 20) + 7_000)] // a little larger
    [InlineData(300 << 10)]         // several to a window
    [InlineData(5 << 20)]           // larger than the window
    public void EachByteOfTheChunks_IsReadOnce(int pageBytes)
    {
        byte[] bytes = File(800_000, pageBytes);
        int dataEnd = bytes.Length - 8 - BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8));
        var source = new RecordingSource(bytes);
        using var file = ParquetFile.Open(source);
        int before = source.Reads.Count;
        using var reader = file.OpenReader(new ParquetReadOptions { Reading = ChunkReading.Stream, MemoryMap = ParquetMemoryMap.Off, MaxDegreeOfParallelism = 1 });
        long rows = 0;
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) rows += batch.Length;
        }
        Assert.Equal(800_000, rows);
        long read = source.Reads.Skip(before).Where(r => r.Offset < dataEnd).Sum(r => (long)r.Length);
        // The data's bytes: after the leading magic, up to the footer (page indexes, not read here, included).
        long data = dataEnd - 4;
        Assert.InRange(read, data * 9 / 10, data + data / 50); // never 2 % more than the data
    }
}
