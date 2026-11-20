using Apache.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet;
using Field = Apache.Arrow.Field;
using Schema = Apache.Arrow.Schema;

namespace Polars.Config.Tests;

/// <summary>
/// The Parquet writer's native scratch is not zeroed: a block the pool hands out again holds whatever its last user
/// left. Tests elsewhere poison fresh blocks with 0xCC, which a stamp array never mistakes for a stamp; here the pool
/// is primed with blocks full of the first page's stamp, as a reused block may well be. The pool is process-wide,
/// hence here, in the serial project (poison off, timer off).
/// </summary>
public sealed unsafe class ParquetWriterReusedMemoryTests : IDisposable
{
    public ParquetWriterReusedMemoryTests()
    {
        NativeMemoryPool.TimerEnabled = false;
        NativeMemoryPool.Trim();
    }

    public void Dispose()
    {
        NativeMemoryPool.Trim();
        NativeMemoryPool.TimerEnabled = true;
    }

    [Fact]
    public void DictionaryPageBounds_DoNotTrustStaleStamps()
    {
        // 20 000 distinct strings, each five times, in one dictionary-encoded page: the page bounds come from the
        // distinct entries, each bounded once per page as its stamp says (the stamps' array then takes an 80 KiB block).
        const int Distinct = 20_000;
        var builder = new StringArray.Builder();
        for (int i = 0; i < 5 * Distinct; i++) builder.Append($"v{i * 7919 % Distinct:D5}");
        StringArray values = builder.Build();
        var schema = new Schema([new Field("s", Apache.Arrow.Types.StringType.Default, false)], null);
        var batch = new RecordBatch(schema, [values], values.Length);

        // Blocks of that size class, full of 1 (the first page's stamp), freed for the writer to get; and of the class
        // below, which a smaller request takes before borrowing one of those (and handing it back with its own bytes,
        // on top of the class's free blocks: the stamps would get that one).
        List<nint> primed = [];
        foreach (long size in (long[])[64 * 1024, Distinct * sizeof(int)])
            for (int b = 0; b < 64; b++)
            {
                byte* block = NativeMemoryPool.Alloc(size);
                new Span<int>(block, (int)(NativeMemoryPool.UsableSize(block) / sizeof(int))).Fill(1);
                primed.Add((nint)block);
            }
        foreach (nint block in primed) NativeMemoryPool.Free((void*)block);

        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, [batch], new ParquetWriteOptions
        {
            DataPageRows = 5 * Distinct,
            DataPageBytes = 8 << 20,
            MaxDegreeOfParallelism = 1,
            LeaveOpen = true,
        });
        stream.Position = 0;
        using var file = ParquetFile.Open(stream);
        var chunk = file.Metadata.Thrift!.RowGroups[0].Columns[0].MetaData!;
        Assert.Contains(Kaminari.Seseragi.Files.Parquet.Metadata.ParquetEncoding.RleDictionary, chunk.Encodings);
        Assert.Equal("v00000"u8.ToArray(), chunk.Statistics!.MinValue);
        Assert.Equal("v19999"u8.ToArray(), chunk.Statistics.MaxValue);
    }
}
