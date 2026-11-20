using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The footer a writer keeps once the file is complete: its bytes are how the file ends; handed over, the taker holds
/// them (the writer no longer); kept, the metadata is read from them even after the writer is disposed.
/// </summary>
public class ParquetFooterHandoverTests
{
    private static readonly Apache.Arrow.Schema Ids = new([new Field("id", Int64Type.Default, true)], null);

    private static RecordBatch Batch(int rows) =>
        new(Ids, [new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => (long)i)).Build()], rows);

    [Fact]
    public void ATakenFooter_HoldsTheBytesEndingTheFile()
    {
        using var stream = new MemoryStream();
        var writer = ParquetWriter.Create(stream, Ids, new ParquetWriteOptions { LeaveOpen = true, RowGroupRows = 4 });
        using (RecordBatch batch = Batch(10)) writer.Write(batch);
        writer.Complete();
        ParquetFooter footer = writer.TakeFooter();
        writer.Dispose();
        byte[] file = stream.ToArray();
        int length = footer.Thrift.Length;
        Assert.True(footer.HasBytes);
        Assert.True(footer.FileTail.Span.SequenceEqual(file.AsSpan(file.Length - length - 8)));
        Assert.True(footer.Thrift.SequenceEqual(file.AsSpan(file.Length - length - 8, length)));
        Assert.Throws<InvalidOperationException>(() => writer.Metadata); // handed over
        Assert.Throws<InvalidOperationException>(writer.TakeFooter);
        Assert.Equal(10, footer.Metadata.RowCount);
        Assert.Equal(3, footer.Metadata.RowGroups.Count);

        footer.Dispose();
        Assert.False(footer.HasBytes);
        Assert.True(footer.Thrift.IsEmpty);
        Assert.Equal(10, footer.Metadata.RowCount); // parsed before: kept

        ParquetFooter unread = ParquetFooter.Of(file.AsSpan(file.Length - length - 8, length));
        unread.Dispose();
        Assert.Throws<ObjectDisposedException>(() => unread.Metadata);
        Assert.Throws<ObjectDisposedException>(() => unread.FileTail);
        Assert.Equal(10, new ParquetFooter(ParquetMetadata.FromFile(file)).Metadata.RowCount); // a footer read elsewhere
    }

    [Fact]
    public void AFooterKept_IsReadAfterTheWriterIsDisposed()
    {
        using var stream = new MemoryStream();
        var writer = ParquetWriter.Create(stream, Ids, new ParquetWriteOptions { LeaveOpen = true });
        using (RecordBatch batch = Batch(10)) writer.Write(batch);
        writer.Dispose(); // completes the file
        Assert.Equal(10, writer.Metadata.RowCount);
        Assert.Equal(ParquetMetadata.FromFile(stream.ToArray()).RowGroups.Count, writer.Metadata.RowGroups.Count);
        Assert.Throws<InvalidOperationException>(writer.TakeFooter); // freed on dispose
    }
}
