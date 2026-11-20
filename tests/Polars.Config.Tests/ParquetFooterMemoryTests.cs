using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Field = Apache.Arrow.Field;
using Schema = Apache.Arrow.Schema;

namespace Polars.Config.Tests;

/// <summary>
/// A writer keeps its file's footer in native memory once complete: freed when the writer is disposed unless handed
/// over, and when the footer cannot be written. The count of footers held is process-wide, hence here, in the serial
/// project.
/// </summary>
public class ParquetFooterMemoryTests
{
    private static readonly Schema Ids = new([new Field("id", Int64Type.Default, true)], null);

    private static RecordBatch Batch() => new(Ids, [new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build()], 3);

    [Fact]
    public void AWritersFooter_IsFreed_WhenDisposed_OrWhenItCannotBeWritten()
    {
        long held = ParquetFooter.Held;
        long size;
        using (var stream = new MemoryStream())
        {
            var writer = ParquetWriter.Create(stream, Ids, new ParquetWriteOptions { LeaveOpen = true });
            using (RecordBatch batch = Batch()) writer.Write(batch);
            writer.Complete();
            Assert.Equal(held + 1, ParquetFooter.Held);
            writer.Dispose();
            Assert.Equal(held, ParquetFooter.Held);
            Assert.Equal(3, writer.Metadata.RowCount); // read from the bytes kept on the managed heap
            size = stream.Length;
        }

        // The stream refuses the footer (the same file, all but its last bytes taken): not complete, its footer freed.
        using (var stream = new FailingAfter(size - 4))
        {
            var writer = ParquetWriter.Create(stream, Ids, new ParquetWriteOptions { LeaveOpen = true });
            using (RecordBatch batch = Batch()) writer.Write(batch);
            Assert.ThrowsAny<IOException>(writer.Complete);
            Assert.Equal(held, ParquetFooter.Held);
            writer.Dispose();
        }
        Assert.Equal(held, ParquetFooter.Held);
    }

    /// <summary>A stream that takes its first bytes and refuses the rest.</summary>
    private sealed class FailingAfter(long bytes) : MemoryStream
    {
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (Length + buffer.Length > bytes) throw new IOException("The store refused the write.");
            base.Write(buffer.ToArray(), 0, buffer.Length);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (Length + count > bytes) throw new IOException("The store refused the write.");
            base.Write(buffer, offset, count);
        }
    }
}
