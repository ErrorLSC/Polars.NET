using Apache.Arrow;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetWriter.Abort"/>: a writer given up is disposed without completing its file (the open row group is
/// neither encoded nor written, no footer), refuses further writes, and does nothing once the file is complete.
/// </summary>
public sealed class ParquetWriterAbortTests
{
    public static TheoryData<int, bool> Threads() => new() { { 1, false }, { 1, true }, { 16, false }, { 16, true } };

    private static bool EndsWithFooter(MemoryStream stream) => stream.Length >= 8 && stream.ToArray().AsSpan()[^4..].SequenceEqual("PAR1"u8);

    [Theory]
    [MemberData(nameof(Threads))]
    public async Task AnAbortedWriter_IsDisposedWithoutWritingItsOpenRowGroupOrFooter(int threads, bool useAsync)
    {
        RecordBatch batch = ParquetWriteData.Wide(81, 6000);
        using var stream = new MemoryStream();
        var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { LeaveOpen = true, MaxDegreeOfParallelism = threads });
        // One row group ended (its bytes in the stream), then rows of the next one buffered.
        if (useAsync)
        {
            await writer.WriteAsync(batch.Slice(0, 4000));
            await writer.EndRowGroupAsync();
            await writer.WriteAsync(batch.Slice(4000, 2000));
        }
        else
        {
            writer.Write(batch.Slice(0, 4000));
            writer.EndRowGroup();
            writer.Write(batch.Slice(4000, 2000));
        }
        long written = stream.Length;
        Assert.True(written > 4);

        writer.Abort();
        writer.Abort(); // repeating it changes nothing
        if (useAsync) await writer.DisposeAsync();
        else writer.Dispose();

        Assert.Equal(written, stream.Length);
        Assert.False(EndsWithFooter(stream));
        Assert.Throws<InvalidOperationException>(() => writer.Metadata);
    }

    [Fact]
    public async Task AnAbortedWriter_RefusesWritesAndCompletion()
    {
        RecordBatch batch = ParquetWriteData.Wide(82, 100);
        using var stream = new MemoryStream();
        using var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { LeaveOpen = true });
        writer.Write(batch);
        writer.Abort();
        Assert.Contains("aborted", Assert.Throws<InvalidOperationException>(() => writer.Write(batch)).Message);
        Assert.Contains("aborted", (await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteAsync(batch).AsTask())).Message);
        Assert.Contains("aborted", Assert.Throws<InvalidOperationException>(writer.EndRowGroup).Message);
        Assert.Contains("aborted", Assert.Throws<InvalidOperationException>(writer.Complete).Message);
        Assert.Contains("aborted", (await Assert.ThrowsAsync<InvalidOperationException>(() => writer.CompleteAsync().AsTask())).Message);
    }

    [Fact]
    public void AbortingACompletedWriter_ChangesNothing()
    {
        RecordBatch batch = ParquetWriteData.Wide(83, 500);
        using var stream = new MemoryStream();
        var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { LeaveOpen = true });
        writer.Write(batch);
        writer.Complete();
        writer.Abort();
        writer.Dispose();
        Assert.True(EndsWithFooter(stream));
        Assert.Equal(500, writer.Metadata.RowCount);
        stream.Position = 0;
        using var file = ParquetFile.Open(stream);
        Assert.Equal(500, file.Metadata.RowCount);
    }

    [Fact]
    public async Task AbortingWhileRowGroupsAreEncoded_LeavesNoFooter()
    {
        // The writer owns the batches and encodes row groups side by side: some are still being encoded at the abort.
        using var stream = new MemoryStream();
        RecordBatch first = ParquetWriteData.Wide(84, 3000);
        var writer = ParquetWriter.Create(stream, first.Schema, new ParquetWriteOptions
        {
            LeaveOpen = true, MaxDegreeOfParallelism = 16, BatchOwnership = ParquetBatchOwnership.Writer, RowGroupRows = 1000,
        });
        await writer.WriteAsync(first);
        for (int k = 1; k < 6; k++) await writer.WriteAsync(ParquetWriteData.Wide(84 + k, 3000));
        writer.Abort();
        await writer.DisposeAsync();
        Assert.False(EndsWithFooter(stream));
        stream.Position = 0;
        Assert.ThrowsAny<Exception>(() => ParquetFile.Open(stream).Dispose());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AbortingWhileRowGroupsAreSealed_WritesNothingMore(bool useAsync)
    {
        // The caller owns the batches, on several threads: each full row group is sealed and finished by a task, so the
        // last one is still sealed (not in the stream) at the abort, rows of the next one open. Neither is written.
        using var stream = new MemoryStream();
        RecordBatch batch = ParquetWriteData.Wide(90, 5500);
        var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { LeaveOpen = true, MaxDegreeOfParallelism = 8, RowGroupRows = 1000 });
        if (useAsync) await writer.WriteAsync(batch);
        else writer.Write(batch);
        long laidOut = writer.BytesWritten;
        Assert.Equal(1, writer.PeakRowGroupsInFlight); // sealed ones, one at a time
        Assert.True(writer.PendingBytes > 0);
        writer.Abort();
        if (useAsync) await writer.DisposeAsync();
        else writer.Dispose();
        Assert.Equal(laidOut, stream.Length);
        Assert.False(EndsWithFooter(stream));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheStaticWrites_GiveTheFileUp_WhenTheBatchesFail(bool useAsync)
    {
        // The batches' enumeration fails after one batch: the helpers abort the writer, so that the stream keeps no
        // footer and the path is deleted (nothing completed for nothing).
        RecordBatch batch = ParquetWriteData.Wide(85, 300);
        using var stream = new MemoryStream();
        string path = Path.Combine(Path.GetTempPath(), $"abort-{Guid.NewGuid():N}.parquet");
        if (useAsync)
        {
            await Assert.ThrowsAsync<IOException>(() => ParquetWriter.WriteAsync(stream, batch.Schema, FailingAsync(batch), new ParquetWriteOptions { LeaveOpen = true }));
            await Assert.ThrowsAsync<IOException>(() => ParquetWriter.WriteAsync(path, batch.Schema, FailingAsync(batch)));
        }
        else
        {
            Assert.Throws<IOException>(() => ParquetWriter.Write(stream, batch.Schema, Failing(batch), new ParquetWriteOptions { LeaveOpen = true }));
            Assert.Throws<IOException>(() => ParquetWriter.Write(path, batch.Schema, Failing(batch)));
        }
        Assert.InRange(stream.Length, 0, 4); // the leading magic at most: the buffered row group was not written
        Assert.False(File.Exists(path));

        static IEnumerable<RecordBatch> Failing(RecordBatch batch)
        {
            yield return batch;
            throw new IOException("injected: the source failed");
        }

        static async IAsyncEnumerable<RecordBatch> FailingAsync(RecordBatch batch)
        {
            yield return batch;
            await Task.Yield();
            throw new IOException("injected: the source failed");
        }
    }
}
