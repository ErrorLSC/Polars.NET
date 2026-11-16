using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetWriter"/> on several threads (<see cref="ParquetWriteOptions.MaxDegreeOfParallelism"/>): the
/// file is the same, byte for byte, whatever the number of threads, in every layout; a failing column reports what one
/// thread would have met first; a stream failing under a row group still on its way faults the writer, and disposing
/// waits for it; an explicit <see cref="ParquetWriter.EndRowGroup"/> returns once its row group reached the stream.
/// </summary>
public sealed class ParquetWriterParallelTests
{
    private static byte[] Written(IEnumerable<RecordBatch> batches, Apache.Arrow.Schema schema, ParquetWriteOptions options)
    {
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, batches, options with { LeaveOpen = true });
        return stream.ToArray();
    }

    public static TheoryData<string> Layouts() => ["flat_v1", "flat_v2_zstd", "flat_columns", "nested", "nested_v2_small", "groups_by_bytes"];

    [Theory]
    [MemberData(nameof(Layouts))]
    public void TheFile_IsTheSame_WhateverTheThreads(string layout)
    {
        RecordBatch wide = ParquetWriteData.Wide(71, 24_000);
        var (nested, _) = ParquetNestedData.Batch(72, 12_000);
        (RecordBatch batch, ParquetWriteOptions options) = layout switch
        {
            "flat_v1" => (wide, new ParquetWriteOptions { RowGroupRows = 9000, DataPageRows = 3000 }),
            "flat_v2_zstd" => (wide, new ParquetWriteOptions { DataPageVersion = ParquetPageVersion.V2, Compression = new ZstandardCompression(), DataPageBytes = 20_000 }),
            "flat_columns" => (wide, new ParquetWriteOptions
            {
                RowGroupRows = 14_000,
                Columns = new Dictionary<string, ParquetColumnOptions>
                {
                    ["i64"] = new() { Encoding = ParquetValueEncoding.DeltaBinaryPacked, BloomFilter = new() },
                    ["str"] = new() { Encoding = ParquetValueEncoding.DeltaByteArray, BloomFilter = new() { DistinctValues = 100 } },
                    ["f64"] = new() { Encoding = ParquetValueEncoding.ByteStreamSplit, Dictionary = false },
                    ["b"] = new() { Encoding = ParquetValueEncoding.Rle },
                },
            }),
            "nested" => (nested, new ParquetWriteOptions { RowGroupRows = 4000 }),
            "nested_v2_small" => (nested, new ParquetWriteOptions { DataPageVersion = ParquetPageVersion.V2, DataPageRows = 300, DataPageBytes = 8000 }),
            _ => (wide, new ParquetWriteOptions { RowGroupBytes = 200_000, DataPageRows = 1000 }),
        };
        // Batches of uneven sizes (a short one, then 3001 rows each), so that segments start mid-page.
        int n = batch.Length;
        RecordBatch[] batches = [batch.Slice(0, 777), .. Enumerable.Range(0, (n - 777 + 3000) / 3001).Select(k => batch.Slice(777 + k * 3001, Math.Min(3001, n - 777 - k * 3001)))];
        byte[] one = Written(batches, batch.Schema, options with { MaxDegreeOfParallelism = 1 });
        foreach (int threads in (int[])[2, 3, 16])
        {
            byte[] many = Written(batches, batch.Schema, options with { MaxDegreeOfParallelism = threads });
            Assert.True(one.AsSpan().SequenceEqual(many), $"{layout}: {threads} threads write another file ({many.Length} bytes against {one.Length})");
        }
        if (layout == "groups_by_bytes")
        {
            using var file = ParquetFile.Open(new MemoryStream(one));
            Assert.True(file.Metadata.RowGroups.Count > 3);
        }
    }

    [Fact]
    public void AFailingColumn_ReportsTheFirstColumnsError()
    {
        // Column 1 holds a date64 that is no whole day, column 3 a decimal wider than its precision: one thread meets column 1 first.
        RecordBatch batch = Batch(
            ("ok", Fixed(Int64Type.Default, [.. Enumerable.Range(0, 5000).Select(i => (long?)i)]), true),
            ("day", Fixed(Date64Type.Default, [.. Enumerable.Range(0, 5000).Select(i => (long?)(i == 4000 ? 1 : 0))]), true),
            ("ok2", Fixed(Int32Type.Default, [.. Enumerable.Range(0, 5000).Select(i => (int?)i)]), true),
            ("dec", Fixed(new Decimal128Type(5, 0), [.. Enumerable.Range(0, 5000).Select(i => (Int128?)(i == 10 ? 1_000_000 : i))]), true));
        foreach (int threads in (int[])[1, 8])
        {
            var e = Assert.Throws<NotSupportedException>(() =>
                ParquetWriter.Write(new MemoryStream(), batch.Schema, [batch], new ParquetWriteOptions { MaxDegreeOfParallelism = threads }));
            Assert.Contains("'day'", e.Message, StringComparison.Ordinal);
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetWriter.Create(new MemoryStream(), batch.Schema, new ParquetWriteOptions { MaxDegreeOfParallelism = 0 }));
    }

    /// <summary>A stream that fails once more than <c>limit</c> bytes were written to it.</summary>
    private sealed class FailingStream(long limit) : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (Length + count > limit) throw new IOException("the disk is full");
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer) => Write(buffer.ToArray(), 0, buffer.Length);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(8, false)]
    [InlineData(8, true)]
    public void AStreamFailingUnderARowGroup_FaultsTheWriter(int threads, bool useAsync)
    {
        RecordBatch batch = ParquetWriteData.Wide(73, 20_000);
        var stream = new FailingStream(200_000);
        var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { RowGroupRows = 4000, MaxDegreeOfParallelism = threads, LeaveOpen = true });
        Exception? failure = null;
        try
        {
            for (int at = 0; at < batch.Length; at += 2000)
            {
                if (useAsync) writer.WriteAsync(batch.Slice(at, 2000)).AsTask().GetAwaiter().GetResult();
                else writer.Write(batch.Slice(at, 2000));
            }
            writer.Complete();
        }
        catch (IOException e)
        {
            failure = e;
        }
        Assert.NotNull(failure);
        Assert.Throws<InvalidOperationException>(() => writer.Write(batch.Slice(0, 10))); // faulted
        writer.Dispose(); // waits for the row group on its way; no footer
        Assert.True(stream.Length <= 200_000);
    }

    /// <summary>A stream whose writes take a while (a row group written behind is then still on its way).</summary>
    private sealed class SlowStream : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count)
        {
            Thread.Sleep(5);
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer) => Write(buffer.ToArray(), 0, buffer.Length);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(8, false)]
    [InlineData(8, true)]
    public void EndRowGroup_ReturnsOnceItsBytesReachedTheStream(int threads, bool useAsync)
    {
        RecordBatch batch = ParquetWriteData.Wide(74, 12_000);
        var stream = new SlowStream();
        using var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { MaxDegreeOfParallelism = threads, LeaveOpen = true });
        for (int at = 0; at < batch.Length; at += 3000)
        {
            if (useAsync)
            {
                writer.WriteAsync(batch.Slice(at, 3000)).AsTask().GetAwaiter().GetResult();
                writer.EndRowGroupAsync().AsTask().GetAwaiter().GetResult();
            }
            else
            {
                writer.Write(batch.Slice(at, 3000));
                writer.EndRowGroup();
            }
            Assert.Equal(writer.BytesWritten, stream.Length);
        }
        writer.Complete();
        Assert.Equal(writer.BytesWritten, stream.Length);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void AStreamFailing_SurfacesAtTheEndRowGroupOfTheGroupThatDidNotFit(int threads)
    {
        RecordBatch batch = ParquetWriteData.Wide(75, 8_000);
        // The stream's length after each group (the same whatever the threads); the limit falls inside the fourth.
        List<long> ends = [];
        using (var whole = new MemoryStream())
        using (var writer = ParquetWriter.Create(whole, batch.Schema, new ParquetWriteOptions { LeaveOpen = true }))
        {
            for (int at = 0; at < batch.Length; at += 1000)
            {
                writer.Write(batch.Slice(at, 1000));
                writer.EndRowGroup();
                ends.Add(whole.Length);
            }
        }
        long limit = (ends[2] + ends[3]) / 2;
        var stream = new FailingStream(limit);
        using var failing = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { MaxDegreeOfParallelism = threads, LeaveOpen = true });
        int ended = 0;
        Assert.Throws<IOException>(() =>
        {
            for (int at = 0; at < batch.Length; at += 1000)
            {
                failing.Write(batch.Slice(at, 1000));
                failing.EndRowGroup();
                ended++;
            }
        });
        Assert.Equal(3, ended); // the fourth EndRowGroup failed, not a later call
        Assert.InRange(stream.Length, ends[2], limit); // the third group whole, the fourth's pieces that fitted
        Assert.Throws<InvalidOperationException>(() => failing.Write(batch.Slice(0, 10))); // faulted
    }
}
