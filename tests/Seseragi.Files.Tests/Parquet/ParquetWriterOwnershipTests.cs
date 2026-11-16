using System.Buffers;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetWriter"/> owning its batches (<see cref="ParquetBatchOwnership.Writer"/>): the file is the one
/// the caller's batches give, whatever the threads (row groups encoded side by side); every batch is disposed once,
/// after its rows are encoded, also when writing fails; row groups by bytes count the batches' Arrow bytes.
/// </summary>
public sealed class ParquetWriterOwnershipTests
{
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    private static extern ArrowBuffer OwnedBuffer(IMemoryOwner<byte> owner);

    /// <summary>Memory that counts its disposals and is overwritten when disposed (a read after it shows in the file).</summary>
    private sealed class Watched(byte[] bytes, Tracker tracker) : IMemoryOwner<byte>
    {
        private int _disposals;

        public Memory<byte> Memory => bytes;

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposals) == 1) bytes.AsSpan().Fill(0xCC);
            tracker.Disposed(_disposals);
        }
    }

    /// <summary>The buffers handed out and how often they were disposed.</summary>
    private sealed class Tracker
    {
        private int _buffers, _disposed, _twice;

        public int Buffers => Volatile.Read(ref _buffers);

        public int DisposedOnce => Volatile.Read(ref _disposed);

        public int DisposedTwice => Volatile.Read(ref _twice);

        public ArrowBuffer Watch(ArrowBuffer buffer)
        {
            Interlocked.Increment(ref _buffers);
            return OwnedBuffer(new Watched(buffer.Span.ToArray(), this));
        }

        public void Disposed(int times)
        {
            if (times == 1) Interlocked.Increment(ref _disposed);
            else Interlocked.Increment(ref _twice);
        }

        /// <summary>A copy of the batch over watched memory (every buffer, children and dictionaries included).</summary>
        public RecordBatch Copy(RecordBatch batch) =>
            new(batch.Schema, [.. Enumerable.Range(0, batch.ColumnCount).Select(c => ArrowArrayFactory.BuildArray(Copy(batch.Column(c).Data)))], batch.Length);

        private ArrayData Copy(ArrayData data) => new(data.DataType, data.Length, data.NullCount, data.Offset,
            [.. (data.Buffers ?? []).Select(Watch)], [.. (data.Children ?? []).Select(Copy)], data.Dictionary is { } dictionary ? Copy(dictionary) : null);

        public void AssertEachDisposedOnce()
        {
            Assert.Equal(0, DisposedTwice);
            Assert.Equal(Buffers, DisposedOnce);
        }
    }

    private static byte[] Written(IEnumerable<RecordBatch> batches, Apache.Arrow.Schema schema, ParquetWriteOptions options)
    {
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, batches, options with { LeaveOpen = true });
        return stream.ToArray();
    }

    /// <summary>Uneven batches (a short one, then 3001 rows each) of the batch, as slices.</summary>
    private static RecordBatch[] Uneven(RecordBatch batch)
    {
        int n = batch.Length;
        return [batch.Slice(0, 777), .. Enumerable.Range(0, (n - 777 + 3000) / 3001).Select(k => batch.Slice(777 + k * 3001, Math.Min(3001, n - 777 - k * 3001)))];
    }

    public static TheoryData<string> Layouts() => ["flat_v1", "flat_v2_zstd", "flat_columns", "nested", "nested_v2_small", "nested_many_groups"];

    [Theory]
    [MemberData(nameof(Layouts))]
    public void TheFile_IsTheCallersFile_WhateverTheThreads_EachBatchDisposedOnce(string layout)
    {
        RecordBatch wide = ParquetWriteData.Wide(81, 24_000);
        var (nested, _) = ParquetNestedData.Batch(82, 12_000);
        (RecordBatch batch, ParquetWriteOptions options) = layout switch
        {
            "flat_v1" => (wide, new ParquetWriteOptions { RowGroupRows = 9000, DataPageRows = 3000 }),
            "flat_v2_zstd" => (wide, new ParquetWriteOptions { DataPageVersion = ParquetPageVersion.V2, Compression = new ZstandardCompression(), DataPageBytes = 20_000 }),
            "flat_columns" => (wide, new ParquetWriteOptions
            {
                RowGroupRows = 4_000,
                Columns = new Dictionary<string, ParquetColumnOptions>
                {
                    ["i64"] = new() { Encoding = ParquetValueEncoding.DeltaBinaryPacked, BloomFilter = new() },
                    ["str"] = new() { Encoding = ParquetValueEncoding.DeltaByteArray, BloomFilter = new() { DistinctValues = 100 } },
                    ["f64"] = new() { Encoding = ParquetValueEncoding.ByteStreamSplit, Dictionary = false },
                },
            }),
            "nested" => (nested, new ParquetWriteOptions { RowGroupRows = 4000 }),
            "nested_v2_small" => (nested, new ParquetWriteOptions { DataPageVersion = ParquetPageVersion.V2, DataPageRows = 300, DataPageBytes = 8000 }),
            _ => (nested, new ParquetWriteOptions { RowGroupRows = 700 }), // row groups shorter than batches, many in flight
        };
        RecordBatch[] slices = Uneven(batch);
        byte[] expected = Written(slices, batch.Schema, options with { MaxDegreeOfParallelism = 1 });
        foreach (int threads in (int[])[1, 2, 3, 16])
        {
            var tracker = new Tracker();
            RecordBatch[] owned = [.. slices.Select(tracker.Copy)];
            byte[] written = Written(owned, batch.Schema, options with { MaxDegreeOfParallelism = threads, BatchOwnership = ParquetBatchOwnership.Writer });
            Assert.True(expected.AsSpan().SequenceEqual(written), $"{layout}: {threads} threads owning the batches write another file ({written.Length} bytes against {expected.Length})");
            tracker.AssertEachDisposedOnce();
        }
    }

    [Fact]
    public void RowGroups_AreEncodedSideBySide_AtMostSoManyAtOnce()
    {
        var (nested, _) = ParquetNestedData.Batch(83, 12_000);
        var tracker = new Tracker();
        var options = new ParquetWriteOptions { RowGroupRows = 500, MaxDegreeOfParallelism = 8, BatchOwnership = ParquetBatchOwnership.Writer, LeaveOpen = true };
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, nested.Schema, options))
        {
            foreach (RecordBatch batch in Uneven(nested)) writer.Write(tracker.Copy(batch));
            writer.Complete();
            Assert.InRange(writer.PeakRowGroupsInFlight, 2, writer.MaxRowGroupsInFlight);
        }
        tracker.AssertEachDisposedOnce();
        using var file = ParquetFile.Open(new MemoryStream(stream.ToArray()));
        Assert.Equal(24, file.Metadata.RowGroups.Count);
    }

    [Fact]
    public void RowGroupsInFlight_StayWithinTheirBytes_OneAloneBeyond()
    {
        var (nested, _) = ParquetNestedData.Batch(88, 12_000);
        var tracker = new Tracker();
        var options = new ParquetWriteOptions
        {
            RowGroupRows = 500, MaxDegreeOfParallelism = 8, BatchOwnership = ParquetBatchOwnership.Writer, LeaveOpen = true, InFlightBytes = 1,
        };
        byte[] expected = Written(Uneven(nested), nested.Schema, options with { MaxDegreeOfParallelism = 1, BatchOwnership = ParquetBatchOwnership.Caller });
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, nested.Schema, options))
        {
            foreach (RecordBatch batch in Uneven(nested)) writer.Write(tracker.Copy(batch));
            writer.Complete();
            Assert.Equal(1, writer.PeakRowGroupsInFlight);
        }
        tracker.AssertEachDisposedOnce();
        Assert.True(expected.AsSpan().SequenceEqual(stream.ToArray()));
    }

    [Fact]
    public void RowGroupsByBytes_CountTheBatchesArrowBytes_TheSameWhateverTheThreads()
    {
        RecordBatch wide = ParquetWriteData.Wide(84, 24_000);
        var options = new ParquetWriteOptions { RowGroupBytes = 300_000, BatchOwnership = ParquetBatchOwnership.Writer };
        byte[]? first = null;
        foreach (int threads in (int[])[2, 3, 16])
        {
            var tracker = new Tracker();
            byte[] written = Written([.. Uneven(wide).Select(tracker.Copy)], wide.Schema, options with { MaxDegreeOfParallelism = threads });
            tracker.AssertEachDisposedOnce();
            first ??= written;
            Assert.True(first.AsSpan().SequenceEqual(written), $"{threads} threads write another file");
        }
        using var file = ParquetFile.Open(new MemoryStream(first!));
        Assert.True(file.Metadata.RowGroups.Count > 3, $"{file.Metadata.RowGroups.Count} row groups");
        Assert.Equal(24_000, file.Metadata.RowGroups.Sum(g => g.RowCount));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void AFailingColumn_ReportsTheFirstError_AndEveryBatchIsDisposed(int threads)
    {
        // A date64 that is no whole day in the third row group's rows, a decimal wider than its precision in the fifth's.
        RecordBatch batch = Batch(
            ("ok", Fixed(Int64Type.Default, [.. Enumerable.Range(0, 5000).Select(i => (long?)i)]), true),
            ("day", Fixed(Date64Type.Default, [.. Enumerable.Range(0, 5000).Select(i => (long?)(i == 2500 ? 1 : 0))]), true),
            ("dec", Fixed(new Decimal128Type(5, 0), [.. Enumerable.Range(0, 5000).Select(i => (Int128?)(i == 4500 ? 1_000_000 : i))]), true));
        var tracker = new Tracker();
        var options = new ParquetWriteOptions { RowGroupRows = 1000, MaxDegreeOfParallelism = threads, BatchOwnership = ParquetBatchOwnership.Writer };
        var e = Assert.Throws<NotSupportedException>(() =>
            ParquetWriter.Write(new MemoryStream(), batch.Schema, Enumerable.Range(0, 10).Select(k => tracker.Copy(batch.Slice(k * 500, 500))), options)); // made as taken
        Assert.Contains("'day'", e.Message, StringComparison.Ordinal);
        tracker.AssertEachDisposedOnce();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void ABatchRefused_IsDisposedToo_AndTheWriterGoesOn(int threads)
    {
        RecordBatch good = Batch(("a", Fixed(Int64Type.Default, [.. Enumerable.Range(0, 3000).Select(i => (long?)i)]), true));
        RecordBatch bad = Batch(("a", Fixed(Int32Type.Default, (int?[])[1, 2, 3]), true)); // another type
        var tracker = new Tracker();
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, good.Schema, new ParquetWriteOptions
        {
            RowGroupRows = 1000, MaxDegreeOfParallelism = threads, BatchOwnership = ParquetBatchOwnership.Writer, LeaveOpen = true,
        }))
        {
            writer.Write(tracker.Copy(good));
            Assert.Throws<ArgumentException>(() => writer.Write(tracker.Copy(bad)));
            writer.Write(tracker.Copy(good));
            writer.Complete();
        }
        tracker.AssertEachDisposedOnce();
        using var file = ParquetFile.Open(new MemoryStream(stream.ToArray()));
        Assert.Equal(6000, file.Metadata.RowGroups.Sum(g => g.RowCount));
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
    [InlineData(8, false)]
    [InlineData(8, true)]
    public void AFailingStream_FaultsTheWriter_AndEveryBatchIsDisposed(int threads, bool useAsync)
    {
        RecordBatch wide = ParquetWriteData.Wide(85, 24_000);
        var tracker = new Tracker();
        var writer = ParquetWriter.Create(new FailingStream(150_000), wide.Schema, new ParquetWriteOptions
        {
            RowGroupRows = 2000, MaxDegreeOfParallelism = threads, BatchOwnership = ParquetBatchOwnership.Writer,
        });
        Exception? failure = null;
        try
        {
            foreach (RecordBatch batch in Uneven(wide))
            {
                if (useAsync) writer.WriteAsync(tracker.Copy(batch)).AsTask().GetAwaiter().GetResult();
                else writer.Write(tracker.Copy(batch));
            }
            writer.Complete();
        }
        catch (IOException e)
        {
            failure = e;
        }
        Assert.NotNull(failure);
        Assert.Throws<InvalidOperationException>(() => writer.Write(tracker.Copy(wide.Slice(0, 10)))); // faulted, the batch still taken
        writer.Dispose();
        tracker.AssertEachDisposedOnce();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void EndRowGroup_ReturnsOnceTheRowGroupsReachedTheStream(int threads)
    {
        RecordBatch wide = ParquetWriteData.Wide(86, 12_000);
        var tracker = new Tracker();
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, wide.Schema, new ParquetWriteOptions
        {
            RowGroupRows = 1000, MaxDegreeOfParallelism = threads, BatchOwnership = ParquetBatchOwnership.Writer, LeaveOpen = true,
        }))
        {
            for (int at = 0; at < wide.Length; at += 4000)
            {
                writer.Write(tracker.Copy(wide.Slice(at, 4000)));
                writer.EndRowGroup();
                Assert.Equal(writer.BytesWritten, stream.Length);
            }
            writer.Complete();
        }
        tracker.AssertEachDisposedOnce();
    }

    [Fact]
    public void AWriterDisposedUnfinished_LetsGoOfEveryBatch()
    {
        RecordBatch wide = ParquetWriteData.Wide(87, 12_000);
        var tracker = new Tracker();
        var writer = ParquetWriter.Create(new FailingStream(0), wide.Schema, new ParquetWriteOptions
        {
            RowGroupRows = 5000, MaxDegreeOfParallelism = 8, BatchOwnership = ParquetBatchOwnership.Writer,
        });
        // The first row group closes and fails to be written later; the second stays open.
        try
        {
            writer.Write(tracker.Copy(wide.Slice(0, 6000)));
        }
        catch (IOException)
        {
            // when the magic bytes are written at once
        }
        Assert.ThrowsAny<Exception>(() => writer.Dispose());
        tracker.AssertEachDisposedOnce();
    }
}
