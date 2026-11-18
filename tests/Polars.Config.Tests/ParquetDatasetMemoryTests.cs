using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Field = Apache.Arrow.Field;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Polars.Config.Tests;

/// <summary>
/// A dataset whose files' columns convert (widened, filled with nulls, handed on as they are, filtered again after
/// converting, cut by a limit, or failing on a value out of range) leaves no batch memory behind once its batches are
/// disposed. The count of live batch memories is process-wide, hence here, in the serial project.
/// </summary>
public class ParquetDatasetMemoryTests
{
    private static void Put(MemoryFileStore store, string path, params (string Name, IArrowArray Values)[] columns)
    {
        var schema = new Apache.Arrow.Schema([.. columns.Select(c => new Field(c.Name, c.Values.Data.DataType, true))], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = 3 }))
        {
            writer.Write(new RecordBatch(schema, [.. columns.Select(c => c.Values)], columns[0].Values.Length));
            writer.Complete();
        }
        store.Add(path, stream.ToArray());
    }

    private static StructArray Struct(IArrowArray a) =>
        new(new StructType([new Field("a", a.Data.DataType, true)]), a.Length, [a], ArrowBuffer.Empty, 0);

    private static MemoryFileStore Files()
    {
        var store = new MemoryFileStore();
        Put(store, "d/a.parquet", ("x", new DoubleArray.Builder().AppendRange([0.5, 1.5, 2.5, 3.5]).Build()),
            ("y", new StringArray.Builder().AppendRange(["a long string value kept as a view", "b", "c", "d"]).Build()),
            ("s", Struct(new Int64Array.Builder().AppendRange([1, 2, 3, 4]).Build())));
        Put(store, "d/b.parquet", ("x", new Int64Array.Builder().AppendRange([7, 1L << 53, (1L << 53) + 1, 9, 10]).Build()),
            ("s", Struct(new Int32Array.Builder().AppendRange([5, 6, 7, 8, 9]).Build())));
        return store;
    }

    [Theory]
    [InlineData("all", null)]
    [InlineData("some", null)]
    [InlineData("none", null)]
    [InlineData("every", null)]
    [InlineData("some", 1L)]
    [InlineData("every", 6L)]
    public void ConvertedAndFilteredBatches_LeaveNoBatchMemoryBehind(string filter, long? limit)
    {
        MemoryFileStore store = Files();
        ScalarExpr x = ScalarExpr.NewColumn("x");
        ScalarExpr? expression = filter switch
        {
            "some" => ScalarExpr.NewBinary(BinaryOp.Eq, x, ScalarExpr.NewLiteral(LiteralValue.NewFloat64(9007199254740992.0))),
            "none" => ScalarExpr.NewBinary(BinaryOp.Eq, x, ScalarExpr.NewLiteral(LiteralValue.NewFloat64(12345.0))),
            "every" => ScalarExpr.NewBinary(BinaryOp.Gt, x, ScalarExpr.NewLiteral(LiteralValue.NewFloat64(-1e30))),
            _ => null,
        };
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long before = NativeBatchMemory.Live;
        long rows = 0;
        using (var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store, AllowLossyCasts = true, AllowMissingColumns = true, FilePathColumn = "file" }))
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = expression, FilterRows = expression is not null, MaxRows = limit, RowIndexColumn = "n" }))
        {
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) rows += batch.Length;
            }
            if (expression is not null) Assert.Equal(1, reader.FilesFilteredAfter);
        }
        Assert.Equal(filter switch { "some" => Math.Min(2, limit ?? 2), "none" => 0, _ => Math.Min(9, limit ?? 9) }, rows);
        Assert.Equal(before, NativeBatchMemory.Live);
    }

    [Fact]
    public void AValueOutOfRange_LeavesNoBatchMemoryBehind()
    {
        var store = new MemoryFileStore();
        Put(store, "d/a.parquet", ("t", new TimestampArray.Builder(TimeUnit.Nanosecond).Append(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero)).Build()),
            ("y", new StringArray.Builder().Append("a").Build()));
        Put(store, "d/b.parquet", ("t", new TimestampArray.Builder(TimeUnit.Millisecond).AppendRange([new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2300, 1, 1, 0, 0, 0, TimeSpan.Zero)]).Build()), ("y", new StringArray.Builder().AppendRange(["b", "c"]).Build()));
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long before = NativeBatchMemory.Live;
        // One value per batch: the second fails after the first was converted (the unit's converted batches are freed).
        using (var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store }))
        using (var reader = dataset.OpenReader(new ParquetReadOptions { BatchSize = 1, MaxDegreeOfParallelism = 8 }))
        {
            Assert.Throws<ParquetFormatException>(() =>
            {
                foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            });
        }
        Assert.Equal(before, NativeBatchMemory.Live);
    }

    private static MemoryFileStore ManyFiles(int files, bool damagedMiddle)
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < files; f++)
        {
            if (damagedMiddle && f == files / 2)
            {
                store.Add($"m/f{f:D3}.parquet", (byte[])[.. "PAR1"u8, .. new byte[64], .. "PAR1"u8]);
                continue;
            }
            Put(store, $"m/f{f:D3}.parquet", ("x", new Int64Array.Builder().AppendRange(Enumerable.Range(f * 9, 9).Select(i => (long)i)).Build()),
                ("y", new StringArray.Builder().AppendRange(Enumerable.Range(f * 9, 9).Select(i => $"a string long enough to leave the view {i}")).Build()));
        }
        return store;
    }

    /// <summary>
    /// Reading several files' row groups side by side leaves no batch memory behind when the consumer stops early,
    /// when a file fails midway, or when an asynchronous read is cancelled: the units decoded ahead are freed.
    /// </summary>
    [Theory]
    [InlineData("stop")]
    [InlineData("error")]
    [InlineData("cancel")]
    [InlineData("all")]
    public async Task TheWindow_LeavesNoBatchMemoryBehind(string how)
    {
        MemoryFileStore store = ManyFiles(60, damagedMiddle: how == "error");
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long before = NativeBatchMemory.Live;
        using (var dataset = ParquetDataset.Open("m/", new ParquetDatasetOptions { Store = store, FilePathColumn = "file" }))
        // Batches of 2 rows: a unit gives several, so stopping takes some of a unit's and leaves the others.
        using (var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 8, RowIndexColumn = "n", BatchSize = 2 }))
        {
            switch (how)
            {
                case "stop":
                {
                    using IEnumerator<RecordBatch> batches = reader.ReadBatches().GetEnumerator();
                    for (int i = 0; i < 3 && batches.MoveNext(); i++) batches.Current.Dispose();
                    break;
                }
                case "error":
                    Assert.Throws<ParquetFormatException>(() =>
                    {
                        foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
                    });
                    break;
                case "cancel":
                {
                    using var cancel = new CancellationTokenSource();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                    {
                        int n = 0;
                        await foreach (RecordBatch batch in reader.ReadBatchesAsync(cancel.Token))
                        {
                            batch.Dispose();
                            if (++n == 4) cancel.Cancel();
                        }
                    });
                    break;
                }
                default:
                    Assert.Equal(540, reader.ReadBatches().Sum(b =>
                    {
                        using (b) return b.Length;
                    }));
                    break;
            }
        }
        Assert.Equal(before, NativeBatchMemory.Live);
    }
}
