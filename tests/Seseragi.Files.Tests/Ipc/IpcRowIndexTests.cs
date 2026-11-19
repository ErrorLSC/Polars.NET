using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// <see cref="IpcReadOptions.RowIndexColumn"/>: every row numbered in the order of the data, before any filter, however
/// the batches are read (in order or side by side, sync or async, one batch alone, from a file or a stream), cut to
/// <see cref="IpcReadOptions.MaxRows"/> or filtered.
/// </summary>
public class IpcRowIndexTests
{
    private const int Batches = 9, Rows = 1000;

    /// <summary>A file (or stream) whose column 'v' holds each row's number; batches of 1,000 rows (the fourth empty).</summary>
    private static byte[] Data(IpcFormat format, IIpcCompression codec)
    {
        var schema = new Schema([new Field("v", Int64Type.Default, false), new Field("s", StringType.Default, true)], null);
        using var memory = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(memory, schema, new IpcWriteOptions { Format = format, Compression = codec, LeaveOpen = true }))
        {
            long row = 0;
            for (int b = 0; b < Batches; b++)
            {
                int rows = b == 3 ? 0 : Rows;
                using var batch = new RecordBatch(schema,
                [
                    new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => row + i)).Build(),
                    new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"s{row + i}")).Build(),
                ], rows);
                writer.Write(batch);
                row += rows;
            }
        }
        return memory.ToArray();
    }

    private static readonly ScalarExpr Some = ScalarExpr.NewBinary(BinaryOp.And,
        ScalarExpr.NewBinary(BinaryOp.GtEq, ScalarExpr.NewColumn("v"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(1500))),
        ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("v"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(5300))));

    /// <summary>The row index and 'v' of the batches: each row's number twice (or the index alone when 'v' is not read).</summary>
    private static List<(long Index, long V)> Pairs(IEnumerable<RecordBatch> batches)
    {
        List<(long, long)> pairs = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch)
            {
                Assert.Equal("row", batch.Schema.FieldsList[0].Name);
                var index = (Int64Array)batch.Column(0);
                var v = batch.Schema.GetFieldIndex("v") is int at and >= 0 ? (Int64Array)batch.Column(at) : null;
                for (int i = 0; i < batch.Length; i++) pairs.Add((index.GetValue(i)!.Value, v?.GetValue(i) ?? index.GetValue(i)!.Value));
            }
        }
        return pairs;
    }

    public static TheoryData<int, string> Ways => new()
    {
        { 1, "none" }, { 8, "none" }, { 1, "lz4" }, { 8, "lz4" },
    };

    [Theory]
    [MemberData(nameof(Ways))]
    public async Task Rows_AreNumberedInOrder_BeforeAnyFilter(int parallelism, string codec)
    {
        byte[] file = Data(IpcFormat.File, codec == "lz4" ? Lz4FrameCompression.Instance : NoCompression.Instance);
        byte[] stream = Data(IpcFormat.Stream, codec == "lz4" ? Lz4FrameCompression.Instance : NoCompression.Instance);
        long total = (Batches - 1) * Rows;
        foreach (var (options, expected) in (IReadOnlyList<(IpcReadOptions, long[])>)
            [
                (new IpcReadOptions(), [.. LongRange(0, total)]),
                (new IpcReadOptions { MaxRows = 2500 }, [.. LongRange(0, 2500)]),
                (new IpcReadOptions { Filter = Some, FilterRows = true }, [.. LongRange(1500, 5300 - 1500)]),
                (new IpcReadOptions { Filter = Some, FilterRows = true, MaxRows = 700 }, [.. LongRange(1500, 700)]),
                (new IpcReadOptions { Columns = ["s"] }, [.. LongRange(0, total)]),
            ])
        {
            IpcReadOptions read = options with { RowIndexColumn = "row", MaxDegreeOfParallelism = parallelism };
            using (IpcFile opened = IpcFile.Open(file))
            using (IpcReader reader = opened.OpenReader(read))
            {
                Assert.Equal("row", reader.Schema.FieldsList[0].Name);
                List<(long Index, long V)> pairs = Pairs(reader.ReadBatches());
                Assert.Equal(expected, pairs.Select(p => p.Index));
                Assert.All(pairs, p => Assert.Equal(p.V, p.Index));
                Assert.Equal(expected, Pairs(await ToListAsync(reader.ReadBatchesAsync())).Select(p => p.Index));
            }
            using (IpcStreamReader reader = IpcStreamReader.Open(new MemoryStream(stream), read))
                Assert.Equal(expected, Pairs(reader.ReadBatches()).Select(p => p.Index));
        }
    }

    [Fact]
    public void OneBatch_IsNumberedAfterTheBatchesBeforeIt()
    {
        using IpcFile file = IpcFile.Open(Data(IpcFormat.File, NoCompression.Instance));
        using IpcReader reader = file.OpenReader(new IpcReadOptions { RowIndexColumn = "row" });
        foreach (int b in (int[])[5, 0, 8, 3])
        {
            long first = (b > 3 ? b - 1 : b) * (long)Rows;
            List<(long Index, long V)> pairs = Pairs([reader.ReadBatch(b)]);
            Assert.Equal(b == 3 ? (IEnumerable<long>)[] : LongRange(first, Rows), pairs.Select(p => p.Index));
        }
    }

    [Fact]
    public void TheRowIndex_NeedsAFreeName()
    {
        using IpcFile file = IpcFile.Open(Data(IpcFormat.File, NoCompression.Instance));
        Assert.Throws<ArgumentException>(() => file.OpenReader(new IpcReadOptions { RowIndexColumn = "v" }));
        Assert.Throws<ArgumentException>(() => file.OpenReader(new IpcReadOptions { RowIndexColumn = "" }));
        using IpcReader other = file.OpenReader(new IpcReadOptions { RowIndexColumn = "v", Columns = ["s"] }); // 'v' not read: free
        Assert.Equal(["v", "s"], other.Schema.FieldsList.Select(f => f.Name));
    }

    private static IEnumerable<long> LongRange(long start, long count)
    {
        for (long i = 0; i < count; i++) yield return start + i;
    }

    private static async Task<List<RecordBatch>> ToListAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        List<RecordBatch> list = [];
        await foreach (RecordBatch batch in batches) list.Add(batch);
        return list;
    }
}
