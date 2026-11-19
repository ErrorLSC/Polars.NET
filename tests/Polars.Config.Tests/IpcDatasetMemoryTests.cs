using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Field = Apache.Arrow.Field;

namespace Polars.Config.Tests;

/// <summary>
/// An IPC dataset read leaves no batch memory behind once its batches are disposed, whether the consumer stops early,
/// a file fails midway, an asynchronous read is cancelled or every row is read; on one thread (file after file) and
/// through the window; with dictionaries unified (the unifier's dictionaries and the file memory it remembers freed)
/// or not. The counts of live batch memories and memory owners are process-wide, hence here, in the serial project.
/// </summary>
public class IpcDatasetMemoryTests
{
    private const int Files = 40, Rows = 30, BatchRows = 7;

    private static MemoryFileStore ManyFiles(bool damagedMiddle)
    {
        var store = new MemoryFileStore();
        var dictionaryType = new DictionaryType(Int16Type.Default, StringType.Default, false);
        var schema = new Apache.Arrow.Schema([new Field("x", Int64Type.Default, true), new Field("y", StringViewType.Default, true), new Field("d", dictionaryType, true)], null);
        for (int f = 0; f < Files; f++)
        {
            if (damagedMiddle && f == Files / 2)
            {
                store.Add($"m/f{f:D3}.arrow", (byte[])[.. "ARROW1\0\0"u8, .. new byte[64], .. "ARROW1"u8]);
                continue;
            }
            using var stream = new MemoryStream();
            using (IpcWriter writer = IpcWriter.Create(stream, schema, new IpcWriteOptions { Compression = f % 2 == 0 ? NoCompression.Instance : Lz4FrameCompression.Instance }))
            {
                for (int at = 0; at < Rows; at += BatchRows)
                {
                    int n = Math.Min(BatchRows, Rows - at);
                    long first = (long)f * Rows + at;
                    StringArray entries = new StringArray.Builder().AppendRange(Enumerable.Range(0, 12).Select(i => $"category {f % 5} {i}")).Build();
                    using var batch = new RecordBatch(schema,
                    [
                        new Int64Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => first + i)).Build(),
                        new StringViewArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => $"a string long enough to leave the view {first + i}")).Build(),
                        new DictionaryArray(dictionaryType, new Int16Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => (short)((first + i) % 12))).Build(), entries),
                    ], n);
                    writer.Write(batch);
                }
                writer.Complete();
            }
            store.Add($"m/f{f:D3}.arrow", stream.ToArray());
        }
        return store;
    }

    [Theory]
    [InlineData("stop", 1, false)]
    [InlineData("stop", 8, false)]
    [InlineData("stop", 1, true)]
    [InlineData("stop", 8, true)]
    [InlineData("error", 1, false)]
    [InlineData("error", 8, true)]
    [InlineData("cancel", 1, true)]
    [InlineData("cancel", 8, false)]
    [InlineData("cancel", 8, true)]
    [InlineData("all", 1, true)]
    [InlineData("all", 8, false)]
    [InlineData("all", 8, true)]
    public async Task AReading_LeavesNoBatchMemoryBehind(string how, int threads, bool unify)
    {
        MemoryFileStore store = ManyFiles(damagedMiddle: how == "error");
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long before = NativeBatchMemory.Live, owners = NativeMemoryOwner.Live;
        List<RecordBatch> kept = [];
        using (var dataset = IpcDataset.Open("m/", new IpcDatasetOptions { Store = store, FilePathColumn = "file", UnifyDictionaries = unify }))
        using (IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads, RowIndexColumn = threads == 1 ? null : "n" }))
        {
            switch (how)
            {
                case "stop":
                {
                    using IEnumerator<RecordBatch> batches = reader.ReadBatches().GetEnumerator();
                    for (int i = 0; i < 3 && batches.MoveNext(); i++) kept.Add(batches.Current); // kept past the reading: they share the unified dictionaries
                    break;
                }
                case "error":
                    Assert.Throws<IpcFormatException>(() =>
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
                    Assert.Equal(Files * Rows, reader.ReadBatches().Sum(b =>
                    {
                        using (b) return b.Length;
                    }));
                    break;
            }
        }
        // The batches kept still read their values once the reading is over.
        foreach (RecordBatch batch in kept)
        {
            var d = (DictionaryArray)batch.Column("d");
            for (int i = 0; i < batch.Length; i++)
            {
                int index = d.Indices switch { Int16Array a => a.GetValue(i)!.Value, Int32Array a => a.GetValue(i)!.Value, var a => throw new InvalidOperationException(a.GetType().Name) };
                Assert.StartsWith("category", ((StringArray)d.Dictionary).GetString(index));
            }
            batch.Dispose();
        }
        Assert.Equal(before, NativeBatchMemory.Live);
        Assert.Equal(owners, NativeMemoryOwner.Live); // file memory, unified dictionaries' blocks and the memory they remember
    }
}
