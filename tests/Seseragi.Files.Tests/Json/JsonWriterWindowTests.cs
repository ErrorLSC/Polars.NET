using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Json;
using static Seseragi.Files.Tests.Json.JsonWriterOracleTests;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// The window of the text writers' engine (shared by the JSON and CSV writers), driven through <see cref="JsonWriter"/>:
/// pieces start only while few enough before them are unwritten, are written in order as they come, and every piece is
/// settled whatever happens (a failure in order, an abort), so nothing hangs.
/// </summary>
public class JsonWriterWindowTests
{
    private static Field F(string name, IArrowType type) => new(name, type, true);

    [Fact]
    public void TheBytes_DoNotDependOnTheWindow_Threads_Pieces_Ownership_OrFlushes()
    {
        var (schema, whole) = ReadArrow("written.arrow");
        byte[] expected;
        try { expected = Encoding.UTF8.GetBytes(JsonWriter.WriteString(schema, whole)); }
        finally { whole.ForEach(b => b.Dispose()); }
        foreach (int window in (int[])[1, 2, 3, 8])
            foreach (int threads in (int[])[1, 4, 8])
                foreach (int piece in (int[])[1, 300])
                    foreach (BatchOwnership ownership in (BatchOwnership[])[BatchOwnership.Caller, BatchOwnership.Writer])
                    {
                        var (_, batches) = ReadArrow("written.arrow");
                        try
                        {
                            // Large and small batches in turn (pieces side by side, rows the caller encodes), a flush now and then.
                            List<RecordBatch> parts = [.. batches.SelectMany(b => Parts(b))];
                            var options = new JsonWriteOptions
                            {
                                PieceBytes = piece, MaxDegreeOfParallelism = threads, WindowPieces = window, BatchOwnership = ownership, LeaveOpen = true,
                            };
                            using var output = new MemoryStream();
                            using (JsonWriter writer = JsonWriter.Create(output, schema, options))
                                for (int k = 0; k < parts.Count; k++)
                                {
                                    writer.Write(ownership == BatchOwnership.Writer ? Owned(parts[k]) : parts[k]);
                                    if (k % 3 == 2) writer.Flush();
                                }
                            AssertSameText(expected, output.ToArray());
                        }
                        finally { batches.ForEach(b => b.Dispose()); }
                    }
    }

    // A batch cut into parts of 9, 1 and the rest rows in turn (slices: rows at offsets in every array).
    private static IEnumerable<RecordBatch> Parts(RecordBatch batch)
    {
        for (int start = 0, k = 0; start < batch.Length; k++)
        {
            int length = Math.Min(k % 3 == 0 ? 9 : k % 3 == 1 ? 1 : batch.Length, batch.Length - start);
            yield return batch.Slice(start, length);
            start += length;
        }
    }

    // A batch of its own, its buffers copied to managed memory (the writer disposes what it owns; the test keeps the original).
    private static RecordBatch Owned(RecordBatch batch) => new(batch.Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.BuildArray(Copy(a.Data)))], batch.Length);

    private static ArrayData Copy(ArrayData data) => new(data.DataType, data.Length, data.NullCount, data.Offset,
        [.. data.Buffers.Select(b => new ArrowBuffer(b.Span.ToArray()))], [.. (data.Children ?? []).Select(Copy)], data.Dictionary is { } d ? Copy(d) : null);

    /// <summary>
    /// Strings not UTF-8 at two rows close enough for their pieces to be encoded at once: the first in order fails the
    /// write, whatever the window, the threads, and which piece fails first.
    /// </summary>
    [Fact]
    public void AFailure_IsTheFirstInOrder_AndNothingHangs()
    {
        var schema = new Schema([F("s", StringType.Default)], null);
        const int rows = 400;
        foreach (int bad in (int[])[0, 7, 150, 399])
        {
            using var batch = Strings(schema, rows, bad, Math.Min(rows - 1, bad + 2));
            for (int i = 0; i < 60; i++)
                foreach (BatchOwnership ownership in (BatchOwnership[])[BatchOwnership.Caller, BatchOwnership.Writer])
                {
                    var options = new JsonWriteOptions
                    {
                        PieceBytes = i % 2 == 0 ? 1 : 1 + i % 200, MaxDegreeOfParallelism = 1 + i % 8, WindowPieces = 1 + i % 8, BatchOwnership = ownership,
                    };
                    using var output = new MemoryStream();
                    var writer = JsonWriter.Create(output, schema, options);
                    // Owned: the failure surfaces at a later call (the batch's pieces are encoded behind).
                    var e = Assert.Throws<JsonValueException>(() =>
                    {
                        writer.Write(ownership == BatchOwnership.Writer ? Strings(schema, rows, bad, Math.Min(rows - 1, bad + 2)) : batch);
                        writer.Complete();
                    });
                    Assert.Equal($"Row {bad}: Column 's': a string that is not UTF-8.", e.Message);
                    writer.Dispose();
                    // What was written is whole rows, all before the failure.
                    string text = Encoding.UTF8.GetString(output.ToArray());
                    Assert.True(text.Length == 0 || text.EndsWith('\n'));
                    Assert.True(text.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length <= bad);
                }
        }
    }

    /// <summary>An abort while owned batches are on their way: disposing returns (every piece settled), nothing after it is written.</summary>
    [Fact]
    public void AnAbort_WhileBatchesAreOnTheirWay_SettlesEveryPiece()
    {
        var schema = new Schema([F("s", StringType.Default)], null);
        for (int i = 0; i < 60; i++)
        {
            using var output = new MemoryStream();
            var writer = JsonWriter.Create(output, schema, new JsonWriteOptions
            {
                PieceBytes = 1 + i % 50, MaxDegreeOfParallelism = 1 + i % 8, WindowPieces = 1 + i % 2, BatchOwnership = BatchOwnership.Writer,
            });
            for (int k = 0; k < 5; k++) writer.Write(Strings(schema, 300, -1, -1));
            writer.Abort();
            writer.Dispose();
            Assert.ThrowsAny<InvalidOperationException>(() => writer.Write(Strings(schema, 1, -1, -1)));
        }
    }

    /// <summary>
    /// An output that takes nothing: the caller's batch stops once the window's pieces are encoded (memory stays bounded,
    /// not the whole batch encoded ahead), and goes on once the output takes bytes again.
    /// </summary>
    [Fact]
    public async Task AnOutputThatTakesNothing_HoldsTheWriter_AWindowAhead()
    {
        var schema = new Schema([F("s", StringType.Default)], null);
        using var batch = Strings(schema, 2000, -1, -1);
        string expected = JsonWriter.WriteString(schema, [batch]);
        using var output = new HeldStream();
        var writer = JsonWriter.Create(output, schema, new JsonWriteOptions { PieceBytes = 100, MaxDegreeOfParallelism = 4, WindowPieces = 3 });
        Task write = Task.Run(() => writer.Write(batch));
        await Task.Delay(300);
        Assert.False(write.IsCompleted);
        output.Release();
        await write;
        writer.Dispose();
        Assert.Equal(expected, Encoding.UTF8.GetString(output.ToArray()));
    }

    // A stream whose writes wait until released.
    private sealed class HeldStream : MemoryStream
    {
        private readonly ManualResetEventSlim _released = new();

        public void Release() => _released.Set();

        public override void Write(ReadOnlySpan<byte> buffer) => Write(buffer.ToArray(), 0, buffer.Length);

        public override void Write(byte[] buffer, int offset, int count)
        {
            _released.Wait();
            base.Write(buffer, offset, count);
        }
    }

    /// <summary>Rows appended a few at a time (as a dataset's partitions are) and handed off, under a window of one piece.</summary>
    [Fact]
    public void AppendedRows_HandedOff_UnderATinyWindow_AreAllWritten()
    {
        var (schema, batches) = ReadArrow("written.arrow");
        try
        {
            string expected = JsonWriter.WriteString(schema, batches);
            foreach (int window in (int[])[1, 2])
            {
                using var output = new MemoryStream();
                using (JsonWriter writer = JsonWriter.Create(output, schema, new JsonWriteOptions { PieceBytes = 64, WindowPieces = window, MaxDegreeOfParallelism = 4, LeaveOpen = true }))
                    foreach (RecordBatch batch in batches)
                    {
                        var cells = JsonWriter.CellsOf(batch, schema);
                        try
                        {
                            for (int start = 0; start < batch.Length; start += 2)
                            {
                                writer.AppendRows(cells, start, Math.Min(2, batch.Length - start));
                                if (writer.OpenBytes > 100) writer.HandOff();
                            }
                        }
                        finally { cells.Dispose(); }
                    }
                Assert.Equal(expected, Encoding.UTF8.GetString(output.ToArray()));
            }
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    // `rows` strings "row n", those at `bad` and `bad2` not UTF-8 (-1: none).
    private static RecordBatch Strings(Schema schema, int rows, int bad, int bad2)
    {
        List<int> offsets = [0];
        List<byte> data = [];
        for (int r = 0; r < rows; r++)
        {
            data.AddRange(r == bad || r == bad2 ? [0xFF, (byte)'x'] : Encoding.UTF8.GetBytes($"row {r}"));
            offsets.Add(data.Count);
        }
        var strings = new StringArray(new ArrayData(StringType.Default, rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(offsets.SelectMany(BitConverter.GetBytes).ToArray()), new ArrowBuffer(data.ToArray())]));
        return new RecordBatch(schema, [strings], rows);
    }
}
