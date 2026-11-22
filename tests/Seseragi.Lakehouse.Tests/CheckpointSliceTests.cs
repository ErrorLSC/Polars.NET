using Apache.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// Checkpoint rows read from a batch that starts at an offset (a slice): the same actions as the same rows of the whole
/// batch. Apache.Arrow slices a struct's fields as it slices the struct, so a row is the same index in both (adding the
/// struct's offset read other rows, or past the end).
/// </summary>
public class CheckpointSliceTests
{
    private const string Schema = """
        {"type":"struct","fields":[
          {"name":"id","type":"long","nullable":true,"metadata":{}},
          {"name":"s","type":"string","nullable":true,"metadata":{}},
          {"name":"p","type":"string","nullable":true,"metadata":{}}
        ]}
        """;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ASlicedBatch_ReadsItsOwnRows(bool statsAsText)
    {
        RecordBatch batch = CheckpointBatch(statsAsText);
        try
        {
            Assert.Equal(!statsAsText, !((Apache.Arrow.Types.StructType)batch.Schema.GetFieldByName("add").DataType).Fields.Any(f => f.Name == "stats"));
            List<string> whole = Read(batch);
            Assert.True(whole.Count > 40, $"{whole.Count} actions");
            foreach (int cut in (int[])[1, 7, batch.Length / 2, batch.Length - 3])
            {
                // The batch read in two slices: the same actions, in the same order.
                List<string> parts = [.. Read(batch.Slice(0, cut)), .. Read(batch.Slice(cut, batch.Length - cut))];
                Assert.Equal(whole, parts);
            }
        }
        finally
        {
            batch.Dispose();
        }
    }

    [Fact]
    public void ParsedStatistics_OfASlice_AreItsOwnRows()
    {
        RecordBatch batch = CheckpointBatch(statsAsText: false);
        try
        {
            var add = (StructArray)batch.Column("add");
            var stats = (StructArray)add.Fields[((Apache.Arrow.Types.StructType)add.Data.DataType).GetFieldIndex("stats_parsed")];
            using var text = new ParsedStatsText(new HashSet<string>());
            var arena = new ByteArena();
            List<string?> whole = [.. Enumerable.Range(0, stats.Length).Select(i => Text(text.Utf8(stats, i, arena)))];
            Assert.Contains(whole, t => t is not null && t.Contains("\"minValues\""));
            var slice = (StructArray)stats.Slice(5, stats.Length - 9);
            for (int i = 0; i < slice.Length; i++) Assert.Equal(whole[5 + i], Text(text.Utf8(slice, i, arena)));
        }
        finally
        {
            batch.Dispose();
        }

        static string? Text(ReadOnlyMemory<byte>? utf8) => utf8 is { } bytes ? System.Text.Encoding.UTF8.GetString(bytes.Span) : null;
    }

    // A checkpoint's one batch: adds (statistics, partition values, tags) and removes (tombstones).
    private static RecordBatch CheckpointBatch(bool statsAsText)
    {
        var fs = new MemoryFileSystem();
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 };
        Dictionary<string, string> configuration = statsAsText ? [] : new() { [CheckpointStatistics.WriteJsonKey] = "false" };
        Transaction create = Transaction.Create(fs, protocol, new MetadataAction { Id = "t", SchemaString = Schema, PartitionColumns = ["p"], Configuration = configuration });
        for (int n = 0; n < 40; n++)
        {
            create.Add(new AddAction
            {
                Path = $"p={n % 3}/f{n}.parquet",
                PartitionValues = new Dictionary<string, string?> { ["p"] = n % 5 == 0 ? null : $"{n % 3}" },
                Size = 100 + n,
                Stats = n % 6 == 0 ? null
                    : $$"""{"numRecords":{{n + 1}},"minValues":{"id":{{n}},"s":"a{{n}}"},"maxValues":{"id":{{n * 10}},"s":"z{{n}}"},"nullCount":{"id":0,"s":""" + $"{n % 2}}}}}",
                Tags = n % 4 == 0 ? new Dictionary<string, string?> { ["t"] = $"v{n}" } : null,
            });
        }
        // Tombstones too: removes of other files.
        for (int n = 0; n < 40; n += 7) create.Add(new RemoveAction { Path = $"p={n % 3}/gone{n}.parquet", DeletionTimestamp = 5, PartitionValues = new Dictionary<string, string?> { ["p"] = $"{n % 3}" } });
        create.Commit();
        byte[] file = fs.ReadAll(Checkpoints.Write(fs, Snapshot.Load(fs)))!;
        using ParquetFile parquet = ParquetFile.Open(file.AsMemory());
        using ParquetReader reader = parquet.OpenReader(new ParquetReadOptions { BinaryAsString = true, ViewTypes = false, MaxDegreeOfParallelism = 1 });
        List<RecordBatch> batches = [.. reader.ReadBatches()];
        Assert.Single(batches);
        return batches[0];
    }

    private static List<string> Read(RecordBatch batch)
    {
        List<DeltaAction> actions = [];
        using var stats = new ParsedStatsText(new HashSet<string>());
        using var shared = new CheckpointActions.Shared();
        CheckpointActions.Read(batch, stats, shared, actions);
        return [.. actions.Select(Describe)];
    }

    private static string Describe(DeltaAction action) => action switch
    {
        AddAction a => $"add {a.Path} {a.Size} {a.Stats} [{string.Join(",", a.PartitionValues.Select(p => $"{p.Key}={p.Value}"))}] [{string.Join(",", (a.Tags ?? new Dictionary<string, string?>()).Select(p => $"{p.Key}={p.Value}"))}]",
        RemoveAction r => $"remove {r.Path} {r.DeletionTimestamp} [{string.Join(",", (r.PartitionValues ?? new Dictionary<string, string?>()).Select(p => $"{p.Key}={p.Value}"))}]",
        MetadataAction m => $"metaData {m.Id}",
        ProtocolAction p => $"protocol {p.MinReaderVersion} {p.MinWriterVersion}",
        _ => action.GetType().Name,
    };
}
