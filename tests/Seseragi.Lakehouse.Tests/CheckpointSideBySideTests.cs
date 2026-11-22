using Apache.Arrow;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// A checkpoint of many batches has its actions made side by side: the same actions, in the same order, as reading
/// its batches one by one; and a failure is the first one in row order, whichever batch finishes first.
/// </summary>
public class CheckpointSideBySideTests
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
    public void ManyBatches_ReadAsOneByOne(bool statsAsText)
    {
        var fs = new MemoryFileSystem();
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 };
        Dictionary<string, string> configuration = statsAsText ? [] : new() { [CheckpointStatistics.WriteJsonKey] = "false" };
        Transaction create = Transaction.Create(fs, protocol, new MetadataAction { Id = "t", SchemaString = Schema, PartitionColumns = ["p"], Configuration = configuration });
        for (int n = 0; n < 40_000; n++)
        {
            create.Add(new AddAction
            {
                Path = $"p={n % 7}/f{n}.parquet",
                PartitionValues = new Dictionary<string, string?> { ["p"] = n % 11 == 0 ? null : $"{n % 7}" },
                Size = 100 + n,
                ModificationTime = n,
                Stats = n % 6 == 0 ? null
                    : $$"""{"numRecords":{{n + 1}},"minValues":{"id":{{n}},"s":"a{{n}}"},"maxValues":{"id":{{n * 10}},"s":"z{{n}}"},"nullCount":{"id":0,"s":""" + $"{n % 2}}}}}",
                Tags = n % 4 == 0 ? new Dictionary<string, string?> { ["t"] = $"v{n}" } : null,
            });
            // Tombstones among the adds.
            if (n % 97 == 0) create.Add(new RemoveAction { Path = $"p={n % 7}/gone{n}.parquet", DeletionTimestamp = 5, PartitionValues = new Dictionary<string, string?> { ["p"] = $"{n % 7}" } });
        }
        create.Commit();
        Snapshot committed = Snapshot.Load(fs);
        string checkpoint = Checkpoints.Write(fs, committed);
        byte[] file = fs.ReadAll(checkpoint)!;

        List<DeltaAction> read = CheckpointReader.Read(fs, checkpoint, file.Length, CheckpointContent.All);
        List<string> sideBySide = [.. read.Select(Describe)];

        List<string> oneByOne = OneByOne(file, out int batches);
        Assert.True(batches > 3, $"{batches} batches");
        Assert.Equal(oneByOne.Count, sideBySide.Count);
        Assert.Equal(oneByOne, sideBySide);

        // Apart from the checkpoint code: the adds are the files the commit holds (statistics text when kept as text).
        Assert.Equal(committed.Files.Select(f => File(f, statsAsText)).Order(), read.OfType<AddAction>().Select(f => File(f, statsAsText)).Order());

        static string File(AddAction a, bool withStats) =>
            $"{a.Path} {a.Size} {a.ModificationTime} [{Entries(a.PartitionValues)}] [{Entries(a.Tags)}] {(withStats ? a.Stats : "")}";
    }

    [Theory]
    [InlineData(9_000, 30_000)]
    [InlineData(30_000, 9_000)]
    [InlineData(20_000, 20_001)]
    public void TheFirstFailingRow_IsReported(int protocolRow, int txnRow)
    {
        // A protocol without its writer version and a txn without its version, in different batches (or the same).
        string[] rows = [.. Enumerable.Range(0, 40_000).Select(n => n switch
        {
            _ when n == protocolRow => """{"protocol":{"minReaderVersion":1}}""",
            _ when n == txnRow => """{"txn":{"appId":"a"}}""",
            _ => $$$"""{"add":{"path":"f{{{n}}}.parquet","size":{{{n}}},"modificationTime":1,"dataChange":true}}""",
        })];
        byte[] file = CheckpointParquet.Write(CheckpointParquet.Classic(), rows);
        var fs = new MemoryFileSystem().PutBytes("_delta_log/00000000000000000001.checkpoint.parquet", file);

        var e = Assert.Throws<DeltaException>(() => CheckpointReader.Read(fs, "_delta_log/00000000000000000001.checkpoint.parquet", file.Length, CheckpointContent.All));
        string expected = protocolRow < txnRow ? "A 'protocol' action lacks the required field 'minWriterVersion'." : "A 'txn' action lacks the required field 'version'.";
        Assert.Equal(expected, e.Message);
    }

    // The checkpoint's batches read in order on this thread, with one set of shared values.
    private static List<string> OneByOne(byte[] file, out int batches)
    {
        using ParquetFile parquet = ParquetFile.Open(file.AsMemory());
        using ParquetReader reader = parquet.OpenReader(new ParquetReadOptions { BatchSize = 8_192, BinaryAsString = true, ViewTypes = false, MaxDegreeOfParallelism = 1 });
        using var stats = new ParsedStatsText(new HashSet<string>());
        using var shared = new CheckpointActions.Shared();
        List<DeltaAction> actions = [];
        batches = 0;
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) CheckpointActions.Read(batch, stats, shared, actions);
            batches++;
        }
        return [.. actions.Select(Describe)];
    }

    private static string Describe(DeltaAction action) => action switch
    {
        AddAction a => $"add {a.Path} {a.Size} {a.ModificationTime} {a.DataChange} {a.Stats} [{Entries(a.PartitionValues)}] [{Entries(a.Tags)}]",
        RemoveAction r => $"remove {r.Path} {r.DeletionTimestamp} [{Entries(r.PartitionValues)}]",
        MetadataAction m => $"metaData {m.Id} {m.SchemaString} [{string.Join(",", m.PartitionColumns)}] [{Entries(m.Configuration.ToDictionary(c => c.Key, c => (string?)c.Value))}]",
        ProtocolAction p => $"protocol {p.MinReaderVersion} {p.MinWriterVersion}",
        _ => action.GetType().Name,
    };

    private static string Entries(IReadOnlyDictionary<string, string?>? map) =>
        map is null ? "none" : string.Join(",", map.Select(p => $"{p.Key}={p.Value ?? "null"}"));
}
