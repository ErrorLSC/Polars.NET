using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;

namespace Seseragi.Lakehouse.Tests;

public class TransactionTests
{
    private const string Schema =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"p","type":"string","nullable":true,"metadata":{}}]}""";

    private static readonly ProtocolAction s_protocol = new() { MinReaderVersion = 1, MinWriterVersion = 2 };

    private static MetadataAction Metadata(params (string Key, string Value)[] configuration) => new()
    {
        Id = "t1",
        SchemaString = Schema,
        PartitionColumns = ["p"],
        Configuration = configuration.ToDictionary(c => c.Key, c => c.Value),
        CreatedTime = 1,
    };

    private static AddAction Add(string path, string p = "x") => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string?> { ["p"] = p },
        Size = 10,
        ModificationTime = 5,
        Stats = """{"numRecords":3}""",
    };

    [Fact]
    public void Actions_RoundTripThroughTheParser()
    {
        DeltaAction[] actions =
        [
            new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = ["deletionVectors"], WriterFeatures = ["deletionVectors", "appendOnly"] },
            Metadata(("delta.appendOnly", "false")),
            new AddAction
            {
                Path = "p=a%20b/f.parquet", PartitionValues = new Dictionary<string, string?> { ["p"] = null }, Size = 7, ModificationTime = 8,
                DataChange = false, Stats = """{"numRecords":1}""", Tags = new Dictionary<string, string?> { ["k"] = "v" },
                DeletionVector = new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^", Offset = 1, SizeInBytes = 34, Cardinality = 2 },
                BaseRowId = 4, DefaultRowCommitVersion = 2,
            },
            new RemoveAction { Path = "old.parquet", DeletionTimestamp = 9, ExtendedFileMetadata = true, PartitionValues = new Dictionary<string, string?> { ["p"] = "x" }, Size = 3 },
            new CdcAction { Path = "_change_data/c.parquet", PartitionValues = new Dictionary<string, string?> { ["p"] = "x" }, Size = 4 },
            new TxnAction { AppId = "app", Version = 12, LastUpdated = 13 },
            new DomainMetadataAction { Domain = "d", Configuration = "{}", Removed = false },
        ];

        byte[] written = ActionWriter.WriteCommit(actions);
        List<DeltaAction> read = ActionParser.ParseCommit(written);
        Assert.Equal(written, ActionWriter.WriteCommit(read));
        Assert.Equal(actions.Length, Encoding.UTF8.GetString(written).Split('\n').Length);

        var add = Assert.IsType<AddAction>(read[2]);
        Assert.Null(add.PartitionValues["p"]);
        Assert.Equal("p=a b/f.parquet", add.DecodedPath);
        Assert.Equal(2, add.DeletionVector!.Cardinality);
    }

    [Fact]
    public void CreateThenCommit_BuildsTheTable()
    {
        var fs = new MemoryFileSystem();
        Assert.Equal(0, Transaction.Create(fs, s_protocol, Metadata()).Add(Add("a.parquet")).Commit());

        Snapshot v0 = Snapshot.Load(fs);
        long v1 = Transaction.Start(fs, v0, "WRITE").Add(Add("b.parquet", "y")).WithParameter("mode", "Append").Commit();
        Assert.Equal(1, v1);

        Snapshot v1Snapshot = Snapshot.Load(fs);
        long v2 = Transaction.Start(fs, v1Snapshot, "DELETE")
            .Add(new RemoveAction { Path = "a.parquet", DeletionTimestamp = 1 })
            .Commit();

        Snapshot latest = Snapshot.Load(fs);
        Assert.Equal(v2, latest.Version);
        Assert.Equal(["b.parquet"], latest.Files.Select(f => f.Path));
        Assert.Equal("y", latest.Files[0].PartitionValues["p"]);

        // Every commit starts with its commitInfo, which records the operation and what it read.
        using JsonDocument info = CommitInfo(fs, 1);
        Assert.Equal("WRITE", info.RootElement.GetProperty("operation").GetString());
        Assert.Equal(0, info.RootElement.GetProperty("readVersion").GetInt64());
        Assert.True(info.RootElement.GetProperty("isBlindAppend").GetBoolean());
        Assert.Equal("Append", info.RootElement.GetProperty("operationParameters").GetProperty("mode").GetString());
        using JsonDocument deleteInfo = CommitInfo(fs, 2);
        Assert.False(deleteInfo.RootElement.GetProperty("isBlindAppend").GetBoolean());
    }

    [Fact]
    public void TwoWritersOfOneVersion_OnlyOneWins()
    {
        var fs = new MemoryFileSystem();
        Transaction.Create(fs, s_protocol, Metadata()).Commit();
        Snapshot read = Snapshot.Load(fs);

        var outcomes = new int[16];
        Parallel.For(0, outcomes.Length, i =>
        {
            try
            {
                Transaction.Start(fs, read, "WRITE").Add(Add($"f{i}.parquet")).Commit();
                outcomes[i] = 1;
            }
            catch (DeltaCommitConflictException e)
            {
                Assert.Equal(1, e.Version);
                outcomes[i] = 2;
            }
        });
        Assert.Equal(1, outcomes.Count(o => o == 1));
        Assert.Equal(outcomes.Length - 1, outcomes.Count(o => o == 2));
        Assert.Single(Snapshot.Load(fs).Files);

        // Creating a table where one exists is a conflict too.
        Assert.Throws<DeltaCommitConflictException>(() => Transaction.Create(fs, s_protocol, Metadata()).Commit());
    }

    [Fact]
    public void AppendOnlyTables_RefuseRemoves()
    {
        var fs = new MemoryFileSystem();
        Transaction.Create(fs, s_protocol, Metadata(("delta.appendOnly", "true"))).Add(Add("a.parquet")).Commit();
        Snapshot read = Snapshot.Load(fs);

        Assert.Throws<DeltaProtocolException>(() =>
            Transaction.Start(fs, read, "DELETE").Add(new RemoveAction { Path = "a.parquet" }).Commit());
        // Rearranging files without changing data is allowed.
        Transaction.Start(fs, read, "OPTIMIZE").Add(new RemoveAction { Path = "a.parquet", DataChange = false })
            .Add(new AddAction { Path = "b.parquet", DataChange = false, PartitionValues = new Dictionary<string, string?> { ["p"] = "x" } }).Commit();
    }

    [Fact]
    public void InCommitTimestamps_AreWrittenFirst_AndStrictlyIncrease()
    {
        var fs = new MemoryFileSystem();
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["inCommitTimestamp"] };
        var at = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        Transaction.Create(fs, protocol, Metadata(("delta.enableInCommitTimestamps", "true"))).Commit(at);

        // A clock that went backwards still gives a later timestamp.
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a.parquet")).Commit(at.AddSeconds(-30));

        using JsonDocument first = CommitInfo(fs, 0);
        using JsonDocument second = CommitInfo(fs, 1);
        long t0 = first.RootElement.GetProperty("inCommitTimestamp").GetInt64();
        long t1 = second.RootElement.GetProperty("inCommitTimestamp").GetInt64();
        Assert.Equal(at.ToUnixTimeMilliseconds(), t0);
        Assert.Equal(t0 + 1, t1);

        // The reader's timeline sees them.
        var timeline = CommitTimeline.Load(fs);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(t1), timeline.TimestampOf(1));
    }

    [Theory]
    [InlineData("rowTracking", null, null)]
    [InlineData("clustering", null, null)]
    [InlineData("invariants", "delta.invariants", null)]
    [InlineData("generatedColumns", "delta.generationExpression", null)]
    [InlineData("identityColumns", "delta.identity.start", null)]
    [InlineData("checkConstraints", null, "delta.constraints.positive")]
    public void UnsupportedWriterFeatures_AreRefused(string feature, string? fieldKey, string? propertyKey)
    {
        string schema = fieldKey is null
            ? Schema
            : Schema.Replace("\"metadata\":{}", $"\"metadata\":{{\"{fieldKey}\":\"x\"}}");
        var metadata = new MetadataAction
        {
            Id = "t1", SchemaString = schema, PartitionColumns = ["p"],
            Configuration = propertyKey is null ? new Dictionary<string, string>() : new Dictionary<string, string> { [propertyKey] = "id > 0" },
        };
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = [feature] };
        var fs = new MemoryFileSystem();

        Assert.Throws<DeltaProtocolException>(() => Transaction.Create(fs, protocol, metadata).Commit());
        Assert.Empty(fs.List("_delta_log"));

        // The same feature on an older writer version is implied, and refused alike when the table uses it.
        if (fieldKey is not null || propertyKey is not null)
        {
            var legacy = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 6 };
            Assert.Throws<DeltaProtocolException>(() => Transaction.Create(fs, legacy, metadata).Commit());
        }
    }

    private static JsonDocument CommitInfo(MemoryFileSystem fs, long version)
    {
        string first = Encoding.UTF8.GetString(fs.ReadAll(LogFile.CommitPath(version))!).Split('\n')[0];
        using JsonDocument line = JsonDocument.Parse(first);
        return JsonDocument.Parse(line.RootElement.GetProperty("commitInfo").GetRawText());
    }
}
