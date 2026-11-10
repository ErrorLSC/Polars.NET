using System.Buffers.Binary;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Log;

namespace Seseragi.Lakehouse.Tests;

public class ChangeFeedTests
{
    private const string Cdf = "\"delta.enableChangeDataFeed\":\"true\"";
    private static readonly DateTimeOffset s_t0 = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static MemoryFileSystem Table(string configuration = Cdf) => new MemoryFileSystem()
        .Put(LogFile.CommitPath(0), string.Join("\n", Lines.Protocol, Lines.Metadata(configuration: configuration), Lines.Add("a.parquet")), s_t0)
        .Put(LogFile.CommitPath(1), string.Join("\n", Lines.Add("b.parquet"), Lines.Remove("a.parquet")), s_t0.AddHours(1))
        .Put(LogFile.CommitPath(2), string.Join("\n",
            """{"cdc":{"path":"_change_data/c1.parquet","partitionValues":{},"size":3,"dataChange":false}}""",
            Lines.Add("c.parquet"), Lines.Remove("b.parquet")), s_t0.AddHours(2))
        .Put(LogFile.CommitPath(3),
            """{"add":{"path":"compacted.parquet","partitionValues":{},"size":10,"modificationTime":5,"dataChange":false}}""" + "\n" +
            """{"remove":{"path":"c.parquet","deletionTimestamp":9,"dataChange":false}}""", s_t0.AddHours(3));

    private static string[] Describe(IEnumerable<ChangeFile> files) =>
        [.. files.Select(f => $"{f.Version}:{f.Kind}:{f.Path}")];

    [Fact]
    public void AddsAreInserts_RemovesAreDeletes_AndCdcFilesStandAlone()
    {
        List<ChangeFile> changes = ChangeFeed.Read(Table(), 0);

        Assert.Equal(
            [
                "0:Insert:a.parquet",
                "1:Insert:b.parquet", "1:Delete:a.parquet",
                "2:Cdc:_change_data/c1.parquet",
                // version 3 only rearranged data (dataChange false): nothing to report
            ],
            Describe(changes));
        Assert.Equal(s_t0.AddHours(1), changes[1].Timestamp);
    }

    [Fact]
    public void Range_IsInclusive_AndChecked()
    {
        MemoryFileSystem fs = Table();

        Assert.Equal(["1:Insert:b.parquet", "1:Delete:a.parquet"], Describe(ChangeFeed.Read(fs, 1, 1)));
        Assert.Empty(ChangeFeed.Read(fs, 3, 3));
        Assert.Contains("after the end", Assert.Throws<DeltaException>(() => ChangeFeed.Read(fs, 2, 1)).Message);
        Assert.Contains("not all in the log", Assert.Throws<DeltaException>(() => ChangeFeed.Read(fs, 0, 9)).Message);
    }

    [Fact]
    public void ReadBetween_SelectsCommitsByTime()
    {
        MemoryFileSystem fs = Table();

        Assert.Equal(["1:Insert:b.parquet", "1:Delete:a.parquet", "2:Cdc:_change_data/c1.parquet"],
            Describe(ChangeFeed.ReadBetween(fs, s_t0.AddMinutes(30), s_t0.AddHours(2))));
        Assert.Equal(["2:Cdc:_change_data/c1.parquet"], Describe(ChangeFeed.ReadBetween(fs, s_t0.AddMinutes(90))));
        Assert.Empty(ChangeFeed.ReadBetween(fs, s_t0.AddDays(1)));
        Assert.Empty(ChangeFeed.ReadBetween(fs, s_t0.AddDays(-2), s_t0.AddDays(-1)));
        Assert.Empty(ChangeFeed.ReadBetween(fs, s_t0.AddMinutes(10), s_t0.AddMinutes(20)));
    }

    [Fact]
    public void FeedMustBeEnabled_UnlessTheCallerWaivesIt()
    {
        MemoryFileSystem fs = Table(configuration: "");

        Assert.Contains("not enabled at version 0", Assert.Throws<DeltaException>(() => ChangeFeed.Read(fs, 0)).Message);
        Assert.Equal(4, ChangeFeed.Read(fs, 0, requireEnabled: false).Count);
    }

    [Fact]
    public void SampleTable_StopsWhereTheFeedIsTurnedOff()
    {
        var fs = new LocalFileSystem(Path.Combine(AppContext.BaseDirectory, "TestData", "table-with-cdf"));

        Assert.Equal(["0:Insert:fake/path/1", "1:Cdc:fake/path/2"], Describe(ChangeFeed.Read(fs, 0, 1)));
        Assert.Equal("partition_bar", ChangeFeed.Read(fs, 1, 1)[0].PartitionValues!["partition_foo"]);
        Assert.Contains("not enabled at version 2", Assert.Throws<DeltaException>(() => ChangeFeed.Read(fs, 0, 2)).Message);
        Assert.Empty(ChangeFeed.Read(fs, 3, 4));
    }

    // ---- deletion vectors ----

    private const string UuidA = "^-aqEH.-t@S}K{vb[*k^";  // d2c639aa-...
    private const string FileA = "deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";

    private static byte[] Vector(params ushort[] rows)
    {
        // magic, one bitmap, high key 0, no-run cookie, one array container, offset, values
        var bytes = new List<byte>();
        void I32(uint v) { var b = new byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(b, v); bytes.AddRange(b); }
        void I16(ushort v) { var b = new byte[2]; BinaryPrimitives.WriteUInt16LittleEndian(b, v); bytes.AddRange(b); }
        I32(1681511377); I32(1); I32(0); I32(0); I32(12346); I32(1); I16(0); I16((ushort)(rows.Length - 1)); I32(0);
        foreach (ushort row in rows) I16(row);
        return [.. bytes];
    }

    private static string InlineDv(params ushort[] rows)
    {
        byte[] data = Vector(rows);
        byte[] padded = [.. data, .. new byte[(4 - data.Length % 4) % 4]];
        return ",\"deletionVector\":{\"storageType\":\"i\",\"pathOrInlineDv\":" + System.Text.Json.JsonSerializer.Serialize(Z85.Encode(padded))
            + ",\"sizeInBytes\":" + data.Length + ",\"cardinality\":" + rows.Length + "}";
    }

    private static string AddWith(string dv) =>
        "{\"add\":{\"path\":\"a.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":5,\"dataChange\":true" + dv + "}}";

    private static string RemoveWith(string dv) =>
        "{\"remove\":{\"path\":\"a.parquet\",\"deletionTimestamp\":9,\"dataChange\":true" + dv + "}}";

    [Fact]
    public void ReplacedDeletionVector_ReportsOnlyTheRowsThatChanged()
    {
        const string protocol = """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}""";
        var fs = new MemoryFileSystem()
            .Commit(0, protocol, Lines.Metadata(configuration: Cdf), AddWith(""))
            .Commit(1, RemoveWith(""), AddWith(InlineDv(2, 5)))                    // rows 2 and 5 deleted
            .Commit(2, RemoveWith(InlineDv(2, 5)), AddWith(InlineDv(5, 7, 9)))     // 7 and 9 deleted, 2 restored
            .Commit(3, RemoveWith(InlineDv(5, 7, 9)));                             // the file goes; rows 5, 7, 9 were already gone

        List<ChangeFile> changes = ChangeFeed.Read(fs, 1);

        Assert.Equal(["1:DeletionVectorChange:a.parquet", "2:DeletionVectorChange:a.parquet", "3:Delete:a.parquet"], Describe(changes));
        var (deleted1, restored1) = changes[0].ChangedRows(fs);
        Assert.Equal([2L, 5L], deleted1);
        Assert.Empty(restored1);
        var (deleted2, restored2) = changes[1].ChangedRows(fs);
        Assert.Equal([7L, 9L], deleted2);
        Assert.Equal([2L], restored2);
        Assert.Equal([5L, 7L, 9L], DeletionVector.Read(fs, changes[2].DeletionVector!).DeletedRows);
        Assert.Throws<InvalidOperationException>(() => changes[2].ChangedRows(fs));
    }
}
