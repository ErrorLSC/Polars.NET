using System.Buffers.Binary;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

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

    // ---- schema changes (rules probed against delta-spark 4.4) ----

    private static MemoryFileSystem SchemaTable(bool mapped)
    {
        var fs = new MemoryFileSystem();
        var configuration = new Dictionary<string, string> { [ChangeFeed.EnableKey] = "true", ["delta.enableTypeWidening"] = "true" };
        string id = mapped ? ""","metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-id"}""" : ""","metadata":{}""";
        string b = mapped ? ""","metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b"}""" : ""","metadata":{}""";
        if (mapped)
        {
            configuration["delta.columnMapping.mode"] = "name";
            configuration["delta.columnMapping.maxColumnId"] = "2";
        }
        var metadata = new MetadataAction
        {
            Id = "t", Configuration = configuration,
            SchemaString = $$"""{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true{{id}}},{"name":"b","type":"string","nullable":true{{b}}}]}""",
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = mapped ? ["columnMapping", "typeWidening"] : ["typeWidening"],
            WriterFeatures = mapped ? ["columnMapping", "typeWidening", "changeDataFeed"] : ["typeWidening", "changeDataFeed"],
        };
        Transaction.Create(fs, protocol, metadata).Commit(s_t0);
        // v1 data, v2 metadata with the same schema, v3 the schema change, v4 data.
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "1.parquet", Size = 1 }).Commit(s_t0.AddHours(1));
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["x"] = "y" }, now: s_t0.AddHours(2));
        if (mapped) AlterTable.RenameColumn(fs, Snapshot.Load(fs), "b", "b2", s_t0.AddHours(3));
        else AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "id", new PrimitiveType(PrimitiveType.Long), s_t0.AddHours(3));
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "4.parquet", Size = 1 }).Commit(s_t0.AddHours(4));
        return fs;
    }

    [Fact]
    public void ColumnMappedTable_IsReadWithTheEndSchema_AndRangesAcrossARenameAreRefused()
    {
        MemoryFileSystem fs = SchemaTable(mapped: true);
        ChangeRange before = ChangeFeed.ReadRange(fs, 0, 2);
        Assert.Equal(2, before.ReadSnapshot.Version); // the end version's schema: b
        Assert.Equal(["1:Insert:1.parquet"], Describe(before.Files));
        Assert.Equal(["4:Insert:4.parquet"], Describe(ChangeFeed.ReadRange(fs, 3, 4).Files));

        // A metadata change inside the range the end schema cannot read (Spark: ..._INCOMPATIBLE_SCHEMA_CHANGE at version 2).
        Assert.Contains("incompatibly at version 2", Assert.Throws<DeltaException>(() => ChangeFeed.ReadRange(fs, 0, 3)).Message);
        // The start version's data cannot be read with the end schema (Spark: ..._INCOMPATIBLE_DATA_SCHEMA).
        Assert.Contains("data of version 2", Assert.Throws<DeltaException>(() => ChangeFeed.ReadRange(fs, 2, 4)).Message);
        // Allowed explicitly: read with the end schema, by physical name.
        Assert.Equal(["1:Insert:1.parquet", "4:Insert:4.parquet"], Describe(ChangeFeed.ReadRange(fs, 0, 4, allowSchemaChanges: true).Files));
    }

    [Fact]
    public void OtherTables_AreReadWithTheLatestSchema_SoAWideningRefusesEarlierRanges()
    {
        MemoryFileSystem fs = SchemaTable(mapped: false);
        // Read with the latest schema (id long), which the data before the widening does not have.
        Assert.Throws<DeltaException>(() => ChangeFeed.ReadRange(fs, 0, 1));
        Assert.Throws<DeltaException>(() => ChangeFeed.ReadRange(fs, 0, 4));
        ChangeRange after = ChangeFeed.ReadRange(fs, 3, 4);
        Assert.Equal(4, after.ReadSnapshot.Version);
        Assert.Equal(["4:Insert:4.parquet"], Describe(after.Files));
        Assert.Equal(4, ChangeFeed.ReadRange(fs, 0, 1, allowSchemaChanges: true).ReadSnapshot.Version);
    }

    [Fact]
    public void ReadCompatibility_FollowsSpark()
    {
        StructType S(string fields) => StructType.Parse($$"""{"type":"struct","fields":[{{fields}}]}""");
        string F(string name, string type, bool nullable = true, string physical = "") =>
            "{\"name\":\"" + name + "\",\"type\":" + type + ",\"nullable\":" + (nullable ? "true" : "false") + ",\"metadata\":{" +
            (physical.Length > 0 ? "\"delta.columnMapping.physicalName\":\"" + physical + "\"" : "") + "}}";

        StructType ab = S(F("a", "\"integer\"") + "," + F("b", "\"string\""));
        Assert.True(ChangeFeed.ReadCompatible(ab, S(F("A", "\"integer\"") + "," + F("b", "\"string\"") + "," + F("c", "\"string\"")), false)); // added, any case
        Assert.False(ChangeFeed.ReadCompatible(ab, S(F("a", "\"integer\"")), false)); // dropped
        Assert.False(ChangeFeed.ReadCompatible(ab, S(F("a", "\"long\"") + "," + F("b", "\"string\"")), false)); // widened
        Assert.True(ChangeFeed.ReadCompatible(ab, S(F("a", "\"integer\"", nullable: false) + "," + F("b", "\"string\"")), false)); // nullability ignored
        // Under column mapping, the same name must be the same physical column (dropped and added again is not).
        Assert.False(ChangeFeed.ReadCompatible(S(F("a", "\"integer\"", physical: "col-1")), S(F("a", "\"integer\"", physical: "col-2")), true));
        Assert.True(ChangeFeed.ReadCompatible(S(F("a", "\"integer\"", physical: "col-1")), S(F("a", "\"integer\"", physical: "col-1")), true));
        // Nested.
        Assert.False(ChangeFeed.ReadCompatible(
            S(F("s", """{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}""")),
            S(F("s", """{"type":"struct","fields":[{"name":"y","type":"integer","nullable":true,"metadata":{}}]}""")), false));
        Assert.False(ChangeFeed.ReadCompatible(
            S(F("l", """{"type":"array","elementType":"integer","containsNull":true}""")),
            S(F("l", """{"type":"array","elementType":"long","containsNull":true}""")), false));
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
