using Kaminari.Seseragi.Delta;
using Kaminari.Seseragi.Delta.Schema;

namespace Seseragi.Delta.Tests;

public class SnapshotTests
{
    private const string DvA = "ab^-aqEH.-t@S}K{vb[*k^";
    private const string DvB = "zz^-aqEH.-t@S}K{vb[*k^";

    [Fact]
    public void EmptyLog_IsNotATable()
    {
        Assert.Throws<DeltaTableNotFoundException>(() => Snapshot.Load(new MemoryFileSystem()));
        Assert.Throws<DeltaTableNotFoundException>(() => Snapshot.Load(new MemoryFileSystem().Put("_delta_log/readme.txt", "x")));
    }

    [Fact]
    public void Appends_AccumulateInLogOrder()
    {
        var fs = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata(), Lines.Add("a.parquet"), Lines.Add("b.parquet"))
            .Commit(1, Lines.Add("c.parquet"));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(1, snapshot.Version);
        Assert.Equal(["a.parquet", "b.parquet", "c.parquet"], snapshot.Files.Select(f => f.Path));
        Assert.Equal(1, snapshot.Protocol.MinReaderVersion);
        Assert.Equal("t1", snapshot.Metadata.Id);
        Assert.Equal(["id", "p"], snapshot.Schema.Fields.Select(f => f.Name));
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(1), snapshot.CommitFileTime);
    }

    [Fact]
    public void Remove_DropsTheFile_AndAReAddBringsItBack()
    {
        var fs = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata(), Lines.Add("a.parquet"), Lines.Add("b.parquet"))
            .Commit(1, Lines.Remove("a.parquet"))
            .Commit(2, Lines.Add("a.parquet"));

        Assert.Equal(["b.parquet"], Snapshot.Load(fs, 1).Files.Select(f => f.Path));
        Assert.Equal(["b.parquet", "a.parquet"], Snapshot.Load(fs, 2).Files.Select(f => f.Path));
    }

    [Fact]
    public void DeletionVector_ReplacesTheFileUnderANewKey()
    {
        var fs = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata(), Lines.Add("a.parquet"))
            .Commit(1, Lines.Remove("a.parquet"), Lines.Add("a.parquet", DvA))
            .Commit(2, Lines.Add("a.parquet", DvB), Lines.Remove("a.parquet", DvA));

        var v1 = Assert.Single(Snapshot.Load(fs, 1).Files);
        Assert.Equal($"u{DvA}@1", v1.DeletionVector!.UniqueId);

        var v2 = Assert.Single(Snapshot.Load(fs, 2).Files);
        Assert.Equal($"u{DvB}@1", v2.DeletionVector!.UniqueId);
        Assert.Equal(2, v2.DeletionVector.Cardinality);
    }

    [Fact]
    public void TimeTravel_ReadsAnOlderVersion_AndRejectsAMissingOne()
    {
        var fs = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata(), Lines.Add("a.parquet"))
            .Commit(1, Lines.Add("b.parquet"));

        Assert.Equal(["a.parquet"], Snapshot.Load(fs, 0).Files.Select(f => f.Path));
        Assert.Contains("latest version is 1", Assert.Throws<DeltaException>(() => Snapshot.Load(fs, 2)).Message);
    }

    [Fact]
    public void NewestProtocolAndMetadataWin()
    {
        var fs = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata())
            .Commit(1, """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}""",
                Lines.Metadata("\"p\"", "\"delta.enableDeletionVectors\":\"true\""));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(3, snapshot.Protocol.MinReaderVersion);
        Assert.Equal(["deletionVectors"], snapshot.Protocol.ReaderFeatures);
        Assert.Equal(["p"], snapshot.PartitionColumns);
        Assert.Equal("true", snapshot.Metadata.Configuration["delta.enableDeletionVectors"]);
        Assert.Empty(Snapshot.Load(fs, 0).PartitionColumns);
    }

    [Fact]
    public void Txn_And_DomainMetadata_KeepTheNewest_AndDropRemovedDomains()
    {
        var fs = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata(),
                """{"txn":{"appId":"app","version":1}}""",
                """{"domainMetadata":{"domain":"d1","configuration":"old","removed":false}}""",
                """{"domainMetadata":{"domain":"d2","configuration":"x","removed":false}}""")
            .Commit(1,
                """{"txn":{"appId":"app","version":7,"lastUpdated":3}}""",
                """{"domainMetadata":{"domain":"d1","configuration":"new","removed":false}}""",
                """{"domainMetadata":{"domain":"d2","configuration":"x","removed":true}}""");

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(7, snapshot.AppTransactions["app"].Version);
        Assert.Equal("new", snapshot.DomainMetadata["d1"].Configuration);
        Assert.False(snapshot.DomainMetadata.ContainsKey("d2"));
    }

    [Fact]
    public void UnknownActions_BlankLines_AndOtherLogFiles_AreIgnored()
    {
        var fs = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, "", Lines.Metadata(), """{"futureAction":{"x":1}}""", Lines.Add("a.parquet"), "")
            .Put("_delta_log/00000000000000000000.crc", "{}")
            .Put("_delta_log/_last_checkpoint", "{}")
            .Put("_delta_log/.tmp/00000000000000000001.json", "garbage");

        Assert.Equal(["a.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path));
    }

    [Fact]
    public void GapInTheLog_OrMissingStart_IsAnError()
    {
        var gap = new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata())
            .Commit(2, Lines.Add("a.parquet"));
        Assert.Contains("version 1 is missing", Assert.Throws<DeltaException>(() => Snapshot.Load(gap)).Message);

        var truncated = new MemoryFileSystem().Commit(5, Lines.Add("a.parquet"));
        Assert.Contains("checkpoint", Assert.Throws<DeltaException>(() => Snapshot.Load(truncated)).Message);
    }

    [Fact]
    public void MissingProtocolOrMetadata_IsAnError()
    {
        Assert.Throws<DeltaException>(() => Snapshot.Load(new MemoryFileSystem().Commit(0, Lines.Metadata())));
        Assert.Throws<DeltaException>(() => Snapshot.Load(new MemoryFileSystem().Commit(0, Lines.Protocol)));
    }

    [Theory]
    [InlineData("""{"protocol":{"minReaderVersion":4,"minWriterVersion":7}}""", "", "reader version 4")]
    [InlineData("""{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["variantType"],"writerFeatures":[]}}""", "", "variantType")]
    [InlineData("""{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}""", "\"delta.columnMapping.mode\":\"sideways\"", "Unknown column mapping mode")]
    [InlineData("""{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""", "\"delta.columnMapping.mode\":\"name\"", "reader version is below 2")]
    public void UnsupportedTables_AreRefused(string protocol, string configuration, string expected)
    {
        var fs = new MemoryFileSystem().Commit(0, protocol, Lines.Metadata(configuration: configuration));

        Assert.Contains(expected, Assert.Throws<DeltaProtocolException>(() => Snapshot.Load(fs)).Message);
    }

    [Theory]
    [InlineData("""{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}""", "")]
    [InlineData("""{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping","deletionVectors","timestampNtz"],"writerFeatures":[]}}""", "\"delta.columnMapping.mode\":\"none\"")]
    public void SupportedProtocols_Load(string protocol, string configuration)
    {
        var fs = new MemoryFileSystem().Commit(0, protocol, Lines.Metadata(configuration: configuration));

        Assert.Equal(0, Snapshot.Load(fs).Version);
    }

    [Theory]
    [InlineData("id", ColumnMappingMode.Id)]
    [InlineData("name", ColumnMappingMode.Name)]
    [InlineData("none", ColumnMappingMode.None)]
    public void ColumnMapping_SampleTables_ExposePhysicalNames(string table, ColumnMappingMode mode)
    {
        var fs = new Kaminari.Seseragi.Delta.Engine.LocalFileSystem(Path.Combine(AppContext.BaseDirectory, "TestData", "partition_cm", table));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(mode, snapshot.ColumnMappingMode);
        Assert.Equal(["category"], snapshot.PartitionColumns);
        StructField category = snapshot.Schema.Field("category")!;
        if (mode == ColumnMappingMode.None)
        {
            Assert.Equal("category", category.PhysicalName);
            Assert.Null(category.FieldId);
        }
        else
        {
            Assert.Equal("col-6dc68f07-711d-4f00-8bd6-1f5bc698e8ad", category.PhysicalName);
            Assert.Equal(2, category.FieldId);
        }
    }

    [Fact]
    public void ColumnMapping_PartitionValues_AreTranslatedToLogicalNames()
    {
        const string metadata = """{"metaData":{"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"col-a\"}},{\"name\":\"p\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"col-b\"}}]}","partitionColumns":["p"],"configuration":{"delta.columnMapping.mode":"name"}}}""";
        var fs = new MemoryFileSystem().Commit(0,
            """{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}""",
            metadata,
            """{"add":{"path":"col-b=x/f.parquet","partitionValues":{"col-b":"x"},"size":1,"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"col-a\":1},\"maxValues\":{\"col-a\":5},\"nullCount\":{\"col-a\":0}}"}}""");

        Snapshot snapshot = Snapshot.Load(fs);
        var file = Assert.Single(snapshot.Files);

        Assert.Equal("x", snapshot.PartitionValuesOf(file)["p"]);
        Assert.Empty(snapshot.FilesMatching(Kaminari.Seseragi.Delta.Skipping.Predicate.Eq("p", "y")));
        Assert.Empty(snapshot.FilesMatching(Kaminari.Seseragi.Delta.Skipping.Predicate.Gt("id", 5)));
        Assert.Single(snapshot.FilesMatching(Kaminari.Seseragi.Delta.Skipping.Predicate.Eq("id", 5)));
    }

    [Fact]
    public void ColumnMapping_WithoutPhysicalNames_IsAnError()
    {
        var fs = new MemoryFileSystem().Commit(0,
            """{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}""",
            Lines.Metadata(configuration: "\"delta.columnMapping.mode\":\"name\""));

        Assert.Contains("no physical name", Assert.Throws<DeltaException>(() => Snapshot.Load(fs)).Message);
    }
}
