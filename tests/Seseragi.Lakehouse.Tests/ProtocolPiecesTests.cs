using System.Text;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Seseragi.Lakehouse.Tests;

public class LogFileTests
{
    private static LogFile? Parse(string name) =>
        LogFile.TryParse(new FileEntry("_delta_log/" + name, 0, default), out LogFile file) ? file : null;

    [Fact]
    public void RecognisesEveryKind()
    {
        Assert.Equal((LogFileKind.Commit, 12L), (Parse("00000000000000000012.json")!.Value.Kind, Parse("00000000000000000012.json")!.Value.Version));
        Assert.Equal(LogFileKind.Checksum, Parse("00000000000000000012.crc")!.Value.Kind);
        Assert.Equal(LogFileKind.ClassicCheckpoint, Parse("00000000000000000010.checkpoint.parquet")!.Value.Kind);
        Assert.Equal(LogFileKind.LastCheckpoint, Parse("_last_checkpoint")!.Value.Kind);

        LogFile multi = Parse("00000000000000000010.checkpoint.0000000002.0000000003.parquet")!.Value;
        Assert.Equal((LogFileKind.MultiPartCheckpoint, 10L, 2, 3), (multi.Kind, multi.Version, multi.Part, multi.NumParts));

        Assert.Equal(LogFileKind.V2Checkpoint, Parse("00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json")!.Value.Kind);
        Assert.Equal(LogFileKind.V2Checkpoint, Parse("00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet")!.Value.Kind);

        LogFile compacted = Parse("00000000000000000004.00000000000000000006.compacted.json")!.Value;
        Assert.Equal((LogFileKind.CompactedCommit, 4L, 6L), (compacted.Kind, compacted.Version, compacted.EndVersion));
    }

    [Theory]
    [InlineData("0000000000000000012.json")]
    [InlineData("00000000000000000012.json.tmp")]
    [InlineData("00000000000000000012.checkpoint.0000000004.0000000003.parquet")]
    [InlineData("00000000000000000012.checkpoint.not-a-uuid.json")]
    [InlineData("0000000000000000001x.json")]
    [InlineData("readme.txt")]
    public void IgnoresOtherNames(string name) => Assert.Null(Parse(name));

    [Fact]
    public void CommitPath_PadsTo20Digits() =>
        Assert.Equal("_delta_log/00000000000000000042.json", LogFile.CommitPath(42));
}

public class ActionParserTests
{
    private static DeltaAction? Parse(string line) => ActionParser.ParseLine(Encoding.UTF8.GetBytes(line));

    [Fact]
    public void Add_ReadsEveryField()
    {
        var add = Assert.IsType<AddAction>(Parse(
            """{"add":{"path":"p=a%20b/part-1.parquet","partitionValues":{"p":"a b","q":null},"size":123,"modificationTime":456,"dataChange":false,"stats":"{\"numRecords\":3}","tags":{"k":"v"},"baseRowId":7,"defaultRowCommitVersion":2,"deletionVector":{"storageType":"i","pathOrInlineDv":"0rJua","sizeInBytes":4,"cardinality":1}}}"""));

        Assert.Equal("p=a%20b/part-1.parquet", add.Path);
        Assert.Equal("p=a b/part-1.parquet", add.DecodedPath);
        Assert.Equal("a b", add.PartitionValues["p"]);
        Assert.Null(add.PartitionValues["q"]);
        Assert.Equal((123L, 456L, false), (add.Size, add.ModificationTime, add.DataChange));
        Assert.Equal("""{"numRecords":3}""", add.Stats);
        Assert.Equal("v", add.Tags!["k"]);
        Assert.Equal((7L, 2L), (add.BaseRowId, add.DefaultRowCommitVersion));
        Assert.True(add.DeletionVector!.IsInline);
        Assert.Null(add.DeletionVector.Offset);
    }

    [Fact]
    public void Remove_Cdc_CommitInfo_AreRead()
    {
        var remove = Assert.IsType<RemoveAction>(Parse("""{"remove":{"path":"a","deletionTimestamp":5,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":9}}"""));
        Assert.Equal((5L, true, 9L), (remove.DeletionTimestamp, remove.ExtendedFileMetadata, remove.Size));

        var cdc = Assert.IsType<CdcAction>(Parse("""{"cdc":{"path":"_change_data/c.parquet","partitionValues":{},"size":4,"dataChange":false}}"""));
        Assert.Equal("_change_data/c.parquet", cdc.Path);

        var info = Assert.IsType<CommitInfoAction>(Parse("""{"commitInfo":{"timestamp":10,"operation":"WRITE","inCommitTimestamp":11,"operationParameters":{"mode":"Append"}}}"""));
        Assert.Equal((10L, 11L, "WRITE"), (info.Timestamp, info.InCommitTimestamp, info.Operation));
        Assert.Contains("operationParameters", info.Json);
    }

    [Fact]
    public void UnknownAction_IsNull_AndBadInputThrows()
    {
        Assert.Null(Parse("""{"somethingNew":{"a":1}}"""));
        Assert.Throws<DeltaException>(() => Parse("{not json"));
        Assert.Throws<DeltaException>(() => Parse("""{"add":{"size":1}}"""));
    }
}

public class DeletionVectorDescriptorTests
{
    [Fact]
    public void UuidStorage_ResolvesTheFilePath_WithAndWithoutPrefix()
    {
        // The example of the protocol specification.
        var dv = new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^", Offset = 4, SizeInBytes = 40, Cardinality = 6 };
        Assert.Equal("ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", dv.FilePath());
        Assert.Equal("uab^-aqEH.-t@S}K{vb[*k^@4", dv.UniqueId);

        var bare = new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = "^-aqEH.-t@S}K{vb[*k^", SizeInBytes = 40, Cardinality = 6 };
        Assert.Equal("deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", bare.FilePath());
        Assert.Equal("u^-aqEH.-t@S}K{vb[*k^", bare.UniqueId);
    }

    [Fact]
    public void Z85_RoundTrips_AndMatchesTheReferenceVector()
    {
        byte[] reference = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
        Assert.Equal("HelloWorld", Z85.Encode(reference));
        Assert.Equal(reference, Z85.Decode("HelloWorld"));
        Assert.Throws<DeltaException>(() => Z85.Decode("abc"));
        Assert.Throws<DeltaException>(() => Z85.Decode("ab\"cd"));
    }

    [Fact]
    public void Z85_GroupAbove32Bits_KeepsTheLowBits_LikeTheReferenceDecoder()
    {
        // "#####" is 85^5 - 1, above 2^32 - 1. Delta's Base85Codec keeps the low 32 bits.
        uint low = unchecked((uint)(85L * 85 * 85 * 85 * 85 - 1));
        byte[] expected = [(byte)(low >> 24), (byte)(low >> 16), (byte)(low >> 8), (byte)low];
        Assert.Equal(expected, Z85.Decode("#####"));
    }

    [Fact]
    public void Inline_And_AbsolutePath()
    {
        var inline = new DeletionVectorDescriptor { StorageType = "i", PathOrInlineDv = "HelloWorld", SizeInBytes = 8, Cardinality = 1 };
        Assert.Equal(8, inline.InlineBytes().Length);
        Assert.Equal(6, new DeletionVectorDescriptor { StorageType = "i", PathOrInlineDv = "HelloWorld", SizeInBytes = 6, Cardinality = 1 }.InlineBytes().Length);
        Assert.Throws<DeltaException>(() => inline.FilePath());

        var absolute = new DeletionVectorDescriptor { StorageType = "p", PathOrInlineDv = "s3://b/dv.bin", SizeInBytes = 8, Cardinality = 1 };
        Assert.Equal("s3://b/dv.bin", absolute.FilePath());
    }
}

public class SchemaTests
{
    [Fact]
    public void ParsesNestedTypes_Nullability_AndMetadata()
    {
        StructType schema = StructType.Parse("""
            {"type":"struct","fields":[
              {"name":"id","type":"long","nullable":false,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-1"}},
              {"name":"amount","type":"decimal(18,2)","nullable":true,"metadata":{}},
              {"name":"tags","type":{"type":"array","elementType":"string","containsNull":false},"nullable":true,"metadata":{}},
              {"name":"attrs","type":{"type":"map","keyType":"string","valueType":{"type":"struct","fields":[{"name":"x","type":"timestamp_ntz","nullable":true,"metadata":{}}]},"valueContainsNull":true},"nullable":true,"metadata":{}}
            ]}
            """);

        StructField id = schema.Field("id")!;
        Assert.False(id.Nullable);
        Assert.Equal("col-1", id.PhysicalName);
        Assert.Equal("1", id.Metadata[StructField.ColumnIdKey]);
        Assert.Equal(PrimitiveType.Long, Assert.IsType<PrimitiveType>(id.Type).Name);

        var amount = Assert.IsType<DecimalType>(schema.Field("amount")!.Type);
        Assert.Equal((18, 2), (amount.Precision, amount.Scale));
        Assert.Equal("amount", schema.Field("amount")!.PhysicalName);

        var tags = Assert.IsType<ArrayType>(schema.Field("tags")!.Type);
        Assert.False(tags.ContainsNull);

        var attrs = Assert.IsType<MapType>(schema.Field("attrs")!.Type);
        Assert.True(attrs.ValueContainsNull);
        Assert.Equal("struct<x:timestamp_ntz>", attrs.ValueType.ToString());
        Assert.Null(schema.Field("missing"));
    }

    [Fact]
    public void InvalidSchemas_Throw()
    {
        Assert.Throws<DeltaException>(() => StructType.Parse("not json"));
        Assert.Throws<DeltaException>(() => StructType.Parse("\"long\""));
        Assert.Throws<DeltaException>(() => StructType.Parse("""{"type":"struct","fields":[{"name":"a","type":{"type":"tuple"}}]}"""));
        Assert.Throws<DeltaException>(() => StructType.Parse("""{"type":"struct","fields":[{"name":"a","type":"decimal(x)"}]}"""));
    }
}

public class LocalFileSystemTests
{
    [Fact]
    public void ListsOnlyDirectFiles_InOrder_AndReadsThem()
    {
        string root = Directory.CreateTempSubdirectory("seseragi-delta-").FullName;
        try
        {
            Directory.CreateDirectory(Path.Combine(root, "_delta_log", "sub"));
            File.WriteAllText(Path.Combine(root, "_delta_log", "00000000000000000001.json"), "b");
            File.WriteAllText(Path.Combine(root, "_delta_log", "00000000000000000000.json"), "a");
            File.WriteAllText(Path.Combine(root, "_delta_log", "sub", "x.json"), "x");
            var fs = new LocalFileSystem(root);

            Assert.Equal(
                ["_delta_log/00000000000000000000.json", "_delta_log/00000000000000000001.json"],
                fs.List("_delta_log").Select(f => f.Path));
            Assert.Equal(["_delta_log/00000000000000000001.json"], fs.List("_delta_log", "00000000000000000001").Select(f => f.Path));
            Assert.Empty(fs.List("nowhere"));
            Assert.Equal("a", Encoding.UTF8.GetString(fs.ReadAll("_delta_log/00000000000000000000.json")!));
            Assert.Null(fs.ReadAll("_delta_log/missing.json"));
            Assert.Null(fs.ReadAll("nowhere/missing.json"));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
