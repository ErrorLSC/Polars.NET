using System.Security.Cryptography;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>The checkpoint writer's output stays byte for byte the same while its Thrift layer is shared.</summary>
public class ParquetMinimalWriterTests
{
    private static ParquetLeaf Leaf(string name, PhysicalType type, Repetition repetition = Repetition.Optional) => new(name, repetition, type);

    /// <summary>Every node kind and annotation the writer knows, with more than 15 schema elements and columns (long list headers).</summary>
    internal static byte[] WriteSample()
    {
        ParquetNode[] schema =
        [
            Leaf("b", PhysicalType.Boolean),
            Leaf("i32", PhysicalType.Int32, Repetition.Required),
            Leaf("i64", PhysicalType.Int64),
            Leaf("f", PhysicalType.Float),
            Leaf("d", PhysicalType.Double),
            new ParquetLeaf("s", Repetition.Optional, PhysicalType.ByteArray, Utf8: true),
            Leaf("bin", PhysicalType.ByteArray),
            new ParquetLeaf("fixed", Repetition.Optional, PhysicalType.FixedLenByteArray, TypeLength: 3),
            new ParquetLeaf("i8", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Int8),
            new ParquetLeaf("i16", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Int16),
            new ParquetLeaf("date", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Date),
            new ParquetLeaf("dec32", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Decimal, Precision: 9, Scale: 2),
            new ParquetLeaf("dec128", Repetition.Optional, PhysicalType.FixedLenByteArray, Annotation: Annotation.Decimal, Precision: 38, Scale: 4, TypeLength: 16),
            new ParquetLeaf("ts", Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.TimestampMicros),
            new ParquetLeaf("lts", Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.LocalTimestampMicros),
            new ParquetStruct("st", Repetition.Optional,
            [
                Leaf("x", PhysicalType.Int64),
                new ParquetList("tags", Repetition.Optional, new ParquetLeaf("element", Repetition.Optional, PhysicalType.ByteArray, Utf8: true)),
                new ParquetStringMap("conf", Repetition.Optional),
            ]),
            new ParquetList("nested", Repetition.Optional, new ParquetList("element", Repetition.Optional, Leaf("element", PhysicalType.Int32))),
        ];
        Dictionary<string, object?> full = new()
        {
            ["b"] = true, ["i32"] = 7, ["i64"] = -5L, ["f"] = 1.5f, ["d"] = 2.25, ["s"] = "héllo", ["bin"] = (byte[])[1, 2],
            ["fixed"] = (byte[])[9, 8, 7], ["i8"] = -3, ["i16"] = 300, ["date"] = 19000, ["dec32"] = 12345,
            ["dec128"] = new byte[16], ["ts"] = 1_700_000_000_000_000L, ["lts"] = 42L,
            ["st"] = new Dictionary<string, object?>
            {
                ["x"] = 1L, ["tags"] = (List<object?>)["a", null, "c"],
                ["conf"] = (List<KeyValuePair<string, string?>>)[new("k", "v"), new("n", null)],
            },
            ["nested"] = (List<object?>)[(List<object?>)[1, 2], null, (List<object?>)[]],
        };
        Dictionary<string, object?> sparse = new() { ["i32"] = 0, ["st"] = new Dictionary<string, object?> { ["tags"] = (List<object?>)[] } };
        return MinimalParquetWriter.Write(schema, [full, sparse, new Dictionary<string, object?> { ["i32"] = 1 }]);
    }

    [Fact]
    public void Output_IsUnchanged()
    {
        byte[] file = WriteSample();
        Assert.Equal("0F3FD31F8BD1E1203FCD0DE461AC6C6C5EC880F40A779D3A8F9905DDC9A6C099", Convert.ToHexString(SHA256.HashData(file)));
    }
}
