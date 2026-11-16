using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetNodeArrow"/>: rows described by <see cref="ParquetNode"/>s, written through Arrow by
/// <see cref="ParquetWriter"/>, give the Parquet schema <see cref="MinimalParquetWriter"/> writes and the same values
/// read back (what Delta checkpoints rely on since they moved to the writer).
/// </summary>
public sealed class ParquetNodeArrowTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-nodes-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static ParquetLeaf Opt(string name, PhysicalType type, Annotation annotation = Annotation.None, bool utf8 = false, int precision = 0, int scale = 0, int length = 0) =>
        new(name, Repetition.Optional, type, utf8, annotation, precision, scale, length);

    /// <summary>Every node kind and annotation a checkpoint uses.</summary>
    private static readonly ParquetNode[] Schema =
    [
        Opt("s", PhysicalType.ByteArray, utf8: true),
        new ParquetLeaf("req", Repetition.Required, PhysicalType.Int64),
        Opt("bin", PhysicalType.ByteArray),
        Opt("b", PhysicalType.Boolean),
        Opt("i8", PhysicalType.Int32, Annotation.Int8),
        Opt("i16", PhysicalType.Int32, Annotation.Int16),
        Opt("i32", PhysicalType.Int32),
        Opt("date", PhysicalType.Int32, Annotation.Date),
        Opt("f", PhysicalType.Float),
        Opt("d", PhysicalType.Double),
        Opt("ts", PhysicalType.Int64, Annotation.TimestampMicros),
        Opt("ntz", PhysicalType.Int64, Annotation.LocalTimestampMicros),
        Opt("dec9", PhysicalType.Int32, Annotation.Decimal, precision: 9, scale: 2),
        Opt("dec18", PhysicalType.Int64, Annotation.Decimal, precision: 18, scale: 4),
        Opt("dec38", PhysicalType.FixedLenByteArray, Annotation.Decimal, precision: 38, scale: 6, length: 16),
        Opt("dec20", PhysicalType.FixedLenByteArray, Annotation.Decimal, precision: 20, scale: 0, length: 9),
        new ParquetStringMap("tags", Repetition.Optional),
        new ParquetList("names", Repetition.Optional, Opt("element", PhysicalType.ByteArray, utf8: true)),
        new ParquetStruct("add", Repetition.Optional,
        [
            Opt("path", PhysicalType.ByteArray, utf8: true),
            new ParquetStringMap("partitionValues", Repetition.Optional),
            new ParquetStruct("stats", Repetition.Optional, [Opt("numRecords", PhysicalType.Int64), Opt("minValues", PhysicalType.Int32)]),
            new ParquetList("features", Repetition.Optional, Opt("element", PhysicalType.ByteArray, utf8: true)),
        ]),
    ];

    private static byte[] BigEndian(System.Numerics.BigInteger value, int length)
    {
        byte[] le = value.ToByteArray(); // little-endian two's complement, minimal
        byte[] be = new byte[length];
        byte fill = value.Sign < 0 ? (byte)0xFF : (byte)0;
        for (int i = 0; i < length; i++) be[length - 1 - i] = i < le.Length ? le[i] : fill;
        return be;
    }

    private static IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows()
    {
        List<IReadOnlyDictionary<string, object?>> rows = [];
        for (int i = 0; i < 40; i++)
        {
            bool nulls = i % 5 == 4;
            Dictionary<string, object?> row = new()
            {
                ["s"] = nulls ? null : $"value {i} ✓",
                ["req"] = (long)i * 1_000_000_007,
                ["bin"] = nulls ? null : new byte[] { (byte)i, 0, 255 },
                ["b"] = nulls ? null : i % 2 == 0,
                ["i8"] = nulls ? null : (sbyte)(i - 20),
                ["i16"] = nulls ? null : (short)(i * -300),
                ["i32"] = nulls ? null : i * 1000,
                ["date"] = nulls ? null : 19_000 + i,
                ["f"] = nulls ? null : i * 0.25f,
                ["d"] = nulls ? null : i * -1.5,
                ["ts"] = nulls ? null : 1_700_000_000_000_000L + i,
                ["ntz"] = nulls ? null : -1_000_000L * i,
                ["dec9"] = nulls ? null : i * 12_345 - 200_000,
                ["dec18"] = nulls ? null : (long)i * -98_765_432_109L,
                ["dec38"] = nulls ? null : BigEndian(System.Numerics.BigInteger.Pow(10, 30) * (i - 20) + i, 16),
                ["dec20"] = nulls ? null : BigEndian(System.Numerics.BigInteger.Pow(10, 19) * (i % 3 - 1) - i, 9),
                ["tags"] = i % 7 == 0 ? null : (IEnumerable<KeyValuePair<string, string?>>)
                    [.. Enumerable.Range(0, i % 3).Select(k => new KeyValuePair<string, string?>($"k{k}", k == 1 ? null : $"v{i}"))],
                ["names"] = i % 6 == 0 ? null : (object?[])[.. Enumerable.Range(0, i % 4).Select(k => k == 2 ? null : (object?)$"n{k}")],
                ["add"] = i % 9 == 0 ? null : new Dictionary<string, object?>
                {
                    ["path"] = $"part-{i:D5}.parquet",
                    ["partitionValues"] = (IEnumerable<KeyValuePair<string, string?>>)[new("year", i % 2 == 0 ? "2024" : null)],
                    ["stats"] = i % 4 == 0 ? null : new Dictionary<string, object?> { ["numRecords"] = (long)i, ["minValues"] = i % 3 == 0 ? null : -i },
                    ["features"] = (object?[])["a", "b"],
                },
            };
            rows.Add(row);
        }
        return rows;
    }

    /// <summary>
    /// A schema element as compared. The writer marks LIST and MAP groups with their logical types too and leaves out
    /// the deprecated MAP_KEY_VALUE on <c>key_value</c>, as pyarrow and parquet-mr do; the minimal writer does neither.
    /// </summary>
    private static object Element(SchemaElement e) => (e.Name, e.Type, e.TypeLength, e.Repetition, e.NumChildren,
        e.ConvertedType == ConvertedType.MapKeyValue ? null : e.ConvertedType, e.Scale, e.Precision,
        e.LogicalType is { Kind: not (LogicalTypeKind.Map or LogicalTypeKind.List) } l ? $"{l.Kind} {l.Precision} {l.Scale} {l.IsAdjustedToUtc} {l.Unit} {l.BitWidth} {l.IsSigned}" : null);

    [Fact]
    public void ThroughTheWriter_TheSchemaAndValuesAreTheMinimalWriters()
    {
        var rows = Rows();
        string minimal = Path.Combine(_dir, "minimal.parquet"), written = Path.Combine(_dir, "written.parquet");
        File.WriteAllBytes(minimal, MinimalParquetWriter.Write(Schema, rows));
        using (var batch = ParquetNodeArrow.ToArrow(Schema, rows))
            ParquetWriter.Write(written, batch.Schema, [batch], new ParquetWriteOptions { StoreArrowSchema = false });

        using (var a = ParquetFile.Open(minimal))
        using (var b = ParquetFile.Open(written))
        {
            Assert.Equal(a.Metadata.Thrift!.Schema.Select(Element), b.Metadata.Thrift!.Schema.Select(Element));
            Assert.Equal(rows.Count, b.Metadata.RowCount);
        }
        List<string>[] expected = ParquetWriterTests.ReadBack(minimal, Schema.Length, out _);
        List<string>[] actual = ParquetWriterTests.ReadBack(written, Schema.Length, out _);
        for (int c = 0; c < Schema.Length; c++) Assert.True(expected[c].SequenceEqual(actual[c]), $"column {Schema[c].Name}");
    }

    [Fact]
    public void NoRows_GiveAnEmptyFile_OfTheSchema()
    {
        string path = Path.Combine(_dir, "empty.parquet");
        using (var batch = ParquetNodeArrow.ToArrow(Schema, []))
            ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { StoreArrowSchema = false });
        using var file = ParquetFile.Open(path);
        Assert.Equal(0, file.Metadata.RowCount);
        Assert.Equal(Schema.Length, file.Metadata.Thrift!.Schema[0].NumChildren);
    }
}
