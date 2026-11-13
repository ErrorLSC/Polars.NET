using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The stored Arrow schema (<c>ARROW:schema</c>) refines the mapping — units, narrower integers, durations, zones —
/// exactly or with an error; it never makes an instant local or a local value an instant. Files from the checkpoint
/// writer with a stored schema added to their footer.
/// </summary>
public class ParquetArrowSchemaTests
{
    /// <summary>The file with <paramref name="stored"/> as its ARROW:schema (an IPC stream of the schema alone, base64).</summary>
    internal static byte[] WithArrowSchema(byte[] file, Apache.Arrow.Schema stored)
    {
        var ipc = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ipc, stored, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteEnd();
        }
        var (offset, length) = ParquetMetadata.FooterRange(file, file.Length, "file");
        FileMetaData meta = FileMetaData.Parse(file.AsSpan((int)offset, length));
        var changed = new FileMetaData
        {
            Version = meta.Version,
            Schema = meta.Schema,
            NumRows = meta.NumRows,
            RowGroups = meta.RowGroups,
            KeyValueMetadata = [new KeyValue("ARROW:schema", Convert.ToBase64String(ipc.ToArray()))],
            CreatedBy = meta.CreatedBy,
        };
        var footer = new ThriftCompactWriter();
        changed.Write(footer);
        byte[] size = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(size, footer.WrittenSpan.Length);
        return [.. file.AsSpan(0, (int)offset), .. footer.WrittenSpan, .. size, .. "PAR1"u8];
    }

    private static byte[] Write(ParquetLeaf leaf, params object?[] values) =>
        MinimalParquetWriter.Write([leaf], [.. values.Select(v => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?> { [leaf.Name] = v })]);

    private static (IArrowType Type, IArrowArray Values) Read(byte[] file)
    {
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader();
        RecordBatch batch = Assert.Single(reader.ReadBatches());
        return (reader.Schema.FieldsList[0].DataType, batch.Column(0));
    }

    private static Apache.Arrow.Schema One(string name, IArrowType type) => new([new Field(name, type, true)], null);

    [Fact]
    public void TimestampUnits_AreRescaledExactly()
    {
        var micros = new ParquetLeaf("ts", Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.TimestampMicros);
        // Finer: multiplied, checked.
        var (type, values) = Read(WithArrowSchema(Write(micros, 1_500L, null), One("ts", new TimestampType(TimeUnit.Nanosecond, "UTC"))));
        Assert.Equal(TimeUnit.Nanosecond, ((TimestampType)type).Unit);
        Assert.Equal(1_500_000L, ((TimestampArray)values).GetValue(0));
        Assert.Contains("does not fit 64 bits", Assert.Throws<ParquetFormatException>(() =>
            Read(WithArrowSchema(Write(micros, long.MaxValue / 100), One("ts", new TimestampType(TimeUnit.Nanosecond, "UTC"))))).Message);
        // Coarser: divided, exactly.
        (type, values) = Read(WithArrowSchema(Write(micros, 3_000L), One("ts", new TimestampType(TimeUnit.Millisecond, "UTC"))));
        Assert.Equal(TimeUnit.Millisecond, ((TimestampType)type).Unit);
        Assert.Equal(3L, ((TimestampArray)values).GetValue(0));
        Assert.Contains("finer part", Assert.Throws<ParquetFormatException>(() =>
            Read(WithArrowSchema(Write(micros, 3_001L), One("ts", new TimestampType(TimeUnit.Millisecond, "UTC"))))).Message);
    }

    [Fact]
    public void Zones_NameInstants_ButNeverChangeWhetherAValueIsOne()
    {
        var instant = new ParquetLeaf("ts", Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.TimestampMicros);
        var local = new ParquetLeaf("ts", Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.LocalTimestampMicros);
        Assert.Equal("Asia/Tokyo", ((TimestampType)Read(WithArrowSchema(Write(instant, 1L), One("ts", new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo")))).Type).Timezone);
        Assert.Equal("UTC", ((TimestampType)Read(WithArrowSchema(Write(instant, 1L), One("ts", new TimestampType(TimeUnit.Microsecond, (string?)null)))).Type).Timezone);
        Assert.Null(((TimestampType)Read(WithArrowSchema(Write(local, 1L), One("ts", new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo")))).Type).Timezone);
    }

    [Fact]
    public void UnannotatedIntegers_AreNarrowedExactly_AndDurationsKept()
    {
        var plain = new ParquetLeaf("n", Repetition.Optional, PhysicalType.Int64);
        var (type, values) = Read(WithArrowSchema(Write(plain, 4_294_967_295L, 0L), One("n", UInt32Type.Default)));
        Assert.IsType<UInt32Type>(type);
        Assert.Equal(4_294_967_295u, ((UInt32Array)values).GetValue(0));
        Assert.Contains("does not fit", Assert.Throws<ParquetFormatException>(() => Read(WithArrowSchema(Write(plain, -1L), One("n", UInt32Type.Default)))).Message);
        Assert.IsType<DurationType>(Read(WithArrowSchema(Write(plain, 5L), One("n", DurationType.Millisecond))).Type);

        // An annotated column keeps its annotation: the Parquet side wins.
        var int16 = new ParquetLeaf("n", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Int16);
        Assert.IsType<Int16Type>(Read(WithArrowSchema(Write(int16, 5), One("n", Int8Type.Default))).Type);
    }

    [Fact]
    public void BinaryAsString_ReadsUnannotatedBinariesAsValidatedStrings()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("apache/data/alltypes_plain.parquet"));
        using (var plain = file.OpenReader(new() { Columns = ["string_col"] })) Assert.IsType<BinaryViewType>(plain.Schema.FieldsList[0].DataType);
        using (var classic = file.OpenReader(new() { Columns = ["string_col"], ViewTypes = false })) Assert.IsType<BinaryType>(classic.Schema.FieldsList[0].DataType);
        using (var strings = file.OpenReader(new() { Columns = ["string_col"], BinaryAsString = true }))
        {
            Assert.IsType<StringViewType>(strings.Schema.FieldsList[0].DataType);
            var batch = Assert.Single(strings.ReadBatches());
            Assert.Equal(["0", "1", "0", "1"], Enumerable.Range(0, 4).Select(i => ((StringViewArray)batch.Column(0)).GetString(i)));
        }
        using (var strings = file.OpenReader(new() { Columns = ["string_col"], BinaryAsString = true, ViewTypes = false }))
        {
            Assert.IsType<StringType>(strings.Schema.FieldsList[0].DataType);
            var batch = Assert.Single(strings.ReadBatches());
            Assert.Equal(["0", "1", "0", "1"], Enumerable.Range(0, 4).Select(i => ((StringArray)batch.Column(0)).GetString(i)));
        }

        byte[] invalid = MinimalParquetWriter.Write([new ParquetLeaf("b", Repetition.Optional, PhysicalType.ByteArray)],
            [new Dictionary<string, object?> { ["b"] = (byte[])[0xFF] }]);
        using var bad = ParquetFile.Open(new MemoryStream(invalid));
        using var badReader = bad.OpenReader(new() { BinaryAsString = true });
        Assert.Contains("UTF-8", Assert.Throws<ParquetFormatException>(() => badReader.ReadBatches().ToList()).Message);
    }
}
