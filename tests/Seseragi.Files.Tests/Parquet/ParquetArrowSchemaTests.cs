using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The stored Arrow schema (<c>ARROW:schema</c>) refines the mapping — durations, zones — where Parquet cannot say it;
/// where it disagrees with the Parquet types (a timestamp unit, an integer width) they win, as pyarrow reads such files.
/// It never makes an instant local or a local value an instant. Files from the checkpoint writer with a stored schema
/// added to their footer.
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
    public void TimestampUnits_StayAsStored()
    {
        // A writer stores seconds as milliseconds, nanoseconds of format 1.0 as microseconds: the values read in the
        // stored unit, unchanged (pyarrow 26: timestamp[us] from a timestamp[ns] hint, timestamp[ms] from timestamp[s]).
        var micros = new ParquetLeaf("ts", Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.TimestampMicros);
        var (type, values) = Read(WithArrowSchema(Write(micros, long.MaxValue / 100, null), One("ts", new TimestampType(TimeUnit.Nanosecond, "UTC"))));
        Assert.Equal((TimeUnit.Microsecond, "UTC"), (((TimestampType)type).Unit, ((TimestampType)type).Timezone));
        Assert.Equal(long.MaxValue / 100, ((TimestampArray)values).GetValue(0));
        (type, values) = Read(WithArrowSchema(Write(micros, 3_001L), One("ts", new TimestampType(TimeUnit.Millisecond, "UTC"))));
        Assert.Equal(TimeUnit.Microsecond, ((TimestampType)type).Unit);
        Assert.Equal(3_001L, ((TimestampArray)values).GetValue(0));
        // The zone of an instant still comes from the stored schema, whatever its unit.
        var zoned = (TimestampType)Read(WithArrowSchema(Write(micros, 1L), One("ts", new TimestampType(TimeUnit.Nanosecond, "Asia/Tokyo")))).Type;
        Assert.Equal((TimeUnit.Microsecond, "Asia/Tokyo"), (zoned.Unit, zoned.Timezone));
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
    public void UnannotatedIntegers_KeepTheirWidth_AndDurationsKept()
    {
        // pyarrow 26 reads an unannotated INT64 as int64 and an INT32 as int32, whatever narrower type the stored schema
        // names (a uint32 written by format 1.0 comes back as int64); a duration over INT64 is restored.
        var plain = new ParquetLeaf("n", Repetition.Optional, PhysicalType.Int64);
        var (type, values) = Read(WithArrowSchema(Write(plain, 4_294_967_295L, -1L), One("n", UInt32Type.Default)));
        Assert.IsType<Int64Type>(type);
        Assert.Equal([4_294_967_295L, -1L], [((Int64Array)values).GetValue(0)!.Value, ((Int64Array)values).GetValue(1)!.Value]);
        Assert.IsType<Int64Type>(Read(WithArrowSchema(Write(plain, 5L), One("n", Int32Type.Default))).Type);
        Assert.IsType<DurationType>(Read(WithArrowSchema(Write(plain, 5L), One("n", DurationType.Millisecond))).Type);
        var plain32 = new ParquetLeaf("n", Repetition.Optional, PhysicalType.Int32);
        Assert.IsType<Int32Type>(Read(WithArrowSchema(Write(plain32, 300), One("n", Int8Type.Default))).Type);
        Assert.IsType<Int32Type>(Read(WithArrowSchema(Write(plain32, 5), One("n", UInt16Type.Default))).Type);

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
