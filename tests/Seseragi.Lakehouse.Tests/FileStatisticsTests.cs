using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using ArrowField = Apache.Arrow.Field;
using ArrowStructType = Apache.Arrow.Types.StructType;
using DeltaStructType = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// The stats JSON of data files from their footers, to the byte: Spark's text of each type, the string cut, JSON
/// escapes, structs holding only what is written for their fields, and footers without some statistics.
/// </summary>
public class FileStatisticsTests
{
    [Fact]
    public void Strings_AreCutAndEscaped()
    {
        string straddling = new string('a', 31) + "😀b"; // 34 UTF-16 units: the cut at 32 would split the emoji
        ParquetMetadata footer = Footer(10,
            ("s1", Strings(straddling, "z")),
            ("s2", Strings("\"q\\\u0001\u2028<&>", "😀")),
            ("s3", Strings("b", new string('c', 33))));
        string stats = FileStatistics.FromParquet(footer, [Field("s1", Primitive(PrimitiveType.String)), Field("s2", Primitive(PrimitiveType.String)), Field("s3", Primitive(PrimitiveType.String))]);
        Assert.Equal("""{"numRecords":2,"minValues":{"s1":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","s2":"\"q\\\u0001\u2028<&>","s3":"b"},"maxValues":{"s1":"z","s2":"\uD83D\uDE00"},"nullCount":{"s1":0,"s2":0,"s3":0}}""", stats);
    }

    [Fact]
    public void Numbers_DecimalsAndTimes_AreWrittenAsSpark()
    {
        ParquetMetadata footer = Footer(10,
            ("d1", Decimals(5, 2, 5, -5, 0)),
            ("d2", Decimals(38, 0, Int128.Parse(new string('9', 38)), -Int128.Parse(new string('9', 38)), 1)),
            ("d3", Decimals(5, 2, 1, 2, 3)),
            ("x", Doubles(-0.0, 1e300, double.NaN)), // a chunk with NaN has no bounds
            ("z", Doubles(-0.0, 1e300, 7)),
            ("y", Doubles(1.5, double.PositiveInfinity, 4e-6)),
            ("f", Floats(0.1f, -3.4e38f, 2f)),
            ("day", Primitive(Date32Type.Default, 4, -1, 0, 19000)),
            ("ms", Primitive(new TimestampType(TimeUnit.Millisecond, "UTC"), 8, -1, 1, null)),
            ("us", Primitive(new TimestampType(TimeUnit.Microsecond, "UTC"), 8, -1, 1_000_999, -1_001)),
            ("ns", Primitive(new TimestampType(TimeUnit.Nanosecond, (string?)null), 8, -1, 999_999_999, -1_000_001)));
        string stats = FileStatistics.FromParquet(footer,
        [
            Field("d1", new DecimalType(5, 2)), Field("d2", new DecimalType(38, 0)), Field("d3", new DecimalType(5, 3)), // d3: another scale, no bounds
            Field("x", Primitive(PrimitiveType.Double)), Field("z", Primitive(PrimitiveType.Double)), Field("y", Primitive(PrimitiveType.Double)), Field("f", Primitive(PrimitiveType.Float)),
            Field("day", Primitive(PrimitiveType.Date)), Field("ms", Primitive(PrimitiveType.Timestamp)), Field("us", Primitive(PrimitiveType.Timestamp)),
            Field("ns", Primitive(PrimitiveType.TimestampNtz)),
        ]);
        Assert.Equal("""{"numRecords":3,"minValues":{"d1":-0.05,"d2":-99999999999999999999999999999999999999,"z":-0,"y":4E-06,"f":-3.4E+38,"day":"1969-12-31","ms":"1969-12-31T23:59:59.999Z","us":"1969-12-31T23:59:59.998Z","ns":"1969-12-31T23:59:59.998"},"maxValues":{"d1":0.05,"d2":99999999999999999999999999999999999999,"z":1E+300,"f":2,"day":"2022-01-08","ms":"1970-01-01T00:00:00.001Z","us":"1970-01-01T00:00:01.000Z","ns":"1970-01-01T00:00:00.999"},"nullCount":{"d1":0,"d2":0,"d3":0,"x":0,"z":0,"y":0,"f":0,"day":0,"ms":1,"us":0,"ns":0}}""", stats);
    }

    [Fact]
    public void Structs_HoldWhatIsWrittenForTheirFields_InTheOrderOfTheirFirstLeaf()
    {
        ParquetMetadata footer = NestedFooter();
        StructField[] fields = NestedFields();
        string stats = FileStatistics.FromParquet(footer, fields);
        Assert.Equal("""{"numRecords":3,"minValues":{"a":-7,"s":{"t.u":{"名":"é"}}},"maxValues":{"a":3,"s":{"t.u":{"名":"ü"}}},"nullCount":{"a":1,"s":{"t.u":{"x\"y":3,"名":1}},"gone":3}}""", stats);

        // Leaves out of schema order: a struct's values still nest under it, at the place of its first leaf.
        IReadOnlyList<StructField[]> leaves = FileStatistics.IndexedLeaves(fields, null);
        Assert.Equal("""{"numRecords":3,"minValues":{"a":-7,"s":{"t.u":{"名":"é"}}},"maxValues":{"a":3,"s":{"t.u":{"名":"ü"}}},"nullCount":{"s":{"t.u":{"x\"y":3,"名":1}},"a":1,"gone":3}}""", FileStatistics.FromParquet(footer, [leaves[1], leaves[0], .. leaves.Skip(2)]));

        // A footer of views alone (no Thrift objects behind it) reads alike.
        var views = new ParquetMetadata { Schema = footer.Schema, RowCount = footer.RowCount, RowGroups = footer.RowGroups };
        Assert.Equal(stats, FileStatistics.FromParquet(views, fields));
        Assert.Equal(FileStatistics.NullCounts(footer), FileStatistics.NullCounts(views));
    }

    [Fact]
    public void RowGroupsWithoutStatistics_LeaveOutWhatTheyCannotProve()
    {
        ParquetMetadata footer = Footer(2, ("a", Primitive(Int32Type.Default, 4, 5, 6, 1, 9)), ("b", Primitive(Int64Type.Default, 8, 1, 2, 3, 4)));
        // The second row group has no statistics for a, the first no bounds for b (only its null count).
        var views = new ParquetMetadata
        {
            Schema = footer.Schema,
            RowCount = footer.RowCount,
            RowGroups =
            [
                new ParquetRowGroup(footer.RowGroups[0].RowCount, [footer.RowGroups[0].Columns[0], footer.RowGroups[0].Columns[1] with { Statistics = new(null, null, 0) }]),
                new ParquetRowGroup(footer.RowGroups[1].RowCount, [footer.RowGroups[1].Columns[0] with { Statistics = null }, footer.RowGroups[1].Columns[1]]),
            ],
        };
        string stats = FileStatistics.FromParquet(views, [Field("a", Primitive(PrimitiveType.Integer)), Field("b", Primitive(PrimitiveType.Long))]);
        Assert.Equal("""{"numRecords":4,"minValues":{},"maxValues":{},"nullCount":{"b":0}}""", stats);
        Assert.Equal(new Dictionary<string, long> { ["b"] = 0 }, FileStatistics.NullCounts(views));
    }

    [Fact]
    public void FootersAsWritten_ReadInPlace_AsParsed()
    {
        StructField[] fields = NestedFields();
        byte[] nested = NestedFile();
        byte[] grouped = File(2, ("a", Primitive(Int32Type.Default, 4, 5, null, 1, 9, 7)), ("s", Strings("x", "é", null, "😀", "")));
        StructField[] groupedFields = [Field("a", Primitive(PrimitiveType.Integer)), Field("s", Primitive(PrimitiveType.String)), Field("gone", Primitive(PrimitiveType.Date))];
        foreach ((byte[] file, StructField[] table) in (ValueTuple<byte[], StructField[]>[])[(nested, fields), (grouped, groupedFields)])
        {
            ParquetMetadata parsed = ParquetMetadata.FromFile(file);
            var leaves = new StatisticsLeaves(FileStatistics.IndexedLeaves(table, null));
            using ParquetFooter footer = BytesOf(file);
            for (int read = 0; read < 2; read++) // the second read with the schema placed already
            {
                Assert.Equal(FileStatistics.FromParquet(parsed, table), FileStatistics.FromFooter(footer, leaves));
                Assert.Equal(FileStatistics.NullCounts(parsed), FileStatistics.NullCounts(footer, leaves));
            }
            // A footer read elsewhere (its metadata alone) reads alike.
            Assert.Equal(FileStatistics.FromParquet(parsed, table), FileStatistics.FromFooter(new ParquetFooter(parsed), leaves));
        }

        // Damaged bytes are read as parsing reads them: refused alike.
        int length = BinaryPrimitives.ReadInt32LittleEndian(nested.AsSpan(nested.Length - 8));
        using ParquetFooter cut = ParquetFooter.Of(nested.AsSpan(nested.Length - 8 - length, length - 3));
        Assert.Throws<ParquetFormatException>(() => FileStatistics.FromFooter(cut, new StatisticsLeaves(FileStatistics.IndexedLeaves(fields, null))));
    }

    // ------------------------------------------------------------------

    private static ParquetMetadata NestedFooter() => ParquetMetadata.FromFile(NestedFile());

    private static byte[] NestedFile()
    {
        var inner = new ArrowStructType([new ArrowField("x\"y", Int32Type.Default, true), new ArrowField("名", StringType.Default, true)]);
        var outer = new ArrowStructType([new ArrowField("t.u", inner, true)]);
        IArrowArray innerValues = new StructArray(inner, 3, [Primitive(Int32Type.Default, 4, null, null, null), Strings("é", "ü", null)], ArrowBuffer.Empty, 0);
        IArrowArray outerValues = new StructArray(outer, 3, [innerValues], ArrowBuffer.Empty, 0);
        return File(10, ("a", Primitive(Int32Type.Default, 4, 3, null, -7)), ("s", outerValues));
    }

    private static StructField[] NestedFields()
    {
        var inner = new DeltaStructType([Field("x\"y", Primitive(PrimitiveType.Integer)), Field("名", Primitive(PrimitiveType.String))]);
        return
        [
            Field("a", Primitive(PrimitiveType.Integer)),
            Field("s", new DeltaStructType([Field("t.u", inner)])),
            Field("gone", Primitive(PrimitiveType.Long)), // not in the file: every row null
        ];
    }

    private static StructField Field(string name, DeltaType type) => new() { Name = name, Type = type };

    private static PrimitiveType Primitive(string name) => new(name);

    private static ParquetMetadata Footer(int rowGroupRows, params (string Name, IArrowArray Values)[] columns) =>
        ParquetMetadata.FromFile(File(rowGroupRows, columns));

    private static byte[] File(int rowGroupRows, params (string Name, IArrowArray Values)[] columns)
    {
        var schema = new Apache.Arrow.Schema([.. columns.Select(c => new ArrowField(c.Name, c.Values.Data.DataType, true))], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows, LeaveOpen = true }))
        {
            using var batch = new RecordBatch(schema, [.. columns.Select(c => c.Values)], columns[0].Values.Length);
            writer.Write(batch);
            writer.Complete();
        }
        return stream.ToArray();
    }

    /// <summary>The footer bytes of a file, as its writer hands them over.</summary>
    private static ParquetFooter BytesOf(byte[] file)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 8));
        return ParquetFooter.Of(file.AsSpan(file.Length - 8 - length, length));
    }

    private static IArrowArray Strings(params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (string? value in values)
        {
            if (value is null) builder.AppendNull();
            else builder.Append(value);
        }
        return builder.Build();
    }

    private static IArrowArray Doubles(params double[] values) => new DoubleArray.Builder().AppendRange(values).Build();

    private static IArrowArray Floats(params float[] values) => new FloatArray.Builder().AppendRange(values).Build();

    /// <summary>Values of a fixed-width type from integers (nulls where null), little-endian in <paramref name="width"/> bytes.</summary>
    private static IArrowArray Primitive(IArrowType type, int width, params long?[] values)
    {
        byte[] bytes = new byte[values.Length * width];
        byte[] validity = new byte[(values.Length + 7) / 8];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is not long v) continue;
            validity[i / 8] |= (byte)(1 << (i % 8));
            if (width == 4) BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4), (int)v);
            else BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 8), v);
        }
        return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Length, -1, 0, [new ArrowBuffer(validity), new ArrowBuffer(bytes)]));
    }

    private static IArrowArray Decimals(int precision, int scale, params Int128[] unscaled)
    {
        byte[] bytes = new byte[unscaled.Length * 16];
        for (int i = 0; i < unscaled.Length; i++) BinaryPrimitives.WriteInt128LittleEndian(bytes.AsSpan(i * 16), unscaled[i]);
        return ArrowArrayFactory.BuildArray(new ArrayData(new Decimal128Type(precision, scale), unscaled.Length, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
    }
}
