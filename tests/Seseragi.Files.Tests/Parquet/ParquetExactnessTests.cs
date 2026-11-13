using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>Values that do not fit their Arrow type are refused, never changed (files written by the checkpoint writer).</summary>
public class ParquetExactnessTests
{
    private static void ReadAll(byte[] file)
    {
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader();
        foreach (var batch in reader.ReadBatches()) batch.Dispose();
    }

    private static byte[] Write(ParquetLeaf leaf, params object?[] values) =>
        MinimalParquetWriter.Write([leaf], [.. values.Select(v => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?> { [leaf.Name] = v })]);

    [Fact]
    public void NarrowedIntegers_OutOfRange_AreRefused()
    {
        var int8 = new ParquetLeaf("b", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Int8);
        ReadAll(Write(int8, -128, 127, null));
        Assert.Contains("does not fit", Assert.Throws<ParquetFormatException>(() => ReadAll(Write(int8, 1, 300))).Message);
        var int16 = new ParquetLeaf("s", Repetition.Required, PhysicalType.Int32, Annotation: Annotation.Int16);
        Assert.Contains("does not fit", Assert.Throws<ParquetFormatException>(() => ReadAll(Write(int16, -40_000))).Message);
    }

    [Fact]
    public void Decimals_WiderThanTheirPrecision_AreRefused()
    {
        var narrow = new ParquetLeaf("d", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Decimal, Precision: 3, Scale: 1);
        ReadAll(Write(narrow, 999, -999));
        Assert.Contains("precision 3", Assert.Throws<ParquetFormatException>(() => ReadAll(Write(narrow, 1000))).Message);

        var fixedBytes = new ParquetLeaf("f", Repetition.Optional, PhysicalType.FixedLenByteArray, Annotation: Annotation.Decimal, Precision: 4, Scale: 0, TypeLength: 4);
        ReadAll(Write(fixedBytes, (byte[])[0, 0, 0x27, 0x0F])); // 9999
        Assert.Contains("precision 4", Assert.Throws<ParquetFormatException>(() => ReadAll(Write(fixedBytes, (byte[])[0, 0, 0x27, 0x10]))).Message); // 10000
    }

    [Fact]
    public void Strings_ThatAreNotUtf8_AreRefused()
    {
        var text = new ParquetLeaf("t", Repetition.Optional, PhysicalType.ByteArray, Utf8: true);
        ReadAll(Write(text, "héllo", null));
        Assert.Contains("UTF-8", Assert.Throws<ParquetFormatException>(() => ReadAll(Write(text, (byte[])[0x68, 0xC3]))).Message);
        var bytes = new ParquetLeaf("b", Repetition.Optional, PhysicalType.ByteArray);
        ReadAll(Write(bytes, (byte[])[0xFF, 0xC3])); // binary is not checked
    }
}
