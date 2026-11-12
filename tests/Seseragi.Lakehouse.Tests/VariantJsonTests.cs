using System.Buffers.Binary;
using System.Text;
using Kaminari.Seseragi.Lakehouse.Variant;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Variant values (the Parquet Variant encoding) rendered as Spark's to_json renders them.</summary>
public class VariantJsonTests
{
    /// <summary>Metadata version 1 with one-byte offsets and the given field names.</summary>
    private static byte[] Metadata(params string[] names)
    {
        List<byte> bytes = [0x01, (byte)names.Length, 0];
        byte[][] encoded = [.. names.Select(Encoding.UTF8.GetBytes)];
        int offset = 0;
        foreach (byte[] name in encoded)
        {
            offset += name.Length;
            bytes.Add((byte)offset);
        }
        foreach (byte[] name in encoded) bytes.AddRange(name);
        return [.. bytes];
    }

    private static readonly byte[] NoNames = Metadata();

    private static byte[] Primitive(int type, params byte[] data) => [(byte)(type << 2), .. data];

    private static byte[] Le(long value, int size)
    {
        var bytes = new byte[size];
        for (int i = 0; i < size; i++) bytes[i] = (byte)(value >> (8 * i));
        return bytes;
    }

    private static byte[] DoubleBytes(double d)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(bytes, d);
        return bytes;
    }

    private static byte[] FloatBytes(float f)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteSingleLittleEndian(bytes, f);
        return bytes;
    }

    private static string Json(byte[] value, byte[]? metadata = null) => VariantJson.ToJson(metadata ?? NoNames, value);

    [Theory]
    [InlineData(1.0, "1.0")]
    [InlineData(-0.0, "-0.0")]
    [InlineData(0.0, "0.0")]
    [InlineData(2.5, "2.5")]
    [InlineData(0.001, "0.001")]
    [InlineData(0.0001, "1.0E-4")]
    [InlineData(1234567.0, "1234567.0")]
    [InlineData(12345678.9, "1.23456789E7")]
    [InlineData(1e10, "1.0E10")]
    [InlineData(1e23, "1.0E23")]
    [InlineData(0.1 + 0.2, "0.30000000000000004")]
    [InlineData(double.MaxValue, "1.7976931348623157E308")]
    [InlineData(double.Epsilon, "4.9E-324")]
    public void Doubles_AreRenderedAsJavaPrintsThem(double d, string expected) =>
        Assert.Equal(expected, Json(Primitive(7, DoubleBytes(d))));

    [Fact]
    public void NonFiniteNumbers_AreQuoted()
    {
        Assert.Equal("\"NaN\"", Json(Primitive(7, DoubleBytes(double.NaN))));
        Assert.Equal("\"-Infinity\"", Json(Primitive(7, DoubleBytes(double.NegativeInfinity))));
        Assert.Equal("\"Infinity\"", Json(Primitive(14, FloatBytes(float.PositiveInfinity))));
    }

    [Theory]
    [InlineData(1.5f, "1.5")]
    [InlineData(0.1f, "0.1")]
    [InlineData(1e10f, "1.0E10")]
    [InlineData(3.4028235e38f, "3.4028235E38")]
    public void Floats_AreRenderedAsJavaPrintsThem(float f, string expected) =>
        Assert.Equal(expected, Json(Primitive(14, FloatBytes(f))));

    [Fact]
    public void IntegersDecimalsAndBooleans_ArePlainNumbersAndLiterals()
    {
        Assert.Equal("-5", Json(Primitive(3, 0xFB)));
        Assert.Equal("300", Json(Primitive(4, Le(300, 2))));
        Assert.Equal("-70000", Json(Primitive(5, Le(-70000, 4))));
        Assert.Equal("5000000000", Json(Primitive(6, Le(5_000_000_000, 8))));
        Assert.Equal("1.25", Json(Primitive(8, [2, .. Le(125, 4)])));
        Assert.Equal("-0.005", Json(Primitive(9, [3, .. Le(-5, 8)])));
        Assert.Equal("1.5", Json(Primitive(8, [2, .. Le(150, 4)])));          // Spark strips trailing zeros
        Assert.Equal("0", Json(Primitive(8, [2, .. Le(0, 4)])));
        Assert.Equal("100", Json(Primitive(8, [0, .. Le(100, 4)])));
        byte[] wide = new byte[16];
        System.Numerics.BigInteger.Parse("-123456789012345678901", System.Globalization.CultureInfo.InvariantCulture).TryWriteBytes(wide, out _);
        for (int i = 9; i < 16; i++) wide[i] = 0xFF; // sign-extend the 9 bytes written
        Assert.Equal("-12345678901234567890.1", Json(Primitive(10, [1, .. wide])));
        Assert.Equal("true", Json(Primitive(1)));
        Assert.Equal("false", Json(Primitive(2)));
        Assert.Equal("null", Json(Primitive(0)));
    }

    [Fact]
    public void DatesTimestampsBinariesAndUuids_AreStrings()
    {
        Assert.Equal("\"2024-02-29\"", Json(Primitive(11, Le(19782, 4))));
        Assert.Equal("\"1969-12-31\"", Json(Primitive(11, Le(-1, 4))));
        Assert.Equal("\"+10000-01-01\"", Json(Primitive(11, Le(2932897, 4))));
        Assert.Equal("\"2024-01-02 03:04:05.5+00:00\"", Json(Primitive(12, Le(1704164645_500000, 8))));
        Assert.Equal("\"1969-12-31 23:59:59.999999\"", Json(Primitive(13, Le(-1, 8))));
        Assert.Equal("\"1970-01-01 00:00:00\"", Json(Primitive(13, Le(0, 8))));
        Assert.Equal("\"AQID\"", Json(Primitive(15, [.. Le(3, 4), 1, 2, 3])));
        Assert.Equal("\"00112233-4455-6677-8899-aabbccddeeff\"",
            Json(Primitive(20, [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])));
    }

    [Fact]
    public void Strings_AreEscapedAsJacksonEscapes()
    {
        byte[] text = Encoding.UTF8.GetBytes("a\"b\\c\nd\te\u0001f/ü😀");
        Assert.Equal("\"a\\\"b\\\\c\\nd\\te\\u0001f/ü😀\"", Json([(byte)(text.Length << 2 | 1), .. text]));
        byte[] longText = Encoding.UTF8.GetBytes(new string('x', 70));
        Assert.Equal($"\"{new string('x', 70)}\"", Json(Primitive(16, [.. Le(70, 4), .. longText])));
    }

    [Fact]
    public void ObjectsAndArrays_NestInTheirStoredOrder()
    {
        // {"a": 1, "b": [true, null]}, field ids 0 = "a", 1 = "b"; one-byte ids and offsets.
        byte[] one = Primitive(3, 1);
        byte[] array = [0b0000_0011, 2, 0, 1, 2, .. Primitive(1), .. Primitive(0)];
        byte[] obj = [0b0000_0010, 2, 0, 1, 0, (byte)one.Length, (byte)(one.Length + array.Length), .. one, .. array];
        Assert.Equal("""{"a":1,"b":[true,null]}""", Json(obj, Metadata("a", "b")));
    }

    [Fact]
    public void LargeArrays_CountWithFourBytes()
    {
        // 300 elements, is_large set, two-byte offsets.
        const int n = 300;
        List<byte> values = [];
        List<byte> offsets = [];
        for (int i = 0; i <= n; i++)
        {
            offsets.AddRange(Le(values.Count, 2));
            if (i < n) values.AddRange(Primitive(0));
        }
        byte header = (byte)(((0b1 << 2) | 0b01) << 2 | 3);
        byte[] array = [header, .. Le(n, 4), .. offsets, .. values];
        Assert.Equal("[" + string.Join(",", Enumerable.Repeat("null", n)) + "]", Json(array));
    }
}
