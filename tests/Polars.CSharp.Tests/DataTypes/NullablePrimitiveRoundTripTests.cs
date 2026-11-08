namespace Polars.CSharp.Tests.DataTypes;

/// <summary>
/// DataFrame.From must keep every nullable primitive value, for frames long enough to take the SIMD packing
/// paths (16+ elements) as well as their scalar tails. bool? false used to become null from 16 rows on.
/// </summary>
public class NullablePrimitiveRoundTripTests
{
    public class Row
    {
        public long Id { get; set; }
        public bool? B { get; set; }
        public sbyte? I8 { get; set; }
        public byte? U8 { get; set; }
        public short? I16 { get; set; }
        public ushort? U16 { get; set; }
        public int? I32 { get; set; }
        public uint? U32 { get; set; }
        public long? I64 { get; set; }
        public ulong? U64 { get; set; }
        public float? F32 { get; set; }
        public double? F64 { get; set; }
        public decimal? M { get; set; }
    }

    private static Row MakeRow(int i)
    {
        // Every third value is null; others cycle through zero / false, extremes and ordinary values.
        bool isNull = i % 3 == 2;
        int k = i % 5;
        return new Row
        {
            Id = i,
            B = isNull ? null : k % 2 == 0,
            I8 = isNull ? null : new sbyte[] { 0, sbyte.MinValue, sbyte.MaxValue, -1, 7 }[k],
            U8 = isNull ? null : new byte[] { 0, byte.MaxValue, 1, 128, 7 }[k],
            I16 = isNull ? null : new short[] { 0, short.MinValue, short.MaxValue, -1, 7 }[k],
            U16 = isNull ? null : new ushort[] { 0, ushort.MaxValue, 1, 32768, 7 }[k],
            I32 = isNull ? null : new[] { 0, int.MinValue, int.MaxValue, -1, 7 }[k],
            U32 = isNull ? null : new uint[] { 0, uint.MaxValue, 1, 2147483648u, 7 }[k],
            I64 = isNull ? null : new[] { 0L, long.MinValue, long.MaxValue, -1L, 7L }[k],
            U64 = isNull ? null : new ulong[] { 0, ulong.MaxValue, 1, 9223372036854775808ul, 7 }[k],
            F32 = isNull ? null : new[] { 0f, -0.5f, float.MaxValue, -1f, 7.25f }[k],
            F64 = isNull ? null : new[] { 0.0, -0.5, double.MaxValue, -1.0, 7.25 }[k],
            M = isNull ? null : new[] { 0m, -0.5m, 12345.678m, -1m, 7.25m }[k],
        };
    }

    [Theory]
    [InlineData(1)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(40)]
    public void Nullable_Primitives_Round_Trip(int count)
    {
        var rows = Enumerable.Range(0, count).Select(MakeRow).ToArray();
        using var df = DataFrame.From(rows);
        var back = new List<Row>();
        using (var en = df.Rows<Row>())
        {
            while (en.MoveNext())
                back.Add(en.Current);
        }

        Assert.Equal(count, back.Count);
        for (int i = 0; i < count; i++)
        {
            var (e, a) = (rows[i], back[i]);
            Assert.Equal((e.Id, e.B, e.I8, e.U8, e.I16, e.U16, e.I32, e.U32), (a.Id, a.B, a.I8, a.U8, a.I16, a.U16, a.I32, a.U32));
            Assert.Equal((e.I64, e.U64, e.F32, e.F64, e.M), (a.I64, a.U64, a.F32, a.F64, a.M));
        }
    }
}
