using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using static Seseragi.Files.Tests.TestBatches;
using Statistics = Kaminari.Seseragi.Files.Parquet.Metadata.Statistics;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The statistics <see cref="ParquetWriter"/> writes: the bounds of every order (signed, unsigned, floating with NaN
/// and zeros, decimals in fixed-length bytes, half floats), truncated string and binary bounds, null and NaN counts;
/// and, since wrong bounds would make readers skip rows, filtered reads of a file with statistics against the same
/// file without them, at the bounds.
/// </summary>
public sealed class ParquetWriterStatisticsTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-stats").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    /// <summary>The statistics of the first row group's column chunk, each column written alone with <paramref name="options"/>.</summary>
    private Statistics?[] Write(ParquetWriteOptions? options, params (string Name, IArrowArray Array, bool Nullable)[] columns) =>
        [.. columns.Select(column =>
        {
            RecordBatch batch = Batch(column);
            string path = Path.Combine(_dir, $"{Guid.NewGuid():N}.parquet");
            ParquetWriter.Write(path, batch.Schema, [batch], options);
            using var file = ParquetFile.Open(path);
            return file.Metadata.Thrift!.RowGroups[0].Columns[0].MetaData!.Statistics;
        })];

    private Statistics?[] Write(params (string Name, IArrowArray Array, bool Nullable)[] columns) => Write(null, columns);

    private static string H(byte[]? bytes) => bytes is null ? "null" : Convert.ToHexString(bytes);

    private static byte[] Le<T>(T value) where T : unmanaged
    {
        byte[] bytes = new byte[System.Runtime.CompilerServices.Unsafe.SizeOf<T>()];
        System.Runtime.InteropServices.MemoryMarshal.Write(bytes, in value);
        return bytes;
    }

    [Fact]
    public void Floats_WriteZerosSigned_AndNoBoundsAmongNaNs()
    {
        Statistics?[] s = Write(
            ("mixed", Fixed(DoubleType.Default, [(double?)1.5, -0d, 3, null]), true),
            ("zeros", Fixed(DoubleType.Default, [(double?)0d, 0d]), true),
            ("negzero", Fixed(DoubleType.Default, [(double?)-5, -0d]), true),
            ("nans", Fixed(DoubleType.Default, [(double?)double.NaN, double.NaN, null]), true),
            ("f32", Fixed(FloatType.Default, [(float?)2f, -1f]), true),
            ("f16", Fixed(HalfFloatType.Default, [(Half?)Half.One, Half.Zero]), true),
            ("withnan", Fixed(DoubleType.Default, [(double?)1, double.NaN, 3]), true),
            ("f16nan", Fixed(HalfFloatType.Default, [(Half?)Half.One, Half.NaN]), true));
        Assert.Equal(Le(-0d), s[0]!.MinValue);
        Assert.Equal(Le(3d), s[0]!.MaxValue);
        Assert.Equal(s[0]!.MinValue, s[0]!.Min); // the deprecated fields too (a signed order)
        Assert.Equal(0, s[0]!.NanCount);
        Assert.Equal(1, s[0]!.NullCount);
        Assert.Equal((H(Le(-0d)), H(Le(0d))), (H(s[1]!.MinValue), H(s[1]!.MaxValue)));
        Assert.Equal((H(Le(-5d)), H(Le(0d))), (H(s[2]!.MinValue), H(s[2]!.MaxValue)));
        Assert.Equal((2L, 1L), (s[3]!.NanCount!.Value, s[3]!.NullCount!.Value));
        Assert.Equal((H(Le(-1f)), H(Le(2f)), 0L), (H(s[4]!.MinValue), H(s[4]!.MaxValue), s[4]!.NanCount!.Value));
        Assert.Equal((H(Le(Half.NegativeZero)), H(Le(Half.One)), 0L), (H(s[5]!.MinValue), H(s[5]!.MaxValue), s[5]!.NanCount!.Value));
        Assert.Null(s[5]!.Min);
        // NaNs among the values: counted, and no bounds (Spark orders NaN above every number; the spec keeps it out).
        foreach (int c in (int[])[3, 6, 7])
        {
            Assert.Null(s[c]!.MinValue);
            Assert.Null(s[c]!.MaxValue);
            Assert.Null(s[c]!.Max);
        }
        Assert.Equal(1, s[6]!.NanCount);
        Assert.Equal(1, s[7]!.NanCount);
    }

    [Fact]
    public void Integers_AreOrderedBySign()
    {
        Statistics?[] s = Write(
            ("i32", Fixed(Int32Type.Default, [(int?)-5, 7, null]), true),
            ("u32", Fixed(UInt32Type.Default, [(uint?)1, uint.MaxValue, 1u << 31]), true),
            ("u64", Fixed(UInt64Type.Default, [(ulong?)ulong.MaxValue, 3]), true),
            ("u8", Fixed(UInt8Type.Default, [(byte?)200, 3]), true),
            ("b", new BooleanArray.Builder().Append(true).Append(true).AppendNull().Build(), true));
        Assert.Equal((H(Le(-5)), H(Le(7)), 1L), (H(s[0]!.MinValue), H(s[0]!.MaxValue), s[0]!.NullCount!.Value));
        Assert.Equal(s[0]!.MaxValue, s[0]!.Max);
        Assert.Equal((H(Le(1u)), H(Le(uint.MaxValue))), (H(s[1]!.MinValue), H(s[1]!.MaxValue)));
        Assert.Null(s[1]!.Min); // no deprecated fields for an unsigned order
        Assert.Equal((H(Le(3UL)), H(Le(ulong.MaxValue))), (H(s[2]!.MinValue), H(s[2]!.MaxValue)));
        Assert.Equal((H(Le(3)), H(Le(200))), (H(s[3]!.MinValue), H(s[3]!.MaxValue)));
        Assert.Equal((H([1]), H([1])), (H(s[4]!.MinValue), H(s[4]!.MaxValue)));
        Assert.All(s, x => Assert.Equal((true, true), (x!.IsMinValueExact, x.IsMaxValueExact)));
    }

    [Fact]
    public void Decimals_AreOrderedAsNumbers()
    {
        Statistics?[] s = Write(
            ("dec30", Fixed(new Decimal128Type(30, 5), [(Int128?)(-1), 1, -1000, null]), true),
            ("dec5", Fixed(new Decimal128Type(5, 1), [(Int128?)(-3), 2]), true),
            ("dec60", Fixed(new Decimal256Type(60, 0), [(ParquetWriteData.Int256?)ParquetWriteData.To256(BigInteger.Pow(10, 50)), ParquetWriteData.To256(-7)]), true));
        Assert.Equal(new BigInteger(-1000), new BigInteger(s[0]!.MinValue, isUnsigned: false, isBigEndian: true));
        Assert.Equal(BigInteger.One, new BigInteger(s[0]!.MaxValue, isUnsigned: false, isBigEndian: true));
        Assert.Equal(13, s[0]!.MinValue!.Length); // the fixed length precision 30 needs
        Assert.Null(s[0]!.Min);
        Assert.Equal((H(Le(-3)), H(Le(2))), (H(s[1]!.MinValue), H(s[1]!.MaxValue)));
        Assert.Null(s[1]!.Min); // decimals in integers: no deprecated fields, as Spark writes them
        Assert.Equal(new BigInteger(-7), new BigInteger(s[2]!.MinValue, isUnsigned: false, isBigEndian: true));
        Assert.Equal(BigInteger.Pow(10, 50), new BigInteger(s[2]!.MaxValue, isUnsigned: false, isBigEndian: true));
    }

    [Fact]
    public void LongStringAndBinaryBounds_AreCut_AndTheUpperRoundedUp()
    {
        string a100 = new('a', 100), x = "zz" + new string('x', 100), e40 = new('é', 40), top = string.Concat(Enumerable.Repeat("\U0010FFFF", 20));
        Statistics?[] s = Write(
            ("s1", Strings(["b", a100, "c"]), true),
            ("s2", Strings(["a", x]), true),
            ("s3", Strings(["a", e40]), true),
            ("s4", Strings([top, top]), true),
            ("ff", new BinaryArray.Builder().Append((ReadOnlySpan<byte>)Enumerable.Repeat((byte)0xFF, 80).ToArray()).Build(), true),
            ("bin", new BinaryArray.Builder().Append((ReadOnlySpan<byte>)[0x01, .. Enumerable.Repeat((byte)0xFF, 70)]).Append((ReadOnlySpan<byte>)[0x00]).Build(), true),
            ("nulls", Strings([null, null]), true),
            ("iv", Fixed(new IntervalType(IntervalUnit.YearMonth), [(int?)5, null]), true),
            ("odd", Strings(["a" + e40, "a" + e40 + "x"]), true));
        byte[] U(string text) => Encoding.UTF8.GetBytes(text);
        Assert.Equal((H(U(new string('a', 64))), false), (H(s[0]!.MinValue), s[0]!.IsMinValueExact!.Value));
        Assert.Equal((H(U("c")), true), (H(s[0]!.MaxValue), s[0]!.IsMaxValueExact!.Value));
        Assert.Equal((H(U("zz" + new string('x', 61) + "y")), false), (H(s[1]!.MaxValue), s[1]!.IsMaxValueExact!.Value));
        Assert.Equal(U(new string('é', 31) + "ê"), s[2]!.MaxValue); // cut on a character, the character incremented
        Assert.Equal(U(string.Concat(Enumerable.Repeat("\U0010FFFF", 16))), s[3]!.MinValue);
        Assert.Null(s[3]!.MaxValue); // nothing shorter bounds U+10FFFF…
        Assert.Null(s[4]!.MaxValue); // …nor 0xFF…
        Assert.Equal((byte[])[0x02], s[5]!.MaxValue);
        Assert.Equal((byte[])[0x00], s[5]!.MinValue);
        Assert.Equal((H(null), H(null), 2L), (H(s[6]!.MinValue), H(s[6]!.MaxValue), s[6]!.NullCount!.Value));
        Assert.Equal((H(null), 1L), (H(s[7]!.MinValue), s[7]!.NullCount!.Value)); // INTERVAL has no order
        // Byte 64 falls inside a character: both bounds are cut before it.
        Assert.Equal((H(U("a" + new string('é', 31))), false), (H(s[8]!.MinValue), s[8]!.IsMinValueExact!.Value));
        Assert.Equal(H(U("a" + new string('é', 30) + "ê")), H(s[8]!.MaxValue));

        Statistics?[] whole = Write(new ParquetWriteOptions { StatisticsTruncateLength = null }, ("s", Strings(["b", a100]), true));
        Assert.Equal((H(U(a100)), true), (H(whole[0]!.MinValue), whole[0]!.IsMinValueExact!.Value));
        Assert.All(Write(new ParquetWriteOptions { Statistics = false }, ("s", Strings(["b"]), true)), Assert.Null);
    }

    /// <summary>A page filled by several batches (each bounded at once by the vector kernel) keeps the widest bounds of all.</summary>
    [Fact]
    public void Bounds_OfAPageFilledInSteps_SpanEveryStep()
    {
        int?[] rising = [.. Enumerable.Range(0, 5000).Select(i => (int?)i)];
        double?[] falling = [.. Enumerable.Range(0, 5000).Select(i => (double?)(5000 - i))];
        RecordBatch batch = Batch(("i", Fixed(Int32Type.Default, rising), true), ("d", Fixed(DoubleType.Default, falling), true));
        string path = Path.Combine(_dir, "steps.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch.Slice(0, 1500), batch.Slice(1500, 3500)]);
        using var file = ParquetFile.Open(path);
        Statistics?[] s = [.. file.Metadata.Thrift!.RowGroups[0].Columns.Select(c => c.MetaData!.Statistics)];
        Assert.Equal((H(Le(0)), H(Le(4999))), (H(s[0]!.MinValue), H(s[0]!.MaxValue)));
        Assert.Equal((H(Le(1d)), H(Le(5000d))), (H(s[1]!.MinValue), H(s[1]!.MaxValue)));
    }

    // ------------------------------------------------------------------
    // Filtered reads: with statistics against without
    // ------------------------------------------------------------------

    private static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);
    private static ScalarExpr Lit(LiteralValue value) => ScalarExpr.NewLiteral(value);

    [Fact]
    public void FilteredReads_KeepEveryMatchingRow_AtTheBounds()
    {
        var random = new Random(20261115);
        RecordBatch batch = ParquetWriteData.Sorted(4000, random);
        string with = Path.Combine(_dir, "with.parquet"), without = Path.Combine(_dir, "without.parquet");
        var options = new ParquetWriteOptions { RowGroupRows = 500, DataPageRows = 100 };
        ParquetWriter.Write(with, batch.Schema, [batch], options);
        ParquetWriter.Write(without, batch.Schema, [batch], options with { Statistics = false });

        List<(string Column, LiteralValue Literal)> probes = [];
        void At(string column, int row, Func<int, LiteralValue?> literal)
        {
            if (literal(row) is { } value) probes.Add((column, value));
        }
        foreach (int row in (int[])[0, 1, 499, 500, 501, 1999, 2000, 3998, 3999, random.Next(4000), random.Next(4000)])
        {
            IArrowArray C(string name) => batch.Column(batch.Schema.GetFieldIndex(name));
            At("i32", row, i => C("i32").IsNull(i) ? null : LiteralValue.NewInt32(((Int32Array)C("i32")).Values[i]));
            At("u32", row, i => C("u32").IsNull(i) ? null : LiteralValue.NewUInt32(((UInt32Array)C("u32")).Values[i]));
            At("u64", row, i => C("u64").IsNull(i) ? null : LiteralValue.NewUInt64(((UInt64Array)C("u64")).Values[i]));
            At("f64", row, i => C("f64").IsNull(i) || double.IsNaN(((DoubleArray)C("f64")).Values[i]) ? null : LiteralValue.NewFloat64(((DoubleArray)C("f64")).Values[i]));
            At("dec30", row, i => C("dec30").IsNull(i) ? null : LiteralValue.NewDecimal(((Decimal128Array)C("dec30")).GetValue(i)!.Value));
            At("dec5", row, i => C("dec5").IsNull(i) ? null : LiteralValue.NewDecimal(((Decimal128Array)C("dec5")).GetValue(i)!.Value));
            At("str", row, i => C("str").IsNull(i) ? null : LiteralValue.NewString(((StringArray)C("str")).GetString(i)));
            At("day", row, i => C("day").IsNull(i) ? null : LiteralValue.NewDate(DateOnly.FromDayNumber(((Date32Array)C("day")).Values[i] + 719_162)));
        }
        probes.AddRange([("f64", LiteralValue.NewFloat64(0)), ("f64", LiteralValue.NewFloat64(-0d)), ("str", LiteralValue.NewString(new string('q', 70))),
            ("str", LiteralValue.NewString(new string('q', 64))), ("str", LiteralValue.NewString(new string('q', 63) + "r")), ("dec30", LiteralValue.NewDecimal(0m))]);

        int pruned = 0;
        foreach (var (column, literal) in probes)
        {
            foreach (BinaryOp op in (BinaryOp[])[BinaryOp.Eq, BinaryOp.Lt, BinaryOp.LtEq, BinaryOp.Gt, BinaryOp.GtEq])
            {
                ScalarExpr filter = ScalarExpr.NewBinary(op, Col(column), Lit(literal));
                (long[] expected, _) = RowsKept(without, filter);
                (long[] actual, int groups) = RowsKept(with, filter);
                Assert.True(expected.SequenceEqual(actual), $"{column} {op} {literal}: {expected.Length} rows without statistics, {actual.Length} with");
                if (groups < 8) pruned++;
            }
        }
        Assert.True(pruned > probes.Count, $"bounds skipped row groups in only {pruned} reads"); // the check is not vacuous
    }

    private static (long[] Rows, int Groups) RowsKept(string path, ScalarExpr filter)
    {
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = true, RowIndexColumn = "row", Columns = [] });
        List<long> rows = [];
        foreach (RecordBatch b in reader.ReadBatches())
        {
            using (b) rows.AddRange(((Int64Array)b.Column(0)).Values.ToArray());
        }
        return ([.. rows], reader.RowGroupsRead.Count);
    }
}
