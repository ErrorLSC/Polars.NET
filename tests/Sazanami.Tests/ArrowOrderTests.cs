using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Ordering;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ArrowOrder"/> and <see cref="OrderCheck"/> against a model order: every kind (plain and as dictionary
/// values, null slots holding garbage, sliced), both directions and null placements, several keys (later keys only
/// under ties), struct fields under null and sliced structs, values the model pool lacks (wide decimals, negative NaN,
/// UTF-8 against UTF-16 order), and the row kept across batches outliving its batch.
/// </summary>
public class ArrowOrderTests
{
    /// <summary>The model order of two values of a key (nulls placed, direction applied).</summary>
    private static int Model(object? a, object? b, SortKey key)
    {
        if (a is null || b is null) return a is null && b is null ? 0 : (a is null) == key.NullsFirst ? -1 : 1;
        int sign = Compare(a, b);
        return key.Descending ? -sign : sign;
    }

    private static int Model(object?[][] columns, int a, int b, SortKey[] keys)
    {
        for (int k = 0; k < keys.Length; k++)
        {
            int sign = Model(columns[k][a], columns[k][b], keys[k]);
            if (sign != 0) return sign;
        }
        return 0;
    }

    private static int ModelFirstOutOfOrder(object?[][] columns, SortKey[] keys, int start, int count)
    {
        for (int row = start + 1; row < start + count; row++)
        {
            if (Model(columns, row - 1, row, keys) > 0) return row;
        }
        return -1;
    }

    private static SortKey RandomKey(Random random) => new(random.Next(2) == 0, random.Next(2) == 0);

    /// <summary>The rows reordered by the model order (stable).</summary>
    private static object?[][] SortedRows(object?[][] columns, SortKey[] keys)
    {
        int[] order = [.. Enumerable.Range(0, columns[0].Length)];
        order = [.. order.Order(Comparer<int>.Create((a, b) => Model(columns, a, b, keys) is var s && s != 0 ? s : a.CompareTo(b)))];
        return [.. columns.Select(c => order.Select(i => c[i]).ToArray())];
    }

    public static TheoryData<string, bool> KindsData()
    {
        var data = new TheoryData<string, bool>();
        foreach (ColumnKind kind in Kinds)
        {
            data.Add(kind.Name, false);
            if (kind.Dictionaries) data.Add(kind.Name, true);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(KindsData))]
    public void EveryKindFollowsTheModelOrder(string name, bool dictionary)
    {
        ColumnKind kind = Kinds.Single(k => k.Name == name);
        var random = new Random(name.GetHashCode(StringComparison.Ordinal) ^ (dictionary ? 1 : 0) ^ 20261128);
        for (int round = 0; round < 60; round++)
        {
            SortKey[] keys = [RandomKey(random)];
            // Short runs (scalar tails), and long ones: vectors, bitmap words, several chunks of pairs.
            int n = random.Next(4) == 0 ? random.Next(100, 9000) : random.Next(1, 40);
            object?[][] columns = [Values(random, kind, n, random.Next(3) * 0.2)];
            if (random.Next(3) > 0) columns = SortedRows(columns, keys);
            if (random.Next(2) == 0 && n > 2)
            {
                // A random swap: out of order where the values differ.
                int i = random.Next(n - 1);
                (columns[0][i], columns[0][i + 1]) = (columns[0][i + 1], columns[0][i]);
            }
            IArrowArray array = dictionary ? BuildDictionary(random, kind, columns[0]) : Build(random, kind, columns[0]);
            int start = random.Next(n), count = random.Next(n - start + 1);
            int expected = ModelFirstOutOfOrder(columns, keys, start, count);
            Assert.Equal(expected, ArrowOrder.FirstOutOfOrder([new OrderColumn(array)], keys, start, count, out int key));
            Assert.Equal(expected < 0 ? -1 : 0, key);

            // Sliced: the same rows from the slice's start.
            if (count > 0)
            {
                IArrowArray slice = ArrowArrayFactory.BuildArray(array.Data.Slice(start, count));
                int shifted = ArrowOrder.FirstOutOfOrder([new OrderColumn(slice)], keys, 0, count, out _);
                Assert.Equal(expected < 0 ? -1 : expected - start, shifted);
            }

            // Any two rows, across arrays (the second laid out anew).
            IArrowArray other = dictionary ? BuildDictionary(random, kind, columns[0]) : Build(random, kind, columns[0]);
            for (int t = 0; t < 5; t++)
            {
                int a = random.Next(n), b = random.Next(n);
                int sign = Math.Sign(ArrowOrder.Compare([new OrderColumn(array)], a, [new OrderColumn(other)], b, keys, out int k));
                int model = Math.Sign(Model(columns, a, b, keys));
                Assert.True(model == sign, $"{name} {keys[0]}: rows {a} ({Show(columns[0][a])}) and {b} ({Show(columns[0][b])}): {sign}, the model {model}");
                Assert.Equal(model == 0 ? -1 : 0, k);
            }
        }
    }

    private static string Show(object? value) => value switch
    {
        null => "null",
        byte[] b => Convert.ToHexString(b),
        _ => value.ToString()!,
    };

    [Fact]
    public void LaterKeysOrderOnlyRowsTheKeysBeforeThemTie()
    {
        var random = new Random(7);
        ColumnKind[] kinds = [Kinds.Single(k => k.Name == "i32"), Kinds.Single(k => k.Name == "utf8"), Kinds.Single(k => k.Name == "f64")];
        for (int round = 0; round < 300; round++)
        {
            SortKey[] keys = [RandomKey(random), RandomKey(random), RandomKey(random)];
            int n = random.Next(2, 50);
            // Few distinct values, so later keys decide often.
            object?[][] columns = [.. kinds.Select(k => (object?[])[.. Values(random, k with { Pool = k.Pool[..3] }, n, 0.15)])];
            if (random.Next(4) > 0) columns = SortedRows(columns, keys);
            if (random.Next(2) == 0)
            {
                int i = random.Next(n - 1), c = random.Next(3);
                (columns[c][i], columns[c][i + 1]) = (columns[c][i + 1], columns[c][i]);
            }
            OrderColumn[] arrays = [.. columns.Select((c, i) => new OrderColumn(Build(random, kinds[i], c)))];
            int expected = ModelFirstOutOfOrder(columns, keys, 0, n);
            int row = ArrowOrder.FirstOutOfOrder(arrays, keys, 0, n, out int key);
            Assert.Equal(expected, row);
            if (row >= 0)
            {
                // The key named is the first that does not tie, and orders the rows wrongly.
                for (int k = 0; k < key; k++) Assert.Equal(0, Model(columns[k][row - 1], columns[k][row], keys[k]));
                Assert.True(Model(columns[key][row - 1], columns[key][row], keys[key]) > 0);
            }
        }
    }

    /// <summary>Strings of at most 16 bytes (those compared 16 bytes at a time): lengths 0, 15, 16, prefixes of each other, high bytes.</summary>
    private static readonly object[] ShortStrings =
    [
        ""u8.ToArray(), "a"u8.ToArray(), "a\0"u8.ToArray(), "ab"u8.ToArray(), "abcdefghijklmno"u8.ToArray(), "abcdefghijklmnop"u8.ToArray(),
        "abcdefghijklmnoq"u8.ToArray(), "abcdefghijklmnp"u8.ToArray(), "abcdefghi"u8.ToArray(), "abcdefghijkl"u8.ToArray(), "abcdefghijkm"u8.ToArray(),
        "abcdefghik"u8.ToArray(), "b"u8.ToArray(), (byte[])[0xFF], (byte[])[0x7F, 0xFF], "zzzzzzzzzzzzzzzz"u8.ToArray(),
    ];

    [Theory]
    [InlineData("utf8")]
    [InlineData("binary")]
    [InlineData("view")]
    public void LongRunsOfShortStringsFollowTheModelOrder(string name)
    {
        // Thousands of rows (whole bitmap words and chunks of pairs) of short values, a second key under their ties,
        // read from inside a longer array (the data holds bytes after the rows) and up to its end.
        ColumnKind kind = Kinds.Single(k => k.Name == name) with { Pool = ShortStrings };
        ColumnKind ints = Kinds.Single(k => k.Name == "i32");
        var random = new Random(name.Length * 31);
        for (int round = 0; round < 40; round++)
        {
            SortKey[] keys = [RandomKey(random), RandomKey(random)];
            int n = random.Next(200, 9000);
            object?[][] columns = [Values(random, kind, n, random.Next(3) * 0.1), Values(random, ints with { Pool = ints.Pool[..3] }, n, 0.1)];
            columns = SortedRows(columns, keys);
            for (int swaps = random.Next(3); swaps > 0; swaps--)
            {
                int i = random.Next(n - 1), c = random.Next(2);
                (columns[c][i], columns[c][i + 1]) = (columns[c][i + 1], columns[c][i]);
            }
            OrderColumn[] arrays = [new(Build(random, kind, columns[0])), new(Build(random, ints, columns[1]))];
            int start = random.Next(2) == 0 ? 0 : random.Next(n / 2), count = random.Next(2) == 0 ? n - start : random.Next(n - start + 1);
            Assert.Equal(ModelFirstOutOfOrder(columns, keys, start, count), ArrowOrder.FirstOutOfOrder(arrays, keys, start, count, out _));
            Assert.Equal(ModelFirstOutOfOrder(columns, keys[..1], start, count), ArrowOrder.FirstOutOfOrder(arrays[..1], keys[..1], start, count, out _));
        }
    }

    /// <summary>Long values sharing long prefixes, differing past byte 16 or 32, prefixes of each other, beside inline ones.</summary>
    private static readonly object[] LongStrings =
    [
        "abc"u8.ToArray(), "abcd"u8.ToArray(), "abcdefghijklm"u8.ToArray(), "abcdefghijklmnopqrstuvw"u8.ToArray(), "abcdefghijklmnopqrstuvx"u8.ToArray(),
        "abcdefghijklmnopqrstuvwxyz0123456789A"u8.ToArray(), "abcdefghijklmnopqrstuvwxyz0123456789B"u8.ToArray(), "abcdefghijklmnopq"u8.ToArray(),
        "abcdefghijklmnopq\0"u8.ToArray(), "abcdefghijklmnopqrstuvwxyz0123456"u8.ToArray(), "abce"u8.ToArray(), (byte[])[.. "abcdefghijklmnop"u8, 0xFF, 1],
    ];

    [Theory]
    [InlineData("utf8")]
    [InlineData("binary")]
    [InlineData("view")]
    [InlineData("bview")]
    public void LongValuesFollowTheModelOrder(string name)
    {
        ColumnKind kind = Kinds.Single(k => k.Name == name) with { Pool = LongStrings };
        ColumnKind ints = Kinds.Single(k => k.Name == "i32");
        var random = new Random(name.Length * 17);
        for (int round = 0; round < 40; round++)
        {
            SortKey[] keys = [RandomKey(random), RandomKey(random)];
            int n = random.Next(2, 3000);
            object?[][] columns = [Values(random, kind, n, random.Next(3) * 0.1), Values(random, ints with { Pool = ints.Pool[..3] }, n, 0.1)];
            if (random.Next(4) > 0) columns = SortedRows(columns, keys);
            for (int swaps = random.Next(3); swaps > 0; swaps--)
            {
                int i = random.Next(n - 1), c = random.Next(2);
                (columns[c][i], columns[c][i + 1]) = (columns[c][i + 1], columns[c][i]);
            }
            OrderColumn[] arrays = [new(Build(random, kind, columns[0])), new(Build(random, ints, columns[1]))];
            Assert.Equal(ModelFirstOutOfOrder(columns, keys, 0, n), ArrowOrder.FirstOutOfOrder(arrays, keys, 0, n, out _));
            for (int t = 0; t < 10; t++)
            {
                int a = random.Next(n), b = random.Next(n);
                Assert.Equal(Math.Sign(Model(columns, a, b, keys)), Math.Sign(ArrowOrder.Compare(arrays, a, arrays, b, keys, out _)));
            }
        }
    }

    [Fact]
    public void ValuesPastSixteenBytesAreComparedWhole()
    {
        // Equal in their first 17 bytes; a third row after them, so the data holds bytes past the second.
        var strings = new StringArray.Builder().Append("abcdefghijklmnopqs").Append("abcdefghijklmnopqr").Append("zzzzzzzzzzzzzzzzzzzz").Build();
        Assert.Equal(1, ArrowOrder.FirstOutOfOrder([new OrderColumn(strings)], [new SortKey()], 0, 2, out _));
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(strings)], [new SortKey(Descending: true)], 0, 2, out _));
    }

    [Fact]
    public void TiesOnEveryFastPathLeaveLaterKeysToDecide()
    {
        // A first key tied over 200 rows (several bitmap words), a second key out of order at row 130 alone. Short
        // strings get rows after the 200 so their data holds 16 bytes past the last pair.
        const int n = 200;
        IArrowArray[] firsts =
        [
            new StringArray.Builder().AppendRange([.. Enumerable.Repeat("aa", n), .. Enumerable.Repeat("zzzzzzzzzzzzzzzz", 4)]).Build(),
            new DoubleArray.Builder().AppendRange(Enumerable.Repeat(double.NaN, n)).Build(),
            new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, n)).Build(),
            new Int64Array.Builder().AppendRange(Enumerable.Repeat(7L, n)).Build(),
            Dictionary(["x", "y"], [.. Enumerable.Repeat((int?)1, n)]), // sorted values: by index
            Dictionary(["y", "x"], [.. Enumerable.Repeat((int?)0, n)]), // not sorted: one index, one value
        ];
        int[] values = [.. Enumerable.Range(0, n)];
        using var sorted = new Int32Array.Builder().AppendRange(values).Build();
        values[130] = -1;
        using var broken = new Int32Array.Builder().AppendRange(values).Build();
        foreach (IArrowArray first in firsts)
        {
            SortKey[] order = [new(), new()];
            Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(first), new OrderColumn(sorted)], order, 0, n, out _));
            Assert.Equal(130, ArrowOrder.FirstOutOfOrder([new OrderColumn(first), new OrderColumn(broken)], order, 0, n, out int key));
            Assert.Equal(1, key);
        }
    }

    [Fact]
    public void ManyValuesPastSixteenBytesAreComparedWhole()
    {
        // 18-byte values, equal in their first 17 bytes, among enough others for the lengths to be looked at a vector at a time.
        // The long ones first, short ones after them (the lengths of all lie in whole vectors).
        string[] rows = [.. Enumerable.Range(0, 16).Select(i => $"abcdefghijklmnopq{(char)('a' + i)}"), .. Enumerable.Range(0, 8).Select(i => $"{(char)('b' + i)}")];
        (rows[6], rows[7]) = (rows[7], rows[6]);
        var strings = new StringArray.Builder().AppendRange([.. rows, "zzzzzzzzzzzzzzzzzzzz"]).Build();
        Assert.Equal(7, ArrowOrder.FirstOutOfOrder([new OrderColumn(strings)], [new SortKey()], 0, rows.Length, out _));
    }

    [Fact]
    public void DictionariesWithRepeatedValuesCompareByValue()
    {
        // ["a", "a", "b"]: ascending but not distinct, so indices 1 then 0 tie (by value), and the second key decides.
        IArrowArray dictionary = new StringArray.Builder().AppendRange(["a", "a", "b"]).Build();
        var data = new ArrayData(new DictionaryType(Int32Type.Default, StringType.Default, false), 4, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer((byte[])[.. ((int[])[1, 0, 0, 2]).SelectMany(BitConverter.GetBytes)])], [], dictionary.Data);
        IArrowArray keys = ArrowArrayFactory.BuildArray(data);
        using var second = new Int32Array.Builder().AppendRange([1, 2, 3, 0]).Build();
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(keys), new OrderColumn(second)], [new SortKey(), new SortKey()], 0, 4, out _));
        Assert.Equal(1, ArrowOrder.FirstOutOfOrder([new OrderColumn(keys), new OrderColumn(second)], [new SortKey(), new SortKey(Descending: true)], 0, 4, out int key));
        Assert.Equal(1, key);
    }

    [Fact]
    public void SortedDictionariesCompareByIndexAsByValue()
    {
        // Distinct ascending values: the indices are compared (a vector at a time); null rows hold garbage indices.
        var random = new Random(5);
        string[] values = ["", "apple", "banana", "cherry", "date", "fig", "grape"];
        foreach (SortKey key in (SortKey[])[new(), new(Descending: true), new(NullsFirst: true), new(true, true)])
        {
            for (int round = 0; round < 30; round++)
            {
                int n = random.Next(values.Length, 3000);
                int?[] rows = [.. Enumerable.Range(0, n).Select(_ => random.Next(8) == 0 ? (int?)null : random.Next(values.Length))];
                object?[][] model = [[.. rows.Select(r => r is int v ? (object)System.Text.Encoding.UTF8.GetBytes(values[v]) : null)]];
                model = SortedRows(model, [key]);
                if (random.Next(2) == 0)
                {
                    int i = random.Next(n - 1);
                    (model[0][i], model[0][i + 1]) = (model[0][i + 1], model[0][i]);
                }
                byte[] validity = new byte[(n + 7) / 8];
                int[] indices = new int[n];
                for (int i = 0; i < n; i++)
                {
                    if (model[0][i] is byte[] b)
                    {
                        validity[i >> 3] |= (byte)(1 << (i & 7));
                        indices[i] = System.Array.IndexOf(values, System.Text.Encoding.UTF8.GetString(b));
                    }
                    else indices[i] = random.Next(2) == 0 ? 99_999 : random.Next(values.Length); // garbage
                }
                IArrowArray dictionary = new StringArray.Builder().AppendRange(values).Build();
                int nulls = model[0].Count(v => v is null);
                var data = new ArrayData(new DictionaryType(Int32Type.Default, StringType.Default, false), n, nulls, 0,
                    [new ArrowBuffer(validity), new ArrowBuffer((byte[])[.. indices.SelectMany(BitConverter.GetBytes)])], [], dictionary.Data);
                IArrowArray array = ArrowArrayFactory.BuildArray(data);
                Assert.Equal(ModelFirstOutOfOrder(model, [key], 0, n), ArrowOrder.FirstOutOfOrder([new OrderColumn(array)], [key], 0, n, out _));
            }
        }
    }

    [Fact]
    public void SegmentsCheckedSideBySideFindTheFirstBreak()
    {
        // 300 000 rows in segments: breaks at a segment's first row, its last, inside, several at once.
        Assert.Equal(4096, ArrowOrder.SegmentRows);
        const int n = 300_000;
        var random = new Random(11);
        foreach (int[] breaks in (int[][])[[], [4096], [4095], [4097], [16_384], [65_537, 200_000], [299_999], [5, 250_000], [196_608], [32_768, 4_099]])
        {
            long[] a = [.. Enumerable.Range(0, n).Select(i => (long)(i / 3))];
            long[] b = [.. Enumerable.Range(0, n).Select(i => (long)(i % 3))];
            foreach (int at in breaks) b[at] = -1; // ties on a there, unless the row starts a run of a
            using var first = new Int64Array.Builder().AppendRange(a).Build();
            using var second = new Int64Array.Builder().AppendRange(b).Build();
            OrderColumn[] keys = [new(first), new(second)];
            SortKey[] order = [new(), new()];
            int serial = ArrowOrder.FirstOutOfOrder(keys, order, 0, n, out int serialKey);
            int expected = breaks.Where(at => at % 3 != 0).DefaultIfEmpty(-1).Min();
            Assert.Equal(expected, serial);
            foreach (int threads in (int[])[2, 4, 8])
            {
                int start = random.Next(2) == 0 ? 0 : 1;
                int parallel = ArrowOrder.FirstOutOfOrder(keys, order, start, n - start, new WorkBudget(threads), out int key);
                Assert.Equal(serial, parallel);
                Assert.Equal(serialKey, key);
            }
        }
    }

    [Fact]
    public void StructFieldsAreNullWhereTheirStructIs()
    {
        // s: {x} with s null at row 1 (x holds 0 there), sliced from row 1 of a longer struct.
        var xType = new StructType([new Field("x", Int32Type.Default, true)]);
        var x = new Int32Array.Builder().AppendRange([100, 5, 0, 7, 8]).Build();
        var validity = new ArrowBuffer((byte[])[0b11011]);
        var s = new StructArray(xType, 5, [x], validity, 1);
        IArrowArray sliced = ArrowArrayFactory.BuildArray(s.Data.Slice(1, 4)); // rows: 5, null, 7, 8
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(sliced, [0])], [new SortKey()], 0, 2, out _)); // 5, null: nulls last
        Assert.Equal(2, ArrowOrder.FirstOutOfOrder([new OrderColumn(sliced, [0])], [new SortKey()], 0, 4, out _));          // null, 7
        Assert.Equal(1, ArrowOrder.FirstOutOfOrder([new OrderColumn(sliced, [0])], [new SortKey(NullsFirst: true)], 0, 4, out _)); // 5, null
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(sliced, [0])], [new SortKey(NullsFirst: true)], 1, 3, out _)); // null, 7, 8
        Assert.Equal("null", ArrowOrder.Format(new OrderColumn(sliced, [0]), 1));
        Assert.Equal("7", ArrowOrder.Format(new OrderColumn(sliced, [0]), 2));
        Assert.Equal(Int32Type.Default, ArrowOrder.KeyType(xType, [0]));
        Assert.Null(ArrowOrder.KeyType(Int32Type.Default, [0]));
        Assert.Throws<ArgumentException>(() => ArrowOrder.FirstOutOfOrder([new OrderColumn(x, [0])], [new SortKey()], 0, 5, out _));
    }

    [Fact]
    public void WideDecimalsCompareByValue()
    {
        // decimal256: values differing in the high half, the low half (unsigned), and the sign.
        System.Numerics.BigInteger[] values = [-(System.Numerics.BigInteger.One << 200), -1, 0, 1, (System.Numerics.BigInteger.One << 127) + 5, (System.Numerics.BigInteger.One << 128), System.Numerics.BigInteger.One << 200];
        var bytes = new byte[values.Length * 32];
        for (int i = 0; i < values.Length; i++)
        {
            byte[] raw = values[i].ToByteArray(); // little-endian two's complement
            Span<byte> slot = bytes.AsSpan(i * 32, 32);
            slot.Fill(values[i].Sign < 0 ? (byte)0xFF : (byte)0);
            raw.CopyTo(slot);
        }
        IArrowArray dec256 = ArrowArrayFactory.BuildArray(new ArrayData(new Decimal256Type(76, 0), values.Length, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(dec256)], [new SortKey()], 0, values.Length, out _));
        Assert.Equal(1, ArrowOrder.FirstOutOfOrder([new OrderColumn(dec256)], [new SortKey(Descending: true)], 0, values.Length, out _));
        for (int a = 0; a < values.Length; a++)
        {
            for (int b = 0; b < values.Length; b++)
            {
                Assert.Equal(values[a].CompareTo(values[b]), Math.Sign(ArrowOrder.Compare([new OrderColumn(dec256)], a, [new OrderColumn(dec256)], b, [new SortKey()], out _)));
            }
        }
        // decimal32 / decimal64 by value.
        IArrowArray dec32 = ArrowArrayFactory.BuildArray(new ArrayData(new Decimal32Type(9, 2), 3, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer((byte[])[.. ((int[])[-5, 0, 7]).SelectMany(BitConverter.GetBytes)])]));
        IArrowArray dec64 = ArrowArrayFactory.BuildArray(new ArrayData(new Decimal64Type(18, 2), 3, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer((byte[])[.. ((long[])[9, -1, 3]).SelectMany(BitConverter.GetBytes)])]));
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(dec32)], [new SortKey()], 0, 3, out _));
        Assert.Equal(1, ArrowOrder.FirstOutOfOrder([new OrderColumn(dec64)], [new SortKey()], 0, 3, out _));
    }

    [Fact]
    public void NegativeNaNSortsAboveEveryNumberAsNaNDoes()
    {
        double negativeNaN = BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8_0000_0000_0001));
        Assert.True(double.IsNaN(negativeNaN));
        var doubles = new DoubleArray.Builder().AppendRange([double.NegativeInfinity, -0.0, 0.0, double.PositiveInfinity, negativeNaN, double.NaN]).Build();
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(doubles)], [new SortKey()], 0, 6, out _));
        var floats = new FloatArray.Builder().AppendRange([1f, BitConverter.Int32BitsToSingle(unchecked((int)0xFFC0_0001)), 2f]).Build();
        Assert.Equal(2, ArrowOrder.FirstOutOfOrder([new OrderColumn(floats)], [new SortKey()], 0, 3, out _));
    }

    [Fact]
    public void StringsCompareInCodePointOrderNotUtf16Order()
    {
        // U+FFFD sorts before U+1F600 by code point (and UTF-8 bytes); UTF-16 ordinal order puts the surrogate pair first.
        Assert.True(string.CompareOrdinal("\U0001F600", "�") < 0);
        var strings = new StringArray.Builder().Append("�").Append("\U0001F600").Build();
        Assert.Equal(-1, ArrowOrder.FirstOutOfOrder([new OrderColumn(strings)], [new SortKey()], 0, 2, out _));
    }

    [Fact]
    public void UnorderedTypesAreRefused()
    {
        Assert.False(ArrowOrder.CanOrder(NullType.Default));
        Assert.False(ArrowOrder.CanOrder(new ListType(Int32Type.Default)));
        Assert.False(ArrowOrder.CanOrder(new DictionaryType(Int32Type.Default, new StructType([new Field("x", Int32Type.Default, true)]), false)));
        Assert.True(ArrowOrder.CanOrder(new DictionaryType(Int8Type.Default, StringType.Default, false)));
        var list = new ListArray.Builder(Int32Type.Default);
        list.Append();
        list.Append();
        Assert.Throws<NotSupportedException>(() => ArrowOrder.FirstOutOfOrder([new OrderColumn(list.Build())], [new SortKey()], 0, 2, out _));
        Assert.Throws<ArgumentException>(() => ArrowOrder.FirstOutOfOrder([new OrderColumn(list.Build())], [], 0, 2, out _));
    }

    [Fact]
    public void TheRowKeptOutlivesItsBatchsMemory()
    {
        // Native batches, freed after each check: the next batch may reuse their memory, the row kept must not see it.
        ColumnKind longs = Kinds.Single(k => k.Name == "i64");
        using var check = new OrderCheck([new SortKey()]);
        for (int round = 0; round < 20; round++)
        {
            check.Restart();
            using (IArrowArray first = BuildNative(longs, [(Int128)1, (Int128)5]))
                Assert.True(check.Check([new OrderColumn(first)], 0, 2, out _));
            using IArrowArray second = BuildNative(longs, [(Int128)6, (Int128)7]);
            Assert.True(check.Check([new OrderColumn(second)], 0, 2, out OrderBreak broken), $"round {round}: {broken}");
        }
    }

    private static DictionaryArray Dictionary(string[] values, int?[] indices)
    {
        var b = new Int32Array.Builder();
        foreach (int? i in indices) b.Append(i);
        return new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false), b.Build(), new StringArray.Builder().AppendRange(values).Build());
    }

    [Fact]
    public void DictionaryKeysAreCheckedAcrossBatchesOfOtherDictionaries()
    {
        // Few rows against larger dictionaries: a sorted one (kept as known), then an unsorted one whose indices rise
        // where its values fall; the row kept between them is the one value alone, nulls too.
        string[] sorted = ["a", "b", "c", "d", "e", "f", "g", "h"], falling = ["h", "g", "f", "e", "d", "c", "b", "a"];
        using var check = new OrderCheck([new SortKey(NullsFirst: true)]);
        Assert.True(check.Check([new OrderColumn(Dictionary(sorted, [null, 0, 2]))], 0, 3, out _));
        Assert.True(check.Check([new OrderColumn(Dictionary(sorted, [3, 4]))], 0, 2, out _));
        Assert.False(check.Check([new OrderColumn(Dictionary(falling, [3, 4, 5]))], 0, 3, out OrderBreak broken)); // e, d, c after e
        Assert.Equal(new OrderBreak(6, 0, "\"e\"", "\"d\""), broken);
        // The value kept is the row's own (e), not the entry its position would name: c after it breaks at once.
        using var kept = new OrderCheck([new SortKey()]);
        Assert.True(kept.Check([new OrderColumn(Dictionary(sorted, [3, 4]))], 0, 2, out _));
        Assert.False(kept.Check([new OrderColumn(Dictionary(falling, [5]))], 0, 1, out broken));
        Assert.Equal(new OrderBreak(2, 0, "\"e\"", "\"c\""), broken);

        using var nulls = new OrderCheck([new SortKey()]);
        Assert.True(nulls.Check([new OrderColumn(Dictionary(sorted, [1, null]))], 0, 2, out _));
        Assert.False(nulls.Check([new OrderColumn(Dictionary(falling, [7]))], 0, 1, out broken)); // a value after the null kept
        Assert.Equal("null", broken.Previous);
    }

    [Fact]
    public void TheCheckKeepsTheLastRowAcrossBatchesAndStartsOver()
    {
        using var check = new OrderCheck([new SortKey(), new SortKey(Descending: true)]);
        Assert.True(check.Check(Keys([1, 1], ["b", "a"], out var first), 0, 2, out _));
        Dispose(first); // the row kept is a copy: the batch may go
        Assert.True(check.Check(Keys([1, 2], ["a", "z"], out var second), 0, 2, out _));
        Dispose(second);
        Assert.Equal(4, check.Rows);
        // 2/"z" kept: 2/"zz" sorts before it (descending).
        Assert.False(check.Check(Keys([2, 3], ["zz", "a"], out var third), 0, 2, out OrderBreak broken));
        Assert.Equal(new OrderBreak(4, 1, "\"z\"", "\"zz\""), broken);
        // A batch's rows from an offset, counted from there.
        Assert.False(check.Check(Keys([9, 2, 3, 1], ["", "", "", ""], out _), 1, 3, out broken));
        Assert.Equal(new OrderBreak(6, 0, "3", "1"), broken);
        check.Restart();
        Assert.Equal(0, check.Rows);
        Assert.True(check.Check(Keys([0], ["a"], out _), 0, 1, out _)); // no longer compared with 2/"z"
        Assert.True(check.Check(Keys([], [], out _), 0, 0, out _));
        Assert.Equal(1, check.Rows);
        Dispose(third);

        static OrderColumn[] Keys(int[] a, string[] b, out IArrowArray[] arrays)
        {
            arrays = [new Int32Array.Builder().AppendRange(a).Build(), new StringArray.Builder().AppendRange(b).Build()];
            return [new OrderColumn(arrays[0]), new OrderColumn(arrays[1])];
        }

        static void Dispose(IArrowArray[] arrays)
        {
            foreach (IArrowArray array in arrays) array.Dispose();
        }
    }
}
