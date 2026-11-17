using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Grouping;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary><see cref="KeyGroups"/> against a model: keys normalized as the IR's grouping takes them, numbered by first appearance.</summary>
public class KeyGroupsTests
{
    /// <summary>NaNs of other payloads and signs than double.NaN's.</summary>
    private static readonly double OtherNaN = BitConverter.Int64BitsToDouble(0x7FF8000000000001L), NegativeNaN = BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8000000000000UL));

    /// <summary>A key column: the kinds a batch may lay it out as (one family), and whether empty strings are null.</summary>
    private sealed record KeyColumn(ColumnKind[] Layouts, bool EmptyIsNull);

    private static ColumnKind Kind(string name) => Kinds.Single(k => k.Name == name);

    private static readonly ColumnKind F64 = Kind("f64") with { Name = "f64nan", Pool = [.. Kind("f64").Pool, OtherNaN, NegativeNaN] };

    /// <summary>The families a key column is drawn from: strings and binaries in any layout, numbers, booleans, decimals, times.</summary>
    private static readonly ColumnKind[][] Families =
    [
        [Kind("utf8"), Kind("view"), Kind("utf8") with { Name = "lutf8", Type = LargeStringType.Default }],
        [Kind("binary"), Kind("bview"), Kind("binary") with { Name = "lbinary", Type = LargeBinaryType.Default }],
        [Kind("i8")], [Kind("u16") with { Pool = [.. Kind("u16").Pool, (Int128)257] }], // 1 and 257 differ in their high byte only [Kind("i32")], [Kind("i64")], [Kind("u64")], [Kind("bool")], [Kind("dec")], [Kind("ts")],
        [Kind("date32")], [Kind("f16")], [Kind("f32")], [F64], [Kind("fsb")],
        [new ColumnKind("fsb3", new FixedSizeBinaryType(3), ["abc"u8.ToArray(), "abd"u8.ToArray(), (byte[])[0, 0, 0], (byte[])[0xFF, 0, 1]], 3)],
    ];

    /// <summary>A value of a key as the model compares it: NaNs as one, -0 as 0, null (and empty when so asked) as one.</summary>
    private static string Model(object? value, bool emptyIsNull) => value switch
    {
        null => "null",
        byte[] { Length: 0 } when emptyIsNull => "null",
        byte[] b => "b:" + Convert.ToHexString(b),
        double d when double.IsNaN(d) => "NaN",
        double d when d == 0 => "0",
        double d => "d:" + d.ToString("R"),
        Int128 i => "i:" + i,
        _ => throw new ArgumentException($"{value}"),
    };

    /// <summary>A column of a layout: plain, large, dictionary, native or managed, maybe sliced.</summary>
    private static IArrowArray Layout(Random random, ColumnKind kind, object?[] values, int offset)
    {
        object?[] all = [.. Enumerable.Repeat<object?>(null, offset), .. values];
        bool large = kind.Type is LargeStringType or LargeBinaryType;
        ColumnKind built = large ? Kind(kind.Type is LargeStringType ? "utf8" : "binary") : kind;
        IArrowArray whole = random.Next(4) switch
        {
            0 when built.Dictionaries && !large => BuildDictionary(random, built, all),
            1 => BuildNative(built, all),
            _ => Build(random, built, all),
        };
        if (large) whole = ArrowTakeTests.ToLarge(whole, kind.Type);
        return offset == 0 ? whole : ArrowArrayFactory.Slice(whole, offset, values.Length);
    }

    [Fact]
    public void RandomBatches_GroupAsTheModel()
    {
        var random = new Random(1124);
        int[] sizes = [0, 1, 5, 64, 65, 300, 2000];
        for (int round = 0; round < 300; round++)
        {
            KeyColumn[] columns = [.. Enumerable.Range(0, random.Next(1, 4)).Select(_ => new KeyColumn(Families[random.Next(Families.Length)], random.Next(3) == 0))];
            using var groups = new KeyGroups(columns.Length, [.. columns.Select(c => c.EmptyIsNull)]);
            Dictionary<string, int> global = [];
            int batches = random.Next(1, 5);
            for (int batch = 0; batch < batches; batch++)
            {
                int rows = sizes[random.Next(sizes.Length)];
                double nulls = random.Next(3) switch { 0 => 0, 1 => 0.1, _ => 0.5 };
                // A small pool per column, so keys repeat.
                object?[][] values = [.. columns.Select(c => Values(random, c.Layouts[0], rows, nulls))];
                IArrowArray[] arrays = [.. columns.Select((c, k) => Layout(random, c.Layouts[random.Next(c.Layouts.Length)], values[k], random.Next(3) == 0 ? random.Next(1, 9) : 0))];
                var ids = new int[rows];
                var batchGroups = new int[rows];
                var firstRows = new int[rows];
                int count = groups.Assign(arrays, ids, batchGroups, firstRows);

                string where = $"round {round} batch {batch}: {string.Join(", ", arrays.Select(a => a.Data.DataType.Name))}, {rows} rows";
                Dictionary<string, int> local = [];
                for (int row = 0; row < rows; row++)
                {
                    string key = string.Join("|", columns.Select((c, k) => Model(values[k][row], c.EmptyIsNull)));
                    if (!local.TryGetValue(key, out int id))
                    {
                        id = local[key] = local.Count;
                        if (!global.ContainsKey(key)) global[key] = global.Count;
                        Assert.True(firstRows[id] == row, $"{where}: first row of group {id}");
                        Assert.True(batchGroups[id] == global[key], $"{where}: global id of group {id} ({key})");
                    }
                    Assert.True(ids[row] == id, $"{where}: row {row} ({key})");
                }
                Assert.True(local.Count == count, where);
                Assert.Equal(global.Count, groups.Count);
                foreach (IArrowArray array in arrays) array.Dispose();
            }
        }
    }

    [Fact]
    public void ManyGroups_OverSeveralBatches()
    {
        using var groups = new KeyGroups(1);
        const int Batch = 100_000;
        var ids = new int[Batch];
        var batchGroups = new int[Batch];
        var firstRows = new int[Batch];
        for (int b = 0; b < 12; b++)
        {
            // Keys b * 87,381 .. + 100,000: each batch adds new keys and repeats some of the last batch's.
            var builder = new Int64Array.Builder();
            for (int i = 0; i < Batch; i++) builder.Append(b * 87_381L + i);
            using Int64Array keys = builder.Build();
            Assert.Equal(Batch, groups.Assign([keys], ids, batchGroups, firstRows));
            // The keys seen are 0 .. max without gaps, first seen in increasing order: a key's global id is the key.
            for (int i = 0; i < Batch; i++)
            {
                if (ids[i] != i || firstRows[i] != i || batchGroups[i] != b * 87_381 + i)
                    Assert.Fail($"batch {b} row {i}: id {ids[i]}, first row {firstRows[i]}, group {batchGroups[i]}");
            }
            Assert.Equal(Batch + b * 87_381, groups.Count);
        }
        Assert.True(groups.ScratchBytes > 0);
        groups.Clear();
        Assert.Equal(0, groups.Count);
        using StringArray strings = new StringArray.Builder().Append("a").Build();
        Assert.Equal(1, groups.Assign([strings], ids, batchGroups, firstRows)); // a cleared column takes a new type
        Assert.Equal(0, batchGroups[0]);
    }

    [Fact]
    public void NullTypeKeys_AreOneNullKey()
    {
        using var groups = new KeyGroups(2);
        using Int32Array ints = new Int32Array.Builder().AppendRange([7, 8, 7, 9]).Build();
        var ids = new int[4];
        Assert.Equal(3, groups.Assign([new NullArray(4), ints], ids, new int[4], new int[4]));
        Assert.Equal([0, 1, 0, 2], ids);
        Assert.Equal(1, groups.Assign([new NullArray(1), ints.Slice(1, 1)], ids, new int[4], new int[4]));
        Assert.Equal(3, groups.Count);
    }

    [Fact]
    public void HashCollisions_AreToldApart_ByTheKeys()
    {
        // A value whose hash is a null's: one column, the value and null collide.
        using var single = new KeyGroups(1);
        using Int64Array values = new Int64Array.Builder().AppendNull().Append(unchecked((long)KeyGroups.NullHash)).AppendNull().Build();
        var ids = new int[3];
        Assert.Equal(2, single.Assign([values], ids, new int[3], new int[3]));
        Assert.Equal([0, 1, 0], ids);

        // Two keys of two columns with one row hash: (1, 5) and (2, b), b solved from the fold.
        ulong target = KeyGroups.Fold(KeyGroups.Fold(KeyGroups.Seed, 1), 5);
        ulong b = KeyGroups.Fold(KeyGroups.Fold(KeyGroups.Seed, 2), 0) ^ target;
        Assert.Equal(target, KeyGroups.Fold(KeyGroups.Fold(KeyGroups.Seed, 2), b));
        using var pairs = new KeyGroups(2);
        using UInt64Array first = new UInt64Array.Builder().AppendRange([1UL, 2UL, 1UL, 2UL]).Build();
        using UInt64Array second = new UInt64Array.Builder().AppendRange([5UL, b, 5UL, 5UL]).Build();
        var pairIds = new int[4];
        Assert.Equal(3, pairs.Assign([first, second], pairIds, new int[4], new int[4]));
        Assert.Equal([0, 1, 0, 2], pairIds);
    }

    [Fact]
    public void Kinds_AndArguments_AreChecked()
    {
        using var groups = new KeyGroups(2, [false, true]);
        using Int32Array ints = new Int32Array.Builder().AppendRange([1, 2]).Build();
        using StringArray strings = new StringArray.Builder().Append("a").Append("").Build();
        var span = new int[2];
        Assert.Equal(2, groups.Assign([ints, strings], span, new int[2], new int[2]));
        // Another integer width, another column count, unequal lengths, short spans.
        using Int64Array longs = new Int64Array.Builder().AppendRange([1L, 2L]).Build();
        Assert.Throws<ArgumentException>(() => groups.Assign([longs, strings], span, new int[2], new int[2]));
        Assert.Throws<ArgumentException>(() => groups.Assign([ints], span, new int[2], new int[2]));
        using Int32Array one = new Int32Array.Builder().Append(1).Build();
        Assert.Throws<ArgumentException>(() => groups.Assign([one, strings], span, new int[2], new int[2]));
        Assert.Throws<ArgumentException>(() => groups.Assign([ints, strings], new int[1], new int[2], new int[2]));
        // Binary is another family than strings; a view of strings is the same.
        using BinaryArray binary = new BinaryArray.Builder().Append("a"u8).Append(""u8).Build();
        Assert.Throws<ArgumentException>(() => groups.Assign([ints, binary], span, new int[2], new int[2]));
        using StringViewArray views = new StringViewArray.Builder().Append("a").Append("").Build();
        var batchGroups = new int[2];
        Assert.Equal(2, groups.Assign([ints, views], span, batchGroups, new int[2]));
        Assert.Equal([0, 1], batchGroups); // (1, "" = null) and (2, "a"): the keys of the first batch
        // Nested keys are not covered; neither is a key count of none.
        var structs = new StructArray(new StructType([new Field("a", Int32Type.Default, true)]), 2, [ints], ArrowBuffer.Empty, 0);
        using var nested = new KeyGroups(1);
        Assert.Throws<NotSupportedException>(() => nested.Assign([structs], span, new int[2], new int[2]));
        Assert.Throws<ArgumentOutOfRangeException>(() => new KeyGroups(0));
        Assert.Throws<ArgumentException>(() => new KeyGroups(2, [true]));
    }
}
