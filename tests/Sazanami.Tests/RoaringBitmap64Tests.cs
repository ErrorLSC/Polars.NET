using System.Buffers;
using System.Buffers.Binary;
using Kaminari.Sazanami.Bitmaps;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="RoaringBitmap64"/> against the Delta codec it replaces (<see cref="RoaringOracle"/>, byte for byte) and
/// against a model (a sorted set of rows).
/// </summary>
public class RoaringBitmap64Tests
{
    private static byte[] Serialize(RoaringBitmap64 bitmap)
    {
        var output = new ArrayBufferWriter<byte>();
        bitmap.Serialize(output);
        Assert.Equal(output.WrittenCount, bitmap.SerializedSize);
        return output.WrittenSpan.ToArray();
    }

    private static long[] Rows(RoaringBitmap64 bitmap)
    {
        var rows = new long[bitmap.Cardinality];
        bitmap.CopyTo(rows);
        return rows;
    }

    /// <summary>The model's maximal ranges.</summary>
    private static List<(long Start, long Count)> RangesOf(long[] rows)
    {
        List<(long, long)> ranges = [];
        for (int i = 0; i < rows.Length;)
        {
            int start = i;
            while (i + 1 < rows.Length && rows[i + 1] == rows[i] + 1) i++;
            ranges.Add((rows[start], i - start + 1));
            i++;
        }
        return ranges;
    }

    /// <summary>Everything one set must satisfy: the oracle's bytes both ways, rows, ranges, membership, masks.</summary>
    private static void AssertMatches(long[] rows, RoaringBitmap64 bitmap, Random random)
    {
        byte[] expected = RoaringOracle.Portable(rows);
        Assert.Equal(rows.LongLength, bitmap.Cardinality);
        Assert.Equal(rows.Length == 0, bitmap.IsEmpty);
        Assert.Equal(rows, Rows(bitmap));
        Assert.Equal(expected, Serialize(bitmap));
        Assert.Equal(RangesOf(rows), bitmap.Ranges.ToList());

        using (RoaringBitmap64 read = RoaringBitmap64.Deserialize([.. expected, 0xAB, 0xCD], out int bytesRead))
        {
            Assert.Equal(expected.Length, bytesRead);
            Assert.True(read.SetEquals(bitmap));
            Assert.Equal(rows, Rows(read));
        }

        HashSet<long> set = [.. rows];
        foreach (long row in rows.Take(50)) Assert.True(bitmap.Contains(row));
        foreach (long row in rows.Take(50).SelectMany<long, long>(r => [r - 1, r + 1, r + 65536, r ^ 1]))
            Assert.Equal(set.Contains(row), bitmap.Contains(row));
        Assert.False(bitmap.Contains(-1));
        Assert.Equal(set.Contains(long.MaxValue), bitmap.Contains(long.MaxValue));

        for (int k = 0; k < 4; k++)
        {
            long first = rows.Length > 0 && random.Next(3) > 0 ? Math.Max(0, rows[random.Next(rows.Length)] - random.Next(200)) : random.Next(1 << 18);
            int count = random.Next(4) == 0 ? random.Next(200_000) : random.Next(300);
            AssertMask(set, bitmap, first, count, complement: random.Next(2) == 0);
        }
    }

    private static void AssertMask(HashSet<long> set, RoaringBitmap64 bitmap, long first, int count, bool complement)
    {
        int words = (count + 63) / 64;
        var mask = new ulong[words + 1];
        mask.AsSpan().Fill(0x5A5A_5A5A_5A5A_5A5AUL); // garbage in, and the word after the mask stays
        bitmap.FillMask(first, count, mask, complement);
        for (int i = 0; i < words * 64; i++)
        {
            bool bit = (mask[i >> 6] >> (i & 63) & 1) != 0;
            bool expected = i < count && set.Contains(first + i) != complement;
            if (bit != expected) Assert.Fail($"mask [{first}, +{count}) complement={complement}: bit {i} is {bit}");
        }
        Assert.Equal(0x5A5A_5A5A_5A5A_5A5AUL, mask[words]);
    }

    // ---- random sets: every container kind, near the 4096 / run boundaries, several high words ----

    private static readonly long[] s_highs = [0, 1, 7, int.MaxValue];
    private static readonly int[] s_keys = [0, 1, 2, 5, 0xFFFE, 0xFFFF];

    private static long[] RandomRows(Random random)
    {
        HashSet<long> rows = [];
        int containers = random.Next(6);
        for (int c = 0; c < containers; c++)
        {
            long high = s_highs[random.Next(random.Next(2) == 0 ? 1 : s_highs.Length)];
            long baseRow = high << 32 | (long)s_keys[random.Next(s_keys.Length)] << 16;
            switch (random.Next(7))
            {
                case 0: // sparse
                    for (int i = random.Next(1, 40); i > 0; i--) rows.Add(baseRow + random.Next(1 << 16));
                    break;
                case 1: // around the array / bitset boundary, no runs worth taking
                {
                    int n = 4096 + random.Next(-2, 3);
                    for (int i = 0; i < n; i++) rows.Add(baseRow + 2 * i);
                    break;
                }
                case 2: // dense: a bitset
                    for (int v = 0; v < 1 << 16; v++)
                        if (random.Next(4) != 0) rows.Add(baseRow + v);
                    break;
                case 3: // runs
                    for (int start = random.Next(100); start < 1 << 16; start += random.Next(1, 3000))
                    {
                        int length = random.Next(1, 600);
                        for (int v = start; v < Math.Min(start + length, 1 << 16); v++) rows.Add(baseRow + v);
                        start += length;
                    }
                    break;
                case 4: // the edges of a container
                    foreach (int v in (int[])[0, 1, 65534, 65535]) if (random.Next(2) == 0) rows.Add(baseRow + v);
                    break;
                case 5: // whole container
                    for (int v = 0; v < 1 << 16; v++) rows.Add(baseRow + v);
                    break;
                default: // a run ending at 65535
                    for (int v = 65535 - random.Next(1, 3000); v < 1 << 16; v++) rows.Add(baseRow + v);
                    break;
            }
        }
        return [.. rows.Order()];
    }

    [Fact]
    public void RandomSets_MatchTheOracleAndTheModel()
    {
        var random = new Random(20261117);
        for (int n = 0; n < 400; n++)
        {
            long[] rows = RandomRows(random);
            using RoaringBitmap64 sorted = RoaringBitmap64.FromSorted(rows);
            AssertMatches(rows, sorted, random);

            long[] shuffled = [.. rows, .. rows.Where((_, i) => i % 7 == 0)];
            random.Shuffle(shuffled);
            using RoaringBitmap64 unsorted = RoaringBitmap64.FromRows(shuffled);
            Assert.True(unsorted.SetEquals(sorted));
        }
    }

    [Fact]
    public void SetOperations_MatchTheModel_AndSerializeAsTheOracle()
    {
        var random = new Random(7);
        for (int n = 0; n < 300; n++)
        {
            long[] a = RandomRows(random), b = random.Next(4) == 0 ? a : RandomRows(random);
            if (random.Next(3) == 0) b = [.. b.Concat(a.Where((_, i) => i % 3 == 0)).Distinct().Order()]; // overlap
            using RoaringBitmap64 x = RoaringBitmap64.FromSorted(a), y = RoaringBitmap64.FromSorted(b);
            using RoaringBitmap64 union = x.Union(y), intersect = x.Intersect(y), except = x.Except(y);
            AssertMatches([.. a.Union(b).Order()], union, random);
            AssertMatches([.. a.Intersect(b).Order()], intersect, random);
            AssertMatches([.. a.Except(b).Order()], except, random);
            Assert.Equal(a.ToHashSet().SetEquals(b), x.SetEquals(y));
        }
    }

    // ---- boundaries ----

    public static TheoryData<string, long[]> Boundaries() => new()
    {
        { "empty", [] },
        { "one row", [42] },
        { "row 0", [0] },
        { "value 65535", [65535] },
        { "key 0xFFFF", [0xFFFFL << 16 | 3] },
        { "2^32 - 1 and 2^32", [(1L << 32) - 1, 1L << 32] },
        { "2^32 - 1 alone", [(1L << 32) - 1] },
        { "last high word", [((long)int.MaxValue << 32) | 0xFFFF_FFFFL] },
        // 2 + 4·runs == 2·cardinality (one run of 3): stays an array.
        { "tie, one run of three", [10, 11, 12] },
        { "tie, five in two runs", [10, 11, 12, 20, 21] },
        { "run wins, one run of four", [10, 11, 12, 13] },
        { "array of 4096", [.. Enumerable.Range(0, 4096).Select(i => 2L * i)] },
        { "bitset of 4097", [.. Enumerable.Range(0, 4097).Select(i => 2L * i)] },
        // A bitset container against runs: 2 + 4·2047 = 8190 < 8192 is a run container, 2048 runs are not.
        { "2047 runs over 5000 values", [.. RunsOf(2047, 5000)] },
        { "2048 runs over 5000 values", [.. RunsOf(2048, 5000)] },
        { "run to 65535", [.. Enumerable.Range(60000, 5536).Select(i => (long)i)] },
        { "touching containers", [65534, 65535, 65536, 65537] },
        { "3 containers with runs (no offsets)", [.. Containers(3, run: true)] },
        { "4 containers with runs (offsets)", [.. Containers(4, run: true)] },
        { "3 containers without runs", [.. Containers(3, run: false)] },
        { "4 containers, one with runs", [.. Containers(3, run: false), 3L << 16, (3L << 16) + 1, (3L << 16) + 2, (3L << 16) + 3] },
    };

    /// <summary><paramref name="runs"/> runs holding <paramref name="values"/> values in container 0.</summary>
    private static IEnumerable<long> RunsOf(int runs, int values)
    {
        int length = values / runs, extra = values % runs;
        long at = 0;
        for (int r = 0; r < runs; r++)
        {
            int n = length + (r < extra ? 1 : 0);
            for (int i = 0; i < n; i++) yield return at + i;
            at += n + 1 + (65536 - values - runs) / runs;
        }
    }

    private static IEnumerable<long> Containers(int count, bool run) =>
        Enumerable.Range(0, count).SelectMany(c => run
            ? Enumerable.Range(0, 10).Select(i => ((long)c << 16) + i)
            : Enumerable.Range(0, 10).Select(i => ((long)c << 16) + 3 * i));

    [Theory]
    [MemberData(nameof(Boundaries))]
    public void Boundaries_MatchTheOracle(string name, long[] rows)
    {
        _ = name;
        using RoaringBitmap64 bitmap = RoaringBitmap64.FromSorted(rows);
        AssertMatches(rows, bitmap, new Random(1));
    }

    [Fact]
    public void Empty_IsEightZeroBytes()
    {
        using RoaringBitmap64 empty = RoaringBitmap64.Empty();
        Assert.Equal(new byte[8], Serialize(empty));
        Assert.True(empty.IsEmpty);
        Assert.Empty(empty.Ranges);
        using RoaringBitmap64 one = RoaringBitmap64.FromSorted([5]);
        using RoaringBitmap64 union = empty.Union(one), except = one.Except(one);
        Assert.True(union.SetEquals(one));
        Assert.True(except.IsEmpty);
        Assert.Equal(new byte[8], Serialize(except));
    }

    // ---- what other writers send: the forms the portable writers would not choose ----

    private enum Form { Array, Bitset, Runs, TouchingRuns }

    private static byte[] Bitmap32(bool zeroOffsets, params (ushort Key, Form Form, ushort[] Values)[] containers)
    {
        bool hasRuns = containers.Any(c => c.Form is Form.Runs or Form.TouchingRuns);
        List<byte> bytes = [];
        void U16(int v) { bytes.Add((byte)v); bytes.Add((byte)(v >> 8)); }
        void U32(uint v) { U16((int)(v & 0xFFFF)); U16((int)(v >> 16)); }
        List<byte[]> payloads = [];
        foreach (var (_, form, values) in containers)
        {
            List<byte> p = [];
            void P16(int v) { p.Add((byte)v); p.Add((byte)(v >> 8)); }
            switch (form)
            {
                case Form.Array:
                    foreach (ushort v in values) P16(v);
                    break;
                case Form.Bitset:
                    var words = new ulong[1024];
                    foreach (ushort v in values) words[v >> 6] |= 1UL << (v & 63);
                    var b = new byte[8192];
                    for (int w = 0; w < 1024; w++) BinaryPrimitives.WriteUInt64LittleEndian(b.AsSpan(8 * w), words[w]);
                    p.AddRange(b);
                    break;
                default:
                    // Maximal runs, or every run split in two touching runs where it can be.
                    List<(int Start, int Last)> runs = [];
                    foreach (var (start, count) in RangesOf([.. values.Select(v => (long)v)]))
                    {
                        if (form == Form.TouchingRuns && count > 1)
                        {
                            runs.Add(((int)start, (int)start));
                            runs.Add(((int)start + 1, (int)(start + count - 1)));
                        }
                        else runs.Add(((int)start, (int)(start + count - 1)));
                    }
                    P16(runs.Count);
                    foreach (var (start, last) in runs) { P16(start); P16(last - start); }
                    break;
            }
            payloads.Add([.. p]);
        }
        int n = containers.Length;
        if (hasRuns)
        {
            U32(12347u | (uint)(n - 1) << 16);
            var flags = new byte[(n + 7) / 8];
            for (int c = 0; c < n; c++)
                if (containers[c].Form is Form.Runs or Form.TouchingRuns) flags[c / 8] |= (byte)(1 << (c % 8));
            bytes.AddRange(flags);
        }
        else
        {
            U32(12346);
            U32((uint)n);
        }
        foreach (var (key, _, values) in containers) { U16(key); U16(values.Length - 1); }
        if (!hasRuns || n >= 4)
        {
            int offset = bytes.Count + 4 * n;
            foreach (byte[] p in payloads) { U32(zeroOffsets ? 0 : (uint)offset); offset += p.Length; }
        }
        foreach (byte[] p in payloads) bytes.AddRange(p);
        return [.. bytes];
    }

    private static byte[] Portable64(params (uint High, byte[] Bitmap)[] bitmaps)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, bitmaps.Length);
        return [.. bytes, .. bitmaps.SelectMany(b => (byte[])[.. BitConverter.GetBytes(b.High), .. b.Bitmap])];
    }

    [Fact]
    public void ForeignForms_ReadAsTheirRows_AndSerializeCanonically()
    {
        var random = new Random(3);
        for (int n = 0; n < 300; n++)
        {
            List<(ushort, Form, ushort[])> containers = [];
            int key = random.Next(3);
            for (int c = random.Next(1, 6); c > 0; c--)
            {
                ushort[] values = random.Next(3) switch
                {
                    0 => [.. Enumerable.Range(0, 1 << 16).Where(_ => random.Next(30) == 0).Select(v => (ushort)v)],
                    1 => [.. Enumerable.Range(random.Next(1000), random.Next(1, 3000)).Select(v => (ushort)v)],
                    _ => [.. Enumerable.Range(0, 1 << 16).Where(v => v % 1000 < 400).Select(v => (ushort)v)],
                };
                if (values.Length == 0) values = [7];
                Form form = (Form)random.Next(4);
                if (form == Form.Array && values.Length > 4096) form = Form.Bitset; // the format's own rule
                if (form == Form.Bitset && values.Length <= 4096) form = Form.Array;
                containers.Add(((ushort)key, form, values));
                key += random.Next(1, 3);
            }
            byte[] portable = Portable64((0, Bitmap32(zeroOffsets: random.Next(2) == 0, [.. containers])), (3, Bitmap32(false, [.. containers])));
            long[] rows = RoaringOracle.DecodePortable(portable);
            using RoaringBitmap64 bitmap = RoaringBitmap64.Deserialize(portable);
            AssertMatches(rows, bitmap, random);
        }
    }

    [Fact]
    public void AnEmpty32BitBitmap_IsDropped()
    {
        byte[] portable = Portable64((0, Bitmap32(false)), (1, Bitmap32(false, (2, Form.Array, [9]))));
        using RoaringBitmap64 bitmap = RoaringBitmap64.Deserialize(portable);
        Assert.Equal([(1L << 32) + (2L << 16) + 9], Rows(bitmap));
        Assert.Equal(RoaringOracle.Portable(Rows(bitmap)), Serialize(bitmap));
    }

    // ---- Spark's deletion vectors (Lakehouse test tables) ----

    [Theory]
    [InlineData("table-with-dv-small", new long[] { 0, 9 })]
    [InlineData("with-short-dv", new long[] { 0, 1, 2 })]
    public void SparkDeletionVectors_ReadAsTheOracleReadsThem(string table, long[] expected)
    {
        string file = Assert.Single(Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "TestData", table), "*.bin"));
        byte[] bytes = File.ReadAllBytes(file);
        Assert.Equal(1, bytes[0]); // the format version, then the size, the vector and its checksum
        int size = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(1));
        byte[] vector = bytes[5..(5 + size)];

        long[] rows = RoaringOracle.Decode(vector);
        Assert.Equal(expected, rows);
        using RoaringBitmap64 bitmap = RoaringBitmap64.Deserialize(vector.AsSpan(4), out int read);
        Assert.Equal(size - 4, read);
        Assert.Equal(rows, Rows(bitmap));
        Assert.Equal(RoaringOracle.Serialize(rows), (byte[])[.. vector[..4], .. Serialize(bitmap)]);
    }

    [Fact]
    public void ManyTouchingRuns_Decode()
    {
        // 40 000 runs of one value each, all touching: more pairs than a container has values.
        (int, int)[] runs = [.. Enumerable.Range(0, 40_000).Select(i => (i, 0))];
        byte[] portable = RunBitmap(runs);
        using RoaringBitmap64 bitmap = RoaringBitmap64.Deserialize(portable);
        long[] rows = [.. Enumerable.Range(0, 40_000).Select(i => (long)i)];
        Assert.Equal(RoaringOracle.DecodePortable(portable), rows);
        AssertMatches(rows, bitmap, new Random(5));

        // The most a run count says: 65535 runs (the bytes are there; they overlap, which is the error).
        byte[] damaged = RunBitmap([.. Enumerable.Range(0, 65535).Select(_ => (0, 0))]);
        Assert.Contains("ascending", Assert.Throws<InvalidDataException>(() => RoaringBitmap64.Deserialize(damaged).Dispose()).Message);
    }

    // ---- damaged input ----

    [Fact]
    public void EveryTruncation_IsAFormatError()
    {
        foreach (long[] rows in (long[][])[[0, 9], [.. RunsOf(10, 300)], [.. Enumerable.Range(0, 5000).Select(i => 3L * i)], [.. Containers(5, run: true), 1L << 32]])
        {
            byte[] bytes = RoaringOracle.Portable(rows);
            for (int length = 0; length < bytes.Length; length++)
                Assert.Throws<InvalidDataException>(() => RoaringBitmap64.Deserialize(bytes.AsSpan(0, length)).Dispose());
        }
    }

    [Fact]
    public void DamagedInput_IsAFormatError()
    {
        static string Error(byte[] bytes) => Assert.Throws<InvalidDataException>(() => RoaringBitmap64.Deserialize(bytes).Dispose()).Message;
        byte[] Valid() => Portable64((0, Bitmap32(false, (0, Form.Array, [1, 2]), (1, Form.Array, [7]))));

        byte[] cookie = Valid();
        cookie[12] = 0x99;
        Assert.Contains("cookie", Error(cookie));

        Assert.Contains("ascending", Error(Portable64((0, Bitmap32(false, (1, Form.Array, [1]), (1, Form.Array, [2]))))));
        Assert.Contains("ascending", Error(Portable64((0, Bitmap32(false, (0, Form.Array, [2, 1]))))));
        Assert.Contains("ascending", Error(Portable64((0, Bitmap32(false, (0, Form.Array, [2, 2]))))));
        Assert.Contains("ascending", Error(Portable64((1, Bitmap32(false, (0, Form.Array, [1]))), (1, Bitmap32(false, (1, Form.Array, [1]))))));
        Assert.Contains("ascending", Error(Portable64((2, Bitmap32(false, (0, Form.Array, [1]))), (1, Bitmap32(false, (1, Form.Array, [1]))))));
        Assert.Contains("2^63", Error(Portable64((0x8000_0000, Bitmap32(false, (0, Form.Array, [1]))))));

        // A bitset whose header claims another cardinality; runs overlapping; a run past 65535; runs short of the header.
        byte[] bitset = Portable64((0, Bitmap32(false, (0, Form.Bitset, [.. Enumerable.Range(0, 5000).Select(i => (ushort)(2 * i))]))));
        bitset[8 + 4 + 8 + 2] ^= 1;
        Assert.Contains("header says", Error(bitset));
        Assert.Contains("ascending", Error(RunBitmap((10, 5), (12, 5))));
        Assert.Contains("past its container", Error(RunBitmap((65530, 10))));
        Assert.Contains("header says", Error(RunBitmap(cardinality: 4, (10, 1))));
        Assert.Contains("header says", Error(RunBitmap(cardinality: 1)));

        byte[] count = Valid();
        BinaryPrimitives.WriteInt64LittleEndian(count, long.MaxValue);
        Assert.Contains("truncated", Error(count));
        BinaryPrimitives.WriteInt64LittleEndian(count, -1);
        Assert.Contains("truncated", Error(count));
        byte[] containers = Valid();
        BinaryPrimitives.WriteInt32LittleEndian(containers.AsSpan(16), int.MaxValue);
        Assert.Contains("containers", Error(containers));
    }

    /// <summary>One run container of (start, length − 1) pairs, its header claiming their size unless told otherwise.</summary>
    private static byte[] RunBitmap(params (int Start, int Length)[] runs) => RunBitmap(-1, runs);

    private static byte[] RunBitmap(int cardinality, params (int Start, int Length)[] runs)
    {
        List<byte> b = [];
        void U16(int v) { b.Add((byte)v); b.Add((byte)(v >> 8)); }
        U16(12347);
        U16(0);
        b.Add(1);
        U16(0);
        U16((cardinality < 0 ? runs.Sum(r => r.Length + 1) : cardinality) - 1);
        U16(runs.Length);
        foreach (var (start, length) in runs) { U16(start); U16(length); }
        return Portable64((0, [.. b]));
    }

    [Fact]
    public void SetEquals_ComparesValues_NotOnlyShapes()
    {
        // Same containers, kinds and cardinalities; one value differs.
        using RoaringBitmap64 a = RoaringBitmap64.FromSorted([1, 5, 70_000]), b = RoaringBitmap64.FromSorted([1, 6, 70_000]);
        using RoaringBitmap64 a2 = RoaringBitmap64.FromSorted([1, 5, 70_000]);
        Assert.False(a.SetEquals(b));
        Assert.True(a.SetEquals(a2));
        using RoaringBitmap64 dense = RoaringBitmap64.FromSorted([.. Enumerable.Range(0, 5000).Select(i => 2L * i)]);
        using RoaringBitmap64 shifted = RoaringBitmap64.FromSorted([.. Enumerable.Range(0, 5000).Select(i => 2L * i + 1)]);
        Assert.False(dense.SetEquals(shifted));
    }

    [Fact]
    public void Clone_OwnsItsMemory()
    {
        long[] rows = [3, 4, 5, 6, 70_000, .. Enumerable.Range(0, 5000).Select(i => (2L << 16) + 2 * i)];
        RoaringBitmap64 original = RoaringBitmap64.FromSorted(rows);
        using RoaringBitmap64 copy = original.Clone();
        original.Dispose();
        Assert.Equal(rows, Rows(copy));
        Assert.Equal(RoaringOracle.Portable(rows), Serialize(copy));
    }

    // ---- building ----

    [Fact]
    public void FromSorted_RefusesDisorderAndNegatives()
    {
        Assert.Throws<ArgumentException>(() => RoaringBitmap64.FromSorted([1, 1]));
        Assert.Throws<ArgumentException>(() => RoaringBitmap64.FromSorted([2, 1]));
        Assert.Throws<ArgumentException>(() => RoaringBitmap64.FromSorted([-1, 1]));
        Assert.Throws<ArgumentException>(() => RoaringBitmap64.FromRows([3, -1]));
    }

    [Fact]
    public void Builder_CollectsInAnyOrder_AndEmptiesOnBuild()
    {
        using var builder = new RoaringBitmap64.Builder();
        builder.Add(70_000);
        builder.AddRange([5, 3, 70_000, 1L << 33]);
        builder.Add(3);
        Assert.Equal(6, builder.Count);
        using (RoaringBitmap64 first = builder.Build()) Assert.Equal([3L, 5, 70_000, 1L << 33], Rows(first));
        Assert.Equal(0, builder.Count);
        builder.Add(1);
        using (RoaringBitmap64 second = builder.Build()) Assert.Equal([1L], Rows(second));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Add(-1));
        Assert.Throws<ArgumentException>(() => builder.AddRange([1, -5]));
    }

    [Fact]
    public void Disposed_Refuses()
    {
        RoaringBitmap64 bitmap = RoaringBitmap64.FromSorted([1, 2]);
        bitmap.Dispose();
        bitmap.Dispose();
        Assert.Throws<ObjectDisposedException>(() => bitmap.Contains(1));
        Assert.Throws<ObjectDisposedException>(() => bitmap.CopyTo(new long[2]));
        Assert.Throws<ObjectDisposedException>(() => bitmap.Serialize(new ArrayBufferWriter<byte>()));
        Assert.Throws<ObjectDisposedException>(() => bitmap.Ranges);
    }

    [Fact]
    public void ShortDestinations_AreRefused()
    {
        using RoaringBitmap64 bitmap = RoaringBitmap64.FromSorted([1, 2, 3]);
        Assert.Throws<ArgumentException>(() => bitmap.CopyTo(new long[2]));
        Assert.Throws<ArgumentException>(() => bitmap.FillMask(0, 65, new ulong[1]));
    }
}
