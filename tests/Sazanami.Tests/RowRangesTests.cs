using Kaminari.Sazanami.Rows;

namespace Sazanami.Tests;

/// <summary><see cref="RowRanges"/> against a model: a bool per row.</summary>
public class RowRangesTests
{
    private const int Universe = 300;

    private static bool[] Model(RowRanges r)
    {
        var rows = new bool[Universe];
        foreach (var (start, count) in r)
        {
            for (long i = start; i < start + count; i++) rows[i] = true;
        }
        return rows;
    }

    private static RowRanges FromModel(bool[] rows) => RowRanges.FromIndices([.. Enumerable.Range(0, rows.Length).Where(i => rows[i]).Select(i => (long)i)]);

    /// <summary>Random sets: sparse, dense, runs, with ranges that overlap and touch.</summary>
    private static RowRanges Random(Random random)
    {
        int n = random.Next(8);
        var ranges = new (long, long)[n];
        for (int i = 0; i < n; i++)
        {
            long start = random.Next(Universe - 1);
            ranges[i] = (start, random.Next(4) == 0 ? 0 : random.Next(1, Math.Min(60, Universe - (int)start) + 1));
        }
        return RowRanges.Of(ranges);
    }

    private static void AssertCanonical(RowRanges r)
    {
        for (int i = 0; i < r.Count; i++)
        {
            Assert.True(r[i].Count > 0);
            if (i > 0) Assert.True(r[i].Start > r[i - 1].Start + r[i - 1].Count, $"{r}: ranges {i - 1} and {i} touch or overlap");
        }
        Assert.Equal(r.Sum(x => x.Count), r.RowCount);
    }

    [Fact]
    public void SetOperations_MatchTheModel()
    {
        var random = new Random(42);
        for (int n = 0; n < 3000; n++)
        {
            RowRanges a = Random(random), b = Random(random);
            bool[] ma = Model(a), mb = Model(b);
            foreach (RowRanges r in (RowRanges[])[a, b, a.Union(b), a.Intersect(b), a.Except(b), a.Complement(Universe)]) AssertCanonical(r);
            Assert.Equal(ma.Zip(mb, (x, y) => x || y), Model(a.Union(b)));
            Assert.Equal(ma.Zip(mb, (x, y) => x && y), Model(a.Intersect(b)));
            Assert.Equal(ma.Zip(mb, (x, y) => x && !y), Model(a.Except(b)));
            Assert.Equal(ma.Select(x => !x), Model(a.Complement(Universe)));
            Assert.Equal(a, FromModel(ma));
            int probe = random.Next(Universe);
            Assert.Equal(ma[probe], a.Contains(probe));
        }
    }

    [Fact]
    public void SliceAndTake_MatchTheModel()
    {
        var random = new Random(7);
        for (int n = 0; n < 3000; n++)
        {
            RowRanges a = Random(random);
            bool[] ma = Model(a);
            int start = random.Next(Universe), count = random.Next(Universe - start + 1);
            RowRanges slice = a.Slice(start, count);
            AssertCanonical(slice);
            Assert.Equal(ma.Skip(start).Take(count).Concat(Enumerable.Repeat(false, Universe - count)), Model(slice));
            int take = random.Next(Universe);
            RowRanges first = a.Take(take);
            AssertCanonical(first);
            int[] chosen = [.. Enumerable.Range(0, Universe).Where(i => ma[i]).Take(take)];
            Assert.Equal(Enumerable.Range(0, Universe).Select(chosen.Contains), Model(first));
        }
    }

    [Fact]
    public void FromBits_ReadsEveryBit_AtAnyOffset()
    {
        var random = new Random(3);
        for (int n = 0; n < 2000; n++)
        {
            int count = random.Next(Universe);
            var bitmap = new byte[(count + 7) / 8 + random.Next(3)];
            int density = random.Next(1, 5);
            for (int i = 0; i < count; i++)
            {
                // Runs of ones and zeros of random lengths, so words start inside runs.
                if (random.Next(density * 8) == 0 || i > 0 && (bitmap[(i - 1) >> 3] >> ((i - 1) & 7) & 1) == 1 && random.Next(density) != 0)
                    bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
            // Bits past the count must be ignored.
            if (bitmap.Length > 0) bitmap[^1] |= 0x80;
            long first = random.Next(1000);
            RowRanges r = RowRanges.FromBits(bitmap, count, first);
            AssertCanonical(r);
            long[] expected = [.. Enumerable.Range(0, count).Where(i => (bitmap[i >> 3] >> (i & 7) & 1) == 1).Select(i => first + i)];
            Assert.Equal(RowRanges.FromIndices(expected), r);
        }
    }

    [Fact]
    public void ADeletionVector_IsTheComplementOfItsRows()
    {
        RowRanges kept = RowRanges.FromIndices([3, 4, 5, 9, 0]).Complement(12);
        Assert.Equal("{1..2, 6..8, 10..11}", kept.ToString());
        Assert.Equal(7, kept.RowCount);
        Assert.Throws<ArgumentOutOfRangeException>(() => RowRanges.Of([(-1, 2)]));
        Assert.Throws<ArgumentOutOfRangeException>(() => RowRanges.FromIndices([-4]));
        Assert.Throws<ArgumentException>(() => RowRanges.FromBits([0xFF], 9));
        Assert.True(RowRanges.Of([(5, 0)]).IsEmpty);
    }
}
