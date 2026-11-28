using Apache.Arrow;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Writing;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The shredder's bulk walk (step by step over every row at once) against its per-row walk, the oracle: the same
/// definition and repetition levels and the same value positions, for lists, large and fixed-size lists, structs and
/// maps nested in each other, with nulls and empty lists at every level, whole columns, windows of them and slices.
/// </summary>
public class ParquetShredderTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void TheBulkWalk_ShredsAsTheRowWalk(int seed)
    {
        using RecordBatch batch = ParquetWriterInteropTests.NestedCommon(seed, 700);
        var (_, plans) = WriteSchema.Plan(batch.Schema, new ParquetWriteOptions());
        int shredded = 0;
        foreach (ColumnPlan plan in plans)
        {
            if (plan.Steps is null) continue;
            IArrowArray column = batch.Column(plan.Field);
            string where = string.Join('.', plan.Path);
            Same(plan.Steps, column, 0, 700, $"{where} whole");
            Same(plan.Steps, column, 13, 500, $"{where} window");
            Same(plan.Steps, column, 699, 1, $"{where} last row");
            // A slice shares the column's buffers, from an offset (not disposed: disposing it would free them).
            IArrowArray slice = ArrowArrayFactory.Slice(column, 37, 600);
            Same(plan.Steps, slice, 0, 600, $"{where} slice");
            Same(plan.Steps, slice, 5, 77, $"{where} slice window");
            shredded++;
        }
        Assert.True(shredded >= 10, $"{shredded} nested leaves");
    }

    /// <summary>
    /// A leveled write's step (whole rows, as many as the page has room for, a few thousand levels at most) against
    /// the level-by-level loop it replaced, on random repetition levels: short and long rows, rooms of one row to
    /// more than a step.
    /// </summary>
    [Fact]
    public void ALeveledStep_TakesTheRowsTheLevelLoopTook()
    {
        var random = new Random(4711);
        for (int trial = 0; trial < 3000; trial++)
        {
            int length = random.Next(1, trial % 3 == 0 ? 20_000 : 600);
            int itemsPerRow = random.Next(1, trial % 5 == 0 ? 3000 : 12);
            var rep = new byte[length];
            for (int i = 1; i < length; i++) rep[i] = random.Next(itemsPerRow) == 0 ? (byte)0 : (byte)random.Next(1, 3);
            int room = random.Next(1, trial % 4 == 0 ? 8000 : 40);
            Assert.Equal(Loop(rep, room), ColumnWriter.Step(rep, room));
        }

        static (int, int) Loop(byte[] rep, int room)
        {
            int levels = 0, rows = 0;
            while (levels < rep.Length && (rep[levels] != 0 || (rows < room && levels < ColumnWriter.LevelStep)))
            {
                if (rep[levels] == 0) rows++;
                levels++;
            }
            return (levels, rows);
        }
    }

    private static void Same(ShredStep[] steps, IArrowArray column, int offset, int count, string what)
    {
        using var bulk = new Shredder(steps);
        using var perRow = new Shredder(steps) { PerRow = true };
        bulk.Shred(column, offset, count);
        perRow.Shred(column, offset, count);
        Assert.True(perRow.Def.SequenceEqual(bulk.Def), $"{what}: definition levels");
        Assert.True(perRow.Rep.SequenceEqual(bulk.Rep), $"{what}: repetition levels");
        Assert.True(perRow.Indices.SequenceEqual(bulk.Indices), $"{what}: value positions");
        Assert.Equal(perRow.Leaf is null, bulk.Leaf is null);
        if (perRow.Leaf is { } leaf)
        {
            Assert.Equal(leaf.Data.Offset, bulk.Leaf!.Data.Offset);
            Assert.Equal(leaf.Length, bulk.Leaf.Length);
        }
    }
}
