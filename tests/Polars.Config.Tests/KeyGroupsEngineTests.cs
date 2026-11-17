using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Sazanami.Grouping;
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using static Polars.CSharp.Polars;

namespace Polars.Config.Tests;

/// <summary>
/// <see cref="KeyGroups"/> on the engine's Arrow export against the engine's own grouping (group_by keeping order):
/// the same groups, in the same first-appearance order, for every key type the engine exports (views, dictionaries,
/// decimals, times, floats with NaNs and signed zeros, nulls), one key column or several, the rows in one batch or in
/// several (global ids).
/// </summary>
public sealed class KeyGroupsEngineTests
{
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_Handle")]
    private static extern DataFrameHandle Handle(DataFrame df);

    private static readonly double[] Floats =
        [0.0, -0.0, double.NaN, BitConverter.Int64BitsToDouble(0x7FF8000000000001L), BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8000000000000UL)), 1.5, -1.5];

    private static DataFrame Keys(int rows)
    {
        var random = new Random(1125);
        string?[] words = [null, "", "a", "b", "a much longer word than twelve bytes", "äpfel"];
        using DataFrame typed = DataFrame.From<Row>([.. Enumerable.Range(0, rows).Select(_ => new Row(
            random.Next(8) == 0 ? null : random.Next(5),
            words[random.Next(words.Length)],
            random.Next(9) == 0 ? null : Floats[random.Next(Floats.Length)],
            random.Next(7) == 0 ? null : random.Next(2) == 0,
            random.Next(8) == 0 ? null : random.Next(3) * 1.25m,
            random.Next(8) == 0 ? null : new DateTime(2024, 1, 1 + random.Next(3), 0, 0, 0, DateTimeKind.Utc)))]);
        return typed.WithColumns(
            Col("S").Cast(DataType.Binary).Alias("Bin"),
            Col("S").Cast(DataType.Categorical()).Alias("Cat"),
            Col("F").Cast(DataType.Float32).Alias("F32"));
    }

    public static TheoryData<string[]> KeySets => new()
    {
        { ["I"] }, { ["S"] }, { ["F"] }, { ["F32"] }, { ["B"] }, { ["D"] }, { ["T"] }, { ["Bin"] }, { ["Cat"] },
        { ["I", "S"] }, { ["F", "B", "Cat"] }, { ["D", "T", "Bin"] }, { ["I", "S", "F", "B", "D", "T", "Bin", "Cat", "F32"] },
    };

    [Theory]
    [MemberData(nameof(KeySets))]
    public void Groups_AreTheEnginesGroups(string[] keys)
    {
        const int Rows = 5000;
        using DataFrame df = Keys(Rows);
        using RecordBatch batch = ArrowFfiBridge.ExportDataFrame(Handle(df));
        // The layouts this test is for (the engine's newest compatibility level).
        Assert.IsType<Apache.Arrow.Types.DictionaryType>(batch.Schema.GetFieldByName("Cat").DataType);
        Assert.IsType<Apache.Arrow.Types.StringViewType>(batch.Schema.GetFieldByName("S").DataType);
        Assert.IsType<Apache.Arrow.Types.BinaryViewType>(batch.Schema.GetFieldByName("Bin").DataType);
        Assert.IsType<Apache.Arrow.Types.Decimal128Type>(batch.Schema.GetFieldByName("D").DataType);
        // The engine keeps -0.0 and NaNs of other payloads and signs as they came: the grouping sees them.
        var f = (DoubleArray)batch.Column(batch.Schema.GetFieldIndex("F"));
        long[] bits = [.. Enumerable.Range(0, f.Length).Where(i => f.IsValid(i)).Select(i => BitConverter.DoubleToInt64Bits(f.GetValue(i)!.Value))];
        Assert.Contains(BitConverter.DoubleToInt64Bits(-0.0), bits);
        Assert.True(bits.Where(b => double.IsNaN(BitConverter.Int64BitsToDouble(b))).Distinct().Count() >= 2, "NaNs of one bit pattern only");

        // The engine's groups, in first-appearance order: each row's group.
        using DataFrame indexed = df.WithRowIndex("__row");
        using DataFrame grouped = indexed.GroupBy([.. keys.Select(k => (IntoExprColumn)k)], maintainOrder: true).Agg(Col("__row").Alias("__rows"));
        using RecordBatch engine = ArrowFfiBridge.ExportDataFrame(Handle(grouped));
        var lists = (LargeListArray)engine.Column(engine.Schema.GetFieldIndex("__rows"));
        var rowsOf = (UInt32Array)lists.Values;
        var expected = new int[Rows];
        for (int g = 0; g < lists.Length; g++)
        {
            for (long k = lists.ValueOffsets[g]; k < lists.ValueOffsets[g + 1]; k++) expected[rowsOf.GetValue((int)k)!.Value] = g;
        }

        // Ours: the whole batch at once, then in chunks (global ids across them).
        IArrowArray[] columns = [.. keys.Select(k => batch.Column(batch.Schema.GetFieldIndex(k)))];
        foreach (int chunk in (int[])[Rows, 777])
        {
            using var groups = new KeyGroups(keys.Length);
            var ids = new int[chunk];
            var batchGroups = new int[chunk];
            var firstRows = new int[chunk];
            for (int start = 0; start < Rows; start += chunk)
            {
                int n = Math.Min(chunk, Rows - start);
                IArrowArray[] slice = [.. columns.Select(c => ArrowArrayFactory.Slice(c, start, n))];
                groups.Assign(slice, ids, batchGroups, firstRows);
                for (int row = 0; row < n; row++)
                {
                    int got = batchGroups[ids[row]];
                    if (got != expected[start + row])
                        Assert.Fail($"keys {string.Join(",", keys)}, chunk {chunk}: row {start + row} in group {got}, the engine's {expected[start + row]}");
                }
            }
            Assert.Equal(lists.Length, groups.Count);
        }
    }

    private sealed record Row(int? I, string? S, double? F, bool? B, decimal? D, DateTime? T);
}
