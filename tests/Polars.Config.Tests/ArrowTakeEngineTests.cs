using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.Config.Tests;

/// <summary>
/// <see cref="ArrowTake"/> on the engine's Arrow export against the engine's own gather, for every type the engine
/// exports (views, large lists, dictionaries, fixed-size lists, structs, decimals, ...): the same rows, value by value.
/// </summary>
public sealed class ArrowTakeEngineTests
{
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_Handle")]
    private static extern DataFrameHandle Handle(DataFrame df);

    private const int Null = ArrowTake.NullRow;

    private static IEnumerable<(string Shape, int[] Rows)> Shapes(Random random, int length) =>
    [
        ("empty", []),
        ("identity", [.. Enumerable.Range(0, length)]),
        ("reversed", [.. Enumerable.Range(0, length).Reverse()]),
        ("repeats", [.. Enumerable.Range(0, 2 * length).Select(_ => random.Next(length))]),
        ("one row", [.. Enumerable.Repeat(random.Next(length), 100)]),
        ("sparse", [.. Enumerable.Range(0, length).Where(_ => random.Next(10) == 0)]),
        ("null rows", [.. Enumerable.Range(0, length).Select(_ => random.Next(6) == 0 ? Null : random.Next(length))]),
        ("all null", [.. Enumerable.Repeat(Null, 50)]),
    ];

    [Fact]
    public void Take_MatchesTheEnginesGather_ForEveryTypeItExports()
    {
        var random = new Random(1122);
        using DataFrame every = ArrowImportFromEngineTests.EveryType(2000);
        // Binaries too (byte[] fields come as lists of bytes).
        using DataFrame all = every.WithColumns(Polars.CSharp.Polars.Col("Text").Cast(DataType.Binary).Alias("Bin"));
        using DataFrame sliced = all.Slice(7, 1500);
        foreach (DataFrame df in (DataFrame[])[all, sliced])
        {
            using RecordBatch batch = ArrowFfiBridge.ExportDataFrame(Handle(df));
            // The layouts this test is for (the engine's newest compatibility level).
            IArrowType[] types = [.. batch.Schema.FieldsList.Select(f => f.DataType)];
            Assert.Contains(types, t => t is LargeListType { ValueDataType: StringViewType });
            Assert.Contains(types, t => t is LargeListType { ValueDataType: Int32Type });
            Assert.Contains(types, t => t is StringViewType);
            Assert.Contains(types, t => t is BinaryViewType);
            Assert.Contains(types, t => t is DictionaryType);
            Assert.Contains(types, t => t is FixedSizeListType);
            Assert.Contains(types, t => t is StructType);
            Assert.Contains(types, t => t is Decimal128Type);
            Assert.Contains(types, t => t is NullType);
            foreach ((string shape, int[] rows) in Shapes(random, checked((int)df.Height)))
            {
                using DataFrame index = DataFrame.From<Index>([.. rows.Select(r => new Index(r == Null ? null : r))]);
                using DataFrame gathered = df.Gather(index.Lazy());
                using RecordBatch expected = ArrowFfiBridge.ExportDataFrame(Handle(gathered));
                foreach ((int parallel, bool? share) in (ReadOnlySpan<(int, bool?)>)[(1, null), (4, null), (1, true), (1, false)])
                {
                    using RecordBatch taken = ArrowTake.Take(batch, rows, share, maxDegreeOfParallelism: parallel);
                    Assert.Equal(expected.Length, taken.Length);
                    for (int c = 0; c < expected.ColumnCount; c++)
                    {
                        string where = $"{df.Height} rows, {shape}, parallel {parallel}, share {share}, column {expected.Schema.FieldsList[c].Name}";
                        Assert.Equal(expected.Schema.FieldsList[c].DataType.TypeId, taken.Schema.FieldsList[c].DataType.TypeId);
                        for (int i = 0; i < expected.Length; i++) AssertSameRow(expected.Column(c), i, taken.Column(c), i, $"{where}, row {i}");
                    }
                }
            }
        }
    }

    /// <summary>Row <paramref name="e"/> of <paramref name="expected"/> holds the value row <paramref name="a"/> of <paramref name="actual"/> does (nested values included).</summary>
    private static void AssertSameRow(IArrowArray expected, int e, IArrowArray actual, int a, string where)
    {
        Assert.True(expected.IsNull(e) == actual.IsNull(a), $"{where}: null {expected.IsNull(e)} vs {actual.IsNull(a)}");
        if (expected.IsNull(e)) return;
        switch (expected)
        {
            case DictionaryArray d:
            {
                var other = (DictionaryArray)actual;
                AssertSameRow(d.Dictionary, Index(d.Indices, e), other.Dictionary, Index(other.Indices, a), $"{where} (entry)");
                break;
            }
            case BooleanArray b:
                Assert.True(b.GetValue(e) == ((BooleanArray)actual).GetValue(a), where);
                break;
            case BinaryViewArray v:
                Assert.True(v.GetBytes(e).SequenceEqual(((BinaryViewArray)actual).GetBytes(a)), where);
                break;
            case BinaryArray b:
                Assert.True(b.GetBytes(e).SequenceEqual(((BinaryArray)actual).GetBytes(a)), where);
                break;
            case LargeBinaryArray b:
                Assert.True(b.GetBytes(e).SequenceEqual(((LargeBinaryArray)actual).GetBytes(a)), where);
                break;
            case StructArray s:
            {
                var other = (StructArray)actual;
                for (int f = 0; f < s.Fields.Count; f++) AssertSameRow(s.Fields[f], e, other.Fields[f], a, $"{where}.{f}");
                break;
            }
            case ListArray l:
            {
                var other = (ListArray)actual;
                Assert.True(l.GetValueLength(e) == other.GetValueLength(a), $"{where}: list length");
                for (int k = 0; k < l.GetValueLength(e); k++)
                    AssertSameRow(l.Values, l.ValueOffsets[e] + k, other.Values, other.ValueOffsets[a] + k, $"{where}[{k}]");
                break;
            }
            case LargeListArray l:
            {
                var other = (LargeListArray)actual;
                Assert.True(l.GetValueLength(e) == other.GetValueLength(a), $"{where}: list length");
                for (int k = 0; k < l.GetValueLength(e); k++)
                    AssertSameRow(l.Values, (int)l.ValueOffsets[e] + k, other.Values, (int)other.ValueOffsets[a] + k, $"{where}[{k}]");
                break;
            }
            case FixedSizeListArray f:
            {
                var other = (FixedSizeListArray)actual;
                int size = ((FixedSizeListType)f.Data.DataType).ListSize;
                for (int k = 0; k < size; k++)
                    AssertSameRow(f.Values, (f.Offset + e) * size + k, other.Values, (other.Offset + a) * size + k, $"{where}[{k}]");
                break;
            }
            default:
            {
                int width = expected.Data.DataType switch
                {
                    FixedSizeBinaryType b => b.ByteWidth,
                    FixedWidthType w => w.BitWidth / 8,
                    _ => throw new NotSupportedException($"{where}: {expected.Data.DataType.Name}"),
                };
                Assert.True(Value(expected, e, width).SequenceEqual(Value(actual, a, width)), where);
                break;
            }
        }

        static ReadOnlySpan<byte> Value(IArrowArray array, int i, int width) =>
            array.Data.Buffers[1].Span.Slice((array.Data.Offset + i) * width, width);

        static int Index(IArrowArray indices, int i) => indices switch
        {
            Int8Array x => x.GetValue(i)!.Value,
            UInt8Array x => x.GetValue(i)!.Value,
            Int16Array x => x.GetValue(i)!.Value,
            UInt16Array x => x.GetValue(i)!.Value,
            Int32Array x => x.GetValue(i)!.Value,
            UInt32Array x => checked((int)x.GetValue(i)!.Value),
            Int64Array x => checked((int)x.GetValue(i)!.Value),
            _ => throw new NotSupportedException(indices.GetType().Name),
        };
    }

    private sealed record Index(int? I);
}
