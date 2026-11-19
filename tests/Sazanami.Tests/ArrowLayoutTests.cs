using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;

namespace Sazanami.Tests;

/// <summary>
/// Conversions between layouts (strings and binaries plain, large, view; lists plain, large, view, large view),
/// between dictionaries of other index types and to and from plain values, and <see cref="DictionaryUnifier"/>,
/// against models of the values: random arrays with nulls, slices and overlapping views.
/// </summary>
public class ArrowLayoutTests
{
    private static readonly IArrowType[] StringLayouts = [StringType.Default, LargeStringType.Default, StringViewType.Default];
    private static readonly IArrowType[] BinaryLayouts = [BinaryType.Default, LargeBinaryType.Default, BinaryViewType.Default];
    private static readonly string[] Words = ["", "a", "bc", "twelve bytes", "thirteen byte", "a string longer than twelve bytes", "äöü", "x"];

    private static ArrowConversion Compile(IArrowType from, IArrowType to)
    {
        Assert.True(ArrowWidening.TryCompile(from, to, default, "x", out ArrowConversion? conversion, out ArrowConversionError? error), error?.Message);
        return conversion;
    }

    private static unsafe IArrowArray Bytes(IArrowType type, IReadOnlyList<byte[]?> values)
    {
        using NativeColumnBuilder builder = NativeColumnBuilder.Create(type);
        foreach (byte[]? value in values)
        {
            if (value is null) builder.AppendNull();
            else ((IBytesBuilder)builder).Append(value);
        }
        var export = new NativeBatchExport();
        return export.ImportArray(builder.Export(export), type);
    }

    private static byte[]?[] RandomBytes(Random random, int length) =>
        [.. Enumerable.Range(0, length).Select(_ => random.Next(6) == 0 ? null : Encoding.UTF8.GetBytes(Words[random.Next(Words.Length)] + (random.Next(3) == 0 ? random.Next(1000).ToString() : "")))];

    private static IArrowArray Sliced(Random random, IArrowArray array, ref int offset)
    {
        if (array.Length < 4 || random.Next(2) == 0) return array;
        offset = random.Next(1, array.Length / 2);
        return ArrowArrayFactory.Slice(array, offset, array.Length - offset);
    }

    [Fact]
    public void StringsAndBinaries_ConvertBetweenEveryLayout()
    {
        var random = new Random(1119);
        for (int round = 0; round < 200; round++)
        {
            IArrowType[] layouts = round % 2 == 0 ? StringLayouts : BinaryLayouts;
            byte[]?[] values = RandomBytes(random, random.Next(0, 300));
            foreach (IArrowType from in layouts)
            {
                using IArrowArray whole = Bytes(from, values);
                int offset = 0;
                IArrowArray source = Sliced(random, whole, ref offset);
                foreach (IArrowType to in layouts)
                {
                    ArrowConversion conversion = Compile(from, to);
                    if (conversion.IsIdentity) continue; // the same layout: as it is
                    using IArrowArray result = conversion.Convert(source);
                    Assert.Equal(to.TypeId, result.Data.DataType.TypeId);
                    Assert.Equal(values.Length - offset, result.Length);
                    for (int i = 0; i < result.Length; i++)
                    {
                        byte[]? expected = values[offset + i];
                        Assert.Equal(expected is null, result.IsNull(i));
                        if (expected is not null) Assert.Equal(expected, RelayoutConversion.Bytes(result, i).ToArray());
                    }
                }
            }
        }
    }

    // ---- lists ----------------------------------------------------------------------------------------------------

    private static readonly Func<IArrowType, IArrowType>[] ListLayouts =
    [
        e => new ListType(new Field("item", e, true)),
        e => new LargeListType(new Field("item", e, true)),
        e => new ListViewType(new Field("item", e, true)),
        e => new LargeListViewType(new Field("item", e, true)),
    ];

    /// <summary>A list array of the layout over Int32 or Int64 elements; views point anywhere into a shuffled child.</summary>
    private static IArrowArray Lists(Random random, IArrowType type, IArrowType element, List<long?>?[] rows)
    {
        bool view = type is ListViewType or LargeListViewType, large = type is LargeListType or LargeListViewType;
        // The child: each row's elements, rows placed in random order with junk between (views only).
        List<long?> child = [];
        var starts = new long[rows.Length];
        int[] order = [.. Enumerable.Range(0, rows.Length)];
        if (view) random.Shuffle(order);
        foreach (int r in order)
        {
            if (view && random.Next(3) == 0) child.Add(random.Next(100));
            starts[r] = child.Count;
            if (rows[r] is { } items) child.AddRange(items);
        }
        if (view && rows.Length > 1 && rows[0] is { Count: > 0 } && rows[1] is { Count: 0 }) starts[1] = starts[0]; // an empty view inside another
        IArrowArray values;
        if (element is Int32Type)
        {
            var builder = new Int32Array.Builder();
            foreach (long? item in child) builder.Append((int?)item);
            values = builder.Build(PooledArrowAllocator.Instance);
        }
        else
        {
            var builder = new Int64Array.Builder();
            foreach (long? item in child) builder.Append(item);
            values = builder.Build(PooledArrowAllocator.Instance);
        }
        byte[] validity = new byte[(rows.Length + 7) / 8 + 1];
        for (int i = 0; i < rows.Length; i++) if (rows[i] is not null) validity[i / 8] |= (byte)(1 << (i % 8));
        int nulls = rows.Count(r => r is null);
        ArrowBuffer Numbers(IEnumerable<long> numbers)
        {
            byte[] bytes = large ? [.. numbers.SelectMany(BitConverter.GetBytes)] : [.. numbers.SelectMany(n => BitConverter.GetBytes((int)n))];
            return new ArrowBuffer(bytes);
        }
        ArrayData data;
        if (view)
        {
            data = new ArrayData(type, rows.Length, nulls, 0, [new ArrowBuffer(validity), Numbers(starts), Numbers(rows.Select(r => (long)(r?.Count ?? 0)))], [values.Data]);
        }
        else
        {
            var offsets = new long[rows.Length + 1];
            for (int i = 0; i < rows.Length; i++) offsets[i + 1] = offsets[i] + (rows[i]?.Count ?? 0);
            data = new ArrayData(type, rows.Length, nulls, 0, [new ArrowBuffer(validity), Numbers(offsets)], [values.Data]);
        }
        return ArrowArrayFactory.BuildArray(data);

    }

    /// <summary>Row <paramref name="i"/> of a list of any layout (null for null).</summary>
    private static List<long?>? Row(IArrowArray array, int i)
    {
        if (array.IsNull(i)) return null;
        ArrayData d = array.Data;
        bool large = d.DataType is LargeListType or LargeListViewType, view = d.DataType is ListViewType or LargeListViewType;
        long Read(int buffer, int at) => large ? d.Buffers[buffer].Span.CastTo<long>()[d.Offset + at] : d.Buffers[buffer].Span.CastTo<int>()[d.Offset + at];
        long start = Read(1, i), size = view ? Read(2, i) : Read(1, i + 1) - start;
        IArrowArray child = ArrowArrayFactory.BuildArray(d.Children[0]);
        return [.. Enumerable.Range((int)start, (int)size).Select(k => child switch
        {
            Int32Array a => (long?)a.GetValue(k),
            Int64Array a => a.GetValue(k),
            _ => throw new NotSupportedException(child.GetType().Name),
        })];
    }

    [Fact]
    public void Lists_ConvertBetweenEveryLayout_ElementsWidened()
    {
        var random = new Random(1120);
        for (int round = 0; round < 150; round++)
        {
            List<long?>?[] rows = [.. Enumerable.Range(0, random.Next(0, 80)).Select(_ => random.Next(6) == 0
                ? null
                : (List<long?>?)[.. Enumerable.Range(0, random.Next(0, 5)).Select(_ => random.Next(5) == 0 ? (long?)null : random.Next(-1000, 1000))])];
            foreach (Func<IArrowType, IArrowType> from in ListLayouts)
            {
                IArrowType source = from(Int32Type.Default);
                using IArrowArray whole = Lists(random, source, Int32Type.Default, rows);
                int offset = 0;
                IArrowArray sliced = Sliced(random, whole, ref offset);
                foreach (Func<IArrowType, IArrowType> to in ListLayouts)
                {
                    foreach (IArrowType element in (IArrowType[])[Int32Type.Default, Int64Type.Default])
                    {
                        IArrowType target = to(element);
                        ArrowConversion conversion = Compile(source, target);
                        if (conversion.IsIdentity) continue;
                        using IArrowArray result = conversion.Convert(sliced);
                        Assert.Equal(target.TypeId, result.Data.DataType.TypeId);
                        for (int i = 0; i < result.Length; i++) Assert.Equal(rows[offset + i], Row(result, i));
                    }
                }
            }
        }
    }

    [Fact]
    public void NullListViews_AreMade()
    {
        foreach (Func<IArrowType, IArrowType> layout in ListLayouts)
        {
            using IArrowArray nulls = NullArrays.Of(layout(Int32Type.Default), 5);
            Assert.Equal(5, nulls.NullCount);
            for (int i = 0; i < 5; i++) Assert.Null(Row(nulls, i));
        }
    }

    // ---- dictionaries ---------------------------------------------------------------------------------------------

    private static readonly IntegerType[] IndexTypes =
        [Int8Type.Default, UInt8Type.Default, Int16Type.Default, UInt16Type.Default, Int32Type.Default, UInt32Type.Default, Int64Type.Default, UInt64Type.Default];

    /// <summary>A dictionary array: its entries (maybe with duplicates and a null entry) and each row's index (or null).</summary>
    private static IArrowArray Dictionary(IntegerType index, IArrowArray entries, int?[] rows)
    {
        int width = index.BitWidth / 8;
        var indices = new byte[Math.Max(1, rows.Length) * width];
        byte[] validity = new byte[(rows.Length + 7) / 8 + 1];
        for (int i = 0; i < rows.Length; i++)
        {
            if (rows[i] is not int v) continue;
            validity[i / 8] |= (byte)(1 << (i % 8));
            BitConverter.GetBytes((long)v).AsSpan(0, width).CopyTo(indices.AsSpan(i * width));
        }
        var type = new DictionaryType(index, entries.Data.DataType, false);
        return ArrowArrayFactory.BuildArray(new ArrayData(type, rows.Length, rows.Count(r => r is null), 0, [new ArrowBuffer(validity), new ArrowBuffer(indices)], [], entries.Data));
    }

    private static string? Value(IArrowArray array, int i)
    {
        if (array.IsNull(i)) return null;
        return array switch
        {
            DictionaryArray d => Value(d.Dictionary, (int)DictionaryIndices.Read(d.Data, (IntegerType)((DictionaryType)d.Data.DataType).IndexType, i)),
            Int64Array a => a.GetValue(i)!.Value.ToString(),
            _ => Encoding.UTF8.GetString(RelayoutConversion.Bytes(array, i)),
        };
    }

    [Fact]
    public void Dictionaries_ConvertToWiderIndices_AndToAndFromValues()
    {
        var random = new Random(1121);
        for (int round = 0; round < 120; round++)
        {
            // Entries: strings (some duplicated, one null) or Int64 numbers.
            bool numbers = round % 3 == 0;
            int count = random.Next(1, 20);
            IArrowArray entries = numbers
                ? new Int64Array.Builder().AppendRange(Enumerable.Range(0, count).Select(_ => (long)random.Next(5))).Build(PooledArrowAllocator.Instance)
                : Bytes(StringLayouts[random.Next(3)], [.. Enumerable.Range(0, count).Select(k => k == 1 ? null : Encoding.UTF8.GetBytes(Words[random.Next(Words.Length)]))]);
            int?[] rows = [.. Enumerable.Range(0, random.Next(0, 100)).Select(_ => random.Next(7) == 0 ? (int?)null : random.Next(Math.Min(count, 127)))];
            IntegerType from = IndexTypes[random.Next(IndexTypes.Length)];
            using IArrowArray source = Dictionary(from, entries, rows);
            string?[] expected = [.. Enumerable.Range(0, rows.Length).Select(i => Value(source, i))];
            IArrowType values = entries.Data.DataType;
            IArrowType[] targets =
            [
                new DictionaryType(Int32Type.Default, values, false), // any index into 32 bits
                new DictionaryType(Int64Type.Default, values, false),
                values,                                               // decoded
                .. numbers ? (IArrowType[])[] : [new DictionaryType(Int32Type.Default, StringViewType.Default, false), LargeStringType.Default],
            ];
            foreach (IArrowType target in targets)
            {
                ArrowConversion conversion = Compile(source.Data.DataType, target);
                if (conversion.IsIdentity) continue;
                using IArrowArray result = conversion.Convert(source);
                Assert.Equal(expected, Enumerable.Range(0, result.Length).Select(i => Value(result, i)));
            }
            // Plain values into a dictionary (indexed in order).
            using IArrowArray decoded = Compile(source.Data.DataType, values).Convert(source);
            using IArrowArray encoded = Compile(values, new DictionaryType(Int16Type.Default, values, false)).Convert(decoded);
            Assert.Equal(expected, Enumerable.Range(0, encoded.Length).Select(i => Value(encoded, i)));
        }
        // An index type too narrow for the values to encode is an error, as narrower indices are refused.
        using IArrowArray many = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 300).Select(i => (long)i)).Build(PooledArrowAllocator.Instance);
        Assert.Throws<OverflowException>(() => Compile(Int64Type.Default, new DictionaryType(Int8Type.Default, Int64Type.Default, false)).Convert(many));
        Assert.False(ArrowWidening.CanConvert(new DictionaryType(Int32Type.Default, StringType.Default, false), new DictionaryType(Int16Type.Default, StringType.Default, false), default));
    }

    // ---- unification ----------------------------------------------------------------------------------------------

    [Fact]
    public void UnifiedDictionaries_HoldEveryValueOnce_AndOutliveTheUnifier()
    {
        var random = new Random(1122);
        var schema = new Schema(
        [
            new Field("s", new DictionaryType(Int32Type.Default, StringType.Default, false), true),
            new Field("v", new DictionaryType(Int32Type.Default, StringViewType.Default, false), true),
            new Field("n", new DictionaryType(Int32Type.Default, Int64Type.Default, false), true),
            new Field("t", new StructType([new Field("l", new DictionaryType(Int32Type.Default, LargeStringType.Default, false), true)]), true),
        ], null);
        List<RecordBatch> batches = [];
        List<string?[][]> models = [];
        using (DictionaryUnifier unifier = DictionaryUnifier.For(schema)!)
        {
            for (int b = 0; b < 30; b++)
            {
                int length = random.Next(0, 70);
                IArrowArray Column(IArrowType values, bool number)
                {
                    int count = random.Next(1, 12);
                    IArrowArray entries = number
                        ? new Int64Array.Builder().AppendRange(Enumerable.Range(0, count).Select(_ => (long)random.Next(30))).Build(PooledArrowAllocator.Instance)
                        : Bytes(values, [.. Enumerable.Range(0, count).Select(k => k == 0 && random.Next(3) == 0 ? null : Encoding.UTF8.GetBytes(Words[random.Next(Words.Length)] + random.Next(8)))]);
                    int?[] rows = [.. Enumerable.Range(0, length).Select(_ => random.Next(6) == 0 ? (int?)null : random.Next(count))];
                    return Dictionary(IndexTypes[random.Next(IndexTypes.Length)], entries, rows);
                }
                IArrowArray s = Column(StringType.Default, false), v = Column(StringViewType.Default, false), n = Column(Int64Type.Default, true);
                IArrowArray l = Column(LargeStringType.Default, false);
                IArrowArray t = new StructArray(new StructType([new Field("l", l.Data.DataType, true)]), length, [l], ArrowBuffer.Empty, 0);
                IArrowArray[] columns = [s, v, n, t];
                models.Add([.. columns.Select(c => Enumerable.Range(0, length).Select(i => c is StructArray st ? Value(st.Fields[0], i) : Value(c, i)).ToArray())]);
                var batch = new RecordBatch(new Schema([.. schema.FieldsList.Select((f, k) => new Field(f.Name, columns[k].Data.DataType, true))], null), columns, length);
                batches.Add(unifier.Apply(batch));
            }
        }
        // The unifier is gone: every batch still reads its values, each value has one index, dictionaries grow as prefixes.
        for (int c = 0; c < 4; c++)
        {
            Dictionary<string, int> indexOf = [];
            List<string?>? last = null;
            for (int b = 0; b < batches.Count; b++)
            {
                IArrowArray column = batches[b].Column(c) is StructArray st ? st.Fields[0] : batches[b].Column(c);
                var dictionary = (DictionaryArray)column;
                Assert.IsType<Int32Type>(((DictionaryType)dictionary.Data.DataType).IndexType);
                List<string?> entries = [.. Enumerable.Range(0, dictionary.Dictionary.Length).Select(i => Value(dictionary.Dictionary, i))];
                Assert.DoesNotContain(null, entries);
                Assert.Equal(entries.Count, entries.Distinct().Count());
                if (last is not null) Assert.Equal(last, entries.Take(last.Count));
                last = entries;
                for (int i = 0; i < column.Length; i++)
                {
                    Assert.Equal(models[b][c][i], Value(column, i));
                    if (column.IsNull(i)) continue;
                    int index = ((Int32Array)dictionary.Indices).GetValue(i)!.Value;
                    Assert.Equal(index, indexOf.TryGetValue(entries[index]!, out int known) ? known : indexOf[entries[index]!] = index);
                }
            }
        }
        foreach (RecordBatch batch in batches) batch.Dispose();
    }

    /// <summary>An array over the same native memory as <paramref name="array"/>, with buffers of its own.</summary>
    private static unsafe IArrowArray Shared(IArrowArray array)
    {
        var export = new NativeBatchExport();
        return export.ImportArray(ArrowConversion.Share(export, array.Data), array.Data.DataType);
    }

    /// <summary>
    /// A dictionary seen again in the same memory but longer (as a file's dictionary grows by deltas) is remapped
    /// again, not by the remap of its shorter self; dictionaries large enough to outgrow the global one's buffers.
    /// </summary>
    [Fact]
    public void Dictionaries_SeenAgainLonger_AreRemappedAgain()
    {
        var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var schema = new Schema([new Field("d", type, true)], null);
        string[] words = [.. Enumerable.Range(0, 3000).Select(i => $"value {i} of a dictionary that outgrows its first buffer")];
        using IArrowArray entries = Bytes(StringType.Default, [.. words.Select(Encoding.UTF8.GetBytes)]);
        List<RecordBatch> batches = [];
        using (DictionaryUnifier unifier = DictionaryUnifier.For(schema)!)
        {
            foreach (int seen in (int[])[2, 1000, 3000])
            {
                // The same native memory, more of it each time (each batch with buffers of its own, as a reader's).
                IArrowArray prefix = ArrowArrayFactory.Slice(Shared(entries), 0, seen);
                int?[] rows = [.. Enumerable.Range(0, 50).Select(i => (int?)(seen - 1 - i % seen))];
                var batch = new RecordBatch(schema, [Dictionary(Int32Type.Default, prefix, rows)], rows.Length);
                batches.Add(unifier.Apply(batch));
            }
        }
        foreach ((RecordBatch batch, int seen) in batches.Zip((int[])[2, 1000, 3000]))
        {
            var d = (DictionaryArray)batch.Column(0);
            for (int i = 0; i < batch.Length; i++) Assert.Equal(words[seen - 1 - i % seen], Value(d, i));
            batch.Dispose();
        }
    }
}
