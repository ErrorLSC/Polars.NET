using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Random nested columns (structs, lists in every layout, maps, run-end encoded and dictionary-encoded nested values)
/// with nulls at every level, built together with the canonical text of every row: the expected values come from the
/// generator's own model, never from reading the arrays back (so offsets and nulls of parents are checked, not
/// assumed). <see cref="Canonical"/> reads nested arrays as a reader returns them in the same text.
/// </summary>
internal static class ParquetNestedData
{
    /// <summary>A column: its field, its array and the canonical text of each row.</summary>
    public sealed record Column(Field Field, IArrowArray Array, string[] Rows);

    private static readonly IArrowType[] Leaves =
    [
        Int32Type.Default, Int64Type.Default, StringType.Default, DoubleType.Default, BooleanType.Default, UInt32Type.Default,
        new Decimal128Type(30, 5), Date32Type.Default, new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), BinaryType.Default,
        new DictionaryType(Int32Type.Default, StringType.Default, false), StringViewType.Default,
    ];

    /// <summary>A random type of at most <paramref name="depth"/> nested levels.</summary>
    public static IArrowType RandomType(Random r, int depth)
    {
        if (depth == 0 || r.Next(4) == 0) return Leaves[r.Next(Leaves.Length)];
        Field Item(string name) => new(name, RandomType(r, depth - 1), r.Next(4) != 0);
        return r.Next(10) switch
        {
            0 => new StructType([.. Enumerable.Range(0, r.Next(1, 4)).Select(k => Item($"f{k}"))]),
            1 => new ListType(Item("item")),
            2 => new LargeListType(Item("item")),
            3 => new FixedSizeListType(Item("item"), r.Next(1, 4)),
            4 => new ListViewType(Item("item")),
            5 => new LargeListViewType(Item("item")),
            6 => new MapType(new Field("key", r.Next(2) == 0 ? StringType.Default : Int64Type.Default, false), Item("value"), false),
            7 => new RunEndEncodedType(Int32Type.Default, RandomType(r, depth - 1)),
            8 => new DictionaryType(Int32Type.Default, new ListType(Item("item")), false),
            _ => new StructType([Item("a"), Item("b")]),
        };
    }

    /// <summary><paramref name="columns"/> random nested columns of <paramref name="rows"/> rows.</summary>
    public static (RecordBatch Batch, string[][] Expected) Batch(int seed, int rows, int columns = 6, int depth = 3)
    {
        var r = new Random(seed);
        List<Column> built = [];
        for (int c = 0; c < columns; c++) built.Add(Build(r, $"c{c}", RandomType(r, depth), r.Next(5) != 0, rows));
        var schema = new Apache.Arrow.Schema([.. built.Select(b => b.Field)], null);
        return (new RecordBatch(schema, built.Select(b => b.Array), rows), [.. built.Select(b => b.Rows)]);
    }

    /// <summary>
    /// A column of <paramref name="type"/>: nulls only where <paramref name="nullable"/>. A quarter of the arrays, at
    /// every depth, are slices of longer ones (an offset of their own, under parents that have theirs).
    /// </summary>
    public static Column Build(Random r, string name, IArrowType type, bool nullable, int n)
    {
        if (n > 0 && r.Next(4) == 0)
        {
            int pad = r.Next(1, 4);
            Column whole = BuildWhole(r, name, type, nullable, n + pad);
            return whole with { Array = ((Apache.Arrow.Array)whole.Array).Slice(pad, n), Rows = whole.Rows[pad..] };
        }
        return BuildWhole(r, name, type, nullable, n);
    }

    private static Column BuildWhole(Random r, string name, IArrowType type, bool nullable, int n)
    {
        bool[] valid = [.. Enumerable.Range(0, n).Select(_ => !nullable || r.Next(5) != 0)];
        int nulls = valid.Count(v => !v);
        ArrowBuffer validity = nulls == 0 ? ArrowBuffer.Empty : Validity(valid);
        string[] rows = new string[n];
        IArrowArray array;
        switch (type)
        {
            case StructType st:
            {
                Column[] children = [.. st.Fields.Select(f => Build(r, f.Name, f.DataType, f.IsNullable, n))];
                array = new StructArray(st, n, children.Select(c => c.Array), validity, nulls);
                for (int i = 0; i < n; i++)
                    rows[i] = valid[i] ? "{" + string.Join(",", children.Select(c => $"{c.Field.Name}={c.Rows[i]}")) + "}" : "null";
                break;
            }
            case ListType or LargeListType or MapType:
            {
                // Null rows sometimes cover items too (Arrow allows it; they must not be written).
                int[] lengths = [.. valid.Select(v => v ? r.Next(4) : r.Next(3) == 0 ? r.Next(1, 3) : 0)];
                int total = lengths.Sum();
                int[] offsets = [0, .. lengths.Select((_, i) => lengths[..(i + 1)].Sum())];
                Column items;
                string[] itemRows;
                if (type is MapType map)
                {
                    // Keys unique within each map (readers may keep only the last value of a repeated key, as the spec allows).
                    int[] slot = [.. lengths.SelectMany(l => Enumerable.Range(0, l))];
                    long salt = r.Next(1000);
                    IArrowArray keyArray = map.KeyField.DataType is StringType
                        ? Strings([.. slot.Select(k => $"k{salt + k}")])
                        : Fixed(Int64Type.Default, [.. slot.Select(k => (long?)(salt + k))]);
                    Column keys = new(new Field("key", keyArray.Data.DataType, false), keyArray, ParquetWriteData.Canonical(keyArray));
                    Column values = Build(r, "value", map.ValueField.DataType, map.ValueField.IsNullable, total);
                    var entries = new StructArray(map.KeyValueType, total, [keys.Array, values.Array], ArrowBuffer.Empty, 0);
                    array = new MapArray(map, n, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), entries, validity, nulls);
                    itemRows = [.. Enumerable.Range(0, total).Select(k => $"{keys.Rows[k]}=>{values.Rows[k]}")];
                }
                else
                {
                    Field item = ((NestedType)type).Fields[0];
                    items = Build(r, item.Name, item.DataType, item.IsNullable, total);
                    itemRows = items.Rows;
                    array = type is ListType
                        ? new ListArray(type, n, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), items.Array, validity, nulls)
                        : new LargeListArray(type, n, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.Select(o => (long)o).ToArray().AsSpan()).ToArray()), items.Array, validity, nulls);
                }
                for (int i = 0; i < n; i++)
                    rows[i] = valid[i] ? List(type is MapType, itemRows[offsets[i]..offsets[i + 1]]) : "null";
                break;
            }
            case FixedSizeListType fixedSize:
            {
                Column items = Build(r, fixedSize.ValueField.Name, fixedSize.ValueDataType, fixedSize.ValueField.IsNullable, n * fixedSize.ListSize);
                array = new FixedSizeListArray(fixedSize, n, items.Array, validity, nulls);
                for (int i = 0; i < n; i++)
                    rows[i] = valid[i] ? List(false, items.Rows[(i * fixedSize.ListSize)..((i + 1) * fixedSize.ListSize)]) : "null";
                break;
            }
            case ListViewType or LargeListViewType:
            {
                // Each row's items lie together, but the rows' ranges in another order.
                int[] sizes = [.. valid.Select(v => v ? r.Next(4) : 0)];
                int[] order = [.. Enumerable.Range(0, n).OrderBy(_ => r.Next())];
                int[] offsets = new int[n];
                int at = 0;
                foreach (int i in order)
                {
                    offsets[i] = at;
                    at += sizes[i];
                }
                Field item = ((NestedType)type).Fields[0];
                Column items = Build(r, item.Name, item.DataType, item.IsNullable, at);
                ArrowBuffer B(int[] values) => type is ListViewType
                    ? new ArrowBuffer(MemoryMarshal.AsBytes(values.AsSpan()).ToArray())
                    : new ArrowBuffer(MemoryMarshal.AsBytes(values.Select(v => (long)v).ToArray().AsSpan()).ToArray());
                array = type is ListViewType
                    ? new ListViewArray(type, n, B(offsets), B(sizes), items.Array, validity, nulls, 0)
                    : new LargeListViewArray(type, n, B(offsets), B(sizes), items.Array, validity, nulls, 0);
                for (int i = 0; i < n; i++)
                    rows[i] = valid[i] ? List(false, items.Rows[offsets[i]..(offsets[i] + sizes[i])]) : "null";
                break;
            }
            case RunEndEncodedType runEnded:
            {
                // Runs of 1–4 rows; the values may be null (a run-end encoded array has no validity of its own).
                List<int> ends = [];
                for (int end = 0; end < n;) ends.Add(end = Math.Min(n, end + r.Next(1, 5)));
                Column values = Build(r, "values", runEnded.ValuesDataType, nullable, ends.Count);
                array = new RunEndEncodedArray(Fixed(Int32Type.Default, [.. ends.Select(e => (int?)e)]), values.Array);
                for (int i = 0, run = 0; i < n; i++)
                {
                    while (ends[run] <= i) run++;
                    rows[i] = values.Rows[run];
                }
                break;
            }
            case DictionaryType { ValueType: NestedType } dictionaryType:
            {
                int size = r.Next(1, 6);
                Column values = Build(r, "values", dictionaryType.ValueType, nullable, size);
                int?[] indices = [.. valid.Select(v => v ? r.Next(size) : (int?)null)];
                array = new DictionaryArray(dictionaryType, Fixed(Int32Type.Default, indices), values.Array);
                for (int i = 0; i < n; i++) rows[i] = indices[i] is int k ? values.Rows[k] : "null";
                break;
            }
            default:
                array = ParquetWriteData.Column(type, valid, r);
                rows = ParquetWriteData.Canonical(array);
                break;
        }
        return new Column(new Field(name, type, nullable), array, rows);
    }

    private static string List(bool map, string[] items) => map ? "{" + string.Join(",", items) + "}" : "[" + string.Join(",", items) + "]";

    // ------------------------------------------------------------------
    // Canonical text of nested arrays as read back
    // ------------------------------------------------------------------

    /// <summary>The rows of an array read back, in the generator's text (nested types included).</summary>
    public static string[] Canonical(IArrowArray array) => [.. Enumerable.Range(0, array.Length).Select(i => Canonical(array, i))];

    /// <summary>
    /// The rows of an array a reader returned for a column written as <paramref name="written"/>: a map read as a
    /// list of key-value structs (Polars has no map type) is written as the map it was.
    /// </summary>
    public static string[] Canonical(IArrowArray array, IArrowType written) => [.. Enumerable.Range(0, array.Length).Select(i => Canonical(array, i, written))];

    public static string Canonical(IArrowArray a, int i, IArrowType? written = null)
    {
        ArrayData data = a.Data;
        IArrowArray Child(int k) => ArrowArrayFactory.BuildArray(data.Children[k]);
        written = written switch
        {
            RunEndEncodedType r => r.ValuesDataType,
            DictionaryType { ValueType: NestedType } d => d.ValueType,
            _ => written,
        };
        switch (a)
        {
            case RunEndEncodedArray runs:
            {
                IArrowArray ends = runs.RunEnds;
                int run = 0;
                while (ParquetWriteData.Canonical(ends, run) is var end && long.Parse(end) <= data.Offset + i) run++;
                return Canonical(runs.Values, run, written);
            }
            case StructArray s:
            {
                if (s.IsNull(i)) return "null";
                var type = (StructType)s.Data.DataType;
                var like = written as StructType;
                return "{" + string.Join(",", type.Fields.Select((f, k) => $"{f.Name}={Canonical(Child(k), data.Offset + i, like?.Fields[k].DataType)}")) + "}";
            }
            case MapArray m:
            {
                if (m.IsNull(i)) return "null";
                ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
                var entries = Child(0);
                var like = written as MapType;
                IArrowArray keys = ArrowArrayFactory.BuildArray(entries.Data.Children[0]), values = ArrowArrayFactory.BuildArray(entries.Data.Children[1]);
                int from = offsets[data.Offset + i], to = offsets[data.Offset + i + 1];
                return List(true, [.. Enumerable.Range(from, to - from).Select(k =>
                    $"{Canonical(keys, entries.Data.Offset + k, like?.KeyField.DataType)}=>{Canonical(values, entries.Data.Offset + k, like?.ValueField.DataType)}")]);
            }
            case ListArray or LargeListArray or FixedSizeListArray or ListViewArray or LargeListViewArray:
            {
                if (a.IsNull(i)) return "null";
                int row = data.Offset + i;
                (long from, long length) = a switch
                {
                    ListArray => (MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span)[row], MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span)[row + 1] - MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span)[row]),
                    LargeListArray => (MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span)[row], MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span)[row + 1] - MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span)[row]),
                    FixedSizeListArray f => ((long)row * ((FixedSizeListType)f.Data.DataType).ListSize, ((FixedSizeListType)f.Data.DataType).ListSize),
                    ListViewArray => (MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span)[row], MemoryMarshal.Cast<byte, int>(data.Buffers[2].Span)[row]),
                    _ => (MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span)[row], MemoryMarshal.Cast<byte, long>(data.Buffers[2].Span)[row]),
                };
                IArrowArray items = Child(0);
                if (written is MapType map && items is StructArray pairs)
                {
                    // A map read as a list of key-value structs.
                    IArrowArray keys = ArrowArrayFactory.BuildArray(pairs.Data.Children[0]), values = ArrowArrayFactory.BuildArray(pairs.Data.Children[1]);
                    return List(true, [.. Enumerable.Range(0, (int)length).Select(k => (int)from + k + pairs.Data.Offset).Select(k =>
                        $"{Canonical(keys, k, map.KeyField.DataType)}=>{Canonical(values, k, map.ValueField.DataType)}")]);
                }
                IArrowType? item = written is NestedType { Fields.Count: 1 } nested ? nested.Fields[0].DataType : null;
                return List(false, [.. Enumerable.Range(0, (int)length).Select(k => Canonical(items, (int)from + k, item))]);
            }
            case DictionaryArray d when d.Dictionary.Data.DataType is NestedType:
                return d.IsNull(i) ? "null" : Canonical(d.Dictionary, ArrowIndex(d, i), written);
            default:
                return ParquetWriteData.Canonical(a, i);
        }
    }

    private static int ArrowIndex(DictionaryArray d, int i) => d.Indices switch
    {
        Int8Array x => x.Values[i],
        Int16Array x => x.Values[i],
        Int32Array x => x.Values[i],
        UInt32Array x => (int)x.Values[i],
        _ => (int)((Int64Array)d.Indices).Values[i],
    };
}
