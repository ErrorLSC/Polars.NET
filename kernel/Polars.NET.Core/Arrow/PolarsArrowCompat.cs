using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow;

/// <summary>
/// Rewrites Arrow types the Polars engine cannot import before handing batches to it:
/// <list type="bullet">
/// <item>Decimal256 (more than 38 digits) is refused with an error naming the column; Polars decimals stop at 38.</item>
/// <item>A month-day-nano interval becomes <c>struct{months: Int32, days: Int32, nanoseconds: Duration(ns)}</c>,
/// the layout Polars itself uses when told to import intervals as structs.</item>
/// </list>
/// Everything else passes through untouched (and without copying).
/// </summary>
internal static class PolarsArrowCompat
{
    private static readonly StructType IntervalStruct = new(
    [
        new Field("months", Int32Type.Default, true),
        new Field("days", Int32Type.Default, true),
        new Field("nanoseconds", DurationType.Nanosecond, true),
    ]);

    /// <summary>The schema Polars will receive; <paramref name="changed"/> tells whether batches need <see cref="Convert"/>.</summary>
    public static Schema Adapt(Schema schema, out bool changed)
    {
        var (fields, anyChanged) = AdaptFields(schema.FieldsList, "");
        changed = anyChanged;
        return changed ? new Schema(fields, schema.Metadata) : schema;
    }

    private static (List<Field> Fields, bool Changed) AdaptFields(IReadOnlyList<Field> fields, string parent)
    {
        List<Field> result = [];
        bool changed = false;
        foreach (var f in fields)
        {
            var (type, c) = AdaptType(f.DataType, parent.Length == 0 ? f.Name : $"{parent}.{f.Name}");
            changed |= c;
            result.Add(c ? new Field(f.Name, type, f.IsNullable, f.Metadata) : f);
        }
        return (result, changed);
    }

    private static (IArrowType Type, bool Changed) AdaptType(IArrowType type, string path) => type switch
    {
        Decimal256Type d => throw new NotSupportedException(
            $"Column '{path}' is a decimal with {d.Precision} digits; Polars supports at most 38."),
        IntervalType { Unit: IntervalUnit.MonthDayNanosecond } => (IntervalStruct, true),
        StructType s => AdaptFields(s.Fields, path) is ({ } fields, true) ? (new StructType(fields), true) : (type, false),
        MapType m => AdaptFields([m.ValueField], path) is ({ } value, true) ? (new MapType(m.KeyField, value[0], m.KeySorted), true) : (type, false),
        ListType l => AdaptFields([l.ValueField], path) is ({ } item, true) ? (new ListType(item[0]), true) : (type, false),
        LargeListType l => AdaptFields([l.ValueField], path) is ({ } item, true) ? (new LargeListType(item[0]), true) : (type, false),
        FixedSizeListType l => AdaptFields([l.ValueField], path) is ({ } item, true) ? (new FixedSizeListType(item[0], l.ListSize), true) : (type, false),
        _ => (type, false),
    };

    /// <summary>
    /// Converts a batch to the adapted schema. Buffers that need no change are shared, so the result owns them:
    /// do not dispose the source batch separately.
    /// </summary>
    public static RecordBatch Convert(RecordBatch batch, Schema adapted)
    {
        var arrays = new IArrowArray[batch.ColumnCount];
        for (int i = 0; i < arrays.Length; i++)
            arrays[i] = ArrowArrayFactory.BuildArray(ConvertData(batch.Column(i).Data, adapted.FieldsList[i].DataType));
        return new RecordBatch(adapted, arrays, batch.Length);
    }

    private static ArrayData ConvertData(ArrayData data, IArrowType target)
    {
        if (ReferenceEquals(data.DataType, target)) return data;
        if (data.DataType is IntervalType { Unit: IntervalUnit.MonthDayNanosecond }) return IntervalToStruct(data);
        // A nested type with an interval somewhere inside: same buffers, converted children (children of nested
        // Arrow data are not sliced by the parent's offset, so the whole child is converted).
        var childTypes = target switch
        {
            StructType s => s.Fields.Select(f => f.DataType).ToArray(),
            MapType m => [m.KeyValueType],
            ListType l => [l.ValueDataType],
            LargeListType l => [l.ValueDataType],
            FixedSizeListType l => [l.ValueDataType],
            _ => throw new InvalidOperationException($"Unexpected Arrow type {target} while adapting for Polars."),
        };
        var children = data.Children.Select((c, i) => ConvertData(c, childTypes[i])).ToArray();
        return new ArrayData(target, data.Length, data.NullCount, data.Offset, data.Buffers, children, data.Dictionary);
    }

    /// <summary>Splits the 16-byte (months, days, nanoseconds) values into three child columns; the validity
    /// bitmap and offset carry over to the struct.</summary>
    private static ArrayData IntervalToStruct(ArrayData data)
    {
        int n = data.Offset + data.Length;
        var raw = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span)[..(n * 2)];
        var months = new int[n];
        var days = new int[n];
        var nanos = new long[n];
        for (int i = 0; i < n; i++)
        {
            long packed = raw[2 * i];
            months[i] = (int)packed;
            days[i] = (int)(packed >> 32);
            nanos[i] = raw[2 * i + 1];
        }
        ArrayData Child(IArrowType type, byte[] values) =>
            new(type, n, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(values)]);
        return new ArrayData(IntervalStruct, data.Length, data.NullCount, data.Offset, [data.Buffers[0]],
        [
            Child(Int32Type.Default, MemoryMarshal.AsBytes(months.AsSpan()).ToArray()),
            Child(Int32Type.Default, MemoryMarshal.AsBytes(days.AsSpan()).ToArray()),
            Child(DurationType.Nanosecond, MemoryMarshal.AsBytes(nanos.AsSpan()).ToArray()),
        ]);
    }
}
