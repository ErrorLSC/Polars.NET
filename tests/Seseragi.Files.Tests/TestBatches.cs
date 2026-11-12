using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Seseragi.Files.Tests;

/// <summary>Builds Arrow arrays from raw values (so every fixed-width type, offset and null can be controlled).</summary>
internal static class TestBatches
{
    public static ArrowBuffer Validity(bool[] valid)
    {
        var bytes = new byte[(valid.Length + 7) / 8];
        for (int i = 0; i < valid.Length; i++)
            if (valid[i]) bytes[i >> 3] |= (byte)(1 << (i & 7));
        return new ArrowBuffer(bytes);
    }

    public static IArrowArray Fixed<T>(IArrowType type, T?[] values) where T : unmanaged
    {
        var raw = values.Select(v => v ?? default).ToArray();
        bool[] valid = [.. values.Select(v => v.HasValue)];
        int nulls = valid.Count(v => !v);
        var data = new ArrayData(type, values.Length, nulls, 0,
            [nulls == 0 ? ArrowBuffer.Empty : Validity(valid), new ArrowBuffer(MemoryMarshal.AsBytes(raw.AsSpan()).ToArray())]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public static IArrowArray Strings(string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values)
            if (v is null) b.AppendNull();
            else b.Append(v);
        return b.Build();
    }

    public static IArrowArray Struct(StructType type, IArrowArray[] children, bool[] valid)
    {
        int nulls = valid.Count(v => !v);
        return new StructArray(type, valid.Length, children, nulls == 0 ? ArrowBuffer.Empty : Validity(valid), nulls);
    }

    public static IArrowArray List(IArrowType type, int[] offsets, IArrowArray values, bool[] valid)
    {
        int nulls = valid.Count(v => !v);
        var data = new ArrayData(type, valid.Length, nulls, 0,
            [nulls == 0 ? ArrowBuffer.Empty : Validity(valid), new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Data]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public static RecordBatch Batch(params (string Name, IArrowArray Array, bool Nullable)[] columns)
    {
        var schema = new Schema([.. columns.Select(c => new Field(c.Name, c.Array.Data.DataType, c.Nullable))], null);
        return new RecordBatch(schema, columns.Select(c => c.Array), columns[0].Array.Length);
    }
}
