using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Builders;

/// <summary>List (or Map, which shares the layout) with 32-bit offsets.</summary>
internal sealed unsafe class NativeListBuilder : NativeColumnBuilder
{
    private NativeGrowBuffer _offsets;

    public NativeListBuilder(IArrowType type, NativeColumnBuilder values) : base(type)
    {
        Values = values;
        _offsets.Append(0);
    }

    public NativeColumnBuilder Values { get; }

    private long _end; // the last offset appended

    /// <summary>The items the lists appended so far span (the last offset).</summary>
    public long ItemCount => _end;

    /// <summary>
    /// Appends one list per entry of <paramref name="lengths"/> (their items appended to <see cref="Values"/> by the
    /// caller); with a <paramref name="mask"/> of the same length, lists whose mask byte is zero are null.
    /// </summary>
    public void AppendLists(ReadOnlySpan<int> lengths, ReadOnlySpan<byte> mask = default)
    {
        long end = _end;
        byte* at = _offsets.Reserve((long)lengths.Length * sizeof(int));
        var offsets = new Span<int>(at, lengths.Length);
        for (int i = 0; i < lengths.Length; i++)
        {
            end += lengths[i];
            if (end > int.MaxValue) throw new InvalidOperationException("A list column holds more than 2^31 items in one batch; use a smaller batch size.");
            offsets[i] = (int)end;
        }
        _end = end;
        if (mask.IsEmpty) MarkValid(lengths.Length);
        else MarkMask(mask);
    }

    /// <summary>Closes a list whose items were appended to <see cref="Values"/> since the previous one.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EndList()
    {
        AppendOffset();
        MarkValid();
    }

    private void AppendOffset()
    {
        long end = Values.Length;
        if (end > int.MaxValue) throw new InvalidOperationException("A list column holds more than 2^31 items in one batch; use a smaller batch size.");
        _offsets.Append((int)end);
        _end = end;
    }

    public override void AppendNull()
    {
        AppendOffset();
        MarkNull();
    }

    public override void AppendDefault()
    {
        AppendOffset();
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        nint offsets = export.Ptr(ref _offsets);
        var child = Values.Export(export);
        var a = export.NewArray(Length, nulls, [validity, offsets], [(nint)child]);
        Length = 0;
        _offsets.Append(0);
        _end = 0;
        return a;
    }

    protected override void FreeBuffers()
    {
        _offsets.Free();
        Values.Dispose();
    }
}

/// <summary>LargeList with 64-bit offsets: the items of each list appended to <see cref="Values"/>, then <see cref="EndList"/>.</summary>
internal sealed unsafe class NativeLargeListBuilder : NativeColumnBuilder
{
    private NativeGrowBuffer _offsets;

    public NativeLargeListBuilder(LargeListType type, NativeColumnBuilder values) : base(type)
    {
        Values = values;
        _offsets.Append(0L);
    }

    public NativeColumnBuilder Values { get; }

    protected override void ReserveValues(long slots, long dataBytes) => _offsets.EnsureAdditional(slots * sizeof(long));

    /// <summary>Closes a list whose items were appended to <see cref="Values"/> since the previous one.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EndList()
    {
        _offsets.Append(Values.Length);
        MarkValid();
    }

    public override void AppendNull()
    {
        _offsets.Append(Values.Length);
        MarkNull();
    }

    public override void AppendDefault()
    {
        _offsets.Append(Values.Length);
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        nint offsets = export.Ptr(ref _offsets);
        var child = Values.Export(export);
        var a = export.NewArray(Length, nulls, [validity, offsets], [(nint)child]);
        Length = 0;
        _offsets.Append(0L);
        return a;
    }

    protected override void FreeBuffers()
    {
        _offsets.Free();
        Values.Dispose();
    }
}

/// <summary>Fixed-size lists: every slot, null or not, holds <c>ListSize</c> items of <see cref="Values"/>.</summary>
internal sealed unsafe class NativeFixedSizeListBuilder(FixedSizeListType type, NativeColumnBuilder values) : NativeColumnBuilder(type)
{
    public NativeColumnBuilder Values { get; } = values;

    public int ListSize { get; } = type.ListSize;

    /// <summary>Marks one slot per byte of <paramref name="mask"/> (null where it is zero; all valid when empty: <paramref name="count"/> slots), the items appended by the caller.</summary>
    public void AppendSlots(int count, ReadOnlySpan<byte> mask = default)
    {
        if (mask.IsEmpty) MarkValid(count);
        else MarkMask(mask[..count]);
    }

    public override void AppendNull()
    {
        for (int i = 0; i < ListSize; i++) Values.AppendNull();
        MarkNull();
    }

    public override void AppendDefault()
    {
        for (int i = 0; i < ListSize; i++) Values.AppendDefault();
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var child = Values.Export(export);
        var a = export.NewArray(Length, nulls, [validity], [(nint)child]);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers() => Values.Dispose();
}

internal sealed unsafe class NativeStructBuilder(StructType type, NativeColumnBuilder[] children) : NativeColumnBuilder(type)
{
    private readonly bool[] _childNullable = [.. type.Fields.Select(f => f.IsNullable)];

    public NativeColumnBuilder[] Children { get; } = children;

    /// <summary>Marks a struct slot valid once its children have each received one value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EndStruct() => MarkValid();

    /// <summary>Marks <paramref name="count"/> struct slots valid once each child has that many more values.</summary>
    public void EndStructs(long count) => MarkValid(count);

    /// <summary>Marks one struct slot per byte of <paramref name="mask"/> (null where it is zero), the children filled by the caller.</summary>
    public void EndStructs(ReadOnlySpan<byte> mask) => MarkMask(mask);

    public override void AppendNull()
    {
        AppendChildPlaceholders();
        MarkNull();
    }

    public override void AppendDefault()
    {
        AppendChildPlaceholders();
        MarkValid();
    }

    private void AppendChildPlaceholders()
    {
        for (int i = 0; i < Children.Length; i++)
        {
            if (_childNullable[i]) Children[i].AppendNull();
            else Children[i].AppendDefault();
        }
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        Span<nint> kids = Children.Length <= 64 ? stackalloc nint[Children.Length] : new nint[Children.Length];
        for (int i = 0; i < Children.Length; i++) kids[i] = (nint)Children[i].Export(export);
        var a = export.NewArray(Length, nulls, [validity], kids);
        Length = 0;
        return a;
    }

    /// <summary>Exports this struct as a record batch (the root array) and imports it.</summary>
    public RecordBatch ExportBatch(Schema schema)
    {
        var export = new NativeBatchExport();
        try
        {
            var root = Export(export);
            return export.Import(root, schema);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    protected override void FreeBuffers()
    {
        foreach (var c in Children) c.Dispose();
    }
}
