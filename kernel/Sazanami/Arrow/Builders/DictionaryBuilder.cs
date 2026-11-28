using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Builders;

/// <summary>
/// Dictionary-encoded strings or binaries with Int32 (or UInt32) indices. The dictionary is either fixed up front
/// (<see cref="Symbols"/>: an Avro enum's symbols or a .NET enum's member names, re-emitted with every batch) or built
/// per batch in <see cref="Entries"/> (exported with the batch and then emptied; Utf8 or Binary values only).
/// </summary>
internal sealed unsafe class NativeDictionaryBuilder : NativeColumnBuilder
{
    private NativeGrowBuffer _indices;
    private byte[][] _symbols = [];
    private readonly NativeBinaryBuilder? _entries;

    public NativeDictionaryBuilder(DictionaryType type) : base(type)
    {
        if (type.IndexType is not (Int32Type or UInt32Type) || type.ValueType is not (StringType or BinaryType or StringViewType or BinaryViewType))
            throw new NotSupportedException("Native dictionary builders produce Int32 or UInt32 indices over Utf8, Binary or view values.");
        if (type.ValueType is StringType or BinaryType) _entries = new NativeBinaryBuilder(type.ValueType);
    }

    public IReadOnlyList<string> Symbols
    {
        set => _symbols = [.. value.Select(Encoding.UTF8.GetBytes)];
    }

    /// <summary>The dictionary of the current batch, when no <see cref="Symbols"/> are set (Utf8 or Binary values).</summary>
    public NativeBinaryBuilder Entries => _entries ?? throw new NotSupportedException("A dictionary of view values takes fixed symbols only.");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(int index)
    {
        _indices.Append(index);
        MarkValid();
    }

    /// <summary>
    /// Appends one slot per byte of <paramref name="mask"/>, null where the byte is zero, and returns their
    /// indices (zeroed) for the caller to fill the valid ones.
    /// </summary>
    public Span<int> AppendSlots(ReadOnlySpan<byte> mask)
    {
        byte* at = _indices.Reserve((long)mask.Length * sizeof(int));
        var slots = new Span<int>(at, mask.Length);
        slots.Clear();
        MarkMask(mask);
        return slots;
    }

    /// <summary>Appends a valid slot per index, the index plus <paramref name="add"/> (copied, then added in vectors).</summary>
    public void AppendIndices(ReadOnlySpan<int> indices, int add = 0)
    {
        var slots = new Span<int>(_indices.Reserve((long)indices.Length * sizeof(int)), indices.Length);
        indices.CopyTo(slots);
        if (add != 0) Kernels.VectorKernels.AddInPlace(slots, add);
        MarkValid(indices.Length);
    }

    public override void AppendNull()
    {
        _indices.Append(0);
        MarkNull();
    }

    public override void AppendDefault() => Append(0);

    protected override void ReserveValues(long slots, long dataBytes) => _indices.EnsureAdditional(slots * sizeof(int));

    public override NativeCArray* Export(NativeBatchExport export)
    {
        NativeCArray* dictionary;
        IArrowType valueType = ((DictionaryType)Type).ValueType;
        if (_symbols.Length > 0 || _entries is null)
        {
            if (valueType is StringViewType or BinaryViewType)
            {
                using var views = new NativeBinaryViewBuilder(valueType);
                foreach (var s in _symbols) views.Append(s);
                dictionary = views.Export(export);
            }
            else
            {
                using var dict = new NativeBinaryBuilder(valueType);
                foreach (var s in _symbols) dict.Append(s);
                dictionary = dict.Export(export);
            }
        }
        else dictionary = _entries.Export(export);
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, export.Ptr(ref _indices)], [], dictionary);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers()
    {
        _indices.Free();
        _entries?.Dispose();
    }
}
