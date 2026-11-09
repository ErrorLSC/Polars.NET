using System.Diagnostics.CodeAnalysis;
using System.Buffers;
using Apache.Arrow;
using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.NET.Core.Data;
using Kaminari.Sazanami.Data;

namespace Polars.NET.ML.DataView;

/// <summary>
/// The global category table of an Arrow Dictionary column (Polars Categorical / Enum), exposed to ML.NET as a
/// <see cref="KeyDataViewType"/> over <see cref="uint"/>: key <c>k</c> (1-based) is <c>Categories[k - 1]</c>,
/// and key 0 is missing (null).
/// </summary>
/// <remarks>
/// Polars exports a Categorical column with a per-batch, compacted dictionary: the same string can carry a
/// different local index in every batch (and slices are re-compacted). Local indices are therefore never used as
/// keys directly; each batch gets a small local-index -> global-key remap table built from its dictionary.
/// Enum columns export their full, fixed category list in every batch, so their remap is the identity.
/// </remarks>
internal sealed class DictionaryKeyColumn
{
    private readonly List<string> _categories = [];
    private readonly Dictionary<string, uint> _keyOf = new(StringComparer.Ordinal);

    public IReadOnlyList<string> Categories => _categories;

    /// <summary>A key column whose category table is known up front (Polars Enum); no data pre-scan needed.</summary>
    public static DictionaryKeyColumn FromCategories(IEnumerable<string> categories)
    {
        var column = new DictionaryKeyColumn();
        foreach (string s in categories) column.Add(s);
        return column;
    }

    private void Add(string s)
    {
        if (_keyOf.ContainsKey(s)) return;
        _categories.Add(s);
        _keyOf.Add(s, (uint)_categories.Count);
    }

    /// <summary>
    /// ML.NET requires a key cardinality of at least 1; an all-null / empty column still gets one (unused) slot.
    /// </summary>
    public ulong KeyCount => (ulong)Math.Max(1, _categories.Count);

    public KeyDataViewType KeyType => new(typeof(uint), KeyCount);

    /// <summary>Registers every value of a batch dictionary, in first-appearance order.</summary>
    public void AddCategories(IArrowArray dictionary)
    {
        using var values = new StringViewAccessor();
        values.SetBatch(dictionary);
        for (int i = 0; i < dictionary.Length; i++)
        {
            if (!values.IsNull(i)) Add(values.GetTypedValue(i));
        }
    }

    /// <summary>Maps each entry of a batch dictionary to its global 1-based key (0 for null or unknown).</summary>
    public uint[] BuildRemap(IArrowArray dictionary)
    {
        var remap = new uint[dictionary.Length];
        using var values = new StringViewAccessor();
        values.SetBatch(dictionary);
        for (int i = 0; i < remap.Length; i++)
        {
            if (!values.IsNull(i) && _keyOf.TryGetValue(values.GetTypedValue(i), out uint key))
                remap[i] = key;
        }
        return remap;
    }

    /// <summary>The standard ML.NET <c>KeyValues</c> annotation, so MapKeyToValue and friends can recover the text.</summary>
    public DataViewSchema.Annotations BuildAnnotations()
    {
        var builder = new DataViewSchema.Annotations.Builder();
        if (_categories.Count == 0)
            return builder.ToAnnotations();

        string[] categories = [.. _categories];
        builder.Add(
            "KeyValues",
            new VectorDataViewType(TextDataViewType.Instance, categories.Length),
            (ref VBuffer<ReadOnlyMemory<char>> value) =>
            {
                var editor = VBufferEditor.Create(ref value, categories.Length);
                for (int i = 0; i < categories.Length; i++) editor.Values[i] = categories[i].AsMemory();
                value = editor.Commit();
            });
        return builder.ToAnnotations();
    }
}

/// <summary>
/// Reads an Arrow DictionaryArray as ML.NET key values: the index buffer is read through a raw pointer
/// (any of the 8 integer index widths) and translated through the per-batch remap table.
/// </summary>
internal sealed unsafe class DictionaryKeyAccessor(DictionaryKeyColumn column) : ColumnAccessor
{
    private MemoryHandle _indicesHandle;
    private byte* _indicesPtr;
    private delegate*<byte*, int, int> _readIndex;
    private uint[] _remap = [];

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(uint);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var dictArray = (DictionaryArray)array;
        BindNullBitmap(dictArray);
        _indicesPtr = DictionaryAccessor.BindIndexBuffer(dictArray.Indices, ref _indicesHandle, out _readIndex);
        _remap = column.BuildRemap(dictArray.Dictionary);
    }

    public override bool IsNull(int index) => IsNullFast(index);

    public uint GetKey(int index)
    {
        if (IsNullFast(index)) return 0;
        int local = _readIndex(_indicesPtr, index);
        uint[] remap = _remap;
        return (uint)local < (uint)remap.Length ? remap[local] : 0;
    }

    public override object? GetValue(int index) => GetKey(index);

    public override void Dispose()
    {
        _indicesHandle.Dispose();
        _indicesPtr = null;
        base.Dispose();
    }
}
