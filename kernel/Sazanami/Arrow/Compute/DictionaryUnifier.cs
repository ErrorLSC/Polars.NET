using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>
/// One dictionary for every batch of a stream (a dataset's files, a file's row groups): each dictionary column, at any
/// depth, gets a global dictionary that only grows, its values in the order first seen; every batch's indices are
/// remapped into it (Int32, or the index type the target schema declares) and the batch carries the global dictionary as it then is. Batches must come in order (a
/// value's index is where it was first seen). Arrays made share the dictionary's memory, which a batch keeps after the
/// unifier is gone.
/// </summary>
/// <remarks>
/// Values are strings, binaries (any layout) or fixed-width values; a null entry of a batch's dictionary makes its
/// rows null (the same value). A dictionary of values that hold dictionaries themselves is refused.
/// </remarks>
internal sealed class DictionaryUnifier : IDisposable
{
    private readonly Schema _schema;
    private readonly ArrowConversion?[] _columns;
    private readonly List<GlobalDictionary> _dictionaries;

    private DictionaryUnifier(Schema schema, ArrowConversion?[] columns, List<GlobalDictionary> dictionaries)
    {
        _schema = schema;
        _columns = columns;
        _dictionaries = dictionaries;
    }

    /// <summary>The type with every dictionary, at any depth, indexed by Int32 (what unified batches hold).</summary>
    public static IArrowType Unified(IArrowType type) => type switch
    {
        DictionaryType d => new DictionaryType(Int32Type.Default, Unified(d.ValueType), d.Ordered),
        StructType s => new StructType([.. s.Fields.Select(Unified)]),
        ListType l => new ListType(Unified(l.ValueField)),
        LargeListType l => new LargeListType(Unified(l.ValueField)),
        ListViewType l => new ListViewType(Unified(l.ValueField)),
        LargeListViewType l => new LargeListViewType(Unified(l.ValueField)),
        FixedSizeListType l => new FixedSizeListType(Unified(l.ValueField), l.ListSize),
        MapType m => new MapType(Unified(m.KeyField), Unified(m.ValueField), m.KeySorted),
        _ => type,
    };

    /// <inheritdoc cref="Unified(IArrowType)"/>
    public static Field Unified(Field field) => new(field.Name, Unified(field.DataType), field.IsNullable, field.Metadata);

    /// <inheritdoc cref="Unified(IArrowType)"/>
    public static Schema Unified(Schema schema) => new([.. schema.FieldsList.Select(Unified)], schema.Metadata);

    /// <summary>
    /// The type unified batches of <paramref name="type"/> (unified: Int32 indices) are delivered as: each dictionary
    /// that <paramref name="declared"/> (a given schema's type, matched by field names and positions) declares too
    /// indexed by its declared index type, the others by Int32.
    /// </summary>
    public static IArrowType Declared(IArrowType type, IArrowType? declared) => (type, declared) switch
    {
        (DictionaryType d, DictionaryType g) => new DictionaryType(g.IndexType, d.ValueType, d.Ordered),
        (StructType s, StructType g) => new StructType([.. s.Fields.Select(f => Declared(f, g.GetFieldIndex(f.Name) is int i and >= 0 ? g.Fields[i] : null))]),
        (MapType m, MapType g) => new MapType(Declared(m.KeyField, g.KeyField), Declared(m.ValueField, g.ValueField), m.KeySorted),
        (ListType l, ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType) => new ListType(Declared(l.ValueField, Element(declared))),
        (LargeListType l, ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType) => new LargeListType(Declared(l.ValueField, Element(declared))),
        (ListViewType l, ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType) => new ListViewType(Declared(l.ValueField, Element(declared))),
        (LargeListViewType l, ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType) => new LargeListViewType(Declared(l.ValueField, Element(declared))),
        (FixedSizeListType l, ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType) => new FixedSizeListType(Declared(l.ValueField, Element(declared)), l.ListSize),
        _ => type,
    };

    private static Field? Element(IArrowType? list) => list switch
    {
        MapType => null,
        NestedType { Fields.Count: 1 } nested => nested.Fields[0],
        _ => null,
    };

    /// <inheritdoc cref="Declared(IArrowType, IArrowType?)"/>
    public static Field Declared(Field field, Field? declared) =>
        declared is null ? field : new(field.Name, Declared(field.DataType, declared.DataType), field.IsNullable, field.Metadata);

    /// <inheritdoc cref="Declared(IArrowType, IArrowType?)"/>
    public static Schema Declared(Schema schema, Schema declared) =>
        new([.. schema.FieldsList.Select(f => Declared(f, declared.GetFieldIndex(f.Name) is int i and >= 0 ? declared.FieldsList[i] : null))], schema.Metadata);

    /// <summary>
    /// The unifier of batches of <paramref name="schema"/> unified (Int32 indices) delivered as <paramref name="schema"/>
    /// says (its dictionaries' index types: see <see cref="Declared(Schema, Schema)"/>), or null when it holds no dictionary.
    /// </summary>
    /// <exception cref="NotSupportedException">A dictionary of values that hold dictionaries, or of values of no fixed width (booleans, nested).</exception>
    public static DictionaryUnifier? For(Schema schema)
    {
        List<GlobalDictionary> dictionaries = [];
        try
        {
            ArrowConversion?[] columns = [.. schema.FieldsList.Select(f => Compile(f.DataType, f.Name, dictionaries))];
            if (dictionaries.Count == 0) return null;
            return new DictionaryUnifier(schema, columns, dictionaries);
        }
        catch
        {
            foreach (GlobalDictionary dictionary in dictionaries) dictionary.Dispose();
            throw;
        }
    }

    /// <summary>The batch with its dictionaries unified; <paramref name="batch"/> is consumed (disposed, or its unchanged columns moved over).</summary>
    public RecordBatch Apply(RecordBatch batch)
    {
        var columns = new IArrowArray[_columns.Length];
        int done = 0;
        try
        {
            for (; done < columns.Length; done++)
            {
                IArrowArray column = batch.Column(done);
                columns[done] = _columns[done] is { } conversion ? conversion.Convert(column) : column;
            }
        }
        catch
        {
            for (int c = 0; c < done; c++)
            {
                if (_columns[c] is not null) columns[c].Dispose();
            }
            throw;
        }
        for (int c = 0; c < columns.Length; c++)
        {
            if (_columns[c] is not null) batch.Column(c).Dispose();
        }
        return new RecordBatch(_schema, columns, batch.Length);
    }

    public void Dispose()
    {
        foreach (GlobalDictionary dictionary in _dictionaries) dictionary.Dispose();
        _dictionaries.Clear();
    }

    /// <summary>The conversion of arrays of <paramref name="type"/> that unifies its dictionaries; null when it holds none.</summary>
    private static ArrowConversion? Compile(IArrowType type, string path, List<GlobalDictionary> dictionaries)
    {
        switch (type)
        {
            case DictionaryType d:
            {
                if (Compile(d.ValueType, path, []) is not null) throw new NotSupportedException($"'{path}': a dictionary of values holding dictionaries cannot be unified.");
                var dictionary = new GlobalDictionary(d.ValueType, path);
                dictionaries.Add(dictionary);
                return new UnifyConversion(d, dictionary);
            }
            case StructType s:
            {
                var fields = new (int, ArrowConversion)[s.Fields.Count];
                bool any = false;
                for (int i = 0; i < fields.Length; i++)
                {
                    ArrowConversion? field = Compile(s.Fields[i].DataType, $"{path}.{s.Fields[i].Name}", dictionaries);
                    any |= field is not null;
                    fields[i] = (i, field ?? new IdentityConversion(s.Fields[i].DataType));
                }
                return any ? new StructConversion(s, fields) : null;
            }
            case MapType m:
                return Compile(m.KeyValueType, path + "{}", dictionaries) is { } entries ? new ListConversion(m, entries) : null;
            case ListType or LargeListType or ListViewType or LargeListViewType:
                return Compile(ArrowWidening.ListElement(type), path + "[]", dictionaries) is { } elements ? new ListConversion(type, elements) : null;
            case FixedSizeListType f:
                return Compile(f.ValueDataType, path + "[]", dictionaries) is { } items ? new FixedSizeListConversion(f, items) : null;
            default:
                return null;
        }
    }
}

/// <summary>A block of the pool that the arrays pointing into it share: freed when the last of them (and its maker) lets go.</summary>
internal sealed unsafe class SharedBlock : NativeMemoryOwner
{
    public SharedBlock(long capacity)
    {
        Capacity = Math.Max(64, capacity);
        Pointer = NativeMemoryPool.Alloc(Capacity);
    }

    public byte* Pointer { get; }

    public long Capacity { get; }

    /// <summary>The first <paramref name="length"/> bytes as a buffer of the export's batch (which then keeps the block too).</summary>
    public nint Share(NativeBatchExport export, long length)
    {
        if (length == 0) return 0;
        ArrowBuffer buffer = NativeBatchExport.NewBuffer(new NativeBufferMemory(this, Pointer, checked((int)length)));
        export.Hold(buffer);
        return export.TryShare(buffer, out nint pointer) ? pointer : throw new InvalidOperationException("A shared block could not be shared.");
    }

    protected override void FreeMemory() => NativeMemoryPool.Free(Pointer);
}

/// <summary>Bytes appended at the end only, whose prefix arrays share: it grows into a new block, the old one kept by the arrays using it.</summary>
internal sealed unsafe class AppendOnlyBuffer(long capacity) : IDisposable
{
    private SharedBlock _block = new(capacity);

    public long Length { get; private set; }

    public void Append(ReadOnlySpan<byte> bytes)
    {
        Ensure(bytes.Length);
        bytes.CopyTo(new Span<byte>(_block.Pointer + Length, bytes.Length));
        Length += bytes.Length;
    }

    public void Append<T>(T value) where T : unmanaged
    {
        Ensure(sizeof(T));
        *(T*)(_block.Pointer + Length) = value;
        Length += sizeof(T);
    }

    /// <inheritdoc cref="SharedBlock.Share"/>
    public nint Share(NativeBatchExport export, long length) => _block.Share(export, length);

    private void Ensure(long more)
    {
        if (Length + more <= _block.Capacity) return;
        var next = new SharedBlock(Math.Max(_block.Capacity * 2, Length + more));
        Buffer.MemoryCopy(_block.Pointer, next.Pointer, next.Capacity, Length);
        _block.Release();
        _block = next;
    }

    public void Dispose() => _block.Release();
}

/// <summary>
/// The global dictionary of one dictionary column: its distinct values in order first seen (looked up by their bytes),
/// stored in the column's value layout for arrays to share.
/// </summary>
internal sealed unsafe class GlobalDictionary : IDisposable
{
    private const int ViewBlock = 1 << 20;
    private const int Remembered = 4;

    private readonly IArrowType _type;
    private readonly string _path;
    private readonly int _width;                        // bytes of a fixed-width value; 0 for strings and binaries
    private readonly ByteArrayDictionary _lookup = new();
    private readonly AppendOnlyBuffer _values;          // fixed-width values; string bytes (plain, large); views (view layout)
    private readonly AppendOnlyBuffer? _offsets;        // plain (int) or large (long) offsets
    private readonly List<(SharedBlock Block, long Used)> _blocks = []; // the data buffers of views
    private readonly List<Remap> _remaps = [];          // the remaps of the last dictionaries seen (their memory held)
    private readonly NativeArray<int> _scratch = new(); // the remap of a dictionary not remembered

    private sealed record Remap(nint Address, int Offset, int Length, NativeMemoryOwner Owner, NativeArray<int> Ids);

    public GlobalDictionary(IArrowType type, string path)
    {
        _type = type;
        _path = path;
        switch (type)
        {
            case StringType or BinaryType:
                _values = new AppendOnlyBuffer(4096);
                _offsets = new AppendOnlyBuffer(1024);
                _offsets.Append(0);
                break;
            case LargeStringType or LargeBinaryType:
                _values = new AppendOnlyBuffer(4096);
                _offsets = new AppendOnlyBuffer(2048);
                _offsets.Append(0L);
                break;
            case StringViewType or BinaryViewType:
                _values = new AppendOnlyBuffer(4096);
                break;
            case FixedWidthType f when f is not BooleanType && f.BitWidth % 8 == 0:
                _width = f.BitWidth / 8;
                _values = new AppendOnlyBuffer(Math.Max(64, 256L * _width));
                break;
            default:
                throw new NotSupportedException($"'{path}': dictionaries of {type.Name} values cannot be unified (strings, binaries and fixed-width values can).");
        }
    }

    public IArrowType Type => _type;

    /// <summary>The column's path (in messages).</summary>
    public string Path => _path;

    public int Count => _lookup.Count;

    /// <summary>
    /// The global index of each entry of <paramref name="dictionary"/> (-1 for a null entry), adding the values not seen
    /// yet. Remembered for a dictionary in native memory seen again (the same address, offset and length; its memory is
    /// held meanwhile, so the address cannot be reused); otherwise valid until the next call.
    /// </summary>
    public ReadOnlySpan<int> Ids(ArrayData dictionary)
    {
        nint address = 0;
        NativeMemoryOwner? owner = null;
        if (dictionary.Buffers.Length > 1
            && System.Runtime.InteropServices.MemoryMarshal.TryGetMemoryManager(dictionary.Buffers[1].Memory, out NativeBufferMemory? manager, out int start, out _))
        {
            address = (nint)System.Runtime.CompilerServices.Unsafe.AsPointer(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(manager.GetSpan())) + start;
            owner = manager.Owner;
            foreach (Remap known in _remaps)
            {
                if (known.Address == address && known.Offset == dictionary.Offset && known.Length == dictionary.Length) return known.Ids.AsSpan(0, known.Length);
            }
        }
        NativeArray<int> ids = owner is null ? _scratch : new NativeArray<int>(Math.Max(1, dictionary.Length));
        try
        {
            ids.Reserve(Math.Max(1, dictionary.Length));
            IArrowArray? values = _width == 0 ? ArrowArrayFactory.BuildArray(dictionary) : null;
            for (int j = 0; j < dictionary.Length; j++)
            {
                if (!ArrowConversion.IsValid(dictionary, j))
                {
                    ids[j] = -1;
                    continue;
                }
                ReadOnlySpan<byte> value = values is not null
                    ? RelayoutConversion.Bytes(values, j)
                    : dictionary.Buffers[1].Span.Slice((dictionary.Offset + j) * _width, _width);
                ids[j] = Add(value);
            }
        }
        catch
        {
            if (owner is not null) ids.Dispose();
            throw;
        }
        if (owner is not null)
        {
            owner.AddBuffer();
            _remaps.Add(new Remap(address, dictionary.Offset, dictionary.Length, owner, ids));
            if (_remaps.Count > Remembered)
            {
                Release(_remaps[0]);
                _remaps.RemoveAt(0);
            }
        }
        return ids.AsSpan(0, dictionary.Length);
    }

    private static void Release(Remap remap)
    {
        remap.Ids.Dispose();
        remap.Owner.Release();
    }

    /// <summary>The global index of a value, added at the end when new.</summary>
    private int Add(ReadOnlySpan<byte> value)
    {
        int before = _lookup.Count;
        if (before == int.MaxValue) throw new OverflowException($"'{_path}': more than {int.MaxValue} distinct dictionary values.");
        int id = _lookup.GetOrAdd(value);
        if (id < before) return id;
        switch (_type)
        {
            case StringType or BinaryType:
                if (_values.Length + value.Length > int.MaxValue) throw new OverflowException($"'{_path}': the distinct dictionary values pass 2 GB (a large or view layout holds more).");
                _values.Append(value);
                _offsets!.Append((int)_values.Length);
                break;
            case LargeStringType or LargeBinaryType:
                _values.Append(value);
                _offsets!.Append(_values.Length);
                break;
            case StringViewType or BinaryViewType:
            {
                int buffer = 0, offset = 0;
                if (value.Length > NativeBinaryViewBuilder.MaxInline)
                {
                    if (_blocks.Count == 0 || _blocks[^1].Used + value.Length > _blocks[^1].Block.Capacity)
                        _blocks.Add((new SharedBlock(Math.Max(ViewBlock, value.Length)), 0));
                    var (block, used) = _blocks[^1];
                    value.CopyTo(new Span<byte>(block.Pointer + used, value.Length));
                    buffer = _blocks.Count - 1;
                    offset = (int)used;
                    _blocks[^1] = (block, used + value.Length);
                }
                _values.Append(NativeBinaryViewBuilder.View(value, buffer, offset));
                break;
            }
            default:
                _values.Append(value);
                break;
        }
        return id;
    }

    /// <summary>The dictionary as it now is, as an array of the export's batch sharing its memory.</summary>
    public NativeCArray* Snapshot(NativeBatchExport export)
    {
        int count = Count;
        switch (_type)
        {
            case StringType or BinaryType:
                return export.NewArray(count, 0, [0, _offsets!.Share(export, (count + 1) * 4L), _values.Share(export, _values.Length)], []);
            case LargeStringType or LargeBinaryType:
                return export.NewArray(count, 0, [0, _offsets!.Share(export, (count + 1) * 8L), _values.Share(export, _values.Length)], []);
            case StringViewType or BinaryViewType:
            {
                nint[] buffers = new nint[2 + _blocks.Count];
                buffers[1] = _values.Share(export, count * 16L);
                for (int b = 0; b < _blocks.Count; b++) buffers[2 + b] = _blocks[b].Block.Share(export, Math.Max(1, _blocks[b].Used));
                return export.NewArray(count, 0, buffers, []);
            }
            default:
                return export.NewArray(count, 0, [0, _values.Share(export, (long)count * _width)], []);
        }
    }

    public void Dispose()
    {
        foreach (Remap remap in _remaps) Release(remap);
        _remaps.Clear();
        _values.Dispose();
        _offsets?.Dispose();
        foreach (var (block, _) in _blocks) block.Release();
        _blocks.Clear();
        _lookup.Dispose();
        _scratch.Dispose();
    }
}

/// <summary>
/// A dictionary column's arrays into the global dictionary: indices remapped into the target's index type (more
/// distinct values than it reaches: an error), the global dictionary as it now is.
/// </summary>
internal sealed unsafe class UnifyConversion(DictionaryType target, GlobalDictionary dictionary) : ArrowConversion(target)
{
    private readonly int _width = ((IntegerType)target.IndexType).BitWidth / 8;

    /// <summary>The largest index the target's index type holds (the global dictionary stops at Int32).</summary>
    private readonly long _most = (IntegerType)target.IndexType switch
    {
        { BitWidth: 8, IsSigned: true } => sbyte.MaxValue,
        { BitWidth: 8 } => byte.MaxValue,
        { BitWidth: 16, IsSigned: true } => short.MaxValue,
        { BitWidth: 16 } => ushort.MaxValue,
        _ => int.MaxValue,
    };

    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        int length = source.Length;
        var indexType = (IntegerType)((DictionaryType)source.DataType).IndexType;
        ReadOnlySpan<int> ids = source.Dictionary is { } values ? dictionary.Ids(values) : [];
        if (dictionary.Count - 1L > _most)
            throw new OverflowException($"'{dictionary.Path}': {dictionary.Count} distinct dictionary values cannot be indexed by {target.IndexType.Name} indices (declare a wider index type).");
        var indices = (byte*)export.Allocate(Math.Max(1, length) * (long)_width);
        var valid = (ulong*)export.Allocate(Math.Max(1, Filters.Masks.Words(length)) * 8L);
        long nulls = 0;
        var slots = new Slots(source, 1, indexType);
        for (int i = 0; i < length; i++)
        {
            int id = -1;
            if (slots.IsValid(i))
            {
                long index = slots[i];
                id = (ulong)index < (ulong)ids.Length ? ids[(int)index] : throw new InvalidDataException($"Dictionary index {index} is past its {ids.Length} entries.");
            }
            if (id < 0)
            {
                nulls++;
                continue;
            }
            switch (_width)
            {
                case 1: indices[i] = (byte)id; break;
                case 2: ((ushort*)indices)[i] = (ushort)id; break;
                case 4: ((int*)indices)[i] = id; break;
                default: ((long*)indices)[i] = id; break;
            }
            valid[i >> 6] |= 1UL << (i & 63);
        }
        NativeCArray* snapshot = dictionary.Snapshot(export);
        return export.NewArray(length, nulls, [nulls == 0 ? 0 : (nint)valid, (nint)indices], [], snapshot);
    }
}
