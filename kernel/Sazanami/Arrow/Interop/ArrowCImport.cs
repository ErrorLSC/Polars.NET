using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow.Interop;

/// <summary>
/// Imports arrays of the Arrow C data interface (from the engine, or any other producer) as Apache.Arrow arrays over
/// the producer's memory, without copying.
/// </summary>
/// <remarks>
/// <para>
/// Apache.Arrow's own importer (<c>CArrowArrayImporter</c>, <c>CArrowArrayStreamImporter</c>) reports every buffer
/// it imports to the GC as memory pressure: with a managed heap of a few MiB, tens of MiB of "pressure" per batch force
/// a full (gen2) collection per batch, all threads stopped. It is banned in this repository (BannedSymbols.txt); this
/// importer replaces it.
/// </para>
/// <para>
/// Import moves the array (the C interface's move semantics): the producer's struct is left released, the array is
/// released once every buffer over its memory is disposed (or, as a safety net, when the arrays are collected).
/// The buffers' lengths, which the interface does not carry, follow from the type's layout.
/// </para>
/// </remarks>
internal static unsafe class ArrowCImport
{
    /// <summary>Imports a struct array (the engine's export of a frame) as a record batch of <paramref name="schema"/>.</summary>
    /// <exception cref="InvalidDataException">The array does not have the schema's layout.</exception>
    public static RecordBatch ImportRecordBatch(CArrowArray* array, Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var source = (NativeCArray*)array;
        using var import = new Import(source);
        NativeCArray* root = import.Root;
        if (root->ChildCount != schema.FieldsList.Count)
            throw new InvalidDataException($"The imported batch has {root->ChildCount} columns; its schema {schema.FieldsList.Count}.");
        if (root->Offset != 0)
            throw new InvalidDataException("The imported batch has an offset; a record batch cannot.");
        var columns = new IArrowArray[schema.FieldsList.Count];
        for (int i = 0; i < columns.Length; i++)
            columns[i] = ArrowArrayFactory.BuildArray(import.Data(root->Children[i], schema.FieldsList[i].DataType));
        import.Complete();
        return new RecordBatch(schema, columns, checked((int)root->Length));
    }

    /// <summary>Imports one array of <paramref name="type"/>.</summary>
    /// <exception cref="InvalidDataException">The array does not have the type's layout.</exception>
    public static IArrowArray ImportArray(CArrowArray* array, IArrowType type)
    {
        ArgumentNullException.ThrowIfNull(type);
        using var import = new Import((NativeCArray*)array);
        IArrowArray result = ArrowArrayFactory.BuildArray(import.Data(import.Root, type));
        import.Complete();
        return result;
    }

    /// <summary>Releases an array struct the caller owns and gives up importing (the struct itself is not freed).</summary>
    public static void Release(CArrowArray* array)
    {
        var a = (NativeCArray*)array;
        if (a->Release != null) a->Release(a);
    }

    /// <summary>Releases a schema struct the caller owns (what Apache.Arrow's <c>CArrowSchema.Free</c> does, without freeing the struct).</summary>
    public static void Release(CArrowSchema* schema)
    {
        var s = (NativeCSchema*)schema;
        if (s->Release != null) s->Release(s);
    }

    /// <summary>
    /// One import: the array moved into memory of its own (the owner of every buffer made), the buffers made so far
    /// (disposed when the import fails, which lets the array go at once).
    /// </summary>
    private sealed class Import : IDisposable
    {
        private readonly ImportedArrayMemory _owner;
        private readonly List<NativeBufferMemory> _made = [];
        private bool _complete;

        public Import(NativeCArray* source)
        {
            if (source->Release == null) throw new ArgumentException("The array is released already.", nameof(source));
            _owner = new ImportedArrayMemory(source);
        }

        public NativeCArray* Root => _owner.Array;

        public void Complete() => _complete = true;

        public void Dispose()
        {
            if (!_complete)
            {
                foreach (NativeBufferMemory memory in _made) ((IDisposable)memory).Dispose();
            }
            _owner.Sealed();
        }

        public ArrayData Data(NativeCArray* array, IArrowType type)
        {
            long length = array->Length, offset = array->Offset;
            if (length < 0 || offset < 0 || length > int.MaxValue || offset > int.MaxValue - length)
                throw new InvalidDataException($"An imported {type.Name} array has length {length} and offset {offset}.");
            long end = length + offset; // the slots the buffers cover
            ArrowBuffer[] buffers;
            ArrayData[] children = [];
            ArrayData? dictionary = null;

            switch (type)
            {
                case ExtensionType extension:
                {
                    ArrayData storage = Data(array, extension.StorageType);
                    return new ArrayData(extension, storage.Length, storage.NullCount, storage.Offset, storage.Buffers, storage.Children, storage.Dictionary);
                }
                case NullType:
                    // No buffers by the spec; the engine (arrow2's export) gives one, absent validity: ignored as Apache.Arrow does.
                    if (array->BufferCount > 1 || array->ChildCount != 0) Expect(array, type, 0, 0);
                    buffers = [];
                    break;
                case BooleanType:
                    Expect(array, type, 2, 0);
                    buffers = [Validity(array, end), Buffer(array, 1, Bits(end))];
                    break;
                case DictionaryType d:
                {
                    Expect(array, type, 2, 0);
                    if (array->Dictionary == null) throw new InvalidDataException("An imported dictionary array has no dictionary.");
                    buffers = [Validity(array, end), Buffer(array, 1, end * Width((FixedWidthType)d.IndexType))];
                    dictionary = Data(array->Dictionary, d.ValueType);
                    break;
                }
                case LargeStringType or LargeBinaryType:
                    Expect(array, type, 3, 0);
                    buffers = [Validity(array, end), Buffer(array, 1, (end + 1) * 8), Buffer(array, 2, LastOffset64(array, end))];
                    break;
                case StringViewType or BinaryViewType:
                {
                    // Validity, views, the variadic data buffers, then the data buffers' sizes (int64 each).
                    if (array->BufferCount < 3 || array->ChildCount != 0)
                        throw new InvalidDataException($"An imported {type.Name} array has {array->BufferCount} buffers and {array->ChildCount} children.");
                    int variadic = checked((int)array->BufferCount - 3);
                    var sizes = (long*)array->Buffers[array->BufferCount - 1];
                    if (variadic > 0 && sizes == null) throw new InvalidDataException($"An imported {type.Name} array has no buffer sizes.");
                    buffers = new ArrowBuffer[2 + variadic];
                    buffers[0] = Validity(array, end);
                    buffers[1] = Buffer(array, 1, end * 16);
                    for (int b = 0; b < variadic; b++) buffers[2 + b] = Buffer(array, 2 + b, sizes[b]);
                    break;
                }
                // (after the large and view kinds, should either derive from these)
                case StringType or BinaryType:
                    Expect(array, type, 3, 0);
                    buffers = [Validity(array, end), Buffer(array, 1, (end + 1) * 4), Buffer(array, 2, LastOffset32(array, end))];
                    break;
                case FixedWidthType fixedWidth: // numbers, dates, times, timestamps, durations, intervals, decimals, fixed size binary
                    Expect(array, type, 2, 0);
                    buffers = [Validity(array, end), Buffer(array, 1, end * Width(fixedWidth))];
                    break;
                case ListType or MapType:
                    Expect(array, type, 2, 1);
                    buffers = [Validity(array, end), Buffer(array, 1, (end + 1) * 4)];
                    children = [Data(array->Children[0], ChildType(type))];
                    break;
                case LargeListType:
                    Expect(array, type, 2, 1);
                    buffers = [Validity(array, end), Buffer(array, 1, (end + 1) * 8)];
                    children = [Data(array->Children[0], ChildType(type))];
                    break;
                case ListViewType:
                    Expect(array, type, 3, 1);
                    buffers = [Validity(array, end), Buffer(array, 1, end * 4), Buffer(array, 2, end * 4)];
                    children = [Data(array->Children[0], ChildType(type))];
                    break;
                case LargeListViewType:
                    Expect(array, type, 3, 1);
                    buffers = [Validity(array, end), Buffer(array, 1, end * 8), Buffer(array, 2, end * 8)];
                    children = [Data(array->Children[0], ChildType(type))];
                    break;
                case FixedSizeListType:
                    Expect(array, type, 1, 1);
                    buffers = [Validity(array, end)];
                    children = [Data(array->Children[0], ChildType(type))];
                    break;
                case StructType s:
                    Expect(array, type, 1, s.Fields.Count);
                    buffers = [Validity(array, end)];
                    children = Children(array, s.Fields);
                    break;
                case UnionType u:
                {
                    // No validity: the type ids (one byte a slot), then for a dense union the offsets (int32).
                    bool dense = u.Mode == UnionMode.Dense;
                    Expect(array, type, dense ? 2 : 1, u.Fields.Count);
                    buffers = dense ? [Buffer(array, 0, end), Buffer(array, 1, end * 4)] : [Buffer(array, 0, end)];
                    children = Children(array, u.Fields);
                    break;
                }
                case RunEndEncodedType r:
                    Expect(array, type, 0, 2);
                    buffers = [];
                    children = [Data(array->Children[0], r.RunEndsDataType), Data(array->Children[1], r.ValuesDataType)];
                    break;
                default:
                    throw new NotSupportedException($"Importing a {type.Name} array is not supported.");
            }
            return new ArrayData(type, (int)length, checked((int)array->NullCount), (int)offset, buffers, children, dictionary);
        }

        private ArrayData[] Children(NativeCArray* array, IReadOnlyList<Field> fields)
        {
            var children = new ArrayData[fields.Count];
            for (int c = 0; c < children.Length; c++) children[c] = Data(array->Children[c], fields[c].DataType);
            return children;
        }

        private static IArrowType ChildType(IArrowType type) => type switch
        {
            MapType m => m.KeyValueType,
            ListType l => l.ValueDataType,
            LargeListType l => l.ValueDataType,
            ListViewType l => l.ValueDataType,
            LargeListViewType l => l.ValueDataType,
            FixedSizeListType f => f.ValueDataType,
            _ => throw new InvalidOperationException($"A {type.Name} array has no single child."),
        };

        private static void Expect(NativeCArray* array, IArrowType type, int buffers, int children)
        {
            if (array->BufferCount != buffers || array->ChildCount != children)
                throw new InvalidDataException(
                    $"An imported {type.Name} array has {array->BufferCount} buffers and {array->ChildCount} children; its layout has {buffers} and {children}.");
        }

        private static long Bits(long slots) => (slots + 7) / 8;

        private static long Width(FixedWidthType type) => type.BitWidth >= 8 ? type.BitWidth / 8
            : throw new NotSupportedException($"Importing a {type.Name} array of {type.BitWidth} bits a value is not supported.");

        private static long LastOffset32(NativeCArray* array, long end)
        {
            var offsets = (int*)array->Buffers[1];
            return offsets == null ? 0 : offsets[end];
        }

        private static long LastOffset64(NativeCArray* array, long end)
        {
            var offsets = (long*)array->Buffers[1];
            return offsets == null ? 0 : offsets[end];
        }

        /// <summary>The validity bitmap (absent when the producer left it out: no nulls).</summary>
        private ArrowBuffer Validity(NativeCArray* array, long end) => Buffer(array, 0, Bits(end));

        /// <summary>Buffer <paramref name="index"/> of <paramref name="bytes"/> bytes, over the producer's memory (empty when absent).</summary>
        private ArrowBuffer Buffer(NativeCArray* array, int index, long bytes)
        {
            byte* pointer = array->Buffers[index];
            if (pointer == null) return ArrowBuffer.Empty;
            if (bytes < 0 || bytes > int.MaxValue) throw new InvalidDataException($"An imported buffer has {bytes} bytes.");
            var memory = new NativeBufferMemory(_owner, pointer, (int)bytes);
            _made.Add(memory);
            return NativeBatchExport.NewBuffer(memory);
        }
    }

    /// <summary>An imported array, moved into memory of its own: released (by its producer's callback) with its last buffer.</summary>
    private sealed class ImportedArrayMemory : NativeMemoryOwner
    {
        public ImportedArrayMemory(NativeCArray* source)
        {
            Array = (NativeCArray*)NativeMemory.Alloc((nuint)sizeof(NativeCArray));
            *Array = *source;
            source->Release = null; // moved
        }

        public NativeCArray* Array { get; }

        protected override void FreeMemory()
        {
            if (Array->Release != null) Array->Release(Array);
            NativeMemory.Free(Array);
        }
    }
}

/// <summary>
/// An Arrow C stream (from the engine) read batch by batch with <see cref="ArrowCImport"/>. The stream is moved in;
/// disposing the reader releases it (a finalizer does as a fallback). Not thread-safe.
/// </summary>
internal sealed unsafe class ArrowCStreamReader : IArrowArrayStream
{
    private CArrowArrayStream* _stream;
    private Schema? _schema;

    /// <summary>Takes the stream (leaving <paramref name="source"/> released).</summary>
    public ArrowCStreamReader(CArrowArrayStream* source)
    {
        if (source->release == null) throw new ArgumentException("The stream is released already.", nameof(source));
        _stream = (CArrowArrayStream*)NativeMemory.Alloc((nuint)sizeof(CArrowArrayStream));
        *_stream = *source;
        source->release = null; // moved
    }

    public ArrowCStreamReader(Apache.Arrow.C.CArrowArrayStream* source) : this((CArrowArrayStream*)source)
    {
    }

    public Schema Schema => _schema ??= ReadSchema();

    private Schema ReadSchema()
    {
        CArrowArrayStream* stream = Stream();
        NativeCSchema schema = default;
        Check(stream->get_schema(stream, (CArrowSchema*)&schema));
        try
        {
            return CArrowSchemaImporter.ImportSchema((CArrowSchema*)&schema);
        }
        finally
        {
            if (schema.Release != null) schema.Release(&schema);
        }
    }

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Schema schema = Schema;
        CArrowArrayStream* stream = Stream();
        NativeCArray array = default;
        Check(stream->get_next(stream, (CArrowArray*)&array));
        if (array.Release == null) return new((RecordBatch?)null); // the end
        try
        {
            return new(ArrowCImport.ImportRecordBatch((CArrowArray*)&array, schema));
        }
        finally
        {
            if (array.Release != null) array.Release(&array); // not moved: the import failed before taking it
        }
    }

    /// <summary>
    /// Moves the C stream on to another consumer (the engine reading it back, no batch imported here): from where the
    /// reader is, which then has nothing left to read.
    /// </summary>
    /// <exception cref="InvalidOperationException">The stream was moved on or released already.</exception>
    public void MoveTo(CArrowArrayStream* destination)
    {
        if (_stream == null || _stream->release == null) throw new InvalidOperationException("The Arrow stream was consumed already.");
        *destination = *_stream;
        _stream->release = null; // moved: disposing the reader frees only its struct
    }

    private CArrowArrayStream* Stream() => _stream != null && _stream->release != null ? _stream
        : _stream == null ? throw new ObjectDisposedException(nameof(ArrowCStreamReader))
        : throw new InvalidOperationException("The Arrow stream was moved on to another consumer.");

    private void Check(int status)
    {
        if (status == 0) return;
        byte* message = _stream->get_last_error(_stream);
        throw new IOException(message == null
            ? $"The Arrow stream failed (error {status})."
            : $"The Arrow stream failed (error {status}): {Marshal.PtrToStringUTF8((nint)message)}");
    }

    public void Dispose()
    {
        Release();
        GC.SuppressFinalize(this);
    }

    ~ArrowCStreamReader() => Release();

    private void Release()
    {
        CArrowArrayStream* stream = _stream;
        if (stream == null) return;
        _stream = null;
        if (stream->release != null) stream->release(stream);
        NativeMemory.Free(stream);
    }
}

/// <summary>The Arrow C data interface <c>ArrowSchema</c> struct (Apache.Arrow keeps its release callback internal).</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeCSchema
{
    public byte* Format;
    public byte* Name;
    public byte* Metadata;
    public long Flags;
    public long ChildCount;
    public NativeCSchema** Children;
    public NativeCSchema* Dictionary;
    public delegate* unmanaged<NativeCSchema*, void> Release;
    public void* PrivateData;
}
