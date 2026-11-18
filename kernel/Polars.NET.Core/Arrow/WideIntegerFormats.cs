using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Data;

namespace Polars.NET.Core.Arrow;

/// <summary>
/// 128-bit integer columns between the engine and Apache.Arrow, which has no type for them (<see cref="WideIntegers"/>:
/// decimal(38, 0) columns marked by field metadata, the same 16 bytes). Into the engine: while it imports an exported
/// schema, the format of each marked field reads <c>_pli128</c> / <c>_plu128</c> (the engine's names), then the
/// exporter's own format is put back (the engine only borrows the schema; its release frees the exporter's strings).
/// Out of the engine: before Apache.Arrow imports the engine's schema, those formats are made <c>d:38,0</c>, and the
/// fields imported get the marker. A schema the engine takes (a stream's) is
/// patched for good and its release wrapped: the formats are put back before the exporter's release frees them.
/// </summary>
internal static unsafe class WideIntegerFormats
{
    private static ReadOnlySpan<byte> Signed => "_pli128\0"u8;
    private static ReadOnlySpan<byte> Unsigned => "_plu128\0"u8;

    /// <summary>A schema exported from <paramref name="schema"/> (a struct whose children are its fields), patched.</summary>
    public static Patched PatchSchema(Schema schema, CArrowSchema* exported)
    {
        var patched = new Patched();
        if (!schema.FieldsList.Any(Marks)) return patched;
        for (int i = 0; i < schema.FieldsList.Count; i++) patched.Field(schema.FieldsList[i], exported->children[i]);
        return patched;
    }

    /// <summary>A schema exported from the type <paramref name="type"/> (its nested fields), patched.</summary>
    public static Patched PatchType(IArrowType type, CArrowSchema* exported)
    {
        var patched = new Patched();
        if (Nested(type).Any(Marks)) patched.Type(type, exported);
        return patched;
    }

    /// <summary>
    /// Patches a schema exported from <paramref name="schema"/> that the engine takes and releases (a stream's): the
    /// root's release first puts the exporter's formats back, then runs the exporter's release.
    /// </summary>
    public static void PatchOwnedSchema(Schema schema, CArrowSchema* exported)
    {
        Patched patched = PatchSchema(schema, exported);
        if (!patched.Any) return;
        var root = (NativeCSchema*)exported; // the C schema's own layout (Apache.Arrow keeps its release internal)
        var owner = new Owner(patched, (nint)root->Release, (nint)root->PrivateData);
        root->PrivateData = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(owner));
        root->Release = &ReleasePatched;
    }

    private sealed record Owner(Patched Patched, nint Release, nint PrivateData);

    [UnmanagedCallersOnly]
    private static void ReleasePatched(NativeCSchema* schema)
    {
        GCHandle handle = GCHandle.FromIntPtr((nint)schema->PrivateData);
        var owner = (Owner)handle.Target!;
        handle.Free();
        owner.Patched.Dispose(); // the exporter's formats back, for its release to free
        schema->PrivateData = (void*)owner.PrivateData;
        schema->Release = (delegate* unmanaged<NativeCSchema*, void>)owner.Release;
        if (schema->Release != null) schema->Release(schema);
    }

    private static ReadOnlySpan<byte> Decimal => "d:38,0\0"u8;

    /// <summary>
    /// Imports a schema the engine exported (Apache.Arrow's importer, which releases it), its 128-bit integer columns as
    /// marked decimals: their formats are replaced beforehand (the engine frees its strings from its private data, never
    /// through the formats, so nothing is put back) and their places noted by path, the C schema not read afterwards.
    /// </summary>
    public static Schema ImportSchema(CArrowSchema* exported)
    {
        var wide = new Dictionary<string, string>();
        for (long i = 0; i < exported->n_children; i++) Engine(exported->children[i], i.ToString(System.Globalization.CultureInfo.InvariantCulture), wide);
        Schema schema = CArrowSchemaImporter.ImportSchema(exported);
        if (wide.Count == 0) return schema;
        return new Schema([.. schema.FieldsList.Select((f, i) => Marked(f, i.ToString(System.Globalization.CultureInfo.InvariantCulture), wide))], schema.Metadata);
    }

    /// <summary>Imports a field the engine exported, its 128-bit integer columns as marked decimals (as <see cref="ImportSchema"/>).</summary>
    public static Field ImportField(CArrowSchema* exported)
    {
        var wide = new Dictionary<string, string>();
        Engine(exported, "", wide);
        Field field = CArrowSchemaImporter.ImportField(exported);
        return wide.Count == 0 ? field : Marked(field, "", wide);
    }

    /// <summary>Gives the engine's 128-bit integer formats at and under <paramref name="node"/> the decimal format, noting their paths.</summary>
    private static void Engine(CArrowSchema* node, string path, Dictionary<string, string> wide)
    {
        ReadOnlySpan<byte> format = MemoryMarshal.CreateReadOnlySpanFromNullTerminated(node->format);
        string? kind = format.SequenceEqual(Signed[..^1]) ? WideIntegers.Signed : format.SequenceEqual(Unsigned[..^1]) ? WideIntegers.Unsigned : null;
        if (kind is not null)
        {
            wide[path] = kind;
            node->format = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(Decimal));
            return;
        }
        for (long i = 0; i < node->n_children; i++) Engine(node->children[i], $"{path}/{i}", wide);
    }

    /// <summary>The field imported at <paramref name="path"/> with the marker where the engine had a 128-bit integer.</summary>
    private static Field Marked(Field field, string path, Dictionary<string, string> wide)
    {
        if (wide.TryGetValue(path, out string? kind))
        {
            Dictionary<string, string> metadata = field.Metadata is { } m ? new(m) : [];
            metadata[WideIntegers.MetadataKey] = kind;
            return new Field(field.Name, field.DataType, field.IsNullable, metadata);
        }
        IArrowType type = field.DataType switch
        {
            StructType s => new StructType([.. s.Fields.Select((c, i) => Marked(c, $"{path}/{i}", wide))]),
            ListType l => new ListType(Marked(l.ValueField, path + "/0", wide)),
            LargeListType l => new LargeListType(Marked(l.ValueField, path + "/0", wide)),
            FixedSizeListType l => new FixedSizeListType(Marked(l.ValueField, path + "/0", wide), l.ListSize),
            MapType m => new MapType(Marked(m.KeyField, path + "/0/0", wide), Marked(m.ValueField, path + "/0/1", wide), m.KeySorted),
            _ => field.DataType,
        };
        return ReferenceEquals(type, field.DataType) ? field : new Field(field.Name, type, field.IsNullable, field.Metadata);
    }

    private static bool Marks(Field field) =>
        field.Metadata?.ContainsKey(WideIntegers.MetadataKey) == true || Nested(field.DataType).Any(Marks);

    private static IEnumerable<Field> Nested(IArrowType type) => type switch
    {
        StructType s => s.Fields,
        ListType l => [l.ValueField],
        LargeListType l => [l.ValueField],
        FixedSizeListType l => [l.ValueField],
        MapType m => [m.KeyField, m.ValueField],
        _ => [],
    };

    /// <summary>The formats replaced, put back by <see cref="Dispose"/>.</summary>
    public sealed class Patched : IDisposable
    {
        private List<(nint Node, nint Format)>? _replaced;

        /// <summary>Whether a format was replaced.</summary>
        public bool Any => _replaced is not null;

        internal void Field(Field field, CArrowSchema* node)
        {
            if (WideIntegers.KindOf(field) is { } kind)
            {
                (_replaced ??= []).Add(((nint)node, (nint)node->format));
                node->format = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(kind == WideIntegers.Unsigned ? Unsigned : Signed));
                return;
            }
            Type(field.DataType, node);
        }

        internal void Type(IArrowType type, CArrowSchema* node)
        {
            switch (type)
            {
                case StructType s:
                    for (int i = 0; i < s.Fields.Count; i++) Field(s.Fields[i], node->children[i]);
                    break;
                case ListType or LargeListType or FixedSizeListType:
                    Field(Nested(type).Single(), node->children[0]);
                    break;
                case MapType m:
                    CArrowSchema* entries = node->children[0];
                    Field(m.KeyField, entries->children[0]);
                    Field(m.ValueField, entries->children[1]);
                    break;
            }
        }

        public void Dispose()
        {
            if (_replaced is null) return;
            foreach (var (node, format) in _replaced) ((CArrowSchema*)node)->format = (byte*)format;
            _replaced = null;
        }
    }
}
