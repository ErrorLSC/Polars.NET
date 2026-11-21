using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet.Schema;

/// <summary>
/// What a file's columns are, apart from its data: its schema elements and the Arrow schema its writer stored, with
/// what is derived from them once (the schema tree, the mappings per reading options, the public schema). Files of a
/// dataset with the same schema bytes share one (see <see cref="FooterInterner"/>); everything here is read-only once
/// made, and safe to use from any thread.
/// </summary>
internal sealed class SchemaShape(SchemaElement[] elements, string? arrowSchema)
{
    private readonly object _lock = new();
    private ParquetSchemaTree? _tree;
    private Apache.Arrow.Schema? _stored;
    private bool _storedRead;
    private IReadOnlyList<ParquetSchemaElement>? _public;
    private readonly List<(bool Views, bool BinaryAsString, TimeUnit Int96, string? Int96Zone, (Apache.Arrow.Schema, ColumnMapping[]) Mapping)> _mappings = [];

    /// <summary>The schema elements, depth first.</summary>
    public SchemaElement[] Elements => elements;

    /// <summary>The <c>ARROW:schema</c> value (base64), or null.</summary>
    public string? ArrowSchema => arrowSchema;

    /// <summary>The shape of a footer: its dataset's shared one, or one of its own.</summary>
    public static SchemaShape Of(FileMetaData meta) => meta.Shape ?? new SchemaShape(meta.Schema, ArrowMapping.ArrowSchemaValue(meta.KeyValueMetadata));

    /// <summary>The schema tree.</summary>
    /// <exception cref="ParquetFormatException">A leaf has an unknown physical type, or the tree nests too deeply.</exception>
    public ParquetSchemaTree Tree
    {
        get
        {
            lock (_lock) return _tree ??= ParquetSchemaTree.Build(elements);
        }
    }

    private int _leaves = -1;

    /// <summary>The number of leaves (checking the element list: see <see cref="SchemaTree.CountLeaves"/>).</summary>
    /// <exception cref="ParquetFormatException">The element list does not make a tree.</exception>
    public int LeafCount
    {
        get
        {
            lock (_lock)
            {
                if (_leaves < 0) _leaves = SchemaTree.CountLeaves(elements);
                return _leaves;
            }
        }
    }

    /// <summary>The public view of the schema elements.</summary>
    public IReadOnlyList<ParquetSchemaElement> PublicSchema
    {
        get
        {
            lock (_lock) return _public ??= [.. elements.Select(ParquetMetadata.SchemaElement)];
        }
    }

    /// <summary>
    /// The columns as a reader with <paramref name="options"/> maps them (the options that shape types: views,
    /// binaries as strings, the INT96 unit), mapped once per shape of options.
    /// </summary>
    /// <exception cref="ParquetFormatException">The schema has a leaf of an unknown physical type.</exception>
    public (Apache.Arrow.Schema Schema, ColumnMapping[] Columns) Mapping(ParquetReadOptions options)
    {
        lock (_lock)
        {
            foreach (var (views, binary, int96, zone, mapping) in _mappings)
            {
                if (views == options.ViewTypes && binary == options.BinaryAsString && int96 == options.Int96Unit && zone == options.Int96Timezone)
                    return mapping;
            }
            if (!_storedRead)
            {
                _stored = ArrowMapping.StoredArrowSchema(arrowSchema);
                _storedRead = true;
            }
            _tree ??= ParquetSchemaTree.Build(elements);
            var mapped = ArrowMapping.Map(_tree, _stored, options);
            _mappings.Add((options.ViewTypes, options.BinaryAsString, options.Int96Unit, options.Int96Timezone, mapped));
            return mapped;
        }
    }
}

/// <summary>
/// Shares what the footers of a dataset's files hold alike: lists of schema elements, of key-value pairs and of column
/// path names, and texts (<c>created_by</c>) are compared by their Thrift bytes and read once, and files of the same
/// elements and stored Arrow schema share one <see cref="SchemaShape"/>. At most <see cref="MaxEntries"/> distinct
/// values of each are kept (<see cref="MaxPaths"/> paths): beyond, footers read their own, so a dataset whose files
/// differ everywhere keeps nothing for them.
/// </summary>
internal sealed class FooterInterner
{
    internal const int MaxEntries = 64;
    internal const int MaxPaths = 4096;

    private readonly object _lock = new();
    private readonly Table<SchemaElement[]> _schemas = new(MaxEntries);
    private readonly Table<KeyValue[]> _keyValues = new(MaxEntries);
    private readonly Table<string[]> _paths = new(MaxPaths);
    private readonly Table<string> _texts = new(MaxEntries);
    private readonly List<SchemaShape> _shapes = [];

    /// <summary>The schema elements of these Thrift list bytes (the value of <c>FileMetaData.schema</c>).</summary>
    public SchemaElement[] Schema(ReadOnlySpan<byte> list) => Intern(_schemas, list, static list => List(list, static (ref ThriftCompactReader r) => r.ReadStructList(SchemaElement.Read)));

    /// <summary>The key-value pairs of these Thrift list bytes (the value of <c>FileMetaData.key_value_metadata</c>).</summary>
    public KeyValue[] KeyValues(ReadOnlySpan<byte> list) => Intern(_keyValues, list, static list => List(list, static (ref ThriftCompactReader r) => r.ReadStructList(KeyValue.Read)));

    /// <summary>The path names of these Thrift list bytes (the value of <c>ColumnMetaData.path_in_schema</c>).</summary>
    public string[] Path(ReadOnlySpan<byte> list) => Intern(_paths, list, static list => List(list, static (ref ThriftCompactReader r) => r.ReadStringList()));

    /// <summary>The text of these string bytes.</summary>
    public string Text(ReadOnlySpan<byte> bytes) => Intern(_texts, bytes, ThriftCompactReader.Decode);

    /// <summary>The shape of files of these elements (shared from <see cref="Schema"/>) and stored Arrow schema.</summary>
    public SchemaShape Shape(SchemaElement[] elements, string? arrowSchema)
    {
        lock (_lock)
        {
            foreach (SchemaShape shape in _shapes)
            {
                if (ReferenceEquals(shape.Elements, elements) && string.Equals(shape.ArrowSchema, arrowSchema, StringComparison.Ordinal)) return shape;
            }
            var made = new SchemaShape(elements, arrowSchema);
            if (_shapes.Count < MaxEntries) _shapes.Add(made);
            return made;
        }
    }

    /// <summary>The number of distinct schema element lists, key-value lists and shapes kept.</summary>
    internal (int Schemas, int KeyValues, int Shapes) Counts
    {
        get
        {
            lock (_lock) return (_schemas.Count, _keyValues.Count, _shapes.Count);
        }
    }

    /// <summary>The number of distinct column paths and texts kept.</summary>
    internal (int Paths, int Texts) TextCounts
    {
        get
        {
            lock (_lock) return (_paths.Count, _texts.Count);
        }
    }

    private delegate T ListReader<T>(ref ThriftCompactReader reader);

    private delegate T BytesReader<T>(ReadOnlySpan<byte> bytes);

    /// <summary>A list read as the footer's own reader reads it: inside the FileMetaData struct.</summary>
    private static T List<T>(ReadOnlySpan<byte> list, ListReader<T> read)
    {
        var reader = new ThriftCompactReader(list);
        reader.EnterStruct();
        return read(ref reader);
    }

    private T Intern<T>(Table<T> table, ReadOnlySpan<byte> bytes, BytesReader<T> read) where T : class
    {
        var hash = new HashCode();
        hash.AddBytes(bytes);
        int h = hash.ToHashCode();
        lock (_lock)
        {
            if (table.Find(h, bytes) is { } known) return known;
        }
        T made = read(bytes); // outside the lock: it may throw (damaged bytes), and others read meanwhile
        lock (_lock)
        {
            if (table.Find(h, bytes) is { } known) return known; // read meanwhile by another file
            table.Add(h, bytes, made);
        }
        return made;
    }

    /// <summary>Values by the hash of their bytes, up to a number of them.</summary>
    private sealed class Table<T>(int max) where T : class
    {
        private readonly Dictionary<int, List<(byte[] Bytes, T Value)>> _byHash = [];

        public int Count { get; private set; }

        public T? Find(int hash, ReadOnlySpan<byte> bytes)
        {
            if (!_byHash.TryGetValue(hash, out var entries)) return null;
            foreach (var (b, value) in entries)
            {
                if (bytes.SequenceEqual(b)) return value;
            }
            return null;
        }

        public void Add(int hash, ReadOnlySpan<byte> bytes, T value)
        {
            if (Count >= max) return;
            if (!_byHash.TryGetValue(hash, out var entries)) _byHash[hash] = entries = [];
            entries.Add((bytes.ToArray(), value));
            Count++;
        }
    }
}
