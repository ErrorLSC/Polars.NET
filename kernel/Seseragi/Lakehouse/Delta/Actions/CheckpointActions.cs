using Apache.Arrow;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using ArrowStruct = Apache.Arrow.Types.StructType;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>
/// Reads the actions of checkpoint rows from Arrow batches. A checkpoint row has one struct column per action kind;
/// an action is there when its struct is not null and its key field (an add's path, a protocol's reader version …)
/// is not null: writers fill the columns that do not apply with nulls, or with structs of nulls.
/// </summary>
/// <remarks>
/// Columns and fields may be missing (older writers, other features); a missing one reads as null. Integers are read
/// whatever their width; a field of another type than the protocol's is an error. The fields of each column are
/// resolved once per batch, so a row costs only the reads of its values.
/// </remarks>
internal static class CheckpointActions
{
    /// <summary>The action columns a checkpoint may have, in the order a row is searched.</summary>
    public static readonly string[] Columns = ["sidecar", "checkpointMetadata", "add", "remove", "metaData", "protocol", "txn", "domainMetadata"];

    /// <summary>Appends the actions of every row of <paramref name="batch"/> to <paramref name="actions"/>.</summary>
    /// <param name="batch">Checkpoint rows.</param>
    /// <param name="stats">Turns <c>add.stats_parsed</c> into statistics text when the batch has no <c>add.stats</c>; null to ignore it.</param>
    /// <param name="actions">Receives the actions, in row order.</param>
    /// <exception cref="DeltaException">An action lacks a required field, or a field has the wrong type.</exception>
    public static void Read(RecordBatch batch, ParsedStatsText? stats, List<DeltaAction> actions)
    {
        SidecarColumns? sidecar = Node.Of(batch, "sidecar") is { } s ? new(s) : null;
        CheckpointMetadataColumns? checkpointMetadata = Node.Of(batch, "checkpointMetadata") is { } c ? new(c) : null;
        AddColumns? add = Node.Of(batch, "add") is { } a ? new(a, stats) : null;
        RemoveColumns? remove = Node.Of(batch, "remove") is { } r ? new(r) : null;
        MetadataColumns? metadata = Node.Of(batch, "metaData") is { } m ? new(m) : null;
        ProtocolColumns? protocol = Node.Of(batch, "protocol") is { } p ? new(p) : null;
        TxnColumns? txn = Node.Of(batch, "txn") is { } t ? new(t) : null;
        DomainColumns? domain = Node.Of(batch, "domainMetadata") is { } d ? new(d) : null;

        for (int row = 0; row < batch.Length; row++)
        {
            DeltaAction? action = sidecar?.TryRead(row);
            action ??= checkpointMetadata?.TryRead(row);
            action ??= add?.TryRead(row);
            action ??= remove?.TryRead(row);
            action ??= metadata?.TryRead(row);
            action ??= protocol?.TryRead(row);
            action ??= txn?.TryRead(row);
            action ??= domain?.TryRead(row);
            if (action is not null) actions.Add(action);
        }
    }

    private static DeltaException Missing(string field, string action) =>
        new($"A '{action}' action lacks the required field '{field}'.");

    private static IReadOnlyDictionary<string, string> NonNull(Dictionary<string, string?>? map)
    {
        if (map is null || map.Count == 0) return EmptyMap.Instance;
        var result = new Dictionary<string, string>(map.Count, StringComparer.Ordinal);
        foreach (var (key, value) in map)
        {
            if (value is not null) result[key] = value;
        }
        return result;
    }

    // ------------------------------------------------------------------ actions

    private sealed class SidecarColumns(Node n)
    {
        private readonly Text _path = n.Text("path");
        private readonly Ints _size = n.Ints("sizeInBytes"), _modified = n.Ints("modificationTime");

        public SidecarAction? TryRead(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            return _path.Get(i) is not string path ? null : new SidecarAction
            {
                Path = path,
                SizeInBytes = _size.Get(i) ?? 0,
                ModificationTime = _modified.Get(i) ?? 0,
            };
        }
    }

    private sealed class CheckpointMetadataColumns(Node n)
    {
        private readonly Ints _version = n.Ints("version");

        public CheckpointMetadataAction? TryRead(int row) =>
            !n.IsNull(row) && _version.Get(n.ChildIndex(row)) is long version ? new CheckpointMetadataAction { Version = version } : null;
    }

    private sealed class AddColumns(Node n, ParsedStatsText? statsText)
    {
        private readonly Text _path = n.Text("path"), _stats = n.Text("stats"), _clustering = n.Text("clusteringProvider");
        private readonly Maps _partitionValues = n.Maps("partitionValues"), _tags = n.Maps("tags");
        private readonly Ints _size = n.Ints("size"), _modified = n.Ints("modificationTime"),
            _baseRowId = n.Ints("baseRowId"), _defaultRowCommitVersion = n.Ints("defaultRowCommitVersion");
        private readonly Bools _dataChange = n.Bools("dataChange");
        private readonly DeletionVectorColumns? _deletionVector = n.Struct("deletionVector") is { } dv ? new(dv) : null;
        private readonly StructArray? _statsParsed =
            statsText is not null && !n.Has("stats") && n.Struct("stats_parsed") is { } parsed ? parsed.Array : null;

        public AddAction? TryRead(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            if (_path.Get(i) is not string path) return null;
            return new AddAction
            {
                Path = path,
                PartitionValues = _partitionValues.Get(i) ?? EmptyMap.Nullable,
                Size = _size.Get(i) ?? 0,
                ModificationTime = _modified.Get(i) ?? 0,
                DataChange = _dataChange.Get(i) ?? true,
                Stats = _statsParsed is null ? _stats.Get(i) : statsText!.Text(_statsParsed, i),
                Tags = _tags.Get(i),
                DeletionVector = _deletionVector?.Read(i),
                BaseRowId = _baseRowId.Get(i),
                DefaultRowCommitVersion = _defaultRowCommitVersion.Get(i),
                ClusteringProvider = _clustering.Get(i),
            };
        }
    }

    private sealed class RemoveColumns(Node n)
    {
        private readonly Text _path = n.Text("path");
        private readonly Maps _partitionValues = n.Maps("partitionValues");
        private readonly Ints _deletionTimestamp = n.Ints("deletionTimestamp"), _size = n.Ints("size"),
            _baseRowId = n.Ints("baseRowId"), _defaultRowCommitVersion = n.Ints("defaultRowCommitVersion");
        private readonly Bools _dataChange = n.Bools("dataChange"), _extended = n.Bools("extendedFileMetadata");
        private readonly DeletionVectorColumns? _deletionVector = n.Struct("deletionVector") is { } dv ? new(dv) : null;

        public RemoveAction? TryRead(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            if (_path.Get(i) is not string path) return null;
            return new RemoveAction
            {
                Path = path,
                DeletionTimestamp = _deletionTimestamp.Get(i),
                DataChange = _dataChange.Get(i) ?? true,
                ExtendedFileMetadata = _extended.Get(i) ?? false,
                PartitionValues = _partitionValues.Get(i),
                Size = _size.Get(i),
                DeletionVector = _deletionVector?.Read(i),
                BaseRowId = _baseRowId.Get(i),
                DefaultRowCommitVersion = _defaultRowCommitVersion.Get(i),
            };
        }
    }

    private sealed class DeletionVectorColumns(Node n)
    {
        private readonly Text _storageType = n.Text("storageType"), _pathOrInline = n.Text("pathOrInlineDv");
        private readonly Ints _offset = n.Ints("offset"), _size = n.Ints("sizeInBytes"), _cardinality = n.Ints("cardinality");

        /// <summary>The deletion vector at row <paramref name="row"/> of the struct (its own index); null when null.</summary>
        public DeletionVectorDescriptor? Read(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            return new DeletionVectorDescriptor
            {
                StorageType = _storageType.Get(i) ?? throw Missing("storageType", "deletionVector"),
                PathOrInlineDv = _pathOrInline.Get(i) ?? throw Missing("pathOrInlineDv", "deletionVector"),
                Offset = (int?)_offset.Get(i),
                SizeInBytes = (int)(_size.Get(i) ?? throw Missing("sizeInBytes", "deletionVector")),
                Cardinality = _cardinality.Get(i) ?? throw Missing("cardinality", "deletionVector"),
            };
        }
    }

    private sealed class MetadataColumns(Node n)
    {
        private readonly Text _id = n.Text("id"), _name = n.Text("name"), _description = n.Text("description"), _schema = n.Text("schemaString");
        private readonly StringLists _partitionColumns = n.StringLists("partitionColumns");
        private readonly Maps _configuration = n.Maps("configuration");
        private readonly Ints _created = n.Ints("createdTime");
        private readonly Node? _format = n.Struct("format");

        public MetadataAction? TryRead(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            if (_id.Get(i) is not string id) return null;
            bool hasFormat = _format is not null && !_format.IsNull(i);
            int f = hasFormat ? _format!.ChildIndex(i) : 0;
            return new MetadataAction
            {
                Id = id,
                Name = _name.Get(i),
                Description = _description.Get(i),
                FormatProvider = (hasFormat ? _format!.Text("provider").Get(f) : null) ?? "parquet",
                FormatOptions = NonNull(hasFormat ? _format!.Maps("options").Get(f) : null),
                SchemaString = _schema.Get(i) ?? throw Missing("schemaString", "metaData"),
                PartitionColumns = _partitionColumns.Get(i) ?? [],
                Configuration = NonNull(_configuration.Get(i)),
                CreatedTime = _created.Get(i),
            };
        }
    }

    private sealed class ProtocolColumns(Node n)
    {
        private readonly Ints _reader = n.Ints("minReaderVersion"), _writer = n.Ints("minWriterVersion");
        private readonly StringLists _readerFeatures = n.StringLists("readerFeatures"), _writerFeatures = n.StringLists("writerFeatures");

        public ProtocolAction? TryRead(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            if (_reader.Get(i) is not long reader) return null;
            return new ProtocolAction
            {
                MinReaderVersion = (int)reader,
                MinWriterVersion = (int)(_writer.Get(i) ?? throw Missing("minWriterVersion", "protocol")),
                ReaderFeatures = _readerFeatures.Get(i),
                WriterFeatures = _writerFeatures.Get(i),
            };
        }
    }

    private sealed class TxnColumns(Node n)
    {
        private readonly Text _appId = n.Text("appId");
        private readonly Ints _version = n.Ints("version"), _lastUpdated = n.Ints("lastUpdated");

        public TxnAction? TryRead(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            if (_appId.Get(i) is not string appId) return null;
            return new TxnAction
            {
                AppId = appId,
                Version = _version.Get(i) ?? throw Missing("version", "txn"),
                LastUpdated = _lastUpdated.Get(i),
            };
        }
    }

    private sealed class DomainColumns(Node n)
    {
        private readonly Text _domain = n.Text("domain"), _configuration = n.Text("configuration");
        private readonly Bools _removed = n.Bools("removed");

        public DomainMetadataAction? TryRead(int row)
        {
            if (n.IsNull(row)) return null;
            int i = n.ChildIndex(row);
            if (_domain.Get(i) is not string domain) return null;
            return new DomainMetadataAction
            {
                Domain = domain,
                Configuration = _configuration.Get(i) ?? "",
                Removed = _removed.Get(i) ?? false,
            };
        }
    }

    // ------------------------------------------------------------------ columns

    /// <summary>
    /// A struct column with its fields by name. Rows are the struct's own (logical) indices; its fields, like every
    /// Arrow child array, are indexed from the struct's offset on (<see cref="ChildIndex"/>).
    /// </summary>
    private sealed class Node
    {
        private readonly Dictionary<string, IArrowArray> _fields = new(StringComparer.Ordinal);

        private Node(StructArray array, string name)
        {
            Array = array;
            Name = name;
            var type = (ArrowStruct)array.Data.DataType;
            for (int i = 0; i < type.Fields.Count; i++) _fields.TryAdd(type.Fields[i].Name, array.Fields[i]);
        }

        public StructArray Array { get; }

        public string Name { get; }

        public static Node? Of(RecordBatch batch, string name)
        {
            int index = batch.Schema.GetFieldIndex(name);
            return index < 0 ? null : batch.Column(index) switch
            {
                StructArray s => new Node(s, name),
                IArrowArray other => throw new DeltaException($"The checkpoint column '{name}' is a {other.Data.DataType.Name}, not a struct."),
            };
        }

        public bool IsNull(int row) => Array.IsNull(row);

        public int ChildIndex(int row) => Array.Offset + row;

        public bool Has(string field) => _fields.ContainsKey(field);

        public Node? Struct(string field) => _fields.GetValueOrDefault(field) switch
        {
            null => null,
            StructArray s => new Node(s, $"{Name}.{field}"),
            IArrowArray other => throw WrongType(field, other, "a struct"),
        };

        public Text Text(string field) => new(_fields.GetValueOrDefault(field) switch
        {
            null => null,
            StringArray s => s,
            IArrowArray other => throw WrongType(field, other, "a string"),
        });

        public Ints Ints(string field) => _fields.GetValueOrDefault(field) switch
        {
            null => default,
            Int64Array or Int32Array or Int16Array or Int8Array => new(_fields[field]),
            IArrowArray other => throw WrongType(field, other, "an integer"),
        };

        public Bools Bools(string field) => new(_fields.GetValueOrDefault(field) switch
        {
            null => null,
            BooleanArray b => b,
            IArrowArray other => throw WrongType(field, other, "a boolean"),
        });

        public Maps Maps(string field) => _fields.GetValueOrDefault(field) switch
        {
            null => default,
            MapArray { Keys: StringArray, Values: StringArray } map => new(map, $"{Name}.{field}"),
            IArrowArray other => throw WrongType(field, other, "a map of strings"),
        };

        public StringLists StringLists(string field) => _fields.GetValueOrDefault(field) switch
        {
            null => default,
            ListArray { Values: StringArray } list => new(list, $"{Name}.{field}"),
            IArrowArray other => throw WrongType(field, other, "a list of strings"),
        };

        private DeltaException WrongType(string field, IArrowArray values, string expected) =>
            new($"The checkpoint field '{Name}.{field}' is a {values.Data.DataType.Name}, not {expected}.");
    }

    /// <summary>A string field; a missing field reads as null.</summary>
    private readonly struct Text(StringArray? array)
    {
        public string? Get(int i) => array?.GetString(i);
    }

    /// <summary>A boolean field; a missing field reads as null.</summary>
    private readonly struct Bools(BooleanArray? array)
    {
        public bool? Get(int i) => array?.GetValue(i);
    }

    /// <summary>An integer field of any width; a missing field reads as null.</summary>
    private readonly struct Ints(IArrowArray? array)
    {
        private readonly Int64Array? _long = array as Int64Array;
        private readonly Int32Array? _int = array as Int32Array;

        public long? Get(int i) =>
            _long is not null ? _long.GetValue(i)
            : _int is not null ? _int.GetValue(i)
            : array switch
            {
                Int16Array a => a.GetValue(i),
                Int8Array a => a.GetValue(i),
                _ => null,
            };
    }

    /// <summary>A map of strings (null values kept); a missing field or null map reads as null.</summary>
    private readonly struct Maps(MapArray? map, string name)
    {
        public Dictionary<string, string?>? Get(int i)
        {
            if (map is null || map.IsNull(i)) return null;
            var keys = (StringArray)map.Keys;
            var values = (StringArray)map.Values;
            int start = map.ValueOffsets[i];
            int end = map.ValueOffsets[i + 1];
            int entries = map.KeyValues.Offset;
            var result = new Dictionary<string, string?>(end - start, StringComparer.Ordinal);
            for (int k = start; k < end; k++)
            {
                string key = keys.GetString(entries + k) ?? throw new DeltaException($"The map '{name}' has a null key.");
                result[key] = values.GetString(entries + k);
            }
            return result;
        }
    }

    /// <summary>A list of strings; a missing field or null list reads as null.</summary>
    private readonly struct StringLists(ListArray? list, string name)
    {
        public List<string>? Get(int i)
        {
            if (list is null || list.IsNull(i)) return null;
            var items = (StringArray)list.Values;
            int start = list.ValueOffsets[i];
            int end = list.ValueOffsets[i + 1];
            List<string> result = new(end - start);
            for (int k = start; k < end; k++)
                result.Add(items.GetString(k) ?? throw new DeltaException($"The list '{name}' holds a null."));
            return result;
        }
    }
}
