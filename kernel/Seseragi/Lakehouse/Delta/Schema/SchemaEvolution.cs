using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Kaminari.Seseragi.Lakehouse.Delta.Schema;

/// <summary>
/// How data of one schema goes into a table of another, as Spark's Delta writer does it: values cast up implicitly,
/// missing nullable columns filled with null, and with schema evolution new columns added (and, on tables with type
/// widening, column types widened). Columns are matched by name at the top level; inside nested columns the shapes
/// must agree.
/// </summary>
public static class SchemaEvolution
{
    /// <summary>The field metadata recording a column's type changes (type widening).</summary>
    public const string TypeChangesKey = "delta.typeChanges";
    /// <summary>The table property allowing writers to widen column types.</summary>
    public const string EnableTypeWideningKey = "delta.enableTypeWidening";
    /// <summary>The table property holding the largest column id handed out under column mapping.</summary>
    public const string MaxColumnIdKey = "delta.columnMapping.maxColumnId";

    /// <summary>
    /// Whether values of <paramref name="from"/> fit <paramref name="to"/> without loss, so a write casts them
    /// without changing the table: the same type, an integer into a wider integer, float into double, an integer
    /// or decimal into a decimal with room for it.
    /// </summary>
    public static bool CastsImplicitly(DeltaType from, DeltaType to)
    {
        if (from.ToString() == to.ToString()) return true;
        return (from, to) switch
        {
            (PrimitiveType f, PrimitiveType t) when IntegerRank(f) is int a && IntegerRank(t) is int b => a < b,
            (PrimitiveType { Name: PrimitiveType.Float }, PrimitiveType { Name: PrimitiveType.Double }) => true,
            (PrimitiveType f, DecimalType t) when IntegerDigits(f) is int digits => t.Precision - t.Scale >= digits,
            (DecimalType f, DecimalType t) => t.Scale >= f.Scale && t.Precision - t.Scale >= f.Precision - f.Scale,
            _ => false,
        };
    }

    /// <summary>
    /// Whether a column of <paramref name="from"/> may become <paramref name="to"/> under type widening: the
    /// changes the protocol lists (integers to wider integers or double or a wide enough decimal, float to double,
    /// date to timestamp_ntz, decimals to wider decimals).
    /// </summary>
    public static bool CanWiden(DeltaType from, DeltaType to)
    {
        if (from.ToString() == to.ToString()) return false;
        return (from, to) switch
        {
            (PrimitiveType { Name: PrimitiveType.Date }, PrimitiveType { Name: PrimitiveType.TimestampNtz }) => true,
            (PrimitiveType f, PrimitiveType { Name: PrimitiveType.Double }) when IntegerRank(f) is int r => r <= 2, // byte, short, int
            _ => CastsImplicitly(from, to),
        };
    }

    private static int? IntegerRank(PrimitiveType t) => t.Name switch
    {
        PrimitiveType.Byte => 0, PrimitiveType.Short => 1, PrimitiveType.Integer => 2, PrimitiveType.Long => 3, _ => null,
    };

    private static int? IntegerDigits(PrimitiveType t) => t.Name switch
    {
        PrimitiveType.Byte => 3, PrimitiveType.Short => 5, PrimitiveType.Integer => 10, PrimitiveType.Long => 20, _ => null,
    };

    /// <summary>
    /// The table schema after writing data of <paramref name="incoming"/> into it, and the metadata to commit
    /// when it changes (null when it does not). Without <paramref name="evolve"/> the schema never changes: data
    /// must fit it (columns it lacks are filled with null when nullable).
    /// </summary>
    /// <exception cref="DeltaException">The data does not fit, or evolving would need what the table does not allow.</exception>
    /// <param name="metadata">The table's metadata.</param>
    /// <param name="protocol">The table's protocol.</param>
    /// <param name="incoming">The schema of the data.</param>
    /// <param name="evolve">Add new columns and (with type widening on) widen column types.</param>
    /// <param name="changed">The metadata to commit, or null when the schema stays.</param>
    /// <param name="allowMissingColumns">Columns the data lacks are fine even when NOT NULL (a merge keeps the
    /// target's values for them).</param>
    public static StructType Merge(
        MetadataAction metadata, ProtocolAction protocol, StructType incoming, bool evolve, out MetadataAction? changed,
        bool allowMissingColumns = false)
    {
        StructType table = StructType.Parse(metadata.SchemaString);
        bool widening = WideningEnabled(metadata, protocol);
        ColumnMappingMode mapping = TableFeatures.ColumnMappingModeOf(metadata);
        int maxId = MaxColumnId(metadata, table);

        var context = new MergeContext(evolve, widening, mapping, allowMissingColumns, maxId);
        var merged = (StructType)MergeStruct(table, incoming, context, path: null, topLevel: true);
        bool schemaChanged = context.Changed;
        maxId = context.MaxId;
        if (schemaChanged) EnsureSupported(protocol, merged);
        changed = schemaChanged ? WithSchema(metadata, merged, mapping == ColumnMappingMode.None ? null : maxId) : null;
        return merged;
    }


    private sealed class MergeContext(bool evolve, bool widening, ColumnMappingMode mapping, bool allowMissing, int maxId)
    {
        public bool Evolve { get; } = evolve;
        public bool Widening { get; } = widening;
        public ColumnMappingMode Mapping { get; } = mapping;
        public bool AllowMissing { get; } = allowMissing;
        public int MaxId = maxId;
        public bool Changed;
    }

    /// <summary>
    /// Merges the data's struct into the table's, field by field and at any depth (inside structs, array elements
    /// and map values), as Spark's mergeSchema: fields the data lacks stay (null), new ones are added at the end,
    /// types cast up or widen.
    /// </summary>
    private static DeltaType MergeStruct(StructType table, StructType incoming, MergeContext context, string? path, bool topLevel)
    {
        var fields = new List<StructField>();
        foreach (StructField field in table.Fields)
        {
            string name = path is null ? field.Name : $"{path}.{field.Name}";
            StructField? given = incoming.Field(field.Name);
            if (given is null)
            {
                if (!field.Nullable && !context.AllowMissing)
                    throw new DeltaException($"The data has no column '{name}', which the table declares NOT NULL.");
                fields.Add(field); // filled with null
                continue;
            }
            fields.Add(MergeField(field, given.Type, context, name, topLevel));
        }

        StructField[] added = [.. incoming.Fields.Where(f => table.Field(f.Name) is null)];
        if (added.Length > 0)
        {
            string names = string.Join(", ", added.Select(f => path is null ? f.Name : $"{path}.{f.Name}"));
            if (!context.Evolve)
                throw new DeltaException($"Schema mismatch: the data has columns the table does not: {names}. Allow schema evolution to add them.");
            foreach (StructField field in added)
            {
                var plain = new StructField { Name = field.Name, Type = Nullable(field.Type), Nullable = true };
                fields.Add(context.Mapping == ColumnMappingMode.None ? plain : Mapped(plain, ref context.MaxId));
            }
            context.Changed = true;
        }
        return new StructType(fields);
    }

    private static StructField MergeField(StructField field, DeltaType given, MergeContext context, string name, bool topLevel)
    {
        _ = topLevel;
        // Widenings inside the field's arrays and maps (not inside its structs, whose fields keep their own
        // history) are recorded on the field, with the path to the element, key or value (as Spark records them).
        var contained = new List<(string Path, DeltaType From, DeltaType To)>();
        DeltaType? nested = MergeNested(field.Type, given, context, name, contained, relative: null);
        if (nested is not null)
        {
            if (ReferenceEquals(nested, field.Type)) return field;
            StructField result = With(field, nested);
            foreach (var (path, from, to) in contained) result = WithTypeChange(result, from, to, path);
            return result;
        }
        if (CastsImplicitly(given, field.Type)) return field;
        if (context.Evolve && context.Widening && CanWiden(field.Type, given))
        {
            context.Changed = true;
            return Widened(field, given, fieldPath: null);
        }
        throw new DeltaException(
            $"The column '{name}' is {field.Type} in the table but {given} in the data" +
            (CanWiden(field.Type, given)
                ? context.Evolve ? "; widening it needs type widening on the table (delta.enableTypeWidening)." : "; allow schema evolution to widen it."
                : "."));
    }

    /// <summary>The merged type when both are structs, arrays or maps; null for other types.</summary>
    /// <param name="contained">Collects widenings of array elements and map keys and values, by their path.</param>
    /// <param name="relative">The path of <paramref name="table"/> below the nearest struct field (null: the field itself).</param>
    private static DeltaType? MergeNested(
        DeltaType table, DeltaType given, MergeContext context, string name, List<(string Path, DeltaType From, DeltaType To)> contained, string? relative)
    {
        switch (table, given)
        {
            case (StructType t, StructType g):
            {
                bool before = context.Changed;
                context.Changed = false;
                var merged = (StructType)MergeStruct(t, g, context, name, topLevel: false);
                bool changed = context.Changed;
                context.Changed = before || changed;
                return changed ? merged : table;
            }
            case (ArrayType t, ArrayType g):
            {
                DeltaType element = MergeContainedType(t.ElementType, g.ElementType, context, $"{name}.element", contained, Join(relative, "element"));
                return ReferenceEquals(element, t.ElementType) ? table : new ArrayType(element, t.ContainsNull);
            }
            case (MapType t, MapType g):
            {
                DeltaType key = MergeContainedType(t.KeyType, g.KeyType, context, $"{name}.key", contained, Join(relative, "key"));
                DeltaType value = MergeContainedType(t.ValueType, g.ValueType, context, $"{name}.value", contained, Join(relative, "value"));
                return ReferenceEquals(key, t.KeyType) && ReferenceEquals(value, t.ValueType) ? table : new MapType(key, value, t.ValueContainsNull);
            }
            case (StructType or ArrayType or MapType, _) or (_, StructType or ArrayType or MapType):
                throw new DeltaException($"The column '{name}' is {table} in the table but {given} in the data.");
            default:
                return null;
        }
    }

    /// <summary>An array element or a map key or value: merged when nested, else cast implicitly or widened.</summary>
    private static DeltaType MergeContainedType(
        DeltaType table, DeltaType given, MergeContext context, string name, List<(string Path, DeltaType From, DeltaType To)> contained, string relative)
    {
        // A struct below resets the path: its fields record their own changes.
        DeltaType? nested = table is StructType && given is StructType
            ? MergeNested(table, given, context, name, [], null)
            : MergeNested(table, given, context, name, contained, relative);
        if (nested is not null) return nested;
        if (CastsImplicitly(given, table)) return table;
        if (context.Evolve && context.Widening && CanWiden(table, given))
        {
            contained.Add((relative, table, given));
            context.Changed = true;
            return given;
        }
        throw new DeltaException(
            $"'{name}' is {table} in the table but {given} in the data" +
            (CanWiden(table, given)
                ? context.Evolve ? "; widening it needs type widening on the table (delta.enableTypeWidening)." : "; allow schema evolution to widen it."
                : "."));
    }

    private static string Join(string? path, string step) => path is null ? step : $"{path}.{step}";

    /// <summary>A type with every nested field made nullable (a field added to a table holds null in older rows).</summary>
    private static DeltaType Nullable(DeltaType type) => type switch
    {
        StructType s => new StructType([.. s.Fields.Select(f => new StructField
        {
            Name = f.Name, Type = Nullable(f.Type), Nullable = true, Metadata = f.Metadata, JsonMetadataKeys = f.JsonMetadataKeys,
        })]),
        ArrayType a => new ArrayType(Nullable(a.ElementType), true),
        MapType m => new MapType(m.KeyType, Nullable(m.ValueType), true),
        _ => type,
    };

    private static StructField With(StructField field, DeltaType type) => new()
    {
        Name = field.Name, Type = type, Nullable = field.Nullable, Metadata = field.Metadata, JsonMetadataKeys = field.JsonMetadataKeys,
    };

    /// <summary>Whether the table lets writers widen column types: the property is on and the protocol has the feature.</summary>
    public static bool WideningEnabled(MetadataAction metadata, ProtocolAction protocol) =>
        IsOn(metadata, EnableTypeWideningKey)
        && TableFeatures.WriterFeaturesOf(protocol).Any(f => f is TableFeatures.TypeWidening or TableFeatures.TypeWideningPreview);

    /// <summary>Refuses a schema the table's protocol cannot hold (timestamps without time zone need their feature).</summary>
    /// <exception cref="DeltaException">The schema needs a table feature the protocol does not have.</exception>
    public static void EnsureSupported(ProtocolAction protocol, StructType schema)
    {
        if (HasNtz(schema) && !TableFeatures.WriterFeaturesOf(protocol).Contains(TableFeatures.TimestampNtz))
            throw new DeltaException(
                "The schema holds timestamps without time zone, which the table's protocol does not support (table feature timestampNtz).");

        static bool HasNtz(DeltaType type) => type switch
        {
            PrimitiveType { Name: PrimitiveType.TimestampNtz } => true,
            StructType s => s.Fields.Any(f => HasNtz(f.Type)),
            ArrayType a => HasNtz(a.ElementType),
            MapType m => HasNtz(m.KeyType) || HasNtz(m.ValueType),
            _ => false,
        };
    }

    /// <summary>
    /// Metadata whose schema is <paramref name="schema"/> outright (overwriting with a new schema): under column
    /// mapping, every column gets a new id and physical name, as Spark assigns them.
    /// </summary>
    public static MetadataAction Replace(MetadataAction metadata, StructType schema, IReadOnlyList<string> partitionColumns)
    {
        ColumnMappingMode mapping = TableFeatures.ColumnMappingModeOf(metadata);
        int maxId = MaxColumnId(metadata, StructType.Parse(metadata.SchemaString));
        StructType fresh = mapping == ColumnMappingMode.None
            ? schema
            : new StructType([.. schema.Fields.Select(f => Mapped(Unmapped(f), ref maxId))]);
        MetadataAction replaced = WithSchema(metadata, fresh, mapping == ColumnMappingMode.None ? null : maxId);
        return new MetadataAction
        {
            Id = replaced.Id, Name = replaced.Name, Description = replaced.Description, FormatProvider = replaced.FormatProvider,
            FormatOptions = replaced.FormatOptions, SchemaString = replaced.SchemaString, PartitionColumns = partitionColumns,
            Configuration = replaced.Configuration, CreatedTime = replaced.CreatedTime,
        };
    }

    /// <summary>
    /// Metadata of a table switching from no column mapping to mapping by name: every field, nested ones included,
    /// gets an id, and its physical name is its current name, since the data files already hold it under that name
    /// (as Spark does). The mode property itself is the caller's to set.
    /// </summary>
    public static MetadataAction WithNameMapping(MetadataAction metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        int maxId = MaxColumnId(metadata, StructType.Parse(metadata.SchemaString));
        var schema = (StructType)Named(StructType.Parse(metadata.SchemaString), ref maxId);
        return WithSchema(metadata, schema, maxId);

        static DeltaType Named(DeltaType type, ref int maxId)
        {
            switch (type)
            {
                case StructType s:
                    var fields = new List<StructField>(s.Fields.Count);
                    foreach (StructField field in s.Fields)
                    {
                        var fieldMetadata = new Dictionary<string, string>(field.Metadata);
                        if (!fieldMetadata.ContainsKey(StructField.ColumnIdKey))
                            fieldMetadata[StructField.ColumnIdKey] = (++maxId).ToString(CultureInfo.InvariantCulture);
                        fieldMetadata.TryAdd(StructField.PhysicalNameKey, field.Name);
                        fields.Add(new StructField
                        {
                            Name = field.Name, Type = Named(field.Type, ref maxId), Nullable = field.Nullable,
                            Metadata = fieldMetadata, JsonMetadataKeys = field.JsonMetadataKeys,
                        });
                    }
                    return new StructType(fields);
                case ArrayType a:
                    return new ArrayType(Named(a.ElementType, ref maxId), a.ContainsNull);
                case MapType m:
                    DeltaType key = Named(m.KeyType, ref maxId);
                    return new MapType(key, Named(m.ValueType, ref maxId), m.ValueContainsNull);
                default:
                    return type;
            }
        }
    }

    private static StructField Widened(StructField field, DeltaType to, string? fieldPath) =>
        With(WithTypeChange(field, field.Type, to, fieldPath), to);

    /// <summary>
    /// The field with <c>{"toType", "fromType"[, "fieldPath"]}</c> appended to its type change history (earlier
    /// entries kept as written); <paramref name="fieldPath"/> names an array element or map key or value inside it
    /// (<c>element</c>, <c>value.element</c>, ...), as Spark writes it.
    /// </summary>
    private static StructField WithTypeChange(StructField field, DeltaType from, DeltaType to, string? fieldPath)
    {
        field.Metadata.TryGetValue(TypeChangesKey, out string? history);
        List<KeyValuePair<string, string>> entry = [new("toType", to.ToString()), new("fromType", from.ToString())];
        if (fieldPath is not null) entry.Add(new("fieldPath", fieldPath));
        string changes = JsonText.Append(history, [.. entry]);
        var metadata = new Dictionary<string, string>(field.Metadata) { [TypeChangesKey] = changes };
        return new StructField
        {
            Name = field.Name, Type = field.Type, Nullable = field.Nullable, Metadata = metadata,
            JsonMetadataKeys = new HashSet<string>(field.JsonMetadataKeys) { TypeChangesKey },
        };
    }

    /// <summary>A new field under column mapping: the next id and a fresh physical name; nested struct fields too.</summary>
    private static StructField Mapped(StructField field, ref int maxId)
    {
        maxId++;
        var metadata = new Dictionary<string, string>(field.Metadata)
        {
            [StructField.ColumnIdKey] = maxId.ToString(CultureInfo.InvariantCulture),
            [StructField.PhysicalNameKey] = $"col-{Guid.NewGuid()}",
        };
        DeltaType type = MappedType(field.Type, ref maxId);
        return new StructField { Name = field.Name, Type = type, Nullable = field.Nullable, Metadata = metadata, JsonMetadataKeys = field.JsonMetadataKeys };
    }

    /// <summary>The struct fields inside a type, at any depth (array elements, map keys and values), mapped in Spark's order.</summary>
    private static DeltaType MappedType(DeltaType type, ref int maxId)
    {
        switch (type)
        {
            case StructType s:
                var inner = new List<StructField>(s.Fields.Count);
                foreach (StructField child in s.Fields) inner.Add(Mapped(child, ref maxId));
                return new StructType(inner);
            case ArrayType a:
                return new ArrayType(MappedType(a.ElementType, ref maxId), a.ContainsNull);
            case MapType m:
                DeltaType key = MappedType(m.KeyType, ref maxId);
                return new MapType(key, MappedType(m.ValueType, ref maxId), m.ValueContainsNull);
            default:
                return type;
        }
    }

    private static StructField Unmapped(StructField field)
    {
        var metadata = field.Metadata.Where(kv => kv.Key is not (StructField.ColumnIdKey or StructField.PhysicalNameKey)).ToDictionary(kv => kv.Key, kv => kv.Value);
        return new StructField { Name = field.Name, Type = UnmappedType(field.Type), Nullable = field.Nullable, Metadata = metadata, JsonMetadataKeys = field.JsonMetadataKeys };
    }

    private static DeltaType UnmappedType(DeltaType type) => type switch
    {
        StructType s => new StructType([.. s.Fields.Select(Unmapped)]),
        ArrayType a => new ArrayType(UnmappedType(a.ElementType), a.ContainsNull),
        MapType m => new MapType(UnmappedType(m.KeyType), UnmappedType(m.ValueType), m.ValueContainsNull),
        _ => type,
    };

    private static int MaxColumnId(MetadataAction metadata, StructType schema)
    {
        int max = metadata.Configuration.TryGetValue(MaxColumnIdKey, out string? text) && int.TryParse(text, out int value) ? value : 0;
        return Math.Max(max, MaxIdIn(schema));

        static int MaxIdIn(DeltaType type) => type switch
        {
            StructType s => s.Fields.Select(f => Math.Max(f.FieldId ?? 0, MaxIdIn(f.Type))).DefaultIfEmpty(0).Max(),
            ArrayType a => MaxIdIn(a.ElementType),
            MapType m => Math.Max(MaxIdIn(m.KeyType), MaxIdIn(m.ValueType)),
            _ => 0,
        };
    }

    private static MetadataAction WithSchema(MetadataAction metadata, StructType schema, int? maxColumnId)
    {
        var configuration = new Dictionary<string, string>(metadata.Configuration);
        if (maxColumnId is int id) configuration[MaxColumnIdKey] = id.ToString(CultureInfo.InvariantCulture);
        return new MetadataAction
        {
            Id = metadata.Id, Name = metadata.Name, Description = metadata.Description, FormatProvider = metadata.FormatProvider,
            FormatOptions = metadata.FormatOptions, SchemaString = schema.ToJson(), PartitionColumns = metadata.PartitionColumns,
            Configuration = configuration, CreatedTime = metadata.CreatedTime,
        };
    }

    private static bool IsOn(MetadataAction metadata, string key) =>
        metadata.Configuration.TryGetValue(key, out string? value) && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
}
