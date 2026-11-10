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

        var fields = new List<StructField>();
        bool schemaChanged = false;
        foreach (StructField field in table.Fields)
        {
            StructField? given = incoming.Field(field.Name);
            if (given is null)
            {
                if (!field.Nullable && !allowMissingColumns)
                    throw new DeltaException($"The data has no column '{field.Name}', which the table declares NOT NULL.");
                fields.Add(field); // filled with null
                continue;
            }
            if (CastsImplicitly(given.Type, field.Type))
            {
                fields.Add(field);
                continue;
            }
            if (evolve && widening && CanWiden(field.Type, given.Type))
            {
                fields.Add(Widened(field, given.Type));
                schemaChanged = true;
                continue;
            }
            throw new DeltaException(
                $"The column '{field.Name}' is {field.Type} in the table but {given.Type} in the data" +
                (CanWiden(field.Type, given.Type)
                    ? evolve ? "; widening it needs type widening on the table (delta.enableTypeWidening)." : "; allow schema evolution to widen it."
                    : "."));
        }

        StructField[] added = [.. incoming.Fields.Where(f => table.Field(f.Name) is null)];
        if (added.Length > 0)
        {
            if (!evolve)
                throw new DeltaException(
                    $"The data has columns the table does not: {string.Join(", ", added.Select(f => f.Name))}. Allow schema evolution to add them.");
            foreach (StructField field in added)
            {
                var plain = new StructField { Name = field.Name, Type = field.Type, Nullable = true };
                fields.Add(mapping == ColumnMappingMode.None ? plain : Mapped(plain, ref maxId));
            }
            schemaChanged = true;
        }

        var merged = new StructType(fields);
        if (schemaChanged) EnsureSupported(protocol, merged);
        changed = schemaChanged ? WithSchema(metadata, merged, mapping == ColumnMappingMode.None ? null : maxId) : null;
        return merged;
    }

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

    private static StructField Widened(StructField field, DeltaType to)
    {
        // Append {"toType", "fromType"} to the field's history, earlier entries kept as written.
        field.Metadata.TryGetValue(TypeChangesKey, out string? history);
        string changes = JsonText.Append(history, [new("toType", to.ToString()), new("fromType", field.Type.ToString())]);
        var metadata = new Dictionary<string, string>(field.Metadata) { [TypeChangesKey] = changes };
        return new StructField
        {
            Name = field.Name, Type = to, Nullable = field.Nullable, Metadata = metadata,
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
        DeltaType type = field.Type;
        if (type is StructType nested)
        {
            var inner = new List<StructField>();
            foreach (StructField child in nested.Fields) inner.Add(Mapped(child, ref maxId));
            type = new StructType(inner);
        }
        return new StructField { Name = field.Name, Type = type, Nullable = field.Nullable, Metadata = metadata, JsonMetadataKeys = field.JsonMetadataKeys };
    }

    private static StructField Unmapped(StructField field)
    {
        var metadata = field.Metadata.Where(kv => kv.Key is not (StructField.ColumnIdKey or StructField.PhysicalNameKey)).ToDictionary(kv => kv.Key, kv => kv.Value);
        DeltaType type = field.Type is StructType s ? new StructType([.. s.Fields.Select(Unmapped)]) : field.Type;
        return new StructField { Name = field.Name, Type = type, Nullable = field.Nullable, Metadata = metadata, JsonMetadataKeys = field.JsonMetadataKeys };
    }

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
