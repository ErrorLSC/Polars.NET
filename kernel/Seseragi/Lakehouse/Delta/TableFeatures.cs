using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>How the columns of the schema are found in the data files.</summary>
public enum ColumnMappingMode
{
    /// <summary>By their logical name.</summary>
    None,
    /// <summary>By the physical name in the field metadata.</summary>
    Name,
    /// <summary>By the field id in the field metadata; the physical name is the name in files that carry no ids.</summary>
    Id,
}

/// <summary>What this kernel can read, and the check that a table stays within it.</summary>
public static class TableFeatures
{
#pragma warning disable CS1591
    public const string DeletionVectors = "deletionVectors";
    public const string TimestampNtz = "timestampNtz";
    public const string ColumnMapping = "columnMapping";
    public const string VacuumProtocolCheck = "vacuumProtocolCheck";
    public const string V2Checkpoint = "v2Checkpoint";
    public const string AppendOnly = "appendOnly";
    public const string Invariants = "invariants";
    public const string CheckConstraints = "checkConstraints";
    public const string ChangeDataFeed = "changeDataFeed";
    public const string GeneratedColumns = "generatedColumns";
    public const string IdentityColumns = "identityColumns";
    public const string DomainMetadata = "domainMetadata";
    public const string InCommitTimestamp = "inCommitTimestamp";
    public const string TypeWidening = "typeWidening";
    public const string TypeWideningPreview = "typeWidening-preview";
#pragma warning restore CS1591

    /// <summary>The table property that makes a table append-only.</summary>
    public const string AppendOnlyKey = "delta.appendOnly";

    /// <summary>The table property that selects the column mapping mode.</summary>
    public const string ColumnMappingModeKey = "delta.columnMapping.mode";

    /// <summary>The column mapping mode the table metadata selects.</summary>
    /// <exception cref="DeltaProtocolException">The mode is not one the protocol defines.</exception>
    public static ColumnMappingMode ColumnMappingModeOf(MetadataAction metadata)
    {
        if (!metadata.Configuration.TryGetValue(ColumnMappingModeKey, out string? mode)) return ColumnMappingMode.None;
        return mode switch
        {
            "none" => ColumnMappingMode.None,
            "name" => ColumnMappingMode.Name,
            "id" => ColumnMappingMode.Id,
            _ => throw new DeltaProtocolException($"Unknown column mapping mode '{mode}'."),
        };
    }

    /// <summary>The highest reader protocol version this kernel understands.</summary>
    public const int MaxReaderVersion = 3;

    /// <summary>The reader table features this kernel implements.</summary>
    public static IReadOnlySet<string> SupportedReaderFeatures { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        DeletionVectors,
        TimestampNtz,
        VacuumProtocolCheck,
        V2Checkpoint,
    };

    /// <summary>Throws when reading the table needs something this kernel does not implement.</summary>
    /// <exception cref="DeltaProtocolException">The reader version or a reader feature is not supported.</exception>
    public static void EnsureReadable(ProtocolAction protocol, MetadataAction metadata)
    {
        if (protocol.MinReaderVersion > MaxReaderVersion)
            throw new DeltaProtocolException(
                $"The table needs reader version {protocol.MinReaderVersion}; this kernel reads up to version {MaxReaderVersion}.");

        ColumnMappingMode mode = ColumnMappingModeOf(metadata);
        if (mode != ColumnMappingMode.None && protocol.MinReaderVersion < 2)
            throw new DeltaProtocolException("The table sets a column mapping mode but its reader version is below 2.");

        if (protocol.MinReaderVersion == 3)
        {
            foreach (string feature in protocol.ReaderFeatures ?? [])
            {
                if (feature == ColumnMapping) continue; // covered by the mode check above
                if (!SupportedReaderFeatures.Contains(feature))
                    throw new DeltaProtocolException($"The table uses the reader feature '{feature}', which this kernel does not implement.");
            }
        }
    }

    /// <summary>The highest writer protocol version this kernel understands.</summary>
    public const int MaxWriterVersion = 7;

    /// <summary>
    /// The writer features this kernel can honour. Some only while the table does not use what they govern:
    /// <see cref="EnsureWritable"/> checks that.
    /// </summary>
    public static IReadOnlySet<string> SupportedWriterFeatures { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        AppendOnly, Invariants, CheckConstraints, ChangeDataFeed, GeneratedColumns, ColumnMapping, IdentityColumns,
        DeletionVectors, TimestampNtz, DomainMetadata, VacuumProtocolCheck, V2Checkpoint, InCommitTimestamp,
        TypeWidening, TypeWideningPreview,
    };

    /// <summary>The writer features a protocol puts in force: listed for version 7, implied by the older versions.</summary>
    public static IEnumerable<string> WriterFeaturesOf(ProtocolAction protocol)
    {
        ArgumentNullException.ThrowIfNull(protocol);
        if (protocol.MinWriterVersion >= 7) return protocol.WriterFeatures ?? [];
        var implied = new List<string>();
        if (protocol.MinWriterVersion >= 2) implied.AddRange([AppendOnly, Invariants]);
        if (protocol.MinWriterVersion >= 3) implied.Add(CheckConstraints);
        if (protocol.MinWriterVersion >= 4) implied.AddRange([ChangeDataFeed, GeneratedColumns]);
        if (protocol.MinWriterVersion >= 5) implied.Add(ColumnMapping);
        if (protocol.MinWriterVersion >= 6) implied.Add(IdentityColumns);
        return implied;
    }

    /// <summary>
    /// Throws when writing to the table needs something this kernel does not do: an unknown writer feature, or a
    /// supported one the table actually uses in a way the kernel cannot enforce (column invariants, check
    /// constraints, generated or identity columns).
    /// </summary>
    /// <exception cref="DeltaProtocolException">The table cannot be written by this kernel.</exception>
    public static void EnsureWritable(ProtocolAction protocol, MetadataAction metadata)
    {
        ArgumentNullException.ThrowIfNull(protocol);
        ArgumentNullException.ThrowIfNull(metadata);
        EnsureReadable(protocol, metadata);
        if (protocol.MinWriterVersion > MaxWriterVersion)
            throw new DeltaProtocolException(
                $"The table needs writer version {protocol.MinWriterVersion}; this kernel writes up to version {MaxWriterVersion}.");

        StructType schema = StructType.Parse(metadata.SchemaString);
        foreach (string feature in WriterFeaturesOf(protocol))
        {
            if (!SupportedWriterFeatures.Contains(feature))
                throw new DeltaProtocolException($"The table uses the writer feature '{feature}', which this kernel does not implement.");
            string? used = feature switch
            {
                Invariants when AnyField(schema, f => f.Metadata.ContainsKey("delta.invariants")) => "column invariants",
                CheckConstraints when metadata.Configuration.Keys.Any(k => k.StartsWith("delta.constraints.", StringComparison.Ordinal)) => "check constraints",
                GeneratedColumns when AnyField(schema, f => f.Metadata.ContainsKey("delta.generationExpression")) => "generated columns",
                IdentityColumns when AnyField(schema, f => f.Metadata.Keys.Any(k => k.StartsWith("delta.identity.", StringComparison.Ordinal))) => "identity columns",
                _ => null,
            };
            if (used is not null)
                throw new DeltaProtocolException($"The table has {used}, which this kernel cannot enforce on write.");
        }
    }

    private static bool AnyField(StructType schema, Func<StructField, bool> test)
    {
        foreach (StructField field in schema.Fields)
        {
            if (test(field)) return true;
            foreach (StructType nested in Nested(field.Type))
            {
                if (AnyField(nested, test)) return true;
            }
        }
        return false;

        static IEnumerable<StructType> Nested(DeltaType type) => type switch
        {
            StructType s => [s],
            ArrayType a => Nested(a.ElementType),
            MapType m => Nested(m.KeyType).Concat(Nested(m.ValueType)),
            _ => [],
        };
    }
}
