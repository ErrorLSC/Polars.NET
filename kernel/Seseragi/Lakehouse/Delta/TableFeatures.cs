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

    /// <summary>The table property that lets writers mark deleted rows with deletion vectors instead of rewriting files.</summary>
    public const string EnableDeletionVectorsKey = "delta.enableDeletionVectors";

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
        // Files written before a column was widened hold the older type; the engine reads them as the table type,
        // and data skipping allows for statistics in the older type (see Skipping.FileFilter).
        TypeWidening,
        TypeWideningPreview,
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
        TypeWidening, TypeWideningPreview, RowTracking.Feature,
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
    /// constraints, generated columns: they need SQL expressions). Identity columns are checked here to be
    /// well-formed; generating their values is up to the writer (see <see cref="IdentityColumns"/>).
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
                _ => null,
            };
            if (used is not null)
                throw new DeltaProtocolException($"The table has {used}, which this kernel cannot enforce on write.");
        }
        Kaminari.Seseragi.Lakehouse.Delta.IdentityColumns.Of(schema); // malformed identity metadata is refused here
        if (RowTracking.IsEnabled(protocol, metadata) && RowTracking.MaterializedColumns(metadata) is null)
            throw new DeltaProtocolException("The table has row tracking on but names no materialized row id columns.");
    }

    /// <summary>The features that are reader and writer features; every other known feature is a writer feature only.</summary>
    private static readonly HashSet<string> ReaderWriterFeatures = new(StringComparer.Ordinal)
    {
        DeletionVectors, TimestampNtz, ColumnMapping, VacuumProtocolCheck, V2Checkpoint, TypeWidening, TypeWideningPreview,
    };

    /// <summary>Whether a feature is a reader (and writer) feature, not a writer feature only.</summary>
    public static bool IsReaderWriterFeature(string feature) => ReaderWriterFeatures.Contains(feature);

    /// <summary>
    /// The protocol name of a feature this kernel can add to a table, matched without regard to case
    /// (<c>TimestampWithoutTimezone</c> is taken for <c>timestampNtz</c>); null when it is not one.
    /// </summary>
    public static string? SupportedFeatureNamed(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (string.Equals(name, "TimestampWithoutTimezone", StringComparison.OrdinalIgnoreCase)) return TimestampNtz;
        return SupportedWriterFeatures.FirstOrDefault(f => string.Equals(f, name, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>The reader features a protocol puts in force: listed for version 3, column mapping for version 2.</summary>
    public static IEnumerable<string> ReaderFeaturesOf(ProtocolAction protocol)
    {
        ArgumentNullException.ThrowIfNull(protocol);
        if (protocol.MinReaderVersion >= 3) return protocol.ReaderFeatures ?? [];
        return protocol.MinReaderVersion == 2 ? [ColumnMapping] : [];
    }

    /// <summary>
    /// The lowest protocol at or above <paramref name="current"/> that supports <paramref name="features"/>, as Spark
    /// upgrades a table: a feature the current versions do not put in force takes the writer to version 7, every
    /// feature the old version implied listed; a reader feature takes the reader to version 3, except column mapping,
    /// for which version 2 suffices. Returns <paramref name="current"/> itself when it already supports them all.
    /// </summary>
    public static ProtocolAction WithFeatures(ProtocolAction current, IEnumerable<string> features)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(features);
        var writerNow = WriterFeaturesOf(current).ToList();
        var readerNow = ReaderFeaturesOf(current).ToList();
        var newWriter = new List<string>();
        var newReader = new List<string>();
        // Features that need another one bring it in first (row tracking keeps its high water mark in a domain).
        IEnumerable<string> withDependencies = features.SelectMany(f => f == RowTracking.Feature ? new[] { DomainMetadata, f } : [f]);
        foreach (string feature in withDependencies.Distinct(StringComparer.Ordinal))
        {
            if (!writerNow.Contains(feature, StringComparer.Ordinal)) newWriter.Add(feature);
            if (IsReaderWriterFeature(feature) && !readerNow.Contains(feature, StringComparer.Ordinal)) newReader.Add(feature);
        }
        if (newWriter.Count == 0 && newReader.Count == 0) return current;

        int readerVersion = current.MinReaderVersion;
        IReadOnlyList<string>? readerFeatures = current.ReaderFeatures;
        if (newReader.Count > 0)
        {
            if (current.MinReaderVersion < 3 && newReader.All(f => f == ColumnMapping))
            {
                readerVersion = Math.Max(current.MinReaderVersion, 2);
            }
            else
            {
                readerVersion = 3;
                readerFeatures = [.. readerNow, .. newReader];
            }
        }
        return new ProtocolAction
        {
            MinReaderVersion = readerVersion,
            MinWriterVersion = 7,
            ReaderFeatures = readerVersion >= 3 ? readerFeatures ?? [] : null,
            WriterFeatures = [.. writerNow, .. newWriter],
        };
    }

    /// <summary>
    /// <paramref name="current"/> raised to at least the given versions (the <c>delta.minReaderVersion</c> and
    /// <c>delta.minWriterVersion</c> table properties); never lowered. Raising to the table-feature versions lists
    /// the features the old versions implied. Returns <paramref name="current"/> itself when nothing is raised.
    /// </summary>
    /// <exception cref="DeltaProtocolException">A version is above what this kernel understands.</exception>
    public static ProtocolAction WithVersions(ProtocolAction current, int? readerVersion, int? writerVersion)
    {
        ArgumentNullException.ThrowIfNull(current);
        if (readerVersion > MaxReaderVersion)
            throw new DeltaProtocolException($"The reader version {readerVersion} is above {MaxReaderVersion}, the highest this kernel reads.");
        if (writerVersion > MaxWriterVersion)
            throw new DeltaProtocolException($"The writer version {writerVersion} is above {MaxWriterVersion}, the highest this kernel writes.");
        int reader = Math.Max(current.MinReaderVersion, readerVersion ?? 0);
        int writer = Math.Max(current.MinWriterVersion, writerVersion ?? 0);
        if (reader == 3) writer = 7; // reader features need writer features
        if (reader == current.MinReaderVersion && writer == current.MinWriterVersion) return current;
        return new ProtocolAction
        {
            MinReaderVersion = reader,
            MinWriterVersion = writer,
            ReaderFeatures = reader >= 3 ? [.. ReaderFeaturesOf(current)] : null,
            WriterFeatures = writer >= 7 ? [.. WriterFeaturesOf(current)] : null,
        };
    }

    /// <summary>
    /// The lowest protocol supporting everything <paramref name="a"/> and <paramref name="b"/> support: the higher
    /// versions, and when features are listed, the features both put in force.
    /// </summary>
    public static ProtocolAction Union(ProtocolAction a, ProtocolAction b)
    {
        ArgumentNullException.ThrowIfNull(a);
        ArgumentNullException.ThrowIfNull(b);
        ProtocolAction raised = WithVersions(a, b.MinReaderVersion, b.MinWriterVersion);
        // Features b lists; for legacy versions, the versions above carry what they imply.
        IEnumerable<string> features = b.MinWriterVersion >= 7 ? WriterFeaturesOf(b).Concat(ReaderFeaturesOf(b)) : [];
        return WithFeatures(raised, features);
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
