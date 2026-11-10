using Kaminari.Seseragi.Delta.Actions;

namespace Kaminari.Seseragi.Delta;

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
#pragma warning restore CS1591

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
}
