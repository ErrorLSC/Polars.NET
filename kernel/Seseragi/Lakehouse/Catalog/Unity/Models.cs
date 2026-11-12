namespace Kaminari.Seseragi.Lakehouse.Catalog.Unity;

/// <summary>Well-known values of <see cref="TableInfo.TableType"/>.</summary>
public static class UnityTableTypes
{
#pragma warning disable CS1591
    public const string Managed = "MANAGED";
    public const string External = "EXTERNAL";
    public const string View = "VIEW";
    public const string MaterializedView = "MATERIALIZED_VIEW";
    public const string StreamingTable = "STREAMING_TABLE";
#pragma warning restore CS1591
}

/// <summary>Well-known values of <see cref="TableInfo.DataSourceFormat"/>.</summary>
public static class UnityDataSourceFormats
{
#pragma warning disable CS1591
    public const string Delta = "DELTA";
    public const string Parquet = "PARQUET";
    public const string Csv = "CSV";
    public const string Json = "JSON";
    public const string Avro = "AVRO";
    public const string Orc = "ORC";
    public const string Text = "TEXT";
#pragma warning restore CS1591
}

/// <summary>A catalog, the top level of the three-level namespace.</summary>
public sealed class CatalogInfo
{
    /// <summary>The catalog name.</summary>
    public required string Name { get; init; }
    /// <summary>The comment, if one is set.</summary>
    public string? Comment { get; init; }
    /// <summary>The storage location, if one is set.</summary>
    public string? StorageLocation { get; init; }
    /// <summary>Properties; empty when the service sends none.</summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = EmptyMap.Instance;
    /// <summary>Options; empty when the service sends none.</summary>
    public IReadOnlyDictionary<string, string> Options { get; init; } = EmptyMap.Instance;
    /// <summary>Creation time.</summary>
    public DateTimeOffset? CreatedAt { get; init; }
    /// <summary>The creator.</summary>
    public string? CreatedBy { get; init; }
    /// <summary>Last update time.</summary>
    public DateTimeOffset? UpdatedAt { get; init; }
    /// <summary>The last updater.</summary>
    public string? UpdatedBy { get; init; }
}

/// <summary>A schema, the second level of the three-level namespace.</summary>
public sealed class SchemaInfo
{
    /// <summary>The schema name.</summary>
    public required string Name { get; init; }
    /// <summary>The comment, if one is set.</summary>
    public string? Comment { get; init; }
    /// <summary>The storage location, if one is set.</summary>
    public string? StorageLocation { get; init; }
    /// <summary>Properties; empty when the service sends none.</summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = EmptyMap.Instance;
    /// <summary>Creation time.</summary>
    public DateTimeOffset? CreatedAt { get; init; }
    /// <summary>The creator.</summary>
    public string? CreatedBy { get; init; }
    /// <summary>Last update time.</summary>
    public DateTimeOffset? UpdatedAt { get; init; }
    /// <summary>The last updater.</summary>
    public string? UpdatedBy { get; init; }
}

/// <summary>A table registered in the catalog.</summary>
public sealed class TableInfo
{
    /// <summary>The table name, without catalog and schema.</summary>
    public required string Name { get; init; }
    /// <summary>The table id used to request temporary credentials.</summary>
    public required string TableId { get; init; }
    /// <summary>The table type as the service names it; see <see cref="UnityTableTypes"/>.</summary>
    public required string TableType { get; init; }
    /// <summary>The comment, if one is set.</summary>
    public string? Comment { get; init; }
    /// <summary>Where the table data lives, when the table has a location.</summary>
    public string? StorageLocation { get; init; }
    /// <summary>The data format as the service names it; see <see cref="UnityDataSourceFormats"/>.</summary>
    public string? DataSourceFormat { get; init; }
    /// <summary>The columns, when the service lists them.</summary>
    public IReadOnlyList<ColumnInfo>? Columns { get; init; }
    /// <summary>Properties; empty when the service sends none.</summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = EmptyMap.Instance;
    /// <summary>Creation time.</summary>
    public DateTimeOffset? CreatedAt { get; init; }
    /// <summary>The creator.</summary>
    public string? CreatedBy { get; init; }
    /// <summary>Last update time.</summary>
    public DateTimeOffset? UpdatedAt { get; init; }
    /// <summary>The last updater.</summary>
    public string? UpdatedBy { get; init; }
}

/// <summary>A column of a table, in the three type forms the service uses.</summary>
public sealed class ColumnInfo
{
    /// <summary>The column name.</summary>
    public required string Name { get; init; }
    /// <summary>For example <c>LONG</c>.</summary>
    public required string TypeName { get; init; }
    /// <summary>For example <c>bigint</c>.</summary>
    public required string TypeText { get; init; }
    /// <summary>The full type as a JSON document.</summary>
    public required string TypeJson { get; init; }
    /// <summary>Zero-based position in the table.</summary>
    public int? Position { get; init; }
    /// <summary>The comment, if one is set.</summary>
    public string? Comment { get; init; }
    /// <summary>Position among the partition columns, when the column is one.</summary>
    public int? PartitionIndex { get; init; }

    /// <summary>Builds a column from a name and a type.</summary>
    public static ColumnInfo Create(string name, UnityDataType type, int? position = null, string? comment = null) => new()
    {
        Name = name ?? throw new ArgumentNullException(nameof(name)),
        TypeName = type.TypeName,
        TypeText = type.TypeText,
        TypeJson = type.ToTypeJson(name),
        Position = position,
        Comment = comment,
    };
}

internal static class EmptyMap
{
    public static readonly IReadOnlyDictionary<string, string> Instance = new Dictionary<string, string>();
}
