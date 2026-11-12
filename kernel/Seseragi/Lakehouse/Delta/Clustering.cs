using System.Globalization;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Liquid clustering (the <c>clustering</c> writer feature): the table names up to four clustering columns in the
/// <c>delta.clustering</c> domain (paths of physical names), and OPTIMIZE lays out the rows by them. Files written by
/// clustering carry <c>clusteringProvider: "liquid"</c> and the tags of their cube (<see cref="CubeIdTag"/>,
/// <see cref="CubeColumnsTag"/>); other files are unclustered. A clustered table is not partitioned. Follows
/// delta-spark 4.4.
/// </summary>
public static class Clustering
{
    /// <summary>The writer feature.</summary>
    public const string Feature = "clustering";

    /// <summary>The domain naming the clustering columns.</summary>
    public const string Domain = "delta.clustering";

    /// <summary>The <c>clusteringProvider</c> of files written by clustering.</summary>
    public const string Provider = "liquid";

    /// <summary>The tag giving the id of a file's cube (the files one clustering run wrote).</summary>
    public const string CubeIdTag = "ZCUBE_ID";

    /// <summary>The tag giving the columns a file's cube was clustered by (logical names, JSON array).</summary>
    public const string CubeColumnsTag = "ZCUBE_ZORDER_BY";

    /// <summary>The most clustering columns (Spark's <c>numClusteringColumnsLimit</c>).</summary>
    public const int MaxColumns = 4;

    /// <summary>
    /// The clustering columns as paths of physical names; null when the table is not clustered (no domain), empty
    /// after <c>CLUSTER BY NONE</c>.
    /// </summary>
    /// <exception cref="DeltaProtocolException">The domain does not hold clustering columns.</exception>
    public static IReadOnlyList<string[]>? PhysicalColumns(IReadOnlyDictionary<string, DomainMetadataAction> domains)
    {
        ArgumentNullException.ThrowIfNull(domains);
        if (!domains.TryGetValue(Domain, out DomainMetadataAction? domain) || domain.Removed) return null;
        try
        {
            using JsonDocument doc = JsonDocument.Parse(domain.Configuration);
            var columns = new List<string[]>();
            foreach (JsonElement path in doc.RootElement.GetProperty("clusteringColumns").EnumerateArray())
                columns.Add([.. path.EnumerateArray().Select(p => p.GetString() ?? throw new FormatException())]);
            return columns;
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            throw new DeltaProtocolException($"The {Domain} domain holds '{domain.Configuration}', which names no clustering columns.");
        }
    }

    /// <summary>The domain action naming the clustering columns, as Spark writes it.</summary>
    public static DomainMetadataAction DomainFor(IEnumerable<string[]> physicalColumns)
    {
        ArgumentNullException.ThrowIfNull(physicalColumns);
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteStartArray("clusteringColumns");
            foreach (string[] path in physicalColumns)
            {
                json.WriteStartArray();
                foreach (string part in path) json.WriteStringValue(part);
                json.WriteEndArray();
            }
            json.WriteEndArray();
            json.WriteString("domainName", Domain);
            json.WriteEndObject();
        }
        return new DomainMetadataAction { Domain = Domain, Configuration = Encoding.UTF8.GetString(stream.ToArray()) };
    }

    /// <summary>The fields along each clustering column's path (physical names resolved in the schema).</summary>
    /// <exception cref="DeltaProtocolException">A path names no field of the schema.</exception>
    public static List<StructField[]> Fields(StructType schema, IReadOnlyList<string[]> physicalColumns)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(physicalColumns);
        var result = new List<StructField[]>(physicalColumns.Count);
        foreach (string[] path in physicalColumns)
        {
            var fields = new StructField[path.Length];
            DeltaType type = schema;
            for (int i = 0; i < path.Length; i++)
            {
                fields[i] = (type as StructType)?.Fields.FirstOrDefault(f => f.PhysicalName == path[i])
                    ?? throw new DeltaProtocolException($"The clustering column {string.Join('.', path)} is not in the table's schema.");
                type = fields[i].Type;
            }
            result.Add(fields);
        }
        return result;
    }

    /// <summary>A clustering column's logical name, as Spark records it (dotted for a nested field).</summary>
    public static string LogicalName(StructField[] path) => string.Join('.', path.Select(f => f.Name));

    /// <summary>
    /// The paths of the columns <paramref name="names"/> gives (dotted for nested fields, any case), checked as Spark
    /// checks clustering columns: at most <see cref="MaxColumns"/>, each a field of a type with statistics, inside
    /// the columns that get statistics, and the table not partitioned.
    /// </summary>
    /// <exception cref="DeltaException">The columns cannot cluster the table.</exception>
    public static List<StructField[]> Resolve(
        StructType schema, IReadOnlyList<string> names, IReadOnlyList<string> partitionColumns, IReadOnlyDictionary<string, string>? configuration)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(names);
        ArgumentNullException.ThrowIfNull(partitionColumns);
        if (names.Count == 0) return [];
        if (partitionColumns.Count > 0)
            throw new DeltaException("Clustering and partitioning cannot both be specified: a clustered table is not partitioned.");
        if (names.Count > MaxColumns)
            throw new DeltaException($"CLUSTER BY supports up to {MaxColumns} clustering columns, but {names.Count} are given.");

        var result = new List<StructField[]>();
        foreach (string name in names)
        {
            IReadOnlyList<string> parts = ColumnNames.Split(name);
            var path = new StructField[parts.Count];
            DeltaType type = schema;
            for (int i = 0; i < parts.Count; i++)
            {
                path[i] = (type as StructType)?.Fields.FirstOrDefault(f => string.Equals(f.Name, parts[i], StringComparison.OrdinalIgnoreCase))
                    ?? throw new DeltaException($"The clustering column '{name}' is not in the table's schema.");
                type = path[i].Type;
            }
            if (!HasStatistics(type))
                throw new DeltaException(
                    $"CLUSTER BY is not supported because the column '{name}' of type {type} does not support data skipping.");
            if (result.Any(p => LogicalName(p).Equals(LogicalName(path), StringComparison.OrdinalIgnoreCase)))
                throw new DeltaException($"The clustering column '{name}' is given twice.");
            result.Add(path);
        }

        string[] missing = [.. result.Where(p => !InStatistics(schema, p, configuration)).Select(LogicalName)];
        if (missing.Length > 0)
            throw new DeltaException(
                $"Clustering requires clustering columns to have statistics; '{string.Join("', '", missing)}' are not among the columns that get " +
                $"them (see {FileStatistics.NumIndexedColsKey} and {FileStatistics.StatsColumnsKey}).");
        return result;
    }

    /// <summary>The physical path of a resolved clustering column.</summary>
    public static string[] PhysicalPath(StructField[] path) => [.. path.Select(f => f.PhysicalName)];

    /// <summary>Whether Spark collects min/max statistics for a type (its data skipping types).</summary>
    internal static bool HasStatistics(DeltaType type) => type switch
    {
        DecimalType => true,
        PrimitiveType p => p.Name is PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Long
            or PrimitiveType.Float or PrimitiveType.Double or PrimitiveType.Date or PrimitiveType.Timestamp or PrimitiveType.TimestampNtz
            or PrimitiveType.String,
        _ => false,
    };

    /// <summary>Whether Spark's statistics include the column (see <see cref="FileStatistics.IndexedLeaves(IReadOnlyList{StructField}, IReadOnlyDictionary{string, string}?)"/>).</summary>
    private static bool InStatistics(StructType schema, StructField[] path, IReadOnlyDictionary<string, string>? configuration)
    {
        string logical = LogicalName(path);
        return FileStatistics.IndexedLeaves(schema.Fields, configuration).Any(leaf => LogicalName(leaf) == logical);
    }
}
