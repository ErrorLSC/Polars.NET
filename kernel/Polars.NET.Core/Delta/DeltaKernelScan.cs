using System.Globalization;
using Kaminari.Seseragi.Delta;
using Kaminari.Seseragi.Delta.Actions;
using Kaminari.Seseragi.Delta.Engine;
using Kaminari.Seseragi.Delta.Schema;
using Kaminari.Seseragi.Delta.Skipping;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Reads Parquet files for the Delta kernel with the engine: scan the file, hand its rows over as JSON lines.
/// </summary>
public sealed class EngineParquetReader(string tableRoot) : IParquetReader
{
    /// <inheritdoc/>
    public byte[] ReadAsJsonLines(string path)
    {
        LazyFrameHandle lf = PolarsWrapper.ScanParquetFiles([DeltaKernelScan.Combine(tableRoot, path)]);
        using DataFrameHandle df = PolarsWrapper.LazyCollect(lf, PlEngine.Auto);
        using NativeBuffer buffer = PolarsWrapper.WriteJsonNativeMemory(df, PlJsonFormat.JsonLines);
        return buffer.Memory.ToArray();
    }
}

/// <summary>
/// Builds a scan of a Delta table from a snapshot the managed kernel resolved. The kernel decides which files make
/// up the table; this turns them into one lazy frame: partition values become columns, deletion vectors remove
/// their rows, mapped columns get their logical names back.
/// Not used by the public API yet: the path-based Delta functions still go through the native implementation.
/// Local tables only for now.
/// </summary>
public static class DeltaKernelScan
{
    private const string RowIndexColumn = "__seseragi_row_index";

    /// <summary>Scans a table at its latest version, at a version, or as of a time.</summary>
    /// <param name="tablePath">The table directory on the local disk.</param>
    /// <param name="version">The version to read; null for the latest.</param>
    /// <param name="asOf">Read the latest version committed at or before this time; not together with <paramref name="version"/>.</param>
    /// <param name="filter">Leaves out files that cannot hold a matching row. It does not filter rows.</param>
    public static LazyFrameHandle Scan(string tablePath, long? version = null, DateTimeOffset? asOf = null, Predicate? filter = null)
    {
        if (version.HasValue && asOf.HasValue)
            throw new ArgumentException("Give a version or a time, not both.");

        var fileSystem = new LocalFileSystem(tablePath);
        var parquet = new EngineParquetReader(fileSystem.Root);
        try
        {
            Snapshot snapshot = asOf is DateTimeOffset time
                ? Snapshot.LoadAsOf(fileSystem, time, parquet)
                : Snapshot.Load(fileSystem, version, parquet);
            return Scan(fileSystem, fileSystem.Root, snapshot, filter);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }

    /// <summary>Scans the files of a snapshot.</summary>
    /// <param name="fileSystem">The table storage, used to read deletion vectors.</param>
    /// <param name="tableRoot">The location data file paths are relative to.</param>
    /// <param name="snapshot">The table state to read.</param>
    /// <param name="filter">Leaves out files that cannot hold a matching row. It does not filter rows.</param>
    public static LazyFrameHandle Scan(IFileSystem fileSystem, string tableRoot, Snapshot snapshot, Predicate? filter = null)
    {
        StructType schema = snapshot.Schema;
        IReadOnlyList<AddAction> files = filter is null ? snapshot.Files : snapshot.FilesMatching(filter);

        var partitionColumns = new HashSet<string>(snapshot.PartitionColumns, StringComparer.Ordinal);
        if (snapshot.ColumnMappingMode != ColumnMappingMode.None)
        {
            foreach (StructField field in schema.Fields)
            {
                if (HasNestedStruct(field.Type))
                    throw new PolarsException($"Column mapping on the nested column '{field.Name}' is not supported yet.");
            }
        }

        // Files that share their partition values are scanned together; a file with a deletion vector on its own.
        var groups = new Dictionary<string, List<AddAction>>(StringComparer.Ordinal);
        var withVectors = new List<AddAction>();
        foreach (AddAction file in files)
        {
            if (file.DeletionVector is not null)
            {
                withVectors.Add(file);
                continue;
            }
            string key = PartitionKey(snapshot, file);
            if (!groups.TryGetValue(key, out List<AddAction>? group)) groups[key] = group = [];
            group.Add(file);
        }

        var frames = new List<LazyFrameHandle>();
        try
        {
            foreach (List<AddAction> group in groups.Values)
            {
                using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns);
                string[] paths = [.. group.Select(f => Combine(tableRoot, f.DecodedPath))];
                LazyFrameHandle lf = PolarsWrapper.ScanParquetFiles(paths, ignoreExtraColumns: true, schema: fileSchema);
                frames.Add(Project(lf, snapshot, group[0], partitionColumns));
            }

            foreach (AddAction file in withVectors)
            {
                long[] deleted = DeletionVector.Read(fileSystem, file.DeletionVector!).DeletedRows;
                using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns);
                LazyFrameHandle lf = PolarsWrapper.ScanParquetFiles(
                    [Combine(tableRoot, file.DecodedPath)], ignoreExtraColumns: true, rowIndexName: RowIndexColumn, schema: fileSchema);

                if (deleted.Length > 0)
                {
                    using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
                    ExprHandle row = PolarsWrapper.ExprCast(
                        PolarsWrapper.Col(RowIndexColumn), PolarsWrapper.DataTypeExprFromDataType(int64), strict: true, wrapNumerical: false);
                    ExprHandle isDeleted = PolarsWrapper.IsIn(
                        row, PolarsWrapper.Implode(PolarsWrapper.Lit(PolarsWrapper.SeriesNew("deleted", deleted)), maintainOrder: false), nullsEqual: false);
                    lf = PolarsWrapper.LazyFilter(lf, PolarsWrapper.Not(isDeleted));
                }
                frames.Add(Project(lf, snapshot, file, partitionColumns));
            }

            if (frames.Count == 0)
            {
                using SchemaHandle logical = LogicalSchema(schema);
                using DataFrameHandle empty = PolarsWrapper.DataFrameFromSchema(logical, 0);
                return PolarsWrapper.DataFrameToLazy(empty);
            }
            LazyFrameHandle[] all = [.. frames];
            frames.Clear(); // from here the result owns them
            if (all.Length == 1) return all[0];
            return PolarsWrapper.LazyConcat(all, PlConcatType.Vertical, rechunk: false, parallel: true, toSupertypes: false, maintainOrder: false);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            foreach (LazyFrameHandle frame in frames) frame.Dispose();
        }
    }

    internal static string Combine(string root, string relative) =>
        relative.Contains("://", StringComparison.Ordinal) || Path.IsPathRooted(relative)
            ? relative
            : Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar));

    private static string PartitionKey(Snapshot snapshot, AddAction file)
    {
        if (snapshot.PartitionColumns.Count == 0) return "";
        var sb = new System.Text.StringBuilder();
        foreach (var (name, value) in snapshot.PartitionValuesOf(file))
        {
            // Length-prefixed so that no two different sets of values share a key.
            sb.Append(name.Length).Append(':').Append(name);
            if (value is null) sb.Append("=N;");
            else sb.Append("=V").Append(value.Length).Append(':').Append(value).Append(';');
        }
        return sb.ToString();
    }

    /// <summary>
    /// Selects the table's columns in schema order: a data column from the file under its logical name, a partition
    /// column as a constant of the file's partition value.
    /// </summary>
    private static LazyFrameHandle Project(LazyFrameHandle lf, Snapshot snapshot, AddAction file, HashSet<string> partitionColumns)
    {
        Dictionary<string, string?>? partitionValues = partitionColumns.Count > 0 ? snapshot.PartitionValuesOf(file) : null;
        var exprs = new List<ExprHandle>();
        try
        {
            foreach (StructField field in snapshot.Schema.Fields)
            {
                if (!partitionColumns.Contains(field.Name))
                {
                    exprs.Add(PolarsWrapper.Alias(PolarsWrapper.Col(field.PhysicalName), field.Name));
                    continue;
                }

                using DataTypeHandle type = ToEngineType(field.Type, physicalNames: false);
                ExprHandle literal = PartitionLiteral(field, partitionValues![field.Name]);
                ExprHandle cast = PolarsWrapper.ExprCast(literal, PolarsWrapper.DataTypeExprFromDataType(type), strict: true, wrapNumerical: false);
                exprs.Add(PolarsWrapper.Alias(cast, field.Name));
            }
            ExprHandle[] all = [.. exprs];
            exprs.Clear();
            return PolarsWrapper.LazySelect(lf, all);
        }
        finally
        {
            foreach (ExprHandle expr in exprs) expr.Dispose();
        }
    }

    /// <summary>A partition value as a literal; the log keeps it as text. The caller casts it to the column type.</summary>
    private static ExprHandle PartitionLiteral(StructField field, string? value)
    {
        if (value is null) return PolarsWrapper.LitNull();
        // An empty string stands for null except in a string column, where writers differ; follow the protocol: null.
        if (value.Length == 0) return PolarsWrapper.LitNull();

        if (field.Type is PrimitiveType primitive)
        {
            switch (primitive.Name)
            {
                case PrimitiveType.Date
                    when DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly date):
                    return PolarsWrapper.Lit(date);
                case PrimitiveType.Timestamp or PrimitiveType.TimestampNtz
                    when DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTimeOffset ts):
                    return PolarsWrapper.Lit(ts.UtcDateTime);
                case PrimitiveType.Boolean when bool.TryParse(value, out bool flag):
                    return PolarsWrapper.Lit(flag);
                case PrimitiveType.Date or PrimitiveType.Timestamp or PrimitiveType.TimestampNtz or PrimitiveType.Boolean:
                    throw new PolarsException($"The partition value '{value}' of column '{field.Name}' is not a valid {primitive.Name}.");
            }
        }
        return PolarsWrapper.Lit(value);
    }

    private static bool HasNestedStruct(DeltaType type) => type switch
    {
        StructType => true,
        ArrayType a => HasNestedStruct(a.ElementType),
        MapType => true, // a map is read as a list of key/value structs
        _ => false,
    };

    /// <summary>The columns expected in a data file: every non-partition column under its physical name.</summary>
    private static SchemaHandle PhysicalFileSchema(StructType schema, HashSet<string> partitionColumns)
    {
        SchemaHandle handle = PolarsWrapper.SchemaCreate();
        foreach (StructField field in schema.Fields)
        {
            if (partitionColumns.Contains(field.Name)) continue;
            using DataTypeHandle type = ToEngineType(field.Type, physicalNames: true);
            PolarsWrapper.SchemaAddField(handle, field.PhysicalName, type);
        }
        return handle;
    }

    private static SchemaHandle LogicalSchema(StructType schema)
    {
        SchemaHandle handle = PolarsWrapper.SchemaCreate();
        foreach (StructField field in schema.Fields)
        {
            using DataTypeHandle type = ToEngineType(field.Type, physicalNames: false);
            PolarsWrapper.SchemaAddField(handle, field.Name, type);
        }
        return handle;
    }

    /// <summary>The engine type of a Delta type. Timestamps are microseconds; a map is a list of key/value structs.</summary>
    private static DataTypeHandle ToEngineType(DeltaType type, bool physicalNames)
    {
        switch (type)
        {
            case PrimitiveType p:
                return p.Name switch
                {
                    PrimitiveType.String => PolarsWrapper.NewPrimitiveType((int)PlDataType.String),
                    PrimitiveType.Long => PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64),
                    PrimitiveType.Integer => PolarsWrapper.NewPrimitiveType((int)PlDataType.Int32),
                    PrimitiveType.Short => PolarsWrapper.NewPrimitiveType((int)PlDataType.Int16),
                    PrimitiveType.Byte => PolarsWrapper.NewPrimitiveType((int)PlDataType.Int8),
                    PrimitiveType.Float => PolarsWrapper.NewPrimitiveType((int)PlDataType.Float32),
                    PrimitiveType.Double => PolarsWrapper.NewPrimitiveType((int)PlDataType.Float64),
                    PrimitiveType.Boolean => PolarsWrapper.NewPrimitiveType((int)PlDataType.Boolean),
                    PrimitiveType.Binary => PolarsWrapper.NewPrimitiveType((int)PlDataType.Binary),
                    PrimitiveType.Date => PolarsWrapper.NewPrimitiveType((int)PlDataType.Date),
                    PrimitiveType.Timestamp => PolarsWrapper.NewDateTimeType((byte)PlTimeUnit.Microseconds, "UTC"),
                    PrimitiveType.TimestampNtz => PolarsWrapper.NewDateTimeType((byte)PlTimeUnit.Microseconds, null),
                    _ => throw new PolarsException($"The Delta type '{p.Name}' cannot be read yet."),
                };

            case DecimalType d:
                return PolarsWrapper.NewDecimalType(d.Precision, d.Scale);

            case ArrayType a:
            {
                using DataTypeHandle element = ToEngineType(a.ElementType, physicalNames);
                return PolarsWrapper.NewListType(element);
            }

            case MapType m:
            {
                using DataTypeHandle key = ToEngineType(m.KeyType, physicalNames);
                using DataTypeHandle value = ToEngineType(m.ValueType, physicalNames);
                using DataTypeHandle entry = PolarsWrapper.NewStructType(["key", "value"], [key, value]);
                return PolarsWrapper.NewListType(entry);
            }

            case StructType s:
            {
                var names = new string[s.Fields.Count];
                var types = new DataTypeHandle[s.Fields.Count];
                try
                {
                    for (int i = 0; i < names.Length; i++)
                    {
                        names[i] = physicalNames ? s.Fields[i].PhysicalName : s.Fields[i].Name;
                        types[i] = ToEngineType(s.Fields[i].Type, physicalNames);
                    }
                    return PolarsWrapper.NewStructType(names, types);
                }
                finally
                {
                    foreach (DataTypeHandle? t in types) t?.Dispose();
                }
            }

            default:
                throw new PolarsException($"The Delta type '{type}' cannot be read yet.");
        }
    }
}
