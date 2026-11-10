using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Reads Parquet files for the Delta kernel with the engine: scan the file, hand its rows over as JSON lines.
/// </summary>
public sealed class EngineParquetReader(string tableRoot, CloudArgs? cloud = null) : IParquetReader
{
    /// <inheritdoc/>
    public byte[] ReadAsJsonLines(string path)
    {
        LazyFrameHandle lf = DeltaKernelScan.ScanFiles([DeltaKernelScan.Combine(tableRoot, path)], cloud ?? CloudArgs.Local, null, null);
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
/// </summary>
public static class DeltaKernelScan
{
    private const string RowIndexColumn = "__seseragi_row_index";

    /// <summary>The change feed column saying what happened to the row: insert, delete, update_preimage or update_postimage.</summary>
    public const string ChangeTypeColumn = "_change_type";
    /// <summary>The change feed column holding the version of the commit that made the change.</summary>
    public const string CommitVersionColumn = "_commit_version";
    /// <summary>The change feed column holding the time of the commit that made the change.</summary>
    public const string CommitTimestampColumn = "_commit_timestamp";

    /// <summary>Scans a table at its latest version, at a version, or as of a time.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="version">The version to read; null for the latest.</param>
    /// <param name="asOf">Read the latest version committed at or before this time; not together with <paramref name="version"/>.</param>
    /// <param name="filter">Leaves out files that cannot hold a matching row. It does not filter rows.</param>
    /// <param name="cloud">Cloud settings for a cloud URL; null for a local table.</param>
    public static LazyFrameHandle Scan(
        string tablePath, long? version = null, DateTimeOffset? asOf = null, Predicate? filter = null, CloudArgs? cloud = null) =>
        ScanPath(tablePath, version, asOf, filter, cloud, scannedFiles: null);

    /// <summary>As <see cref="Scan(string, long?, DateTimeOffset?, Predicate?, CloudArgs?)"/>, reporting how many data files are scanned.</summary>
    internal static LazyFrameHandle ScanPath(
        string tablePath, long? version, DateTimeOffset? asOf, Predicate? filter, CloudArgs? cloud, Action<int>? scannedFiles)
    {
        if (version.HasValue && asOf.HasValue)
            throw new ArgumentException("Give a version or a time, not both.");

        // A local table is read with plain file calls; anything with a scheme goes through the object store.
        bool isUrl = tablePath.Contains("://", StringComparison.Ordinal);
        IFileSystem fileSystem;
        string root;
        if (isUrl)
        {
            root = tablePath.TrimEnd('/');
            fileSystem = new ObjectStoreFileSystem(root, cloud);
        }
        else
        {
            var local = new LocalFileSystem(tablePath);
            root = local.Root;
            fileSystem = local;
        }

        try
        {
            var parquet = new EngineParquetReader(root, cloud);
            Snapshot snapshot = asOf is DateTimeOffset time
                ? Snapshot.LoadAsOf(fileSystem, time, parquet)
                : Snapshot.Load(fileSystem, version, parquet);
            return ScanSnapshot(fileSystem, root, snapshot, filter, cloud, scannedFiles);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            (fileSystem as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// Reads the row-level changes of the commits from <paramref name="startVersion"/> to
    /// <paramref name="endVersion"/>, both included: the table's columns followed by
    /// <see cref="ChangeTypeColumn"/>, <see cref="CommitVersionColumn"/> and <see cref="CommitTimestampColumn"/>.
    /// The columns are those of the table at the end version.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="startVersion">The first commit to report.</param>
    /// <param name="endVersion">The last commit to report; null for the latest.</param>
    /// <param name="requireEnabled">Fail unless the change data feed was enabled for the whole range.</param>
    /// <param name="cloud">Cloud settings for a cloud URL; null for a local table.</param>
    public static LazyFrameHandle ReadChangeFeed(
        string tablePath, long startVersion, long? endVersion = null, bool requireEnabled = true, CloudArgs? cloud = null) =>
        WithTable(tablePath, cloud, (fileSystem, root, parquet) =>
        {
            List<ChangeFile> changes = ChangeFeed.Read(fileSystem, startVersion, endVersion, requireEnabled, parquet);
            Snapshot snapshot = Snapshot.Load(fileSystem, endVersion, parquet);
            return ChangeFrame(fileSystem, root, snapshot, changes, cloud ?? CloudArgs.Local);
        });

    /// <summary>
    /// Reads the row-level changes of the commits made from <paramref name="start"/> to <paramref name="end"/>,
    /// both included. See <see cref="ReadChangeFeed(string, long, long?, bool, CloudArgs?)"/>.
    /// </summary>
    public static LazyFrameHandle ReadChangeFeed(
        string tablePath, DateTimeOffset start, DateTimeOffset? end = null, bool requireEnabled = true, CloudArgs? cloud = null) =>
        WithTable(tablePath, cloud, (fileSystem, root, parquet) =>
        {
            List<ChangeFile> changes = ChangeFeed.ReadBetween(fileSystem, start, end, requireEnabled, parquet);
            Snapshot snapshot = changes.Count > 0
                ? Snapshot.Load(fileSystem, changes[^1].Version, parquet)
                : Snapshot.Load(fileSystem, null, parquet);
            return ChangeFrame(fileSystem, root, snapshot, changes, cloud ?? CloudArgs.Local);
        });

    private static LazyFrameHandle WithTable(
        string tablePath, CloudArgs? cloud, Func<IFileSystem, string, IParquetReader, LazyFrameHandle> body)
    {
        bool isUrl = tablePath.Contains("://", StringComparison.Ordinal);
        IFileSystem fileSystem;
        string root;
        if (isUrl)
        {
            root = tablePath.TrimEnd('/');
            fileSystem = new ObjectStoreFileSystem(root, cloud);
        }
        else
        {
            var local = new LocalFileSystem(tablePath);
            root = local.Root;
            fileSystem = local;
        }

        try
        {
            return body(fileSystem, root, new EngineParquetReader(root, cloud));
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            (fileSystem as IDisposable)?.Dispose();
        }
    }

    private static LazyFrameHandle ChangeFrame(
        IFileSystem fileSystem, string tableRoot, Snapshot snapshot, List<ChangeFile> changes, CloudArgs cloud)
    {
        StructType schema = snapshot.Schema;
        var partitionColumns = new HashSet<string>(snapshot.PartitionColumns, StringComparer.Ordinal);
        EnsureNoNestedMapping(snapshot);

        var frames = new List<LazyFrameHandle>();
        try
        {
            foreach (ChangeFile change in changes)
            {
                if (partitionColumns.Count > 0 && change.PartitionValues is null)
                    throw new PolarsException(
                        $"The commit of version {change.Version} removed '{change.Path}' without recording its partition values, so its rows cannot be reported.");

                string path = Combine(tableRoot, change.DecodedPath);
                switch (change.Kind)
                {
                    case ChangeFileKind.Cdc:
                    {
                        using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns, withChangeType: true);
                        LazyFrameHandle lf = ScanFiles([path], cloud, fileSchema, null);
                        frames.Add(ProjectChange(lf, snapshot, change, partitionColumns, changeType: null));
                        break;
                    }

                    case ChangeFileKind.Insert or ChangeFileKind.Delete:
                    {
                        // Rows the deletion vector marks were not live: not inserted by an add, already gone at a remove.
                        long[] dead = change.DeletionVector is null ? [] : DeletionVector.Read(fileSystem, change.DeletionVector).DeletedRows;
                        using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns);
                        LazyFrameHandle lf = ScanFiles([path], cloud, fileSchema, dead.Length > 0 ? RowIndexColumn : null);
                        if (dead.Length > 0) lf = FilterRows(lf, dead, keep: false);
                        frames.Add(ProjectChange(lf, snapshot, change, partitionColumns, change.Kind == ChangeFileKind.Insert ? "insert" : "delete"));
                        break;
                    }

                    case ChangeFileKind.DeletionVectorChange:
                    {
                        var (deleted, restored) = change.ChangedRows(fileSystem);
                        foreach (var (rows, changeType) in new[] { (deleted, "delete"), (restored, "insert") })
                        {
                            if (rows.Length == 0) continue;
                            using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns);
                            LazyFrameHandle lf = FilterRows(ScanFiles([path], cloud, fileSchema, RowIndexColumn), rows, keep: true);
                            frames.Add(ProjectChange(lf, snapshot, change, partitionColumns, changeType));
                        }
                        break;
                    }
                }
            }

            if (frames.Count == 0)
            {
                using SchemaHandle logical = LogicalSchema(schema, withChangeColumns: true);
                using DataFrameHandle empty = PolarsWrapper.DataFrameFromSchema(logical, 0);
                return PolarsWrapper.DataFrameToLazy(empty);
            }

            LazyFrameHandle[] all = [.. frames];
            frames.Clear(); // from here the result owns them
            if (all.Length == 1) return all[0];
            // Keep the order of the commits.
            return PolarsWrapper.LazyConcat(all, PlConcatType.Vertical, rechunk: false, parallel: true, toSupertypes: false, maintainOrder: true);
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

    /// <summary>The table columns, then the change type (from the file when <paramref name="changeType"/> is null), version and time.</summary>
    private static LazyFrameHandle ProjectChange(
        LazyFrameHandle lf, Snapshot snapshot, ChangeFile change, HashSet<string> partitionColumns, string? changeType) =>
        Project(lf, snapshot, change.PartitionValues, partitionColumns, exprs =>
        {
            exprs.Add(changeType is null
                ? PolarsWrapper.Col(ChangeTypeColumn)
                : PolarsWrapper.Alias(PolarsWrapper.Lit(changeType), ChangeTypeColumn));
            exprs.Add(PolarsWrapper.Alias(PolarsWrapper.Lit(change.Version), CommitVersionColumn));

            using DataTypeHandle timestamp = PolarsWrapper.NewDateTimeType((byte)PlTimeUnit.Microseconds, "UTC");
            ExprHandle time = PolarsWrapper.ExprCast(
                PolarsWrapper.Lit(change.Timestamp.UtcDateTime), PolarsWrapper.DataTypeExprFromDataType(timestamp), strict: true, wrapNumerical: false);
            exprs.Add(PolarsWrapper.Alias(time, CommitTimestampColumn));
        });

    private static void EnsureNoNestedMapping(Snapshot snapshot)
    {
        if (snapshot.ColumnMappingMode == ColumnMappingMode.None) return;
        foreach (StructField field in snapshot.Schema.Fields)
        {
            if (HasNestedStruct(field.Type))
                throw new PolarsException($"Column mapping on the nested column '{field.Name}' is not supported yet.");
        }
    }

    /// <summary>Scans the files of a snapshot.</summary>
    /// <param name="fileSystem">The table storage, used to read deletion vectors.</param>
    /// <param name="tableRoot">The location data file paths are relative to.</param>
    /// <param name="snapshot">The table state to read.</param>
    /// <param name="filter">Leaves out files that cannot hold a matching row. It does not filter rows.</param>
    /// <param name="cloud">Cloud settings used to read the data files; null for a local table.</param>
    public static LazyFrameHandle Scan(IFileSystem fileSystem, string tableRoot, Snapshot snapshot, Predicate? filter = null, CloudArgs? cloud = null) =>
        ScanSnapshot(fileSystem, tableRoot, snapshot, filter, cloud, scannedFiles: null);

    private static LazyFrameHandle ScanSnapshot(
        IFileSystem fileSystem, string tableRoot, Snapshot snapshot, Predicate? filter, CloudArgs? cloud, Action<int>? scannedFiles)
    {
        cloud ??= CloudArgs.Local;
        StructType schema = snapshot.Schema;
        IReadOnlyList<AddAction> files = filter is null ? snapshot.Files : snapshot.FilesMatching(filter);
        scannedFiles?.Invoke(files.Count);

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
                LazyFrameHandle lf = ScanFiles(paths, cloud, fileSchema, null);
                frames.Add(Project(lf, snapshot, group[0].PartitionValues, partitionColumns));
            }

            foreach (AddAction file in withVectors)
            {
                long[] deleted = DeletionVector.Read(fileSystem, file.DeletionVector!).DeletedRows;
                using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns);
                LazyFrameHandle lf = ScanFiles([Combine(tableRoot, file.DecodedPath)], cloud, fileSchema, RowIndexColumn);

                if (deleted.Length > 0) lf = FilterRows(lf, deleted, keep: false);
                frames.Add(Project(lf, snapshot, file.PartitionValues, partitionColumns));
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

    internal static LazyFrameHandle ScanFiles(string[] paths, CloudArgs cloud, SchemaHandle? schema, string? rowIndexName) =>
        PolarsWrapper.ScanParquetFiles(
            paths,
            ignoreExtraColumns: true,
            lenientCasts: true,
            rowIndexName: rowIndexName,
            schema: schema,
            cloudProvider: cloud.Provider,
            cloudRetries: cloud.Retries,
            cloudRetryTimeoutMs: cloud.RetryTimeoutMs,
            cloudRetryInitBackoffMs: cloud.RetryInitBackoffMs,
            cloudRetryMaxBackoffMs: cloud.RetryMaxBackoffMs,
            cloudCacheTtl: cloud.CacheTtl,
            cloudKeys: cloud.Keys,
            cloudValues: cloud.Values);

    internal static string Combine(string root, string relative)
    {
        if (relative.Contains("://", StringComparison.Ordinal) || Path.IsPathRooted(relative)) return relative;
        return root.Contains("://", StringComparison.Ordinal)
            ? root.TrimEnd('/') + "/" + relative
            : Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar));
    }

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
    /// column as a constant of the file's partition value. <paramref name="extra"/> adds columns after them.
    /// </summary>
    private static LazyFrameHandle Project(
        LazyFrameHandle lf, Snapshot snapshot, IReadOnlyDictionary<string, string?>? loggedPartitionValues,
        HashSet<string> partitionColumns, Action<List<ExprHandle>>? extra = null)
    {
        Dictionary<string, string?>? partitionValues =
            partitionColumns.Count > 0 ? snapshot.LogicalPartitionValues(loggedPartitionValues ?? EmptyValues) : null;
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
            extra?.Invoke(exprs);
            ExprHandle[] all = [.. exprs];
            exprs.Clear();
            return PolarsWrapper.LazySelect(lf, all);
        }
        finally
        {
            foreach (ExprHandle expr in exprs) expr.Dispose();
        }
    }

    private static readonly IReadOnlyDictionary<string, string?> EmptyValues = new Dictionary<string, string?>();

    /// <summary>Keeps, or drops, the rows of a frame at the given positions. The frame must carry the row index column.</summary>
    private static LazyFrameHandle FilterRows(LazyFrameHandle lf, long[] rows, bool keep)
    {
        using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
        ExprHandle row = PolarsWrapper.ExprCast(
            PolarsWrapper.Col(RowIndexColumn), PolarsWrapper.DataTypeExprFromDataType(int64), strict: true, wrapNumerical: false);
        ExprHandle isListed = PolarsWrapper.IsIn(
            row, PolarsWrapper.Implode(PolarsWrapper.Lit(PolarsWrapper.SeriesNew("rows", rows)), maintainOrder: false), nullsEqual: false);
        return PolarsWrapper.LazyFilter(lf, keep ? isListed : PolarsWrapper.Not(isListed));
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
    private static SchemaHandle PhysicalFileSchema(StructType schema, HashSet<string> partitionColumns, bool withChangeType = false)
    {
        SchemaHandle handle = PolarsWrapper.SchemaCreate();
        if (withChangeType)
        {
            using DataTypeHandle text = PolarsWrapper.NewPrimitiveType((int)PlDataType.String);
            PolarsWrapper.SchemaAddField(handle, ChangeTypeColumn, text);
        }
        foreach (StructField field in schema.Fields)
        {
            if (partitionColumns.Contains(field.Name)) continue;
            using DataTypeHandle type = ToEngineType(field.Type, physicalNames: true);
            PolarsWrapper.SchemaAddField(handle, field.PhysicalName, type);
        }
        return handle;
    }

    private static SchemaHandle LogicalSchema(StructType schema, bool withChangeColumns = false)
    {
        SchemaHandle handle = PolarsWrapper.SchemaCreate();
        foreach (StructField field in schema.Fields)
        {
            using DataTypeHandle type = ToEngineType(field.Type, physicalNames: false);
            PolarsWrapper.SchemaAddField(handle, field.Name, type);
        }
        if (withChangeColumns)
        {
            using DataTypeHandle text = PolarsWrapper.NewPrimitiveType((int)PlDataType.String);
            using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
            using DataTypeHandle timestamp = PolarsWrapper.NewDateTimeType((byte)PlTimeUnit.Microseconds, "UTC");
            PolarsWrapper.SchemaAddField(handle, ChangeTypeColumn, text);
            PolarsWrapper.SchemaAddField(handle, CommitVersionColumn, int64);
            PolarsWrapper.SchemaAddField(handle, CommitTimestampColumn, timestamp);
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
