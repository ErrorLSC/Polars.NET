using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Helpers;
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

/// <summary>How a Delta table is read, beyond which version.</summary>
/// <param name="NRows">The most rows to return; null for all.</param>
/// <param name="RowIndexName">Adds a row index column of this name; null for none.</param>
/// <param name="RowIndexOffset">The first row index.</param>
/// <param name="IncludePathColumn">Adds a column of this name holding each row's data file; null for none.</param>
/// <param name="Parallel">How the engine parallelizes reading the files.</param>
/// <param name="LowMemory">Read with less memory, more slowly.</param>
/// <param name="UseStatistics">Skip row groups by their Parquet statistics.</param>
/// <param name="Rechunk">Make the result contiguous in memory.</param>
/// <param name="Cache">Cache the scan's result within a query.</param>
/// <param name="Schema">Return these columns (of the table, or the path column), cast to these types (strictly);
/// null for the table's columns. Borrowed, not consumed.</param>
public sealed record KernelScanOptions(
    ulong? NRows = null,
    string? RowIndexName = null,
    uint RowIndexOffset = 0,
    string? IncludePathColumn = null,
    PlParallelStrategy Parallel = PlParallelStrategy.Auto,
    bool LowMemory = false,
    bool UseStatistics = true,
    bool Rechunk = false,
    bool Cache = true,
    SchemaHandle? Schema = null);

/// <summary>
/// Builds a scan of a Delta table from a snapshot the managed kernel resolved. The kernel decides which files make
/// up the table; this turns them into one lazy frame: partition values become columns, deletion vectors remove
/// their rows, mapped columns get their logical names back.
/// Not used by the public API yet: the path-based Delta functions still go through the native implementation.
/// </summary>
public static class DeltaKernelScan
{
    internal const string RowIndexColumn = "__seseragi_row_index";

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
    /// <param name="options">Reading options; null for the defaults.</param>
    public static LazyFrameHandle Scan(
        string tablePath, long? version = null, DateTimeOffset? asOf = null, Predicate? filter = null, CloudArgs? cloud = null,
        KernelScanOptions? options = null) =>
        ScanPath(tablePath, version, asOf, filter, cloud, scannedFiles: null, options);

    /// <summary>
    /// The time of a time-travel read given as text: ISO-8601 (RFC 3339), such as <c>2026-02-09T12:00:00Z</c>; a time
    /// without an offset is UTC.
    /// </summary>
    /// <exception cref="ArgumentException">The text is not such a time.</exception>
    public static DateTimeOffset ParseAsOf(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (DateTimeOffset.TryParse(text.Trim(), System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AllowWhiteSpaces, out DateTimeOffset time))
            return time;
        throw new ArgumentException($"'{text}' is not an ISO-8601 time such as 2026-02-09T12:00:00Z.", nameof(text));
    }

    /// <summary>As <see cref="Scan"/>, reporting how many data files are scanned.</summary>
    internal static LazyFrameHandle ScanPath(
        string tablePath, long? version, DateTimeOffset? asOf, Predicate? filter, CloudArgs? cloud, Action<int>? scannedFiles,
        KernelScanOptions? options = null)
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
            LazyFrameHandle scanned = ScanSnapshot(fileSystem, root, snapshot, filter, cloud, scannedFiles, options);
            return options is null ? scanned : ApplyOptions(scanned, snapshot.Schema, options);
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
        string[] widened = WidenedColumns(snapshot);

        var frames = new List<LazyFrameHandle>();
        try
        {
            foreach (ChangeFile change in changes)
            {
                if (partitionColumns.Count > 0 && change.PartitionValues is null)
                    throw new PolarsException(
                        $"The commit of version {change.Version} removed '{change.Path}' without recording its partition values, so its rows cannot be reported.");

                string path = Combine(tableRoot, change.DecodedPath);
                IReadOnlyDictionary<string, DeltaType> oldTypes = OldTypes(path, cloud, widened);
                switch (change.Kind)
                {
                    case ChangeFileKind.Cdc:
                    {
                        using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns, withChangeType: true, oldTypes);
                        LazyFrameHandle lf = ScanFiles([path], cloud, fileSchema, null);
                        frames.Add(ProjectChange(lf, snapshot, change, partitionColumns, changeType: null, oldTypes));
                        break;
                    }

                    case ChangeFileKind.Insert or ChangeFileKind.Delete:
                    {
                        // Rows the deletion vector marks were not live: not inserted by an add, already gone at a remove.
                        long[] dead = change.DeletionVector is null ? [] : DeletionVector.Read(fileSystem, change.DeletionVector).DeletedRows;
                        using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns, oldTypes: oldTypes);
                        LazyFrameHandle lf = ScanFiles([path], cloud, fileSchema, dead.Length > 0 ? RowIndexColumn : null);
                        if (dead.Length > 0) lf = FilterRows(lf, dead, keep: false);
                        frames.Add(ProjectChange(lf, snapshot, change, partitionColumns, change.Kind == ChangeFileKind.Insert ? "insert" : "delete", oldTypes));
                        break;
                    }

                    case ChangeFileKind.DeletionVectorChange:
                    {
                        var (deleted, restored) = change.ChangedRows(fileSystem);
                        foreach (var (rows, changeType) in new[] { (deleted, "delete"), (restored, "insert") })
                        {
                            if (rows.Length == 0) continue;
                            using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns, oldTypes: oldTypes);
                            LazyFrameHandle lf = FilterRows(ScanFiles([path], cloud, fileSchema, RowIndexColumn), rows, keep: true);
                            frames.Add(ProjectChange(lf, snapshot, change, partitionColumns, changeType, oldTypes));
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
        LazyFrameHandle lf, Snapshot snapshot, ChangeFile change, HashSet<string> partitionColumns, string? changeType,
        IReadOnlyDictionary<string, DeltaType> oldTypes) =>
        Project(lf, snapshot, change.PartitionValues, partitionColumns, oldTypes, exprs =>
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

    /// <summary>Scans the files of a snapshot.</summary>
    /// <param name="fileSystem">The table storage, used to read deletion vectors.</param>
    /// <param name="tableRoot">The location data file paths are relative to.</param>
    /// <param name="snapshot">The table state to read.</param>
    /// <param name="filter">Leaves out files that cannot hold a matching row. It does not filter rows.</param>
    /// <param name="cloud">Cloud settings used to read the data files; null for a local table.</param>
    public static LazyFrameHandle Scan(IFileSystem fileSystem, string tableRoot, Snapshot snapshot, Predicate? filter = null, CloudArgs? cloud = null) =>
        ScanSnapshot(fileSystem, tableRoot, snapshot, filter, cloud, scannedFiles: null);

    /// <summary>
    /// The scan options that act on the whole result: the columns of <see cref="KernelScanOptions.Schema"/> (strict
    /// casts), then a row index, then the row limit.
    /// </summary>
    private static LazyFrameHandle ApplyOptions(LazyFrameHandle lf, StructType tableSchema, KernelScanOptions options)
    {
        if (options.Schema is SchemaHandle wanted)
        {
            ulong count = PolarsWrapper.GetSchemaLen(wanted);
            var exprs = new ExprHandle[count];
            for (ulong i = 0; i < count; i++)
            {
                PolarsWrapper.GetSchemaFieldAt(wanted, i, out string name, out DataTypeHandle type);
                using (type)
                {
                    bool known = tableSchema.Field(name) is not null
                        || (options.IncludePathColumn is not null && name == options.IncludePathColumn);
                    if (!known)
                    {
                        foreach (ExprHandle e in exprs.Take((int)i)) e.Dispose();
                        lf.Dispose();
                        throw new PolarsException($"The schema names the column '{name}', which the Delta table does not have.");
                    }
                    exprs[i] = PolarsWrapper.Alias(
                        PolarsWrapper.ExprCast(PolarsWrapper.Col(name), PolarsWrapper.DataTypeExprFromDataType(type), strict: true, wrapNumerical: false),
                        name);
                }
            }
            lf = PolarsWrapper.LazySelect(lf, exprs);
        }
        if (options.RowIndexName is string index)
        {
            if (options.RowIndexOffset > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(options), $"The row index offset {options.RowIndexOffset} is too large.");
            lf = PolarsWrapper.LazyFrameWithRowIndex(lf, index, (int)options.RowIndexOffset);
        }
        if (options.NRows is ulong n)
            lf = PolarsWrapper.LazySlice(lf, 0, (uint)Math.Min(n, uint.MaxValue));
        return lf;
    }

    private static LazyFrameHandle ScanSnapshot(
        IFileSystem fileSystem, string tableRoot, Snapshot snapshot, Predicate? filter, CloudArgs? cloud, Action<int>? scannedFiles,
        KernelScanOptions? options = null)
    {
        string? pathColumn = options?.IncludePathColumn;
        Action<List<ExprHandle>>? keepPath = pathColumn is null ? null : exprs => exprs.Add(PolarsWrapper.Col(pathColumn));
        cloud ??= CloudArgs.Local;
        StructType schema = snapshot.Schema;
        IReadOnlyList<AddAction> files = filter is null ? snapshot.Files : snapshot.FilesMatching(filter);
        scannedFiles?.Invoke(files.Count);

        var partitionColumns = new HashSet<string>(snapshot.PartitionColumns, StringComparer.Ordinal);
        string[] widened = WidenedColumns(snapshot);

        // Files that share their partition values (and, after a date column was widened, their physical type for
        // it) are scanned together; a file with a deletion vector on its own.
        var groups = new Dictionary<string, (List<AddAction> Files, IReadOnlyDictionary<string, DeltaType> AsDate)>(StringComparer.Ordinal);
        var withVectors = new List<AddAction>();
        foreach (AddAction file in files)
        {
            if (file.DeletionVector is not null)
            {
                withVectors.Add(file);
                continue;
            }
            IReadOnlyDictionary<string, DeltaType> oldTypes = OldTypes(Combine(tableRoot, file.DecodedPath), cloud, widened);
            string key = PartitionKey(snapshot, file) + "|" + string.Join(",", oldTypes.OrderBy(t => t.Key, StringComparer.Ordinal).Select(t => $"{t.Key}:{t.Value}"));
            if (!groups.TryGetValue(key, out var group)) groups[key] = group = ([], oldTypes);
            group.Files.Add(file);
        }

        var frames = new List<LazyFrameHandle>();
        try
        {
            foreach (var (group, oldTypes) in groups.Values)
            {
                using SchemaHandle fileSchema = PhysicalFileSchema(schema, partitionColumns, oldTypes: oldTypes);
                string[] paths = [.. group.Select(f => Combine(tableRoot, f.DecodedPath))];
                LazyFrameHandle lf = ScanFiles(paths, cloud, fileSchema, null, options);
                frames.Add(Project(lf, snapshot, group[0].PartitionValues, partitionColumns, oldTypes, keepPath));
            }

            foreach (AddAction file in withVectors)
                frames.Add(ScanFile(fileSystem, tableRoot, snapshot, file, cloud, options: options));

            if (frames.Count == 0)
            {
                using SchemaHandle logical = LogicalSchema(schema);
                using DataFrameHandle empty = PolarsWrapper.DataFrameFromSchema(logical, 0);
                LazyFrameHandle none = PolarsWrapper.DataFrameToLazy(empty);
                if (pathColumn is null) return none;
                using DataTypeHandle text = PolarsWrapper.NewPrimitiveType((int)PlDataType.String);
                return PolarsWrapper.LazyWithColumns(none, [PolarsWrapper.Alias(
                    PolarsWrapper.ExprCast(PolarsWrapper.LitNull(), PolarsWrapper.DataTypeExprFromDataType(text), strict: true, wrapNumerical: false), pathColumn)]);
            }
            LazyFrameHandle[] all = [.. frames];
            frames.Clear(); // from here the result owns them
            if (all.Length == 1) return all[0];
            return PolarsWrapper.LazyConcat(all, PlConcatType.Vertical, rechunk: options?.Rechunk ?? false, parallel: true, toSupertypes: false, maintainOrder: false);
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

    /// <summary>
    /// Scans one data file of a snapshot as table rows. Rows its deletion vector marks are left out, and so are
    /// <paramref name="dropRows"/>; with <paramref name="onlyRows"/> only those rows are kept. With
    /// <paramref name="withRowIndex"/> the file's row positions follow the table's columns, as Int64 in
    /// <see cref="RowIndexColumn"/>.
    /// </summary>
    internal static LazyFrameHandle ScanFile(
        IFileSystem fileSystem, string tableRoot, Snapshot snapshot, AddAction file, CloudArgs cloud,
        long[]? dropRows = null, long[]? onlyRows = null, bool withRowIndex = false, KernelScanOptions? options = null)
    {
        var partitionColumns = new HashSet<string>(snapshot.PartitionColumns, StringComparer.Ordinal);
        long[] dead = file.DeletionVector is null ? [] : DeletionVector.Read(fileSystem, file.DeletionVector).DeletedRows;
        if (dropRows is { Length: > 0 }) dead = DeletionVector.FromRows(dead.Concat(dropRows)).DeletedRows;
        bool needIndex = withRowIndex || dead.Length > 0 || onlyRows is not null;

        string path = Combine(tableRoot, file.DecodedPath);
        IReadOnlyDictionary<string, DeltaType> oldTypes = OldTypes(path, cloud, WidenedColumns(snapshot));
        using SchemaHandle fileSchema = PhysicalFileSchema(snapshot.Schema, partitionColumns, oldTypes: oldTypes);
        LazyFrameHandle lf = ScanFiles([path], cloud, fileSchema, needIndex ? RowIndexColumn : null, options);
        if (dead.Length > 0) lf = FilterRows(lf, dead, keep: false);
        if (onlyRows is not null) lf = FilterRows(lf, onlyRows, keep: true);
        string? pathColumn = options?.IncludePathColumn;
        if (!withRowIndex)
            return Project(lf, snapshot, file.PartitionValues, partitionColumns, oldTypes,
                pathColumn is null ? null : exprs => exprs.Add(PolarsWrapper.Col(pathColumn)));

        return Project(lf, snapshot, file.PartitionValues, partitionColumns, oldTypes, exprs =>
        {
            using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
            exprs.Add(PolarsWrapper.Alias(
                PolarsWrapper.ExprCast(PolarsWrapper.Col(RowIndexColumn), PolarsWrapper.DataTypeExprFromDataType(int64), strict: true, wrapNumerical: false),
                RowIndexColumn));
        });
    }

    private static readonly IReadOnlyDictionary<string, DeltaType> NoOldTypes = new Dictionary<string, DeltaType>();

    /// <summary>
    /// The physical names of the top-level data columns whose type was widened (type widening). The engine casts
    /// only some types up while scanning (not decimals, not dates to timestamps), so for these the scan reads each
    /// file's own type and casts it to the table's.
    /// </summary>
    /// <exception cref="DeltaException">A widening inside a nested column that the scan cannot cast.</exception>
    private static string[] WidenedColumns(Snapshot snapshot)
    {
        var columns = new List<string>();
        foreach (StructField field in snapshot.Schema.Fields)
        {
            if (snapshot.PartitionColumns.Contains(field.Name)) continue; // partition values are text
            var changes = TypeChanges(field);
            if (changes.Any(c => c.FieldPath is null)) columns.Add(field.PhysicalName);
            if (changes.Any(c => c.FieldPath is not null && !CastWhileScanning(c)) || NestedUncastable(field.Type))
                throw new DeltaException(
                    $"The column '{field.Name}' holds a field widened from one type to another that cannot be read yet inside a nested column.");
        }
        return [.. columns];

        static bool NestedUncastable(DeltaType type) => type switch
        {
            StructType s => s.Fields.Any(f => TypeChanges(f).Any(c => !CastWhileScanning(c)) || NestedUncastable(f.Type)),
            ArrayType a => NestedUncastable(a.ElementType),
            MapType m => NestedUncastable(m.KeyType) || NestedUncastable(m.ValueType),
            _ => false,
        };

        // Integer to a wider integer and float to double: the scan's lenient casts do these at any depth.
        static bool CastWhileScanning((string From, string To, string? FieldPath) c) =>
            (IsInteger(c.From) && IsInteger(c.To)) || (c.From == PrimitiveType.Float && c.To == PrimitiveType.Double);

        static bool IsInteger(string t) => t is PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Long;
    }

    /// <summary>The field's type widening history (<c>delta.typeChanges</c>).</summary>
    private static List<(string From, string To, string? FieldPath)> TypeChanges(StructField field)
    {
        var result = new List<(string, string, string?)>();
        if (!field.Metadata.TryGetValue("delta.typeChanges", out string? json)) return result;
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != System.Text.Json.JsonValueKind.Array) return result;
        foreach (var change in doc.RootElement.EnumerateArray())
        {
            string? Text(string name) => change.TryGetProperty(name, out var v) && v.ValueKind == System.Text.Json.JsonValueKind.String ? v.GetString() : null;
            result.Add((Text("fromType") ?? "", Text("toType") ?? "", Text("fieldPath")));
        }
        return result;
    }

    /// <summary>
    /// Of <paramref name="candidates"/>, the columns the file at <paramref name="path"/> holds in another type than
    /// the table's, with that type. Reads the file's schema only when there are candidates.
    /// </summary>
    private static IReadOnlyDictionary<string, DeltaType> OldTypes(string path, CloudArgs cloud, string[] candidates)
    {
        if (candidates.Length == 0) return NoOldTypes;
        using LazyFrameHandle lf = ScanFiles([path], cloud, null, null);
        var result = new Dictionary<string, DeltaType>(StringComparer.Ordinal);
        foreach (var (name, type) in PolarsSchemaExtractor.GetFields(lf))
        {
            using (type)
            {
                if (!candidates.Contains(name)) continue;
                DeltaType? old = PolarsWrapper.GetDataTypeKind(type) switch
                {
                    PlDataType.Int8 => new PrimitiveType(PrimitiveType.Byte),
                    PlDataType.Int16 => new PrimitiveType(PrimitiveType.Short),
                    PlDataType.Int32 => new PrimitiveType(PrimitiveType.Integer),
                    PlDataType.Float32 => new PrimitiveType(PrimitiveType.Float),
                    PlDataType.Date => new PrimitiveType(PrimitiveType.Date),
                    PlDataType.Decimal => DecimalOf(type),
                    _ => null, // the table's own type, or one the scan casts anyway
                };
                if (old is not null) result[name] = old;
            }
        }
        return result;

        static DecimalType DecimalOf(DataTypeHandle type)
        {
            PolarsWrapper.GetDecimalInfo(type, out int precision, out int scale);
            return new DecimalType(precision, scale);
        }
    }

    internal static LazyFrameHandle ScanFiles(
        string[] paths, CloudArgs cloud, SchemaHandle? schema, string? rowIndexName, KernelScanOptions? options = null) =>
        PolarsWrapper.ScanParquetFiles(
            paths,
            parallel: options?.Parallel ?? PlParallelStrategy.Auto,
            lowMemory: options?.LowMemory ?? false,
            useStatistics: options?.UseStatistics ?? true,
            ignoreExtraColumns: true,
            lenientCasts: true,
            rechunk: options?.Rechunk ?? false,
            cache: options?.Cache ?? true,
            rowIndexName: rowIndexName,
            includePathColumn: options?.IncludePathColumn,
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
        HashSet<string> partitionColumns, IReadOnlyDictionary<string, DeltaType>? oldTypes = null, Action<List<ExprHandle>>? extra = null)
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
                    ExprHandle column = PolarsWrapper.Col(field.PhysicalName);
                    if (oldTypes is not null && oldTypes.ContainsKey(field.PhysicalName))
                    {
                        // A file from before the column was widened: its values in the older type, cast up
                        // (a date becomes midnight of that day).
                        using DataTypeHandle target = ToEngineType(field.Type, physicalNames: false);
                        column = PolarsWrapper.ExprCast(column, PolarsWrapper.DataTypeExprFromDataType(target), strict: true, wrapNumerical: false);
                    }
                    exprs.Add(PolarsWrapper.Alias(ToLogical(column, field.Type), field.Name));
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

    /// <summary>Whether a column read under physical names has a field, at some depth, whose logical name differs.</summary>
    private static bool NeedsRenaming(DeltaType type) => type switch
    {
        StructType s => s.Fields.Any(f => f.PhysicalName != f.Name || NeedsRenaming(f.Type)),
        ArrayType a => NeedsRenaming(a.ElementType),
        MapType m => NeedsRenaming(m.KeyType) || NeedsRenaming(m.ValueType),
        _ => false,
    };

    /// <summary>
    /// Gives a column read under physical names (column mapping) the logical names of its struct fields, at every
    /// depth: inside structs, list elements and map entries. Takes ownership of <paramref name="physical"/>.
    /// A struct cast would not do: the engine casts struct fields by name.
    /// </summary>
    private static ExprHandle ToLogical(ExprHandle physical, DeltaType type)
    {
        if (!NeedsRenaming(type)) return physical;
        switch (type)
        {
            case StructType s when !s.Fields.Any(f => NeedsRenaming(f.Type)):
                // Only this level is renamed; the rename keeps null structs null.
                return PolarsWrapper.StructRenameFields(physical, [.. s.Fields.Select(f => f.Name)]);

            case StructType s:
            {
                var fields = new ExprHandle[s.Fields.Count];
                for (int i = 0; i < fields.Length; i++)
                {
                    ExprHandle field = PolarsWrapper.StructFieldByName(PolarsWrapper.CloneExpr(physical), [s.Fields[i].PhysicalName]);
                    fields[i] = PolarsWrapper.Alias(ToLogical(field, s.Fields[i].Type), s.Fields[i].Name);
                }
                // Rebuilt field by field, so a null struct is put back explicitly.
                return PolarsWrapper.IfElse(PolarsWrapper.IsNull(physical), PolarsWrapper.LitNull(), PolarsWrapper.AsStruct(fields));
            }

            case ArrayType a:
                return PolarsWrapper.ListEval(physical, ToLogical(PolarsWrapper.Col(""), a.ElementType));

            case MapType m:
            {
                // A map is a list of key/value structs; the entry names are fixed, their contents may be renamed.
                ExprHandle key = PolarsWrapper.Alias(ToLogical(PolarsWrapper.StructFieldByName(PolarsWrapper.Col(""), ["key"]), m.KeyType), "key");
                ExprHandle value = PolarsWrapper.Alias(ToLogical(PolarsWrapper.StructFieldByName(PolarsWrapper.Col(""), ["value"]), m.ValueType), "value");
                return PolarsWrapper.ListEval(physical, PolarsWrapper.AsStruct([key, value]));
            }

            default:
                return physical;
        }
    }

    /// <summary>The columns expected in a data file: every non-partition column under its physical name.</summary>
    private static SchemaHandle PhysicalFileSchema(
        StructType schema, HashSet<string> partitionColumns, bool withChangeType = false, IReadOnlyDictionary<string, DeltaType>? oldTypes = null)
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
            using DataTypeHandle type = oldTypes is not null && oldTypes.TryGetValue(field.PhysicalName, out DeltaType? old)
                ? ToEngineType(old, physicalNames: true)
                : ToEngineType(field.Type, physicalNames: true);
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
    /// <summary>The engine type a table column is read as (logical field names).</summary>
    internal static DataTypeHandle EngineType(DeltaType type) => ToEngineType(type, physicalNames: false);

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
