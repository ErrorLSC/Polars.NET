using System.Globalization;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Parquet;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>How a write treats a table that already exists.</summary>
internal enum KernelWriteMode
{
    /// <summary>Fail when the table exists.</summary>
    ErrorIfExists,
    /// <summary>Add the rows to the table.</summary>
    Append,
    /// <summary>Replace the rows of the table.</summary>
    Overwrite,
    /// <summary>Write nothing when the table exists.</summary>
    Ignore,
}

/// <summary>How a write lays out its data files.</summary>
/// <param name="MaxRowsPerFile">The most rows in one file; 0 for no limit.</param>
/// <param name="ApproxBytesPerFile">The approximate in-memory size of the rows of one file (on disk the file is
/// smaller, being compressed); null for the table's <c>delta.targetFileSize</c>, else 128 MiB; 0 for no limit.</param>
/// <param name="Compression">The Parquet compression.</param>
/// <param name="CompressionLevel">The compression level; -1 for the codec's default.</param>
/// <param name="RowGroupSize">Rows per row group; 0 for the engine's default.</param>
/// <param name="DataPageSize">Bytes per data page; 0 for the engine's default.</param>
/// <param name="KeysPreGrouped">The rows come grouped by partition already (lets the sink buffer less).</param>
/// <param name="MaintainOrder">Keep the order of the rows within each file.</param>
/// <param name="SyncOnClose">Sync local files to disk when they are closed.</param>
internal sealed record KernelWriteOptions(
    ulong MaxRowsPerFile = 0,
    ulong? ApproxBytesPerFile = null,
    PlParquetCompression Compression = PlParquetCompression.Snappy,
    int CompressionLevel = -1,
    int RowGroupSize = 0,
    int DataPageSize = 0,
    bool KeysPreGrouped = false,
    bool MaintainOrder = true,
    PlSyncOnClose SyncOnClose = PlSyncOnClose.None)
{
    /// <summary>The defaults.</summary>
    public static KernelWriteOptions Default { get; } = new();

    /// <summary>The file size Spark compacts toward, and the default here.</summary>
    public const long DefaultTargetFileSize = 128L * 1024 * 1024;
}

/// <summary>
/// Writes rows to a Delta table through the managed kernel: the engine streams the data files (split by partition
/// and size) at paths the kernel names, the kernel reads their statistics from the footers and commits them. The
/// public Delta API and the row-level operations call it.
/// </summary>
internal static class DeltaKernelWrite
{
    /// <summary>The longest string kept whole in statistics; a longer minimum is cut to this many characters.</summary>
    private const int StringStatsLength = 32;

    /// <summary>Writes the rows of <paramref name="data"/> and returns the version committed.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="data">The rows; consumed.</param>
    /// <param name="mode">What to do when the table exists.</param>
    /// <param name="partitionBy">Partition columns for a new table; for an existing one they must be its own, or null.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    /// <param name="configuration">Table properties for a new table.</param>
    /// <param name="canEvolve">Schema evolution, as Spark: an append adds the data's new columns (and, with type
    /// widening on, widens column types); an overwrite replaces the schema and partitioning with the data's.</param>
    public static long Write(
        string tablePath, LazyFrameHandle data, KernelWriteMode mode, string[]? partitionBy = null, CloudArgs? cloud = null,
        IReadOnlyDictionary<string, string>? configuration = null, bool canEvolve = false, KernelWriteOptions? options = null,
        string[]? clusterBy = null)
    {
        options ??= KernelWriteOptions.Default;
        using LazyFrameHandle owned = data; // consumed by the sink; disposed here when the write fails before it
        IWritableFileSystem store = TableStorage.Open(tablePath, cloud).Store;
        // Data files are deleted again if the write fails before its commit lands.
        var fileSystem = new TrackingFileSystem(store);
        try
        {
            Snapshot? existing = TryLoad(fileSystem, tablePath, cloud);
            if (existing is not null && mode == KernelWriteMode.ErrorIfExists)
                throw new PolarsException($"Table already exists at {tablePath} (a Delta table; the save mode is ErrorIfExists).");

            if (existing is not null && mode == KernelWriteMode.Ignore) return existing.Version;

            // Identity columns the data lacks are generated (one the data has is checked to allow it).
            IReadOnlyList<IdentityColumn> identity = existing is null ? [] : IdentityColumns.Of(existing.Schema);
            List<IdentityColumn> generated = [];
            using LazyFrameHandle? withIdentity = identity.Count == 0
                ? null
                : DeltaKernelIdentity.Fill(PolarsWrapper.LazyClone(data), identity, SchemaOf(data), out generated);
            LazyFrameHandle rows = withIdentity ?? data;
            StructType incoming = SchemaOf(rows);

            StructType schema;
            string[] partitions;
            Transaction transaction;
            MetadataAction? changed = null; // the metadata change of a write to an existing table
            if (existing is null)
            {
                partitions = partitionBy ?? [];
                MetadataAction metadata;
                (metadata, schema) = NewTableMetadata(incoming, partitions, configuration);
                List<StructField[]> clustered = Clustering.Resolve(schema, clusterBy ?? [], partitions, metadata.Configuration);
                ProtocolAction protocol = ProtocolFor(schema, metadata.Configuration);
                if (clustered.Count > 0) protocol = TableFeatures.WithFeatures(protocol, [Clustering.Feature]);
                transaction = Transaction.Create(fileSystem, protocol, metadata, "WRITE");
                if (clustered.Count > 0) transaction.Add(Clustering.DomainFor(clustered.Select(Clustering.PhysicalPath)));
            }
            else
            {
                transaction = Transaction.Start(fileSystem, existing, "WRITE");
                if (mode == KernelWriteMode.Overwrite && canEvolve && !SameShape(existing, incoming, partitionBy ?? []))
                {
                    // overwriteSchema, as Spark: the data's schema replaces the table's, and so does the partitioning:
                    // partitionBy, or none when it is not given (checked against delta-spark 4.4).
                    partitions = partitionBy ?? [];
                    foreach (string column in partitions)
                    {
                        StructField field = incoming.Field(column) ?? throw new PolarsException($"The partition column '{column}' is not in the data.");
                        if (!IsPartitionable(field.Type))
                            throw new PolarsException($"The column '{column}' of type {field.Type} cannot be a partition column.");
                    }
                    changed = SchemaEvolution.Replace(existing.Metadata, incoming, partitions);
                    schema = StructType.Parse(changed.SchemaString);
                    SchemaEvolution.EnsureSupported(existing.Protocol, schema);
                    // The clustering columns stay; under column mapping they now have new physical names.
                    if (ClusteringNames(existing) is { Length: > 0 } names)
                        transaction.Add(Clustering.DomainFor(
                            Clustering.Resolve(schema, names, partitions, changed.Configuration).Select(Clustering.PhysicalPath)));
                }
                else
                {
                    partitions = [.. existing.PartitionColumns];
                    if (partitionBy is not null && !partitionBy.SequenceEqual(partitions))
                        throw new PolarsException(
                            $"The table is partitioned by [{string.Join(", ", partitions)}], not [{string.Join(", ", partitionBy)}]" +
                            (mode == KernelWriteMode.Overwrite ? "; overwrite with schema evolution to change it." : "."));
                    schema = SchemaEvolution.Merge(existing.Metadata, existing.Protocol, incoming, canEvolve, out changed);
                }
            }
            if (existing is not null && clusterBy is { Length: > 0 }
                && !(ClusteringNames(existing) ?? []).SequenceEqual(clusterBy, StringComparer.OrdinalIgnoreCase))
                throw new PolarsException(
                    $"The table is clustered by [{string.Join(", ", ClusteringNames(existing) ?? [])}], not [{string.Join(", ", clusterBy)}]; " +
                    "change the clustering columns with Delta.ClusterBy.");
            transaction.WithParameter("mode", mode switch
            {
                KernelWriteMode.Overwrite => "Overwrite",
                KernelWriteMode.Append => "Append",
                KernelWriteMode.Ignore => "Ignore",
                _ => "ErrorIfExists",
            })
                       .WithParameter("partitionBy", JsonText.StringArray(partitions));

            // The rows streamed in the table's column order and exact types, split by partition and size.
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            IReadOnlyDictionary<string, string>? tableProperties = existing?.Metadata.Configuration ?? configuration;
            List<AddAction> adds = StreamFiles(fileSystem, tablePath, rows, schema, partitions, now, options, tableProperties, cloud);
            foreach (AddAction add in adds) transaction.Add(add);
            if (existing is not null)
            {
                // The metadata goes in once the files are written: the identity values generated move the high water marks.
                MetadataAction? final = DeltaKernelIdentity.WithMarks(changed, existing.Metadata, DeltaKernelIdentity.Marks(generated, DeltaKernelIdentity.RowsOf(adds)));
                if (final is not null) transaction.SetMetadata(final);
            }

            if (mode == KernelWriteMode.Overwrite && existing is not null)
            {
                // Replacing the table depends on all of it: a concurrent change to it is a conflict.
                transaction.ReadPredicate(null).ReadFiles(existing.Files);
                foreach (AddAction old in existing.Files)
                {
                    transaction.Add(new RemoveAction
                    {
                        Path = old.Path,
                        DeletionTimestamp = now,
                        DataChange = true,
                        ExtendedFileMetadata = true,
                        PartitionValues = old.PartitionValues,
                        Size = old.Size,
                        DeletionVector = old.DeletionVector,
                        BaseRowId = old.BaseRowId,
                        DefaultRowCommitVersion = old.DefaultRowCommitVersion,
                    });
                }
            }
            long version = transaction.Commit();
            TryCheckpoint(fileSystem, tablePath, version, cloud);
            return version;
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            try
            {
                fileSystem.Rollback();
            }
            finally
            {
                fileSystem.Dispose();
            }
        }
    }

    /// <summary>
    /// Writes a checkpoint when one is due at <paramref name="version"/>. A checkpoint only speeds up reading: when it
    /// cannot be written the commit stands, and a later commit writes the next one.
    /// </summary>
    internal static void TryCheckpoint(IWritableFileSystem fileSystem, string tablePath, long version, CloudArgs? cloud)
    {
        try
        {
            Snapshot snapshot = Snapshot.Load(fileSystem, version, new EngineParquetReader(RootOf(fileSystem, tablePath), cloud));
            if (!Checkpoints.IsDue(snapshot)) return;
            Checkpoints.Write(fileSystem, snapshot);
            // With a new checkpoint, older log files may have become unnecessary.
            LogCleanup.Run(fileSystem, snapshot);
        }
        catch (Exception e) when (e is DeltaException or PolarsException or IOException)
        {
            System.Diagnostics.Trace.TraceWarning($"No checkpoint was written at version {version} of {tablePath}: {e.Message}");
        }
    }

    internal static Snapshot? TryLoad(IFileSystem fileSystem, string tablePath, CloudArgs? cloud)
    {
        try
        {
            return Snapshot.Load(fileSystem, parquet: new EngineParquetReader(RootOf(fileSystem, tablePath), cloud));
        }
        catch (DeltaTableNotFoundException)
        {
            return null;
        }
    }

    /// <summary>The logical names of a table's clustering columns; null when it is not clustered.</summary>
    internal static string[]? ClusteringNames(Snapshot table) =>
        Clustering.PhysicalColumns(table.DomainMetadata) is { } columns
            ? [.. Clustering.Fields(table.Schema, columns).Select(Clustering.LogicalName)]
            : null;

    /// <summary>Whether data of <paramref name="incoming"/> written with <paramref name="partitionBy"/> leaves the table as it is.</summary>
    private static bool SameShape(Snapshot existing, StructType incoming, string[] partitionBy) =>
        partitionBy.SequenceEqual(existing.PartitionColumns)
        && incoming.Fields.Count == existing.Schema.Fields.Count
        && incoming.Fields.Zip(existing.Schema.Fields).All(p => p.First.Name == p.Second.Name && p.First.Type.ToString() == p.Second.Type.ToString());

    /// <summary>The location data file paths are relative to: the local directory, or the URL.</summary>
    internal static string RootOf(IFileSystem fileSystem, string tablePath) => TableStorage.RootOf(fileSystem, tablePath);

    /// <summary>
    /// The lowest protocol for the schema and table properties: reader 1 / writer 2, writer 4 for the change data
    /// feed, reader 3 / writer 7 with features listed once a type or property needs a table feature.
    /// </summary>
    internal static ProtocolAction ProtocolFor(StructType schema, IReadOnlyDictionary<string, string> configuration)
    {
        bool On(string key) => configuration.TryGetValue(key, out string? v) && string.Equals(v, "true", StringComparison.OrdinalIgnoreCase);
        bool ntz = AnyType(schema, t => t is PrimitiveType { Name: PrimitiveType.TimestampNtz });
        bool vectors = On(TableFeatures.EnableDeletionVectorsKey);
        bool changeFeed = On(ChangeFeed.EnableKey);
        bool mapping = configuration.TryGetValue(TableFeatures.ColumnMappingModeKey, out string? mode) && mode is "name" or "id";
        bool widening = On(SchemaEvolution.EnableTypeWideningKey);
        bool inCommitTimestamps = On(Kaminari.Seseragi.Lakehouse.Delta.Log.CommitTimeline.InCommitTimestampsKey);
        bool v2Checkpoint = configuration.TryGetValue("delta.checkpointPolicy", out string? policy) && policy == "v2";
        bool rowTracking = On(RowTracking.EnableKey);

        if (!ntz && !vectors && !widening && !inCommitTimestamps && !v2Checkpoint && !rowTracking)
            return mapping
                ? new ProtocolAction { MinReaderVersion = 2, MinWriterVersion = 5 } // column mapping implies the change data feed's writer 4
                : new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = changeFeed ? 4 : 2 };

        // A feature no legacy version implies: table features, listed from 1/2 up as an ALTER would list them
        // (writer-only features keep reader 1, column mapping alone reader 2).
        var features = new List<string>();
        if (changeFeed) features.Add(TableFeatures.ChangeDataFeed);
        if (ntz) features.Add(TableFeatures.TimestampNtz);
        if (vectors) features.Add(TableFeatures.DeletionVectors);
        if (mapping) features.Add(TableFeatures.ColumnMapping);
        if (widening) features.Add(TableFeatures.TypeWidening);
        if (inCommitTimestamps) features.Add(TableFeatures.InCommitTimestamp);
        if (v2Checkpoint) features.Add(TableFeatures.V2Checkpoint);
        if (rowTracking) features.Add(RowTracking.Feature);
        return TableFeatures.WithFeatures(new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, features);
    }

    /// <summary>
    /// The metadata of a new table, and its schema as the table keeps it: partition columns checked, row tracking
    /// columns named when it is on, ids and physical names given under column mapping.
    /// </summary>
    internal static (MetadataAction Metadata, StructType Schema) NewTableMetadata(
        StructType schema, string[] partitions, IReadOnlyDictionary<string, string>? configuration)
    {
        foreach (string column in partitions)
        {
            StructField field = schema.Field(column) ?? throw new PolarsException($"The partition column '{column}' is not in the data.");
            if (!IsPartitionable(field.Type))
                throw new PolarsException($"The column '{column}' of type {field.Type} cannot be a partition column.");
        }
        var metadata = new MetadataAction
        {
            Id = Guid.NewGuid().ToString(),
            SchemaString = schema.ToJson(),
            PartitionColumns = partitions,
            Configuration = WithRowTrackingNames(configuration ?? new Dictionary<string, string>()),
            CreatedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };
        if (TableFeatures.ColumnMappingModeOf(metadata) != ColumnMappingMode.None)
        {
            // Column mapping from the start: every column gets an id and a physical name.
            metadata = SchemaEvolution.Replace(metadata, schema, partitions);
            schema = StructType.Parse(metadata.SchemaString);
        }
        return (metadata, schema);
    }

    /// <summary>A new table's properties: with row tracking on, the names of its materialized columns too (as Spark).</summary>
    private static IReadOnlyDictionary<string, string> WithRowTrackingNames(IReadOnlyDictionary<string, string> configuration) =>
        configuration.TryGetValue(RowTracking.EnableKey, out string? on) && string.Equals(on, "true", StringComparison.OrdinalIgnoreCase)
            ? RowTracking.WithMaterializedColumnNames(configuration)
            : configuration;

    private static bool AnyType(DeltaType type, Func<DeltaType, bool> test) => test(type) || type switch
    {
        StructType s => s.Fields.Any(f => AnyType(f.Type, test)),
        ArrayType a => AnyType(a.ElementType, test),
        MapType m => AnyType(m.KeyType, test) || AnyType(m.ValueType, test),
        _ => false,
    };

    private static bool IsPartitionable(DeltaType type) =>
        type is DecimalType || type is PrimitiveType { Name: not (PrimitiveType.Binary or PrimitiveType.Variant) };

    // ------------------------------------------------------------------
    // Schema: engine types to Delta types
    // ------------------------------------------------------------------

    internal static StructType SchemaOf(DataFrameHandle df)
    {
        using LazyFrameHandle lf = PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(df));
        return SchemaOf(lf);
    }

    /// <summary>The Delta schema of a lazy frame's columns, from its plan (nothing is computed).</summary>
    internal static StructType SchemaOf(LazyFrameHandle lf)
    {
        var fields = new List<StructField>();
        foreach (var (name, type) in PolarsSchemaExtractor.GetFields(lf))
        {
            using (type)
                fields.Add(new StructField { Name = name, Type = DeltaTypeOf(type, name) });
        }
        return new StructType(fields);
    }

    /// <summary>The Delta type that holds the values of an engine type; the column is cast to it on write.</summary>
    internal static DeltaType DeltaTypeOf(DataTypeHandle type, string column)
    {
        PlDataType kind = PolarsWrapper.GetDataTypeKind(type);
        switch (kind)
        {
            case PlDataType.Boolean: return new PrimitiveType(PrimitiveType.Boolean);
            case PlDataType.Int8: return new PrimitiveType(PrimitiveType.Byte);
            case PlDataType.Int16 or PlDataType.UInt8: return new PrimitiveType(PrimitiveType.Short);
            case PlDataType.Int32 or PlDataType.UInt16: return new PrimitiveType(PrimitiveType.Integer);
            case PlDataType.Int64 or PlDataType.UInt32: return new PrimitiveType(PrimitiveType.Long);
            case PlDataType.Float16 or PlDataType.Float32: return new PrimitiveType(PrimitiveType.Float);
            case PlDataType.Float64: return new PrimitiveType(PrimitiveType.Double);
            case PlDataType.String or PlDataType.Categorical or PlDataType.Enum: return new PrimitiveType(PrimitiveType.String);
            case PlDataType.Binary: return new PrimitiveType(PrimitiveType.Binary);
            case PlDataType.Date: return new PrimitiveType(PrimitiveType.Date);
            case PlDataType.Datetime:
                return new PrimitiveType(string.IsNullOrEmpty(PolarsWrapper.GetTimeZone(type)) ? PrimitiveType.TimestampNtz : PrimitiveType.Timestamp);
            case PlDataType.Decimal:
            {
                PolarsWrapper.GetDecimalInfo(type, out int precision, out int scale);
                if (precision > 38) throw new PolarsException($"The column '{column}' has decimal precision {precision}; Delta allows up to 38.");
                return new DecimalType(precision, scale);
            }
            case PlDataType.List or PlDataType.Array:
            {
                using DataTypeHandle inner = PolarsWrapper.GetInnerType(type);
                return new ArrayType(DeltaTypeOf(inner, column), containsNull: true);
            }
            case PlDataType.Struct:
            {
                var fields = new List<StructField>();
                ulong count = PolarsWrapper.GetStructLen(type);
                for (ulong i = 0; i < count; i++)
                {
                    PolarsWrapper.GetStructField(type, i, out string name, out DataTypeHandle fieldType);
                    using (fieldType)
                        fields.Add(new StructField { Name = name, Type = DeltaTypeOf(fieldType, $"{column}.{name}") });
                }
                return new StructType(fields);
            }
            default:
                throw new PolarsException(
                    $"The column '{column}' has the type {PolarsWrapper.GetDataTypeString(type)}, which no Delta type holds.");
        }
    }

    /// <summary>
    /// Rejects rows with a null in a column the schema declares <c>nullable: false</c>. A nested field counts only
    /// where the struct holding it is not null, as Spark checks it. Arrays and maps are not checked.
    /// </summary>
    internal static void EnsureNotNull(DataFrameHandle df, StructType schema)
    {
        var checks = new List<(string Path, Func<ExprHandle> Violation)>();
        foreach (StructField field in schema.Fields)
            Collect(field, [field.Name], parentsPresent: null);
        if (checks.Count == 0 || PolarsWrapper.DataFrameHeight(df) == 0) return;

        ExprHandle[] exprs = [.. checks.Select((c, i) => PolarsWrapper.Alias(PolarsWrapper.Any(c.Violation(), ignoreNulls: true), $"c{i}"))];
        using JsonDocument row = OneRow(df, exprs);
        string[] violated = [.. checks.Where((_, i) => row.RootElement.GetProperty($"c{i}").ValueKind == JsonValueKind.True).Select(c => c.Path)];
        if (violated.Length > 0)
            throw new PolarsException(
                $"The data has null values in columns the table declares NOT NULL: {string.Join(", ", violated)}.");

        void Collect(StructField field, string[] path, Func<ExprHandle>? parentsPresent)
        {
            ExprHandle column() => Access(path);
            if (!field.Nullable)
            {
                Func<ExprHandle>? present = parentsPresent;
                checks.Add((string.Join('.', path), present is null
                    ? () => PolarsWrapper.IsNull(column())
                    : () => PolarsWrapper.And(present(), PolarsWrapper.IsNull(column()))));
            }
            if (field.Type is StructType nested)
            {
                Func<ExprHandle> here = parentsPresent is null
                    ? () => PolarsWrapper.IsNotNull(column())
                    : () => PolarsWrapper.And(parentsPresent(), PolarsWrapper.IsNotNull(column()));
                foreach (StructField child in nested.Fields)
                    Collect(child, [.. path, child.Name], here);
            }
        }

        static ExprHandle Access(string[] path)
        {
            ExprHandle e = PolarsWrapper.Col(path[0]);
            for (int i = 1; i < path.Length; i++) e = PolarsWrapper.StructFieldByName(e, [path[i]]);
            return e;
        }
    }

    /// <summary>The rows in the table's column order, every column cast to the exact type the table reads.</summary>
    internal static DataFrameHandle Normalize(DataFrameHandle df, StructType schema)
    {
        var present = SchemaOf(df).Fields.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        var exprs = new ExprHandle[schema.Fields.Count];
        for (int i = 0; i < exprs.Length; i++)
        {
            StructField field = schema.Fields[i];
            using DataTypeHandle target = DeltaKernelScan.EngineType(field.Type);
            // A column the data lacks is null (the schema check allowed it).
            ExprHandle value = present.Contains(field.Name) ? PolarsWrapper.Col(field.Name) : PolarsWrapper.LitNull();
            exprs[i] = PolarsWrapper.Alias(
                PolarsWrapper.ExprCast(value, PolarsWrapper.DataTypeExprFromDataType(target), strict: true, wrapNumerical: false),
                field.Name);
        }
        LazyFrameHandle lf = PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(df));
        return PolarsWrapper.LazyCollect(PolarsWrapper.LazySelect(lf, exprs), PlEngine.Auto);
    }

    // ------------------------------------------------------------------
    // Data files
    // ------------------------------------------------------------------

    /// <summary>
    /// Streams the rows of <paramref name="data"/> (consumed) into data files: the engine's partitioned sink splits
    /// them by partition and size and writes each file at a path chosen here; the statistics come from the file
    /// footers afterwards. Nothing is collected, except to check NOT NULL fields inside structs.
    /// </summary>
    private static List<AddAction> StreamFiles(
        TrackingFileSystem fileSystem, string tablePath, LazyFrameHandle data, StructType schema, string[] partitions, long now,
        KernelWriteOptions options, IReadOnlyDictionary<string, string>? tableProperties, CloudArgs? cloud) =>
        DataFiles(
            fileSystem, RootOf(fileSystem, tablePath), NormalizeLazy(data, schema), schema, partitions, now, tableProperties, checkNotNull: true,
            options, options.ApproxBytesPerFile ?? TargetFileSize(tableProperties), materialized: null, cloud);

    /// <summary>
    /// Streams the rows of a row-level operation or optimize (table columns, logical names; consumed) into data
    /// files, split by partition and size as a write splits them; statistics from the file footers. No rows, no
    /// files. Under row tracking the rows carry <see cref="DmlTable.RowIdColumns"/>, written as the table's
    /// materialized columns (null where a row takes the file's base id or the commit's version).
    /// </summary>
    /// <param name="schema">The table's schema after the operation (a merge may evolve it).</param>
    /// <param name="checkNotNull">Check the table's NOT NULL columns (rows with new values); rows kept as they
    /// were need no check.</param>
    /// <param name="maxRowsPerFile">The most rows in one file; 0 for no limit.</param>
    /// <param name="approxBytesPerFile">The approximate in-memory size of the rows of one file; null for the
    /// table's <c>delta.targetFileSize</c>, else 128 MiB; 0 for no limit.</param>
    /// <param name="bucketColumn">A column of the rows (not of the table) whose values split the rows into files, as
    /// a partition column would but without directories or partition values (clustering's buckets); null for none.</param>
    internal static List<AddAction> WriteFiles(
        DmlTable table, LazyFrameHandle rows, StructType schema, long now, bool checkNotNull,
        ulong maxRowsPerFile = 0, ulong? approxBytesPerFile = null, string? bucketColumn = null)
    {
        IReadOnlyDictionary<string, string> properties = table.Snapshot.Metadata.Configuration;
        string[] extra = bucketColumn is null ? table.RowIdColumns : [.. table.RowIdColumns, bucketColumn];
        return DataFiles(
            table.FileSystem, table.Root, NormalizeLazy(rows, schema, extra), schema, table.PartitionColumns, now, properties, checkNotNull,
            KernelWriteOptions.Default with { MaxRowsPerFile = maxRowsPerFile }, approxBytesPerFile ?? TargetFileSize(properties),
            table.MaterializedRowColumns, table.Cloud, bucketColumn);
    }

    /// <summary>
    /// Streams change rows (table columns, logical names, then <see cref="DeltaKernelScan.ChangeTypeColumn"/>;
    /// consumed) into change data files under <c>_change_data</c>, split by partition and size. No rows, no files.
    /// </summary>
    internal static List<CdcAction> WriteChangeFiles(DmlTable table, LazyFrameHandle rows, StructType schema)
    {
        string[] partitions = table.PartitionColumns;
        List<SunkFile> files = SinkFiles(
            table.FileSystem, table.Root, NormalizeLazy(rows, schema, [.. table.RowIdColumns, DeltaKernelScan.ChangeTypeColumn]), schema, partitions,
            changeData: true, KernelWriteOptions.Default, TargetFileSize(table.Snapshot.Metadata.Configuration), table.MaterializedRowColumns, table.Cloud);
        return [.. files.Select(f => new CdcAction { Path = EncodeLogPath(f.Relative), PartitionValues = Logged(f.Values, schema, partitions), Size = f.Size })];
    }

    /// <summary>Data files of normalized rows (consumed), with statistics and NOT NULL checks from their footers.</summary>
    private static List<AddAction> DataFiles(
        TrackingFileSystem fileSystem, string root, LazyFrameHandle rows, StructType schema, string[] partitions, long now,
        IReadOnlyDictionary<string, string>? tableProperties, bool checkNotNull, KernelWriteOptions options, ulong approxBytesPerFile,
        (string RowId, string CommitVersion)? materialized, CloudArgs? cloud, string? bucketColumn = null)
    {
        if (checkNotNull && schema.Fields.Any(f => (f.Type is StructType s && HasNotNullInside(s)) || (!f.Nullable && f.Type is StructType or ArrayType or MapType)))
        {
            // Footers count nulls of primitive columns only (and a null inside a null struct is fine, which they
            // cannot tell apart): check NOT NULL nested columns on the rows.
            using DataFrameHandle collected = PolarsWrapper.LazyCollect(rows, PlEngine.Auto);
            EnsureNotNull(collected, schema);
            rows = PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(collected));
        }

        List<SunkFile> files = SinkFiles(
            fileSystem, root, rows, schema, partitions, changeData: false, options, approxBytesPerFile, materialized, cloud, bucketColumn);

        StructField[] dataFields = [.. schema.Fields.Where(f => !partitions.Contains(f.Name))];
        // NOT NULL primitive columns are checked on the footers' null counts (nested ones were checked on the rows).
        StructField[] notNullLeaves = checkNotNull
            ? [.. dataFields.Where(f => !f.Nullable && f.Type is not (StructType or ArrayType or MapType))]
            : [];
        IReadOnlyList<StructField[]> indexed = FileStatistics.IndexedLeaves(dataFields, tableProperties);
        var adds = new List<AddAction>(files.Count);
        foreach (SunkFile file in files)
        {
            if (notNullLeaves.Length > 0)
            {
                IReadOnlyDictionary<string, long> nulls = FileStatistics.NullCounts(file.Footer);
                string[] violated = [.. notNullLeaves.Where(f => !nulls.TryGetValue(f.PhysicalName, out long n) || n > 0).Select(f => f.Name)];
                if (violated.Length > 0)
                    throw new PolarsException($"The data has null values in columns the table declares NOT NULL: {string.Join(", ", violated)}.");
            }
            adds.Add(new AddAction
            {
                Path = EncodeLogPath(file.Relative),
                PartitionValues = Logged(file.Values, schema, partitions),
                Size = file.Size,
                ModificationTime = now,
                DataChange = true,
                Stats = FileStatistics.FromParquet(file.Footer, indexed),
            });
        }
        return adds;
    }

    /// <summary>A file the sink wrote: its table-relative path, partition values (logical names), size and footer.</summary>
    private readonly record struct SunkFile(string Relative, Dictionary<string, string?> Values, long Size, ParquetMetadata Footer);

    /// <summary>
    /// Sinks normalized rows (consumed; for change data followed by the change type column) as Parquet files named
    /// the way Spark names them, each tracked before it is written, and reads back their sizes and footers. The
    /// sink writes no file for no rows.
    /// </summary>
    private static List<SunkFile> SinkFiles(
        TrackingFileSystem fileSystem, string root, LazyFrameHandle rows, StructType schema, string[] partitions, bool changeData,
        KernelWriteOptions options, ulong approxBytesPerFile, (string RowId, string CommitVersion)? materialized, CloudArgs? cloud,
        string? bucketColumn = null)
    {
        StructField[] dataFields = [.. schema.Fields.Where(f => !partitions.Contains(f.Name))];
        EnsureNoMaps(dataFields);
        // Partition columns under their names (the sink keys on them and leaves them out of the files), data
        // columns under their physical names, then the materialized row tracking columns, then the change type.
        ExprHandle[] columns =
        [
            .. partitions.Select(PolarsWrapper.Col),
            // A bucket column is a sink key like the partition columns: it splits files and is left out of them.
            .. bucketColumn is null ? Array.Empty<ExprHandle>() : [PolarsWrapper.Col(bucketColumn)],
            .. dataFields.Select(f => PolarsWrapper.Alias(DeltaKernelScan.Renamed(PolarsWrapper.Col(f.Name), f.Type, toPhysical: true), f.PhysicalName)),
            .. materialized is var (rowId, commitVersion)
                ? [PolarsWrapper.Alias(PolarsWrapper.Col(DeltaKernelScan.RowIdColumn), rowId),
                   PolarsWrapper.Alias(PolarsWrapper.Col(DeltaKernelScan.RowCommitVersionColumn), commitVersion)]
                : Array.Empty<ExprHandle>(),
            .. changeData ? [PolarsWrapper.Col(DeltaKernelScan.ChangeTypeColumn)] : Array.Empty<ExprHandle>(),
        ];
        using LazyFrameHandle shaped = PolarsWrapper.LazySelect(rows, columns); // consumed by the sink

        var written = new List<(string Relative, Dictionary<string, string?> Values)>();
        string suffix = CodecSuffix(options.Compression);
        string[] notNullPartitions = [.. partitions.Where(p => !schema.Field(p)!.Nullable)];
        string[] sinkKeys = bucketColumn is null ? partitions : [.. partitions, bucketColumn];
        HostPathSink.Sink(
            shaped, root, sinkKeys, options.KeysPreGrouped, options.MaxRowsPerFile, approxBytesPerFile,
            (keys, index) =>
            {
                Dictionary<string, string?> values = PartitionValues(keys, schema, partitions);
                foreach (string p in notNullPartitions)
                {
                    if (values[p] is null)
                        throw new PolarsException($"The data has null values in columns the table declares NOT NULL: {p}.");
                }
                string directory = (changeData ? "_change_data/" : "") + string.Concat(partitions.Select(p =>
                    $"{EscapePartitionName(schema.Field(p)!.PhysicalName)}={EscapePartitionName(values[p] ?? HiveDefaultPartition)}/"));
                // Spark's names: part-00000-UUID-c000.snappy.parquet, cdc-00000-UUID.c000.snappy.parquet.
                string relative = changeData
                    ? $"{directory}cdc-{index:D5}-{Guid.NewGuid()}.c000{suffix}"
                    : $"{directory}part-{index:D5}-{Guid.NewGuid()}-c000{suffix}";
                fileSystem.Track(relative); // deleted again if the operation fails
                written.Add((relative, values));
                return relative;
            },
            options.Compression, options.CompressionLevel, options.RowGroupSize, options.DataPageSize, options.MaintainOrder,
            options.SyncOnClose, cloud, FieldIds(dataFields));

        // Sizes and footers: one read of each file's end where the store can, else a listing per directory.
        (long Size, ParquetMetadata Footer)[] footers;
        try
        {
            footers = ParquetFooters.Read(fileSystem, [.. written.Select(w => w.Relative)]);
        }
        catch (FileNotFoundException e)
        {
            throw new PolarsException(e.Message);
        }
        return [.. written.Select((w, i) => new SunkFile(w.Relative, w.Values, footers[i].Size, footers[i].Footer))];
    }

    /// <summary>
    /// The Parquet field ids of the data columns under column mapping (null without), as Spark writes them: every
    /// struct field at any depth (inside list elements and map entries too) gets its column mapping id. In the
    /// sink's form: <c>path TAB id</c> lines, a path being the physical names from the top joined by U+001F, with
    /// <c>[]</c> for a list's element (a map is a list of key/value structs).
    /// </summary>
    internal static string? FieldIds(IEnumerable<StructField> dataFields)
    {
        var lines = new StringBuilder();
        foreach (StructField field in dataFields) Field(field, null);
        return lines.Length == 0 ? null : lines.ToString();

        void Field(StructField field, string? parent)
        {
            string path = parent is null ? field.PhysicalName : parent + PathSeparator + field.PhysicalName;
            if (field.FieldId is int id) lines.Append(path).Append('\t').Append(id.ToString(CultureInfo.InvariantCulture)).Append('\n');
            Nested(field.Type, path);
        }

        void Nested(DeltaType type, string path)
        {
            switch (type)
            {
                case StructType s:
                    foreach (StructField child in s.Fields) Field(child, path);
                    break;
                case ArrayType a:
                    Nested(a.ElementType, path + PathSeparator + "[]");
                    break;
                case MapType m:
                    Nested(m.KeyType, path + PathSeparator + "[]" + PathSeparator + "key");
                    Nested(m.ValueType, path + PathSeparator + "[]" + PathSeparator + "value");
                    break;
            }
        }
    }

    private const char PathSeparator = '\u001f';

    /// <summary>
    /// Refuses writing files with map columns (at any depth). The engine has no map type: it holds a map as a list
    /// of key/value structs and would write it as a Parquet LIST, which Spark and other readers then refuse to read
    /// as the table's map, so a rewrite would leave the table unreadable to them.
    /// TEMPORARY: remove once the engine writes Parquet MAP columns (an upstream Polars release with a map type).
    /// </summary>
    internal static void EnsureNoMaps(IEnumerable<StructField> dataFields)
    {
        string[] maps = [.. dataFields.Where(f => HasMap(f.Type)).Select(f => f.Name)];
        if (maps.Length > 0)
            throw new PolarsException(
                $"The table has map columns ({string.Join(", ", maps)}), which cannot be written yet in a form other readers " +
                "(Spark, Databricks) read, so data files of this table are not written. Deletes on a table with deletion " +
                "vectors (delta.enableDeletionVectors) need no rewrite and are not affected.");

        static bool HasMap(DeltaType type) => type switch
        {
            MapType => true,
            StructType s => s.Fields.Any(f => HasMap(f.Type)),
            ArrayType a => HasMap(a.ElementType),
            _ => false,
        };
    }

    /// <summary>Partition values as the log keys them: by physical column name.</summary>
    private static Dictionary<string, string?> Logged(Dictionary<string, string?> values, StructType schema, string[] partitions) =>
        partitions.ToDictionary(p => schema.Field(p)!.PhysicalName, p => values[p]);

    private static bool HasNotNullInside(StructType type) =>
        type.Fields.Any(f => !f.Nullable || (f.Type is StructType s && HasNotNullInside(s)));

    /// <summary>
    /// The file size the table aims at: <c>delta.targetFileSize</c> as a byte count (delta-rs) or a size such as
    /// <c>128mb</c> (Databricks; b, k, m, g, t with an optional b, 1024-based), else 128 MiB.
    /// </summary>
    /// <exception cref="PolarsException">The property is set to something that is not a positive size.</exception>
    internal static ulong TargetFileSize(IReadOnlyDictionary<string, string>? properties)
    {
        if (properties is null || !properties.TryGetValue("delta.targetFileSize", out string? text))
            return (ulong)KernelWriteOptions.DefaultTargetFileSize;
        string value = text.Trim().ToLowerInvariant();
        int digits = 0;
        while (digits < value.Length && char.IsAsciiDigit(value[digits])) digits++;
        ulong unit = value[digits..].TrimStart() switch
        {
            "" or "b" => 1,
            "k" or "kb" => 1UL << 10,
            "m" or "mb" => 1UL << 20,
            "g" or "gb" => 1UL << 30,
            "t" or "tb" => 1UL << 40,
            _ => 0,
        };
        if (digits == 0 || unit == 0 || !ulong.TryParse(value.AsSpan(0, digits), NumberStyles.None, CultureInfo.InvariantCulture, out ulong count)
            || count == 0 || count > ulong.MaxValue / unit)
            throw new PolarsException($"The table property delta.targetFileSize is '{text}', which is not a size (such as 134217728 or 128mb).");
        return count * unit;
    }

    /// <summary>The file name ending Spark gives a codec.</summary>
    private static string CodecSuffix(PlParquetCompression compression) => compression switch
    {
        PlParquetCompression.Uncompressed => ".parquet",
        PlParquetCompression.Snappy => ".snappy.parquet",
        PlParquetCompression.Gzip => ".gz.parquet",
        PlParquetCompression.Brotli => ".br.parquet",
        PlParquetCompression.ZSTD => ".zstd.parquet",
        PlParquetCompression.Lz4Raw => ".lz4raw.parquet",
        _ => ".parquet",
    };

    /// <summary>
    /// The rows (consumed) in the table's column order, every column cast to the exact type the table reads,
    /// followed by the <paramref name="extraColumns"/> as they are.
    /// </summary>
    internal static LazyFrameHandle NormalizeLazy(LazyFrameHandle lf, StructType schema, string[]? extraColumns = null)
    {
        var present = SchemaOf(lf).Fields.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        var exprs = new ExprHandle[schema.Fields.Count + (extraColumns?.Length ?? 0)];
        for (int i = 0; i < schema.Fields.Count; i++)
        {
            StructField field = schema.Fields[i];
            using DataTypeHandle target = DeltaKernelScan.EngineType(field.Type);
            // A column the data lacks is null (the schema check allowed it).
            ExprHandle value = present.Contains(field.Name) ? PolarsWrapper.Col(field.Name) : PolarsWrapper.LitNull();
            exprs[i] = PolarsWrapper.Alias(
                PolarsWrapper.ExprCast(value, PolarsWrapper.DataTypeExprFromDataType(target), strict: true, wrapNumerical: false),
                field.Name);
        }
        for (int i = 0; i < (extraColumns?.Length ?? 0); i++) exprs[schema.Fields.Count + i] = PolarsWrapper.Col(extraColumns![i]);
        return PolarsWrapper.LazySelect(lf, exprs);
    }

    private const string HiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

    /// <summary>Escapes a partition directory name the way Hive and Spark do.</summary>
    private static string EscapePartitionName(string value)
    {
        var sb = new StringBuilder(value.Length);
        foreach (char c in value)
        {
            if (c < 0x20 || c == 0x7F || "\"#%'*/:=?\\{[]^".Contains(c))
                sb.Append('%').Append(((int)c).ToString("X2", CultureInfo.InvariantCulture));
            else
                sb.Append(c);
        }
        return sb.ToString();
    }

    /// <summary>A table-relative file path as the log writes it: a URI path, percent-encoded.</summary>
    private static string EncodeLogPath(string relative) =>
        string.Join('/', relative.Split('/').Select(segment => Uri.EscapeDataString(segment).Replace("%3D", "=")));

    /// <summary>The partition values of one group in the text form of the log; null for a null value.</summary>
    private static Dictionary<string, string?> PartitionValues(DataFrameHandle group, StructType schema, string[] partitions)
    {
        var values = new Dictionary<string, string?>(StringComparer.Ordinal);
        if (partitions.Length == 0) return values;

        var exprs = partitions.Select(p => PolarsWrapper.Alias(PolarsWrapper.First(Comparable(p, schema.Field(p)!.Type), ignoreNulls: false), p)).ToArray();
        using JsonDocument row = OneRow(group, exprs);
        foreach (string p in partitions)
        {
            JsonElement v = row.RootElement.GetProperty(p);
            values[p] = v.ValueKind == JsonValueKind.Null ? null : FormatValue(v, schema.Field(p)!.Type, forStatistics: false);
            if (values[p] is { Length: 0 }) values[p] = null; // an empty string partition is null, as Spark writes it
        }
        return values;
    }

    /// <summary>The <c>stats</c> JSON of a data file: row count, and per column minimum, maximum and null count.</summary>
    internal static string Statistics(DataFrameHandle group, StructField[] fields)
    {
        var exprs = new List<ExprHandle>();
        var withBounds = new List<StructField>();
        var leaves = new List<StructField>();
        foreach (StructField field in fields)
        {
            if (field.Type is StructType or ArrayType or MapType) continue; // statistics of top-level leaves only
            leaves.Add(field);
            exprs.Add(PolarsWrapper.Alias(PolarsWrapper.NullCount(PolarsWrapper.Col(field.Name)), "n:" + field.Name));
            if (field.Type is DecimalType || field.Type is PrimitiveType { Name: not (PrimitiveType.Boolean or PrimitiveType.Binary) })
            {
                withBounds.Add(field);
                exprs.Add(PolarsWrapper.Alias(PolarsWrapper.Min(Comparable(field.Name, field.Type)), "lo:" + field.Name));
                exprs.Add(PolarsWrapper.Alias(PolarsWrapper.Max(Comparable(field.Name, field.Type)), "hi:" + field.Name));
            }
        }

        long rows = PolarsWrapper.DataFrameHeight(group);
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, Kaminari.Seseragi.Lakehouse.Delta.Actions.ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteNumber("numRecords", rows);
            if (exprs.Count > 0)
            {
                using JsonDocument row = OneRow(group, [.. exprs]);
                JsonElement r = row.RootElement;
                WriteBounds(json, "minValues", withBounds, f => r.GetProperty("lo:" + f.Name), isMax: false);
                WriteBounds(json, "maxValues", withBounds, f => r.GetProperty("hi:" + f.Name), isMax: true);
                json.WriteStartObject("nullCount");
                foreach (StructField f in leaves) json.WriteNumber(f.PhysicalName, r.GetProperty("n:" + f.Name).GetInt64());
                json.WriteEndObject();
            }
            json.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WriteBounds(Utf8JsonWriter json, string section, List<StructField> fields, Func<StructField, JsonElement> value, bool isMax)
    {
        json.WriteStartObject(section);
        foreach (StructField field in fields)
        {
            JsonElement v = value(field);
            if (v.ValueKind == JsonValueKind.Null) continue;
            if (field.Type is PrimitiveType { Name: PrimitiveType.String })
            {
                string s = v.GetString()!;
                // A cut minimum is still a lower bound; a cut maximum would not be an upper bound, so it is left out.
                if (s.Length > StringStatsLength)
                {
                    if (isMax) continue;
                    s = s[..StringStatsLength];
                    if (char.IsHighSurrogate(s[^1])) s = s[..^1];
                }
                json.WriteString(field.PhysicalName, s);
                continue;
            }
            if (v.ValueKind == JsonValueKind.Number && field.Type is PrimitiveType { Name: PrimitiveType.Float or PrimitiveType.Double }
                && !double.IsFinite(v.GetDouble()))
                continue;
            string? text = FormatValue(v, field.Type, forStatistics: true);
            if (text is null) continue;
            json.WritePropertyName(field.PhysicalName);
            if (field.Type is DecimalType || field.Type is PrimitiveType { Name: PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Long or PrimitiveType.Float or PrimitiveType.Double })
                json.WriteRawValue(text);
            else
                json.WriteStringValue(text);
        }
        json.WriteEndObject();
    }

    /// <summary>A column in a form whose JSON is exact: dates as day numbers, timestamps as microseconds.</summary>
    private static ExprHandle Comparable(string column, DeltaType type)
    {
        PlDataType? as_ = type switch
        {
            PrimitiveType { Name: PrimitiveType.Date } => PlDataType.Int32,
            PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } => PlDataType.Int64,
            _ => null,
        };
        ExprHandle col = PolarsWrapper.Col(column);
        if (as_ is not PlDataType target) return col;
        using DataTypeHandle t = PolarsWrapper.NewPrimitiveType((int)target);
        return PolarsWrapper.ExprCast(col, PolarsWrapper.DataTypeExprFromDataType(t), strict: true, wrapNumerical: false);
    }

    /// <summary>A value as the log writes it, from the JSON of its <see cref="Comparable"/> form.</summary>
    private static string? FormatValue(JsonElement v, DeltaType type, bool forStatistics)
    {
        switch (type)
        {
            case PrimitiveType { Name: PrimitiveType.Date }:
                return DateOnly.FromDayNumber(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + v.GetInt32())
                    .ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            case PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } p:
            {
                DateTime t = DateTime.UnixEpoch.AddTicks(v.GetInt64() * 10);
                bool utc = p.Name == PrimitiveType.Timestamp;
                // Statistics keep milliseconds (readers allow for the cut); partition values keep microseconds.
                return forStatistics
                    ? t.AddTicks(-(t.Ticks % TimeSpan.TicksPerMillisecond)).ToString(utc ? "yyyy-MM-dd'T'HH:mm:ss.fff'Z'" : "yyyy-MM-dd'T'HH:mm:ss.fff", CultureInfo.InvariantCulture)
                    : t.ToString(utc ? "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'" : "yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
            }
            case PrimitiveType { Name: PrimitiveType.Boolean }:
                return v.GetBoolean() ? "true" : "false";
            default:
                return v.ValueKind == JsonValueKind.String ? v.GetString() : v.GetRawText();
        }
    }

    internal static JsonDocument OneRow(DataFrameHandle df, ExprHandle[] exprs)
    {
        LazyFrameHandle lf = PolarsWrapper.LazySelect(PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(df)), exprs);
        using DataFrameHandle result = PolarsWrapper.LazyCollect(lf, PlEngine.Auto);
        using NativeBuffer buffer = PolarsWrapper.WriteJsonNativeMemory(result, PlJsonFormat.JsonLines);
        return JsonDocument.Parse(buffer.Memory.ToArray()); // the document keeps its memory; the buffer does not outlive this call
    }
}
