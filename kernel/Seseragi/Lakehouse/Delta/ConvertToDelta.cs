using System.Globalization;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Turns a directory of Parquet files into a Delta table in place, as Spark's <c>CONVERT TO DELTA parquet.`…`</c>:
/// version 0 lists every file (none is rewritten), with the schema their footers hold and, unless asked not to, the
/// statistics their footers give.
/// </summary>
public static class ConvertToDelta
{
    /// <summary>The operation name, as Spark records it.</summary>
    public const string Operation = "CONVERT";

    private const string NullPartition = "__HIVE_DEFAULT_PARTITION__";

    /// <summary>
    /// Converts the directory; returns 0, or null when it holds a Delta table already (nothing is done, as in Spark).
    /// Every file outside directories (and files) whose names start with <c>_</c> or <c>.</c> is read as Parquet. The
    /// schema is the union of the files' columns (the first file's order, others' new columns after it); a column
    /// whose type differs between files is refused, as Spark refuses it. Partitioned data lies in Hive directories
    /// (<c>key=value</c>, the value percent-encoded, <c>__HIVE_DEFAULT_PARTITION__</c> for null), whose keys are
    /// <paramref name="partitionSchema"/>'s columns; those come last in the table's schema.
    /// </summary>
    /// <param name="fileSystem">The directory's storage.</param>
    /// <param name="partitionSchema">The partition columns and their types; null or empty for none.</param>
    /// <param name="collectStatistics">Give each file the statistics of its footer (Spark's default); false for none
    /// (Spark's <c>NO STATISTICS</c>).</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">The directory holds no file, a file is not Parquet or holds a type Delta has
    /// not, the files' columns disagree, or a file's directories do not match the partition columns.</exception>
    public static long? Convert(IWritableFileSystem fileSystem, StructType? partitionSchema, bool collectStatistics = true, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        IReadOnlyList<StructField> partitions = partitionSchema?.Fields ?? [];
        if (Log.LogSegment.ListCommits(fileSystem).Count > 0) return null;

        FileEntry[] files = [.. fileSystem.ListAll("").Where(f => !f.Path.Split('/').Any(part => part.StartsWith('_') || part.StartsWith('.'))).OrderBy(f => f.Path, StringComparer.Ordinal)];
        if (files.Length == 0) throw new DeltaException("No file found in the directory: there is nothing to convert.");

        (long Size, ParquetMetadata Footer)[] footers;
        try
        {
            footers = ParquetFooters.Read(fileSystem, [.. files.Select(f => f.Path)]);
        }
        catch (Exception e) when (e is InvalidDataException or DeltaException or FormatException)
        {
            throw new DeltaException($"Could not read the footers of the files: every file of the directory is read as Parquet ({e.Message}).", e);
        }

        StructType? data = null;
        for (int i = 0; i < files.Length; i++)
        {
            StructType held = SchemaOf(footers[i].Footer, files[i].Path);
            held = new StructType([.. held.Fields.Where(f => !partitions.Any(p => string.Equals(p.Name, f.Name, StringComparison.OrdinalIgnoreCase)))]);
            data = data is null ? held : Merge(data, held, files[i].Path);
        }
        var schema = new StructType([.. data!.Fields, .. partitions.Select(p => new StructField { Name = p.Name, Type = p.Type, Nullable = true })]);

        var metadata = new MetadataAction
        {
            Id = Guid.NewGuid().ToString(), SchemaString = schema.ToJson(), PartitionColumns = [.. partitions.Select(p => p.Name)],
            Configuration = new Dictionary<string, string>(), CreatedTime = (now ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds(),
        };
        ProtocolAction protocol = SchemaEvolution.HasNtz(schema)
            ? TableFeatures.WithFeatures(new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, [TableFeatures.TimestampNtz])
            : new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 };

        Transaction transaction = Transaction.Create(fileSystem, protocol, metadata, Operation)
            .WithParameter("numFiles", files.Length.ToString(CultureInfo.InvariantCulture))
            .WithParameter("partitionedBy", JsonText.StringArray(partitions.Select(p => p.Name)))
            .WithParameter("collectStats", collectStatistics)
            .WithParameter("sourceFormat", "parquet")
            .WithMetric("numConvertedFiles", files.Length);
        for (int i = 0; i < files.Length; i++)
        {
            transaction.Add(new AddAction
            {
                Path = EncodedPath(files[i].Path), PartitionValues = PartitionValues(files[i].Path, partitions), Size = files[i].Size,
                ModificationTime = files[i].LastModified.ToUnixTimeMilliseconds(), DataChange = true,
                Stats = collectStatistics ? FileStatistics.FromParquet(footers[i].Footer, data.Fields) : null,
            });
        }
        return transaction.Commit(now);
    }

    /// <summary>
    /// The columns a Parquet file holds, as Spark reads them: unsigned integers widened (UINT64 refused: Spark reads it
    /// as DECIMAL(20,0), which the engine cannot),
    /// timestamps adjusted to UTC as TIMESTAMP and the others as TIMESTAMP_NTZ, INT96 as TIMESTAMP, fixed-length
    /// binaries as BINARY; every column nullable.
    /// </summary>
    /// <param name="footer">The file's footer.</param>
    /// <param name="file">The file, named in refusals.</param>
    /// <exception cref="DeltaException">The file holds a type Delta has not (times, nanosecond timestamps, …).</exception>
    public static StructType SchemaOf(ParquetMetadata footer, string file)
    {
        ArgumentNullException.ThrowIfNull(footer);
        int at = 1;
        ParquetSchemaElement root = footer.Schema[0];
        var fields = new List<StructField>(root.ChildCount);
        for (int i = 0; i < root.ChildCount; i++) fields.Add(Field(footer.Schema, ref at, file));
        return new StructType(fields);
    }

    private static StructField Field(IReadOnlyList<ParquetSchemaElement> schema, ref int at, string file)
    {
        ParquetSchemaElement element = schema[at++];
        DeltaType type = TypeOf(element, schema, ref at, file);
        // A repeated field outside a LIST or MAP group is a list of itself (Parquet's legacy one-level list).
        if (element.Repetition == 2) type = new ArrayType(type, containsNull: false);
        return new StructField { Name = element.Name, Type = type, Nullable = true };
    }

    private static DeltaType TypeOf(ParquetSchemaElement element, IReadOnlyList<ParquetSchemaElement> schema, ref int at, string file)
    {
        if (element.ChildCount == 0) return Leaf(element, file);
        if (element.IsVariant) throw Unsupported(element, file, "VARIANT");
        int start = at;
        var children = new List<StructField>(element.ChildCount);
        for (int i = 0; i < element.ChildCount; i++) children.Add(Field(schema, ref at, file));
        if (element.IsList && children is [{ } single])
        {
            ParquetSchemaElement repeated = schema[start];
            // Three levels (list > repeated group > element), or two (list > repeated element).
            if (repeated.ChildCount == 1 && single.Type is ArrayType { ElementType: StructType { Fields: [{ } inner] } })
                return new ArrayType(inner.Type, containsNull: schema[start + 1].Repetition != 0);
            if (single.Type is ArrayType a) return new ArrayType(a.ElementType, containsNull: false);
        }
        if (element.IsMap && children is [{ Type: ArrayType { ElementType: StructType { Fields: [{ } key, { } value] } } }])
            return new MapType(key.Type, value.Type, valueContainsNull: true);
        return new StructType(children);
    }

    private static DeltaType Leaf(ParquetSchemaElement e, string file)
    {
        if (e.IsTime) throw Unsupported(e, file, "TIME");
        if (e.Annotation is { } annotation && annotation is not "UUID") throw Unsupported(e, file, annotation);
        if (e.DecimalScale is int scale)
            return e.DecimalPrecision is int precision ? new DecimalType(precision, scale) : throw Unsupported(e, file, "DECIMAL without a precision");
        return e.PhysicalType switch
        {
            0 => new PrimitiveType(PrimitiveType.Boolean),
            1 when e.IsDate => new PrimitiveType(PrimitiveType.Date),
            1 => new PrimitiveType((e.IntBitWidth, e.IsUnsigned) switch
            {
                (8, false) => PrimitiveType.Byte,
                (8, true) or (16, false) => PrimitiveType.Short,
                (16, true) => PrimitiveType.Integer,
                (_, true) => PrimitiveType.Long,
                _ => PrimitiveType.Integer,
            }),
            2 when e.TimeUnit is ParquetTimeUnit.Nanos => throw Unsupported(e, file, "TIMESTAMP(NANOS)"),
            2 when e.TimeUnit is not null => new PrimitiveType(e.IsAdjustedToUtc ? PrimitiveType.Timestamp : PrimitiveType.TimestampNtz),
            // Spark reads UINT64 as DECIMAL(20,0); the engine cannot read such a file as that type, so the table would
            // be one the engine cannot read.
            2 when e.IsUnsigned => throw Unsupported(e, file, "UINT64, which Spark reads as DECIMAL(20,0) and this engine cannot"),
            2 => new PrimitiveType(PrimitiveType.Long),
            3 => new PrimitiveType(PrimitiveType.Timestamp), // INT96
            4 => new PrimitiveType(PrimitiveType.Float),
            5 => new PrimitiveType(PrimitiveType.Double),
            6 when e.IsString => new PrimitiveType(PrimitiveType.String),
            6 or 7 => new PrimitiveType(PrimitiveType.Binary),
            _ => throw Unsupported(e, file, $"physical type {e.PhysicalType}"),
        };
    }

    private static DeltaException Unsupported(ParquetSchemaElement e, string file, string what) =>
        new($"The column '{e.Name}' of {file} is of a type Delta has no counterpart for ({what}); such a file cannot be converted.");

    /// <summary>The union of two files' columns, struct fields at any depth too; a type that differs is refused.</summary>
    private static StructType Merge(StructType table, StructType file, string path)
    {
        List<StructField> fields = [.. table.Fields];
        foreach (StructField field in file.Fields)
        {
            int i = fields.FindIndex(f => string.Equals(f.Name, field.Name, StringComparison.OrdinalIgnoreCase));
            if (i < 0)
            {
                fields.Add(field);
                continue;
            }
            fields[i] = new StructField { Name = fields[i].Name, Type = MergeType(fields[i].Type, field.Type, fields[i].Name, path), Nullable = true };
        }
        return new StructType(fields);

        static DeltaType MergeType(DeltaType a, DeltaType b, string name, string path) => (a, b) switch
        {
            (StructType x, StructType y) => Merge(x, y, path),
            (ArrayType x, ArrayType y) => new ArrayType(MergeType(x.ElementType, y.ElementType, name, path), x.ContainsNull || y.ContainsNull),
            (MapType x, MapType y) => new MapType(MergeType(x.KeyType, y.KeyType, name, path), MergeType(x.ValueType, y.ValueType, name, path), true),
            (PrimitiveType x, PrimitiveType y) when x.Name == y.Name => a,
            (DecimalType x, DecimalType y) when x.Precision == y.Precision && x.Scale == y.Scale => a,
            _ => throw new DeltaException($"Failed to merge the column '{name}': {path} holds it as {b}, earlier files as {a}."),
        };
    }

    /// <summary>The partition values of a file from its Hive directories, as the log writes them.</summary>
    private static Dictionary<string, string?> PartitionValues(string path, IReadOnlyList<StructField> partitions)
    {
        string[] directories = path.Split('/')[..^1];
        if (directories.Length != partitions.Count)
            throw new DeltaException(
                $"Expecting {partitions.Count} partition column(s): [{string.Join(", ", partitions.Select(p => p.Name))}], but found {directories.Length} directories in the path {path}.");
        var values = new Dictionary<string, string?>(StringComparer.Ordinal);
        for (int i = 0; i < directories.Length; i++)
        {
            int eq = directories[i].IndexOf('=', StringComparison.Ordinal);
            string key = eq < 0 ? "" : Uri.UnescapeDataString(directories[i][..eq]);
            StructField? column = partitions.FirstOrDefault(p => string.Equals(p.Name, key, StringComparison.OrdinalIgnoreCase));
            if (column is null || values.ContainsKey(column.Name))
                throw new DeltaException($"The directory '{directories[i]}' of {path} names no partition column (expected key=value with one of: {string.Join(", ", partitions.Select(p => p.Name))}).");
            string raw = directories[i][(eq + 1)..];
            string? value = raw.Length == 0 || raw == NullPartition ? null : Uri.UnescapeDataString(raw);
            if (value is not null && !Fits(value, column.Type))
                throw new DeltaException($"The partition value '{value}' of {path} is not a valid {column.Type} (the partition column {column.Name}).");
            values[column.Name] = value;
        }
        return values;
    }

    /// <summary>Whether partition text is a value of the type (as the log's partition values are written).</summary>
    private static bool Fits(string value, DeltaType type) => type switch
    {
        PrimitiveType { Name: PrimitiveType.Boolean } => bool.TryParse(value, out _),
        PrimitiveType { Name: PrimitiveType.Byte } => sbyte.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _),
        PrimitiveType { Name: PrimitiveType.Short } => short.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _),
        PrimitiveType { Name: PrimitiveType.Integer } => int.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _),
        PrimitiveType { Name: PrimitiveType.Long } => long.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _),
        PrimitiveType { Name: PrimitiveType.Float or PrimitiveType.Double } => double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _),
        DecimalType => decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out _),
        PrimitiveType { Name: PrimitiveType.Date } => DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _),
        PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } =>
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out _),
        PrimitiveType { Name: PrimitiveType.String or PrimitiveType.Binary } => true,
        _ => false,
    };

    /// <summary>A directory-relative path as the log writes it: percent-encoded segments (the <c>=</c> of a Hive key kept).</summary>
    private static string EncodedPath(string relative) =>
        string.Join('/', relative.Split('/').Select(segment => Uri.EscapeDataString(segment).Replace("%3D", "=", StringComparison.Ordinal)));
}
