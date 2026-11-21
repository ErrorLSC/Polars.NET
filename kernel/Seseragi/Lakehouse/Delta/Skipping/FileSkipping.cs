using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Pruning;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using DecimalType = Kaminari.Seseragi.Lakehouse.Delta.Schema.DecimalType;
using StructType = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;
using ParquetLogicalType = Kaminari.Seseragi.Files.Parquet.Metadata.LogicalType;

namespace Kaminari.Seseragi.Lakehouse.Delta.Skipping;

/// <summary>
/// Decides, from a file's partition values and statistics alone, whether a data file may hold a row a filter keeps.
/// The filter is the query compiler's IR (<see cref="ScalarExpr"/>, columns by logical name, struct fields through
/// <c>StructFieldByName</c>): it is compiled by the Parquet reader's statistics pruning, with each data file as one
/// unit whose statistics are the log's. The table's columns are laid out as Spark writes them in Parquet (the
/// statistics' types), arrays, maps and variants left out (nothing is known of their values).
/// </summary>
/// <remarks>
/// The log's statistics are read by Delta's rules, all of which only widen what a file may hold: timestamps are cut
/// to milliseconds (the maximum may be 999 µs later); a string minimum may be cut (still a lower bound; a cut
/// maximum would be no upper bound, so writers leave it out); integers and floats written as text are not trusted; the statistics of files from before a float was
/// widened to double are float values (their bounds are widened by a float step); a file without a column's
/// statistics, or without statistics, may hold anything. A partition column holds the file's partition value in
/// every row.
/// </remarks>
internal sealed class FileSkipping
{
    private const long MicrosPerMilli = 1000;

    private readonly PruneNode _root;
    private readonly Leaf[] _leaves;

    private FileSkipping(PruneNode root, Leaf[] leaves) => (_root, _leaves) = (root, leaves);

    /// <summary>The filter compiled for the table; null when the statistics can rule no file out.</summary>
    /// <param name="schema">The table's schema.</param>
    /// <param name="partitionColumns">The partition columns, by logical name.</param>
    /// <param name="filter">The rows a query keeps.</param>
    public static FileSkipping? Create(StructType schema, IReadOnlyList<string> partitionColumns, ScalarExpr filter)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(partitionColumns);
        ArgumentNullException.ThrowIfNull(filter);
        var leaves = new List<Leaf>();
        var elements = new List<SchemaElement> { new() { Name = "schema", NumChildren = 0 } };
        int top = 0;
        foreach (StructField field in schema.Fields)
        {
            bool partition = partitionColumns.Contains(field.Name);
            if (Add(field, [], partition, elements, leaves)) top++;
        }
        elements[0] = new SchemaElement { Name = "schema", NumChildren = top };
        if (leaves.Count == 0) return null;

        SchemaElement[] all = [.. elements];
        var meta = new FileMetaData
        {
            Version = 1,
            Schema = all,
            NumRows = 0,
            RowGroups = [],
            ColumnOrders = [.. leaves.Select(_ => ColumnOrder.TypeDefinedOrder)],
        };
        var compiler = new FilterCompiler(ParquetSchemaTree.Build(all), meta, new ParquetReadOptions());
        // The canonical shapes the compiler reads (a StartsWith call as the operator, constants folded), as a reader's filter.
        return compiler.Compile(FilterShapes.normalize(filter)) is { } root ? new FileSkipping(root, [.. leaves]) : null;
    }

    /// <summary>Whether <paramref name="file"/> may hold a row the filter keeps.</summary>
    public bool MightMatch(AddAction file) => (_root.Evaluate(new FileStatistics(file, _leaves)) & Outcomes.True) != 0;

    // ------------------------------------------------------------------ the table as Parquet leaves

    /// <summary>How a leaf's statistics are read.</summary>
    private enum Kind { Integer, Float, Double, Decimal, String, Boolean, Date, Timestamp }

    /// <param name="Physical">The path of physical names (the keys of the statistics and partition values).</param>
    /// <param name="Kind">How the leaf's statistics are read.</param>
    /// <param name="IsPartition">A partition column: its value is in the file's partition values.</param>
    /// <param name="Float32Stats">A double column widened from float: older files' statistics are float values.</param>
    /// <param name="Scale">The scale of a decimal.</param>
    /// <param name="Storage">The stored width of an integer or decimal: 4, 8, or the bytes of a fixed-length decimal.</param>
    private sealed record Leaf(string[] Physical, Kind Kind, bool IsPartition, bool Float32Stats, int Scale, int Storage);

    /// <summary>Adds the field's elements and leaves; false when the field has no testable leaf (and adds nothing).</summary>
    private static bool Add(StructField field, string[] parent, bool partition, List<SchemaElement> elements, List<Leaf> leaves)
    {
        string[] physical = [.. parent, field.PhysicalName];
        if (field.Type is StructType nested)
        {
            int at = elements.Count;
            elements.Add(null!); // the group, once its children are known
            int children = 0;
            foreach (StructField child in nested.Fields)
            {
                if (Add(child, physical, partition: false, elements, leaves)) children++;
            }
            if (children == 0)
            {
                elements.RemoveAt(at);
                return false;
            }
            elements[at] = new SchemaElement { Name = field.Name, Repetition = FieldRepetition.Optional, NumChildren = children };
            return true;
        }
        if (Element(field, physical, partition) is not var (element, leaf)) return false;
        elements.Add(element);
        leaves.Add(leaf);
        return true;
    }

    /// <summary>The Parquet element of a primitive field, as Spark writes it, and how its statistics are read.</summary>
    private static (SchemaElement, Leaf)? Element(StructField field, string[] physical, bool partition)
    {
        SchemaElement Make(ParquetType type, ParquetLogicalType? logical = null, ConvertedType? converted = null, int? length = null,
            int? scale = null, int? precision = null) => new()
        {
            Name = field.Name, Type = type, Repetition = FieldRepetition.Optional, LogicalType = logical, ConvertedType = converted,
            TypeLength = length, Scale = scale, Precision = precision,
        };
        Leaf L(Kind kind, int storage = 0, int scale = 0, bool float32 = false) => new(physical, kind, partition, float32, scale, storage);

        switch (field.Type)
        {
            case DecimalType d:
            {
                var logical = new ParquetLogicalType(LogicalTypeKind.Decimal) { Precision = d.Precision, Scale = d.Scale };
                if (d.Precision <= 9) return (Make(ParquetType.Int32, logical, ConvertedType.Decimal, scale: d.Scale, precision: d.Precision), L(Kind.Decimal, 4, d.Scale));
                if (d.Precision <= 18) return (Make(ParquetType.Int64, logical, ConvertedType.Decimal, scale: d.Scale, precision: d.Precision), L(Kind.Decimal, 8, d.Scale));
                return (Make(ParquetType.FixedLenByteArray, logical, ConvertedType.Decimal, length: 16, scale: d.Scale, precision: d.Precision), L(Kind.Decimal, 16, d.Scale));
            }
            case PrimitiveType p:
                return p.Name switch
                {
                    PrimitiveType.Byte => (Make(ParquetType.Int32, Integer(8)), L(Kind.Integer, 4)),
                    PrimitiveType.Short => (Make(ParquetType.Int32, Integer(16)), L(Kind.Integer, 4)),
                    PrimitiveType.Integer => (Make(ParquetType.Int32), L(Kind.Integer, 4)),
                    PrimitiveType.Long => (Make(ParquetType.Int64), L(Kind.Integer, 8)),
                    PrimitiveType.Float => (Make(ParquetType.Float), L(Kind.Float)),
                    PrimitiveType.Double => (Make(ParquetType.Double), L(Kind.Double, float32: WidenedFrom(field, PrimitiveType.Float))),
                    PrimitiveType.String => (Make(ParquetType.ByteArray, new ParquetLogicalType(LogicalTypeKind.String), ConvertedType.Utf8), L(Kind.String)),
                    PrimitiveType.Boolean => (Make(ParquetType.Boolean), L(Kind.Boolean)),
                    PrimitiveType.Date => (Make(ParquetType.Int32, new ParquetLogicalType(LogicalTypeKind.Date), ConvertedType.Date), L(Kind.Date, 4)),
                    PrimitiveType.Timestamp => (Make(ParquetType.Int64, Timestamp(utc: true)), L(Kind.Timestamp, 8)),
                    PrimitiveType.TimestampNtz => (Make(ParquetType.Int64, Timestamp(utc: false)), L(Kind.Timestamp, 8)),
                    _ => null, // binary, variant: no statistics of their values
                };
            default:
                return null; // arrays and maps: many values per row
        }

        static ParquetLogicalType Integer(int bits) => new(LogicalTypeKind.Integer) { BitWidth = (sbyte)bits, IsSigned = true };

        static ParquetLogicalType Timestamp(bool utc) =>
            new(LogicalTypeKind.Timestamp) { IsAdjustedToUtc = utc, Unit = ParquetTimeUnit.Micros, UnitMember = 2 };
    }

    /// <summary>
    /// Whether the field itself (not an element or entry inside it) once had the type <paramref name="fromType"/>,
    /// as its type widening history (<c>delta.typeChanges</c>) records.
    /// </summary>
    private static bool WidenedFrom(StructField field, string fromType)
    {
        if (!field.Metadata.TryGetValue(SchemaEvolution.TypeChangesKey, out string? json)) return false;
        try
        {
            using JsonDocument doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return true; // unreadable history: assume the worst
            foreach (JsonElement change in doc.RootElement.EnumerateArray())
            {
                if (change.ValueKind != JsonValueKind.Object) return true;
                if (change.TryGetProperty("fieldPath", out _)) continue; // a change inside an array or map
                if (change.TryGetProperty("fromType", out JsonElement from) && from.ValueKind == JsonValueKind.String && from.GetString() == fromType)
                    return true;
            }
            return false;
        }
        catch (JsonException)
        {
            return true;
        }
    }

    // ------------------------------------------------------------------ a file's statistics

    /// <summary>A data file as one unit: its statistics by leaf, in the stored representation of Parquet statistics.</summary>
    private sealed class FileStatistics(AddAction file, Leaf[] leaves) : IUnitStatistics
    {
        public bool TryGet(int leaf, LeafDomain domain, out LeafStatistics statistics)
        {
            statistics = default;
            if ((uint)leaf >= (uint)leaves.Length) return false;
            Leaf column = leaves[leaf];
            FileStats? stats = file.ParsedStats;
            long? records = stats?.NumRecords is long n && n >= 0 ? n : null;

            if (column.IsPartition)
            {
                // Every row holds the partition value; a missing entry tells nothing.
                if (!file.PartitionValues.TryGetValue(column.Physical[0], out string? text)) return false;
                long rows = Math.Max(records ?? 1, 1);
                if (text is null)
                {
                    statistics = new LeafStatistics(null, null, rows, rows, 0);
                    return true;
                }
                if (FromText(text, column) is not { } value) return false;
                statistics = new LeafStatistics(value, value, 0, rows, 0);
                return true;
            }

            if (stats is null || records is not long count) return false;
            long? nulls = stats.NullCount(column.Physical) is long k && k >= 0 && k <= count ? k : null;
            byte[]? min = stats.TryGetMin(column.Physical, out JsonElement minJson) ? FromStat(minJson, column) : null;
            byte[]? max = stats.TryGetMax(column.Physical, out JsonElement maxJson) ? FromStat(maxJson, column) : null;
            switch (column.Kind)
            {
                case Kind.Timestamp when max is not null:
                    // Cut to milliseconds: the true maximum may be up to 999 microseconds later.
                    max = Int64Bytes(BinaryPrimitives.ReadInt64LittleEndian(max) + MicrosPerMilli - 1);
                    break;
                case Kind.Double when column.Float32Stats:
                    // Statistics of a file from before the widening are float values, as their decimal text.
                    if (min is not null) min = DoubleBytes(MathF.BitDecrement((float)BitConverter.ToDouble(min)));
                    if (max is not null) max = DoubleBytes(MathF.BitIncrement((float)BitConverter.ToDouble(max)));
                    break;
            }
            statistics = new LeafStatistics(min, max, nulls, count, null); // NaN counts are not written
            return true;
        }
    }

    /// <summary>A statistics value (a JSON number, or text for dates, timestamps, strings and decimals) as stored bytes; null when unusable.</summary>
    private static byte[]? FromStat(JsonElement stat, Leaf column) => stat.ValueKind switch
    {
        JsonValueKind.Number => column.Kind switch
        {
            Kind.Integer when stat.TryGetInt64(out long l) => IntegerBytes(l, column.Storage),
            Kind.Float when stat.TryGetDouble(out double d) && Fits((float)d, d) => FloatBytes((float)d),
            Kind.Double when stat.TryGetDouble(out double d) && double.IsFinite(d) => DoubleBytes(d),
            Kind.Decimal => DecimalBytes(stat.GetRawText(), column),
            _ => null,
        },
        // Integers and floats as text are not trusted; booleans have no statistics.
        JsonValueKind.String when column.Kind is Kind.String or Kind.Date or Kind.Timestamp or Kind.Decimal => FromText(stat.GetString()!, column),
        _ => null,
    };

    /// <summary>A value as the log writes partition values (text) as stored bytes; null when unusable.</summary>
    private static byte[]? FromText(string text, Leaf column)
    {
        switch (column.Kind)
        {
            case Kind.Integer when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l):
                return IntegerBytes(l, column.Storage);
            case Kind.Float when float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out float f) && float.IsFinite(f):
                return FloatBytes(f);
            case Kind.Double when double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d) && double.IsFinite(d):
                return DoubleBytes(d);
            case Kind.Decimal:
                return DecimalBytes(text, column);
            case Kind.String:
                return Encoding.UTF8.GetBytes(text);
            case Kind.Boolean when bool.TryParse(text, out bool b):
                return [b ? (byte)1 : (byte)0];
            case Kind.Date when DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly date):
                return IntegerBytes(date.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber, 4);
            case Kind.Timestamp when DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTimeOffset ts):
                return Int64Bytes((ts.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10);
            default:
                return null;
        }
    }

    private static bool Fits(float f, double d) => float.IsFinite(f) && double.IsFinite(d);

    private static byte[]? IntegerBytes(long value, int width)
    {
        if (width == 4)
        {
            if (value is < int.MinValue or > int.MaxValue) return null;
            byte[] four = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(four, (int)value);
            return four;
        }
        return Int64Bytes(value);
    }

    private static byte[] Int64Bytes(long value)
    {
        byte[] bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] FloatBytes(float value) => BitConverter.GetBytes(value);

    private static byte[] DoubleBytes(double value) => BitConverter.GetBytes(value);

    /// <summary>A decimal's text (plain, as the log writes it) as its unscaled value in the column's storage; null when it does not fit exactly.</summary>
    private static byte[]? DecimalBytes(string text, Leaf column)
    {
        if (!TryUnscaled(text, column.Scale, out BigInteger unscaled)) return null;
        switch (column.Storage)
        {
            case 4 or 8:
                return unscaled >= long.MinValue && unscaled <= long.MaxValue ? IntegerBytes((long)unscaled, column.Storage) : null;
            default:
            {
                // Big-endian two's complement, sign-extended to the fixed length.
                byte[] bytes = unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
                if (bytes.Length > column.Storage) return null;
                byte[] fixedLength = new byte[column.Storage];
                if (unscaled.Sign < 0) fixedLength.AsSpan(0, column.Storage - bytes.Length).Fill(0xFF);
                bytes.CopyTo(fixedLength, column.Storage - bytes.Length);
                return fixedLength;
            }
        }
    }

    /// <summary>The value of a plain decimal text (sign, digits, optional fraction) times 10^scale; false when inexact or not plain.</summary>
    private static bool TryUnscaled(string text, int scale, out BigInteger unscaled)
    {
        unscaled = default;
        ReadOnlySpan<char> s = text.AsSpan().Trim();
        bool negative = s.StartsWith("-");
        if (negative || s.StartsWith("+")) s = s[1..];
        int dot = s.IndexOf('.');
        ReadOnlySpan<char> whole = dot < 0 ? s : s[..dot], fraction = dot < 0 ? [] : s[(dot + 1)..];
        fraction = fraction.TrimEnd('0');
        if (whole.IsEmpty && fraction.IsEmpty || fraction.Length > scale) return false;
        foreach (char c in whole) if (!char.IsAsciiDigit(c)) return false;
        foreach (char c in fraction) if (!char.IsAsciiDigit(c)) return false;
        string digits = string.Concat(whole, fraction, new string('0', scale - fraction.Length));
        if (!BigInteger.TryParse(digits.Length == 0 ? "0" : digits, NumberStyles.None, CultureInfo.InvariantCulture, out unscaled)) return false;
        if (negative) unscaled = -unscaled;
        return true;
    }
}
