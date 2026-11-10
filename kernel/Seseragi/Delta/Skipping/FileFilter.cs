using System.Globalization;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Delta.Actions;
using Kaminari.Seseragi.Delta.Schema;

namespace Kaminari.Seseragi.Delta.Skipping;

/// <summary>
/// Decides, from a file's partition values and statistics alone, whether the file can hold a row matching a
/// predicate. It never drops a file that might: whenever something is unknown the file is kept.
/// </summary>
public sealed class FileFilter
{
    private readonly Node _root;

    /// <summary>Prepares a predicate for a table.</summary>
    /// <param name="schema">The table schema.</param>
    /// <param name="partitionColumns">The partition columns, by logical name.</param>
    /// <param name="predicate">The condition rows must meet.</param>
    public FileFilter(StructType schema, IReadOnlyList<string> partitionColumns, Predicate predicate)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(partitionColumns);
        ArgumentNullException.ThrowIfNull(predicate);
        _root = Build(schema, partitionColumns, predicate, negated: false);
    }

    /// <summary>False only when no row of the file can match.</summary>
    public bool MightMatch(AddAction file)
    {
        ArgumentNullException.ThrowIfNull(file);
        return _root.MightMatch(file);
    }

    // ------------------------------------------------------------------
    // Preparation: push negations down to the leaves and resolve the columns once.
    // ------------------------------------------------------------------

    private static Node Build(StructType schema, IReadOnlyList<string> partitionColumns, Predicate predicate, bool negated)
    {
        switch (predicate)
        {
            case NotPredicate not:
                return Build(schema, partitionColumns, not.Child, !negated);

            case JunctionPredicate junction:
            {
                var children = new Node[junction.Children.Length];
                for (int i = 0; i < children.Length; i++) children[i] = Build(schema, partitionColumns, junction.Children[i], negated);
                // De Morgan: a negated AND is an OR of the negated children, and the other way round.
                return new JunctionNode(junction.IsAnd != negated, children);
            }

            case NullPredicate isNull:
            {
                Column? column = Resolve(schema, partitionColumns, isNull.Column);
                return column is null ? KeepNode.Instance : new NullNode(column, isNull.WantNull != negated);
            }

            case ComparePredicate compare:
            {
                Column? column = Resolve(schema, partitionColumns, compare.Column);
                if (column is null || !TryConvertLiteral(compare.Value, column.Kind, out Value literal)) return KeepNode.Instance;
                CompareOp op = negated ? Negate(compare.Op) : compare.Op;
                return new CompareNode(column, op, literal);
            }

            default:
                return KeepNode.Instance;
        }
    }

    private static CompareOp Negate(CompareOp op) => op switch
    {
        CompareOp.Eq => CompareOp.Ne,
        CompareOp.Ne => CompareOp.Eq,
        CompareOp.Lt => CompareOp.Ge,
        CompareOp.Le => CompareOp.Gt,
        CompareOp.Gt => CompareOp.Le,
        _ => CompareOp.Lt,
    };

    private enum Kind { Integer, Float, Decimal, String, Boolean, Date, Timestamp }

    /// <summary>A column of the table as the log knows it: by physical names.</summary>
    private sealed record Column(string[] PhysicalPath, Kind Kind, bool IsPartition);

    private static Column? Resolve(StructType schema, IReadOnlyList<string> partitionColumns, ColumnPath path)
    {
        if (path.Segments is null) return null;
        var physical = new string[path.Segments.Length];
        StructType current = schema;
        for (int i = 0; i < path.Segments.Length; i++)
        {
            StructField? field = current.Field(path.Segments[i]);
            if (field is null) return null;
            physical[i] = field.PhysicalName;

            if (i < path.Segments.Length - 1)
            {
                if (field.Type is not StructType next) return null;
                current = next;
                continue;
            }

            Kind? kind = field.Type switch
            {
                DecimalType => Kind.Decimal,
                PrimitiveType p => p.Name switch
                {
                    PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Long => Kind.Integer,
                    PrimitiveType.Float or PrimitiveType.Double => Kind.Float,
                    PrimitiveType.String => Kind.String,
                    PrimitiveType.Boolean => Kind.Boolean,
                    PrimitiveType.Date => Kind.Date,
                    PrimitiveType.Timestamp or PrimitiveType.TimestampNtz => Kind.Timestamp,
                    _ => null,
                },
                _ => null,
            };
            if (kind is null) return null;
            bool isPartition = path.Segments.Length == 1 && partitionColumns.Contains(path.Segments[0]);
            return new Column(physical, kind.Value, isPartition);
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Values: one comparable form per kind.
    // ------------------------------------------------------------------

    /// <summary>Integer, date (days) and timestamp (microseconds) in Long; the others in their own field.</summary>
    private readonly record struct Value(Kind Kind, long Long, double Double, decimal Decimal, string? String)
    {
        public int CompareTo(Value other) => Kind switch
        {
            Kind.Float => Double.CompareTo(other.Double),
            Kind.Decimal => Decimal.CompareTo(other.Decimal),
            // Strings order by their UTF-8 bytes, as the engines that write the statistics do.
            Kind.String => Encoding.UTF8.GetBytes(String!).AsSpan().SequenceCompareTo(Encoding.UTF8.GetBytes(other.String!)),
            _ => Long.CompareTo(other.Long),
        };
    }

    private static Value L(Kind kind, long value) => new(kind, value, 0, 0, null);

    private const long MicrosPerMilli = 1000;
    private static long ToMicros(DateTimeOffset value) => (value.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10;

    private static bool TryConvertLiteral(object literal, Kind kind, out Value value)
    {
        value = default;
        try
        {
            switch (kind)
            {
                case Kind.Integer when literal is sbyte or byte or short or ushort or int or uint or long:
                    value = L(kind, Convert.ToInt64(literal, CultureInfo.InvariantCulture));
                    return true;
                case Kind.Float when literal is float or double or sbyte or byte or short or ushort or int or uint or long:
                    double d = Convert.ToDouble(literal, CultureInfo.InvariantCulture);
                    if (double.IsNaN(d)) return false;
                    value = new Value(kind, 0, d, 0, null);
                    return true;
                case Kind.Decimal when literal is decimal or sbyte or byte or short or ushort or int or uint or long:
                    value = new Value(kind, 0, 0, Convert.ToDecimal(literal, CultureInfo.InvariantCulture), null);
                    return true;
                case Kind.String when literal is string s:
                    value = new Value(kind, 0, 0, 0, s);
                    return true;
                case Kind.Boolean when literal is bool b:
                    value = L(kind, b ? 1 : 0);
                    return true;
                case Kind.Date when literal is DateOnly date:
                    value = L(kind, date.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber);
                    return true;
                case Kind.Timestamp when literal is DateTimeOffset dto:
                    value = L(kind, ToMicros(dto));
                    return true;
                case Kind.Timestamp when literal is DateTime dt:
                    // A DateTime that does not say otherwise is taken as UTC, like the values in the log.
                    value = L(kind, ToMicros(new DateTimeOffset(dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : DateTime.SpecifyKind(dt, DateTimeKind.Utc))));
                    return true;
            }
        }
        catch (OverflowException)
        {
        }
        return false;
    }

    /// <summary>Reads a partition value, which the log keeps as text.</summary>
    private static bool TryParseText(string text, Kind kind, out Value value)
    {
        value = default;
        switch (kind)
        {
            case Kind.Integer when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l):
                value = L(kind, l);
                return true;
            case Kind.Float when double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d) && !double.IsNaN(d):
                value = new Value(kind, 0, d, 0, null);
                return true;
            case Kind.Decimal when decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal m):
                value = new Value(kind, 0, 0, m, null);
                return true;
            case Kind.String:
                value = new Value(kind, 0, 0, 0, text);
                return true;
            case Kind.Boolean when bool.TryParse(text, out bool b):
                value = L(kind, b ? 1 : 0);
                return true;
            case Kind.Date when DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly date):
                value = L(kind, date.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber);
                return true;
            case Kind.Timestamp when DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTimeOffset ts):
                value = L(kind, ToMicros(ts));
                return true;
        }
        return false;
    }

    /// <summary>Reads a statistics value, which is a JSON number or string.</summary>
    private static bool TryParseStat(JsonElement stat, Kind kind, out Value value)
    {
        value = default;
        switch (stat.ValueKind)
        {
            case JsonValueKind.Number:
                switch (kind)
                {
                    case Kind.Integer when stat.TryGetInt64(out long l):
                        value = L(kind, l);
                        return true;
                    case Kind.Float when stat.TryGetDouble(out double d) && !double.IsNaN(d):
                        value = new Value(kind, 0, d, 0, null);
                        return true;
                    case Kind.Decimal when stat.TryGetDecimal(out decimal m):
                        value = new Value(kind, 0, 0, m, null);
                        return true;
                }
                return false;
            case JsonValueKind.String:
                // Booleans have no statistics; a number written as text is not trusted.
                return kind is Kind.String or Kind.Date or Kind.Timestamp && TryParseText(stat.GetString()!, kind, out value);
            default:
                return false;
        }
    }

    // ------------------------------------------------------------------
    // Evaluation
    // ------------------------------------------------------------------

    private abstract class Node
    {
        public abstract bool MightMatch(AddAction file);
    }

    private sealed class KeepNode : Node
    {
        public static readonly KeepNode Instance = new();
        public override bool MightMatch(AddAction file) => true;
    }

    private sealed class JunctionNode(bool isAnd, Node[] children) : Node
    {
        public override bool MightMatch(AddAction file)
        {
            foreach (Node child in children)
            {
                bool might = child.MightMatch(file);
                if (isAnd && !might) return false;
                if (!isAnd && might) return true;
            }
            return isAnd;
        }
    }

    private sealed class NullNode(Column column, bool wantNull) : Node
    {
        public override bool MightMatch(AddAction file)
        {
            if (column.IsPartition)
            {
                // Every row of the file has the partition value. A missing entry tells nothing.
                return !file.PartitionValues.TryGetValue(column.PhysicalPath[0], out string? text) || (text is null) == wantNull;
            }

            FileStats? stats = file.ParsedStats;
            long? nulls = stats?.NullCount(column.PhysicalPath);
            if (nulls is null) return true;
            return wantNull ? nulls != 0 : nulls != stats!.NumRecords;
        }
    }

    private sealed class CompareNode(Column column, CompareOp op, Value literal) : Node
    {
        public override bool MightMatch(AddAction file)
        {
            if (column.IsPartition)
            {
                if (!file.PartitionValues.TryGetValue(column.PhysicalPath[0], out string? text)) return true;
                if (text is null) return false; // a comparison with null matches nothing
                if (!TryParseText(text, column.Kind, out Value value)) return true;
                int c = value.CompareTo(literal);
                return op switch
                {
                    CompareOp.Eq => c == 0,
                    CompareOp.Ne => c != 0,
                    CompareOp.Lt => c < 0,
                    CompareOp.Le => c <= 0,
                    CompareOp.Gt => c > 0,
                    _ => c >= 0,
                };
            }

            FileStats? stats = file.ParsedStats;
            if (stats is null) return true;

            // A column that is null in every row matches no comparison.
            long? nulls = stats.NullCount(column.PhysicalPath);
            if (nulls is not null && nulls == stats.NumRecords) return false;

            bool hasMin = stats.TryGetMin(column.PhysicalPath, out JsonElement minJson) && TryParseStat(minJson, column.Kind, out _);
            bool hasMax = stats.TryGetMax(column.PhysicalPath, out JsonElement maxJson) && TryParseStat(maxJson, column.Kind, out _);
            Value min = default, max = default;
            if (hasMin) TryParseStat(minJson, column.Kind, out min);
            if (hasMax)
            {
                TryParseStat(maxJson, column.Kind, out max);
                // Timestamps in statistics are cut to milliseconds, so the true maximum may be up to 999 microseconds later.
                if (column.Kind == Kind.Timestamp) max = L(Kind.Timestamp, max.Long + MicrosPerMilli - 1);
            }

            switch (op)
            {
                case CompareOp.Eq:
                    return !(hasMin && literal.CompareTo(min) < 0) && !(hasMax && literal.CompareTo(max) > 0);
                case CompareOp.Lt:
                    return !hasMin || min.CompareTo(literal) < 0;
                case CompareOp.Le:
                    return !hasMin || min.CompareTo(literal) <= 0;
                case CompareOp.Gt:
                    return !hasMax || max.CompareTo(literal) > 0;
                case CompareOp.Ge:
                    return !hasMax || max.CompareTo(literal) >= 0;
                default: // Ne: no row differs only when every value equals the literal
                    // Strings may be truncated and timestamps are cut, so equal bounds prove nothing for them.
                    if (column.Kind is Kind.String or Kind.Timestamp) return true;
                    return !(hasMin && hasMax && min.CompareTo(literal) == 0 && max.CompareTo(literal) == 0);
            }
        }
    }
}
