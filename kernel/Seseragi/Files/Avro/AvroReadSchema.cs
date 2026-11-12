using System.Globalization;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>
/// Maps an Avro writer schema to Arrow. Unions follow spark-avro: <c>["null", T]</c> is a nullable T,
/// <c>[int, long]</c> is long, <c>[float, double]</c> is double (each ± null), and any other union is a struct
/// with nullable fields <c>member0</c>, <c>member1</c>, … for its non-null branches. A file whose top-level
/// schema is not a record is read as one column named <c>value</c>. Recursive records cannot be expressed in
/// Arrow and are refused unless projected away.
/// </summary>
internal static class AvroReadSchema
{
    public const string TopLevelValueColumn = "value";

    /// <summary>How a union reads: as a single (possibly promoted) type or as a member struct.</summary>
    internal enum UnionShape { NullOnly, Single, PromoteToLong, PromoteToDouble, MemberStruct }

    public static UnionShape ShapeOf(AvroUnionSchema union, out IReadOnlyList<int> nonNullBranches)
    {
        List<int> branches = [];
        for (int i = 0; i < union.Branches.Count; i++)
            if (union.Branches[i].Type != AvroType.Null) branches.Add(i);
        nonNullBranches = branches;
        if (branches.Count == 0) return UnionShape.NullOnly;
        if (branches.Count == 1) return UnionShape.Single;
        if (branches.Count == 2)
        {
            var a = union.Branches[branches[0]];
            var b = union.Branches[branches[1]];
            if (IsPlain(a, AvroType.Int, AvroType.Long) && IsPlain(b, AvroType.Int, AvroType.Long) && a.Type != b.Type)
                return UnionShape.PromoteToLong;
            if (IsPlain(a, AvroType.Float, AvroType.Double) && IsPlain(b, AvroType.Float, AvroType.Double) && a.Type != b.Type)
                return UnionShape.PromoteToDouble;
        }
        return UnionShape.MemberStruct;

        static bool IsPlain(AvroSchema s, AvroType x, AvroType y) =>
            (s.Type == x || s.Type == y) && s.Logical == AvroLogicalKind.None;
    }

    /// <summary>The top-level fields a file exposes (record fields, or the single <c>value</c> column).</summary>
    public static IReadOnlyList<(string Name, AvroSchema Schema, int? FieldId)> TopLevelFields(AvroSchema schema) =>
        schema is AvroRecordSchema record
            ? [.. record.Fields.Select(f => (f.Name, f.Schema, f.FieldId))]
            : [(TopLevelValueColumn, schema, null)];

    /// <summary>The Arrow field for an Avro field; an id attribute becomes <see cref="ArrowFieldIds.MetadataKey"/>
    /// metadata (on the field, the list item, or the map key and value).</summary>
    public static Field ToField(string name, AvroSchema schema, int? fieldId = null)
    {
        var type = ToArrowType(schema, out bool nullable, []);
        return NewField(name, type, nullable, fieldId);
    }

    private static Field NewField(string name, IArrowType type, bool nullable, int? fieldId) => fieldId is int id
        ? new Field(name, type, nullable, [new(ArrowFieldIds.MetadataKey, id.ToString(CultureInfo.InvariantCulture))])
        : new Field(name, type, nullable);

    private static IArrowType ToArrowType(AvroSchema schema, out bool nullable, HashSet<AvroRecordSchema> inProgress)
    {
        nullable = schema.Type == AvroType.Null || schema is AvroUnionSchema { NullIndex: >= 0 };
        bool innerNullable = false;
        var type = schema switch
        {
            AvroUnionSchema union => Union(union, out innerNullable),
            AvroRecordSchema record => Record(record),
            AvroEnumSchema => new DictionaryType(Int32Type.Default, StringType.Default, false),
            AvroFixedSchema { Logical: AvroLogicalKind.Decimal } f when IsArrowDecimal(f) => DecimalType(f.Precision, f.Scale),
            AvroFixedSchema { Logical: AvroLogicalKind.Duration } => IntervalType.MonthDayNanosecond,
            AvroFixedSchema f => new FixedSizeBinaryType(f.Size),
            AvroArraySchema a => new ListType(Child("item", a.Items, a.ElementId)),
            AvroMapSchema m => new MapType(NewField("key", StringType.Default, false, m.KeyId), Child("value", m.Values, m.ValueId)),
            _ => PrimitiveType(schema),
        };
        nullable |= innerNullable;
        return type;

        Field Child(string name, AvroSchema s, int? fieldId = null)
        {
            var t = ToArrowType(s, out bool n, inProgress);
            return NewField(name, t, n, fieldId);
        }

        IArrowType Union(AvroUnionSchema union, out bool singleNullable)
        {
            singleNullable = false;
            var shape = ShapeOf(union, out var branches);
            return shape switch
            {
                UnionShape.NullOnly => NullType.Default,
                UnionShape.Single => ToArrowType(union.Branches[branches[0]], out singleNullable, inProgress),
                UnionShape.PromoteToLong => Int64Type.Default,
                UnionShape.PromoteToDouble => DoubleType.Default,
                _ => new StructType([.. branches.Select((b, i) => new Field($"member{i}", ToArrowType(union.Branches[b], out _, inProgress), true))]),
            };
        }

        StructType Record(AvroRecordSchema record)
        {
            if (!inProgress.Add(record))
                throw new NotSupportedException(
                    $"The Avro record '{record.FullName}' is recursive, which Arrow cannot represent; project it away to read the other columns.");
            var fields = record.Fields.Select(f => Child(f.Name, f.Schema, f.FieldId)).ToList();
            inProgress.Remove(record);
            return new StructType(fields);
        }
    }

    /// <summary>Decimals up to 76 digits map to Decimal128/256; wider ones stay raw bytes.</summary>
    public static bool IsArrowDecimal(AvroSchema s) => s.Logical == AvroLogicalKind.Decimal && s.Precision <= 76;

    private static IArrowType DecimalType(int precision, int scale) =>
        precision <= 38 ? new Decimal128Type(precision, scale) : new Decimal256Type(precision, scale);

    private static IArrowType PrimitiveType(AvroSchema s) => (s.Type, s.Logical) switch
    {
        (AvroType.Null, _) => NullType.Default,
        (AvroType.Boolean, _) => BooleanType.Default,
        (AvroType.Int, AvroLogicalKind.Date) => Date32Type.Default,
        (AvroType.Int, AvroLogicalKind.TimeMillis) => TimeType.Millisecond,
        (AvroType.Int, AvroLogicalKind.SparkYearMonthInterval) => IntervalType.YearMonth,
        (AvroType.Int, _) => Int32Type.Default,
        (AvroType.Long, AvroLogicalKind.TimeMicros) => TimeType.Microsecond,
        (AvroType.Long, AvroLogicalKind.TimestampMillis) => new TimestampType(TimeUnit.Millisecond, s.Timezone ?? "UTC"),
        (AvroType.Long, AvroLogicalKind.TimestampMicros) => new TimestampType(TimeUnit.Microsecond, s.Timezone ?? "UTC"),
        (AvroType.Long, AvroLogicalKind.TimestampNanos) => new TimestampType(TimeUnit.Nanosecond, s.Timezone ?? "UTC"),
        (AvroType.Long, AvroLogicalKind.LocalTimestampMillis) => new TimestampType(TimeUnit.Millisecond, (string?)null),
        (AvroType.Long, AvroLogicalKind.LocalTimestampMicros) => new TimestampType(TimeUnit.Microsecond, (string?)null),
        (AvroType.Long, AvroLogicalKind.LocalTimestampNanos) => new TimestampType(TimeUnit.Nanosecond, (string?)null),
        (AvroType.Long, AvroLogicalKind.SparkDayTimeInterval) => DurationType.Microsecond,
        (AvroType.Long, _) => Int64Type.Default,
        (AvroType.Float, _) => FloatType.Default,
        (AvroType.Double, _) => DoubleType.Default,
        (AvroType.Bytes, AvroLogicalKind.Decimal) when IsArrowDecimal(s) => DecimalType(s.Precision, s.Scale),
        (AvroType.Bytes, _) => BinaryType.Default,
        (AvroType.String, _) => StringType.Default,
        _ => throw new InvalidOperationException($"Unexpected Avro type {s.Type}."),
    };
}
